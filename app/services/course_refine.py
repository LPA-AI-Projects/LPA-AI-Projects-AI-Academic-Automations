"""
Shared course outline refine logic for Zoho and Bitrix24.
"""
from __future__ import annotations

import asyncio
import json
import os
import uuid
from asyncio import TimeoutError as AsyncTimeoutError
from asyncio import wait_for

from sqlalchemy import and_, func, or_, select
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.models.course import Course, CourseVersion
from app.models.job import CourseJob
from app.schemas.course import CourseVersionResponse
from app.services.claude import ClaudeService
from app.services.crm_outline_hooks import CrmSource
from app.services.google_drive import GoogleDriveUploadError, upload_course_outline_pdf_to_drive
from app.services.pdf_service import generate_pdf_path_async
from app.services.zoho_integration import zoho_notify_refined_outline_version
from app.utils.logger import get_logger

logger = get_logger(__name__)

REGIONS_SERVED_CONSTANT = "UAE, Saudi Arabia, Africa, MENA, and Europe"
BITRIX_OUTLINE_JOB_TYPE = "bitrix_outline"


def _enforce_regions_served_constant(payload) -> None:
    try:
        payload.course_details.regions_served = REGIONS_SERVED_CONSTANT
    except Exception:
        pass


def _build_pdf_url(file_path: str) -> str:
    filename = os.path.basename(file_path)
    return f"{settings.BASE_URL}/pdfs/{filename}"


def _job_payload_course_name(job: CourseJob) -> str:
    raw = str(job.payload_json or "").strip()
    if not raw:
        return ""
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            return str(data.get("course_name") or "").strip()
    except Exception:
        return ""
    return ""


def _derive_course_name_from_outline(outline_text: str | None) -> str:
    import re

    text = str(outline_text or "").strip()
    if not text:
        return "course"
    try:
        data = json.loads(text)
        if isinstance(data, dict):
            raw = str(data.get("course_title") or "").strip()
            if raw:
                return raw
    except Exception:
        pass
    m = re.search(r"(?im)^\s*course\s*title\s*:\s*(.+)$", text)
    if m and str(m.group(1)).strip():
        return str(m.group(1)).strip()
    lines = [ln.strip().lstrip("#").strip() for ln in text.splitlines() if ln.strip()]
    if lines:
        return lines[0][:120]
    return "course"


async def refine_course_for_record(
    record_id: str,
    feedback: str,
    course_name: str,
    *,
    crm_source: CrmSource = "zoho",
) -> CourseVersionResponse | None:
    """
    Refine latest outline version for a CRM/task record id.

    Returns None on missing course / versions (logs warning). Raises on AI/DB hard failures
  when used from HTTP handlers; Bitrix webhook background task should catch exceptions.
    """
    rid = (record_id or "").strip()
    requested_course_name = (course_name or "").strip()
    if not rid or not requested_course_name:
        logger.warning(
            "Refine skipped: missing record_id or course_name | record_id=%s",
            rid,
        )
        return None

    async with AsyncSessionLocal() as db:
        try:
            if crm_source == "bitrix":
                job_filter = CourseJob.job_type == BITRIX_OUTLINE_JOB_TYPE
            else:
                job_filter = or_(
                    CourseJob.job_type.is_(None),
                    and_(
                        CourseJob.job_type != "slides",
                        CourseJob.job_type != "assessment",
                        CourseJob.job_type != BITRIX_OUTLINE_JOB_TYPE,
                    ),
                )
            jres = await db.execute(
                select(CourseJob)
                .where(
                    CourseJob.zoho_record_id == rid,
                    CourseJob.course_id.is_not(None),
                    CourseJob.status == "completed",
                    job_filter,
                )
                .order_by(CourseJob.created_at.desc())
            )
            jobs = jres.scalars().all()
        except (SQLAlchemyError, OSError, Exception):
            logger.exception("Database error while reading course jobs for refine | record_id=%s", rid)
            return None

        if not jobs:
            logger.warning("Refine skipped: no completed jobs | record_id=%s crm=%s", rid, crm_source)
            return None

        target_course_id: uuid.UUID | None = None
        for job in jobs:
            jname = _job_payload_course_name(job)
            if jname and jname.strip().lower() == requested_course_name.strip().lower():
                target_course_id = job.course_id
                break
        if target_course_id is None:
            logger.warning(
                "Refine skipped: no matching course_name | record_id=%s requested=%s",
                rid,
                requested_course_name,
            )
            return None

        try:
            result = await db.execute(
                select(CourseVersion)
                .where(CourseVersion.course_id == target_course_id)
                .order_by(CourseVersion.version_number.desc())
            )
            versions = result.scalars().all()
        except (SQLAlchemyError, OSError, Exception):
            logger.exception("Database error while reading versions for refine | record_id=%s", rid)
            return None

        if not versions:
            logger.warning("Refine skipped: no versions | record_id=%s", rid)
            return None

        base_version = versions[0]
        base_outline = base_version.outline_text or ""
        course_uuid = target_course_id

    try:
        ai = ClaudeService()
        refined_payload = None
        try:
            logger.info("Refine AI started | record_id=%s crm=%s", rid, crm_source)
            refined_payload = await wait_for(
                ai.refine_course_outline_json(base_outline, feedback),
                timeout=600,
            )
            _enforce_regions_served_constant(refined_payload)
            updated_outline = json.dumps(refined_payload.model_dump(), ensure_ascii=False, indent=2)
        except RuntimeError:
            logger.warning("Refine AI structured mode failed, using fallback | record_id=%s", rid)
            context_text = json.dumps(
                {"previous_outline": base_outline, "feedback": feedback},
                ensure_ascii=False,
                indent=2,
            )
            updated_outline = await wait_for(
                ai.build_roi_course_outline(context_text, base_outline),
                timeout=600,
            )
            refined_payload = None
    except AsyncTimeoutError:
        logger.warning("Refine AI timed out | record_id=%s", rid)
        return None
    except (ValueError, RuntimeError) as e:
        logger.warning("Refine AI failed | record_id=%s error=%s", rid, e)
        return None

    name_for_file = requested_course_name or _derive_course_name_from_outline(base_outline)
    pdf_path: str | None = None
    pdf_url = None
    try:
        pdf_path = await generate_pdf_path_async(
            refined_payload if refined_payload is not None else updated_outline
        )
        pdf_url = _build_pdf_url(pdf_path)
    except RuntimeError as e:
        logger.warning("Refine PDF generation failed | record_id=%s error=%s", rid, e)

    async with AsyncSessionLocal() as db:
        try:
            locked = await db.execute(
                select(Course).where(Course.id == course_uuid).with_for_update()
            )
            if locked.scalars().first() is None:
                logger.warning("Refine skipped: course row missing | record_id=%s", rid)
                return None

            current_max = await db.execute(
                select(func.max(CourseVersion.version_number)).where(
                    CourseVersion.course_id == course_uuid
                )
            )
            new_version_number = int(current_max.scalar_one_or_none() or 0) + 1

            new_version = CourseVersion(
                course_id=course_uuid,
                version_number=new_version_number,
                outline_text=updated_outline,
                pdf_url=pdf_url,
                feedback=feedback,
            )
            db.add(new_version)
            await db.commit()
            await db.refresh(new_version)
            new_version_id = new_version.id
            saved_outline = new_version.outline_text or ""
            saved_pdf_url = new_version.pdf_url
            saved_created_at = new_version.created_at
        except (SQLAlchemyError, OSError, Exception):
            logger.exception("Database error while saving refined version | record_id=%s", rid)
            return None

    if pdf_path and os.path.isfile(pdf_path):
        try:
            drive_up = await asyncio.to_thread(
                upload_course_outline_pdf_to_drive,
                pdf_path,
                course_name=name_for_file,
                zoho_record_id=rid,
                version_number=new_version_number,
            )
            if drive_up and isinstance(drive_up.get("edit_link"), str) and drive_up["edit_link"].strip():
                edit_link = drive_up["edit_link"].strip()
                async with AsyncSessionLocal() as db:
                    vres = await db.execute(
                        select(CourseVersion).where(CourseVersion.id == new_version_id)
                    )
                    vrow = vres.scalars().first()
                    if vrow is not None:
                        vrow.pdf_url = edit_link
                        await db.commit()
                        await db.refresh(vrow)
                        saved_pdf_url = vrow.pdf_url
        except (GoogleDriveUploadError, Exception) as e:
            logger.warning("Refine Drive upload failed | record_id=%s error=%s", rid, e)

    if crm_source == "bitrix":
        try:
            from app.services.bitrix_tasks import deliver_outline_pdf_to_bitrix_task, send_task_comment

            await deliver_outline_pdf_to_bitrix_task(
                task_id=rid,
                pdf_path=pdf_path,
                pdf_url=saved_pdf_url,
                course_name=f"{name_for_file} v{new_version_number}",
            )
            await send_task_comment(
                rid,
                f"""Course outline refined to v{new_version_number}.

Course: {name_for_file}

PDF:
{saved_pdf_url or "(attached to this task)"}

Uploaded to Bitrix Drive.""",
            )
        except Exception:
            logger.exception("Bitrix refine delivery failed | task_id=%s", rid)
    else:
        try:
            await zoho_notify_refined_outline_version(
                zoho_record_id=rid,
                pdf_url=saved_pdf_url,
                version_number=new_version_number,
                course_name_for_title=f"{name_for_file} — outline v{new_version_number}",
            )
        except Exception:
            logger.exception("Zoho refine notify failed | record_id=%s", rid)

    logger.info(
        "Course refined | record_id=%s version=%s crm=%s",
        rid,
        new_version_number,
        crm_source,
    )
    return CourseVersionResponse(
        version_id=new_version_id,
        zoho_record_id=rid,
        version_number=new_version_number,
        pdf_url=saved_pdf_url,
        outline=saved_outline,
        created_at=saved_created_at,
    )


async def run_bitrix_comment_refine(*, task_id: str, feedback: str, course_name: str) -> None:
    """Background worker for ONTASKCOMMENTADD refine webhooks."""
    try:
        await refine_course_for_record(
            task_id,
            feedback,
            course_name,
            crm_source="bitrix",
        )
    except Exception:
        logger.exception("Bitrix comment refine failed | task_id=%s", task_id)

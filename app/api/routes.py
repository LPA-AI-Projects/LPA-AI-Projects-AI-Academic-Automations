import uuid
import os
import json
import re
from urllib.parse import parse_qs
from typing import Optional, Any

from fastapi import APIRouter, Depends, HTTPException, Header, Query, status, BackgroundTasks, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, or_, and_
from sqlalchemy.exc import SQLAlchemyError
import asyncio
from asyncio import wait_for, TimeoutError as AsyncTimeoutError

from sqlalchemy import text

from app.core.database import get_db, AsyncSessionLocal
from app.core.config import settings
from app.schemas.course import (
    CourseInputData,
    GenerateCourseRequest,
    RefineCourseRequest,
    CourseVersionResponse,
    CourseVersionsResponse,
    VersionSummary,
)
from app.schemas.integration import CourseOutlineIntegrationStatus
from app.schemas.job import CourseOutlineJobResponse, CourseOutlineQueuedResponse
from app.models.course import Course, CourseVersion
from app.models.job import CourseJob
from app.services.claude import ClaudeService
from app.services.pdf_service import generate_pdf_path_async
from app.services.google_drive import GoogleDriveUploadError, upload_course_outline_pdf_to_drive
from app.services.public_course_sheet import lookup_public_course_pdf_url
from app.services.zoho_integration import (
    zoho_notify_course_outline_job_finished,
    zoho_notify_refined_outline_version,
)
from app.utils.logger import get_logger

logger = get_logger(__name__)
router = APIRouter(prefix="/api/v1", tags=["courses"])
REGIONS_SERVED_CONSTANT = "UAE, Saudi Arabia, Africa, MENA, and Europe"


# ─── Auth dependency ──────────────────────────────────────────────────────────

def verify_api_key(
    x_api_key: Optional[str] = Header(None, description="Your API secret key"),
):
    """All routes require X-API-Key header matching API_SECRET_KEY in .env"""
    if not x_api_key or x_api_key != settings.API_SECRET_KEY:
        logger.warning("Rejected request: invalid API key")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key.",
        )


auth = Depends(verify_api_key)


def _input_data_dict_for_job(data: CourseInputData) -> dict:
    """Flatten outline job input for JSON context; drop unset optional CRM fields."""
    return data.model_dump(exclude_none=True, mode="json")


def _enforce_regions_served_constant(payload) -> None:
    """
    Keep brochure region text fixed regardless of model variation.
    """
    try:
        payload.course_details.regions_served = REGIONS_SERVED_CONSTANT
    except Exception:
        # Structured payload may be absent in fallback paths.
        pass


@router.get(
    "/integrations/course-outline-status",
    response_model=CourseOutlineIntegrationStatus,
    dependencies=[auth],
    tags=["integrations"],
    summary="Course outline: Google Drive + Zoho env configuration (no secrets)",
    description=(
        "Returns booleans only: whether OAuth/folder/webhook/CRM attach prerequisites are set. "
        "Does not validate tokens against Google or Zoho."
    ),
)
def course_outline_integration_status():
    return get_course_outline_integration_status()


def _job_to_course_outline_response(job: CourseJob) -> CourseOutlineJobResponse:
    return CourseOutlineJobResponse(
        job_id=job.id,
        zoho_record_id=job.zoho_record_id,
        status=job.status,
        pdf_url=job.pdf_url,
        version_number=job.version_number,
        error=job.error,
        created_at=job.created_at,
    )


# ─── Helper ───────────────────────────────────────────────────────────────────

def _build_pdf_url(file_path: str) -> str:
    filename = os.path.basename(file_path)
    return f"{settings.BASE_URL}/pdfs/{filename}"


def split_courses(text: str) -> list[str]:
    if not text:
        return []
    parts = re.split(r"[\n,]+", text)
    courses: list[str] = []
    for part in parts:
        cleaned = re.sub(r"^\s*\d+\s*[\.\-\)]\s*", "", str(part or "").strip())
        if cleaned:
            courses.append(cleaned)
    return courses


def _is_product_section_duration(value: str | None) -> bool:
    s = str(value or "").strip().lower()
    if not s:
        return False
    return "refer to product section" in s


def _looks_like_duration(value: str | None) -> bool:
    s = str(value or "").strip().lower()
    if not s:
        return False
    return any(tok in s for tok in ("day", "days", "week", "weeks", "hour", "hours", "hr", "hrs"))


def _parse_product_row_line(line: str) -> tuple[str, str, str] | None:
    """
    Parse one row formatted as:
      Product Name, No of Pax, Duration
    Product names may contain commas, so parse from the right-most two columns.
    """
    raw = str(line or "").strip()
    if not raw:
        return None
    m = re.match(r"^\s*(.+?)\s*,\s*([^,]+?)\s*,\s*([^,]+?)\s*$", raw)
    if not m:
        return None
    return m.group(1).strip(), m.group(2).strip(), m.group(3).strip()


def _should_parse_product_rows(raw: str, duration_hint: str | None) -> bool:
    if _is_product_section_duration(duration_hint):
        return True
    lines = [ln.strip() for ln in str(raw or "").splitlines() if ln.strip()]
    if not lines:
        return False
    # Auto-detect when each line looks like: Product Name, No of Pax, Duration
    matched = 0
    for line in lines:
        parsed = _parse_product_row_line(line)
        if parsed is None:
            continue
        _, pax, dur = parsed
        if pax and re.search(r"\d", pax) and _looks_like_duration(dur):
            matched += 1
    return matched == len(lines)


def parse_course_rows(course_text: str, duration_hint: str | None) -> list[dict[str, str]]:
    """
    Supports two formats:
    1) Existing: "Course A, Course B" or newline list -> [{"course_name": "..."}]
    2) Zoho product rows (when duration says 'Refer to Product Section'):
       "Product Name, No of Pax, Duration" per line.
    """
    raw = str(course_text or "").strip()
    if not raw:
        return []

    if _should_parse_product_rows(raw, duration_hint):
        rows: list[dict[str, str]] = []
        for line in [ln.strip() for ln in raw.splitlines() if ln.strip()]:
            parsed = _parse_product_row_line(line)
            if parsed is None:
                continue
            course_name, no_of_pax, duration = parsed
            if course_name:
                rows.append(
                    {
                        "course_name": course_name,
                        "no_of_pax": no_of_pax,
                        "duration": duration,
                    }
                )
        if rows:
            return rows

    # Default existing behavior (backward-compatible)
    return [{"course_name": c} for c in split_courses(raw)]


def parse_title(title: str) -> tuple[str, int | None]:
    match = re.match(r"(.+)_v(\d+)$", str(title or "").strip(), re.IGNORECASE)
    if match:
        return match.group(1).strip(), int(match.group(2))
    return str(title or "").strip(), None


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
    """
    Best-effort title extraction for naming files/folders in refine flow.
    Priority:
    1) structured JSON field: course_title
    2) text line: "Course Title: ..."
    3) first markdown heading / first non-empty line
    """
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


async def _parse_generate_request(request: Request) -> GenerateCourseRequest:
    """
    Support both:
    - application/json (preferred)
    - application/x-www-form-urlencoded (common in Zoho webhooks)
    """
    content_type = (request.headers.get("content-type") or "").lower()
    required_input_fields = ["course_name"]

    payload: dict
    if "application/json" in content_type:
        try:
            payload = await request.json()
        except Exception:
            logger.exception("Failed to parse JSON body for /courses")
            raise HTTPException(status_code=422, detail="Invalid JSON body.")
    elif "application/x-www-form-urlencoded" in content_type:
        body_bytes = await request.body()
        raw_body = body_bytes.decode("utf-8", errors="replace")
        parsed = parse_qs(raw_body, keep_blank_values=True)
        # Keep first value for each key (Zoho sends scalar fields in form encoding)
        form_data = {k: (v[0] if isinstance(v, list) and v else "") for k, v in parsed.items()}

        zoho_record_id = (
            form_data.get("zoho_record_id")
            or form_data.get("record_id")
            or form_data.get("id")
            or form_data.get("crm_record_id")
        )
        if not zoho_record_id:
            raise HTTPException(
                status_code=422,
                detail=(
                    "Missing zoho_record_id in webhook payload. "
                    "Provide one of: zoho_record_id, record_id, id, crm_record_id."
                ),
            )

        # Normalize webhook fields into input_data expected by course generation.
        input_data = {
            "company_name": form_data.get("company_name", ""),
            "course_name": form_data.get("course_name", ""),
            "need_of_training": form_data.get("need_of_training", ""),
            "specific_questions": form_data.get("specific_questions", []),
            "goal_of_training": form_data.get("goal_of_training", ""),
            "size_of_company": form_data.get("size_of_company", ""),
            "department": form_data.get("department", ""),
            "designation": form_data.get("designation", ""),
            "duration": form_data.get("duration", ""),
            "level_of_training": form_data.get("level_of_training", ""),
            # Optional Zoho / CRM fields (also accept arbitrary extra keys below).
            "no_of_pax": form_data.get("no_of_pax", ""),
            "languages_prefered": form_data.get("languages_prefered")
            or form_data.get("languages_preferred", ""),
            "additional_certifications": form_data.get("additional_certifications", ""),
            "additional_notes": form_data.get("additional_notes", ""),
            "important_topics": form_data.get("important_topics", ""),
        }

        # If questions are provided as a plain text blob, split by lines.
        if isinstance(input_data["specific_questions"], str):
            questions_raw = input_data["specific_questions"].strip()
            if questions_raw:
                input_data["specific_questions"] = [
                    q.strip("-• ").strip()
                    for q in questions_raw.splitlines()
                    if q.strip()
                ]
            else:
                input_data["specific_questions"] = []

        # Pass through any other non-empty form fields (future Zoho columns) into input_data.
        _reserved_top = {"zoho_record_id", "record_id", "id", "crm_record_id"}
        for k, v in form_data.items():
            if k in _reserved_top:
                continue
            if k not in input_data and str(v).strip():
                input_data[k] = v.strip() if isinstance(v, str) else v

        payload = {
            "zoho_record_id": str(zoho_record_id),
            "input_data": input_data,
        }
    elif "multipart/form-data" in content_type:
        # Multipart needs python-multipart package; provide clear error instead of 500.
        raise HTTPException(
            status_code=415,
            detail=(
                "multipart/form-data is not enabled. "
                "Use application/x-www-form-urlencoded or install python-multipart."
            ),
        )

    else:
        raise HTTPException(
            status_code=415,
            detail="Unsupported content type. Use application/json or application/x-www-form-urlencoded.",
        )

    # Validate required input_data keys for both JSON and form payloads.
    input_data_payload = payload.get("input_data") if isinstance(payload, dict) else None
    if not isinstance(input_data_payload, dict):
        logger.warning("Invalid /courses payload: input_data is not an object")
        raise HTTPException(status_code=422, detail="input_data must be an object.")

    course_type = str(input_data_payload.get("course_type") or "").strip().lower()
    if course_type not in {"public", "pub"}:
        required_input_fields = [
            "company_name",
            "course_name",
            "department",
            "designation",
            "level_of_training",
        ]

    missing_required = [
        key for key in required_input_fields if not str(input_data_payload.get(key, "")).strip()
    ]
    if missing_required:
        logger.warning(
            "Validation failed for /courses: missing required input_data fields=%s",
            ",".join(missing_required),
        )
        raise HTTPException(
            status_code=422,
            detail=f"Missing required input_data fields: {', '.join(missing_required)}",
        )

    try:
        return GenerateCourseRequest.model_validate(payload)
    except Exception:
        logger.exception("Payload validation failed for /courses")
        raise HTTPException(status_code=422, detail="Payload validation failed for /courses.")


async def _parse_refine_request(request: Request) -> tuple[str, RefineCourseRequest]:
    """
    Support both:
    - application/json
    - application/x-www-form-urlencoded (Zoho-friendly)
    """
    content_type = (request.headers.get("content-type") or "").lower()
    payload: dict
    if "application/json" in content_type:
        try:
            payload = await request.json()
        except Exception:
            logger.exception("Failed to parse JSON body for /courses/refine")
            raise HTTPException(status_code=422, detail="Invalid JSON body.")
    elif "application/x-www-form-urlencoded" in content_type:
        body_bytes = await request.body()
        raw_body = body_bytes.decode("utf-8", errors="replace")
        parsed = parse_qs(raw_body, keep_blank_values=True)
        payload = {k: (v[0] if isinstance(v, list) and v else "") for k, v in parsed.items()}
    else:
        raise HTTPException(
            status_code=415,
            detail="Unsupported content type. Use application/json or application/x-www-form-urlencoded.",
        )

    rid = str(
        payload.get("zoho_record_id")
        or payload.get("record_id")
        or payload.get("id")
        or payload.get("crm_record_id")
        or ""
    ).strip()
    if not rid:
        raise HTTPException(
            status_code=422,
            detail=(
                "Missing zoho_record_id in refine payload. "
                "Provide one of: zoho_record_id, record_id, id, crm_record_id."
            ),
        )

    refine_payload = {
        "feedback": payload.get("feedback"),
        "course_name": (
            payload.get("course_name")
            or payload.get("title")
            or payload.get("note_title")
        ),
    }
    try:
        req = RefineCourseRequest.model_validate(refine_payload)
    except Exception:
        logger.exception("Payload validation failed for /courses/refine")
        raise HTTPException(status_code=422, detail="Payload validation failed for /courses/refine.")

    return rid, req


async def process_course_job(job_id: uuid.UUID, zoho_record_id: str, input_data: dict) -> None:
    async with AsyncSessionLocal() as db:
        job: CourseJob | None = None
        created_course_id: uuid.UUID | None = None
        created_version_number: int | None = None
        try:
            logger.info(
                "Background job started | job_id=%s zoho_record_id=%s",
                str(job_id),
                zoho_record_id,
            )
            job_result = await db.execute(select(CourseJob).where(CourseJob.id == job_id))
            job = job_result.scalars().first()
            if job is None:
                logger.error("Background job missing in DB | job_id=%s", str(job_id))
                return

            job.status = "processing"
            job.error = None
            await db.commit()
            logger.info("Job status set to processing | job_id=%s", str(job_id))

            course_title = str((input_data or {}).get("course_name") or "").strip() or "course"
            course_type = str((input_data or {}).get("course_type") or "").strip().lower()

            outline: str = ""
            outline_payload = None
            pdf_path: str | None = None
            pdf_url: str | None = None
            used_public_sheet_pdf = False

            if course_type in {"public", "pub"} and (settings.PUBLIC_COURSE_SHEET_CSV_URL or "").strip():
                try:
                    sheet_pdf = await lookup_public_course_pdf_url(course_title)
                    if sheet_pdf and str(sheet_pdf).strip():
                        used_public_sheet_pdf = True
                        pdf_url = str(sheet_pdf).strip()
                        outline = json.dumps(
                            {
                                "source": "public_course_sheet",
                                "course_name": course_title,
                                "pdf_url": pdf_url,
                                "note": "Outline PDF is taken from PUBLIC_COURSE_SHEET_CSV_URL; no AI outline text is stored.",
                            },
                            ensure_ascii=False,
                            indent=2,
                        )
                        logger.info(
                            "Public course sheet hit | job_id=%s course_name=%s pdf_url=%s",
                            str(job_id),
                            course_title,
                            pdf_url,
                        )
                except Exception:
                    logger.exception(
                        "Public course sheet lookup failed; falling back to AI | job_id=%s",
                        str(job_id),
                    )

            if not used_public_sheet_pdf:
                context_text = json.dumps(input_data, ensure_ascii=False, indent=2)
                ai = ClaudeService()
                # Legacy outline pipeline only (no LangGraph / multi-node graph).
                logger.info("Outline generation started | job_id=%s", str(job_id))
                learning_objectives = await wait_for(ai.build_learning_objectives(context_text), timeout=310)
                try:
                    outline_payload = await wait_for(
                        ai.build_roi_course_outline_json(context_text, learning_objectives),
                        timeout=310,
                    )
                    _enforce_regions_served_constant(outline_payload)
                    outline = json.dumps(outline_payload.model_dump(), ensure_ascii=False, indent=2)
                except RuntimeError:
                    outline = await wait_for(
                        ai.build_roi_course_outline(context_text, learning_objectives),
                        timeout=310,
                    )
                    outline_payload = None
                logger.info("Outline generation completed | job_id=%s", str(job_id))

                try:
                    logger.info("PDF generation started | job_id=%s", str(job_id))
                    pdf_path = await generate_pdf_path_async(
                        outline_payload if outline_payload is not None else outline
                    )
                    pdf_url = _build_pdf_url(pdf_path)
                    logger.info("PDF generation completed | job_id=%s pdf_url=%s", str(job_id), pdf_url)
                except RuntimeError as e:
                    logger.warning("PDF generation failed in job | job_id=%s error=%s", str(job_id), str(e))

            if not used_public_sheet_pdf and pdf_path and os.path.isfile(pdf_path):
                try:
                    drive_up = await asyncio.to_thread(
                        upload_course_outline_pdf_to_drive,
                        pdf_path,
                        course_name=course_title,
                        zoho_record_id=zoho_record_id,
                        version_number=1,
                    )
                    if drive_up and isinstance(drive_up.get("edit_link"), str) and drive_up["edit_link"].strip():
                        pdf_url = drive_up["edit_link"].strip()
                        logger.info(
                            "Course outline PDF uploaded to Drive | job_id=%s url=%s",
                            str(job_id),
                            pdf_url,
                        )
                except GoogleDriveUploadError as e:
                    logger.warning(
                        "Google Drive outline upload failed; keeping local pdf_url | job_id=%s error=%s",
                        str(job_id),
                        str(e),
                    )
                except Exception:
                    logger.exception("Google Drive outline upload failed | job_id=%s", str(job_id))

            async with db.begin():
                logger.info("Persisting course + version | job_id=%s", str(job_id))
                course = Course(zoho_record_id=zoho_record_id)
                db.add(course)
                await db.flush()
                version = CourseVersion(
                    course_id=course.id,
                    version_number=1,
                    outline_text=outline,
                    pdf_url=pdf_url,
                )
                db.add(version)
                created_course_id = course.id
                created_version_number = 1
                logger.info(
                    "Persisted initial version | job_id=%s course_id=%s version=%s",
                    str(job_id),
                    str(created_course_id),
                    created_version_number,
                )

            job.status = "completed"
            job.pdf_url = pdf_url
            job.course_id = created_course_id
            job.version_number = created_version_number
            await db.commit()
            logger.info(
                "Job completed | job_id=%s course_id=%s version=%s pdf_url=%s",
                str(job_id),
                str(created_course_id) if created_course_id else None,
                created_version_number,
                pdf_url,
            )
            await zoho_notify_course_outline_job_finished(
                job,
                created_version_number,
                attach_course_title=f"{course_title} — outline",
            )
        except Exception as e:
            if job is not None:
                job.status = "failed"
                job.error = str(e)[:4000]
                await db.commit()
                await zoho_notify_course_outline_job_finished(
                    job,
                    created_version_number,
                    attach_course_title=None,
                )
            logger.exception(
                "Background job failed | job_id=%s zoho_record_id=%s course_id=%s",
                str(job_id),
                zoho_record_id,
                str(created_course_id) if created_course_id else None,
            )


# ─── Routes ───────────────────────────────────────────────────────────────────

@router.post(
    "/courses",
    dependencies=[auth],
    summary="Create course job (async 202, or sync 200 with full result)",
    description=(
        "After a 202 response, poll GET /api/v1/courses/{zoho_record_id}/outline-job. "
        "When the job completes, optional Zoho integration runs if configured: "
        "(1) ZOHO_CALLBACK_URL — HTTP POST to your URL with job_id, zoho_record_id, status, pdf_urls, version_number; "
        "(2) ZOHO_ATTACH_PDF_LINK_TO_CRM=true — attaches PDF link to the CRM record via API. "
        "Neither runs automatically unless those env vars are set."
    ),
)
async def generate_course(
    request: Request,
    background_tasks: BackgroundTasks,
    sync: bool = Query(
        False,
        description=(
            "If true: wait for AI+PDF to finish and return full JSON in this response (HTTP 200). "
            "Use when the caller (e.g. Zoho) must receive pdf_url in one shot — "
            "request may take several minutes and can time out. "
            "If false (default): HTTP 202 with job_id, status, and polling URL; poll GET /api/v1/courses/{zoho_record_id}/outline-job."
        ),
    ),
):
    req = await _parse_generate_request(request)
    course_rows = parse_course_rows(
        str(req.input_data.course_name or ""),
        str(req.input_data.duration or ""),
    )
    if not course_rows:
        raise HTTPException(status_code=422, detail="input_data.course_name is required.")
    if sync and len(course_rows) > 1:
        logger.warning(
            "sync=true with multiple courses is not supported; queueing async jobs instead | count=%s zoho_record_id=%s",
            len(course_rows),
            req.zoho_record_id,
        )
        sync = False
    logger.info("Queueing course generation | zoho_record_id=%s sync=%s", req.zoho_record_id, sync)
    # Debug visibility for Zoho webhook mappings: log full request payload and top-level input keys.
    logger.info("Incoming /courses payload: %s", json.dumps(req.model_dump(mode="json"), ensure_ascii=False))
    logger.info(
        "Incoming input_data keys: %s",
        sorted(req.input_data.model_dump(exclude_none=False).keys()),
    )
    jobs_with_input: list[tuple[uuid.UUID, dict]] = []
    try:
        # Use one short-lived DB session per job row to avoid transaction overlap
        # during multi-course queueing.
        for row in course_rows:
            course_name = str(row.get("course_name") or "").strip()
            if not course_name:
                continue
            input_copy = req.input_data.model_copy()
            input_copy.course_name = course_name
            row_pax = str(row.get("no_of_pax") or "").strip()
            row_duration = str(row.get("duration") or "").strip()
            if row_pax:
                input_copy.no_of_pax = row_pax
            if row_duration:
                input_copy.duration = row_duration
            input_for_job = _input_data_dict_for_job(input_copy)
            async with AsyncSessionLocal() as db:
                async with db.begin():
                    job = CourseJob(
                        zoho_record_id=req.zoho_record_id,
                        status="pending",
                        payload_json=json.dumps(input_for_job, ensure_ascii=False),
                    )
                    db.add(job)
                await db.refresh(job)
                jobs_with_input.append((job.id, input_for_job))
                logger.info(
                    "Course generation job created | job_id=%s zoho_record_id=%s course_name=%s no_of_pax=%s duration=%s",
                    str(job.id),
                    req.zoho_record_id,
                    course_name,
                    row_pax,
                    row_duration,
                )
                logger.info(
                    "Course generation context | job_id=%s department=%s designation=%s level_of_training=%s company_name=%s",
                    str(job.id),
                    str(input_for_job.get("department") or ""),
                    str(input_for_job.get("designation") or ""),
                    str(input_for_job.get("level_of_training") or ""),
                    str(input_for_job.get("company_name") or ""),
                )
    except (SQLAlchemyError, OSError, Exception):
        logger.exception("Database error while creating job")
        raise HTTPException(status_code=503, detail="Database unavailable. Please try again.")

    if sync:
        job_id, job_input = jobs_with_input[0]
        await process_course_job(job_id, req.zoho_record_id, job_input)
        async with AsyncSessionLocal() as db2:
            result = await db2.execute(select(CourseJob).where(CourseJob.id == job_id))
            job_done = result.scalars().first()
        if job_done is None:
            raise HTTPException(status_code=500, detail="Job finished but could not be reloaded.")
        logger.info("Sync course generation finished | job_id=%s status=%s", job_id, job_done.status)
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=_job_to_course_outline_response(job_done).model_dump(mode="json"),
        )

    for job_id, job_input in jobs_with_input:
        background_tasks.add_task(
            process_course_job,
            job_id,
            req.zoho_record_id,
            job_input,
        )
        logger.info("Background task scheduled | job_id=%s", str(job_id))
    rid = req.zoho_record_id
    if len(jobs_with_input) == 1:
        job_id = jobs_with_input[0][0]
        body = CourseOutlineQueuedResponse(
            job_id=job_id,
            zoho_record_id=rid,
            status="pending",
            message="Course outline generation queued.",
            polling={
                "by_zoho_record_id": f"/api/v1/courses/{rid}/outline-job",
            },
        )
        content = body.model_dump(mode="json")
    else:
        content = {
            "message": "Multiple course outlines queued",
            "zoho_record_id": rid,
            "job_ids": [str(job_id) for job_id, _ in jobs_with_input],
            "polling": {"by_zoho_record_id": f"/api/v1/courses/{rid}/outline-job"},
        }
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content=content,
    )


@router.post(
    "/courses/refine",
    response_model=CourseVersionResponse,
    dependencies=[auth],
    summary="Refine a course outline with feedback (zoho_record_id in request body)",
)
async def refine_course_from_body(request: Request):
    rid, req = await _parse_refine_request(request)
    return await refine_course(zoho_record_id=rid, req=req)


@router.post(
    "/courses/{zoho_record_id}/refine",
    response_model=CourseVersionResponse,
    dependencies=[auth],
    summary="Refine a course outline with feedback (keyed by Zoho CRM record id)",
)
async def refine_course(
    zoho_record_id: str,
    req: RefineCourseRequest,
):
    rid = (zoho_record_id or "").strip()
    if not rid:
        raise HTTPException(status_code=422, detail="zoho_record_id is required.")

    requested_title = str((req.course_name or "")).strip()
    requested_course_name, requested_version = parse_title(requested_title)
    logger.info(
        "Refine requested | zoho_record_id=%s title=%s parsed_course=%s requested_version=%s",
        rid,
        requested_title,
        requested_course_name,
        requested_version,
    )

    async with AsyncSessionLocal() as db:
        try:
            jres = await db.execute(
                select(CourseJob)
                .where(
                    CourseJob.zoho_record_id == rid,
                    CourseJob.course_id.is_not(None),
                    or_(
                        CourseJob.job_type.is_(None),
                        and_(CourseJob.job_type != "slides", CourseJob.job_type != "assessment"),
                    ),
                )
                .order_by(CourseJob.created_at.desc())
            )
            jobs = jres.scalars().all()
        except (SQLAlchemyError, OSError, Exception):
            logger.exception("Database error while reading course jobs for refine")
            raise HTTPException(status_code=503, detail="Database unavailable. Please try again.")

        if not jobs:
            logger.warning("Refine failed: no completed course jobs | zoho_record_id=%s", rid)
            raise HTTPException(status_code=404, detail="Course not found for this zoho_record_id.")

        target_course_id: uuid.UUID | None = None
        # Prefer explicit title mapping against payload_json.course_name.
        if requested_course_name:
            for job in jobs:
                jname = _job_payload_course_name(job)
                if jname and jname.strip().lower() == requested_course_name.strip().lower():
                    target_course_id = job.course_id
                    break
            if target_course_id is None:
                logger.warning(
                    "Refine failed: no matching course_name track found | zoho_record_id=%s requested=%s",
                    rid,
                    requested_course_name,
                )
                raise HTTPException(
                    status_code=404,
                    detail=f"No generated course track found for title '{requested_course_name}'.",
                )
        else:
            # No title provided: use latest generated course track for this Zoho record.
            target_course_id = jobs[0].course_id

        course_uuid = target_course_id
        if course_uuid is None:
            raise HTTPException(status_code=404, detail="Course track not found.")

        try:
            result = await db.execute(
                select(CourseVersion)
                .where(CourseVersion.course_id == course_uuid)
                .order_by(CourseVersion.version_number.desc())
            )
            versions = result.scalars().all()
        except (SQLAlchemyError, OSError, Exception):
            logger.exception("Database error while reading versions for refine")
            raise HTTPException(status_code=503, detail="Database unavailable. Please try again.")

        if not versions:
            logger.warning("Refine failed: no versions | zoho_record_id=%s", rid)
            raise HTTPException(status_code=404, detail="No outline versions found for this course.")

        if requested_version is not None:
            base_version = next((v for v in versions if int(v.version_number) == int(requested_version)), None)
            if base_version is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Version v{requested_version} not found for this course track.",
                )
        else:
            base_version = versions[0]

        base_outline = base_version.outline_text or ""

    # Call AI (structured refine first, fallback to text refine) — no DB session held here
    try:
        ai = ClaudeService()
        try:
            logger.info("Refine AI started with structured mode | zoho_record_id=%s", rid)
            refined_payload = await wait_for(
                ai.refine_course_outline_json(base_outline, req.feedback),
                timeout=310,
            )
            _enforce_regions_served_constant(refined_payload)
            updated_outline = json.dumps(refined_payload.model_dump(), ensure_ascii=False, indent=2)
            logger.info("Refine AI structured mode completed | zoho_record_id=%s", rid)
        except RuntimeError:
            logger.warning("Refine AI structured mode failed, using fallback | zoho_record_id=%s", rid)
            context_text = json.dumps(
                {
                    "previous_outline": base_outline,
                    "feedback": req.feedback,
                },
                ensure_ascii=False,
                indent=2,
            )
            updated_outline = await wait_for(
                ai.build_roi_course_outline(context_text, base_outline),
                timeout=310,
            )
            refined_payload = None
            logger.info("Refine AI fallback completed | zoho_record_id=%s", rid)
    except AsyncTimeoutError:
        logger.warning("AI refine timed out for zoho_record_id=%s", rid)
        raise HTTPException(status_code=504, detail="AI service timed out.")
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except RuntimeError as e:
        logger.warning("AI refine failed for zoho_record_id=%s error=%s", rid, str(e))
        raise HTTPException(status_code=502, detail="AI service failed. Please retry.")

    name_for_file = requested_course_name or _derive_course_name_from_outline(base_outline)

    # Generate PDF off the event loop
    pdf_path: str | None = None
    pdf_url = None
    try:
        logger.info("Refine PDF generation started | zoho_record_id=%s", rid)
        pdf_path = await generate_pdf_path_async(refined_payload if refined_payload is not None else updated_outline)
        pdf_url = _build_pdf_url(pdf_path)
        logger.info("Refine PDF generation completed | zoho_record_id=%s pdf_url=%s", rid, pdf_url)
    except RuntimeError as e:
        logger.warning(
            "PDF generation unavailable; continuing without PDF | zoho_record_id=%s error=%s",
            rid,
            str(e),
        )

    new_version_id: uuid.UUID
    new_version_number: int
    saved_outline: str
    saved_pdf_url: str | None
    saved_created_at: object

    async with AsyncSessionLocal() as db:
        try:
            locked_course = await db.execute(
                select(Course).where(Course.id == course_uuid).with_for_update()
            )
            if locked_course.scalars().first() is None:
                raise HTTPException(status_code=404, detail="Course not found.")

            current_max = await db.execute(
                select(func.max(CourseVersion.version_number)).where(
                    CourseVersion.course_id == course_uuid
                )
            )
            max_version_number = current_max.scalar_one_or_none() or 0
            new_version_number = int(max_version_number) + 1

            new_version = CourseVersion(
                course_id=course_uuid,
                version_number=new_version_number,
                outline_text=updated_outline,
                pdf_url=pdf_url,
                feedback=req.feedback,
            )
            db.add(new_version)
            await db.commit()
            await db.refresh(new_version)
            new_version_id = new_version.id
            saved_outline = new_version.outline_text or ""
            saved_pdf_url = new_version.pdf_url
            saved_created_at = new_version.created_at
        except HTTPException:
            raise
        except (SQLAlchemyError, OSError, Exception):
            logger.exception("Database error while saving refined version")
            raise HTTPException(status_code=503, detail="Database unavailable. Please try again.")

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
                logger.info(
                    "Refine: outline PDF uploaded to Drive | zoho_record_id=%s url=%s",
                    rid,
                    saved_pdf_url,
                )
        except GoogleDriveUploadError as e:
            logger.warning(
                "Refine: Google Drive upload failed; keeping local pdf_url | zoho_record_id=%s error=%s",
                rid,
                str(e),
            )
        except Exception:
            logger.exception("Refine: Google Drive upload failed | zoho_record_id=%s", rid)

    try:
        await zoho_notify_refined_outline_version(
            zoho_record_id=rid,
            pdf_url=saved_pdf_url,
            version_number=new_version_number,
            course_name_for_title=f"{name_for_file} — outline v{new_version_number}",
        )
    except Exception:
        logger.exception("Refine: Zoho notify skipped due to error | zoho_record_id=%s", rid)

    logger.info("Course refined | zoho_record_id=%s version=%s", rid, new_version_number)

    return CourseVersionResponse(
        version_id=new_version_id,
        zoho_record_id=rid,
        version_number=new_version_number,
        pdf_url=saved_pdf_url,
        outline=saved_outline,
        created_at=saved_created_at,
    )


@router.get(
    "/courses/{zoho_record_id}/versions",
    response_model=CourseVersionsResponse,
    dependencies=[auth],
    summary="List all versions of a course (by Zoho CRM record id)",
)
async def list_versions(
    zoho_record_id: str,
    db: AsyncSession = Depends(get_db),
):
    rid = (zoho_record_id or "").strip()
    logger.info("List versions requested | zoho_record_id=%s", rid)

    # Fetch course
    try:
        result = await db.execute(select(Course).where(Course.zoho_record_id == rid))
        course = result.scalars().first()
    except (SQLAlchemyError, OSError, Exception):
        logger.exception("Database error while reading course")
        raise HTTPException(status_code=503, detail="Database unavailable. Please try again.")
    if not course:
        raise HTTPException(status_code=404, detail="Course not found for this zoho_record_id.")

    course_uuid = course.id

    # Fetch versions
    try:
        result = await db.execute(
            select(CourseVersion)
            .where(CourseVersion.course_id == course_uuid)
            .order_by(CourseVersion.version_number)
        )
        versions = result.scalars().all()
    except (SQLAlchemyError, OSError, Exception):
        logger.exception("Database error while listing versions")
        raise HTTPException(status_code=503, detail="Database unavailable. Please try again.")

    return CourseVersionsResponse(
        zoho_record_id=course.zoho_record_id,
        versions=[
            VersionSummary(
                version_id=v.id,
                version_number=v.version_number,
                pdf_url=v.pdf_url,
                feedback=v.feedback,
                created_at=v.created_at,
            )
            for v in versions
        ],
    )


@router.get(
    "/courses/{zoho_record_id}/versions/{version_number}",
    response_model=CourseVersionResponse,
    dependencies=[auth],
    summary="Get a specific version of a course (by Zoho CRM record id)",
)
async def get_version(
    zoho_record_id: str,
    version_number: int,
    db: AsyncSession = Depends(get_db),
):
    rid = (zoho_record_id or "").strip()
    logger.info("Get version requested | zoho_record_id=%s version=%s", rid, version_number)

    try:
        cres = await db.execute(select(Course).where(Course.zoho_record_id == rid))
        course = cres.scalars().first()
    except (SQLAlchemyError, OSError, Exception):
        logger.exception("Database error while reading course")
        raise HTTPException(status_code=503, detail="Database unavailable. Please try again.")
    if not course:
        raise HTTPException(status_code=404, detail="Course not found for this zoho_record_id.")

    course_uuid = course.id

    try:
        result = await db.execute(
            select(CourseVersion)
            .where(
                CourseVersion.course_id == course_uuid,
                CourseVersion.version_number == version_number,
            )
        )
        version = result.scalars().first()
    except (SQLAlchemyError, OSError, Exception):
        logger.exception("Database error while reading version")
        raise HTTPException(status_code=503, detail="Database unavailable. Please try again.")

    if not version:
        raise HTTPException(status_code=404, detail="Version not found.")

    return CourseVersionResponse(
        version_id=version.id,
        zoho_record_id=rid,
        version_number=version.version_number,
        pdf_url=version.pdf_url,
        outline=version.outline_text,
        created_at=version.created_at,
    )


@router.get("/health", tags=["meta"], summary="Health check")
async def health():
    # Keep this endpoint dependency-free and fast; report DB connectivity best-effort.
    db_connected = False
    try:
        from app.core.database import engine

        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        db_connected = True
    except Exception:
        db_connected = False

    return {"status": "ok", "db_connected": db_connected}


@router.get(
    "/courses/{zoho_record_id}/outline-job",
    dependencies=[auth],
    summary="Latest course-outline generation job by Zoho record id (excludes slides and assessment jobs)",
)
async def get_latest_course_outline_job(
    zoho_record_id: str,
    db: AsyncSession = Depends(get_db),
):
    rid = (zoho_record_id or "").strip()
    if not rid:
        raise HTTPException(status_code=422, detail="zoho_record_id is required.")
    try:
        result = await db.execute(
            select(CourseJob)
            .where(
                CourseJob.zoho_record_id == rid,
                or_(
                    CourseJob.job_type.is_(None),
                    and_(CourseJob.job_type != "slides", CourseJob.job_type != "assessment"),
                ),
            )
            .order_by(CourseJob.created_at.desc())
        )
        jobs = result.scalars().all()
    except Exception:
        logger.exception("Database error while reading latest course outline job")
        raise HTTPException(status_code=503, detail="Database unavailable. Please try again.")
    if not jobs:
        raise HTTPException(
            status_code=404,
            detail="No course outline job found for this zoho_record_id.",
        )
    job = jobs[0]

    # Multi-course support: include all completed PDF links for this zoho_record_id.
    all_pdf_urls = [
        str(j.pdf_url).strip()
        for j in jobs
        if str(j.status or "").strip().lower() == "completed"
        and isinstance(j.pdf_url, str)
        and str(j.pdf_url).strip()
    ]
    # Keep order stable by created_at desc from query while removing duplicates.
    dedup_pdf_urls: list[str] = []
    for u in all_pdf_urls:
        if u not in dedup_pdf_urls:
            dedup_pdf_urls.append(u)

    payload = _job_to_course_outline_response(job).model_dump(mode="json")
    payload.pop("pdf_url", None)
    payload["pdf_urls"] = dedup_pdf_urls
    payload["job_ids"] = [str(j.id) for j in jobs]
    payload["total_jobs"] = len(jobs)
    payload["completed_jobs"] = sum(1 for j in jobs if str(j.status or "").strip().lower() == "completed")
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=payload,
    )


import uuid
import os
import json
from urllib.parse import parse_qs
from typing import Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Header, Query, status, BackgroundTasks, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from sqlalchemy.exc import SQLAlchemyError
import asyncio
from asyncio import wait_for, TimeoutError as AsyncTimeoutError

from sqlalchemy import text

from app.core.database import get_db, AsyncSessionLocal
from app.core.config import settings
from app.schemas.course import (
    GenerateCourseRequest,
    RefineCourseRequest,
    CourseVersionResponse,
    CourseVersionsResponse,
    VersionSummary,
)
from app.schemas.job import JobQueuedResponse, JobResponse
from app.models.course import Course, CourseVersion
from app.models.job import CourseJob
from app.services.claude import ClaudeService
from app.services.pdf_service import generate_pdf_path_async
from app.services.zoho_crm import maybe_attach_course_pdf
from app.utils.logger import get_logger

logger = get_logger(__name__)
router = APIRouter(prefix="/api/v1", tags=["courses"])


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


def _job_to_response(job: CourseJob) -> JobResponse:
    return JobResponse(
        job_id=job.id,
        zoho_record_id=job.zoho_record_id,
        job_type=getattr(job, "job_type", None),
        status=job.status,
        pdf_url=job.pdf_url,
        ppt_url=getattr(job, "ppt_url", None),
        error=job.error,
        course_id=job.course_id,
        version_number=job.version_number,
        created_at=job.created_at,
    )


# ─── Helper ───────────────────────────────────────────────────────────────────

def _build_pdf_url(file_path: str) -> str:
    filename = os.path.basename(file_path)
    return f"{settings.BASE_URL}/pdfs/{filename}"


async def _parse_generate_request(request: Request) -> GenerateCourseRequest:
    """
    Support both:
    - application/json (preferred)
    - application/x-www-form-urlencoded (common in Zoho webhooks)
    """
    content_type = (request.headers.get("content-type") or "").lower()
    required_input_fields = [
        "company_name",
        "course_name",
        "department",
        "designation",
        "level_of_training",
    ]

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


def _zoho_callback_url_is_placeholder(url: str) -> bool:
    u = (url or "").strip().lower()
    if not u:
        return True
    # README / template placeholders — never resolve in DNS
    return any(
        x in u
        for x in (
            "example.com",
            "your-zoho",
            "your-api-domain",
            "callback-endpoint.example",
            "localhost",
        )
    )


async def _post_zoho_callback(job: CourseJob, course_id: uuid.UUID | None, version_number: int | None) -> None:
    if not settings.ZOHO_CALLBACK_URL or _zoho_callback_url_is_placeholder(settings.ZOHO_CALLBACK_URL):
        logger.info(
            "Zoho callback skipped: ZOHO_CALLBACK_URL not set or is placeholder | job_id=%s",
            str(job.id),
        )
        return
    # Note: this sends metadata + public pdf_url only — not the PDF bytes. For CRM attachment on
    # the record, enable ZOHO_ATTACH_PDF_LINK_TO_CRM + OAuth, or download pdf_url inside Zoho.
    raw_fields = {
        "job_id": str(job.id),
        "zoho_record_id": job.zoho_record_id,
        "status": job.status,
        "pdf_url": job.pdf_url,
        "course_id": str(course_id) if course_id else None,
        "version_number": version_number,
        "error": job.error,
    }
    fmt = (settings.ZOHO_CALLBACK_BODY_FORMAT or "json").strip().lower()
    try:
        logger.info(
            "Posting Zoho callback | job_id=%s format=%s status=%s",
            str(job.id),
            fmt,
            job.status,
        )
        async with httpx.AsyncClient(timeout=30.0) as client:
            if fmt == "form":
                form_data = {
                    k: ("" if v is None else str(v)) for k, v in raw_fields.items()
                }
                response = await client.post(settings.ZOHO_CALLBACK_URL, data=form_data)
            else:
                json_body = {k: v for k, v in raw_fields.items() if v is not None}
                response = await client.post(settings.ZOHO_CALLBACK_URL, json=json_body)
            logger.info(
                "Zoho callback response | job_id=%s status_code=%s",
                str(job.id),
                response.status_code,
            )
            if response.status_code >= 400:
                logger.warning(
                    "Zoho callback rejected | job_id=%s body=%s",
                    str(job.id),
                    (response.text or "")[:2000],
                )
    except Exception:
        logger.exception("Zoho callback failed | job_id=%s", str(job.id))


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

            context_text = json.dumps(input_data, ensure_ascii=False, indent=2)
            ai = ClaudeService()
            logger.info("AI step 1 started (learning objectives) | job_id=%s", str(job_id))
            learning_objectives = await wait_for(ai.build_learning_objectives(context_text), timeout=310)
            logger.info("AI step 1 completed | job_id=%s", str(job_id))
            try:
                logger.info("AI step 2 started (structured outline) | job_id=%s", str(job_id))
                outline_payload = await wait_for(
                    ai.build_roi_course_outline_json(context_text, learning_objectives),
                    timeout=310,
                )
                outline = json.dumps(outline_payload.model_dump(), ensure_ascii=False, indent=2)
                logger.info("AI step 2 completed with structured output | job_id=%s", str(job_id))
            except RuntimeError:
                logger.warning("Structured AI output failed, using text fallback | job_id=%s", str(job_id))
                outline = await wait_for(
                    ai.build_roi_course_outline(context_text, learning_objectives),
                    timeout=310,
                )
                outline_payload = None
                logger.info("AI fallback output completed | job_id=%s", str(job_id))

            pdf_url = None
            try:
                logger.info("PDF generation started | job_id=%s", str(job_id))
                pdf_path = await generate_pdf_path_async(outline_payload if outline_payload is not None else outline)
                pdf_url = _build_pdf_url(pdf_path)
                logger.info("PDF generation completed | job_id=%s pdf_url=%s", str(job_id), pdf_url)
            except RuntimeError as e:
                logger.warning("PDF generation failed in job | job_id=%s error=%s", str(job_id), str(e))

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
            course_title = str((input_data or {}).get("course_name") or "").strip() or "Course outline"
            await maybe_attach_course_pdf(
                zoho_record_id=zoho_record_id,
                pdf_url=pdf_url,
                course_name_for_title=f"{course_title} — outline",
            )
            await _post_zoho_callback(job, created_course_id, created_version_number)
        except Exception as e:
            if job is not None:
                job.status = "failed"
                job.error = str(e)[:4000]
                await db.commit()
                await _post_zoho_callback(job, created_course_id, created_version_number)
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
)
async def generate_course(
    request: Request,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    sync: bool = Query(
        False,
        description=(
            "If true: wait for AI+PDF to finish and return full JSON in this response (HTTP 200). "
            "Use when the caller (e.g. Zoho) must receive pdf_url/course_id in one shot — "
            "request may take several minutes and can time out. "
            "If false (default): return only job_id + zoho_record_id (HTTP 202); poll GET /jobs/{job_id}."
        ),
    ),
):
    req = await _parse_generate_request(request)
    logger.info("Queueing course generation | zoho_record_id=%s sync=%s", req.zoho_record_id, sync)
    # Debug visibility for Zoho webhook mappings: log full request payload and top-level input keys.
    logger.info("Incoming /courses payload: %s", json.dumps(req.model_dump(), ensure_ascii=False))
    logger.info("Incoming input_data keys: %s", sorted(list((req.input_data or {}).keys())))
    try:
        async with db.begin():
            job = CourseJob(
                zoho_record_id=req.zoho_record_id,
                status="pending",
            )
            db.add(job)
        await db.refresh(job)
        logger.info(
            "Course generation job created | job_id=%s zoho_record_id=%s",
            str(job.id),
            req.zoho_record_id,
        )
    except (SQLAlchemyError, OSError, Exception):
        logger.exception("Database error while creating job")
        raise HTTPException(status_code=503, detail="Database unavailable. Please try again.")

    if sync:
        await process_course_job(job.id, req.zoho_record_id, req.input_data)
        async with AsyncSessionLocal() as db2:
            result = await db2.execute(select(CourseJob).where(CourseJob.id == job.id))
            job_done = result.scalars().first()
        if job_done is None:
            raise HTTPException(status_code=500, detail="Job finished but could not be reloaded.")
        logger.info("Sync course generation finished | job_id=%s status=%s", job.id, job_done.status)
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=_job_to_response(job_done).model_dump(mode="json"),
        )

    background_tasks.add_task(process_course_job, job.id, req.zoho_record_id, req.input_data)
    logger.info("Background task scheduled | job_id=%s", str(job.id))
    body = JobQueuedResponse(job_id=job.id, zoho_record_id=req.zoho_record_id)
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content=body.model_dump(mode="json"),
    )


@router.post(
    "/courses/{course_id}/refine",
    response_model=CourseVersionResponse,
    dependencies=[auth],
    summary="Refine a course outline with feedback",
)
async def refine_course(
    course_id: str,
    req: RefineCourseRequest,
    db: AsyncSession = Depends(get_db),
):
    # Validate UUID
    try:
        course_uuid = uuid.UUID(course_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid course_id format.")

    logger.info("Refine requested | course_id=%s", course_id)

    # Fetch latest version (read-only; do not hold locks during AI call)
    try:
        result = await db.execute(
            select(CourseVersion)
            .where(CourseVersion.course_id == course_uuid)
            .order_by(CourseVersion.version_number.desc())
        )
        last_version = result.scalars().first()
    except (SQLAlchemyError, OSError, Exception):
        logger.exception("Database error while reading last version")
        raise HTTPException(status_code=503, detail="Database unavailable. Please try again.")

    if not last_version:
        logger.warning("Refine failed: course not found | course_id=%s", course_id)
        raise HTTPException(status_code=404, detail="Course not found.")

    # Close out the implicit transaction opened by the SELECT so we don't hold it
    # during the long-running AI call (and to avoid "transaction already begun").
    base_outline = last_version.outline_text
    await db.rollback()

    # Call AI (structured refine first, fallback to text refine)
    try:
        ai = ClaudeService()
        try:
            logger.info("Refine AI started with structured mode | course_id=%s", course_id)
            refined_payload = await wait_for(
                ai.refine_course_outline_json(base_outline, req.feedback),
                timeout=310,
            )
            updated_outline = json.dumps(refined_payload.model_dump(), ensure_ascii=False, indent=2)
            logger.info("Refine AI structured mode completed | course_id=%s", course_id)
        except RuntimeError:
            logger.warning("Refine AI structured mode failed, using fallback | course_id=%s", course_id)
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
            logger.info("Refine AI fallback completed | course_id=%s", course_id)
    except AsyncTimeoutError:
        logger.warning("AI refine timed out for course_id=%s", course_id)
        raise HTTPException(status_code=504, detail="AI service timed out.")
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except RuntimeError as e:
        logger.warning("AI refine failed for course_id=%s error=%s", course_id, str(e))
        raise HTTPException(status_code=502, detail="AI service failed. Please retry.")

    # Generate PDF off the event loop
    pdf_url = None
    try:
        logger.info("Refine PDF generation started | course_id=%s", course_id)
        pdf_path = await generate_pdf_path_async(refined_payload if "refined_payload" in locals() and refined_payload is not None else updated_outline)
        pdf_url = _build_pdf_url(pdf_path)
        logger.info("Refine PDF generation completed | course_id=%s pdf_url=%s", course_id, pdf_url)
    except RuntimeError as e:
        logger.warning(
            "PDF generation unavailable; continuing without PDF | course_id=%s error=%s",
            course_id,
            str(e),
        )

    # Save new version (single transaction + basic race-safe increment)
    try:
        # Lock the course row so concurrent refinements serialize version increments.
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
    except (SQLAlchemyError, OSError, Exception):
        logger.exception("Database error while saving refined version")
        raise HTTPException(status_code=503, detail="Database unavailable. Please try again.")

    logger.info(f"Course refined: course_id={course_id} version={new_version_number}")

    return CourseVersionResponse(
        version_id=new_version.id,
        course_id=course_uuid,
        version_number=new_version.version_number,
        pdf_url=new_version.pdf_url,
        outline=new_version.outline_text,
        created_at=new_version.created_at,
    )


@router.get(
    "/courses/{course_id}/versions",
    response_model=CourseVersionsResponse,
    dependencies=[auth],
    summary="List all versions of a course",
)
async def list_versions(
    course_id: str,
    db: AsyncSession = Depends(get_db),
):
    logger.info("List versions requested | course_id=%s", course_id)
    try:
        course_uuid = uuid.UUID(course_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid course_id format.")

    # Fetch course
    try:
        result = await db.execute(select(Course).where(Course.id == course_uuid))
        course = result.scalars().first()
    except (SQLAlchemyError, OSError, Exception):
        logger.exception("Database error while reading course")
        raise HTTPException(status_code=503, detail="Database unavailable. Please try again.")
    if not course:
        raise HTTPException(status_code=404, detail="Course not found.")

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
        course_id=course.id,
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
    "/courses/{course_id}/versions/{version_number}",
    response_model=CourseVersionResponse,
    dependencies=[auth],
    summary="Get a specific version of a course",
)
async def get_version(
    course_id: str,
    version_number: int,
    db: AsyncSession = Depends(get_db),
):
    logger.info("Get version requested | course_id=%s version=%s", course_id, version_number)
    try:
        course_uuid = uuid.UUID(course_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid course_id format.")

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
        course_id=course_uuid,
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
    "/jobs/{job_id}",
    response_model=JobResponse,
    dependencies=[auth],
    summary="Get async job status",
)
async def get_job_status(
    job_id: str,
    db: AsyncSession = Depends(get_db),
):
    logger.info("Job status requested | job_id=%s", job_id)
    try:
        job_uuid = uuid.UUID(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job_id format.")

    try:
        result = await db.execute(select(CourseJob).where(CourseJob.id == job_uuid))
        job = result.scalars().first()
    except Exception:
        logger.exception("Database error while reading job")
        raise HTTPException(status_code=503, detail="Database unavailable. Please try again.")

    if not job:
        logger.warning("Job status lookup failed: not found | job_id=%s", job_id)
        raise HTTPException(status_code=404, detail="Job not found.")

    logger.info("Job status response | job_id=%s status=%s", job_id, job.status)

    return _job_to_response(job)
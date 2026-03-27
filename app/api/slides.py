import json
import os
import uuid
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, Header, HTTPException, UploadFile, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import select

from app.core.config import settings
from app.core.database import get_db
from app.models.job import CourseJob
from app.services.slides_service import process_slides_job
from app.utils.logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1", tags=["slides"])


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


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _job_to_dict(job: CourseJob) -> dict:
    created_at = getattr(job, "created_at", None)
    if isinstance(created_at, datetime):
        created_at = created_at.isoformat()
    google_file_id = None
    google_batch_links: list[str] = []
    gamma_batch_links: list[str] = []
    google_drive_course_folder_link = None
    try:
        payload = json.loads(getattr(job, "payload_json", "") or "{}")
        if isinstance(payload, dict):
            raw_id = payload.get("google_file_id")
            if isinstance(raw_id, str) and raw_id.strip():
                google_file_id = raw_id.strip()
            raw_links = payload.get("google_batch_links")
            if isinstance(raw_links, list):
                google_batch_links = [str(x).strip() for x in raw_links if str(x).strip()]
            raw_gamma_links = payload.get("gamma_batch_links")
            if isinstance(raw_gamma_links, list):
                gamma_batch_links = [str(x).strip() for x in raw_gamma_links if str(x).strip()]
            raw_folder_link = payload.get("google_drive_course_folder_link")
            if isinstance(raw_folder_link, str) and raw_folder_link.strip():
                google_drive_course_folder_link = raw_folder_link.strip()
    except Exception:
        google_file_id = None
        google_batch_links = []
        gamma_batch_links = []
        google_drive_course_folder_link = None
    return {
        "zoho_record_id": job.zoho_record_id,
        "job_type": getattr(job, "job_type", None),
        "status": job.status,
        "pdf_url": getattr(job, "pdf_url", None),
        "ppt_url": getattr(job, "ppt_url", None),
        "google_file_id": google_file_id,
        "google_batch_links": google_batch_links,
        "google_drive_course_folder_link": google_drive_course_folder_link,
        "gamma_batch_links": gamma_batch_links,
        "error": getattr(job, "error", None),
        "course_id": str(job.course_id) if getattr(job, "course_id", None) else None,
        "version_number": getattr(job, "version_number", None),
        "created_at": created_at,
    }


@router.post(
    "/slides/generate",
    dependencies=[auth],
    summary="Generate instructor slides (PPT) asynchronously",
)
@router.post(
    "/v2/slides/generate",
    dependencies=[auth],
    summary="Generate instructor slides (PPT) asynchronously [v2]",
)
async def generate_slides(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    zoho_record_id: str = Form(...),
    course_name: str | None = Form(None),
    outline_pdf: UploadFile = File(...),
    lesson_plan_and_activity_plan_pdf: UploadFile | None = File(None),
    instructor_ppt: UploadFile | None = File(None),
):
    """
    Creates a new job of type 'slides' and processes it in the background.
    """
    rid = (zoho_record_id or "").strip()
    if not rid:
        raise HTTPException(status_code=422, detail="zoho_record_id is required.")

    job_id = uuid.uuid4()
    upload_dir = os.path.join("uploads", "slides", str(job_id))
    _ensure_dir(upload_dir)
    logger.info(
        "Slides generate accepted | job_id=%s zoho_record_id=%s upload_dir=%s",
        str(job_id),
        rid,
        upload_dir,
    )

    async def _save_upload(upload: UploadFile, filename: str) -> str:
        file_path = os.path.join(upload_dir, filename)
        data = await upload.read()
        if not data:
            raise HTTPException(status_code=422, detail=f"Uploaded file {filename} is empty.")
        with open(file_path, "wb") as f:
            f.write(data)
        logger.info(
            "Slides upload saved | job_id=%s file=%s bytes=%s content_type=%s",
            str(job_id),
            filename,
            len(data),
            getattr(upload, "content_type", None),
        )
        return file_path

    outline_path = await _save_upload(outline_pdf, "outline.pdf")
    lesson_path = None
    instructor_path = None
    if lesson_plan_and_activity_plan_pdf is not None:
        lesson_path = await _save_upload(lesson_plan_and_activity_plan_pdf, "lesson_activity.pdf")
    if instructor_ppt is not None:
        instructor_path = await _save_upload(instructor_ppt, "instructor.pptx")

    payload = {
        "outline_pdf_path": outline_path,
        "lesson_plan_and_activity_plan_pdf_path": lesson_path,
        "instructor_ppt_path": instructor_path,
        "course_name": (course_name or "").strip() or "course",
    }
    logger.info(
        "Slides job payload prepared | job_id=%s has_lesson=%s has_instructor_ppt=%s",
        str(job_id),
        bool(lesson_path),
        bool(instructor_path),
    )

    try:
        async with db.begin():
            job = CourseJob(
                id=job_id,
                job_type="slides",
                zoho_record_id=rid,
                status="queued",
                payload_json=json.dumps(payload),
            )
            db.add(job)
    except (SQLAlchemyError, OSError, Exception):
        logger.exception("Database error while creating slides job")
        raise HTTPException(status_code=503, detail="Database unavailable. Please try again.")

    background_tasks.add_task(process_slides_job, job_id)
    logger.info("Slides background task scheduled | job_id=%s", str(job_id))
    body = {
        "zoho_record_id": rid,
        "job_type": "slides",
        "status": "queued",
        "message": "Slides job queued. Poll using zoho_record_id endpoint.",
        "polling": {
            "by_zoho_record_id": f"/api/v1/jobs/zoho/{rid}",
        },
    }
    return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=body)


@router.get(
    "/jobs/zoho/{zoho_record_id}",
    dependencies=[auth],
    summary="Get latest job status by zoho_record_id",
)
@router.get(
    "/v2/jobs/zoho/{zoho_record_id}",
    dependencies=[auth],
    summary="Get latest job status by zoho_record_id [deprecated alias]",
)
async def get_latest_job_by_zoho_record_id(
    zoho_record_id: str,
    db: AsyncSession = Depends(get_db),
):
    rid = (zoho_record_id or "").strip()
    if not rid:
        raise HTTPException(status_code=422, detail="zoho_record_id is required.")
    try:
        result = await db.execute(
            select(CourseJob)
            .where(CourseJob.zoho_record_id == rid)
            .order_by(CourseJob.created_at.desc())
        )
        job = result.scalars().first()
    except Exception:
        logger.exception("Database error while reading latest job by zoho_record_id")
        raise HTTPException(status_code=503, detail="Database unavailable. Please try again.")
    if not job:
        raise HTTPException(status_code=404, detail="No jobs found for this zoho_record_id.")
    return JSONResponse(status_code=status.HTTP_200_OK, content=_job_to_dict(job))


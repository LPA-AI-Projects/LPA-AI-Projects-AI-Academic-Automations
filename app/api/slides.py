import json
import os
import uuid
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, Header, HTTPException, UploadFile, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import settings
from app.core.database import get_db
from app.models.job import CourseJob
from app.schemas.job import JobQueuedResponse
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


@router.post(
    "/slides/generate",
    dependencies=[auth],
    summary="Generate instructor slides (PPT) asynchronously",
)
async def generate_slides(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    zoho_record_id: str = Form(...),
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
    body = JobQueuedResponse(job_id=job_id, zoho_record_id=rid)
    return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=body.model_dump(mode="json"))


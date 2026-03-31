import json
import os
import shutil
import uuid
from datetime import datetime
from typing import Optional

import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, Header, HTTPException, UploadFile, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import select

from app.core.config import settings
from app.core.database import get_db
from app.core.storage_paths import slides_upload_dir
from app.models.job import CourseJob
from app.services.slides_service import process_slides_job
from app.services.zoho_crm import (
    download_file_upload_content,
    get_record_file_upload_field,
    get_slides_module_api_name,
)
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
    module_gamma_links: list[dict[str, str | None]] = []
    zoho_attachment_payload: dict | None = None
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
            raw_module_links = payload.get("module_gamma_links")
            if isinstance(raw_module_links, list):
                for item in raw_module_links:
                    if isinstance(item, dict):
                        module_gamma_links.append(
                            {
                                "module_index": str(item.get("module_index") or "").strip() or None,
                                "link_name": str(item.get("link_name") or "").strip() or None,
                                "module_name": str(item.get("module_name") or "").strip() or None,
                                "gamma_link": str(item.get("gamma_link") or "").strip() or None,
                                "drive_link": str(item.get("drive_link") or "").strip() or None,
                                "file_id": str(item.get("file_id") or "").strip() or None,
                            }
                        )
            raw_zoho_payload = payload.get("zoho_attachment_payload")
            if isinstance(raw_zoho_payload, dict):
                zoho_attachment_payload = raw_zoho_payload
    except Exception:
        google_file_id = None
        google_batch_links = []
        gamma_batch_links = []
        google_drive_course_folder_link = None
        module_gamma_links = []
        zoho_attachment_payload = None
    return {
        "zoho_record_id": job.zoho_record_id,
        "job_type": getattr(job, "job_type", None),
        "status": job.status,
        "module_gamma_links": module_gamma_links,
        "error": getattr(job, "error", None),
        "created_at": created_at,
    }


@router.post(
    "/slides/",
    dependencies=[auth],
    summary="Generate instructor slides asynchronously (preferred path)",
)
@router.post(
    "/slides",
    dependencies=[auth],
    summary="Generate instructor slides asynchronously (no trailing slash)",
)
@router.post(
    "/slides/generate",
    dependencies=[auth],
    summary="Generate instructor slides (legacy path; use POST /api/v1/slides/)",
)
@router.post(
    "/v2/slides/generate",
    dependencies=[auth],
    summary="Generate instructor slides [deprecated alias]",
)
async def generate_slides(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    zoho_record_id: str = Form(...),
    course_name: str | None = Form(None),
    program_name: str | None = Form(None),
    outline_pdf: UploadFile | None = File(None),
    outline_pdf_url: str | None = Form(None),
    lesson_plan_and_activity_plan_pdf: UploadFile | None = File(None),
    lesson_plan_and_activity_plan_pdf_url: str | None = Form(None),
    instructor_ppt: UploadFile | None = File(None),
    instructor_ppt_url: str | None = Form(None),
):
    """
    Creates a new job of type 'slides' and processes it in the background.
    """
    rid = (zoho_record_id or "").strip()
    if not rid:
        raise HTTPException(status_code=422, detail="zoho_record_id is required.")

    job_id = uuid.uuid4()
    upload_dir = os.path.join(slides_upload_dir(), str(job_id))
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

    async def _save_from_url(url: str, filename: str) -> str:
        u = (url or "").strip()
        file_path = os.path.join(upload_dir, filename)
        if os.path.exists(u):
            shutil.copyfile(u, file_path)
            logger.info("Slides local file copied | job_id=%s file=%s src=%s", str(job_id), filename, u)
            return file_path
        if not (u.startswith("http://") or u.startswith("https://")):
            raise HTTPException(status_code=422, detail=f"Invalid URL/path for {filename}.")
        async with httpx.AsyncClient(timeout=180.0) as client:
            resp = await client.get(u, follow_redirects=True)
        if resp.status_code >= 400:
            raise HTTPException(
                status_code=422,
                detail=f"Failed to download input URL for {filename}: HTTP {resp.status_code}",
            )
        if not resp.content:
            raise HTTPException(status_code=422, detail=f"Downloaded file is empty for {filename}.")
        with open(file_path, "wb") as f:
            f.write(resp.content)
        logger.info("Slides URL downloaded | job_id=%s file=%s bytes=%s url=%s", str(job_id), filename, len(resp.content), u)
        return file_path

    async def _resolve_input_path(upload: UploadFile | None, url: str | None, filename: str, required: bool) -> str | None:
        if upload is not None:
            return await _save_upload(upload, filename)
        if (url or "").strip():
            return await _save_from_url(str(url), filename)
        if required and filename == "outline.pdf":
            # Test mode: if outline is not passed in request, fetch Zoho File Upload field `outline`.
            logger.info(
                "Slides outline not provided in request; fetching from Zoho CRM | job_id=%s zoho_record_id=%s field=outline",
                str(job_id),
                rid,
            )
            try:
                outline_meta = await get_record_file_upload_field(
                    module_api_name=get_slides_module_api_name(),
                    crm_record_id=rid,
                    field_api_name="outline",
                )
                logger.info(
                    "Slides Zoho outline metadata | job_id=%s file_id=%s has_download_url=%s file_name=%s",
                    str(job_id),
                    outline_meta.get("file_id"),
                    bool(outline_meta.get("download_url")),
                    outline_meta.get("file_name"),
                )
                file_bytes = await download_file_upload_content(
                    file_id=outline_meta.get("file_id"),
                    download_url=outline_meta.get("download_url"),
                )
                with open(os.path.join(upload_dir, filename), "wb") as f:
                    f.write(file_bytes)
                logger.info(
                    "Slides Zoho outline downloaded and saved | job_id=%s file=%s bytes=%s",
                    str(job_id),
                    filename,
                    len(file_bytes),
                )
                return os.path.join(upload_dir, filename)
            except Exception as e:
                logger.exception(
                    "Slides failed to fetch outline from Zoho CRM | job_id=%s zoho_record_id=%s",
                    str(job_id),
                    rid,
                )
                raise HTTPException(
                    status_code=422,
                    detail=f"Failed to fetch outline from Zoho CRM field 'outline': {str(e)}",
                )
        if required:
            raise HTTPException(
                status_code=422,
                detail=f"{filename} is required. Provide either file upload or URL field.",
            )
        return None

    outline_path = await _resolve_input_path(outline_pdf, outline_pdf_url, "outline.pdf", True)
    lesson_path = await _resolve_input_path(
        lesson_plan_and_activity_plan_pdf,
        lesson_plan_and_activity_plan_pdf_url,
        "lesson_activity.pdf",
        False,
    )
    instructor_path = await _resolve_input_path(instructor_ppt, instructor_ppt_url, "instructor.pptx", False)

    payload = {
        "outline_pdf_path": outline_path,
        "lesson_plan_and_activity_plan_pdf_path": lesson_path,
        "instructor_ppt_path": instructor_path,
        "course_name": (course_name or "").strip() or "course",
        "program_name": (program_name or "").strip() or None,
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
            "by_zoho_record_id": f"/api/v1/slides/{rid}",
        },
    }
    return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=body)


@router.get(
    "/slides/{zoho_record_id}",
    dependencies=[auth],
    summary="Get latest slides job status by zoho_record_id",
)
async def get_latest_slides_job_by_zoho_record_id(
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
                CourseJob.job_type == "slides",
            )
            .order_by(CourseJob.created_at.desc())
        )
        job = result.scalars().first()
    except Exception:
        logger.exception("Database error while reading latest slides job by zoho_record_id")
        raise HTTPException(status_code=503, detail="Database unavailable. Please try again.")
    if not job:
        raise HTTPException(
            status_code=404,
            detail="No slides jobs found for this zoho_record_id.",
        )
    return JSONResponse(status_code=status.HTTP_200_OK, content=_job_to_dict(job))

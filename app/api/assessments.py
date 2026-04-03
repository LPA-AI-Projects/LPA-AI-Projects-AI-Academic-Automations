"""
Pre/post assessment: curriculum PDF → MCQs in job payload_json (job_type=assessment).
"""
from __future__ import annotations

import json
import os
import shutil
import uuid

import httpx
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    File,
    Form,
    Header,
    HTTPException,
    Query,
    UploadFile,
    status,
)
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import AsyncSessionLocal, get_db
from app.core.storage_paths import assessments_upload_dir
from app.models.job import CourseJob
from app.schemas.assessment import AssessmentJobStatusResponse, AssessmentQueuedResponse
from app.schemas.assessment import AssessmentAppGenerateResponse
from app.services.assessment_service import (
    JOB_TYPE,
    DEFAULT_NUM_QUESTIONS,
    find_latest_completed_pre_job,
    find_post_job_for_pre,
    normalize_difficulty,
    post_difficulty_from_pre,
    process_assessment_job,
)
from app.services.assessment_app_service import (
    build_assessment_react_app_local,
    extract_outline_text_from_upload,
    resolve_course_name_for_reuse,
    resolve_outline_text_for_reuse,
)
from app.utils.logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1", tags=["assessments"])


def verify_api_key(
    x_api_key: str | None = Header(None, description="Your API secret key"),
):
    if not x_api_key or x_api_key != settings.API_SECRET_KEY:
        logger.warning("Rejected request: invalid API key")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key.",
        )


auth = Depends(verify_api_key)


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _payload_dict(job: CourseJob) -> dict:
    try:
        p = json.loads(job.payload_json or "{}")
        return p if isinstance(p, dict) else {}
    except Exception:
        return {}


def _to_status_response(job: CourseJob) -> AssessmentJobStatusResponse:
    p = _payload_dict(job)
    phase = str(p.get("phase") or p.get("assessment_type") or "pre").lower()
    questions = p.get("questions") if job.status == "completed" else []
    if not isinstance(questions, list):
        questions = []
    docx_url = p.get("assessment_docx_drive_url")
    if isinstance(docx_url, str) and docx_url.strip():
        docx_url = docx_url.strip()
    else:
        docx_url = None
    return AssessmentJobStatusResponse(
        job_id=job.id,
        zoho_record_id=job.zoho_record_id,
        status=job.status,
        type=phase,
        questions=questions,
        assessment_docx_drive_url=docx_url,
        error=job.error,
        created_at=job.created_at,
        num_questions=p.get("num_questions"),
        difficulty=p.get("difficulty"),
        course_name=p.get("course_name"),
    )


@router.post(
    "/assessments/pre",
    dependencies=[auth],
    status_code=status.HTTP_202_ACCEPTED,
    summary="Queue pre-assessment MCQs from curriculum PDF",
    response_model=AssessmentQueuedResponse,
)
async def create_pre_assessment(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    zoho_record_id: str = Form(...),
    course_name: str = Form(...),
    difficulty: str = Form("intermediate"),
    curriculum_pdf: UploadFile | None = File(None),
    curriculum_pdf_url: str | None = Form(None),
    num_questions: int | None = Form(None),
):
    rid = (zoho_record_id or "").strip()
    if not rid:
        raise HTTPException(status_code=422, detail="zoho_record_id is required.")
    cname = (course_name or "").strip()
    if not cname:
        raise HTTPException(status_code=422, detail="course_name is required.")

    nq = int(num_questions) if num_questions is not None else DEFAULT_NUM_QUESTIONS
    if nq < 1:
        nq = DEFAULT_NUM_QUESTIONS
    if nq > 50:
        nq = 50

    job_id = uuid.uuid4()
    upload_dir = os.path.join(assessments_upload_dir(), str(job_id))
    _ensure_dir(upload_dir)

    async def _save_upload(upload: UploadFile, filename: str) -> str:
        file_path = os.path.join(upload_dir, filename)
        data = await upload.read()
        if not data:
            raise HTTPException(status_code=422, detail=f"Uploaded file {filename} is empty.")
        with open(file_path, "wb") as f:
            f.write(data)
        return file_path

    async def _save_from_url(url: str, filename: str) -> str:
        u = (url or "").strip()
        file_path = os.path.join(upload_dir, filename)
        if os.path.exists(u):
            shutil.copyfile(u, file_path)
            return file_path
        if not (u.startswith("http://") or u.startswith("https://")):
            raise HTTPException(status_code=422, detail="Invalid curriculum_pdf_url.")
        async with httpx.AsyncClient(timeout=180.0) as client:
            resp = await client.get(u, follow_redirects=True)
        if resp.status_code >= 400 or not resp.content:
            raise HTTPException(
                status_code=422,
                detail=f"Failed to download curriculum PDF: HTTP {resp.status_code}",
            )
        with open(file_path, "wb") as f:
            f.write(resp.content)
        return file_path

    if curriculum_pdf is not None:
        pdf_path = await _save_upload(curriculum_pdf, "curriculum.pdf")
    elif (curriculum_pdf_url or "").strip():
        pdf_path = await _save_from_url(str(curriculum_pdf_url), "curriculum.pdf")
    else:
        raise HTTPException(
            status_code=422,
            detail="Provide curriculum_pdf file or curriculum_pdf_url.",
        )

    diff = normalize_difficulty(difficulty)
    payload = {
        "phase": "pre",
        "assessment_type": "pre",
        "course_name": cname,
        "difficulty": diff,
        "num_questions": nq,
        "curriculum_pdf_path": pdf_path,
        "training_completed": False,
        "post_requested": False,
    }

    try:
        async with db.begin():
            job = CourseJob(
                id=job_id,
                job_type=JOB_TYPE,
                zoho_record_id=rid,
                status="queued",
                payload_json=json.dumps(payload),
            )
            db.add(job)
    except (SQLAlchemyError, OSError, Exception):
        logger.exception("Database error while creating assessment job")
        raise HTTPException(status_code=503, detail="Database unavailable. Please try again.")

    background_tasks.add_task(process_assessment_job, job_id)
    body = AssessmentQueuedResponse(
        job_id=job_id,
        zoho_record_id=rid,
        status="queued",
        message="Pre-assessment queued. Poll GET /api/v1/assessments/{zoho_record_id} or /api/v1/status/{zoho_record_id}.",
        polling={
            "by_zoho_record_id": f"/api/v1/assessments/{rid}",
            "status_alias": f"/api/v1/status/{rid}",
        },
    )
    return body


@router.post(
    "/assessments/{zoho_record_id}/complete",
    dependencies=[auth],
    summary="Mark training complete and queue post-assessment (higher difficulty)",
)
async def mark_training_complete_and_queue_post(
    zoho_record_id: str,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    num_questions: int | None = Query(
        None,
        description="Override question count; defaults to pre-assessment count.",
    ),
):
    rid = (zoho_record_id or "").strip()
    if not rid:
        raise HTTPException(status_code=422, detail="zoho_record_id is required.")

    pre = await find_latest_completed_pre_job(db, rid)
    if pre is None:
        raise HTTPException(
            status_code=404,
            detail="No completed pre-assessment found for this zoho_record_id.",
        )

    pre_payload = _payload_dict(pre)
    pre_diff = normalize_difficulty(str(pre_payload.get("difficulty") or "intermediate"))
    post_level = post_difficulty_from_pre(pre_diff)
    excerpt = str(pre_payload.get("curriculum_text_excerpt") or "").strip()
    if not excerpt:
        raise HTTPException(
            status_code=400,
            detail="Pre-assessment payload missing curriculum excerpt; cannot run post.",
        )

    nq = int(num_questions) if num_questions is not None else int(
        pre_payload.get("num_questions") or DEFAULT_NUM_QUESTIONS
    )
    if nq < 1:
        nq = DEFAULT_NUM_QUESTIONS
    if nq > 50:
        nq = 50

    existing = await find_post_job_for_pre(db, rid, str(pre.id))
    if existing is not None:
        if existing.status in ("queued", "processing"):
            return JSONResponse(
                status_code=status.HTTP_202_ACCEPTED,
                content={
                    "message": "Post-assessment already in progress.",
                    "job_id": str(existing.id),
                    "status": existing.status,
                },
            )
        if existing.status == "completed":
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={
                    "message": "Post-assessment already completed for this pre-assessment.",
                    "job_id": str(existing.id),
                    "status": existing.status,
                },
            )

    job_id = uuid.uuid4()
    course_name = str(pre_payload.get("course_name") or "course")
    payload = {
        "phase": "post",
        "assessment_type": "post",
        "course_name": course_name,
        "difficulty": post_level,
        "pre_difficulty": pre_diff,
        "num_questions": nq,
        "curriculum_text_excerpt": excerpt,
        "pre_job_id": str(pre.id),
        "training_completed": True,
        "post_requested": True,
    }

    try:
        async with db.begin():
            job = CourseJob(
                id=job_id,
                job_type=JOB_TYPE,
                zoho_record_id=rid,
                status="queued",
                payload_json=json.dumps(payload),
            )
            db.add(job)
    except (SQLAlchemyError, OSError, Exception):
        logger.exception("Database error while creating post-assessment job")
        raise HTTPException(status_code=503, detail="Database unavailable. Please try again.")

    background_tasks.add_task(process_assessment_job, job_id)
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={
            "job_id": str(job_id),
            "zoho_record_id": rid,
            "status": "queued",
            "message": "Post-assessment queued.",
            "polling": {
                "by_zoho_record_id": f"/api/v1/assessments/{rid}",
                "status_alias": f"/api/v1/status/{rid}",
            },
        },
    )


@router.get(
    "/assessments/{zoho_record_id}",
    dependencies=[auth],
    response_model=AssessmentJobStatusResponse,
    summary="Latest assessment job (pre or post) for Zoho record id",
)
@router.get(
    "/status/{zoho_record_id}",
    dependencies=[auth],
    response_model=AssessmentJobStatusResponse,
    summary="Alias: latest assessment job status (same as /assessments/{zoho_record_id})",
)
async def get_latest_assessment_status(
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
                CourseJob.job_type == JOB_TYPE,
            )
            .order_by(CourseJob.created_at.desc())
        )
        job = result.scalars().first()
    except Exception:
        logger.exception("Database error while reading assessment job")
        raise HTTPException(status_code=503, detail="Database unavailable. Please try again.")

    if job is None:
        raise HTTPException(status_code=404, detail="No assessment job found for this zoho_record_id.")

    return _to_status_response(job)


@router.post(
    "/assessments/app",
    dependencies=[auth],
    summary="Generate local React pre/post assessment app from outline (local test mode)",
    response_model=AssessmentAppGenerateResponse,
)
async def generate_assessment_react_app(
    course_name: str | None = Form(None),
    pre_level: str = Form(...),
    post_level: str = Form(...),
    num_questions: int = Form(DEFAULT_NUM_QUESTIONS),
    deploy_to_codesandbox: bool = Form(False),
    outline_text: str | None = Form(None),
    outline_pdf: UploadFile | None = File(None),
    zoho_record_id: str | None = Form(None),
):
    pre_level = normalize_difficulty(pre_level)
    post_level = normalize_difficulty(post_level)

    nq = int(num_questions) if num_questions is not None else DEFAULT_NUM_QUESTIONS
    if nq < 1:
        nq = DEFAULT_NUM_QUESTIONS
    if nq > 50:
        nq = 50

    text = (outline_text or "").strip()
    rid = (zoho_record_id or "").strip()
    cname = (course_name or "").strip()

    if not cname and rid:
        try:
            async with AsyncSessionLocal() as db:
                resolved_name = await resolve_course_name_for_reuse(db=db, zoho_record_id=rid)
                if resolved_name:
                    cname = resolved_name
        except Exception:
            logger.warning("DB unavailable for course_name reuse | zoho_record_id=%s", rid)

    if not cname:
        cname = f"course_{rid}" if rid else "course"
    if not text and not outline_pdf and rid:
        try:
            async with AsyncSessionLocal() as db:
                reused = await resolve_outline_text_for_reuse(db=db, zoho_record_id=rid)
                if reused:
                    text = reused
        except Exception:
            # DB might be down locally; allow caller to fall back to outline_text/outline_pdf.
            logger.warning("DB unavailable for reuse; provide outline_text or outline_pdf | zoho_record_id=%s", rid)
    if not text and outline_pdf is not None:
        b = await outline_pdf.read()
        if not b:
            raise HTTPException(status_code=422, detail="outline_pdf is empty.")
        text = await extract_outline_text_from_upload(file_bytes=b)
    if not text:
        raise HTTPException(status_code=422, detail="Provide outline_text or outline_pdf.")

    try:
        result = await build_assessment_react_app_local(
            course_name=cname,
            outline_text=text,
            pre_level=pre_level,
            post_level=post_level,
            num_questions=nq,
            deploy_to_codesandbox=deploy_to_codesandbox,
        )
    except Exception as e:
        logger.exception("Assessment React app generation failed")
        raise HTTPException(status_code=500, detail=str(e))

    return AssessmentAppGenerateResponse(**result)

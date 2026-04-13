"""
Pre/post assessment: curriculum PDF → MCQs in job payload_json (job_type=assessment).
"""
from __future__ import annotations

import json
import os
import shutil
import uuid
from typing import Literal

from pydantic import BaseModel, Field

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
from app.core.database import get_db
from app.core.storage_paths import assessments_upload_dir
from app.models.job import CourseJob
from app.schemas.assessment import AssessmentJobStatusResponse, AssessmentQueuedResponse
from app.services.assessment_service import (
    JOB_TYPE,
    DEFAULT_NUM_QUESTIONS,
    find_latest_completed_pre_job,
    find_post_job_for_pre,
    normalize_difficulty,
    post_difficulty_from_pre,
    process_assessment_job,
    generate_assessment_questions_from_text,
)
from app.services.assessment_app_service import (
    build_lovable_assessment_prompt,
    build_react_quiz_files,
    create_codesandbox_from_files,
    create_lovable_build_url,
    flatten_validated_slides_to_text,
    lovable_prompt_for_build_url,
)
from app.utils.logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1", tags=["assessments"])


class AssessmentAppFromCacheRequest(BaseModel):
    cache_path: str = Field(..., description="Absolute path to validated_slides.json")
    course_name: str = Field("Assessment Quiz")
    difficulty: str = Field("intermediate")
    num_questions: int = Field(15, ge=1, le=50)
    seconds_per_question: int = Field(60, ge=10, le=300)
    deploy_target: Literal["lovable", "codesandbox"] = Field(
        "lovable",
        description="lovable: Lovable Build-with-URL + full prompt (UI generated in Lovable). "
        "codesandbox: ship the built-in React/Vite template to CodeSandbox.",
    )


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
    summary="Generate assessment app: Lovable (default) or CodeSandbox from slides cache",
)
async def generate_assessment_app_from_cache(req: AssessmentAppFromCacheRequest):
    cache_path = str(req.cache_path or "").strip()
    if not cache_path or not os.path.isfile(cache_path):
        raise HTTPException(status_code=422, detail="cache_path not found on disk.")

    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            cache_data = json.load(f)
    except Exception:
        raise HTTPException(status_code=422, detail="Failed to parse cache JSON.")

    curriculum_text = flatten_validated_slides_to_text(cache_data)
    if not curriculum_text:
        raise HTTPException(status_code=422, detail="No modules/slides found in cache file.")

    questions = await generate_assessment_questions_from_text(
        phase="post",
        difficulty=req.difficulty,
        course_name=req.course_name,
        curriculum_text=curriculum_text,
        num_questions=req.num_questions,
        pre_difficulty=None,
    )
    if not questions:
        raise HTTPException(status_code=500, detail="Failed to generate questions from cache.")

    if req.deploy_target == "lovable":
        full_prompt = build_lovable_assessment_prompt(
            course_name=req.course_name,
            questions=questions,
            seconds_per_question=req.seconds_per_question,
        )
        url_prompt, truncated = lovable_prompt_for_build_url(
            full_prompt,
            course_name=req.course_name,
            seconds_per_question=req.seconds_per_question,
        )
        lovable_url = create_lovable_build_url(prompt=url_prompt)
        return {
            "status": "ok",
            "cache_path": cache_path,
            "questions_count": len(questions),
            "deploy_provider": "lovable",
            "lovable_build_url": lovable_url,
            "lovable_prompt": full_prompt,
            "lovable_prompt_in_url_truncated": truncated,
            "questions": questions,
            "message": (
                "Open lovable_build_url to start Lovable. If the URL used a short bootstrap "
                "(lovable_prompt_in_url_truncated=true), paste lovable_prompt into Lovable so it has the full question JSON."
            ),
        }

    files = build_react_quiz_files(
        title=req.course_name,
        questions=questions,
        seconds_per_question=req.seconds_per_question,
    )
    try:
        sandbox_id, app_url, editor_url = await create_codesandbox_from_files(files)
    except Exception as e:
        logger.exception("CodeSandbox deploy failed")
        raise HTTPException(
            status_code=502,
            detail=f"Failed to create CodeSandbox: {e!s}",
        ) from e

    out: dict = {
        "status": "ok",
        "cache_path": cache_path,
        "questions_count": len(questions),
        "deploy_provider": "codesandbox-sdk",
        "codesandbox_id": sandbox_id,
        "app_url": app_url,
        "message": "Assessment app deployed to a CodeSandbox VM sandbox (Devbox). Open app_url for the running Vite preview.",
    }
    if editor_url:
        out["codesandbox_editor_url"] = editor_url
    return out

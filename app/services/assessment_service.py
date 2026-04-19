"""
Pre/post assessment: curriculum PDF text extraction + LLM MCQs in payload_json;
optional Word (.docx) upload to Google Drive under ai_automation/pre_and_post_assistance/...
"""
from __future__ import annotations

import asyncio
import json
import os
from typing import Any

from sqlalchemy import select

from app.core.database import AsyncSessionLocal
from app.models.job import CourseJob
from app.services.assessment_docx import build_assessment_docx_bytes
from app.services.claude import ClaudeService
from app.services.document_extractor import extract_pdf_text_async
from app.services.google_drive import GoogleDriveUploadError, upload_assessment_docx_to_drive
from app.utils.logger import get_logger

logger = get_logger(__name__)

JOB_TYPE = "assessment"

# Post-assessment difficulty is one level above pre (per product spec)
LEVEL_MAP: dict[str, str] = {
    "basic": "intermediate",
    "intermediate": "advanced",
    "advanced": "advanced",
}

DEFAULT_NUM_QUESTIONS = 15
MAX_CURRICULUM_CHARS = 100_000


def normalize_difficulty(raw: str | None) -> str:
    s = (raw or "").strip().lower()
    if s in ("basic", "beginner", "fundamental", "entry"):
        return "basic"
    if s in ("intermediate", "intermed", "medium"):
        return "intermediate"
    if s in ("advanced", "expert"):
        return "advanced"
    return "intermediate"


def post_difficulty_from_pre(pre_level: str) -> str:
    return LEVEL_MAP.get(normalize_difficulty(pre_level), "advanced")


def _truncate(text: str, max_chars: int) -> str:
    t = (text or "").strip()
    if len(t) <= max_chars:
        return t
    return t[:max_chars] + "\n\n[… curriculum truncated for model context …]"


def _strip_json_fence(raw: str) -> str:
    text = (raw or "").strip()
    if text.startswith("```"):
        lines = text.split("\n")
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines)
    return text.strip()


def _parse_questions_json(raw_llm: str) -> list[dict[str, Any]]:
    text = _strip_json_fence(raw_llm)
    data = json.loads(text)
    if isinstance(data, dict) and "questions" in data:
        qs = data["questions"]
    elif isinstance(data, list):
        qs = data
    else:
        raise ValueError("Expected JSON object with 'questions' array or a top-level array.")

    if not isinstance(qs, list):
        raise ValueError("questions must be a list.")

    cleaned: list[dict[str, Any]] = []
    for i, q in enumerate(qs):
        if not isinstance(q, dict):
            continue
        stem = str(q.get("question") or q.get("stem") or "").strip()
        opts = q.get("options")
        if not isinstance(opts, list):
            opts = []
        opts = [str(o).strip() for o in opts[:4]]
        while len(opts) < 4:
            opts.append("")
        ci = q.get("correct_index")
        if ci is None and q.get("correct_answer") is not None:
            # letter A-D
            letter = str(q.get("correct_answer")).strip().upper()
            ci = {"A": 0, "B": 1, "C": 2, "D": 3}.get(letter[:1], 0)
        try:
            cidx = int(ci)
        except (TypeError, ValueError):
            cidx = 0
        cidx = max(0, min(3, cidx))
        cleaned.append(
            {
                "id": i + 1,
                "question": stem,
                "options": opts,
                "correct_index": cidx,
            }
        )
    return cleaned


def build_system_prompt(*, phase: str, difficulty: str, num_questions: int) -> str:
    phase = "post" if phase == "post" else "pre"
    return f"""You are an assessment generator for corporate training.

Rules:
- Output ONLY valid JSON (no markdown fences, no explanation text outside JSON).
- Generate exactly {num_questions} multiple-choice questions.
- Each question has exactly 4 options (strings) and one correct answer indicated by "correct_index" (0-3).
- Do not include explanations or rationale in the JSON.
- Difficulty for this assessment: {difficulty}.
- Assessment phase: {phase}.
"""


def build_user_prompt(
    *,
    phase: str,
    difficulty: str,
    course_name: str,
    curriculum_excerpt: str,
    num_questions: int,
    pre_difficulty: str | None = None,
    nonce: str | None = None,
) -> str:
    phase = "post" if phase == "post" else "pre"
    extra = ""
    if phase == "post":
        extra = (
            f"\nThe learner already completed training on this curriculum. "
            f"Pre-training difficulty was approximately {pre_difficulty or 'the level below'}. "
            "Write questions that measure learning growth and deeper application — "
            "harder than a baseline pre-test and aligned to the elevated difficulty.\n"
        )
    nonce_block = ""
    if nonce:
        # The nonce is a per-request salt that pushes the model to vary phrasing,
        # answer ordering and topic emphasis between successive calls. It is NOT
        # used to seed any deterministic randomness — its only job is to make
        # otherwise identical prompts produce different question sets.
        nonce_block = (
            f"\nGeneration nonce (for variability — do NOT echo, do NOT include in output): {nonce}\n"
            "Treat this as a fresh assessment authoring session. Vary question phrasing, "
            "topic emphasis within the curriculum, distractor wording and option ordering "
            "compared to any previous session you might imagine.\n"
        )
    return f"""Course name: {course_name}

Curriculum content (excerpt):
---
{_truncate(curriculum_excerpt, MAX_CURRICULUM_CHARS)}
---
{extra}{nonce_block}
Produce JSON in this exact shape:
{{
  "questions": [
    {{
      "question": "Question text here",
      "options": ["Option A", "Option B", "Option C", "Option D"],
      "correct_index": 0
    }}
  ]
}}

Use exactly {num_questions} questions. correct_index must be 0, 1, 2, or 3.
"""


async def _generate_mcqs(
    *,
    phase: str,
    difficulty: str,
    course_name: str,
    curriculum_text: str,
    num_questions: int,
    pre_difficulty: str | None,
    nonce: str | None = None,
) -> list[dict[str, Any]]:
    ai = ClaudeService()
    sys_p = build_system_prompt(phase=phase, difficulty=difficulty, num_questions=num_questions)
    usr_p = build_user_prompt(
        phase=phase,
        difficulty=difficulty,
        course_name=course_name,
        curriculum_excerpt=curriculum_text,
        num_questions=num_questions,
        pre_difficulty=pre_difficulty,
        nonce=nonce,
    )
    raw = await ai.generate_text_completion(system_prompt=sys_p, user_prompt=usr_p, timeout_s=300.0)
    questions = _parse_questions_json(raw)
    if len(questions) < num_questions:
        logger.warning(
            "Assessment: model returned fewer questions than requested | got=%s want=%s",
            len(questions),
            num_questions,
        )
    return questions[:num_questions]


async def generate_assessment_questions_from_text(
    *,
    phase: str,
    difficulty: str,
    course_name: str,
    curriculum_text: str,
    num_questions: int,
    pre_difficulty: str | None = None,
    nonce: str | None = None,
) -> list[dict[str, Any]]:
    """
    Reusable assessment generation helper for non-assessment pipelines (e.g. slides).
    Does not persist any DB state.

    Pass a fresh ``nonce`` per call when you need successive invocations to
    produce DIFFERENT question sets from the same curriculum (e.g. on-demand
    generation behind a public learner URL).
    """
    phase_norm = "post" if str(phase or "").strip().lower() == "post" else "pre"
    diff = normalize_difficulty(difficulty)
    nq = int(num_questions or DEFAULT_NUM_QUESTIONS)
    if nq < 1:
        nq = DEFAULT_NUM_QUESTIONS
    if nq > 50:
        nq = 50
    text = _truncate(str(curriculum_text or ""), MAX_CURRICULUM_CHARS)
    if not text:
        return []
    return await _generate_mcqs(
        phase=phase_norm,
        difficulty=diff,
        course_name=str(course_name or "course").strip() or "course",
        curriculum_text=text,
        num_questions=nq,
        pre_difficulty=pre_difficulty,
        nonce=nonce,
    )


async def process_assessment_job(job_id) -> None:
    """Background worker: extract PDF (pre only), generate MCQs, persist payload_json."""
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(CourseJob).where(CourseJob.id == job_id))
        job = result.scalars().first()
        if job is None:
            logger.error("Assessment job missing | job_id=%s", str(job_id))
            return

        try:
            payload: dict[str, Any] = {}
            try:
                payload = json.loads(job.payload_json or "{}")
            except Exception:
                payload = {}

            if not isinstance(payload, dict):
                payload = {}

            phase = str(payload.get("phase") or payload.get("assessment_type") or "pre").lower()
            if phase not in ("pre", "post"):
                phase = "pre"

            job.status = "processing"
            job.error = None
            await db.commit()

            course_name = str(payload.get("course_name") or "course").strip() or "course"
            difficulty = normalize_difficulty(str(payload.get("difficulty") or "intermediate"))
            num_q = int(payload.get("num_questions") or DEFAULT_NUM_QUESTIONS)
            if num_q < 1:
                num_q = DEFAULT_NUM_QUESTIONS
            if num_q > 50:
                num_q = 50

            curriculum_text = str(payload.get("curriculum_text_excerpt") or "").strip()

            if phase == "pre":
                pdf_path = str(payload.get("curriculum_pdf_path") or "").strip()
                if not pdf_path or not os.path.isfile(pdf_path):
                    raise RuntimeError("curriculum_pdf_path missing or file not found for pre-assessment.")
                with open(pdf_path, "rb") as f:
                    b = f.read()
                curriculum_text = await extract_pdf_text_async(b)
                curriculum_text = _truncate(curriculum_text, MAX_CURRICULUM_CHARS)
                payload["curriculum_text_excerpt"] = curriculum_text
                payload["difficulty"] = difficulty

            if not curriculum_text:
                raise RuntimeError("No curriculum text available for assessment generation.")

            pre_diff = None
            if phase == "post":
                pre_diff = str(payload.get("pre_difficulty") or "").strip() or None

            questions = await _generate_mcqs(
                phase=phase,
                difficulty=difficulty,
                course_name=course_name,
                curriculum_text=curriculum_text,
                num_questions=num_q,
                pre_difficulty=pre_diff,
            )

            payload["questions"] = questions
            payload["phase"] = phase
            payload["assessment_type"] = phase
            payload["num_questions"] = len(questions)
            payload["training_completed"] = bool(payload.get("training_completed"))

            try:
                docx_bytes = await asyncio.to_thread(
                    build_assessment_docx_bytes,
                    course_name=course_name,
                    phase=phase,
                    difficulty=difficulty,
                    questions=questions,
                )
                drive_up = await asyncio.to_thread(
                    upload_assessment_docx_to_drive,
                    docx_bytes,
                    course_name=course_name,
                    zoho_record_id=job.zoho_record_id,
                    phase=phase,
                )
                payload["assessment_docx_drive_url"] = drive_up.get("edit_link")
                payload["assessment_docx_file_id"] = drive_up.get("file_id")
            except GoogleDriveUploadError as e:
                logger.warning(
                    "Assessment DOCX not uploaded to Drive | job_id=%s phase=%s error=%s",
                    str(job.id),
                    phase,
                    str(e),
                )
            except Exception:
                logger.exception(
                    "Assessment DOCX build/upload failed | job_id=%s phase=%s",
                    str(job.id),
                    phase,
                )

            job.payload_json = json.dumps(payload)
            job.status = "completed"
            job.error = None
            await db.commit()
            logger.info(
                "Assessment job completed | job_id=%s phase=%s questions=%s",
                str(job.id),
                phase,
                len(questions),
            )
        except Exception as e:
            err = str(e)[:4000]
            logger.exception("Assessment job failed | job_id=%s", str(job_id))
            try:
                job_result = await db.execute(select(CourseJob).where(CourseJob.id == job_id))
                job2 = job_result.scalars().first()
                if job2 is not None:
                    job2.status = "failed"
                    job2.error = err
                    await db.commit()
            except Exception:
                logger.exception("Assessment job failed to persist error state | job_id=%s", str(job_id))


async def find_latest_completed_pre_job(db, zoho_record_id: str) -> CourseJob | None:
    rid = (zoho_record_id or "").strip()
    result = await db.execute(
        select(CourseJob)
        .where(
            CourseJob.zoho_record_id == rid,
            CourseJob.job_type == JOB_TYPE,
            CourseJob.status == "completed",
        )
        .order_by(CourseJob.created_at.desc())
    )
    rows = result.scalars().all()
    for j in rows:
        try:
            p = json.loads(j.payload_json or "{}")
        except Exception:
            continue
        if not isinstance(p, dict):
            continue
        ph = str(p.get("phase") or p.get("assessment_type") or "").lower()
        if ph == "pre" and p.get("questions"):
            return j
    return None


async def find_post_job_for_pre(db, zoho_record_id: str, pre_job_id: str) -> CourseJob | None:
    """Return latest post job for this Zoho record that references the given pre job."""
    rid = (zoho_record_id or "").strip()
    result = await db.execute(
        select(CourseJob)
        .where(
            CourseJob.job_type == JOB_TYPE,
            CourseJob.zoho_record_id == rid,
        )
        .order_by(CourseJob.created_at.desc())
    )
    for j in result.scalars().all():
        try:
            p = json.loads(j.payload_json or "{}")
        except Exception:
            continue
        if not isinstance(p, dict):
            continue
        if str(p.get("pre_job_id") or "") == pre_job_id:
            ph = str(p.get("phase") or "").lower()
            if ph == "post":
                return j
    return None

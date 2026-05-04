"""
On-demand courseware assessments — pre / post MCQs generated FRESH per request.

Design notes:

- These endpoints are the public-facing surface that the Vercel quiz pages
  proxy to. There is **no caching of generated questions** server-side: every
  call runs a fresh LLM generation with a per-request nonce so two learners
  opening the same link see DIFFERENT question sets.
- Source content is resolved through ``courseware_assessment_resolver``, which
  always picks the LATEST completed slides job for the record (handles re-runs
  and content drift).
- Auth: the trusted Vercel server route should pass ``X-API-Key``. For browser-
  facing public links you can additionally enable ``ASSESSMENT_LINK_REQUIRE_TOKEN``
  to require the signed ``t=`` query parameter that was minted at slides job
  completion.
- Rate limits + structured metrics replace caching as the primary cost control.
"""
from __future__ import annotations

import hashlib
import secrets
import time
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import get_db
from app.services.assessment_service import (
    DEFAULT_NUM_QUESTIONS,
    generate_assessment_questions_from_text,
    normalize_difficulty,
    post_difficulty_from_pre,
)
from app.services.courseware_assessment_resolver import (
    CoursewareContentMissing,
    CoursewareContentNotReady,
    ResolvedCoursewareContent,
    resolve_courseware_content,
    verify_assessment_link_token,
)
from app.services.courseware_metrics import (
    record_error,
    record_generation,
    record_rate_limit_hit,
    snapshot,
)
from app.services.rate_limiter import InMemoryRateLimiter
from app.utils.logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/courseware-assessments", tags=["courseware-assessments"])

# One limiter instance per process; window = 60s. Limit applies independently
# per IP and per zoho_record_id so abuse against a single record can't exhaust
# the global IP budget for legitimate learners on other records.
_per_ip_limiter = InMemoryRateLimiter(
    max_requests=int(settings.ASSESSMENT_RATE_LIMIT_PER_MIN),
    window_seconds=60.0,
)
_per_record_limiter = InMemoryRateLimiter(
    max_requests=int(settings.ASSESSMENT_RATE_LIMIT_PER_MIN),
    window_seconds=60.0,
)


def _client_ip(request: Request) -> str:
    fwd = request.headers.get("x-forwarded-for", "")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _verify_api_key_or_token(
    *,
    zoho_record_id: str,
    phase: str,
    x_api_key: str | None,
    t: str | None,
) -> None:
    """
    Two ways for a caller to be authorized:

    1. ``X-API-Key`` matches ``API_SECRET_KEY`` (used by the trusted Vercel
       server-side proxy / internal tools).
    2. The signed ``t=`` token matches the one minted for this
       (zoho_record_id, phase) pair (used when the URL is opened directly via
       a public link and ``ASSESSMENT_LINK_REQUIRE_TOKEN`` is on).

    When ``ASSESSMENT_LINK_REQUIRE_TOKEN`` is true, BOTH conditions become
    available paths but at least one must succeed.
    """
    api_key_ok = bool(x_api_key) and x_api_key == settings.API_SECRET_KEY
    token_ok = verify_assessment_link_token(
        zoho_record_id=zoho_record_id, phase=phase, token=t
    )
    if settings.ASSESSMENT_LINK_REQUIRE_TOKEN:
        if not (api_key_ok or token_ok):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="A valid X-API-Key or signed link token (?t=) is required.",
            )
    else:
        if not api_key_ok:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="A valid X-API-Key is required.",
            )


async def _enforce_rate_limit(*, ip: str, zoho_record_id: str, phase: str) -> None:
    ip_key = f"ip:{ip}"
    rec_key = f"rec:{zoho_record_id}"
    ip_ok, _, ip_retry = await _per_ip_limiter.check(ip_key)
    rec_ok, _, rec_retry = await _per_record_limiter.check(rec_key)
    if not ip_ok or not rec_ok:
        record_rate_limit_hit()
        await record_error(
            phase=phase,
            zoho_record_id=zoho_record_id,
            kind="rate_limit",
            detail=f"ip={ip}",
        )
        retry_after = max(ip_retry, rec_retry)
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Rate limit exceeded. Try again in {int(retry_after) + 1}s.",
            headers={"Retry-After": str(int(retry_after) + 1)},
        )


def _normalize_num_questions(raw: int | None) -> int:
    nq = int(raw or settings.COURSEWARE_ASSESSMENT_DEFAULT_NUM_QUESTIONS or DEFAULT_NUM_QUESTIONS)
    return max(1, min(50, nq))


def _verify_api_key_only(*, x_api_key: str | None) -> None:
    if not x_api_key or x_api_key != settings.API_SECRET_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="A valid X-API-Key is required.",
        )


class ModuleCurriculumBlock(BaseModel):
    """One training module: heading + free-text body (Markdown/plain)."""

    module_name: str = Field(default="", description="Display name, e.g. Module 1: Leadership basics")
    content: str = Field(default="", description="Full subject content for this module")


class GenerateFromModulesRequest(BaseModel):
    """
    Build an assessment from explicit curriculum (no Zoho / slides job).

    Either pass ``curriculum_text`` as one blob or pass ``modules``; if both are
    set, ``curriculum_text`` wins. Shape matches how post-assessment flattens
    multi-module courseware (``##`` headings + body).
    """

    phase: str = Field(default="post", description="pre or post")
    course_name: str = Field(default="Course", min_length=1)
    modules: list[ModuleCurriculumBlock] = Field(default_factory=list)
    curriculum_text: str | None = Field(
        default=None,
        description="Optional single document; if non-empty, used instead of modules",
    )
    difficulty: str | None = None
    num_questions: int | None = Field(default=None, ge=1, le=50)
    pre_difficulty: str | None = Field(
        default=None,
        description="For post: approximate pre-test level for prompt context",
    )


def _flatten_modules_to_curriculum(modules: list[ModuleCurriculumBlock]) -> str:
    lines: list[str] = []
    for m in modules:
        name = (m.module_name or "").strip() or "Module"
        body = (m.content or "").strip()
        lines.append(f"## {name}")
        if body:
            lines.append(body)
        lines.append("")
    return "\n".join(lines).strip()


def _resolve_num_questions(
    *,
    phase_norm: str,
    query_num_questions: int | None,
    content: ResolvedCoursewareContent,
) -> int:
    """
    Per-request ``num_questions`` wins; else use the slides-job defaults from
    ``pre_assessment_num_questions`` / ``post_assessment_num_questions``; else env default.
    """
    if query_num_questions is not None:
        return _normalize_num_questions(query_num_questions)
    if phase_norm == "pre" and content.pre_assessment_num_questions is not None:
        return max(1, min(50, int(content.pre_assessment_num_questions)))
    if phase_norm == "post" and content.post_assessment_num_questions is not None:
        return max(1, min(50, int(content.post_assessment_num_questions)))
    return _normalize_num_questions(None)


async def _generate_for_phase(
    *,
    request: Request,
    phase: str,
    zoho_record_id: str,
    difficulty: str | None,
    num_questions: int | None,
    x_api_key: str | None,
    t: str | None,
    db: AsyncSession,
) -> dict[str, Any]:
    phase_norm = "post" if phase == "post" else "pre"
    rid = (zoho_record_id or "").strip()
    if not rid:
        raise HTTPException(status_code=400, detail="zoho_record_id is required.")

    _verify_api_key_or_token(
        zoho_record_id=rid, phase=phase_norm, x_api_key=x_api_key, t=t
    )

    ip = _client_ip(request)
    await _enforce_rate_limit(ip=ip, zoho_record_id=rid, phase=phase_norm)

    try:
        content = await resolve_courseware_content(db, rid, phase=phase_norm)
    except CoursewareContentNotReady as exc:
        await record_error(
            phase=phase_norm, zoho_record_id=rid, kind="not_ready", detail=str(exc)
        )
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except CoursewareContentMissing as exc:
        await record_error(
            phase=phase_norm, zoho_record_id=rid, kind="content_missing", detail=str(exc)
        )
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    nq = _resolve_num_questions(
        phase_norm=phase_norm,
        query_num_questions=num_questions,
        content=content,
    )
    default_diff = (
        settings.COURSEWARE_ASSESSMENT_DEFAULT_DIFFICULTY or "intermediate"
    )
    if phase_norm == "pre":
        diff = normalize_difficulty(difficulty or default_diff)
        curriculum = content.outline_text
        pre_diff_for_post: str | None = None
    else:
        # Post: per-request ?difficulty= wins; else use post_assessment_difficulty
        # from the slides job; else derive one level above pre_assessment_difficulty.
        if difficulty:
            diff = normalize_difficulty(difficulty)
        elif content.post_assessment_difficulty:
            diff = normalize_difficulty(content.post_assessment_difficulty)
        else:
            base = content.pre_difficulty or default_diff
            diff = post_difficulty_from_pre(base)
        curriculum = content.post_curriculum_text
        pre_diff_for_post = content.pre_difficulty

    nonce = secrets.token_urlsafe(8)
    started = time.monotonic()
    try:
        questions = await generate_assessment_questions_from_text(
            phase=phase_norm,
            difficulty=diff,
            course_name=content.course_name,
            curriculum_text=curriculum,
            num_questions=nq,
            pre_difficulty=pre_diff_for_post,
            nonce=nonce,
        )
    except Exception as exc:
        elapsed_ms = (time.monotonic() - started) * 1000.0
        await record_error(
            phase=phase_norm,
            zoho_record_id=rid,
            kind="generation_failed",
            detail=f"{type(exc).__name__}: {exc}",
        )
        logger.exception(
            "courseware_assessment generation failed | phase=%s zoho_record_id=%s elapsed_ms=%.1f",
            phase_norm,
            rid,
            elapsed_ms,
        )
        raise HTTPException(
            status_code=502,
            detail="Question generation failed upstream. Please try again.",
        ) from exc

    elapsed_ms = (time.monotonic() - started) * 1000.0
    await record_generation(
        phase=phase_norm,
        zoho_record_id=rid,
        elapsed_ms=elapsed_ms,
        content_hash=content.content_hash,
    )

    return {
        "zoho_record_id": rid,
        "phase": phase_norm,
        "course_name": content.course_name,
        "difficulty": diff,
        "num_questions": len(questions),
        "questions": questions,
        "content_hash": content.content_hash,
        "generation_ms": int(elapsed_ms),
        "nonce": nonce,
    }


@router.post("/from-modules")
async def post_generate_from_modules(
    body: GenerateFromModulesRequest,
    request: Request,
    x_api_key: str | None = Header(None, alias="X-API-Key"),
) -> dict[str, Any]:
    """
    Generate pre/post MCQs from explicit multi-module curriculum (or one ``curriculum_text``).

    Intended for internal / Vercel-proxy use: same ``X-API-Key`` as other courseware routes.
    Flattens ``modules`` the same way as post-assessment course text (``##`` title + body per module).
    """
    _verify_api_key_only(x_api_key=x_api_key)
    phase_raw = (body.phase or "post").strip().lower()
    phase_norm = "post" if phase_raw == "post" else "pre"
    rid_metric = "from-modules"
    ip = _client_ip(request)
    await _enforce_rate_limit(ip=ip, zoho_record_id=f"{rid_metric}:{ip}", phase=phase_norm)

    curriculum = (body.curriculum_text or "").strip()
    if not curriculum:
        curriculum = _flatten_modules_to_curriculum(body.modules)
    if not curriculum:
        raise HTTPException(
            status_code=422,
            detail="Provide non-empty curriculum_text or at least one module with content.",
        )

    default_diff = settings.COURSEWARE_ASSESSMENT_DEFAULT_DIFFICULTY or "intermediate"
    if phase_norm == "pre":
        diff = normalize_difficulty(body.difficulty or default_diff)
        pre_for_post: str | None = None
    else:
        if body.difficulty:
            diff = normalize_difficulty(body.difficulty)
        elif body.pre_difficulty:
            diff = post_difficulty_from_pre(body.pre_difficulty)
        else:
            diff = post_difficulty_from_pre(default_diff)
        pre_for_post = normalize_difficulty(body.pre_difficulty or default_diff)

    nq = _normalize_num_questions(body.num_questions)
    nonce = secrets.token_urlsafe(8)
    started = time.monotonic()
    try:
        questions = await generate_assessment_questions_from_text(
            phase=phase_norm,
            difficulty=diff,
            course_name=body.course_name,
            curriculum_text=curriculum,
            num_questions=nq,
            pre_difficulty=pre_for_post,
            nonce=nonce,
        )
    except Exception as exc:
        elapsed_ms = (time.monotonic() - started) * 1000.0
        await record_error(
            phase=phase_norm,
            zoho_record_id=rid_metric,
            kind="generation_failed",
            detail=f"{type(exc).__name__}: {exc}",
        )
        logger.exception(
            "courseware_assessment from-modules failed | phase=%s elapsed_ms=%.1f",
            phase_norm,
            elapsed_ms,
        )
        raise HTTPException(
            status_code=502,
            detail="Question generation failed upstream. Please try again.",
        ) from exc

    if not questions:
        raise HTTPException(
            status_code=502,
            detail="The model returned no usable questions. Please try again.",
        )

    elapsed_ms = (time.monotonic() - started) * 1000.0
    content_hash = hashlib.sha256(curriculum.encode("utf-8")).hexdigest()
    await record_generation(
        phase=phase_norm,
        zoho_record_id=rid_metric,
        elapsed_ms=elapsed_ms,
        content_hash=content_hash,
    )

    return {
        "zoho_record_id": None,
        "phase": phase_norm,
        "course_name": body.course_name,
        "difficulty": diff,
        "num_questions": len(questions),
        "questions": questions,
        "content_hash": content_hash,
        "generation_ms": int(elapsed_ms),
        "nonce": nonce,
        "source": "from-modules",
        "seconds_per_question": 45,
    }


@router.get("/{zoho_record_id}/pre")
async def get_pre_assessment(
    zoho_record_id: str,
    request: Request,
    difficulty: str | None = Query(None, description="basic | intermediate | advanced"),
    num_questions: int | None = Query(None, ge=1, le=50),
    t: str | None = Query(None, description="Optional signed link token."),
    x_api_key: str | None = Header(None, alias="X-API-Key"),
    db: AsyncSession = Depends(get_db),
):
    return await _generate_for_phase(
        request=request,
        phase="pre",
        zoho_record_id=zoho_record_id,
        difficulty=difficulty,
        num_questions=num_questions,
        x_api_key=x_api_key,
        t=t,
        db=db,
    )


@router.post("/{zoho_record_id}/pre")
async def post_pre_assessment(
    zoho_record_id: str,
    request: Request,
    difficulty: str | None = Query(None),
    num_questions: int | None = Query(None, ge=1, le=50),
    t: str | None = Query(None),
    x_api_key: str | None = Header(None, alias="X-API-Key"),
    db: AsyncSession = Depends(get_db),
):
    return await _generate_for_phase(
        request=request,
        phase="pre",
        zoho_record_id=zoho_record_id,
        difficulty=difficulty,
        num_questions=num_questions,
        x_api_key=x_api_key,
        t=t,
        db=db,
    )


@router.get("/{zoho_record_id}/post")
async def get_post_assessment(
    zoho_record_id: str,
    request: Request,
    difficulty: str | None = Query(None),
    num_questions: int | None = Query(None, ge=1, le=50),
    t: str | None = Query(None),
    x_api_key: str | None = Header(None, alias="X-API-Key"),
    db: AsyncSession = Depends(get_db),
):
    return await _generate_for_phase(
        request=request,
        phase="post",
        zoho_record_id=zoho_record_id,
        difficulty=difficulty,
        num_questions=num_questions,
        x_api_key=x_api_key,
        t=t,
        db=db,
    )


@router.post("/{zoho_record_id}/post")
async def post_post_assessment(
    zoho_record_id: str,
    request: Request,
    difficulty: str | None = Query(None),
    num_questions: int | None = Query(None, ge=1, le=50),
    t: str | None = Query(None),
    x_api_key: str | None = Header(None, alias="X-API-Key"),
    db: AsyncSession = Depends(get_db),
):
    return await _generate_for_phase(
        request=request,
        phase="post",
        zoho_record_id=zoho_record_id,
        difficulty=difficulty,
        num_questions=num_questions,
        x_api_key=x_api_key,
        t=t,
        db=db,
    )


@router.get("/_metrics")
async def metrics_snapshot(
    x_api_key: str | None = Header(None, alias="X-API-Key"),
):
    if not x_api_key or x_api_key != settings.API_SECRET_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key.")
    return snapshot()

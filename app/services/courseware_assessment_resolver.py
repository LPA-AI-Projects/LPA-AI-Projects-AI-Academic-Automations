"""
Resolver for the on-demand courseware assessments flow.

Responsibilities:

- Locate the LATEST `slides` ``CourseJob`` that is in a terminal "content ready"
  state for a given ``zoho_record_id`` (so re-runs of the slides job rebind to
  the newest content).
- Surface the canonical curriculum inputs needed to drive question generation:
    * ``outline_text`` -> source for **pre** assessments (extracted from the
      original outline PDF the slides job consumed).
    * ``post_curriculum_text`` -> source for **post** assessments (flattened
      from ``validated_slides.json``, either from the on-disk cache OR from
      the inline blob persisted onto ``CourseJob.payload_json`` for
      multi-replica deploys).
- Bind every resolution to the slides job's ``content_hash`` so callers can
  log/observe which content version was used.

This module is intentionally cache-FREE for generated questions. It only reads
already-produced courseware artifacts and never persists MCQs.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.job import CourseJob
from app.services.document_extractor import extract_pdf_text_async
from app.services.slides_service import _build_post_curriculum_from_modules
from app.utils.logger import get_logger

logger = get_logger(__name__)


class CoursewareContentNotReady(RuntimeError):
    """Raised when no completed slides job exists for the record yet."""


class CoursewareContentMissing(RuntimeError):
    """Raised when the slides job exists but its content artifacts are unreachable."""


class InvalidAssessmentToken(RuntimeError):
    """Raised when a required ``t=`` query token is missing or fails verification."""


@dataclass(frozen=True)
class ResolvedCoursewareContent:
    job_id: str
    zoho_record_id: str
    content_hash: str
    course_name: str
    # Stored pre/post defaults from the slides job payload (on-demand links).
    pre_difficulty: str | None
    post_assessment_difficulty: str | None
    pre_assessment_num_questions: int | None
    post_assessment_num_questions: int | None
    cache_dir: str | None
    validated_slides_path: str | None
    outline_pdf_path: str | None
    outline_text: str
    post_curriculum_text: str


def _int_nq_from_payload(payload: dict[str, Any], key: str) -> int | None:
    """Parse 1–50 question count from job payload, or None."""
    v = payload.get(key)
    if v is None:
        return None
    try:
        n = int(v)
    except (TypeError, ValueError):
        return None
    return max(1, min(50, n))


def _payload_dict(job: CourseJob) -> dict[str, Any]:
    try:
        p = json.loads(job.payload_json or "{}")
        return p if isinstance(p, dict) else {}
    except Exception:
        return {}


# Statuses where slides content is guaranteed to exist on disk / payload.
# `attaching` and `merging` come AFTER validation in `process_slides_job`, so
# `validated_slides.json` is already written by then. We allow them so that the
# learner URL works as soon as content is ready, even if the Gamma/Zoho tail
# is still in progress.
_CONTENT_READY_STATUSES = ("completed", "attaching", "merging")


async def _latest_slides_job(db: AsyncSession, zoho_record_id: str) -> CourseJob | None:
    """Most-recent slides job for the record whose content is ready (`completed` / late stages)."""
    rid = (zoho_record_id or "").strip()
    if not rid:
        return None
    stmt = (
        select(CourseJob)
        .where(CourseJob.zoho_record_id == rid)
        .where(CourseJob.job_type == "slides")
        .where(CourseJob.status.in_(_CONTENT_READY_STATUSES))
        .order_by(CourseJob.created_at.desc())
    )
    result = await db.execute(stmt)
    return result.scalars().first()


def _load_validated_modules(payload: dict[str, Any]) -> list[dict[str, Any]] | None:
    """Read validated module entries either from inline blob or on-disk cache."""
    blob = payload.get("validated_slides_blob")
    if isinstance(blob, dict):
        modules = blob.get("modules")
        if isinstance(modules, list):
            return [m for m in modules if isinstance(m, dict)]

    path = str(payload.get("validated_slides_path") or "").strip()
    if path and os.path.isfile(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and isinstance(data.get("modules"), list):
                return [m for m in data["modules"] if isinstance(m, dict)]
            if isinstance(data, list):
                return [m for m in data if isinstance(m, dict)]
        except Exception:
            logger.exception("courseware_resolver: failed to read validated_slides.json | path=%s", path)
    return None


async def _load_outline_text(payload: dict[str, Any]) -> str:
    """Prefer the cheap stored excerpt, fall back to re-extracting the source PDF."""
    cached = str(payload.get("outline_text_excerpt") or "").strip()
    if cached:
        return cached
    pdf_path = str(payload.get("outline_pdf_path") or "").strip()
    if pdf_path and os.path.isfile(pdf_path):
        try:
            with open(pdf_path, "rb") as f:
                raw = f.read()
            return (await extract_pdf_text_async(raw)).strip()
        except Exception:
            logger.exception("courseware_resolver: re-extract outline failed | path=%s", pdf_path)
    return ""


async def resolve_courseware_content(
    db: AsyncSession,
    zoho_record_id: str,
    *,
    phase: str,
) -> ResolvedCoursewareContent:
    """
    Locate the latest slides job for ``zoho_record_id`` and assemble the content
    needed to drive ``phase`` (``"pre"`` or ``"post"``) generation.
    """
    phase_norm = "post" if str(phase or "").lower() == "post" else "pre"
    job = await _latest_slides_job(db, zoho_record_id)
    if job is None:
        raise CoursewareContentNotReady(
            f"No completed slides job found for zoho_record_id={zoho_record_id!r}."
        )

    payload = _payload_dict(job)
    content_hash = str(payload.get("content_hash") or "").strip()
    cache_dir = str(payload.get("cache_dir") or "").strip() or None
    validated_path = str(payload.get("validated_slides_path") or "").strip() or None
    outline_pdf_path = str(payload.get("outline_pdf_path") or "").strip() or None
    course_name = str(payload.get("course_name") or "course").strip() or "course"
    pre_difficulty = (
        str(payload.get("pre_assessment_difficulty") or "").strip().lower() or None
    )
    post_assessment_difficulty = (
        str(payload.get("post_assessment_difficulty") or "").strip().lower() or None
    )
    pre_assessment_num_questions = _int_nq_from_payload(payload, "pre_assessment_num_questions")
    post_assessment_num_questions = _int_nq_from_payload(payload, "post_assessment_num_questions")

    if phase_norm == "post":
        modules = _load_validated_modules(payload)
        if not modules:
            raise CoursewareContentMissing(
                "validated_slides.json is unavailable for this slides job — "
                "the API container may be missing access to the cache directory."
            )
        post_text = _build_post_curriculum_from_modules(modules)
        outline_text = ""
    else:
        post_text = ""
        outline_text = await _load_outline_text(payload)
        if not outline_text:
            raise CoursewareContentMissing(
                "Outline text could not be loaded for pre-assessment generation."
            )

    logger.info(
        "courseware_resolver: resolved | phase=%s zoho_record_id=%s job_id=%s content_hash=%s "
        "validated_path=%s outline_chars=%s post_chars=%s",
        phase_norm,
        zoho_record_id,
        str(job.id),
        content_hash[:16] or "-",
        validated_path or "-",
        len(outline_text),
        len(post_text),
    )

    return ResolvedCoursewareContent(
        job_id=str(job.id),
        zoho_record_id=zoho_record_id,
        content_hash=content_hash,
        course_name=course_name,
        pre_difficulty=pre_difficulty,
        post_assessment_difficulty=post_assessment_difficulty,
        pre_assessment_num_questions=pre_assessment_num_questions,
        post_assessment_num_questions=post_assessment_num_questions,
        cache_dir=cache_dir,
        validated_slides_path=validated_path,
        outline_pdf_path=outline_pdf_path,
        outline_text=outline_text,
        post_curriculum_text=post_text,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Signed t= token (HMAC-SHA256 truncated, URL-safe base64).
# Bound to (zoho_record_id, phase) so a leaked pre token can't unlock post.
# Optional: when ASSESSMENT_LINK_REQUIRE_TOKEN is true, every public request
# must carry a valid token regardless of API key.
# ──────────────────────────────────────────────────────────────────────────────


def _signing_key() -> bytes:
    secret = (settings.ASSESSMENT_LINK_SECRET or "").strip()
    if not secret:
        # Fall back to API_SECRET_KEY so links can still be minted in dev when
        # ASSESSMENT_LINK_SECRET isn't explicitly set.
        secret = (settings.API_SECRET_KEY or "").strip()
    return secret.encode("utf-8")


def mint_assessment_link_token(zoho_record_id: str, phase: str) -> str | None:
    """
    Return a short URL-safe token binding the link to (record_id, phase).

    ``None`` is returned when no signing material is available.
    """
    key = _signing_key()
    if not key:
        return None
    msg = f"{(zoho_record_id or '').strip()}|{('post' if phase == 'post' else 'pre')}".encode("utf-8")
    digest = hmac.new(key, msg, hashlib.sha256).digest()
    # 18 bytes -> 24 url-safe base64 chars (no padding).
    return base64.urlsafe_b64encode(digest[:18]).decode("ascii").rstrip("=")


def verify_assessment_link_token(
    *, zoho_record_id: str, phase: str, token: str | None
) -> bool:
    expected = mint_assessment_link_token(zoho_record_id, phase)
    if not expected:
        return False
    if not token:
        return False
    candidate = token.strip()
    # ``hmac.compare_digest`` raises ``TypeError`` if either string contains
    # non-ASCII characters. Tokens we mint are url-safe base64 (ASCII only), so
    # any non-ASCII input is by definition not a valid token — treat it as a
    # failed comparison rather than letting the exception bubble into a 500.
    try:
        return hmac.compare_digest(expected, candidate)
    except (TypeError, ValueError):
        return False

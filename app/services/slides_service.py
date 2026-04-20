from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import time
from typing import Any

from sqlalchemy import select

from app.core.config import settings
from app.core.storage_paths import ppts_dir
from app.core.database import AsyncSessionLocal
from app.models.job import CourseJob
from app.services.document_extractor import (
    extract_lesson_document_text_async,
    extract_pdf_module_rows_async,
    extract_pdf_text_async,
    extract_ppt_text_async,
    slice_instructor_ppt_for_module,
)
from app.services.assessment_service import (
    DEFAULT_NUM_QUESTIONS,
    generate_assessment_questions_from_text,
)
from app.services.gamma_client import generate_ppt
from app.services.slides_graph import run_module_slides_pipeline
from app.services.zoho_crm import update_assessment_links_field, update_slides_links_field
from app.utils.logger import get_logger

logger = get_logger(__name__)


async def _extract_instructor_file_text_async(blob: bytes, basename: str) -> str:
    """PPTX/PPT via python-pptx; fallback to PDF/DOCX-style extraction."""
    lower = basename.lower()
    likely_pptx = lower.endswith((".pptx", ".ppt")) or (
        len(blob) >= 2 and blob[:2] == b"PK" and not lower.endswith(".pdf")
    )
    if likely_pptx:
        try:
            return await extract_ppt_text_async(blob)
        except Exception:
            logger.warning(
                "instructor deck not valid as PPTX; trying generic extract | basename=%s",
                basename,
            )
    return await extract_lesson_document_text_async(blob, basename)


MAX_SLIDES_PER_BATCH = 60
CACHE_VERSION = "slides_cache_v3"


def _safe_course_name(name: str | None) -> str:
    cleaned = (name or "").strip()
    forbidden = '\\/:*?"<>|'
    for ch in forbidden:
        cleaned = cleaned.replace(ch, "_")
    return cleaned[:80] or "course"


def _safe_id(name: str | None) -> str:
    cleaned = (name or "").strip()
    forbidden = '\\/:*?"<>|'
    for ch in forbidden:
        cleaned = cleaned.replace(ch, "_")
    return cleaned[:120] or "unknown"


def _normalize_for_hash(text: str | None) -> str:
    raw = (text or "").lower()
    raw = re.sub(r"\s+", " ", raw)
    return raw.strip()


def _hash_text(text: str | None) -> str:
    normalized = _normalize_for_hash(text)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _normalize_instructor_ppt_priority(raw: str | None) -> str:
    """Per-job override or ``settings.SLIDES_INSTRUCTOR_PPT_PRIORITY`` (default: supplement)."""
    s = (raw or "").strip().lower()
    if s == "primary":
        return "primary"
    if s == "supplement":
        return "supplement"
    env = (getattr(settings, "SLIDES_INSTRUCTOR_PPT_PRIORITY", None) or "").strip().lower()
    if env == "primary":
        return "primary"
    return "supplement"


def _gamma_input_from_batch(slides_batch: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    for i, s in enumerate(slides_batch, start=1):
        title = str(s.get("title") or "").strip()
        bullets = s.get("bullets") if isinstance(s.get("bullets"), list) else []
        notes = str(s.get("notes") or "").strip()
        visual = str(s.get("visual") or "").strip()
        lines.append(f"Slide {i}: {title}")
        for b in bullets[:8]:
            lines.append(f"- {str(b).strip()}")
        if notes:
            lines.append(f"Speaker notes: {notes}")
        if visual:
            lines.append(f"Visual suggestion: {visual}")
        lines.append("")
    return "\n".join(lines).strip()


def _build_module_cover_slide(module_name: str) -> dict[str, Any]:
    heading = str(module_name or "Module").strip() or "Module"
    return {
        "title": heading,
        "bullets": [f"Overview and key outcomes for {heading}"],
        "notes": f"Module cover slide for {heading}.",
        "visual": "Clean section divider slide with module title",
    }


def _safe_filename(name: str | None) -> str:
    cleaned = (name or "").strip()
    forbidden = '\\/:*?"<>|'
    for ch in forbidden:
        cleaned = cleaned.replace(ch, "_")
    return cleaned[:120] or "module"


def _extract_outline_modules(outline_text: str, *, program_name: str | None = None) -> list[dict[str, str]]:
    """
    Split outline text into modules using heading-like markers.
    Falls back to a single module when no module headings are found.
    """
    text = (outline_text or "").strip()
    if not text:
        return [{"module_name": (program_name or "Module 1").strip() or "Module 1", "module_text": ""}]

    def _dedupe_matches(raw: list[re.Match[str]], *, min_distance: int = 24) -> list[re.Match[str]]:
        out: list[re.Match[str]] = []
        seen_numbers: set[int] = set()
        last_pos = -10_000
        for m in raw:
            num = int(m.group(2))
            pos = m.start()
            if num in seen_numbers:
                continue
            if pos - last_pos < min_distance:
                continue
            seen_numbers.add(num)
            out.append(m)
            last_pos = pos
        return out

    def _build_modules(matches: list[re.Match[str]]) -> list[dict[str, str]]:
        modules: list[dict[str, str]] = []
        for idx, m in enumerate(matches):
            start = m.start()
            end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
            module_num = int(m.group(2))
            module_title = str((m.group(3) or "")).strip(" :-\u2013\u2014\t")
            heading = f"Module {module_num}" + (f": {module_title}" if module_title else "")
            body = text[start:end].strip()
            modules.append({"module_name": heading, "module_text": body})
        return modules

    # 1) Strict line-based heading detection.
    strict_pattern = re.compile(
        r"(?im)^\s*(?:[#*\-\u2022]\s*)?(module)\s+(\d{1,3})\s*(?:[:\-\u2013\u2014\)]\s*([^\n]{0,180}))?\s*$"
    )
    matches = _dedupe_matches(list(strict_pattern.finditer(text)))

    # 2) Relaxed heading detection for PDFs where heading line has trailing noise.
    if len(matches) < 2:
        relaxed_pattern = re.compile(
            r"(?im)^\s*(?:[#*\-\u2022]\s*)?(module)\s+(\d{1,3})\b(?:\s*[:\-\u2013\u2014\)]\s*([^\n]{0,180}))?"
        )
        matches = _dedupe_matches(list(relaxed_pattern.finditer(text)))

    # 3) Inline fallback for poor extraction where headings collapse into large paragraphs.
    if len(matches) < 2:
        inline_pattern = re.compile(
            r"(?i)(?:^|[\n\r]|[.!?]\s+)(module)\s+(\d{1,3})\s*(?:[:\-\u2013\u2014\)]\s*([^.\n\r]{0,120}))?"
        )
        matches = _dedupe_matches(list(inline_pattern.finditer(text)), min_distance=20)

    # 4) Table-style fallback for outlines rendered like:
    # Sno. | Modules | Topics | Exercises
    # 01
    # Understanding Leadership ...
    if len(matches) < 2:
        table_row_pattern = re.compile(r"(?im)^\s*0?(\d{1,2})\s*$")
        table_rows = list(table_row_pattern.finditer(text))
        if len(table_rows) >= 2:
            table_modules: list[dict[str, str]] = []
            for idx, row in enumerate(table_rows):
                start = row.start()
                end = table_rows[idx + 1].start() if idx + 1 < len(table_rows) else len(text)
                row_num = int(row.group(1))
                body = text[start:end].strip()

                # Derive module title from first non-empty lines after row number until bullets start.
                after_number = text[row.end() : end]
                title_lines: list[str] = []
                for ln in after_number.splitlines():
                    s = ln.strip()
                    if not s:
                        continue
                    if re.match(r"^(?:[-*•]\s+)", s):
                        break
                    if re.match(r"^(?:topics?|exercises?|sno\.?)\b", s, flags=re.IGNORECASE):
                        continue
                    title_lines.append(s)
                    if len(" ".join(title_lines).split()) >= 12:
                        break
                    if len(title_lines) >= 3:
                        break
                title = " ".join(title_lines).strip(" :-\u2013\u2014\t")
                heading = f"Module {row_num}" + (f": {title}" if title else "")
                table_modules.append({"module_name": heading, "module_text": body})

            # Keep rows with minimally meaningful body length; avoid dropping real short module rows.
            table_modules = [m for m in table_modules if len((m.get("module_text") or "").strip()) >= 20]
            if len(table_modules) >= 2:
                return table_modules

    if len(matches) < 2:
        return [{"module_name": (program_name or "Module 1").strip() or "Module 1", "module_text": text}]

    modules = _build_modules(matches)
    return modules or [{"module_name": (program_name or "Module 1").strip() or "Module 1", "module_text": text}]


def _build_ppt_url(file_path: str) -> str:
    filename = os.path.basename(file_path)
    return f"{settings.BASE_URL}/ppts/{filename}"


def _build_ppt_url_from_relative_path(rel_path: str) -> str:
    normalized = str(rel_path).replace("\\", "/").lstrip("/")
    return f"{settings.BASE_URL}/ppts/{normalized}"


async def _set_status(db, job: CourseJob, status: str, *, error: str | None = None) -> None:
    job.status = status
    if error is not None:
        job.error = error[:4000]
    await db.commit()
    logger.info(
        "Slides job status updated | job_id=%s status=%s",
        str(job.id),
        status,
    )


def _batch_slides(slides: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    return [slides[i : i + MAX_SLIDES_PER_BATCH] for i in range(0, len(slides), MAX_SLIDES_PER_BATCH)]


def _build_assessment_urls(zoho_record_id: str) -> dict[str, str | None]:
    """
    Build absolute Vercel-frontend URLs for pre/post assessment links.

    Returns ``{"pre_assessment_url": str|None, "post_assessment_url": str|None,
    "pre_token": str|None, "post_token": str|None}``.

    Returns ``None`` URLs when ``FRONTEND_BASE_URL`` is not configured.

    Tokens are appended (``?t=...``) only when ``ASSESSMENT_LINK_SECRET`` (or
    fallback ``API_SECRET_KEY``) is available; this binds each URL to its
    (record_id, phase) pair so leaks can't unlock the other phase.
    """
    # Local imports keep the resolver helper out of the import cycle for
    # background-task workers that don't need it.
    from app.services.courseware_assessment_resolver import mint_assessment_link_token

    base = (settings.FRONTEND_BASE_URL or "").rstrip("/")
    rid = (zoho_record_id or "").strip()
    if not base or not rid:
        return {
            "pre_assessment_url": None,
            "post_assessment_url": None,
            "pre_token": None,
            "post_token": None,
        }
    pre_t = mint_assessment_link_token(rid, "pre")
    post_t = mint_assessment_link_token(rid, "post")
    pre_url = f"{base}/assessment/{rid}/pre"
    post_url = f"{base}/assessment/{rid}/post"
    if pre_t:
        pre_url = f"{pre_url}?t={pre_t}"
    if post_t:
        post_url = f"{post_url}?t={post_t}"
    return {
        "pre_assessment_url": pre_url,
        "post_assessment_url": post_url,
        "pre_token": pre_t,
        "post_token": post_t,
    }


def _build_post_curriculum_from_modules(module_entries: list[dict[str, Any]]) -> str:
    """Flatten validated module slides into compact text for post-assessment generation."""
    lines: list[str] = []
    for module in module_entries:
        module_name = str(module.get("module_name") or "Module").strip()
        lines.append(f"## {module_name}")
        slides = module.get("slides") if isinstance(module.get("slides"), list) else []
        for s in slides:
            title = str(s.get("title") or "").strip()
            if title:
                lines.append(f"Slide: {title}")
            bullets = s.get("bullets") if isinstance(s.get("bullets"), list) else []
            for b in bullets:
                bt = str(b).strip()
                if bt:
                    lines.append(f"- {bt}")
        lines.append("")
    return "\n".join(lines).strip()


async def process_slides_job(job_id) -> None:
    """
    Pipeline:
    extracting -> planning -> generating_slides -> validating -> batching
    -> gamma_rendering -> merging -> attaching -> completed
    """
    started = time.time()
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(CourseJob).where(CourseJob.id == job_id))
        job = result.scalars().first()
        if job is None:
            logger.error("Slides job missing in DB | job_id=%s", str(job_id))
            return

        logger.info(
            "Slides job started | job_id=%s zoho_record_id=%s",
            str(job.id),
            job.zoho_record_id,
        )

        try:
            await _set_status(db, job, "extracting")
            payload = {}
            try:
                payload = json.loads(job.payload_json or "{}")
            except Exception:
                payload = {}

            outline_path = payload.get("outline_pdf_path")
            raw_lp_paths = payload.get("lesson_plan_and_activity_plan_pdf_paths")
            lesson_paths: list[str] = []
            if isinstance(raw_lp_paths, list):
                lesson_paths = [str(p).strip() for p in raw_lp_paths if str(p).strip()]
            if not lesson_paths:
                lp_one = payload.get("lesson_plan_and_activity_plan_pdf_path")
                if lp_one and str(lp_one).strip():
                    lesson_paths = [str(lp_one).strip()]
            raw_inst_paths = payload.get("instructor_ppt_paths")
            instructor_paths: list[str] = []
            if isinstance(raw_inst_paths, list):
                instructor_paths = [str(p).strip() for p in raw_inst_paths if str(p).strip()]
            if not instructor_paths:
                ip_one = payload.get("instructor_ppt_path")
                if ip_one and str(ip_one).strip():
                    instructor_paths = [str(ip_one).strip()]
            instructor_ppt_priority = _normalize_instructor_ppt_priority(payload.get("instructor_ppt_priority"))
            course_name = _safe_course_name(payload.get("course_name"))
            program_name = str(payload.get("program_name") or "").strip() or None
            # Optional default difficulties from the slides creation form — surfaced
            # to on-demand /pre and /post when the learner URL omits ?difficulty=.
            pre_assessment_difficulty = (
                str(payload.get("pre_assessment_difficulty") or "").strip().lower() or None
            )
            post_assessment_difficulty = (
                str(payload.get("post_assessment_difficulty") or "").strip().lower() or None
            )

            def _payload_nq(key: str) -> int | None:
                v = payload.get(key)
                if v is None:
                    return None
                try:
                    n = int(v)
                except (TypeError, ValueError):
                    return None
                return max(1, min(50, n))

            pre_assessment_num_questions = _payload_nq("pre_assessment_num_questions")
            post_assessment_num_questions = _payload_nq("post_assessment_num_questions")
            assessments_enabled = bool(getattr(settings, "SLIDES_ASSESSMENTS_ENABLED", False))
            pre_q_count = max(
                1,
                int(getattr(settings, "SLIDES_PRE_ASSESSMENT_QUESTIONS", DEFAULT_NUM_QUESTIONS) or DEFAULT_NUM_QUESTIONS),
            )
            post_q_count = max(
                1,
                int(getattr(settings, "SLIDES_POST_ASSESSMENT_QUESTIONS", DEFAULT_NUM_QUESTIONS) or DEFAULT_NUM_QUESTIONS),
            )
            pre_assessment_task: asyncio.Task | None = None
            post_assessment_task: asyncio.Task | None = None
            cache_root = os.path.join(ppts_dir(), "cache", _safe_id(job.zoho_record_id))
            cache_dir = cache_root
            os.makedirs(cache_dir, exist_ok=True)
            validated_cache_path = os.path.join(cache_dir, "validated_slides.json")
            validated_text_path = os.path.join(cache_dir, "validated_slides.txt")

            if not outline_path or not os.path.exists(outline_path):
                raise RuntimeError("outline_pdf is missing on disk for this job.")

            with open(outline_path, "rb") as f:
                outline_bytes = f.read()
            table_modules = await extract_pdf_module_rows_async(outline_bytes)
            outline_text = await extract_pdf_text_async(outline_bytes)
            if assessments_enabled and outline_text.strip():
                pre_assessment_task = asyncio.create_task(
                    generate_assessment_questions_from_text(
                        phase="pre",
                        difficulty="intermediate",
                        course_name=course_name,
                        curriculum_text=outline_text,
                        num_questions=pre_q_count,
                    )
                )
                logger.info(
                    "Slides pre-assessment started in parallel | job_id=%s questions=%s",
                    str(job.id),
                    pre_q_count,
                )
            logger.info(
                "Slides extracted outline | job_id=%s chars=%s",
                str(job.id),
                len(outline_text or ""),
            )

            lesson_text = None
            if lesson_paths:
                parts: list[str] = []
                for idx, lp in enumerate(lesson_paths):
                    if not lp or not os.path.exists(lp):
                        continue
                    with open(lp, "rb") as f:
                        blob = f.read()
                    chunk = await extract_lesson_document_text_async(blob, os.path.basename(lp))
                    if chunk and chunk.strip():
                        parts.append(
                            f"--- LP/AP document {idx + 1}: {os.path.basename(lp)} ---\n{chunk.strip()}"
                        )
                if parts:
                    lesson_text = "\n\n".join(parts)
                logger.info(
                    "Slides extracted lesson/activity | job_id=%s files=%s chars=%s",
                    str(job.id),
                    len(parts),
                    len(lesson_text or ""),
                )

            instructor_text = None
            if instructor_paths:
                inst_parts: list[str] = []
                for idx, ip in enumerate(instructor_paths):
                    if not ip or not os.path.exists(ip):
                        continue
                    with open(ip, "rb") as f:
                        blob = f.read()
                    chunk = await _extract_instructor_file_text_async(blob, os.path.basename(ip))
                    if chunk and chunk.strip():
                        inst_parts.append(
                            f"--- Instructor deck {idx + 1}: {os.path.basename(ip)} ---\n{chunk.strip()}"
                        )
                if inst_parts:
                    instructor_text = "\n\n".join(inst_parts)
                logger.info(
                    "Slides extracted instructor ppt text | job_id=%s files=%s chars=%s",
                    str(job.id),
                    len(inst_parts),
                    len(instructor_text or ""),
                )

            if len(table_modules) >= 2:
                modules = table_modules
                logger.info(
                    "Outline modules detected via table extraction | job_id=%s modules=%s",
                    str(job.id),
                    len(modules),
                )
            else:
                modules = _extract_outline_modules(outline_text, program_name=program_name)
                logger.info("Outline modules detected | job_id=%s modules=%s", str(job.id), len(modules))
            outline_hash = _hash_text(outline_text)
            lesson_hash = _hash_text(lesson_text)
            instructor_hash = _hash_text(instructor_text)
            content_hash = hashlib.sha256(
                f"{outline_hash}|{lesson_hash}|{instructor_hash}|{instructor_ppt_priority}".encode("utf-8")
            ).hexdigest()
            cache_dir = os.path.join(cache_root, f"{CACHE_VERSION}_{content_hash[:16]}")
            os.makedirs(cache_dir, exist_ok=True)
            validated_cache_path = os.path.join(cache_dir, "validated_slides.json")
            validated_text_path = os.path.join(cache_dir, "validated_slides.txt")
            pre_assessment_cache_path = os.path.join(cache_dir, "pre_assessment.json")
            post_assessment_cache_path = os.path.join(cache_dir, "post_assessment.json")

            module_entries: list[dict[str, Any]] = []
            planner_model = (settings.SLIDES_PLANNER_MODEL or "").strip() or None
            generator_model = (settings.SLIDES_GENERATOR_MODEL or "").strip() or None
            validator_model = (settings.SLIDES_VALIDATOR_MODEL or "").strip() or None
            min_per_module = max(1, int(getattr(settings, "SLIDES_MIN_PER_MODULE", 10) or 10))
            max_per_module = max(min_per_module, int(getattr(settings, "SLIDES_MAX_PER_MODULE", 20) or 20))
            max_loops = max(1, int(getattr(settings, "SLIDES_VALIDATION_MAX_LOOPS", 2) or 2))
            module_parallelism = max(1, int(getattr(settings, "SLIDES_MODULE_PARALLELISM", 3) or 3))
            if os.path.exists(validated_cache_path):
                with open(validated_cache_path, "r", encoding="utf-8") as f:
                    cached = json.load(f)
                if isinstance(cached, dict) and isinstance(cached.get("modules"), list):
                    cached_hash = str(cached.get("content_hash") or "").strip()
                    cached_version = str(cached.get("cache_version") or "").strip()
                    cached_modules_detected = int(cached.get("modules_detected") or 0)
                    cache_valid = cached_hash == content_hash and cached_version == CACHE_VERSION
                    if len(modules) >= 2 and cached_modules_detected < 2:
                        cache_valid = False
                    if cache_valid:
                        module_entries = [m for m in cached["modules"] if isinstance(m, dict)]
                    else:
                        logger.info(
                            "Ignoring stale/low-quality cache | job_id=%s cache_version=%s cached_modules=%s detected_modules=%s",
                            str(job.id),
                            cached_version or "-",
                            cached_modules_detected,
                            len(modules),
                        )
                elif isinstance(cached, list):
                    # Backward-compatible cache format.
                    module_entries = [{"module_name": modules[0]["module_name"], "slides": cached}]
                if not module_entries:
                    raise RuntimeError("Cached validated slides file is invalid.")
                logger.info(
                    "Using cached module slides | job_id=%s cache_path=%s modules=%s",
                    str(job.id),
                    validated_cache_path,
                    len(module_entries),
                )
            else:
                await _set_status(db, job, "planning")
                await _set_status(db, job, "generating_slides")
                await _set_status(db, job, "validating")

                logger.info(
                    "Module pipeline parallel start | job_id=%s modules=%s parallelism=%s",
                    str(job.id),
                    len(modules),
                    module_parallelism,
                )
                semaphore = asyncio.Semaphore(module_parallelism)

                async def _run_single_module(mi: int, mod: dict[str, str]) -> tuple[int, str, list[dict[str, Any]]]:
                    module_name = str(mod.get("module_name") or f"Module {mi}").strip() or f"Module {mi}"
                    module_text = str(mod.get("module_text") or "").strip()
                    mod_instructor = (
                        slice_instructor_ppt_for_module(
                            instructor_text, module_name, module_text, max_chars=150_000
                        )
                        if instructor_text
                        else None
                    )
                    t0 = time.time()
                    async with semaphore:
                        validated_module = await run_module_slides_pipeline(
                            module_name=module_name,
                            module_text=module_text,
                            lesson_text=lesson_text,
                            instructor_text=mod_instructor,
                            planner_model=planner_model,
                            generator_model=generator_model,
                            validator_model=validator_model,
                            min_slides=min_per_module,
                            max_slides=max_per_module,
                            max_loops=max_loops,
                            instructor_ppt_priority=instructor_ppt_priority,
                        )
                    logger.info(
                        "Module pipeline completed | job_id=%s module=%s slides=%s seconds=%.2f",
                        str(job.id),
                        module_name,
                        len(validated_module),
                        time.time() - t0,
                    )
                    return mi, module_name, validated_module

                module_tasks = [
                    asyncio.create_task(_run_single_module(mi, mod))
                    for mi, mod in enumerate(modules, start=1)
                ]
                module_results = await asyncio.gather(*module_tasks)

                for _, module_name, validated_module in module_results:
                    module_entries.append({"module_name": module_name, "slides": validated_module})
                    job.payload_json = json.dumps(
                        {
                            "progress": {
                                "module": module_name,
                                "generated": len(validated_module),
                                "total": len(validated_module),
                            }
                        }
                    )
                    await db.commit()

                if not module_entries:
                    raise RuntimeError("No modules produced slides.")

                with open(validated_cache_path, "w", encoding="utf-8") as f:
                    json.dump(
                        {
                            "cache_version": CACHE_VERSION,
                            "content_hash": content_hash,
                            "modules_detected": len(modules),
                            "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                            "modules": module_entries,
                        },
                        f,
                        ensure_ascii=False,
                        indent=2,
                    )
                with open(validated_text_path, "w", encoding="utf-8") as f:
                    for module in module_entries:
                        m_name = str(module.get("module_name") or "Module").strip()
                        f.write(f"## {m_name}\n")
                        slides = module.get("slides") if isinstance(module.get("slides"), list) else []
                        for i, slide in enumerate(slides, start=1):
                            f.write(f"Slide {i}: {str(slide.get('title') or '').strip()}\n")
                            bullets = slide.get("bullets") if isinstance(slide.get("bullets"), list) else []
                            for b in bullets:
                                f.write(f"- {str(b).strip()}\n")
                            f.write("\n")

            await _set_status(db, job, "batching")
            logger.info(
                "Module batching ready | job_id=%s modules=%s",
                str(job.id),
                len(module_entries),
            )

            pre_assessment_questions: list[dict[str, Any]] = []
            post_assessment_questions: list[dict[str, Any]] = []
            if assessments_enabled:
                if pre_assessment_task is not None:
                    try:
                        pre_assessment_questions = await pre_assessment_task
                        with open(pre_assessment_cache_path, "w", encoding="utf-8") as f:
                            json.dump(
                                {
                                    "phase": "pre",
                                    "source": "outline_text",
                                    "num_questions": len(pre_assessment_questions),
                                    "questions": pre_assessment_questions,
                                },
                                f,
                                ensure_ascii=False,
                                indent=2,
                            )
                    except Exception:
                        logger.exception("Slides pre-assessment generation failed | job_id=%s", str(job.id))
                post_context = _build_post_curriculum_from_modules(module_entries)
                if post_context:
                    post_assessment_task = asyncio.create_task(
                        generate_assessment_questions_from_text(
                            phase="post",
                            difficulty="advanced",
                            course_name=course_name,
                            curriculum_text=post_context,
                            num_questions=post_q_count,
                            pre_difficulty="intermediate",
                        )
                    )
                    logger.info(
                        "Slides post-assessment started in parallel | job_id=%s questions=%s",
                        str(job.id),
                        post_q_count,
                    )

            await _set_status(db, job, "gamma_rendering")
            ppt_paths: list[str] = []
            gamma_batch_links: list[str] = []
            gamma_generation_ids: list[str] = []
            gamma_request_log: list[dict[str, Any]] = []
            google_batch_links: list[str] = []
            google_batch_file_ids: list[str] = []
            module_gamma_links: list[dict[str, str | None]] = []
            # In this branch, Drive upload is intentionally disabled.
            drive_folder_id: str | None = None
            drive_folder_link: str | None = None

            for mi, module in enumerate(module_entries, start=1):
                module_name = str(module.get("module_name") or f"Module {mi}").strip() or f"Module {mi}"
                module_slides = module.get("slides") if isinstance(module.get("slides"), list) else []
                if not module_slides:
                    continue
                # Ensure each module presentation starts with a clear module heading slide.
                module_slides_for_gamma = [_build_module_cover_slide(module_name), *module_slides]
                module_batches = _batch_slides(module_slides_for_gamma)
                logger.info(
                    "Gamma rendering module | job_id=%s module=%s module_index=%s slides=%s batches=%s",
                    str(job.id),
                    module_name,
                    mi,
                    len(module_slides_for_gamma),
                    len(module_batches),
                )

                for bi, slides_batch in enumerate(module_batches, start=1):
                    batch_input_path = os.path.join(cache_dir, f"module_{mi}_batch_{bi}_input.txt")
                    with open(batch_input_path, "w", encoding="utf-8") as f:
                        f.write(_gamma_input_from_batch(slides_batch))

                    logger.info(
                        "Gamma rendering batch | job_id=%s module_index=%s batch_index=%s slides=%s",
                        str(job.id),
                        mi,
                        bi,
                        len(slides_batch),
                    )
                    gamma_result = await generate_ppt(slides_batch, include_export_bytes=False)
                    req_payload = gamma_result.get("request_payload")
                    if not isinstance(req_payload, dict):
                        req_payload = {}
                    # Persist the exact JSON payload sent to Gamma for each module batch.
                    gamma_payload_dump_path = os.path.join(
                        cache_dir, f"module_{mi}_batch_{bi}_gamma_request.json"
                    )
                    with open(gamma_payload_dump_path, "w", encoding="utf-8") as f:
                        json.dump(req_payload, f, ensure_ascii=False, indent=2)
                    gamma_request_log.append(
                        {
                            "module_index": mi,
                            "module_name": module_name,
                            "request_payload": req_payload,
                        }
                    )
                    gamma_url = str(gamma_result.get("gamma_url") or "").strip()
                    editable_gamma_url = str(gamma_result.get("editable_gamma_url") or "").strip()
                    generation_id = str(gamma_result.get("generation_id") or "").strip()
                    if gamma_url:
                        gamma_batch_links.append(gamma_url)
                    if generation_id:
                        gamma_generation_ids.append(generation_id)

                    drive_link: str | None = None
                    drive_file_id: str | None = None
                    suffix = f" Batch {bi}" if len(module_batches) > 1 else ""
                    module_gamma_links.append(
                        {
                            "module_index": str(mi),
                            "link_name": f"Module {mi}{suffix}",
                            "module_name": module_name,
                            "gamma_link": gamma_url or None,
                            "editable_gamma_link": editable_gamma_url or gamma_url or None,
                            "drive_link": drive_link,
                            "file_id": drive_file_id,
                        }
                    )
                # Persist links incrementally so polling shows completed modules even if later modules fail.
                try:
                    payload_progress = json.loads(job.payload_json or "{}")
                    if not isinstance(payload_progress, dict):
                        payload_progress = {}
                except Exception:
                    payload_progress = {}
                payload_progress["module_gamma_links"] = module_gamma_links
                payload_progress["gamma_batch_links"] = gamma_batch_links
                payload_progress["gamma_generation_ids"] = gamma_generation_ids
                payload_progress["gamma_request_log"] = gamma_request_log
                job.payload_json = json.dumps(payload_progress)
                await db.commit()
                await asyncio.sleep(0)

            # Keep status progression unchanged; merging deferred.
            await _set_status(db, job, "merging")
            logger.info("Merging deferred | job_id=%s batch_files=%s", str(job.id), len(ppt_paths))

            primary_link = gamma_batch_links[0] if gamma_batch_links else None

            payload_state: dict[str, Any] = {}
            try:
                payload_state = json.loads(job.payload_json or "{}")
                if not isinstance(payload_state, dict):
                    payload_state = {}
            except Exception:
                payload_state = {}
            payload_state["gamma_batch_links"] = gamma_batch_links
            payload_state["gamma_generation_ids"] = gamma_generation_ids
            payload_state["google_batch_links"] = google_batch_links
            payload_state["google_batch_file_ids"] = google_batch_file_ids
            payload_state["google_file_id"] = google_batch_file_ids[0] if google_batch_file_ids else None
            payload_state["google_drive_course_folder_id"] = drive_folder_id
            payload_state["google_drive_course_folder_link"] = drive_folder_link
            payload_state["module_gamma_links"] = module_gamma_links
            payload_state["gamma_request_log"] = gamma_request_log
            payload_state["zoho_attachment_payload"] = {
                "zoho_record_id": job.zoho_record_id,
                "primary_link": primary_link,
                "module_links": module_gamma_links,
            }

            # Resolver-friendly metadata: lets the courseware-assessments
            # endpoints find this job's content artifacts without rederiving
            # cache directory naming. content_hash binds the resolver to the
            # exact slides version the learner is being assessed on.
            payload_state["course_name"] = course_name
            payload_state["program_name"] = program_name
            payload_state["pre_assessment_difficulty"] = pre_assessment_difficulty
            payload_state["post_assessment_difficulty"] = post_assessment_difficulty
            payload_state["pre_assessment_num_questions"] = pre_assessment_num_questions
            payload_state["post_assessment_num_questions"] = post_assessment_num_questions
            payload_state["content_hash"] = content_hash
            payload_state["cache_dir"] = cache_dir
            payload_state["validated_slides_path"] = validated_cache_path
            payload_state["outline_pdf_path"] = outline_path
            # Stored excerpt avoids re-extracting the source PDF on every
            # /pre request; truncated to keep payload_json bounded.
            try:
                payload_state["outline_text_excerpt"] = (outline_text or "")[:200_000]
            except Exception:
                payload_state["outline_text_excerpt"] = ""
            # Optionally inline the validated slides into payload_json so
            # multi-replica deployments (where the cache dir is local-only)
            # can serve post-assessment generation from any container.
            try:
                if bool(getattr(settings, "COURSEWARE_VALIDATED_BLOB_IN_PAYLOAD", True)):
                    payload_state["validated_slides_blob"] = {
                        "cache_version": CACHE_VERSION,
                        "content_hash": content_hash,
                        "modules_detected": len(modules),
                        "modules": module_entries,
                    }
            except Exception:
                logger.exception(
                    "Failed to inline validated_slides into payload_json | job_id=%s",
                    str(job.id),
                )

            # Build pre/post assessment URLs (idempotent: only refresh when
            # content_hash changes between runs of the same record).
            previous_links = payload_state.get("courseware_assessment_links")
            previous_hash = (
                previous_links.get("content_hash") if isinstance(previous_links, dict) else None
            )
            if (
                isinstance(previous_links, dict)
                and previous_hash == content_hash
                and previous_links.get("pre_assessment_url")
                and previous_links.get("post_assessment_url")
            ):
                # Same content version — keep the previously-issued URLs to
                # avoid CRM churn (no Zoho re-write below either).
                links = previous_links
                links["refreshed_at"] = links.get("issued_at")
                logger.info(
                    "Courseware assessment links reused (content unchanged) | job_id=%s record=%s",
                    str(job.id),
                    job.zoho_record_id,
                )
            else:
                built = _build_assessment_urls(job.zoho_record_id)
                links = {
                    "pre_assessment_url": built["pre_assessment_url"],
                    "post_assessment_url": built["post_assessment_url"],
                    "pre_token": built["pre_token"],
                    "post_token": built["post_token"],
                    "content_hash": content_hash,
                    "issued_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                }
                logger.info(
                    "Courseware assessment links minted | job_id=%s record=%s pre=%s post=%s",
                    str(job.id),
                    job.zoho_record_id,
                    links.get("pre_assessment_url") or "-",
                    links.get("post_assessment_url") or "-",
                )
            payload_state["courseware_assessment_links"] = links
            payload_state["pre_assessment_url"] = links.get("pre_assessment_url")
            payload_state["post_assessment_url"] = links.get("post_assessment_url")

            if assessments_enabled:
                if post_assessment_task is not None:
                    try:
                        post_assessment_questions = await post_assessment_task
                        with open(post_assessment_cache_path, "w", encoding="utf-8") as f:
                            json.dump(
                                {
                                    "phase": "post",
                                    "source": "validated_slides",
                                    "num_questions": len(post_assessment_questions),
                                    "questions": post_assessment_questions,
                                },
                                f,
                                ensure_ascii=False,
                                indent=2,
                            )
                    except Exception:
                        logger.exception("Slides post-assessment generation failed | job_id=%s", str(job.id))
                payload_state["pre_assessment"] = {
                    "cache_path": pre_assessment_cache_path if os.path.exists(pre_assessment_cache_path) else None,
                    "num_questions": len(pre_assessment_questions),
                }
                payload_state["post_assessment"] = {
                    "cache_path": post_assessment_cache_path if os.path.exists(post_assessment_cache_path) else None,
                    "num_questions": len(post_assessment_questions),
                }
            job.payload_json = json.dumps(payload_state)

            job.ppt_url = primary_link
            await db.commit()
            logger.info(
                "Slides output ready (Gamma links) | job_id=%s primary_link=%s gamma_links=%s",
                str(job.id),
                primary_link,
                len(gamma_batch_links),
            )

            # Attaching to Zoho is intentionally skipped for the first testing pass.
            # This lets you validate PPT generation + `/ppts/...` URL in Postman
            # before we enable CRM attachment wiring.
            await _set_status(db, job, "attaching")
            logger.info(
                "Zoho attaching PPT link skipped (test mode) | job_id=%s zoho_record_id=%s ppt_url=%s",
                str(job.id),
                job.zoho_record_id,
                primary_link,
            )
            try:
                await update_slides_links_field(
                    zoho_record_id=job.zoho_record_id,
                    module_links=module_gamma_links,
                )
            except Exception:
                logger.exception(
                    "Zoho slides links write-back skipped due to error | job_id=%s zoho_record_id=%s",
                    str(job.id),
                    job.zoho_record_id,
                )

            # Idempotent: only push assessment URLs to Zoho when the content
            # hash changed (or hasn't been pushed yet for this hash). Keeps
            # the CRM field stable across slides re-runs that don't actually
            # produce different content.
            try:
                links_state = payload_state.get("courseware_assessment_links") or {}
                pushed_hash = str(links_state.get("zoho_pushed_content_hash") or "").strip()
                pre_url = links_state.get("pre_assessment_url")
                post_url = links_state.get("post_assessment_url")
                should_push = (
                    bool(pre_url or post_url)
                    and (pushed_hash != content_hash)
                )
                if should_push:
                    await update_assessment_links_field(
                        zoho_record_id=job.zoho_record_id,
                        pre_assessment_url=pre_url,
                        post_assessment_url=post_url,
                    )
                    links_state["zoho_pushed_content_hash"] = content_hash
                    links_state["zoho_pushed_at"] = time.strftime(
                        "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
                    )
                    payload_state["courseware_assessment_links"] = links_state
                    job.payload_json = json.dumps(payload_state)
                    await db.commit()
                else:
                    logger.info(
                        "Zoho assessment links push skipped (idempotent) | job_id=%s "
                        "content_hash=%s pushed_hash=%s",
                        str(job.id),
                        content_hash[:16],
                        pushed_hash[:16] or "-",
                    )
            except Exception:
                logger.exception(
                    "Zoho assessment links write-back skipped due to error | job_id=%s zoho_record_id=%s",
                    str(job.id),
                    job.zoho_record_id,
                )

            await _set_status(db, job, "completed")
            logger.info(
                "Slides job completed | job_id=%s seconds=%.2f",
                str(job.id),
                time.time() - started,
            )
        except Exception as e:
            logger.exception("Slides job failed | job_id=%s", str(job_id))
            await _set_status(db, job, "failed", error=str(e))
            logger.info(
                "Slides job failed (final) | job_id=%s seconds=%.2f",
                str(job.id),
                time.time() - started,
            )

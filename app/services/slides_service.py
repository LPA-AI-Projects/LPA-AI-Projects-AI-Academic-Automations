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
from app.services.document_extractor import extract_pdf_text_async, extract_ppt_text_async
from app.services.gamma_client import generate_ppt
from app.services.slides_graph import run_module_slides_pipeline
from app.services.zoho_crm import update_slides_links_field
from app.utils.logger import get_logger

logger = get_logger(__name__)

MAX_SLIDES_PER_BATCH = 60
CACHE_VERSION = "slides_cache_v2"


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
            lesson_path = payload.get("lesson_plan_and_activity_plan_pdf_path")
            instructor_path = payload.get("instructor_ppt_path")
            course_name = _safe_course_name(payload.get("course_name"))
            program_name = str(payload.get("program_name") or "").strip() or None
            cache_root = os.path.join(ppts_dir(), "cache", _safe_id(job.zoho_record_id))
            cache_dir = cache_root
            os.makedirs(cache_dir, exist_ok=True)
            validated_cache_path = os.path.join(cache_dir, "validated_slides.json")
            validated_text_path = os.path.join(cache_dir, "validated_slides.txt")

            if not outline_path or not os.path.exists(outline_path):
                raise RuntimeError("outline_pdf is missing on disk for this job.")

            with open(outline_path, "rb") as f:
                outline_bytes = f.read()
            outline_text = await extract_pdf_text_async(outline_bytes)
            logger.info(
                "Slides extracted outline | job_id=%s chars=%s",
                str(job.id),
                len(outline_text or ""),
            )

            lesson_text = None
            if lesson_path and os.path.exists(lesson_path):
                with open(lesson_path, "rb") as f:
                    lesson_text = await extract_pdf_text_async(f.read())
                logger.info(
                    "Slides extracted lesson/activity | job_id=%s chars=%s",
                    str(job.id),
                    len(lesson_text or ""),
                )

            instructor_text = None
            if instructor_path and os.path.exists(instructor_path):
                with open(instructor_path, "rb") as f:
                    instructor_text = await extract_ppt_text_async(f.read())
                logger.info(
                    "Slides extracted instructor ppt text | job_id=%s chars=%s",
                    str(job.id),
                    len(instructor_text or ""),
                )

            modules = _extract_outline_modules(outline_text, program_name=program_name)
            logger.info("Outline modules detected | job_id=%s modules=%s", str(job.id), len(modules))
            outline_hash = _hash_text(outline_text)
            lesson_hash = _hash_text(lesson_text)
            instructor_hash = _hash_text(instructor_text)
            content_hash = hashlib.sha256(
                f"{outline_hash}|{lesson_hash}|{instructor_hash}".encode("utf-8")
            ).hexdigest()
            cache_dir = os.path.join(cache_root, f"{CACHE_VERSION}_{content_hash[:16]}")
            os.makedirs(cache_dir, exist_ok=True)
            validated_cache_path = os.path.join(cache_dir, "validated_slides.json")
            validated_text_path = os.path.join(cache_dir, "validated_slides.txt")

            module_entries: list[dict[str, Any]] = []
            planner_model = (settings.SLIDES_PLANNER_MODEL or "").strip() or None
            generator_model = (settings.SLIDES_GENERATOR_MODEL or "").strip() or None
            validator_model = (settings.SLIDES_VALIDATOR_MODEL or "").strip() or None
            min_per_module = max(1, int(getattr(settings, "SLIDES_MIN_PER_MODULE", 10) or 10))
            max_per_module = max(min_per_module, int(getattr(settings, "SLIDES_MAX_PER_MODULE", 20) or 20))
            max_loops = max(1, int(getattr(settings, "SLIDES_VALIDATION_MAX_LOOPS", 2) or 2))
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

                for mi, mod in enumerate(modules, start=1):
                    module_name = str(mod.get("module_name") or f"Module {mi}").strip() or f"Module {mi}"
                    module_text = str(mod.get("module_text") or "").strip()
                    t0 = time.time()
                    validated_module = await run_module_slides_pipeline(
                        module_name=module_name,
                        module_text=module_text,
                        lesson_text=lesson_text,
                        instructor_text=instructor_text,
                        planner_model=planner_model,
                        generator_model=generator_model,
                        validator_model=validator_model,
                        min_slides=min_per_module,
                        max_slides=max_per_module,
                        max_loops=max_loops,
                    )
                    logger.info(
                        "Module pipeline completed | job_id=%s module=%s slides=%s seconds=%.2f",
                        str(job.id),
                        module_name,
                        len(validated_module),
                        time.time() - t0,
                    )
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

            await _set_status(db, job, "gamma_rendering")
            ppt_paths: list[str] = []
            gamma_batch_links: list[str] = []
            gamma_generation_ids: list[str] = []
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
            payload_state["zoho_attachment_payload"] = {
                "zoho_record_id": job.zoho_record_id,
                "primary_link": primary_link,
                "module_links": module_gamma_links,
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

from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any

from sqlalchemy import select

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.models.job import CourseJob
from app.services.document_extractor import extract_pdf_text_async, extract_ppt_text_async
from app.services.gamma_client import generate_ppt
from app.services.google_drive import ensure_drive_folder, upload_ppt_bytes_to_google_drive
from app.services.slide_generator import generate_slide
from app.services.slide_planner import plan_slides
from app.services.slide_validator import validate_slides
from app.utils.logger import get_logger

logger = get_logger(__name__)

MAX_SLIDES_PER_BATCH = 60


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
            cache_dir = os.path.join("generated_ppts", "cache", _safe_id(job.zoho_record_id))
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

            validated: list[dict[str, Any]]
            if os.path.exists(validated_cache_path):
                with open(validated_cache_path, "r", encoding="utf-8") as f:
                    cached = json.load(f)
                if not isinstance(cached, list) or not cached:
                    raise RuntimeError("Cached validated slides file is invalid.")
                validated = cached
                logger.info(
                    "Using cached validated slides | job_id=%s cache_path=%s slides=%s",
                    str(job.id),
                    validated_cache_path,
                    len(validated),
                )
            else:
                await _set_status(db, job, "planning")
                t0 = time.time()
                plan = await plan_slides(
                    outline=outline_text,
                    lesson=lesson_text,
                    activity=None,
                    instructor=instructor_text,
                )
                logger.info(
                    "Slides planning done | job_id=%s seconds=%.2f",
                    str(job.id),
                    time.time() - t0,
                )
                planned_slides = plan.get("slides") if isinstance(plan, dict) else None
                if not isinstance(planned_slides, list) or not planned_slides:
                    raise RuntimeError("Slide planner returned no slides.")
                logger.info(
                    "Slides plan received | job_id=%s planned_slides=%s",
                    str(job.id),
                    len(planned_slides),
                )

                await _set_status(db, job, "generating_slides")
                context = {
                    "outline": outline_text[:150000],
                    "lesson": (lesson_text or "")[:150000],
                    "instructor": (instructor_text or "")[:150000],
                }
                generated: list[dict[str, Any]] = []
                gen_started = time.time()
                for idx, s in enumerate(planned_slides, start=1):
                    slide_out = await generate_slide(slide=s, context=context)
                    generated.append(slide_out)
                    if idx % 5 == 0:
                        # keep some heartbeat in job payload for debugging
                        job.payload_json = json.dumps(
                            {"progress": {"generated": idx, "total": len(planned_slides)}}
                        )
                        await db.commit()
                        logger.info(
                            "Slides generated progress | job_id=%s generated=%s total=%s",
                            str(job.id),
                            idx,
                            len(planned_slides),
                        )
                logger.info(
                    "Slides generation done | job_id=%s slides=%s seconds=%.2f",
                    str(job.id),
                    len(generated),
                    time.time() - gen_started,
                )

                await _set_status(db, job, "validating")
                t0 = time.time()
                validated = validate_slides(planned_slides=planned_slides, generated_slides=generated)
                logger.info(
                    "Slides validated | job_id=%s slides=%s seconds=%.2f",
                    str(job.id),
                    len(validated),
                    time.time() - t0,
                )
                with open(validated_cache_path, "w", encoding="utf-8") as f:
                    json.dump(validated, f, ensure_ascii=False, indent=2)
                with open(validated_text_path, "w", encoding="utf-8") as f:
                    for i, slide in enumerate(validated, start=1):
                        f.write(f"Slide {i}: {str(slide.get('title') or '').strip()}\n")
                        bullets = slide.get("bullets") if isinstance(slide.get("bullets"), list) else []
                        for b in bullets:
                            f.write(f"- {str(b).strip()}\n")
                        notes = str(slide.get("notes") or "").strip()
                        if notes:
                            f.write(f"Notes: {notes}\n")
                        f.write("\n")
                logger.info(
                    "Slides debug cache saved | job_id=%s json=%s txt=%s",
                    str(job.id),
                    validated_cache_path,
                    validated_text_path,
                )

            await _set_status(db, job, "batching")
            batches = _batch_slides(validated)
            logger.info(
                "Slides batched | job_id=%s batches=%s max_per_batch=%s",
                str(job.id),
                len(batches),
                MAX_SLIDES_PER_BATCH,
            )

            await _set_status(db, job, "gamma_rendering")
            ppt_paths: list[str] = []
            batch_dir = os.path.join("generated_ppts", "batches", str(job_id))
            os.makedirs(batch_dir, exist_ok=True)
            gamma_batch_links: list[str] = []
            gamma_generation_ids: list[str] = []
            google_batch_links: list[str] = []
            google_batch_file_ids: list[str] = []
            drive_folder_id: str | None = None
            drive_folder_link: str | None = None
            drive_parent_id = (os.getenv("GOOGLE_DRIVE_FOLDER_ID") or "").strip() or None
            try:
                folder_name = f"{course_name}_{_safe_id(job.zoho_record_id)}"
                folder = await asyncio.to_thread(
                    ensure_drive_folder,
                    folder_name,
                    parent_folder_id=drive_parent_id,
                )
                drive_folder_id = str(folder.get("folder_id") or "").strip() or None
                drive_folder_link = str(folder.get("folder_link") or "").strip() or None
                logger.info(
                    "Google Drive course folder ready | job_id=%s folder_id=%s course_name=%s",
                    str(job.id),
                    drive_folder_id,
                    course_name,
                )
            except Exception:
                logger.exception(
                    "Google Drive folder create/find failed; continuing without Drive upload | job_id=%s",
                    str(job.id),
                )
            for bi, batch in enumerate(batches, start=1):
                logger.info(
                    "Gamma rendering batch | job_id=%s batch=%s/%s slides=%s",
                    str(job.id),
                    bi,
                    len(batches),
                    len(batch),
                )
                batch_input_path = os.path.join(cache_dir, f"batch_{bi}_input.txt")
                with open(batch_input_path, "w", encoding="utf-8") as f:
                    f.write(_gamma_input_from_batch(batch))

                stable_local_ppt = os.path.join(cache_dir, f"{course_name}_{bi}.pptx")
                out_path = os.path.join(batch_dir, f"batch_{bi}.pptx")
                gamma_url = ""
                generation_id = ""
                if os.path.exists(stable_local_ppt):
                    with open(stable_local_ppt, "rb") as f:
                        ppt_bytes = f.read()
                    logger.info(
                        "Using cached local PPT for batch | job_id=%s batch=%s path=%s",
                        str(job.id),
                        bi,
                        stable_local_ppt,
                    )
                else:
                    gamma_result = await generate_ppt(batch)
                    ppt_bytes = gamma_result.get("ppt_bytes", b"")
                    gamma_url = str(gamma_result.get("gamma_url") or "").strip()
                    generation_id = str(gamma_result.get("generation_id") or "").strip()
                    with open(stable_local_ppt, "wb") as f:
                        f.write(ppt_bytes)
                    logger.info(
                        "Saved stable local PPT | job_id=%s batch=%s path=%s",
                        str(job.id),
                        bi,
                        stable_local_ppt,
                    )
                with open(out_path, "wb") as f:
                    f.write(ppt_bytes)
                ppt_paths.append(out_path)
                logger.info(
                    "Gamma batch saved | job_id=%s batch=%s bytes=%s path=%s",
                    str(job.id),
                    bi,
                    len(ppt_bytes),
                    out_path,
                )
                if gamma_url:
                    gamma_batch_links.append(gamma_url)
                if generation_id:
                    gamma_generation_ids.append(generation_id)
                if drive_folder_id:
                    try:
                        drive_upload = await asyncio.to_thread(
                            upload_ppt_bytes_to_google_drive,
                            ppt_bytes,
                            f"{course_name}_{bi}.pptx",
                            parent_folder_id=drive_folder_id,
                            convert_to_google_slides=False,
                        )
                        g_file_id = str(drive_upload.get("file_id") or "").strip()
                        g_link = str(drive_upload.get("edit_link") or "").strip()
                        if g_file_id:
                            google_batch_file_ids.append(g_file_id)
                        if g_link:
                            google_batch_links.append(g_link)
                    except Exception:
                        logger.exception(
                            "Google Drive upload failed for batch | job_id=%s batch=%s",
                            str(job.id),
                            bi,
                        )
                await asyncio.sleep(0)  # cooperative

            # Keep status progression unchanged; merging deferred.
            await _set_status(db, job, "merging")
            logger.info("Merging deferred | job_id=%s batch_files=%s", str(job.id), len(ppt_paths))

            primary_link = gamma_batch_links[0] if gamma_batch_links else _build_ppt_url_from_relative_path(
                os.path.join("batches", str(job_id), "batch_1.pptx")
            )

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

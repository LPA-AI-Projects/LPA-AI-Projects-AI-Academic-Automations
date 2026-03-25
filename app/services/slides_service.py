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
from app.services.ppt_merger import merge_ppt_files_async
from app.services.slide_generator import generate_slide
from app.services.slide_planner import plan_slides
from app.services.slide_validator import validate_slides
from app.utils.logger import get_logger

logger = get_logger(__name__)

MAX_SLIDES_PER_BATCH = 40


def _build_ppt_url(file_path: str) -> str:
    filename = os.path.basename(file_path)
    return f"{settings.BASE_URL}/ppts/{filename}"


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
            for bi, batch in enumerate(batches, start=1):
                logger.info(
                    "Gamma rendering batch | job_id=%s batch=%s/%s slides=%s",
                    str(job.id),
                    bi,
                    len(batches),
                    len(batch),
                )
                ppt_bytes = await generate_ppt(batch)
                out_path = os.path.join(batch_dir, f"batch_{bi}.pptx")
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
                await asyncio.sleep(0)  # cooperative

            await _set_status(db, job, "merging")
            final_path = os.path.join("generated_ppts", f"{job_id}.pptx")
            t0 = time.time()
            merged_path = await merge_ppt_files_async(ppt_paths, output_path=final_path)
            logger.info(
                "PPT merge completed | job_id=%s files=%s seconds=%.2f path=%s",
                str(job.id),
                len(ppt_paths),
                time.time() - t0,
                merged_path,
            )
            ppt_url = _build_ppt_url(merged_path)
            job.ppt_url = ppt_url
            await db.commit()
            logger.info("Slides PPT ready | job_id=%s ppt_url=%s", str(job.id), ppt_url)

            # Attaching to Zoho is intentionally skipped for the first testing pass.
            # This lets you validate PPT generation + `/ppts/...` URL in Postman
            # before we enable CRM attachment wiring.
            await _set_status(db, job, "attaching")
            logger.info(
                "Zoho attaching PPT link skipped (test mode) | job_id=%s zoho_record_id=%s ppt_url=%s",
                str(job.id),
                job.zoho_record_id,
                ppt_url,
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

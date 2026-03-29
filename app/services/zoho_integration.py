"""
Zoho integration: completion webhooks (ZOHO_CALLBACK_URL) + CRM PDF link attach (zoho_crm).

Flow after a course outline job finishes:
1. Optionally attach ``pdf_url`` to the CRM record (ZOHO_ATTACH_PDF_LINK_TO_CRM + OAuth).
2. Optionally POST job metadata to ZOHO_CALLBACK_URL (JSON or form).

Refine (new version) uses the same webhook payload shape with ``job_id`` omitted when there is no outline job row.
"""
from __future__ import annotations

from typing import Any

import httpx

from app.core.config import settings
from app.models.job import CourseJob
from app.services.zoho_crm import maybe_attach_course_pdf
from app.utils.logger import get_logger

logger = get_logger(__name__)

# README / template URLs — do not treat as real callbacks
_CALLBACK_PLACEHOLDER_MARKERS = (
    "example.com",
    "your-zoho",
    "your-api-domain",
    "callback-endpoint.example",
    "localhost",
)


def zoho_completion_webhook_is_configured() -> bool:
    """True when ZOHO_CALLBACK_URL is set and not a documented placeholder."""
    url = (settings.ZOHO_CALLBACK_URL or "").strip()
    if not url:
        return False
    u = url.lower()
    return not any(m in u for m in _CALLBACK_PLACEHOLDER_MARKERS)


async def post_zoho_completion_webhook(
    *,
    job_id: str | None,
    zoho_record_id: str,
    status: str,
    pdf_url: str | None,
    version_number: int | None,
    error: str | None,
) -> None:
    """
    POST completion payload to ZOHO_CALLBACK_URL (skipped if unset or placeholder).
    """
    if not zoho_completion_webhook_is_configured():
        logger.info(
            "Zoho completion webhook skipped: ZOHO_CALLBACK_URL not set or placeholder | job_id=%s",
            job_id or "",
        )
        return

    raw_fields: dict[str, Any] = {
        "job_id": job_id,
        "zoho_record_id": zoho_record_id,
        "status": status,
        "pdf_url": pdf_url,
        "version_number": version_number,
        "error": error,
    }
    fmt = (settings.ZOHO_CALLBACK_BODY_FORMAT or "json").strip().lower()
    url = (settings.ZOHO_CALLBACK_URL or "").strip()
    try:
        logger.info(
            "Posting Zoho completion webhook | job_id=%s format=%s status=%s",
            job_id or "",
            fmt,
            status,
        )
        async with httpx.AsyncClient(timeout=30.0) as client:
            if fmt == "form":
                form_data = {k: ("" if v is None else str(v)) for k, v in raw_fields.items()}
                response = await client.post(url, data=form_data)
            else:
                json_body = {k: v for k, v in raw_fields.items() if v is not None}
                response = await client.post(url, json=json_body)
            logger.info(
                "Zoho completion webhook response | job_id=%s status_code=%s",
                job_id or "",
                response.status_code,
            )
            if response.status_code >= 400:
                logger.warning(
                    "Zoho completion webhook rejected | job_id=%s body=%s",
                    job_id or "",
                    (response.text or "")[:2000],
                )
    except Exception:
        logger.exception("Zoho completion webhook failed | job_id=%s", job_id or "")


async def post_zoho_completion_webhook_for_job(job: CourseJob, version_number: int | None) -> None:
    """Send webhook using fields from a persisted ``CourseJob`` row."""
    await post_zoho_completion_webhook(
        job_id=str(job.id),
        zoho_record_id=job.zoho_record_id,
        status=job.status,
        pdf_url=job.pdf_url,
        version_number=version_number,
        error=job.error,
    )


async def zoho_notify_course_outline_job_finished(
    job: CourseJob,
    version_number: int | None,
    *,
    attach_course_title: str | None,
) -> None:
    """
    After a terminal job state is committed: optional CRM attach, then optional webhook.

    * ``attach_course_title`` — if set, ``maybe_attach_course_pdf`` runs (when enabled in settings).
    * Use ``attach_course_title=None`` for failed jobs (webhook only).
    """
    if attach_course_title:
        await maybe_attach_course_pdf(
            zoho_record_id=job.zoho_record_id,
            pdf_url=job.pdf_url,
            course_name_for_title=attach_course_title,
        )
    await post_zoho_completion_webhook_for_job(job, version_number)


async def zoho_notify_refined_outline_version(
    *,
    zoho_record_id: str,
    pdf_url: str | None,
    version_number: int,
    course_name_for_title: str,
) -> None:
    """CRM attach + same webhook payload as jobs (no ``job_id``) for refine flows."""
    await maybe_attach_course_pdf(
        zoho_record_id=zoho_record_id,
        pdf_url=pdf_url,
        course_name_for_title=course_name_for_title,
    )
    await post_zoho_completion_webhook(
        job_id=None,
        zoho_record_id=zoho_record_id,
        status="completed",
        pdf_url=pdf_url,
        version_number=version_number,
        error=None,
    )

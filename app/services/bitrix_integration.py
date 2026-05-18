"""
Bitrix24 integration after course outline jobs: optional CRM field updates + callback URL.
"""
from __future__ import annotations

from typing import Any

import httpx

from app.core.config import settings
from app.models.job import CourseJob
from app.api.auth_deps import bitrix_application_token_configured
from app.services.bitrix_crm import bitrix_configured, maybe_attach_course_pdf, update_outline_record_fields
from app.utils.logger import get_logger

logger = get_logger(__name__)

_CALLBACK_PLACEHOLDER_MARKERS = (
    "example.com",
    "your-bitrix",
    "localhost",
)


def bitrix_completion_webhook_is_configured() -> bool:
    url = (settings.BITRIX_CALLBACK_URL or "").strip()
    if not url:
        return False
    u = url.lower()
    return not any(m in u for m in _CALLBACK_PLACEHOLDER_MARKERS)


def get_bitrix_course_outline_integration_status() -> dict[str, bool]:
    return {
        "bitrix_webhook_configured": bitrix_configured(),
        "bitrix_application_token_configured": bitrix_application_token_configured(),
        "bitrix_crm_attach_configured": bool(
            settings.BITRIX_ATTACH_PDF_TO_CRM
            and bitrix_configured()
            and (settings.BITRIX_OUTLINE_PDF_FIELD or "").strip()
        ),
        "bitrix_task_attach_configured": bool(
            settings.BITRIX_TASK_ATTACH_ENABLED
            and bitrix_configured()
            and (settings.BITRIX_DRIVE_FOLDER_ID or "").strip()
        ),
        "bitrix_completion_callback_configured": bitrix_completion_webhook_is_configured(),
    }


async def post_bitrix_completion_webhook(
    *,
    job_id: str | None,
    bitrix_record_id: str,
    status: str,
    pdf_urls: list[str] | None,
    version_number: int | None,
    error: str | None,
) -> None:
    if not bitrix_completion_webhook_is_configured():
        return

    raw_fields: dict[str, Any] = {
        "job_id": job_id,
        "bitrix_record_id": bitrix_record_id,
        "status": status,
        "pdf_urls": [u for u in (pdf_urls or []) if str(u).strip()],
        "version_number": version_number,
        "error": error,
    }
    fmt = (settings.BITRIX_CALLBACK_BODY_FORMAT or "json").strip().lower()
    url = (settings.BITRIX_CALLBACK_URL or "").strip()
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            if fmt == "form":
                form_data = {k: ("" if v is None else str(v)) for k, v in raw_fields.items()}
                response = await client.post(url, data=form_data)
            else:
                json_body = {k: v for k, v in raw_fields.items() if v is not None}
                response = await client.post(url, json=json_body)
            if response.status_code >= 400:
                logger.warning(
                    "Bitrix completion callback rejected | status=%s body=%s",
                    response.status_code,
                    (response.text or "")[:2000],
                )
    except Exception:
        logger.exception("Bitrix completion callback failed | record_id=%s", bitrix_record_id)


async def bitrix_notify_course_outline_job_finished(
    job: CourseJob,
    version_number: int | None,
    *,
    attach_course_title: str | None,
) -> None:
    if attach_course_title:
        await maybe_attach_course_pdf(
            bitrix_record_id=job.zoho_record_id,
            pdf_url=job.pdf_url,
            course_name_for_title=attach_course_title,
        )
    urls: list[str] = []
    if isinstance(job.pdf_url, str) and job.pdf_url.strip():
        urls = [job.pdf_url.strip()]
    await post_bitrix_completion_webhook(
        job_id=str(job.id),
        bitrix_record_id=job.zoho_record_id,
        status=job.status,
        pdf_urls=urls,
        version_number=version_number,
        error=job.error,
    )


async def bitrix_outline_status_in_progress(bitrix_record_id: str) -> None:
    await _bitrix_outline_status(bitrix_record_id, "in_progress")


async def bitrix_outline_status_completed(bitrix_record_id: str) -> None:
    await _bitrix_outline_status(bitrix_record_id, "completed")


async def bitrix_outline_status_failed(bitrix_record_id: str) -> None:
    await _bitrix_outline_status(bitrix_record_id, "failed")


async def _bitrix_outline_status(bitrix_record_id: str, phase: str) -> None:
    field = (settings.BITRIX_OUTLINE_STATUS_FIELD or "").strip()
    if not field:
        return
    values = {
        "in_progress": (settings.BITRIX_STATUS_IN_PROGRESS or "In Progress").strip(),
        "completed": (settings.BITRIX_STATUS_COMPLETED or "Completed").strip(),
        "failed": (settings.BITRIX_STATUS_FAILED or "Failed to create - Try Again").strip(),
    }
    value = values.get(phase, "")
    if not value:
        return
    try:
        await update_outline_record_fields(
            bitrix_record_id=bitrix_record_id,
            fields={field: value},
        )
    except Exception:
        logger.exception(
            "Bitrix status update failed | record_id=%s phase=%s",
            bitrix_record_id,
            phase,
        )

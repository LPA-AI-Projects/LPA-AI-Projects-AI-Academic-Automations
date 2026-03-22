"""
Zoho CRM V8 integration: OAuth (refresh token) + attach PDF by public URL.

Docs:
- OAuth: https://www.zoho.com/crm/developer/docs/api/v8/oauth-overview.html
- Upload attachment / link: https://www.zoho.com/crm/developer/docs/api/v8/upload-attachment.html

Requires CRM record id (numeric string from Zoho) for the Attachments API path.
"""
from __future__ import annotations

import time
from typing import Any

import httpx

from app.core.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)

# Cached access token (process-local; refresh ~1h expiry before edge)
_token_cache: str | None = None
_token_expires_at: float = 0.0


class ZohoCrmNotConfigured(RuntimeError):
    """OAuth env vars missing."""


def _crm_configured() -> bool:
    return bool(
        settings.ZOHO_CLIENT_ID
        and settings.ZOHO_CLIENT_SECRET
        and settings.ZOHO_REFRESH_TOKEN
        and settings.ZOHO_CRM_MODULE_API_NAME
    )


async def get_access_token() -> str:
    """Exchange refresh token for access token; cache until ~5 min before expiry."""
    global _token_cache, _token_expires_at

    if not _crm_configured():
        raise ZohoCrmNotConfigured("Zoho CRM OAuth is not fully configured in environment.")

    now = time.time()
    if _token_cache and now < _token_expires_at - 300:
        return _token_cache

    token_url = f"{settings.ZOHO_ACCOUNTS_BASE_URL.rstrip('/')}/oauth/v2/token"
    data = {
        "grant_type": "refresh_token",
        "client_id": settings.ZOHO_CLIENT_ID,
        "client_secret": settings.ZOHO_CLIENT_SECRET,
        "refresh_token": settings.ZOHO_REFRESH_TOKEN,
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(token_url, data=data)
        response.raise_for_status()
        payload: dict[str, Any] = response.json()

    access = payload.get("access_token")
    if not isinstance(access, str) or not access:
        raise RuntimeError("Zoho token response missing access_token")

    expires_in = int(payload.get("expires_in", 3600))
    _token_cache = access
    _token_expires_at = now + max(60, expires_in)
    logger.info("Zoho CRM access token refreshed (expires_in=%s)", expires_in)
    return access


async def attach_pdf_link_to_record(
    *,
    module_api_name: str,
    crm_record_id: str,
    public_pdf_url: str,
    attachment_title: str,
) -> dict[str, Any]:
    """
    POST .../crm/v8/{module}/{record_id}/Attachments with multipart field attachmentUrl.

    See: Upload an Attachment API (link variant).
    """
    token = await get_access_token()
    base = settings.ZOHO_CRM_API_BASE.rstrip("/")
    path = f"{base}/crm/v8/{module_api_name.strip('/')}/{crm_record_id}/Attachments"
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}

    # Multipart form: attachmentUrl + optional title (per Zoho docs)
    files = {
        "attachmentUrl": (None, public_pdf_url),
        "title": (None, attachment_title),
    }

    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(path, headers=headers, files=files)

    if response.status_code >= 400:
        logger.warning(
            "Zoho attach link failed | status=%s body=%s",
            response.status_code,
            (response.text or "")[:2000],
        )
        response.raise_for_status()

    return response.json()


async def maybe_attach_course_pdf(
    *,
    zoho_record_id: str,
    pdf_url: str | None,
    course_name_for_title: str,
) -> None:
    """
    If enabled and configured, attach generated PDF public URL to the CRM record.
    Failures are logged only (do not fail the background job).
    """
    if not pdf_url or not settings.ZOHO_ATTACH_PDF_LINK_TO_CRM:
        return
    if not _crm_configured():
        logger.info("Zoho CRM attach skipped: OAuth/module not configured")
        return

    try:
        await attach_pdf_link_to_record(
            module_api_name=settings.ZOHO_CRM_MODULE_API_NAME,
            crm_record_id=zoho_record_id,
            public_pdf_url=pdf_url,
            attachment_title=course_name_for_title or "Course outline",
        )
        logger.info(
            "Zoho CRM: PDF link attached | record_id=%s title=%s",
            zoho_record_id,
            course_name_for_title,
        )
    except Exception:
        logger.exception("Zoho CRM: attach PDF link failed | record_id=%s", zoho_record_id)

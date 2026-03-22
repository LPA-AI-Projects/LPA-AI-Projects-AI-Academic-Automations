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
    raw = response.text or ""
    try:
        payload = response.json()
    except Exception:
        logger.error(
            "Zoho OAuth token endpoint returned non-JSON | status=%s url=%s body=%s",
            response.status_code,
            token_url,
            raw[:2000],
        )
        raise RuntimeError(
            "Zoho OAuth token response was not JSON. Check ZOHO_ACCOUNTS_BASE_URL matches your "
            "Zoho data center (e.g. .com vs .eu vs .in) and credentials."
        ) from None

    if not isinstance(payload, dict):
        raise RuntimeError(f"Zoho OAuth unexpected response type: {type(payload)}")

    err_code = payload.get("error")
    err_desc = str(payload.get("error_description") or "")

    if response.status_code >= 400:
        logger.error(
            "Zoho OAuth HTTP error | status=%s error=%s description=%s",
            response.status_code,
            err_code,
            err_desc or raw[:1500],
        )
        raise RuntimeError(
            f"Zoho OAuth token HTTP {response.status_code}: {err_code or 'unknown'} — {err_desc or raw[:500]}"
        )

    access = payload.get("access_token")
    if not isinstance(access, str) or not access:
        logger.error(
            "Zoho OAuth success status but no access_token | error=%s description=%s raw_keys=%s",
            err_code,
            err_desc,
            list(payload.keys()),
        )
        raise RuntimeError(
            f"Zoho OAuth did not return access_token. "
            f"error={err_code!r} description={err_desc!r} "
            f"(fix refresh token, client id/secret, or use correct accounts domain for your DC)."
        )

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
    rid = (crm_record_id or "").strip()
    mod = module_api_name.strip("/")
    token = await get_access_token()
    base = settings.ZOHO_CRM_API_BASE.rstrip("/")
    path = f"{base}/crm/v8/{mod}/{rid}/Attachments"
    logger.info(
        "Zoho CRM attaching PDF link | module=%s record_id=%s url_len=%s",
        mod,
        rid,
        len(public_pdf_url or ""),
    )
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
        logger.info(
            "Zoho CRM attach skipped: set ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, "
            "ZOHO_REFRESH_TOKEN, and ZOHO_CRM_MODULE_API_NAME (e.g. Course_Outline), "
            "and ZOHO_ATTACH_PDF_LINK_TO_CRM=true"
        )
        return

    try:
        await attach_pdf_link_to_record(
            module_api_name=settings.ZOHO_CRM_MODULE_API_NAME,
            crm_record_id=zoho_record_id.strip(),
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

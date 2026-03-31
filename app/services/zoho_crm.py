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
from urllib.parse import quote

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
    )


def get_outline_module_api_name() -> str:
    """
    Module API name used for course-outline CRM attachment flow.
    Backward compatible with legacy ZOHO_CRM_MODULE_API_NAME.
    """
    return (
        (settings.ZOHO_CRM_OUTLINE_MODULE_API_NAME or "").strip()
        or (settings.ZOHO_CRM_MODULE_API_NAME or "").strip()
        or "Course_Outline"
    )


def get_slides_module_api_name() -> str:
    """
    Module API name used for slides input fetch flow.
    Backward compatible with legacy ZOHO_CRM_MODULE_API_NAME.
    """
    return (
        (settings.ZOHO_CRM_SLIDES_MODULE_API_NAME or "").strip()
        or (settings.ZOHO_CRM_MODULE_API_NAME or "").strip()
        or "Course_Outline"
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
            "Zoho OAuth success status but no access_token | url=%s error=%s description=%s raw_keys=%s raw=%s",
            token_url,
            err_code,
            err_desc,
            list(payload.keys()),
            raw[:800],
        )
        hint = (
            "invalid_code on refresh_token grant usually means: refresh token does not belong to this "
            "client_id, was revoked, or was issued on a different Zoho DC — regenerate refresh token "
            "in API Console for this client, and set ZOHO_ACCOUNTS_BASE_URL to the same DC "
            "(.com / .eu / .in / .com.au)."
        )
        if err_code == "invalid_code":
            hint = (
                "Zoho returned invalid_code: your ZOHO_REFRESH_TOKEN is not accepted for this "
                "client_id + ZOHO_ACCOUNTS_BASE_URL. Regenerate a new refresh token (same app, same DC). "
                "Do not paste a short-lived authorization `code` from the URL — only the refresh_token "
                "from the /oauth/v2/token exchange response."
            )
        raise RuntimeError(
            f"Zoho OAuth did not return access_token. error={err_code!r} description={err_desc!r}. {hint}"
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
            "ZOHO_REFRESH_TOKEN, and outline module API name "
            "(ZOHO_CRM_OUTLINE_MODULE_API_NAME or ZOHO_CRM_MODULE_API_NAME), "
            "and ZOHO_ATTACH_PDF_LINK_TO_CRM=true"
        )
        return

    try:
        await attach_pdf_link_to_record(
            module_api_name=get_outline_module_api_name(),
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


def _auth_header(token: str) -> dict[str, str]:
    return {"Authorization": f"Zoho-oauthtoken {token}"}


def _extract_file_upload_candidate(value: Any) -> dict[str, Any]:
    """
    Normalize Zoho File Upload field value into a small dict.
    Handles list/dict/string formats seen across tenants/DCs.
    """
    item: Any = value
    if isinstance(value, list):
        item = value[0] if value else {}
    if isinstance(item, str):
        return {"file_id": item.strip() or None, "download_url": None, "file_name": None, "raw": value}
    if not isinstance(item, dict):
        return {"file_id": None, "download_url": None, "file_name": None, "raw": value}

    file_id = (
        item.get("id")
        or item.get("file_id")
        or item.get("File_Id")
        or item.get("attachment_id")
        or item.get("$file_id")
    )
    download_url = (
        item.get("download_url")
        or item.get("Download_URL")
        or item.get("file_url")
        or item.get("File_URL")
        or item.get("link_url")
        or item.get("preview_url")
    )
    file_name = item.get("name") or item.get("file_name") or item.get("File_Name")
    return {
        "file_id": str(file_id).strip() if file_id else None,
        "download_url": str(download_url).strip() if download_url else None,
        "file_name": str(file_name).strip() if file_name else None,
        "raw": value,
    }


async def get_record_file_upload_field(
    *,
    module_api_name: str,
    crm_record_id: str,
    field_api_name: str = "outline",
) -> dict[str, Any]:
    """
    Fetch a File Upload field from a Zoho CRM record.
    Returns normalized metadata and logs the raw field shape.
    """
    rid = (crm_record_id or "").strip()
    mod = (module_api_name or "").strip("/")
    field = (field_api_name or "").strip()
    token = await get_access_token()
    base = settings.ZOHO_CRM_API_BASE.rstrip("/")
    url = f"{base}/crm/v8/{mod}/{quote(rid, safe='')}?fields={quote(field, safe=',_')}"

    logger.info(
        "Zoho CRM fetch file field | module=%s record_id=%s field=%s",
        mod,
        rid,
        field,
    )
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(url, headers=_auth_header(token))
    if response.status_code >= 400:
        logger.warning(
            "Zoho CRM fetch file field failed | status=%s body=%s",
            response.status_code,
            (response.text or "")[:2000],
        )
        response.raise_for_status()

    payload = response.json() if response.content else {}
    records = payload.get("data") if isinstance(payload, dict) else None
    record = records[0] if isinstance(records, list) and records else {}
    raw_value = record.get(field) if isinstance(record, dict) else None
    normalized = _extract_file_upload_candidate(raw_value)
    logger.info(
        "Zoho CRM file field payload | record_id=%s field=%s file_id=%s has_download_url=%s file_name=%s raw=%s",
        rid,
        field,
        normalized.get("file_id"),
        bool(normalized.get("download_url")),
        normalized.get("file_name"),
        str(raw_value)[:1200],
    )
    return normalized


async def download_file_upload_content(*, file_id: str | None, download_url: str | None) -> bytes:
    """
    Download Zoho File Upload bytes.
    Tries direct URL first (if provided), then known Zoho file endpoints by file_id.
    """
    token = await get_access_token()
    headers = _auth_header(token)
    base = settings.ZOHO_CRM_API_BASE.rstrip("/")
    candidates: list[tuple[str, str]] = []

    if (download_url or "").strip():
        candidates.append(("direct_download_url", (download_url or "").strip()))
    if (file_id or "").strip():
        fid = quote((file_id or "").strip(), safe="")
        candidates.append(("files_query", f"{base}/crm/v8/files?id={fid}"))
        candidates.append(("files_path", f"{base}/crm/v8/files/{fid}"))

    if not candidates:
        raise RuntimeError("No Zoho file identifier found (missing file_id/download_url).")

    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
        for label, url in candidates:
            try:
                logger.info("Zoho file download attempt | strategy=%s url=%s", label, url)
                resp = await client.get(url, headers=headers)
                content_type = (resp.headers.get("content-type") or "").lower()
                if resp.status_code >= 400:
                    logger.warning(
                        "Zoho file download rejected | strategy=%s status=%s body=%s",
                        label,
                        resp.status_code,
                        (resp.text or "")[:500],
                    )
                    continue
                if "application/json" in content_type and not resp.content.startswith(b"%PDF"):
                    logger.warning(
                        "Zoho file download returned JSON, not file bytes | strategy=%s body=%s",
                        label,
                        (resp.text or "")[:700],
                    )
                    continue
                if not resp.content:
                    logger.warning("Zoho file download empty response | strategy=%s", label)
                    continue
                logger.info(
                    "Zoho file download success | strategy=%s bytes=%s content_type=%s",
                    label,
                    len(resp.content),
                    resp.headers.get("content-type"),
                )
                return resp.content
            except Exception:
                logger.exception("Zoho file download exception | strategy=%s", label)

    raise RuntimeError("Unable to download Zoho file from any supported endpoint.")

"""
API key authentication for protected routes.

Accepts the secret via either:
- Header: ``X-API-Key: <API_SECRET_KEY>``
- Query: ``?x-api-key=<API_SECRET_KEY>`` (when the caller puts it on the webhook URL)

Bitrix-only (``verify_bitrix_api_key``) also accepts:
- JSON/form field: ``api_key`` or ``x-api-key`` in the POST body

All must match ``API_SECRET_KEY`` in environment.
"""
from __future__ import annotations

import json
from typing import Optional
from urllib.parse import parse_qs

from fastapi import Depends, Header, HTTPException, Request, status

from app.core.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)

_QUERY_KEY_NAMES = ("x-api-key", "api_key", "apikey")
_BODY_KEY_NAMES = ("x-api-key", "api_key", "apikey", "x_api_key")


def resolve_api_key(
    request: Request | None,
    *,
    header_key: str | None = None,
) -> str | None:
    """Header wins; then query string (x-api-key, api_key, apikey)."""
    if header_key is not None:
        hk = str(header_key).strip()
        if hk:
            return hk
    if request is not None:
        for name in _QUERY_KEY_NAMES:
            raw = request.query_params.get(name)
            if raw is not None and str(raw).strip():
                return str(raw).strip()
    return None


def extract_api_key_from_body(body_bytes: bytes, content_type: str | None) -> str | None:
    """Parse api_key from JSON or form body (Bitrix often cannot set query params on POST)."""
    if not body_bytes:
        return None
    ct = (content_type or "").lower()
    try:
        if "application/json" in ct:
            data = json.loads(body_bytes.decode("utf-8", errors="replace"))
            if isinstance(data, dict):
                for name in _BODY_KEY_NAMES:
                    v = data.get(name)
                    if v is not None and str(v).strip():
                        return str(v).strip()
        if "application/x-www-form-urlencoded" in ct or "multipart/form-data" in ct:
            raw = body_bytes.decode("utf-8", errors="replace")
            parsed = parse_qs(raw, keep_blank_values=True)
            for name in _BODY_KEY_NAMES:
                vals = parsed.get(name)
                if vals and str(vals[0]).strip():
                    return str(vals[0]).strip()
    except Exception:
        return None
    return None


def reinject_request_body(request: Request, body_bytes: bytes) -> None:
    """Allow route handlers to read the body after auth consumed it."""

    async def receive():
        return {"type": "http.request", "body": body_bytes, "more_body": False}

    request._receive = receive  # type: ignore[attr-defined]


def api_key_is_valid(key: str | None) -> bool:
    expected = (settings.API_SECRET_KEY or "").strip()
    if not expected or not key:
        return False
    return key.strip() == expected


def log_bitrix_incoming_request(request: Request, body_bytes: bytes) -> None:
    """Debug helper when BITRIX_LOG_INCOMING_REQUESTS=true (or auth failed)."""
    if not getattr(settings, "BITRIX_LOG_INCOMING_REQUESTS", False):
        return
    raw_body = body_bytes.decode("utf-8", errors="replace") if body_bytes else ""
    logger.info(
        "Bitrix incoming | method=%s url=%s query=%s content_type=%s body_preview=%s",
        request.method,
        str(request.url),
        dict(request.query_params),
        request.headers.get("content-type"),
        raw_body[:4000],
    )


def verify_api_key(
    request: Request,
    x_api_key: Optional[str] = Header(
        None,
        alias="X-API-Key",
        description="API secret (alternative: ?x-api-key= query parameter)",
    ),
) -> None:
    key = resolve_api_key(request, header_key=x_api_key)
    if not api_key_is_valid(key):
        logger.warning(
            "Rejected request: invalid or missing API key | path=%s query=%s",
            request.url.path,
            dict(request.query_params),
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=(
                "Invalid or missing API key. "
                "Send X-API-Key header or x-api-key query parameter matching API_SECRET_KEY."
            ),
        )


async def verify_bitrix_api_key(
    request: Request,
    x_api_key: Optional[str] = Header(
        None,
        alias="X-API-Key",
        description="API secret; or ?x-api-key= on URL; or api_key in JSON body",
    ),
) -> None:
    """
    Bitrix automations often POST without query string even when the UI shows a long URL.
    Also accept api_key inside the JSON/form body.
    """
    key = resolve_api_key(request, header_key=x_api_key)
    body_bytes = b""
    if not api_key_is_valid(key):
        body_bytes = await request.body()
        key = extract_api_key_from_body(body_bytes, request.headers.get("content-type"))
        reinject_request_body(request, body_bytes)
        log_bitrix_incoming_request(request, body_bytes)

    if not api_key_is_valid(key):
        logger.warning(
            "Bitrix auth failed | path=%s method=%s query=%s has_body=%s content_type=%s",
            request.url.path,
            request.method,
            dict(request.query_params),
            bool(body_bytes),
            request.headers.get("content-type"),
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=(
                "Invalid or missing API key for Bitrix. Use one of: "
                "(1) webhook URL ending with ?x-api-key=YOUR_API_SECRET_KEY, "
                "(2) JSON body field \"api_key\": \"YOUR_API_SECRET_KEY\", "
                "(3) header X-API-Key. "
                "Railway API_SECRET_KEY must match exactly."
            ),
        )


auth = Depends(verify_api_key)
bitrix_auth = Depends(verify_bitrix_api_key)

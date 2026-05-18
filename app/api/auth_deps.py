"""
API key and Bitrix outgoing-webhook authentication.

Bitrix automation (outgoing webhook) sends ``application/x-www-form-urlencoded`` with:
- ``auth[application_token]`` — validate against ``BITRIX_APPLICATION_TOKEN``
- ``data[FIELDS_AFTER][ID]`` — task id (handled in bitrix_task_parser)

Manual / test calls can still use ``X-API-Key``, ``?x-api-key=``, or ``api_key`` in JSON.
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
_BITRIX_TOKEN_FORM_KEYS = (
    "auth[application_token]",
    "auth.application_token",
)


def parse_urlencoded_form(body_bytes: bytes) -> dict[str, str]:
    if not body_bytes:
        return {}
    parsed = parse_qs(body_bytes.decode("utf-8", errors="replace"), keep_blank_values=True)
    return {k: (v[0] if isinstance(v, list) and v else "") for k, v in parsed.items()}


def resolve_api_key(
    request: Request | None,
    *,
    header_key: str | None = None,
) -> str | None:
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
            form = parse_urlencoded_form(body_bytes)
            for name in _BODY_KEY_NAMES:
                v = form.get(name)
                if v and str(v).strip():
                    return str(v).strip()
    except Exception:
        return None
    return None


def extract_bitrix_application_token(form_data: dict[str, str]) -> str | None:
    for key in _BITRIX_TOKEN_FORM_KEYS:
        v = form_data.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    return None


def bitrix_application_token_configured() -> bool:
    return bool((settings.BITRIX_APPLICATION_TOKEN or "").strip())


def bitrix_application_token_is_valid(token: str | None) -> bool:
    expected = (settings.BITRIX_APPLICATION_TOKEN or "").strip()
    if not expected or not token:
        return False
    return token.strip() == expected


def reinject_request_body(request: Request, body_bytes: bytes) -> None:
    async def receive():
        return {"type": "http.request", "body": body_bytes, "more_body": False}

    request._receive = receive  # type: ignore[attr-defined]


def api_key_is_valid(key: str | None) -> bool:
    expected = (settings.API_SECRET_KEY or "").strip()
    if not expected or not key:
        return False
    return key.strip() == expected


def log_bitrix_incoming_request(request: Request, body_bytes: bytes) -> None:
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
        description="Optional; Bitrix webhooks use auth[application_token] instead",
    ),
) -> None:
    """
  Authenticate Bitrix ``/bitrix/courses``:

  1. Bitrix outgoing webhook: ``auth[application_token]`` == ``BITRIX_APPLICATION_TOKEN``
  2. Otherwise: same as global API key (header / query / body api_key)
    """
    key = resolve_api_key(request, header_key=x_api_key)
    if api_key_is_valid(key):
        return

    body_bytes = await request.body()
    content_type = request.headers.get("content-type")
    reinject_request_body(request, body_bytes)
    log_bitrix_incoming_request(request, body_bytes)

    ct = (content_type or "").lower()
    if "application/x-www-form-urlencoded" in ct or "multipart/form-data" in ct:
        form_data = parse_urlencoded_form(body_bytes)
        app_token = extract_bitrix_application_token(form_data)
        if bitrix_application_token_is_valid(app_token):
            event = str(form_data.get("event") or "").strip()
            logger.info(
                "Bitrix webhook authenticated via application_token | event=%s",
                event or "-",
            )
            return

    key = extract_api_key_from_body(body_bytes, content_type)
    if api_key_is_valid(key):
        return

    logger.warning(
        "Bitrix auth failed | path=%s query=%s event_form=%s app_token_configured=%s",
        request.url.path,
        dict(request.query_params),
        bool("event=" in (body_bytes.decode(errors="ignore")[:200] if body_bytes else "")),
        bitrix_application_token_configured(),
    )
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=(
            "Unauthorized. For Bitrix outgoing webhooks set BITRIX_APPLICATION_TOKEN in Railway "
            "to the same value as auth[application_token] in the webhook payload. "
            "For manual calls use X-API-Key or ?x-api-key= matching API_SECRET_KEY."
        ),
    )


auth = Depends(verify_api_key)
bitrix_auth = Depends(verify_bitrix_api_key)

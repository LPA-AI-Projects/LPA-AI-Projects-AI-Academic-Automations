"""
API key authentication for protected routes.

Accepts the secret via either:
- Header: ``X-API-Key: <API_SECRET_KEY>``
- Query: ``?x-api-key=<API_SECRET_KEY>`` (recommended for Bitrix24 automations)

Both must match ``API_SECRET_KEY`` in environment (not a separate token system).
"""
from __future__ import annotations

from typing import Optional

from fastapi import Depends, Header, HTTPException, Request, status

from app.core.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)

_QUERY_KEY_NAMES = ("x-api-key", "api_key", "apikey")


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


def api_key_is_valid(key: str | None) -> bool:
    expected = (settings.API_SECRET_KEY or "").strip()
    if not expected or not key:
        return False
    return key.strip() == expected


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
        logger.warning("Rejected request: invalid or missing API key")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=(
                "Invalid or missing API key. "
                "Send X-API-Key header or x-api-key query parameter matching API_SECRET_KEY."
            ),
        )


auth = Depends(verify_api_key)

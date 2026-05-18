"""
Bitrix24 REST API via incoming webhook.

Docs: https://apidocs.bitrix24.com/local-integrations/local-webhooks.html

Set BITRIX_WEBHOOK_URL to the full webhook base, e.g.:
  https://your-domain.bitrix24.com/rest/1/your_secret_code/

Methods are called as POST {base}/crm.deal.update.json with JSON body.
"""
from __future__ import annotations

from typing import Any

import httpx

from app.core.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


class BitrixCrmNotConfigured(RuntimeError):
    """Incoming webhook URL missing."""


def bitrix_configured() -> bool:
    return bool((settings.BITRIX_WEBHOOK_URL or "").strip())


def _webhook_base() -> str:
    url = (settings.BITRIX_WEBHOOK_URL or "").strip().rstrip("/")
    if not url:
        raise BitrixCrmNotConfigured(
            "BITRIX_WEBHOOK_URL is not set. Create an incoming webhook in Bitrix24 "
            "(Applications → Developer resources → Incoming webhook)."
        )
    return url


def _update_method() -> str:
    entity = (settings.BITRIX_CRM_ENTITY or "deal").strip().lower()
    if entity == "lead":
        return "crm.lead.update"
    if entity in ("dynamic", "item", "spa"):
        return "crm.item.update"
    return "crm.deal.update"


def _build_update_payload(record_id: str, fields: dict[str, Any]) -> dict[str, Any]:
    """Map fields to the REST shape for deal/lead vs universal item."""
    rid = str(record_id or "").strip()
    if not rid:
        raise ValueError("bitrix_record_id is required.")
    try:
        numeric_id = int(rid)
    except ValueError:
        numeric_id = rid

    entity = (settings.BITRIX_CRM_ENTITY or "deal").strip().lower()
    if entity in ("dynamic", "item", "spa"):
        entity_type_id = int(settings.BITRIX_CRM_ENTITY_TYPE_ID or 2)
        return {
            "entityTypeId": entity_type_id,
            "id": numeric_id,
            "fields": fields,
        }
    return {"ID": numeric_id, "FIELDS": fields}


async def bitrix_call(method: str, params: dict[str, Any] | None = None) -> Any:
    """POST to incoming webhook REST method; returns ``result`` on success."""
    base = _webhook_base()
    method_name = (method or "").strip().strip("/")
    if method_name.endswith(".json"):
        method_name = method_name[:-5]
    url = f"{base}/{method_name}.json"
    body = params or {}
    logger.info("Bitrix REST call | method=%s", method_name)
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(url, json=body)
    raw = response.text or ""
    try:
        payload = response.json()
    except Exception:
        logger.error(
            "Bitrix REST non-JSON | method=%s status=%s body=%s",
            method_name,
            response.status_code,
            raw[:2000],
        )
        raise RuntimeError(
            f"Bitrix REST returned non-JSON (HTTP {response.status_code}). "
            "Check BITRIX_WEBHOOK_URL and portal HTTPS access."
        ) from None

    if not isinstance(payload, dict):
        raise RuntimeError(f"Bitrix REST unexpected response type: {type(payload)}")

    if payload.get("error"):
        err = payload.get("error")
        desc = str(payload.get("error_description") or "")
        logger.warning(
            "Bitrix REST error | method=%s error=%s description=%s",
            method_name,
            err,
            desc[:500],
        )
        raise RuntimeError(f"Bitrix REST error: {err} — {desc}")

    if response.status_code >= 400:
        raise RuntimeError(f"Bitrix REST HTTP {response.status_code}: {raw[:500]}")

    return payload.get("result")


async def update_outline_record_fields(
    *,
    bitrix_record_id: str,
    fields: dict[str, str],
) -> None:
    """
    Update CRM fields on the outline record (deal/lead/spa item).

    Field API names are Bitrix keys, e.g. ``UF_CRM_1725365197310`` or ``STAGE_ID``.
    """
    if not bitrix_configured():
        logger.info("Bitrix record update skipped: BITRIX_WEBHOOK_URL not set.")
        return

    cleaned: dict[str, str] = {
        str(k).strip(): str(v).strip()
        for k, v in (fields or {}).items()
        if str(k).strip() and str(v).strip()
    }
    if not cleaned:
        return

    method = _update_method()
    body = _build_update_payload(bitrix_record_id, cleaned)
    logger.info(
        "Bitrix outline record update | method=%s record_id=%s keys=%s",
        method,
        bitrix_record_id,
        sorted(cleaned.keys()),
    )
    await bitrix_call(method, body)
    logger.info("Bitrix outline record update success | record_id=%s", bitrix_record_id)


async def maybe_attach_course_pdf(
    *,
    bitrix_record_id: str,
    pdf_url: str | None,
    course_name_for_title: str,
) -> None:
    """
    Write the public PDF URL into a string custom field on the CRM record.
    """
    if not pdf_url or not settings.BITRIX_ATTACH_PDF_TO_CRM:
        return
    field = (settings.BITRIX_OUTLINE_PDF_FIELD or "").strip()
    if not field:
        logger.info(
            "Bitrix PDF attach skipped: set BITRIX_OUTLINE_PDF_FIELD (UF_CRM_...) "
            "and BITRIX_ATTACH_PDF_TO_CRM=true"
        )
        return
    if not bitrix_configured():
        logger.info("Bitrix PDF attach skipped: BITRIX_WEBHOOK_URL not configured.")
        return

    try:
        await update_outline_record_fields(
            bitrix_record_id=bitrix_record_id,
            fields={field: pdf_url.strip()},
        )
        logger.info(
            "Bitrix: PDF URL written to %s | record_id=%s title=%s",
            field,
            bitrix_record_id,
            course_name_for_title,
        )
    except Exception:
        logger.exception(
            "Bitrix: PDF URL write failed | record_id=%s field=%s",
            bitrix_record_id,
            field,
        )

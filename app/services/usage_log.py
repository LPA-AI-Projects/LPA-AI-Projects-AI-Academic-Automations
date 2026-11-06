"""
Persist per-call Anthropic token usage so the cost dashboard can show real spend per request.

Recording is best effort: it runs in the background and any failure is logged and swallowed, so it can
never slow down or break outline generation or refinement.
"""
from __future__ import annotations

import asyncio
import uuid
from typing import Any

from app.utils.logger import get_logger

logger = get_logger(__name__)

# Keep references so fire-and-forget tasks are not garbage collected before they finish.
_pending: set[asyncio.Task[None]] = set()


def _as_uuid(value: Any) -> uuid.UUID | None:
    if value is None or value == "":
        return None
    if isinstance(value, uuid.UUID):
        return value
    try:
        return uuid.UUID(str(value))
    except ValueError:
        return None


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def usage_row_from_response(
    context: dict[str, Any],
    *,
    label: str,
    model: str,
    data: dict[str, Any],
    structured: bool,
) -> dict[str, Any]:
    """Column values for one LlmUsage row from an Anthropic /v1/messages response body."""
    usage = data.get("usage") if isinstance(data, dict) else None
    usage = usage if isinstance(usage, dict) else {}
    tool_use = usage.get("server_tool_use")
    tool_use = tool_use if isinstance(tool_use, dict) else {}
    return {
        "kind": str(context.get("kind") or "outline"),
        "job_id": _as_uuid(context.get("job_id")),
        "course_id": _as_uuid(context.get("course_id")),
        "zoho_record_id": (str(context["zoho_record_id"]) if context.get("zoho_record_id") else None),
        "label": label or "unlabeled",
        "model": model,
        "input_tokens": _int(usage.get("input_tokens")),
        "output_tokens": _int(usage.get("output_tokens")),
        "cache_write_tokens": _int(usage.get("cache_creation_input_tokens")),
        "cache_read_tokens": _int(usage.get("cache_read_input_tokens")),
        "web_searches": _int(tool_use.get("web_search_requests")),
        "stop_reason": (str(data.get("stop_reason")) if isinstance(data, dict) and data.get("stop_reason") else None),
        "structured": bool(structured),
    }


async def _insert(row: dict[str, Any]) -> None:
    from app.core.database import AsyncSessionLocal
    from app.models.usage import LlmUsage

    try:
        async with AsyncSessionLocal() as db:
            db.add(LlmUsage(**row))
            await db.commit()
    except Exception:
        logger.exception("Could not record LLM usage | label=%s kind=%s", row.get("label"), row.get("kind"))


def record_usage_in_background(
    context: dict[str, Any] | None,
    *,
    label: str,
    model: str,
    data: dict[str, Any],
    structured: bool,
) -> None:
    """Schedule a usage insert. Does nothing without a usage context or outside a running event loop."""
    if not context:
        return
    try:
        row = usage_row_from_response(context, label=label, model=model, data=data, structured=structured)
        task = asyncio.get_running_loop().create_task(_insert(row))
    except Exception:
        logger.exception("Could not schedule LLM usage record | label=%s", label)
        return
    _pending.add(task)
    task.add_done_callback(_pending.discard)

"""
Bitrix-only outline add-on: AI Integration, Skills Achieved, Industry Simulation, Automation Sandbox.

Present only when the task field AI integration is Yes. No or blank must not
change the brochure.
"""
from __future__ import annotations

import json
import re
from typing import Any

from app.schemas.outline_payload import CourseOutlinePayload, IndustrySimulation

_YES = frozenset({"yes", "y", "true", "1"})
_REMOVE_AI = re.compile(
    r"\b(?:remove|drop|delete|without|no)\b.{0,40}\bai integration\b"
    r"|\bai integration\b.{0,20}\b(?:off|no|remove)\b",
    re.IGNORECASE | re.DOTALL,
)


def ai_integration_enabled(input_data: dict[str, Any] | None) -> bool:
    if not isinstance(input_data, dict):
        return False
    raw = str(input_data.get("ai_integration") or "").strip().lower()
    return raw in _YES


def context_requests_ai_integration(context_text: str) -> bool:
    try:
        data = json.loads(context_text or "")
    except Exception:
        return False
    return ai_integration_enabled(data if isinstance(data, dict) else None)


def feedback_removes_ai(feedback: str | None) -> bool:
    return bool(_REMOVE_AI.search(str(feedback or "")))


def outline_text_has_ai_addons(text: str | None) -> bool:
    try:
        data = json.loads(text or "")
    except Exception:
        return False
    if not isinstance(data, dict):
        return False
    for key in ("industry_simulation", "automation_sandbox"):
        box = data.get(key)
        if isinstance(box, dict) and any(str(p).strip() for p in (box.get("paragraphs") or [])):
            return True
    for module in data.get("modules") or []:
        if not isinstance(module, dict):
            continue
        if any(str(x).strip() for x in (module.get("ai_integration") or [])) or any(
            str(x).strip() for x in (module.get("skills_achieved") or [])
        ):
            return True
    return False


def dump_outline_dict(payload: CourseOutlinePayload) -> dict[str, Any]:
    """
    Persist outline JSON.

    Empty AI add-on keys are omitted so Zoho and Bitrix-No outlines keep the previous shape.
    """
    data = payload.model_dump()
    for module in data.get("modules") or []:
        if not isinstance(module, dict):
            continue
        if not any(str(item).strip() for item in (module.get("ai_integration") or [])):
            module.pop("ai_integration", None)
        if not any(str(item).strip() for item in (module.get("skills_achieved") or [])):
            module.pop("skills_achieved", None)
    for key in ("industry_simulation", "automation_sandbox"):
        box = data.get(key)
        has_box = isinstance(box, dict) and any(
            str(paragraph).strip() for paragraph in (box.get("paragraphs") or [])
        )
        if not has_box:
            data.pop(key, None)
    return data


def clear_ai_outline_addons(payload: CourseOutlinePayload | None) -> None:
    if payload is None:
        return
    payload.industry_simulation = None
    payload.automation_sandbox = None
    for module in payload.modules or []:
        module.ai_integration = []
        module.skills_achieved = []


def _clean_lines(raw: Any) -> list[str]:
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, list):
        return []
    return [str(x).strip() for x in raw if str(x).strip()]


def _restore_box(payload: CourseOutlinePayload, attr: str, raw: Any, default_title: str) -> None:
    current = getattr(payload, attr)
    current_paras: list[str] = []
    if current is not None:
        current_paras = _clean_lines(current.paragraphs)
    if isinstance(raw, dict) and not current_paras:
        paras = _clean_lines(raw.get("paragraphs"))
        if paras:
            setattr(
                payload,
                attr,
                IndustrySimulation(
                    title=str(raw.get("title") or default_title).strip() or default_title,
                    subtitle=str(raw.get("subtitle") or "").strip(),
                    paragraphs=paras,
                ),
            )


def restore_ai_addons_if_dropped(
    previous_text: str,
    payload: CourseOutlinePayload | None,
    feedback: str | None,
) -> None:
    """
    Keep AI add-on copy on refine when the previous outline already had it.

    New modules are left to the model. Feedback that asks to remove AI integration clears it.
    """
    if payload is None:
        return
    if feedback_removes_ai(feedback):
        clear_ai_outline_addons(payload)
        return
    if not outline_text_has_ai_addons(previous_text):
        clear_ai_outline_addons(payload)
        return
    try:
        prev = json.loads(previous_text)
    except Exception:
        return
    if not isinstance(prev, dict):
        return

    prev_modules = prev.get("modules") or []
    for index, module in enumerate(payload.modules or []):
        if index >= len(prev_modules) or not isinstance(prev_modules[index], dict):
            continue
        src = prev_modules[index]
        if not module.ai_integration:
            module.ai_integration = _clean_lines(src.get("ai_integration"))
        if not module.skills_achieved:
            module.skills_achieved = _clean_lines(src.get("skills_achieved"))

    _restore_box(
        payload,
        "industry_simulation",
        prev.get("industry_simulation"),
        "Industry Simulation",
    )
    _restore_box(
        payload,
        "automation_sandbox",
        prev.get("automation_sandbox"),
        "Automation Sandbox",
    )

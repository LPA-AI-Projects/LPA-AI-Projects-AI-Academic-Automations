"""
**Module brief** — one LLM call per module: topics, exercises, activities, LP focus, card count.

No slide-title list; content is generated as a single Markdown module body downstream.
Invoked from ``slides_graph`` (brief node) via ``plan_module_brief``.
"""
from __future__ import annotations

import json
from typing import Any

from app.services.claude import ClaudeService
from app.utils.logger import get_logger

logger = get_logger(__name__)


def _module_brief_system_prompt(instructor_ppt_priority: str) -> str:
    if instructor_ppt_priority == "primary":
        priority_rules = """
- Source priority: (1) Course outline for this module (2) Instructor PPT for detail (3) LP/AP
"""
    else:
        priority_rules = """
- Source priority: (1) Course outline for this module (2) LP/AP (3) Instructor PPT as supplement
"""
    return f"""
You are a senior instructional designer.

Return ONLY valid JSON with exactly this shape (no other top-level keys, no "slides" array):
{{
  "module_plan": {{
    "module_name": "string (short display name)",
    "topics": "string — topics covered in this module, comma or newline separated",
    "exercises": "string — exercises from outline/LP that must be addressable in training",
    "activities": "string — activities / timing from LP/AP for this module",
    "lesson_plan_focus": "string — what the lesson plan emphasizes here",
    "no_of_slides": 15
  }}
}}

Rules:
{priority_rules.strip()}
- ``no_of_slides``: integer 10–25, target deck size for Gamma for this module (approximate card count).
- Be faithful to the supplied outline slice and LP/AP; do not invent major topics not in sources.
- ``exercises`` and ``activities`` must reflect what is in the sources (or say "None specified" if absent).
"""


def _safe_json(text: str) -> dict[str, Any]:
    raw = (text or "").strip()
    first = raw.find("{")
    last = raw.rfind("}")
    if first >= 0 and last > first:
        raw = raw[first : last + 1]
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("Brief planner returned non-object JSON.")
    mp = data.get("module_plan")
    if not isinstance(mp, dict):
        raise ValueError("Brief planner JSON missing module_plan object.")
    return data


async def plan_module_brief(
    *,
    module_name: str,
    outline: str,
    lesson: str | None,
    instructor: str | None,
    course_map: str,
    instructor_ppt_priority: str = "supplement",
    model: str | None = None,
    min_slides: int = 10,
    max_slides: int = 25,
) -> dict[str, Any]:
    """Returns ``{{ "module_plan": {{...}} }}`` — no slide list."""
    ai = ClaudeService()
    priority = instructor_ppt_priority if instructor_ppt_priority in ("primary", "supplement") else "supplement"
    user_prompt = (
        f"MODULE_DISPLAY_NAME: {module_name}\n\n"
        "COURSE_MAP (global context; stay consistent with it):\n"
        f"{(course_map or '')[:40_000]}\n\n"
        "OUTLINE — THIS MODULE ONLY (PDF text slice):\n"
        f"{outline}\n\n"
        "LESSON PLAN + ACTIVITY PLAN (full text; pick what applies to this module):\n"
        f"{lesson or ''}\n\n"
        "INSTRUCTOR PPT (text for this module, optional):\n"
        f"{instructor or ''}\n"
    )
    raw = await ai.generate_text_completion(
        system_prompt=_module_brief_system_prompt(priority),
        user_prompt=user_prompt,
        timeout_s=120.0,
        model_override=model,
    )
    data = _safe_json(raw)
    mp = data.get("module_plan") if isinstance(data.get("module_plan"), dict) else {}
    # Clamp no_of_slides
    try:
        n = int(mp.get("no_of_slides") or min_slides)
    except (TypeError, ValueError):
        n = min_slides
    n = max(min_slides, min(max_slides, n))
    mp["no_of_slides"] = n
    mp.setdefault("module_name", module_name)
    mp.setdefault("topics", "")
    mp.setdefault("exercises", "")
    mp.setdefault("activities", "")
    mp.setdefault("lesson_plan_focus", "")
    return {"module_plan": mp}


async def plan_slides(
    *,
    outline: str,
    lesson: str | None,
    activity: str | None,
    instructor: str | None,
    instructor_ppt_priority: str = "supplement",
    model: str | None = None,
) -> dict[str, Any]:
    """
    Legacy entry point: redirects to a minimal ``module_plan`` without per-slide titles.

    ``activity`` is folded into ``lesson`` for the brief call.
    """
    merged_lesson = "\n\n".join(
        x for x in (lesson or "", activity or "") if str(x).strip()
    )
    return await plan_module_brief(
        module_name="Module",
        outline=outline,
        lesson=merged_lesson or None,
        instructor=instructor,
        course_map="",
        instructor_ppt_priority=instructor_ppt_priority,
        model=model,
    )

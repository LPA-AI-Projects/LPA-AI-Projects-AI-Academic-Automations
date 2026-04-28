"""
**Slide planner** — given outline/lesson/instructor text, returns a list of planned slides
(titles + types). Invoked from ``slides_graph._planner_node`` via ``plan_slides``.
"""
from __future__ import annotations

import json
from typing import Any

from app.services.claude import ClaudeService
from app.utils.logger import get_logger

logger = get_logger(__name__)


def _slide_plan_system_prompt(instructor_ppt_priority: str) -> str:
    if instructor_ppt_priority == "primary":
        priority_rules = """
- Primary source priority:
  1) Course Outline (mandatory: module scope, topics, and sequencing)
  2) Instructor PPT (primary for factual detail, examples, definitions, and slide-relevant wording)
  3) Lesson Plan / Activity Plan (secondary)
"""
    else:
        priority_rules = """
- Primary source priority:
  1) Course Outline (mandatory structure)
  2) Lesson Plan / Activity Plan
  3) Instructor PPT (supplement only)
"""
    return f"""
You are a senior instructional designer and slide strategist.
Your job: produce a slide plan (titles + types) from the provided training materials.

Return ONLY valid JSON with this shape:
{{
  "slides": [
    {{"title": "...", "type": "content"}},
    {{"title": "...", "type": "activity"}}
  ]
}}

Rules:
{priority_rules.strip()}
- Generate 10-20 slides per module
- Hard maximum: 20
- Minimum target: 10 (if content allows)
- Include exactly 1 summary slide
- Use activity slides only when LP/AP context exists
- Avoid duplicate or near-duplicate slides
- title must be short and specific (max 10 words)
- type must be one of: "content", "activity", "summary"
"""


def _safe_json(text: str) -> dict[str, Any]:
    raw = (text or "").strip()
    first = raw.find("{")
    last = raw.rfind("}")
    if first >= 0 and last > first:
        raw = raw[first : last + 1]
    data = json.loads(raw)
    if not isinstance(data, dict) or "slides" not in data:
        raise ValueError("Planner returned invalid JSON structure.")
    return data


async def plan_slides(
    *,
    outline: str,
    lesson: str | None,
    activity: str | None,
    instructor: str | None,
    instructor_ppt_priority: str = "supplement",
    model: str | None = None,
) -> dict[str, Any]:
    ai = ClaudeService()
    priority = instructor_ppt_priority if instructor_ppt_priority in ("primary", "supplement") else "supplement"
    user_prompt = (
        "OUTLINE (PDF text):\n"
        f"{outline}\n\n"
        "LESSON PLAN + ACTIVITY PLAN (PDF text, optional):\n"
        f"{lesson or ''}\n\n"
        "ACTIVITY PLAN (if separate, optional):\n"
        f"{activity or ''}\n\n"
        "INSTRUCTOR PPT (text extracted; may be scoped to this module, optional):\n"
        f"{instructor or ''}\n"
    )

    raw = await ai.generate_text_completion(
        system_prompt=_slide_plan_system_prompt(priority),
        user_prompt=user_prompt,
        timeout_s=120.0,
        model_override=model,
    )
    return _safe_json(raw)


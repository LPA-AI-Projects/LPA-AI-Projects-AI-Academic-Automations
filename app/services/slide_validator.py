from __future__ import annotations

import json
from typing import Any

from app.services.claude import ClaudeService
from app.utils.logger import get_logger

logger = get_logger(__name__)

SLIDE_VALIDATOR_SYSTEM_PROMPT = """
You are a slide quality validator.

Check:
- Slide count is between 10 and 20
- Content follows Course Outline structure
- LP/AP activities included when present
- Instructor PPT used only as supplement
- No duplicate slides
- Logical progression

Return ONLY valid JSON:
{
  "approved": true,
  "issues": [],
  "fix_instructions": ""
}
"""


def _safe_json(text: str) -> dict[str, Any]:
    raw = (text or "").strip()
    first = raw.find("{")
    last = raw.rfind("}")
    if first >= 0 and last > first:
        raw = raw[first : last + 1]
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("Validator returned non-object JSON.")
    approved = bool(data.get("approved"))
    issues = data.get("issues")
    if not isinstance(issues, list):
        issues = []
    fix = data.get("fix_instructions")
    if not isinstance(fix, str):
        fix = ""
    return {"approved": approved, "issues": [str(i) for i in issues if str(i).strip()], "fix_instructions": fix}


async def validate_slides_ai(
    *,
    planned_slides: list[dict[str, Any]],
    generated_slides: list[dict[str, Any]],
    has_lesson_plan: bool,
    model: str | None = None,
) -> dict[str, Any]:
    ai = ClaudeService()
    user_prompt = (
        f"has_lesson_plan={has_lesson_plan}\n\n"
        f"PLANNED SLIDES:\n{json.dumps(planned_slides, ensure_ascii=False)[:120000]}\n\n"
        f"GENERATED SLIDES:\n{json.dumps(generated_slides, ensure_ascii=False)[:120000]}\n"
    )
    raw = await ai.generate_text_completion(
        system_prompt=SLIDE_VALIDATOR_SYSTEM_PROMPT,
        user_prompt=user_prompt,
        timeout_s=120.0,
        model_override=model,
    )
    return _safe_json(raw)


def validate_slides(
    *,
    planned_slides: list[dict[str, Any]],
    generated_slides: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Lightweight validation/correction:
    - drop empty slides
    - ensure bullets exist
    - ensure activity slides have an "Activity" cue in notes if missing
    - ensure count matches planned length (trim or pad with placeholders)
    """
    cleaned: list[dict[str, Any]] = []

    for s in generated_slides:
        title = str(s.get("title") or "").strip()
        bullets = s.get("bullets")
        if not title:
            continue
        if not isinstance(bullets, list) or not [b for b in bullets if str(b).strip()]:
            s["bullets"] = ["(content to be filled)"]
        if "notes" not in s or not isinstance(s.get("notes"), str):
            s["notes"] = ""
        if "visual" not in s or not isinstance(s.get("visual"), str):
            s["visual"] = ""
        cleaned.append(s)

    # Activity slide checks
    planned_activity = sum(1 for p in planned_slides if str(p.get("type", "")).lower() == "activity")
    generated_activity = 0
    for idx, plan in enumerate(planned_slides[: len(cleaned)]):
        if str(plan.get("type", "")).lower() == "activity":
            generated_activity += 1
            if idx < len(cleaned):
                notes = str(cleaned[idx].get("notes") or "")
                if "activity" not in notes.lower():
                    cleaned[idx]["notes"] = (notes + "\n\nActivity: Facilitate participant exercise.").strip()

    if planned_activity and generated_activity == 0:
        logger.warning("Validator: planned activity slides exist but none generated as activity-style notes.")

    # Count mismatch handling
    if len(cleaned) > len(planned_slides):
        cleaned = cleaned[: len(planned_slides)]
    elif len(cleaned) < len(planned_slides):
        missing = len(planned_slides) - len(cleaned)
        for i in range(missing):
            cleaned.append(
                {
                    "title": f"Placeholder slide {len(cleaned) + 1}",
                    "bullets": ["(content to be filled)"],
                    "notes": "",
                    "visual": "",
                }
            )

    return cleaned


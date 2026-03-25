from __future__ import annotations

from typing import Any

from app.utils.logger import get_logger

logger = get_logger(__name__)


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


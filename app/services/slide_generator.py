from __future__ import annotations

import json
from typing import Any

from app.services.claude import ClaudeService
from app.utils.logger import get_logger

logger = get_logger(__name__)


SLIDE_GENERATION_SYSTEM_PROMPT = """
You are an expert slide writer.
Generate ONE slide worth of content as JSON only.

Return ONLY valid JSON with this shape:
{
  "title": "...",
  "bullets": ["...", "..."],
  "notes": "...",
  "visual": "..."
}

Rules:
- title must match the requested slide title closely
- bullets: 3-6 bullets for content slides; 2-5 bullets for activity slides
- notes: short instructor notes (1-4 sentences)
- visual: describe a suggested visual (icon/chart/diagram/photo), not a URL
"""


def _safe_slide_json(text: str) -> dict[str, Any]:
    raw = (text or "").strip()
    first = raw.find("{")
    last = raw.rfind("}")
    if first >= 0 and last > first:
        raw = raw[first : last + 1]
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("Slide generator returned non-object JSON.")
    if not isinstance(data.get("title"), str):
        raise ValueError("Slide generator JSON missing title.")
    if not isinstance(data.get("bullets"), list):
        raise ValueError("Slide generator JSON missing bullets.")
    return data


async def generate_slide(
    *,
    slide: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    title = str(slide.get("title") or "").strip()
    slide_type = str(slide.get("type") or "content").strip().lower()

    ai = ClaudeService()
    user_prompt = (
        "SLIDE REQUEST:\n"
        f"- title: {title}\n"
        f"- type: {slide_type}\n\n"
        "GLOBAL CONTEXT:\n"
        f"{json.dumps(context, ensure_ascii=False)[:150000]}\n"
    )
    raw = await ai._call_messages_api(  # type: ignore[attr-defined]
        system_prompt=SLIDE_GENERATION_SYSTEM_PROMPT,
        user_prompt=user_prompt,
        timeout_s=120.0,
        max_attempts=3,
    )
    return _safe_slide_json(raw)


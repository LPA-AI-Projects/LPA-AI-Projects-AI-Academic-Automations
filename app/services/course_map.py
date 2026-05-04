"""
One-shot course map for slides jobs: short global context reused per module.

Avoids sending full outline + full LP/AP into every module call.
"""
from __future__ import annotations

from app.services.claude import ClaudeService
from app.utils.logger import get_logger

logger = get_logger(__name__)

COURSE_MAP_SYSTEM_PROMPT = """You write a compact course map for instructional designers (plain text only, no JSON).

Keep it under ~500 words. Include:
- Working title / topic and inferred level if obvious
- Audience and delivery context if inferable from sources
- Numbered module list: for each module from the outline, one line (name + core focus)
- LP/AP: bullet list of major activities or timing themes (only if present in source)
- Terminology or standards to keep consistent across modules

Do not invent modules or topics not supported by the sources."""


async def build_course_map(
    *,
    outline_text: str,
    lesson_text: str | None,
    course_name: str,
    model: str | None = None,
    timeout_s: float = 90.0,
) -> str:
    """Single LLM call after extraction; result is passed into each module pipeline."""
    ai = ClaudeService()
    user = (
        f"COURSE_NAME: {course_name or 'Training'}\n\n"
        "OUTLINE (full text excerpt):\n"
        f"{(outline_text or '')[:120_000]}\n\n"
        "LESSON PLAN + ACTIVITY PLAN (optional):\n"
        f"{(lesson_text or '')[:120_000]}\n"
    )
    raw = await ai.generate_text_completion(
        system_prompt=COURSE_MAP_SYSTEM_PROMPT,
        user_prompt=user,
        timeout_s=timeout_s,
        model_override=model,
    )
    out = (raw or "").strip()
    if not out:
        return ""
    return out[:25_000]

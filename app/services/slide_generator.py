"""
Module body generation: one Markdown document per module (no JSON slide array).
Gamma receives this as plain text after validation.
"""
from __future__ import annotations

import json
from typing import Any

from app.services.claude import ClaudeService
from app.utils.logger import get_logger

logger = get_logger(__name__)

MODULE_BODY_SYSTEM_PROMPT = """You are an expert instructional designer and curriculum writer.
Produce ONE continuous module document in Markdown. Do NOT output JSON.

This document will be used as the sole text source for an automated presentation generator
(Gamma), which will decide slide breaks and layout.

Rules:
- Cover every topic, exercise, and activity implied by MODULE_PLAN and the sources.
- Use ## and ### headings to structure sections; use bullets for lists; keep wording concise (deck-friendly).
- Optional short **Facilitator notes** under sections where helpful.
- Optional **Suggested visual** lines: name diagram/chart type and labels (no URLs).
- Stay factually aligned with the Course Outline and LP/AP; do not invent standards/versions.
- Output only the module document — no preamble or closing commentary."""


def _generator_source_priority_block(instructor_ppt_priority: str) -> str:
    if instructor_ppt_priority == "primary":
        return (
            "SOURCE PRIORITY:\n"
            "- course_outline defines module scope and sequencing (mandatory)\n"
            "- instructor_ppt is the primary source for facts and examples when relevant\n"
            "- lesson_plan_and_activity_plan is secondary\n\n"
        )
    return (
        "SOURCE PRIORITY:\n"
        "- course_outline is mandatory primary source\n"
        "- lesson_plan_and_activity_plan is secondary\n"
        "- instructor_ppt is supplement only\n\n"
    )


async def generate_module_body_text(
    *,
    module_plan: dict[str, Any],
    context: dict[str, Any],
    instructor_ppt_priority: str = "supplement",
    model: str | None = None,
    fix_instructions: str | None = None,
) -> str:
    """Single model call: full module Markdown body."""
    priority = instructor_ppt_priority if instructor_ppt_priority in ("primary", "supplement") else "supplement"
    ai = ClaudeService()
    user_prompt = (
        "MODULE_PLAN (authoritative structure for this module):\n"
        f"{json.dumps(module_plan, ensure_ascii=False)[:50_000]}\n\n"
        f"{_generator_source_priority_block(priority)}"
        "CONTEXT (course map + scoped sources):\n"
        f"{json.dumps(context, ensure_ascii=False)[:150_000]}\n"
    )
    if (fix_instructions or "").strip():
        user_prompt += f"\nREVISE PER VALIDATOR:\n{str(fix_instructions).strip()}\n"
    raw = await ai.generate_text_completion(
        system_prompt=MODULE_BODY_SYSTEM_PROMPT,
        user_prompt=user_prompt,
        timeout_s=180.0,
        model_override=model,
    )
    body = (raw or "").strip()
    if not body:
        logger.warning("Module body generation returned empty; using minimal fallback")
        return f"## Module content\n\n{_generator_source_priority_block(priority)}*(Regenerate: model returned empty text.)*\n"
    return body

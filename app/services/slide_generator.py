"""
**Per-slide content generation** — for each planned slide, returns JSON
(title, bullets, notes, visual). Invoked from ``slides_graph._generator_node`` via ``generate_slide``.
"""
from __future__ import annotations

import json
import re
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
- visual: required, 1-2 sentences, not a URL. Name a concrete graphic to render on the slide:
  - For frameworks, comparisons, or categorization: say the diagram type (e.g. 2x2 matrix, Venn, pyramid,
    swimlane, cycle/steps, before/after).
  - For processes: flowchart, numbered path, or decision tree.
  - For data: simple bar/line/pie (describe what is compared), or icon row with labels.
  - For people/roles: personas or org-style icons, not stock photos of faces unless essential.
  - Section dividers: minimal (band, large numeral, or single hero shape) — still describe it.
  Avoid vague "nice image"; always specify diagram/infographic/illustration style.
- In JSON string values, never place a raw double-quote (") in the middle of a string — it breaks JSON.
  Rephrase (use single quotes in English) or use \\" for an internal double-quote.
"""


STRICT_JSON_REPAIR_SYSTEM = """You convert model output into valid JSON only.
Output a single JSON object, no markdown, no code fences, no commentary.
Schema: {"title": string, "bullets": [string, ...], "notes": string, "visual": string}
Rules: every string must be valid JSON (escape " as \\" if needed; prefer rephrasing to avoid inner quotes)."""


def _strip_markdown_fences(s: str) -> str:
    t = (s or "").strip()
    if t.startswith("```"):
        t = re.sub(r"^```[a-zA-Z0-9]*\s*\n?", "", t)
        t = re.sub(r"\n?```\s*$", "", t, flags=re.DOTALL)
    return t.strip()


def _fix_trailing_commas_in_json(s: str) -> str:
    return re.sub(r",(\s*[\}\]])", r"\1", s)


def _normalize_for_json_loads(blob: str) -> str:
    t = _strip_markdown_fences(blob)
    first, last = t.find("{"), t.rfind("}")
    if first >= 0 and last > first:
        t = t[first : last + 1]
    t = t.replace("\u201c", '"').replace("\u201d", '"')
    t = _fix_trailing_commas_in_json(t)
    return t


def _safe_slide_json(text: str) -> dict[str, Any]:
    t = _normalize_for_json_loads(text)
    data = json.loads(t)
    if not isinstance(data, dict):
        raise ValueError("Slide generator returned non-object JSON.")
    if not isinstance(data.get("title"), str):
        raise ValueError("Slide generator JSON missing title.")
    if not isinstance(data.get("bullets"), list):
        raise ValueError("Slide generator JSON missing bullets.")
    return data


def _fallback_slide_dict(expected_title: str) -> dict[str, Any]:
    """Last resort so a whole module does not fail when the model will not emit valid JSON."""
    return {
        "title": expected_title,
        "bullets": [
            "Review this slide in the final deck and tighten wording to match the module outcomes.",
        ],
        "notes": "Auto-fallback: model returned invalid JSON for this slide; replace with content from the outline if needed.",
        "visual": "Simple labeled diagram or icon row matching the slide title; adjust after export.",
    }


def _generator_source_priority_block(instructor_ppt_priority: str) -> str:
    if instructor_ppt_priority == "primary":
        return (
            "SOURCE PRIORITY:\n"
            "- course_outline defines module scope and sequencing (mandatory)\n"
            "- instructor_ppt is the primary source for facts, examples, and phrasing when relevant\n"
            "- lesson_plan_and_activity_plan is secondary\n\n"
        )
    return (
        "SOURCE PRIORITY:\n"
        "- course_outline is mandatory primary source\n"
        "- lesson_plan_and_activity_plan is secondary source\n"
        "- instructor_ppt is supplement only\n\n"
    )


async def generate_slide(
    *,
    slide: dict[str, Any],
    context: dict[str, Any],
    instructor_ppt_priority: str = "supplement",
    model: str | None = None,
    fix_instructions: str | None = None,
) -> dict[str, Any]:
    title = str(slide.get("title") or "").strip()
    slide_type = str(slide.get("type") or "content").strip().lower()
    priority = instructor_ppt_priority if instructor_ppt_priority in ("primary", "supplement") else "supplement"

    ai = ClaudeService()
    user_prompt = (
        "SLIDE REQUEST:\n"
        f"- title: {title}\n"
        f"- type: {slide_type}\n\n"
        f"{_generator_source_priority_block(priority)}"
        "GLOBAL CONTEXT:\n"
        f"{json.dumps(context, ensure_ascii=False)[:150000]}\n"
    )
    if (fix_instructions or "").strip():
        user_prompt += f"\nVALIDATOR FIX INSTRUCTIONS:\n{str(fix_instructions).strip()}\n"
    raw = await ai.generate_text_completion(
        system_prompt=SLIDE_GENERATION_SYSTEM_PROMPT,
        user_prompt=user_prompt,
        timeout_s=120.0,
        model_override=model,
    )
    try:
        return _safe_slide_json(raw)
    except (json.JSONDecodeError, ValueError) as exc:
        logger.warning(
            "Slide JSON parse failed; retrying once | title=%s err=%s",
            title,
            exc,
        )
        repair_prompt = (
            user_prompt
            + "\n\nYour previous reply was not valid JSON. "
            "Return ONLY one JSON object with keys title, bullets, notes, visual. No markdown fences. "
            "String values must not contain unescaped double quotes; rephrase or use JSON-escaped quotes where required."
        )
        raw2 = await ai.generate_text_completion(
            system_prompt=SLIDE_GENERATION_SYSTEM_PROMPT,
            user_prompt=repair_prompt,
            timeout_s=120.0,
            model_override=model,
        )
        try:
            return _safe_slide_json(raw2)
        except (json.JSONDecodeError, ValueError) as exc2:
            logger.warning(
                "Slide JSON still invalid after one retry; strict repair or fallback | title=%s err=%s",
                title,
                exc2,
            )
            repair2 = (
                f"The slide title must be: {json.dumps(title)}\n\n"
                f"Invalid output (fix into valid JSON only, same meaning):\n{raw2[:12000]}\n"
            )
            raw3 = await ai.generate_text_completion(
                system_prompt=STRICT_JSON_REPAIR_SYSTEM,
                user_prompt=repair2,
                timeout_s=90.0,
                model_override=model,
            )
            try:
                return _safe_slide_json(raw3)
            except (json.JSONDecodeError, ValueError) as exc3:
                logger.error(
                    "Slide JSON unrecoverable; using placeholder slide | title=%s err=%s",
                    title,
                    exc3,
                )
                return _fallback_slide_dict(title)


"""
**Content generation** — per-slide JSON (``generate_slide``) or one Markdown module body
(``generate_module_body_text``) for the text-first pipeline.
"""
from __future__ import annotations

import json
import re
from typing import Any

from app.services.claude import ClaudeService
from app.utils.logger import get_logger

logger = get_logger(__name__)

MODULE_BODY_SYSTEM_PROMPT = """You are an expert facilitator and instructional designer.

Output ONE continuous Markdown document for a single training module (plain Markdown only — no JSON, no code fences wrapping the whole document).

Structure the document clearly with headings (##, ###) so a deck tool (e.g. Gamma) can infer sections. Include:
- Opening: module outcomes / what learners will be able to do
- Topics from the module plan woven into explanatory prose and bullet lists where helpful
- Exercises: concrete steps, prompts, or worksheets as described in sources — do not skip exercises from the brief
- Activities from LP/AP when relevant (timing, groupings, materials if stated)
- Application / reflection where appropriate
- Closing summary and optional knowledge check (2–4 MCQ-style questions inline is OK)

Rules:
- Stay faithful to the course map, module outline slice, lesson plan, and instructor materials per the stated source priority.
- Do NOT invent major topics, vendor names, version numbers, or compliance claims not supported by sources.
- Prefer dense, usable training text over filler. Use Markdown lists and short paragraphs.
- Aim for length appropriate to the target card count hint (roughly one screen of content per ~1–2 cards; do not pad).
- If fix instructions are provided, incorporate them fully while keeping source alignment.
"""


def _module_body_priority_block(instructor_ppt_priority: str) -> str:
    if instructor_ppt_priority == "primary":
        return (
            "SOURCE PRIORITY:\n"
            "1) Course outline (this module) — mandatory scope\n"
            "2) Instructor PPT — primary for facts and examples when relevant\n"
            "3) Lesson / activity plan — secondary\n\n"
        )
    return (
        "SOURCE PRIORITY:\n"
        "1) Course outline (this module) — mandatory\n"
        "2) Lesson / activity plan\n"
        "3) Instructor PPT — supplement only\n\n"
    )


async def generate_module_body_text(
    *,
    module_name: str,
    module_plan: dict[str, Any],
    module_outline_text: str,
    lesson_text: str | None,
    instructor_text: str | None,
    course_map: str,
    instructor_ppt_priority: str,
    target_card_count: int,
    model: str | None,
    fix_instructions: str = "",
) -> str:
    """Single LLM call: full module body as Markdown for Gamma + validators."""
    priority = instructor_ppt_priority if instructor_ppt_priority in ("primary", "supplement") else "supplement"
    plan_json = json.dumps(module_plan, ensure_ascii=False, indent=2)[:80_000]
    user = (
        f"MODULE_NAME: {module_name}\n"
        f"TARGET_CARD_COUNT_HINT: approximately {max(1, int(target_card_count))} segments/cards\n\n"
        f"{_module_body_priority_block(priority)}"
        "MODULE_PLAN (authoritative checklist for this module — cover all parts):\n"
        f"{plan_json}\n\n"
        "COURSE_MAP (global):\n"
        f"{(course_map or '')[:60_000]}\n\n"
        "OUTLINE — THIS MODULE:\n"
        f"{(module_outline_text or '')[:120_000]}\n\n"
        "LESSON / ACTIVITY PLAN (full):\n"
        f"{(lesson_text or '')[:120_000]}\n\n"
        "INSTRUCTOR PPT (this module slice):\n"
        f"{(instructor_text or '')[:120_000]}\n"
    )
    if (fix_instructions or "").strip():
        user += f"\nVALIDATOR FIX INSTRUCTIONS (must satisfy):\n{fix_instructions.strip()}\n"

    ai = ClaudeService()
    raw = await ai.generate_text_completion(
        system_prompt=MODULE_BODY_SYSTEM_PROMPT,
        user_prompt=user,
        timeout_s=300.0,
        model_override=model,
    )
    out = (raw or "").strip()
    if out.startswith("```"):
        out = _strip_markdown_fences(out)
    return out.strip()


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
- Keep on-slide density suitable for 16:9: concise bullets, no paragraph walls.
  If content is too dense for one slide, keep only the core points for this slide
  and indicate continuation in notes (the planner can split into follow-up slides).
- visual: required, 1-2 sentences, not a URL. Name a concrete graphic to render on the slide:
  - For frameworks, comparisons, or categorization: say the diagram type (e.g. 2x2 matrix, Venn, pyramid,
    swimlane, cycle/steps, before/after).
  - For processes: flowchart, numbered path, or decision tree.
  - For data: simple bar/line/pie (describe what is compared), or icon row with labels.
  - For people/roles: personas or org-style icons, not stock photos of faces unless essential.
  - Section dividers: minimal (band, large numeral, or single hero shape) — still describe it.
  Avoid vague "nice image"; always specify diagram/infographic/illustration style.
- Prefer diagrams/infographics over plain decorative images whenever content can be structured.
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


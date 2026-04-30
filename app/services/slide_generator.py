"""
Module-level slide content generation.

This file supports:
- full-module generation in one model call (preferred path)
- targeted slide regeneration for validator-selected indices only
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
Generate a FULL MODULE deck as JSON only.

Return ONLY valid JSON with this shape:
{
  "slides": [
    {
      "title": "...",
      "bullets": ["...", "..."],
      "notes": "...",
      "visual": "..."
    }
  ]
}

Rules:
- Build all slides together with coherent flow from the requested plan/module blueprint.
- Keep each generated slide title aligned to the requested planned title.
- bullets: 3-6 bullets for content slides; 2-5 bullets for activity slides.
- notes: short instructor notes (1-4 sentences).
- Keep on-slide density suitable for 16:9: concise bullets, no paragraph walls.
  If content is too dense for one slide, keep only core points and mention continuation in notes.
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
Schema: {"slides":[{"title": string, "bullets": [string, ...], "notes": string, "visual": string}, ...]}
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


def _safe_module_slides_json(text: str) -> list[dict[str, Any]]:
    t = _normalize_for_json_loads(text)
    data = json.loads(t)
    if not isinstance(data, dict):
        raise ValueError("Slide generator returned non-object JSON.")
    slides = data.get("slides")
    if not isinstance(slides, list):
        raise ValueError("Slide generator JSON missing slides list.")
    out: list[dict[str, Any]] = []
    for i, raw in enumerate(slides, start=1):
        if not isinstance(raw, dict):
            continue
        title = str(raw.get("title") or "").strip() or f"Slide {i}"
        bullets = raw.get("bullets")
        if not isinstance(bullets, list):
            bullets = []
        cleaned_bullets = [str(b).strip() for b in bullets if str(b).strip()]
        notes = str(raw.get("notes") or "").strip()
        visual = str(raw.get("visual") or "").strip()
        out.append(
            {
                "title": title,
                "bullets": cleaned_bullets,
                "notes": notes,
                "visual": visual,
            }
        )
    if not out:
        raise ValueError("Slide generator returned empty slides list.")
    return out


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


def _fallback_module_slides(planned_slides: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        _fallback_slide_dict(str(s.get("title") or f"Slide {i + 1}"))
        for i, s in enumerate(planned_slides)
    ] or [_fallback_slide_dict("Module Overview")]


def _normalize_to_plan_length(
    generated_slides: list[dict[str, Any]],
    planned_slides: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    target = len(planned_slides)
    cleaned = generated_slides[:target]
    if len(cleaned) < target:
        for i in range(len(cleaned), target):
            title = str(planned_slides[i].get("title") or f"Slide {i + 1}")
            cleaned.append(_fallback_slide_dict(title))
    # Keep titles aligned to plan to preserve sequence for downstream checks.
    for i, plan in enumerate(planned_slides[: len(cleaned)]):
        expected = str(plan.get("title") or "").strip()
        if expected:
            cleaned[i]["title"] = expected
    return cleaned


async def generate_module_slides(
    *,
    planned_slides: list[dict[str, Any]],
    context: dict[str, Any],
    instructor_ppt_priority: str = "supplement",
    model: str | None = None,
    fix_instructions: str | None = None,
) -> list[dict[str, Any]]:
    priority = instructor_ppt_priority if instructor_ppt_priority in ("primary", "supplement") else "supplement"

    plan_rows: list[dict[str, str]] = []
    for i, s in enumerate(planned_slides, start=1):
        plan_rows.append(
            {
                "index": str(i),
                "title": str(s.get("title") or f"Slide {i}").strip(),
                "type": str(s.get("type") or "content").strip().lower(),
            }
        )

    ai = ClaudeService()
    user_prompt = (
        "MODULE SLIDE REQUEST:\n"
        "- Generate the complete module deck in one response.\n"
        "- Keep number of generated slides exactly equal to planned slides length.\n"
        "- Preserve planned ordering.\n\n"
        "PLANNED SLIDES:\n"
        f"{json.dumps(plan_rows, ensure_ascii=False)}\n\n"
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
        parsed = _safe_module_slides_json(raw)
        return _normalize_to_plan_length(parsed, planned_slides)
    except (json.JSONDecodeError, ValueError) as exc:
        logger.warning(
            "Module slides JSON parse failed; retrying once | planned=%s err=%s",
            len(planned_slides),
            exc,
        )
        repair_prompt = (
            user_prompt
            + "\n\nYour previous reply was not valid JSON. "
            "Return ONLY one JSON object with key slides (list of slide objects). No markdown fences. "
            "String values must not contain unescaped double quotes; rephrase or use JSON-escaped quotes where required."
        )
        raw2 = await ai.generate_text_completion(
            system_prompt=SLIDE_GENERATION_SYSTEM_PROMPT,
            user_prompt=repair_prompt,
            timeout_s=120.0,
            model_override=model,
        )
        try:
            parsed2 = _safe_module_slides_json(raw2)
            return _normalize_to_plan_length(parsed2, planned_slides)
        except (json.JSONDecodeError, ValueError) as exc2:
            logger.warning(
                "Module slides JSON still invalid after retry; strict repair or fallback | planned=%s err=%s",
                len(planned_slides),
                exc2,
            )
            repair2 = (
                f"Expected slide count: {len(planned_slides)}\n\n"
                f"Invalid output (fix into valid JSON only, same meaning):\n{raw2[:12000]}\n"
            )
            raw3 = await ai.generate_text_completion(
                system_prompt=STRICT_JSON_REPAIR_SYSTEM,
                user_prompt=repair2,
                timeout_s=90.0,
                model_override=model,
            )
            try:
                parsed3 = _safe_module_slides_json(raw3)
                return _normalize_to_plan_length(parsed3, planned_slides)
            except (json.JSONDecodeError, ValueError) as exc3:
                logger.error(
                    "Module slides JSON unrecoverable; using fallback slides | planned=%s err=%s",
                    len(planned_slides),
                    exc3,
                )
                return _fallback_module_slides(planned_slides)


async def regenerate_selected_slides(
    *,
    planned_slides: list[dict[str, Any]],
    existing_slides: list[dict[str, Any]],
    slides_to_revise: list[int],
    context: dict[str, Any],
    instructor_ppt_priority: str = "supplement",
    model: str | None = None,
    fix_instructions: str | None = None,
) -> list[dict[str, Any]]:
    """
    Regenerate only validator-selected slides (1-based indices) and merge back.
    """
    selected = sorted({i for i in slides_to_revise if isinstance(i, int) and i >= 1})
    if not selected:
        return existing_slides
    subset_plan: list[dict[str, Any]] = []
    index_map: list[int] = []
    for idx in selected:
        if idx <= len(planned_slides):
            subset_plan.append(planned_slides[idx - 1])
            index_map.append(idx - 1)
    if not subset_plan:
        return existing_slides
    regenerated = await generate_module_slides(
        planned_slides=subset_plan,
        context=context,
        instructor_ppt_priority=instructor_ppt_priority,
        model=model,
        fix_instructions=fix_instructions,
    )
    out = [dict(s) if isinstance(s, dict) else {} for s in existing_slides]
    if len(out) < len(planned_slides):
        out = _normalize_to_plan_length(out, planned_slides)
    for local_i, global_idx in enumerate(index_map):
        if local_i < len(regenerated):
            out[global_idx] = regenerated[local_i]
    return out


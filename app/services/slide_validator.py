from __future__ import annotations

import json
import re
from typing import Any

from app.services.claude import ClaudeService
from app.utils.logger import get_logger

logger = get_logger(__name__)

MODULE_BODY_VALIDATOR_SYSTEM_PROMPT = """
You validate ONE training module document (Markdown/plain text) against sources.

Check:
- All ``module_plan`` topics are substantially covered in the generated body
- Exercises called out in ``module_plan`` (and outline) appear with actionable treatment (not just a title line)
- LP/AP activities / lesson_plan_focus are reflected where the sources specify them
- No contradictions with outline/LP/instructor facts (versions, counts, names, frameworks)
- No invented major claims absent from sources

Return ONLY valid JSON:
{
  "approved": true,
  "issues": [],
  "fix_instructions": ""
}

If not approved: issues is a short list of concrete gaps; fix_instructions tells the writer how to rewrite
the ENTIRE module body (full-module regeneration, not partial edits).
"""

SLIDE_VALIDATOR_SYSTEM_PROMPT = """
You are a slide quality validator.

Check:
- Slide count is between 10 and 20
- Content follows Course Outline structure
- LP/AP activities included when present
- Instructor PPT used only as supplement
- No duplicate slides
- Logical progression
- Source alignment: generated slides must match source content facts from module/outline/LP/PPT.
- Detect factual contradictions in standards, versions, dates, counts, frameworks, definitions, and terminology.
- Flag missing or altered core facts from source (numbers, model names, process steps, compliance references).
- Verify module exercises from source content are represented in generated slides (title/bullets/notes).

Return ONLY valid JSON:
{
  "approved": true,
  "issues": [],
  "fix_instructions": ""
}
"""

_STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "from",
    "that",
    "this",
    "into",
    "your",
    "you",
    "are",
    "not",
    "use",
    "using",
    "across",
    "over",
    "under",
    "about",
    "plan",
    "activity",
    "exercise",
    "module",
}


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
    module_text: str | None = None,
    lesson_text: str | None = None,
    instructor_text: str | None = None,
    model: str | None = None,
) -> dict[str, Any]:
    ai = ClaudeService()
    user_prompt = (
        f"has_lesson_plan={has_lesson_plan}\n\n"
        f"MODULE TEXT (source):\n{str(module_text or '')[:90000]}\n\n"
        f"LESSON/ACTIVITY TEXT (source):\n{str(lesson_text or '')[:90000]}\n\n"
        f"INSTRUCTOR PPT TEXT (source):\n{str(instructor_text or '')[:90000]}\n\n"
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


def _slides_text_blob(generated_slides: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    for s in generated_slides:
        parts.append(str(s.get("title") or ""))
        bullets = s.get("bullets")
        if isinstance(bullets, list):
            parts.extend(str(b or "") for b in bullets)
        parts.append(str(s.get("notes") or ""))
        parts.append(str(s.get("visual") or ""))
    return "\n".join(parts).lower()


def _extract_exercise_lines(module_text: str | None) -> list[str]:
    text = str(module_text or "")
    if not text.strip():
        return []
    m = re.search(r"(?is)\bExercises?\s*:\s*(.+)$", text)
    if not m:
        return []
    body = m.group(1).strip()
    raw_lines = [ln.strip(" -•\t") for ln in re.split(r"[\n\r;]+", body) if ln.strip()]
    out: list[str] = []
    for ln in raw_lines:
        cleaned = re.sub(r"\s+", " ", ln).strip()
        if cleaned and cleaned.lower() not in {"exercise", "exercises"}:
            out.append(cleaned[:180])
    return out[:20]


def _keywords(text: str) -> list[str]:
    toks = re.findall(r"[a-z0-9][a-z0-9\-]{2,}", text.lower())
    return [t for t in toks if t not in _STOPWORDS]


async def validate_module_body_ai(
    *,
    module_plan: dict[str, Any],
    generated_body: str,
    module_text: str | None,
    lesson_text: str | None,
    instructor_text: str | None,
    model: str | None = None,
) -> dict[str, Any]:
    ai = ClaudeService()
    user_prompt = (
        "MODULE_PLAN (checklist):\n"
        f"{json.dumps(module_plan, ensure_ascii=False)[:60_000]}\n\n"
        f"MODULE OUTLINE (source):\n{str(module_text or '')[:90_000]}\n\n"
        f"LESSON/ACTIVITY (source):\n{str(lesson_text or '')[:90_000]}\n\n"
        f"INSTRUCTOR PPT (source):\n{str(instructor_text or '')[:90_000]}\n\n"
        "GENERATED_MODULE_BODY:\n"
        f"{str(generated_body or '')[:200_000]}\n"
    )
    raw = await ai.generate_text_completion(
        system_prompt=MODULE_BODY_VALIDATOR_SYSTEM_PROMPT,
        user_prompt=user_prompt,
        timeout_s=180.0,
        model_override=model,
    )
    return _safe_json(raw)


def _exercise_coverage_module_body(module_text: str | None, body: str) -> list[str]:
    exercises = _extract_exercise_lines(module_text)
    if not exercises:
        return []
    blob = (body or "").lower()
    missing: list[str] = []
    for ex in exercises:
        kws = _keywords(ex)
        if not kws:
            continue
        if not any(k in blob for k in kws[:6]):
            missing.append(ex)
    if not missing:
        return []
    preview = ", ".join(missing[:3])
    more = f" (+{len(missing) - 3} more)" if len(missing) > 3 else ""
    return [
        "Exercise coverage: some outline exercises may be missing or too thin in the module body. "
        f"Examples: {preview}{more}."
    ]


def merge_module_body_validator_result(
    *,
    ai_result: dict[str, Any],
    module_text: str | None,
    generated_body: str,
) -> dict[str, Any]:
    issues = [str(i) for i in (ai_result.get("issues") or []) if str(i).strip()]
    issues.extend(_exercise_coverage_module_body(module_text, generated_body))
    fix = str(ai_result.get("fix_instructions") or "").strip()
    if issues:
        deterministic = (
            "Regenerate the FULL module body in Markdown. Address every issue; keep strict alignment "
            "with module_plan, outline, LP/AP, and instructor text. Do not return JSON slide objects."
        )
        fix = f"{fix}\n\n{deterministic}".strip() if fix else deterministic
    return {
        "approved": bool(ai_result.get("approved")) and not issues,
        "issues": issues,
        "fix_instructions": fix,
    }


def _exercise_coverage_issues(module_text: str | None, generated_slides: list[dict[str, Any]]) -> list[str]:
    exercises = _extract_exercise_lines(module_text)
    if not exercises:
        return []
    blob = _slides_text_blob(generated_slides)
    missing: list[str] = []
    for ex in exercises:
        kws = _keywords(ex)
        if not kws:
            continue
        # Coverage heuristic: at least one non-trivial exercise keyword appears in generated content.
        if not any(k in blob for k in kws[:6]):
            missing.append(ex)
    if not missing:
        return []
    preview = ", ".join(missing[:3])
    more = f" (+{len(missing) - 3} more)" if len(missing) > 3 else ""
    return [
        "Exercise-to-slide mismatch: some module exercises are missing from generated slides. "
        f"Missing examples: {preview}{more}."
    ]


def merge_validator_result_with_local_checks(
    *,
    ai_result: dict[str, Any],
    module_text: str | None,
    lesson_text: str | None,
    instructor_text: str | None,
    generated_slides: list[dict[str, Any]],
) -> dict[str, Any]:
    issues = [str(i) for i in (ai_result.get("issues") or []) if str(i).strip()]
    issues.extend(_exercise_coverage_issues(module_text, generated_slides))
    fix = str(ai_result.get("fix_instructions") or "").strip()
    if issues:
        deterministic_fix = (
            "Regenerate with strict source alignment: every key fact must match the provided module/outline/LP/PPT "
            "context (versions, dates, counts, terms, framework structures). Do not introduce contradictory facts. "
            "Ensure each module exercise from source appears in at least one activity/content slide."
        )
        fix = f"{fix}\n\n{deterministic_fix}".strip() if fix else deterministic_fix
    return {
        "approved": bool(ai_result.get("approved")) and not issues,
        "issues": issues,
        "fix_instructions": fix,
    }


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
        vis = str(s.get("visual") or "").strip()
        if not vis and "placeholder" not in title.lower():
            s["visual"] = (
                "Vector-style diagram or structured infographic (flowchart, matrix, steps, or labeled icons) "
                f"that illustrates the slide topic; not a blank placeholder."
            )
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


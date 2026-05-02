from __future__ import annotations

import json
import re
from typing import Any

from app.services.claude import ClaudeService
from app.utils.logger import get_logger

logger = get_logger(__name__)

MODULE_VALIDATOR_SYSTEM_PROMPT = """
You validate one module curriculum document (Markdown/plain text) against source materials.

Check:
- Topics and outcomes implied by MODULE_PLAN are covered in the document
- Lesson plan / activity plan themes appear when LP/AP content exists
- Source alignment: facts match module outline and LP/AP (versions, dates, counts, frameworks)
- Exercises from the module outline are reflected in the document (not necessarily verbatim)
- No internal contradictions

Return ONLY valid JSON:
{
  "approved": true,
  "issues": [],
  "fix_instructions": ""
}

If not approved, fix_instructions must be concrete so the generator can rewrite the full module body.
Do not reference slide numbers — the deck is built later by Gamma.
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


async def validate_module_body_ai(
    *,
    module_body: str,
    module_plan: dict[str, Any],
    course_map: str,
    module_text: str | None,
    lesson_text: str | None,
    instructor_text: str | None,
    has_lesson_plan: bool,
    model: str | None = None,
) -> dict[str, Any]:
    ai = ClaudeService()
    user_prompt = (
        f"has_lesson_plan={has_lesson_plan}\n\n"
        f"COURSE_MAP (short global context):\n{str(course_map or '')[:40_000]}\n\n"
        f"MODULE_PLAN:\n{json.dumps(module_plan, ensure_ascii=False)[:40_000]}\n\n"
        f"MODULE TEXT — outline excerpt (source):\n{str(module_text or '')[:90_000]}\n\n"
        f"LESSON/ACTIVITY TEXT (source):\n{str(lesson_text or '')[:90_000]}\n\n"
        f"INSTRUCTOR PPT TEXT (source):\n{str(instructor_text or '')[:90_000]}\n\n"
        f"GENERATED MODULE BODY:\n{str(module_body or '')[:120_000]}\n"
    )
    raw = await ai.generate_text_completion(
        system_prompt=MODULE_VALIDATOR_SYSTEM_PROMPT,
        user_prompt=user_prompt,
        timeout_s=120.0,
        model_override=model,
    )
    return _safe_json(raw)


def _keywords(text: str) -> list[str]:
    toks = re.findall(r"[a-z0-9][a-z0-9\-]{2,}", text.lower())
    return [t for t in toks if t not in _STOPWORDS]


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


def _exercise_coverage_issues(module_text: str | None, generated_body: str) -> list[str]:
    exercises = _extract_exercise_lines(module_text)
    if not exercises:
        return []
    blob = (generated_body or "").lower()
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
        "Exercise coverage: some module exercises may be missing from the generated module body. "
        f"Examples: {preview}{more}."
    ]


def merge_validator_result_with_local_checks(
    *,
    ai_result: dict[str, Any],
    module_text: str | None,
    lesson_text: str | None,
    instructor_text: str | None,
    generated_body: str,
) -> dict[str, Any]:
    issues = [str(i) for i in (ai_result.get("issues") or []) if str(i).strip()]
    issues.extend(_exercise_coverage_issues(module_text, generated_body))
    fix = str(ai_result.get("fix_instructions") or "").strip()
    if issues:
        deterministic_fix = (
            "Regenerate the full module body with strict source alignment to outline/LP/PPT. "
            "Include every listed exercise theme; do not contradict facts (versions, dates, counts, terms)."
        )
        fix = f"{fix}\n\n{deterministic_fix}".strip() if fix else deterministic_fix
    return {
        "approved": bool(ai_result.get("approved")) and not issues,
        "issues": issues,
        "fix_instructions": fix,
    }


def normalize_module_body(body: str) -> str:
    s = (body or "").strip()
    return s if s else "(No content generated — retry or check sources.)"

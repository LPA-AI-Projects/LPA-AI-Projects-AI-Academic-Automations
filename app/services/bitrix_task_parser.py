"""
Parse Bitrix24 task webhooks and task DESCRIPTION tables into course outline input_data.

Task DESCRIPTION often uses BBCode tables, e.g.::

    [table][tr][td]Company Name:  NA[/td][/tr]...
"""
from __future__ import annotations

import re
from typing import Any

# Refine: / refine : / Refine : (case-insensitive); instruction may continue on next lines
REFINE_COMMENT_PATTERN = re.compile(r"^\s*refine\s*:\s*(.+)$", re.IGNORECASE | re.DOTALL)
_REFINE_ANYWHERE_PATTERN = re.compile(r"refine\s*:\s*(.+)$", re.IGNORECASE | re.DOTALL)
_BITRIX_USER_TAG_PREFIX = re.compile(r"^\[USER=\d+\][^\[]*\[/USER\]\s*", re.IGNORECASE)

from app.utils.logger import get_logger

logger = get_logger(__name__)

_NA_VALUES = frozenset({"na", "n/a", "none", "-", ""})

# "Total Duration: 6 to 8 Weeks" or "Course Duration 6 to 8 Weeks (...)" in one table cell
_DURATION_LABEL_PREFIX = re.compile(
    r"^(total duration|course duration)\s*:?\s*(.+)$",
    re.IGNORECASE | re.DOTALL,
)
_BITRIX_URL_TAG = re.compile(
    r"\[url(?:=([^\]]+))?\](.*?)\[/url\]",
    re.IGNORECASE | re.DOTALL,
)
_HTTP_URL = re.compile(r"https?://[^\s\]<>\",']+", re.IGNORECASE)


def _parse_labeled_cell(cell: str) -> tuple[str, str] | None:
    """Return (normalized_key, value) from a BBCode table cell."""
    cell = re.sub(r"\[/?[bi]\]", "", cell, flags=re.IGNORECASE).strip()
    if not cell:
        return None
    if ":" in cell:
        label, _, value = cell.partition(":")
        label_k = _normalize_label(label)
        if label_k:
            return label_k, _clean_value(value)
    m = _DURATION_LABEL_PREFIX.match(cell)
    if m:
        label_k = _normalize_label(m.group(1))
        if label_k:
            return label_k, _clean_value(m.group(2))
    return None


def _clean_value(raw: str | None) -> str:
    s = str(raw or "").strip()
    s = re.sub(r"\[/?[bi]\]", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    if s.lower() in _NA_VALUES:
        return ""
    return s


def _extract_referral_course_links(raw: str | None) -> str:
    """
    Normalize Bitrix referral link cells to plain URL text for ``referral_course_links``.

    Handles BBCode ``[url=...]...[/url]``, multiple URLs, and plain https links — same field Zoho sends.
    """
    text = str(raw or "").strip()
    if not text or text.lower() in _NA_VALUES:
        return ""

    urls: list[str] = []
    for match in _BITRIX_URL_TAG.finditer(text):
        href = (match.group(1) or match.group(2) or "").strip()
        if href.lower().startswith(("http://", "https://")):
            urls.append(href.rstrip(".,;"))

    if not urls:
        plain = re.sub(r"\[/?[^\]]+\]", " ", text)
        for match in _HTTP_URL.finditer(plain):
            urls.append(match.group(0).rstrip(".,;"))

    if urls:
        seen: set[str] = set()
        unique: list[str] = []
        for url in urls:
            if url not in seen:
                seen.add(url)
                unique.append(url)
        return "\n".join(unique)

    cleaned = _clean_value(text)
    if "http://" in cleaned.lower() or "https://" in cleaned.lower():
        return cleaned
    return ""


def _parse_two_column_row(label_cell: str, value_cell: str) -> tuple[str, str] | None:
    """Return (normalized_key, value) from a two-cell BBCode table row."""
    parsed_label = _parse_labeled_cell(label_cell)
    if parsed_label:
        label_k, inline_value = parsed_label
        value = inline_value or _clean_value(value_cell)
    else:
        label_raw = re.sub(r"\[/?[bi]\]", "", label_cell, flags=re.IGNORECASE).strip()
        label_k = _normalize_label(label_raw.rstrip(":").strip())
        value = _clean_value(value_cell)
    if label_k and value:
        return label_k, value
    return None


def parse_task_description_table(description: str | None) -> dict[str, Any]:
    """
    Extract label → value pairs from Bitrix task DESCRIPTION (BBCode table or plain text).
    """
    text = str(description or "")
    if not text.strip():
        return {}

    out: dict[str, str] = {}

    # BBCode rows: [tr][td]Label[/td][td]value[/td][/tr] (two-column CRM template)
    for m in re.finditer(
        r"\[tr\]\s*\[td\](.*?)\[/td\]\s*\[td\](.*?)\[/td\]\s*\[/tr\]",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        parsed_row = _parse_two_column_row(m.group(1), m.group(2))
        if parsed_row:
            label_k, value = parsed_row
            out[label_k] = value

    # BBCode rows: [tr][td]Label: value[/td][/tr] (single-column; skip two-column rows)
    for m in re.finditer(
        r"\[tr\]\s*\[td\](.*?)\[/td\](?!\s*\[td\])\s*\[/tr\]",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        parsed_cell = _parse_labeled_cell(m.group(1))
        if parsed_cell:
            label_k, value = parsed_cell
            if value and label_k not in out:
                out[label_k] = value

    # Plain lines: "Label: value"
    if not out:
        for line in text.splitlines():
            line = re.sub(r"\[/?[^\]]+\]", "", line).strip()
            parsed_line = _parse_labeled_cell(line)
            if parsed_line:
                label_k, value = parsed_line
                if value:
                    out[label_k] = value

    return out


def _normalize_label(label: str) -> str:
    s = re.sub(r"\s+", " ", str(label or "").strip().lower())
    s = re.sub(r"[^\w\s/]", "", s)
    aliases = {
        "company name": "company_name",
        "product course name": "course_name",
        "product / course name": "course_name",
        "level of training": "level_of_training",
        "level training beginner intermediate or expert": "level_of_training",
        "level training": "level_of_training",
        "sector of the company": "industry_domain",
        "industry / domain": "industry_domain",
        "industry domain": "industry_domain",
        "size of the company": "size_of_company",
        "learning objective of the training": "goal_of_training",
        "goal of training": "goal_of_training",
        "language of the candidates": "languages_preferred",
        "location of the training": "location_of_training",
        "mode of training": "mode_of_training",
        "no of pax": "no_of_pax",
        "department": "department",
        "department of product": "department",
        "designation": "designation",
        "designation of learnerlearners": "designation",
        "designation of learner/learners": "designation",
        "focus area of training theory practical role play simulations games and activities": (
            "focus_area_of_training"
        ),
        "focus area of training": "focus_area_of_training",
        "suggested topics by the client / trainer": "suggested_topics",
        "suggested topics by the client trainer": "suggested_topics",
        "referral course links if any": "referral_course_links",
        "referral course links": "referral_course_links",
        "is this course meant for certification skill development or any other details": (
            "course_purpose"
        ),
        "is this course meant for": "course_purpose",
        "duration in hours": "per_day_duration_in_hours",
        "course duration": "course_duration",
        "current challenges / pain points": "pain_points",
        "current challenges pain points": "pain_points",
        "expected outcome after training": "expected_outcome",
        "expected outcome after training participants should be able to": "expected_outcome",
        "target job role after training": "target_job_role",
        "professional experience": "professional_experience",
        "current skill level": "current_skill_level",
        "schedule proposed": "schedule_proposed",
        "preferred schedule for trainer finalization": "preferred_schedule",
        "any specific requirements": "specific_requirements",
        "topic attachment from the client": "topic_attachment",
    }
    if s in aliases:
        return aliases[s]
    if "course name" in s or ("product" in s and "course" in s):
        return "course_name"
    if "company" in s and "name" in s:
        return "company_name"
    if "meant for" in s:
        return "course_purpose"
    if "pain" in s or "challenge" in s:
        return "pain_points"
    if "expected outcome" in s:
        return "expected_outcome"
    if "target job" in s or "job role" in s:
        return "target_job_role"
    if "goal" in s and "training" in s:
        return "goal_of_training"
    if "learning objective" in s:
        return "goal_of_training"
    if "suggested topic" in s:
        return "suggested_topics"
    if "focus area" in s:
        return "focus_area_of_training"
    if "level" in s and "training" in s:
        return "level_of_training"
    if "mode" in s and "training" in s:
        return "mode_of_training"
    if "location" in s and "training" in s:
        return "location_of_training"
    if "industry" in s or "domain" in s:
        return "industry_domain"
    if "professional experience" in s:
        return "professional_experience"
    if "skill level" in s:
        return "current_skill_level"
    if "schedule proposed" in s or (s.startswith("schedule") and "preferred" not in s):
        return "schedule_proposed"
    if "preferred schedule" in s:
        return "preferred_schedule"
    if "pax" in s or "participants" in s:
        return "no_of_pax"
    if "department" in s:
        return "department"
    if "designation" in s:
        return "designation"
    if "referral" in s and "link" in s:
        return "referral_course_links"
    if "duration" in s and "hour" in s:
        return "per_day_duration_in_hours"
    if "course" in s and "duration" in s:
        return "course_duration"
    if "total" in s and "duration" in s:
        return "bitrix_total_duration_note"
    if "specific requirement" in s:
        return "specific_requirements"
    # Trainer experience / pricing / CV / nationality are not used in outline input.
    if (
        "trainer experience" in s
        or "certified trainer" in s
        or "pricing" in s
        or "customer cv" in s
        or "nationality" in s
        or "asian/arab" in s
    ):
        return "_ignored"
    return s.replace(" ", "_")[:80]


def _append_note(notes_parts: list[str], label: str, value: str | None) -> None:
    v = _clean_value(value)
    if v:
        notes_parts.append(f"{label}: {v}")


def _combine_text(*parts: str) -> str:
    cleaned = [_clean_value(p) for p in parts]
    return " | ".join(p for p in cleaned if p)


def _structured_bitrix_fields(parsed: dict[str, str]) -> dict[str, str]:
    """Top-level optional fields from Bitrix B2C template (not dumped into additional_notes)."""
    out: dict[str, str] = {}
    for key in (
        "course_purpose",
        "schedule_proposed",
        "industry_domain",
        "professional_experience",
        "current_skill_level",
        "focus_area_of_training",
        "location_of_training",
        "target_job_role",
        "pain_points",
        "expected_outcome",
        "specific_requirements",
        "preferred_schedule",
        "topic_attachment",
    ):
        value = _clean_value(parsed.get(key))
        if value:
            out[key] = value
    return out


def _normalize_mode(raw: str) -> str:
    mode = (raw or "").strip()
    lower = mode.lower()
    if "hybrid" in lower:
        return "Hybrid"
    if "online" in lower:
        return "Online"
    if "onsite" in lower or "offline" in lower or "classroom" in lower:
        return "Onsite"
    return mode


def _map_parsed_to_input_data(parsed: dict[str, str]) -> dict[str, Any]:
    """Map parsed task table labels to CourseInputData-compatible dict."""
    course_name = parsed.get("course_name") or ""
    company_name = parsed.get("company_name") or ""
    department = parsed.get("department") or ""
    designation = parsed.get("designation") or ""

    level = parsed.get("level_of_training") or ""
    if level.lower().startswith("beginner"):
        level = "Beginner"
    elif level.lower().startswith("intermediate"):
        level = "Intermediate"
    elif level.lower().startswith("expert") or level.lower().startswith("advanced"):
        level = "Advanced"

    mode = _normalize_mode(parsed.get("mode_of_training") or "")
    location = _clean_value(parsed.get("location_of_training"))
    if not mode and location:
        mode = _normalize_mode(location)

    # Prefer explicit suggested topics; keep legacy focus-area → topics for older templates.
    suggested_topics = _clean_value(parsed.get("suggested_topics"))
    focus_area = _clean_value(
        parsed.get("focus_area_of_training") or parsed.get("topics_to_include")
    )
    topics_to_include = suggested_topics or focus_area

    pain_points = _clean_value(parsed.get("pain_points"))
    target_job_role = _clean_value(parsed.get("target_job_role"))
    goal = _clean_value(parsed.get("goal_of_training"))
    expected_outcome = _clean_value(parsed.get("expected_outcome"))

    need_of_training = _combine_text(pain_points, target_job_role) or goal
    goal_of_training = _combine_text(goal, expected_outcome) or need_of_training

    # Only legacy / miscellaneous CRM lines belong in additional_notes.
    notes_parts: list[str] = []
    for label, key in (
        ("Sector", "sector"),
        ("Language of the trainer", "language_of_the_trainer"),
        ("Training material", "training_material"),
        ("Yrs of experience of trainers", "yrs_of_experience_of_trainers_if_any_specific"),
    ):
        _append_note(notes_parts, label, parsed.get(key) or "")

    # Legacy free-form keys from older Bitrix templates.
    for key in (
        "additional_notes",
        "any_specific_requirement_if_any_please_specify",
    ):
        _append_note(notes_parts, key.replace("_", " "), parsed.get(key))

    per_day_hours = (
        parsed.get("per_day_duration_in_hours")
        or parsed.get("duration_in_hours")
        or ""
    )
    course_duration = parsed.get("course_duration") or ""
    total_duration_note = parsed.get("bitrix_total_duration_note") or ""
    referral_course_links = _extract_referral_course_links(parsed.get("referral_course_links"))

    result: dict[str, Any] = {
        "company_name": company_name or "NA",
        "course_name": course_name,
        "department": department or "NA",
        "designation": designation or "NA",
        "level_of_training": level or None,
        "mode_of_training": mode or None,
        "goal_of_training": goal_of_training,
        "need_of_training": need_of_training,
        "size_of_company": parsed.get("size_of_company") or "",
        "no_of_pax": parsed.get("no_of_pax") or "",
        "languages_preferred": parsed.get("languages_preferred") or "",
        "topics_to_include": topics_to_include or "",
        "additional_notes": "\n".join(notes_parts) if notes_parts else "",
        "per_day_duration_in_hours": per_day_hours or None,
    }
    result.update(_structured_bitrix_fields(parsed))
    if referral_course_links:
        result["referral_course_links"] = referral_course_links
    if course_duration:
        result["course_duration"] = course_duration
    if total_duration_note:
        extra = f"Total duration (CRM): {total_duration_note}"
        result["additional_notes"] = (
            f"{result['additional_notes']}\n{extra}".strip()
            if result.get("additional_notes")
            else extra
        )
    return result


def format_bitrix_cover_duration_hours(raw_hours: Any) -> str | None:
    """
    Format Bitrix ``Duration in hours`` (e.g. ``8``, ``8hr``, ``8 hours``) for the cover line.

    Cover template renders: ``Total Duration: {value}``.
    """
    s = str(raw_hours or "").strip()
    if not s:
        return None
    try:
        m = re.search(r"([\d.]+)", s.replace(",", ""))
        if m:
            ph_f = float(m.group(1))
            return f"{ph_f:g} Hour" if ph_f == 1 else f"{ph_f:g} Hours"
    except ValueError:
        pass
    return s


def apply_bitrix_client_duration_to_outline(outline_payload: Any, input_data: dict[str, Any] | None) -> None:
    """
    Bitrix-only: cover ``Total Duration`` uses **Duration in hours** from the task, not AI days/weeks.

    Optional ``course_duration`` from the task still updates the Course Details table row when present.
    """
    if outline_payload is None or not isinstance(input_data, dict):
        return
    cover = format_bitrix_cover_duration_hours(input_data.get("per_day_duration_in_hours"))
    if cover:
        outline_payload.duration = cover
    cd = str(input_data.get("course_duration") or "").strip()
    if cd:
        try:
            outline_payload.course_details.course_duration = cd
        except Exception:
            pass


def _positive_task_id(value: Any) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw or raw.lower() == "undefined":
        return None
    try:
        n = int(raw)
    except ValueError:
        return None
    return str(n) if n > 0 else None


def extract_message_id(payload: dict[str, Any]) -> str | None:
    """Task chat message id from ONTASKCOMMENTADD (new task card)."""
    if not isinstance(payload, dict):
        return None
    for key in (
        "MESSAGE_ID",
        "message_id",
        "data[FIELDS_AFTER][MESSAGE_ID]",
        "data[FIELDS_BEFORE][MESSAGE_ID]",
    ):
        mid = _positive_task_id(payload.get(key))
        if mid:
            return mid
    data = payload.get("data")
    if isinstance(data, dict):
        after = data.get("FIELDS_AFTER")
        if isinstance(after, dict):
            mid = _positive_task_id(after.get("MESSAGE_ID"))
            if mid:
                return mid
    return None


def _log_task_id_resolution(payload: dict[str, Any], event: str, resolved: str | None) -> None:
    logger.info(
        "Bitrix extract_task_id | event=%s AFTER_ID=%s TASK_ID=%s resolved=%s",
        event or "(none)",
        payload.get("data[FIELDS_AFTER][ID]"),
        payload.get("data[FIELDS_AFTER][TASK_ID]"),
        resolved,
    )


def extract_task_id(payload: dict[str, Any]) -> str | None:
    """Resolve task id from automation / webhook / task.getdata shapes."""
    if not isinstance(payload, dict):
        return None

    event = str(payload.get("event") or "").upper()

    for key in (
        "taskId",
        "task_id",
        "bitrix_record_id",
        "document_id",
        "data[FIELDS_AFTER][TASK_ID]",
        "data[FIELDS_BEFORE][TASK_ID]",
    ):
        tid = _positive_task_id(payload.get(key))
        if tid:
            _log_task_id_resolution(payload, event, tid)
            return tid

    for key in ("ID", "id"):
        tid = _positive_task_id(payload.get(key))
        if tid:
            _log_task_id_resolution(payload, event, tid)
            return tid

    for key in (
        "data[FIELDS_AFTER][ID]",
        "data[FIELDS_BEFORE][ID]",
        "data[FIELDS][ID]",
    ):
        tid = _positive_task_id(payload.get(key))
        if tid:
            _log_task_id_resolution(payload, event, tid)
            return tid

    data = payload.get("data")
    if isinstance(data, dict):
        after = data.get("FIELDS_AFTER")
        if isinstance(after, dict):
            tid = _positive_task_id(after.get("TASK_ID")) or _positive_task_id(after.get("ID"))
            if tid:
                _log_task_id_resolution(payload, event, tid)
                return tid
        before = data.get("FIELDS_BEFORE")
        if isinstance(before, dict):
            tid = _positive_task_id(before.get("TASK_ID")) or _positive_task_id(before.get("ID"))
            if tid:
                _log_task_id_resolution(payload, event, tid)
                return tid
        fields = data.get("FIELDS")
        if isinstance(fields, dict):
            tid = _positive_task_id(fields.get("TASK_ID")) or _positive_task_id(
                fields.get("ID") or fields.get("id")
            )
            if tid:
                _log_task_id_resolution(payload, event, tid)
                return tid
        tid = _positive_task_id(data.get("taskId") or data.get("ID") or data.get("id"))
        if tid:
            _log_task_id_resolution(payload, event, tid)
            return tid

    result = payload.get("result")
    if isinstance(result, dict):
        nested_task = result.get("task")
        if isinstance(nested_task, dict):
            tid = _positive_task_id(nested_task.get("ID") or nested_task.get("id"))
            if tid:
                _log_task_id_resolution(payload, event, tid)
                return tid
        tid = _positive_task_id(result.get("ID") or result.get("id"))
        if tid:
            _log_task_id_resolution(payload, event, tid)
            return tid

    _log_task_id_resolution(payload, event, None)
    return None


def _normalize_task_field_dict(task: dict[str, Any]) -> dict[str, Any]:
    """Promote lowercase tasks.task.get keys to the uppercase shape used elsewhere."""
    out = dict(task)
    for low, up in (
        ("description", "DESCRIPTION"),
        ("title", "TITLE"),
        ("id", "ID"),
        ("groupId", "GROUP_ID"),
        ("flowId", "FLOW_ID"),
    ):
        if task.get(low) is not None and not str(out.get(up) or "").strip():
            out[up] = task[low]
    return out


def normalize_bitrix_comment_text(raw: str | None) -> str:
    """Normalize Bitrix task chat / comment text before refine parsing."""
    text = str(raw or "")
    text = re.sub(r"\[BR\]", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"\[/?[bi]\]", "", text, flags=re.IGNORECASE)
    return text.strip()


def parse_refine_feedback_from_comment(
    comment_text: str | None,
    *,
    prefix: str | None = None,  # noqa: ARG001 — kept for API compat; matching is regex-only
) -> str | None:
    """
    Extract refine instruction when comment matches ``Refine:`` (flexible spacing/case).

    Accepts single-line and multi-line, e.g. ``Refine :\\nchange it to 24 hours``.
    Also handles Bitrix chat prefixes such as ``[USER=123]Name[/USER] refine: ...``.
    """
    comment = normalize_bitrix_comment_text(comment_text)
    if not comment:
        return None
    comment = _BITRIX_USER_TAG_PREFIX.sub("", comment)
    match = REFINE_COMMENT_PATTERN.match(comment)
    if match:
        instruction = match.group(1).strip()
        return instruction or None
    match = _REFINE_ANYWHERE_PATTERN.search(comment)
    if match:
        instruction = match.group(1).strip()
        return instruction or None
    return None


def extract_comment_from_webhook_payload(payload: dict[str, Any]) -> str:
    """Best-effort comment body from ONTASKCOMMENTADD webhook (when Bitrix includes it)."""
    if not isinstance(payload, dict):
        return ""
    for key in (
        "data[FIELDS_AFTER][POST_MESSAGE]",
        "POST_MESSAGE",
        "post_message",
        "postMessage",
    ):
        value = payload.get(key)
        if value is not None and str(value).strip():
            return normalize_bitrix_comment_text(str(value))
    data = payload.get("data")
    if isinstance(data, dict):
        after = data.get("FIELDS_AFTER")
        if isinstance(after, dict):
            for key in ("POST_MESSAGE", "postMessage", "MESSAGE", "TEXT", "text"):
                value = after.get(key)
                if value is not None and str(value).strip():
                    return normalize_bitrix_comment_text(str(value))
    return ""


def extract_task_fields(payload: dict[str, Any]) -> dict[str, Any]:
    """Get task field dict from nested webhook or flat task.getdata result."""
    if not isinstance(payload, dict):
        return {}
    result = payload.get("result")
    if isinstance(result, dict):
        nested_task = result.get("task")
        if isinstance(nested_task, dict):
            return _normalize_task_field_dict(nested_task)
        if result.get("DESCRIPTION") is not None or result.get("description") is not None:
            return _normalize_task_field_dict(result)
    if payload.get("DESCRIPTION") is not None or payload.get("description") is not None:
        return _normalize_task_field_dict(payload)
    data = payload.get("data")
    if isinstance(data, dict) and isinstance(data.get("FIELDS"), dict):
        return data["FIELDS"]
    return payload


def resolve_bitrix_task_request(payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """
    Return (task_id, input_data dict) from a Bitrix task webhook or task API body.

    Raises ValueError when task id or course_name cannot be resolved.
    """
    logger.info(
        "Bitrix task payload keys: %s",
        sorted(payload.keys()) if isinstance(payload, dict) else type(payload).__name__,
    )

    task_fields = extract_task_fields(payload)
    task_id = extract_task_id(payload) or extract_task_id(task_fields)
    if not task_id:
        raise ValueError(
            "Could not find task id in payload. Expected ID, taskId, data.FIELDS.ID, or bitrix_record_id."
        )

    # Explicit input_data from caller wins
    raw_input = payload.get("input_data")
    if isinstance(raw_input, dict) and str(raw_input.get("course_name") or "").strip():
        out = dict(raw_input)
        out["bitrix_task_id"] = task_id
        return task_id, out

    description = str(
        task_fields.get("DESCRIPTION")
        or task_fields.get("description")
        or payload.get("DESCRIPTION")
        or payload.get("description")
        or ""
    )
    parsed = parse_task_description_table(description)
    input_data = _map_parsed_to_input_data(parsed)

    title = _clean_value(
        str(
            task_fields.get("TITLE")
            or task_fields.get("title")
            or payload.get("TITLE")
            or payload.get("title")
            or ""
        )
    )
    if not input_data.get("course_name") and title:
        input_data["course_name"] = title

    if not str(input_data.get("course_name") or "").strip():
        raise ValueError(
            "course_name could not be parsed from task DESCRIPTION or TITLE. "
            "Send input_data.course_name in the webhook body."
        )

    input_data["bitrix_task_id"] = task_id
    input_data["bitrix_task_title"] = title
    logger.info(
        "Bitrix task resolved | task_id=%s course_name=%s company=%s referral_links=%s",
        task_id,
        input_data.get("course_name"),
        input_data.get("company_name"),
        bool(input_data.get("referral_course_links")),
    )
    return task_id, input_data

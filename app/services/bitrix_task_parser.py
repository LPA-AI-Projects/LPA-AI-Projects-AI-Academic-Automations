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

from app.utils.logger import get_logger

logger = get_logger(__name__)

_NA_VALUES = frozenset({"na", "n/a", "none", "-", ""})

# "Total Duration: 6 to 8 Weeks" or "Course Duration 6 to 8 Weeks (...)" in one table cell
_DURATION_LABEL_PREFIX = re.compile(
    r"^(total duration|course duration)\s*:?\s*(.+)$",
    re.IGNORECASE | re.DOTALL,
)


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

    # BBCode rows: [tr][td]Label: value[/td][/tr] (single-column)
    for m in re.finditer(
        r"\[tr\]\s*\[td\](.*?)\[/td\]\s*\[/tr\]",
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
        "level training beginner intermediate or expert": "level_of_training",
        "level training": "level_of_training",
        "sector of the company": "sector",
        "size of the company": "size_of_company",
        "learning objective of the training": "goal_of_training",
        "language of the candidates": "languages_preferred",
        "location of the training": "mode_of_training",
        "no of pax": "no_of_pax",
        "department": "department",
        "designation": "designation",
        "focus area of training theory practical role play simulations games and activities": "topics_to_include",
        "focus area of training": "topics_to_include",
        "referral course links if any": "referral_course_links",
        "is this course meant for certification skill development or any other details": "additional_notes",
        "duration in hours": "per_day_duration_in_hours",
        "course duration": "course_duration",
    }
    if s in aliases:
        return aliases[s]
    if "course name" in s or "product" in s and "course" in s:
        return "course_name"
    if "company" in s:
        return "company_name"
    if "learning objective" in s or "objective" in s:
        return "goal_of_training"
    if "level" in s and "training" in s:
        return "level_of_training"
    if "location" in s or "online" in s and "training" in s:
        return "mode_of_training"
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
    return s.replace(" ", "_")[:80]


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

    mode = parsed.get("mode_of_training") or ""
    if "online" in mode.lower():
        mode = "Online"
    elif "onsite" in mode.lower() or "classroom" in mode.lower():
        mode = "Onsite"
    elif "hybrid" in mode.lower():
        mode = "Hybrid"

    notes_parts: list[str] = []
    for key in (
        "sector",
        "additional_notes",
        "language of the trainer",
        "nationality of the trainer if anything specific",
        "training material",
        "proposed pricing in aed",
        "trainer asian/arab/ european",
        "any specific requirement if any please specify",
        "yrs of experience of trainers if any specific",
        "certified trainer mandatory or not",
    ):
        v = parsed.get(key.replace(" ", "_")) or parsed.get(key) or ""
        if v:
            notes_parts.append(f"{key}: {v}")

    per_day_hours = (
        parsed.get("per_day_duration_in_hours")
        or parsed.get("duration_in_hours")
        or ""
    )
    course_duration = parsed.get("course_duration") or ""
    total_duration_note = parsed.get("bitrix_total_duration_note") or ""

    result: dict[str, Any] = {
        "company_name": company_name or "NA",
        "course_name": course_name,
        "department": department or "NA",
        "designation": designation or "NA",
        "level_of_training": level or None,
        "mode_of_training": mode or None,
        "goal_of_training": parsed.get("goal_of_training") or "",
        "need_of_training": parsed.get("goal_of_training") or "",
        "size_of_company": parsed.get("size_of_company") or "",
        "no_of_pax": parsed.get("no_of_pax") or "",
        "languages_preferred": parsed.get("languages_preferred") or "",
        "topics_to_include": parsed.get("topics_to_include") or "",
        "referral_course_links": parsed.get("referral_course_links") or "",
        "additional_notes": "\n".join(notes_parts) if notes_parts else "",
        "per_day_duration_in_hours": per_day_hours or None,
    }
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


def parse_refine_feedback_from_comment(
    comment_text: str | None,
    *,
    prefix: str | None = None,  # noqa: ARG001 — kept for API compat; matching is regex-only
) -> str | None:
    """
    Extract refine instruction when comment matches ``Refine:`` (flexible spacing/case).

    Accepts single-line and multi-line, e.g. ``Refine :\\nchange it to 24 hours``.
    """
    comment = str(comment_text or "").strip()
    if not comment:
        return None
    match = REFINE_COMMENT_PATTERN.match(comment)
    if not match:
        return None
    instruction = match.group(1).strip()
    return instruction or None


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
        "Bitrix task resolved | task_id=%s course_name=%s company=%s",
        task_id,
        input_data.get("course_name"),
        input_data.get("company_name"),
    )
    return task_id, input_data

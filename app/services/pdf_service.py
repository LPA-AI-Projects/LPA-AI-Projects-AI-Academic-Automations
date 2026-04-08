
from __future__ import annotations

import asyncio
import json
import os
import re
import tempfile
import uuid
from html import escape
from pathlib import Path
from typing import Iterable

from app.core.storage_paths import pdfs_dir
from app.schemas.outline_payload import CourseOutlinePayload
from app.utils.logger import get_logger

logger = get_logger(__name__)
REGIONS_SERVED_CONSTANT = "UAE, Saudi Arabia, Africa, MENA, and Europe"

OUTPUT_DIR = pdfs_dir()
os.makedirs(OUTPUT_DIR, exist_ok=True)

TEMPLATE_DIR = Path(__file__).resolve().parents[1] / "templates"
TEMPLATE_PATH = TEMPLATE_DIR / "index.html"

# Brochure copy: strip em/en dashes so output matches print style (commas instead of "AI" dashes).
_DASH_CHARS = ("\u2014", "\u2013", "\u2012", "\u2015")


def _brochure_strip_dashes(text: str | None) -> str:
    if text is None:
        return ""
    s = str(text)
    for ch in _DASH_CHARS:
        s = s.replace(ch, ", ")
    s = re.sub(r",\s*,+", ", ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _brochure_details_table_blurb(text: str | None, *, max_sentences: int = 2, max_words: int = 58) -> str:
    """
    Key Benefits / Value Addition cells: match compact brochure samples (two short sentences, ~40-50 words).
    If the model returns long multi-sentence blocks, keep the first sentences and trim words.
    """
    s = _brochure_strip_dashes((text or "").strip())
    if not s:
        return ""
    parts = [p.strip() for p in re.split(r"(?<=[.!?])\s+", s) if p.strip()]
    if not parts:
        return _compress_text(s, max_words)
    merged = " ".join(parts[:max_sentences]).strip()
    if merged and merged[-1] not in ".!?":
        merged += "."
    if len(merged.split()) > max_words:
        merged = _compress_text(merged, max_words)
        if merged and merged[-1] not in ".!?":
            merged += "."
    return merged


def _details_page_summary_from_insight(paragraphs: list[str]) -> str:
    """
    Course Details page (pDetSummary): one flowing paragraph of three sentences, built from the
    first sentence of each Program Insight paragraph so it matches full-brochure samples.
    """
    sentences: list[str] = []
    for raw in (paragraphs or [])[:3]:
        ps = _brochure_strip_dashes(str(raw).strip())
        if not ps:
            continue
        parts = [p.strip() for p in re.split(r"(?<=[.!?])\s+", ps) if p.strip()]
        if parts:
            sentences.append(parts[0])
    merged = " ".join(sentences).strip()
    if not merged:
        return ""
    if merged[-1] not in ".!?":
        merged += "."
    if len(merged.split()) > 105:
        merged = _compress_text(merged, 100)
        if merged and merged[-1] not in ".!?":
            merged += "."
    return merged


def _slim_insight_paragraph(text: str, max_sentences: int = 2, max_words: int = 62) -> str:
    """Program Insight body: max two sentences per paragraph, brochure density."""
    s = _brochure_strip_dashes(text.strip())
    if not s:
        return ""
    parts = [p.strip() for p in re.split(r"(?<=[.!?])\s+", s) if p.strip()]
    merged = " ".join(parts[:max_sentences]).strip()
    if merged and merged[-1] not in ".!?":
        merged += "."
    if len(merged.split()) > max_words:
        merged = _compress_text(merged, max_words)
        if merged and merged[-1] not in ".!?":
            merged += "."
    return merged


def _slim_insight_bullet(text: str, max_words: int = 18) -> str:
    """Six outcome lines: one compact line each."""
    s = _brochure_strip_dashes(text.strip())
    if not s:
        return ""
    return _compress_text(s, max_words)


def _slim_capability_row_description(text: str | None, max_words: int = 28) -> str:
    """One sentence per row; trim if the model returns multiple sentences or long text."""
    s = _brochure_strip_dashes((text or "").strip())
    if not s:
        return ""
    parts = [p.strip() for p in re.split(r"(?<=[.!?])\s+", s) if p.strip()]
    first = parts[0] if parts else s
    return _compress_text(first, max_words)


def _slim_capability_closing(text: str | None, max_sentences: int = 3, max_words: int = 88) -> str:
    """Single closing paragraph: ignore extra paragraphs; cap sentences and words."""
    s = _brochure_strip_dashes((text or "").strip())
    if not s:
        return ""
    first_block = s.split("\n\n")[0].strip()
    parts = [p.strip() for p in re.split(r"(?<=[.!?])\s+", first_block) if p.strip()]
    merged = " ".join(parts[:max_sentences]).strip()
    if merged and merged[-1] not in ".!?":
        merged += "."
    if len(merged.split()) > max_words:
        merged = _compress_text(merged, max_words)
        if merged and merged[-1] not in ".!?":
            merged += "."
    return merged


def _clean_learning_objective_title(title: str | None) -> str:
    """Match brochure samples: compact heading, no trailing colon, cap length if the model over-stuffs the title."""
    t = _brochure_strip_dashes((title or "").strip())
    while t.endswith(":"):
        t = t[:-1].strip()
    return _compress_text(t, 12)


def _clean_module_title_for_table(title: str | None) -> str:
    """Remove redundant 'Module 1:' prefixes; table column already means module."""
    t = _brochure_strip_dashes(title)
    t = re.sub(r"(?i)^module\s*\d+\s*[:.)-]?\s*", "", t)
    t = re.sub(r"(?i)^module\s+[a-z]\s*[:.)-]?\s*", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def load_template() -> str:
    """
    Reads `app/templates/index.html` and returns it as a string.
    """
    try:
        return TEMPLATE_PATH.read_text(encoding="utf-8")
    except FileNotFoundError as e:
        raise RuntimeError(f"Missing PDF template file: {TEMPLATE_PATH}") from e
    except Exception as e:
        raise RuntimeError(f"Failed to read PDF template: {e}") from e


def _extract_title(outline_text: str) -> str:
    # Try "Course Title: ..." format first (Claude's standard output)
    match = re.search(r"(?im)^\s*course\s*title\s*:\s*(.+)$", outline_text or "")
    if match:
        return _clean_line(match.group(1))

    # Try markdown heading
    first_line = (outline_text or "").strip().splitlines()[0:1]
    if not first_line:
        return "COURSE OUTLINE"
    line = first_line[0].strip()
    if line.startswith("#"):
        return line.lstrip("#").strip() or "COURSE OUTLINE"
    return "COURSE OUTLINE"


def _extract_subtitle(outline_text: str) -> str:
    """Extract a subtitle/tagline from the outline text."""
    # Try explicit subtitle patterns
    for pattern in [r"(?im)^\s*sub[- ]?title\s*:\s*(.+)$", r"(?im)^\s*tagline\s*:\s*(.+)$"]:
        match = re.search(pattern, outline_text or "")
        if match:
            return _clean_line(match.group(1))

    # Use first non-empty, non-heading line after "Course Title:" as subtitle
    lines = (outline_text or "").strip().splitlines()
    found_title = False
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        low = stripped.lower()
        if "course title" in low:
            found_title = True
            continue
        if found_title and not stripped.startswith("#") and not re.match(r"(?i)^(duration|total|module|overview|training)", stripped):
            cleaned = _clean_line(stripped)
            if cleaned and len(cleaned) < 120:
                return cleaned
    return "Course Curriculum"


def _extract_duration(outline_text: str) -> str:
    match = re.search(r"(?im)^\s*duration\s*:\s*(.+)$", outline_text or "")
    return match.group(1).strip() if match else "—"


def _extract_total_hours(outline_text: str) -> str:
    match = re.search(r"(?im)^\s*(total\s*(no\.?\s*of\s*)?hours?)\s*:\s*(.+)$", outline_text or "")
    if match:
        return match.group(3).strip()

    # Fallback: detect values like "16 hours"
    fallback = re.search(r"(?im)\b(\d+\s*(?:hours?|hrs?))\b", outline_text or "")
    return fallback.group(1).strip() if fallback else "—"


def _clean_line(line: str) -> str:
    cleaned = re.sub(r'^\s*["\']+\s*', "", line.strip())
    cleaned = re.sub(r"\s*['\"]+\s*$", "", cleaned)
    cleaned = re.sub(r"^\s*[-*•]\s*", "", cleaned)
    cleaned = re.sub(r"^\s*\d+[\.\)]\s*", "", cleaned)
    cleaned = re.sub(r"\*\*(.*?)\*\*", r"\1", cleaned)
    cleaned = re.sub(r"`([^`]+)`", r"\1", cleaned)
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    return cleaned.strip()


def _compress_text(text: str, max_words: int) -> str:
    cleaned = _clean_line(text)
    if not cleaned:
        return ""
    words = cleaned.split()
    return " ".join(words[:max_words]).strip()


def _cap_activity_after_label(line: str, max_words: int = 6) -> str:
    """
    Brochure rule: at most max_words after the first colon (label: tail).
    Keeps short lines like "Simulation: resolving data issues."
    """
    s = _clean_line(line)
    if ":" not in s:
        return s
    label, sep, tail = s.partition(":")
    tail = tail.strip()
    if not tail:
        return f"{label.strip()}{sep} "
    words = tail.rstrip(".").strip().split()
    if not words:
        return f"{label.strip()}{sep} "
    capped = " ".join(words[:max_words]).rstrip(",;:")
    return f"{label.strip()}{sep} {capped}."


def _collect_section_lines(lines: list[str], heading_keywords: Iterable[str]) -> list[str]:
    """
    Collect lines after a matching markdown heading until the next heading.
    """
    keywords = [k.lower() for k in heading_keywords]
    active = False
    collected: list[str] = []

    for raw in lines:
        line = raw.rstrip()
        stripped = line.strip()
        if not stripped:
            if active:
                collected.append("")
            continue

        is_heading = stripped.startswith("#")
        if is_heading:
            normalized_heading = stripped.lstrip("#").strip().lower()
            if any(k in normalized_heading for k in keywords):
                active = True
                continue
            if active:
                break
            continue

        if active:
            collected.append(stripped)

    return [line for line in collected if line.strip()]


def _extract_overview(outline_text: str) -> str:
    lines = (outline_text or "").splitlines()
    overview_lines = _collect_section_lines(lines, ("overview", "course overview"))
    if overview_lines:
        return "\n".join(overview_lines).strip()

    # Fallback: first non-heading paragraph block after title.
    body_lines: list[str] = []
    for raw in lines:
        stripped = raw.strip()
        if not stripped:
            if body_lines:
                break
            continue
        if stripped.startswith("#"):
            continue
        body_lines.append(stripped)
        if len(body_lines) >= 8:
            break
    fallback = "\n".join(body_lines).strip()
    if fallback:
        return fallback

    # Final fallback: keep visible content even if headings are unusual.
    return (outline_text or "").strip()


def _extract_bullets_from_section(outline_text: str, heading_keywords: Iterable[str]) -> list[str]:
    lines = (outline_text or "").splitlines()
    section_lines = _collect_section_lines(lines, heading_keywords)
    bullets: list[str] = []
    for line in section_lines:
        cleaned = _clean_line(line)
        if cleaned:
            bullets.append(cleaned)
    return bullets


def _extract_modules(outline_text: str) -> list[str]:
    lines = (outline_text or "").splitlines()
    modules: list[str] = []

    # First choice: explicit "modules" section
    in_modules_section = False
    for raw in lines:
        stripped = raw.strip()
        if not stripped:
            continue

        if stripped.startswith("#"):
            heading = stripped.lstrip("#").strip().lower()
            if "module" in heading:
                in_modules_section = True
                continue
            if in_modules_section:
                break
            continue

        if in_modules_section:
            cleaned = _clean_line(stripped)
            if cleaned:
                modules.append(cleaned)

    if modules:
        return modules[:20]

    # Fallback: lines that look like "Module 1: ..."
    for raw in lines:
        stripped = raw.strip()
        if re.match(r"(?i)^module\s*\d+\s*[:\-]", stripped):
            modules.append(stripped)
    return modules[:20]


def _extract_conclusion(outline_text: str) -> str:
    lines = (outline_text or "").splitlines()
    conclusion_lines = _collect_section_lines(lines, ("conclusion",))
    if conclusion_lines:
        return "\n".join(conclusion_lines).strip()
    return ""


def _replace_inner_html_by_id(html: str, element_id: str, new_inner_html: str) -> str:
    """
    Replace inner HTML for an element with an id.
    Uses a conservative regex to avoid breaking the template structure.
    """
    pattern = re.compile(
        rf'(<(?P<tag>[a-zA-Z0-9]+)\b[^>]*\bid="{re.escape(element_id)}"[^>]*>)(?P<inner>.*?)(</(?P=tag)>)',
        re.DOTALL,
    )
    updated, count = pattern.subn(rf"\g<1>{new_inner_html}\g<4>", html, count=1)
    if count == 0:
        raise RuntimeError(f"Template missing element id='{element_id}'")
    return updated


def _build_list_html(items: list[str]) -> str:
    return "".join(f"<li>{escape(item)}</li>" for item in items if item.strip())


def _build_modules_html(modules: list[str]) -> str:
    blocks: list[str] = []
    for module in modules:
        parts = module.split(":", 1)
        if len(parts) == 2:
            left, right = parts[0].strip(), parts[1].strip()
            blocks.append(
                '<div style="margin-bottom:16px">'
                f'<div style="font-size:16px"><span style="font-weight:700;color:var(--teal)">{escape(left)}:</span> {escape(right)}</div>'
                "</div>"
            )
        else:
            blocks.append(
                '<div style="margin-bottom:16px">'
                f'<div style="font-size:16px">{escape(module)}</div>'
                "</div>"
            )
    return "".join(blocks)


def _render_bold_markdown_to_html(text: str) -> str:
    escaped = escape(text)
    return re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", escaped)


_MODULE_EXERCISE_LABEL = re.compile(
    r"^\s*((?:Exercise|Case study|Simulation|Hands-on|Role-play)\s*:\s*)",
    re.IGNORECASE,
)


def _render_module_exercise_line_html(text: str) -> str:
    """
    Training Modules / Exercises column: bold the label (Exercise:, Case study:, etc.), normal weight after.
    Matches brochure reference styling.
    """
    s = (text or "").strip()
    if not s:
        return ""
    m = _MODULE_EXERCISE_LABEL.match(s)
    if not m:
        return _render_bold_markdown_to_html(s)
    prefix = m.group(1).strip()
    rest = s[m.end() :].strip()
    head = f"<strong>{escape(prefix)}</strong>"
    if rest:
        return f"{head} {escape(rest)}"
    return head


def _build_cover_title_html(title: str) -> str:
    """
    Keep cover heading visually aligned for short and long titles.
    """
    normalized = (title or "COURSE OUTLINE").strip()
    safe_title = escape(normalized)
    length = len(safe_title)
    if length > 90:
        size_px = 30
    elif length > 72:
        size_px = 34
    elif length > 56:
        size_px = 38
    else:
        size_px = 44
    words = normalized.split()
    if len(words) >= 2:
        lead = escape(" ".join(words[:-1]))
        tail = escape(words[-1])
        title_with_dot = (
            f'{lead} <span style="white-space:nowrap;">{tail}<span class="dot"></span></span>'
        )
    else:
        title_with_dot = f'{safe_title}<span class="dot"></span>'

    return (
        f'<span style="display:inline-block;font-size:{size_px}px;line-height:1.05;'
        'letter-spacing:-0.5px;white-space:normal;overflow-wrap:anywhere;">'
        f"{title_with_dot}"
        "</span>"
    )


def _build_cover_title_text(raw_title: str) -> str:
    """
    Cover main line. Only split on colon ":" so hyphenated program names stay intact.
    Example: "Data Analytics - Power BI for HR" stays one title (not split into title + subtitle).
    Use "Title: Tagline" only when the tagline is truly separate after a colon.
    """
    base = _clean_line(raw_title or "COURSE OUTLINE")
    if not base:
        return "COURSE OUTLINE"

    # Split only on colon — not on hyphen (hyphens are common inside course names).
    split_match = re.split(r"\s*:\s*", base, maxsplit=1)
    primary = split_match[0].strip() if split_match else base

    # Remove bracketed suffix noise from title line.
    primary = re.sub(r"\s*\([^)]*\)\s*", " ", primary).strip()

    # Keep heading short and readable on cover.
    words = primary.split()
    if len(words) > 12:
        primary = " ".join(words[:12]).strip()
    if len(primary) > 80:
        primary = primary[:80].rstrip()

    return primary or "COURSE OUTLINE"


def _build_cover_subtitle_text(raw_title: str, current_subtitle: str = "") -> str:
    """
    Subtitle: explicit subtitle, or text after a single colon in course_title, or generic fallback.
    Hyphens in the title do not create a subtitle (see _build_cover_title_text).
    """
    subtitle = _clean_line(current_subtitle)
    if subtitle and subtitle.lower() != "course curriculum":
        return subtitle[:90]

    raw = _clean_line(raw_title)
    # Only "Title: tagline" uses colon — not hyphen.
    split_match = re.split(r"\s*:\s*", raw, maxsplit=1)
    if len(split_match) == 2:
        rhs = split_match[1].strip()
        if rhs:
            return rhs[:90]

    bracket = re.search(r"\(([^)]{2,40})\)", raw)
    if bracket:
        return f"Certification preparation for {bracket.group(1).strip()}."
    return "Certification preparation for delivery-ready implementation."


def _format_insight_html(text: str) -> str:
    blocks = re.split(r"\n\s*\n", (text or "").strip())
    out: list[str] = []
    for block in blocks:
        lines = [_clean_line(l) for l in block.splitlines() if l.strip()]
        if not lines:
            continue
        bullet_lines = [l for l in lines if re.match(r"^(?:[-*•]|\d+[.)])\s*", l)]
        if bullet_lines and len(bullet_lines) == len(lines):
            items = []
            for item in lines:
                cleaned = re.sub(r"^(?:[-*•]|\d+[.)])\s*", "", item).strip()
                if cleaned:
                    items.append(f'<li class="insight-bullet-item">{_render_bold_markdown_to_html(cleaned)}</li>')
            out.append('<ul class="insight-bullets">' + "".join(items) + "</ul>")
        else:
            merged = "<br>".join(_render_bold_markdown_to_html(l) for l in lines)
            out.append(f'<p class="insight-para">{merged}</p>')
    return "".join(out)


def _extract_details_value(outline_text: str, labels: Iterable[str], default: str = "—") -> str:
    for label in labels:
        match = re.search(rf"(?im)^\s*{re.escape(label)}\s*:\s*(.+)$", outline_text or "")
        if match:
            return _clean_line(match.group(1))
    return default


def _extract_objective_items(outline_text: str) -> list[tuple[str, str]]:
    lines = (outline_text or "").splitlines()
    # Find the objectives section
    section_lines = _collect_section_lines(lines, ("objective", "objectives", "learning objectives", "learning objective"))
    items: list[tuple[str, str]] = []

    i = 0
    while i < len(section_lines):
        line = section_lines[i].strip()
        if not line:
            i += 1
            continue

        # Pattern: "a. Head Title" on its own line, followed by description on next line(s)
        letter_match = re.match(r"^([a-z])\.\s+(.+)$", line)
        if letter_match:
            head = _clean_line(letter_match.group(2))
            desc_parts = []
            # Collect following non-lettered lines as description
            j = i + 1
            while j < len(section_lines):
                next_line = section_lines[j].strip()
                if not next_line:
                    j += 1
                    continue
                if re.match(r"^[a-z]\.\s+", next_line):
                    break
                desc_parts.append(_clean_line(next_line))
                j += 1
            desc = " ".join(desc_parts)
            items.append((head, desc))
            i = j
            continue

        # Pattern: "- Head: description" or "1. Head: description"
        cleaned = _clean_line(line)
        if not cleaned:
            i += 1
            continue
        sep_match = re.search(r"\s*[:\-–—]\s*", cleaned)
        if sep_match:
            idx = sep_match.start()
            head = cleaned[:idx].strip()
            desc = cleaned[sep_match.end():].strip()
            if head and desc and len(head) < 80:
                items.append((head, desc))
                i += 1
                continue
        items.append((cleaned, ""))
        i += 1

    return items[:12]


def _extract_impact_items(outline_text: str) -> list[tuple[str, str]]:
    # Collect lines from multiple possible impact/ROI section headings
    text = outline_text or ""
    all_lines: list[str] = []

    for keyword_group in [
        ("immediate impact", "short-term impact"),
        ("impact", "roi", "return on investment", "business impact"),
        ("capability impact",),
    ]:
        found = _collect_section_lines(text.splitlines(), keyword_group)
        if found:
            all_lines = found
            break

    # Fallback: search whole text for bold-titled bullet lines
    if not all_lines:
        all_lines = text.splitlines()

    items: list[tuple[str, str]] = []
    i = 0
    while i < len(all_lines):
        line = all_lines[i].strip()
        if not line:
            i += 1
            continue
        # Match lines that are bold headings (e.g. "**Measurable performance improvement**")
        bold_match = re.match(r"^\*\*(.+?)\*\*$", line)
        if bold_match:
            head = _clean_line(bold_match.group(1))
            desc_parts = []
            j = i + 1
            while j < len(all_lines):
                next_line = all_lines[j].strip()
                if not next_line:
                    j += 1
                    break
                if re.match(r"^\*\*", next_line) or re.match(r"^#+\s", next_line):
                    break
                desc_parts.append(_clean_line(next_line))
                j += 1
            desc = " ".join(d for d in desc_parts if d)
            if head:
                items.append((head, desc))
            i = j
            continue

        # Match bullet lines with "Head: desc" pattern
        if re.match(r"^\s*(?:[-*•]|\d+[.)])\s*", line):
            text_part = _clean_line(line)
            if not text_part:
                i += 1
                continue
            colon_parts = text_part.split(":", 1)
            if len(colon_parts) == 2:
                items.append((colon_parts[0].strip(), colon_parts[1].strip()))
            else:
                items.append((text_part, ""))
        i += 1

    return items[:10]


def _extract_module_records(outline_text: str) -> list[dict[str, list[str] | str]]:
    lines = (outline_text or "").splitlines()
    records: list[dict[str, list[str] | str]] = []
    current: dict[str, list[str] | str] | None = None
    mode = "topics"
    for raw in lines:
        stripped = raw.strip()
        if not stripped:
            continue
        m = re.match(r"(?i)^#+\s*module\s*(\d+)\s*[:\-]\s*(.+)$", stripped) or re.match(
            r"(?i)^module\s*(\d+)\s*[:\-]\s*(.+)$", stripped
        )
        if m:
            if current:
                records.append(current)
            module_num = m.group(1)
            module_name = _compress_text(m.group(2), 12)
            current = {"name": module_name, "topics": [], "exercises": []}
            mode = "topics"
            continue
        if current is None:
            continue
        low = stripped.lower()
        if "topics covered" in low:
            mode = "topics"
            continue
        if low.startswith("activity"):
            mode = "exercises"
            continue
        if re.match(r"^\s*(?:[-*•]|\d+[.)])\s*", stripped):
            cleaned = _compress_text(stripped, 12 if mode == "topics" else 15)
            if cleaned:
                key = "exercises" if mode == "exercises" else "topics"
                casted = current[key]
                if isinstance(casted, list):
                    casted.append(cleaned)
    if current:
        records.append(current)
    # Safety limits to keep the module table readable
    limited_records: list[dict[str, list[str] | str]] = []
    for rec in records[:6]:
        topics = rec.get("topics", [])
        exercises = rec.get("exercises", [])
        limited_records.append(
            {
                "name": _compress_text(str(rec.get("name", "Module")), 12),
                "topics": [_compress_text(str(t), 12) for t in (topics if isinstance(topics, list) else [])][:6],
                "exercises": [_compress_text(str(e), 15) for e in (exercises if isinstance(exercises, list) else [])][:4],
            }
        )
    return limited_records


def _build_objective_html(items: list[tuple[str, str]], intro: str, closing: str) -> str:
    alpha = "abcdefghijklmnopqrstuvwxyz"
    blocks = ['<div class="obj-card">']
    if intro:
        blocks.append(f'<p class="obj-intro">{_render_bold_markdown_to_html(intro)}</p>')
    if items:
        blocks.append('<div class="obj-list">')
        for idx, (head, desc) in enumerate(items):
            prefix = alpha[idx] if idx < len(alpha) else str(idx + 1)
            blocks.append('<div class="obj-item">')
            blocks.append(
                f'<div class="obj-item-head">{escape(prefix)}. {_render_bold_markdown_to_html(head)}</div>'
            )
            if desc:
                blocks.append(f'<div class="obj-item-desc">{_render_bold_markdown_to_html(desc)}</div>')
            blocks.append("</div>")
        blocks.append("</div>")
    if closing:
        for para in closing.split("\n\n"):
            p = para.strip()
            if p:
                blocks.append(f'<p class="obj-closing">{_render_bold_markdown_to_html(p)}</p>')
    blocks.append("</div>")
    return "".join(blocks)


def _build_impact_html(items: list[tuple[str, str]], intro: str, closing: str) -> str:
    rows: list[str] = []
    if intro:
        rows.append(f'<p class="impact-intro">{_render_bold_markdown_to_html(intro)}</p>')
    for head, desc in items:
        rows.append(
            '<div class="impact-row">'
            '<div class="impact-icon-box"><svg class="impact-check" viewBox="0 0 24 24" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg></div>'
            '<div>'
            f'<div class="impact-row-title">{_render_bold_markdown_to_html(head)}</div>'
            f'{"<div class=\"impact-row-desc\">" + _render_bold_markdown_to_html(desc) + "</div>" if desc else ""}'
            "</div>"
            "</div>"
        )
    if closing:
        for para in closing.split("\n\n"):
            p = para.strip()
            if p:
                rows.append(f'<p class="impact-closing">{_render_bold_markdown_to_html(p)}</p>')
    return "".join(rows)


def _build_dynamic_module_pages(records: list[dict[str, list[str] | str]]) -> str:
    if not records:
        return ""
    pages: list[str] = []
    usable_height = 800
    thead_height = 42
    min_rows_per_page = 2

    def estimate_row_height(rec: dict[str, list[str] | str]) -> int:
        """
        Conservative row-height estimate so rows move to next page before clipping.
        We intentionally over-estimate to avoid bottom cut-offs in generated PDFs.
        """
        name = str(rec.get("name", ""))
        topics = rec.get("topics", [])
        exercises = rec.get("exercises", [])
        topics_list = topics if isinstance(topics, list) else []
        exercises_list = exercises if isinstance(exercises, list) else []

        # Approx wrapped line count by character length.
        # Use less conservative values so table can keep multiple rows per page.
        def line_units(text: str, chars_per_line: int = 42) -> int:
            t = str(text or "").strip()
            if not t:
                return 0
            return max(1, (len(t) + chars_per_line - 1) // chars_per_line)

        units = 0
        units += line_units(name, 20)  # module column is narrower
        for t in topics_list:
            units += line_units(str(t), 42) + 1  # +1 bullet padding
        for e in exercises_list:
            units += line_units(str(e), 42) + 1

        # Moderately conservative baseline + per-line height
        return 68 + (units * 8)

    def render_rows(chunk: list[dict[str, list[str] | str]], base_idx: int) -> str:
        rows: list[str] = []
        for offset, rec in enumerate(chunk, start=0):
            idx = base_idx + offset
            name = escape(str(rec.get("name", f"Module {idx}")))
            topics = rec.get("topics", [])
            exercises = rec.get("exercises", [])
            topics_html = "".join(
                f"<li>{escape(str(t).strip())}</li>"
                for t in (topics if isinstance(topics, list) else [])
                if str(t).strip()
            )
            ex_html = "".join(
                f"<li>{_render_module_exercise_line_html(str(e))}</li>"
                for e in (exercises if isinstance(exercises, list) else [])
                if str(e).strip()
            )
            rows.append(
                "<tr>"
                f'<td class="col-sno">{idx:02d}</td>'
                f'<td class="col-mod">{name}</td>'
                f'<td class="col-top"><ul>{topics_html}</ul></td>'
                f'<td class="col-ex"><ul>{ex_html}</ul></td>'
                "</tr>"
            )
        return "".join(rows)

    page_number = 5
    current_chunk: list[dict[str, list[str] | str]] = []
    current_start_idx = 1
    used_height = thead_height

    def flush_chunk(chunk: list[dict[str, list[str] | str]], start_idx: int) -> None:
        nonlocal page_number
        if not chunk:
            return
        rows_html = render_rows(chunk, start_idx)
        pages.append(
            '<section class="page page-fixed bg-modules">'
            '<div class="modules-wrap"><table class="mod-table"><thead><tr>'
            '<th class="col-sno">Sno.</th><th class="col-mod">Modules</th><th class="col-top">Topics</th><th class="col-ex">Exercises</th>'
            "</tr></thead><tbody>"
            + rows_html
            + f'</tbody></table></div><div class="page-num-overlay">Page {page_number:02d}</div></section>'
        )
        page_number += 1

    for idx, rec in enumerate(records, start=1):
        row_h = estimate_row_height(rec)
        if current_chunk and (used_height + row_h > usable_height):
            # Prefer at least 2 rows/page when feasible; avoid one-row pages.
            if len(current_chunk) < min_rows_per_page and used_height < usable_height + 40:
                current_chunk.append(rec)
                used_height += row_h
                continue
            flush_chunk(current_chunk, current_start_idx)
            current_chunk = []
            current_start_idx = idx
            used_height = thead_height

        current_chunk.append(rec)
        used_height += row_h

    flush_chunk(current_chunk, current_start_idx)

    return "".join(pages)


# Brochure activity lines: Exercise / Case study / Simulation / Hands-on / Role-play
_ACTIVITY_LABEL_OK = re.compile(
    r"(?i)^(case study|exercise|simulation|hands-on|role-play)\s*:",
)


def _collect_payload_module_exercises(module: dict[str, object]) -> list[str]:
    """
    Merge exercises + case_studies + simulations slots into the three table lines (order preserved).
    Lines should already include a label; Role-play and Hands-on are first-class.
    """
    out: list[str] = []

    def _push(raw: str, default_if_unlabeled: str) -> None:
        s = _brochure_strip_dashes(str(raw).strip())
        if not s:
            return
        line = s if _ACTIVITY_LABEL_OK.match(s) else f"{default_if_unlabeled}: {s}"
        out.append(_cap_activity_after_label(line, 6))

    for a in module.get("exercises") or []:
        _push(a, "Exercise")
    if len(out) >= 3:
        return out[:8]
    for a in module.get("case_studies") or []:
        _push(a, "Case study")
    for a in module.get("simulations") or []:
        _push(a, "Simulation")
    for a in module.get("activities") or []:
        _push(a, "Hands-on")
    return out[:8]


def _build_dynamic_module_pages_from_payload(modules: list[dict[str, object]]) -> str:
    records: list[dict[str, list[str] | str]] = []
    for module in modules[:12]:
        title = _compress_text(_clean_module_title_for_table(str(module.get("module_title", ""))), 16)
        topics = [
            _compress_text(_brochure_strip_dashes(str(t)), 8)
            for t in (module.get("topics", []) or [])
            if str(t).strip()
        ][:8]
        activity_pool = _collect_payload_module_exercises(module)
        records.append({"name": title or "Module", "topics": topics, "exercises": activity_pool})
    return _build_dynamic_module_pages(records)


def inject_content_from_structured_payload(html: str, payload: CourseOutlinePayload) -> str:
    insight_html_parts: list[str] = []
    for p in payload.program_insight.paragraphs:
        ps = _slim_insight_paragraph(p)
        if ps:
            insight_html_parts.append(f'<p class="insight-para">{_render_bold_markdown_to_html(ps)}</p>')
    if payload.program_insight.bullets:
        bullets = "".join(
            f'<li class="insight-bullet-item">{_render_bold_markdown_to_html(_slim_insight_bullet(b))}</li>'
            for b in payload.program_insight.bullets
            if str(b).strip()
        )
        insight_html_parts.append(f'<ul class="insight-bullets">{bullets}</ul>')
    insight_html = "".join(insight_html_parts) or '<p class="insight-para">Content pending.</p>'

    lo_intro = _brochure_strip_dashes((payload.learning_objectives_intro or "").strip())
    lo_closing = _brochure_strip_dashes((payload.learning_objectives_closing or "").strip())
    objective_rows: list[tuple[str, str]] = []
    for o in payload.learning_objectives:
        t = _clean_learning_objective_title(o.title)
        d = _brochure_strip_dashes((o.description or "").strip())
        if d:
            d = _compress_text(d, 18)
        objective_rows.append((t, d))
    objective_html = _build_objective_html(
        objective_rows,
        intro=lo_intro
        or (
            "This program focuses on enhancing participants' ability to apply new skills in a real work context. "
            "It is designed to build on existing knowledge with clear methods and practical exercises. "
            "Participants will learn to structure work effectively and produce outcomes that support decisions. "
            "The objective is to enable confident use of what they learn after the program ends."
        ),
        closing=lo_closing
        or (
            "By the end of the program, participants will be equipped to apply methods independently in their roles. "
            "They will gain hands-on experience in scenarios that mirror real workflows, ensuring immediate applicability. "
            "The training emphasizes **practical application and analytical confidence**, moving beyond basic familiarity.\n\n"
            "Participants will also strengthen how they interpret and present insights to stakeholders. "
            "The program supports **job-ready skills and improved decision-making capability** for organizational goals. "
            "Learners will be better prepared to use analytics as part of day-to-day responsibilities."
        ),
    )
    raw_ci = (payload.capability_impact_intro or "").strip()
    ci_intro = _brochure_details_table_blurb(raw_ci, max_sentences=2, max_words=58) if raw_ci else ""
    raw_cc = (payload.capability_impact_closing or "").strip()
    ci_closing = _slim_capability_closing(raw_cc) if raw_cc else ""
    impact_rows: list[tuple[str, str]] = []
    for i in payload.capability_impact:
        impact_rows.append(
            (
                _brochure_strip_dashes(i.title),
                _slim_capability_row_description(i.description),
            )
        )
    impact_html = _build_impact_html(
        impact_rows,
        intro=ci_intro
        or (
            "This program strengthens how teams apply new skills in real workflows and measure results. "
            "Outcomes depend on reinforcement, leadership support, and consistent use of insights after training."
        ),
        closing=ci_closing
        or (
            "The shift from ad hoc reporting to repeatable analytics is a meaningful upgrade for people and planning. "
            "**Sustainable performance** grows when teams keep using what they learned in day-to-day decisions."
        ),
    )
    modules_html = _build_dynamic_module_pages_from_payload(
        [
            {
                "module_title": m.module_title,
                "topics": m.topics,
                "exercises": m.exercises,
                "case_studies": m.case_studies,
                "simulations": m.simulations,
                "activities": m.activities,
            }
            for m in payload.modules
        ]
    )

    display_title = _build_cover_title_text(payload.course_title)
    display_subtitle = _build_cover_subtitle_text(payload.course_title)

    updated = html
    updated = _replace_inner_html_by_id(updated, "pTitle", _build_cover_title_html(display_title))
    updated = _replace_inner_html_by_id(updated, "pSubtitle", escape(display_subtitle))
    updated = _replace_inner_html_by_id(updated, "pDuration", escape(_brochure_strip_dashes(payload.duration or "TBC")))
    updated = _replace_inner_html_by_id(updated, "pInsight", insight_html)
    det_summary = _details_page_summary_from_insight(payload.program_insight.paragraphs)
    updated = _replace_inner_html_by_id(updated, "pDetSummary", _render_bold_markdown_to_html(det_summary))
    updated = _replace_inner_html_by_id(
        updated,
        "pRegions",
        escape(_brochure_strip_dashes(REGIONS_SERVED_CONSTANT)),
    )
    updated = _replace_inner_html_by_id(
        updated,
        "pDetDuration",
        escape(_brochure_strip_dashes(payload.course_details.course_duration or payload.duration or "TBC")),
    )
    updated = _replace_inner_html_by_id(
        updated,
        "pHours",
        escape(_brochure_strip_dashes(payload.course_details.total_learning_hours or payload.total_hours or "TBC")),
    )
    updated = _replace_inner_html_by_id(
        updated,
        "pBenefits",
        escape(
            _brochure_details_table_blurb(payload.course_details.key_benefits or "See program overview.")
        ),
    )
    updated = _replace_inner_html_by_id(
        updated,
        "pValue",
        escape(
            _brochure_details_table_blurb(payload.course_details.value_addition or "See program overview.")
        ),
    )
    updated = _replace_inner_html_by_id(updated, "pLocation", escape(payload.course_details.location or "To be confirmed"))
    updated = _replace_inner_html_by_id(updated, "pDatetime", escape(payload.course_details.date_time or "To be confirmed"))
    updated = _replace_inner_html_by_id(updated, "pObjective", objective_html)
    updated = _replace_inner_html_by_id(updated, "pImpact", impact_html)
    updated = _replace_inner_html_by_id(updated, "dynamicModules", modules_html)
    return updated


def inject_content_into_template(html: str, outline_text: str) -> str:
    """
    Simple v1 injection:
    - Extract title from first line (`# Title`) and inject into `pCoverTitle2`
    - Put the full outline into `pOverview` as temporary overview
    """
    title = _extract_title(outline_text)
    subtitle = _extract_subtitle(outline_text)

    overview = _extract_overview(outline_text)
    objectives = _extract_bullets_from_section(outline_text, ("objective", "objectives"))
    outcomes = _extract_bullets_from_section(outline_text, ("outcome", "outcomes", "key outcomes"))
    modules = _extract_modules(outline_text)
    module_records = _extract_module_records(outline_text)
    duration = _extract_duration(outline_text)
    total_hours = _extract_total_hours(outline_text)
    conclusion = _extract_conclusion(outline_text)
    detail_summary = _extract_details_value(
        outline_text,
        ("Business Impact", "Expected Business Impact & Returns", "Course Overview"),
        default=overview,
    )
    regions = _extract_details_value(outline_text, ("Regions Served",), default="Global")
    location = _extract_details_value(outline_text, ("Location", "Venue"), default="To be confirmed")
    date_time = _extract_details_value(outline_text, ("Date & Time", "Date and Time"), default="To be confirmed")
    benefits = _extract_details_value(outline_text, ("Key Benefits",), default="Improved capability and role readiness")
    value_add = _extract_details_value(outline_text, ("Value Addition & Impact", "Value Addition"), default="Measurable operational and performance uplift")

    insight_html = _format_insight_html(overview + (f"\n\n{conclusion}" if conclusion else ""))
    objective_items = _extract_objective_items(outline_text)
    objective_html = _build_objective_html(
        objective_items,
        intro="By the end of this program, participants will be able to:",
        closing="These objectives align to the target role and business context.",
    )
    impact_items = _extract_impact_items(outline_text)
    impact_html = _build_impact_html(
        impact_items,
        intro="Expected capability and business impact from this program:",
        closing="Final outcomes depend on adoption, reinforcement, and execution discipline.",
    )
    modules_html = _build_dynamic_module_pages(module_records)

    display_title = _build_cover_title_text(title)
    display_subtitle = _build_cover_subtitle_text(title, subtitle)

    updated = html
    updated = _replace_inner_html_by_id(updated, "pTitle", _build_cover_title_html(display_title))
    updated = _replace_inner_html_by_id(updated, "pSubtitle", escape(display_subtitle))
    updated = _replace_inner_html_by_id(updated, "pDuration", escape(duration))
    updated = _replace_inner_html_by_id(updated, "pInsight", insight_html)
    updated = _replace_inner_html_by_id(updated, "pDetSummary", _render_bold_markdown_to_html(detail_summary))
    updated = _replace_inner_html_by_id(updated, "pRegions", escape(regions))
    updated = _replace_inner_html_by_id(updated, "pDetDuration", escape(duration))
    updated = _replace_inner_html_by_id(updated, "pHours", escape(total_hours))
    updated = _replace_inner_html_by_id(updated, "pBenefits", escape(benefits))
    updated = _replace_inner_html_by_id(updated, "pValue", escape(value_add))
    updated = _replace_inner_html_by_id(updated, "pLocation", escape(location))
    updated = _replace_inner_html_by_id(updated, "pDatetime", escape(date_time))
    updated = _replace_inner_html_by_id(updated, "pObjective", objective_html)
    updated = _replace_inner_html_by_id(updated, "pImpact", impact_html)
    updated = _replace_inner_html_by_id(updated, "dynamicModules", modules_html)

    # Keep old extractors referenced so we can quickly fallback if prompt format changes.
    _ = objectives, outcomes, modules
    return updated


def _ensure_base_href(html: str, base_dir: Path) -> str:
    """
    Ensure relative assets (pngs) resolve by injecting a <base href="file:///.../"> tag.
    """
    if re.search(r"<base\\b", html, flags=re.IGNORECASE):
        return html

    base_href = base_dir.resolve().as_uri().rstrip("/") + "/"
    base_tag = f'<base href="{base_href}">'

    updated, count = re.subn(r"</head\\s*>", base_tag + "\n</head>", html, flags=re.IGNORECASE, count=1)
    if count == 0:
        # Extremely defensive; template should always have </head>
        return base_tag + "\n" + html
    return updated


def _strip_scripts_for_pdf(html: str) -> str:
    """
    Remove script tags so template JS doesn't overwrite server-injected content.
    """
    return re.sub(r"<script\b[^>]*>.*?</script>", "", html, flags=re.IGNORECASE | re.DOTALL)


def _generate_pdf_with_playwright_sync(html_content: str, file_path: str) -> None:
    """
    Render HTML -> PDF using Playwright (sync API).

    Note: Python 3.14 on Windows can raise NotImplementedError for asyncio subprocess
    transports. The sync API avoids that path and is more reliable here.
    """
    from playwright.sync_api import sync_playwright
    temp_html_path: Path | None = None
    try:
        # Use a real file URL so all local assets from templates resolve reliably.
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".html",
            prefix="render_",
            dir=str(TEMPLATE_DIR),
            delete=False,
        ) as temp_file:
            temp_file.write(html_content)
            temp_html_path = Path(temp_file.name)

        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            page.goto(temp_html_path.resolve().as_uri(), wait_until="networkidle")
            page.pdf(path=file_path, format="A4", print_background=True)
            browser.close()
    finally:
        if temp_html_path and temp_html_path.exists():
            try:
                temp_html_path.unlink()
            except OSError:
                # Non-fatal cleanup failure.
                pass


async def generate_pdf_path_async(outline_text: str | CourseOutlinePayload, version: int = 1) -> str:
    """
    Async-friendly wrapper for API usage (does not require threads).
    """
    template_html = load_template()
    if isinstance(outline_text, CourseOutlinePayload):
        final_html = inject_content_from_structured_payload(template_html, outline_text)
    else:
        # Prefer structured rendering even when outline is provided as JSON text.
        parsed_payload: CourseOutlinePayload | None = None
        try:
            candidate = json.loads(outline_text or "")
            if isinstance(candidate, dict):
                parsed_payload = CourseOutlinePayload(**candidate)
        except Exception:
            parsed_payload = None
        if parsed_payload is not None:
            final_html = inject_content_from_structured_payload(template_html, parsed_payload)
        else:
            final_html = inject_content_into_template(template_html, outline_text)
    final_html = _ensure_base_href(final_html, TEMPLATE_DIR)
    final_html = _strip_scripts_for_pdf(final_html)

    file_name = f"{uuid.uuid4()}.pdf"
    file_path = os.path.join(OUTPUT_DIR, file_name)

    await asyncio.to_thread(_generate_pdf_with_playwright_sync, final_html, file_path)
    logger.info(f"PDF generated: {file_path}")
    return file_path


def generate_pdf(outline_text: str, version: int = 1) -> str:
    """
    Generate a PDF using the designed `app/templates/index.html`.
    Returns the file path (relative to project root).
    """
    try:
        # Sync wrapper for scripts/one-off usage.
        return asyncio.run(generate_pdf_path_async(outline_text, version=version))
    except OSError as e:
        logger.exception("PDF generation failed (OS error)")
        raise RuntimeError(f"Failed to generate PDF: {e}") from e
    except Exception as e:
        logger.exception("PDF generation failed")
        raise RuntimeError(f"Failed to generate PDF: {e}")

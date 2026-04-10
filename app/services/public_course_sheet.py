"""
Fetch public course catalog rows from a Google Sheet published as CSV.

Use a stable export URL (not the /edit UI link):
  https://docs.google.com/spreadsheets/d/<SHEET_ID>/export?format=csv&gid=<TAB_GID>

The sheet should be shared so "Anyone with the link" can view, or unauthenticated export may fail.
"""

from __future__ import annotations

import csv
import io
import re
from typing import Optional
from urllib.parse import parse_qs, urlparse

import httpx

from app.core.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)

_SHEET_ID_RE = re.compile(r"/spreadsheets/d/([a-zA-Z0-9-_]+)")


def normalize_google_sheet_csv_export_url(raw: str) -> str:
    """
    Turn a normal Sheets browser URL into a CSV export URL on docs.google.com
    (avoids saving fragile googleusercontent.com redirect targets).
    """
    raw = (raw or "").strip()
    if not raw:
        return ""

    # Strip fragment for parsing, but recover gid from #gid= later
    fragment = ""
    if "#" in raw:
        raw, fragment = raw.split("#", 1)

    parsed = urlparse(raw)
    if "docs.google.com" not in (parsed.netloc or "").lower():
        return raw.strip()

    path_lower = (parsed.path or "").lower()
    query_lower = (parsed.query or "").lower()
    # Published Sheets URL format:
    # https://docs.google.com/spreadsheets/d/e/<pub_id>/pub?output=csv
    # Keep it as-is (already stable and directly downloadable).
    if "/spreadsheets/d/e/" in path_lower and "output=csv" in query_lower:
        return raw.strip()

    m = _SHEET_ID_RE.search(parsed.path or "")
    if not m:
        return raw.strip()

    sheet_id = m.group(1)
    qs = parse_qs(parsed.query)
    gid = None
    if "gid" in qs and qs["gid"]:
        gid = str(qs["gid"][0]).strip()
    if not gid and fragment:
        fm = re.search(r"gid=(\d+)", fragment)
        if fm:
            gid = fm.group(1)
    if not gid:
        gid = "0"

    if "/export" in path_lower and "format=csv" in (parsed.query or "").lower():
        base = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export"
        q = f"format=csv&gid={gid}"
        return f"{base}?{q}"

    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"


def _normalize_course_key(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _looks_like_url(s: str) -> bool:
    t = (s or "").strip()
    return t.startswith("http://") or t.startswith("https://")


def _detect_column_indices(
    header: list[str],
    *,
    course_column: str,
    pdf_column: str,
) -> tuple[Optional[int], Optional[int]]:
    """Return (course_idx, pdf_idx) using explicit names or fuzzy header match."""

    def norm_cell(h: str) -> str:
        return re.sub(r"[^a-z0-9]+", "_", (h or "").strip().lower()).strip("_")

    normed = [norm_cell(h) for h in header]

    course_idx: Optional[int] = None
    pdf_idx: Optional[int] = None

    ec = norm_cell(course_column) if course_column else ""
    ep = norm_cell(pdf_column) if pdf_column else ""

    if ec:
        for i, n in enumerate(normed):
            if n == ec or n.endswith("_" + ec) or ec.endswith("_" + n):
                course_idx = i
                break
        if course_idx is None:
            for i, h in enumerate(header):
                if ec == norm_cell(h):
                    course_idx = i
                    break

    if ep:
        for i, n in enumerate(normed):
            if n == ep or n.endswith("_" + ep):
                pdf_idx = i
                break
        if pdf_idx is None:
            for i, h in enumerate(header):
                if ep == norm_cell(h):
                    pdf_idx = i
                    break

    if course_idx is None:
        for i, n in enumerate(normed):
            if n in {"course_name", "course", "program", "title", "name", "product_name", "product"}:
                course_idx = i
                break
            if "course" in n and "pdf" not in n and "link" not in n:
                course_idx = i
                break
            if n in {"product_name", "program_name"}:
                course_idx = i
                break

    if pdf_idx is None:
        for i, n in enumerate(normed):
            if n in {
                "pdf_url",
                "outline_pdf",
                "course_outline_pdf",
                "pdf_link",
                "pdf",
                "outline_pdf_link",
                "formatted_curriculum",
                "final_formatted_curriculum",
                "formatted_curriculum_url",
                "final_formatted_curriculum_url",
            }:
                pdf_idx = i
                break
        if pdf_idx is None:
            for i, n in enumerate(normed):
                if (
                    "pdf" in n
                    or "formatted_curriculum" in n
                    or "curriculum_link" in n
                    or "curriculum_url" in n
                    or n in {"link", "url"}
                ):
                    pdf_idx = i
                    break

    # Two-column sheet without clear headers: first = title, second = link
    if course_idx is None and pdf_idx is None and len(header) >= 2:
        course_idx, pdf_idx = 0, 1

    return course_idx, pdf_idx


def find_pdf_url_for_course(csv_text: str, course_name: str) -> Optional[str]:
    """
    Parse CSV and return the PDF URL cell for the row whose course column matches course_name
    (case-insensitive, normalized whitespace).
    """
    want = _normalize_course_key(course_name)
    if not want:
        return None

    text = (csv_text or "").lstrip("\ufeff")
    if not text or text.lstrip().startswith("<"):
        return None

    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        return None

    header = [c.strip() for c in rows[0]]
    ci, pi = _detect_column_indices(
        header,
        course_column=(settings.PUBLIC_COURSE_SHEET_COURSE_COLUMN or "").strip(),
        pdf_column=(settings.PUBLIC_COURSE_SHEET_PDF_COLUMN or "").strip(),
    )
    if ci is None or pi is None:
        logger.warning(
            "Public course sheet: could not detect course/pdf columns | header=%s",
            header[:12],
        )
        return None

    for row in rows[1:]:
        if max(ci, pi) >= len(row):
            continue
        ccell = row[ci].strip()
        pcell = row[pi].strip()
        if _normalize_course_key(ccell) != want:
            continue
        if _looks_like_url(pcell):
            return pcell
    return None


async def fetch_public_course_sheet_csv(url: str) -> str:
    """GET the CSV body; follows redirects within docs.google.com."""
    export_url = normalize_google_sheet_csv_export_url(url)
    if not export_url:
        raise ValueError("PUBLIC_COURSE_SHEET_CSV_URL is empty")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; course-ai-backend/1.0; +https://www.google.com/bot.html)"
        ),
    }
    timeout = httpx.Timeout(45.0, connect=15.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=headers) as client:
        resp = await client.get(export_url)
        resp.raise_for_status()
        return resp.text


async def lookup_public_course_pdf_url(course_name: str) -> Optional[str]:
    """
    If PUBLIC_COURSE_SHEET_CSV_URL is set, fetch CSV and return PDF URL for course_name, else None.
    """
    base = (settings.PUBLIC_COURSE_SHEET_CSV_URL or "").strip()
    if not base:
        return None
    if not (settings.PUBLIC_COURSE_SHEET_LOOKUP_ENABLED):
        return None

    try:
        csv_text = await fetch_public_course_sheet_csv(base)
    except Exception as e:
        logger.warning("Public course sheet fetch failed | error=%s", str(e)[:500])
        return None

    return find_pdf_url_for_course(csv_text, course_name)

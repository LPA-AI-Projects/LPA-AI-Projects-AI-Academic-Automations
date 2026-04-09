from __future__ import annotations

import asyncio
from io import BytesIO
import re
from typing import Iterable

from app.utils.logger import get_logger

logger = get_logger(__name__)


def _join_nonempty(parts: Iterable[str]) -> str:
    cleaned = [p.strip() for p in parts if isinstance(p, str) and p.strip()]
    return "\n\n".join(cleaned).strip()


def extract_pdf_text(file_bytes: bytes) -> str:
    """
    Extract text from PDF bytes using PyMuPDF (fitz).
    Synchronous; call via asyncio.to_thread for non-blocking usage.
    """
    import fitz  # PyMuPDF

    doc = fitz.open(stream=file_bytes, filetype="pdf")
    try:
        pages: list[str] = []
        for page in doc:
            pages.append(page.get_text("text"))
        return _join_nonempty(pages)
    finally:
        doc.close()


def extract_ppt_text(file_bytes: bytes) -> str:
    """
    Extract text from PPTX bytes using python-pptx.
    Synchronous; call via asyncio.to_thread for non-blocking usage.
    """
    from io import BytesIO

    from pptx import Presentation

    prs = Presentation(BytesIO(file_bytes))
    chunks: list[str] = []
    for slide in prs.slides:
        for shape in slide.shapes:
            if hasattr(shape, "text") and shape.text:
                chunks.append(str(shape.text))
    return _join_nonempty(chunks)


async def extract_pdf_text_async(file_bytes: bytes) -> str:
    return await asyncio.to_thread(extract_pdf_text, file_bytes)


async def extract_ppt_text_async(file_bytes: bytes) -> str:
    return await asyncio.to_thread(extract_ppt_text, file_bytes)


def extract_pdf_module_rows(file_bytes: bytes) -> list[dict[str, str]]:
    """
    Extract module rows from table-like course outlines (Sno | Modules | Topics | Exercises).
    Returns [] when no usable table rows are found.
    """
    try:
        import pdfplumber
    except Exception:
        return []

    def _norm(v: object) -> str:
        return str(v or "").strip()

    def _is_header_row(row: list[str]) -> bool:
        joined = " | ".join(row).lower()
        return (
            "sno" in joined
            and "module" in joined
            and ("topic" in joined or "content" in joined)
            and ("exercise" in joined or "activity" in joined)
        )

    modules: list[dict[str, str]] = []
    with pdfplumber.open(BytesIO(file_bytes)) as pdf:
        for page in pdf.pages:
            for table in page.extract_tables() or []:
                if not table:
                    continue
                rows: list[list[str]] = [[_norm(c) for c in (r or [])] for r in table]
                if not rows:
                    continue

                start_idx = -1
                for i, row in enumerate(rows):
                    if _is_header_row(row):
                        start_idx = i + 1
                        break
                # Only parse table rows when a valid module-table header exists.
                if start_idx < 0:
                    continue

                for row in rows[start_idx:]:
                    if len(row) < 3:
                        continue
                    sno = _norm(row[0])
                    if not sno:
                        continue
                    m = re.match(r"^0?(\d{1,2})$", sno)
                    if not m:
                        continue
                    module_num = int(m.group(1))
                    module_name = _norm(row[1])
                    topics = _norm(row[2]) if len(row) > 2 else ""
                    exercises = _norm(row[3]) if len(row) > 3 else ""
                    if not module_name:
                        continue
                    if module_name.strip().lower() in {"module", "modules"}:
                        continue
                    module_text_parts = [
                        f"Module {module_num}: {module_name}",
                        f"Topics:\n{topics}" if topics else "",
                        f"Exercises:\n{exercises}" if exercises else "",
                    ]
                    module_text = "\n\n".join([p for p in module_text_parts if p]).strip()
                    modules.append(
                        {
                            "module_name": f"Module {module_num}: {module_name}",
                            "module_text": module_text,
                        }
                    )

    # Deduplicate by module number, keep first occurrence.
    deduped: list[dict[str, str]] = []
    seen: set[int] = set()
    for item in modules:
        n_match = re.search(r"Module\s+(\d+)", item.get("module_name", ""))
        if not n_match:
            continue
        n = int(n_match.group(1))
        if n in seen:
            continue
        seen.add(n)
        deduped.append(item)
    return deduped


async def extract_pdf_module_rows_async(file_bytes: bytes) -> list[dict[str, str]]:
    return await asyncio.to_thread(extract_pdf_module_rows, file_bytes)


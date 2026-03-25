from __future__ import annotations

import asyncio
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


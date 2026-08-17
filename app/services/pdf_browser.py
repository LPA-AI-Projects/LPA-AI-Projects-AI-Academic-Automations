"""Persistent Chromium for HTML → PDF. Outline generation does not use this module."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from app.utils.logger import get_logger

logger = get_logger(__name__)

_CHROMIUM_LAUNCH_ARGS = [
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-software-rasterizer",
    "--disable-extensions",
    "--no-first-run",
]


class PdfBrowser:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._playwright: Any = None
        self._browser: Any = None

    def _browser_alive(self) -> bool:
        try:
            return self._browser is not None and self._browser.is_connected()
        except Exception:
            return False

    async def start(self) -> None:
        async with self._lock:
            await self._start_locked()

    async def stop(self) -> None:
        async with self._lock:
            await self._stop_locked()

    async def html_file_to_pdf(self, html_path: Path, pdf_path: str) -> None:
        """Serialize renders so only one page uses the shared browser at a time."""
        async with self._lock:
            await self._render_locked(html_path, pdf_path, relaunch=True)

    async def _start_locked(self) -> None:
        if self._browser_alive():
            return
        await self._stop_locked()
        from playwright.async_api import async_playwright

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            args=_CHROMIUM_LAUNCH_ARGS,
            chromium_sandbox=False,
        )
        logger.info("PDF Chromium started (persistent)")

    async def _stop_locked(self) -> None:
        browser = self._browser
        playwright = self._playwright
        self._browser = None
        self._playwright = None
        if browser is not None:
            try:
                await browser.close()
            except Exception:
                logger.warning("PDF Chromium close failed", exc_info=True)
        if playwright is not None:
            try:
                await playwright.stop()
            except Exception:
                logger.warning("Playwright stop failed", exc_info=True)

    async def _render_locked(self, html_path: Path, pdf_path: str, *, relaunch: bool) -> None:
        try:
            if not self._browser_alive():
                await self._start_locked()
            page = await self._browser.new_page()
            try:
                await page.goto(html_path.resolve().as_uri(), wait_until="networkidle")
                await page.pdf(path=pdf_path, format="A4", print_background=True)
            finally:
                await page.close()
        except Exception:
            if not relaunch:
                raise
            logger.warning("PDF render failed; relaunching Chromium once", exc_info=True)
            await self._stop_locked()
            await self._start_locked()
            await self._render_locked(html_path, pdf_path, relaunch=False)


pdf_browser = PdfBrowser()

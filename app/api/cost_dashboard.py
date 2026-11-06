"""
Serve the built cost dashboard (``dashboard/dist``) at ``/cost-dashboard``.

The dashboard is a client-side single-page app with no data of its own: inputs and pasted usage logs
stay in the browser. If the build output is missing (e.g. Node stage skipped), nothing is mounted and
the API starts normally.
"""
from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from starlette.staticfiles import StaticFiles

from app.core.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)

# Must match `base` in dashboard/vite.config.ts and `basepath` in dashboard/src/main.tsx.
BASE_PATH = "/cost-dashboard"

_REPO_ROOT = Path(__file__).resolve().parents[2]
_NO_CACHE = {"Cache-Control": "no-cache"}


class _HashedAssets(StaticFiles):
    """Vite fingerprints asset file names, so they can be cached for a year."""

    async def get_response(self, path, scope):  # type: ignore[no-untyped-def]
        response = await super().get_response(path, scope)
        if response.status_code == 200:
            response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        return response


def default_dist_dir() -> Path:
    configured = str(getattr(settings, "COST_DASHBOARD_DIST_DIR", "") or "").strip()
    return Path(configured) if configured else _REPO_ROOT / "dashboard" / "dist"


def mount_cost_dashboard(app: FastAPI, dist_dir: Path | str | None = None) -> bool:
    """Register the dashboard routes. Returns False (and mounts nothing) when there is no build."""
    dist = Path(dist_dir) if dist_dir else default_dist_dir()
    index = dist / "index.html"
    if not index.is_file():
        logger.info("Cost dashboard not mounted: no build at %s", index)
        return False
    dist_root = dist.resolve()

    assets = dist / "assets"
    if assets.is_dir():
        # Registered before the catch-all route below so /assets/* is served by StaticFiles (404 if missing).
        app.mount(f"{BASE_PATH}/assets", _HashedAssets(directory=assets), name="cost-dashboard-assets")

    def _index() -> FileResponse:
        return FileResponse(index, media_type="text/html", headers=_NO_CACHE)

    @app.get(BASE_PATH, include_in_schema=False)
    async def cost_dashboard_root() -> FileResponse:
        return _index()

    @app.get(BASE_PATH + "/{path:path}", include_in_schema=False)
    async def cost_dashboard_page(path: str) -> FileResponse:
        if not path:
            return _index()
        candidate = (dist_root / path).resolve()
        if candidate.is_file() and candidate.is_relative_to(dist_root):
            return FileResponse(candidate)
        if Path(path).suffix:  # a missing .js/.css/.map/etc. must 404, not return the HTML shell
            raise HTTPException(status_code=404, detail="Not found")
        return _index()  # client-side route such as /cost-dashboard/compare

    logger.info("Cost dashboard mounted at %s from %s", BASE_PATH, dist_root)
    return True

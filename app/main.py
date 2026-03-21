import os
import sys
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi import Request
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import router
from app.core.database import Base, engine
from app.utils.logger import get_logger

logger = get_logger(__name__)

OUTPUT_DIR = "generated_pdfs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Playwright launches Chromium via subprocess; ensure an event loop policy
# that supports subprocesses on Windows.
if sys.platform.startswith("win"):
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())  # type: ignore[attr-defined]
    except Exception:
        # If setting the policy fails for any reason, continue; PDF generation will surface errors.
        pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Run startup/shutdown tasks."""
    # Keep the API runnable locally even if Postgres isn't up yet.
    # Routes that require DB will still fail, but the server can start and serve /health.
    logger.info("Starting up — creating database tables...")
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Database tables ready.")
    except Exception:
        logger.exception("Database unavailable at startup; continuing without migrations")
    yield
    logger.info("Shutting down — disposing DB engine.")
    await engine.dispose()


app = FastAPI(
    title="Course AI Backend",
    description="Generate and refine corporate training course outlines using AI.",
    version="1.0.0",
    lifespan=lifespan,
)

# ── CORS ──────────────────────────────────────────────────────────────────────
# Restrict origins in production: replace "*" with your frontend domain
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Static files: serve generated PDFs at /pdfs/<filename> ───────────────────
app.mount("/pdfs", StaticFiles(directory=OUTPUT_DIR), name="pdfs")

# ── Routes ────────────────────────────────────────────────────────────────────
app.include_router(router)


@app.middleware("http")
async def log_raw_course_requests(request: Request, call_next):
    """
    Temporary debug logger:
    - Logs raw body for POST /api/v1/courses (even if malformed JSON).
    - Re-injects body so downstream handlers can still read it.
    """
    if request.method == "POST" and request.url.path == "/api/v1/courses":
        body_bytes = await request.body()
        raw_body = body_bytes.decode("utf-8", errors="replace")
        logger.info(
            "RAW /api/v1/courses request | content_type=%s body=%s",
            request.headers.get("content-type"),
            raw_body[:8000],
        )

        async def receive():
            return {"type": "http.request", "body": body_bytes, "more_body": False}

        request._receive = receive  # type: ignore[attr-defined]

    response = await call_next(request)
    return response
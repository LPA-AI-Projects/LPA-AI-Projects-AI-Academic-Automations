"""
Data behind the cost dashboard's Requests, Total and month views (API key required).

A "request" is one outline-generation job (Zoho ``courses`` jobs or Bitrix ``bitrix_outline`` jobs); a
"refinement" is every later version of that job's course. Slides jobs are not part of the dashboard.

Token usage is only known for calls recorded in ``llm_usage`` (recording starts when this code is
deployed). Everything else is counted from the existing jobs / versions tables; the dashboard prices
recorded usage exactly and estimates the rest.
"""
from __future__ import annotations

import asyncio
import json
import uuid
from collections import defaultdict
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
import time

from sqlalchemy import and_, exists, func, inspect, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.auth_deps import verify_api_key
from app.core.database import get_db
from app.models.course import CourseVersion
from app.models.job import CourseJob
from app.models.usage import LlmUsage
from app.utils.logger import get_logger

logger = get_logger(__name__)

router = APIRouter(
    prefix="/api/v1/cost-dashboard",
    tags=["cost-dashboard"],
    dependencies=[Depends(verify_api_key)],
)

# Must equal BITRIX_OUTLINE_JOB_TYPE in app/services/course_refine.py (checked by a test); not imported here
# so this module stays light.
BITRIX_OUTLINE_JOB_TYPE = "bitrix_outline"
SLIDES_JOB_TYPE = "slides"
_OUTLINE_JOB = or_(CourseJob.job_type.is_(None), CourseJob.job_type != SLIDES_JOB_TYPE)

# Shown first (in this order) in the request detail; the rest follow alphabetically.
_INPUT_ORDER = [
    "course_name", "company_name", "department", "designation", "level_of_training", "mode_of_training",
    "training_days", "per_day_duration_in_hours", "duration", "no_of_pax", "size_of_company",
    "need_of_training", "goal_of_training", "topics_to_include", "referral_course_links",
    "bitrix_task_id", "bitrix_task_title",
]
_HIDDEN_INPUTS = {"crm_source"}


# ───────────────────────── pure helpers (unit-tested) ─────────────────────────


def source_of(job_type: str | None) -> str:
    return "bitrix" if job_type == BITRIX_OUTLINE_JOB_TYPE else "zoho"


def month_key(dt: datetime | None) -> str | None:
    return f"{dt.year:04d}-{dt.month:02d}" if dt else None


def month_bounds(month: str | None) -> tuple[datetime, datetime] | None:
    """``"2026-11"`` -> (2026-11-01, 2026-12-01). ``None`` / ``"all"`` -> no bounds."""
    if not month or month == "all":
        return None
    try:
        y, m = (int(x) for x in month.split("-"))
        start = datetime(y, m, 1)
    except (ValueError, TypeError):
        raise HTTPException(status_code=422, detail="month must look like 2026-11 or be 'all'")
    return start, datetime(y + (m == 12), 1 if m == 12 else m + 1, 1)


def clean_inputs(payload_json: str | None) -> dict[str, Any]:
    """Job inputs for display: parsed, empty values dropped, important fields first."""
    try:
        data = json.loads(payload_json or "{}")
    except (TypeError, ValueError):
        return {}
    if not isinstance(data, dict):
        return {}
    kept = {
        k: v
        for k, v in data.items()
        if k not in _HIDDEN_INPUTS and v not in (None, "", [], {}) and not (isinstance(v, str) and not v.strip())
    }
    first = [k for k in _INPUT_ORDER if k in kept]
    rest = sorted(k for k in kept if k not in first)
    return {k: kept[k] for k in first + rest}


def summarize_names(payload_json: str | None) -> tuple[str, str]:
    data = clean_inputs(payload_json)
    return str(data.get("course_name") or ""), str(data.get("company_name") or "")


def _bucket() -> dict[str, int]:
    return {"outlines": 0, "completed": 0, "failed": 0, "other": 0, "refines": 0}


def build_by_month(
    job_rows: list[tuple[str | None, str | None, datetime | None]],
    refine_rows: list[tuple[str | None, datetime | None]],
) -> list[dict[str, Any]]:
    """Per month and source: outlines (by status) and refinements. Rows are (job_type, status, created_at)."""
    months: dict[str, dict[str, dict[str, int]]] = defaultdict(lambda: {"zoho": _bucket(), "bitrix": _bucket()})
    for job_type, status, created in job_rows:
        key = month_key(created)
        if not key:
            continue
        b = months[key][source_of(job_type)]
        b["outlines"] += 1
        if status == "completed":
            b["completed"] += 1
        elif status == "failed":
            b["failed"] += 1
        else:
            b["other"] += 1
    for job_type, created in refine_rows:
        key = month_key(created)
        if key:
            months[key][source_of(job_type)]["refines"] += 1
    return [{"month": m, **months[m]} for m in sorted(months)]


# ───────────────────────── queries ─────────────────────────

_usage_table_cache: dict[str, Any] = {"ok": False, "checked": 0.0}


async def usage_table_exists(db: AsyncSession) -> bool:
    """True once llm_usage has been created (it is created at API startup). Avoids running failing queries."""
    if _usage_table_cache["ok"]:
        return True
    if time.monotonic() - _usage_table_cache["checked"] < 30:
        return False
    conn = await db.connection()
    ok = bool(await conn.run_sync(lambda sync_conn: inspect(sync_conn).has_table(LlmUsage.__tablename__)))
    _usage_table_cache.update(ok=ok, checked=time.monotonic())
    if not ok:
        logger.warning("llm_usage is not available yet; reporting no recorded usage")
    return ok



async def _usage_rows(db: AsyncSession, where: list[Any] | None = None) -> list[Any]:
    """Aggregated usage rows; an empty list when llm_usage does not exist yet."""
    year = func.extract("year", LlmUsage.created_at)
    mon = func.extract("month", LlmUsage.created_at)
    stmt = (
        select(
            year.label("y"),
            mon.label("m"),
            LlmUsage.kind,
            LlmUsage.label,
            LlmUsage.model,
            func.count().label("calls"),
            func.coalesce(func.sum(LlmUsage.input_tokens), 0).label("input"),
            func.coalesce(func.sum(LlmUsage.output_tokens), 0).label("output"),
            func.coalesce(func.sum(LlmUsage.cache_write_tokens), 0).label("cache_write"),
            func.coalesce(func.sum(LlmUsage.cache_read_tokens), 0).label("cache_read"),
            func.coalesce(func.sum(LlmUsage.web_searches), 0).label("searches"),
            func.count(func.distinct(LlmUsage.job_id)).label("jobs"),
        )
        .group_by(year, mon, LlmUsage.kind, LlmUsage.label, LlmUsage.model)
    )
    for w in where or []:
        stmt = stmt.where(w)
    if not await usage_table_exists(db):
        return []
    return list((await db.execute(stmt)).all())


def _usage_dict(r: Any) -> dict[str, Any]:
    return {
        "month": f"{int(r.y):04d}-{int(r.m):02d}",
        "kind": r.kind,
        "label": r.label,
        "model": r.model,
        "calls": int(r.calls),
        "input": int(r.input),
        "output": int(r.output),
        "cache_write": int(r.cache_write),
        "cache_read": int(r.cache_read),
        "searches": int(r.searches),
        "jobs": int(r.jobs),
    }


@router.get("/summary", summary="Monthly request counts and recorded token usage")
async def summary(db: AsyncSession = Depends(get_db)) -> dict[str, Any]:
    jobs = (
        await db.execute(select(CourseJob.job_type, CourseJob.status, CourseJob.created_at).where(_OUTLINE_JOB))
    ).all()
    refines = (
        await db.execute(
            select(CourseJob.job_type, CourseVersion.created_at)
            .join(CourseVersion, CourseVersion.course_id == CourseJob.course_id)
            .where(_OUTLINE_JOB, CourseVersion.version_number > 1)
        )
    ).all()
    usage = [_usage_dict(r) for r in await _usage_rows(db)]
    first = (await db.execute(select(func.min(LlmUsage.created_at)))).scalar() if await usage_table_exists(db) else None
    by_month = build_by_month([tuple(r) for r in jobs], [tuple(r) for r in refines])  # type: ignore[arg-type]
    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "months": [m["month"] for m in by_month],
        "by_month": by_month,
        "usage": usage,
        "usage_recording": {"available": bool(usage) or first is not None, "first_recorded": first.isoformat() if first else None},
    }


def _status_filter(status: str) -> Any | None:
    if status == "all":
        return None
    if status in ("completed", "failed"):
        return CourseJob.status == status
    if status == "in_progress":
        return CourseJob.status.not_in(["completed", "failed"])
    raise HTTPException(status_code=422, detail="status must be all, completed, failed or in_progress")


async def _version_counts(db: AsyncSession, course_ids: list[uuid.UUID]) -> dict[uuid.UUID, tuple[int, int]]:
    """course_id -> (versions, refinements)."""
    if not course_ids:
        return {}
    rows = (
        await db.execute(
            select(CourseVersion.course_id, func.count(), func.count().filter(CourseVersion.version_number > 1))
            .where(CourseVersion.course_id.in_(course_ids))
            .group_by(CourseVersion.course_id)
        )
    ).all()
    return {r[0]: (int(r[1]), int(r[2])) for r in rows}


async def _usage_by_owner(db: AsyncSession, col: Any, ids: list[uuid.UUID], kind: str) -> dict[uuid.UUID, dict[str, int]]:
    if not ids:
        return {}
    stmt = (
        select(
            col,
            func.count(),
            func.coalesce(func.sum(LlmUsage.input_tokens), 0),
            func.coalesce(func.sum(LlmUsage.output_tokens), 0),
            func.coalesce(func.sum(LlmUsage.cache_write_tokens), 0),
            func.coalesce(func.sum(LlmUsage.cache_read_tokens), 0),
            func.coalesce(func.sum(LlmUsage.web_searches), 0),
        )
        .where(col.in_(ids), LlmUsage.kind == kind)
        .group_by(col)
    )
    if not await usage_table_exists(db):
        return {}
    rows = (await db.execute(stmt)).all()
    keys = ("calls", "input", "output", "cache_write", "cache_read", "searches")
    return {r[0]: {k: int(v) for k, v in zip(keys, r[1:])} for r in rows}


@router.get("/requests", summary="Paginated list of outline requests")
async def list_requests(
    month: str = Query("all", description="YYYY-MM or 'all'"),
    source: str = Query("all", description="all | zoho | bitrix"),
    status: str = Query("all", description="all | completed | failed | in_progress"),
    refined: str = Query("all", description="all | yes | no"),
    q: str = Query("", max_length=200, description="matches course, company or record id"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    conds: list[Any] = [_OUTLINE_JOB]
    bounds = month_bounds(month)
    if bounds:
        conds += [CourseJob.created_at >= bounds[0], CourseJob.created_at < bounds[1]]
    if source == "bitrix":
        conds.append(CourseJob.job_type == BITRIX_OUTLINE_JOB_TYPE)
    elif source == "zoho":
        conds.append(or_(CourseJob.job_type.is_(None), CourseJob.job_type.not_in([BITRIX_OUTLINE_JOB_TYPE, SLIDES_JOB_TYPE])))
    elif source != "all":
        raise HTTPException(status_code=422, detail="source must be all, zoho or bitrix")
    sf = _status_filter(status)
    if sf is not None:
        conds.append(sf)
    has_refinement = exists().where(and_(CourseVersion.course_id == CourseJob.course_id, CourseVersion.version_number > 1))
    if refined == "yes":
        conds.append(has_refinement)
    elif refined == "no":
        conds.append(~has_refinement)
    elif refined != "all":
        raise HTTPException(status_code=422, detail="refined must be all, yes or no")
    needle = q.strip().lower()
    if needle:
        like = f"%{needle}%"
        conds.append(or_(func.lower(CourseJob.payload_json).like(like), func.lower(CourseJob.zoho_record_id).like(like)))

    total = int((await db.execute(select(func.count()).select_from(CourseJob).where(*conds))).scalar() or 0)
    jobs = (
        await db.execute(select(CourseJob).where(*conds).order_by(CourseJob.created_at.desc()).limit(limit).offset(offset))
    ).scalars().all()

    counts = await _version_counts(db, [j.course_id for j in jobs if j.course_id])
    out_usage = await _usage_by_owner(db, LlmUsage.job_id, [j.id for j in jobs], "outline")
    ref_usage = await _usage_by_owner(db, LlmUsage.course_id, [j.course_id for j in jobs if j.course_id], "refine")
    items = []
    for j in jobs:
        course, company = summarize_names(j.payload_json)
        versions, refinements = counts.get(j.course_id, (0, 0)) if j.course_id else (0, 0)
        items.append(
            {
                "id": str(j.id),
                "created_at": j.created_at.isoformat() if j.created_at else None,
                "source": source_of(j.job_type),
                "record_id": j.zoho_record_id,
                "course_name": course,
                "company_name": company,
                "status": j.status,
                "failed": j.status == "failed",
                "versions": versions,
                "refinements": refinements,
                "pdf_url": j.pdf_url,
                "usage": {"outline": out_usage.get(j.id), "refine": ref_usage.get(j.course_id) if j.course_id else None},
            }
        )
    return {"total": total, "limit": limit, "offset": offset, "items": items}


async def _responsible(source: str, record_id: str, inputs: dict[str, Any]) -> dict[str, Any] | None:
    """Best effort, live lookup of who owns the request. Returns None when nothing is available."""
    try:
        if source == "bitrix":
            from app.services.bitrix_tasks import fetch_task_for_outline

            task_id = str(inputs.get("bitrix_task_id") or record_id)
            task = await asyncio.wait_for(fetch_task_for_outline(task_id), timeout=8)
            responsible = str(task.get("RESPONSIBLE_NAME") or "").strip()
            creator = str(task.get("CREATED_BY_NAME") or "").strip()
            if responsible or creator:
                return {"name": responsible or None, "created_by": creator or None, "from": "Bitrix task"}
            return None
        from app.services.zoho_crm import get_outline_module_api_name, get_record_field_values

        vals = await asyncio.wait_for(
            get_record_field_values(
                module_api_name=get_outline_module_api_name(),
                crm_record_id=record_id,
                field_api_names=["Owner", "Course_Outline_Owner", "Created_By"],
            ),
            timeout=8,
        )
        name = vals.get("Course_Outline_Owner") or vals.get("Owner")
        if name or vals.get("Created_By"):
            return {"name": name, "created_by": vals.get("Created_By"), "from": "Zoho record"}
    except Exception as exc:  # network, auth or missing record: simply not shown
        logger.info("Responsible person unavailable | source=%s record=%s err=%r", source, record_id, exc)
    return None


@router.get("/requests/{job_id}", summary="Everything about one request")
async def request_detail(job_id: uuid.UUID, db: AsyncSession = Depends(get_db)) -> dict[str, Any]:
    job = (await db.execute(select(CourseJob).where(CourseJob.id == job_id, _OUTLINE_JOB))).scalars().first()
    if job is None:
        raise HTTPException(status_code=404, detail="Request not found")
    source = source_of(job.job_type)
    inputs = clean_inputs(job.payload_json)

    versions: list[dict[str, Any]] = []
    if job.course_id:
        rows = (
            await db.execute(
                select(CourseVersion.version_number, CourseVersion.created_at, CourseVersion.feedback, CourseVersion.pdf_url)
                .where(CourseVersion.course_id == job.course_id)
                .order_by(CourseVersion.version_number)
            )
        ).all()
        versions = [
            {
                "version": int(r[0] or 0),
                "created_at": r[1].isoformat() if r[1] else None,
                "feedback": r[2],
                "pdf_url": r[3],
                "is_refinement": int(r[0] or 0) > 1,
            }
            for r in rows
        ]

    calls: list[dict[str, Any]] = []
    owners = [LlmUsage.job_id == job.id]
    if job.course_id:
        owners.append(and_(LlmUsage.course_id == job.course_id, LlmUsage.kind == "refine"))
    if await usage_table_exists(db):
        usage = (await db.execute(select(LlmUsage).where(or_(*owners)).order_by(LlmUsage.created_at))).scalars().all()
        calls = [
            {
                "kind": u.kind,
                "label": u.label,
                "model": u.model,
                "created_at": u.created_at.isoformat() if u.created_at else None,
                "input": u.input_tokens,
                "output": u.output_tokens,
                "cache_write": u.cache_write_tokens,
                "cache_read": u.cache_read_tokens,
                "searches": u.web_searches,
                "stop": u.stop_reason,
                "structured": u.structured,
            }
            for u in usage
        ]

    return {
        "id": str(job.id),
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "source": source,
        "record_id": job.zoho_record_id,
        "status": job.status,
        "error": (job.error or "")[:2000] or None,
        "pdf_url": job.pdf_url,
        "inputs": inputs,
        "versions": versions,
        "refinements": sum(1 for v in versions if v["is_refinement"]),
        "usage_calls": calls,
        "responsible": await _responsible(source, job.zoho_record_id, inputs),
    }

"""
Bitrix24 course outline API — separate from the Zoho flow.

Supports:
- Explicit JSON: ``{ "bitrix_record_id": "78776", "input_data": { ... } }``
- Task webhook / ``task.item.getdata`` body with ``DESCRIPTION`` table (parsed automatically)
- Minimal trigger: ``{ "taskId": 78776 }`` — task fields are fetched from Bitrix
- ``ONTASKCOMMENTADD`` is ignored (prevents webhook loops when bot posts comments).
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any
from urllib.parse import parse_qs

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.auth_deps import bitrix_auth
from app.api.routes import (
    _input_data_dict_for_job,
    _is_public_course_type,
    _job_to_course_outline_response,
    _normalize_single_course_name,
    process_course_job,
)
from app.core.config import settings
from app.core.database import AsyncSessionLocal, get_db
from app.models.job import CourseJob
from app.schemas.bitrix import BitrixGenerateCourseRequest
from app.schemas.course import CourseInputData
from app.schemas.integration import BitrixCourseOutlineIntegrationStatus
from app.schemas.job import CourseOutlineQueuedResponse
from app.services.bitrix_integration import get_bitrix_course_outline_integration_status
from app.services.bitrix_task_parser import extract_task_id, resolve_bitrix_task_request
from app.services.bitrix_tasks import fetch_task_item_data
from app.services.course_refine import BITRIX_OUTLINE_JOB_TYPE
from app.utils.logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/bitrix", tags=["bitrix"])

BITRIX_GENERATE_EVENTS = frozenset({"ONTASKADD"})


def _allowed_bitrix_project_ids() -> frozenset[str]:
    """GROUP_ID and/or Bitrix task flow id (UI field ``flow`` → often FLOW_ID in API)."""
    raw = (settings.BITRIX_ALLOWED_GROUP_IDS or "34,36").strip()
    return frozenset(part.strip() for part in raw.split(",") if part.strip())


def _enforce_task_project_allowed(task_body: dict[str, Any], task_id: str) -> None:
    """Raise HTTP 200 when task is outside allowed Bitrix project / flow IDs."""
    group_id = str(
        task_body.get("GROUP_ID") or task_body.get("groupId") or ""
    ).strip()
    group_name = str(
        task_body.get("GROUP_NAME") or task_body.get("GROUP") or ""
    ).strip()
    flow_id = str(
        task_body.get("FLOW_ID")
        or task_body.get("flowId")
        or task_body.get("flow")
        or task_body.get("FLOW")
        or ""
    ).strip()
    flow_name = str(
        task_body.get("FLOW_NAME") or task_body.get("flowName") or ""
    ).strip()

    logger.info(
        "BITRIX PROJECT CHECK | task=%s group_id=%s group=%s flow_id=%s flow=%s",
        task_id,
        group_id,
        group_name,
        flow_id,
        flow_name,
    )

    allowed = _allowed_bitrix_project_ids()
    if group_id in allowed or flow_id in allowed:
        return

    logger.info(
        "Ignoring task outside outline projects | task=%s group_id=%s flow_id=%s "
        "group=%s flow=%s allowed=%s",
        task_id,
        group_id,
        flow_id,
        group_name,
        flow_name,
        sorted(allowed),
    )
    raise HTTPException(status_code=status.HTTP_200_OK, detail="project_ignored")


class BitrixWebhookKind(str, Enum):
    GENERATE = "generate"
    REFINE = "refine"
    IGNORED = "ignored"


@dataclass
class BitrixWebhookDispatch:
    kind: BitrixWebhookKind
    generate_req: BitrixGenerateCourseRequest | None = None
    refine_task_id: str | None = None
    refine_feedback: str | None = None
    refine_course_name: str | None = None
    ignore_reason: str | None = None


def _ignored_response(reason: str) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"ok": True, "status": "ignored", "reason": reason},
    )


def _is_task_payload(payload: dict) -> bool:
    if extract_task_id(payload):
        return True
    if str(payload.get("DESCRIPTION") or "").strip():
        return True
    result = payload.get("result")
    if isinstance(result, dict) and (
        extract_task_id(result) or str(result.get("DESCRIPTION") or "").strip()
    ):
        return True
    event = str(payload.get("event") or "").upper()
    if "TASK" in event:
        return True
    return False


async def _read_bitrix_payload(request: Request) -> dict[str, Any]:
    content_type = (request.headers.get("content-type") or "").lower()
    if "application/json" in content_type:
        try:
            payload = await request.json()
        except Exception:
            raise HTTPException(status_code=422, detail="Invalid JSON body.")
    elif "application/x-www-form-urlencoded" in content_type:
        body_bytes = await request.body()
        raw_body = body_bytes.decode("utf-8", errors="replace")
        parsed = parse_qs(raw_body, keep_blank_values=True)
        payload = {k: (v[0] if isinstance(v, list) and v else "") for k, v in parsed.items()}
    else:
        raise HTTPException(
            status_code=415,
            detail="Use application/json or application/x-www-form-urlencoded.",
        )
    if not isinstance(payload, dict):
        raise HTTPException(status_code=422, detail="Request body must be a JSON object.")
    return payload


async def _parse_bitrix_generate_request(payload: dict[str, Any]) -> BitrixGenerateCourseRequest:
    logger.info(
        "Bitrix /courses incoming payload (preview) | keys=%s event=%s",
        sorted(payload.keys()),
        payload.get("event"),
    )

    event = str(payload.get("event") or "").upper()
    if event == "ONTASKCOMMENTADD":
        raise HTTPException(
            status_code=422,
            detail="Use webhook dispatch for ONTASKCOMMENTADD (refine path).",
        )
    if event and event not in BITRIX_GENERATE_EVENTS and _is_task_payload(payload):
        raise HTTPException(
            status_code=422,
            detail=f"Unsupported Bitrix event for generate: {event}",
        )

    if _is_task_payload(payload):
        try:
            if extract_task_id(payload) and not str(payload.get("DESCRIPTION") or "").strip() and not (
                isinstance(payload.get("result"), dict)
                and str(payload["result"].get("DESCRIPTION") or "").strip()
            ):
                tid = extract_task_id(payload)
                if not tid:
                    raise HTTPException(status_code=422, detail="Invalid Bitrix task ID.")
                logger.info("Bitrix task fetch | taskId=%s", tid)
                try:
                    task_body = await fetch_task_item_data(str(tid))
                except Exception as e:
                    logger.warning("Bitrix task fetch failed | taskId=%s error=%s", tid, e)
                    raise HTTPException(
                        status_code=422,
                        detail=f"Could not load task {tid} from Bitrix.",
                    ) from e
                logger.info(
                    "BITRIX TASK RAW = %s",
                    json.dumps(task_body, indent=2, ensure_ascii=False),
                )
                _enforce_task_project_allowed(task_body, str(tid))
                payload = {"result": task_body}

            task_id, input_dict = resolve_bitrix_task_request(payload)
            input_data = CourseInputData.model_validate(input_dict)
            return BitrixGenerateCourseRequest(
                bitrix_record_id=task_id,
                input_data=input_data,
            )
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e)) from e
        except ValidationError as e:
            raise HTTPException(status_code=422, detail=str(e)) from e

    bitrix_record_id = (
        str(payload.get("bitrix_record_id") or "")
        or str(payload.get("deal_id") or "")
        or str(payload.get("record_id") or "")
        or str(payload.get("id") or "")
        or str(payload.get("crm_record_id") or "")
    ).strip()

    input_data_payload = payload.get("input_data")
    if not isinstance(input_data_payload, dict):
        input_data_payload = {
            k: v
            for k, v in payload.items()
            if k
            not in {
                "bitrix_record_id",
                "deal_id",
                "record_id",
                "id",
                "crm_record_id",
            }
        }

    if not bitrix_record_id:
        raise HTTPException(
            status_code=422,
            detail=(
                "Missing bitrix_record_id or task id. Send a task webhook body, "
                '{ "taskId": 78776 }, or { "bitrix_record_id": "...", "input_data": {...} }.'
            ),
        )

    if _is_public_course_type(input_data_payload.get("course_type")):
        required_input_fields = ["course_name"]
    else:
        required_input_fields = ["company_name", "course_name", "department"]

    missing = [
        key
        for key in required_input_fields
        if not str(input_data_payload.get(key, "")).strip()
    ]
    if missing:
        raise HTTPException(
            status_code=422,
            detail=f"Missing required input_data fields: {', '.join(missing)}",
        )

    try:
        return BitrixGenerateCourseRequest.model_validate(
            {"bitrix_record_id": bitrix_record_id, "input_data": input_data_payload}
        )
    except Exception:
        logger.exception("Payload validation failed for /bitrix/courses")
        raise HTTPException(status_code=422, detail="Payload validation failed for /bitrix/courses.")


async def dispatch_bitrix_webhook(request: Request) -> BitrixWebhookDispatch:
    payload = await _read_bitrix_payload(request)
    event = str(payload.get("event") or "").upper()

    if event == "ONTASKCOMMENTADD":
        logger.info("Bitrix webhook ignored | event=ONTASKCOMMENTADD")
        return BitrixWebhookDispatch(
            kind=BitrixWebhookKind.IGNORED,
            ignore_reason="comment_events_ignored",
        )

    if event and event not in BITRIX_GENERATE_EVENTS:
        if _is_task_payload(payload):
            return BitrixWebhookDispatch(
                kind=BitrixWebhookKind.IGNORED,
                ignore_reason=f"unsupported_event:{event}",
            )

    try:
        req = await _parse_bitrix_generate_request(payload)
        return BitrixWebhookDispatch(kind=BitrixWebhookKind.GENERATE, generate_req=req)
    except HTTPException as e:
        if event in BITRIX_GENERATE_EVENTS or _is_task_payload(payload):
            logger.warning("Bitrix generate parse failed | event=%s detail=%s", event, e.detail)
            return BitrixWebhookDispatch(
                kind=BitrixWebhookKind.IGNORED,
                ignore_reason=str(e.detail),
            )
        raise


@router.get(
    "/integrations/course-outline-status",
    response_model=BitrixCourseOutlineIntegrationStatus,
    dependencies=[bitrix_auth],
    summary="Bitrix24 env configuration (no secrets)",
)
def bitrix_course_outline_integration_status():
    return get_bitrix_course_outline_integration_status()


@router.post(
    "/courses",
    dependencies=[bitrix_auth],
    summary="Create course outline from Bitrix24 new task (ONTASKADD)",
    description=(
        "ONTASKADD only: generate when task GROUP_ID or flow id is in BITRIX_ALLOWED_GROUP_IDS (default 34,36). "
        "ONTASKUPDATE / ONTASKCOMMENTADD are ignored. Auth: auth[application_token] or X-API-Key."
    ),
)
async def generate_course_from_bitrix(
    request: Request,
    background_tasks: BackgroundTasks,
    sync: bool = Query(False, description="Wait for completion and return full job JSON."),
):
    dispatch = await dispatch_bitrix_webhook(request)

    if dispatch.kind == BitrixWebhookKind.IGNORED:
        return _ignored_response(dispatch.ignore_reason or "ignored")

    req = dispatch.generate_req
    if req is None:
        return _ignored_response("missing_generate_request")

    rid = req.bitrix_record_id.strip()
    course_name = _normalize_single_course_name(req.input_data.course_name)
    if not course_name:
        raise HTTPException(status_code=422, detail="input_data.course_name is required.")

    logger.info(
        "Queueing Bitrix course generation | bitrix_record_id=%s sync=%s task=%s",
        rid,
        sync,
        bool(getattr(req.input_data, "bitrix_task_id", None) or rid.isdigit()),
    )

    input_copy = req.input_data.model_copy()
    input_copy.course_name = course_name
    input_for_job = _input_data_dict_for_job(input_copy)
    input_for_job["crm_source"] = "bitrix"
    if not input_for_job.get("bitrix_task_id"):
        input_for_job["bitrix_task_id"] = rid

    try:
        async with AsyncSessionLocal() as db:
            async with db.begin():
                job = CourseJob(
                    job_type=BITRIX_OUTLINE_JOB_TYPE,
                    zoho_record_id=rid,
                    status="pending",
                    payload_json=json.dumps(input_for_job, ensure_ascii=False),
                )
                db.add(job)
            await db.refresh(job)
            job_id = job.id
    except (SQLAlchemyError, OSError, Exception):
        logger.exception("Database error while creating Bitrix course job")
        raise HTTPException(status_code=503, detail="Database unavailable. Please try again.")

    if sync:
        await process_course_job(job_id, rid, input_for_job, crm_source="bitrix")
        async with AsyncSessionLocal() as db2:
            result = await db2.execute(select(CourseJob).where(CourseJob.id == job_id))
            job_done = result.scalars().first()
        if job_done is None:
            raise HTTPException(status_code=500, detail="Job finished but could not be reloaded.")
        body = _job_to_course_outline_response(job_done).model_dump(mode="json")
        body["bitrix_record_id"] = rid
        body["bitrix_task_id"] = input_for_job.get("bitrix_task_id")
        return JSONResponse(status_code=status.HTTP_200_OK, content=body)

    background_tasks.add_task(
        process_course_job,
        job_id,
        rid,
        input_for_job,
        crm_source="bitrix",
    )
    body = CourseOutlineQueuedResponse(
        job_id=job_id,
        zoho_record_id=rid,
        status="pending",
        message="Course outline generation queued (Bitrix24).",
        polling={"by_bitrix_record_id": f"/api/v1/bitrix/courses/{rid}/outline-job"},
    )
    content = body.model_dump(mode="json")
    content["bitrix_record_id"] = rid
    content["bitrix_task_id"] = input_for_job.get("bitrix_task_id")
    return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=content)


@router.get(
    "/courses/{bitrix_record_id}/outline-job",
    dependencies=[bitrix_auth],
    summary="Latest course-outline job for a Bitrix task or CRM record id",
)
async def get_latest_bitrix_course_outline_job(
    bitrix_record_id: str,
    db: AsyncSession = Depends(get_db),
):
    rid = (bitrix_record_id or "").strip()
    if not rid:
        raise HTTPException(status_code=422, detail="bitrix_record_id is required.")

    try:
        result = await db.execute(
            select(CourseJob)
            .where(
                CourseJob.zoho_record_id == rid,
                CourseJob.job_type == BITRIX_OUTLINE_JOB_TYPE,
            )
            .order_by(CourseJob.created_at.desc())
        )
        jobs = result.scalars().all()
    except Exception:
        logger.exception("Database error while reading Bitrix outline job")
        raise HTTPException(status_code=503, detail="Database unavailable. Please try again.")

    if not jobs:
        raise HTTPException(
            status_code=404,
            detail="No course outline job found for this bitrix_record_id.",
        )

    job = jobs[0]
    all_pdf_urls = [
        str(j.pdf_url).strip()
        for j in jobs
        if str(j.status or "").strip().lower() == "completed"
        and isinstance(j.pdf_url, str)
        and str(j.pdf_url).strip()
    ]
    dedup_pdf_urls: list[str] = []
    for u in all_pdf_urls:
        if u not in dedup_pdf_urls:
            dedup_pdf_urls.append(u)

    payload = _job_to_course_outline_response(job).model_dump(mode="json")
    payload.pop("pdf_url", None)
    payload["bitrix_record_id"] = rid
    payload["bitrix_task_id"] = rid
    payload["pdf_urls"] = dedup_pdf_urls
    payload["job_ids"] = [str(j.id) for j in jobs]
    payload["total_jobs"] = len(jobs)
    payload["completed_jobs"] = sum(
        1 for j in jobs if str(j.status or "").strip().lower() == "completed"
    )
    return JSONResponse(status_code=status.HTTP_200_OK, content=payload)

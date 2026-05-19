"""
Bitrix24 course outline API — separate from the Zoho flow.

Supports:
- Explicit JSON: ``{ "bitrix_record_id": "78776", "input_data": { ... } }``
- Task webhook / ``task.item.getdata`` body with ``DESCRIPTION`` table (parsed automatically)
- Minimal trigger: ``{ "taskId": 78776 }`` — task fields are fetched from Bitrix
- ``POST /api/v1/bitrix/courses`` — ``ONTASKADD`` (generate)
- ``POST /api/v1/bitrix/courses/refine`` — ``ONTASKCOMMENTADD`` with ``Refine:`` prefix
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

from app.api.auth_deps import bitrix_auth, bitrix_refine_auth
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
from app.services.bitrix_task_parser import (
    extract_message_id,
    extract_task_id,
    parse_refine_feedback_from_comment,
    resolve_bitrix_task_request,
)
from app.services.bitrix_tasks import fetch_task_for_outline, get_task_comment
from app.services.course_refine import BITRIX_OUTLINE_JOB_TYPE, run_bitrix_comment_refine
from app.utils.logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/bitrix", tags=["bitrix"])

BITRIX_GENERATE_EVENTS = frozenset({"ONTASKADD"})
BITRIX_REFINE_EVENTS = frozenset({"ONTASKCOMMENTADD"})

_TASK_LOG_KEYS = (
    "ID",
    "TITLE",
    "GROUP_ID",
    "GROUP_NAME",
    "GROUP",
    "FLOW_ID",
    "flow",
    "flowId",
    "STATUS",
    "REAL_STATUS",
    "RESPONSIBLE_ID",
    "RESPONSIBLE_NAME",
    "CREATED_BY_NAME",
    "CREATED_DATE",
    "DEADLINE",
)


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


def _log_task_summary(task_body: dict[str, Any], task_id: str, *, context: str) -> None:
    summary = {k: task_body.get(k) for k in _TASK_LOG_KEYS if k in task_body}
    logger.info(
        "BITRIX TASK %s | task_id=%s summary=%s",
        context,
        task_id,
        json.dumps(summary, ensure_ascii=False),
    )


async def _latest_bitrix_course_name(task_id: str) -> str | None:
    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(CourseJob)
                .where(
                    CourseJob.zoho_record_id == task_id,
                    CourseJob.job_type == BITRIX_OUTLINE_JOB_TYPE,
                    CourseJob.status == "completed",
                )
                .order_by(CourseJob.created_at.desc())
            )
            job = result.scalars().first()
        if job is None:
            return None
        raw = str(job.payload_json or "").strip()
        if not raw:
            return None
        data = json.loads(raw)
        if isinstance(data, dict):
            name = str(data.get("course_name") or "").strip()
            return name or None
    except Exception:
        logger.exception("Failed to load course_name for Bitrix refine | task_id=%s", task_id)
    return None


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
                    task_body = await fetch_task_for_outline(str(tid))
                except Exception as e:
                    logger.warning("Bitrix task fetch failed | taskId=%s error=%s", tid, e)
                    raise HTTPException(
                        status_code=422,
                        detail=f"Could not load task {tid} from Bitrix.",
                    ) from e
                _log_task_summary(task_body, str(tid), context="generate")
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


async def dispatch_bitrix_generate_webhook(request: Request) -> BitrixWebhookDispatch:
    payload = await _read_bitrix_payload(request)
    event = str(payload.get("event") or "").upper()

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


async def dispatch_bitrix_refine_webhook(payload: dict[str, Any]) -> BitrixWebhookDispatch:
    event = str(payload.get("event") or "").upper()
    logger.info("Bitrix /courses/refine | event=%s keys=%s", event, sorted(payload.keys()))

    if event not in BITRIX_REFINE_EVENTS:
        return BitrixWebhookDispatch(
            kind=BitrixWebhookKind.IGNORED,
            ignore_reason=f"unsupported_event:{event or 'missing'}",
        )

    task_id = extract_task_id(payload)
    if not task_id:
        return BitrixWebhookDispatch(
            kind=BitrixWebhookKind.IGNORED,
            ignore_reason="missing_task_id",
        )

    message_id = extract_message_id(payload)
    if not message_id:
        return BitrixWebhookDispatch(
            kind=BitrixWebhookKind.IGNORED,
            ignore_reason="missing_message_id",
        )

    try:
        task_body = await fetch_task_for_outline(task_id)
    except Exception as e:
        logger.warning("Bitrix refine task fetch failed | taskId=%s error=%s", task_id, e)
        return BitrixWebhookDispatch(
            kind=BitrixWebhookKind.IGNORED,
            ignore_reason="task_fetch_failed",
        )

    _log_task_summary(task_body, task_id, context="refine")
    try:
        _enforce_task_project_allowed(task_body, task_id)
    except HTTPException:
        return BitrixWebhookDispatch(
            kind=BitrixWebhookKind.IGNORED,
            ignore_reason="project_ignored",
        )

    try:
        comment_text = await get_task_comment(task_id, message_id)
    except Exception as e:
        logger.warning(
            "Bitrix refine comment fetch failed | taskId=%s messageId=%s error=%s",
            task_id,
            message_id,
            e,
        )
        return BitrixWebhookDispatch(
            kind=BitrixWebhookKind.IGNORED,
            ignore_reason="comment_fetch_failed",
        )

    prefix = (settings.BITRIX_REFINE_COMMENT_PREFIX or "Refine:").strip()
    if not comment_text or not comment_text.lower().startswith(prefix.lower()):
        logger.info(
            "Bitrix refine ignored (no %s prefix) | taskId=%s preview=%s",
            prefix,
            task_id,
            (comment_text or "")[:120],
        )
        return BitrixWebhookDispatch(
            kind=BitrixWebhookKind.IGNORED,
            ignore_reason="not_a_refine_comment",
        )

    feedback = parse_refine_feedback_from_comment(comment_text)
    if not feedback:
        return BitrixWebhookDispatch(
            kind=BitrixWebhookKind.IGNORED,
            ignore_reason="refine_feedback_too_short",
        )

    course_name = await _latest_bitrix_course_name(task_id)
    if not course_name:
        return BitrixWebhookDispatch(
            kind=BitrixWebhookKind.IGNORED,
            ignore_reason="no_completed_outline_for_task",
        )

    logger.info(
        "Bitrix refine queued | taskId=%s course_name=%s feedback_len=%s",
        task_id,
        course_name,
        len(feedback),
    )
    return BitrixWebhookDispatch(
        kind=BitrixWebhookKind.REFINE,
        refine_task_id=task_id,
        refine_feedback=feedback,
        refine_course_name=course_name,
    )


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
        "Outgoing webhook: ONTASKADD only → generate outline. "
        "Project/flow ids in BITRIX_ALLOWED_GROUP_IDS (default 34,36). "
        "Use /api/v1/bitrix/courses/refine for comment-based refine."
    ),
)
async def generate_course_from_bitrix(
    request: Request,
    background_tasks: BackgroundTasks,
    sync: bool = Query(False, description="Wait for completion and return full job JSON."),
):
    dispatch = await dispatch_bitrix_generate_webhook(request)

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


@router.post(
    "/courses/refine",
    dependencies=[bitrix_refine_auth],
    summary="Refine course outline from Bitrix task comment (ONTASKCOMMENTADD)",
    description=(
        "Separate outgoing webhook: ONTASKCOMMENTADD with comment starting with "
        "BITRIX_REFINE_COMMENT_PREFIX (default Refine:). Maps bitrix task id to prior outline "
        "(same as Zoho zoho_record_id). Auth: BITRIX_REFINE_APPLICATION_TOKEN or shared token."
    ),
)
async def refine_course_from_bitrix_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
):
    payload = await _read_bitrix_payload(request)
    dispatch = await dispatch_bitrix_refine_webhook(payload)

    if dispatch.kind == BitrixWebhookKind.IGNORED:
        return _ignored_response(dispatch.ignore_reason or "ignored")

    if dispatch.kind != BitrixWebhookKind.REFINE:
        return _ignored_response("missing_refine_request")

    background_tasks.add_task(
        run_bitrix_comment_refine,
        task_id=dispatch.refine_task_id or "",
        feedback=dispatch.refine_feedback or "",
        course_name=dispatch.refine_course_name or "",
    )
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={
            "ok": True,
            "status": "refine_queued",
            "bitrix_task_id": dispatch.refine_task_id,
            "bitrix_record_id": dispatch.refine_task_id,
            "course_name": dispatch.refine_course_name,
        },
    )


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

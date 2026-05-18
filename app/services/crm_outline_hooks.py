"""
CRM-agnostic hooks for the course outline background job.

- **Zoho** (default): ``POST /api/v1/courses`` — same behavior as before Bitrix was added.
- **Bitrix24**: ``POST /api/v1/bitrix/courses`` only — never touches Zoho OAuth/webhooks unless misconfigured.
"""
from __future__ import annotations

from typing import Literal

from app.models.job import CourseJob
from app.services import bitrix_integration, zoho_integration
from app.services.bitrix_crm import update_outline_record_fields as bitrix_update_fields
from app.services.zoho_crm import update_outline_module_record_fields

CrmSource = Literal["zoho", "bitrix"]


async def outline_status_in_progress(record_id: str, *, crm_source: CrmSource) -> None:
    if crm_source == "bitrix":
        await bitrix_integration.bitrix_outline_status_in_progress(record_id)
        return
    await update_outline_module_record_fields(
        zoho_record_id=record_id,
        fields={"Status": "In Progress"},
    )


async def outline_status_completed(record_id: str, *, crm_source: CrmSource) -> None:
    if crm_source == "bitrix":
        await bitrix_integration.bitrix_outline_status_completed(record_id)
        return
    await update_outline_module_record_fields(
        zoho_record_id=record_id,
        fields={"Status": "Completed"},
    )


async def outline_status_failed(record_id: str, *, crm_source: CrmSource) -> None:
    if crm_source == "bitrix":
        await bitrix_integration.bitrix_outline_status_failed(record_id)
        return
    await update_outline_module_record_fields(
        zoho_record_id=record_id,
        fields={"Status": "Failed to create - Try Again"},
    )


async def outline_update_public_curriculum_field(
    record_id: str,
    field_name: str,
    value: str,
    *,
    crm_source: CrmSource,
) -> None:
    if not field_name.strip():
        return
    if crm_source == "bitrix":
        await bitrix_update_fields(
            bitrix_record_id=record_id,
            fields={field_name: value},
        )
    else:
        await update_outline_module_record_fields(
            zoho_record_id=record_id,
            fields={field_name: value},
        )


async def outline_notify_job_finished(
    job: CourseJob,
    version_number: int | None,
    *,
    crm_source: CrmSource,
    attach_course_title: str | None,
) -> None:
    if crm_source == "bitrix":
        await bitrix_integration.bitrix_notify_course_outline_job_finished(
            job, version_number, attach_course_title=attach_course_title
        )
    else:
        await zoho_integration.zoho_notify_course_outline_job_finished(
            job, version_number, attach_course_title=attach_course_title
        )

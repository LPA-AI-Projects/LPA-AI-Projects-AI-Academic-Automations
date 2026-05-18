from datetime import datetime
import uuid
from typing import Any

from pydantic import BaseModel, Field


class CourseOutlineQueuedResponse(BaseModel):
    """Async POST /api/v1/courses: accepted; poll GET /api/v1/courses/{zoho_record_id}/outline-job."""

    job_id: uuid.UUID
    zoho_record_id: str
    status: str
    message: str = "Course outline generation queued."
    polling: dict[str, str] = Field(
        default_factory=dict,
        description="Preferred poll URL keyed by zoho_record_id.",
    )


class CourseOutlineJobResponse(BaseModel):
    """Latest course-outline generation job — no slides/Gamma/Drive-PPT fields."""

    job_id: uuid.UUID
    zoho_record_id: str
    status: str
    pdf_url: str | None = None
    version_number: int | None = None
    error: str | None = None
    created_at: datetime


class JobQueuedResponse(BaseModel):
    """Deprecated shape; prefer CourseOutlineQueuedResponse for POST /courses."""

    job_id: uuid.UUID
    zoho_record_id: str


class SlidesPipelineJobResponse(BaseModel):
    """Slides (Gamma) jobs only — use GET /api/v1/slides/{zoho_record_id}."""

    job_id: uuid.UUID
    zoho_record_id: str
    job_type: str | None = None
    status: str
    pdf_url: str | None = None
    ppt_url: str | None = None
    google_file_id: str | None = None
    google_batch_links: list[str] | None = None
    google_drive_course_folder_link: str | None = None
    gamma_batch_links: list[str] | None = None
    module_gamma_links: list[dict[str, str | None]] | None = None
    gamma_request_log: list[dict[str, Any]] | None = None
    zoho_attachment_payload: dict | None = None
    error: str | None = None
    version_number: int | None = None
    created_at: datetime


class JobResponse(BaseModel):
    """
    Legacy combined shape; prefer CourseOutlineJobResponse + GET /courses/.../outline-job
    or SlidesPipelineJobResponse + GET /slides/{zoho_record_id}.
    """

    job_id: uuid.UUID
    zoho_record_id: str
    job_type: str | None = None
    status: str
    pdf_url: str | None = None
    ppt_url: str | None = None
    google_file_id: str | None = None
    google_batch_links: list[str] | None = None
    google_drive_course_folder_link: str | None = None
    gamma_batch_links: list[str] | None = None
    module_gamma_links: list[dict[str, str | None]] | None = None
    gamma_request_log: list[dict[str, Any]] | None = None
    zoho_attachment_payload: dict | None = None
    error: str | None = None
    version_number: int | None = None
    created_at: datetime

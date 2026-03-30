"""Request/response models for pre/post assessment jobs."""
from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class AssessmentJobStatusResponse(BaseModel):
    job_id: UUID
    zoho_record_id: str
    status: str
    type: str = Field(description="pre or post")
    questions: list[dict[str, Any]] = Field(default_factory=list)
    assessment_docx_drive_url: str | None = None
    error: str | None = None
    created_at: datetime | None = None
    num_questions: int | None = None
    difficulty: str | None = None
    course_name: str | None = None


class AssessmentQueuedResponse(BaseModel):
    job_id: UUID
    zoho_record_id: str
    status: str
    message: str
    polling: dict[str, str]

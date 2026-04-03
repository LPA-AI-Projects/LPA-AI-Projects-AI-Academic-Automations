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


class AssessmentAppGenerateResponse(BaseModel):
    course_name: str
    pre_level: str
    post_level: str
    num_questions: int
    project_dir: str
    commands: list[str]
    validation: dict[str, Any]
    pre_questions: list[dict[str, Any]]
    post_questions: list[dict[str, Any]]
    # Two separate builds (pre + post); each has its own theme/layout for LMS variety.
    project_dir_pre: str | None = None
    project_dir_post: str | None = None
    zip_path: str | None = None
    zip_path_pre: str | None = None
    zip_path_post: str | None = None
    validation_pre: dict[str, Any] | None = None
    validation_post: dict[str, Any] | None = None
    ui_variant_pre: dict[str, Any] | None = None
    ui_variant_post: dict[str, Any] | None = None
    codesandbox_url_pre: str | None = None
    codesandbox_url_post: str | None = None
    codesandbox_id_pre: str | None = None
    codesandbox_id_post: str | None = None
    codesandbox_deploy_error_pre: str | None = None
    codesandbox_deploy_error_post: str | None = None
    # Backward compatibility: first successful deploy URL / pre id.
    codesandbox_url: str | None = None
    codesandbox_id: str | None = None
    codesandbox_deploy_error: str | None = None

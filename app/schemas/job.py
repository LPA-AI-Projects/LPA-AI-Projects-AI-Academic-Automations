from datetime import datetime
import uuid

from pydantic import BaseModel, Field


class JobCreateResponse(BaseModel):
    job_id: uuid.UUID
    status: str
    message: str = Field(
        ...,
        description="How to obtain course_id and pdf_url after async work finishes.",
    )


class JobStatusResponse(BaseModel):
    job_id: uuid.UUID
    zoho_record_id: str
    status: str
    pdf_url: str | None
    error: str | None
    course_id: uuid.UUID | None = None
    version_number: int | None = None
    created_at: datetime

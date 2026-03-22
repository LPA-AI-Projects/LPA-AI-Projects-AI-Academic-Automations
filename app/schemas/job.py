from datetime import datetime
import uuid

from pydantic import BaseModel, Field


class JobQueuedResponse(BaseModel):
    """Async POST /courses: job accepted; poll GET /api/v1/jobs/{job_id} for full result."""

    job_id: uuid.UUID
    zoho_record_id: str


class JobResponse(BaseModel):
    """
    Full job state:
    - GET /jobs/{job_id}
    - POST /courses?sync=true (waits until AI+PDF finish — may take minutes; Zoho may timeout)
    """

    job_id: uuid.UUID
    zoho_record_id: str
    status: str
    pdf_url: str | None = None
    error: str | None = None
    course_id: uuid.UUID | None = None
    version_number: int | None = None
    created_at: datetime

from datetime import datetime
import uuid

from pydantic import BaseModel


class JobCreateResponse(BaseModel):
    job_id: uuid.UUID
    status: str


class JobStatusResponse(BaseModel):
    job_id: uuid.UUID
    zoho_record_id: str
    status: str
    pdf_url: str | None
    error: str | None
    created_at: datetime

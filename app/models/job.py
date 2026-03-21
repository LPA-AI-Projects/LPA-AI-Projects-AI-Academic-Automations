from sqlalchemy import Column, String, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from sqlalchemy import DateTime
import uuid

from app.core.database import Base


class CourseJob(Base):
    __tablename__ = "course_jobs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    zoho_record_id = Column(String, nullable=False)
    status = Column(String, nullable=False, default="pending")
    pdf_url = Column(String, nullable=True)
    error = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now())

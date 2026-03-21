from sqlalchemy import Column, String, Integer, Text, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from sqlalchemy import DateTime
import uuid

from app.core.database import Base


class Course(Base):
    __tablename__ = "courses"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    zoho_record_id = Column(String)
    created_at = Column(DateTime, server_default=func.now())


class CourseVersion(Base):
    __tablename__ = "course_versions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    course_id = Column(UUID(as_uuid=True), ForeignKey("courses.id"))
    version_number = Column(Integer)
    outline_text = Column(Text)
    pdf_url = Column(String)
    feedback = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now())

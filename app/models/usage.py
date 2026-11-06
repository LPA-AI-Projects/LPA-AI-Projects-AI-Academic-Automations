import uuid

from sqlalchemy import Boolean, Column, DateTime, Integer, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from app.core.database import Base


class LlmUsage(Base):
    """One Anthropic call made for an outline generation or a refinement (token counts as billed)."""

    __tablename__ = "llm_usage"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # "outline" or "refine"
    kind = Column(String, nullable=False, index=True)
    # course_jobs.id for outline generation (null for refinements)
    job_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    # courses.id for refinements (null for outline generation)
    course_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    zoho_record_id = Column(String, nullable=True, index=True)
    # learning_objectives, context_profile, research, outline_json, outline_text_fallback, refine_outline, ...
    label = Column(String, nullable=False)
    model = Column(String, nullable=False)
    input_tokens = Column(Integer, nullable=False, default=0)
    output_tokens = Column(Integer, nullable=False, default=0)
    cache_write_tokens = Column(Integer, nullable=False, default=0)
    cache_read_tokens = Column(Integer, nullable=False, default=0)
    web_searches = Column(Integer, nullable=False, default=0)
    stop_reason = Column(String, nullable=True)
    structured = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, server_default=func.now(), index=True)

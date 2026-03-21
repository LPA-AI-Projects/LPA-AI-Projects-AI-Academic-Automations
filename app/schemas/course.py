from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
import uuid


# ─── Request schemas ──────────────────────────────────────────────────────────

class GenerateCourseRequest(BaseModel):
    zoho_record_id: str = Field(..., min_length=1, description="Zoho CRM record ID")
    input_data: dict = Field(..., description="All course generation parameters")

    model_config = {
        "json_schema_extra": {
            "example": {
                "zoho_record_id": "ZHO-12345",
                "input_data": {
                    "company_name": "LearnQuest",
                    "course_name": "Avaloq - Core MDB",
                    "department": "Information Technology (IT)",
                    "designation": "Delivery Education Manager",
                    "level_of_training": "Intermediate",
                    "need_of_training": "End Customer --- IBM",
                    "size_of_company": "Above 10k",
                    "duration": "4 weeks",
                }
            }
        }
    }


class RefineCourseRequest(BaseModel):
    feedback: str = Field(..., min_length=10, description="Feedback for refinement")

    model_config = {
        "json_schema_extra": {
            "example": {
                "feedback": "Please add more practical exercises and reduce theory sections."
            }
        }
    }


# ─── Response schemas ─────────────────────────────────────────────────────────

class CourseVersionResponse(BaseModel):
    version_id: uuid.UUID
    course_id: uuid.UUID
    version_number: int
    pdf_url: Optional[str]
    outline: str
    created_at: datetime

    model_config = {"from_attributes": True}


class VersionSummary(BaseModel):
    version_id: uuid.UUID
    version_number: int
    pdf_url: Optional[str]
    feedback: Optional[str]
    created_at: datetime

    model_config = {"from_attributes": True}


class CourseVersionsResponse(BaseModel):
    course_id: uuid.UUID
    zoho_record_id: str
    versions: list[VersionSummary]
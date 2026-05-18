from __future__ import annotations

from pydantic import BaseModel, Field

from app.schemas.course import CourseInputData


class BitrixGenerateCourseRequest(BaseModel):
    """Same outline payload as Zoho; CRM id is the Bitrix deal/lead/spa item id."""

    bitrix_record_id: str = Field(..., min_length=1, description="Bitrix CRM record ID (deal, lead, etc.)")
    input_data: CourseInputData = Field(..., description="Course generation parameters")

    model_config = {
        "json_schema_extra": {
            "example": {
                "bitrix_record_id": "123",
                "input_data": {
                    "company_name": "Acme Corp",
                    "course_name": "Leadership Essentials",
                    "department": "HR",
                    "designation": "Manager",
                    "level_of_training": "Intermediate",
                    "mode_of_training": "Hybrid",
                },
            }
        }
    }

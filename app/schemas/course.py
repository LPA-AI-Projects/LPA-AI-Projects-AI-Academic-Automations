from __future__ import annotations

from datetime import datetime
from typing import Any, Optional
import uuid
import re

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator


# ─── Request schemas ──────────────────────────────────────────────────────────


class CourseInputData(BaseModel):
    """
    Fields sent from Zoho (or JSON clients) for course outline generation.

    Required fields must be non-empty strings. All other fields are optional;
    omitted or blank optional values are dropped from the AI context (exclude_none).
    Unknown keys are allowed so CRM can add future fields without API changes.
    """

    model_config = ConfigDict(extra="allow")

    company_name: str = ""
    course_name: str = Field(..., min_length=1)
    department: str = ""
    designation: str = ""
    level_of_training: str = ""

    need_of_training: str = ""
    specific_questions: list[str] | str = Field(default_factory=list)
    goal_of_training: str = ""
    size_of_company: str = ""
    duration: str = ""

    no_of_pax: Optional[str] = Field(
        None,
        description="Optional: expected number of participants (from Zoho).",
    )
    languages_preferred: Optional[str] = Field(
        None,
        validation_alias=AliasChoices("languages_preferred", "languages_prefered"),
        description="Optional: preferred training language(s). Accepts languages_prefered (Zoho spelling).",
    )
    additional_certifications: Optional[str] = Field(
        None,
        description="Optional: related or target certifications (e.g. exam paths).",
    )
    additional_notes: Optional[str] = Field(
        None,
        description="Optional: any other CRM notes for design (delivery, constraints, etc.).",
    )
    important_topics: Optional[list[str] | str] = Field(
        None,
        description=(
            "Optional: must-cover topics from CRM. If provided, these topics must appear "
            "in the generated outline/modules."
        ),
    )

    @field_validator(
        "no_of_pax",
        "languages_preferred",
        "additional_certifications",
        "additional_notes",
        mode="before",
    )
    @classmethod
    def _optional_str_strip(cls, v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, str):
            s = v.strip()
            return s if s else None
        return v

    @field_validator("important_topics", mode="before")
    @classmethod
    def _coerce_important_topics(cls, v: Any) -> list[str] | None:
        if v is None:
            return None
        if isinstance(v, list):
            out = [str(x).strip() for x in v if str(x).strip()]
            return out or None
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return None
            # Accept comma or newline separated values from CRM.
            parts = [p.strip() for p in re.split(r"[\n,]+", s) if p.strip()]
            return parts or None
        return None

    @field_validator("specific_questions", mode="before")
    @classmethod
    def _coerce_specific_questions(cls, v: Any) -> list[str] | str:
        if v is None:
            return []
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            if not v.strip():
                return []
            return [
                q.strip("-• ").strip()
                for q in v.splitlines()
                if q.strip()
            ]
        return []


class GenerateCourseRequest(BaseModel):
    zoho_record_id: str = Field(..., min_length=1, description="Zoho CRM record ID")
    input_data: CourseInputData = Field(..., description="Course generation parameters")

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
                    "no_of_pax": "24",
                    "languages_prefered": "English",
                    "additional_certifications": "PMP alignment optional",
                },
            }
        }
    }


class RefineCourseRequest(BaseModel):
    feedback: str = Field(..., min_length=10, description="Feedback for refinement")
    course_name: Optional[str] = Field(
        None,
        validation_alias=AliasChoices("course_name", "title", "note_title"),
        description=(
            "Which course track to refine when zoho_record_id has multiple outlines. "
            "Must match the per-job payload course_name from generation. "
            "Also accepted as JSON keys title or note_title (same as /courses/refine)."
        ),
    )

    model_config = {
        "populate_by_name": True,
        "json_schema_extra": {
            "example": {
                "feedback": "Please add more practical exercises and reduce theory sections.",
                "course_name": "My Course Title",
            }
        },
    }


# ─── Response schemas ─────────────────────────────────────────────────────────


class CourseVersionResponse(BaseModel):
    version_id: uuid.UUID
    zoho_record_id: str
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
    zoho_record_id: str
    versions: list[VersionSummary]

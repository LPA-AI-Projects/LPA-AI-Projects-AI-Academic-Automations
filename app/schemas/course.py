from __future__ import annotations

from datetime import datetime
from typing import Any, Optional, Union
import uuid

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
    level_of_training: Optional[str] = Field(
        None,
        description="Optional: participant level (e.g. Beginner, Intermediate, Advanced). If omitted, prompts assume Intermediate unless context implies otherwise.",
    )
    mode_of_training: Optional[str] = Field(
        None,
        description="Delivery format: Online, Onsite (offline/classroom), or Hybrid.",
    )

    need_of_training: str = ""
    specific_questions: list[str] | str = Field(default_factory=list)
    goal_of_training: str = ""
    size_of_company: str = ""
    duration: str = ""
    training_days: Optional[int] = Field(
        None,
        ge=1,
        description="Optional: number of training days. Preferred over free-text duration when provided.",
    )
    per_day_duration_in_hours: Optional[float] = Field(
        None,
        gt=0,
        description="Optional: duration per training day in hours. Preferred over free-text duration when provided.",
    )

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
    topics_to_include: Optional[str] = Field(
        None,
        validation_alias=AliasChoices(
            "topics_to_include",
            "topics_must_include",
            "mandatory_topics",
            "important_topics",
        ),
        description=(
            "Topics or themes that must appear in the course design (mandatory inclusions). "
            "Comma- or newline-separated lists are OK. Legacy key topics_must_include is accepted."
        ),
    )
    referral_course_links: Optional[Union[str, list[str]]] = Field(
        None,
        validation_alias=AliasChoices(
            "referral_course_links",
            "referral_course_link",
            "referral_links",
        ),
        description=(
            "Optional reference page URLs (comma-, semicolon-, or newline-separated, or a JSON array). "
            "The outline generator uses web research to read these pages and align modules and topics "
            "with their content where appropriate, in addition to the client's brief."
        ),
    )

    @field_validator(
        "no_of_pax",
        "languages_preferred",
        "additional_certifications",
        "additional_notes",
        "topics_to_include",
        "level_of_training",
        "mode_of_training",
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

    @field_validator("training_days", mode="before")
    @classmethod
    def _coerce_training_days(cls, v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return None
            try:
                return int(float(s))
            except Exception:
                return v
        if isinstance(v, (int, float)):
            return int(v)
        return v

    @field_validator("per_day_duration_in_hours", mode="before")
    @classmethod
    def _coerce_per_day_hours(cls, v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return None
            try:
                return float(s)
            except Exception:
                return v
        if isinstance(v, (int, float)):
            return float(v)
        return v

    @field_validator("referral_course_links", mode="before")
    @classmethod
    def _coerce_referral_course_links(cls, v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, list):
            parts = [str(x).strip() for x in v if str(x).strip()]
            return "\n".join(parts) if parts else None
        if isinstance(v, str):
            s = v.strip()
            return s if s else None
        return v

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
                    "mode_of_training": "Hybrid",
                    "need_of_training": "End Customer --- IBM",
                    "size_of_company": "Above 10k",
                    "duration": "4 weeks",
                    "training_days": 2,
                    "per_day_duration_in_hours": 6,
                    "no_of_pax": "24",
                    "languages_prefered": "English",
                    "additional_certifications": "PMP alignment optional",
                    "topics_to_include": "Agile estimation, risk register, stakeholder communication",
                    "referral_course_links": "https://example.com/reference-course-overview",
                },
            }
        }
    }


class RefineCourseRequest(BaseModel):
    feedback: str = Field(..., min_length=10, description="Feedback for refinement")
    course_name: str = Field(
        ...,
        min_length=1,
        validation_alias=AliasChoices("course_name", "title", "note_title"),
        description=(
            "Required: which course track to refine for this zoho_record_id. "
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

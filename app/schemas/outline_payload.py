from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator


class ProgramInsight(BaseModel):
    paragraphs: list[str] = Field(default_factory=list)
    bullets: list[str] = Field(default_factory=list)


class CourseDetails(BaseModel):
    regions_served: str = ""
    course_duration: str = ""
    total_learning_hours: str = ""
    # Narrative above the Course Details table (distinct from program_insight; see brochure prompt).
    details_page_intro: str = ""
    key_benefits: str = ""
    value_addition: str = ""
    location: str = ""
    date_time: str = ""


class Objective(BaseModel):
    title: str
    description: str = ""


class CapabilityImpact(BaseModel):
    title: str
    description: str = ""


def _coerce_str_list(v: Any) -> list[str]:
    if v is None or v == "":
        return []
    if isinstance(v, str):
        s = v.strip()
        return [s] if s else []
    if isinstance(v, list):
        return v
    return v


class IndustrySimulation(BaseModel):
    """One course-level box printed after the modules table when AI integration is on."""

    title: str = "Industry Simulation"
    subtitle: str = ""
    paragraphs: list[str] = Field(default_factory=list)

    @field_validator("paragraphs", mode="before")
    @classmethod
    def _paragraphs_list(cls, v: Any) -> Any:
        return _coerce_str_list(v)


class ModuleItem(BaseModel):
    module_title: str
    overview: str = ""
    topics: list[str] = Field(default_factory=list)
    exercises: list[str] = Field(default_factory=list)
    case_studies: list[str] = Field(default_factory=list)
    simulations: list[str] = Field(default_factory=list)
    # Backward compatibility for older payloads.
    activities: list[str] = Field(default_factory=list)
    # Bitrix AI-integration add-on only. Empty on every other outline.
    ai_integration: list[str] = Field(default_factory=list)
    skills_achieved: list[str] = Field(default_factory=list)

    @field_validator("ai_integration", "skills_achieved", mode="before")
    @classmethod
    def _addon_lists(cls, v: Any) -> Any:
        return _coerce_str_list(v)


class CourseOutlinePayload(BaseModel):
    course_title: str
    duration: str
    total_hours: str
    program_insight: ProgramInsight
    course_details: CourseDetails
    # Narrative before lettered objectives (Learning Objective page).
    learning_objectives_intro: str = ""
    learning_objectives: list[Objective] = Field(default_factory=list)
    # After lettered objectives: two paragraphs separated by \\n\\n (see brochure prompt for line targets).
    learning_objectives_closing: str = ""
    # Capability Impact page: intro before the 6 points, closing after (optional).
    capability_impact_intro: str = ""
    capability_impact: list[CapabilityImpact] = Field(default_factory=list)
    # After the six rows: typically three paragraphs separated by \\n\\n (three sentences each).
    capability_impact_closing: str = ""
    modules: list[ModuleItem] = Field(default_factory=list)
    # Bitrix AI-integration add-on only. Omitted from the PDF when empty.
    industry_simulation: IndustrySimulation | None = None
    automation_sandbox: IndustrySimulation | None = None

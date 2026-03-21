from __future__ import annotations

from pydantic import BaseModel, Field


class ProgramInsight(BaseModel):
    paragraphs: list[str] = Field(default_factory=list)
    bullets: list[str] = Field(default_factory=list)


class CourseDetails(BaseModel):
    regions_served: str = ""
    course_duration: str = ""
    total_learning_hours: str = ""
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


class ModuleItem(BaseModel):
    module_title: str
    topics: list[str] = Field(default_factory=list)
    activities: list[str] = Field(default_factory=list)


class CourseOutlinePayload(BaseModel):
    course_title: str
    duration: str
    total_hours: str
    program_insight: ProgramInsight
    course_details: CourseDetails
    learning_objectives: list[Objective] = Field(default_factory=list)
    capability_impact: list[CapabilityImpact] = Field(default_factory=list)
    modules: list[ModuleItem] = Field(default_factory=list)

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from app.core.config import settings
from app.schemas.outline_payload import CourseOutlinePayload
from app.services.claude import ClaudeService
from app.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class OutlineGraphResult:
    context_profile: str
    learning_objectives: str
    research_notes: str
    outline_text: str
    outline_payload: CourseOutlinePayload


async def run_outline_langgraph_like_pipeline(*, input_data: dict[str, Any]) -> OutlineGraphResult:
    """
    LangGraph-style modular pipeline (sequential node execution):
    fetch/context -> objectives -> research -> outline -> normalize.
    """
    ai = ClaudeService()
    context_text = json.dumps(input_data, ensure_ascii=False, indent=2)
    read_timeout = max(120.0, float(getattr(settings, "ANTHROPIC_READ_TIMEOUT_S", 600.0) or 600.0))

    logger.info("Outline graph node started | node=context_profile")
    context_profile = await ai.build_context_profile(context_text, timeout_s=min(read_timeout, 180.0))

    logger.info("Outline graph node started | node=learning_objectives")
    learning_objectives = await ai.build_learning_objectives(context_text, timeout_s=min(read_timeout, 300.0))

    logger.info("Outline graph node started | node=research")
    research_notes = await ai.research_support_data(
        context_text, learning_objectives, timeout_s=read_timeout
    )

    logger.info("Outline graph node started | node=roi_outline")
    outline_text = await ai.build_roi_outline_with_research(
        context_text=context_text,
        learning_objectives_text=learning_objectives,
        research_notes_text=research_notes,
        context_profile_text=context_profile,
        timeout_s=read_timeout,
    )

    logger.info("Outline graph node started | node=normalize")
    try:
        outline_payload = await ai.normalize_to_payload(outline_text, timeout_s=min(read_timeout, 300.0))
    except RuntimeError:
        # Fallback to direct structured generation to preserve reliability.
        outline_payload = await ai.build_roi_course_outline_json(
            context_text=context_text,
            learning_objectives_text=learning_objectives,
            research_notes_text=research_notes,
            context_profile_text=context_profile,
        )

    return OutlineGraphResult(
        context_profile=context_profile,
        learning_objectives=learning_objectives,
        research_notes=research_notes,
        outline_text=outline_text,
        outline_payload=outline_payload,
    )


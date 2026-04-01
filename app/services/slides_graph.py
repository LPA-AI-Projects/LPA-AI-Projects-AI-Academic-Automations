from __future__ import annotations

from typing import Any, TypedDict

from langgraph.graph import END, StateGraph

from app.services.slide_generator import generate_slide
from app.services.slide_planner import plan_slides
from app.services.slide_validator import validate_slides, validate_slides_ai
from app.utils.logger import get_logger

logger = get_logger(__name__)


class ModulePipelineState(TypedDict):
    module_name: str
    module_text: str
    lesson_text: str | None
    instructor_text: str | None
    planner_model: str | None
    generator_model: str | None
    validator_model: str | None
    min_slides: int
    max_slides: int
    max_loops: int
    loop_count: int
    planned_slides: list[dict[str, Any]]
    generated_slides: list[dict[str, Any]]
    approved: bool
    issues: list[str]
    fix_instructions: str
    has_lesson_plan: bool
    failed: bool


def _normalize_plan_slides(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for i, raw in enumerate(items, start=1):
        if not isinstance(raw, dict):
            continue
        title = str(raw.get("title") or "").strip()
        if not title:
            title = f"Slide {i}"
        slide_type = str(raw.get("type") or "content").strip().lower()
        if slide_type not in {"content", "activity", "summary"}:
            slide_type = "content"
        out.append({"title": title[:120], "type": slide_type})
    return out


def _enforce_slide_count(slides: list[dict[str, Any]], *, min_slides: int, max_slides: int) -> list[dict[str, Any]]:
    cleaned = slides[:max_slides]
    if not cleaned:
        cleaned = [{"title": "Module Overview", "type": "content"}]

    has_summary = any(str(s.get("type") or "").lower() == "summary" for s in cleaned)
    if not has_summary:
        cleaned.append({"title": "Module Summary and Key Takeaways", "type": "summary"})

    if len(cleaned) > max_slides:
        cleaned = cleaned[: max_slides - 1] + [{"title": "Module Summary and Key Takeaways", "type": "summary"}]

    while len(cleaned) < min_slides:
        idx = len(cleaned) + 1
        cleaned.insert(max(0, len(cleaned) - 1), {"title": f"Applied Concept {idx}", "type": "content"})
        if len(cleaned) >= max_slides:
            break
    return cleaned


async def _planner_node(state: ModulePipelineState) -> ModulePipelineState:
    logger.info(
        "Slides planner started | module=%s model=%s",
        state["module_name"],
        state.get("planner_model") or "default",
    )
    plan = await plan_slides(
        outline=state["module_text"],
        lesson=state["lesson_text"],
        activity=None,
        instructor=state["instructor_text"],
        model=state["planner_model"],
    )
    raw_slides = plan.get("slides") if isinstance(plan, dict) else []
    normalized = _normalize_plan_slides(raw_slides if isinstance(raw_slides, list) else [])
    state["planned_slides"] = _enforce_slide_count(
        normalized,
        min_slides=state["min_slides"],
        max_slides=state["max_slides"],
    )
    logger.info(
        "Slides planner completed | module=%s planned_slides=%s",
        state["module_name"],
        len(state["planned_slides"]),
    )
    return state


async def _generator_node(state: ModulePipelineState) -> ModulePipelineState:
    logger.info(
        "Slides generator started | module=%s model=%s slides=%s loop=%s",
        state["module_name"],
        state.get("generator_model") or "default",
        len(state.get("planned_slides") or []),
        int(state.get("loop_count", 0)),
    )
    context = {
        "course_outline": state["module_text"][:150000],
        "lesson_plan_and_activity_plan": (state["lesson_text"] or "")[:150000],
        "instructor_ppt": (state["instructor_text"] or "")[:150000],
    }
    generated: list[dict[str, Any]] = []
    for slide in state["planned_slides"]:
        generated.append(
            await generate_slide(
                slide=slide,
                context=context,
                model=state["generator_model"],
                fix_instructions=state.get("fix_instructions") or "",
            )
        )
    state["generated_slides"] = generated
    logger.info(
        "Slides generator completed | module=%s generated_slides=%s",
        state["module_name"],
        len(generated),
    )
    return state


async def _validator_node(state: ModulePipelineState) -> ModulePipelineState:
    logger.info(
        "Slides validator started | module=%s model=%s",
        state["module_name"],
        state.get("validator_model") or "default",
    )
    result = await validate_slides_ai(
        planned_slides=state["planned_slides"],
        generated_slides=state["generated_slides"],
        has_lesson_plan=state["has_lesson_plan"],
        model=state["validator_model"],
    )
    state["approved"] = bool(result.get("approved"))
    state["issues"] = [str(i) for i in (result.get("issues") or [])]
    state["fix_instructions"] = str(result.get("fix_instructions") or "").strip()

    # Always run local structural sanitizer.
    state["generated_slides"] = validate_slides(
        planned_slides=state["planned_slides"],
        generated_slides=state["generated_slides"],
    )
    if not state["approved"]:
        state["loop_count"] = int(state.get("loop_count", 0)) + 1
    logger.info(
        "Slides validator completed | module=%s approved=%s issues=%s loop=%s",
        state["module_name"],
        state["approved"],
        len(state["issues"]),
        int(state.get("loop_count", 0)),
    )
    return state


def _route_after_validator(state: ModulePipelineState) -> str:
    if state.get("approved"):
        return "approved"
    if int(state.get("loop_count", 0)) < int(state.get("max_loops", 2)):
        return "retry"
    state["failed"] = True
    return "failed"


def _build_module_graph():
    graph = StateGraph(ModulePipelineState)
    graph.add_node("planner", _planner_node)
    graph.add_node("generator", _generator_node)
    graph.add_node("validator", _validator_node)

    graph.set_entry_point("planner")
    graph.add_edge("planner", "generator")
    graph.add_edge("generator", "validator")
    graph.add_conditional_edges(
        "validator",
        _route_after_validator,
        {
            "retry": "generator",
            "approved": END,
            "failed": END,
        },
    )
    return graph.compile()


_MODULE_GRAPH = _build_module_graph()


async def run_module_slides_pipeline(
    *,
    module_name: str,
    module_text: str,
    lesson_text: str | None,
    instructor_text: str | None,
    planner_model: str | None,
    generator_model: str | None,
    validator_model: str | None,
    min_slides: int,
    max_slides: int,
    max_loops: int,
) -> list[dict[str, Any]]:
    initial: ModulePipelineState = {
        "module_name": module_name,
        "module_text": module_text,
        "lesson_text": lesson_text,
        "instructor_text": instructor_text,
        "planner_model": planner_model,
        "generator_model": generator_model,
        "validator_model": validator_model,
        "min_slides": min_slides,
        "max_slides": max_slides,
        "max_loops": max_loops,
        "loop_count": 0,
        "planned_slides": [],
        "generated_slides": [],
        "approved": False,
        "issues": [],
        "fix_instructions": "",
        "has_lesson_plan": bool((lesson_text or "").strip()),
        "failed": False,
    }
    out = await _MODULE_GRAPH.ainvoke(initial)
    if out.get("failed"):
        raise RuntimeError(
            f"Module '{module_name}' failed validation after {max_loops} attempts: "
            + "; ".join(out.get("issues") or ["validator rejected output"])
        )
    return out.get("generated_slides") or []

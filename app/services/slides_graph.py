"""
Per-module pipeline (LangGraph).

- **Planning**: ``plan_slides`` → ``module_plan`` only (no per-slide JSON)
- **Generation**: ``generate_module_body_text`` → one Markdown module document
- **Validation**: ``validate_module_body_ai`` + local exercise coverage checks

Graph entry: ``run_module_slides_pipeline`` (used from ``slides_service``).
"""
from __future__ import annotations

from typing import Any, TypedDict

from langgraph.graph import END, StateGraph

from app.services.slide_generator import generate_module_body_text
from app.services.slide_planner import plan_slides
from app.services.slide_validator import (
    merge_validator_result_with_local_checks,
    normalize_module_body,
    validate_module_body_ai,
)
from app.utils.logger import get_logger

logger = get_logger(__name__)


class ModulePipelineState(TypedDict):
    module_name: str
    module_text: str
    lesson_text: str | None
    instructor_text: str | None
    course_map: str
    instructor_ppt_priority: str
    planner_model: str | None
    generator_model: str | None
    validator_model: str | None
    min_slides: int
    max_slides: int
    max_loops: int
    loop_count: int
    module_plan: dict[str, Any]
    target_card_count: int
    module_body: str
    approved: bool
    issues: list[str]
    fix_instructions: str
    has_lesson_plan: bool
    failed: bool


def _clamp_card_count(raw: Any, *, lo: int, hi: int) -> int:
    try:
        n = int(raw)
    except (TypeError, ValueError):
        n = lo
    return max(lo, min(hi, n))


async def _planner_node(state: ModulePipelineState) -> ModulePipelineState:
    logger.info(
        "Module planner started | module=%s model=%s",
        state["module_name"],
        state.get("planner_model") or "default",
    )
    plan = await plan_slides(
        outline=state["module_text"],
        lesson=state["lesson_text"],
        activity=None,
        instructor=state["instructor_text"],
        instructor_ppt_priority=state["instructor_ppt_priority"],
        model=state["planner_model"],
    )
    mp = plan.get("module_plan") if isinstance(plan.get("module_plan"), dict) else {}
    state["module_plan"] = mp
    state["target_card_count"] = _clamp_card_count(
        mp.get("no_of_slides"),
        lo=state["min_slides"],
        hi=state["max_slides"],
    )
    logger.info(
        "Module planner completed | module=%s target_card_count=%s",
        state["module_name"],
        state["target_card_count"],
    )
    return state


async def _generator_node(state: ModulePipelineState) -> ModulePipelineState:
    logger.info(
        "Module body generator started | module=%s model=%s loop=%s",
        state["module_name"],
        state.get("generator_model") or "default",
        int(state.get("loop_count", 0)),
    )
    context = {
        "course_map": (state.get("course_map") or "")[:50_000],
        "module_plan": state.get("module_plan") or {},
        "course_outline": state["module_text"][:150000],
        "lesson_plan_and_activity_plan": (state["lesson_text"] or "")[:150000],
        "instructor_ppt": (state["instructor_text"] or "")[:150000],
    }
    fix = state.get("fix_instructions") or "" if int(state.get("loop_count", 0)) > 0 else ""
    body = await generate_module_body_text(
        module_plan=state.get("module_plan") or {},
        context=context,
        instructor_ppt_priority=state["instructor_ppt_priority"],
        model=state["generator_model"],
        fix_instructions=fix,
    )
    state["module_body"] = body
    logger.info(
        "Module body generator completed | module=%s chars=%s",
        state["module_name"],
        len(body or ""),
    )
    return state


async def _validator_node(state: ModulePipelineState) -> ModulePipelineState:
    logger.info(
        "Module validator started | module=%s model=%s",
        state["module_name"],
        state.get("validator_model") or "default",
    )
    ai_result = await validate_module_body_ai(
        module_body=state["module_body"],
        module_plan=state.get("module_plan") or {},
        course_map=state.get("course_map") or "",
        module_text=state.get("module_text"),
        lesson_text=state.get("lesson_text"),
        instructor_text=state.get("instructor_text"),
        has_lesson_plan=state["has_lesson_plan"],
        model=state["validator_model"],
    )
    result = merge_validator_result_with_local_checks(
        ai_result=ai_result,
        module_text=state.get("module_text"),
        lesson_text=state.get("lesson_text"),
        instructor_text=state.get("instructor_text"),
        generated_body=state["module_body"],
    )
    state["approved"] = bool(result.get("approved"))
    state["issues"] = [str(i) for i in (result.get("issues") or [])]
    state["fix_instructions"] = str(result.get("fix_instructions") or "").strip()
    state["module_body"] = normalize_module_body(state["module_body"])
    if not state["approved"]:
        state["loop_count"] = int(state.get("loop_count", 0)) + 1
    logger.info(
        "Module validator completed | module=%s approved=%s issues=%s loop=%s",
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
    course_map: str = "",
    planner_model: str | None,
    generator_model: str | None,
    validator_model: str | None,
    min_slides: int,
    max_slides: int,
    max_loops: int,
    instructor_ppt_priority: str = "supplement",
) -> dict[str, Any]:
    """
    Returns ``module_body`` (Markdown), ``card_count`` for Gamma batch sizing, and ``module_plan``.
    """
    initial: ModulePipelineState = {
        "module_name": module_name,
        "module_text": module_text,
        "lesson_text": lesson_text,
        "instructor_text": instructor_text,
        "course_map": course_map,
        "instructor_ppt_priority": instructor_ppt_priority
        if instructor_ppt_priority in ("primary", "supplement")
        else "supplement",
        "planner_model": planner_model,
        "generator_model": generator_model,
        "validator_model": validator_model,
        "min_slides": min_slides,
        "max_slides": max_slides,
        "max_loops": max_loops,
        "loop_count": 0,
        "module_plan": {},
        "target_card_count": min_slides,
        "module_body": "",
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
    return {
        "module_body": str(out.get("module_body") or ""),
        "card_count": int(out.get("target_card_count") or min_slides),
        "module_plan": out.get("module_plan") if isinstance(out.get("module_plan"), dict) else {},
    }

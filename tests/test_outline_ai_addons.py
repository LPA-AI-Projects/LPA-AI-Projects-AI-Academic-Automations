"""AI-integration outline add-on stays off unless Bitrix says Yes."""
import json

from app.schemas.outline_payload import CourseOutlinePayload, IndustrySimulation, ModuleItem
from app.services.claude import (
    AI_INTEGRATION_OUTLINE_ADDON,
    COURSE_OUTLINE_JSON_BROCHURE,
    STRICT_JSON_OUTPUT_RULES,
)
from app.services.outline_ai_addons import (
    clear_ai_outline_addons,
    context_requests_ai_integration,
    dump_outline_dict,
    outline_text_has_ai_addons,
    restore_ai_addons_if_dropped,
)
from app.services.pdf_service import _build_dynamic_module_pages


def _payload() -> CourseOutlinePayload:
    return CourseOutlinePayload(
        course_title="PMP",
        duration="5 Days",
        total_hours="40 Hours",
        program_insight={"paragraphs": ["A"], "bullets": ["B"]},
        course_details={"regions_served": "UAE"},
        modules=[
            ModuleItem(
                module_title="Scope",
                topics=["Scope planning"],
                exercises=["Exercise: build a scope statement."],
                ai_integration=["Analyze scope deviation risks early."],
                skills_achieved=["Scope planning, WBS development, and change control."],
            )
        ],
        industry_simulation=IndustrySimulation(
            subtitle="Enterprise delivery simulation",
            paragraphs=["Participants lead a transformation program."],
        ),
        automation_sandbox=IndustrySimulation(
            title="Automation Sandbox",
            subtitle="AI-powered project delivery sandbox",
            paragraphs=["Participants design automated project workflows."],
        ),
    )


def test_zoho_prompt_and_saved_json_stay_unchanged():
    base_prompt = COURSE_OUTLINE_JSON_BROCHURE + "\n\n" + STRICT_JSON_OUTPUT_RULES
    zoho_context = json.dumps(
        {"company_name": "Acme", "course_name": "Excel", "goal_of_training": "Upskill"}
    )
    bitrix_yes = json.dumps({"course_name": "PMP", "ai_integration": "Yes"})
    bitrix_no = json.dumps({"course_name": "PMP"})

    assert context_requests_ai_integration(zoho_context) is False
    assert context_requests_ai_integration(bitrix_no) is False
    assert context_requests_ai_integration(bitrix_yes) is True
    assert "AI INTEGRATION ADD-ON" not in base_prompt
    assert "industry_simulation" not in base_prompt
    assert "automation_sandbox" not in base_prompt
    assert "Automation Sandbox" in AI_INTEGRATION_OUTLINE_ADDON
    assert "AI INTEGRATION ADD-ON" in (base_prompt + "\n\n" + AI_INTEGRATION_OUTLINE_ADDON)

    cleared = _payload()
    clear_ai_outline_addons(cleared)
    saved = dump_outline_dict(cleared)
    assert "industry_simulation" not in saved
    assert "automation_sandbox" not in saved
    assert "ai_integration" not in saved["modules"][0]
    assert "skills_achieved" not in saved["modules"][0]
    assert saved["modules"][0]["exercises"]


def test_pdf_omits_addon_when_fields_empty():
    html = _build_dynamic_module_pages(
        [{"name": "Scope", "topics": ["Planning"], "exercises": ["Exercise: plan the work."]}]
    )
    assert "AI Integration" not in html
    assert "Skills Achieved" not in html
    assert "Industry Simulation" not in html
    assert "Automation Sandbox" not in html
    assert "Exercise:" in html


def test_pdf_renders_addon_only_when_present():
    html = _build_dynamic_module_pages(
        [
            {
                "name": "Scope",
                "topics": ["Planning"],
                "exercises": ["Exercise: plan the work."],
                "ai_integration": ["Analyze scope deviation risks early."],
                "skills_achieved": ["Scope planning, WBS development, and change control."],
            }
        ],
        IndustrySimulation(
            subtitle="Enterprise delivery simulation",
            paragraphs=["Participants lead a transformation program and balance delivery risk."],
        ),
        IndustrySimulation(
            title="Automation Sandbox",
            subtitle="AI-powered project delivery sandbox",
            paragraphs=["Participants automate status reporting and risk logs for the program."],
        ),
    )
    assert "AI Integration" in html
    assert "Analyze scope deviation risks early." in html
    assert "Skills Achieved" in html
    assert "Industry Simulation" in html
    assert "Enterprise delivery simulation" in html
    assert "Automation Sandbox" in html
    assert html.index("Industry Simulation") < html.index("Automation Sandbox")
    assert html.index("Exercise:") < html.index("AI Integration")


def test_clear_removes_addon_from_payload():
    payload = _payload()
    clear_ai_outline_addons(payload)
    assert payload.industry_simulation is None
    assert payload.automation_sandbox is None
    assert payload.modules[0].ai_integration == []
    assert payload.modules[0].skills_achieved == []
    assert payload.modules[0].exercises


def test_refine_restores_dropped_addon_and_clears_when_absent():
    previous = _payload().model_dump_json()
    dropped = _payload()
    dropped.modules[0].ai_integration = []
    dropped.modules[0].skills_achieved = []
    dropped.industry_simulation = None
    dropped.automation_sandbox = None
    restore_ai_addons_if_dropped(previous, dropped, "shorten module 1")
    assert dropped.modules[0].ai_integration
    assert dropped.industry_simulation is not None
    assert dropped.industry_simulation.subtitle == "Enterprise delivery simulation"
    assert dropped.automation_sandbox is not None
    assert dropped.automation_sandbox.subtitle == "AI-powered project delivery sandbox"

    plain = json.dumps({"modules": [{"module_title": "Scope", "exercises": ["Exercise: plan."]}]})
    fresh = _payload()
    restore_ai_addons_if_dropped(plain, fresh, "add a topic")
    assert fresh.industry_simulation is None
    assert fresh.automation_sandbox is None
    assert fresh.modules[0].ai_integration == []
    assert not outline_text_has_ai_addons(plain)
    assert outline_text_has_ai_addons(previous)

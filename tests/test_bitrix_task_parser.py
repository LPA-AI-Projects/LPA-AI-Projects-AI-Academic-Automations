"""Unit tests for Bitrix task webhook parsing."""
from app.services.bitrix_task_parser import (
    _map_parsed_to_input_data,
    extract_message_id,
    extract_task_id,
    format_bitrix_cover_duration_hours,
    parse_refine_feedback_from_comment,
    parse_task_description_table,
    resolve_bitrix_task_request,
)


def test_extract_task_id_ontaskupdate():
    payload = {
        "event": "ONTASKUPDATE",
        "data[FIELDS_AFTER][ID]": "78776",
    }
    assert extract_task_id(payload) == "78776"


def test_extract_task_id_ontaskcommentadd():
    payload = {
        "event": "ONTASKCOMMENTADD",
        "data[FIELDS_AFTER][ID]": "0",
        "data[FIELDS_AFTER][TASK_ID]": "78776",
        "data[FIELDS_AFTER][MESSAGE_ID]": "39988698",
    }
    assert extract_task_id(payload) == "78776"


def test_extract_task_id_zero_only():
    payload = {
        "event": "ONTASKCOMMENTADD",
        "data[FIELDS_AFTER][ID]": "0",
    }
    assert extract_task_id(payload) is None


def test_extract_message_id():
    payload = {
        "data[FIELDS_AFTER][MESSAGE_ID]": "39988698",
    }
    assert extract_message_id(payload) == "39988698"


def test_parse_refine_variants():
    assert parse_refine_feedback_from_comment("Refine: change to 2 day program") == (
        "change to 2 day program"
    )
    assert parse_refine_feedback_from_comment("refine: add activities") == "add activities"
    assert parse_refine_feedback_from_comment("Refine : add roleplay") == "add roleplay"
    assert parse_refine_feedback_from_comment("refine : increase hours") == "increase hours"


def test_parse_refine_multiline():
    text = "Refine :\nchange it to 24 hours"
    assert parse_refine_feedback_from_comment(text) == "change it to 24 hours"


def test_parse_refine_multiline_add_modules():
    text = """refine: Add Module 13: BIM Coordination Meetings
Clash Report Preparation
Coordination Snapshot Views
Add Module 14: LoIN (Level of Information Need)
Digital Handover"""
    result = parse_refine_feedback_from_comment(text)
    assert result is not None
    assert "Add Module 13: BIM Coordination Meetings" in result
    assert "Add Module 14: LoIN" in result
    assert "Digital Handover" in result


def test_parse_refine_after_bitrix_user_tag():
    text = "[USER=161836]Abinand K Vinod[/USER] refine: Add Module 13"
    assert parse_refine_feedback_from_comment(text) == "Add Module 13"


def test_parse_refine_with_br_line_breaks():
    text = "refine: Add Module 13[BR]Clash Report Preparation[BR]Add Module 14"
    result = parse_refine_feedback_from_comment(text)
    assert result is not None
    assert "Clash Report Preparation" in result
    assert "Add Module 14" in result


def test_bitrix_cover_uses_hours_not_weeks_on_total_duration_row():
    desc = (
        "[table][tr][td]Product / Course Name: Blockchain[/td][/tr]"
        "[tr][td]Total Duration: 6 to 8 Weeks[/td][/tr]"
        "[tr][td]Course Duration 6 to 8 Weeks (16 Sessions, 90 Minutes Each)[/td][/tr]"
        "[tr][td]Duration in hours: 30[/td][/tr][/table]"
    )
    parsed = parse_task_description_table(desc)
    mapped = _map_parsed_to_input_data(parsed)
    assert "duration" not in mapped
    assert mapped["course_duration"] == "6 to 8 Weeks (16 Sessions, 90 Minutes Each)"
    assert mapped["per_day_duration_in_hours"] == "30"
    assert "6 to 8 Weeks" in mapped.get("additional_notes", "")
    assert format_bitrix_cover_duration_hours("30") == "30 Hours"
    assert format_bitrix_cover_duration_hours("8hr") == "8 Hours"


def test_duration_in_hours_maps_to_per_day_hours():
    desc = (
        "[table][tr][td]Product / Course Name: Excel Basics[/td][/tr]"
        "[tr][td]Duration in hours: 8[/td][/tr][/table]"
    )
    parsed = parse_task_description_table(desc)
    mapped = _map_parsed_to_input_data(parsed)
    assert mapped["course_name"] == "Excel Basics"
    assert mapped["per_day_duration_in_hours"] == "8"


def test_parse_two_column_b2c_task_table():
    """Bitrix B2C template puts labels in column 1 and values in column 2."""
    desc = (
        "[table][tr][td]For B2C[/td][td][/td][/tr]"
        "[tr][td]Product / Course Name:[/td][td] Public Relationship Officer[/td][/tr]"
        "[tr][td]Level Of Training ( beginner, intermediate or Expert ) :[/td][td] Expert[/td][/tr]"
        "[tr][td]Mode of Training ( Online/ Offline ):[/td][td] Online[/td][/tr]"
        "[tr][td]Focus Area of Training ( Theory, Practical, Role Play, Simulations, Games and activities ) :"
        "[/td][td] End to End[/td][/tr][/table]"
    )
    parsed = parse_task_description_table(desc)
    mapped = _map_parsed_to_input_data(parsed)
    assert mapped["course_name"] == "Public Relationship Officer"
    assert mapped["level_of_training"] == "Advanced"
    assert mapped["mode_of_training"] == "Online"
    # Legacy templates used Focus Area as topic scope when Suggested Topics is absent.
    assert mapped["topics_to_include"] == "End to End"


def test_map_new_bitrix_b2c_template_to_course_input():
    """Current Bitrix B2C table fields map into CourseInputData for better outlines."""
    desc = (
        "[table]\n"
        "[tr][td][b]Product / Course Name[/b]\n[/td][td]SAP HCM Payroll Process\n[/td][/tr]"
        "[tr][td][b]Department of Product[/b]\n[/td][td]HR\n[/td][/tr]"
        "[tr][td][b]Is this course meant for? ([/b]Certification / Skill Development[b])[/b]\n"
        "[/td][td]Skill Development\n[/td][/tr]"
        "[tr][td][b]Level of Training ([/b]Beginner / Intermediate[b])[/b]\n[/td][td]Intermediate\n[/td][/tr]"
        "[tr][td][b]Schedule Proposed ([/b]Weekdays / Weekends[b])[/b]\n[/td][td]Weekdays\n[/td][/tr]"
        "[tr][td][b]Mode of Training ([/b]Online / Offline / Hybrid)\n[/td][td]Online\n[/td][/tr]"
        "[tr][td][b]Duration in Hours[/b]\n[/td][td]18Hours\n[/td][/tr]"
        "[tr][td][b]Location of the Training[/b]\n[/td][td]online\n[/td][/tr]"
        "[tr][td][b]Designation of Learner/Learners[/b]\n[/td][td]HR\n[/td][/tr]"
        "[tr][td][b]Target Job Role (After Training)[/b]\n[/td][td]She has newly joined  the  company\n[/td][/tr]"
        "[tr][td][b]Professional Experience ([/b]2–5 Years[b])[/b]\n[/td][td]2-5 years\n[/td][/tr]"
        "[tr][td][b]Industry / Domain[/b]\n[/td][td]Project Management\n[/td][/tr]"
        "[tr][td][b]Current Skill Level ([/b]Basic Awareness[b])[/b]\n[/td][td]Basic Awarness\n[/td][/tr]"
        "[tr][td][b]Current Challenges / Pain Points[/b]\n"
        "[/td][td]Wanted to understand SAP HCM tool for payroll process\n[/td][/tr]"
        "[tr][td][b]Goal of Training[/b]\n[/td][td]Upskill\n[/td][/tr]"
        "[tr][td][b]Expected Outcome After Training ([/b]Participants should be able to...[b])[/b]\n"
        "[/td][td]She should understand how to use the tool\n[/td][/tr]"
        "[tr][td][b]Focus Area of Training ([/b]Theory / Practical[b])[/b]\n[/td][td]Practical\n[/td][/tr]"
        "[tr][td][b]Suggested Topics by the Client / Trainer[/b]\n[/td][td]Payroll schemas\n[/td][/tr]"
        "[tr][td][b]Referral Course Links (If Any)[/b]\n[/td][td]NA\n[/td][/tr]"
        "[/table]"
    )
    parsed = parse_task_description_table(desc)
    mapped = _map_parsed_to_input_data(parsed)

    assert mapped["course_name"] == "SAP HCM Payroll Process"
    assert mapped["department"] == "HR"
    assert mapped["designation"] == "HR"
    assert mapped["level_of_training"] == "Intermediate"
    assert mapped["mode_of_training"] == "Online"
    assert mapped["per_day_duration_in_hours"] == "18Hours"
    assert mapped["topics_to_include"] == "Payroll schemas"
    assert mapped["course_purpose"] == "Skill Development"
    assert mapped["schedule_proposed"] == "Weekdays"
    assert mapped["industry_domain"] == "Project Management"
    assert mapped["professional_experience"] == "2-5 years"
    assert mapped["current_skill_level"] == "Basic Awarness"
    assert mapped["focus_area_of_training"] == "Practical"
    assert mapped["location_of_training"] == "online"
    assert mapped["target_job_role"] == "She has newly joined the company"
    assert mapped["pain_points"] == "Wanted to understand SAP HCM tool for payroll process"
    assert mapped["expected_outcome"] == "She should understand how to use the tool"
    assert "Wanted to understand SAP HCM tool for payroll process" in mapped["need_of_training"]
    assert "She has newly joined" in mapped["need_of_training"]
    assert "Upskill" in mapped["goal_of_training"]
    assert "She should understand how to use the tool" in mapped["goal_of_training"]
    assert "Skill Development" not in (mapped.get("additional_notes") or "")
    assert "Weekdays" not in (mapped.get("additional_notes") or "")
    assert "Project Management" not in (mapped.get("additional_notes") or "")
    assert "referral_course_links" not in mapped


def test_map_bitrix_ignores_trainer_pricing_cv_fields():
    desc = (
        "[table]"
        "[tr][td][b]Preferred Trainer Experience[/b][/td][td]10-15 Years[/td][/tr]"
        "[tr][td][b]Certified Trainer Mandatory[/b][/td][td]Yes[/td][/tr]"
        "[tr][td][b]Any Specific Requirements[/b][/td][td]Female trainer preferred[/td][/tr]"
        "[tr][td][b]Proposed Pricing[/b][/td][td]5000 AED[/td][/tr]"
        "[tr][td][b]Preferred Trainer Nationality[/b][/td][td]Asian[/td][/tr]"
        "[tr][td][b]Customer CV Available[/b][/td][td]Yes[/td][/tr]"
        "[tr][td][b]Preferred Schedule (For Trainer Finalization)[/b][/td][td]Mon-Wed 6pm[/td][/tr]"
        "[/table]"
    )
    mapped = _map_parsed_to_input_data(parse_task_description_table(desc))
    assert "preferred_trainer_experience" not in mapped
    assert "certified_trainer_mandatory" not in mapped
    assert "proposed_pricing" not in mapped
    assert "preferred_trainer_nationality" not in mapped
    assert "customer_cv_available" not in mapped
    assert mapped["specific_requirements"] == "Female trainer preferred"
    assert mapped["preferred_schedule"] == "Mon-Wed 6pm"


def test_referral_course_links_two_column_plain_url():
    desc = (
        "[table][tr][td]Product / Course Name:[/td][td]Excel Basics[/td][/tr]"
        "[tr][td]Referral course links if any:[/td][td]https://example.com/reference-course[/td][/tr]"
        "[/table]"
    )
    parsed = parse_task_description_table(desc)
    mapped = _map_parsed_to_input_data(parsed)
    assert mapped["referral_course_links"] == "https://example.com/reference-course"


def test_referral_course_links_bitrix_bbcode_url_tag():
    desc = (
        "[table][tr][td]Referral course links if any:[/td][td]"
        "[url=https://learnerspoint.com/pro-course]PRO Course[/url][/td][/tr][/table]"
    )
    parsed = parse_task_description_table(desc)
    mapped = _map_parsed_to_input_data(parsed)
    assert mapped["referral_course_links"] == "https://learnerspoint.com/pro-course"


def test_referral_course_links_single_column():
    desc = (
        "[table][tr][td]Referral course links if any: "
        "https://a.example.com, https://b.example.com[/td][/tr][/table]"
    )
    parsed = parse_task_description_table(desc)
    mapped = _map_parsed_to_input_data(parsed)
    assert "https://a.example.com" in mapped["referral_course_links"]
    assert "https://b.example.com" in mapped["referral_course_links"]


def test_referral_course_links_omitted_when_empty():
    desc = "[table][tr][td]Referral course links if any:[/td][td][/td][/tr][/table]"
    parsed = parse_task_description_table(desc)
    mapped = _map_parsed_to_input_data(parsed)
    assert "referral_course_links" not in mapped


def test_resolve_bitrix_task_request_nested_tasks_task_get():
    payload = {
        "result": {
            "task": {
                "id": "79566",
                "title": "Course Outline + Trainer requirement Task",
                "description": (
                    "[table][tr][td]Product / Course Name:[/td]"
                    "[td]Public Relationship Officer[/td][/tr][/table]"
                ),
            }
        }
    }
    task_id, input_data = resolve_bitrix_task_request(payload)
    assert task_id == "79566"
    assert input_data["course_name"] == "Public Relationship Officer"
    assert input_data["bitrix_task_id"] == "79566"


def test_parse_refine_rejects_non_refine():
    for text in (
        "Tauqeer created this task",
        "started the task",
        "hello",
        "please change",
        "kindly check once",
    ):
        assert parse_refine_feedback_from_comment(text) is None

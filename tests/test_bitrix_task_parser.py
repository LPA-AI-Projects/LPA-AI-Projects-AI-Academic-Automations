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
    assert mapped["topics_to_include"] == "End to End"


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

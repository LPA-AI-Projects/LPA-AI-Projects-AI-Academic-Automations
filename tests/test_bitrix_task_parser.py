"""Unit tests for Bitrix task webhook parsing."""
from app.services.bitrix_task_parser import (
    _map_parsed_to_input_data,
    extract_message_id,
    extract_task_id,
    parse_refine_feedback_from_comment,
    parse_task_description_table,
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


def test_total_and_course_duration_mapping():
    desc = (
        "[table][tr][td]Product / Course Name: Blockchain[/td][/tr]"
        "[tr][td]Total Duration: 6 to 8 Weeks[/td][/tr]"
        "[tr][td]Course Duration 6 to 8 Weeks (16 Sessions, 90 Minutes Each)[/td][/tr]"
        "[tr][td]Duration in hours: 30[/td][/tr][/table]"
    )
    parsed = parse_task_description_table(desc)
    mapped = _map_parsed_to_input_data(parsed)
    assert mapped["duration"] == "6 to 8 Weeks"
    assert mapped["course_duration"] == "6 to 8 Weeks (16 Sessions, 90 Minutes Each)"
    assert mapped["per_day_duration_in_hours"] == "30"


def test_duration_in_hours_maps_to_per_day_hours():
    desc = (
        "[table][tr][td]Product / Course Name: Excel Basics[/td][/tr]"
        "[tr][td]Duration in hours: 8[/td][/tr][/table]"
    )
    parsed = parse_task_description_table(desc)
    mapped = _map_parsed_to_input_data(parsed)
    assert mapped["course_name"] == "Excel Basics"
    assert mapped["per_day_duration_in_hours"] == "8"


def test_parse_refine_rejects_non_refine():
    for text in (
        "Tauqeer created this task",
        "started the task",
        "hello",
        "please change",
        "kindly check once",
    ):
        assert parse_refine_feedback_from_comment(text) is None

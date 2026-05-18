"""Unit tests for Bitrix task webhook parsing."""
from app.services.bitrix_task_parser import (
    extract_message_id,
    extract_task_id,
    parse_refine_feedback_from_comment,
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


def test_parse_refine_prefix():
    assert (
        parse_refine_feedback_from_comment(
            "Refine: Make this intermediate with roleplay exercises",
            prefix="Refine:",
        )
        == "Make this intermediate with roleplay exercises"
    )


def test_parse_refine_no_prefix():
    assert parse_refine_feedback_from_comment("kindly check once", prefix="Refine:") is None

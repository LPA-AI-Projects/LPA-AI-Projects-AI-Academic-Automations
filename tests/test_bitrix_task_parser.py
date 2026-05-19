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


def test_parse_refine_rejects_non_refine():
    for text in (
        "Tauqeer created this task",
        "started the task",
        "hello",
        "please change",
        "kindly check once",
    ):
        assert parse_refine_feedback_from_comment(text) is None

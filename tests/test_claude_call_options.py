"""Anthropic call options: usage logging, prompt caching, structured output, outline timeouts."""
import asyncio
import copy
import inspect
import json as jsonlib
import logging
import os

os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost:5432/db")
os.environ.setdefault("API_SECRET_KEY", "test-secret")

import httpx
import pytest

from app.services import claude as claude_mod
from app.services.claude import (
    JSON_OUTLINE_TIMEOUT_S,
    ClaudeService,
    _outline_json_schema,
)

_OK_BODY = {
    "content": [{"type": "text", "text": "hello"}],
    "stop_reason": "end_turn",
    "usage": {
        "input_tokens": 1200,
        "output_tokens": 345,
        "cache_creation_input_tokens": 900,
        "cache_read_input_tokens": 0,
        "server_tool_use": {"web_search_requests": 2},
    },
}
_SCHEMA_400 = {
    "type": "error",
    "error": {"type": "invalid_request_error", "message": "output_config.format: schema not supported"},
}


class _FakeClient:
    calls: list = []
    queue: list = []

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, url, headers=None, json=None):
        _FakeClient.calls.append(copy.deepcopy(json))
        status, body = _FakeClient.queue.pop(0)
        return httpx.Response(status, json=body, request=httpx.Request("POST", url))


@pytest.fixture
def fake_http(monkeypatch):
    _FakeClient.calls = []
    _FakeClient.queue = []
    monkeypatch.setattr(claude_mod.httpx, "AsyncClient", _FakeClient)
    monkeypatch.setattr(claude_mod.settings, "ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setattr(claude_mod.settings, "AI_PROVIDER", "anthropic")
    monkeypatch.setattr(claude_mod.settings, "ANTHROPIC_FALLBACK_MODELS", "none")
    monkeypatch.setattr(claude_mod.settings, "OPENAI_API_KEY", "")
    return _FakeClient


def _call(**kwargs) -> str:
    ai = ClaudeService()
    return asyncio.run(
        ai._call_messages_api(
            system_prompt="SYSTEM",
            user_prompt="USER",
            timeout_s=5.0,
            max_attempts=1,
            **kwargs,
        )
    )


def _walk(node):
    yield node
    if isinstance(node, dict):
        for key, value in node.items():
            if key in ("properties", "$defs"):
                for sub in value.values():
                    yield from _walk(sub)
            else:
                yield from _walk(value)
    elif isinstance(node, list):
        for item in node:
            yield from _walk(item)


def test_outline_schema_is_strict_and_keeps_title_fields():
    schema = _outline_json_schema()
    jsonlib.dumps(schema)
    objects = [n for n in _walk(schema) if isinstance(n, dict) and "properties" in n]
    assert objects
    for obj in objects:
        assert obj["additionalProperties"] is False
        assert obj["required"] == list(obj["properties"].keys())
    for node in _walk(schema):
        if isinstance(node, dict):
            assert "default" not in node
            assert "minLength" not in node and "maximum" not in node
    # a field *named* "title" must survive keyword stripping
    assert "title" in schema["$defs"]["Objective"]["properties"]
    assert "course_title" in schema["required"] and "modules" in schema["required"]


def test_system_prompt_is_cacheable_by_default(fake_http, monkeypatch):
    monkeypatch.setattr(claude_mod.settings, "ANTHROPIC_PROMPT_CACHING", True)
    fake_http.queue = [(200, _OK_BODY)]
    assert _call() == "hello"
    system = fake_http.calls[0]["system"]
    assert system == [{"type": "text", "text": "SYSTEM", "cache_control": {"type": "ephemeral"}}]
    assert fake_http.calls[0]["tools"][0]["name"] == "web_search"  # web search stays on
    assert "output_config" not in fake_http.calls[0]


def test_caching_can_be_disabled(fake_http, monkeypatch):
    monkeypatch.setattr(claude_mod.settings, "ANTHROPIC_PROMPT_CACHING", False)
    fake_http.queue = [(200, _OK_BODY)]
    _call()
    assert fake_http.calls[0]["system"] == "SYSTEM"


def test_usage_is_logged(fake_http, caplog):
    fake_http.queue = [(200, _OK_BODY)]
    with caplog.at_level(logging.INFO):
        _call(label="outline_json")
    text = " ".join(r.getMessage() for r in caplog.records)
    assert "Anthropic usage" in text
    assert "label=outline_json" in text
    assert "input=1200" in text and "output=345" in text
    assert "cache_write=900" in text and "web_searches=2" in text


def test_max_tokens_stop_warns(fake_http, caplog):
    fake_http.queue = [(200, {**_OK_BODY, "stop_reason": "max_tokens"})]
    with caplog.at_level(logging.WARNING):
        _call(label="x")
    assert any("max_tokens" in r.getMessage() for r in caplog.records)


def test_structured_output_sent_when_schema_given(fake_http):
    schema = _outline_json_schema()
    fake_http.queue = [(200, _OK_BODY)]
    _call(json_schema=schema)
    assert fake_http.calls[0]["output_config"] == {"format": {"type": "json_schema", "schema": schema}}


def test_structured_output_rejected_falls_back_to_plain_call(fake_http):
    fake_http.queue = [(400, _SCHEMA_400), (200, _OK_BODY)]
    assert _call(json_schema=_outline_json_schema()) == "hello"
    assert len(fake_http.calls) == 2
    assert "output_config" in fake_http.calls[0]
    assert "output_config" not in fake_http.calls[1]


def test_unrelated_400_is_not_swallowed(fake_http):
    body = {"type": "error", "error": {"type": "invalid_request_error", "message": "messages: bad role"}}
    fake_http.queue = [(400, body)]
    with pytest.raises(RuntimeError):
        _call(json_schema=_outline_json_schema())
    assert len(fake_http.calls) == 1


def test_structured_output_and_caching_are_on_by_default():
    fields = claude_mod.settings.__class__.model_fields
    assert fields["ANTHROPIC_STRUCTURED_OUTPUT"].default is True
    assert fields["ANTHROPIC_PROMPT_CACHING"].default is True


def test_json_outline_methods_default_to_long_timeout():
    for name in ("build_roi_course_outline_json", "refine_course_outline_json", "normalize_to_payload"):
        default = inspect.signature(getattr(ClaudeService, name)).parameters["timeout_s"].default
        assert default == JSON_OUTLINE_TIMEOUT_S == 240.0
    # research timeout and the rest keep their own values
    assert inspect.signature(ClaudeService.build_context_profile).parameters["timeout_s"].default == 120.0


_MIN_OUTLINE = jsonlib.dumps(
    {
        "course_title": "PMP",
        "duration": "2 Days",
        "total_hours": "16 Hours",
        "program_insight": {"paragraphs": [], "bullets": []},
        "course_details": {},
    }
)


@pytest.mark.parametrize("flag", [False, True])
def test_outline_and_refine_pass_schema_only_when_enabled(monkeypatch, flag):
    monkeypatch.setattr(claude_mod.settings, "ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setattr(claude_mod.settings, "ANTHROPIC_STRUCTURED_OUTPUT", flag)
    seen = []

    async def fake_call(self, **kwargs):
        seen.append(kwargs)
        return _MIN_OUTLINE

    monkeypatch.setattr(ClaudeService, "_call_messages_api", fake_call)
    ai = ClaudeService()
    asyncio.run(ai.build_roi_course_outline_json("ctx", "objectives"))
    asyncio.run(ai.refine_course_outline_json(_MIN_OUTLINE, "add a module"))
    assert [k["label"] for k in seen] == ["outline_json", "refine_outline"]
    for kwargs in seen:
        assert kwargs["timeout_s"] == 240.0
        assert (kwargs["json_schema"] is not None) is flag

"""Cost dashboard API helpers, usage recording and API-key protection."""
import asyncio
import os
import re
import uuid
from datetime import datetime

os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost:5432/db")
os.environ.setdefault("API_SECRET_KEY", "test-secret")

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from app.api import cost_dashboard_api as api
from app.services import usage_log


def test_bitrix_job_type_matches_the_real_constant():
    src = open(os.path.join(os.path.dirname(__file__), "..", "app", "services", "course_refine.py"), encoding="utf-8").read()
    real = re.search(r'BITRIX_OUTLINE_JOB_TYPE\s*=\s*"([^"]+)"', src).group(1)
    assert api.BITRIX_OUTLINE_JOB_TYPE == real


def test_source_of():
    assert api.source_of("bitrix_outline") == "bitrix"
    assert api.source_of("courses") == "zoho"
    assert api.source_of(None) == "zoho"


def test_month_bounds():
    assert api.month_bounds("all") is None and api.month_bounds(None) is None
    assert api.month_bounds("2026-11") == (datetime(2026, 11, 1), datetime(2026, 12, 1))
    assert api.month_bounds("2026-12") == (datetime(2026, 12, 1), datetime(2027, 1, 1))
    for bad in ("2026", "oops", "2026-13", "2026-00"):
        with pytest.raises(HTTPException) as e:
            api.month_bounds(bad)
        assert e.value.status_code == 422


def test_clean_inputs_drops_empty_values_and_orders_important_fields_first():
    raw = '{"zeta": "z", "company_name": "Acme", "crm_source": "bitrix", "course_name": "PMP", "notes": "", "tags": [], "n": null, "alpha": 1}'
    out = api.clean_inputs(raw)
    assert list(out) == ["course_name", "company_name", "alpha", "zeta"]
    assert api.clean_inputs("not json") == {} and api.clean_inputs(None) == {} and api.clean_inputs("[1]") == {}
    assert api.summarize_names(raw) == ("PMP", "Acme")


def test_build_by_month_counts_by_status_and_source():
    d = datetime
    jobs = [
        ("courses", "completed", d(2026, 10, 3)),
        ("courses", "failed", d(2026, 10, 4)),
        (None, "pending", d(2026, 10, 5)),
        ("bitrix_outline", "completed", d(2026, 10, 6)),
        ("bitrix_outline", "completed", d(2026, 11, 1)),
        ("courses", "completed", None),  # no date: ignored
    ]
    refines = [("bitrix_outline", d(2026, 11, 2)), ("courses", d(2026, 10, 9)), ("courses", d(2026, 10, 10))]
    rows = api.build_by_month(jobs, refines)
    assert [r["month"] for r in rows] == ["2026-10", "2026-11"]
    oct_, nov = rows
    assert oct_["zoho"] == {"outlines": 3, "completed": 1, "failed": 1, "other": 1, "refines": 2}
    assert oct_["bitrix"] == {"outlines": 1, "completed": 1, "failed": 0, "other": 0, "refines": 0}
    assert nov["bitrix"]["refines"] == 1 and nov["zoho"]["outlines"] == 0


def test_usage_row_from_response_maps_anthropic_usage():
    jid = uuid.uuid4()
    data = {
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 300, "output_tokens": 5200, "cache_creation_input_tokens": 4700, "cache_read_input_tokens": 0, "server_tool_use": {"web_search_requests": 2}},
    }
    row = usage_log.usage_row_from_response({"kind": "outline", "job_id": str(jid), "zoho_record_id": 123}, label="outline_json", model="claude-sonnet-4-6", data=data, structured=True)
    assert row["job_id"] == jid and row["course_id"] is None and row["zoho_record_id"] == "123"
    assert (row["input_tokens"], row["output_tokens"], row["cache_write_tokens"], row["cache_read_tokens"], row["web_searches"]) == (300, 5200, 4700, 0, 2)
    assert row["structured"] is True and row["stop_reason"] == "end_turn" and row["kind"] == "outline"
    # missing / malformed usage never raises
    empty = usage_log.usage_row_from_response({"kind": "refine", "course_id": "not-a-uuid"}, label="", model="m", data={}, structured=False)
    assert empty["input_tokens"] == 0 and empty["course_id"] is None and empty["label"] == "unlabeled"


def test_recording_is_skipped_without_context_and_scheduled_with_one(monkeypatch):
    seen = []

    async def fake_insert(row):
        seen.append(row)

    monkeypatch.setattr(usage_log, "_insert", fake_insert)

    async def run():
        usage_log.record_usage_in_background(None, label="x", model="m", data={}, structured=False)
        usage_log.record_usage_in_background({"kind": "outline", "job_id": uuid.uuid4()}, label="research", model="m", data={"usage": {"input_tokens": 5}}, structured=False)
        await asyncio.gather(*list(usage_log._pending))

    asyncio.run(run())
    assert len(seen) == 1 and seen[0]["label"] == "research" and seen[0]["input_tokens"] == 5
    # outside an event loop it must not raise
    usage_log.record_usage_in_background({"kind": "outline"}, label="x", model="m", data={}, structured=False)


def test_endpoints_require_the_api_key():
    app = FastAPI()
    app.include_router(api.router)
    c = TestClient(app)
    for url in ("/api/v1/cost-dashboard/summary", "/api/v1/cost-dashboard/requests", f"/api/v1/cost-dashboard/requests/{uuid.uuid4()}"):
        assert c.get(url).status_code == 401, url
        assert c.get(url, headers={"X-API-Key": "wrong"}).status_code == 401, url

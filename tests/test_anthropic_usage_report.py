"""scripts/anthropic_usage_report.py: parsing and caching cost maths."""
import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "anthropic_usage_report", Path(__file__).resolve().parents[1] / "scripts" / "anthropic_usage_report.py"
)
rep = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rep)


def _line(label, inp, out, write, read, searches=0, stop="end_turn"):
    return (
        "2026-11-06 10:00:00 | INFO     | app.services.claude | Anthropic usage | "
        f"label={label} model=claude-sonnet-4-6 input={inp} output={out} cache_write={write} "
        f"cache_read={read} web_searches={searches} stop={stop} structured=False"
    )


def test_parses_lines_and_none_values():
    rows = rep.parse(["noise", _line("outline_json", 100, 50, 4000, 0).replace("cache_write=4000", "cache_write=None")])
    assert len(rows) == 1 and rows[0]["cache_write"] == 0 and rows[0]["label"] == "outline_json"


def test_cache_hits_save_and_isolated_writes_cost():
    # write once, then three reads of the same 4,000-token prompt -> saves money
    hit_rows = rep.parse(
        [_line("a", 200, 100, 4000, 0)] + [_line("a", 200, 100, 0, 4000) for _ in range(3)]
    )
    saved = sum(
        rep.cost(r, 3, 15, 10, cached=False) - rep.cost(r, 3, 15, 10, cached=True) for r in hit_rows
    )
    assert saved > 0
    # a lone write that is never read -> costs 25% more on those tokens
    miss = rep.parse([_line("b", 200, 100, 4000, 0)])[0]
    assert rep.cost(miss, 3, 15, 10, cached=True) > rep.cost(miss, 3, 15, 10, cached=False)


def test_report_text_and_empty_input():
    assert "No 'Anthropic usage' lines" in rep.report([], 3, 15, 10)
    rows = rep.parse([_line("a", 200, 100, 4000, 0), _line("a", 200, 100, 0, 4000, stop="max_tokens")])
    text = rep.report(rows, 3, 15, 10)
    assert "SAVING money" in text and "max_tokens" in text and "hit%" in text

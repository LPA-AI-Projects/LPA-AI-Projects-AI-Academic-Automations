"""
Summarise "Anthropic usage | ..." log lines (Railway log export or any text file) and show whether
prompt caching is paying off.

    python scripts/anthropic_usage_report.py logs.txt
    python scripts/anthropic_usage_report.py logs.txt --input-price 3 --output-price 15

Prices are USD per million tokens and are ASSUMPTIONS: check https://www.anthropic.com/pricing and
pass your model's real numbers. Cache writes bill 1.25x and cache reads 0.1x the input price (5-min cache).
"""
from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict

LINE_RE = re.compile(
    r"Anthropic usage \| label=(?P<label>\S+) model=(?P<model>\S+) input=(?P<input>\S+) "
    r"output=(?P<output>\S+) cache_write=(?P<cache_write>\S+) cache_read=(?P<cache_read>\S+) "
    r"web_searches=(?P<web_searches>\S+) stop=(?P<stop>\S+) structured=(?P<structured>\S+)"
)
FIELDS = ("input", "output", "cache_write", "cache_read", "web_searches")


def _num(value: str) -> int:
    try:
        return int(value)
    except ValueError:  # "None" when the API omitted the field
        return 0


def parse(lines) -> list[dict]:
    rows = []
    for line in lines:
        m = LINE_RE.search(line)
        if m:
            row = m.groupdict()
            for f in FIELDS:
                row[f] = _num(row[f])
            rows.append(row)
    return rows


def cost(row: dict, in_price: float, out_price: float, search_price: float, *, cached: bool) -> float:
    per_tok_in, per_tok_out = in_price / 1e6, out_price / 1e6
    if cached:
        prompt = (
            row["input"] * per_tok_in
            + row["cache_write"] * per_tok_in * 1.25
            + row["cache_read"] * per_tok_in * 0.10
        )
    else:  # what the same call would have cost with no caching at all
        prompt = (row["input"] + row["cache_write"] + row["cache_read"]) * per_tok_in
    return prompt + row["output"] * per_tok_out + row["web_searches"] * search_price / 1000


def report(rows: list[dict], in_price: float, out_price: float, search_price: float) -> str:
    if not rows:
        return "No 'Anthropic usage' lines found. Deploy the build that logs usage, run some jobs, export the logs."
    groups: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        groups[r["label"]].append(r)
    out = [
        f"{'label':24s} {'calls':>5s} {'in':>9s} {'out':>9s} {'c_write':>9s} {'c_read':>9s} "
        f"{'hit%':>5s} {'cost$':>8s} {'no-cache$':>9s} {'saved$':>8s}"
    ]
    tot = defaultdict(float)
    for label, rs in sorted(groups.items()):
        s = {f: sum(r[f] for r in rs) for f in FIELDS}
        actual = sum(cost(r, in_price, out_price, search_price, cached=True) for r in rs)
        plain = sum(cost(r, in_price, out_price, search_price, cached=False) for r in rs)
        cacheable = s["cache_write"] + s["cache_read"]
        hit = 100 * s["cache_read"] / cacheable if cacheable else 0.0
        out.append(
            f"{label:24s} {len(rs):5d} {s['input']:9d} {s['output']:9d} {s['cache_write']:9d} "
            f"{s['cache_read']:9d} {hit:5.0f} {actual:8.3f} {plain:9.3f} {plain - actual:8.3f}"
        )
        tot["actual"] += actual
        tot["plain"] += plain
    saved = tot["plain"] - tot["actual"]
    out.append("")
    out.append(f"Total with caching:    ${tot['actual']:.3f}")
    out.append(f"Same calls, no cache:  ${tot['plain']:.3f}")
    verdict = "caching is SAVING money" if saved > 0 else "caching is COSTING money (set ANTHROPIC_PROMPT_CACHING=false)"
    out.append(f"Net difference:        ${saved:+.3f}  -> {verdict}")
    truncated = sum(1 for r in rows if r["stop"] == "max_tokens")
    if truncated:
        out.append(f"WARNING: {truncated} call(s) stopped at max_tokens (output may be cut off).")
    return "\n".join(out)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("logfile")
    ap.add_argument("--input-price", type=float, default=3.0, help="USD per 1M input tokens (default 3, assumption)")
    ap.add_argument("--output-price", type=float, default=15.0, help="USD per 1M output tokens (default 15, assumption)")
    ap.add_argument("--search-price", type=float, default=10.0, help="USD per 1000 web searches (default 10, assumption)")
    args = ap.parse_args()
    with open(args.logfile, encoding="utf-8", errors="replace") as fh:
        rows = parse(fh)
    print(report(rows, args.input_price, args.output_price, args.search_price))


if __name__ == "__main__":
    sys.exit(main())

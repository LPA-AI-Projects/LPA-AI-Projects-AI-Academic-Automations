"""
Lightweight in-process metrics for the on-demand courseware assessments flow.

We intentionally avoid pulling in a Prometheus client to keep deployment
unchanged. Counters and a rolling latency histogram are exposed via simple
helpers, and a periodic snapshot is logged so cost / SLO trends are visible
in stdout-based log aggregation (Railway, CloudWatch, etc.).

If you later add a Prometheus endpoint, swap these primitives for
``prometheus_client.Counter`` / ``Histogram`` without changing call sites.
"""
from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque

from app.utils.logger import get_logger

logger = get_logger(__name__)

# Last 1024 generation latencies (ms), shared across all phases / records.
_LATENCY_SAMPLES: deque[float] = deque(maxlen=1024)
_LATENCY_LOCK = asyncio.Lock()

# {(phase, zoho_record_id): count}
_GEN_COUNTERS: dict[tuple[str, str], int] = defaultdict(int)
_ERR_COUNTERS: dict[tuple[str, str], int] = defaultdict(int)
_RATE_LIMIT_HITS: int = 0

_LAST_SNAPSHOT_TS: float = 0.0
_SNAPSHOT_INTERVAL_S: float = 300.0  # 5 min


async def record_generation(
    *, phase: str, zoho_record_id: str, elapsed_ms: float, content_hash: str | None
) -> None:
    """Record a successful generation."""
    key = (phase, zoho_record_id)
    _GEN_COUNTERS[key] = _GEN_COUNTERS.get(key, 0) + 1
    async with _LATENCY_LOCK:
        _LATENCY_SAMPLES.append(float(elapsed_ms))
    logger.info(
        "courseware_metrics: generated | phase=%s zoho_record_id=%s "
        "content_hash=%s elapsed_ms=%.1f",
        phase,
        zoho_record_id,
        (content_hash or "-")[:16],
        elapsed_ms,
    )
    await _maybe_log_snapshot()


async def record_error(*, phase: str, zoho_record_id: str, kind: str, detail: str = "") -> None:
    key = (phase, zoho_record_id)
    _ERR_COUNTERS[key] = _ERR_COUNTERS.get(key, 0) + 1
    logger.warning(
        "courseware_metrics: error | phase=%s zoho_record_id=%s kind=%s detail=%s",
        phase,
        zoho_record_id,
        kind,
        (detail or "")[:300],
    )


def record_rate_limit_hit() -> None:
    global _RATE_LIMIT_HITS
    _RATE_LIMIT_HITS += 1


def _percentile(samples: list[float], p: float) -> float:
    if not samples:
        return 0.0
    s = sorted(samples)
    k = max(0, min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1)))))
    return s[k]


async def _maybe_log_snapshot() -> None:
    global _LAST_SNAPSHOT_TS
    now = time.time()
    if now - _LAST_SNAPSHOT_TS < _SNAPSHOT_INTERVAL_S:
        return
    _LAST_SNAPSHOT_TS = now
    async with _LATENCY_LOCK:
        samples = list(_LATENCY_SAMPLES)
    p50 = _percentile(samples, 50)
    p95 = _percentile(samples, 95)
    total_gen = sum(_GEN_COUNTERS.values())
    total_err = sum(_ERR_COUNTERS.values())
    logger.info(
        "courseware_metrics: snapshot | generations_total=%s errors_total=%s "
        "rate_limit_hits=%s latency_ms_p50=%.1f latency_ms_p95=%.1f distinct_records=%s",
        total_gen,
        total_err,
        _RATE_LIMIT_HITS,
        p50,
        p95,
        len({rid for _, rid in _GEN_COUNTERS}),
    )


def snapshot() -> dict[str, object]:
    """Return a dict snapshot suitable for /metrics or /healthz exposure."""
    samples = list(_LATENCY_SAMPLES)
    return {
        "generations_total": sum(_GEN_COUNTERS.values()),
        "errors_total": sum(_ERR_COUNTERS.values()),
        "rate_limit_hits_total": _RATE_LIMIT_HITS,
        "latency_ms_p50": _percentile(samples, 50),
        "latency_ms_p95": _percentile(samples, 95),
        "distinct_records": len({rid for _, rid in _GEN_COUNTERS}),
        "by_record": [
            {"phase": phase, "zoho_record_id": rid, "count": count}
            for (phase, rid), count in _GEN_COUNTERS.items()
        ],
    }

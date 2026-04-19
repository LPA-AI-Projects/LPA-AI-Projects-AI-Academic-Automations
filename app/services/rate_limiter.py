"""
Lightweight in-process rate limiter (token bucket) for public endpoints.

This is intentionally dependency-free and per-process. For multi-replica
deployments back this with Redis (or your API gateway / Cloudflare rules) —
the same `check_rate_limit` interface can be reused.

Used to protect on-demand assessment generation, where every miss costs a real
LLM call. Limits are checked per ``(scope, key)``, e.g. per IP and per
``zoho_record_id``.
"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field

from app.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class _Bucket:
    timestamps: deque[float] = field(default_factory=deque)


class InMemoryRateLimiter:
    def __init__(self, max_requests: int, window_seconds: float = 60.0) -> None:
        self.max_requests = max(1, int(max_requests))
        self.window_seconds = float(window_seconds)
        self._buckets: dict[str, _Bucket] = {}
        self._lock = asyncio.Lock()

    async def check(self, key: str) -> tuple[bool, int, float]:
        """
        Returns ``(allowed, remaining, retry_after_seconds)``.

        ``retry_after_seconds`` is 0 when allowed.
        """
        if not key:
            return True, self.max_requests, 0.0
        now = time.monotonic()
        async with self._lock:
            bucket = self._buckets.setdefault(key, _Bucket())
            # Drop expired entries.
            window_start = now - self.window_seconds
            while bucket.timestamps and bucket.timestamps[0] < window_start:
                bucket.timestamps.popleft()
            if len(bucket.timestamps) >= self.max_requests:
                retry_after = self.window_seconds - (now - bucket.timestamps[0])
                return False, 0, max(0.0, retry_after)
            bucket.timestamps.append(now)
            remaining = self.max_requests - len(bucket.timestamps)
            return True, remaining, 0.0

    def gc(self) -> None:
        """Drop empty buckets to keep memory bounded over long uptimes."""
        now = time.monotonic()
        window_start = now - self.window_seconds
        for key, bucket in list(self._buckets.items()):
            while bucket.timestamps and bucket.timestamps[0] < window_start:
                bucket.timestamps.popleft()
            if not bucket.timestamps:
                self._buckets.pop(key, None)

"""Per-tenant, per-table buffer with time- and size-based flush logic.

The Persistence service uses TenantBuffer to accumulate rows before flushing
them to ClickHouse and the filesystem, reducing write amplification.
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class BufferKey:
    tenant_id: str
    table: str


class TenantBuffer:
    """Buffers rows grouped by (tenant_id, table) and decides when to flush.

    Flush triggers:
    - Size threshold: buffer length >= flush_threshold
    - Time threshold: time since last flush >= flush_interval (seconds)
    """

    def __init__(self, flush_interval: int, flush_threshold: int) -> None:
        self.flush_interval = flush_interval
        self.flush_threshold = flush_threshold
        self._buffers: dict[BufferKey, list[dict[str, Any]]] = defaultdict(list)
        self._last_flush: dict[BufferKey, float] = {}

    def append(self, key: BufferKey, row: dict[str, Any]) -> None:
        """Add a row to the buffer. Initializes the flush timer on first append."""
        if key not in self._last_flush:
            self._last_flush[key] = time.monotonic()
        self._buffers[key].append(row)

    def should_flush(self, key: BufferKey) -> bool:
        """Return True if size >= threshold or elapsed time >= interval."""
        buf = self._buffers.get(key, [])
        if not buf:
            return False
        if len(buf) >= self.flush_threshold:
            return True
        last = self._last_flush.get(key, time.monotonic())
        return (time.monotonic() - last) >= self.flush_interval

    def drain(self, key: BufferKey) -> list[dict[str, Any]]:
        """Return and clear the buffer for *key*. Resets the flush timer."""
        rows = list(self._buffers.get(key, []))
        self._buffers[key] = []
        self._last_flush[key] = time.monotonic()
        return rows

    def all_keys(self) -> list[BufferKey]:
        """Return all keys that have at least one buffered row."""
        return [k for k, v in self._buffers.items() if v]

    def drain_all(self) -> dict[BufferKey, list[dict[str, Any]]]:
        """Drain every non-empty buffer (used on shutdown)."""
        result: dict[BufferKey, list[dict[str, Any]]] = {}
        for key in list(self._buffers.keys()):
            rows = self.drain(key)
            if rows:
                result[key] = rows
        return result

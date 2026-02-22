"""Observable wrapper around DatabaseInterface that adds timing metrics.

Records histogram metrics for every database call with the caller's source
location (file:line) as a tag, enabling p50/p95/p99 latency breakdown by
call site.
"""

from __future__ import annotations

import inspect
import time
from pathlib import PurePosixPath
from typing import Any

from de_platform.services.database.interface import DatabaseInterface
from de_platform.services.metrics.interface import MetricsInterface

_METRIC = "db_query_duration_seconds"
_DB_PACKAGE = "de_platform/services/database"


def _caller_tag() -> str:
    """Walk the stack to find the first frame outside the database package."""
    for frame_info in inspect.stack(0):
        filename = frame_info.filename
        if _DB_PACKAGE not in filename and "observable_database" not in filename:
            short = str(PurePosixPath(filename))
            # Strip to project-relative path if possible
            idx = short.find("de_platform/")
            if idx != -1:
                short = short[idx:]
            else:
                idx = short.find("tests/")
                if idx != -1:
                    short = short[idx:]
            return f"{short}:{frame_info.lineno}"
    return "unknown"


class ObservableDatabase(DatabaseInterface):
    """Transparent wrapper that records timing histograms for every DB call."""

    def __init__(self, inner: DatabaseInterface, metrics: MetricsInterface) -> None:
        self._inner = inner
        self._metrics = metrics

    # -- Delegation helpers ---------------------------------------------------

    def _record(self, operation: str, elapsed: float, caller: str) -> None:
        self._metrics.histogram(
            _METRIC,
            elapsed,
            tags={"operation": operation, "caller": caller},
        )

    # -- Sync API -------------------------------------------------------------

    def connect(self) -> None:
        t0 = time.perf_counter()
        self._inner.connect()
        self._record("connect", time.perf_counter() - t0, _caller_tag())

    def disconnect(self) -> None:
        t0 = time.perf_counter()
        self._inner.disconnect()
        self._record("disconnect", time.perf_counter() - t0, _caller_tag())

    def is_connected(self) -> bool:
        return self._inner.is_connected()

    def execute(self, query: str, params: list[Any] | None = None) -> int:
        caller = _caller_tag()
        t0 = time.perf_counter()
        result = self._inner.execute(query, params)
        self._record("execute", time.perf_counter() - t0, caller)
        return result

    def fetch_one(self, query: str, params: list[Any] | None = None) -> dict[str, Any] | None:
        caller = _caller_tag()
        t0 = time.perf_counter()
        result = self._inner.fetch_one(query, params)
        self._record("fetch_one", time.perf_counter() - t0, caller)
        return result

    def fetch_all(self, query: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        caller = _caller_tag()
        t0 = time.perf_counter()
        result = self._inner.fetch_all(query, params)
        self._record("fetch_all", time.perf_counter() - t0, caller)
        return result

    def insert_one(self, table: str, row: dict[str, Any]) -> int:
        caller = _caller_tag()
        t0 = time.perf_counter()
        result = self._inner.insert_one(table, row)
        self._record("insert_one", time.perf_counter() - t0, caller)
        return result

    def bulk_insert(self, table: str, rows: list[dict[str, Any]]) -> int:
        caller = _caller_tag()
        t0 = time.perf_counter()
        result = self._inner.bulk_insert(table, rows)
        self._record("bulk_insert", time.perf_counter() - t0, caller)
        return result

    def health_check(self) -> bool:
        return self._inner.health_check()

    # -- Async API ------------------------------------------------------------

    async def connect_async(self) -> None:
        t0 = time.perf_counter()
        await self._inner.connect_async()
        self._record("connect", time.perf_counter() - t0, _caller_tag())

    async def disconnect_async(self) -> None:
        t0 = time.perf_counter()
        await self._inner.disconnect_async()
        self._record("disconnect", time.perf_counter() - t0, _caller_tag())

    async def execute_async(self, query: str, params: list[Any] | None = None) -> int:
        caller = _caller_tag()
        t0 = time.perf_counter()
        result = await self._inner.execute_async(query, params)
        self._record("execute", time.perf_counter() - t0, caller)
        return result

    async def fetch_one_async(
        self, query: str, params: list[Any] | None = None
    ) -> dict[str, Any] | None:
        caller = _caller_tag()
        t0 = time.perf_counter()
        result = await self._inner.fetch_one_async(query, params)
        self._record("fetch_one", time.perf_counter() - t0, caller)
        return result

    async def fetch_all_async(
        self, query: str, params: list[Any] | None = None
    ) -> list[dict[str, Any]]:
        caller = _caller_tag()
        t0 = time.perf_counter()
        result = await self._inner.fetch_all_async(query, params)
        self._record("fetch_all", time.perf_counter() - t0, caller)
        return result

    async def insert_one_async(self, table: str, row: dict[str, Any]) -> int:
        caller = _caller_tag()
        t0 = time.perf_counter()
        result = await self._inner.insert_one_async(table, row)
        self._record("insert_one", time.perf_counter() - t0, caller)
        return result

    async def bulk_insert_async(self, table: str, rows: list[dict[str, Any]]) -> int:
        caller = _caller_tag()
        t0 = time.perf_counter()
        result = await self._inner.bulk_insert_async(table, rows)
        self._record("bulk_insert", time.perf_counter() - t0, caller)
        return result

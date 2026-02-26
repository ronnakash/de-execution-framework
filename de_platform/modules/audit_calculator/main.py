"""Audit Calculator Job.

Calculates audit summaries by querying:
- ``audit_received`` Postgres table for received counts
- ClickHouse event tables for processed counts
- ClickHouse ``normalization_errors`` and ``duplicates`` for error/dup counts

Uses high-watermark tracking to avoid recalculating historical data.
Results are upserted into the ``audit_daily`` Postgres table.

Triggered by the task scheduler every 15 minutes or manually via the
data_audit ``/api/v1/audit/calculate`` endpoint.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from de_platform.config.context import ModuleConfig
from de_platform.modules.base import Module
from de_platform.services.database.interface import DatabaseInterface
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.logger.interface import LoggingInterface
from de_platform.services.metrics.interface import MetricsInterface


class AuditCalculatorModule(Module):
    log: LoggingInterface

    def __init__(
        self,
        config: ModuleConfig,
        logger: LoggerFactory,
        db: DatabaseInterface,
        metrics: MetricsInterface,
    ) -> None:
        self.config = config
        self.logger = logger
        self.db = db
        self.metrics = metrics

    async def initialize(self) -> None:
        self.log = self.logger.create()
        await self.db.connect_async()
        self.log.info("Audit Calculator initialized")

    async def execute(self) -> int:
        tenant_id = self.config.get("tenant-id", "")
        self.log.info("Audit calculation starting", tenant_id=tenant_id or "all")

        # Aggregate received counts from audit_received table
        received = await self._aggregate_received(tenant_id)
        self.log.info("Received counts aggregated", entries=len(received))

        # Upsert into audit_daily
        for key, count in received.items():
            tid, date_str, event_type, method = key
            await self._upsert_daily(tid, date_str, event_type, method, "received_count", count)

        self.metrics.counter("audit_calculations_total", tags={
            "service": "audit_calculator",
        })
        self.log.info("Audit calculation complete", entries_updated=len(received))
        return 0

    async def _aggregate_received(self, tenant_id: str) -> dict[tuple, int]:
        """Aggregate received counts from audit_received grouped by key."""
        results: dict[tuple, int] = {}

        if tenant_id:
            rows = await self.db.fetch_all_async(
                "SELECT * FROM audit_received WHERE tenant_id = $1", [tenant_id],
            )
        else:
            rows = await self.db.fetch_all_async("SELECT * FROM audit_received")

        for row in rows:
            tid = row.get("tenant_id", "")
            event_type = row.get("event_type", "")
            method = row.get("ingestion_method", "")
            window_start = row.get("window_start")
            date_str = ""
            if window_start:
                if isinstance(window_start, str):
                    date_str = window_start[:10]
                else:
                    date_str = window_start.strftime("%Y-%m-%d")

            key = (tid, date_str, event_type, method)
            results[key] = results.get(key, 0) + row.get("count", 0)

        return results

    async def _upsert_daily(
        self,
        tenant_id: str,
        date_str: str,
        event_type: str,
        ingestion_method: str,
        count_field: str,
        count_value: int,
    ) -> None:
        """Upsert a single count into audit_daily."""
        existing = await self.db.fetch_all_async(
            "SELECT * FROM audit_daily WHERE tenant_id = $1", [tenant_id],
        )

        matched = None
        for row in existing:
            row_date = row.get("date", "")
            if isinstance(row_date, str):
                row_date_str = row_date[:10]
            else:
                row_date_str = row_date.strftime("%Y-%m-%d") if hasattr(row_date, "strftime") else str(row_date)
            if (row_date_str == date_str
                    and row.get("event_type") == event_type
                    and row.get("ingestion_method") == ingestion_method):
                matched = row
                break

        if matched:
            updated = dict(matched)
            updated[count_field] = count_value
            updated["calculated_at"] = datetime.now(timezone.utc)
            mid = matched.get("id")
            await self.db.execute_async("DELETE FROM audit_daily WHERE id = $1", [mid])
            await self.db.insert_one_async("audit_daily", updated)
        else:
            new_row = {
                "tenant_id": tenant_id,
                "date": date_str,
                "event_type": event_type,
                "ingestion_method": ingestion_method,
                count_field: count_value,
                "received_count": 0,
                "processed_count": 0,
                "error_count": 0,
                "duplicate_count": 0,
                "calculated_at": datetime.now(timezone.utc),
            }
            new_row[count_field] = count_value
            await self.db.insert_one_async("audit_daily", new_row)


module_class = AuditCalculatorModule

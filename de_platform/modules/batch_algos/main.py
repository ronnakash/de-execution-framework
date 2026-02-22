"""Batch Fraud Detection Algos.

Job module that queries ClickHouse for historical events in a date range,
runs the SlidingWindowEngine across the entire dataset, deduplicates the
resulting alerts, and publishes them to the alerts Kafka topic.  The
alert_manager service handles persistence and case aggregation.

Usage::

    python -m de_platform run batch_algos \\
        --db events=clickhouse --cache redis --mq kafka \\
        --tenant-id acme --start-date 2026-01-01 --end-date 2026-01-31
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from de_platform.config.context import ModuleConfig
from de_platform.modules.base import Module
from de_platform.pipeline.algorithms import (
    Alert,
    LargeNotionalAlgo,
    SuspiciousCounterpartyAlgo,
    VelocityAlgo,
)
from de_platform.pipeline.topics import ALERTS
from de_platform.pipeline.window_engine import SlidingWindowEngine, WindowConfig
from de_platform.services.cache.interface import CacheInterface
from de_platform.services.database.factory import DatabaseFactory
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.logger.interface import LoggingInterface
from de_platform.services.message_queue.interface import MessageQueueInterface
from de_platform.services.metrics.interface import MetricsInterface


class BatchAlgosModule(Module):
    log: LoggingInterface

    def __init__(
        self,
        config: ModuleConfig,
        logger: LoggerFactory,
        db_factory: DatabaseFactory,
        cache: CacheInterface,
        mq: MessageQueueInterface,
        metrics: MetricsInterface,
    ) -> None:
        self.config = config
        self.logger = logger
        self.db_factory = db_factory
        self.cache = cache
        self.mq = mq
        self.metrics = metrics

    async def initialize(self) -> None:
        self.log = self.logger.create()
        self.events_db = self.db_factory.get("events")
        await self.events_db.connect_async()

        suspicious_ids_str = self.config.get("suspicious-counterparty-ids", "")
        suspicious_ids = {x.strip() for x in suspicious_ids_str.split(",") if x.strip()}

        self.algorithms = [
            LargeNotionalAlgo(),
            VelocityAlgo(),
            SuspiciousCounterpartyAlgo(suspicious_ids=suspicious_ids),
        ]

        from de_platform.pipeline.client_config_cache import ClientConfigCache

        self.config_cache = ClientConfigCache(self.cache)

        self.engine = SlidingWindowEngine(
            algorithms=self.algorithms,
            get_window_config=self._get_window_config,
            get_algo_config=self._get_algo_config,
        )

        self.log.info("Batch algos initialized")

    def _get_window_config(self, tenant_id: str) -> WindowConfig:
        config = self.config_cache._get_client(tenant_id)
        if config:
            return WindowConfig(
                window_size_minutes=config.get("window_size_minutes", 5),
                window_slide_minutes=config.get("window_slide_minutes", 1),
            )
        return WindowConfig(window_size_minutes=5, window_slide_minutes=1)

    def _get_algo_config(self, tenant_id: str, algo_name: str) -> tuple[bool, dict]:
        enabled = self.config_cache.is_algo_enabled(tenant_id, algo_name)
        thresholds = self.config_cache.get_algo_thresholds(tenant_id, algo_name)
        return enabled, thresholds

    async def execute(self) -> int:
        tenant_id = self.config.get("tenant-id", "")
        start_date = self.config.get("start-date", "")
        end_date = self.config.get("end-date", "")

        if not tenant_id or not start_date or not end_date:
            self.log.error("Missing required args: tenant-id, start-date, end-date")
            return 1

        start_dt = datetime.fromisoformat(f"{start_date}T00:00:00+00:00")
        end_dt = datetime.fromisoformat(f"{end_date}T23:59:59+00:00")

        self.log.info("Batch algos starting", tenant_id=tenant_id,
                      start_date=start_date, end_date=end_date)

        events = await self._load_events(tenant_id, start_date, end_date)
        self.log.info("Events loaded", tenant_id=tenant_id, count=len(events))

        if not events:
            self.log.info("No events in range, exiting")
            return 0

        events.sort(key=lambda e: e.get("transact_time", ""))

        alerts = self.engine.run_batch(events, tenant_id, start_dt, end_dt)
        self.log.info("Window evaluation complete", alert_count=len(alerts))

        alerts = self._dedup_in_run(alerts)
        self.log.info("After in-run dedup", alert_count=len(alerts))

        for alert in alerts:
            alert_dict = alert.to_dict()
            msg_key = f"{tenant_id}:{alert.event_id}" if tenant_id else None
            self.mq.publish(ALERTS, alert_dict, key=msg_key)

        self.metrics.counter("events_processed_total", tags={
            "service": "batch_algos", "tenant_id": tenant_id,
        })
        self.log.info("Batch algos complete", tenant_id=tenant_id,
                      alerts_published=len(alerts))
        return 0

    async def _load_events(
        self, tenant_id: str, start: str, end: str,
    ) -> list[dict[str, Any]]:
        all_events: list[dict[str, Any]] = []
        for table in ["orders", "executions", "transactions"]:
            rows = await self.events_db.fetch_all_async(
                f"SELECT * FROM {table} WHERE tenant_id = $1 "
                f"AND transact_time >= $2 AND transact_time <= $3",
                [tenant_id, f"{start}T00:00:00", f"{end}T23:59:59"],
            )
            all_events.extend(rows)
        return all_events

    @staticmethod
    def _dedup_in_run(alerts: list[Alert]) -> list[Alert]:
        seen: set[tuple[str, str]] = set()
        unique: list[Alert] = []
        for alert in alerts:
            key = (alert.algorithm, alert.event_id)
            if key not in seen:
                seen.add(key)
                unique.append(alert)
        return unique


module_class = BatchAlgosModule

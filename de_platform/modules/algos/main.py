"""Fraud Detection Algos Service.

Reads enriched events from algos topics, runs pluggable fraud detection
algorithms, publishes Alert dicts to the alerts Kafka topic, and persists them
to the alerts PostgreSQL table.

Topics consumed:
    trades_algos       — enriched orders and executions
    transactions_algos — enriched transactions

Topics produced:
    alerts             — Alert dicts for the Data API
"""

from __future__ import annotations

import asyncio
from typing import Any

from de_platform.config.context import ModuleConfig
from de_platform.modules.base import Module
from de_platform.pipeline.algorithms import (
    FraudAlgorithm,
    LargeNotionalAlgo,
    SuspiciousCounterpartyAlgo,
    VelocityAlgo,
)
from de_platform.pipeline.topics import ALERTS, TRADES_ALGOS, TRANSACTIONS_ALGOS
from de_platform.services.cache.interface import CacheInterface
from de_platform.services.database.interface import DatabaseInterface
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.logger.interface import LoggingInterface
from de_platform.services.message_queue.interface import MessageQueueInterface
from de_platform.services.metrics.interface import MetricsInterface


class AlgosModule(Module):
    log: LoggingInterface

    def __init__(
        self,
        config: ModuleConfig,
        logger: LoggerFactory,
        mq: MessageQueueInterface,
        db: DatabaseInterface,
        cache: CacheInterface,
        lifecycle: LifecycleManager,
        metrics: MetricsInterface,
    ) -> None:
        self.config = config
        self.logger = logger
        self.mq = mq
        self.db = db
        self.cache = cache
        self.lifecycle = lifecycle
        self.metrics = metrics
        self.algorithms: list[FraudAlgorithm] = []

    async def initialize(self) -> None:
        self.log = self.logger.create()
        await self.db.connect_async()
        suspicious_ids_str: str = self.config.get("suspicious-counterparty-ids", "")
        suspicious_ids: set[str] = (
            {x.strip() for x in suspicious_ids_str.split(",") if x.strip()}
        )
        self.algorithms = [
            LargeNotionalAlgo(threshold_usd=1_000_000),
            VelocityAlgo(cache=self.cache),
            SuspiciousCounterpartyAlgo(suspicious_ids=suspicious_ids),
        ]
        self.lifecycle.on_shutdown(self.db.disconnect_async)
        self.log.info(
            "Algos initialized",
            algorithms=[a.name() for a in self.algorithms],
        )

    async def execute(self) -> int:
        self.log.info("Algos running", module="algos")
        while not self.lifecycle.is_shutting_down:
            try:
                for topic in [TRADES_ALGOS, TRANSACTIONS_ALGOS]:
                    msg = self.mq.consume_one(topic)
                    if msg:
                        await self._evaluate(msg)
            except Exception as exc:
                self.log.error("Processing error", module="algos", error=str(exc))
            await asyncio.sleep(0.01)
        return 0

    async def _evaluate(self, event: dict[str, Any]) -> None:
        tenant_id = event.get("tenant_id", "")
        event_id = event.get("id", "")
        event_type = event.get("event_type", "")
        self.metrics.counter("events_received_total", tags={"service": "algos", "tenant_id": tenant_id})
        self.log.debug(
            "Evaluating event",
            tenant_id=tenant_id,
            event_id=event_id,
            event_type=event_type,
        )
        for algo in self.algorithms:
            alert = algo.evaluate(event)
            if alert:
                alert_dict = alert.to_dict()
                msg_key = f"{tenant_id}:{event_id}" if tenant_id else None
                self.mq.publish(ALERTS, alert_dict, key=msg_key)
                # Postgres TIMESTAMP needs datetime, not ISO string
                db_row = dict(alert_dict)
                if isinstance(db_row.get("created_at"), str):
                    from datetime import datetime
                    dt = datetime.fromisoformat(db_row["created_at"])
                    db_row["created_at"] = dt.replace(tzinfo=None)
                await self.db.insert_one_async("alerts", db_row)
                self.metrics.counter("alerts_generated_total", tags={
                    "algorithm": algo.name(), "severity": alert.severity, "tenant_id": tenant_id,
                })
                self.log.info(
                    "Alert generated",
                    algorithm=algo.name(),
                    tenant_id=tenant_id,
                    event_id=event_id,
                    event_type=event_type,
                    severity=alert.severity,
                )


module_class = AlgosModule

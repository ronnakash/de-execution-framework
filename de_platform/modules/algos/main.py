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
from dataclasses import asdict
from typing import Any

from de_platform.config.context import ModuleConfig
from de_platform.modules.base import AsyncModule
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


class AlgosModule(AsyncModule):
    log: LoggingInterface

    def __init__(
        self,
        config: ModuleConfig,
        logger: LoggerFactory,
        mq: MessageQueueInterface,
        db: DatabaseInterface,
        cache: CacheInterface,
        lifecycle: LifecycleManager,
    ) -> None:
        self.config = config
        self.logger = logger
        self.mq = mq
        self.db = db
        self.cache = cache
        self.lifecycle = lifecycle
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
        self.log.info("Algos running")
        while not self.lifecycle.is_shutting_down:
            for topic in [TRADES_ALGOS, TRANSACTIONS_ALGOS]:
                msg = self.mq.consume_one(topic)
                if msg:
                    self._evaluate(msg)
            await asyncio.sleep(0.01)
        return 0

    def _evaluate(self, event: dict[str, Any]) -> None:
        for algo in self.algorithms:
            alert = algo.evaluate(event)
            if alert:
                alert_dict = asdict(alert)
                self.mq.publish(ALERTS, alert_dict)
                self.db.bulk_insert("alerts", [alert_dict])
                self.log.info(
                    "Alert generated",
                    algorithm=algo.name(),
                    tenant_id=event.get("tenant_id", ""),
                    event_id=event.get("id", ""),
                    severity=alert.severity,
                )


module_class = AlgosModule

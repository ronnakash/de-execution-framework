"""Fraud Detection Algos Service.

Reads enriched events from algos topics, runs pluggable fraud detection
algorithms via the SlidingWindowEngine, and publishes Alert dicts to the
alerts Kafka topic.  The alert_manager service handles persistence and
case aggregation.

Topics consumed:
    trades_algos       — enriched orders and executions
    transactions_algos — enriched transactions

Topics produced:
    alerts             — Alert dicts for the Alert Manager
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
from de_platform.pipeline.window_engine import SlidingWindowEngine, WindowConfig
from de_platform.services.cache.interface import CacheInterface
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
        cache: CacheInterface,
        lifecycle: LifecycleManager,
        metrics: MetricsInterface,
    ) -> None:
        self.config = config
        self.logger = logger
        self.mq = mq
        self.cache = cache
        self.lifecycle = lifecycle
        self.metrics = metrics
        self.algorithms: list[FraudAlgorithm] = []

    async def initialize(self) -> None:
        self.log = self.logger.create()
        suspicious_ids_str: str = self.config.get("suspicious-counterparty-ids", "")
        suspicious_ids: set[str] = (
            {x.strip() for x in suspicious_ids_str.split(",") if x.strip()}
        )
        self.algorithms = [
            LargeNotionalAlgo(threshold_usd=1_000_000),
            VelocityAlgo(),
            SuspiciousCounterpartyAlgo(suspicious_ids=suspicious_ids),
        ]

        from de_platform.pipeline.client_config_cache import ClientConfigCache

        self.config_cache = ClientConfigCache(self.cache)
        self.config_cache.start()
        self.lifecycle.on_shutdown(lambda: self.config_cache.stop())

        self.engine = SlidingWindowEngine(
            algorithms=self.algorithms,
            get_window_config=self._get_window_config,
            get_algo_config=self._get_algo_config,
        )

        self.log.info(
            "Algos initialized",
            algorithms=[a.name() for a in self.algorithms],
        )

    def _get_window_config(self, tenant_id: str) -> WindowConfig:
        config = self.config_cache._get_client(tenant_id)
        if config:
            return WindowConfig(
                window_size_minutes=config.get("window_size_minutes", 0),
                window_slide_minutes=config.get("window_slide_minutes", 0),
            )
        # Default: immediate evaluation (window_size=0)
        return WindowConfig(window_size_minutes=0, window_slide_minutes=0)

    def _get_algo_config(self, tenant_id: str, algo_name: str) -> tuple[bool, dict]:
        enabled = self.config_cache.is_algo_enabled(tenant_id, algo_name)
        thresholds = self.config_cache.get_algo_thresholds(tenant_id, algo_name)
        return enabled, thresholds

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

        alerts = self.engine.ingest(event)

        for alert in alerts:
            alert_dict = alert.to_dict()
            msg_key = f"{tenant_id}:{event_id}" if tenant_id else None
            self.mq.publish(ALERTS, alert_dict, key=msg_key)
            self.metrics.counter("alerts_generated_total", tags={
                "algorithm": alert.algorithm, "severity": alert.severity, "tenant_id": tenant_id,
            })
            self.log.info(
                "Alert published",
                algorithm=alert.algorithm,
                tenant_id=tenant_id,
                event_id=event_id,
                event_type=event_type,
                severity=alert.severity,
            )


module_class = AlgosModule

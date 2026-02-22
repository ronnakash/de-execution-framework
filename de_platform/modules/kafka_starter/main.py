"""Kafka Starter module.

Reads client events from inbound Kafka topics, validates them, and forwards
valid events to the appropriate normalization topic. Invalid events are
published to both the internal ``normalization_errors`` topic and the
client-facing ``client_errors`` topic.

Topics consumed (configurable via module args):
    client_orders
    client_executions
    client_transactions

Topics produced:
    trade_normalization   — valid orders + executions
    tx_normalization      — valid transactions
    normalization_errors  — validation errors (internal)
    client_errors         — validation errors (client-facing)
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

from de_platform.config.context import ModuleConfig
from de_platform.modules.base import Module
from de_platform.pipeline.serialization import _now_iso, error_to_dict
from de_platform.pipeline.topics import (
    NORMALIZATION_ERRORS,
    TRADE_NORMALIZATION,
    TX_NORMALIZATION,
)
from de_platform.pipeline.validation import group_errors_by_event, validate_events
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.logger.interface import LoggingInterface
from de_platform.services.message_queue.interface import MessageQueueInterface
from de_platform.services.metrics.interface import MetricsInterface


class KafkaStarterModule(Module):
    log: LoggingInterface

    def __init__(
        self,
        config: ModuleConfig,
        logger: LoggerFactory,
        mq: MessageQueueInterface,
        lifecycle: LifecycleManager,
        metrics: MetricsInterface,
    ) -> None:
        self.config = config
        self.logger = logger
        self.mq = mq
        self.lifecycle = lifecycle
        self.metrics = metrics

    async def initialize(self) -> None:
        self.log = self.logger.create()
        orders_topic = self.config.get("client-orders-topic", "client_orders")
        executions_topic = self.config.get("client-executions-topic", "client_executions")
        transactions_topic = self.config.get("client-transactions-topic", "client_transactions")
        self.client_errors_topic: str = self.config.get("client-errors-topic", "client_errors")

        # inbound_topic -> (event_type, normalization_topic)
        self._routes: dict[str, tuple[str, str]] = {
            orders_topic: ("order", TRADE_NORMALIZATION),
            executions_topic: ("execution", TRADE_NORMALIZATION),
            transactions_topic: ("transaction", TX_NORMALIZATION),
        }
        self.log.info(
            "Kafka Starter initialized",
            module="kafka_starter",
            topics=list(self._routes.keys()),
        )

    async def execute(self) -> int:
        self.log.info("Kafka Starter running", topics=list(self._routes.keys()))
        while True:
            try:
                for inbound_topic, (event_type, norm_topic) in self._routes.items():
                    msg = self.mq.consume_one(inbound_topic)
                    if msg is not None:
                        self._process_message(event_type, norm_topic, msg)
            except Exception as exc:
                self.log.error("Processing error", module="kafka_starter", error=str(exc))
            if self.lifecycle.is_shutting_down:
                break
            await asyncio.sleep(0.01)
        return 0

    def _process_message(
        self,
        event_type: str,
        norm_topic: str,
        raw: Any,
    ) -> None:
        """Validate and route a single raw message dict."""
        events: list[Any] = [raw] if isinstance(raw, dict) else [raw]
        valid, errors = validate_events(event_type, events)

        for validated in valid:
            msg = dict(validated)
            msg["message_id"] = uuid.uuid4().hex
            msg["ingested_at"] = _now_iso()
            msg["event_type"] = event_type
            tenant_id = msg.get("tenant_id", "")
            symbol = msg.get("symbol", "")
            msg_key = f"{tenant_id}:{symbol}" if tenant_id else None
            self.mq.publish(norm_topic, msg, key=msg_key)

        for event_index, event_errors in group_errors_by_event(errors).items():
            raw_event = events[event_index] if event_index < len(events) else {}
            err_msg = error_to_dict(raw_event, event_type, event_errors)
            tenant_id = raw_event.get("tenant_id", "")
            msg_key = f"{tenant_id}:" if tenant_id else None
            self.mq.publish(NORMALIZATION_ERRORS, err_msg, key=msg_key)
            self.mq.publish(self.client_errors_topic, err_msg, key=msg_key)

        if valid:
            self.metrics.counter("events_ingested_total", value=float(len(valid)), tags={"service": "kafka_starter", "event_type": event_type, "method": "kafka"})
        if errors:
            self.metrics.counter("events_errors_total", value=float(len(errors)), tags={"service": "kafka_starter", "event_type": event_type})
        if valid or errors:
            self.log.info(
                "Processed message",
                event_type=event_type,
                topic=norm_topic,
                accepted=len(valid),
                rejected=len(errors),
            )


module_class = KafkaStarterModule

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
from de_platform.pipeline.audit_accumulator import AuditAccumulator
from de_platform.pipeline.serialization import _now_iso, error_to_dict
from de_platform.pipeline.topics import (
    NORMALIZATION_ERRORS,
    TRADE_NORMALIZATION,
    TX_NORMALIZATION,
)
from de_platform.pipeline.validation import group_errors_by_event, validate_events
from de_platform.services.cache.interface import CacheInterface
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.logger.interface import LoggingInterface
from de_platform.services.message_queue.interface import MessageQueueInterface
from de_platform.services.metrics.interface import MetricsInterface

# Event type suffixes for per-client topics
_EVENT_SUFFIXES = {
    "orders": ("order", TRADE_NORMALIZATION),
    "executions": ("execution", TRADE_NORMALIZATION),
    "transactions": ("transaction", TX_NORMALIZATION),
}


class KafkaStarterModule(Module):
    log: LoggingInterface

    def __init__(
        self,
        config: ModuleConfig,
        logger: LoggerFactory,
        mq: MessageQueueInterface,
        lifecycle: LifecycleManager,
        metrics: MetricsInterface,
        cache: CacheInterface | None = None,
    ) -> None:
        self.config = config
        self.logger = logger
        self.mq = mq
        self.lifecycle = lifecycle
        self.metrics = metrics
        self._cache = cache
        self._config_cache: Any = None
        # Per-client error topic overrides: tenant_id -> error_topic
        self._client_error_topics: dict[str, str] = {}

    async def initialize(self) -> None:
        self.log = self.logger.create()
        self._audit = AuditAccumulator(self.mq, source="kafka")
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

        # Set up per-client topic subscriptions from ClientConfigCache
        if self._cache is not None:
            from de_platform.pipeline.client_config_cache import ClientConfigCache
            self._config_cache = ClientConfigCache(self._cache)
            self._config_cache.start()
            self.lifecycle.on_shutdown(lambda: self._config_cache.stop())
            self._refresh_client_topics()

        self.log.info(
            "Kafka Starter initialized",
            module="kafka_starter",
            topics=list(self._routes.keys()),
        )

    def _refresh_client_topics(self) -> None:
        """Add per-client topic routes from ClientConfigCache."""
        if self._config_cache is None:
            return
        for tenant_id in self._config_cache.get_all_client_ids():
            topic_config = self._config_cache.get_topic_config(tenant_id)
            prefix = topic_config.get("inbound_topic_prefix", "")
            if prefix:
                for suffix, (event_type, norm_topic) in _EVENT_SUFFIXES.items():
                    client_topic = f"{prefix}{suffix}"
                    if client_topic not in self._routes:
                        self._routes[client_topic] = (event_type, norm_topic)
                        self.log.info("Added client topic",
                                      tenant_id=tenant_id, topic=client_topic)
            error_topic = topic_config.get("error_topic", "")
            if error_topic:
                self._client_error_topics[tenant_id] = error_topic

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
            self._audit.maybe_flush()
            if self.lifecycle.is_shutting_down:
                break
            await asyncio.sleep(0.01)
        self._audit.flush()
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
            msg["ingestion_method"] = "kafka"
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
            # Use per-client error topic if configured, else shared topic
            error_topic = self._client_error_topics.get(tenant_id, self.client_errors_topic)
            self.mq.publish(error_topic, err_msg, key=msg_key)

        if valid:
            for validated in valid:
                tenant_id = validated.get("tenant_id", "unknown")
                self._audit.count(tenant_id, event_type)
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

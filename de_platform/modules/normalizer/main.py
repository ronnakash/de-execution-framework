"""Normalizer Service.

Reads raw events from normalization topics, deduplicates, enriches (notional
calculation + currency conversion), and publishes to persistence + algos topics.

Topics consumed:
    trade_normalization  — orders and executions
    tx_normalization     — transactions

Topics produced:
    orders_persistence / executions_persistence / transactions_persistence
    trades_algos / transactions_algos
    duplicates           — external duplicate events
"""

from __future__ import annotations

import asyncio

from de_platform.config.context import ModuleConfig
from de_platform.modules.base import Module
from de_platform.pipeline.currency import CurrencyConverter
from de_platform.pipeline.dedup import EventDeduplicator
from de_platform.pipeline.enrichment import (
    compute_primary_key,
    enrich_trade_event,
    enrich_transaction_event,
    now_iso,
)
from de_platform.pipeline.topics import (
    DUPLICATES,
    EXECUTIONS_PERSISTENCE,
    ORDERS_PERSISTENCE,
    TRADE_NORMALIZATION,
    TRADES_ALGOS,
    TRANSACTIONS_ALGOS,
    TRANSACTIONS_PERSISTENCE,
    TX_NORMALIZATION,
)
from de_platform.services.cache.interface import CacheInterface
from de_platform.services.database.interface import DatabaseInterface
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.logger.interface import LoggingInterface
from de_platform.services.message_queue.interface import MessageQueueInterface
from de_platform.services.metrics.interface import MetricsInterface

_PERSISTENCE_TOPIC: dict[str, str] = {
    "order": ORDERS_PERSISTENCE,
    "execution": EXECUTIONS_PERSISTENCE,
    "transaction": TRANSACTIONS_PERSISTENCE,
}


class NormalizerModule(Module):
    log: LoggingInterface

    def __init__(
        self,
        config: ModuleConfig,
        logger: LoggerFactory,
        mq: MessageQueueInterface,
        cache: CacheInterface,
        db: DatabaseInterface,
        lifecycle: LifecycleManager,
        metrics: MetricsInterface,
    ) -> None:
        self.config = config
        self.logger = logger
        self.mq = mq
        self.cache = cache
        self.db = db
        self.lifecycle = lifecycle
        self.metrics = metrics

    async def initialize(self) -> None:
        self.log = self.logger.create()
        await self.db.connect_async()
        self.deduplicator = EventDeduplicator(self.cache)
        self.currency_converter = CurrencyConverter(self.db, self.cache)

        from de_platform.pipeline.client_config_cache import ClientConfigCache

        self.config_cache = ClientConfigCache(self.cache)
        self.config_cache.start()
        self.lifecycle.on_shutdown(lambda: self.config_cache.stop())
        self.lifecycle.on_shutdown(self.db.disconnect_async)
        self.log.info("Normalizer initialized", module="normalizer")

    async def execute(self) -> int:
        self.log.info("Normalizer running", module="normalizer")
        while not self.lifecycle.is_shutting_down:
            try:
                self._poll_and_process(TRADE_NORMALIZATION, "trade")
                self._poll_and_process(TX_NORMALIZATION, "transaction")
            except Exception as exc:
                self.log.error("Processing error", module="normalizer", error=str(exc))
            await asyncio.sleep(0.01)
        return 0

    def _poll_and_process(self, topic: str, category: str) -> None:
        msg = self.mq.consume_one(topic)
        if msg is None:
            return

        event_type: str = msg.get("event_type", "order")
        tenant_id: str = msg.get("tenant_id", "")
        message_id: str = msg.get("message_id", "")
        symbol: str = msg.get("symbol", "")
        msg_key = f"{tenant_id}:{symbol}" if tenant_id else None
        tags = {"service": "normalizer", "event_type": event_type, "tenant_id": tenant_id}

        self.metrics.counter("events_received_total", tags={"service": "normalizer", "topic": topic})

        primary_key = compute_primary_key(
            tenant_id, event_type, msg.get("id", ""), msg.get("transact_time", ""),
        )

        dedup_status = self.deduplicator.check(primary_key, message_id)

        if dedup_status == "internal_duplicate":
            self.metrics.counter("duplicates_detected_total", tags={**tags, "dedup_type": "internal"})
            self.metrics.counter("events_processed_total", tags={**tags, "status": "duplicate"})
            self.log.debug(
                "Internal duplicate dropped",
                tenant_id=tenant_id,
                event_type=event_type,
                message_id=message_id,
                primary_key=primary_key,
                dedup_status="internal_duplicate",
            )
            return

        if dedup_status == "external_duplicate":
            self.metrics.counter("duplicates_detected_total", tags={**tags, "dedup_type": "external"})
            self.metrics.counter("events_processed_total", tags={**tags, "status": "duplicate"})
            self.log.info(
                "External duplicate detected",
                tenant_id=tenant_id,
                event_type=event_type,
                message_id=message_id,
                primary_key=primary_key,
                dedup_status="external_duplicate",
            )
            dup_record = {
                "event_type": event_type,
                "primary_key": primary_key,
                "message_id": message_id,
                "tenant_id": tenant_id,
                "received_at": now_iso(),
                "original_event": msg,
            }
            self.mq.publish(DUPLICATES, dup_record, key=msg_key)
            return

        normalized_at = now_iso()
        is_realtime = self.config_cache.get_client_mode(tenant_id) == "realtime"

        if category == "trade":
            enriched = enrich_trade_event(msg, self.currency_converter, normalized_at)
            persistence_topic = _PERSISTENCE_TOPIC.get(event_type, ORDERS_PERSISTENCE)
            self.mq.publish(persistence_topic, enriched, key=msg_key)
            self.metrics.counter("kafka_messages_published_total", tags={"service": "normalizer", "topic": persistence_topic})
            if is_realtime:
                self.mq.publish(TRADES_ALGOS, enriched, key=msg_key)
                self.metrics.counter("kafka_messages_published_total", tags={"service": "normalizer", "topic": TRADES_ALGOS})
        else:
            enriched = enrich_transaction_event(msg, self.currency_converter, normalized_at)
            self.mq.publish(TRANSACTIONS_PERSISTENCE, enriched, key=msg_key)
            self.metrics.counter("kafka_messages_published_total", tags={"service": "normalizer", "topic": TRANSACTIONS_PERSISTENCE})
            if is_realtime:
                self.mq.publish(TRANSACTIONS_ALGOS, enriched, key=msg_key)
                self.metrics.counter("kafka_messages_published_total", tags={"service": "normalizer", "topic": TRANSACTIONS_ALGOS})

        self.metrics.counter("events_processed_total", tags={**tags, "status": "ok"})
        self.log.info(
            "Event normalized",
            tenant_id=tenant_id,
            event_type=event_type,
            event_id=msg.get("id", ""),
            message_id=message_id,
            primary_key=primary_key,
            dedup_status=dedup_status,
        )


module_class = NormalizerModule

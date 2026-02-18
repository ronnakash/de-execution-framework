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
from de_platform.modules.base import AsyncModule
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

_PERSISTENCE_TOPIC: dict[str, str] = {
    "order": ORDERS_PERSISTENCE,
    "execution": EXECUTIONS_PERSISTENCE,
    "transaction": TRANSACTIONS_PERSISTENCE,
}


class NormalizerModule(AsyncModule):
    log: LoggingInterface

    def __init__(
        self,
        config: ModuleConfig,
        logger: LoggerFactory,
        mq: MessageQueueInterface,
        cache: CacheInterface,
        db: DatabaseInterface,
        lifecycle: LifecycleManager,
    ) -> None:
        self.config = config
        self.logger = logger
        self.mq = mq
        self.cache = cache
        self.db = db
        self.lifecycle = lifecycle

    async def initialize(self) -> None:
        self.log = self.logger.create()
        self.db.connect()
        self.deduplicator = EventDeduplicator(self.cache)
        self.currency_converter = CurrencyConverter(self.db, self.cache)
        self.lifecycle.on_shutdown(self.db.disconnect)
        self.log.info("Normalizer initialized")

    async def execute(self) -> int:
        self.log.info("Normalizer running")
        while not self.lifecycle.is_shutting_down:
            self._poll_and_process(TRADE_NORMALIZATION, "trade")
            self._poll_and_process(TX_NORMALIZATION, "transaction")
            await asyncio.sleep(0.01)
        return 0

    def _poll_and_process(self, topic: str, category: str) -> None:
        msg = self.mq.consume_one(topic)
        if msg is None:
            return

        event_type: str = msg.get("event_type", "order")
        message_id: str = msg.get("message_id", "")
        primary_key = compute_primary_key(
            msg.get("tenant_id", ""),
            event_type,
            msg.get("id", ""),
            msg.get("transact_time", ""),
        )

        dedup_status = self.deduplicator.check(primary_key, message_id)

        if dedup_status == "internal_duplicate":
            self.log.debug("Internal duplicate dropped", primary_key=primary_key)
            return

        if dedup_status == "external_duplicate":
            self.log.info("External duplicate detected", primary_key=primary_key)
            self.mq.publish(DUPLICATES, msg)
            return

        normalized_at = now_iso()
        if category == "trade":
            enriched = enrich_trade_event(msg, self.currency_converter, normalized_at)
            persistence_topic = _PERSISTENCE_TOPIC.get(event_type, ORDERS_PERSISTENCE)
            self.mq.publish(persistence_topic, enriched)
            self.mq.publish(TRADES_ALGOS, enriched)
        else:
            enriched = enrich_transaction_event(msg, self.currency_converter, normalized_at)
            self.mq.publish(TRANSACTIONS_PERSISTENCE, enriched)
            self.mq.publish(TRANSACTIONS_ALGOS, enriched)

        self.log.info(
            "Event normalized",
            event_type=event_type,
            primary_key=primary_key,
            message_id=message_id,
        )


module_class = NormalizerModule

"""Tests for the Kafka Starter module using MemoryQueue."""

from __future__ import annotations

import pytest

from de_platform.config.context import ModuleConfig
from de_platform.pipeline.topics import (
    NORMALIZATION_ERRORS,
    TRADE_NORMALIZATION,
    TX_NORMALIZATION,
)
from de_platform.services.lifecycle.lifecycle_manager import LifecycleManager
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.message_queue.memory_queue import MemoryQueue
from de_platform.services.metrics.noop_metrics import NoopMetrics


# ── Fixtures ──────────────────────────────────────────────────────────────────

VALID_ORDER = {
    "id": "ord-1", "tenant_id": "t1", "status": "new",
    "transact_time": "2026-01-15T10:00:00Z", "symbol": "AAPL",
    "side": "buy", "quantity": 100.0, "price": 150.0,
    "order_type": "limit", "currency": "USD",
}

VALID_EXECUTION = {
    "id": "exec-1", "tenant_id": "t1", "status": "filled",
    "transact_time": "2026-01-15T10:00:00Z", "order_id": "ord-1",
    "symbol": "AAPL", "side": "sell", "quantity": 50.0, "price": 151.0,
    "execution_venue": "NYSE", "currency": "EUR",
}

VALID_TX = {
    "id": "tx-1", "tenant_id": "t1", "status": "settled",
    "transact_time": "2026-01-15T10:00:00Z", "account_id": "acc-1",
    "counterparty_id": "cp-1", "amount": 10000.0, "currency": "USD",
    "transaction_type": "wire",
}


def _build_module(mq: MemoryQueue, config_overrides: dict | None = None):
    from de_platform.modules.kafka_starter.main import KafkaStarterModule

    config = ModuleConfig(config_overrides or {})
    logger = LoggerFactory()
    lifecycle = LifecycleManager()
    module = KafkaStarterModule(config, logger, mq, lifecycle, NoopMetrics())
    return module, lifecycle


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _run_with_messages(mq: MemoryQueue, config: dict | None = None) -> tuple:
    """Initialize module, set shutdown, run execute (processes all pending msgs)."""
    module, lifecycle = _build_module(mq, config)
    await module.initialize()
    lifecycle._shutting_down = True
    await module.execute()
    return module, lifecycle


# ── Tests ─────────────────────────────────────────────────────────────────────

async def test_valid_order_forwarded_to_trade_normalization():
    mq = MemoryQueue()
    mq.publish("client_orders", VALID_ORDER)
    await _run_with_messages(mq)

    msg = mq.consume_one(TRADE_NORMALIZATION)
    assert msg is not None
    assert msg["event_type"] == "order"
    assert msg["id"] == "ord-1"
    assert "message_id" in msg
    assert "ingested_at" in msg


async def test_valid_execution_forwarded_to_trade_normalization():
    mq = MemoryQueue()
    mq.publish("client_executions", VALID_EXECUTION)
    await _run_with_messages(mq)

    msg = mq.consume_one(TRADE_NORMALIZATION)
    assert msg is not None
    assert msg["event_type"] == "execution"


async def test_valid_transaction_forwarded_to_tx_normalization():
    mq = MemoryQueue()
    mq.publish("client_transactions", VALID_TX)
    await _run_with_messages(mq)

    msg = mq.consume_one(TX_NORMALIZATION)
    assert msg is not None
    assert msg["event_type"] == "transaction"


async def test_invalid_event_written_to_client_errors_topic():
    mq = MemoryQueue()
    bad = {**VALID_ORDER, "quantity": -5}
    mq.publish("client_orders", bad)
    await _run_with_messages(mq)

    # Nothing on normalization topic
    assert mq.consume_one(TRADE_NORMALIZATION) is None

    # Error on normalization_errors
    norm_err = mq.consume_one(NORMALIZATION_ERRORS)
    assert norm_err is not None
    assert norm_err["event_type"] == "order"
    assert len(norm_err["errors"]) > 0

    # Error also on client_errors
    client_err = mq.consume_one("client_errors")
    assert client_err is not None


async def test_transactions_forwarded_to_tx_normalization():
    mq = MemoryQueue()
    mq.publish("client_transactions", VALID_TX)
    await _run_with_messages(mq)

    msg = mq.consume_one(TX_NORMALIZATION)
    assert msg is not None
    assert msg["id"] == "tx-1"


async def test_custom_topic_names():
    mq = MemoryQueue()
    mq.publish("my_orders", VALID_ORDER)
    await _run_with_messages(mq, {
        "client-orders-topic": "my_orders",
        "client-errors-topic": "my_errors",
    })

    msg = mq.consume_one(TRADE_NORMALIZATION)
    assert msg is not None
    assert msg["event_type"] == "order"


async def test_multi_field_invalid_event_produces_single_error_message():
    """An event with multiple validation failures should produce exactly 1 error message."""
    mq = MemoryQueue()
    bad = {**VALID_ORDER, "id": "", "currency": "x", "quantity": -5}
    mq.publish("client_orders", bad)
    await _run_with_messages(mq)

    # Exactly 1 consolidated error on normalization_errors
    err1 = mq.consume_one(NORMALIZATION_ERRORS)
    assert err1 is not None
    assert err1["event_type"] == "order"
    assert len(err1["errors"]) >= 3  # id, currency, quantity at minimum
    # No second error message
    assert mq.consume_one(NORMALIZATION_ERRORS) is None

    # Also 1 on client_errors
    client_err = mq.consume_one("client_errors")
    assert client_err is not None
    assert len(client_err["errors"]) >= 3
    assert mq.consume_one("client_errors") is None


async def test_multiple_messages_in_one_tick():
    mq = MemoryQueue()
    mq.publish("client_orders", VALID_ORDER)
    mq.publish("client_orders", {**VALID_ORDER, "id": "ord-2"})

    module, lifecycle = _build_module(mq)
    await module.initialize()

    # Run two ticks manually (is_shutting_down stays False for the first iteration)
    for _ in range(2):
        for inbound_topic, (event_type, norm_topic) in module._routes.items():
            msg = mq.consume_one(inbound_topic)
            if msg is not None:
                module._process_message(event_type, norm_topic, msg)

    msgs = []
    while True:
        m = mq.consume_one(TRADE_NORMALIZATION)
        if m is None:
            break
        msgs.append(m)
    assert len(msgs) == 2

"""Tests for the File Processor module using MemoryFileSystem and MemoryQueue."""

from __future__ import annotations

import json

import pytest

from de_platform.config.context import ModuleConfig
from de_platform.pipeline.topics import (
    NORMALIZATION_ERRORS,
    TRADE_NORMALIZATION,
    TX_NORMALIZATION,
)
from de_platform.services.filesystem.memory_filesystem import MemoryFileSystem
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.message_queue.memory_queue import MemoryQueue


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


# ── Helper ────────────────────────────────────────────────────────────────────

async def _run(
    file_path: str,
    event_type: str,
    data: bytes,
    mq: MemoryQueue | None = None,
) -> tuple[int, MemoryQueue]:
    from de_platform.modules.file_processor.main import FileProcessorModule

    fs = MemoryFileSystem()
    fs.write(file_path, data)
    if mq is None:
        mq = MemoryQueue()
    config = ModuleConfig({"file-path": file_path, "event-type": event_type})
    module = FileProcessorModule(config, LoggerFactory(), fs, mq)
    return await module.run(), mq


def _drain(mq: MemoryQueue, topic: str) -> list:
    msgs = []
    while True:
        m = mq.consume_one(topic)
        if m is None:
            break
        msgs.append(m)
    return msgs


# ── Tests ─────────────────────────────────────────────────────────────────────

async def test_valid_file_publishes_all():
    data = json.dumps([VALID_ORDER, VALID_ORDER]).encode()
    rc, mq = await _run("orders.json", "order", data)
    assert rc == 0
    msgs = _drain(mq, TRADE_NORMALIZATION)
    assert len(msgs) == 2


async def test_orders_go_to_trade_normalization():
    data = json.dumps([VALID_ORDER]).encode()
    rc, mq = await _run("orders.json", "order", data)
    assert rc == 0
    msg = mq.consume_one(TRADE_NORMALIZATION)
    assert msg is not None
    assert msg["event_type"] == "order"
    assert msg["id"] == "ord-1"
    assert "message_id" in msg
    assert "ingested_at" in msg


async def test_executions_go_to_trade_normalization():
    data = json.dumps([VALID_EXECUTION]).encode()
    rc, mq = await _run("exec.json", "execution", data)
    assert rc == 0
    msg = mq.consume_one(TRADE_NORMALIZATION)
    assert msg is not None
    assert msg["event_type"] == "execution"


async def test_transactions_go_to_tx_normalization():
    data = json.dumps([VALID_TX]).encode()
    rc, mq = await _run("tx.json", "transaction", data)
    assert rc == 0
    msg = mq.consume_one(TX_NORMALIZATION)
    assert msg is not None
    assert msg["event_type"] == "transaction"


async def test_jsonl_format_parsed_correctly():
    lines = "\n".join(json.dumps(VALID_ORDER) for _ in range(3))
    rc, mq = await _run("orders.jsonl", "order", lines.encode())
    assert rc == 0
    msgs = _drain(mq, TRADE_NORMALIZATION)
    assert len(msgs) == 3


async def test_file_with_errors_publishes_valid_and_errors_separately():
    bad = {**VALID_ORDER, "price": -1}
    data = json.dumps([VALID_ORDER, bad]).encode()
    rc, mq = await _run("orders.json", "order", data)
    assert rc == 0
    valid_msgs = _drain(mq, TRADE_NORMALIZATION)
    err_msgs = _drain(mq, NORMALIZATION_ERRORS)
    assert len(valid_msgs) == 1
    assert len(err_msgs) == 1
    assert err_msgs[0]["event_type"] == "order"


async def test_multi_field_invalid_event_produces_single_error_message():
    """An event with multiple validation failures produces exactly 1 error message."""
    bad = {**VALID_ORDER, "id": "", "currency": "x", "quantity": -5}
    data = json.dumps([bad]).encode()
    rc, mq = await _run("bad_orders.json", "order", data)
    assert rc == 0

    err_msgs = _drain(mq, NORMALIZATION_ERRORS)
    assert len(err_msgs) == 1
    assert len(err_msgs[0]["errors"]) >= 3  # id, currency, quantity at minimum
    assert err_msgs[0]["event_type"] == "order"


async def test_empty_file_returns_zero_no_messages():
    rc, mq = await _run("empty.json", "order", b"")
    assert rc == 0
    assert mq.consume_one(TRADE_NORMALIZATION) is None
    assert mq.consume_one(NORMALIZATION_ERRORS) is None


async def test_invalid_event_type_raises_on_validate():
    from de_platform.modules.file_processor.main import FileProcessorModule

    fs = MemoryFileSystem()
    fs.write("x.json", json.dumps([VALID_ORDER]).encode())
    module = FileProcessorModule(
        ModuleConfig({"file-path": "x.json", "event-type": "banana"}),
        LoggerFactory(), fs, MemoryQueue(),
    )
    await module.initialize()
    with pytest.raises(ValueError, match="event-type"):
        await module.validate()


async def test_missing_file_path_raises():
    from de_platform.modules.file_processor.main import FileProcessorModule

    module = FileProcessorModule(
        ModuleConfig({"event-type": "order"}),
        LoggerFactory(), MemoryFileSystem(), MemoryQueue(),
    )
    await module.initialize()
    with pytest.raises(ValueError, match="file-path"):
        await module.validate()

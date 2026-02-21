"""Integration tests: FileProcessorModule reads from real MinIO.

Verifies that file_processor can read JSONL files from MinIO and
publish valid/error events to the message queue.
"""

from __future__ import annotations

import json
import uuid

import pytest

from de_platform.config.context import ModuleConfig
from de_platform.modules.file_processor.main import FileProcessorModule
from de_platform.pipeline.topics import NORMALIZATION_ERRORS, TRADE_NORMALIZATION
from de_platform.services.logger.factory import LoggerFactory
from de_platform.services.message_queue.memory_queue import MemoryQueue


pytestmark = pytest.mark.real_infra


VALID_ORDER = {
    "id": "ord-integ-1",
    "tenant_id": "integ-test",
    "status": "new",
    "transact_time": "2026-01-15T10:00:00Z",
    "symbol": "AAPL",
    "side": "buy",
    "quantity": 100.0,
    "price": 150.0,
    "order_type": "limit",
    "currency": "USD",
}


async def test_file_processor_reads_from_minio(minio_fs):
    """FileProcessorModule reads a JSONL file from MinIO and publishes events."""
    mq = MemoryQueue()
    path = f"integ-test/orders/{uuid.uuid4().hex[:8]}.jsonl"
    events = [VALID_ORDER, {**VALID_ORDER, "id": "ord-integ-2"}]
    content = "\n".join(json.dumps(e) for e in events).encode()
    minio_fs.write(path, content)

    module = FileProcessorModule(
        config=ModuleConfig({"file-path": path, "event-type": "order"}),
        logger=LoggerFactory(default_impl="memory"),
        fs=minio_fs,
        mq=mq,
    )
    rc = await module.run()
    assert rc == 0

    msgs = []
    while True:
        m = mq.consume_one(TRADE_NORMALIZATION)
        if m is None:
            break
        msgs.append(m)
    assert len(msgs) == 2
    assert msgs[0]["event_type"] == "order"


async def test_file_processor_minio_with_invalid_events(minio_fs):
    """FileProcessorModule reports validation errors for bad events from MinIO."""
    mq = MemoryQueue()
    bad_order = {**VALID_ORDER, "price": -1, "id": "ord-bad"}
    path = f"integ-test/orders/{uuid.uuid4().hex[:8]}.jsonl"
    content = json.dumps(bad_order).encode()
    minio_fs.write(path, content)

    module = FileProcessorModule(
        config=ModuleConfig({"file-path": path, "event-type": "order"}),
        logger=LoggerFactory(default_impl="memory"),
        fs=minio_fs,
        mq=mq,
    )
    rc = await module.run()
    assert rc == 0

    errors = []
    while True:
        m = mq.consume_one(NORMALIZATION_ERRORS)
        if m is None:
            break
        errors.append(m)
    assert len(errors) == 1
    assert errors[0]["event_type"] == "order"

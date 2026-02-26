"""Tests for the AuditAccumulator class."""

from __future__ import annotations

import time

from de_platform.pipeline.audit_accumulator import AuditAccumulator
from de_platform.pipeline.topics import AUDIT_COUNTS
from de_platform.services.message_queue.memory_queue import MemoryQueue


def _drain(mq: MemoryQueue, topic: str) -> list[dict]:
    msgs = []
    while True:
        m = mq.consume_one(topic)
        if m is None:
            break
        msgs.append(m)
    return msgs


def test_count_accumulates():
    mq = MemoryQueue()
    acc = AuditAccumulator(mq, source="rest")
    acc.count("t1", "order")
    acc.count("t1", "order")
    acc.count("t1", "order")
    acc.flush()
    msgs = _drain(mq, AUDIT_COUNTS)
    assert len(msgs) == 1
    assert msgs[0]["received"] == 3


def test_flush_publishes_to_audit_counts_topic():
    mq = MemoryQueue()
    acc = AuditAccumulator(mq, source="rest")
    acc.count("t1", "order", 5)
    acc.flush()
    msgs = _drain(mq, AUDIT_COUNTS)
    assert len(msgs) == 1
    assert msgs[0]["tenant_id"] == "t1"
    assert msgs[0]["source"] == "rest"
    assert msgs[0]["event_type"] == "order"
    assert msgs[0]["received"] == 5
    assert "timestamp" in msgs[0]


def test_flush_clears_counts():
    mq = MemoryQueue()
    acc = AuditAccumulator(mq, source="kafka")
    acc.count("t1", "order")
    acc.flush()
    assert acc._counts == {}
    # Second flush publishes nothing
    acc.flush()
    msgs = _drain(mq, AUDIT_COUNTS)
    # First flush produced 1 message, second produced nothing
    assert len(msgs) == 1


def test_maybe_flush_respects_interval():
    mq = MemoryQueue()
    acc = AuditAccumulator(mq, source="rest", flush_interval=10.0)
    acc.count("t1", "order")

    # Immediately after construction, maybe_flush should not flush
    acc.maybe_flush()
    msgs = _drain(mq, AUDIT_COUNTS)
    assert len(msgs) == 0

    # Simulate time passing past the interval
    acc._last_flush = time.monotonic() - 11.0
    acc.maybe_flush()
    msgs = _drain(mq, AUDIT_COUNTS)
    assert len(msgs) == 1


def test_multiple_tenants_and_event_types():
    mq = MemoryQueue()
    acc = AuditAccumulator(mq, source="kafka")
    acc.count("t1", "order", 2)
    acc.count("t2", "transaction", 3)
    acc.flush()
    msgs = _drain(mq, AUDIT_COUNTS)
    assert len(msgs) == 2
    by_key = {(m["tenant_id"], m["event_type"]): m["received"] for m in msgs}
    assert by_key[("t1", "order")] == 2
    assert by_key[("t2", "transaction")] == 3


def test_flush_no_op_when_empty():
    mq = MemoryQueue()
    acc = AuditAccumulator(mq, source="file")
    acc.flush()
    msgs = _drain(mq, AUDIT_COUNTS)
    assert len(msgs) == 0


def test_count_with_n_parameter():
    mq = MemoryQueue()
    acc = AuditAccumulator(mq, source="file")
    acc.count("t1", "execution", n=5)
    acc.flush()
    msgs = _drain(mq, AUDIT_COUNTS)
    assert len(msgs) == 1
    assert msgs[0]["received"] == 5

"""Tests for stress test event generators."""

from tests.stress.generators import (
    EVENT_GENERATORS,
    generate_batch,
    generate_execution,
    generate_mixed_batch,
    generate_order,
    generate_transaction,
)


def test_generate_order_has_required_fields():
    event = generate_order("t1")
    assert event["tenant_id"] == "t1"
    assert event["id"].startswith("o-")
    assert "symbol" in event
    assert "price" in event
    assert "quantity" in event
    assert "currency" in event


def test_generate_execution_has_required_fields():
    event = generate_execution("t1")
    assert event["tenant_id"] == "t1"
    assert event["id"].startswith("e-")
    assert "order_id" in event
    assert "execution_venue" in event


def test_generate_transaction_has_required_fields():
    event = generate_transaction("t1")
    assert event["tenant_id"] == "t1"
    assert event["id"].startswith("tx-")
    assert "counterparty_id" in event
    assert "amount" in event


def test_generate_batch_returns_correct_count():
    batch = generate_batch("t1", "order", 50)
    assert len(batch) == 50
    assert all(e["tenant_id"] == "t1" for e in batch)


def test_generate_mixed_batch_returns_correct_count():
    batch = generate_mixed_batch("t1", 100)
    assert len(batch) == 100
    types_seen = {etype for etype, _ in batch}
    # With 100 events we should see multiple types
    assert len(types_seen) >= 2


def test_generate_mixed_batch_respects_event_types():
    batch = generate_mixed_batch("t1", 50, event_types=["order"])
    assert all(etype == "order" for etype, _ in batch)


def test_event_generators_map_has_all_types():
    assert set(EVENT_GENERATORS.keys()) == {"order", "execution", "transaction"}


def test_generated_ids_are_unique():
    events = generate_batch("t1", "order", 100)
    ids = [e["id"] for e in events]
    assert len(set(ids)) == 100

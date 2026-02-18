"""Tests for Event dataclass creation, inheritance, and round-trips."""

from __future__ import annotations

from de_platform.pipeline.events import ExecutionEvent, OrderEvent, TransactionEvent


def _order_event() -> OrderEvent:
    return OrderEvent(
        id="ord-1", tenant_id="t1", status="new",
        transact_time="2026-01-15T10:00:00Z", symbol="AAPL",
        side="buy", quantity=100.0, price=150.0, order_type="limit", currency="USD",
        message_id="msg-1", notional=15000.0, notional_usd=15000.0,
        ingested_at="2026-01-15T10:00:01Z", normalized_at="2026-01-15T10:00:02Z",
        primary_key="t1_order_ord-1_2026-01-15",
    )


def _execution_event() -> ExecutionEvent:
    return ExecutionEvent(
        id="exec-1", tenant_id="t1", status="filled",
        transact_time="2026-01-15T10:00:00Z", order_id="ord-1",
        symbol="AAPL", side="sell", quantity=50.0, price=151.0,
        execution_venue="NYSE", currency="EUR",
        message_id="msg-2", notional=7550.0, notional_usd=8200.0,
        ingested_at="2026-01-15T10:00:01Z", normalized_at="2026-01-15T10:00:02Z",
        primary_key="t1_execution_exec-1_2026-01-15",
    )


def _transaction_event() -> TransactionEvent:
    return TransactionEvent(
        id="tx-1", tenant_id="t1", status="settled",
        transact_time="2026-01-15T10:00:00Z", account_id="acc-1",
        counterparty_id="cp-1", amount=10000.0, currency="USD",
        transaction_type="wire",
        message_id="msg-3", amount_usd=10000.0,
        ingested_at="2026-01-15T10:00:01Z", normalized_at="2026-01-15T10:00:02Z",
        primary_key="t1_transaction_tx-1_2026-01-15",
    )


def test_order_event_inherits_dto_fields():
    e = _order_event()
    assert e.id == "ord-1"
    assert e.symbol == "AAPL"
    assert e.notional == 15000.0


def test_order_event_round_trip():
    orig = _order_event()
    restored = OrderEvent.from_dict(orig.to_dict())
    assert restored == orig


def test_execution_event_round_trip():
    orig = _execution_event()
    restored = ExecutionEvent.from_dict(orig.to_dict())
    assert restored == orig


def test_transaction_event_round_trip():
    orig = _transaction_event()
    restored = TransactionEvent.from_dict(orig.to_dict())
    assert restored == orig


def test_order_event_to_dict_has_all_fields():
    d = _order_event().to_dict()
    for key in ("id", "tenant_id", "symbol", "message_id", "notional", "notional_usd",
                "ingested_at", "normalized_at", "primary_key"):
        assert key in d, f"Missing field: {key}"


def test_transaction_event_to_dict_has_amount_usd():
    d = _transaction_event().to_dict()
    assert "amount_usd" in d
    assert d["amount_usd"] == 10000.0

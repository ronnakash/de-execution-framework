"""Tests for DTO serialization round-trips."""

from __future__ import annotations

from de_platform.pipeline.dto import ExecutionDTO, OrderDTO, TransactionDTO
from de_platform.pipeline.serialization import dto_to_message, error_to_dict, message_to_dto
from de_platform.pipeline.validation import ValidationError


def _order() -> OrderDTO:
    return OrderDTO(
        id="ord-1", tenant_id="t1", status="new",
        transact_time="2026-01-15T10:00:00Z", symbol="AAPL",
        side="buy", quantity=100.0, price=150.0, order_type="limit", currency="USD",
    )


def _execution() -> ExecutionDTO:
    return ExecutionDTO(
        id="exec-1", tenant_id="t1", status="filled",
        transact_time="2026-01-15T10:00:00Z", order_id="ord-1",
        symbol="AAPL", side="sell", quantity=50.0, price=151.0,
        execution_venue="NYSE", currency="EUR",
    )


def _transaction() -> TransactionDTO:
    return TransactionDTO(
        id="tx-1", tenant_id="t1", status="settled",
        transact_time="2026-01-15T10:00:00Z", account_id="acc-1",
        counterparty_id="cp-1", amount=10000.0, currency="USD",
        transaction_type="wire",
    )


# ── to_dict / from_dict round-trips ──────────────────────────────────────────

def test_order_round_trip():
    orig = _order()
    restored = OrderDTO.from_dict(orig.to_dict())
    assert restored == orig


def test_execution_round_trip():
    orig = _execution()
    restored = ExecutionDTO.from_dict(orig.to_dict())
    assert restored == orig


def test_transaction_round_trip():
    orig = _transaction()
    restored = TransactionDTO.from_dict(orig.to_dict())
    assert restored == orig


# ── dto_to_message ────────────────────────────────────────────────────────────

def test_dto_to_message_includes_metadata():
    msg = dto_to_message(_order(), message_id="msg-abc")
    assert msg["message_id"] == "msg-abc"
    assert "ingested_at" in msg
    assert msg["event_type"] == "order"


def test_dto_to_message_execution():
    msg = dto_to_message(_execution(), message_id="msg-xyz")
    assert msg["event_type"] == "execution"
    assert msg["order_id"] == "ord-1"


def test_dto_to_message_transaction():
    msg = dto_to_message(_transaction(), message_id="msg-1")
    assert msg["event_type"] == "transaction"
    assert msg["amount"] == 10000.0


def test_dto_to_message_explicit_event_type():
    msg = dto_to_message(_order(), message_id="m1", event_type="order")
    assert msg["event_type"] == "order"


# ── message_to_dto ────────────────────────────────────────────────────────────

def test_message_to_dto_order():
    msg = dto_to_message(_order(), message_id="m1")
    restored = message_to_dto("order", msg)
    assert isinstance(restored, OrderDTO)
    assert restored.id == "ord-1"


def test_message_to_dto_execution():
    msg = dto_to_message(_execution(), message_id="m2")
    restored = message_to_dto("execution", msg)
    assert isinstance(restored, ExecutionDTO)


def test_message_to_dto_transaction():
    msg = dto_to_message(_transaction(), message_id="m3")
    restored = message_to_dto("transaction", msg)
    assert isinstance(restored, TransactionDTO)


# ── error_to_dict ─────────────────────────────────────────────────────────────

def test_error_to_dict_structure():
    raw = {"id": "", "tenant_id": "t1"}
    errs = [ValidationError(field="id", message="id is required", event_index=0)]
    result = error_to_dict(raw, "order", errs, tenant_id="t1")
    assert result["event_type"] == "order"
    assert result["tenant_id"] == "t1"
    assert result["raw_data"] == raw
    assert len(result["errors"]) == 1
    assert result["errors"][0]["field"] == "id"
    assert "created_at" in result

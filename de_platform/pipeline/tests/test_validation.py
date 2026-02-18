"""Tests for pipeline event validation."""

from __future__ import annotations

import pytest

from de_platform.pipeline.validation import (
    validate_events,
    validate_execution,
    validate_order,
    validate_transaction,
)

# ── Fixtures ──────────────────────────────────────────────────────────────────

VALID_ORDER = {
    "id": "ord-1",
    "tenant_id": "t1",
    "status": "new",
    "transact_time": "2026-01-15T10:00:00Z",
    "symbol": "AAPL",
    "side": "buy",
    "quantity": 100.0,
    "price": 150.0,
    "order_type": "limit",
    "currency": "USD",
}

VALID_EXECUTION = {
    "id": "exec-1",
    "tenant_id": "t1",
    "status": "filled",
    "transact_time": "2026-01-15T10:00:00Z",
    "order_id": "ord-1",
    "symbol": "AAPL",
    "side": "sell",
    "quantity": 50.0,
    "price": 151.0,
    "execution_venue": "NYSE",
    "currency": "EUR",
}

VALID_TRANSACTION = {
    "id": "tx-1",
    "tenant_id": "t1",
    "status": "settled",
    "transact_time": "2026-01-15T10:00:00Z",
    "account_id": "acc-123",
    "counterparty_id": "cp-456",
    "amount": 10000.0,
    "currency": "USD",
    "transaction_type": "wire",
}

# ── Order validation ──────────────────────────────────────────────────────────

def test_valid_order_has_no_errors():
    assert validate_order(VALID_ORDER, 0) == []


def test_order_missing_id():
    data = {**VALID_ORDER, "id": ""}
    errs = validate_order(data, 0)
    assert any(e.field == "id" for e in errs)


def test_order_missing_tenant_id():
    data = {**VALID_ORDER}
    del data["tenant_id"]
    errs = validate_order(data, 0)
    assert any(e.field == "tenant_id" for e in errs)


def test_order_invalid_transact_time():
    data = {**VALID_ORDER, "transact_time": "not-a-date"}
    errs = validate_order(data, 0)
    assert any(e.field == "transact_time" for e in errs)


def test_order_zero_quantity():
    data = {**VALID_ORDER, "quantity": 0}
    errs = validate_order(data, 0)
    assert any(e.field == "quantity" for e in errs)


def test_order_negative_price():
    data = {**VALID_ORDER, "price": -1.0}
    errs = validate_order(data, 0)
    assert any(e.field == "price" for e in errs)


def test_order_invalid_currency():
    data = {**VALID_ORDER, "currency": "usd"}  # lowercase
    errs = validate_order(data, 0)
    assert any(e.field == "currency" for e in errs)


def test_order_invalid_side():
    data = {**VALID_ORDER, "side": "long"}
    errs = validate_order(data, 0)
    assert any(e.field == "side" for e in errs)


# ── Execution validation ──────────────────────────────────────────────────────

def test_valid_execution_has_no_errors():
    assert validate_execution(VALID_EXECUTION, 0) == []


def test_execution_missing_order_id():
    data = {**VALID_EXECUTION, "order_id": ""}
    errs = validate_execution(data, 0)
    assert any(e.field == "order_id" for e in errs)


def test_execution_missing_venue():
    data = {**VALID_EXECUTION}
    del data["execution_venue"]
    errs = validate_execution(data, 0)
    assert any(e.field == "execution_venue" for e in errs)


# ── Transaction validation ────────────────────────────────────────────────────

def test_valid_transaction_has_no_errors():
    assert validate_transaction(VALID_TRANSACTION, 0) == []


def test_transaction_missing_account_id():
    data = {**VALID_TRANSACTION, "account_id": ""}
    errs = validate_transaction(data, 0)
    assert any(e.field == "account_id" for e in errs)


def test_transaction_zero_amount():
    data = {**VALID_TRANSACTION, "amount": 0}
    errs = validate_transaction(data, 0)
    assert any(e.field == "amount" for e in errs)


def test_transaction_invalid_currency_length():
    data = {**VALID_TRANSACTION, "currency": "USDD"}
    errs = validate_transaction(data, 0)
    assert any(e.field == "currency" for e in errs)


# ── validate_events batch ─────────────────────────────────────────────────────

def test_validate_events_all_valid():
    valid, errors = validate_events("order", [VALID_ORDER, VALID_ORDER])
    assert len(valid) == 2
    assert errors == []


def test_validate_events_mixed_batch():
    bad = {**VALID_ORDER, "quantity": -5}
    valid, errors = validate_events("order", [VALID_ORDER, bad, VALID_ORDER])
    assert len(valid) == 2
    assert len(errors) == 1
    assert errors[0].event_index == 1


def test_validate_events_all_invalid():
    bad = {"id": "", "tenant_id": ""}
    valid, errors = validate_events("order", [bad])
    assert valid == []
    assert len(errors) > 0


def test_validate_events_empty_batch():
    valid, errors = validate_events("transaction", [])
    assert valid == []
    assert errors == []


def test_validate_events_unknown_type():
    with pytest.raises(ValueError, match="Unknown event_type"):
        validate_events("unknown", [])


def test_validate_events_iso8601_with_offset():
    """Timestamps with +HH:MM offset should also be accepted."""
    data = {**VALID_ORDER, "transact_time": "2026-01-15T10:00:00+05:30"}
    valid, errors = validate_events("order", [data])
    assert len(valid) == 1
    assert errors == []

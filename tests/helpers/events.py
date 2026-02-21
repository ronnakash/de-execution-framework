"""Canonical event factories and routing maps for E2E tests.

All test files should import from here instead of defining their own factories.
"""

from __future__ import annotations

import uuid
from typing import Any, Callable

from de_platform.pipeline.topics import (
    ORDERS_PERSISTENCE,
    EXECUTIONS_PERSISTENCE,
    TRANSACTIONS_PERSISTENCE,
    TRADE_NORMALIZATION,
    TRADES_ALGOS,
    TRANSACTIONS_ALGOS,
    TX_NORMALIZATION,
)


# ── Event factories ──────────────────────────────────────────────────────────


def make_order(
    id_: str | None = None,
    tenant_id: str = "acme",
    quantity: float = 100.0,
    price: float = 200.0,
    currency: str = "USD",
) -> dict[str, Any]:
    return {
        "id": id_ or f"o-{uuid.uuid4().hex[:8]}",
        "tenant_id": tenant_id,
        "status": "new",
        "transact_time": "2026-01-15T10:00:00+00:00",
        "symbol": "AAPL",
        "side": "buy",
        "quantity": quantity,
        "price": price,
        "order_type": "limit",
        "currency": currency,
    }


def make_execution(
    id_: str | None = None,
    tenant_id: str = "acme",
    quantity: float = 50.0,
    price: float = 210.0,
    currency: str = "USD",
) -> dict[str, Any]:
    return {
        "id": id_ or f"e-{uuid.uuid4().hex[:8]}",
        "tenant_id": tenant_id,
        "status": "filled",
        "transact_time": "2026-01-15T10:00:00+00:00",
        "order_id": "o-abc",
        "symbol": "AAPL",
        "side": "buy",
        "quantity": quantity,
        "price": price,
        "execution_venue": "NYSE",
        "currency": currency,
    }


def make_transaction(
    id_: str | None = None,
    tenant_id: str = "acme",
    amount: float = 500.0,
    currency: str = "USD",
    counterparty_id: str = "cp-safe",
) -> dict[str, Any]:
    return {
        "id": id_ or f"tx-{uuid.uuid4().hex[:8]}",
        "tenant_id": tenant_id,
        "status": "settled",
        "transact_time": "2026-01-15T10:00:00+00:00",
        "account_id": "acc-1",
        "counterparty_id": counterparty_id,
        "amount": amount,
        "currency": currency,
        "transaction_type": "wire",
    }


def make_invalid(event_type: str, tenant_id: str = "acme") -> dict[str, Any]:
    """Return an event with an empty id — fails the non_empty_str validation check."""
    evt = EVENT_FACTORY[event_type](tenant_id=tenant_id)
    evt["id"] = ""
    return evt


def make_multi_invalid(event_type: str, tenant_id: str = "acme") -> dict[str, Any]:
    """Return an event with multiple validation failures (empty id + bad currency)."""
    evt = EVENT_FACTORY[event_type](tenant_id=tenant_id)
    evt["id"] = ""
    evt["currency"] = "x"
    return evt


# ── Lookup maps ──────────────────────────────────────────────────────────────

EVENT_FACTORY: dict[str, Callable[..., dict[str, Any]]] = {
    "order": make_order,
    "execution": make_execution,
    "transaction": make_transaction,
}

EVENT_TABLE: dict[str, str] = {
    "order": "orders",
    "execution": "executions",
    "transaction": "transactions",
}

REST_ENDPOINT: dict[str, str] = {
    "order": "orders",
    "execution": "executions",
    "transaction": "transactions",
}

CLIENT_TOPIC: dict[str, str] = {
    "order": "client_orders",
    "execution": "client_executions",
    "transaction": "client_transactions",
}

NORM_TOPIC: dict[str, str] = {
    "order": TRADE_NORMALIZATION,
    "execution": TRADE_NORMALIZATION,
    "transaction": TX_NORMALIZATION,
}

CATEGORY: dict[str, str] = {
    "order": "trade",
    "execution": "trade",
    "transaction": "transaction",
}

PERSIST_TOPIC_TABLE: dict[str, tuple[str, str]] = {
    "order": (ORDERS_PERSISTENCE, "orders"),
    "execution": (EXECUTIONS_PERSISTENCE, "executions"),
    "transaction": (TRANSACTIONS_PERSISTENCE, "transactions"),
}

ALGOS_TOPIC: dict[str, str] = {
    "order": TRADES_ALGOS,
    "execution": TRADES_ALGOS,
    "transaction": TRANSACTIONS_ALGOS,
}

FULL_MATRIX: list[tuple[str, str]] = [
    (method, etype)
    for method in ("rest", "kafka", "files")
    for etype in ("order", "execution", "transaction")
]

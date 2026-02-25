"""Event generators for stress testing.

Produces randomized events at scale for orders, executions, and transactions.
"""

from __future__ import annotations

import random
import uuid
from typing import Any

SYMBOLS = ["AAPL", "GOOG", "MSFT", "AMZN", "META", "NVDA", "TSLA", "JPM", "BAC", "WMT"]
CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CHF"]
SIDES = ["buy", "sell"]
ORDER_TYPES = ["limit", "market", "stop"]
VENUES = ["NYSE", "NASDAQ", "LSE", "TSE", "HKEX"]
TX_TYPES = ["wire", "ach", "swift", "internal"]
COUNTERPARTIES = [f"cp-{i}" for i in range(20)]


def generate_order(tenant_id: str) -> dict[str, Any]:
    """Generate a single randomized order event."""
    return {
        "id": f"o-{uuid.uuid4().hex[:12]}",
        "tenant_id": tenant_id,
        "status": "new",
        "transact_time": "2026-01-15T10:00:00+00:00",
        "symbol": random.choice(SYMBOLS),
        "side": random.choice(SIDES),
        "quantity": round(random.uniform(1, 10000), 2),
        "price": round(random.uniform(1, 5000), 2),
        "order_type": random.choice(ORDER_TYPES),
        "currency": random.choice(CURRENCIES),
    }


def generate_execution(tenant_id: str) -> dict[str, Any]:
    """Generate a single randomized execution event."""
    return {
        "id": f"e-{uuid.uuid4().hex[:12]}",
        "tenant_id": tenant_id,
        "status": "filled",
        "transact_time": "2026-01-15T10:00:00+00:00",
        "order_id": f"o-{uuid.uuid4().hex[:8]}",
        "symbol": random.choice(SYMBOLS),
        "side": random.choice(SIDES),
        "quantity": round(random.uniform(1, 10000), 2),
        "price": round(random.uniform(1, 5000), 2),
        "execution_venue": random.choice(VENUES),
        "currency": random.choice(CURRENCIES),
    }


def generate_transaction(tenant_id: str) -> dict[str, Any]:
    """Generate a single randomized transaction event."""
    return {
        "id": f"tx-{uuid.uuid4().hex[:12]}",
        "tenant_id": tenant_id,
        "status": "settled",
        "transact_time": "2026-01-15T10:00:00+00:00",
        "account_id": f"acc-{random.randint(1, 100)}",
        "counterparty_id": random.choice(COUNTERPARTIES),
        "amount": round(random.uniform(10, 100000), 2),
        "currency": random.choice(CURRENCIES),
        "transaction_type": random.choice(TX_TYPES),
    }


EVENT_GENERATORS: dict[str, Any] = {
    "order": generate_order,
    "execution": generate_execution,
    "transaction": generate_transaction,
}


def generate_batch(
    tenant_id: str,
    event_type: str,
    count: int,
) -> list[dict[str, Any]]:
    """Generate a batch of events of the specified type."""
    gen = EVENT_GENERATORS[event_type]
    return [gen(tenant_id) for _ in range(count)]


def generate_mixed_batch(
    tenant_id: str,
    count: int,
    event_types: list[str] | None = None,
) -> list[tuple[str, dict[str, Any]]]:
    """Generate a mixed batch of events with random types.

    Returns list of (event_type, event_dict) tuples.
    """
    types = event_types or list(EVENT_GENERATORS.keys())
    result = []
    for _ in range(count):
        etype = random.choice(types)
        result.append((etype, EVENT_GENERATORS[etype](tenant_id)))
    return result

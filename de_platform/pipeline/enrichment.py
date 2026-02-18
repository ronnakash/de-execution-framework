"""Enrichment helpers used by the Normalizer service.

Adds computed fields (notional, currency conversion, primary_key, timestamps)
to raw Kafka message dicts before they are published to downstream topics.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from de_platform.pipeline.currency import CurrencyConverter


def now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()


def compute_primary_key(
    tenant_id: str,
    event_type: str,
    event_id: str,
    transact_time: str,
) -> str:
    """Build the dedup/lookup key: ``{tenant_id}_{event_type}_{event_id}_{YYYY-MM-DD}``."""
    date = transact_time[:10]  # YYYY-MM-DD
    return f"{tenant_id}_{event_type}_{event_id}_{date}"


def enrich_trade_event(
    msg: dict[str, Any],
    currency_converter: CurrencyConverter,
    normalized_at: str,
) -> dict[str, Any]:
    """Add notional, notional_usd, normalized_at, primary_key to an order/execution message."""
    event_type: str = msg.get("event_type", "order")
    notional = float(msg["quantity"]) * float(msg["price"])
    notional_usd = currency_converter.convert(notional, msg["currency"])
    primary_key = compute_primary_key(
        msg["tenant_id"], event_type, msg["id"], msg["transact_time"]
    )
    return {
        **msg,
        "notional": notional,
        "notional_usd": notional_usd,
        "normalized_at": normalized_at,
        "primary_key": primary_key,
    }


def enrich_transaction_event(
    msg: dict[str, Any],
    currency_converter: CurrencyConverter,
    normalized_at: str,
) -> dict[str, Any]:
    """Add amount_usd, normalized_at, primary_key to a transaction message."""
    amount_usd = currency_converter.convert(float(msg["amount"]), msg["currency"])
    primary_key = compute_primary_key(
        msg["tenant_id"], "transaction", msg["id"], msg["transact_time"]
    )
    return {
        **msg,
        "amount_usd": amount_usd,
        "normalized_at": normalized_at,
        "primary_key": primary_key,
    }

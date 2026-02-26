"""Event classes representing normalized + enriched events produced by the Normalizer.

Each event class extends its corresponding DTO with enrichment fields added during
normalization (notional, currency conversion, dedup key, timestamps, primary key).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from de_platform.pipeline.dto import ExecutionDTO, OrderDTO, TransactionDTO


@dataclass
class OrderEvent(OrderDTO):
    """Normalized and enriched order event."""

    message_id: str         # UUID assigned on ingestion
    notional: float         # quantity * price
    notional_usd: float     # notional converted to USD
    ingested_at: str        # ISO 8601 timestamp of receipt by a starter
    normalized_at: str      # ISO 8601 timestamp of normalization
    primary_key: str        # {tenant_id}_{order}_{id}_{date}

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "OrderEvent":
        return cls(
            id=data["id"],
            tenant_id=data["tenant_id"],
            status=data["status"],
            transact_time=data["transact_time"],
            symbol=data["symbol"],
            side=data["side"],
            quantity=float(data["quantity"]),
            price=float(data["price"]),
            order_type=data["order_type"],
            currency=data["currency"],
            message_id=data["message_id"],
            notional=float(data["notional"]),
            notional_usd=float(data["notional_usd"]),
            ingested_at=data["ingested_at"],
            normalized_at=data["normalized_at"],
            primary_key=data["primary_key"],
        )


@dataclass
class ExecutionEvent(ExecutionDTO):
    """Normalized and enriched execution event."""

    message_id: str
    notional: float
    notional_usd: float
    ingested_at: str
    normalized_at: str
    primary_key: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ExecutionEvent":
        return cls(
            id=data["id"],
            tenant_id=data["tenant_id"],
            status=data["status"],
            transact_time=data["transact_time"],
            order_id=data["order_id"],
            symbol=data["symbol"],
            side=data["side"],
            quantity=float(data["quantity"]),
            price=float(data["price"]),
            execution_venue=data["execution_venue"],
            currency=data["currency"],
            message_id=data["message_id"],
            notional=float(data["notional"]),
            notional_usd=float(data["notional_usd"]),
            ingested_at=data["ingested_at"],
            normalized_at=data["normalized_at"],
            primary_key=data["primary_key"],
        )


@dataclass
class TransactionEvent(TransactionDTO):
    """Normalized and enriched transaction event."""

    message_id: str
    amount_usd: float       # amount converted to USD
    ingested_at: str
    normalized_at: str
    primary_key: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TransactionEvent":
        return cls(
            id=data["id"],
            tenant_id=data["tenant_id"],
            status=data["status"],
            transact_time=data["transact_time"],
            account_id=data["account_id"],
            counterparty_id=data["counterparty_id"],
            amount=float(data["amount"]),
            currency=data["currency"],
            transaction_type=data["transaction_type"],
            message_id=data["message_id"],
            amount_usd=float(data["amount_usd"]),
            ingested_at=data["ingested_at"],
            normalized_at=data["normalized_at"],
            primary_key=data["primary_key"],
        )

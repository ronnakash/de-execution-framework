"""Data Transfer Object (DTO) classes representing what clients send to the pipeline.

All DTOs are plain dataclasses with ``to_dict()`` / ``from_dict()`` helpers for
JSON serialization over Kafka and REST.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class OrderDTO:
    """A single trade order submitted by a client."""

    id: str
    tenant_id: str
    status: str             # e.g. "new", "filled", "cancelled"
    transact_time: str      # ISO 8601
    symbol: str
    side: str               # "buy" | "sell"
    quantity: float
    price: float
    order_type: str         # "market" | "limit"
    currency: str           # ISO 4217

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "OrderDTO":
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
        )


@dataclass
class ExecutionDTO:
    """A trade execution report submitted by a client."""

    id: str
    tenant_id: str
    status: str
    transact_time: str
    order_id: str
    symbol: str
    side: str               # "buy" | "sell"
    quantity: float
    price: float
    execution_venue: str
    currency: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ExecutionDTO":
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
        )


@dataclass
class TransactionDTO:
    """A financial transaction submitted by a client."""

    id: str
    tenant_id: str
    status: str
    transact_time: str
    account_id: str
    counterparty_id: str
    amount: float
    currency: str
    transaction_type: str   # "wire" | "ach" | "internal"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TransactionDTO":
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
        )

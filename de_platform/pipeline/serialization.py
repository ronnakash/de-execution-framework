"""Shared serialization helpers for converting DTOs to Kafka messages and back.

Kafka messages include the original DTO fields plus ``message_id`` (assigned on
ingestion) and ``ingested_at`` (timestamp of receipt). The ``event_type`` field
is also embedded so consumers can dispatch without out-of-band information.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from de_platform.pipeline.dto import ExecutionDTO, OrderDTO, TransactionDTO

_DTO_TYPES = {
    "order": OrderDTO,
    "execution": ExecutionDTO,
    "transaction": TransactionDTO,
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def dto_to_message(
    dto: OrderDTO | ExecutionDTO | TransactionDTO,
    message_id: str,
    event_type: str | None = None,
) -> dict[str, Any]:
    """Convert a DTO to a Kafka message dict.

    Adds ``message_id``, ``ingested_at``, and ``event_type`` to the DTO's fields.

    Args:
        dto: The DTO to serialize.
        message_id: Unique identifier assigned on ingestion (e.g. ``uuid.uuid4().hex``).
        event_type: Optional explicit type string. If omitted, inferred from the
                    class name (``OrderDTO`` → ``"order"``).
    """
    if event_type is None:
        cls_name = type(dto).__name__.lower()
        event_type = cls_name.replace("dto", "")

    msg = dto.to_dict()
    msg["message_id"] = message_id
    msg["ingested_at"] = _now_iso()
    msg["event_type"] = event_type
    return msg


def message_to_dto(
    event_type: str,
    data: dict[str, Any],
) -> OrderDTO | ExecutionDTO | TransactionDTO:
    """Reconstruct a DTO from a Kafka message dict.

    Args:
        event_type: One of ``"order"``, ``"execution"``, ``"transaction"``.
        data: The message payload (may include extra fields like ``message_id``).
    """
    cls = _DTO_TYPES.get(event_type)
    if cls is None:
        raise ValueError(f"Unknown event_type: {event_type!r}")
    return cls.from_dict(data)


def error_to_dict(
    raw_event: dict[str, Any],
    event_type: str,
    errors: list[Any],
    tenant_id: str = "",
) -> dict[str, Any]:
    """Build a normalization error message for the errors topic.

    Args:
        raw_event: The original (invalid) event dict.
        event_type: One of ``"order"``, ``"execution"``, ``"transaction"``.
        errors: List of ValidationError objects.
        tenant_id: Extracted tenant_id if available.
    """
    return {
        "event_type": event_type,
        "tenant_id": tenant_id or raw_event.get("tenant_id", ""),
        "raw_data": raw_event,
        "errors": [
            {"field": e.field, "message": e.message, "event_index": e.event_index}
            for e in errors
        ],
        "created_at": _now_iso(),
    }

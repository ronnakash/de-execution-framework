"""Batch ETL transformation logic. Pure functions with no infrastructure dependencies."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone


def normalize_event_type(event_type: str) -> str:
    """Lowercase and strip whitespace from event type."""
    return event_type.strip().lower()


def ensure_payload_dict(payload: object) -> dict:
    """If payload is a JSON string, parse it. Otherwise return as-is."""
    if isinstance(payload, str):
        return json.loads(payload)
    if isinstance(payload, dict):
        return payload
    raise ValueError(f"Cannot convert payload of type {type(payload).__name__} to dict")


def compute_dedup_key(event: dict) -> str:
    """Compute a deterministic hash key for deduplication.

    Key is based on (event_type, serialized payload, event_date).
    """
    canonical = json.dumps(
        {
            "event_type": event.get("event_type", ""),
            "payload": event.get("payload", {}),
            "event_date": event.get("event_date", ""),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def transform_events(events: list[dict], date: str) -> list[dict]:
    """Transform validated events into the cleaned format.

    Adds: normalized event_type, parsed payload, event_date, created_at.
    """
    now = datetime.now(timezone.utc).isoformat()
    transformed = []
    for event in events:
        transformed.append({
            "event_type": normalize_event_type(event["event_type"]),
            "payload": ensure_payload_dict(event["payload"]),
            "source": event["source"],
            "event_date": date,
            "created_at": now,
        })
    return transformed

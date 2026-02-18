"""Event normalizer transformer: validates and normalizes raw event dicts."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from de_platform.services.logger.factory import LoggerFactory
from de_platform.shared.etl.interfaces import Transformer

_REQUIRED_FIELDS = ("event_type", "payload", "source")


class EventNormalizerTransformer(Transformer):
    """Validates and normalizes raw event dicts into the cleaned_events schema.

    Drops invalid records (missing required fields) and attaches:
    - normalized ``event_type`` (lowercase, stripped)
    - parsed ``payload`` (JSON string → dict)
    - ``event_date`` from params (injected via ``set_params``)
    - ``created_at`` timestamp
    - ``_dedup_key`` SHA-256 hash for idempotent writes

    The ``params`` dict should include ``date`` (YYYY-MM-DD).
    """

    processing_method = "inline"
    workers = 1

    def __init__(self, logger: LoggerFactory) -> None:
        self.log = logger.create()
        self.params: dict[str, Any] = {}

    def transform(self, item: Any) -> Any:
        if not isinstance(item, dict):
            self.log.warn("Skipping non-dict item", type=type(item).__name__)
            return None  # signals caller to drop this item

        for field in _REQUIRED_FIELDS:
            if not item.get(field):
                self.log.warn("Dropping invalid event", missing=field)
                return None

        date = self.params.get("date", "")
        event_type = item["event_type"].strip().lower()

        payload = item["payload"]
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                self.log.warn("Dropping event with unparsable payload")
                return None

        canonical = json.dumps(
            {"event_type": event_type, "payload": payload, "event_date": date},
            sort_keys=True,
            separators=(",", ":"),
        )
        dedup_key = hashlib.sha256(canonical.encode()).hexdigest()
        now = datetime.now(timezone.utc).isoformat()

        return {
            "event_type": event_type,
            "payload": payload,
            "source": item["source"],
            "event_date": date,
            "created_at": now,
            "_dedup_key": dedup_key,
        }

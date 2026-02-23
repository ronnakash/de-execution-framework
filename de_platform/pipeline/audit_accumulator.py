"""In-memory audit count accumulator with periodic Kafka publishing."""

from __future__ import annotations

import time
from datetime import datetime, timezone

from de_platform.pipeline.topics import AUDIT_COUNTS
from de_platform.services.message_queue.interface import MessageQueueInterface


class AuditAccumulator:
    """Accumulates event counts per (tenant_id, event_type) and publishes
    batched counts to the audit_counts Kafka topic periodically.

    Usage in a starter module::

        self._audit = AuditAccumulator(self.mq, source="rest")

        # After publishing each valid event:
        self._audit.count(tenant_id, event_type)

        # In the main loop:
        self._audit.maybe_flush()

        # On shutdown:
        self._audit.flush()
    """

    def __init__(
        self,
        mq: MessageQueueInterface,
        source: str,
        flush_interval: float = 5.0,
    ) -> None:
        self._mq = mq
        self._source = source
        self._flush_interval = flush_interval
        self._counts: dict[tuple[str, str], int] = {}  # (tenant_id, event_type) -> count
        self._last_flush = time.monotonic()

    def count(self, tenant_id: str, event_type: str, n: int = 1) -> None:
        """Increment the counter for a (tenant_id, event_type) pair."""
        key = (tenant_id, event_type)
        self._counts[key] = self._counts.get(key, 0) + n

    def maybe_flush(self) -> None:
        """Flush if the flush interval has elapsed."""
        if time.monotonic() - self._last_flush >= self._flush_interval:
            self.flush()

    def flush(self) -> None:
        """Publish all accumulated counts to the audit_counts topic and reset."""
        if not self._counts:
            self._last_flush = time.monotonic()
            return

        snapshot = dict(self._counts)
        self._counts.clear()
        self._last_flush = time.monotonic()

        now = datetime.now(timezone.utc).isoformat()
        for (tenant_id, event_type), received in snapshot.items():
            self._mq.publish(AUDIT_COUNTS, {
                "tenant_id": tenant_id,
                "source": self._source,
                "event_type": event_type,
                "received": received,
                "timestamp": now,
            })

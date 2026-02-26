"""Kafka topic name constants for the fraud detection pipeline."""

from __future__ import annotations

# ── Inbound normalization topics (producers: starters) ────────────────────────
TRADE_NORMALIZATION = "trade_normalization"       # orders + executions
TX_NORMALIZATION = "tx_normalization"             # transactions

# ── Error / anomaly topics ─────────────────────────────────────────────────────
NORMALIZATION_ERRORS = "normalization_errors"     # validation failures
DUPLICATES = "duplicates"                         # external duplicate events

# ── Persistence topics (producer: Normalizer) ─────────────────────────────────
ORDERS_PERSISTENCE = "orders_persistence"
EXECUTIONS_PERSISTENCE = "executions_persistence"
TRANSACTIONS_PERSISTENCE = "transactions_persistence"

# ── Algos topics (producer: Normalizer) ───────────────────────────────────────
TRADES_ALGOS = "trades_algos"         # orders + executions
TRANSACTIONS_ALGOS = "transactions_algos"

# ── Downstream topics ─────────────────────────────────────────────────────────
ALERTS = "alerts"                     # producer: Algos, consumer: Data API

# ── Audit topics ─────────────────────────────────────────────────────────────
AUDIT_COUNTS = "audit_counts"                # per-source ingestion counts
AUDIT_FILE_UPLOADS = "audit_file_uploads"    # file upload records


# ── Partition count metadata per topic ───────────────────────────────────────
# High-throughput topics: 12 partitions
# Medium topics: 6 partitions
# Low topics: 3 partitions
TOPIC_PARTITIONS: dict[str, int] = {
    # Inbound — high throughput
    TRADE_NORMALIZATION: 12,
    TX_NORMALIZATION: 12,
    # Persistence — high throughput
    ORDERS_PERSISTENCE: 12,
    EXECUTIONS_PERSISTENCE: 12,
    TRANSACTIONS_PERSISTENCE: 12,
    # Algos — medium throughput
    TRADES_ALGOS: 6,
    TRANSACTIONS_ALGOS: 6,
    ALERTS: 6,
    # Errors — medium throughput
    NORMALIZATION_ERRORS: 6,
    DUPLICATES: 6,
    # Audit — low throughput
    AUDIT_COUNTS: 3,
    AUDIT_FILE_UPLOADS: 3,
}

DEFAULT_PARTITIONS = 3


def get_partitions(topic: str) -> int:
    """Return the configured partition count for a topic."""
    return TOPIC_PARTITIONS.get(topic, DEFAULT_PARTITIONS)

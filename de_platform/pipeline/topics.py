"""Kafka topic name constants for the fraud detection pipeline."""

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

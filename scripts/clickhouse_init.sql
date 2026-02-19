-- ClickHouse tables for the fraud detection pipeline
-- Run against the fraud_pipeline database on startup.
-- Engine: MergeTree for efficient append and tenant-range scans.

CREATE TABLE IF NOT EXISTS orders
(
    id              String,
    tenant_id       String,
    status          String,
    transact_time   String,
    symbol          String,
    side            String,
    quantity        Float64,
    price           Float64,
    order_type      String,
    currency        String,
    message_id      String,
    notional        Float64,
    notional_usd    Float64,
    ingested_at     String,
    normalized_at   String,
    primary_key     String
)
ENGINE = MergeTree()
ORDER BY (tenant_id, primary_key);

CREATE TABLE IF NOT EXISTS executions
(
    id              String,
    tenant_id       String,
    status          String,
    transact_time   String,
    order_id        String,
    symbol          String,
    side            String,
    quantity        Float64,
    price           Float64,
    execution_venue String,
    currency        String,
    message_id      String,
    notional        Float64,
    notional_usd    Float64,
    ingested_at     String,
    normalized_at   String,
    primary_key     String
)
ENGINE = MergeTree()
ORDER BY (tenant_id, primary_key);

CREATE TABLE IF NOT EXISTS transactions
(
    id               String,
    tenant_id        String,
    status           String,
    transact_time    String,
    account_id       String,
    counterparty_id  String,
    amount           Float64,
    currency         String,
    transaction_type String,
    message_id       String,
    amount_usd       Float64,
    ingested_at      String,
    normalized_at    String,
    primary_key      String
)
ENGINE = MergeTree()
ORDER BY (tenant_id, primary_key);

CREATE TABLE IF NOT EXISTS duplicates
(
    event_type  String,
    primary_key String,
    message_id  String,
    tenant_id   String,
    received_at String
)
ENGINE = MergeTree()
ORDER BY (tenant_id, primary_key);

CREATE TABLE IF NOT EXISTS normalization_errors
(
    event_type    String,
    tenant_id     String,
    error_field   String,
    error_message String,
    raw_data      String,
    created_at    String
)
ENGINE = MergeTree()
ORDER BY (tenant_id, created_at);

CREATE TABLE IF NOT EXISTS alerts
(
    alert_id    String,
    algorithm   String,
    severity    String,
    tenant_id   String,
    event_id    String,
    event_type  String,
    details     String,
    created_at  String
)
ENGINE = MergeTree()
ORDER BY (tenant_id, created_at);

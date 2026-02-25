-- ClickHouse tables for the fraud detection pipeline
-- Run against the fraud_pipeline database on startup.
-- Engine: MergeTree for efficient append and tenant-range scans.

CREATE TABLE IF NOT EXISTS orders
(
    id              String,
    tenant_id       String,
    event_type      String,
    status          String,
    transact_time   String,
    symbol          String,
    side            String,
    quantity        Float64,
    price           Float64,
    order_type      String,
    currency        String,
    message_id      String,
    notional          Float64,
    notional_usd      Float64,
    ingested_at       String,
    normalized_at     String,
    primary_key       String,
    client_id         String DEFAULT '',
    ingestion_method  String DEFAULT '',
    additional_fields String DEFAULT '{}'
)
ENGINE = MergeTree()
ORDER BY (tenant_id, primary_key);

CREATE TABLE IF NOT EXISTS executions
(
    id                String,
    tenant_id         String,
    event_type        String,
    status            String,
    transact_time     String,
    order_id          String,
    symbol            String,
    side              String,
    quantity          Float64,
    price             Float64,
    execution_venue   String,
    currency          String,
    message_id        String,
    notional          Float64,
    notional_usd      Float64,
    ingested_at       String,
    normalized_at     String,
    primary_key       String,
    client_id         String DEFAULT '',
    ingestion_method  String DEFAULT '',
    additional_fields String DEFAULT '{}'
)
ENGINE = MergeTree()
ORDER BY (tenant_id, primary_key);

CREATE TABLE IF NOT EXISTS transactions
(
    id                String,
    tenant_id         String,
    event_type        String,
    status            String,
    transact_time     String,
    account_id        String,
    counterparty_id   String,
    amount            Float64,
    currency          String,
    transaction_type  String,
    message_id        String,
    amount_usd        Float64,
    ingested_at       String,
    normalized_at     String,
    primary_key       String,
    client_id         String DEFAULT '',
    ingestion_method  String DEFAULT '',
    additional_fields String DEFAULT '{}'
)
ENGINE = MergeTree()
ORDER BY (tenant_id, primary_key);

CREATE TABLE IF NOT EXISTS duplicates
(
    event_type     String,
    primary_key    String,
    message_id     String,
    tenant_id      String,
    received_at    String,
    original_event String,
    client_id      String DEFAULT ''
)
ENGINE = MergeTree()
ORDER BY (tenant_id, primary_key);

CREATE TABLE IF NOT EXISTS normalization_errors
(
    event_type    String,
    tenant_id     String,
    errors        String,
    raw_data      String,
    created_at    String,
    client_id     String DEFAULT ''
)
ENGINE = MergeTree()
ORDER BY (tenant_id, created_at);

CREATE TABLE IF NOT EXISTS alerts
(
    alert_id    String,
    tenant_id   String,
    event_type  String,
    event_id    String,
    message_id  String,
    algorithm   String,
    severity    String,
    description String,
    details     String,
    created_at  String
)
ENGINE = MergeTree()
ORDER BY (tenant_id, created_at);

-- ── Schema migrations (idempotent ALTER for pre-existing tables) ────────────
-- Phase 3 added client_id, ingestion_method, additional_fields columns.
-- ALTER TABLE ADD COLUMN IF NOT EXISTS is safe to run on tables that already
-- have these columns (no-op) or were created before Phase 3 (adds them).

ALTER TABLE orders ADD COLUMN IF NOT EXISTS client_id String DEFAULT '';
ALTER TABLE orders ADD COLUMN IF NOT EXISTS ingestion_method String DEFAULT '';
ALTER TABLE orders ADD COLUMN IF NOT EXISTS additional_fields String DEFAULT '{}';

ALTER TABLE executions ADD COLUMN IF NOT EXISTS client_id String DEFAULT '';
ALTER TABLE executions ADD COLUMN IF NOT EXISTS ingestion_method String DEFAULT '';
ALTER TABLE executions ADD COLUMN IF NOT EXISTS additional_fields String DEFAULT '{}';

ALTER TABLE transactions ADD COLUMN IF NOT EXISTS client_id String DEFAULT '';
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS ingestion_method String DEFAULT '';
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS additional_fields String DEFAULT '{}';

ALTER TABLE duplicates ADD COLUMN IF NOT EXISTS client_id String DEFAULT '';

ALTER TABLE normalization_errors ADD COLUMN IF NOT EXISTS client_id String DEFAULT '';

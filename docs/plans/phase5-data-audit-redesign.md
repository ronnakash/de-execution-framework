# Phase 5: Data Audit Redesign

## Overview

Complete redesign of the data audit service. Currently it listens to all pipeline Kafka topics to count messages, which is inefficient and produces incorrect sums (bugs.md #8). Replace with a publisher-side counting model where ingestion services report their own counts.

**Source:** bugs.md #1, #8; changes.md #9; changes.md Data Audit Redesign spec (items 1-12)

## Current Architecture (Being Replaced)

```
kafka_starter ──────┐
rest_starter ───────┤
normalizer ─────────┤──► all pipeline topics ──► data_audit (consumes everything)
persistence ────────┤                            counts messages per topic
algos ──────────────┘                            flushes to postgres
```

**Problems:**
- Listening to all topics is wasteful and fragile
- Message counts from pipeline-internal topics don't represent "received" vs "processed"
- Sums don't add up (bugs.md #8)
- Can't distinguish ingestion method (file vs rest vs kafka)

## New Architecture

```
kafka_starter ──► audit_counts topic ──┐
rest_starter  ──► audit_counts topic ──┤──► data_audit service ──► postgres
file_processor ─► audit_file_uploads   │    (just stores + serves)
                                       │
                   ClickHouse ─────────┤──► audit_calculator job ──► postgres
                   (errors, dupes)     │    (scheduled every 15min)
                                       │
                   postgres ◄──────────┘
                   (audit results)
```

### Design Spec (from changes.md)

1. Data audit listens to 2 topics: `audit_counts` (incoming counts) and `audit_file_uploads` (file uploads)
2. Incoming counts published by REST and Kafka starter services, accumulated and published periodically, partitioned by message type and tenant
3. File upload data contains incoming file event count, file name, tenant, etc.
4. Processed errors and duplicate count obtained by querying ClickHouse
5. A separate job module calculates audit summaries
6. Scheduler runs calculation job every 15 minutes
7. Timestamp high watermark prevents recalculating the same data
8. Calculation results stored in Postgres
9. Service queries just read from Postgres (no live computation)
10. Service has a `calculate` endpoint to trigger a calculation batch job
11. Events contain `ingestion_method` field (file, rest, kafka) — depends on Phase 3
12. Separate calculations for files vs realtime methods (kafka, rest)

## Implementation Plan

### 5.1 Audit Topics

**`audit_counts` topic** — published by kafka_starter and rest_starter:
```json
{
  "tenant_id": "acme",
  "event_type": "order",
  "ingestion_method": "kafka",
  "count": 42,
  "window_start": "2026-01-15T10:00:00Z",
  "window_end": "2026-01-15T10:00:05Z"
}
```

Publishers accumulate counts in-memory and flush every 5 seconds (configurable). The `AuditAccumulator` helper already exists in `de_platform/pipeline/audit_accumulator.py` — kafka_starter uses it. Extend to rest_starter.

**`audit_file_uploads` topic** — published by file_processor:
```json
{
  "tenant_id": "acme",
  "event_type": "execution",
  "ingestion_method": "file",
  "file_name": "executions_2026-01-15.jsonl",
  "event_count": 1000,
  "uploaded_at": "2026-01-15T10:05:00Z"
}
```

### 5.2 Data Audit Service (simplified)

The service no longer consumes pipeline topics. It:
1. Consumes `audit_counts` and `audit_file_uploads` topics
2. Accumulates received counts in Postgres `audit_received` table
3. Serves REST API reading from Postgres (no live computation)
4. Has a `/api/v1/audit/calculate` POST endpoint that triggers a calculation job

**New Postgres tables:**

```sql
-- Raw received counts (from audit_counts topic)
CREATE TABLE audit_received (
    id SERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    ingestion_method TEXT NOT NULL,
    count INTEGER NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- File upload records (from audit_file_uploads topic)
CREATE TABLE audit_file_uploads (
    id SERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    file_name TEXT NOT NULL,
    event_count INTEGER NOT NULL,
    uploaded_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Calculated audit summaries (written by audit_calculator job)
CREATE TABLE audit_daily (
    id SERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    date DATE NOT NULL,
    event_type TEXT NOT NULL,
    ingestion_method TEXT NOT NULL,
    received_count INTEGER DEFAULT 0,
    processed_count INTEGER DEFAULT 0,
    error_count INTEGER DEFAULT 0,
    duplicate_count INTEGER DEFAULT 0,
    calculated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (tenant_id, date, event_type, ingestion_method)
);

-- High watermark for incremental calculation
CREATE TABLE audit_watermarks (
    tenant_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    high_watermark TIMESTAMP NOT NULL,
    PRIMARY KEY (tenant_id, metric)
);
```

### 5.3 Audit Calculator Job (new module)

**Module:** `de_platform/modules/audit_calculator/` (type: `job`)

Triggered by scheduler every 15 minutes, or manually via the data_audit `/calculate` endpoint.

**Logic:**
1. Read high watermark for each (tenant, metric) from `audit_watermarks`
2. Query ClickHouse for processed counts since watermark:
   - `SELECT count(*) FROM orders WHERE ingested_at > $watermark GROUP BY tenant_id, event_type`
   - Same for `executions`, `transactions`
3. Query ClickHouse for error/duplicate counts since watermark:
   - `SELECT count(*) FROM normalization_errors WHERE created_at > $watermark GROUP BY tenant_id, event_type`
   - `SELECT count(*) FROM duplicates WHERE received_at > $watermark GROUP BY tenant_id, event_type`
4. Aggregate received counts from `audit_received` since watermark
5. Upsert results into `audit_daily` table
6. Update high watermarks

**File vs realtime separation:** Calculations are done per `ingestion_method`, so file and realtime (kafka/rest) results are separate rows in `audit_daily`.

### 5.4 Scheduler Integration

Add a default task definition for audit calculation:
```json
{
  "name": "audit_calculate",
  "module": "audit_calculator",
  "schedule_cron": "*/15 * * * *",
  "args": {}
}
```

### 5.5 REST API Changes

Keep existing endpoints but backed by Postgres reads:

```
GET /api/v1/audit/summary?tenant_id=X          — aggregated totals from audit_daily
GET /api/v1/audit/daily?tenant_id=X&date=Y      — daily breakdown by event_type and method
POST /api/v1/audit/calculate                     — trigger audit_calculator job
POST /api/v1/query/audit/daily                   — query with filters/sort/pagination
```

## Files to Create

| File | Purpose |
|------|---------|
| `de_platform/modules/audit_calculator/__init__.py` | Package marker |
| `de_platform/modules/audit_calculator/main.py` | AuditCalculatorModule (job) |
| `de_platform/modules/audit_calculator/module.json` | Module descriptor |
| `de_platform/modules/audit_calculator/tests/test_main.py` | Unit tests |
| `de_platform/migrations/data_audit/002_redesign.up.sql` | New tables |

## Files to Modify

| File | Changes |
|------|---------|
| `de_platform/modules/data_audit/main.py` | Remove pipeline topic consumption; consume only audit topics; simplify to Postgres reads |
| `de_platform/modules/rest_starter/main.py` | Add AuditAccumulator to publish counts |
| `de_platform/modules/file_processor/main.py` | Publish to `audit_file_uploads` topic |
| `de_platform/pipeline/topics.py` | Add `AUDIT_FILE_UPLOADS` topic constant |
| `tests/helpers/harness.py` | Include audit_calculator in SharedPipeline |

## Dependencies

- Phase 3 (`ingestion_method` field on events) should be done first so ClickHouse queries can group by ingestion method

## Acceptance Criteria

1. Data audit service no longer listens to pipeline-internal topics
2. kafka_starter and rest_starter publish audit counts to `audit_counts` topic
3. file_processor publishes to `audit_file_uploads` topic
4. Audit calculator job correctly queries ClickHouse for processed/error/duplicate counts
5. High watermark prevents duplicate calculations
6. `/api/v1/audit/summary` returns correct totals from Postgres
7. Scheduler runs audit calculation every 15 minutes
8. Manual calculation trigger works via REST API
9. File and realtime audit results are separated

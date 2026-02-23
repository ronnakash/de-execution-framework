# Phase 13: Data Audit Redesign

**Goal:** Per-source, per-file audit tracking with Kafka-based count accumulation and UI drill-down.

## Context

Current issues with data audit (from Phase 10 bug analysis):

1. **Flush upsert fetches ALL rows from `daily_audit` table on every flush** -- the `_flush_counters()` method in `DataAuditModule` calls `self.db.fetch_all_async("SELECT * FROM daily_audit")` once per counter key per flush cycle, scanning the entire table to find a matching row. This is O(N*M) where N is the number of counter keys being flushed and M is the total number of rows in the table. Under production load this becomes progressively slower and introduces race conditions when multiple flush cycles overlap.

2. **No `source` column** -- the `daily_audit` table schema (defined in `de_platform/migrations/data_audit/001_create_audit_tables.up.sql`) has a UNIQUE constraint on `(tenant_id, date, event_type)` but does not distinguish between events ingested via REST (`rest_starter`), Kafka (`kafka_starter`), or file upload (`file_processor`). This makes it impossible to answer "how many events came from each ingestion method?"

3. **File processing not tracked at per-file granularity** -- the `file_audit` table exists in the schema but `FileProcessorModule` (at `de_platform/modules/file_processor/main.py`) does not write any rows to it. The file processor publishes events to normalization topics and logs completion, but there is no database tracking of individual file processing runs.

## Design

### Audit Counts Topic

Starters (`rest_starter`, `kafka_starter`, `file_processor`) accumulate counts in-memory for a configurable period (default 5 seconds), then publish a single audit count message to a new `audit_counts` Kafka topic:

```python
{
    "tenant_id": "acme",
    "source": "rest",           # "rest" | "kafka" | "file"
    "event_type": "order",
    "received": 47,
    "timestamp": "2026-01-15T10:00:05Z"
}
```

This decouples counting from the raw event topics -- `data_audit` no longer needs to consume from `TRADE_NORMALIZATION` and `TX_NORMALIZATION` to count received events. Instead, the starters (which know their source type) publish pre-aggregated counts, which reduces message volume on the audit path and provides source attribution at the point of ingestion.

### Data Audit Service Changes

The redesigned `data_audit` service:

1. **Consumes from `audit_counts` topic** for **received** counts (instead of counting individual messages from normalization topics)
2. **Continues consuming** persistence/error/duplicate topics for **processed/error/duplicate** counts (these downstream topics remain unchanged)
3. **Tracks `source` dimension** in `daily_audit` table via a new column with updated UNIQUE constraint
4. **Uses per-key fetch** instead of fetch-all -- the flush logic queries for a specific `(tenant_id, date, event_type, source)` row rather than scanning the entire table

### Architecture Diagram

```
rest_starter ──┐
               ├──> audit_counts topic ──> data_audit service ──> daily_audit table
kafka_starter ─┤                                   ^                (with source col)
               │                                   │
file_processor ┘                                   │
                                                   │
orders_persistence ────────────────────────────────┤
executions_persistence ────────────────────────────┤
transactions_persistence ──────────────────────────┤
normalization_errors ──────────────────────────────┤
duplicates ────────────────────────────────────────┘
```

---

## Implementation Details

### Step 1: New topic constant

**File:** `de_platform/pipeline/topics.py`

Add a new topic constant for the audit counts channel. This follows the existing pattern of grouping constants by purpose with section comments.

```python
# ── Audit topics ─────────────────────────────────────────────────────────────
AUDIT_COUNTS = "audit_counts"                # per-source ingestion counts
```

Place this after the existing `ALERTS` constant at the end of the file.

---

### Step 2: Add source column to daily_audit

The existing migration is `001_create_audit_tables.up.sql` which defines the `daily_audit` table with `UNIQUE (tenant_id, date, event_type)`. We need migration 002 to add the `source` column and update the constraint.

**New file:** `de_platform/migrations/data_audit/002_add_source.up.sql`

```sql
-- Add source column (default 'unknown' for existing rows)
ALTER TABLE daily_audit ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'unknown';

-- Drop old unique constraint and create new one including source
ALTER TABLE daily_audit DROP CONSTRAINT IF EXISTS daily_audit_tenant_id_date_event_type_key;
ALTER TABLE daily_audit ADD CONSTRAINT daily_audit_unique
    UNIQUE (tenant_id, date, event_type, source);

-- Index on source for filtered queries
CREATE INDEX IF NOT EXISTS idx_daily_audit_source ON daily_audit(source);
```

**New file:** `de_platform/migrations/data_audit/002_add_source.down.sql`

```sql
DROP INDEX IF EXISTS idx_daily_audit_source;
ALTER TABLE daily_audit DROP CONSTRAINT IF EXISTS daily_audit_unique;
ALTER TABLE daily_audit DROP COLUMN IF EXISTS source;
ALTER TABLE daily_audit ADD CONSTRAINT daily_audit_tenant_id_date_event_type_key
    UNIQUE (tenant_id, date, event_type);
```

**Note:** The constraint name `daily_audit_tenant_id_date_event_type_key` is the auto-generated PostgreSQL name for the original `UNIQUE (tenant_id, date, event_type)` clause in migration 001.

---

### Step 3: Shared audit accumulator utility

Create a reusable accumulator that all three starters will use. This avoids duplicating the accumulation + flush logic across modules.

**New file:** `de_platform/pipeline/audit_accumulator.py`

```python
"""In-memory audit count accumulator with periodic Kafka publishing."""

from __future__ import annotations

import time
from datetime import datetime, timezone

from de_platform.services.message_queue.interface import MessageQueueInterface
from de_platform.pipeline.topics import AUDIT_COUNTS


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
```

**Design decisions:**

- Uses `time.monotonic()` instead of wall-clock time to avoid issues with clock adjustments.
- The `flush()` method snapshots and clears `_counts` atomically (single-threaded context) before publishing, so no counts are lost if publishing is slow.
- The `n` parameter in `count()` supports bulk counting (e.g., file processor counting all events in a file at once).
- No async needed -- `MessageQueueInterface.publish()` is synchronous.

---

### Step 4: Integrate accumulator into rest_starter

**File:** `de_platform/modules/rest_starter/main.py`

Changes:

1. **Import** `AuditAccumulator` from `de_platform.pipeline.audit_accumulator`.

2. **Create accumulator** in `initialize()`:
   ```python
   async def initialize(self) -> None:
       self.log = self.logger.create()
       self.port = self.config.get("port", 8001)
       self._audit = AuditAccumulator(self.mq, source="rest")
   ```

3. **Count events** in `_handle_ingest()` after publishing valid events. Add after the valid-event publishing loop (around line 142, after the `self.mq.publish(topic, msg, key=msg_key)` loop):
   ```python
   # Count valid events for audit
   if valid:
       # Group by tenant_id since a single batch can contain events from multiple tenants
       tenant_counts: dict[str, int] = {}
       for raw in valid:
           tid = raw.get("tenant_id", "unknown")
           tenant_counts[tid] = tenant_counts.get(tid, 0) + 1
       for tid, count in tenant_counts.items():
           self._audit.count(tid, event_type, count)
       self._audit.maybe_flush()
   ```

4. **Register shutdown flush** in `execute()`, before the main loop:
   ```python
   self.lifecycle.on_shutdown(self._flush_audit)
   ```
   And add the callback:
   ```python
   async def _flush_audit(self) -> None:
       self._audit.flush()
   ```

**Note:** `rest_starter` currently has no `_audit` attribute. The accumulator is lightweight (just a dict) and adds negligible overhead to the request path. The `maybe_flush()` call inside the request handler means flush happens at most once every 5 seconds, not on every request.

---

### Step 5: Integrate accumulator into kafka_starter

**File:** `de_platform/modules/kafka_starter/main.py`

Changes:

1. **Import** `AuditAccumulator`.

2. **Create accumulator** in `initialize()`:
   ```python
   self._audit = AuditAccumulator(self.mq, source="kafka")
   ```

3. **Count events** in `_process_message()` after the valid-event publishing loop:
   ```python
   if valid:
       for validated in valid:
           tenant_id = validated.get("tenant_id", "unknown")
           self._audit.count(tenant_id, event_type)
   ```

4. **Call `maybe_flush()`** in the main `execute()` loop. Add after the inner for-loop over `self._routes.items()`:
   ```python
   self._audit.maybe_flush()
   ```

5. **Flush on shutdown** -- `kafka_starter` currently uses `self.lifecycle.is_shutting_down` to break the loop. Add a flush call right after the loop exits:
   ```python
   self._audit.flush()
   ```

---

### Step 6: Integrate accumulator into file_processor

**File:** `de_platform/modules/file_processor/main.py`

Changes:

1. **Import** `AuditAccumulator`.

2. **Add `DatabaseInterface` as a constructor dependency** (file_processor currently does not have a DB connection -- it needs one to write `file_audit` rows). Add `db: DatabaseInterface` to `__init__` and update `module.json` if needed.

3. **Create accumulator** in `initialize()`:
   ```python
   self._audit = AuditAccumulator(self.mq, source="file")
   ```

4. **Count and flush** in `execute()` after the valid-event publishing loop:
   ```python
   # Group by tenant for audit counting
   tenant_counts: dict[str, int] = {}
   for raw in valid:
       tid = raw.get("tenant_id", "unknown")
       tenant_counts[tid] = tenant_counts.get(tid, 0) + 1
   for tid, count in tenant_counts.items():
       self._audit.count(tid, self.event_type, count)
   self._audit.flush()  # Immediate flush since file_processor is a job, not a service
   ```

5. **Write `file_audit` records** -- the `file_audit` table already exists in the schema but is never populated. Add tracking:

   At the start of `execute()`, before reading the file:
   ```python
   file_id = uuid.uuid4().hex
   await self.db.insert_one_async("file_audit", {
       "file_id": file_id,
       "tenant_id": "unknown",  # Will be updated after parsing
       "file_name": self.file_path,
       "event_type": self.event_type,
       "total_count": 0,
       "processed_count": 0,
       "error_count": 0,
       "duplicate_count": 0,
       "status": "processing",
       "created_at": datetime.utcnow(),
       "updated_at": datetime.utcnow(),
   })
   ```

   After processing completes, update the row. Since `MemoryDatabase` does not support `UPDATE` SQL, use the delete + re-insert pattern (consistent with existing `_flush_counters` in data_audit):
   ```python
   await self.db.execute_async("DELETE FROM file_audit WHERE file_id = $1", [file_id])
   # Determine tenant_id from the events (use first event's tenant_id or "mixed")
   tenant_ids = {e.get("tenant_id", "unknown") for e in events}
   file_tenant = tenant_ids.pop() if len(tenant_ids) == 1 else "mixed"
   await self.db.insert_one_async("file_audit", {
       "file_id": file_id,
       "tenant_id": file_tenant,
       "file_name": self.file_path,
       "event_type": self.event_type,
       "total_count": len(events),
       "processed_count": len(valid),
       "error_count": len(errors),
       "duplicate_count": 0,  # Duplicates are detected downstream by normalizer
       "status": "completed",
       "created_at": datetime.utcnow(),
       "updated_at": datetime.utcnow(),
   })
   ```

   On error, wrap the execute body in try/except and write a `status="failed"` row.

**Note on file_processor module type:** `file_processor` is a `job` module (runs once and exits). It does not have a `LifecycleManager`. Since the accumulator flush is called once at the end of `execute()`, there is no need for a shutdown hook.

**Note on DatabaseInterface dependency:** Adding `db: DatabaseInterface` to `FileProcessorModule.__init__` means the DI container will inject a database connection. The module's `module.json` args schema and the `module_specs` in `SharedPipeline._start_subprocesses()` will need a `--db` flag. For unit tests, `MemoryDatabase` will be injected directly.

---

### Step 7: Redesign data_audit consumer

**File:** `de_platform/modules/data_audit/main.py`

This is the largest change. The module currently consumes from 7 topics (2 normalization + 3 persistence + errors + duplicates) and uses a 3-tuple counter key `(tenant_id, date_str, event_type)`.

#### 7a. Update imports

Replace the `TRADE_NORMALIZATION` and `TX_NORMALIZATION` imports with `AUDIT_COUNTS`:

```python
from de_platform.pipeline.topics import (
    AUDIT_COUNTS,         # NEW
    DUPLICATES,
    EXECUTIONS_PERSISTENCE,
    NORMALIZATION_ERRORS,
    ORDERS_PERSISTENCE,
    TRANSACTIONS_PERSISTENCE,
    # TRADE_NORMALIZATION removed
    # TX_NORMALIZATION removed
)
```

#### 7b. Update topic mapping

Remove the two normalization topic entries from `_TOPIC_MAPPING`. These are now handled via the `audit_counts` topic:

```python
_TOPIC_MAPPING: list[tuple[str, str, str | None]] = [
    # Received counts now come from AUDIT_COUNTS topic (Step 7c)
    (ORDERS_PERSISTENCE, _PROCESSED, "order"),
    (EXECUTIONS_PERSISTENCE, _PROCESSED, "execution"),
    (TRANSACTIONS_PERSISTENCE, _PROCESSED, "transaction"),
    (NORMALIZATION_ERRORS, _ERROR, None),
    (DUPLICATES, _DUPLICATE, None),
]
```

#### 7c. Update counter key to 4-tuple

Change `_counters` type from `dict[tuple[str, str, str], dict[str, int]]` to `dict[tuple[str, str, str, str], dict[str, int]]`:

```python
# In __init__:
self._counters: dict[tuple[str, str, str, str], dict[str, int]] = {}
# Key: (tenant_id, date_str, event_type, source)
```

#### 7d. Add audit_counts consumer method

```python
def _consume_audit_counts(self) -> None:
    """Consume from audit_counts topic for received counts."""
    msg = self.mq.consume_one(AUDIT_COUNTS)
    if msg:
        tenant_id = msg.get("tenant_id", "unknown")
        source = msg.get("source", "unknown")
        event_type = msg.get("event_type", "unknown")
        received = msg.get("received", 0)
        date_str = _extract_date(msg)

        key = (tenant_id, date_str, event_type, source)
        if key not in self._counters:
            self._counters[key] = {
                _RECEIVED: 0, _PROCESSED: 0, _ERROR: 0, _DUPLICATE: 0,
            }
        self._counters[key][_RECEIVED] += received
        self._msg_count_since_flush += 1

        self.metrics.counter("audit_events_counted_total", tags={
            "service": "data_audit", "tenant_id": tenant_id,
            "event_type": event_type, "counter": _RECEIVED,
            "source": source,
        })
```

**Note on `_extract_date`:** The `audit_counts` message uses a `timestamp` field (not `transact_time`). Update `_extract_date` to also check `timestamp`:

```python
def _extract_date(msg: dict) -> str:
    """Extract date string (YYYY-MM-DD) from message transact_time/timestamp or today."""
    for field in ("transact_time", "timestamp"):
        tt = msg.get(field)
        if tt:
            try:
                if isinstance(tt, str):
                    dt = datetime.fromisoformat(tt)
                elif isinstance(tt, datetime):
                    dt = tt
                else:
                    continue
                return dt.date().isoformat()
            except (ValueError, TypeError):
                continue
    return date.today().isoformat()
```

#### 7e. Update `_count_message` for source dimension

For persistence/error/duplicate topics (which do not carry a `source` field), use `"unknown"` as the default source. This means downstream counts are not source-attributed, which is correct -- a processed event may have been ingested via any source, and the persistence topic does not carry that information.

```python
def _count_message(
    self, msg: dict, counter_field: str, default_event_type: str | None,
) -> None:
    tenant_id = msg.get("tenant_id", "unknown")
    event_type = default_event_type or msg.get("event_type", "unknown")
    date_str = _extract_date(msg)
    source = msg.get("source", "unknown")  # NEW

    key = (tenant_id, date_str, event_type, source)
    if key not in self._counters:
        self._counters[key] = {
            _RECEIVED: 0, _PROCESSED: 0, _ERROR: 0, _DUPLICATE: 0,
        }
    self._counters[key][counter_field] += 1
    self._msg_count_since_flush += 1

    self.metrics.counter("audit_events_counted_total", tags={
        "service": "data_audit", "tenant_id": tenant_id,
        "event_type": event_type, "counter": counter_field,
        "source": source,
    })
```

#### 7f. Update `_consume_all_topics` to include audit_counts

```python
def _consume_all_topics(self) -> None:
    """Poll one message from each topic per iteration."""
    self._consume_audit_counts()
    for topic, counter_field, default_event_type in _TOPIC_MAPPING:
        msg = self.mq.consume_one(topic)
        if msg:
            self._count_message(msg, counter_field, default_event_type)
```

#### 7g. Fix `_flush_counters` -- replace fetch-all with per-key lookup

The current implementation fetches ALL rows and scans them in Python. Replace with a per-key approach that is still compatible with `MemoryDatabase` (which only supports single-column WHERE queries).

Since `MemoryDatabase` cannot do multi-column WHERE (`WHERE tenant_id = $1 AND date = $2 AND event_type = $3 AND source = $4`), we must keep the fetch-all + filter pattern for MemoryDatabase compatibility. However, we improve it by:

1. Fetching the table **once per flush** (not once per counter key)
2. Building a lookup dict from the fetched rows
3. Processing all counter keys against that single snapshot

```python
async def _flush_counters(self) -> None:
    """Upsert accumulated counters into daily_audit table."""
    if not self._counters:
        return

    # Snapshot and reset
    snapshot = dict(self._counters)
    self._counters = {}
    self._msg_count_since_flush = 0
    self._last_flush_time = time.monotonic()

    # Fetch all existing rows ONCE (not per key)
    all_rows = await self.db.fetch_all_async("SELECT * FROM daily_audit")

    # Build lookup: (tenant_id, date_str, event_type, source) -> row
    existing_lookup: dict[tuple[str, str, str, str], dict] = {}
    for row in all_rows:
        row_date = row.get("date")
        if isinstance(row_date, date):
            row_date = row_date.isoformat()
        row_key = (
            row.get("tenant_id", ""),
            str(row_date),
            row.get("event_type", ""),
            row.get("source", "unknown"),
        )
        existing_lookup[row_key] = row

    for key, counts in snapshot.items():
        tenant_id, date_str, event_type, source = key
        existing = existing_lookup.get(key)

        if existing:
            merged = dict(existing)
            merged[_RECEIVED] = merged.get(_RECEIVED, 0) + counts[_RECEIVED]
            merged[_PROCESSED] = merged.get(_PROCESSED, 0) + counts[_PROCESSED]
            merged[_ERROR] = merged.get(_ERROR, 0) + counts[_ERROR]
            merged[_DUPLICATE] = merged.get(_DUPLICATE, 0) + counts[_DUPLICATE]
            merged["updated_at"] = datetime.utcnow()
            row_id = existing.get("id")
            await self.db.execute_async(
                "DELETE FROM daily_audit WHERE id = $1", [row_id],
            )
            await self.db.insert_one_async("daily_audit", merged)
        else:
            await self.db.insert_one_async("daily_audit", {
                "tenant_id": tenant_id,
                "date": date.fromisoformat(date_str),
                "event_type": event_type,
                "source": source,
                _RECEIVED: counts[_RECEIVED],
                _PROCESSED: counts[_PROCESSED],
                _ERROR: counts[_ERROR],
                _DUPLICATE: counts[_DUPLICATE],
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            })

    self.log.info("Counters flushed", module="data_audit", keys=len(snapshot))
```

This changes the flush from O(N*M) to O(N+M) where N = counter keys and M = existing rows.

#### 7h. Add source filtering to REST API

Update `_get_daily` to support a `source` query parameter:

```python
async def _get_daily(self, request: web.Request) -> web.Response:
    # ... existing code ...
    source = request.rel_url.query.get("source")

    rows = await self.db.fetch_all_async("SELECT * FROM daily_audit")

    if tenant_id:
        rows = [r for r in rows if r.get("tenant_id") == tenant_id]
    if source:
        rows = [r for r in rows if r.get("source") == source]
    # ... rest of existing filters ...
```

Update `_get_summary` to also include `by_source` breakdown:

```python
summary: dict[str, Any] = {
    "total_received": 0,
    "total_processed": 0,
    "total_errors": 0,
    "total_duplicates": 0,
    "by_event_type": {},
    "by_source": {},  # NEW
}

for row in rows:
    # ... existing by_event_type logic ...

    # NEW: by_source aggregation
    src = row.get("source", "unknown")
    if src not in summary["by_source"]:
        summary["by_source"][src] = {
            "received": 0, "processed": 0, "errors": 0, "duplicates": 0,
        }
    summary["by_source"][src]["received"] += row.get(_RECEIVED, 0)
    summary["by_source"][src]["processed"] += row.get(_PROCESSED, 0)
    summary["by_source"][src]["errors"] += row.get(_ERROR, 0)
    summary["by_source"][src]["duplicates"] += row.get(_DUPLICATE, 0)
```

---

### Step 8: Update harness for E2E tests

**File:** `tests/helpers/harness.py`

In `SharedPipeline._start_subprocesses()`, update the `_subscribe` dict for `data_audit` to replace normalization topics with the new audit_counts topic:

```python
# Before (current):
"data_audit": ",".join([
    TRADE_NORMALIZATION, TX_NORMALIZATION,
    ORDERS_PERSISTENCE, EXECUTIONS_PERSISTENCE,
    TRANSACTIONS_PERSISTENCE, NORMALIZATION_ERRORS, DUPLICATES,
]),

# After:
"data_audit": ",".join([
    AUDIT_COUNTS,
    ORDERS_PERSISTENCE, EXECUTIONS_PERSISTENCE,
    TRANSACTIONS_PERSISTENCE, NORMALIZATION_ERRORS, DUPLICATES,
]),
```

This requires importing `AUDIT_COUNTS` from `de_platform.pipeline.topics` in the `_start_subprocesses` method's local import block.

Also add `AUDIT_COUNTS` to the `_subscribe` dict for any starters that publish to it, if they use pre-subscription. Currently `rest_starter` and `kafka_starter` use `--mq kafka` but only subscribe to inbound topics. Since starters only **produce** to `audit_counts` (not consume), no subscription change is needed for them.

---

### Step 9: UI changes -- source column and filter

#### 9a. Update TypeScript API interface

**File:** `ui/src/api/audit.ts`

Add `source` to the `DailyAudit` interface:

```typescript
export interface DailyAudit {
  tenant_id: string;
  date: string;
  event_type: string;
  source: string;           // NEW
  received_count: number;
  processed_count: number;
  error_count: number;
  duplicate_count: number;
}
```

Update `DailyParams` to support source filtering:

```typescript
interface DailyParams {
  tenant_id?: string;
  date?: string;
  start_date?: string;
  end_date?: string;
  source?: string;          // NEW
}
```

Update `fetchDailyAudit` to pass the `source` param:

```typescript
export function fetchDailyAudit(params: DailyParams = {}) {
  const qs = new URLSearchParams();
  if (params.tenant_id) qs.set("tenant_id", params.tenant_id);
  if (params.date) qs.set("date", params.date);
  if (params.start_date) qs.set("start_date", params.start_date);
  if (params.end_date) qs.set("end_date", params.end_date);
  if (params.source) qs.set("source", params.source);  // NEW
  const query = qs.toString();
  return fetchApi<DailyAudit[]>(
    `/api/v1/audit/daily${query ? `?${query}` : ""}`,
  );
}
```

Update `AuditSummary` to include `by_source`:

```typescript
export interface AuditSummary {
  total_received: number;
  total_processed: number;
  total_errors: number;
  total_duplicates: number;
  by_event_type: Record<string, { received: number; processed: number; errors: number; duplicates: number }>;
  by_source: Record<string, { received: number; processed: number; errors: number; duplicates: number }>;  // NEW
}
```

#### 9b. Update DataAuditPage component

**File:** `ui/src/pages/DataAuditPage.tsx`

1. **Add source state variable:**
   ```typescript
   const [source, setSource] = useState("");
   ```

2. **Add source to query keys and fetch params:**
   ```typescript
   const dailyQuery = useQuery({
     queryKey: ["audit-daily", tenantId, startDate, endDate, source],
     queryFn: () =>
       fetchDailyAudit({
         tenant_id: tenantId || undefined,
         start_date: startDate || undefined,
         end_date: endDate || undefined,
         source: source || undefined,
       }),
   });
   ```

3. **Add Source column to the table** (insert after "Event Type"):
   ```typescript
   const columns = [
     { key: "date", header: "Date" },
     { key: "tenant_id", header: "Tenant" },
     { key: "event_type", header: "Event Type" },
     { key: "source", header: "Source" },   // NEW
     // ... existing count columns ...
   ];
   ```

4. **Add source filter dropdown** in the filter bar (after the Tenant ID input):
   ```tsx
   <div>
     <label className="block text-xs text-gray-500 mb-1">Source</label>
     <select
       value={source}
       onChange={(e) => setSource(e.target.value)}
       className="px-3 py-1.5 border border-gray-300 rounded text-sm"
     >
       <option value="">All sources</option>
       <option value="rest">REST</option>
       <option value="kafka">Kafka</option>
       <option value="file">File</option>
     </select>
   </div>
   ```

---

## Unit Tests

### New test file: `de_platform/pipeline/tests/test_audit_accumulator.py`

Test the `AuditAccumulator` class in isolation using `MemoryQueue`:

| Test | Description |
|------|-------------|
| `test_count_accumulates` | Multiple `count()` calls for the same `(tenant_id, event_type)` sum correctly |
| `test_flush_publishes_to_audit_counts_topic` | After `count()` + `flush()`, verify a message appears on `AUDIT_COUNTS` topic with correct `source`, `tenant_id`, `event_type`, and `received` fields |
| `test_flush_clears_counts` | After `flush()`, internal `_counts` dict is empty; a second `flush()` publishes nothing |
| `test_maybe_flush_respects_interval` | With `flush_interval=10.0`, calling `maybe_flush()` immediately after construction does not flush; after advancing time past the interval, it does |
| `test_multiple_tenants_and_event_types` | Counts for `(t1, order)` and `(t2, transaction)` produce separate messages on flush |
| `test_flush_no_op_when_empty` | Calling `flush()` with no prior `count()` calls publishes nothing |
| `test_count_with_n_parameter` | `count(tenant, event_type, n=5)` adds 5 in a single call |

### Updated test file: `de_platform/modules/data_audit/tests/test_main.py`

Existing tests need updates for the source dimension:

1. **Tests that publish to `TRADE_NORMALIZATION` / `TX_NORMALIZATION` for received counts** -- these must be rewritten to publish to `AUDIT_COUNTS` instead, since `data_audit` no longer consumes from normalization topics. Affected tests:
   - `test_received_count_from_trade_normalization` -- rename to `test_received_count_from_audit_counts` and publish an audit_counts message with `source="rest"`
   - `test_received_count_from_tx_normalization` -- rename to `test_received_count_from_audit_counts_transaction` and publish an audit_counts message with `source="kafka"`
   - `test_multi_tenant_isolation` -- update to use `AUDIT_COUNTS` for received counts
   - `test_counters_accumulate_across_flushes` -- update to use `AUDIT_COUNTS`
   - `test_flush_triggered_by_threshold` -- update to use `AUDIT_COUNTS`
   - `test_rest_get_daily` -- update to use `AUDIT_COUNTS`
   - `test_rest_get_daily_filtered_by_date` -- update to use `AUDIT_COUNTS`
   - `test_rest_get_summary` -- update to use `AUDIT_COUNTS` for received, keep `ORDERS_PERSISTENCE` for processed

2. **Tests that verify persistence/error/duplicate counts remain unchanged** -- these continue to publish to the same topics (`ORDERS_PERSISTENCE`, `EXECUTIONS_PERSISTENCE`, `TRANSACTIONS_PERSISTENCE`, `NORMALIZATION_ERRORS`, `DUPLICATES`). The counter key changes from 3-tuple to 4-tuple, but these tests don't directly inspect the key.

3. **New test cases to add:**
   - `test_source_column_in_daily_audit_row` -- verify the `source` field appears in flushed rows
   - `test_different_sources_create_separate_rows` -- publish audit_counts messages with `source="rest"` and `source="kafka"` for the same tenant/date/event_type; verify two separate rows
   - `test_rest_get_daily_filter_by_source` -- verify `?source=rest` filters correctly
   - `test_rest_get_summary_includes_by_source` -- verify the summary response contains `by_source` breakdown
   - `test_flush_single_fetch_all` -- verify that flush fetches the table only once (can mock/spy `fetch_all_async` to count calls)

### Updated starter tests

**File:** `de_platform/modules/rest_starter/tests/test_main.py`

Add tests verifying audit accumulator integration:
- `test_ingest_publishes_audit_count` -- after ingesting events via REST, verify that `AUDIT_COUNTS` topic has a message with `source="rest"` and correct count

**File:** `de_platform/modules/kafka_starter/tests/test_main.py`

Add tests verifying audit accumulator integration:
- `test_process_message_publishes_audit_count` -- after processing a message, verify `AUDIT_COUNTS` topic has a message with `source="kafka"`

**File:** `de_platform/modules/file_processor/tests/test_main.py`

Add tests verifying:
- `test_file_processing_publishes_audit_count` -- verify `AUDIT_COUNTS` topic message with `source="file"`
- `test_file_processing_creates_file_audit_record` -- verify `file_audit` row with correct counts and `status="completed"`
- `test_file_processing_error_creates_failed_record` -- verify `file_audit` row with `status="failed"` on error

---

## Files Changed Summary

| File | Change Type | Description |
|------|-------------|-------------|
| `de_platform/pipeline/topics.py` | Modified | Add `AUDIT_COUNTS` constant |
| `de_platform/pipeline/audit_accumulator.py` | **New** | Shared in-memory accumulator utility |
| `de_platform/migrations/data_audit/002_add_source.up.sql` | **New** | Add `source` column + updated UNIQUE constraint |
| `de_platform/migrations/data_audit/002_add_source.down.sql` | **New** | Reverse migration |
| `de_platform/modules/rest_starter/main.py` | Modified | Add audit accumulator integration |
| `de_platform/modules/kafka_starter/main.py` | Modified | Add audit accumulator integration |
| `de_platform/modules/file_processor/main.py` | Modified | Add audit accumulator + file_audit record writing + DB dependency |
| `de_platform/modules/file_processor/module.json` | Modified | Add `db` to args schema if needed |
| `de_platform/modules/data_audit/main.py` | Modified | Replace normalization topic consumption with audit_counts; add source to counter key; fix flush to single fetch-all; add source filtering to REST |
| `de_platform/pipeline/tests/test_audit_accumulator.py` | **New** | Unit tests for AuditAccumulator |
| `de_platform/modules/data_audit/tests/test_main.py` | Modified | Update tests for source dimension + audit_counts topic |
| `de_platform/modules/rest_starter/tests/test_main.py` | Modified | Add audit accumulator tests |
| `de_platform/modules/kafka_starter/tests/test_main.py` | Modified | Add audit accumulator tests |
| `de_platform/modules/file_processor/tests/test_main.py` | Modified | Add audit accumulator + file_audit tests |
| `tests/helpers/harness.py` | Modified | Update data_audit topic subscription |
| `ui/src/api/audit.ts` | Modified | Add `source` to interfaces + fetch params |
| `ui/src/pages/DataAuditPage.tsx` | Modified | Add source column + filter dropdown |

---

## Implementation Order

The steps should be implemented in this order due to dependencies:

1. **Step 1** (topic constant) -- no dependencies, needed by everything else
2. **Step 2** (migration) -- no code dependencies, can be done in parallel with Step 1
3. **Step 3** (accumulator utility) -- depends on Step 1
4. **Steps 4-6** (starter integrations) -- depend on Step 3, can be done in parallel with each other
5. **Step 7** (data_audit redesign) -- depends on Steps 1-2
6. **Step 8** (harness update) -- depends on Step 1
7. **Step 9** (UI changes) -- depends on Step 7 (needs backend source field)
8. **Unit tests** -- after the code they test is written

---

## Verification

1. `make test` -- all unit tests pass (including updated data_audit tests and new accumulator tests)
2. `make lint` -- no ruff lint errors
3. Run migration: `make migrate cmd=up args="data_audit --db postgres"`
4. `make migrate cmd=status args="data_audit --db postgres"` -- verify 002 shows as applied
5. Manual verification with infrastructure:
   - `make infra-up`
   - Start pipeline modules
   - Ingest events via REST: `curl -X POST http://localhost:8001/api/v1/orders -d '{"events": [...]}'`
   - Ingest events via Kafka starter (publish to `client_orders` topic)
   - Query audit: `curl http://localhost:8005/api/v1/audit/daily` -- verify `source` column populated with `"rest"` and `"kafka"` respectively
   - Query with filter: `curl http://localhost:8005/api/v1/audit/daily?source=rest` -- verify only REST-sourced rows returned
   - Query summary: `curl http://localhost:8005/api/v1/audit/summary` -- verify `by_source` breakdown present
6. `make test-integration` -- integration tests pass (if applicable)
7. `make test-e2e` -- E2E tests pass with updated harness subscription

## Rollback

If migration 002 needs to be rolled back:
```bash
make migrate cmd=down args="data_audit --count 1 --db postgres"
```

This drops the `source` column and restores the original UNIQUE constraint. Code changes would also need to be reverted (revert the branch).

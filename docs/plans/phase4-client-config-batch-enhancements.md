# Phase 4: Client Config & Batch Processing Enhancements

## Overview

The core client config (batch/realtime mode), normalizer mode-gating, batch_algos module, and task scheduler already exist. This phase addresses the gaps: CLI algo overrides for batch runs, per-client Kafka topics, and e2e test coverage for the batch flow.

**Source:** changes.md #4, #5, #11

## What Already Exists

- `clients` table with `mode` (batch/realtime), `algo_run_hour`, window config columns
- `client_algo_config` table with per-tenant algo enabled/thresholds
- Normalizer reads `ClientConfigCache`, gates algos forwarding on `mode == "realtime"`
- `batch_algos` module reads from ClickHouse, uses SlidingWindowEngine
- Task scheduler with cron-based scheduling and subprocess execution
- ClientConfigCache with pub-sub invalidation

## What's Missing

### 4.1 CLI algo override for batch runs

**Why:** changes.md #5 — "allow us to override that when doing batch runs manually by explicitly providing the algos to run as command line args"

Currently `batch_algos` loads all three algorithms unconditionally. It checks `ClientConfigCache` for enabled/disabled per-tenant, but there's no way to override via CLI.

**Changes to `de_platform/modules/batch_algos/module.json`:**
```json
{
  "name": "algos",
  "description": "Comma-separated list of algorithms to run (overrides client config). Options: large_notional, velocity, suspicious_counterparty",
  "required": false,
  "type": "string",
  "default": ""
}
```

**Changes to `de_platform/modules/batch_algos/main.py`:**
- Parse `--algos` arg into a set of algo names
- If provided, filter `self.algorithms` to only those named
- Override `_get_algo_config()` to force-enable the specified algos regardless of client config

```python
async def initialize(self) -> None:
    # ... existing setup ...
    algo_override = self.config.get("algos", "")
    self._algo_override: set[str] | None = None
    if algo_override:
        self._algo_override = {a.strip() for a in algo_override.split(",") if a.strip()}

def _get_algo_config(self, tenant_id: str, algo_name: str) -> tuple[bool, dict]:
    if self._algo_override is not None:
        enabled = algo_name in self._algo_override
        thresholds = self.config_cache.get_algo_thresholds(tenant_id, algo_name)
        return enabled, thresholds
    # Default: use client config
    enabled = self.config_cache.is_algo_enabled(tenant_id, algo_name)
    thresholds = self.config_cache.get_algo_thresholds(tenant_id, algo_name)
    return enabled, thresholds
```

**CLI usage:**
```bash
# Run only velocity and large_notional for tenant acme
python -m de_platform run batch_algos \
    --tenant-id acme --start-date 2026-01-01 --end-date 2026-01-31 \
    --algos velocity,large_notional
```

### 4.2 Per-client Kafka topics

**Why:** changes.md #11 — "kafka starter service should have topics to read/write back to the client for each client in the system. We should have that configured in the client config service."

Currently `kafka_starter` reads from fixed topics (`client_orders`, `client_executions`, `client_transactions`). All clients share the same inbound topics.

**Design:**
- Client config stores optional `inbound_topic_prefix` per client (e.g., `acme_` → topics `acme_orders`, `acme_executions`, `acme_transactions`)
- `kafka_starter` subscribes to topics for all configured clients on startup
- `kafka_starter` listens to `client_config_updates` pub-sub for new/removed client topics
- Error responses go to per-client error topics (e.g., `acme_errors`)

**Changes:**
- Add `inbound_topic_prefix` and `error_topic` columns to `clients` table (migration)
- Update `kafka_starter` to dynamically subscribe to per-client topics
- Update `ClientConfigCache` to expose topic configuration
- Default: clients without custom topics use the shared `client_*` topics

### 4.3 Batch algo e2e test coverage

**Why:** changes.md #4 — "I do not see any unit test coverage and no e2e test coverage for batch algo runs as batch jobs that are triggered manually or by the scheduler service"

Unit tests exist in `de_platform/modules/batch_algos/tests/test_main.py` (4 tests). No integration or e2e coverage.

**E2E test to add** (`tests/e2e/test_batch_algos.py`):
1. Ingest events for a tenant via REST (so they land in ClickHouse)
2. Wait for persistence to complete
3. Run `batch_algos` module as a subprocess with the tenant and date range
4. Wait for alerts to appear in alert_manager
5. Verify alert count and content match expectations

**E2E test for scheduler-triggered batch** (`tests/e2e/test_scheduler_batch.py`):
1. Configure a client as batch mode with `algo_run_hour`
2. Create a task definition for batch_algos
3. Trigger the task via scheduler API
4. Wait for run to complete
5. Verify alerts generated

## Files to Modify

| File | Changes |
|------|---------|
| `de_platform/modules/batch_algos/module.json` | Add `algos` optional arg |
| `de_platform/modules/batch_algos/main.py` | Parse `--algos`, override algo config |
| `de_platform/modules/kafka_starter/main.py` | Dynamic per-client topic subscription |
| `de_platform/pipeline/client_config_cache.py` | Expose topic config per client |

## Migrations to Create

| File | Purpose |
|------|---------|
| `de_platform/migrations/client_config/004_add_topic_config.up.sql` | Add `inbound_topic_prefix`, `error_topic` to clients |

## Tests to Create

| File | Purpose |
|------|---------|
| `tests/e2e/test_batch_algos.py` | E2E: ingest → batch_algos subprocess → verify alerts |
| `tests/e2e/test_scheduler_batch.py` | E2E: scheduler triggers batch_algos → verify alerts |

## Acceptance Criteria

1. `batch_algos --algos velocity` runs only the velocity algorithm
2. `batch_algos` without `--algos` uses client config (existing behavior)
3. Per-client Kafka topics configured and consumed by kafka_starter
4. New/removed client topics picked up via pub-sub without restart
5. E2E test passes for manual batch_algos run
6. E2E test passes for scheduler-triggered batch run

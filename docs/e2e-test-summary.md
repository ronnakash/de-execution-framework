# E2E & Integration Test Summary

## Test Files Overview

| File | Location | # Tests | Infrastructure | Execution Model |
|---|---|---|---|---|
| `test_pipeline_e2e.py` | `tests/integration/` | 5 | In-memory stubs | Manual step-by-step |
| `test_matrix_e2e.py` | `tests/integration/` | 34 | In-memory stubs | Manual step-by-step |
| `test_inprocess_e2e.py` | `tests/e2e/` | 34 | In-memory stubs | Async tasks (concurrent) |
| `test_matrix_real_infra.py` | `tests/e2e/` | 34 | Real services | Manual step-by-step |
| `test_inprocess_real_infra.py` | `tests/e2e/` | 34 | Real services | Async tasks (concurrent) |
| `test_subprocess_e2e.py` | `tests/e2e/` | 34 | Real services | OS subprocesses |

**Total: ~175 tests across 6 files.**

---

## Two Directories, Two Meanings

- **`tests/integration/`** — All in-memory. No Docker containers needed. Fast (~0.3s). Misleading name: these are really in-memory E2E tests.
- **`tests/e2e/`** — Mix of in-memory and real infrastructure. Requires Docker (testcontainers or `make infra-up`). Slow (30–120s).

---

## Four Execution Modes

### Mode 1: Manual Step-by-Step (Synchronous)

**Files:** `test_pipeline_e2e.py`, `test_matrix_e2e.py`, `test_matrix_real_infra.py`

Modules are instantiated directly. The test manually drives each stage:

```
ingest → _run_normalizer() → _flush_persistence() → _evaluate_algos() → assert
```

The test is in full control of sequencing. No event loop drives the modules.

### Mode 2: In-Process Async Tasks

**Files:** `test_inprocess_e2e.py`, `test_inprocess_real_infra.py`

All 6 pipeline modules run as real `asyncio.Task`s sharing the same event loop. Tests interact via HTTP (REST) or message queue publish, then `await poll_until(condition)` for assertions. Closest to production behavior without subprocesses.

### Mode 3: OS Subprocesses (Most Production-Like)

**File:** `test_subprocess_e2e.py`

Each module runs as `python3 -m de_platform run <module> --db ... --mq kafka ...`. Tests interact only via HTTP and Kafka. Assertions query databases directly.

### Mode 4: Dev Infra vs Testcontainers

Controlled by `USE_DEV_INFRA` env var:
- **Unset** → Testcontainers spins up 5 Docker containers per session
- **`USE_DEV_INFRA=1`** → Uses existing containers from `make infra-up`

---

## Conftest Hierarchy

### Root `conftest.py`
- `postgres_container` (session) — single Postgres testcontainer
- `warehouse_db` (function) — connects, migrates, yields, rolls back

Used only by `tests/integration/` for Postgres-specific integration tests.

### `tests/e2e/conftest.py`
- `infra` (session) — starts all 5 testcontainers OR reads dev infra config
- `_init_schemas` (session, autouse) — runs migrations, seeds currency rates. Skips if no `@real_infra` tests collected.
- `_cleanup_between_tests` (function, autouse) — truncates all tables, flushes Redis, re-seeds. Only for `@real_infra` tests.
- Per-test fixtures: `secrets`, `warehouse_db`, `alerts_db`, `clickhouse_db`, `redis_cache`, `kafka_mq`, `minio_fs`

---

## Test Scenarios (34 Tests per Matrix File)

### Matrix: 3 ingestion methods x 3 event types x 3 scenarios = 27

| Ingestion | Event Types | Scenarios |
|---|---|---|
| REST, Kafka, Files | order, execution, transaction | 100 valid → stored correctly |
| | | 100 invalid → in errors table |
| | | Same event x100 → 1 valid + 99 duplicates |

### Non-Matrix: 7 additional tests

- Internal dedup (same `message_id` → silently dropped)
- Alert generation via REST / Kafka / Files
- `LargeNotionalAlgo` (fires above $1M notional)
- `VelocityAlgo` (fires when rate exceeds threshold)
- `SuspiciousCounterpartyAlgo` (fires on blocklisted counterparty)

---

## Ingestion Path Implementations

### REST
- **In-memory:** `aiohttp.test_utils.TestClient` with inline minimal app
- **In-process real:** HTTP POST to running `RestStarterModule`
- **Subprocess:** HTTP POST to subprocess server

### Kafka
- **In-memory (manual):** direct `KafkaStarterModule._process_message()` call
- **In-memory (async):** `mq.publish()` to `client_{event_type}` topics
- **Real infra:** `KafkaQueue.publish()` to real Kafka

### Files
- **In-memory:** `fs.write()` then `FileProcessorModule.execute()` directly
- **Real infra:** write to MinIO, then run `FileProcessorModule` in-process
- **Subprocess:** write to MinIO, then launch `file_processor` as OS process

---

## Common Patterns

1. **Currency rate seeding** — Always 3 rates: EUR→USD (1.10), GBP→USD (1.25), USD→USD (1.00). Seeded in both cache (Redis/MemoryCache) and DB to work around MemoryDatabase's limited SQL parser.

2. **Persistence flush config** — `flush-threshold: 1, flush-interval: 0` for deterministic flushing.

3. **Kafka group isolation** — Each test/module gets a unique consumer group ID (`uuid4` based) to prevent cross-test interference.

4. **Assertion polling** — `poll_until(lambda: condition, timeout, interval)` for async tests. `_wait_for_rows(db, query, expected, timeout)` with `time.sleep` for sync real-infra tests.

---

## Known Issues

1. **Event factories are duplicated** — Each file defines its own `_order()`, `_execution()`, `_transaction()` with slightly different defaults (`tenant_id` is `"acme"` in some, `"t1"` in others).

2. **Invalid event definition varies** — Some files set `id=""`, others `pop("status")`. Different validation paths are exercised.

3. **Velocity algo threshold inconsistency** — Manual tests use `max_events=5`, in-process tests use the module default (100).

4. **Sync-in-async `run_until_complete` calls** — `PostgresDatabase` sync wrappers fail when called from a running event loop. We've fixed this in `AlgosModule._evaluate()` and `DataApiModule` handlers, but other modules may still have this latent issue.

5. **Timing-dependent negative assertions** — Internal dedup tests use `asyncio.sleep(3–5s)` to assert "nothing arrived", which is flaky.

6. **Private attribute access** — `test_matrix_real_infra.py` accesses `kafka_producer._bootstrap` directly.

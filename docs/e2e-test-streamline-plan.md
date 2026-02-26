# Plan: Streamline E2E Tests

## Problem Statement

The current e2e/integration test suite has 6 files with ~175 tests that share 80%+ of the same test logic but duplicate it entirely. Event factories, ingestion helpers, assertion patterns, and module wiring are copy-pasted with slight variations across files. This makes the suite fragile (changes must be propagated to 3–6 files) and hard to reason about.

Additionally, several latent sync-in-async bugs exist because modules use `run_until_complete()` wrappers that fail when called from a running event loop — the exact issue we've been fixing piecemeal.

---

## Proposed Changes

### 1. Extract Shared Event Factories

**Current state:** Each file defines `_order()`, `_execution()`, `_transaction()`, `_invalid_event()` with inconsistent defaults.

**Proposed:** Create `tests/helpers/events.py` with canonical factory functions:

```python
def make_order(id_="ord-1", tenant_id="acme", ...) -> dict
def make_execution(id_="exec-1", ...) -> dict
def make_transaction(id_="tx-1", ...) -> dict
def make_invalid(event: dict, method="remove_id") -> dict
```

All test files import from here. One place to update when schema changes.

### 2. Extract Shared Pipeline Helpers

**Current state:** `_run_normalizer()`, `_drain_algos_topics()`, `_flush_persistence()`, `_wait_for_rows()`, `poll_until()` are duplicated with minor variations.

**Proposed:** Create `tests/helpers/pipeline.py`:

```python
def run_normalizer(normalizer, event_type, count)
async def drain_algos(algos, mq, event_type)
def flush_persistence(persistence)
async def poll_until(predicate, timeout, interval)
def wait_for_rows(db, query, expected, timeout)
```

### 3. Consolidate Directory Structure

**Current structure:**
```
tests/
  integration/        ← confusing name, actually in-memory e2e
    test_pipeline_e2e.py
    test_matrix_e2e.py
  e2e/                ← mix of in-memory and real infra
    test_inprocess_e2e.py
    test_inprocess_real_infra.py
    test_matrix_real_infra.py
    test_subprocess_e2e.py
    conftest.py
```

**Proposed:**
```
tests/
  helpers/
    events.py         ← shared event factories
    pipeline.py       ← shared step/assertion helpers
    ingestion.py      ← shared ingestion helpers (REST, Kafka, Files)
  e2e/
    conftest.py       ← infrastructure fixtures (unchanged)
    test_pipeline.py              ← the original 5 hand-written scenario tests (in-memory)
    test_matrix_memory.py         ← 34 matrix tests, in-memory manual-step
    test_matrix_real_infra.py     ← 34 matrix tests, real infra manual-step
    test_inprocess_memory.py      ← 34 tests, async tasks, in-memory
    test_inprocess_real_infra.py  ← 34 tests, async tasks, real infra
    test_subprocess.py            ← 34 tests, OS subprocesses, real infra
```

Key changes:
- Rename `tests/integration/` → merge into `tests/e2e/` (it's all e2e, just different infra)
- Rename files to make infra mode obvious from the filename
- Move `test_pipeline_e2e.py` (the original 5 scenario tests) as `test_pipeline.py`

### 4. Fix All Sync-in-Async Database Calls

**Current state:** We've fixed `AlgosModule._evaluate()` and `DataApiModule` handlers. But other modules may still call sync DB wrappers from async contexts.

**Proposed audit:** Search every `AsyncModule` subclass for calls to sync DB methods (`fetch_all`, `fetch_one`, `execute`, `bulk_insert`, `insert_one`) and replace with their `_async` counterparts. This prevents `run_until_complete` failures when running with real Postgres in an async event loop.

Modules to audit:
- `NormalizerModule`
- `PersistenceModule`
- `AlgosModule` (done)
- `DataApiModule` (done)
- `RestStarterModule`
- `KafkaStarterModule`
- `FileProcessorModule`

### 5. Standardize Ingestion Helpers

**Current state:** Each file re-implements `_ingest_rest()`, `_ingest_kafka()`, `_ingest_files()` differently for memory vs real infra.

**Proposed:** Create `tests/helpers/ingestion.py` with two layers:

```python
# Low-level (used by manual-step tests)
async def ingest_rest(port, event_type, events)
def ingest_kafka(mq, event_type, events)
def ingest_files(fs, event_type, events, topic_map)

# High-level (auto-detects memory vs real)
async def ingest(method, event_type, events, ctx)
```

### 6. Remove Timing-Based Negative Assertions

**Current state:** Internal dedup tests sleep 3–5 seconds then check nothing arrived.

**Proposed:** Instead of sleeping, run one more processing cycle and assert the queue is empty / row count unchanged. For subprocess mode, use a short timeout poll that's expected to fail.

---

## Execution Order

| Phase | Effort | Impact | Risk |
|---|---|---|---|
| 4. Fix sync-in-async DB calls | Small | High | Low — mechanical, each fix is independent |
| 1. Extract event factories | Small | Medium | Low — pure extraction, no behavior change |
| 2. Extract pipeline helpers | Medium | Medium | Low — pure extraction |
| 5. Standardize ingestion helpers | Medium | Medium | Medium — ingestion differences are intentional in some cases |
| 3. Consolidate directory structure | Small | High (clarity) | Medium — must update imports and Makefile targets |
| 6. Fix timing assertions | Small | Low | Low |

Recommended: Start with phases 4 and 1 (low risk, immediate value), then 2 and 3 together.

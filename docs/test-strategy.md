# Test Strategy

## Current State

We have ~420 tests spread across 46 files, run by 4 Makefile commands that don't map cleanly to a coherent strategy.

### Current Makefile Commands

| Command | What it runs | Filter logic |
|---------|-------------|--------------|
| `make test` | `de_platform/ tests/` | `-m "not real_infra"` |
| `make test-unit` | `de_platform/ tests/` | `-k "not postgres" -m "not real_infra"` |
| `make test-e2e` | `tests/e2e/` | `-m "not real_infra"` |
| `make test-real-infra` | `tests/e2e/ tests/integration/` | `-m real_infra -n 12` |

Problems:
- `make test` and `make test-unit` overlap almost completely (differ only by `-k "not postgres"`)
- The `-k "not postgres"` filter is brittle — it silently excludes `test_postgres_database.py` by name, not by what it actually needs
- `make test-e2e` runs only in-memory E2E tests, which are really just unit tests with a different harness
- `make test-real-infra` lumps integration tests (single module + real service) with E2E tests (full pipeline + all services) into one bucket
- No clear boundary between "needs Docker" and "doesn't need Docker"

### Current Test Inventory

#### In `de_platform/` (288 tests, no infra needed except 1 file)

| Category | Location | Tests | Infra |
|----------|----------|-------|-------|
| Framework/config | `config/tests/`, `cli/tests/` | 23 | None |
| Services (memory impls) | `services/*/tests/` | ~85 | None |
| Services (postgres) | `services/database/tests/test_postgres_database.py` | 11 | Docker Postgres |
| Migrations | `migrations/tests/` | 9 | None (MemoryDatabase) |
| Pipeline logic | `pipeline/tests/` | 39 | None |
| Module unit tests | `modules/*/tests/` | ~130 | None |

One outlier: `test_postgres_database.py` lives with the service unit tests but needs real Postgres. It uses `pytest.importorskip("asyncpg")` to silently skip when asyncpg isn't installed.

#### In `tests/e2e/` (122 tests across 4 files)

| File | Tests | Infra | Marker |
|------|-------|-------|--------|
| `test_pipeline.py` | 6 | None (in-memory, manual stepping) | None |
| `test_memory.py` | 34 | None (MemoryHarness, scenario-based) | None |
| `test_subprocess.py` | 44 | All Docker services + 6 subprocesses | `real_infra` |
| `test_containers.py` | 44 | All Docker services + 6 subprocesses | `real_infra` |

`test_pipeline.py` and `test_memory.py` use in-memory implementations only. They're called "E2E" but don't touch real infrastructure — they exercise the pipeline logic through the `PipelineHarness` protocol with in-memory stubs.

`test_subprocess.py` and `test_containers.py` are identical — both use `RealInfraHarness` backed by a session-scoped `SharedPipeline` (6 module subprocesses, real Kafka/Postgres/ClickHouse/Redis/MinIO). They run the same 10 scenarios (44 tests each) against the same shared pipeline. Having both is redundant.

#### In `tests/integration/` (11 tests across 6 files)

Each file tests **one module** against **its real infrastructure dependency**:

| File | Module | Real Services |
|------|--------|---------------|
| `test_normalizer_redis.py` | Normalizer dedup | Redis |
| `test_algos_postgres.py` | Algos alert persistence | Postgres |
| `test_persistence_clickhouse.py` | Persistence flush | ClickHouse, Kafka |
| `test_data_api_postgres_clickhouse.py` | Data API queries | Postgres, ClickHouse |
| `test_currency_loader_postgres.py` | Currency loader | Postgres |
| `test_file_processor_minio.py` | File processor | MinIO, Kafka |

These are the most valuable "integration" tests — they verify that each module works correctly with its real service dependency, without needing the full pipeline.

---

## Proposed Structure

Three tiers with clear boundaries:

### 1. Unit Tests (`make test-unit`)

**What**: Individual components with in-memory/mock dependencies. No Docker. No network.

**Runs**: ~300 tests in <2s. Every PR, every commit, local dev.

**Includes**:
- Everything currently in `de_platform/*/tests/` **except** `test_postgres_database.py`
- `tests/e2e/test_pipeline.py` (rename/move — it's really a unit test of the pipeline logic)
- `tests/e2e/test_memory.py` (rename/move — scenario-based unit tests via MemoryHarness)

**Marker**: None (default). Selected by exclusion: `-m "not integration and not e2e"`.

**Key principle**: If it uses `MemoryDatabase`, `MemoryQueue`, `MemoryCache`, `MemoryFileSystem` — it's a unit test, regardless of whether it exercises one function or the full pipeline.

### 2. Integration Tests (`make test-integration`)

**What**: Individual modules tested against real infrastructure services. Requires Docker.

**Runs**: ~25 tests in ~30s. Pre-merge CI, local with `make infra-up`.

**Two flavors**:

**a) Service integration** — verifying our service implementations (PostgresDatabase, RedisCache, KafkaQueue, etc.) work correctly against the real backends:
- `test_postgres_database.py` (moved from `de_platform/services/` to `tests/integration/services/`)

**b) Module integration** — verifying each pipeline module works with its real dependencies:
- `test_normalizer_redis.py` — normalizer + Redis dedup
- `test_algos_postgres.py` — algos + Postgres alerts
- `test_persistence_clickhouse.py` — persistence + ClickHouse
- `test_data_api_postgres_clickhouse.py` — data API + Postgres + ClickHouse
- `test_currency_loader_postgres.py` — currency loader + Postgres
- `test_file_processor_minio.py` — file processor + MinIO

**Marker**: `@pytest.mark.integration`

**Key principle**: One module under test, real service dependencies, no full pipeline. Tests the seam between our code and the infrastructure.

### 3. E2E Tests (`make test-e2e`)

**What**: Full pipeline — all 6 modules running as subprocesses, all infrastructure services, end-to-end message flow.

**Runs**: 44 tests in ~4-6 min. Pre-merge CI (gated), nightly.

**Includes**:
- The 44 scenario-based tests (currently duplicated in `test_subprocess.py` and `test_containers.py` — consolidate to one file)
- All 10 scenarios: valid events, invalid events, duplicates, internal dedup, alert generation, large notional, velocity, suspicious counterparty, multi-error consolidation, duplicate contains original

**Architecture**: Session-scoped `SharedPipeline` starts 6 subprocesses once. Each test gets a `RealInfraHarness` with a unique `tenant_id` for data isolation. No cleanup between tests.

**Marker**: `@pytest.mark.e2e`

**Key principle**: Full system behavior. Validates that all modules work together as a pipeline, messages flow through Kafka topics correctly, data lands in the right databases, alerts fire. The most production-realistic test mode.

---

## Proposed Directory Layout

```
de_platform/
  modules/*/tests/          # Unit tests (in-memory only)
  services/*/tests/         # Unit tests (in-memory only)
  pipeline/tests/           # Unit tests (in-memory only)
  config/tests/             # Unit tests
  cli/tests/                # Unit tests
  migrations/tests/         # Unit tests

tests/
  unit/
    test_pipeline.py        # Full pipeline, in-memory (moved from tests/e2e/test_pipeline.py)
    test_scenarios.py       # 44 scenario tests via MemoryHarness (moved from tests/e2e/test_memory.py)

  integration/
    conftest.py             # Service fixtures (Postgres, ClickHouse, Redis, Kafka, MinIO)
    services/
      test_postgres.py      # PostgresDatabase against real Postgres
      # Future: test_redis.py, test_clickhouse.py, test_kafka.py, test_minio.py
    modules/
      test_normalizer.py    # Normalizer + Redis
      test_algos.py         # Algos + Postgres
      test_persistence.py   # Persistence + ClickHouse
      test_data_api.py      # Data API + Postgres + ClickHouse
      test_currency.py      # Currency loader + Postgres
      test_file_processor.py # File processor + MinIO

  e2e/
    conftest.py             # SharedPipeline fixture, schema init
    test_pipeline.py        # 44 scenario tests via RealInfraHarness (single file, not 2)

  helpers/
    harness.py              # MemoryHarness, RealInfraHarness, SharedPipeline
    scenarios.py            # 10 shared scenarios
    events.py               # Event factories
    ingestion.py            # Ingestion helpers
    diagnostics.py          # Test diagnostics
```

## Proposed Makefile Commands

```makefile
test-unit:
    $(PYTEST) de_platform/ tests/unit/ -v -m "not integration and not e2e"

test-integration:
    $(PYTEST) tests/integration/ -v -m integration --tb=short

test-e2e:
    $(PYTEST) tests/e2e/ -v -m e2e --tb=short

test:  # All non-infra tests (CI fast path)
    $(PYTEST) de_platform/ tests/unit/ -v -m "not integration and not e2e"

test-all:  # Everything (requires Docker)
    $(PYTEST) -v --tb=short
```

## Migration Steps

1. Create `tests/unit/` directory, move `test_pipeline.py` and `test_memory.py` there
2. Restructure `tests/integration/` into `services/` and `modules/` subdirs
3. Move `test_postgres_database.py` to `tests/integration/services/`
4. Consolidate `test_subprocess.py` + `test_containers.py` into single `tests/e2e/test_pipeline.py`
5. Replace `@pytest.mark.real_infra` with `@pytest.mark.integration` and `@pytest.mark.e2e`
6. Update `pyproject.toml` markers
7. Update Makefile commands
8. Update CLAUDE.md
9. Clean up dead code: `SubprocessHarness` and `ContainerHarness` classes in `harness.py` (replaced by `SharedPipeline` + `RealInfraHarness`)

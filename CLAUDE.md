# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Local setup (outside devcontainer)
make setup                   # venv + core deps only (unit tests)
make setup-full              # venv + core + infra deps (integration/e2e tests)
source .venv/bin/activate

# Tests (3-tier: unit / integration / e2e)
make test                    # unit tests only (de_platform/ + tests/unit/, no infra)
make test-unit               # same as make test
make test-integration        # module + service integration tests (requires make infra-up)
make test-e2e                # full pipeline E2E tests (requires make infra-up)
make test-all                # all tests (unit + integration + e2e)

# Run a single test
pytest de_platform/modules/normalizer/tests/test_main.py -v
pytest de_platform/modules/normalizer/tests/test_main.py::test_name -v

# Lint / format
make lint                    # ruff check de_platform/
make format                  # ruff format de_platform/

# Run a module
make run module=normalizer args="--db warehouse=postgres --mq kafka --cache redis"
# Equivalent: python -m de_platform run normalizer --db warehouse=postgres ...

# Migrations
make migrate cmd=up args="warehouse --db postgres"
make migrate cmd=down args="warehouse --count 1 --db postgres"

# Infrastructure (Docker)
make infra-up                # postgres, redis, minio, kafka, zookeeper, clickhouse
make infra-down
```

**pytest config:** `asyncio_mode = "auto"` (pyproject.toml) — all async tests run automatically. Use `@pytest.mark.asyncio` decorator for clarity but it is not required.

## Architecture

### Interface-Driven Dependency Injection

Every external dependency is an ABC in `de_platform/services/<name>/interface.py`. At runtime the CLI selects concrete implementations via flags; the same module code runs unchanged with in-memory stubs (unit tests), docker-compose services (integration tests), or real infrastructure (production).

**Optional infra deps:** Heavy libraries (asyncpg, redis, confluent-kafka, minio, clickhouse-connect) are in the `[infra]` optional group. Unit tests run without them (`pip install -e '.[dev]'`). Integration/e2e tests need them (`pip install -e '.[dev,infra]'`). Imports are lazy — modules like `PostgresDatabase` and `RedisCache` only import their libraries at connection time.

**Seven interfaces and their implementations:**

| Interface | memory | real |
|---|---|---|
| `DatabaseInterface` | `MemoryDatabase` | `PostgresDatabase`, `ClickHouseDatabase` |
| `FileSystemInterface` | `MemoryFileSystem` | `LocalFileSystem`, `MinioFileSystem` |
| `CacheInterface` | `MemoryCache` | `RedisCache` |
| `MessageQueueInterface` | `MemoryQueue` | `KafkaQueue` |
| `MetricsInterface` | `MemoryMetrics` | `PrometheusMetrics`, `NoopMetrics` |
| `LoggingInterface` | `MemoryLogger` (via `LoggerFactory`) | `PrettyLogger`, `LokiLogger` |
| `SecretsInterface` | — | `EnvSecrets` |

**`MemoryDatabase` limitation:** Its SQL parser only handles `SELECT * FROM <table> WHERE col = $1` (single WHERE clause, `SELECT *` only). Multi-column WHERE queries or `SELECT <col>` will silently return `None`. When unit-testing code that does complex DB queries (e.g., `CurrencyConverter`), pre-populate the cache instead of the DB.

### DI Container (`de_platform/config/container.py`)

Type-based injection: `Container.resolve(SomeClass)` inspects `__init__` type hints, looks each parameter type up in its registry, and instantiates `SomeClass` with all dependencies wired. Modules declare their dependencies purely through constructor type annotations — no service-locator calls.

### Registry (`de_platform/services/registry.py`)

Maps `("db", "postgres")` → dotted import path strings. Classes are imported lazily so heavy libraries (asyncpg, confluent-kafka, boto3) are only loaded when selected. Add new implementations here.

### Module System

All modules live in `de_platform/modules/<name>/` and must have:
- `module.json` — descriptor: `name`, `type` (`job`|`service`|`worker`), `args` schema
- `main.py` — must expose `module_class = MyModuleClass` at module level
- `__init__.py`

Module classes extend `Module` (sync) or `AsyncModule` (async) from `de_platform/modules/base.py`. The lifecycle is `initialize → validate → execute → teardown`, all wrapped by `run()`. The runner resolves `module_class` through the DI container, so all constructor parameters are auto-injected by type.

**Service/Worker modules** also get a `LifecycleManager` injected. They poll `lifecycle.is_shutting_down` in their `execute()` loop and register cleanup via `lifecycle.on_shutdown(callback)`. Hooks run in reverse registration order (LIFO) on SIGTERM/SIGINT.

### CLI entry point (`de_platform/__main__.py`)

Dispatches to `de_platform.cli.runner` (module execution) or `de_platform.cli.migrate` (migrations). The runner:
1. Loads `module.json`, parses global flags (`--db`, `--fs`, `--cache`, `--mq`, `--metrics`, `--log`, `--env-file`) and module-specific args
2. Builds DI container with selected implementations
3. Imports `de_platform.modules.<name>.main`, resolves `module_class`, calls `.run()`

`--db` is repeatable and supports named instances: `--db warehouse=postgres --db alerts=postgres` creates two separate DB connections accessible via `DatabaseFactory`.

### Migration System

Migrations live in `de_platform/migrations/<namespace>/` as `NNN_name.up.sql` / `NNN_name.down.sql` pairs. `MigrationRunner` tracks applied migrations in a `_migrations` table. Run via `python -m de_platform migrate <up|down|status> <namespace> --db <impl>`.

### Pipeline Domain (`de_platform/pipeline/`)

Fraud detection pipeline built on top of the framework:

- **`topics.py`** — Kafka topic name constants (`TRADE_NORMALIZATION`, `ORDERS_PERSISTENCE`, `ALERTS`, etc.)
- **`currency.py`** — `CurrencyConverter`: cache-first, DB-fallback rate lookup; key format `currency_rate:{FROM}_{TO}`
- **`dedup.py`** — `EventDeduplicator`: Redis cache keyed `dedup:{primary_key}` → returns `"new"`, `"internal_duplicate"`, or `"external_duplicate"`
- **`enrichment.py`** — `enrich_trade_event()`, `enrich_transaction_event()`, `compute_primary_key()` (format: `{tenant}_{type}_{id}_{date}`)
- **`algorithms.py`** — `FraudAlgorithm` ABC; built-ins: `LargeNotionalAlgo` (> $1M notional_usd), `VelocityAlgo` (rate via cache), `SuspiciousCounterpartyAlgo`. All accept optional `thresholds` dict in `evaluate()` for per-tenant overrides.
- **`client_config_cache.py`** — `ClientConfigCache`: reads per-tenant config from Redis cache keys (`client_config:{tenant_id}`, `algo_config:{tenant_id}:{algo}`). Subscribes to `client_config_updates` pub-sub channel for real-time invalidation. Used by normalizer (mode gating) and algos (per-tenant thresholds).

**Pipeline modules:** `normalizer` (enrich + dedup, mode-gated algos forwarding), `persistence` (buffer → ClickHouse + filesystem), `algos` (fraud algorithms → alerts, per-tenant config), `data_api` (HTTP API + static UI at `/ui`), `currency_loader` (fetches rates into DB), `client_config` (REST API for per-tenant configuration — mode, algo thresholds, enabled algos)

### Testing Patterns (3-tier)

**Unit tests** (`de_platform/*/tests/` + `tests/unit/`): use all memory implementations, instantiate modules directly without the DI container. No infra deps required. Includes in-memory pipeline scenario tests (`tests/unit/test_scenarios.py` via `MemoryHarness`, `tests/unit/test_pipeline_e2e.py` for narrative pipeline walkthrough).

**Integration tests** (`tests/integration/`): individual modules tested against real infrastructure services. Two subdirs: `services/` (e.g. `test_postgres_database.py`) and `modules/` (e.g. `test_normalizer_redis.py`). Marked `@pytest.mark.integration`. Requires `make infra-up`.

**E2E tests** (`tests/e2e/`): full pipeline — 7 module subprocesses, all infrastructure services, end-to-end message flow. Session-scoped `SharedPipeline` starts subprocesses once; each test gets a `RealInfraHarness` with unique `tenant_id` for data isolation. Marked `@pytest.mark.e2e`. Requires `make infra-up`.

**Test helpers** (`tests/helpers/`): `PipelineHarness` protocol and implementations (`MemoryHarness`, `SharedPipeline`, `RealInfraHarness`) in `harness.py`. 10 shared scenarios in `scenarios.py`. Event factories in `events.py`. Ingestion helpers in `ingestion.py`.

**DEVCONTAINER detection:** When `DEVCONTAINER=1` is set (automatic in devcontainer), test fixtures use Docker DNS names (postgres, redis, kafka:29092). Otherwise they default to localhost.

### Observability

**Metrics:** All pipeline modules emit metrics via `MetricsInterface` (counter, gauge, histogram). `NoopMetrics` is registered as default when `--metrics` is not passed. Standard metric names: `events_received_total`, `events_processed_total`, `duplicates_detected_total`, `rows_flushed_total`, `alerts_generated_total`, `events_ingested_total`, `events_errors_total`, `http_requests_total`.

**Structured logging:** All modules use `LoggerFactory` → `LoggingInterface` with structured context (tenant_id, event_type, event_id, etc.). `LokiLogger` sends logs to Grafana Loki. `PrettyLogger` for local dev. `MemoryLogger` for tests.

**Grafana dashboards:** 6 provisioned dashboards in `grafana/dashboards/`: pipeline_overview, normalizer, algos, persistence, data_api, test_runs. Datasources: Prometheus + Loki.

**Test diagnostics:** `TestDiagnostics` class in `tests/helpers/diagnostics.py` takes `PipelineSnapshot` (Kafka watermarks, DB row counts, module health, metrics). All harness timeout paths include snapshots in error messages. Pipeline report plugin (`tests/helpers/pytest_pipeline_report.py`) generates JSON + HTML reports in `reports/` after each test session.

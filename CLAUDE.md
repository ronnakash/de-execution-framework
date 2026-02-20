# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Tests
make test                    # all tests (de_platform/ + tests/)
make test-unit               # unit tests only (skips postgres integration tests)
make test-e2e                # end-to-end pipeline tests (tests/integration/)
make test-integration        # testcontainer-backed postgres tests

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

Every external dependency is an ABC in `de_platform/services/<name>/interface.py`. At runtime the CLI selects concrete implementations via flags; the same module code runs unchanged with in-memory stubs (unit tests), testcontainers (integration tests), or real infrastructure (production).

**Seven interfaces and their implementations:**

| Interface | memory | real |
|---|---|---|
| `DatabaseInterface` | `MemoryDatabase` | `PostgresDatabase`, `ClickHouseDatabase` |
| `FileSystemInterface` | `MemoryFileSystem` | `LocalFileSystem`, `MinioFileSystem` |
| `CacheInterface` | `MemoryCache` | `RedisCache` |
| `MessageQueueInterface` | `MemoryQueue` | `KafkaQueue` |
| `MetricsInterface` | `MemoryMetrics` | `PrometheusMetrics`, `NoopMetrics` |
| `LoggingInterface` | `MemoryLogger` (via `LoggerFactory`) | `PrettyLogger` |
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
- **`algorithms.py`** — `FraudAlgorithm` ABC; built-ins: `LargeNotionalAlgo` (> $1M notional_usd), `VelocityAlgo` (rate via cache), `SuspiciousCounterpartyAlgo`

**Pipeline modules:** `normalizer` (enrich + dedup), `persistence` (buffer → ClickHouse + filesystem), `algos` (fraud algorithms → alerts), `data_api` (HTTP API + static UI at `/ui`), `currency_loader` (fetches rates into DB)

### Testing Patterns

**Unit tests** (per-module in `de_platform/modules/<name>/tests/`): use all memory implementations, instantiate the module directly without the DI container.

**E2E tests** (`tests/integration/test_pipeline_e2e.py`): wire multiple modules together with shared memory implementations, drive them synchronously via `_poll_and_process()` / `_flush_all()` / `_evaluate()`, assert on the shared `MemoryQueue` / `MemoryDatabase` / `MemoryFileSystem` state.

**Integration tests** (`conftest.py` root fixtures): `postgres_container` (session-scoped testcontainer) → `warehouse_db` fixture runs real migrations then yields a connected `PostgresDatabase`. Marked with `-k "postgres"`.

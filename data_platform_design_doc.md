# Data Platform Template — Technical Design Document

> A production-grade Python monorepo for building data platforms
>
> Version 1.0 · February 2026

---

## 1. Executive Summary

This document describes the architecture and design of the **Data Platform Template**, a production-grade Python monorepo that demonstrates how to build real-world data platforms. The project is structured as a collection of independently-deployable **modules** (microservices, batch jobs, stream processors) that share a common infrastructure layer built on pluggable interfaces.

The platform uses **dependency injection via interfaces** to allow every external dependency (databases, file systems, caches, message queues) to be swapped between real and mock implementations. This enables local development, isolated testing with testcontainers, and production deployment without code changes — only configuration changes.

### 1.1 Goals

- Demonstrate production-grade data platform architecture in a single, explorable repository
- Provide a modular system where each component can be run, tested, and deployed independently
- Use interface-driven design so every infrastructure dependency is pluggable and testable
- Support local development, integration testing, and production deployment from the same codebase
- Serve as a portfolio piece that hiring managers can clone, run, and immediately understand

### 1.2 Non-Goals

- This is not a framework or library to be published on PyPI
- Not intended to cover every possible data platform pattern — it covers the most common and impactful ones
- Not a production-ready product; it is a template and reference architecture

---

## 2. Project Structure

The project is organized as a Python monorepo. All modules, shared libraries, interfaces, and infrastructure live under a single repository with a clear separation of concerns.

### 2.1 Directory Layout

```
data-platform/
├── .devcontainer/              # Dev container configuration
│   ├── devcontainer.json
│   └── Dockerfile
├── .env/                       # Environment files
│   ├── local.env               # Local development
│   ├── test.env                # Integration tests
│   └── staging.env             # Staging environment
├── modules/                    # All runnable modules
│   ├── stream_ingest/          # Kafka stream ingestion
│   │   ├── module.json         # Module descriptor
│   │   ├── __init__.py
│   │   ├── main.py             # Module entry point
│   │   └── tests/
│   ├── batch_etl/              # Batch ETL processing
│   ├── api_gateway/            # REST API service
│   ├── data_quality/           # Data quality checks
│   └── metric_aggregator/      # Metric rollups
├── platform/                   # Shared platform library
│   ├── interfaces/             # Abstract interfaces
│   │   ├── database.py
│   │   ├── filesystem.py
│   │   ├── cache.py
│   │   ├── message_queue.py
│   │   ├── logging.py
│   │   ├── metrics.py
│   │   └── secrets.py
│   ├── implementations/        # Concrete implementations
│   │   ├── postgres.py
│   │   ├── cassandra.py
│   │   ├── local_fs.py
│   │   ├── s3_fs.py
│   │   ├── redis_cache.py
│   │   ├── memory_cache.py
│   │   ├── kafka_queue.py
│   │   ├── memory_queue.py
│   │   └── ...
│   ├── config/                 # Configuration & DI
│   │   ├── env_loader.py       # .env file loading
│   │   ├── arg_parser.py       # CLI argument handling
│   │   └── container.py        # Dependency injection container
│   ├── health/                 # Health check system
│   ├── lifecycle/              # Startup/shutdown hooks
│   └── migrations/             # Database migration system
├── tests/                      # Integration & E2E tests
│   ├── conftest.py             # Shared testcontainer fixtures
│   ├── integration/
│   └── e2e/
├── deploy/                     # Deployment configs
│   ├── docker/
│   │   ├── Dockerfile          # Production image
│   │   └── docker-compose.yml  # Local orchestration
│   └── k8s/                    # Kubernetes manifests
├── scripts/                    # Dev tooling
│   └── scaffold.py             # Module scaffolding CLI
├── Makefile
├── pyproject.toml
└── README.md
```

### 2.2 Module Anatomy

Each module is a self-contained unit inside `modules/`. Every module must contain:

| File | Purpose |
|------|---------|
| `module.json` | Module descriptor: name, arguments, dependencies, description |
| `main.py` | Entry point with a `run(context)` function that receives the DI container |
| `tests/` | Unit tests specific to this module |

Modules are **never imported by other modules directly**. Inter-module communication happens exclusively through message queues or shared databases, enforcing clean service boundaries.

---

## 3. Module Descriptor (module.json)

Each module declares its metadata and argument schema in a `module.json` file. This file is used by the CLI runner for argument validation, help text generation, and by the scaffolding tool.

### 3.1 Schema

```json
{
  "name": "stream_ingest",
  "display_name": "Stream Ingestion Service",
  "description": "Consumes events from Kafka and writes to the data lake",
  "type": "service",
  "version": "1.0.0",
  "dependencies": {
    "interfaces": ["database", "filesystem", "message_queue", "logging"],
    "modules": []
  },
  "args": [
    {
      "name": "topic",
      "description": "Kafka topic to consume from",
      "required": true,
      "type": "string"
    },
    {
      "name": "batch-size",
      "description": "Number of messages to process per batch",
      "required": false,
      "type": "integer",
      "default": 100
    },
    {
      "name": "output-format",
      "description": "Format for data lake output",
      "required": false,
      "type": "string",
      "choices": ["parquet", "json", "csv"],
      "default": "parquet"
    }
  ]
}
```

### 3.2 Module Types

| Type | Behavior | Example |
|------|----------|---------|
| `service` | Long-running process, exposes health checks, graceful shutdown | API gateway, stream consumer |
| `job` | Runs once with parameters, exits with status code | Batch ETL, data migration, backfill |
| `worker` | Long-running, pulls work from a queue, processes items | Metric aggregator, notification sender |

### 3.3 Argument Validation

The platform validates all arguments before a module starts. Validation rules are derived from `module.json`:

- **Required arguments**: must be present; error if missing
- **Type checking**: values are cast to declared type (string, integer, float, boolean)
- **Choices**: if a `choices` array is declared, the value must be one of the listed options
- **Defaults**: applied when an optional argument is not provided

Validation errors produce a clear error message listing all problems, not just the first one. The platform also generates `--help` text automatically from `module.json`.

---

## 4. CLI Runner

The platform uses a single entry point to run any module. The CLI parses global flags (infrastructure selection) and delegates module-specific arguments to the module itself.

### 4.1 Invocation

```bash
python -m platform run <module_name> [global flags] [module args]
```

Examples:

```bash
# Run stream ingestion with Postgres + S3 + Kafka
python -m platform run stream_ingest --db postgres --fs s3 --mq kafka \
  --topic user-events --batch-size 500 --output-format parquet

# Run batch ETL locally with all local implementations
python -m platform run batch_etl --db postgres --fs local --mq memory \
  --env local --date 2026-01-15

# Run with environment-based config (production style)
python -m platform run api_gateway --env production
```

### 4.2 Global Flags

Global flags control which infrastructure implementations are injected. They apply to all modules.

| Flag | Options | Default | Description |
|------|---------|---------|-------------|
| `--db` | postgres, cassandra, memory | postgres | Database implementation |
| `--fs` | local, s3, minio | local | File system implementation |
| `--cache` | redis, memory | memory | Cache implementation |
| `--mq` | kafka, redis-stream, memory | memory | Message queue implementation |
| `--log` | json, pretty | pretty | Logging format |
| `--metrics` | prometheus, memory, noop | noop | Metrics backend |
| `--env` | local, test, staging, production | local | Environment (loads `.env/<env>.env`) |
| `--secrets` | env, file, vault | env | Secrets provider |

### 4.3 Startup Sequence

When a module is invoked, the platform executes the following sequence:

1. **Load environment**: Read `.env/<env>.env` file, merge with actual environment variables (env vars take precedence, supporting k8s pod injection)
2. **Parse arguments**: Parse global flags and module-specific arguments from `module.json`
3. **Validate arguments**: Run full validation against `module.json` schema
4. **Build DI container**: Instantiate the selected implementations for each interface
5. **Validate dependencies**: Check that all interfaces declared in `module.json` dependencies are available in the container
6. **Run migrations**: If the module declares a database dependency, run pending migrations
7. **Start health checks**: For service and worker types, start the health check endpoint
8. **Execute module**: Call the module `run(context)` function
9. **Shutdown**: On exit or SIGTERM/SIGINT, execute graceful shutdown hooks in reverse registration order

---

## 5. Interface System

The interface system is the core architectural pattern of the platform. Every external dependency is accessed through an abstract Python interface (ABC), with concrete implementations selected at runtime via CLI flags or environment configuration.

### 5.1 Design Principles

- **Interfaces are pure abstractions**: No implementation details, no imports of external libraries
- **Implementations are leaf nodes**: They import the external library and implement the interface
- **Modules depend only on interfaces**: Never import implementations directly
- **The DI container wires everything**: Module code never selects its own implementations

### 5.2 Interface Catalog

#### 5.2.1 Database Interface

Provides relational and document-style data access. Methods include connection management, query execution, transaction support, and bulk operations.

| Implementation | Backend | Use Case |
|----------------|---------|----------|
| `PostgresDatabase` | PostgreSQL via asyncpg | Production relational workloads |
| `CassandraDatabase` | Apache Cassandra via cassandra-driver | High-throughput distributed workloads |
| `MemoryDatabase` | In-memory dict-based store | Unit testing, fast local iteration |

Key methods: `connect()`, `disconnect()`, `execute(query, params)`, `fetch_one()`, `fetch_all()`, `transaction()` (context manager), `bulk_insert(table, rows)`.

#### 5.2.2 File System Interface

Abstracts file storage for data lake and warehouse patterns. Supports reading, writing, listing, and deleting files with path-based access.

| Implementation | Backend | Use Case |
|----------------|---------|----------|
| `LocalFileSystem` | Local disk | Local development and testing |
| `S3FileSystem` | AWS S3 via boto3 | Production cloud storage |
| `MinIOFileSystem` | MinIO (S3-compatible) | Local S3-compatible testing |

Key methods: `read(path)`, `write(path, data)`, `list(prefix)`, `delete(path)`, `exists(path)`, `get_signed_url(path, expiry)`.

#### 5.2.3 Cache Interface

Provides key-value caching with TTL support.

| Implementation | Backend | Use Case |
|----------------|---------|----------|
| `RedisCache` | Redis via redis-py | Production distributed caching |
| `MemoryCache` | In-memory dict with TTL tracking | Local development and testing |

Key methods: `get(key)`, `set(key, value, ttl)`, `delete(key)`, `exists(key)`, `flush()`.

#### 5.2.4 Message Queue Interface

Covers both publish/subscribe and work queue patterns for inter-module communication.

| Implementation | Backend | Use Case |
|----------------|---------|----------|
| `KafkaQueue` | Apache Kafka via confluent-kafka | Production event streaming |
| `RedisStreamQueue` | Redis Streams | Lightweight production queues |
| `MemoryQueue` | In-memory asyncio.Queue | Testing and local development |

Key methods: `publish(topic, message)`, `subscribe(topic, handler)`, `ack(message_id)`, `create_topic(name, partitions)`.

#### 5.2.5 Logging Interface

Structured logging with correlation ID propagation.

| Implementation | Backend | Use Case |
|----------------|---------|----------|
| `JsonLogger` | JSON to stdout | Production (parsed by log aggregators) |
| `PrettyLogger` | Colorized human-readable output | Local development |

Key methods: `info(msg, **ctx)`, `warn(msg, **ctx)`, `error(msg, **ctx)`, `debug(msg, **ctx)`. All methods accept arbitrary keyword arguments that become structured fields. A `correlation_id` is automatically attached to every log entry and propagated through the context.

#### 5.2.6 Metrics Interface

| Implementation | Backend | Use Case |
|----------------|---------|----------|
| `PrometheusMetrics` | Prometheus client | Production metrics collection |
| `MemoryMetrics` | In-memory counters/gauges | Testing assertions on metric values |
| `NoopMetrics` | Discards all metrics | Local dev when metrics not needed |

Key methods: `counter(name, value, tags)`, `gauge(name, value, tags)`, `histogram(name, value, tags)`, `timer(name)` (context manager).

#### 5.2.7 Secrets Interface

| Implementation | Backend | Use Case |
|----------------|---------|----------|
| `EnvSecrets` | Environment variables | Production (k8s secrets as env vars) |
| `FileSecrets` | .env files | Local development |
| `VaultSecrets` | HashiCorp Vault | Enterprise environments |

Key methods: `get(key)`, `get_or_default(key, default)`, `require(key)` (raises if missing).

---

## 6. Dependency Injection Container

The DI container is the central wiring mechanism. It constructs implementations based on CLI flags and environment config, then provides them to modules via a typed context object.

### 6.1 Container Design

The container follows a simple pattern: **register interface-to-implementation mappings, then resolve them lazily**. Implementations are singletons within a container instance (one Postgres connection pool shared across the module, not one per call).

```python
class PlatformContext:
    db: DatabaseInterface
    fs: FileSystemInterface
    cache: CacheInterface
    mq: MessageQueueInterface
    log: LoggingInterface
    metrics: MetricsInterface
    secrets: SecretsInterface
    config: ModuleConfig   # Parsed and validated module args
```

### 6.2 Wiring Logic

The container uses a registry pattern. Each interface has a mapping from CLI flag value to implementation class:

```python
REGISTRY = {
    "database": {
        "postgres": PostgresDatabase,
        "cassandra": CassandraDatabase,
        "memory": MemoryDatabase,
    },
    "filesystem": {
        "local": LocalFileSystem,
        "s3": S3FileSystem,
        "minio": MinIOFileSystem,
    },
    # ... etc
}
```

Each implementation receives its configuration (connection URLs, credentials) from the secrets/env layer, never from CLI args. CLI args only select **which** implementation; the implementation reads its own config from the environment.

---

## 7. Environment & Configuration

### 7.1 .env File Strategy

Environment files live in `.env/` and are selected via the `--env` flag. The loading priority (highest wins):

1. **Actual environment variables** (always highest priority; supports k8s pod injection)
2. **`.env/<env>.env`** file (selected by `--env` flag)
3. **`.env/local.env`** (fallback default)

In production deployments (Kubernetes), no `.env` files are read. All config comes from environment variables injected into the pod via ConfigMaps and Secrets. The `.env` system is a development convenience only.

### 7.2 Configuration Keys

All configuration follows a namespaced convention:

```bash
# Database
DB_POSTGRES_URL=postgresql://user:pass@localhost:5432/platform
DB_CASSANDRA_HOSTS=localhost:9042
DB_CASSANDRA_KEYSPACE=platform

# File System
FS_LOCAL_ROOT=/tmp/data-platform
FS_S3_BUCKET=data-platform-lake
FS_S3_REGION=us-east-1
FS_MINIO_ENDPOINT=http://localhost:9000
FS_MINIO_ACCESS_KEY=minioadmin
FS_MINIO_SECRET_KEY=minioadmin

# Cache
CACHE_REDIS_URL=redis://localhost:6379/0

# Message Queue
MQ_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
MQ_REDIS_STREAM_URL=redis://localhost:6379/1

# Observability
LOG_LEVEL=INFO
METRICS_PROMETHEUS_PORT=9090
```

---

## 8. Health Check System

All service and worker modules expose health checks. The health system serves two purposes: Kubernetes liveness/readiness probes and local debugging.

### 8.1 Health Check Types

| Check | Purpose | Behavior |
|-------|---------|----------|
| Liveness | Is the process alive and not deadlocked? | Returns 200 if the event loop is responsive |
| Readiness | Can the service handle traffic? | Returns 200 only if all interface dependencies are connected and healthy |
| Startup | Has initial setup completed? | Returns 200 after the module `run()` function begins execution |

### 8.2 Interface Health

Each interface implementation registers a health check function. The readiness probe aggregates all of them:

- **Database**: Executes `SELECT 1`
- **Cache**: Executes `PING`
- **Message Queue**: Checks broker connectivity
- **File System**: Attempts to list the root prefix

### 8.3 Endpoint

For service modules, a lightweight HTTP server (using aiohttp or just raw asyncio) serves health endpoints on a configurable port (default 8080):

```
GET /health/live     -> { "status": "ok" }
GET /health/ready    -> { "status": "ok", "checks": { "db": "ok", "cache": "ok" } }
GET /health/startup  -> { "status": "ok" }
```

For job modules, health checks are not exposed via HTTP. Instead, the platform writes a health status file that can be checked by orchestrators.

---

## 9. Graceful Shutdown

The platform handles SIGTERM and SIGINT signals to perform clean shutdown. This is critical for Kubernetes deployments where pods receive SIGTERM before being killed.

### 9.1 Shutdown Sequence

1. **Signal received**: Mark service as unhealthy (readiness probe fails, k8s stops sending traffic)
2. **Drain period**: Wait for in-flight requests/messages to complete (configurable timeout, default 30s)
3. **Cleanup hooks**: Execute registered shutdown hooks in reverse registration order
4. **Close connections**: Disconnect from databases, flush caches, close message queue consumers
5. **Final flush**: Flush any remaining logs and metrics
6. **Exit**: Process exits with appropriate status code

### 9.2 Shutdown Hooks

Modules and implementations can register cleanup callbacks:

```python
async def run(ctx: PlatformContext):
    buffer = EventBuffer()
    ctx.lifecycle.on_shutdown(buffer.flush)
    ctx.lifecycle.on_shutdown(lambda: ctx.log.info("Module shutting down"))
```

---

## 10. Testing Strategy

### 10.1 Test Layers

| Layer | Tools | What It Tests |
|-------|-------|---------------|
| Unit tests | pytest + mocks | Module business logic in isolation, using MemoryDatabase, MemoryCache, etc. |
| Integration tests | pytest + testcontainers | Interface implementations against real infrastructure (Postgres, Redis, Kafka) |
| End-to-end tests | pytest + testcontainers + subprocess | Full module invocation via CLI with real infrastructure |

### 10.2 Testcontainers Setup

Integration and E2E tests use **testcontainers-python** to spin up real infrastructure in Docker. Shared fixtures in `tests/conftest.py` provide:

- **postgres_container**: PostgreSQL instance with migrations applied
- **redis_container**: Redis instance
- **kafka_container**: Kafka broker (using confluentinc/cp-kafka)
- **minio_container**: MinIO instance with test bucket created

Containers are scoped to the test session by default (one set of containers for all tests) to minimize startup overhead. Tests that need isolation can request function-scoped containers.

### 10.3 End-to-End Test Pattern

E2E tests invoke modules exactly as the CLI would, using subprocess:

```python
def test_stream_ingest_e2e(postgres_url, kafka_url, minio_url):
    # Publish test events to Kafka
    publish_test_events(kafka_url, topic="test-events", count=100)

    # Run the module via CLI (same invocation as production)
    result = subprocess.run([
        "python", "-m", "platform", "run", "stream_ingest",
        "--db", "postgres",
        "--fs", "minio",
        "--mq", "kafka",
        "--topic", "test-events",
        "--batch-size", "50",
    ], env={
        "DB_POSTGRES_URL": postgres_url,
        "MQ_KAFKA_BOOTSTRAP_SERVERS": kafka_url,
        "FS_MINIO_ENDPOINT": minio_url,
        ...
    }, timeout=60)

    assert result.returncode == 0

    # Verify results in Postgres
    assert count_rows(postgres_url, "ingested_events") == 100

    # Verify files in MinIO
    assert file_exists(minio_url, "data-lake/test-events/")
```

### 10.4 Contract Tests

When modules communicate via message queues, contract tests verify that producer and consumer agree on message schemas. These live alongside integration tests and use the MemoryQueue implementation:

- **Producer test**: Verify that published messages match the declared schema
- **Consumer test**: Verify that the consumer can parse and process messages matching the schema
- **Schema files**: JSON Schema files in each module that declares message formats

---

## 11. Observability

### 11.1 Structured Logging

All log output is structured (JSON in production, pretty-printed locally). Every log entry contains:

- **timestamp**: ISO 8601
- **level**: DEBUG, INFO, WARN, ERROR
- **module**: Which module emitted the log
- **correlation_id**: Unique ID for tracing a request/event through the system
- **message**: Human-readable message
- **context**: Arbitrary key-value pairs from the caller

Correlation IDs are generated at ingestion boundaries (API request, message consumption) and propagated through all downstream operations via context variables.

### 11.2 Metrics

Each module declares its metrics in code using the metrics interface. Standard metrics are automatically collected by the platform:

- `platform.module.startup_time_ms` — Time to initialize the module
- `platform.module.uptime_seconds` — Gauge for service modules
- `platform.db.query_duration_ms` — Histogram of database query times
- `platform.mq.messages_processed` — Counter of messages consumed
- `platform.mq.processing_duration_ms` — Histogram of message processing time
- `platform.cache.hit_rate` — Gauge of cache hit ratio

### 11.3 Distributed Tracing (v2)

In v2, the platform will integrate OpenTelemetry for distributed tracing. The interface is designed to be forward-compatible: the NoopMetrics and structured logging already propagate correlation IDs that can be upgraded to trace/span IDs.

---

## 12. Database Migrations

The platform includes a migration system integrated into the module lifecycle. Migrations run automatically before module startup (configurable) or can be run manually.

### 12.1 Migration Structure

```
platform/migrations/
├── versions/
│   ├── 001_create_events_table.py
│   ├── 002_add_user_index.py
│   └── 003_create_metrics_table.py
└── runner.py
```

Each migration file contains `up()` and `down()` functions. The migration runner tracks applied migrations in a `_migrations` table. Migrations are idempotent and run inside transactions where supported.

### 12.2 Running Migrations

```bash
# Auto-run before module startup (default for dev)
python -m platform run batch_etl --auto-migrate

# Manual migration commands
python -m platform migrate up        # Apply all pending
python -m platform migrate down 1    # Roll back last N
python -m platform migrate status    # Show applied/pending
```

---

## 13. Local Orchestration

A `docker-compose.yml` provides the full infrastructure stack for local development. This lets developers run the complete platform on their laptop.

### 13.1 Services

| Service | Image | Ports |
|---------|-------|-------|
| PostgreSQL | postgres:16 | 5432 |
| Redis | redis:7 | 6379 |
| Kafka + Zookeeper | confluentinc/cp-kafka | 9092 |
| MinIO | minio/minio | 9000, 9001 (console) |
| Prometheus | prom/prometheus | 9090 |
| Grafana | grafana/grafana | 3000 |

Starting the full stack:

```bash
make infra-up      # docker compose up -d
make infra-down    # docker compose down
make infra-reset   # down + remove volumes + up
```

---

## 14. Deployment

### 14.1 Docker Image

A single Docker image contains the entire platform. The module to run is selected at container startup via the CMD/entrypoint:

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY . .
RUN pip install -e .
ENTRYPOINT ["python", "-m", "platform", "run"]
```

```bash
# Run different modules from the same image:
docker run platform stream_ingest --topic user-events
docker run platform batch_etl --date 2026-01-15
docker run platform api_gateway
```

### 14.2 Kubernetes

Each module deployment gets its own Kubernetes manifest in `deploy/k8s/`. Service modules use Deployments; job modules use CronJobs or Jobs.

- **ConfigMaps**: Non-sensitive configuration (log level, feature flags)
- **Secrets**: Credentials and connection strings
- **Health probes**: Mapped to the `/health/*` endpoints
- **Resource limits**: CPU and memory limits per module
- **HPA**: Horizontal pod autoscaler for service modules based on custom metrics

---

## 15. Dev Container

The project includes a `.devcontainer` configuration for VS Code / GitHub Codespaces. Opening the project in a dev container provides a fully configured development environment with no local setup required.

### 15.1 What the Dev Container Provides

- **Python 3.12** with all project dependencies pre-installed
- **Docker-in-Docker** for running testcontainers and docker-compose
- **Pre-installed tools**: ruff, mypy, pytest, make
- **VS Code extensions**: Python, Docker, GitLens, TOML
- **Port forwarding**: Automatic forwarding for all infrastructure services
- **Post-create script**: Runs `make setup` to initialize the environment

### 15.2 devcontainer.json

```json
{
  "name": "Data Platform Template",
  "build": { "dockerfile": "Dockerfile" },
  "features": {
    "ghcr.io/devcontainers/features/docker-in-docker:2": {}
  },
  "forwardPorts": [5432, 6379, 9092, 9000, 9090, 3000],
  "postCreateCommand": "make setup",
  "customizations": {
    "vscode": {
      "extensions": [
        "ms-python.python",
        "ms-azuretools.vscode-docker",
        "tamasfe.even-better-toml"
      ]
    }
  }
}
```

---

## 16. Developer Tooling

### 16.1 Makefile Commands

| Command | Description |
|---------|-------------|
| `make setup` | Install dependencies, create `.env/local.env` from template, run infra-up |
| `make run module=X` | Run a module with local defaults (`--env local`) |
| `make test` | Run all unit tests |
| `make test-integration` | Run integration tests with testcontainers |
| `make test-e2e` | Run end-to-end tests with testcontainers |
| `make lint` | Run ruff check + mypy type checking |
| `make format` | Run ruff format |
| `make scaffold name=X` | Generate a new module from template |
| `make infra-up` | Start all infrastructure containers |
| `make infra-down` | Stop all infrastructure containers |
| `make infra-reset` | Reset infrastructure (remove volumes) |
| `make migrate` | Run database migrations |
| `make docker-build` | Build the production Docker image |

### 16.2 Module Scaffolding

The scaffold command generates a new module with all boilerplate:

```bash
make scaffold name=my_new_module
```

This creates:

```
modules/my_new_module/
├── module.json      # Pre-filled descriptor template
├── __init__.py
├── main.py          # Entry point with run(ctx) stub
└── tests/
    ├── __init__.py
    └── test_main.py # Test template with memory implementations
```

### 16.3 Pre-commit Hooks

- **ruff check**: Linting
- **ruff format --check**: Formatting verification
- **mypy**: Type checking
- **module.json schema validation**: Ensures all module descriptors are valid

---

## 17. Example Modules

The template ships with several example modules that demonstrate different patterns and module types.

### 17.1 Stream Ingest (Service)

A long-running service that consumes events from a Kafka topic, performs basic transformation, and writes batched output to the data lake (file system) while recording metadata in the database. Demonstrates: message queue consumption, batch writing, structured logging, graceful shutdown with in-flight batch flushing.

### 17.2 Batch ETL (Job)

A one-time job that reads raw data from the data lake, applies transformations (deduplication, schema validation, type casting), and writes cleaned data to the warehouse (database). Demonstrates: parameterized jobs (`--date` flag), file system reading, bulk database inserts, progress logging, exit codes.

### 17.3 API Gateway (Service)

A REST API (using aiohttp) that serves data from the database and cache. Demonstrates: HTTP service with health checks, cache-aside pattern, request logging with correlation IDs, metrics collection.

### 17.4 Data Quality (Job)

A job that runs quality checks against the warehouse: null rate checks, uniqueness constraints, freshness checks, and custom SQL assertions. Demonstrates: database querying, structured result reporting, non-zero exit code on check failures, module-specific arguments for check configuration.

### 17.5 Metric Aggregator (Worker)

A worker that pulls raw events from a queue, computes time-windowed aggregations (counts, sums, averages), and writes rollup rows to the database. Demonstrates: worker pattern with continuous queue consumption, windowed aggregation, database upserts.

---

## 18. V2 Roadmap

The following features are planned for v2 and are designed to be additive (no breaking changes to v1 modules):

### 18.1 Circuit Breaker Pattern

An interface for external calls with automatic retry, exponential backoff, and circuit-breaking. Implementations: configurable thresholds for open/half-open/closed states. Integrates with the metrics interface to track circuit state transitions.

### 18.2 Feature Flags

A simple feature flag interface with implementations backed by environment variables (v2 baseline), JSON files, or external services (LaunchDarkly, Unleash). Modules can gate behavior on feature flags without code changes.

### 18.3 Distributed Tracing

Full OpenTelemetry integration: automatic span creation for database queries, message queue operations, and HTTP requests. Trace context propagation through message queues. Export to Jaeger or OTLP-compatible backends.

### 18.4 Schema Registry

A centralized schema registry for message formats (JSON Schema or Avro). Producers validate outgoing messages; consumers validate incoming messages. Schema evolution rules (backward/forward compatibility) enforced on registration.

### 18.5 RBAC / Service Auth

Authentication and authorization for service-to-service communication. Implementations for mTLS (production) and token-based auth (development). Integrates with the API Gateway module for request-level authorization.

### 18.6 Performance Benchmarks

A benchmark harness per module that runs standardized workloads and reports throughput/latency metrics. Integrated into CI to detect performance regressions. Results stored historically for trend analysis.

---

## 19. Implementation Plan

Recommended implementation order, optimized for getting a working demo as quickly as possible while building foundations correctly.

### Phase 1: Foundation

- Project structure, `pyproject.toml`, Makefile
- Interface definitions (all ABCs with docstrings)
- Memory implementations for all interfaces (enables immediate testing)
- CLI runner with `module.json` parsing and argument validation
- DI container and wiring logic
- `.env` loader with environment precedence

### Phase 2: First Working Module

- Batch ETL module (simplest to verify end-to-end)
- `LocalFileSystem` implementation
- `PostgresDatabase` implementation
- Database migration system
- Unit tests with memory implementations

### Phase 3: Infrastructure

- `docker-compose.yml` with all services
- Dev container configuration
- Testcontainers fixtures in `conftest.py`
- Integration tests for Postgres and LocalFileSystem implementations
- E2E test for Batch ETL module

### Phase 4: Streaming & Services

- `KafkaQueue` implementation
- Stream Ingest module
- API Gateway module
- Health check system
- Graceful shutdown
- `RedisCache` implementation

### Phase 5: Polish

- Remaining modules (Data Quality, Metric Aggregator)
- S3/MinIO file system implementations
- `CassandraDatabase` implementation
- Structured logging (JSON + Pretty implementations)
- Metrics (Prometheus + Memory implementations)
- Module scaffolding CLI
- Pre-commit hooks
- Kubernetes manifests
- README with architecture diagrams

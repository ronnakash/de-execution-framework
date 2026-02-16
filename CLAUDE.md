# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Data Platform Execution Framework — a Python monorepo template for building data platforms with pluggable infrastructure implementations. Currently in the **design/specification phase** with two detailed spec documents but no source code yet.

## Key Spec Documents

- `data_platform_design_doc.md` — Full architecture: interfaces, DI container, module system, CLI, health checks, graceful shutdown, migrations, logging, metrics, testing strategy (~850 lines)
- `data_platform_phase2_spec.md` — Phase 2 implementation spec: Batch ETL module, PostgresDatabase, LocalFileSystem, migration system (~854 lines)

**Read these documents before implementing anything.** They contain precise interface signatures, implementation details, error handling rules, and testing requirements.

## Architecture

### Core Pattern: Interface-Driven Dependency Injection

Every external dependency (database, filesystem, cache, message queue, logging, metrics, secrets) is accessed through an abstract Python interface (ABC). At runtime, a DI container selects concrete implementations based on CLI flags (`--db postgres`, `--fs local`, etc.). Modules receive a `PlatformContext` object with all wired interfaces.

This enables the same code to run with in-memory implementations (local dev/unit tests), testcontainers (integration tests), or real infrastructure (production) — only configuration changes, never code.

### Planned Directory Structure

```
platform/
├── interfaces/          # ABCs for all infrastructure (7 interfaces)
├── implementations/     # Concrete implementations grouped by interface
├── config/              # DI container, environment loading
├── cli/                 # Entry point, argument parsing, module routing
├── migrations/          # Migration runner and version files
└── shared/              # Cross-cutting utilities (correlation IDs, etc.)
modules/
├── batch_etl/           # Phase 2 first module (Job type)
│   ├── module.json      # Module descriptor with argument schema
│   ├── main.py          # async def run(context: PlatformContext) -> int
│   └── tests/
```

### Module Types

- **Service**: Long-running, exposes health check endpoints, graceful shutdown
- **Job**: Runs once with parameters, exits with status code
- **Worker**: Long-running, pulls work from a queue

Modules never import each other directly — they communicate via message queues or databases only.

### Seven Core Interfaces

DatabaseInterface, FileSystemInterface, CacheInterface, MessageQueueInterface, LoggingInterface, MetricsInterface, SecretsInterface — each with multiple concrete implementations (real + memory/mock).

## Planned Build/Test/Run Commands

These are specified in the design docs but not yet implemented:

```bash
make setup              # Install deps, create .env, start infra
make run module=X       # Run a module with local defaults
make test               # Unit tests (memory implementations, no Docker)
make test-integration   # Integration tests with testcontainers
make lint               # ruff check + mypy
make format             # ruff format
make scaffold name=X    # Generate new module boilerplate
make infra-up           # docker-compose up (Postgres, Redis, Kafka, MinIO, etc.)
make migrate            # Run database migrations

# CLI entry point
python -m platform run <module_name> [--db postgres] [--fs local] [--env local] [module args]
```

## Tech Stack (Specified)

- Python 3.12+, asyncio throughout (async-first)
- asyncpg (Postgres), boto3/minio (S3), redis, confluent-kafka
- pytest + testcontainers for testing
- ruff + mypy for linting/type checking
- aiohttp for HTTP services
- prometheus-client for metrics

## Implementation Rules from Specs

- All module entry points: `async def run(context: PlatformContext) -> int`
- Interface implementations receive `SecretsInterface` to read their own config
- Database operations use connection pools, never single connections
- Migrations tracked in `_migrations` table, support `up()` and `down()`
- Structured logging: every entry includes timestamp, level, module, correlation_id, message, context
- Graceful shutdown: mark unhealthy → drain in-flight → run hooks in reverse → disconnect → exit
- Module argument validation via JSON Schema subset in `module.json`
- Batch operations use configurable batch sizes, idempotent writes (`ON CONFLICT`)

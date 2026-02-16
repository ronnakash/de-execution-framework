# Data Platform Template — Phase 2 Implementation Spec

> Phase 2: First Working Module
>
> **Prerequisite**: Phase 1 (Foundation) is fully implemented and working. This means: project structure exists, all interfaces are defined as ABCs, all memory implementations work, CLI runner parses global flags and module args from `module.json`, DI container wires implementations, `.env` loader works with environment precedence.

---

## 1. Phase 2 Goals

Phase 2 delivers the first real, working module (Batch ETL) running against real infrastructure (Postgres, local file system). By the end of this phase, you can run:

```bash
# Run batch ETL against real Postgres and local files
python -m platform run batch_etl --db postgres --fs local --env local \
  --date 2026-01-15 --source-path raw/events --target-table cleaned_events

# Run batch ETL with memory implementations (already works from Phase 1)
python -m platform run batch_etl --db memory --fs memory --env local \
  --date 2026-01-15 --source-path raw/events --target-table cleaned_events
```

And all unit tests pass using memory implementations, plus the Postgres and LocalFileSystem implementations are individually tested.

---

## 2. Deliverables

| # | Deliverable | Description |
|---|-------------|-------------|
| 1 | `PostgresDatabase` implementation | Full implementation of `DatabaseInterface` using asyncpg |
| 2 | `LocalFileSystem` implementation | Full implementation of `FileSystemInterface` using local disk |
| 3 | Database migration system | Migration runner with up/down/status commands |
| 4 | Batch ETL module | Complete module with `module.json`, `main.py`, business logic |
| 5 | Unit tests | Tests for Batch ETL using memory implementations |
| 6 | Implementation tests | Tests for `PostgresDatabase` and `LocalFileSystem` individually (no testcontainers yet — that's Phase 3) |

---

## 3. PostgresDatabase Implementation

### 3.1 File Location

```
platform/implementations/postgres.py
```

### 3.2 Dependencies

```
asyncpg>=0.29.0
```

Add to `pyproject.toml` under project dependencies.

### 3.3 Interface Methods to Implement

Implement every method defined in `platform/interfaces/database.py`. The implementation must:

**Connection Management:**
- `connect()` — Create an asyncpg connection pool using `asyncpg.create_pool()`. Read connection URL from environment variable `DB_POSTGRES_URL`. Pool settings: `min_size=2`, `max_size=10` (configurable via `DB_POSTGRES_POOL_MIN` and `DB_POSTGRES_POOL_MAX` env vars).
- `disconnect()` — Close the connection pool gracefully. Must be safe to call multiple times (idempotent).
- `is_connected()` — Return `True` if the pool exists and is not closed.

**Query Execution:**
- `execute(query: str, params: list | None = None) -> int` — Execute a query, return rows affected. Use asyncpg's `execute()` with parameterized queries (`$1`, `$2` style — asyncpg uses numbered placeholders, not `%s`).
- `fetch_one(query: str, params: list | None = None) -> dict | None` — Fetch a single row as a dict. Return `None` if no rows. Use `fetchrow()` and convert the `asyncpg.Record` to a dict.
- `fetch_all(query: str, params: list | None = None) -> list[dict]` — Fetch all rows as a list of dicts.

**Transactions:**
- `transaction()` — Return an async context manager that wraps operations in a transaction. On `__aenter__`, acquire a connection from the pool and start a transaction. On `__aexit__`, commit on success, rollback on exception. The connection used within the transaction block must be the same connection (not pulled from pool per query). This requires a mechanism to pass the transaction connection through — use a `contextvars.ContextVar` to store the active transaction connection, and have `execute`/`fetch_one`/`fetch_all` check for it before acquiring from pool.

**Bulk Operations:**
- `bulk_insert(table: str, rows: list[dict]) -> int` — Insert multiple rows efficiently. Use asyncpg's `copy_records_to_table()` for best performance. Fall back to `executemany()` with an INSERT if `copy_records_to_table` fails (e.g., if the table has generated columns). Return number of rows inserted.

**Health Check:**
- `health_check() -> bool` — Execute `SELECT 1` and return `True` if it succeeds, `False` otherwise. Must not raise exceptions.

### 3.4 Important Implementation Details

- **Parameterized queries**: asyncpg uses `$1`, `$2` numbered placeholders, NOT `%s` or `?`. All query methods must use parameterized queries — never string interpolation.
- **Record to dict conversion**: asyncpg returns `Record` objects. Convert using `dict(record)`.
- **Pool lifecycle**: The pool is created in `connect()` and closed in `disconnect()`. All query methods must raise a clear error if called before `connect()`.
- **Context var for transactions**: Store the active connection in a `contextvars.ContextVar[asyncpg.Connection | None]`. Query methods check this var first; if set, use that connection instead of acquiring from pool. This ensures all queries within a `transaction()` block use the same connection.

### 3.5 Configuration

The implementation reads these environment variables (via the secrets interface):

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DB_POSTGRES_URL` | Yes | — | PostgreSQL connection URL |
| `DB_POSTGRES_POOL_MIN` | No | `2` | Minimum pool size |
| `DB_POSTGRES_POOL_MAX` | No | `10` | Maximum pool size |
| `DB_POSTGRES_STATEMENT_TIMEOUT` | No | `30000` | Statement timeout in ms |

### 3.6 Constructor Pattern

The implementation receives the secrets interface in its constructor so it can read config:

```python
class PostgresDatabase(DatabaseInterface):
    def __init__(self, secrets: SecretsInterface):
        self._secrets = secrets
        self._pool: asyncpg.Pool | None = None
        self._tx_conn: contextvars.ContextVar[asyncpg.Connection | None] = contextvars.ContextVar(
            "pg_tx_conn", default=None
        )
```

This pattern should be consistent across all real implementations — they receive the `SecretsInterface` (or the full `PlatformContext` if they need multiple interfaces, but prefer minimal dependencies).

---

## 4. LocalFileSystem Implementation

### 4.1 File Location

```
platform/implementations/local_fs.py
```

### 4.2 Dependencies

None beyond the standard library. Uses `pathlib.Path`, `os`, `shutil`.

### 4.3 Interface Methods to Implement

Implement every method defined in `platform/interfaces/filesystem.py`:

- `read(path: str) -> bytes` — Read file contents. The `path` is relative to the configured root directory. Raise `FileNotFoundError` if the file doesn't exist.
- `write(path: str, data: bytes) -> None` — Write data to a file. Create intermediate directories automatically (`mkdir -p` behavior). Overwrite if the file already exists.
- `list(prefix: str) -> list[str]` — List all files under the given prefix (recursive). Return relative paths from root. Return empty list if prefix doesn't exist.
- `delete(path: str) -> bool` — Delete a file. Return `True` if deleted, `False` if it didn't exist. Never raise on missing file.
- `exists(path: str) -> bool` — Check if a file exists.
- `get_signed_url(path: str, expiry: int = 3600) -> str` — For local filesystem, return a `file://` URI. The `expiry` parameter is ignored but accepted for interface compatibility.

**Health Check:**
- `health_check() -> bool` — Check that the root directory exists and is writable. Create it if it doesn't exist.

### 4.4 Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `FS_LOCAL_ROOT` | No | `/tmp/data-platform` | Root directory for all file operations |

### 4.5 Important Implementation Details

- **All paths are relative**: Methods receive paths like `raw/events/2026-01-15/data.parquet`. The implementation prepends the root directory. Never allow absolute paths or path traversal (`..`). Validate and reject any path containing `..`.
- **Atomic writes**: Write to a temporary file first, then rename. This prevents partial writes from corrupting data if the process crashes mid-write. Use `tempfile.NamedTemporaryFile` in the same directory, write, flush, `os.fsync()`, then `os.rename()`.
- **Directory creation**: `write()` must create parent directories automatically.
- **Thread safety**: Use `pathlib.Path` operations which are generally safe. No shared mutable state needed.

### 4.6 Constructor Pattern

```python
class LocalFileSystem(FileSystemInterface):
    def __init__(self, secrets: SecretsInterface):
        root = secrets.get_or_default("FS_LOCAL_ROOT", "/tmp/data-platform")
        self._root = Path(root)
```

---

## 5. Database Migration System

### 5.1 File Structure

```
platform/migrations/
├── __init__.py
├── runner.py              # Migration runner logic
└── versions/
    ├── __init__.py
    └── (migration files go here)
```

### 5.2 Migration File Format

Each migration is a Python file with a specific naming convention and two required functions:

```python
# platform/migrations/versions/001_create_events_table.py

"""Create the events table for raw event storage."""

async def up(conn) -> None:
    """Apply this migration."""
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id BIGSERIAL PRIMARY KEY,
            event_type VARCHAR(255) NOT NULL,
            payload JSONB NOT NULL,
            source VARCHAR(255),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            processed_at TIMESTAMPTZ
        )
    """)
    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_events_type ON events (event_type)
    """)
    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_events_created ON events (created_at)
    """)

async def down(conn) -> None:
    """Reverse this migration."""
    await conn.execute("DROP TABLE IF EXISTS events CASCADE")
```

**Naming convention**: `NNN_description.py` where NNN is a zero-padded sequential number. The runner sorts by filename to determine order.

### 5.3 Migration Runner

The runner (`platform/migrations/runner.py`) manages migration state:

**Tracking table**: On first run, create a `_migrations` table:

```sql
CREATE TABLE IF NOT EXISTS _migrations (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
```

**Commands**:

- `up(db: DatabaseInterface, target: int | None = None)` — Apply all pending migrations (or up to `target` number). Each migration runs inside its own transaction. On failure, that migration is rolled back but previously applied migrations remain.
- `down(db: DatabaseInterface, count: int = 1)` — Roll back the last `count` migrations. Each rollback runs in its own transaction.
- `status(db: DatabaseInterface) -> list[MigrationStatus]` — Return a list of all migrations with their status (applied/pending) and applied timestamp if applicable.

**Discovery**: The runner scans `platform/migrations/versions/` for Python files matching the naming pattern, imports them, and validates they have `up` and `down` functions.

### 5.4 CLI Integration

Add migration subcommands to the CLI runner:

```bash
python -m platform migrate up                    # Apply all pending
python -m platform migrate up --target 3         # Apply up to migration 003
python -m platform migrate down                  # Roll back last 1
python -m platform migrate down --count 3        # Roll back last 3
python -m platform migrate status                # Show migration status table
python -m platform migrate create "add_users"    # Create new migration file
```

The `migrate` commands need the `--db` and `--env` global flags to know which database to run against:

```bash
python -m platform migrate up --db postgres --env local
python -m platform migrate status --db postgres --env test
```

### 5.5 Auto-Migrate Flag

When running a module with `--auto-migrate`, the startup sequence (Phase 1, section 4.3, step 6) calls `migrate up` before executing the module. This is the default for `--env local` and `--env test`, but disabled for `--env staging` and `--env production` (where migrations should be run explicitly).

### 5.6 Initial Migrations for Phase 2

Create these migrations to support the Batch ETL module:

**001_create_migrations_table.py** — Creates the `_migrations` tracking table itself (bootstrap migration).

**002_create_events_table.py** — Raw events table:

```sql
CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    source VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ
);
CREATE INDEX idx_events_type ON events (event_type);
CREATE INDEX idx_events_created ON events (created_at);
```

**003_create_cleaned_events_table.py** — Cleaned/transformed events table (Batch ETL output):

```sql
CREATE TABLE cleaned_events (
    id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    source VARCHAR(255) NOT NULL,
    event_date DATE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (event_type, payload, event_date)
);
CREATE INDEX idx_cleaned_events_date ON cleaned_events (event_date);
CREATE INDEX idx_cleaned_events_type ON cleaned_events (event_type, event_date);
```

---

## 6. Batch ETL Module

### 6.1 File Structure

```
modules/batch_etl/
├── module.json
├── __init__.py
├── main.py                # Entry point
├── transformer.py         # Business logic: transformation functions
├── validator.py           # Business logic: data validation
└── tests/
    ├── __init__.py
    ├── test_main.py       # Integration test of the run() function with memory impls
    ├── test_transformer.py
    └── test_validator.py
```

### 6.2 module.json

```json
{
  "name": "batch_etl",
  "display_name": "Batch ETL Job",
  "description": "Reads raw data from the data lake, applies transformations (deduplication, validation, type casting), and writes cleaned data to the warehouse database.",
  "type": "job",
  "version": "1.0.0",
  "dependencies": {
    "interfaces": ["database", "filesystem", "logging"],
    "modules": []
  },
  "args": [
    {
      "name": "date",
      "description": "Processing date in YYYY-MM-DD format. Determines which raw data partition to read.",
      "required": true,
      "type": "string"
    },
    {
      "name": "source-path",
      "description": "Path prefix in the file system where raw data files are stored.",
      "required": true,
      "type": "string"
    },
    {
      "name": "target-table",
      "description": "Database table to write cleaned data into.",
      "required": false,
      "type": "string",
      "default": "cleaned_events"
    },
    {
      "name": "batch-size",
      "description": "Number of records to insert per database batch.",
      "required": false,
      "type": "integer",
      "default": 500
    },
    {
      "name": "dry-run",
      "description": "If true, validate and transform data but do not write to database.",
      "required": false,
      "type": "boolean",
      "default": false
    }
  ]
}
```

### 6.3 Business Logic

The Batch ETL module performs these steps:

**Step 1: Read raw data**
- List all files under `{source-path}/{date}/` in the file system
- Read each file (assume JSON lines format — one JSON object per line)
- Parse into a list of raw event dicts
- Log: number of files found, total raw records read

**Step 2: Validate**
- Each raw event must have: `event_type` (non-empty string), `payload` (dict), `source` (non-empty string)
- Events missing required fields are logged as warnings and skipped
- Events with malformed JSON in payload are logged as errors and skipped
- Log: number of valid records, number of invalid records (with breakdown by reason)

**Step 3: Transform**
- Parse and normalize `event_type` to lowercase, strip whitespace
- Ensure `payload` is a dict (if it's a JSON string, parse it)
- Extract `event_date` from the `--date` argument
- Add `created_at` timestamp

**Step 4: Deduplicate**
- Deduplicate on the combination of (`event_type`, `payload`, `event_date`) — use a hash of the serialized payload
- Log: number of duplicates removed

**Step 5: Write to database**
- Unless `--dry-run` is set, bulk insert into the target table in batches of `--batch-size`
- Use `ON CONFLICT DO NOTHING` on the unique constraint to handle duplicates that already exist in the database from previous runs (idempotent)
- Log: number of rows inserted, number of rows skipped (already existed)

**Step 6: Summary**
- Log a final summary: total raw → valid → unique → inserted
- Exit with code 0 on success, 1 if any critical errors occurred (e.g., couldn't connect to DB, couldn't read files)

### 6.4 main.py Structure

```python
"""Batch ETL module entry point."""

import json
import sys
from datetime import datetime

from platform.interfaces.database import DatabaseInterface
from platform.interfaces.filesystem import FileSystemInterface
from platform.interfaces.logging import LoggingInterface
from platform.config.container import PlatformContext

from .transformer import transform_events
from .validator import validate_events


async def run(ctx: PlatformContext) -> int:
    """
    Main entry point for the Batch ETL job.

    Returns exit code: 0 for success, 1 for failure.
    """
    log = ctx.log
    db = ctx.db
    fs = ctx.fs
    config = ctx.config

    date = config.get("date")
    source_path = config.get("source-path")
    target_table = config.get("target-table")
    batch_size = config.get("batch-size")
    dry_run = config.get("dry-run")

    log.info("Starting Batch ETL", date=date, source_path=source_path,
             target_table=target_table, dry_run=dry_run)

    # Step 1: Read raw data
    raw_events = await read_raw_data(fs, log, source_path, date)
    if not raw_events:
        log.warn("No raw data found", date=date, source_path=source_path)
        return 0

    # Step 2: Validate
    valid_events, invalid_count = validate_events(raw_events, log)

    # Step 3: Transform
    transformed = transform_events(valid_events, date)

    # Step 4: Deduplicate
    unique_events, dup_count = deduplicate(transformed)
    log.info("Deduplication complete",
             before=len(transformed), after=len(unique_events), duplicates=dup_count)

    # Step 5: Write
    inserted = 0
    if not dry_run:
        inserted = await write_to_db(db, log, target_table, unique_events, batch_size)

    # Step 6: Summary
    log.info("Batch ETL complete",
             raw=len(raw_events), valid=len(valid_events),
             unique=len(unique_events), inserted=inserted,
             invalid=invalid_count, duplicates=dup_count)

    return 0
```

### 6.5 transformer.py

Keep transformation logic in a pure module with no infrastructure dependencies. Every function takes data in, returns data out. This makes unit testing trivial.

```python
def transform_events(events: list[dict], date: str) -> list[dict]:
    """Transform validated events into the cleaned format."""
    ...

def normalize_event_type(event_type: str) -> str:
    """Lowercase and strip whitespace from event type."""
    ...

def ensure_payload_dict(payload) -> dict:
    """If payload is a JSON string, parse it. Otherwise return as-is."""
    ...

def compute_dedup_key(event: dict) -> str:
    """Compute a hash key for deduplication."""
    ...
```

### 6.6 validator.py

Validation logic is also pure — no infrastructure dependencies.

```python
@dataclass
class ValidationResult:
    valid: list[dict]
    invalid: list[dict]
    error_counts: dict[str, int]  # reason -> count

def validate_events(events: list[dict], log: LoggingInterface) -> tuple[list[dict], int]:
    """Validate raw events, return (valid_events, invalid_count)."""
    ...

def validate_single_event(event: dict) -> tuple[bool, str | None]:
    """Validate a single event. Returns (is_valid, error_reason)."""
    ...
```

---

## 7. Test Data

### 7.1 Fixture Files

Create test fixture files that the unit tests and Batch ETL module can use:

```
modules/batch_etl/tests/fixtures/
├── valid_events.jsonl          # 10 valid events
├── mixed_events.jsonl          # 7 valid + 3 invalid events
├── duplicate_events.jsonl      # 5 events with 2 duplicates
└── empty.jsonl                 # Empty file
```

**valid_events.jsonl** — each line is a JSON object:

```jsonl
{"event_type": "page_view", "payload": {"url": "/home", "user_id": "u1"}, "source": "web"}
{"event_type": "click", "payload": {"element": "signup_btn", "user_id": "u2"}, "source": "web"}
{"event_type": "purchase", "payload": {"item_id": "p100", "amount": 29.99, "user_id": "u1"}, "source": "mobile"}
...
```

**mixed_events.jsonl** — includes invalid entries:

```jsonl
{"event_type": "page_view", "payload": {"url": "/home"}, "source": "web"}
{"event_type": "", "payload": {"url": "/home"}, "source": "web"}
{"payload": {"url": "/home"}, "source": "web"}
{"event_type": "click", "payload": "not_a_dict", "source": "web"}
...
```

### 7.2 Test Data Helpers

Create a helper module that both unit tests and future E2E tests can use:

```
tests/
├── helpers/
│   ├── __init__.py
│   └── data_factory.py    # Functions to generate test events
```

```python
# tests/helpers/data_factory.py

def make_event(event_type: str = "page_view", source: str = "web", **payload_fields) -> dict:
    """Create a single valid test event."""
    ...

def make_events(count: int, **overrides) -> list[dict]:
    """Create multiple test events with sequential IDs."""
    ...

def make_invalid_event(reason: str = "missing_type") -> dict:
    """Create an invalid event for a specific failure reason."""
    ...

def events_to_jsonl(events: list[dict]) -> bytes:
    """Serialize events to JSONL bytes (for writing to filesystem)."""
    ...
```

---

## 8. Unit Tests

### 8.1 Test Coverage Requirements

Every test uses **memory implementations only** — no Docker, no external services.

**test_transformer.py:**
- `test_normalize_event_type` — verifies lowercase + strip
- `test_ensure_payload_dict_from_string` — JSON string payload is parsed
- `test_ensure_payload_dict_passthrough` — dict payload returned as-is
- `test_compute_dedup_key_deterministic` — same input produces same key
- `test_compute_dedup_key_different` — different inputs produce different keys
- `test_transform_events_adds_fields` — verifies event_date and created_at are added

**test_validator.py:**
- `test_validate_valid_event` — event with all fields passes
- `test_validate_missing_event_type` — event without event_type fails
- `test_validate_empty_event_type` — event with empty string event_type fails
- `test_validate_missing_payload` — event without payload fails
- `test_validate_missing_source` — event without source fails
- `test_validate_events_mixed` — batch with valid and invalid events returns correct split

**test_main.py:**
- `test_run_success` — full run with valid data, verify rows in memory DB
- `test_run_no_data` — run with empty file system, exits 0 with warning
- `test_run_dry_run` — run with `dry_run=True`, verify no rows written to DB
- `test_run_deduplication` — run with duplicate events, verify correct count in DB
- `test_run_idempotent` — run twice with same data, verify no duplicate rows

### 8.2 Test Setup Pattern

```python
import pytest
from platform.implementations.memory_db import MemoryDatabase
from platform.implementations.memory_fs import MemoryFileSystem
from platform.implementations.memory_logger import MemoryLogger
from tests.helpers.data_factory import make_events, events_to_jsonl


@pytest.fixture
def ctx():
    """Create a PlatformContext with all memory implementations."""
    db = MemoryDatabase()
    fs = MemoryFileSystem()
    log = MemoryLogger()
    # ... wire into a PlatformContext with config
    return context


async def test_run_success(ctx):
    # Arrange: write test data to memory filesystem
    events = make_events(10)
    await ctx.fs.write("raw/events/2026-01-15/batch_0.jsonl", events_to_jsonl(events))

    # Act: run the module
    exit_code = await run(ctx)

    # Assert
    assert exit_code == 0
    rows = await ctx.db.fetch_all("SELECT * FROM cleaned_events")
    assert len(rows) == 10
```

---

## 9. Implementation Tests (Non-Testcontainer)

These tests verify the Postgres and LocalFileSystem implementations work correctly. Since testcontainers are Phase 3, these tests run against locally-available infrastructure or are skipped if not available.

### 9.1 Strategy

Use `pytest.mark.skipif` to skip if the required infrastructure isn't running. This lets them run in the devcontainer (where docker-compose provides Postgres) and be skipped in CI until Phase 3 adds testcontainers.

```python
import pytest
import asyncio
import os

POSTGRES_AVAILABLE = os.environ.get("DB_POSTGRES_URL") is not None

@pytest.mark.skipif(not POSTGRES_AVAILABLE, reason="Postgres not available")
class TestPostgresDatabase:
    ...
```

### 9.2 PostgresDatabase Tests

```
tests/integration/test_postgres.py
```

- `test_connect_disconnect` — connects and disconnects without error
- `test_execute_create_table` — creates a table
- `test_fetch_one` — inserts a row, fetches it back
- `test_fetch_all` — inserts multiple rows, fetches all
- `test_fetch_one_no_results` — returns None when no rows match
- `test_transaction_commit` — operations within transaction() are visible after
- `test_transaction_rollback` — operations are rolled back on exception
- `test_bulk_insert` — inserts 100 rows, verifies count
- `test_health_check_connected` — returns True when connected
- `test_health_check_disconnected` — returns False when not connected
- `test_parameterized_queries` — verifies SQL injection is prevented

### 9.3 LocalFileSystem Tests

```
tests/integration/test_local_fs.py
```

- `test_write_and_read` — write bytes, read them back
- `test_write_creates_directories` — write to nested path, verify dirs created
- `test_read_nonexistent` — raises FileNotFoundError
- `test_list_files` — write multiple files, list returns all
- `test_list_empty_prefix` — returns empty list
- `test_delete_existing` — returns True
- `test_delete_nonexistent` — returns False
- `test_exists` — True for existing file, False for nonexistent
- `test_overwrite` — writing to existing path overwrites content
- `test_path_traversal_blocked` — paths containing `..` are rejected
- `test_atomic_write` — (verify temp file pattern works correctly)
- `test_health_check` — returns True, creates root dir if needed

---

## 10. DI Container Updates

### 10.1 Register New Implementations

Update the registry in `platform/config/container.py` to include the new implementations:

```python
# Add to REGISTRY:
"database": {
    "postgres": "platform.implementations.postgres.PostgresDatabase",
    "memory": "platform.implementations.memory_db.MemoryDatabase",
    # cassandra comes in Phase 5
},
"filesystem": {
    "local": "platform.implementations.local_fs.LocalFileSystem",
    "memory": "platform.implementations.memory_fs.MemoryFileSystem",
    # s3, minio come in Phase 5
},
```

Use string paths (lazy imports) so that importing the container doesn't pull in asyncpg/boto3/etc. unless that implementation is actually selected. Resolve with `importlib.import_module()` at wiring time.

### 10.2 Lifecycle Integration

Update the startup sequence to:

1. After building the DI container, call `await db.connect()` if a database implementation was created
2. Register `db.disconnect()` as a shutdown hook
3. If `--auto-migrate` is set (or env is local/test), run `migrate up` after connect

---

## 11. CLI Updates

### 11.1 Add `migrate` Subcommand

The Phase 1 CLI supports `python -m platform run <module>`. Add:

```bash
python -m platform migrate <command> [flags]
```

Where `<command>` is one of: `up`, `down`, `status`, `create`.

The `migrate` subcommand needs the same global flags as `run` (at minimum `--db` and `--env`) since it needs to connect to a database.

### 11.2 Add `--auto-migrate` Flag

Add `--auto-migrate` as a global flag (default: `True` for local/test envs, `False` for staging/production).

---

## 12. .env Updates

### 12.1 Update local.env

Add Postgres connection URL to `.env/local.env`:

```bash
# Database
DB_POSTGRES_URL=postgresql://platform:platform@localhost:5432/platform

# File System
FS_LOCAL_ROOT=/tmp/data-platform

# Logging
LOG_LEVEL=DEBUG
```

### 12.2 Create test.env

```bash
# Database (testcontainers will override this, but provide a default)
DB_POSTGRES_URL=postgresql://platform:platform@localhost:5432/platform_test

# File System
FS_LOCAL_ROOT=/tmp/data-platform-test

# Logging
LOG_LEVEL=DEBUG
```

---

## 13. Makefile Updates

Add these targets:

```makefile
# Run a specific module
run:
	python -m platform run $(module) --env local $(args)

# Database migrations
migrate-up:
	python -m platform migrate up --db postgres --env local

migrate-down:
	python -m platform migrate down --db postgres --env local

migrate-status:
	python -m platform migrate status --db postgres --env local

migrate-create:
	python -m platform migrate create $(name) --db postgres --env local

# Run unit tests only (no infrastructure needed)
test-unit:
	pytest modules/ -v --tb=short

# Run integration tests (needs local infrastructure)
test-impl:
	pytest tests/integration/ -v --tb=short
```

---

## 14. Acceptance Criteria

Phase 2 is complete when all of the following are true:

| # | Criterion | How to Verify |
|---|-----------|---------------|
| 1 | Batch ETL runs with memory implementations | `python -m platform run batch_etl --db memory --fs memory --date 2026-01-15 --source-path raw/events` completes with exit code 0 |
| 2 | Batch ETL runs with Postgres + local FS | `python -m platform run batch_etl --db postgres --fs local --env local --date 2026-01-15 --source-path raw/events` completes with exit code 0 (requires Postgres running) |
| 3 | Migrations work | `python -m platform migrate up --db postgres --env local` creates tables, `status` shows them as applied |
| 4 | All unit tests pass | `make test-unit` passes |
| 5 | Implementation tests pass | `make test-impl` passes (with Postgres running) |
| 6 | Dry run works | Running with `--dry-run` processes data but writes zero rows |
| 7 | Deduplication works | Running twice with same data produces no additional rows |
| 8 | Invalid data is handled | Mixed valid/invalid data file processes valid events and logs invalid ones |
| 9 | --help works | `python -m platform run batch_etl --help` shows module description and all arguments from module.json |
| 10 | Auto-migrate works | Running with `--auto-migrate` applies pending migrations before module execution |

---

## 15. Implementation Order

Recommended order to minimize blocked work:

1. **LocalFileSystem** implementation + tests (no external deps, fastest to verify)
2. **PostgresDatabase** implementation + tests (needs Postgres running)
3. **Migration system** (runner + CLI subcommand + initial migrations)
4. **.env updates** + DI container registration + lifecycle hooks
5. **Batch ETL module**: validator.py → transformer.py → main.py (build bottom-up)
6. **Test data** fixtures + factory helpers
7. **Unit tests** for Batch ETL
8. **Integration tests** for implementations
9. **Makefile updates**
10. **Acceptance criteria verification** — run through all 10 checks

# Phase 16: Docker Containerization

**Goal:** Build a single Docker image for all application modules, with a root-level `docker-compose.yml` that uses profiles to support production, CI, and local development workflows.

**Status:** Planning

---

## Context

The project currently uses `.devcontainer/docker-compose.yml` exclusively for infrastructure services (Postgres, Redis, Kafka, ClickHouse, MinIO, Prometheus, Loki, Grafana). Application modules run as local Python processes, either individually via `make run` or all at once via `scripts/dev.sh`. There is no CI/CD pipeline and no way to deploy the application as containers.

The framework's CLI already supports a clean per-module invocation pattern:

```bash
python -m de_platform run <module_name> --db name=impl --mq impl --cache impl ...
```

This maps naturally to a containerization model where one Docker image contains the full platform code and each container runs a single module by varying the `CMD`.

### Current Module Inventory

| Module | Type | Default Port | Dependencies | CLI Flags |
|---|---|---|---|---|
| `rest_starter` | service | 8001 | mq | `--mq kafka --port 8001` |
| `normalizer` | service | -- | db, cache, mq | `--db warehouse=postgres --cache redis --mq kafka` |
| `persistence` | service | -- | db, fs, mq | `--db clickhouse=clickhouse --fs minio --mq kafka` |
| `algos` | service | -- | cache, mq | `--cache redis --mq kafka` |
| `alert_manager` | service | 8007 | db, mq, cache | `--db alerts=postgres --mq kafka --cache redis --port 8007` |
| `data_api` | service | 8002 | db | `--db events=clickhouse --port 8002` |
| `client_config` | service | 8003 | db, cache | `--db client_config=postgres --cache redis --port 8003` |
| `auth` | service | 8004 | db | `--db auth=postgres --port 8004` |
| `data_audit` | service | 8005 | db, mq | `--db data_audit=postgres --mq kafka --port 8005` |
| `task_scheduler` | service | 8006 | db | `--db task_scheduler=postgres --port 8006` |

### Migration Namespaces

Seven Postgres migration namespaces must be applied before application modules start:

1. `warehouse` (4 migrations)
2. `alerts` (1 migration)
3. `client_config` (3 migrations)
4. `auth` (3 migrations)
5. `alert_manager` (1 migration)
6. `data_audit` (1 migration)
7. `task_scheduler` (1 migration)

### Existing Infrastructure Configuration

The devcontainer uses these credentials (from `.envfiles/devcontainer.env`):

- **Postgres:** user=`platform`, password=`platform`, db=`platform`, port 5432
- **ClickHouse:** user=`default`, password=`clickhouse`, db=`fraud_pipeline`, port 8123
- **Redis:** `redis://redis:6379/0`
- **Kafka:** `kafka:29092` (internal), `localhost:9092` (host)
- **MinIO:** user=`minioadmin`, password=`minioadmin`, port 9000

The new `docker-compose.yml` should use these same credentials for consistency.

---

## Implementation Details

### Step 1: Application Dockerfile

**New file:** `Dockerfile` (repository root)

Build a single image containing the full `de_platform` package with all infrastructure dependencies installed. Each container invokes one module via the entrypoint.

```dockerfile
FROM python:3.12-slim AS base

# System dependencies for building native extensions (asyncpg, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy project metadata and source first for better layer caching.
# When only application code changes, pip install layer is cached.
COPY pyproject.toml README.md ./
COPY de_platform/ de_platform/

# Install with all infra dependencies (asyncpg, redis, confluent-kafka, minio, clickhouse-connect)
RUN pip install --no-cache-dir '.[infra]'

# Default entrypoint: the platform CLI
ENTRYPOINT ["python", "-m", "de_platform"]
# Default command: show help
CMD ["--help"]
```

**Design decisions:**

- Single-stage build is sufficient since there are no compiled frontend assets in the image (the UI is pre-built and copied into `de_platform/modules/data_api/static/`).
- The `[infra]` optional group is required because all modules in Docker run against real infrastructure.
- The `[dev]` group (pytest, ruff, mypy) is deliberately excluded from the production image.
- `gcc` and `libpq-dev` are needed at install time for `asyncpg`. They could be removed in a multi-stage build to save ~50MB, but this is a future optimization.

**Usage pattern:**

```bash
# Build
docker build -t de-platform:latest .

# Run a single module
docker run de-platform:latest run normalizer --db warehouse=postgres --mq kafka --cache redis

# Run migrations
docker run de-platform:latest migrate up warehouse --db postgres
```

### Step 2: .dockerignore

**New file:** `.dockerignore`

Exclude everything not needed at runtime to keep the build context small and avoid leaking secrets.

```
.git
.venv
.devcontainer
__pycache__
*.pyc
.pytest_cache
.ruff_cache
node_modules
ui/node_modules
ui/
.env
.env.*
.envfiles/
reports/
*.egg-info
dist/
build/
.claude/
docs/
tests/
grafana/
prometheus/
scripts/
Makefile
*.md
!README.md
.dev-logs/
.dev-pids
```

### Step 3: Docker Compose File

**New file:** `docker-compose.yml` (repository root, separate from `.devcontainer/docker-compose.yml`)

Three profiles control which services start:

| Profile | What starts | Use case |
|---|---|---|
| `infra` | Infrastructure only (postgres, redis, kafka, etc.) | Local dev, running modules as local processes, CI tests |
| `app` | Application modules + migrations init container | Running app against externally-managed infra |
| `full` | Everything (infra + app) | One-command full stack, demos, production-like environments |

#### YAML Anchors

Define shared configuration blocks to avoid repetition:

- `x-infra-env` -- environment variables for connecting to infrastructure services (using Docker DNS names)
- `x-app-common` -- shared build/image/restart/depends_on configuration for all application containers

#### Environment Variables

Use the same credential scheme as `.envfiles/devcontainer.env` for consistency. The key environment variables for all application containers:

```yaml
x-infra-env: &infra-env
  DB_POSTGRES_HOST: postgres
  DB_POSTGRES_PORT: "5432"
  DB_POSTGRES_USER: platform
  DB_POSTGRES_PASSWORD: platform
  DB_POSTGRES_DATABASE: platform
  DB_CLICKHOUSE_HOST: clickhouse
  DB_CLICKHOUSE_PORT: "8123"
  DB_CLICKHOUSE_DATABASE: fraud_pipeline
  DB_CLICKHOUSE_USER: default
  DB_CLICKHOUSE_PASSWORD: clickhouse
  MQ_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
  CACHE_REDIS_URL: redis://redis:6379/0
  FS_MINIO_ENDPOINT: minio:9000
  FS_MINIO_ACCESS_KEY: minioadmin
  FS_MINIO_SECRET_KEY: minioadmin
  FS_MINIO_BUCKET: de-platform
  FS_MINIO_SECURE: "false"
  JWT_SECRET: "docker-compose-jwt-secret-at-least-32-bytes!!"
```

#### Application Common Block

```yaml
x-app-common: &app-common
  build: .
  image: de-platform:latest
  restart: unless-stopped
  depends_on:
    postgres:
      condition: service_healthy
    redis:
      condition: service_healthy
    kafka:
      condition: service_healthy
  environment:
    <<: *infra-env
```

#### Infrastructure Services

All infrastructure services belong to both `infra` and `full` profiles. The definitions should match `.devcontainer/docker-compose.yml` closely but add **health checks** so application containers can use `depends_on: condition: service_healthy`.

| Service | Image | Health Check | Exposed Ports |
|---|---|---|---|
| `postgres` | `postgres:16` | `pg_isready -U platform` | 5432 |
| `redis` | `redis:7-alpine` | `redis-cli ping` | 6379 |
| `zookeeper` | `confluentinc/cp-zookeeper:7.6.0` | `echo ruok \| nc localhost 2181` | 2181 |
| `kafka` | `confluentinc/cp-kafka:7.6.0` | `kafka-topics --bootstrap-server localhost:29092 --list` | 9092 |
| `clickhouse` | `clickhouse/clickhouse-server:latest` | `clickhouse-client --password clickhouse -q 'SELECT 1'` | 8123, 9009 |
| `minio` | `minio/minio:latest` | `curl -f http://localhost:9000/minio/health/live` | 9000, 9001 |
| `prometheus` | `prom/prometheus:latest` | -- | 9090 |
| `loki` | `grafana/loki:3.0.0` | -- | 3100 |
| `grafana` | `grafana/grafana:latest` | -- | 3000 |

**Important details matching existing devcontainer config:**

- Postgres: `command: postgres -c max_connections=500` and db name `platform`
- Kafka: `KAFKA_NUM_PARTITIONS: 3` and listeners matching existing config
- ClickHouse: native protocol port remapped to 9009 to avoid MinIO conflict; mount `scripts/clickhouse_init.sql` for table initialization

#### Migrations Init Container

A one-shot container that runs all Postgres migrations before application modules start. Must use `restart: "no"` and iterate all seven namespaces:

```yaml
migrations:
  <<: *app-common
  profiles: [app, full]
  restart: "no"
  depends_on:
    postgres:
      condition: service_healthy
  command: >
    sh -c "
      python -m de_platform migrate up warehouse --db postgres &&
      python -m de_platform migrate up alerts --db postgres &&
      python -m de_platform migrate up client_config --db postgres &&
      python -m de_platform migrate up auth --db postgres &&
      python -m de_platform migrate up alert_manager --db postgres &&
      python -m de_platform migrate up data_audit --db postgres &&
      python -m de_platform migrate up task_scheduler --db postgres
    "
```

**Note:** This overrides the `ENTRYPOINT` with `sh -c` because we need to chain multiple migration commands. An alternative is to create a small shell script at `scripts/migrate_all.sh` and copy it into the image.

#### ClickHouse Init Container

ClickHouse tables are created via `scripts/clickhouse_init.sql` which is mounted into the ClickHouse container's init directory. This happens automatically when the ClickHouse container starts, matching the existing devcontainer approach. No separate init container is needed for ClickHouse.

#### Application Module Containers

All application containers belong to both `app` and `full` profiles. Each depends on `migrations` completing successfully.

The `--health-port` flag is a real global CLI flag (default 8080) that starts an HTTP health check server on the specified port. Each module container should get a unique health port to avoid conflicts. Health checks use Python's `urllib.request` since `curl` is not available in `python:3.12-slim`.

The `--log` flag should be set to `pretty` for readable container logs (or `loki` if Loki forwarding is desired).

**Service definitions:**

| Container | Command | Ports | Health Port | Extra Env |
|---|---|---|---|---|
| `rest-starter` | `run rest_starter --mq kafka --port 8001 --health-port 9100 --log pretty` | 8001 | 9100 | -- |
| `normalizer` | `run normalizer --db warehouse=postgres --cache redis --mq kafka --health-port 9101 --log pretty` | -- | 9101 | -- |
| `persistence` | `run persistence --db clickhouse=clickhouse --fs minio --mq kafka --health-port 9102 --log pretty` | -- | 9102 | -- |
| `algos` | `run algos --cache redis --mq kafka --health-port 9103 --log pretty` | -- | 9103 | -- |
| `alert-manager` | `run alert_manager --db alerts=postgres --mq kafka --cache redis --port 8007 --health-port 9104 --log pretty` | 8007 | 9104 | -- |
| `data-api` | `run data_api --db events=clickhouse --port 8002 --health-port 9105 --log pretty` | 8002 | 9105 | `ALERT_MANAGER_URL`, `CLIENT_CONFIG_URL`, `DATA_AUDIT_URL`, `TASK_SCHEDULER_URL`, `AUTH_URL` |
| `client-config` | `run client_config --db client_config=postgres --cache redis --port 8003 --health-port 9106 --log pretty` | 8003 | 9106 | -- |
| `auth` | `run auth --db auth=postgres --port 8004 --health-port 9107 --log pretty` | 8004 | 9107 | -- |
| `data-audit` | `run data_audit --db data_audit=postgres --mq kafka --port 8005 --health-port 9108 --log pretty` | 8005 | 9108 | -- |
| `task-scheduler` | `run task_scheduler --db task_scheduler=postgres --port 8006 --health-port 9109 --log pretty` | 8006 | 9109 | -- |

**Health check for each app container:**

```yaml
healthcheck:
  test: ["CMD-SHELL", "python -c \"import urllib.request; urllib.request.urlopen('http://localhost:PORT/health/startup')\""]
  interval: 10s
  timeout: 5s
  retries: 5
  start_period: 15s
```

Where `PORT` is the module's health port.

**`data-api` extra environment variables** for cross-service communication (using Docker DNS names):

```yaml
environment:
  <<: *infra-env
  ALERT_MANAGER_URL: "http://alert-manager:8007"
  CLIENT_CONFIG_URL: "http://client-config:8003"
  DATA_AUDIT_URL: "http://data-audit:8005"
  TASK_SCHEDULER_URL: "http://task-scheduler:8006"
  AUTH_URL: "http://auth:8004"
```

#### Resource Limits

All application containers should have resource limits to prevent runaway memory/CPU consumption:

```yaml
deploy:
  resources:
    limits:
      memory: 512M
      cpus: "0.5"
    reservations:
      memory: 128M
      cpus: "0.1"
```

**Note:** `deploy.resources` requires `docker compose` (v2) not `docker-compose` (v1). This is fine since v1 is deprecated.

#### Named Volumes

Persist infrastructure data across restarts:

```yaml
volumes:
  pgdata:
  clickhousedata:
  miniodata:
  prometheusdata:
  grafanadata:
```

### Step 4: Prometheus Configuration Update

The existing `prometheus/prometheus.yml` has a single scrape target `app:9091`. With containerized modules, each module exposes its own metrics port. Update the prometheus config to scrape all module health ports.

**File:** `prometheus/prometheus.yml`

Add scrape targets for all application modules by their Docker DNS names and health ports (9100-9109). The health check server also serves `/metrics`.

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: "rest-starter"
    static_configs:
      - targets: ["rest-starter:9100"]
  - job_name: "normalizer"
    static_configs:
      - targets: ["normalizer:9101"]
  - job_name: "persistence"
    static_configs:
      - targets: ["persistence:9102"]
  - job_name: "algos"
    static_configs:
      - targets: ["algos:9103"]
  - job_name: "alert-manager"
    static_configs:
      - targets: ["alert-manager:9104"]
  - job_name: "data-api"
    static_configs:
      - targets: ["data-api:9105"]
  - job_name: "client-config"
    static_configs:
      - targets: ["client-config:9106"]
  - job_name: "auth"
    static_configs:
      - targets: ["auth:9107"]
  - job_name: "data-audit"
    static_configs:
      - targets: ["data-audit:9108"]
  - job_name: "task-scheduler"
    static_configs:
      - targets: ["task-scheduler:9109"]
```

**Note:** This requires that the health check server exposes Prometheus metrics at `/metrics` on the health port. Verify this is the case in the `HealthCheckServer` implementation. If metrics are served on a separate port, the config needs adjustment.

### Step 5: Makefile Targets

**File:** `Makefile`

Add new targets for Docker workflows. These should coexist with the existing `infra-up`/`infra-down` targets which use the devcontainer compose file.

```makefile
# ── Docker ─────────────────────────────────────────────────────────
docker-build:
	docker build -t de-platform:latest .

docker-up:
	docker compose --profile full up -d

docker-down:
	docker compose --profile full down

docker-infra:
	docker compose --profile infra up -d

docker-app:
	docker compose --profile app up -d

docker-logs:
	docker compose --profile full logs -f

docker-ps:
	docker compose --profile full ps

docker-clean:
	docker compose --profile full down -v
```

**Relationship to existing targets:**

- `make infra-up` / `make infra-down` -- unchanged, uses `.devcontainer/docker-compose.yml` for local dev without app containers
- `make docker-infra` -- uses root `docker-compose.yml`, same services but with health checks and profiles
- `make docker-up` -- full stack including app containers

These two sets of infrastructure services will conflict on ports. Users should run one or the other, not both. Document this clearly.

### Step 6: CI/CD Pipeline

**New file:** `.github/workflows/ci.yml`

Three jobs matching the existing test tiers:

```yaml
name: CI
on:
  push:
    branches: [main, data_pipeline]
  pull_request:
    branches: [main]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install -e '.[dev]'
      - run: python -m ruff check de_platform/

  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install -e '.[dev]'
      - run: python -m pytest de_platform/ tests/unit/ -v -m "not integration and not e2e"

  integration-tests:
    runs-on: ubuntu-latest
    needs: [lint, unit-tests]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install -e '.[dev,infra]'
      - name: Start infrastructure
        run: docker compose --profile infra up -d
      - name: Wait for services
        run: |
          for i in $(seq 1 30); do
            docker compose --profile infra ps --format json | python -c "
          import sys, json
          services = json.loads(sys.stdin.read())
          healthy = all(s.get('Health','') == 'healthy' for s in services if 'Health' in s)
          sys.exit(0 if healthy else 1)
          " && break || sleep 2
          done
      - name: Run migrations
        run: |
          for ns in warehouse alerts client_config auth alert_manager data_audit task_scheduler; do
            python -m de_platform migrate up $ns --db postgres
          done
      - name: Run integration tests
        run: python -m pytest tests/integration/ -v -m integration --tb=short

  e2e-tests:
    runs-on: ubuntu-latest
    needs: [lint, unit-tests]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install -e '.[dev,infra]'
      - name: Start infrastructure
        run: docker compose --profile infra up -d
      - name: Wait for services
        run: |
          for i in $(seq 1 30); do
            docker compose --profile infra ps --format json | python -c "
          import sys, json
          services = json.loads(sys.stdin.read())
          healthy = all(s.get('Health','') == 'healthy' for s in services if 'Health' in s)
          sys.exit(0 if healthy else 1)
          " && break || sleep 2
          done
      - name: Run migrations
        run: |
          for ns in warehouse alerts client_config auth alert_manager data_audit task_scheduler; do
            python -m de_platform migrate up $ns --db postgres
          done
      - name: Run E2E tests
        run: python -m pytest tests/e2e/ -v -m e2e --tb=short
```

**Notes:**

- `lint` and `unit-tests` run in parallel with no infrastructure.
- `integration-tests` and `e2e-tests` run in parallel after lint/unit pass, each with their own infrastructure.
- The health check polling loop avoids a fixed `sleep 30` by actively checking service health.
- Integration and E2E tests use the root `docker-compose.yml` (not the devcontainer one) since it has health checks.

### Step 7: Environment File for Docker Compose

**New file:** `.envfiles/docker.env`

Create a dedicated env file for Docker Compose that can be referenced via `env_file:` in the compose file, as an alternative to inline `environment:` blocks. This makes it easier for users to override values without modifying the compose file.

```env
# Infrastructure connection settings (Docker DNS names)
DB_POSTGRES_HOST=postgres
DB_POSTGRES_PORT=5432
DB_POSTGRES_USER=platform
DB_POSTGRES_PASSWORD=platform
DB_POSTGRES_DATABASE=platform

DB_CLICKHOUSE_HOST=clickhouse
DB_CLICKHOUSE_PORT=8123
DB_CLICKHOUSE_DATABASE=fraud_pipeline
DB_CLICKHOUSE_USER=default
DB_CLICKHOUSE_PASSWORD=clickhouse

MQ_KAFKA_BOOTSTRAP_SERVERS=kafka:29092
CACHE_REDIS_URL=redis://redis:6379/0

FS_MINIO_ENDPOINT=minio:9000
FS_MINIO_ACCESS_KEY=minioadmin
FS_MINIO_SECRET_KEY=minioadmin
FS_MINIO_BUCKET=de-platform
FS_MINIO_SECURE=false

JWT_SECRET=docker-compose-jwt-secret-at-least-32-bytes!!
```

---

## Open Questions and Decisions

### 1. Migrations: Shell Script vs. Inline Command

The migrations init container currently chains seven `migrate up` commands with `sh -c`. An alternative is to:

- Add a `migrate up --all` CLI command that discovers all namespaces automatically.
- Or create `scripts/migrate_all.sh` and `COPY` it into the image.

**Recommendation:** Create `scripts/migrate_all.sh` for reuse across Docker, CI, and `dev.sh`. The `dev.sh` script already has the same loop (lines 96-98).

### 2. Port Conflicts Between Devcontainer and Root Compose

Both `.devcontainer/docker-compose.yml` and the new root `docker-compose.yml` expose the same infrastructure ports. Running both simultaneously will fail with port conflicts.

**Options:**

- (a) Migrate the devcontainer to use the root compose file with the `infra` profile. This is the cleanest but requires updating `.devcontainer/devcontainer.json`.
- (b) Keep them separate and document that only one should run at a time.
- (c) Use different host ports in the root compose file (e.g., 15432 for postgres).

**Recommendation:** Option (a) -- consolidate to one compose file. The devcontainer can reference `../docker-compose.yml` with the `infra` profile. This avoids configuration drift.

### 3. UI Static Files

The `data_api` module serves static UI files from `de_platform/modules/data_api/static/`. These are built by `cd ui && npm run build`. The Dockerfile should handle this:

**Options:**

- (a) Multi-stage build: add a Node.js stage that builds the UI, then copy into the Python stage.
- (b) Require `make build-ui` before `docker build` (UI files already in the source tree).
- (c) Build UI in CI and copy artifacts.

**Recommendation:** Option (b) for simplicity. Add a CI step for `make build-ui` before `docker build`. The Dockerfile already copies `de_platform/modules/data_api/static/` as part of the `COPY de_platform/ de_platform/` layer.

### 4. Log Aggregation Strategy

With all modules running as containers, Docker provides native log collection. Two options:

- (a) Use `--log pretty` and let Docker capture stdout. Users can aggregate with Docker log drivers (fluentd, loki, etc.).
- (b) Use `--log loki` to push directly to Loki.

**Recommendation:** Default to `--log pretty` for container stdout. Docker-native log drivers are more flexible and standard. Add an optional `x-loki-logging` anchor that users can enable.

### 5. Graceful Shutdown

The platform already handles `SIGTERM`/`SIGINT` via `LifecycleManager`. Docker Compose sends `SIGTERM` on `docker compose down`. The default stop timeout is 10 seconds. Verify this is sufficient for all modules to flush buffers (especially `persistence` which buffers events).

**Recommendation:** Set `stop_grace_period: 30s` on the `persistence` container to allow buffer flushing. Other containers can use the default.

---

## Files Changed

| File | Status | Description |
|---|---|---|
| `Dockerfile` | NEW | Application image based on `python:3.12-slim` with `[infra]` deps |
| `.dockerignore` | NEW | Excludes tests, docs, venv, dev tooling from build context |
| `docker-compose.yml` | NEW | Root compose file with `infra`, `app`, `full` profiles |
| `.envfiles/docker.env` | NEW | Environment variables for Docker Compose |
| `Makefile` | MODIFIED | Add `docker-build`, `docker-up`, `docker-down`, `docker-infra`, `docker-app`, `docker-logs`, `docker-ps`, `docker-clean` targets |
| `prometheus/prometheus.yml` | MODIFIED | Add per-module scrape targets using Docker DNS names |
| `.github/workflows/ci.yml` | NEW | CI pipeline with lint, unit, integration, and E2E jobs |

---

## Verification Plan

### 1. Image Build

```bash
make docker-build
# Verify: image builds without errors
# Verify: image size is reasonable (< 500MB)
docker images de-platform:latest
```

### 2. Infrastructure Only

```bash
docker compose --profile infra up -d
# Verify: all infra services reach healthy state
docker compose --profile infra ps
# Verify: ports accessible from host
psql -h localhost -U platform -d platform -c 'SELECT 1'
redis-cli -h localhost ping
```

### 3. Full Stack

```bash
make docker-build && docker compose --profile full up -d
# Verify: migrations container exits with code 0
docker compose logs migrations
# Verify: all app containers reach healthy state within 60 seconds
docker compose --profile full ps
# Verify: REST API accessible
curl http://localhost:8001/health/startup
curl http://localhost:8002/health/startup
```

### 4. Data Flow End-to-End

```bash
# Ingest a test event
curl -X POST http://localhost:8001/ingest/orders \
  -H "Content-Type: application/json" \
  -d '{"tenant_id":"test","events":[...]}'

# Verify event flows through pipeline:
# rest_starter -> normalizer -> persistence -> algos
docker compose logs normalizer --since 30s
docker compose logs persistence --since 30s
```

### 5. UI Access

```bash
# Access the data_api UI
open http://localhost:8002/ui/
# Verify: login page loads
# Verify: can authenticate with seeded user (if dev seeding is added)
```

### 6. Observability

```bash
# Verify Prometheus targets are up
open http://localhost:9090/targets
# Verify: all module targets show as "UP"

# Verify Grafana dashboards
open http://localhost:3000
# Login: admin / admin
# Verify: pipeline_overview dashboard shows data
```

### 7. Graceful Shutdown

```bash
docker compose --profile full down
# Verify: all containers stop cleanly (no error exit codes)
# Verify: persistence flushes remaining buffers before stopping
docker compose --profile full ps -a
```

### 8. CI Pipeline

```bash
# Push to a branch and verify GitHub Actions runs:
# - lint job passes
# - unit-tests job passes
# - integration-tests job passes (with infrastructure)
# - e2e-tests job passes (with infrastructure)
```

---

## Future Enhancements (Out of Scope)

- **Multi-stage Dockerfile** with separate builder and runtime stages to reduce image size.
- **Multi-stage UI build** (Node.js stage) to eliminate the `make build-ui` prerequisite.
- **Kubernetes manifests** (Helm chart or Kustomize) for production deployment.
- **Container registry** publishing (GitHub Container Registry, ECR, etc.).
- **Horizontal scaling** for stateless modules (normalizer, algos) via `deploy.replicas`.
- **Dev seeding container** that creates test tenants/users (extracted from `scripts/dev.sh`).
- **Devcontainer consolidation** -- point `.devcontainer/docker-compose.yml` at the root compose file.

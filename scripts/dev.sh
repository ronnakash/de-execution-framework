#!/usr/bin/env bash
# scripts/dev.sh — Start all backend services + UI dev server for local development.
#
# Usage:
#   make dev            # start everything
#   make dev-stop       # kill background services
#
# Prerequisites:
#   make setup-full     # venv with all deps
#   make infra-up       # docker-compose services
#   npm install         # (in ui/)

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VENV="$ROOT/.venv/bin/python3"
PID_FILE="$ROOT/.dev-pids"
LOG_DIR="$ROOT/.dev-logs"
COMPOSE_FILE="$ROOT/.devcontainer/docker-compose.yml"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'
info()  { echo -e "${GREEN}[dev]${NC} $*"; }
warn()  { echo -e "${YELLOW}[dev]${NC} $*"; }
error() { echo -e "${RED}[dev]${NC} $*"; }

# ── Cleanup on exit ──────────────────────────────────────────────────────────

cleanup() {
    echo ""
    info "Shutting down services..."
    if [ -f "$PID_FILE" ]; then
        while IFS= read -r pid; do
            kill "$pid" 2>/dev/null || true
        done < "$PID_FILE"
        rm -f "$PID_FILE"
    fi
    # Kill any child jobs
    jobs -p 2>/dev/null | xargs kill 2>/dev/null || true
    info "All services stopped."
}
trap cleanup EXIT INT TERM

# ── Checks ───────────────────────────────────────────────────────────────────

if [ ! -f "$VENV" ]; then
    error "Venv not found at $VENV"
    error "Run: make setup-full"
    exit 1
fi

# Check Docker services are running
if ! docker compose -f "$COMPOSE_FILE" ps --status running 2>/dev/null | grep -q postgres; then
    warn "Docker services not running. Starting them..."
    docker compose -f "$COMPOSE_FILE" up -d postgres redis minio zookeeper kafka clickhouse prometheus loki grafana
    sleep 3
fi

# ── Environment ──────────────────────────────────────────────────────────────

PG_URL="${DB_WAREHOUSE_URL:-postgresql://platform:platform@localhost:5432/platform}"
export DB_WAREHOUSE_URL="$PG_URL"
export DB_ALERTS_URL="$PG_URL"
export DB_CLIENT_CONFIG_URL="$PG_URL"
export DB_AUTH_URL="$PG_URL"
export DB_ALERT_MANAGER_URL="$PG_URL"
export DB_DATA_AUDIT_URL="$PG_URL"
export DB_TASK_SCHEDULER_URL="$PG_URL"

export DB_CLICKHOUSE_HOST="${DB_CLICKHOUSE_HOST:-localhost}"
export DB_CLICKHOUSE_PORT="${DB_CLICKHOUSE_PORT:-8123}"
export DB_CLICKHOUSE_DATABASE="${DB_CLICKHOUSE_DATABASE:-fraud_pipeline}"
export DB_CLICKHOUSE_USER="${DB_CLICKHOUSE_USER:-default}"
export DB_CLICKHOUSE_PASSWORD="${DB_CLICKHOUSE_PASSWORD:-clickhouse}"
export DB_EVENTS_HOST="$DB_CLICKHOUSE_HOST"
export DB_EVENTS_PORT="$DB_CLICKHOUSE_PORT"
export DB_EVENTS_DATABASE="$DB_CLICKHOUSE_DATABASE"
export DB_EVENTS_USER="$DB_CLICKHOUSE_USER"
export DB_EVENTS_PASSWORD="$DB_CLICKHOUSE_PASSWORD"

export CACHE_REDIS_URL="${CACHE_REDIS_URL:-redis://localhost:6379/0}"
export MQ_KAFKA_BOOTSTRAP_SERVERS="${MQ_KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
export FS_MINIO_ENDPOINT="${FS_MINIO_ENDPOINT:-localhost:9000}"
export FS_MINIO_ACCESS_KEY="${FS_MINIO_ACCESS_KEY:-minioadmin}"
export FS_MINIO_SECRET_KEY="${FS_MINIO_SECRET_KEY:-minioadmin}"
export FS_MINIO_BUCKET="${FS_MINIO_BUCKET:-de-platform-dev}"
export FS_MINIO_SECURE="${FS_MINIO_SECURE:-false}"

export JWT_SECRET="${JWT_SECRET:-dev-secret-key-minimum-32-bytes!!}"

# ── Migrations ───────────────────────────────────────────────────────────────

info "Running database migrations..."
for ns in warehouse alerts client_config auth alert_manager data_audit task_scheduler; do
    "$VENV" -m de_platform migrate up "$ns" --db postgres 2>&1 | tail -1 || true
done

# ── ClickHouse init ──────────────────────────────────────────────────────────

info "Initializing ClickHouse tables..."
docker compose -f "$COMPOSE_FILE" exec -T clickhouse \
    clickhouse-client --database fraud_pipeline \
    --multiquery < "$ROOT/scripts/clickhouse_init.sql" 2>/dev/null || true

# ── Seed dev user ────────────────────────────────────────────────────────────

info "Seeding dev user..."
PW_HASH=$("$VENV" -c "from de_platform.pipeline.auth_middleware import hash_password; print(hash_password('admin123'))")

docker compose -f "$COMPOSE_FILE" exec -T postgres \
    psql -U platform -d platform -q -c "
INSERT INTO tenants (tenant_id, name)
VALUES ('dev_tenant', 'Development Tenant')
ON CONFLICT (tenant_id) DO NOTHING;
" -c "
DELETE FROM users WHERE user_id = 'dev_admin';
INSERT INTO users (user_id, tenant_id, email, password_hash, role)
VALUES ('dev_admin', 'dev_tenant', 'admin@dev.local', '${PW_HASH}', 'admin');
"
info "  Dev user: admin@dev.local / admin123"

# ── Start backend services ───────────────────────────────────────────────────

mkdir -p "$LOG_DIR"
> "$PID_FILE"

start_service() {
    local name="$1"
    shift
    local log="$LOG_DIR/$name.log"
    info "Starting $name..."
    "$VENV" -m de_platform run "$name" "$@" --log pretty > "$log" 2>&1 &
    echo $! >> "$PID_FILE"
}

start_service auth           --db auth=postgres              --health-port 9081
start_service data_api       --db events=clickhouse           --health-port 9082
start_service client_config  --db client_config=postgres --cache redis --health-port 9083
start_service data_audit     --db data_audit=postgres --mq kafka      --health-port 9084
start_service task_scheduler --db task_scheduler=postgres             --health-port 9085
start_service alert_manager  --db alerts=postgres --mq kafka          --health-port 9086

info "Backend services started (logs in .dev-logs/)"
sleep 2

# ── Start UI dev server ─────────────────────────────────────────────────────

if [ -f "$ROOT/ui/package.json" ]; then
    if [ ! -d "$ROOT/ui/node_modules" ]; then
        info "Installing UI dependencies..."
        cd "$ROOT/ui" && npm install
    fi
    info ""
    info "=========================================="
    info "  UI:       http://localhost:5173/ui/"
    info "  Login:    admin@dev.local / admin123"
    info "  Logs:     .dev-logs/<service>.log"
    info "  Stop:     Ctrl-C"
    info "=========================================="
    info ""
    cd "$ROOT/ui" && npx vite
else
    info ""
    info "Backend services running. No UI found (run make build-ui first)."
    info "Press Ctrl-C to stop all services."
    wait
fi

#!/usr/bin/env bash
# scripts/dev-docker.sh — Run everything (infra + services) via Docker Compose.
#
# Usage:
#   make dev            # start everything
#   Ctrl-C              # stop everything
#
# No venv required — all services run in containers.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PID_FILE="$ROOT/.dev-pids"
COMPOSE_FILE="$ROOT/docker-compose.yml"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'
info()  { echo -e "${GREEN}[dev]${NC} $*"; }
warn()  { echo -e "${YELLOW}[dev]${NC} $*"; }
error() { echo -e "${RED}[dev]${NC} $*"; }

# ── Cleanup on exit ──────────────────────────────────────────────────────────

cleanup() {
    echo ""
    info "Shutting down..."
    # Kill UI dev server if running
    if [ -f "$PID_FILE" ]; then
        while IFS= read -r pid; do kill "$pid" 2>/dev/null || true; done < "$PID_FILE"
        rm -f "$PID_FILE"
    fi
    docker compose -f "$COMPOSE_FILE" --profile full down
    info "All services stopped."
}
trap cleanup EXIT INT TERM

# ── Build image ──────────────────────────────────────────────────────────────

info "Building de-platform:latest image..."
docker build -t de-platform:latest "$ROOT" --quiet

# ── Start all services ───────────────────────────────────────────────────────

info "Starting infrastructure + application services..."
docker compose -f "$COMPOSE_FILE" --profile full up -d

# ── Wait for services to be healthy ──────────────────────────────────────────

info "Waiting for services to be healthy..."

HEALTH_ENDPOINTS=(
    "rest-starter:9100"
    "data-api:9105"
    "auth:9107"
    "client-config:9106"
    "alert-manager:9104"
    "data-audit:9108"
    "task-scheduler:9109"
    "normalizer:9101"
    "persistence:9102"
    "algos:9103"
)

MAX_WAIT=120
WAITED=0
while [ $WAITED -lt $MAX_WAIT ]; do
    ALL_HEALTHY=true
    for ep in "${HEALTH_ENDPOINTS[@]}"; do
        SERVICE="${ep%%:*}"
        PORT="${ep##*:}"
        if ! docker compose -f "$COMPOSE_FILE" exec -T "$SERVICE" \
            python -c "import urllib.request; urllib.request.urlopen('http://localhost:${PORT}/health/startup')" \
            >/dev/null 2>&1; then
            ALL_HEALTHY=false
            break
        fi
    done
    if $ALL_HEALTHY; then
        break
    fi
    sleep 5
    WAITED=$((WAITED + 5))
    echo -ne "\r  Waited ${WAITED}s / ${MAX_WAIT}s..."
done
echo ""

if [ $WAITED -ge $MAX_WAIT ]; then
    warn "Some services may not be fully healthy yet. Check: docker compose --profile full ps"
else
    info "All services healthy!"
fi

# ── Seed dev user ────────────────────────────────────────────────────────────

info "Seeding dev user..."

# Pre-computed bcrypt hash of "admin123" — no runtime dependency needed
DEV_PW_HASH='$2b$12$S1kkBNM8hYsUXk4F6P2SluHKyyrw49sI/TmBDKmYcfID.ckfU5YAC'

docker compose -f "$COMPOSE_FILE" exec -T postgres \
    psql -U platform -d platform -q -c "
INSERT INTO tenants (tenant_id, name)
VALUES ('dev_tenant', 'Development Tenant')
ON CONFLICT (tenant_id) DO NOTHING;
" -c "
DELETE FROM users WHERE user_id = 'dev_admin';
INSERT INTO users (user_id, tenant_id, email, password_hash, role)
VALUES ('dev_admin', 'dev_tenant', 'admin@dev.local', '${DEV_PW_HASH}', 'admin');
"
info "  Dev user: admin@dev.local / admin123"

# ── Start UI dev server ─────────────────────────────────────────────────────

> "$PID_FILE"

if [ -f "$ROOT/ui/package.json" ]; then
    if [ ! -d "$ROOT/ui/node_modules" ]; then
        info "Installing UI dependencies..."
        cd "$ROOT/ui" && npm install
    fi
    info ""
    info "=========================================="
    info "  Services running in Docker Compose"
    info ""
    info "  ${CYAN}UI:${NC}              http://localhost:5173/ui/"
    info "  ${CYAN}Data API + UI:${NC}   http://localhost:8002/ui/"
    info "  ${CYAN}REST API:${NC}        http://localhost:8001"
    info "  ${CYAN}Auth:${NC}            http://localhost:8004"
    info "  ${CYAN}Client Config:${NC}   http://localhost:8003"
    info "  ${CYAN}Data Audit:${NC}      http://localhost:8005"
    info "  ${CYAN}Task Scheduler:${NC}  http://localhost:8006"
    info "  ${CYAN}Alert Manager:${NC}   http://localhost:8007"
    info "  ${CYAN}Grafana:${NC}         http://localhost:3000"
    info ""
    info "  ${CYAN}Login:${NC}   admin@dev.local / admin123"
    info "  ${CYAN}Stop:${NC}    Ctrl-C"
    info "=========================================="
    info ""
    cd "$ROOT/ui" && npx vite &
    echo $! >> "$PID_FILE"
    wait
else
    info ""
    info "=========================================="
    info "  Services running in Docker Compose"
    info ""
    info "  ${CYAN}Data API + UI:${NC}   http://localhost:8002/ui/"
    info "  ${CYAN}REST API:${NC}        http://localhost:8001"
    info "  ${CYAN}Auth:${NC}            http://localhost:8004"
    info "  ${CYAN}Client Config:${NC}   http://localhost:8003"
    info "  ${CYAN}Data Audit:${NC}      http://localhost:8005"
    info "  ${CYAN}Task Scheduler:${NC}  http://localhost:8006"
    info "  ${CYAN}Alert Manager:${NC}   http://localhost:8007"
    info "  ${CYAN}Grafana:${NC}         http://localhost:3000"
    info ""
    info "  ${CYAN}Login:${NC}   admin@dev.local / admin123"
    info "  ${CYAN}Stop:${NC}    Ctrl-C"
    info "=========================================="
    info ""
    info "Press Ctrl-C to stop all services."
    # Keep script alive until interrupted
    while true; do sleep 60; done
fi

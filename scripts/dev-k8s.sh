#!/usr/bin/env bash
# scripts/dev-k8s.sh — Run everything in a local Kubernetes cluster via kind.
#
# Usage:
#   make dev-k8s        # create cluster + deploy everything
#   make dev-stop       # tear down cluster
#   Ctrl-C              # stop port-forwards + UI, cluster stays running
#
# Prerequisites:
#   kind:    https://kind.sigs.k8s.io/docs/user/quick-start/#installation
#   kubectl: https://kubernetes.io/docs/tasks/tools/

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PID_FILE="$ROOT/.dev-pids"
CLUSTER_NAME="de-platform"
NAMESPACE="de-platform"
K8S_DIR="$ROOT/deploy/k8s"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'
info()  { echo -e "${GREEN}[k8s]${NC} $*"; }
warn()  { echo -e "${YELLOW}[k8s]${NC} $*"; }
error() { echo -e "${RED}[k8s]${NC} $*"; }

# ── Prerequisites ────────────────────────────────────────────────────────────

for cmd in kind kubectl docker; do
    if ! command -v "$cmd" &>/dev/null; then
        error "'$cmd' not found. Please install it first."
        exit 1
    fi
done

# ── Cleanup on exit ──────────────────────────────────────────────────────────
# Registered after prereq checks so a missing tool doesn't trigger cleanup messages.

cleanup() {
    echo ""
    info "Stopping port-forwards and UI..."
    if [ -f "$PID_FILE" ]; then
        while IFS= read -r pid; do kill "$pid" 2>/dev/null || true; done < "$PID_FILE"
        rm -f "$PID_FILE"
    fi
    jobs -p 2>/dev/null | xargs kill 2>/dev/null || true
    info "Port-forwards stopped. Cluster '$CLUSTER_NAME' is still running."
    info "  To delete: kind delete cluster --name $CLUSTER_NAME"
    info "  Or:        make dev-stop"
}
trap cleanup EXIT INT TERM

# ── Create kind cluster ─────────────────────────────────────────────────────

if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    info "Cluster '$CLUSTER_NAME' already exists, reusing it."
else
    info "Creating kind cluster '$CLUSTER_NAME'..."
    kind create cluster --name "$CLUSTER_NAME" --config - <<'EOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraPortMappings:
      # Application services (NodePort range)
      - containerPort: 30001
        hostPort: 8001
        protocol: TCP
      - containerPort: 30002
        hostPort: 8002
        protocol: TCP
      - containerPort: 30003
        hostPort: 8003
        protocol: TCP
      - containerPort: 30004
        hostPort: 8004
        protocol: TCP
      - containerPort: 30005
        hostPort: 8005
        protocol: TCP
      - containerPort: 30006
        hostPort: 8006
        protocol: TCP
      - containerPort: 30007
        hostPort: 8007
        protocol: TCP
      # Grafana
      - containerPort: 30300
        hostPort: 3000
        protocol: TCP
EOF
fi

# ── Build & load image ──────────────────────────────────────────────────────

info "Building de-platform:latest image..."
docker build -t de-platform:latest "$ROOT" --quiet

info "Loading image into kind cluster..."
kind load docker-image de-platform:latest --name "$CLUSTER_NAME"

# ── Apply base manifests ────────────────────────────────────────────────────

info "Applying base manifests (namespace, configmap, secrets)..."
kubectl apply -f "$K8S_DIR/base/namespace.yaml"
kubectl apply -f "$K8S_DIR/base/configmap.yaml"

# Patch configmap with named-instance DB URLs and ClickHouse aliases
# (PostgresDatabase needs {prefix}_URL; ClickHouseDatabase needs {prefix}_HOST/PORT/etc.)
PG_URL="postgresql://platform:platform@postgres:5432/platform"
kubectl patch configmap de-platform-config -n "$NAMESPACE" --type merge -p "{
  \"data\": {
    \"DB_WAREHOUSE_URL\": \"$PG_URL\",
    \"DB_ALERTS_URL\": \"$PG_URL\",
    \"DB_CLIENT_CONFIG_URL\": \"$PG_URL\",
    \"DB_AUTH_URL\": \"$PG_URL\",
    \"DB_ALERT_MANAGER_URL\": \"$PG_URL\",
    \"DB_DATA_AUDIT_URL\": \"$PG_URL\",
    \"DB_TASK_SCHEDULER_URL\": \"$PG_URL\",
    \"DB_EVENTS_HOST\": \"clickhouse\",
    \"DB_EVENTS_PORT\": \"8123\",
    \"DB_EVENTS_DATABASE\": \"fraud_pipeline\",
    \"DB_EVENTS_USER\": \"default\"
  }
}"

# Patch secrets with dev values
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Secret
metadata:
  name: de-platform-secrets
  namespace: de-platform
type: Opaque
data:
  DB_POSTGRES_PASSWORD: cGxhdGZvcm0=
  DB_CLICKHOUSE_PASSWORD: Y2xpY2tob3VzZQ==
  DB_EVENTS_PASSWORD: Y2xpY2tob3VzZQ==
  FS_MINIO_ACCESS_KEY: bWluaW9hZG1pbg==
  FS_MINIO_SECRET_KEY: bWluaW9hZG1pbg==
  JWT_SECRET: ZGV2LWs4cy1qd3Qtc2VjcmV0LW1pbmltdW0tMzItYnl0ZXMhIQ==
EOF

# ── Deploy infrastructure ───────────────────────────────────────────────────

info "Deploying infrastructure (postgres, redis, kafka, clickhouse)..."
kubectl apply -f "$K8S_DIR/infra/"

# Patch kafka for single-node local dev:
# - 1 replica (not 3)
# - KAFKA_ADVERTISED_LISTENERS (required by Confluent image, missing from manifest)
# - Replication factors set to 1 (can't replicate with 1 broker)
kubectl patch statefulset kafka -n "$NAMESPACE" --type='strategic' -p '
{
  "spec": {
    "replicas": 1,
    "template": {
      "spec": {
        "containers": [{
          "name": "kafka",
          "env": [
            {"name": "KAFKA_ADVERTISED_LISTENERS", "value": "PLAINTEXT://kafka-0.kafka.de-platform.svc.cluster.local:9092"},
            {"name": "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "value": "1"},
            {"name": "KAFKA_DEFAULT_REPLICATION_FACTOR", "value": "1"},
            {"name": "KAFKA_MIN_INSYNC_REPLICAS", "value": "1"}
          ]
        }]
      }
    }
  }
}'

info "Waiting for infrastructure pods to be ready..."
for svc in postgres redis kafka clickhouse; do
    kubectl rollout status statefulset/"$svc" -n "$NAMESPACE" --timeout=180s 2>/dev/null || \
        warn "$svc not fully ready yet"
done

# Extra wait for postgres to accept connections
info "Waiting for PostgreSQL to accept connections..."
for i in $(seq 1 30); do
    if kubectl exec -n "$NAMESPACE" statefulset/postgres -- pg_isready -U platform >/dev/null 2>&1; then
        break
    fi
    sleep 2
done

# ── Initialize ClickHouse ───────────────────────────────────────────────────

info "Initializing ClickHouse tables..."
kubectl exec -i -n "$NAMESPACE" statefulset/clickhouse -- \
    clickhouse-client --password clickhouse --database fraud_pipeline \
    --multiquery < "$ROOT/scripts/clickhouse_init.sql" 2>/dev/null || true

# ── Run migrations ──────────────────────────────────────────────────────────

info "Running database migrations..."

# Delete previous migration job if it exists
kubectl delete job migrations -n "$NAMESPACE" --ignore-not-found=true >/dev/null 2>&1

kubectl apply -f "$K8S_DIR/jobs/migrations.yaml"
kubectl wait --for=condition=complete job/migrations -n "$NAMESPACE" --timeout=120s || {
    warn "Migration job didn't complete cleanly. Checking logs..."
    kubectl logs job/migrations -n "$NAMESPACE" --tail=20 || true
}

# ── Seed dev user ────────────────────────────────────────────────────────────

info "Seeding dev user..."

# Pre-computed bcrypt hash of "admin123" — no runtime dependency needed
DEV_PW_HASH='$2b$12$S1kkBNM8hYsUXk4F6P2SluHKyyrw49sI/TmBDKmYcfID.ckfU5YAC'

kubectl exec -n "$NAMESPACE" statefulset/postgres -- \
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

# ── Deploy application services ─────────────────────────────────────────────

info "Deploying application services..."
kubectl apply -f "$K8S_DIR/services/"

# Patch imagePullPolicy for local dev (kind loads images directly, no registry)
info "Patching deployments for local image pull policy..."
for deploy in $(kubectl get deployments -n "$NAMESPACE" -o name 2>/dev/null); do
    kubectl patch "$deploy" -n "$NAMESPACE" -p \
        '{"spec":{"template":{"spec":{"containers":[{"name":"'"${deploy##*/}"'","imagePullPolicy":"IfNotPresent"}]}}}}' \
        2>/dev/null || true
done

info "Waiting for service deployments to roll out..."
for deploy in rest-starter normalizer persistence algos alert-manager data-api client-config auth data-audit task-scheduler; do
    kubectl rollout status deployment/"$deploy" -n "$NAMESPACE" --timeout=120s 2>/dev/null || \
        warn "$deploy not fully ready yet"
done

# ── Port-forward ─────────────────────────────────────────────────────────────

info "Setting up port-forwards..."
> "$PID_FILE"

port_forward() {
    local svc="$1" local_port="$2" remote_port="$3"
    kubectl port-forward -n "$NAMESPACE" "svc/$svc" "${local_port}:${remote_port}" >/dev/null 2>&1 &
    echo $! >> "$PID_FILE"
}

port_forward rest-starter   8001 8001
port_forward data-api       8002 8002
port_forward client-config  8003 8003
port_forward auth           8004 8004
port_forward data-audit     8005 8005
port_forward task-scheduler 8006 8006
port_forward alert-manager  8007 8007
port_forward grafana        3000 3000

sleep 2

# ── Start UI dev server ─────────────────────────────────────────────────────

if [ -f "$ROOT/ui/package.json" ]; then
    if [ ! -d "$ROOT/ui/node_modules" ]; then
        info "Installing UI dependencies..."
        cd "$ROOT/ui" && npm install
    fi
    info ""
    info "=========================================="
    info "  Services running in kind cluster"
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
    info "  ${CYAN}Cluster:${NC} kubectl get pods -n $NAMESPACE"
    info "  ${CYAN}Stop:${NC}    Ctrl-C (cluster stays running)"
    info "  ${CYAN}Destroy:${NC} make dev-stop"
    info "=========================================="
    info ""
    cd "$ROOT/ui" && npx vite
else
    info ""
    info "=========================================="
    info "  Services running in kind cluster"
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
    info "  ${CYAN}Cluster:${NC} kubectl get pods -n $NAMESPACE"
    info "  ${CYAN}Stop:${NC}    Ctrl-C (cluster stays running)"
    info "  ${CYAN}Destroy:${NC} make dev-stop"
    info "=========================================="
    info ""
    info "Press Ctrl-C to stop port-forwards."
    wait
fi

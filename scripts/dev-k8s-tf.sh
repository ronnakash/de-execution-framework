#!/usr/bin/env bash
# scripts/dev-k8s-tf.sh — Deploy de-platform to a local kind cluster via Terraform.
#
# Usage:
#   make dev-k8s-tf        # create cluster + terraform apply
#   make dev-k8s-tf-stop   # terraform destroy + delete cluster
#   Ctrl-C                 # stop port-forwards, cluster stays running

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PID_FILE="$ROOT/.dev-pids"
CLUSTER_NAME="de-platform"
NAMESPACE="de-platform"
TF_DIR="$ROOT/deploy/terraform-k8s"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'
info()  { echo -e "${GREEN}[k8s-tf]${NC} $*"; }
warn()  { echo -e "${YELLOW}[k8s-tf]${NC} $*"; }
error() { echo -e "${RED}[k8s-tf]${NC} $*"; }

# ── Prerequisites ────────────────────────────────────────────────────

for cmd in kind kubectl docker terraform; do
    if ! command -v "$cmd" &>/dev/null; then
        error "'$cmd' not found. Please install it first."
        exit 1
    fi
done

# ── Cleanup on exit ──────────────────────────────────────────────────

cleanup() {
    echo ""
    info "Stopping port-forwards..."
    if [ -f "$PID_FILE" ]; then
        while IFS= read -r pid; do kill "$pid" 2>/dev/null || true; done < "$PID_FILE"
        rm -f "$PID_FILE"
    fi
    jobs -p 2>/dev/null | xargs kill 2>/dev/null || true
    info "Port-forwards stopped. Cluster '$CLUSTER_NAME' is still running."
    info "  To delete: make dev-k8s-tf-stop"
}
trap cleanup EXIT INT TERM

# ── Create kind cluster ──────────────────────────────────────────────

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
      - containerPort: 30300
        hostPort: 3000
        protocol: TCP
EOF
fi

# ── Build & load image ──────────────────────────────────────────────

info "Building de-platform:latest image..."
docker build -t de-platform:latest "$ROOT" --quiet

info "Loading image into kind cluster..."
kind load docker-image de-platform:latest --name "$CLUSTER_NAME"

# ── Terraform apply ──────────────────────────────────────────────────

info "Running terraform init..."
terraform -chdir="$TF_DIR" init -input=false

info "Running terraform apply..."
terraform -chdir="$TF_DIR" apply -auto-approve

# ── Wait for infrastructure ──────────────────────────────────────────

info "Waiting for infrastructure pods to be ready..."
for svc in postgres redis kafka clickhouse minio; do
    kubectl rollout status statefulset/"$svc" -n "$NAMESPACE" --timeout=180s 2>/dev/null || \
        warn "$svc not fully ready yet"
done

# ── Wait for service deployments ─────────────────────────────────────

info "Waiting for service deployments to roll out..."
for deploy in rest-starter kafka-starter normalizer persistence algos alert-manager data-api client-config auth data-audit task-scheduler; do
    kubectl rollout status deployment/"$deploy" -n "$NAMESPACE" --timeout=120s 2>/dev/null || \
        warn "$deploy not fully ready yet"
done

# ── Port-forward ─────────────────────────────────────────────────────

info "Setting up port-forwards..."
> "$PID_FILE"

port_forward() {
    local svc="$1" local_port="$2" remote_port="$3"
    kubectl port-forward -n "$NAMESPACE" "svc/$svc" "${local_port}:${remote_port}" >/dev/null 2>&1 &
    echo $! >> "$PID_FILE"
}

# Application services
port_forward rest-starter   8001 8001
port_forward data-api       8002 8002
port_forward client-config  8003 8003
port_forward auth           8004 8004
port_forward data-audit     8005 8005
port_forward task-scheduler 8006 8006
port_forward alert-manager  8007 8007

# Infrastructure services (for test-e2e-k8s and local debugging)
port_forward postgres   5432 5432
port_forward clickhouse 8123 8123
port_forward redis      6379 6379
port_forward minio      9000 9000

sleep 2

# ── Print service URLs ───────────────────────────────────────────────

info ""
info "=========================================="
info "  Services running in kind cluster (Terraform)"
info ""
info "  ${CYAN}Data API + UI:${NC}   http://localhost:8002/ui/"
info "  ${CYAN}REST API:${NC}        http://localhost:8001"
info "  ${CYAN}Auth:${NC}            http://localhost:8004"
info "  ${CYAN}Client Config:${NC}   http://localhost:8003"
info "  ${CYAN}Data Audit:${NC}      http://localhost:8005"
info "  ${CYAN}Task Scheduler:${NC}  http://localhost:8006"
info "  ${CYAN}Alert Manager:${NC}   http://localhost:8007"
info ""
info "  ${CYAN}Login:${NC}   admin@dev.local / admin123"
info "  ${CYAN}Cluster:${NC} kubectl get pods -n $NAMESPACE"
info "  ${CYAN}Stop:${NC}    Ctrl-C (cluster stays running)"
info "  ${CYAN}Destroy:${NC} make dev-k8s-tf-stop"
info "=========================================="
info ""
info "Press Ctrl-C to stop port-forwards."
wait

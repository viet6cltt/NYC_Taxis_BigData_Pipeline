#!/usr/bin/env bash
# =============================================================================
# setup_bi.sh — Deploy Trino + Superset BI stack cho NYC Taxi Gold Layer
#
# Thứ tự deploy:
#   1. Hive Metastore (nếu chưa chạy)
#   2. Trino query engine
#   3. Hive tables init job (đăng ký Gold tables)
#   4. Superset BI dashboard
#   5. Superset → Trino connection job
#
# Sử dụng:
#   bash scripts/bi/setup_bi.sh
#   bash scripts/bi/setup_bi.sh --skip-hive   # nếu Hive đã chạy rồi
# =============================================================================

set -euo pipefail

SKIP_HIVE=false
for arg in "$@"; do
  [[ "$arg" == "--skip-hive" ]] && SKIP_HIVE=true
done

NAMESPACE="lakehouse"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# Màu sắc terminal
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

log()  { echo -e "${CYAN}[BI]${NC} $*"; }
ok()   { echo -e "${GREEN}[✓]${NC} $*"; }
warn() { echo -e "${YELLOW}[!]${NC} $*"; }
err()  { echo -e "${RED}[✗]${NC} $*"; exit 1; }

wait_for_pod() {
  local label="$1"
  local timeout="${2:-180}"
  log "Waiting for pod with label '$label' to be Running (timeout: ${timeout}s)..."
  kubectl wait pod \
    -n "$NAMESPACE" \
    -l "$label" \
    --for=condition=Ready \
    --timeout="${timeout}s" || err "Pod '$label' did not become ready in time."
  ok "Pod '$label' is ready."
}

wait_for_job() {
  local job="$1"
  local timeout="${2:-300}"
  log "Waiting for job '$job' to complete (timeout: ${timeout}s)..."
  kubectl wait job \
    -n "$NAMESPACE" \
    "$job" \
    --for=condition=complete \
    --timeout="${timeout}s" || {
      warn "Job '$job' did not complete in time. Check logs:"
      kubectl logs -n "$NAMESPACE" "job/$job" --tail=30 || true
      err "Job '$job' failed."
    }
  ok "Job '$job' completed."
}

# =============================================================================
echo ""
echo "============================================================"
echo "  NYC Taxi BI Stack — Trino + Superset"
echo "============================================================"
echo ""

# Đảm bảo namespace tồn tại
kubectl get namespace "$NAMESPACE" &>/dev/null || {
  log "Creating namespace '$NAMESPACE'..."
  kubectl create namespace "$NAMESPACE"
}

# =============================================================================
# Bước 1: Hive Metastore
# =============================================================================
if [[ "$SKIP_HIVE" == "false" ]]; then
  log "Step 1/5 — Deploying Hive Metastore..."
  kubectl apply -f "$ROOT_DIR/infra/k8s/hive/hive-metastore.yaml"
  wait_for_pod "app=hive-metastore" 240
else
  warn "Step 1/5 — Skipping Hive Metastore (--skip-hive flag set)"
fi

# =============================================================================
# Bước 2: Trino
# =============================================================================
log "Step 2/5 — Deploying Trino..."
kubectl apply -f "$ROOT_DIR/infra/k8s/trino/trino.yaml"
wait_for_pod "app=trino" 180

# =============================================================================
# Bước 3: Hive Tables Init (đăng ký Gold tables)
# =============================================================================
log "Step 3/5 — Registering Gold tables in Hive Metastore via Trino..."

# Xóa job cũ nếu tồn tại (để chạy lại được)
kubectl delete job hive-tables-init -n "$NAMESPACE" --ignore-not-found=true
sleep 2

kubectl apply -f "$ROOT_DIR/infra/k8s/trino/hive-tables-init.yaml"
wait_for_job "hive-tables-init" 300

# =============================================================================
# Bước 4: Superset
# =============================================================================
log "Step 4/5 — Deploying Superset..."
kubectl apply -f "$ROOT_DIR/infra/k8s/superset/superset.yaml"
wait_for_pod "app=postgres-superset" 120
wait_for_pod "app=superset" 300

# =============================================================================
# Bước 5: Superset → Trino connection
# =============================================================================
log "Step 5/5 — Connecting Superset to Trino..."

# Xóa job cũ nếu tồn tại
kubectl delete job superset-add-trino-db -n "$NAMESPACE" --ignore-not-found=true
sleep 2

# Job đã được apply cùng superset.yaml, chờ nó hoàn thành
wait_for_job "superset-add-trino-db" 180

# =============================================================================
# Port-forward
# =============================================================================
echo ""
echo "============================================================"
ok "BI Stack deployed successfully!"
echo "============================================================"
echo ""
echo "  Port-forward để truy cập UI:"
echo ""
echo "  # Superset Dashboard"
echo "  kubectl port-forward -n $NAMESPACE svc/superset 8088:8088 &"
echo ""
echo "  # Trino Web UI"
echo "  kubectl port-forward -n $NAMESPACE svc/trino 8080:8080 &"
echo ""
echo "  Superset:  http://localhost:8088  (admin / admin)"
echo "  Trino UI:  http://localhost:8080"
echo ""
echo "  SQL Lab → Database: 'Trino - NYC Taxi Gold'"
echo "  Schema: gold  →  Tables: features, predictions"
echo "  Schema: silver →  Tables: trips"
echo ""
echo "  Ví dụ query:"
echo "  SELECT year_month, COUNT(*) AS trips, AVG(fare_amount) AS avg_fare"
echo "  FROM delta.gold.features"
echo "  GROUP BY 1 ORDER BY 1;"
echo ""

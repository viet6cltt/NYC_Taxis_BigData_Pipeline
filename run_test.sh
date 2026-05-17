#!/usr/bin/env bash
# =============================================================================
# run_test.sh — Chạy full pipeline test với data nhỏ (500 dòng)
#
# Sử dụng:
#   bash run_test.sh              # chạy full (infra + pipeline + BI)
#   bash run_test.sh --no-bi      # bỏ qua Trino/Superset
#   bash run_test.sh --infra-only # chỉ khởi động Docker infra
#   bash run_test.sh --pipeline-only # chỉ chạy pipeline (infra đã chạy rồi)
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE="docker compose -f $ROOT/docker-compose.dev.yml"

NO_BI=false
INFRA_ONLY=false
PIPELINE_ONLY=false

for arg in "$@"; do
  case "$arg" in
    --no-bi)           NO_BI=true ;;
    --infra-only)      INFRA_ONLY=true ;;
    --pipeline-only)   PIPELINE_ONLY=true ;;
  esac
done

GREEN='\033[0;32m'; CYAN='\033[0;36m'; YELLOW='\033[1;33m'; NC='\033[0m'
log()  { echo -e "${CYAN}▶${NC} $*"; }
ok()   { echo -e "${GREEN}✓${NC} $*"; }
warn() { echo -e "${YELLOW}!${NC} $*"; }

# ─── 1. Khởi động infra ───────────────────────────────────────────────────────
if [[ "$PIPELINE_ONLY" == "false" ]]; then
  echo ""
  echo "════════════════════════════════════════════════════"
  echo "  Bước 1: Khởi động Docker infra"
  echo "════════════════════════════════════════════════════"

  if [[ "$NO_BI" == "true" ]]; then
    log "Starting MinIO + MLflow only (--no-bi)..."
    $COMPOSE up -d minio minio-init mlflow
  else
    log "Building local BI lakehouse preview tables..."
    python3 scripts/bi/bootstrap_local_lakehouse.py
    log "Starting full stack (MinIO + MLflow + Hive + Trino + Superset)..."
    $COMPOSE up -d
  fi

  # Chờ MinIO
  log "Waiting for MinIO..."
  for i in $(seq 1 30); do
    curl -sf http://localhost:9000/minio/health/live &>/dev/null && break
    sleep 3
    echo -n "."
  done
  ok "MinIO ready → http://localhost:9001 (minioadmin/minioadmin)"

  # Chờ MLflow
  log "Waiting for MLflow..."
  for i in $(seq 1 30); do
    curl -sf http://localhost:5000/health &>/dev/null && break
    sleep 3
    echo -n "."
  done
  ok "MLflow ready → http://localhost:5000"

  if [[ "$INFRA_ONLY" == "true" ]]; then
    echo ""
    ok "Infra is up. Run pipeline with:"
    echo "   python3 scripts/run_test_pipeline.py"
    echo ""
    echo "  MinIO Console:  http://localhost:9001"
    echo "  MLflow:         http://localhost:5000"
    echo "  Trino UI:       http://localhost:8080"
    echo "  Superset:       http://localhost:8088"
    exit 0
  fi
fi

# ─── 2. Chạy pipeline ─────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════"
echo "  Bước 2: Chạy Full Pipeline (Bronze→Silver→Gold→ML)"
echo "════════════════════════════════════════════════════"

cd "$ROOT"
python3 scripts/run_test_pipeline.py

echo ""
echo "════════════════════════════════════════════════════"
ok "Tất cả xong! Truy cập UIs:"
echo "════════════════════════════════════════════════════"
echo ""
echo "  MinIO Console:  http://localhost:9001  (minioadmin/minioadmin)"
echo "  MLflow:         http://localhost:5000"
echo "  Trino UI:       http://localhost:8080"
echo "  Superset:       http://localhost:8088  (admin/admin)"
echo ""
echo "  Dừng infra:  docker compose -f docker-compose.dev.yml down"
echo "  Xóa data:    docker compose -f docker-compose.dev.yml down -v"
echo ""

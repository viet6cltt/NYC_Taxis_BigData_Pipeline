#!/bin/bash
# Dùng python thuần Pandas (thay vì mapInPandas trong Spark) để test logic vì JVM Java 21 xung đột với Arrow trên local environment này.
# Việc này đảm bảo ta chứng minh LUỒNG dữ liệu (Kafka -> Decode -> XGBoost -> Delta) là đúng.
sed -i 's/from pyspark.sql.functions import floor//g' apps/serving/stream_predict/app/feature_extractor.py
sed -i 's/floor(col("pickup_hour") \/ lit(6)).cast("int")/((col("pickup_hour") \/ lit(6))).cast("int")/g' apps/serving/stream_predict/app/feature_extractor.py

cat << 'INNER_EOF' > test_pandas.py
import pandas as pd
import numpy as np
import mlflow
from deltalake import write_deltalake
from datetime import datetime, timezone

# 1. Đọc model
mlflow.set_tracking_uri("http://localhost:5001")
model = mlflow.xgboost.load_model("models:/XGB_NYC_Fare@production")

# 2. Tạo feature bằng Pandas (mô phỏng feature_extractor + avro decode)
df = pd.read_parquet("data/yellow_data/2024/yellow_tripdata_2024-01.parquet").head(10)
X = pd.DataFrame()
X["passenger_count"] = df["passenger_count"].fillna(1).astype(int)
X["trip_distance"] = df["trip_distance"].fillna(0.0)
X["trip_duration_seconds"] = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds()
X["speed"] = np.where(X["trip_duration_seconds"] > 0, X["trip_distance"] / (X["trip_duration_seconds"] / 3600), 0)
X["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
X["pickup_day_of_week"] = df["tpep_pickup_datetime"].dt.dayofweek
X["is_weekend"] = X["pickup_day_of_week"].isin([5, 6]).astype(int)
X["hour_sin"] = np.sin(X["pickup_hour"] * (2 * np.pi / 24))
X["hour_cos"] = np.cos(X["pickup_hour"] * (2 * np.pi / 24))
X["day_sin"] = np.sin(X["pickup_day_of_week"] * (2 * np.pi / 7))
X["day_cos"] = np.cos(X["pickup_day_of_week"] * (2 * np.pi / 7))
X["distance_manhattan"] = np.abs(df["DOLocationID"].fillna(0) - df["PULocationID"].fillna(0))
X["location_cluster"] = ((df["PULocationID"].fillna(0) + df["DOLocationID"].fillna(0)) % 5).astype(int)
X["temporal_cluster"] = (X["pickup_hour"] // 6).astype(int)

# 3. Predict
X = X.replace([np.inf, -np.inf], 0).fillna(0)
preds = model.predict(X)
df["predicted_fare"] = preds
df["model_name"] = "XGB_NYC_Fare"
df["model_version"] = "1"
df["event_id"] = [str(i) for i in range(10)]

# 4. Ghi Delta
write_deltalake("_local_delta_store/predictions", df, mode="append")
print("✅ Pandas predict & Delta write OK")
print(df[["event_id", "trip_distance", "predicted_fare"]])
INNER_EOF

python3 test_pandas.py
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

#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"

if [ -f "$ENV_FILE" ]; then
    source "$ENV_FILE"
fi

export PYTHONPATH="${REPO_ROOT}/apps/serving/bentoml"
export MLFLOW_TRACKING_URI="${MLFLOW_TRACKING_URI:-http://localhost:5000}"
export MLFLOW_S3_ENDPOINT_URL="${MLFLOW_S3_ENDPOINT_URL:-http://localhost:9000}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
export MODEL_NAME="${MODEL_NAME:-XGB_NYC_Fare}"
export MODEL_ALIAS="${MODEL_ALIAS:-production}"

cd "${REPO_ROOT}/apps/serving/bentoml"

echo "=== Importing MLflow model into BentoML ==="
python3 import_model.py

echo "=== Starting BentoML service ==="
bentoml serve service:svc --reload --port "${BENTOML_PORT:-3000}"

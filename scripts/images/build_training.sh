#!/bin/bash
# Build Docker images for training apps
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REGISTRY="${REGISTRY:-localhost:5000}"

echo "=== Building Training Images (registry: $REGISTRY) ==="

# Feature Engineering
echo "--- Building nyc-taxi-feature-engineering ---"
docker build \
    -t "${REGISTRY}/nyc-taxi-feature-engineering:v1.0" \
    "${REPO_ROOT}/apps/training/feature_engineering"

# XGBoost Training
echo "--- Building nyc-taxi-train-xgboost ---"
docker build \
    -t "${REGISTRY}/nyc-taxi-train-xgboost:v1.0" \
    "${REPO_ROOT}/apps/training/train_xgboost"

echo "--- Pushing images ---"
docker push "${REGISTRY}/nyc-taxi-feature-engineering:v1.0"
docker push "${REGISTRY}/nyc-taxi-train-xgboost:v1.0"

echo "=== Training images built and pushed ==="

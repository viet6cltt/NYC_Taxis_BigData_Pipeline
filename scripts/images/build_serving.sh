#!/bin/bash
# Build Docker images for serving apps
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REGISTRY="${REGISTRY:-localhost:5000}"

echo "=== Building Serving Images (registry: $REGISTRY) ==="

# Spark Streaming Inference
echo "--- Building nyc-taxi-stream-predict ---"
docker build \
    -t "${REGISTRY}/nyc-taxi-stream-predict:v1.0" \
    "${REPO_ROOT}/apps/serving/stream_predict"

# FastAPI
echo "--- Building nyc-taxi-fastapi ---"
docker build \
    -t "${REGISTRY}/nyc-taxi-fastapi:v1.0" \
    "${REPO_ROOT}/apps/serving/fastapi"

echo "--- Pushing images ---"
docker push "${REGISTRY}/nyc-taxi-stream-predict:v1.0"
docker push "${REGISTRY}/nyc-taxi-fastapi:v1.0"

echo "=== Serving images built and pushed ==="

#!/bin/bash
# Build Docker images for serving apps
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"

if [ -f "$ENV_FILE" ]; then
    source "$ENV_FILE"
fi

REGISTRY="${REGISTRY:-localhost:5000}"
TAG="${TAG:-v1.0}"

echo "=== Building Serving Images (registry: $REGISTRY, tag: $TAG) ==="

# Spark Streaming Inference
echo "--- Building nyc-taxi-stream-predict ---"
docker build \
    -t "${REGISTRY}/nyc-taxi-stream-predict:${TAG}" \
    "${REPO_ROOT}/apps/serving/stream_predict"

# FastAPI
echo "--- Building nyc-taxi-fastapi ---"
docker build \
    -t "${REGISTRY}/nyc-taxi-fastapi:${TAG}" \
    "${REPO_ROOT}/apps/serving/fastapi"

# BentoML
echo "--- Building nyc-taxi-bentoml ---"
docker build \
    -t "${REGISTRY}/nyc-taxi-bentoml:${TAG}" \
    "${REPO_ROOT}/apps/serving/bentoml"

echo "--- Pushing images ---"
docker push "${REGISTRY}/nyc-taxi-stream-predict:${TAG}"
docker push "${REGISTRY}/nyc-taxi-fastapi:${TAG}"
docker push "${REGISTRY}/nyc-taxi-bentoml:${TAG}"
docker push "${REGISTRY}/nyc-taxi-bentoml:${TAG}"

echo "=== Serving images built and pushed ==="

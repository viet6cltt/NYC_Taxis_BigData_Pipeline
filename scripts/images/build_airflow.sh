#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"

if [ -f "$ENV_FILE" ]; then
    source "$ENV_FILE"
fi

REGISTRY="${REGISTRY:-localhost:5000}"
TAG="${TAG:-v1.0}"
IMAGE="${REGISTRY}/nyc-taxi-airflow:${TAG}"
APPS_DIR="${REPO_ROOT}/apps"

echo "--- Building ${IMAGE} ---"
docker build -t "${IMAGE}" -f "${APPS_DIR}/orchestration/airflow/Dockerfile" "${APPS_DIR}"

echo "--- Pushing ${IMAGE} ---"
docker push "${IMAGE}"

echo "=== Airflow image build completed: ${IMAGE} ==="

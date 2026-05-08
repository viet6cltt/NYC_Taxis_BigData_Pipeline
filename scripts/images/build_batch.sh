#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"

if [ -f "$ENV_FILE" ]; then
    source "$ENV_FILE"
fi


APPS_DIR="$(cd "${SCRIPT_DIR}/../../apps" && pwd)" # go to apps directory
cd "${APPS_DIR}"

REGISTRY="${REGISTRY}"
IMAGE_NAME="nyc-taxi-batch"
TAG="v1.0"

docker build -t ${REGISTRY}/${IMAGE_NAME}:${TAG} -f ingestion/batch/historical_to_bronze/Dockerfile .

docker push ${REGISTRY}/${IMAGE_NAME}:${TAG}



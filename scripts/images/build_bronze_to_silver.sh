#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"

if [ -f "$ENV_FILE" ]; then
    source "$ENV_FILE"
fi
REGISTRY="${REGISTRY:-localhost:5000}"


APPS_DIR="$(cd "${SCRIPT_DIR}/../../apps" && pwd)" # go to apps directory
cd "${APPS_DIR}"

IMAGE_NAME="nyc-taxi-silver-consumer"
TAG="v1.0"

docker build -t ${REGISTRY}/${IMAGE_NAME}:${TAG} -f processing/bronze_to_silver/Dockerfile .

docker push ${REGISTRY}/${IMAGE_NAME}:${TAG}


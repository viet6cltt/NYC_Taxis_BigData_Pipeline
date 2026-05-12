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
TARGET="${1:-all}"

APPS_DIR="$(cd "${SCRIPT_DIR}/../../apps" && pwd)"
cd "${APPS_DIR}"

build_and_push() {
    local image_name="$1"
    local dockerfile="$2"
    local image="${REGISTRY}/${image_name}:${TAG}"

    echo "--- Building ${image} ---"
    docker build -t "${image}" -f "${dockerfile}" .

    echo "--- Pushing ${image} ---"
    docker push "${image}"
}

build_batch() {
    build_and_push "nyc-taxi-batch" "ingestion/batch/historical_to_bronze/Dockerfile"
}

build_replay() {
    build_and_push "nyc-taxi-replay" "ingestion/streaming/replay_producer/Dockerfile"
}

build_streaming_consumer() {
    build_and_push "nyc-taxi-streaming-consumer" "ingestion/streaming/kafka_to_bronze/Dockerfile"
}

build_silver_consumer() {
    build_and_push "nyc-taxi-silver-consumer" "processing/bronze_to_silver/Dockerfile"
}

case "$TARGET" in
    all)
        build_batch
        build_replay
        build_streaming_consumer
        build_silver_consumer
        ;;
    batch)
        build_batch
        ;;
    replay)
        build_replay
        ;;
    bronze_consumer)
        build_streaming_consumer
        ;;
    silver_consumer)
        build_silver_consumer
        ;;
    *)
        echo "Usage: $0 [all|batch|replay|bronze_consumer|silver_consumer]"
        exit 1
        ;;
esac

echo "=== Image build completed: target=${TARGET}, registry=${REGISTRY}, tag=${TAG} ==="

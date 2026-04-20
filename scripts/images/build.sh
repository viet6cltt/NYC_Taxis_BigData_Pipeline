#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APPS_DIR="$(cd "${SCRIPT_DIR}/../../apps" && pwd)" # go to apps directory
cd "${APPS_DIR}"

TARGET=${1:-all} 

build_batch() {
    echo "--- Building Batch Image ---"
    docker build -t nyc-taxi-batch:v1 -f ingestion/batch/historical_to_bronze/Dockerfile .
}

build_replay() {
    echo "--- Building Streaming Replay Image ---"
    docker build -t nyc-taxi-replay:v1 -f ingestion/streaming/replay_producer/Dockerfile .
}

build_streaming_consumer() {
    echo "--- Building Streaming Consumer Image ---"
    docker build -t nyc-taxi-streaming-consumer:v1 -f ingestion/streaming/kafka_to_bronze/Dockerfile .
}

build_silver_consumer() {
    echo "--- Building Silver Consumer Image ---"
    docker build -t nyc-taxi-silver-consumer:v1 -f processing/bronze_to_silver/Dockerfile .
}

if [ "$TARGET" == "batch" ]; then
    build_batch
elif [ "$TARGET" == "replay" ]; then
    build_replay
elif [ "$TARGET" == "bronze_consumer" ]; then
    build_streaming_consumer
elif [ "$TARGET" == "silver_consumer" ]; then
    build_silver_consumer
else
    build_batch
    build_replay
    build_streaming_consumer
    build_silver_consumer
fi
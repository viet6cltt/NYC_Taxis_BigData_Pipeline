#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INGESTION_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${INGESTION_DIR}"

TARGET=${1:-all} 

build_batch() {
    echo "--- Building Batch Image ---"
    docker build -t nyc-taxi-batch:v1 -f batch/historical_to_bronze/Dockerfile .
}

build_replay() {
    echo "--- Building Streaming Replay Image ---"
    docker build -t nyc-taxi-replay:v1 -f streaming/replay_producer/Dockerfile .
}

if [ "$TARGET" == "batch" ]; then
    build_batch
elif [ "$TARGET" == "replay" ]; then
    build_replay
else
    build_batch
    build_replay
fi
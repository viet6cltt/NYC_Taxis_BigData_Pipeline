#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"

[ -f "$ENV_FILE" ] && source "$ENV_FILE"

NAMESPACE="${BENCHMARK_NAMESPACE:-lakehouse}"
INGESTION_NAMESPACE="${BENCHMARK_INGESTION_NAMESPACE:-ingestion}"
REGISTRY="${REGISTRY:-localhost:5000}"
TAG="${TAG:-v1.0}"
EVENT_KIND="${BENCHMARK_EVENT_KIND:-started}"
YEAR="${BENCHMARK_YEAR:-2024}"
DATA_DIR="${BENCHMARK_DATA_DIR:-/data/yellow_data}"
RUN_SECONDS="${BENCHMARK_RUN_SECONDS:-300}"
SAMPLE_INTERVAL_SECONDS="${BENCHMARK_SAMPLE_INTERVAL_SECONDS:-10}"
TRIGGER_INTERVAL="${BENCHMARK_TRIGGER_INTERVAL:-10 seconds}"
STREAMING_BATCH_SIZE="${BENCHMARK_STREAMING_BATCH_SIZE:-100}"
STREAMING_MAX_SLEEP_SECONDS="${BENCHMARK_STREAMING_MAX_SLEEP_SECONDS:-5}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-my-kafka-cluster-kafka-bootstrap.ingestion.svc.cluster.local:9092}"
KAFKA_STARTED_TOPIC="${KAFKA_STARTED_TOPIC:-nyc-taxi-trip-started}"
KAFKA_COMPLETED_TOPIC="${KAFKA_COMPLETED_TOPIC:-nyc-taxi-trip-completed}"
SCENARIOS="${BENCHMARK_SCENARIOS:-low:1:500,medium:5:2000,high:20:10000}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
OUTPUT_ROOT="${BENCHMARK_OUTPUT_DIR:-${REPO_ROOT}/benchmark_results/streaming_${RUN_ID}}"
SUMMARY_CSV="${OUTPUT_ROOT}/summary.csv"

mkdir -p "$OUTPUT_ROOT"

log() {
    printf '[benchmark] %s\n' "$*"
}

find_spark_driver_pod() {
    local app_name="$1"
    kubectl get pods -n "$NAMESPACE" --no-headers 2>/dev/null \
        | awk -v app="$app_name" '$1 ~ app && $1 ~ /driver/ {print $1}' \
        | tail -n 1
}

wait_for_spark_driver() {
    local app_name="$1"
    local pod=""
    for _ in $(seq 1 90); do
        pod="$(find_spark_driver_pod "$app_name")"
        if [ -n "$pod" ]; then
            local phase
            phase="$(kubectl get pod "$pod" -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
            if [ "$phase" = "Running" ]; then
                echo "$pod"
                return 0
            fi
        fi
        sleep 2
    done
    return 1
}

kafka_pod() {
    kubectl get pods -n "$INGESTION_NAMESPACE" --no-headers 2>/dev/null \
        | awk '/my-kafka-cluster/ && /Running/ {print $1}' \
        | head -n 1
}

sample_resources() {
    local scenario_dir="$1"
    local end_epoch="$2"
    local out="${scenario_dir}/resource_samples.tsv"
    while [ "$(date +%s)" -lt "$end_epoch" ]; do
        {
            printf '\n# timestamp=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
            kubectl top pod -n "$NAMESPACE" --containers 2>&1 || true
            kubectl top pod -n "$INGESTION_NAMESPACE" --containers 2>&1 || true
        } >> "$out"
        sleep "$SAMPLE_INTERVAL_SECONDS"
    done
}

sample_kafka_lag() {
    local scenario_dir="$1"
    local group_id="$2"
    local end_epoch="$3"
    local pod
    pod="$(kafka_pod || true)"
    if [ -z "$pod" ]; then
        echo "No Kafka pod found in namespace=${INGESTION_NAMESPACE}" > "${scenario_dir}/kafka_lag_samples.txt"
        return 0
    fi

    while [ "$(date +%s)" -lt "$end_epoch" ]; do
        {
            printf '\n# timestamp=%s group=%s pod=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$group_id" "$pod"
            kubectl exec -n "$INGESTION_NAMESPACE" "$pod" -- \
                /opt/kafka/bin/kafka-consumer-groups.sh \
                --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
                --describe \
                --group "$group_id" 2>&1 || true
        } >> "${scenario_dir}/kafka_lag_samples.txt"
        sleep "$SAMPLE_INTERVAL_SECONDS"
    done
}

cleanup_spark_app() {
    local app_name="$1"
    kubectl get pods -n "$NAMESPACE" -o name 2>/dev/null \
        | grep "$app_name" \
        | xargs -r kubectl delete -n "$NAMESPACE" --ignore-not-found=true >/dev/null 2>&1 || true
}

cleanup_replay_job() {
    local job_name="$1"
    kubectl delete job -n "$NAMESPACE" "$job_name" --ignore-not-found=true >/dev/null 2>&1 || true
}

run_replay_job() {
    local scenario="$1"
    local speed_multiplier="$2"
    local job_name="nyc-taxi-replay-bench-${scenario}-${RUN_ID,,}"

    cleanup_replay_job "$job_name"
    cat <<YAML | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job_name}
  namespace: ${NAMESPACE}
  labels:
    app: nyc-taxi-replay-benchmark
    benchmark-scenario: ${scenario}
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        app: nyc-taxi-replay-benchmark
        benchmark-scenario: ${scenario}
    spec:
      restartPolicy: Never
      containers:
        - name: replay
          image: ${REGISTRY}/nyc-taxi-replay:${TAG}
          imagePullPolicy: Always
          env:
            - name: KAFKA_STARTED_TOPIC
              value: "${KAFKA_STARTED_TOPIC}"
            - name: KAFKA_COMPLETED_TOPIC
              value: "${KAFKA_COMPLETED_TOPIC}"
            - name: KAFKA_BOOTSTRAP_SERVERS
              value: "${KAFKA_BOOTSTRAP_SERVERS}"
            - name: DATA_DIR
              value: "${DATA_DIR}"
            - name: YEAR
              value: "${YEAR}"
            - name: LOOP_STREAMING
              value: "false"
            - name: STREAMING_SPEED_MULTIPLIER
              value: "${speed_multiplier}"
            - name: STREAMING_BATCH_SIZE
              value: "${STREAMING_BATCH_SIZE}"
            - name: STREAMING_MAX_SLEEP_SECONDS
              value: "${STREAMING_MAX_SLEEP_SECONDS}"
          resources:
            requests:
              cpu: "250m"
              memory: "512Mi"
            limits:
              cpu: "1"
              memory: "2Gi"
          volumeMounts:
            - name: taxi-data
              mountPath: /data
      volumes:
        - name: taxi-data
          persistentVolumeClaim:
            claimName: nfs-nyc-taxi-pvc
YAML
}

run_scenario() {
    local scenario="$1"
    local speed_multiplier="$2"
    local max_offsets="$3"
    local scenario_dir="${OUTPUT_ROOT}/${scenario}"
    local app_name="nyc-taxi-${EVENT_KIND}-to-bronze"
    local group_id_prefix="nyc-taxi-bench-${scenario}-${RUN_ID}"
    local group_id="${group_id_prefix}-${EVENT_KIND}"
    local replay_job_name="nyc-taxi-replay-bench-${scenario}-${RUN_ID,,}"
    local checkpoint="s3a://lakehouse/_checkpoints/benchmark/${RUN_ID}/${scenario}/${EVENT_KIND}"
    local output_path="s3a://lakehouse/benchmark/bronze/${RUN_ID}/${scenario}/${EVENT_KIND}"

    mkdir -p "$scenario_dir"
    log "Scenario=${scenario}, speed=${speed_multiplier}, maxOffsets=${max_offsets}, seconds=${RUN_SECONDS}"

    cleanup_spark_app "$app_name"

    (
        cd "$REPO_ROOT"
        KAFKA_BOOTSTRAP_SERVERS="$KAFKA_BOOTSTRAP_SERVERS" \
        KAFKA_STARTED_TOPIC="$KAFKA_STARTED_TOPIC" \
        KAFKA_COMPLETED_TOPIC="$KAFKA_COMPLETED_TOPIC" \
        STARTED_OUTPUT_PATH="$output_path" \
        STARTED_CHECKPOINT_LOCATION="$checkpoint" \
        TRIGGER_INTERVAL="$TRIGGER_INTERVAL" \
        MAX_OFFSETS_PER_TRIGGER="$max_offsets" \
        KAFKA_GROUP_ID_PREFIX="$group_id_prefix" \
        BENCHMARK_METRICS_ENABLED=true \
        REGISTRY="$REGISTRY" \
        bash scripts/ingestion/run_streaming.sh "$EVENT_KIND"
    ) > "${scenario_dir}/spark_submit.log" 2>&1 &
    local spark_submit_pid=$!

    local driver_pod
    if ! driver_pod="$(wait_for_spark_driver "$app_name")"; then
        log "Spark driver did not become Running. See ${scenario_dir}/spark_submit.log"
        kill "$spark_submit_pid" >/dev/null 2>&1 || true
        return 1
    fi
    echo "$driver_pod" > "${scenario_dir}/spark_driver_pod.txt"
    log "Spark driver pod=${driver_pod}"

    local end_epoch
    end_epoch="$(($(date +%s) + RUN_SECONDS))"
    sample_resources "$scenario_dir" "$end_epoch" &
    local resource_pid=$!
    sample_kafka_lag "$scenario_dir" "$group_id" "$end_epoch" &
    local lag_pid=$!

    run_replay_job "$scenario" "$speed_multiplier"
    sleep "$RUN_SECONDS"

    kubectl logs -n "$NAMESPACE" "$driver_pod" > "${scenario_dir}/spark_driver.log" 2>&1 || true
    kubectl logs -n "$NAMESPACE" "job/${replay_job_name}" \
        > "${scenario_dir}/replay_job.log" 2>&1 || true

    kill "$resource_pid" "$lag_pid" >/dev/null 2>&1 || true
    wait "$resource_pid" "$lag_pid" 2>/dev/null || true
    kill "$spark_submit_pid" >/dev/null 2>&1 || true
    cleanup_replay_job "$replay_job_name"
    cleanup_spark_app "$app_name"

    python3 "${SCRIPT_DIR}/parse_streaming_benchmark.py" \
        --scenario "$scenario" \
        --speed-multiplier "$speed_multiplier" \
        --max-offsets-per-trigger "$max_offsets" \
        --spark-log "${scenario_dir}/spark_driver.log" \
        --output "$SUMMARY_CSV" \
        --notes "logs=${scenario_dir}"
}

IFS=',' read -ra scenario_items <<< "$SCENARIOS"
for item in "${scenario_items[@]}"; do
    IFS=':' read -r scenario speed max_offsets <<< "$item"
    run_scenario "$scenario" "$speed" "$max_offsets"
done

log "Benchmark complete: ${SUMMARY_CSV}"
column -s, -t "$SUMMARY_CSV" 2>/dev/null || cat "$SUMMARY_CSV"

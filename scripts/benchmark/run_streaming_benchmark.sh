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
DEFAULT_PRODUCER_REPLICAS="${BENCHMARK_PRODUCER_REPLICAS:-1}"
DEFAULT_KAFKA_PARTITIONS="${BENCHMARK_KAFKA_PARTITIONS:-6}"
DEFAULT_KAFKA_REPLICAS="${BENCHMARK_KAFKA_REPLICAS:-3}"
DEFAULT_SPARK_EXECUTORS="${BENCHMARK_SPARK_EXECUTORS:-1}"
DEFAULT_SPARK_SHUFFLE_PARTITIONS="${BENCHMARK_SPARK_SHUFFLE_PARTITIONS:-4}"
WARMUP_SECONDS="${BENCHMARK_WARMUP_SECONDS:-30}"
TOPIC_RETENTION_MS="${BENCHMARK_TOPIC_RETENTION_MS:-3600000}"
TOPIC_PREFIX="${BENCHMARK_TOPIC_PREFIX:-nyc-taxi-bench}"
KEEP_TOPICS="${BENCHMARK_KEEP_TOPICS:-false}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
OUTPUT_ROOT="${BENCHMARK_OUTPUT_DIR:-${REPO_ROOT}/benchmark_results/streaming_${RUN_ID}}"
SUMMARY_CSV="${OUTPUT_ROOT}/summary.csv"

mkdir -p "$OUTPUT_ROOT"

log() {
    printf '[benchmark] %s\n' "$*"
}

resource_name() {
    local value="$1"
    value="$(printf '%s' "$value" | tr '[:upper:]_' '[:lower:]-' | tr -cd 'a-z0-9-')"
    value="${value#-}"
    value="${value%-}"
    printf '%s\n' "${value:-scenario}"
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

    cat > "${scenario_dir}/kafka_lag_samples.txt" <<EOF
# Kafka consumer-group lag is not sampled for this Spark Structured Streaming run.
# group=${group_id}
# Spark stores source progress in its checkpoint and reports latest_offset/end_offset
# in the benchmark progress log. parse_streaming_benchmark.py derives kafka lag from
# those Spark offsets so the metric matches the microbatch source backlog.
EOF

    while [ "$(date +%s)" -lt "$end_epoch" ]; do
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

cleanup_topics() {
    local started_topic="$1"
    local completed_topic="$2"
    if [ "$KEEP_TOPICS" = "true" ]; then
        return 0
    fi
    kubectl delete kafkatopic -n "$INGESTION_NAMESPACE" "$started_topic" "$completed_topic" \
        --ignore-not-found=true >/dev/null 2>&1 || true
}

create_benchmark_topics() {
    local started_topic="$1"
    local completed_topic="$2"
    local partitions="$3"
    local replicas="$4"

    cat <<YAML | kubectl apply -f -
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: ${started_topic}
  namespace: ${INGESTION_NAMESPACE}
  labels:
    strimzi.io/cluster: my-kafka-cluster
    app: nyc-taxi-benchmark
spec:
  partitions: ${partitions}
  replicas: ${replicas}
  config:
    retention.ms: ${TOPIC_RETENTION_MS}
    segment.bytes: 268435456
---
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: ${completed_topic}
  namespace: ${INGESTION_NAMESPACE}
  labels:
    strimzi.io/cluster: my-kafka-cluster
    app: nyc-taxi-benchmark
spec:
  partitions: ${partitions}
  replicas: ${replicas}
  config:
    retention.ms: ${TOPIC_RETENTION_MS}
    segment.bytes: 268435456
YAML
    kubectl wait -n "$INGESTION_NAMESPACE" --for=condition=Ready --timeout=120s \
        "kafkatopic/${started_topic}" >/dev/null 2>&1 || true
    kubectl wait -n "$INGESTION_NAMESPACE" --for=condition=Ready --timeout=120s \
        "kafkatopic/${completed_topic}" >/dev/null 2>&1 || true
}

run_replay_job() {
    local scenario="$1"
    local speed_multiplier="$2"
    local producer_index="$3"
    local started_topic="$4"
    local completed_topic="$5"
    local job_name="nyc-taxi-replay-bench-${scenario}-${producer_index}-${RUN_ID,,}"

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
              value: "${started_topic}"
            - name: KAFKA_COMPLETED_TOPIC
              value: "${completed_topic}"
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
              cpu: "2"
              memory: "3Gi"
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
    local producer_replicas="$4"
    local kafka_partitions="$5"
    local kafka_replicas="$6"
    local spark_executors="$7"
    local shuffle_partitions="$8"
    local scenario_id
    scenario_id="$(resource_name "$scenario")"
    local scenario_dir="${OUTPUT_ROOT}/${scenario}"
    local app_name="nyc-taxi-${EVENT_KIND}-to-bronze"
    local group_id_prefix="nyc-taxi-bench-${scenario_id}-${RUN_ID}"
    local group_id="${group_id_prefix}-${EVENT_KIND}"
    local started_topic="${TOPIC_PREFIX}-${RUN_ID,,}-${scenario_id}-started"
    local completed_topic="${TOPIC_PREFIX}-${RUN_ID,,}-${scenario_id}-completed"
    local kafka_topic="$started_topic"
    if [ "$EVENT_KIND" = "completed" ]; then
        kafka_topic="$completed_topic"
    fi
    local checkpoint="s3a://lakehouse/_checkpoints/benchmark/${RUN_ID}/${scenario}/${EVENT_KIND}"
    local output_path="s3a://lakehouse/benchmark/bronze/${RUN_ID}/${scenario}/${EVENT_KIND}"

    mkdir -p "$scenario_dir"
    log "Scenario=${scenario}, speed=${speed_multiplier}, maxOffsets=${max_offsets}, producerReplicas=${producer_replicas}, kafkaPartitions=${kafka_partitions}, kafkaReplicas=${kafka_replicas}, sparkExecutors=${spark_executors}, shufflePartitions=${shuffle_partitions}, seconds=${RUN_SECONDS}"

    cat > "${scenario_dir}/scenario.env" <<EOF
scenario=${scenario}
speed_multiplier=${speed_multiplier}
max_offsets_per_trigger=${max_offsets}
producer_replicas=${producer_replicas}
kafka_partitions=${kafka_partitions}
kafka_replicas=${kafka_replicas}
spark_executors=${spark_executors}
spark_shuffle_partitions=${shuffle_partitions}
event_kind=${EVENT_KIND}
kafka_topic=${kafka_topic}
started_topic=${started_topic}
completed_topic=${completed_topic}
run_seconds=${RUN_SECONDS}
warmup_seconds=${WARMUP_SECONDS}
trigger_interval=${TRIGGER_INTERVAL}
EOF

    cleanup_spark_app "$app_name"
    cleanup_topics "$started_topic" "$completed_topic"
    create_benchmark_topics "$started_topic" "$completed_topic" "$kafka_partitions" "$kafka_replicas"

    (
        cd "$REPO_ROOT"
        KAFKA_BOOTSTRAP_SERVERS="$KAFKA_BOOTSTRAP_SERVERS" \
        KAFKA_STARTED_TOPIC="$started_topic" \
        KAFKA_COMPLETED_TOPIC="$completed_topic" \
        STARTED_OUTPUT_PATH="$output_path" \
        STARTED_CHECKPOINT_LOCATION="$checkpoint" \
        TRIGGER_INTERVAL="$TRIGGER_INTERVAL" \
        MAX_OFFSETS_PER_TRIGGER="$max_offsets" \
        KAFKA_GROUP_ID_PREFIX="$group_id_prefix" \
        BENCHMARK_METRICS_ENABLED=true \
        SPARK_EXECUTOR_INSTANCES="$spark_executors" \
        SPARK_SQL_SHUFFLE_PARTITIONS="$shuffle_partitions" \
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

    for producer_index in $(seq 1 "$producer_replicas"); do
        run_replay_job "$scenario_id" "$speed_multiplier" "$producer_index" "$started_topic" "$completed_topic"
    done
    sleep "$RUN_SECONDS"

    kubectl logs -n "$NAMESPACE" "$driver_pod" > "${scenario_dir}/spark_driver.log" 2>&1 || true
    for producer_index in $(seq 1 "$producer_replicas"); do
        local replay_job_name="nyc-taxi-replay-bench-${scenario_id}-${producer_index}-${RUN_ID,,}"
        kubectl logs -n "$NAMESPACE" "job/${replay_job_name}" \
            > "${scenario_dir}/replay_job_${producer_index}.log" 2>&1 || true
    done

    kill "$resource_pid" "$lag_pid" >/dev/null 2>&1 || true
    wait "$resource_pid" "$lag_pid" 2>/dev/null || true
    kill "$spark_submit_pid" >/dev/null 2>&1 || true
    for producer_index in $(seq 1 "$producer_replicas"); do
        cleanup_replay_job "nyc-taxi-replay-bench-${scenario_id}-${producer_index}-${RUN_ID,,}"
    done
    cleanup_spark_app "$app_name"
    cleanup_topics "$started_topic" "$completed_topic"

    python3 "${SCRIPT_DIR}/parse_streaming_benchmark.py" \
        --scenario "$scenario" \
        --speed-multiplier "$speed_multiplier" \
        --max-offsets-per-trigger "$max_offsets" \
        --producer-replicas "$producer_replicas" \
        --kafka-partitions "$kafka_partitions" \
        --kafka-replicas "$kafka_replicas" \
        --spark-executors "$spark_executors" \
        --spark-shuffle-partitions "$shuffle_partitions" \
        --warmup-seconds "$WARMUP_SECONDS" \
        --spark-log "${scenario_dir}/spark_driver.log" \
        --kafka-lag-log "${scenario_dir}/kafka_lag_samples.txt" \
        --resource-log "${scenario_dir}/resource_samples.tsv" \
        --output "$SUMMARY_CSV" \
        --notes "logs=${scenario_dir}"
}

IFS=',' read -ra scenario_items <<< "$SCENARIOS"
for item in "${scenario_items[@]}"; do
    IFS=':' read -r scenario speed max_offsets producer_replicas kafka_partitions kafka_replicas spark_executors shuffle_partitions <<< "$item"
    producer_replicas="${producer_replicas:-$DEFAULT_PRODUCER_REPLICAS}"
    kafka_partitions="${kafka_partitions:-$DEFAULT_KAFKA_PARTITIONS}"
    kafka_replicas="${kafka_replicas:-$DEFAULT_KAFKA_REPLICAS}"
    spark_executors="${spark_executors:-$DEFAULT_SPARK_EXECUTORS}"
    shuffle_partitions="${shuffle_partitions:-$DEFAULT_SPARK_SHUFFLE_PARTITIONS}"
    run_scenario "$scenario" "$speed" "$max_offsets" "$producer_replicas" "$kafka_partitions" "$kafka_replicas" "$spark_executors" "$shuffle_partitions"
done

log "Benchmark complete: ${SUMMARY_CSV}"
column -s, -t "$SUMMARY_CSV" 2>/dev/null || cat "$SUMMARY_CSV"

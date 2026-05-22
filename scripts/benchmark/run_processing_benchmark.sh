#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"

[ -f "$ENV_FILE" ] && source "$ENV_FILE"

RUN_ID="${PROCESSING_BENCHMARK_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
YEAR="${PROCESSING_BENCHMARK_YEAR:-2024}"
TRIALS="${PROCESSING_BENCHMARK_TRIALS:-3}"
BENCHMARK_ROOT="${PROCESSING_BENCHMARK_ROOT:-s3a://lakehouse/benchmark/processing}"
BRONZE_PATH="${PROCESSING_BENCHMARK_BRONZE_PATH:-s3a://lakehouse/bronze/nyc-taxi/trip_completed}"
FILTER_MONTH="${PROCESSING_BENCHMARK_FILTER_MONTH:-2024-01}"
FILTER_QUARTER="${PROCESSING_BENCHMARK_FILTER_QUARTER:-2024-01,2024-02,2024-03}"
PRUNING_TRIALS="${PROCESSING_BENCHMARK_PRUNING_TRIALS:-3}"
PRUNING_SOURCE_TRIAL="${PROCESSING_BENCHMARK_PRUNING_SOURCE_TRIAL:-1}"
MERGE_TRIALS="${PROCESSING_BENCHMARK_MERGE_TRIALS:-3}"
MERGE_SOURCE_TRIAL="${PROCESSING_BENCHMARK_MERGE_SOURCE_TRIAL:-1}"
MERGE_INCLUDE_ALL_MONTHS="${PROCESSING_BENCHMARK_MERGE_INCLUDE_ALL_MONTHS:-true}"
REQUIRE_FULL_YEAR="${PROCESSING_BENCHMARK_REQUIRE_FULL_YEAR:-true}"
OUTPUT_DIR="${PROCESSING_BENCHMARK_OUTPUT_DIR:-${REPO_ROOT}/benchmark_results/processing_${RUN_ID}}"
LOG_DIR="${OUTPUT_DIR}/logs"
STAGE_RUNS_JSONL="${OUTPUT_DIR}/stage_runs.jsonl"
SPARK_TOOL_FILE="local:///opt/spark/work-dir/app/processing_benchmark.py"
MERGE_TOOL_FILE="local:///opt/spark/work-dir/app/merge_benchmark.py"

SPARK_DRIVER_MEMORY="${PROCESSING_BENCHMARK_SPARK_DRIVER_MEMORY:-2g}"
SPARK_EXECUTOR_INSTANCES="${PROCESSING_BENCHMARK_SPARK_EXECUTOR_INSTANCES:-3}"
SPARK_EXECUTOR_CORES="${PROCESSING_BENCHMARK_SPARK_EXECUTOR_CORES:-2}"
SPARK_EXECUTOR_MEMORY="${PROCESSING_BENCHMARK_SPARK_EXECUTOR_MEMORY:-4g}"
SPARK_SHUFFLE_PARTITIONS="${PROCESSING_BENCHMARK_SPARK_SHUFFLE_PARTITIONS:-6}"

mkdir -p "$LOG_DIR" "${OUTPUT_DIR}/plans"
: > "$STAGE_RUNS_JSONL"

log() {
    printf '[processing-benchmark] %s\n' "$*"
}

epoch_ms() {
    local epoch_ns
    epoch_ns="$(date +%s%N)"
    printf '%s\n' "$((epoch_ns / 1000000))"
}

assert_spark_log_succeeded() {
    local label="$1"
    local log_path="$2"
    local final_exit_code
    local final_reason

    final_exit_code="$(awk -F ': ' '/^[[:space:]]*exit code:/{value=$2} END{print value}' "$log_path")"
    final_reason="$(awk -F ': ' '/^[[:space:]]*termination reason:/{value=$2} END{print value}' "$log_path")"

    if [ -z "$final_exit_code" ] || [ -z "$final_reason" ]; then
        log "Spark final status was not found for ${label}; see ${log_path}"
        return 1
    fi

    if [ "$final_exit_code" != "0" ] || [ "$final_reason" != "Completed" ]; then
        log "Spark application failed for ${label}; see ${log_path}"
        return 1
    fi
}

append_driver_log() {
    local label="$1"
    local log_path="$2"
    local driver_pod

    driver_pod="$(awk '/pod name: .*driver/{pod=$NF} END{print pod}' "$log_path")"
    if [ -z "$driver_pod" ]; then
        log "Spark driver pod was not found for ${label}; metrics may be missing from ${log_path}"
        return 0
    fi

    {
        printf '\n--- Spark driver log: %s ---\n' "$driver_pod"
        kubectl logs -n lakehouse "$driver_pod"
    } >> "$log_path" 2>&1 || log "Could not append Spark driver log for ${label}; see ${log_path}"
}

assert_and_append_driver_log() {
    local label="$1"
    local log_path="$2"

    append_driver_log "$label" "$log_path"
    assert_spark_log_succeeded "$label" "$log_path"
}

append_stage_run() {
    local trial="$1"
    local stage="$2"
    local start_ms="$3"
    local end_ms="$4"
    local input_path="$5"
    local output_path="$6"
    local log_path="$7"

    printf '{"trial":%s,"stage":"%s","start_epoch_ms":%s,"end_epoch_ms":%s,"duration_ms":%s,"input_path":"%s","output_path":"%s","log":"%s","spark_executor_instances":"%s","spark_executor_cores":"%s","spark_executor_memory":"%s","spark_driver_memory":"%s","spark_shuffle_partitions":"%s"}\n' \
        "$trial" \
        "$stage" \
        "$start_ms" \
        "$end_ms" \
        "$((end_ms - start_ms))" \
        "$input_path" \
        "$output_path" \
        "$log_path" \
        "$SPARK_EXECUTOR_INSTANCES" \
        "$SPARK_EXECUTOR_CORES" \
        "$SPARK_EXECUTOR_MEMORY" \
        "$SPARK_DRIVER_MEMORY" \
        "$SPARK_SHUFFLE_PARTITIONS" \
        >> "$STAGE_RUNS_JSONL"
}

feature_submit() {
    APP_FILE="$SPARK_TOOL_FILE" \
    SPARK_APP_NAME="nyc-taxi-processing-benchmark-helper" \
    GOLD_JOB="features" \
    SPARK_DRIVER_MEMORY="$SPARK_DRIVER_MEMORY" \
    SPARK_EXECUTOR_INSTANCES="$SPARK_EXECUTOR_INSTANCES" \
    SPARK_EXECUTOR_CORES="$SPARK_EXECUTOR_CORES" \
    SPARK_EXECUTOR_MEMORY="$SPARK_EXECUTOR_MEMORY" \
    SPARK_SHUFFLE_PARTITIONS="$SPARK_SHUFFLE_PARTITIONS" \
    BENCHMARK_EXPECTED_YEAR="$YEAR" \
    BENCHMARK_REQUIRE_FULL_YEAR="$REQUIRE_FULL_YEAR" \
    BENCHMARK_FILTER_MONTH="$FILTER_MONTH" \
    BENCHMARK_FILTER_QUARTER="$FILTER_QUARTER" \
    BENCHMARK_PRUNING_TRIALS="$PRUNING_TRIALS" \
    bash "${REPO_ROOT}/scripts/training/run_feature_engineering.sh" features
}

merge_submit() {
    APP_FILE="$MERGE_TOOL_FILE" \
    SPARK_APP_NAME="nyc-taxi-lifecycle-merge-benchmark" \
    SPARK_DRIVER_MEMORY="$SPARK_DRIVER_MEMORY" \
    SPARK_EXECUTOR_INSTANCES="$SPARK_EXECUTOR_INSTANCES" \
    SPARK_EXECUTOR_CORES="$SPARK_EXECUTOR_CORES" \
    SPARK_EXECUTOR_MEMORY="$SPARK_EXECUTOR_MEMORY" \
    SPARK_SHUFFLE_PARTITIONS="$SPARK_SHUFFLE_PARTITIONS" \
    BENCHMARK_FILTER_MONTH="$FILTER_MONTH" \
    BENCHMARK_FILTER_QUARTER="$FILTER_QUARTER" \
    BENCHMARK_MERGE_TRIALS="$MERGE_TRIALS" \
    BENCHMARK_MERGE_INCLUDE_ALL_MONTHS="$MERGE_INCLUDE_ALL_MONTHS" \
    bash "${REPO_ROOT}/scripts/processing/run_silver.sh" completed batch
}

inspect_delta() {
    local label="$1"
    local path="$2"
    local log_path="${LOG_DIR}/${label}.log"

    log "Inspect ${label}: ${path}"
    BENCHMARK_ACTION=inspect \
    BENCHMARK_INPUT_PATH="$path" \
    feature_submit > "$log_path" 2>&1
    assert_and_append_driver_log "$label" "$log_path"
}

run_stage() {
    local trial="$1"
    local stage="$2"
    local input_path="$3"
    local output_path="$4"
    local log_path="$5"
    shift 5

    local start_ms
    local end_ms
    start_ms="$(epoch_ms)"
    log "Trial ${trial}: ${stage}"
    "$@" > "$log_path" 2>&1
    assert_and_append_driver_log "trial ${trial} ${stage}" "$log_path"
    end_ms="$(epoch_ms)"
    append_stage_run "$trial" "$stage" "$start_ms" "$end_ms" "$input_path" "$output_path" "$log_path"
}

run_silver_completed() {
    local output_path="$1"
    BRONZE_COMPLETED_PATH="$BRONZE_PATH" \
    SILVER_COMPLETED_PATH="$output_path" \
    SPARK_DRIVER_MEMORY="$SPARK_DRIVER_MEMORY" \
    SPARK_EXECUTOR_INSTANCES="$SPARK_EXECUTOR_INSTANCES" \
    SPARK_EXECUTOR_CORES="$SPARK_EXECUTOR_CORES" \
    SPARK_EXECUTOR_MEMORY="$SPARK_EXECUTOR_MEMORY" \
    SPARK_SHUFFLE_PARTITIONS="$SPARK_SHUFFLE_PARTITIONS" \
    bash "${REPO_ROOT}/scripts/processing/run_silver.sh" completed batch
}

run_gold_route_estimates() {
    local silver_path="$1"
    local route_path="$2"
    SILVER_COMPLETED_PATH="$silver_path" \
    GOLD_ROUTE_ESTIMATES_PATH="$route_path" \
    SPARK_DRIVER_MEMORY="$SPARK_DRIVER_MEMORY" \
    SPARK_EXECUTOR_INSTANCES="$SPARK_EXECUTOR_INSTANCES" \
    SPARK_EXECUTOR_CORES="$SPARK_EXECUTOR_CORES" \
    SPARK_EXECUTOR_MEMORY="$SPARK_EXECUTOR_MEMORY" \
    SPARK_SHUFFLE_PARTITIONS="$SPARK_SHUFFLE_PARTITIONS" \
    bash "${REPO_ROOT}/scripts/training/run_feature_engineering.sh" route_estimates
}

run_gold_features() {
    local silver_path="$1"
    local route_path="$2"
    local features_path="$3"
    SILVER_COMPLETED_PATH="$silver_path" \
    GOLD_ROUTE_ESTIMATES_PATH="$route_path" \
    GOLD_FEATURES_PATH="$features_path" \
    SPARK_DRIVER_MEMORY="$SPARK_DRIVER_MEMORY" \
    SPARK_EXECUTOR_INSTANCES="$SPARK_EXECUTOR_INSTANCES" \
    SPARK_EXECUTOR_CORES="$SPARK_EXECUTOR_CORES" \
    SPARK_EXECUTOR_MEMORY="$SPARK_EXECUTOR_MEMORY" \
    SPARK_SHUFFLE_PARTITIONS="$SPARK_SHUFFLE_PARTITIONS" \
    bash "${REPO_ROOT}/scripts/training/run_feature_engineering.sh" features
}

log "Preflight Bronze full-year ${YEAR}: ${BRONZE_PATH}"
BENCHMARK_ACTION=preflight \
BENCHMARK_INPUT_PATH="$BRONZE_PATH" \
feature_submit > "${LOG_DIR}/preflight_bronze.log" 2>&1
assert_and_append_driver_log "Bronze preflight" "${LOG_DIR}/preflight_bronze.log"
inspect_delta "inspect_bronze" "$BRONZE_PATH"

for trial in $(seq 1 "$TRIALS"); do
    trial_root="${BENCHMARK_ROOT}/${RUN_ID}/trial_${trial}"
    silver_path="${trial_root}/silver/trip_completed"
    route_path="${trial_root}/gold/route_estimates"
    features_path="${trial_root}/gold/features"
    e2e_start_ms="$(epoch_ms)"

    run_stage \
        "$trial" \
        "bronze_to_silver_completed_batch" \
        "$BRONZE_PATH" \
        "$silver_path" \
        "${LOG_DIR}/trial_${trial}_silver_completed.log" \
        run_silver_completed "$silver_path"
    inspect_delta "trial_${trial}_inspect_silver" "$silver_path"

    run_stage \
        "$trial" \
        "gold_route_estimates" \
        "$silver_path" \
        "$route_path" \
        "${LOG_DIR}/trial_${trial}_gold_route_estimates.log" \
        run_gold_route_estimates "$silver_path" "$route_path"
    inspect_delta "trial_${trial}_inspect_route_estimates" "$route_path"

    run_stage \
        "$trial" \
        "gold_features" \
        "$silver_path" \
        "$features_path" \
        "${LOG_DIR}/trial_${trial}_gold_features.log" \
        run_gold_features "$silver_path" "$route_path" "$features_path"
    inspect_delta "trial_${trial}_inspect_features" "$features_path"

    e2e_end_ms="$(epoch_ms)"
    append_stage_run \
        "$trial" \
        "end_to_end_bronze_to_gold" \
        "$e2e_start_ms" \
        "$e2e_end_ms" \
        "$BRONZE_PATH" \
        "$features_path" \
        "${LOG_DIR}/trial_${trial}_gold_features.log"
done

merge_source="${BENCHMARK_ROOT}/${RUN_ID}/trial_${MERGE_SOURCE_TRIAL}/silver/trip_completed"
merge_root="${BENCHMARK_ROOT}/${RUN_ID}/lifecycle_merge"
log "Run lifecycle MERGE benchmark from ${merge_source}"
BENCHMARK_ACTION=lifecycle_merge \
BENCHMARK_INPUT_PATH="$merge_source" \
BENCHMARK_PARTITIONED_PATH="${merge_root}/partitioned_lifecycle" \
BENCHMARK_UNPARTITIONED_PATH="${merge_root}/unpartitioned_lifecycle" \
merge_submit > "${LOG_DIR}/lifecycle_merge.log" 2>&1
assert_and_append_driver_log "Lifecycle MERGE" "${LOG_DIR}/lifecycle_merge.log"

pruning_source="${BENCHMARK_ROOT}/${RUN_ID}/trial_${PRUNING_SOURCE_TRIAL}/gold/features"
pruning_root="${BENCHMARK_ROOT}/${RUN_ID}/pruning"
log "Run Delta pruning benchmark from ${pruning_source}"
BENCHMARK_ACTION=pruning \
BENCHMARK_INPUT_PATH="$pruning_source" \
BENCHMARK_PARTITIONED_PATH="${pruning_root}/partitioned_features" \
BENCHMARK_UNPARTITIONED_PATH="${pruning_root}/unpartitioned_features" \
feature_submit > "${LOG_DIR}/pruning.log" 2>&1
assert_and_append_driver_log "Delta pruning" "${LOG_DIR}/pruning.log"

python3 "${SCRIPT_DIR}/parse_processing_benchmark.py" \
    --run-dir "$OUTPUT_DIR" \
    --run-id "$RUN_ID" \
    --year "$YEAR" \
    --bronze-path "$BRONZE_PATH"

log "Benchmark complete: ${OUTPUT_DIR}"
log "Report: ${OUTPUT_DIR}/report.md"

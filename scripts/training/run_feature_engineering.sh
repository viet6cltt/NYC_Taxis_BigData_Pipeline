#!/bin/bash
# Run Gold ML batch jobs:
#   route_estimates       Silver completed -> Gold route estimates
#   features              Silver completed + route estimates -> Gold training features
#   prediction_actuals    Gold predictions + Silver completed -> Gold delayed-label table
#   model_quality_daily   Gold prediction_actuals -> Gold model quality metrics
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"

if [ -f "$ENV_FILE" ]; then
    source "$ENV_FILE"
fi

K8S_API_SERVER=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
K8S_MASTER="k8s://${K8S_API_SERVER}"

SPARK_VERSION="4.1.1"
SPARK_DIR="$HOME/Downloads/spark-${SPARK_VERSION}-bin-hadoop3"
SPARK_TGZ="${SPARK_DIR}.tgz"
SPARK_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz"

# Kubernetes
K8S_API_SERVER=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
K8S_MASTER="k8s://${K8S_API_SERVER}"
NAMESPACE="lakehouse"
SERVICE_ACCOUNT="spark-user"
K8S_CA_CERT_FILE="${K8S_CA_CERT_FILE:-$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.certificate-authority}')}"
if [ -z "$K8S_CA_CERT_FILE" ]; then
    K8S_CA_CERT_FILE="/tmp/spark-k8s-ca.crt"
    kubectl config view --minify --raw -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' | base64 -d > "$K8S_CA_CERT_FILE"
fi
K8S_SUBMISSION_TOKEN_FILE="/tmp/spark-k8s-submission.token"
kubectl create token "$SERVICE_ACCOUNT" -n "$NAMESPACE" > "$K8S_SUBMISSION_TOKEN_FILE"
chmod 600 "$K8S_SUBMISSION_TOKEN_FILE"

GOLD_JOB="${1:-features}"
SILVER_COMPLETED_PATH="${SILVER_COMPLETED_PATH:-s3a://lakehouse/silver/nyc-taxi/trip_completed}"
GOLD_ROUTE_ESTIMATES_PATH="${GOLD_ROUTE_ESTIMATES_PATH:-s3a://lakehouse/gold/ml/route_estimates}"
GOLD_FEATURES_PATH="${GOLD_FEATURES_PATH:-s3a://lakehouse/gold/ml/features}"
GOLD_PREDICTIONS_PATH="${GOLD_PREDICTIONS_PATH:-s3a://lakehouse/gold/ml/predictions}"
GOLD_PREDICTION_ACTUALS_PATH="${GOLD_PREDICTION_ACTUALS_PATH:-s3a://lakehouse/gold/ml/prediction_actuals}"
GOLD_MODEL_QUALITY_DAILY_PATH="${GOLD_MODEL_QUALITY_DAILY_PATH:-s3a://lakehouse/gold/monitoring/model_quality_daily}"
WRITE_MODE="${WRITE_MODE:-overwrite}"

MINIO_INTERNAL_ENDPOINT="${MINIO_INTERNAL_ENDPOINT:-http://minio-api.storage.svc.cluster.local:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"

# Image
IMAGE="${REGISTRY:-localhost:5000}/nyc-taxi-feature-engineering:v1.0"
APP_FILE="local:///opt/spark/work-dir/app/main.py"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-1g}"
SPARK_EXECUTOR_INSTANCES="${SPARK_EXECUTOR_INSTANCES:-1}"
SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-2g}"
SPARK_EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-1}"
SPARK_DYNAMIC_ALLOCATION_ENABLED="${SPARK_DYNAMIC_ALLOCATION_ENABLED:-false}"
SPARK_DYNAMIC_ALLOCATION_SHUFFLE_TRACKING_ENABLED="${SPARK_DYNAMIC_ALLOCATION_SHUFFLE_TRACKING_ENABLED:-true}"
SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS="${SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS:-1}"
SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS="${SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS:-3}"
SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS="${SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS:-$SPARK_EXECUTOR_INSTANCES}"
BENCHMARK_METRICS_ENABLED="${BENCHMARK_METRICS_ENABLED:-true}"
N_LOCATION_CLUSTERS="${N_LOCATION_CLUSTERS:-5}"
N_TEMPORAL_CLUSTERS="${N_TEMPORAL_CLUSTERS:-4}"
MIN_TRIP_DISTANCE="${MIN_TRIP_DISTANCE:-0.05}"
MAX_TRIP_DISTANCE="${MAX_TRIP_DISTANCE:-100.0}"
MIN_TRIP_DURATION_SECONDS="${MIN_TRIP_DURATION_SECONDS:-60}"
MAX_TRIP_DURATION_SECONDS="${MAX_TRIP_DURATION_SECONDS:-14400}"
MIN_FARE_AMOUNT="${MIN_FARE_AMOUNT:-2.5}"
MAX_FARE_AMOUNT="${MAX_FARE_AMOUNT:-300.0}"
MAX_TOTAL_AMOUNT="${MAX_TOTAL_AMOUNT:-500.0}"
MIN_AVG_SPEED_MPH="${MIN_AVG_SPEED_MPH:-1.0}"
MAX_AVG_SPEED_MPH="${MAX_AVG_SPEED_MPH:-80.0}"
MAX_PASSENGER_COUNT="${MAX_PASSENGER_COUNT:-6}"

if [ ! -d "$SPARK_DIR" ]; then
    echo "--- Downloading Spark ${SPARK_VERSION} ---"
    mkdir -p "$HOME/Downloads"
    curl -L "${SPARK_URL}" -o "${SPARK_DIR}.tgz"
    tar -xzf "${SPARK_DIR}.tgz" -C "$HOME/Downloads"
    rm "${SPARK_DIR}.tgz"
fi

echo "--- Submitting Gold ML job=${GOLD_JOB} to Kubernetes ---"

dynamic_allocation_conf=()
if [ "$SPARK_DYNAMIC_ALLOCATION_ENABLED" = "true" ]; then
    dynamic_allocation_conf=(
        --conf "spark.dynamicAllocation.enabled=$SPARK_DYNAMIC_ALLOCATION_ENABLED"
        --conf "spark.dynamicAllocation.shuffleTracking.enabled=$SPARK_DYNAMIC_ALLOCATION_SHUFFLE_TRACKING_ENABLED"
        --conf "spark.dynamicAllocation.minExecutors=$SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS"
        --conf "spark.dynamicAllocation.maxExecutors=$SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS"
        --conf "spark.dynamicAllocation.initialExecutors=$SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS"
    )
fi

"$SPARK_DIR/bin/spark-submit" \
    --master "$K8S_MASTER" \
    --deploy-mode cluster \
    --name "nyc-taxi-gold-${GOLD_JOB}" \
    --conf spark.kubernetes.namespace="$NAMESPACE" \
    --conf spark.kubernetes.driver.node.selector.workload=spark \
    --conf spark.kubernetes.executor.node.selector.workload=spark \
    --conf spark.kubernetes.container.image="$IMAGE" \
    --conf spark.kubernetes.container.image.pullPolicy=Always \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName="$SERVICE_ACCOUNT" \
    --conf spark.kubernetes.authenticate.caCertFile="$K8S_CA_CERT_FILE" \
    --conf spark.kubernetes.authenticate.submission.caCertFile="$K8S_CA_CERT_FILE" \
    --conf spark.kubernetes.authenticate.submission.oauthTokenFile="$K8S_SUBMISSION_TOKEN_FILE" \
    --conf spark.kubernetes.authenticate.trustServerCertificate=true \
    --conf spark.ui.prometheus.enabled=true \
    --conf spark.kubernetes.driver.annotation.prometheus.io/scrape=true \
    --conf spark.kubernetes.driver.annotation.prometheus.io/path=/metrics/prometheus \
    --conf spark.kubernetes.driver.annotation.prometheus.io/port=4040 \
    \
    --conf spark.kubernetes.driverEnv.PYTHONPATH="/opt/spark/work-dir" \
    --conf spark.executorEnv.PYTHONPATH="/opt/spark/work-dir" \
    --conf spark.kubernetes.driverEnv.GOLD_JOB="$GOLD_JOB" \
    --conf spark.kubernetes.driverEnv.SILVER_COMPLETED_PATH="$SILVER_COMPLETED_PATH" \
    --conf spark.kubernetes.driverEnv.GOLD_ROUTE_ESTIMATES_PATH="$GOLD_ROUTE_ESTIMATES_PATH" \
    --conf spark.kubernetes.driverEnv.GOLD_FEATURES_PATH="$GOLD_FEATURES_PATH" \
    --conf spark.kubernetes.driverEnv.GOLD_PREDICTIONS_PATH="$GOLD_PREDICTIONS_PATH" \
    --conf spark.kubernetes.driverEnv.GOLD_PREDICTION_ACTUALS_PATH="$GOLD_PREDICTION_ACTUALS_PATH" \
    --conf spark.kubernetes.driverEnv.GOLD_MODEL_QUALITY_DAILY_PATH="$GOLD_MODEL_QUALITY_DAILY_PATH" \
    --conf spark.kubernetes.driverEnv.WRITE_MODE="$WRITE_MODE" \
    --conf spark.kubernetes.driverEnv.BENCHMARK_METRICS_ENABLED="$BENCHMARK_METRICS_ENABLED" \
    --conf spark.kubernetes.driverEnv.N_LOCATION_CLUSTERS="$N_LOCATION_CLUSTERS" \
    --conf spark.kubernetes.driverEnv.N_TEMPORAL_CLUSTERS="$N_TEMPORAL_CLUSTERS" \
    --conf spark.kubernetes.driverEnv.MIN_TRIP_DISTANCE="$MIN_TRIP_DISTANCE" \
    --conf spark.kubernetes.driverEnv.MAX_TRIP_DISTANCE="$MAX_TRIP_DISTANCE" \
    --conf spark.kubernetes.driverEnv.MIN_TRIP_DURATION_SECONDS="$MIN_TRIP_DURATION_SECONDS" \
    --conf spark.kubernetes.driverEnv.MAX_TRIP_DURATION_SECONDS="$MAX_TRIP_DURATION_SECONDS" \
    --conf spark.kubernetes.driverEnv.MIN_FARE_AMOUNT="$MIN_FARE_AMOUNT" \
    --conf spark.kubernetes.driverEnv.MAX_FARE_AMOUNT="$MAX_FARE_AMOUNT" \
    --conf spark.kubernetes.driverEnv.MAX_TOTAL_AMOUNT="$MAX_TOTAL_AMOUNT" \
    --conf spark.kubernetes.driverEnv.MIN_AVG_SPEED_MPH="$MIN_AVG_SPEED_MPH" \
    --conf spark.kubernetes.driverEnv.MAX_AVG_SPEED_MPH="$MAX_AVG_SPEED_MPH" \
    --conf spark.kubernetes.driverEnv.MAX_PASSENGER_COUNT="$MAX_PASSENGER_COUNT" \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    \
    --conf spark.hadoop.fs.s3a.endpoint="$MINIO_INTERNAL_ENDPOINT" \
    --conf spark.hadoop.fs.s3a.access.key="$MINIO_ACCESS_KEY" \
    --conf spark.hadoop.fs.s3a.secret.key="$MINIO_SECRET_KEY" \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    \
    --conf spark.driver.memory="$SPARK_DRIVER_MEMORY" \
    --conf spark.executor.instances="$SPARK_EXECUTOR_INSTANCES" \
    --conf spark.executor.cores="$SPARK_EXECUTOR_CORES" \
    --conf spark.executor.memory="$SPARK_EXECUTOR_MEMORY" \
    --conf spark.sql.shuffle.partitions=6 \
    --conf spark.sql.adaptive.enabled=true \
    \
    "${dynamic_allocation_conf[@]}" \
    "$APP_FILE"

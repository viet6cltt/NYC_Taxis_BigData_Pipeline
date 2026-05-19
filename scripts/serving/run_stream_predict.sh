#!/bin/bash
# Run Spark Streaming Inference:
# Silver trip_started -> route_estimates lookup -> XGBoost -> Gold predictions
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"
[ -f "$ENV_FILE" ] && source "$ENV_FILE"

K8S_API_SERVER=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
K8S_MASTER="k8s://${K8S_API_SERVER}"

SPARK_VERSION="4.1.1"
SPARK_DIR="$HOME/Downloads/spark-${SPARK_VERSION}-bin-hadoop3"

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
IMAGE="${REGISTRY:-localhost:5000}/nyc-taxi-stream-predict:v1.0"
IMAGE_PULL_POLICY="${IMAGE_PULL_POLICY:-Always}"
APP_FILE="local:///opt/spark/work-dir/app/main.py"

MINIO_INTERNAL_ENDPOINT="${MINIO_INTERNAL_ENDPOINT:-http://minio-api.storage.svc.cluster.local:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
MLFLOW_TRACKING_URI="${MLFLOW_TRACKING_URI:-http://mlflow.mlops.svc.cluster.local:5000}"
SILVER_STARTED_PATH="${SILVER_STARTED_PATH:-s3a://lakehouse/silver/nyc-taxi/trip_started}"
GOLD_ROUTE_ESTIMATES_PATH="${GOLD_ROUTE_ESTIMATES_PATH:-s3a://lakehouse/gold/ml/route_estimates}"
GOLD_PREDICTIONS_PATH="${GOLD_PREDICTIONS_PATH:-s3a://lakehouse/gold/ml/predictions}"
CHECKPOINT_LOCATION="${CHECKPOINT_LOCATION:-s3a://lakehouse/_checkpoints/gold/ml/stream_predict}"
TRIGGER_INTERVAL="${TRIGGER_INTERVAL:-30 seconds}"
STARTING_VERSION="${STARTING_VERSION:-}"
MAX_FILES_PER_TRIGGER="${MAX_FILES_PER_TRIGGER:-4}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-2g}"
SPARK_DRIVER_MEMORY_OVERHEAD="${SPARK_DRIVER_MEMORY_OVERHEAD:-1g}"
SPARK_EXECUTOR_INSTANCES="${SPARK_EXECUTOR_INSTANCES:-1}"
SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-4g}"
SPARK_EXECUTOR_MEMORY_OVERHEAD="${SPARK_EXECUTOR_MEMORY_OVERHEAD:-1g}"
SPARK_WAIT_APP_COMPLETION="${SPARK_WAIT_APP_COMPLETION:-true}"
SPARK_DYNAMIC_ALLOCATION_ENABLED="${SPARK_DYNAMIC_ALLOCATION_ENABLED:-false}"
SPARK_DYNAMIC_ALLOCATION_SHUFFLE_TRACKING_ENABLED="${SPARK_DYNAMIC_ALLOCATION_SHUFFLE_TRACKING_ENABLED:-true}"
SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS="${SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS:-1}"
SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS="${SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS:-2}"
SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS="${SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS:-$SPARK_EXECUTOR_INSTANCES}"
N_LOCATION_CLUSTERS="${N_LOCATION_CLUSTERS:-5}"
N_TEMPORAL_CLUSTERS="${N_TEMPORAL_CLUSTERS:-4}"

echo "--- Submitting Streaming Inference job to Kubernetes ---"

starting_version_conf=()
if [ -n "$STARTING_VERSION" ]; then
    starting_version_conf=(--conf "spark.kubernetes.driverEnv.STARTING_VERSION=$STARTING_VERSION")
fi

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
    --name nyc-taxi-stream-predict \
    --conf spark.kubernetes.namespace="$NAMESPACE" \
    --conf spark.kubernetes.driver.node.selector.workload=spark \
    --conf spark.kubernetes.executor.node.selector.workload=spark \
    --conf spark.kubernetes.container.image="$IMAGE" \
    --conf spark.kubernetes.container.image.pullPolicy="$IMAGE_PULL_POLICY" \
    --conf spark.kubernetes.submission.waitAppCompletion="$SPARK_WAIT_APP_COMPLETION" \
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
    --conf spark.kubernetes.driverEnv.MLFLOW_TRACKING_URI="$MLFLOW_TRACKING_URI" \
    --conf spark.kubernetes.driverEnv.MLFLOW_S3_ENDPOINT_URL="$MINIO_INTERNAL_ENDPOINT" \
    --conf spark.kubernetes.driverEnv.AWS_ACCESS_KEY_ID="$MINIO_ACCESS_KEY" \
    --conf spark.kubernetes.driverEnv.AWS_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY" \
    --conf spark.kubernetes.driverEnv.MINIO_ENDPOINT="$MINIO_INTERNAL_ENDPOINT" \
    --conf spark.kubernetes.driverEnv.MINIO_ACCESS_KEY="$MINIO_ACCESS_KEY" \
    --conf spark.kubernetes.driverEnv.MINIO_SECRET_KEY="$MINIO_SECRET_KEY" \
    --conf spark.kubernetes.driverEnv.SILVER_STARTED_PATH="$SILVER_STARTED_PATH" \
    --conf spark.kubernetes.driverEnv.GOLD_ROUTE_ESTIMATES_PATH="$GOLD_ROUTE_ESTIMATES_PATH" \
    --conf spark.kubernetes.driverEnv.GOLD_PREDICTIONS_PATH="$GOLD_PREDICTIONS_PATH" \
    --conf spark.kubernetes.driverEnv.CHECKPOINT_LOCATION="$CHECKPOINT_LOCATION" \
    --conf spark.kubernetes.driverEnv.TRIGGER_INTERVAL="$TRIGGER_INTERVAL" \
    --conf spark.kubernetes.driverEnv.MAX_FILES_PER_TRIGGER="$MAX_FILES_PER_TRIGGER" \
    --conf spark.kubernetes.driverEnv.N_LOCATION_CLUSTERS="$N_LOCATION_CLUSTERS" \
    --conf spark.kubernetes.driverEnv.N_TEMPORAL_CLUSTERS="$N_TEMPORAL_CLUSTERS" \
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
    --conf spark.driver.memoryOverhead="$SPARK_DRIVER_MEMORY_OVERHEAD" \
    --conf spark.executor.instances="$SPARK_EXECUTOR_INSTANCES" \
    --conf spark.executor.memory="$SPARK_EXECUTOR_MEMORY" \
    --conf spark.executor.memoryOverhead="$SPARK_EXECUTOR_MEMORY_OVERHEAD" \
    --conf spark.sql.shuffle.partitions=4 \
    \
    "${dynamic_allocation_conf[@]}" \
    "${starting_version_conf[@]}" \
    "$APP_FILE"

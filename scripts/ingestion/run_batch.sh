#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"

if [ -f "$ENV_FILE" ]; then
    source "$ENV_FILE"
fi

# Dynamically get K8S_MASTER from kubectl to match kubeconfig exactly
K8S_API_SERVER=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
K8S_MASTER="k8s://${K8S_API_SERVER}"
K8S_CA_CERT_FILE="${K8S_CA_CERT_FILE:-$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.certificate-authority}')}"
if [ -z "$K8S_CA_CERT_FILE" ]; then
    K8S_CA_CERT_FILE="/tmp/spark-k8s-ca.crt"
    kubectl config view --minify --raw -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' | base64 -d > "$K8S_CA_CERT_FILE"
fi
SPARK_VERSION="4.1.1"
SPARK_DIR="$HOME/Downloads/spark-${SPARK_VERSION}-bin-hadoop3"
SPARK_TGZ="${SPARK_DIR}.tgz"
SPARK_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz"

# Kubernetes
NAMESPACE="lakehouse"
SERVICE_ACCOUNT="spark-user"
K8S_SUBMISSION_TOKEN_FILE="/tmp/spark-k8s-submission.token"
kubectl create token "$SERVICE_ACCOUNT" -n "$NAMESPACE" > "$K8S_SUBMISSION_TOKEN_FILE"
chmod 600 "$K8S_SUBMISSION_TOKEN_FILE"

# Image
IMAGE="nyc-taxi-batch:v1.0"
APP_FILE="local:///opt/spark/work-dir/app/main.py"

# MinIO / Delta
# Spark driver/executors run inside Kubernetes, so use the in-cluster Service DNS.
MINIO_INTERNAL_ENDPOINT="${MINIO_INTERNAL_ENDPOINT:-http://minio-api.storage.svc.cluster.local:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"

# Historical input / Bronze output
STORAGE_PATH="${STORAGE_PATH:-/data/yellow_data}"
YEAR="${YEAR:-2024}"
OUTPUT_PATH="${OUTPUT_PATH:-s3a://lakehouse/bronze/nyc-taxi/trip_completed}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-2g}"
SPARK_EXECUTOR_INSTANCES="${SPARK_EXECUTOR_INSTANCES:-2}"
SPARK_EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-2}"
SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-4g}"
SPARK_DYNAMIC_ALLOCATION_ENABLED="${SPARK_DYNAMIC_ALLOCATION_ENABLED:-false}"
SPARK_DYNAMIC_ALLOCATION_SHUFFLE_TRACKING_ENABLED="${SPARK_DYNAMIC_ALLOCATION_SHUFFLE_TRACKING_ENABLED:-true}"
SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS="${SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS:-1}"
SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS="${SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS:-4}"
SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS="${SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS:-$SPARK_EXECUTOR_INSTANCES}"
BENCHMARK_METRICS_ENABLED="${BENCHMARK_METRICS_ENABLED:-true}"

# Kiểm tra và tải Spark client nếu máy local chưa có
if [ ! -d "$SPARK_DIR" ]; then
    echo "--- Không tìm thấy Spark ${SPARK_VERSION}. Đang tải... ---"
    mkdir -p "$HOME/Downloads"
    curl -L "$SPARK_URL" -o "$SPARK_TGZ"
    tar -xzf "$SPARK_TGZ" -C "$HOME/Downloads"
    rm "$SPARK_TGZ"
    echo "--- Cài Spark client xong ---"
else
    echo "--- Đã tìm thấy Spark tại $SPARK_DIR ---"
fi

echo "--- Đang submit batch job lên Kubernetes ---"
echo "    STORAGE_PATH=${STORAGE_PATH}"
echo "    YEAR=${YEAR}"
echo "    OUTPUT_PATH=${OUTPUT_PATH}"
echo "    Spark executors: ${SPARK_EXECUTOR_INSTANCES} x ${SPARK_EXECUTOR_CORES} cores, ${SPARK_EXECUTOR_MEMORY}"

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
    --name nyc-taxi-historical-to-bronze \
    --conf spark.kubernetes.namespace="$NAMESPACE" \
    --conf spark.kubernetes.driver.node.selector.workload=spark \
    --conf spark.kubernetes.executor.node.selector.workload=spark \
    --conf spark.kubernetes.container.image="${REGISTRY}/${IMAGE}" \
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
    --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.data-vol.mount.path=/data \
    --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.data-vol.options.claimName=nfs-nyc-taxi-pvc \
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.data-vol.mount.path=/data \
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.data-vol.options.claimName=nfs-nyc-taxi-pvc \
    \
    --conf spark.kubernetes.driverEnv.PYTHONPATH="/opt/spark/work-dir" \
    --conf spark.executorEnv.PYTHONPATH="/opt/spark/work-dir" \
    --conf spark.kubernetes.driverEnv.STORAGE_PATH="$STORAGE_PATH" \
    --conf spark.kubernetes.driverEnv.YEAR="$YEAR" \
    --conf spark.kubernetes.driverEnv.OUTPUT_PATH="$OUTPUT_PATH" \
    --conf spark.kubernetes.driverEnv.BENCHMARK_METRICS_ENABLED="$BENCHMARK_METRICS_ENABLED" \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    \
    --conf spark.hadoop.fs.s3a.endpoint="$MINIO_INTERNAL_ENDPOINT" \
    --conf spark.hadoop.fs.s3a.access.key="$MINIO_ACCESS_KEY" \
    --conf spark.hadoop.fs.s3a.secret.key="$MINIO_SECRET_KEY" \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.hadoop.fs.s3a.attempts.maximum=3 \
    \
    --conf spark.driver.memory="$SPARK_DRIVER_MEMORY" \
    --conf spark.executor.instances="$SPARK_EXECUTOR_INSTANCES" \
    --conf spark.executor.cores="$SPARK_EXECUTOR_CORES" \
    --conf spark.executor.memory="$SPARK_EXECUTOR_MEMORY" \
    --conf spark.kubernetes.driver.request.cores=1 \
    --conf spark.kubernetes.driver.limit.cores=2 \
    --conf spark.kubernetes.executor.request.cores="$SPARK_EXECUTOR_CORES" \
    --conf spark.kubernetes.executor.limit.cores="$SPARK_EXECUTOR_CORES" \
    \
    --conf spark.sql.shuffle.partitions=6 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    \
    "${dynamic_allocation_conf[@]}" \
    "$APP_FILE"

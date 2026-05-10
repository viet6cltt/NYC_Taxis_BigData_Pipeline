#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"

if [ -f "$ENV_FILE" ]; then
    source "$ENV_FILE"
fi

SPARK_VERSION="4.1.1"
SPARK_DIR="$HOME/Downloads/spark-${SPARK_VERSION}-bin-hadoop3"
SPARK_TGZ="${SPARK_DIR}.tgz"
SPARK_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz"

# Kubernetes
K8S_API_SERVER=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
K8S_MASTER="k8s://${K8S_API_SERVER}"
NAMESPACE="spark-operator"
SERVICE_ACCOUNT="spark-user"

# Runtime
TARGET="${1:-all}"                     # all|started|completed|expire
REQUESTED_PIPELINE_MODE="${2:-streaming}" # streaming|batch
REGISTRY="${REGISTRY:-localhost:5000}"
IMAGE="nyc-taxi-silver-consumer:v1.0"
APP_FILE="local:///opt/spark/work-dir/app/main.py"

# Paths
BRONZE_STARTED_PATH="s3a://lakehouse/bronze/nyc-taxi/trip_started"
BRONZE_COMPLETED_PATH="s3a://lakehouse/bronze/nyc-taxi/trip_completed"
SILVER_STARTED_PATH="s3a://lakehouse/silver/nyc-taxi/trip_started"
SILVER_COMPLETED_PATH="s3a://lakehouse/silver/nyc-taxi/trip_completed"
LIFECYCLE_PATH="s3a://lakehouse/silver/nyc-taxi/trip_lifecycle"
STARTED_CHECKPOINT_PATH="s3a://lakehouse/_checkpoints/silver/trip_started/processor"
COMPLETED_CHECKPOINT_PATH="s3a://lakehouse/_checkpoints/silver/trip_completed/processor"

TRIGGER_INTERVAL="${TRIGGER_INTERVAL:-30 seconds}"
WATERMARK_DELAY="${WATERMARK_DELAY:-48 hours}"
LIFECYCLE_TTL_HOURS="${LIFECYCLE_TTL_HOURS:-48}"
STARTING_VERSION="${STARTING_VERSION:-}"

# MinIO / Delta
MINIO_ENDPOINT="${MINIO_INTERNAL_ENDPOINT:-http://minio-api.minio.svc.cluster.local:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"

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

submit_silver_job() {
    local silver_job="$1"
    local pipeline_mode="$2"
    local input_path="$3"
    local output_path="$4"
    local checkpoint_path="$5"

    local starting_version_conf=()
    if [ "$pipeline_mode" = "streaming" ] && [ -n "$STARTING_VERSION" ]; then
        starting_version_conf=(--conf "spark.kubernetes.driverEnv.STARTING_VERSION=$STARTING_VERSION")
    fi

    echo "--- Submit silver job=${silver_job}, mode=${pipeline_mode} ---"

    "$SPARK_DIR/bin/spark-submit" \
    --master "$K8S_MASTER" \
    --deploy-mode cluster \
    --name "nyc-taxi-silver-${silver_job}" \
    --conf spark.kubernetes.namespace="$NAMESPACE" \
    --conf spark.kubernetes.container.image="${REGISTRY}/${IMAGE}" \
    --conf spark.kubernetes.container.image.pullPolicy=Always \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName="$SERVICE_ACCOUNT" \
    --conf spark.kubernetes.authenticate.caCertFile="" \
    --conf spark.kubernetes.authenticate.submission.caCertFile="" \
    --conf spark.kubernetes.authenticate.trustServerCertificate=true \
    \
    --conf spark.kubernetes.driverEnv.SILVER_JOB="$silver_job" \
    --conf spark.kubernetes.driverEnv.PIPELINE_MODE="$pipeline_mode" \
    --conf spark.kubernetes.driverEnv.INPUT_PATH="$input_path" \
    --conf spark.kubernetes.driverEnv.OUTPUT_PATH="$output_path" \
    --conf spark.kubernetes.driverEnv.CHECKPOINT_LOCATION="$checkpoint_path" \
    --conf spark.kubernetes.driverEnv.LIFECYCLE_PATH="$LIFECYCLE_PATH" \
    --conf spark.kubernetes.driverEnv.TRIGGER_INTERVAL="$TRIGGER_INTERVAL" \
    --conf spark.kubernetes.driverEnv.WATERMARK_DELAY="$WATERMARK_DELAY" \
    --conf spark.kubernetes.driverEnv.LIFECYCLE_TTL_HOURS="$LIFECYCLE_TTL_HOURS" \
    \
    --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.data-vol.mount.path=/data \
    --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.data-vol.options.claimName=nfs-nyc-taxi-pvc \
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.data-vol.mount.path=/data \
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.data-vol.options.claimName=nfs-nyc-taxi-pvc \
    \
    --conf spark.kubernetes.driverEnv.PYTHONPATH="/opt/spark/work-dir" \
    --conf spark.executorEnv.PYTHONPATH="/opt/spark/work-dir" \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    \
    --conf spark.hadoop.fs.s3a.endpoint="$MINIO_ENDPOINT" \
    --conf spark.hadoop.fs.s3a.access.key="$MINIO_ACCESS_KEY" \
    --conf spark.hadoop.fs.s3a.secret.key="$MINIO_SECRET_KEY" \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.hadoop.fs.s3a.attempts.maximum=3 \
    \
    --conf spark.driver.memory=1g \
    --conf spark.executor.instances=2 \
    --conf spark.executor.memory=2048m \
    --conf spark.kubernetes.driver.request.cores=0.5 \
    --conf spark.kubernetes.driver.limit.cores=1.5 \
    --conf spark.kubernetes.executor.request.cores=1 \
    --conf spark.kubernetes.executor.limit.cores=2.5 \
    \
    --conf spark.sql.shuffle.partitions=4 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    \
    "${starting_version_conf[@]}" \
    "$APP_FILE"
}

submit_expire_job() {
    submit_silver_job "expire" "batch" "" "" ""
}

submit_started_job() {
    submit_silver_job \
        "started" \
        "$REQUESTED_PIPELINE_MODE" \
        "$BRONZE_STARTED_PATH" \
        "$SILVER_STARTED_PATH" \
        "$STARTED_CHECKPOINT_PATH"
}

submit_completed_job() {
    submit_silver_job \
        "completed" \
        "$REQUESTED_PIPELINE_MODE" \
        "$BRONZE_COMPLETED_PATH" \
        "$SILVER_COMPLETED_PATH" \
        "$COMPLETED_CHECKPOINT_PATH"
}

case "$TARGET" in
    all)
        submit_expire_job
        submit_started_job &
        started_pid=$!
        submit_completed_job &
        completed_pid=$!
        wait "$started_pid"
        wait "$completed_pid"
        ;;
    started)
        submit_started_job
        ;;
    completed)
        submit_completed_job
        ;;
    expire)
        submit_expire_job
        ;;
    *)
        echo "Usage: $0 [all|started|completed|expire] [streaming|batch]"
        exit 1
        ;;
esac

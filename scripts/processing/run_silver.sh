#!/bin/bash
set -euo pipefail

SPARK_VERSION="4.1.1"
SPARK_DIR="$HOME/Downloads/spark-${SPARK_VERSION}-bin-hadoop3"
SPARK_TGZ="${SPARK_DIR}.tgz"
SPARK_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz"

# Kubernetes
K8S_MASTER="k8s://192.168.58.2:8443"
NAMESPACE="spark-operator"
SERVICE_ACCOUNT="spark-user"

# Path
BRONZE_PATH="s3a://lakehouse/bronze/nyc-taxi"
SILVER_PATH="s3a://lakehouse/silver/nyc-taxi/trips"
SILVER_CHECKPOINT_PATH="s3a://lakehouse/silver/nyc-taxi/_checkpoint"
TRIGGER_INTERVAL="30 seconds"
WATERMARK_DELAY="1 minute"

PIPELINE_MODE="batch"
STARTING_VERSION="1"

# Image
IMAGE="nyc-taxi-silver-consumer:v1"
APP_FILE="local:///opt/spark/work-dir/app/main.py"

# MinIO / Delta
MINIO_ENDPOINT="http://minio-api.minio.svc.cluster.local:9000"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"

TARGET=${1:-all} 

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

echo "--- Đang submit bronze_to_silver job lên Kubernetes ---"

run_batch() {
    "$SPARK_DIR/bin/spark-submit" \
    --master "$K8S_MASTER" \
    --deploy-mode cluster \
    --name nyc-taxi-bronze-to-silver \
    --conf spark.kubernetes.namespace="$NAMESPACE" \
    --conf spark.kubernetes.container.image="$IMAGE" \
    --conf spark.kubernetes.container.image.pullPolicy=Never \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName="$SERVICE_ACCOUNT" \
    \
    --conf spark.kubernetes.driverEnv.BRONZE_PATH="$BRONZE_PATH" \
    --conf spark.kubernetes.driverEnv.SILVER_PATH="$SILVER_PATH" \
    --conf spark.kubernetes.driverEnv.SILVER_CHECKPOINT_PATH="$SILVER_CHECKPOINT_PATH" \
    --conf spark.kubernetes.driverEnv.TRIGGER_INTERVAL="$TRIGGER_INTERVAL" \
    --conf spark.kubernetes.driverEnv.WATERMARK_DELAY="$WATERMARK_DELAY" \
    --conf spark.kubernetes.driverEnv.PIPELINE_MODE="$PIPELINE_MODE" \
    \
    --conf spark.kubernetes.driver.volumes.hostPath.data-vol.mount.path=/data \
    --conf spark.kubernetes.driver.volumes.hostPath.data-vol.options.path=/mnt/nyc-data \
    --conf spark.kubernetes.executor.volumes.hostPath.data-vol.mount.path=/data \
    --conf spark.kubernetes.executor.volumes.hostPath.data-vol.options.path=/mnt/nyc-data \
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
    --conf spark.executor.memory=2g \
    --conf spark.kubernetes.driver.request.cores=0.5 \
    --conf spark.kubernetes.driver.limit.cores=1 \
    --conf spark.kubernetes.executor.request.cores=0.75 \
    --conf spark.kubernetes.executor.limit.cores=1.2 \
    \
    --conf spark.sql.shuffle.partitions=4 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    \
    "$APP_FILE"
}

run_streaming() {
    PIPELINE_MODE="streaming"
    STARTING_VERSION_CONF=()
    if [ -n "$STARTING_VERSION" ]; then
        STARTING_VERSION_CONF=(--conf "spark.kubernetes.driverEnv.STARTING_VERSION=$STARTING_VERSION")
    fi

    "$SPARK_DIR/bin/spark-submit" \
    --master "$K8S_MASTER" \
    --deploy-mode cluster \
    --name nyc-taxi-bronze-to-silver \
    --conf spark.kubernetes.namespace="$NAMESPACE" \
    --conf spark.kubernetes.container.image="$IMAGE" \
    --conf spark.kubernetes.container.image.pullPolicy=Never \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName="$SERVICE_ACCOUNT" \
    \
    --conf spark.kubernetes.driverEnv.BRONZE_PATH="$BRONZE_PATH" \
    --conf spark.kubernetes.driverEnv.SILVER_PATH="$SILVER_PATH" \
    --conf spark.kubernetes.driverEnv.SILVER_CHECKPOINT_PATH="$SILVER_CHECKPOINT_PATH" \
    --conf spark.kubernetes.driverEnv.TRIGGER_INTERVAL="$TRIGGER_INTERVAL" \
    --conf spark.kubernetes.driverEnv.WATERMARK_DELAY="$WATERMARK_DELAY" \
    --conf spark.kubernetes.driverEnv.PIPELINE_MODE="$PIPELINE_MODE" \
    \
    --conf spark.kubernetes.driver.volumes.hostPath.data-vol.mount.path=/data \
    --conf spark.kubernetes.driver.volumes.hostPath.data-vol.options.path=/mnt/nyc-data \
    --conf spark.kubernetes.executor.volumes.hostPath.data-vol.mount.path=/data \
    --conf spark.kubernetes.executor.volumes.hostPath.data-vol.options.path=/mnt/nyc-data \
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
    --conf spark.executor.memory=2g \
    --conf spark.kubernetes.driver.request.cores=0.5 \
    --conf spark.kubernetes.driver.limit.cores=1 \
    --conf spark.kubernetes.executor.request.cores=0.75 \
    --conf spark.kubernetes.executor.limit.cores=1.2 \
    \
    --conf spark.sql.shuffle.partitions=4 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    \
    "${STARTING_VERSION_CONF[@]}" \
    \
    "$APP_FILE"
}

if [ "$TARGET" == "batch" ]; then
    run_batch
elif [ "$TARGET" == "streaming" ]; then
    run_streaming
else
    run_batch
    run_streaming
fi

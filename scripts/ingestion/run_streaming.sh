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
# Dynamically get K8S_MASTER from kubectl to match kubeconfig exactly
K8S_API_SERVER=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
K8S_MASTER="k8s://${K8S_API_SERVER}"
NAMESPACE="spark-operator"
SERVICE_ACCOUNT="spark-user"

# Kafka / Bronze
TARGET_EVENT_KIND="${1:-all}"
KAFKA_STARTED_TOPIC="nyc-taxi-trip-started"
KAFKA_COMPLETED_TOPIC="nyc-taxi-trip-completed"
KAFKA_BOOTSTRAP_SERVERS="my-kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
STARTED_OUTPUT_PATH="s3a://lakehouse/bronze/nyc-taxi/trip_started"
COMPLETED_OUTPUT_PATH="s3a://lakehouse/bronze/nyc-taxi/trip_completed"
STARTED_CHECKPOINT_LOCATION="s3a://lakehouse/_checkpoints/bronze/trip_started/kafka_to_bronze"
COMPLETED_CHECKPOINT_LOCATION="s3a://lakehouse/_checkpoints/bronze/trip_completed/kafka_to_bronze"
TRIGGER_INTERVAL="30 seconds"

# Image
REGISTRY="${REGISTRY:-localhost:5000}"
IMAGE="nyc-taxi-streaming-consumer:v1.0"
APP_FILE="local:///opt/spark/work-dir/app/main.py"

# MinIO / Delta
MINIO_ENDPOINT="http://minio-api.minio.svc.cluster.local:9000"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"

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

submit_streaming_job() {
    local event_kind="$1"
    local kafka_topic="$2"
    local output_path="$3"
    local checkpoint_location="$4"

    echo "--- Đang submit ${event_kind} streaming-to-bronze job lên Kubernetes ---"

    "$SPARK_DIR/bin/spark-submit" \
    --master "$K8S_MASTER" \
    --deploy-mode cluster \
    --name "nyc-taxi-${event_kind}-to-bronze" \
    --conf spark.kubernetes.namespace="$NAMESPACE" \
    --conf spark.kubernetes.container.image="${REGISTRY}/${IMAGE}" \
    --conf spark.kubernetes.container.image.pullPolicy=Always \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName="$SERVICE_ACCOUNT" \
    --conf spark.kubernetes.authenticate.caCertFile="" \
    --conf spark.kubernetes.authenticate.submission.caCertFile="" \
    --conf spark.kubernetes.authenticate.trustServerCertificate=true \
    \
    --conf spark.kubernetes.driverEnv.EVENT_KIND="$event_kind" \
    --conf spark.kubernetes.driverEnv.KAFKA_TOPIC="$kafka_topic" \
    --conf spark.kubernetes.driverEnv.OUTPUT_PATH="$output_path" \
    --conf spark.kubernetes.driverEnv.KAFKA_BOOTSTRAP_SERVERS="$KAFKA_BOOTSTRAP_SERVERS" \
    --conf spark.kubernetes.driverEnv.CHECKPOINT_LOCATION="$checkpoint_location" \
    --conf spark.kubernetes.driverEnv.AVRO_SCHEMA_PATH="schemas/taxi_trip_event.avsc" \
    --conf spark.kubernetes.driverEnv.TRIGGER_INTERVAL="$TRIGGER_INTERVAL" \
    \
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
    --conf spark.executor.memory=2g \
    --conf spark.kubernetes.driver.request.cores=0.5 \
    --conf spark.kubernetes.driver.limit.cores=1 \
    --conf spark.kubernetes.executor.request.cores=0.5 \
    --conf spark.kubernetes.executor.limit.cores=1 \
    \
    --conf spark.sql.shuffle.partitions=4 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    \
    "$APP_FILE"
}

case "$TARGET_EVENT_KIND" in
    all)
        submit_streaming_job "started" "$KAFKA_STARTED_TOPIC" "$STARTED_OUTPUT_PATH" "$STARTED_CHECKPOINT_LOCATION" &
        started_submit_pid=$!

        submit_streaming_job "completed" "$KAFKA_COMPLETED_TOPIC" "$COMPLETED_OUTPUT_PATH" "$COMPLETED_CHECKPOINT_LOCATION" &
        completed_submit_pid=$!

        wait "$started_submit_pid"
        wait "$completed_submit_pid"
        ;;
    started)
        submit_streaming_job "started" "$KAFKA_STARTED_TOPIC" "$STARTED_OUTPUT_PATH" "$STARTED_CHECKPOINT_LOCATION"
        ;;
    completed)
        submit_streaming_job "completed" "$KAFKA_COMPLETED_TOPIC" "$COMPLETED_OUTPUT_PATH" "$COMPLETED_CHECKPOINT_LOCATION"
        ;;
    *)
        echo "Usage: $0 [all|started|completed]"
        exit 1
        ;;
esac

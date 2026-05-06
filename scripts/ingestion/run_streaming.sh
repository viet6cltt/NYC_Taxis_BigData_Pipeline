#!/bin/bash
set -euo pipefail

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

# Kakfa
KAFKA_TOPIC="nyc-taxi-events"
OUTPUT_PATH="s3a://lakehouse/bronze/nyc-taxi"
KAFKA_BOOTSTRAP_SERVERS="my-kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
CHECKPOINT_LOCATION="s3a://lakehouse/bronze/_checkpoints/nyc_taxi_stream"

# Image
IMAGE="nyc-taxi-streaming-consumer:v1"
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

echo "--- Đang submit streaming lên Kubernetes ---"

"$SPARK_DIR/bin/spark-submit" \
    --master "$K8S_MASTER" \
    --deploy-mode cluster \
    --name nyc-taxi-streaming-to-bronze \
    --conf spark.kubernetes.namespace="$NAMESPACE" \
    --conf spark.kubernetes.container.image="$IMAGE" \
    --conf spark.kubernetes.container.image.pullPolicy=Never \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName="$SERVICE_ACCOUNT" \
    --conf spark.kubernetes.authenticate.caCertFile="" \
    --conf spark.kubernetes.authenticate.submission.caCertFile="" \
    --conf spark.kubernetes.authenticate.trustServerCertificate=true \
    \
    --conf spark.kubernetes.driverEnv.KAFKA_TOPIC="$KAFKA_TOPIC" \
    --conf spark.kubernetes.driverEnv.OUTPUT_PATH="$OUTPUT_PATH" \
    --conf spark.kubernetes.driverEnv.KAFKA_BOOTSTRAP_SERVERS="$KAFKA_BOOTSTRAP_SERVERS" \
    --conf spark.kubernetes.driverEnv.CHECKPOINT_LOCATION="$CHECKPOINT_LOCATION" \
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
    --conf spark.executor.instances=1 \
    --conf spark.executor.memory=2g \
    --conf spark.kubernetes.driver.request.cores=0.2 \
    --conf spark.kubernetes.driver.limit.cores=0.3 \
    --conf spark.kubernetes.executor.request.cores=0.5 \
    --conf spark.kubernetes.executor.limit.cores=1 \
    \
    --conf spark.sql.shuffle.partitions=2 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    \
    "$APP_FILE"
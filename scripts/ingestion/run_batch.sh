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

"$SPARK_DIR/bin/spark-submit" \
    --master "$K8S_MASTER" \
    --deploy-mode cluster \
    --name nyc-taxi-historical-to-bronze \
    --conf spark.kubernetes.namespace="$NAMESPACE" \
    --conf spark.kubernetes.container.image="${REGISTRY}/${IMAGE}" \
    --conf spark.kubernetes.container.image.pullPolicy=Always \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName="$SERVICE_ACCOUNT" \
    --conf spark.kubernetes.authenticate.caCertFile="$K8S_CA_CERT_FILE" \
    --conf spark.kubernetes.authenticate.submission.caCertFile="$K8S_CA_CERT_FILE" \
    --conf spark.kubernetes.authenticate.submission.oauthTokenFile="$K8S_SUBMISSION_TOKEN_FILE" \
    --conf spark.kubernetes.authenticate.trustServerCertificate=true \
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
    --conf spark.driver.memory=1g \
    --conf spark.executor.instances=1 \
    --conf spark.executor.memory=3g \
    --conf spark.kubernetes.driver.request.cores=1 \
    --conf spark.kubernetes.driver.limit.cores=2 \
    --conf spark.kubernetes.executor.request.cores=1 \
    --conf spark.kubernetes.executor.limit.cores=2 \
    --conf spark.kubernetes.executor.node.selector.role=storage \
    --conf spark.kubernetes.driver.node.selector.role=storage \
    \
    --conf spark.sql.shuffle.partitions=2 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    \
    "$APP_FILE"

#!/bin/bash
# Run Feature Engineering: Silver Delta → Gold Delta
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
NAMESPACE="spark-operator"
SERVICE_ACCOUNT="spark-user"


SILVER_PATH="${SILVER_PATH:-s3a://lakehouse/silver/nyc-taxi/trips}"
GOLD_FEATURES_PATH="${GOLD_FEATURES_PATH:-s3a://lakehouse/gold/nyc-taxi/features}"
WRITE_MODE="${WRITE_MODE:-overwrite}"

MINIO_INTERNAL_ENDPOINT="${MINIO_INTERNAL_ENDPOINT:-http://minio-api.minio.svc.cluster.local:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"

# Image
IMAGE="${REGISTRY:-localhost:5000}/nyc-taxi-feature-engineering:v1.0"
APP_FILE="local:///opt/spark/work-dir/app/main.py"

if [ ! -d "$SPARK_DIR" ]; then
    echo "--- Downloading Spark ${SPARK_VERSION} ---"
    mkdir -p "$HOME/Downloads"
    curl -L "${SPARK_URL}" -o "${SPARK_DIR}.tgz"
    tar -xzf "${SPARK_DIR}.tgz" -C "$HOME/Downloads"
    rm "${SPARK_DIR}.tgz"
fi

echo "--- Submitting Feature Engineering job to Kubernetes ---"

"$SPARK_DIR/bin/spark-submit" \
    --master "$K8S_MASTER" \
    --deploy-mode cluster \
    --name nyc-taxi-feature-engineering \
    --conf spark.kubernetes.namespace="$NAMESPACE" \
    --conf spark.kubernetes.container.image="$IMAGE" \
    --conf spark.kubernetes.container.image.pullPolicy=Always \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName="$SERVICE_ACCOUNT" \
    --conf spark.kubernetes.authenticate.caCertFile="" \
    --conf spark.kubernetes.authenticate.submission.caCertFile="" \
    --conf spark.kubernetes.authenticate.trustServerCertificate=true \
    \
    --conf spark.kubernetes.driverEnv.PYTHONPATH="/opt/spark/work-dir" \
    --conf spark.executorEnv.PYTHONPATH="/opt/spark/work-dir" \
    --conf spark.kubernetes.driverEnv.SILVER_PATH="$SILVER_PATH" \
    --conf spark.kubernetes.driverEnv.GOLD_FEATURES_PATH="$GOLD_FEATURES_PATH" \
    --conf spark.kubernetes.driverEnv.WRITE_MODE="$WRITE_MODE" \
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
    --conf spark.driver.memory=2g \
    --conf spark.executor.instances=4 \
    --conf spark.executor.memory=4g \
    --conf spark.sql.shuffle.partitions=6 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.kubenetes.driver.node.selector.node-role.kubernetes.io/control-plane=true \
    \
    "$APP_FILE"

#!/bin/bash
# Run XGBoost Training: Gold Delta → MLflow Model Registry
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"
[ -f "$ENV_FILE" ] && source "$ENV_FILE"

K8S_API_SERVER=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
K8S_MASTER="k8s://${K8S_API_SERVER}"

SPARK_VERSION="4.1.1"
SPARK_DIR="$HOME/Downloads/spark-${SPARK_VERSION}-bin-hadoop3"

NAMESPACE="spark-operator"
SERVICE_ACCOUNT="spark-user"
IMAGE="${REGISTRY:-localhost:5000}/nyc-taxi-train-xgboost:v1.0"
APP_FILE="local:///opt/spark/work-dir/app/main.py"

MINIO_INTERNAL_ENDPOINT="${MINIO_INTERNAL_ENDPOINT:-http://minio-api.minio.svc.cluster.local:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
MLFLOW_TRACKING_URI="${MLFLOW_TRACKING_URI:-http://mlflow.mlflow.svc.cluster.local:5000}"

echo "--- Submitting XGBoost Training job to Kubernetes ---"
echo "    MLflow Tracking URI: $MLFLOW_TRACKING_URI"

"$SPARK_DIR/bin/spark-submit" \
    --master "$K8S_MASTER" \
    --deploy-mode cluster \
    --name nyc-taxi-train-xgboost \
    --conf spark.kubernetes.namespace="$NAMESPACE" \
    --conf spark.kubernetes.container.image="$IMAGE" \
    --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName="$SERVICE_ACCOUNT" \
    --conf spark.kubernetes.authenticate.trustServerCertificate=true \
    \
    --conf spark.kubernetes.driverEnv.PYTHONPATH="/opt/spark/work-dir" \
    --conf spark.executorEnv.PYTHONPATH="/opt/spark/work-dir" \
    --conf spark.kubernetes.driverEnv.MLFLOW_TRACKING_URI="$MLFLOW_TRACKING_URI" \
    --conf spark.kubernetes.driverEnv.MINIO_ENDPOINT="$MINIO_INTERNAL_ENDPOINT" \
    --conf spark.kubernetes.driverEnv.MINIO_ACCESS_KEY="$MINIO_ACCESS_KEY" \
    --conf spark.kubernetes.driverEnv.MINIO_SECRET_KEY="$MINIO_SECRET_KEY" \
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
    --conf spark.driver.memory=4g \
    --conf spark.executor.instances=1 \
    --conf spark.executor.memory=4g \
    \
    "$APP_FILE"

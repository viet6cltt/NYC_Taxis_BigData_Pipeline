#!/bin/bash
# Run XGBoost Training: Gold Delta → MLflow Model Registry
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"
[ -f "$ENV_FILE" ] && source "$ENV_FILE"

K8S_API_SERVER=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
K8S_MASTER="k8s://${K8S_API_SERVER}"

SPARK_VERSION="${SPARK_VERSION:-4.1.1}"
SPARK_DIR="${SPARK_DIR:-$HOME/Downloads/spark-${SPARK_VERSION}-bin-hadoop3}"

NAMESPACE="lakehouse"
SERVICE_ACCOUNT="spark-user"
IMAGE="${REGISTRY:-localhost:5000}/nyc-taxi-train-xgboost:v1.0"
APP_FILE="local:///opt/spark/work-dir/app/main.py"
GOLD_FEATURES_PATH="${GOLD_FEATURES_PATH:-s3a://lakehouse/gold/ml/features}"

MINIO_INTERNAL_ENDPOINT="${MINIO_INTERNAL_ENDPOINT:-http://minio-api.storage.svc.cluster.local:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
MLFLOW_TRACKING_URI="${MLFLOW_TRACKING_URI:-http://mlflow.mlops.svc.cluster.local:5000}"
XGB_NUM_WORKERS="${XGB_NUM_WORKERS:-2}"
SPLIT_STRATEGY="${SPLIT_STRATEGY:-random}"
TIME_SPLIT_MONTH="${TIME_SPLIT_MONTH:-2024-11}"

IMAGE_PULL_POLICY="${IMAGE_PULL_POLICY:-Always}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-1g}"
SPARK_EXECUTOR_INSTANCES="${SPARK_EXECUTOR_INSTANCES:-1}"
SPARK_EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-3}"
SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-4g}"
SPARK_EXECUTOR_DELETE_ON_TERMINATION="${SPARK_EXECUTOR_DELETE_ON_TERMINATION:-false}"

echo "--- Submitting XGBoost Training job to Kubernetes ---"
echo "    MLflow Tracking URI: $MLFLOW_TRACKING_URI"
echo "    XGBoost Spark workers: $XGB_NUM_WORKERS"
echo "    Split strategy: $SPLIT_STRATEGY"
echo "    Spark executors: ${SPARK_EXECUTOR_INSTANCES} x ${SPARK_EXECUTOR_CORES} cores, ${SPARK_EXECUTOR_MEMORY}"

"$SPARK_DIR/bin/spark-submit" \
    --master "$K8S_MASTER" \
    --deploy-mode cluster \
    --name nyc-taxi-train-xgboost \
    --conf spark.kubernetes.namespace="$NAMESPACE" \
    --conf spark.kubernetes.container.image="$IMAGE" \
    --conf spark.kubernetes.container.image.pullPolicy="$IMAGE_PULL_POLICY" \
    --conf spark.kubernetes.executor.deleteOnTermination="$SPARK_EXECUTOR_DELETE_ON_TERMINATION" \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName="$SERVICE_ACCOUNT" \
    --conf spark.kubernetes.authenticate.caCertFile="" \
    --conf spark.kubernetes.authenticate.submission.caCertFile="" \
    --conf spark.kubernetes.authenticate.trustServerCertificate=true \
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
    --conf spark.kubernetes.driverEnv.GOLD_FEATURES_PATH="$GOLD_FEATURES_PATH" \
    --conf spark.kubernetes.driverEnv.XGB_NUM_WORKERS="$XGB_NUM_WORKERS" \
    --conf spark.kubernetes.driverEnv.SPLIT_STRATEGY="$SPLIT_STRATEGY" \
    --conf spark.kubernetes.driverEnv.TIME_SPLIT_MONTH="$TIME_SPLIT_MONTH" \
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
    --conf spark.memory.fraction=0.8 \
    \
    "$APP_FILE"

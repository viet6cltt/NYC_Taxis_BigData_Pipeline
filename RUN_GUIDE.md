# Run Guide - NYC Taxi Streaming Lakehouse

Guide này dùng cho nhánh `dev` sau khi gộp streaming mới, BentoML và BI/Superset.

Flow chạy chi tiết nằm ở [RUN_FLOW.md](RUN_FLOW.md). File này chỉ giữ thứ tự triển khai ngắn gọn để tránh dùng nhầm pipeline local cũ.

## 1. Chuẩn bị hạ tầng

```bash
kubectl apply -f infra/k8s/minio/minio.yaml
kubectl apply -f infra/k8s/common/spark-rabc.yaml

kubectl create namespace kafka --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
kubectl apply -f infra/k8s/ingestion/kafka/kafka-cluster.yaml
kubectl apply -f infra/k8s/ingestion/kafka/kafka-topics.yaml

kubectl apply -f infra/k8s/mlflow/mlflow.yaml
```

Tạo bucket chính:

```bash
kubectl run minio-init --image=minio/mc:latest --restart=Never -n minio --rm -it -- sh -c "
  mc alias set local http://minio-api:9000 minioadmin minioadmin;
  mc mb --ignore-existing local/lakehouse;
  mc mb --ignore-existing local/mlflow;
  mc mb --ignore-existing local/warehouse;
"
```

## 2. Build images

```bash
bash scripts/images/build.sh all
bash scripts/images/build_training.sh
bash scripts/images/build_serving.sh
```

## 3. Historical path cho training

```bash
bash scripts/ingestion/run_batch.sh
bash scripts/processing/run_silver.sh completed batch
bash scripts/training/run_feature_engineering.sh route_estimates
bash scripts/training/run_feature_engineering.sh features
bash scripts/training/run_training.sh
```

Output chính:

```text
s3a://lakehouse/bronze/nyc-taxi/trip_completed
s3a://lakehouse/silver/nyc-taxi/trip_completed
s3a://lakehouse/silver/nyc-taxi/trip_lifecycle
s3a://lakehouse/gold/ml/route_estimates
s3a://lakehouse/gold/ml/features
MLflow model: XGB_NYC_Fare / Production
```

## 4. Realtime path

```bash
bash scripts/ingestion/run_streaming.sh all
bash scripts/processing/run_silver.sh started streaming
bash scripts/processing/run_silver.sh completed streaming
bash scripts/serving/run_stream_predict.sh
```

Output realtime:

```text
s3a://lakehouse/bronze/nyc-taxi/trip_started
s3a://lakehouse/bronze/nyc-taxi/trip_completed
s3a://lakehouse/silver/nyc-taxi/trip_started
s3a://lakehouse/silver/nyc-taxi/trip_completed
s3a://lakehouse/gold/ml/predictions
```

## 5. Monitoring ML

```bash
bash scripts/training/run_feature_engineering.sh prediction_actuals
bash scripts/training/run_feature_engineering.sh model_quality_daily
```

Output:

```text
s3a://lakehouse/gold/ml/prediction_actuals
s3a://lakehouse/gold/monitoring/model_quality_daily
```

## 6. Serving API

FastAPI:

```bash
kubectl apply -f infra/k8s/serving/fastapi_deployment.yaml
```

BentoML:

```bash
kubectl apply -f infra/k8s/serving/bentoml_deployment.yaml
```

Local BentoML:

```bash
bash scripts/serving/run_bentoml_local.sh
```

API input dùng `estimated_trip_distance` và `estimated_trip_duration_seconds`, không dùng actual distance/duration vì prediction xảy ra tại thời điểm `trip_started`.

## 7. BI với Superset

Chạy sau khi đã có Silver/Gold Delta tables:

```bash
bash scripts/bi/setup_bi.sh

kubectl port-forward -n lakehouse svc/superset 8088:8088 &
kubectl port-forward -n lakehouse svc/trino 8080:8080 &

python3 scripts/bi/create_dashboard.py
```

Superset SQL Lab dùng database:

```text
Trino - NYC Taxi Lakehouse
```

Các schema chính:

```text
delta.silver_nyc_taxi
delta.gold_ml
delta.gold_monitoring
```

## Notes

- Flow local cũ đã được bỏ để tránh nhầm với streaming lakehouse mới.
- Historical backfill nên dùng `bash scripts/processing/run_silver.sh completed batch`, không dùng default streaming cho dữ liệu vài GB.
- Nếu rebuild sạch, xóa cả data path và checkpoint path tương ứng trong bucket `lakehouse`.

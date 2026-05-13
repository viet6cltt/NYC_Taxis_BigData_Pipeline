# Run Flow End-to-End

File này ghi thứ tự chạy chuẩn từ dữ liệu raw đến training, realtime prediction và monitoring.

## 0. Build Images

```bash
bash scripts/images/build.sh all
bash scripts/images/build_training.sh
bash scripts/images/build_serving.sh
```

Nếu chỉ sửa một phần:

```bash
bash scripts/images/build.sh batch
bash scripts/images/build.sh silver_consumer
bash scripts/images/build_training.sh
bash scripts/images/build_serving.sh
```

## 1. Historical 2024 -> Bronze Completed

```bash
bash scripts/ingestion/run_batch.sh
```

Log đúng phải có:

```text
[batch] Reading historical parquet files from /data/yellow_data/2024
[batch] Writing bronze data to s3a://lakehouse/bronze/nyc-taxi/trip_completed
```

Output:

```text
s3a://lakehouse/bronze/nyc-taxi/trip_completed
```

## 2. Bronze Completed -> Silver Completed + Lifecycle

Chạy batch cho backfill historical:

```bash
bash scripts/processing/run_silver.sh completed batch
```

Output:

```text
s3a://lakehouse/silver/nyc-taxi/trip_completed
s3a://lakehouse/silver/nyc-taxi/trip_lifecycle
```

Không dùng lệnh thiếu `batch` cho backfill lớn, vì default là streaming:

```bash
# Tránh dùng cho historical backfill lớn
bash scripts/processing/run_silver.sh completed
```

## 3. Gold Route Estimates

Chạy sau khi `silver/trip_completed` đã có dữ liệu:

```bash
bash scripts/training/run_feature_engineering.sh route_estimates
```

Output:

```text
s3a://lakehouse/gold/ml/route_estimates
```

## 4. Gold Training Features

Chạy sau khi `route_estimates` đã build xong:

```bash
bash scripts/training/run_feature_engineering.sh features
```

Output:

```text
s3a://lakehouse/gold/ml/features
```

## 5. Train Model

```bash
bash scripts/training/run_training.sh
```

Kết quả cần có trong MLflow:

```text
model name: XGB_NYC_Fare
stage: Production
```

## 6. Realtime Ingestion

Khi cần demo realtime, bật producer/replay trước nếu chưa có data vào Kafka, rồi chạy Bronze streaming consumers:

```bash
bash scripts/ingestion/run_streaming.sh all
```

Output realtime:

```text
s3a://lakehouse/bronze/nyc-taxi/trip_started
s3a://lakehouse/bronze/nyc-taxi/trip_completed
```

## 7. Realtime Silver

Chạy 2 job riêng:

```bash
bash scripts/processing/run_silver.sh started streaming
bash scripts/processing/run_silver.sh completed streaming
```

Output realtime:

```text
s3a://lakehouse/silver/nyc-taxi/trip_started
s3a://lakehouse/silver/nyc-taxi/trip_completed
s3a://lakehouse/silver/nyc-taxi/trip_lifecycle
```

## 8. Realtime Prediction

Chạy sau khi đã có:

```text
gold/ml/route_estimates
MLflow Production model
silver/trip_started realtime stream
```

Lệnh:

```bash
bash scripts/serving/run_stream_predict.sh
```

Flow:

```text
silver/trip_started
  + gold/ml/route_estimates
  + MLflow Production model
  -> gold/ml/predictions
```

Output:

```text
s3a://lakehouse/gold/ml/predictions
```

## 9. Prediction Actuals

Chạy sau khi có cả predictions và completed actuals:

```bash
bash scripts/training/run_feature_engineering.sh prediction_actuals
```

Output:

```text
s3a://lakehouse/gold/ml/prediction_actuals
```

## 10. Model Quality Daily

```bash
bash scripts/training/run_feature_engineering.sh model_quality_daily
```

Output:

```text
s3a://lakehouse/gold/monitoring/model_quality_daily
```

## 11. FastAPI

```bash
kubectl apply -f infra/k8s/serving/fastapi_deployment.yaml
```

FastAPI load model từ MLflow. Request `/predict` dùng estimated distance/duration:

```json
{
  "passenger_count": 2,
  "estimated_trip_distance": 3.5,
  "estimated_trip_duration_seconds": 900,
  "pickup_hour": 14,
  "pickup_day_of_week": 2,
  "pulocation_id": 161,
  "dolocation_id": 236
}
```

## 12. BentoML

BentoML dùng cùng MLflow model và cùng feature contract `estimated_*` như FastAPI.

```bash
kubectl apply -f infra/k8s/serving/bentoml_deployment.yaml
```

Chạy local nếu cần test nhanh:

```bash
bash scripts/serving/run_bentoml_local.sh
```

## 13. BI with Trino + Superset

Chạy sau khi Silver/Gold Delta tables đã tồn tại:

```bash
bash scripts/bi/setup_bi.sh

kubectl port-forward -n lakehouse svc/superset 8088:8088 &
kubectl port-forward -n lakehouse svc/trino 8080:8080 &

python3 scripts/bi/create_dashboard.py
```

Các schema chính trong Superset SQL Lab:

```text
delta.silver_nyc_taxi
delta.gold_ml
delta.gold_monitoring
```

## Recommended Full Order

```bash
# Build
bash scripts/images/build.sh all
bash scripts/images/build_training.sh
bash scripts/images/build_serving.sh

# Historical training path
bash scripts/ingestion/run_batch.sh
bash scripts/processing/run_silver.sh completed batch
bash scripts/training/run_feature_engineering.sh route_estimates
bash scripts/training/run_feature_engineering.sh features
bash scripts/training/run_training.sh

# Realtime path
bash scripts/ingestion/run_streaming.sh all
bash scripts/processing/run_silver.sh started streaming
bash scripts/processing/run_silver.sh completed streaming
bash scripts/serving/run_stream_predict.sh

# Monitoring path
bash scripts/training/run_feature_engineering.sh prediction_actuals
bash scripts/training/run_feature_engineering.sh model_quality_daily

# API
kubectl apply -f infra/k8s/serving/fastapi_deployment.yaml
kubectl apply -f infra/k8s/serving/bentoml_deployment.yaml

# BI
bash scripts/bi/setup_bi.sh
```

## Operational Notes

- Trong lúc historical backfill lớn, nên tắt bớt streaming jobs để giảm tải MinIO/S3A.
- `run_silver.sh completed batch` dùng cho historical backfill.
- `run_silver.sh completed streaming` dùng cho dữ liệu mới sau backfill.
- Nếu rebuild sạch, xóa cả data path và checkpoint path tương ứng trong MinIO.
- Nếu thấy S3A upload timeout khi ghi Bronze/Silver, tăng timeout trong script hoặc chạy ít Spark jobs hơn cùng lúc.

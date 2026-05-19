# Full Demo Run Flow

File này là kịch bản chạy full pipeline để demo với thầy:

```text
Raw NYC Taxi data
  -> Kafka / Spark ingestion
  -> Bronze / Silver / Gold Delta Lake on MinIO
  -> MLflow training + registry
  -> Airflow MLOps retrain
  -> FastAPI / streaming prediction
  -> BI + monitoring + benchmark
```

## 0. Pre-Demo Checklist

Kiểm tra cluster:

```bash
kubectl get nodes -o wide
kubectl get pods -A | grep -E "Error|CrashLoop|Pending" || true
```

Kiểm tra Kafka:

```bash
kubectl get kafka,kafkanodepool,kafkatopic -n ingestion
```

Kết quả mong muốn:

```text
my-kafka-cluster Ready=True
kafka-node replicas=3
topics partitions=6 replication factor=3
```

Port-forward nên mở sẵn trong các terminal riêng:

```bash
kubectl port-forward -n mlops svc/mlflow 5000:5000
kubectl port-forward -n lakehouse svc/nyc-taxi-airflow 8081:8080
kubectl port-forward -n serving svc/nyc-taxi-fastapi 8000:8000
kubectl port-forward -n monitoring svc/grafana 30030:3000
```

UI:

```text
MLflow  : http://localhost:5000
Airflow : http://localhost:8081  admin / admin
FastAPI : http://localhost:8000/docs
Grafana : http://localhost:30030 admin / admin
```

## 1. Build Images

Build tất cả image chính:

```bash
bash scripts/images/build.sh all
bash scripts/images/build_training.sh
bash scripts/images/build_serving.sh
bash scripts/images/build_airflow.sh
```

Nếu chỉ sửa benchmark/streaming:

```bash
bash scripts/images/build.sh replay
bash scripts/images/build.sh bronze_consumer
```

## 2. Apply Infrastructure

```bash
kubectl apply -f infra/k8s/common/
kubectl apply -f infra/k8s/minio/
kubectl apply -f infra/k8s/ingestion/kafka/
kubectl apply -f infra/k8s/mlflow/
kubectl apply -f infra/k8s/airflow/airflow.yaml
kubectl apply -f infra/k8s/monitoring/prometheus-grafana-loki.yaml
```

Check:

```bash
kubectl get pods -n storage
kubectl get pods -n ingestion
kubectl get pods -n mlops
kubectl get pods -n lakehouse
kubectl get pods -n monitoring
```

Nói với thầy:

```text
Hệ thống chạy trên K3s. Kafka dùng 2 brokers, topic 6 partitions và replication factor 2.
Spark jobs chạy trên Kubernetes theo mô hình driver/executor pod.
MinIO làm object storage cho Delta Lake và MLflow artifacts.
```

## 2.5 Spark Resource Presets

Các script Spark đã hỗ trợ override tài nguyên bằng biến môi trường. Dynamic allocation bật theo kiểu opt-in:

```bash
SPARK_DYNAMIC_ALLOCATION_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_SHUFFLE_TRACKING_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS=1 \
SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS=4 \
SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS=1 \
...
```

Đánh giá khả thi:

```text
Khả thi cho các job batch/merge chạy một lần: raw -> bronze, bronze -> silver completed, lifecycle merge, route_estimates, features.
Với Spark trên Kubernetes, shuffleTracking=true là lựa chọn đúng vì không cần external shuffle service.
maxExecutors=4 hợp lý cho cluster nhỏ/demo, miễn là còn đủ CPU/RAM cho MinIO, Kafka, MLflow, Airflow.
Dynamic allocation không làm job nhẹ hơn; nó chỉ cho Spark tạo thêm executor khi có backlog task và thu bớt khi rảnh.
Nếu pod executor bị Pending lâu, giảm maxExecutors hoặc RAM executor.
Không nên lạm dụng dynamic allocation cho Structured Streaming và XGBoost training: streaming cần latency ổn định, XGBoost cần số worker cố định.
```

## 3. Historical Batch Path

### 3.1 Raw Parquet -> Bronze

```bash
SPARK_DRIVER_MEMORY=2g \
SPARK_EXECUTOR_MEMORY=3g \
SPARK_EXECUTOR_INSTANCES=1 \
SPARK_DYNAMIC_ALLOCATION_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_SHUFFLE_TRACKING_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS=1 \
SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS=4 \
SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS=1 \
bash scripts/ingestion/run_batch.sh
```

Nói:

```text
Spark batch job đọc parquet NYC Taxi từ volume dữ liệu, chuẩn hóa schema và ghi vào Bronze Delta Lake trên MinIO.
```

Output:

```text
s3a://lakehouse/bronze/nyc-taxi/trip_completed
```

### 3.2 Bronze -> Silver

```bash
SPARK_DRIVER_MEMORY=2g \
SPARK_EXECUTOR_MEMORY=4g \
SPARK_EXECUTOR_INSTANCES=1 \
SPARK_SHUFFLE_PARTITIONS=12 \
SPARK_DYNAMIC_ALLOCATION_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_SHUFFLE_TRACKING_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS=1 \
SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS=3 \
SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS=1 \
bash scripts/processing/run_silver.sh completed batch
```

Nói:

```text
Silver layer làm sạch dữ liệu, chuẩn hóa completed trips và tạo lifecycle state cho trip.
```

Output:

```text
s3a://lakehouse/silver/nyc-taxi/trip_completed
```

### 3.3 Silver Completed -> Lifecycle

Lệnh wrapper này tương đương `bash scripts/processing/run_silver.sh lifecycle batch`. Job đọc các bảng Silver started/completed hiện có và merge vào lifecycle table.

```bash
SPARK_DRIVER_MEMORY=2g \
SPARK_EXECUTOR_MEMORY=3g \
SPARK_EXECUTOR_INSTANCES=1 \
SPARK_SHUFFLE_PARTITIONS=12 \
SPARK_DYNAMIC_ALLOCATION_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_SHUFFLE_TRACKING_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS=1 \
SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS=4 \
SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS=1 \
bash scripts/processing/run_lifecycle_merge.sh
```

Output:

```text
s3a://lakehouse/silver/nyc-taxi/trip_lifecycle
```

### 3.4 Silver -> Gold Features

```bash
SPARK_DRIVER_MEMORY=2g \
SPARK_EXECUTOR_MEMORY=3g \
SPARK_EXECUTOR_INSTANCES=1 \
SPARK_DYNAMIC_ALLOCATION_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_SHUFFLE_TRACKING_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS=1 \
SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS=2 \
SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS=1 \
bash scripts/training/run_feature_engineering.sh route_estimates

SPARK_DRIVER_MEMORY=2g \
SPARK_EXECUTOR_MEMORY=3g \
SPARK_EXECUTOR_INSTANCES=1 \
SPARK_DYNAMIC_ALLOCATION_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_SHUFFLE_TRACKING_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS=1 \
SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS=4 \
SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS=1 \
bash scripts/training/run_feature_engineering.sh features
```

Nói:

```text
Gold layer tạo route estimates và training features. Feature contract này được dùng chung cho training và serving.
```

Output:

```text
s3a://lakehouse/gold/ml/route_estimates
s3a://lakehouse/gold/ml/features
```

## 4. Model Training + MLflow

```bash
SPARK_DRIVER_MEMORY=2g \
SPARK_DRIVER_MEMORY_OVERHEAD=1g \
SPARK_EXECUTOR_INSTANCES=2 \
SPARK_EXECUTOR_CORES=2 \
SPARK_EXECUTOR_MEMORY=7g \
XGB_NUM_WORKERS=2 \
bash scripts/training/run_training.sh
```

Ghi chú: training XGBoost nên giữ executor/worker cố định. Chỉ bật dynamic allocation cho training nếu bạn đã chỉnh `XGB_NUM_WORKERS` và chắc chắn executor không bị scale xuống trong lúc train.

Mở MLflow:

```text
http://localhost:5000
```

Chỉ cho thầy:

```text
Experiment: NYC_Taxi_Fare_Prediction
Model name: XGB_NYC_Fare
Metrics: train_r2, test_r2, test_rmse, test_mae
Artifacts: feature_importance, model
Registry stage: Production
```

Nói:

```text
Training job train XGBoost bằng Spark, log params/metrics/artifacts vào MLflow, register model và dùng MLflow Registry để quản lý version.
```

## 5. Airflow MLOps Retrain

Mở Airflow:

```text
http://localhost:8081
admin / admin
```

Trigger DAG:

```text
nyc_taxi_mlops_retrain
```

Flow DAG:

```text
build_route_estimates
  -> build_training_features
  -> train_candidate_model
  -> collect_candidate_metrics
  -> quality_gate
  -> promote_candidate_to_production OR skip_promotion
```

Quality gate:

```text
candidate_r2 >= current_r2 - 0.03
candidate_rmse <= current_rmse * 1.05
abs(train_r2 - test_r2) <= 0.12
test_rows >= 10000
smoke_test_passed == true
```

Nói:

```text
Airflow không promote mù quáng. Model mới chỉ lên Production nếu metric không tệ hơn Production hiện tại quá ngưỡng cho phép, không overfit mạnh, đủ test rows và load model smoke test thành công.
```

## 6. Realtime Ingestion Path

Terminal 1: chạy Spark streaming consumers Kafka -> Bronze:

```bash
SPARK_DRIVER_MEMORY=2g \
SPARK_EXECUTOR_MEMORY=2g \
SPARK_EXECUTOR_INSTANCES=1 \
MAX_OFFSETS_PER_TRIGGER=5000 \
TRIGGER_INTERVAL="30 seconds" \
bash scripts/ingestion/run_streaming.sh all
```

Terminal 2: bật replay producer:

```bash
kubectl apply -f infra/k8s/ingestion/streaming/streaming_deployment.yaml
```

Manifest này đang cấu hình replay producer giả lập nhanh hơn real-time 500 lần:

```yaml
STREAMING_SPEED_MULTIPLIER: "500"
```

Check:

```bash
kubectl get pods -n ingestion
kubectl get pods -n lakehouse
kubectl logs -n lakehouse -l spark-role=driver --tail=80
```

Nói:

```text
Replay producer mô phỏng taxi events realtime và đẩy started/completed events vào Kafka.
Kafka đóng vai trò buffer và tách producer khỏi Spark consumer.
Spark Structured Streaming đọc Kafka theo microbatch, decode Avro và ghi Bronze Delta.
```

Output:

```text
s3a://lakehouse/bronze/nyc-taxi/trip_started
s3a://lakehouse/bronze/nyc-taxi/trip_completed
```

## 7. Realtime Silver + Lifecycle

Chạy Silver streaming:

```bash
SPARK_DRIVER_MEMORY=1g \
SPARK_EXECUTOR_MEMORY=2g \
SPARK_EXECUTOR_INSTANCES=1 \
SPARK_SHUFFLE_PARTITIONS=4 \
bash scripts/processing/run_silver.sh started streaming

SPARK_DRIVER_MEMORY=1g \
SPARK_EXECUTOR_MEMORY=2g \
SPARK_EXECUTOR_INSTANCES=1 \
SPARK_SHUFFLE_PARTITIONS=4 \
bash scripts/processing/run_silver.sh completed streaming
```

Ghi chú: với streaming, ưu tiên executor cố định nhỏ để latency ổn định. Chỉ bật dynamic allocation khi benchmark cho thấy backlog tăng liên tục và cluster còn dư tài nguyên.

Airflow lifecycle merge:

```text
DAG: nyc_taxi_lifecycle_merge
Schedule: every 5 minutes
```

Nếu cần trigger manual trong Airflow UI:

```text
nyc_taxi_lifecycle_merge -> Trigger DAG
```

Nói:

```text
Airflow định kỳ submit Spark job để merge started/completed events thành lifecycle table. Spark driver/executor pod được cấu hình tự cleanup sau khi job hoàn thành.
```

## 8. Serving API

Apply FastAPI:

```bash
kubectl apply -f infra/k8s/serving/fastapi_deployment.yaml
```

Test predict:

```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "passenger_count": 2,
    "estimated_trip_distance": 3.5,
    "estimated_trip_duration_seconds": 900,
    "pickup_hour": 14,
    "pickup_day_of_week": 2,
    "pulocation_id": 161,
    "dolocation_id": 236
  }'
```

Nói:

```text
FastAPI load Production model từ MLflow Registry và trả về predicted fare.
```

## 9. Streaming Prediction

Chạy sau khi đã có:

```text
gold/ml/route_estimates
MLflow Production model
silver/trip_started realtime stream
```

Lệnh:

```bash
SPARK_DRIVER_MEMORY=2g \
SPARK_EXECUTOR_MEMORY=4g \
SPARK_EXECUTOR_INSTANCES=1 \
MAX_FILES_PER_TRIGGER=8 \
TRIGGER_INTERVAL="30 seconds" \
bash scripts/serving/run_stream_predict.sh
```

Flow:

```text
silver/trip_started
  + gold/ml/route_estimates
  + MLflow Production model
  -> gold/ml/predictions
```

Nói:

```text
Streaming prediction đọc trip_started từ Silver, lookup route estimates, load Production model từ MLflow, dự đoán fare và ghi prediction log vào Gold.
```

## 10. Model Quality Monitoring

Chạy sau khi có predictions và completed actuals:

```bash
SPARK_DRIVER_MEMORY=1g \
SPARK_EXECUTOR_MEMORY=2g \
SPARK_EXECUTOR_INSTANCES=1 \
SPARK_DYNAMIC_ALLOCATION_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_SHUFFLE_TRACKING_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS=1 \
SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS=2 \
SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS=1 \
bash scripts/training/run_feature_engineering.sh prediction_actuals

SPARK_DRIVER_MEMORY=1g \
SPARK_EXECUTOR_MEMORY=2g \
SPARK_EXECUTOR_INSTANCES=1 \
SPARK_DYNAMIC_ALLOCATION_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_SHUFFLE_TRACKING_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS=1 \
SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS=2 \
SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS=1 \
bash scripts/training/run_feature_engineering.sh model_quality_daily
```

Output:

```text
s3a://lakehouse/gold/ml/prediction_actuals
s3a://lakehouse/gold/monitoring/model_quality_daily
```

Nói:

```text
Khi actual fare đến muộn, hệ thống join prediction với actual để tính MAE/RMSE/bias theo ngày và model version.
```

## 11. BI With Trino + Superset

```bash
bash scripts/bi/setup_bi.sh
kubectl port-forward -n serving svc/superset 8088:8088
kubectl port-forward -n serving svc/trino 8080:8080
python3 scripts/bi/create_dashboard.py
```

Schemas chính trong Superset SQL Lab:

```text
delta.silver_nyc_taxi
delta.gold_ml
delta.gold_monitoring
```

Nói:

```text
Superset đọc Silver/Gold Delta tables qua Trino để visualize business metrics và model quality.
```

## 12. Streaming Benchmark

Chạy benchmark khuyến nghị:

```bash
BENCHMARK_RUN_SECONDS=180 \
BENCHMARK_TRIGGER_INTERVAL="10 seconds" \
BENCHMARK_SCENARIOS="low:20:500,medium:100:2000,high:500:10000" \
bash scripts/benchmark/run_streaming_benchmark.sh
```

Đọc summary mới nhất:

```bash
latest=$(find benchmark_results -name summary.csv -type f -printf '%T@ %p\n' | sort -nr | head -1 | cut -d' ' -f2-)
cat "$latest"
```

Ý nghĩa scenario:

```text
low:20:500
  low  = scenario name
  20   = STREAMING_SPEED_MULTIPLIER, producer replay nhanh hơn real-time 20 lần
  500  = MAX_OFFSETS_PER_TRIGGER, Spark đọc tối đa 500 Kafka messages mỗi microbatch
```

Các dấu hiệu chưa nghẽn:

```text
processed_rows_per_sec >= events_per_sec
avg_batch_duration không cao hơn trigger interval quá nhiều
Kafka lag không tăng liên tục
rows/batch không liên tục chạm maxOffsetsPerTrigger
CPU/RAM pod không chạm limit, không restart
```

Nói:

```text
Benchmark đo input events/sec, rows/batch, batch duration và processed rows/sec.
Nếu processed rows/sec lớn hơn events/sec và Kafka lag không tăng liên tục, pipeline đang xử lý kịp luồng dữ liệu.
```

## 13. Grafana / Prometheus / Loki

Apply nếu chưa có:

```bash
kubectl apply -f infra/k8s/monitoring/prometheus-grafana-loki.yaml
```

Mở Grafana:

```text
http://localhost:30030
admin / admin
```

Dashboard:

```text
NYC Taxi Streaming Benchmark
```

Panels chính:

```text
Pod CPU / Pod Memory
Spark Executor Autoscaling
Spark Microbatch Duration
Spark Batch Input Rows
Spark Rows Per Second
Spark Batch/Training Job Duration
Training Quality
Spark Microbatch Progress Logs
Spark Batch/Training Benchmark Logs
```

Loki query streaming:

```logql
{namespace="lakehouse"} |= "[benchmark][spark_progress]"
```

Loki query batch/training:

```logql
{namespace="lakehouse"} |~ "\\[benchmark\\]\\[spark_(batch|training)_job\\]"
```

Benchmark batch/training đang bật mặc định trong các lệnh `run_batch.sh`, `run_silver.sh ... batch`, `run_lifecycle_merge.sh`, `run_feature_engineering.sh`, `run_training.sh`.
Nếu cần tắt log benchmark cho một lần chạy:

```bash
BENCHMARK_METRICS_ENABLED=false bash scripts/processing/run_silver.sh completed batch
```

Prometheus query CPU/RAM:

```promql
sum(rate(container_cpu_usage_seconds_total{namespace=~"lakehouse|ingestion", container!="POD", image!=""}[1m]) and on(namespace,pod,container) (time() - container_last_seen{namespace=~"lakehouse|ingestion", container!="POD", image!=""} < 30)) by (namespace, pod)
sum(container_memory_working_set_bytes{namespace=~"lakehouse|ingestion", container!="POD", image!=""} and on(namespace,pod,container) (time() - container_last_seen{namespace=~"lakehouse|ingestion", container!="POD", image!=""} < 30)) by (namespace, pod)
count(count by (pod) (container_memory_working_set_bytes{namespace="lakehouse", pod=~".*-exec-[0-9]+.*", container!="POD", image!=""} and on(namespace,pod,container) (time() - container_last_seen{namespace="lakehouse", pod=~".*-exec-[0-9]+.*", container!="POD", image!=""} < 30))) or vector(0)
```

Spark driver Prometheus scrape:

```bash
--conf spark.ui.prometheus.enabled=true
--conf spark.kubernetes.driver.annotation.prometheus.io/scrape=true
--conf spark.kubernetes.driver.annotation.prometheus.io/path=/metrics/prometheus
--conf spark.kubernetes.driver.annotation.prometheus.io/port=4040
```

Nói:

```text
Prometheus/Grafana dùng để quan sát CPU/RAM pod và số executor đang chạy. Loki/Promtail thu logs Spark, bao gồm microbatch progress để vẽ batch duration và throughput.
```

## 14. Recommended Full Order

Chạy theo thứ tự này nếu muốn demo end-to-end:

```bash
# Build
bash scripts/images/build.sh all
bash scripts/images/build_training.sh
bash scripts/images/build_serving.sh
bash scripts/images/build_airflow.sh

# Infra
kubectl apply -f infra/k8s/common/
kubectl apply -f infra/k8s/minio/
kubectl apply -f infra/k8s/ingestion/kafka/
kubectl apply -f infra/k8s/mlflow/
kubectl apply -f infra/k8s/airflow/airflow.yaml
kubectl apply -f infra/k8s/monitoring/prometheus-grafana-loki.yaml

# Historical training path
SPARK_DRIVER_MEMORY=2g \
SPARK_EXECUTOR_MEMORY=3g \
SPARK_EXECUTOR_INSTANCES=1 \
SPARK_DYNAMIC_ALLOCATION_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_SHUFFLE_TRACKING_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS=1 \
SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS=4 \
SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS=1 \
bash scripts/ingestion/run_batch.sh

SPARK_DRIVER_MEMORY=2g \
SPARK_EXECUTOR_MEMORY=3g \
SPARK_EXECUTOR_INSTANCES=1 \
SPARK_SHUFFLE_PARTITIONS=12 \
SPARK_DYNAMIC_ALLOCATION_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_SHUFFLE_TRACKING_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS=1 \
SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS=4 \
SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS=1 \
bash scripts/processing/run_silver.sh completed batch

SPARK_DRIVER_MEMORY=2g \
SPARK_EXECUTOR_MEMORY=3g \
SPARK_EXECUTOR_INSTANCES=1 \
SPARK_SHUFFLE_PARTITIONS=12 \
SPARK_DYNAMIC_ALLOCATION_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_SHUFFLE_TRACKING_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS=1 \
SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS=4 \
SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS=1 \
bash scripts/processing/run_lifecycle_merge.sh

SPARK_DRIVER_MEMORY=2g \
SPARK_EXECUTOR_MEMORY=3g \
SPARK_EXECUTOR_INSTANCES=1 \
SPARK_DYNAMIC_ALLOCATION_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_SHUFFLE_TRACKING_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS=1 \
SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS=4 \
SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS=1 \
bash scripts/training/run_feature_engineering.sh route_estimates

SPARK_DRIVER_MEMORY=2g \
SPARK_EXECUTOR_MEMORY=3g \
SPARK_EXECUTOR_INSTANCES=1 \
SPARK_DYNAMIC_ALLOCATION_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_SHUFFLE_TRACKING_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS=1 \
SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS=4 \
SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS=1 \
bash scripts/training/run_feature_engineering.sh features

SPARK_DRIVER_MEMORY=2g \
SPARK_DRIVER_MEMORY_OVERHEAD=1g \
SPARK_EXECUTOR_INSTANCES=1 \
SPARK_EXECUTOR_CORES=2 \
SPARK_EXECUTOR_MEMORY=4g \
XGB_NUM_WORKERS=1 \
bash scripts/training/run_training.sh

# Airflow MLOps
# Open UI and trigger: nyc_taxi_mlops_retrain

# Realtime ingestion
SPARK_DRIVER_MEMORY=2g \
SPARK_EXECUTOR_MEMORY=2g \
SPARK_EXECUTOR_INSTANCES=1 \
MAX_OFFSETS_PER_TRIGGER=5000 \
TRIGGER_INTERVAL="30 seconds" \
bash scripts/ingestion/run_streaming.sh all
kubectl apply -f infra/k8s/ingestion/streaming/streaming_deployment.yaml

# Realtime silver
SPARK_DRIVER_MEMORY=1g \
SPARK_EXECUTOR_MEMORY=2g \
SPARK_EXECUTOR_INSTANCES=1 \
SPARK_SHUFFLE_PARTITIONS=4 \
bash scripts/processing/run_silver.sh started streaming

SPARK_DRIVER_MEMORY=1g \
SPARK_EXECUTOR_MEMORY=2g \
SPARK_EXECUTOR_INSTANCES=1 \
SPARK_SHUFFLE_PARTITIONS=4 \
bash scripts/processing/run_silver.sh completed streaming

# Serving
kubectl apply -f infra/k8s/serving/fastapi_deployment.yaml
SPARK_DRIVER_MEMORY=2g \
SPARK_EXECUTOR_MEMORY=2g \
SPARK_EXECUTOR_INSTANCES=1 \
MAX_FILES_PER_TRIGGER=8 \
TRIGGER_INTERVAL="30 seconds" \
bash scripts/serving/run_stream_predict.sh

# Model quality
SPARK_DRIVER_MEMORY=1g \
SPARK_EXECUTOR_MEMORY=2g \
SPARK_EXECUTOR_INSTANCES=1 \
SPARK_DYNAMIC_ALLOCATION_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_SHUFFLE_TRACKING_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS=1 \
SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS=2 \
SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS=1 \
bash scripts/training/run_feature_engineering.sh prediction_actuals

SPARK_DRIVER_MEMORY=1g \
SPARK_EXECUTOR_MEMORY=2g \
SPARK_EXECUTOR_INSTANCES=1 \
SPARK_DYNAMIC_ALLOCATION_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_SHUFFLE_TRACKING_ENABLED=true \
SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS=1 \
SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS=2 \
SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS=1 \
bash scripts/training/run_feature_engineering.sh model_quality_daily

# BI
bash scripts/bi/setup_bi.sh

# Benchmark
BENCHMARK_RUN_SECONDS=180 \
BENCHMARK_TRIGGER_INTERVAL="10 seconds" \
BENCHMARK_SCENARIOS="low:20:500,medium:100:2000,high:500:10000" \
bash scripts/benchmark/run_streaming_benchmark.sh
```

## 15. Short 3-Minute Script

```text
Đầu tiên em kiểm tra cluster K3s gồm 2 node. Kafka được cấu hình 2 broker, topic có 6 partitions và replication factor 2 để phù hợp với streaming benchmark.

Pipeline bắt đầu từ ingestion. Dữ liệu lịch sử parquet được Spark batch đưa vào Bronze Delta Lake. Với realtime, replay producer mô phỏng taxi events và đẩy vào Kafka. Spark Structured Streaming đọc Kafka theo microbatch và ghi Bronze.

Sau đó Spark processing chuyển Bronze sang Silver, làm sạch và chuẩn hóa started/completed events. Airflow orchestration chạy lifecycle merge định kỳ.

Từ Silver, feature engineering tạo Gold route estimates và Gold training features. Training job train XGBoost, log metrics/artifacts vào MLflow và register model. DAG MLOps retrain có quality gate để so sánh candidate model với Production model trước khi promote.

Serving dùng FastAPI load Production model từ MLflow Registry. Ngoài API trực tiếp, hệ thống còn có streaming prediction ghi prediction log vào Gold.

Cuối cùng em benchmark Kafka + Spark Streaming với low/medium/high load. Kết quả cho thấy khi tăng load, throughput tăng; nếu processed rows/sec lớn hơn events/sec và batch duration không tăng mất kiểm soát thì pipeline xử lý ổn định, chưa bị nghẽn.
```

## 16. Operational Notes

- Nếu historical backfill lớn, nên tắt bớt streaming jobs để giảm tải MinIO/S3A.
- `run_silver.sh completed batch` dùng cho historical backfill.
- `run_silver.sh completed streaming` dùng cho dữ liệu mới sau backfill.
- Nếu rebuild sạch, xóa cả data path và checkpoint path tương ứng trong MinIO.
- Benchmark output ghi vào `s3a://lakehouse/benchmark/...`, không đè production Bronze.
- Nếu `kubectl top pod` lỗi, cần bật hoặc cài `metrics-server`.
- Kafka 2 broker là cấu hình hợp lý cho cluster 2 node. KRaft production lý tưởng vẫn là 3 controller trên 3 node.

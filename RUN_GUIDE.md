# Hướng dẫn chạy NYC Taxis BigData Pipeline từng bước (Step-by-Step)

Tài liệu này hướng dẫn cách khởi chạy và kiểm tra toàn bộ Data & ML Pipeline cho dự án NYC Taxi từ đầu đến cuối trên môi trường cục bộ (K3s Kubernetes).

---

## Bước 1: Khởi động Infrastructure (K3s)

Pipeline cần các dịch vụ cơ bản: **MinIO** (Data Lake), **Kafka** (Streaming Source), và **MLflow** (Model Registry).

1. Cài đặt các thành phần trên cluster K3s:
   ```bash
   export KUBECONFIG=~/.kube/config

   # 1. Cài đặt MinIO
   kubectl apply -f infra/k8s/minio/minio.yaml
   
   # 2. Cài đặt Strimzi (Kafka Operator) và Kafka Cluster
   kubectl create namespace kafka
   kubectl apply -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
   # Chờ Strimzi chạy xong, sau đó cài đặt Kafka:
   kubectl apply -f infra/k8s/ingestion/kafka/kafka-cluster.yaml
   kubectl apply -f infra/k8s/ingestion/kafka/kafka-topics.yaml

   # 3. Cài đặt MLflow
   kubectl apply -f infra/k8s/mlflow/mlflow.yaml
   ```

2. Port-forward các dịch vụ ra localhost (để có thể truy cập UI và chạy Spark local):
   ```bash
   # Port-forward MinIO (Data) và MinIO Console (UI)
   kubectl port-forward -n minio svc/minio-api 9000:9000 &
   kubectl port-forward -n minio svc/minio-ui 30001:30001 &

   # Port-forward MLflow Tracking Server
   kubectl port-forward -n mlflow svc/mlflow 5000:5000 &
   ```
   > **UI Dashboard:**
   > - MinIO Console: `http://localhost:30001` (Tài khoản: `minioadmin` / `minioadmin`)
   > - MLflow UI: `http://localhost:5000`

---

## Bước 2: Chuẩn bị Bucket và Môi trường

1. Tạo các bucket trên MinIO để chứa dữ liệu:
   ```bash
   kubectl run minio-init --image=minio/mc:latest --restart=Never -n minio --rm -it -- sh -c "
     mc alias set local http://minio-api:9000 minioadmin minioadmin;
     mc mb --ignore-existing local/bronze;
     mc mb --ignore-existing local/silver;
     mc mb --ignore-existing local/gold;
     mc mb --ignore-existing local/mlflow;
   "
   ```

2. Cài đặt các thư viện Python cần thiết:
   ```bash
   pip install pyspark==3.5.3 delta-spark==3.2.0 mlflow==2.13.0 xgboost scikit-learn pandas numpy pyarrow boto3
   ```

---

## Bước 3: Chạy Data Pipeline (Bronze -> Silver -> Gold) & Training

Chúng ta có một file script tổng hợp chạy cục bộ (kết nối với K3s infra) để thực thi toàn bộ luồng Batch Ingestion và ML Training.

1. Khởi chạy Pipeline:
   ```bash
   # Chạy script pipeline (sẽ mất khoảng 5-10 phút tuỳ vào cấu hình máy)
   # Các biến môi trường AWS_* được truyền để MLflow có thể lưu model lên MinIO S3
   AWS_ACCESS_KEY_ID=minioadmin \
   AWS_SECRET_ACCESS_KEY=minioadmin \
   MLFLOW_S3_ENDPOINT_URL=http://localhost:9000 \
   python3 scripts/run_pipeline_local.py
   ```

2. Các bước script này thực hiện:
   - **Step 1 (Bronze):** Đọc file parquet raw từ `/data/yellow_data/2024`, map schema chuẩn và lưu thành Delta Table tại `s3a://bronze/trips/`.
   - **Step 2 (Silver):** Đọc Bronze, lọc các dữ liệu ngoại lai (giá cước âm, quãng đường bất thường), tính `trip_duration_seconds`, lưu vào `s3a://silver/trips/`.
   - **Step 3 (Gold):** Đọc Silver, tạo ra các Features (sine/cosine time, khoảng cách Manhattan, cluster heuristics), lưu vào `s3a://gold/features/`.
   - **Step 4 (ML Training):** Dùng XGBoost để huấn luyện mô hình dự đoán giá cước (`fare_amount`). Lưu model artifacts lên **MLflow**.

3. Sau khi chạy xong, hãy mở `http://localhost:5000` để xem kết quả Model XGBoost (R², MAE, RMSE) và check model đã được gán nhãn `production`.

---

## Bước 4: Chạy Real-time Serving (Inference)

Sau khi model đã sẵn sàng trên MLflow, bạn có thể triển khai hệ thống dự đoán theo thời gian thực (Real-time Streaming) và API.

### 4.1. Khởi chạy Spark Streaming Inference Job
Job này sẽ lắng nghe Kafka topic, nhận dữ liệu chuyến đi mới, áp dụng model MLflow và ghi kết quả (Prediction) ra Delta Lake.

```bash
# Đảm bảo Spark có thể kết nối với Kafka trên K3s
export KAFKA_BOOTSTRAP_SERVERS="my-kafka-cluster-kafka-bootstrap.kafka.svc:9092"

# Nộp job (Bạn có thể đóng gói vào Docker và chạy trên K8s thông qua script trong thư mục scripts/serving/)
bash scripts/serving/run_stream_predict.sh
```

### 4.2. Khởi chạy FastAPI Prediction Service
Cung cấp REST API cho các ứng dụng Frontend.

```bash
cd apps/serving/fastapi
pip install -r requirements.txt

# Chạy server FastAPI
MLFLOW_TRACKING_URI=http://localhost:5000 \
MLFLOW_S3_ENDPOINT_URL=http://localhost:9000 \
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
uvicorn app.main:app --host 0.0.0.0 --port 8000
```
> **Kiểm tra API:** Mở tài liệu API tại `http://localhost:8000/docs`. Bạn có thể gửi một POST request với thông tin điểm đón/trả và thời gian để nhận lại `predicted_fare_amount`.

---

## Xử lý sự cố (Troubleshooting)

1. **Lỗi `Unable to locate credentials` khi chạy XGBoost Training:** 
   Đảm bảo bạn đã truyền đủ 3 biến môi trường `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, và `MLFLOW_S3_ENDPOINT_URL` trước khi gọi python script.
2. **PySpark OOM (Out of Memory):** 
   Dữ liệu Gold năm 2024 cực lớn (~41 triệu dòng). Script hiện tại đang sample 10% (khoảng 4 triệu dòng) để build Pandas DataFrame cho XGBoost. Nếu vẫn lỗi OOM, có thể sửa `scripts/run_pipeline_local.py` và giảm tham số `fraction=0.1` xuống `0.05` hoặc `0.01`.
3. **Pods kẹt ở trạng thái Pending:**
   Chạy `kubectl describe pod <tên-pod> -n <namespace>` để xem lỗi. Thường do thiếu CPU/RAM trên node.

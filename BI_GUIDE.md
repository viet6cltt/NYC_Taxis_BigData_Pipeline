# NYC Taxi BI Stack Guide

BI trên nhánh `dev` dùng lại kiến trúc streaming lakehouse mới:

```text
Spark / Delta Lake on MinIO
  -> Hive Metastore metadata
  -> Trino delta catalog
  -> Superset dashboard
```

Superset không đọc trực tiếp Kafka hoặc raw Silver lớn cho từng chart. Dữ liệu được query qua Trino từ các Delta table đã được register:

```text
delta.silver_nyc_taxi.trip_started
delta.silver_nyc_taxi.trip_completed
delta.silver_nyc_taxi.trip_lifecycle
delta.gold_ml.route_estimates
delta.gold_ml.features
delta.gold_ml.predictions
delta.gold_ml.prediction_actuals
delta.gold_monitoring.model_quality_daily
```

## K3s Setup

Chạy sau khi MinIO đã có dữ liệu Lakehouse:

```bash
bash scripts/bi/setup_bi.sh
```

Port-forward:

```bash
kubectl port-forward -n lakehouse svc/superset 8088:8088 &
kubectl port-forward -n lakehouse svc/trino 8080:8080 &
```

Truy cập:

```text
Superset: http://localhost:8088
Login: admin / admin
```

Tạo dashboard mẫu:

```bash
python3 scripts/bi/create_dashboard.py
```

## Local BI Preview

Khi chưa bật máy chủ dữ liệu thật, có thể tạo bộ Delta tables local theo đúng contract production rồi chạy toàn bộ BI stack:

```bash
python3 scripts/bi/bootstrap_local_lakehouse.py
docker compose -f docker-compose.dev.yml up -d
python3 scripts/bi/create_dashboard.py
```

Script bootstrap dựng dữ liệu preview cho:

```text
delta.silver_nyc_taxi.trip_completed
delta.silver_nyc_taxi.trip_lifecycle
delta.gold_ml.route_estimates
delta.gold_ml.features
delta.gold_ml.predictions
delta.gold_ml.prediction_actuals
delta.gold_monitoring.model_quality_daily
```

Đây là dữ liệu preview sinh từ sample local, không phải kết quả model production thật; mục đích là để thiết kế và kiểm tra dashboard trước khi lakehouse thật chạy.

Script mặc định dùng:

- `TRINO_URL=http://localhost:8080` để host machine kiểm tra health.
- `TRINO_SQLALCHEMY_URI=trino://trino@trino:8080/delta` để **Superset container** kết nối tới service Trino trong Docker Compose.

Nếu chạy với K3s + port-forward, dùng:

```bash
TRINO_SQLALCHEMY_URI=trino://hive@trino.lakehouse.svc.cluster.local:8080/delta \
python3 scripts/bi/create_dashboard.py
```

## Sample Query

```sql
SELECT
    year_month,
    COUNT(*) AS completed_trips,
    ROUND(SUM(total_amount), 0) AS total_revenue,
    ROUND(AVG(fare_amount), 2) AS avg_fare
FROM delta.silver_nyc_taxi.trip_lifecycle
WHERE status = 'completed'
GROUP BY year_month
ORDER BY year_month;
```

Xem thêm query mẫu tại [scripts/bi/sample_queries.sql](scripts/bi/sample_queries.sql).

## Scope Dashboard

Script `scripts/bi/create_dashboard.py` tạo 3 dashboard riêng để mỗi dashboard kể một câu chuyện rõ:

1. **NYC Taxi - Business Overview**
   - completed trips, revenue, avg fare
   - xu hướng tháng, nhu cầu theo giờ
   - top routes, payment mix, lifecycle status
   - nguồn chính: `delta.silver_nyc_taxi.trip_lifecycle`

2. **NYC Taxi - Realtime Prediction Ops**
   - tổng số prediction, avg predicted fare
   - prediction volume theo ngày/giờ
   - tỷ lệ fallback `route_time` / `route` / `global`
   - volume theo model version, route coverage
   - nguồn chính: `delta.gold_ml.predictions`, `delta.gold_ml.route_estimates`

3. **NYC Taxi - Model Quality**
   - MAE, RMSE, bias, label delay
   - predicted fare vs actual fare
   - error theo `estimate_level`
   - top route có lỗi cao
   - nguồn chính: `delta.gold_ml.prediction_actuals`, `delta.gold_monitoring.model_quality_daily`

Ý đồ thiết kế:

- **Silver** giữ vai trò “business truth”.
- **Gold** giữ vai trò “ML serving + monitoring truth”.
- Khi dữ liệu realtime chưa có, dashboard business vẫn hữu ích; hai dashboard Gold sẽ đầy dần khi `predictions`, `prediction_actuals`, `model_quality_daily` xuất hiện.

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

Dashboard v1 tập trung chứng minh hệ thống hoạt động:

- completed trips và revenue theo tháng/giờ từ `trip_lifecycle` để tránh duplicate event.
- top pickup zones và routes.
- lifecycle status từ `trip_lifecycle`.
- model quality daily nếu đã chạy delayed-label monitoring.

BI nâng cao/star schema chi tiết có thể làm ở phase sau.

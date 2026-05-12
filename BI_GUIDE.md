# 🚕 NYC Taxi BI Stack Guide

Hướng dẫn thiết lập và vận hành hệ thống Business Intelligence (BI) cho dự án NYC Taxi Pipeline trên môi trường Local/Docker.

## 🏗️ Kiến trúc hệ thống
Dữ liệu di chuyển qua các tầng sau:
1.  **MinIO (S3)**: Lưu trữ các tệp Parquet (Bronze -> Silver -> Gold).
2.  **Spark**: Xử lý ETL và Feature Engineering.
3.  **Hive Metastore**: Quản lý Metadata và định nghĩa bảng.
4.  **Trino**: Query Engine tốc độ cao kết nối với Hive.
5.  **Superset**: Giao diện trực quan hóa dữ liệu và Dashboard.

---

## 🚀 Quy trình triển khai (3 Bước)

### Bước 1: Khởi động Hạ tầng Docker
Chạy toàn bộ các dịch vụ cần thiết (MinIO, Postgres, Hive, Trino, Superset):
```bash
docker-compose -f docker-compose.dev.yml up -d
```

### Bước 2: Chạy Data Pipeline
Đưa dữ liệu thực tế vào hệ thống và thực hiện Feature Engineering:
```bash
python3 run_pipeline_v3.py
```
*Lưu ý: Script này sẽ tự động ánh xạ các cột và tính toán giờ giấc, ngày tháng để phục vụ BI.*

### Bước 3: Khởi tạo Dashboard tự động
Tự động đăng ký Dataset và tạo Dashboard mẫu trên Superset:
```bash
python3 scripts/bi/init_nyc_dashboard.py
```

---

## 🔗 Thông tin truy cập các dịch vụ

| Dịch vụ | Địa chỉ (Local) | Tài khoản |
| :--- | :--- | :--- |
| **Superset** | `http://localhost:8088` | `admin` / `admin` |
| **Trino** | `http://localhost:8080` | `trino` (không pass) |
| **MinIO Console** | `http://localhost:9001` | `minioadmin` / `minioadmin` |
| **MLflow** | `http://localhost:5000` | - |

---

## 📊 Các biểu đồ có sẵn trong Dashboard
Sau khi chạy Bước 3, bạn mở Superset và tìm Dashboard **"NYC Taxi Executive Dashboard"**:
*   **Trip Distribution by Hour**: Thống kê khung giờ cao điểm.
*   **Top 10 Pickup Locations**: Các khu vực đón khách nhộn nhịp nhất.
*   **Revenue by Day of Week**: Doanh thu phân bổ theo các thứ trong tuần.

---

## 🛠️ Xử lý sự cố thường gặp

### 1. Dữ liệu trên biểu đồ bị `<NULL>` hoặc `N/A`
*   **Nguyên nhân**: Pipeline chưa chạy hoặc tên cột trong file Parquet không khớp với định dạng Hive.
*   **Khắc phục**: Chạy lại `python3 run_pipeline_v3.py`. Script này đã được tối chuẩn hóa tên cột về chữ thường và có gạch dưới (ví dụ: `pulocation_id`).

### 2. Lỗi kết nối "Max retries exceeded" trong Superset
*   **Nguyên nhân**: Thường do cache DNS của Superset container bị kẹt địa chỉ cũ của Trino.
*   **Khắc phục**: 
    ```bash
    docker restart nyc-superset
    ```

### 3. Cổng 8088 bị trùng (nếu chạy ở local máy cá nhân)
*   Sử dụng tính năng **Port Forwarding** của Lightning Studio để map cổng 8088 sang một cổng khác (ví dụ: 8089) trên máy của bạn.

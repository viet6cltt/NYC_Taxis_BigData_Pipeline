# Hướng dẫn Setup Superset với Trino

Tài liệu hướng dẫn cài đặt Superset (BI & Data Visualization) kết nối với Trino Query Engine.

---

## 🚀 Bước 1: Deploy Superset trên K8s

### 1.1 Tạo namespace `serving` (nếu chưa có)
```bash
kubectl create namespace serving
```

### 1.2 Deploy Superset stack (PostgreSQL + Superset + Trino connector)
```bash
kubectl apply -f infra/k8s/superset/superset.yaml
```

Các thành phần được deploy:
- **PostgreSQL**: Lưu metadata của Superset
- **Superset**: BI dashboard & SQL editor
- **Init Job**: Tự động tạo Trino connection

### 1.3 Kiểm tra deployment
```bash
# Xem pods
kubectl get pods -n serving -l app=superset

# Xem logs
kubectl logs -n serving -l app=superset -f

# Xem init job
kubectl get jobs -n serving
kubectl logs -n serving job/superset-add-trino-db
```

---

## 2️⃣ Bước 2: Port-forward để truy cập UI

```bash
# Port-forward Superset UI
kubectl port-forward -n serving svc/superset 8088:8088 &

# Hoặc dùng NodePort (đã cấu hình port 30088)
# Truy cập: http://<node-ip>:30088
```

---

## 3️⃣ Bước 3: Đăng nhập Superset

### 3.1 Thông tin đăng nhập mặc định:
```
Username: admin
Password: admin
```

### 3.2 Truy cập UI:
```
http://localhost:8088
```

---

## 4️⃣ Bước 4: Kiểm tra Trino Connection

### 4.1 Vào `Admin Panel` → `Databases`
- Nếu Init Job thành công, sẽ thấy database `trino` đã được thêm

### 4.2 Test connection
```sql
SELECT 1 AS test_query
```

---

## 5️⃣ Bước 5: Tạo Dataset & Dashboard

### 5.1 Tạo Dataset từ Trino
1. Vào `SQL Lab` → `SQL Editor`
2. Chọn database `trino`
3. Chạy query:
   ```sql
   SELECT * FROM delta.silver_nyc_taxi.trip_lifecycle WHERE status = 'completed' LIMIT 10
   ```
4. Lưu lại thành **Dataset**

### 5.2 Tạo Dashboard
1. Vào `Dashboards` → `+ Dashboard`
2. Thêm charts từ datasets
3. Tùy chỉnh layout & visualization

---

## 🔧 Bước 6: Cấu hình nâng cao (tuỳ chọn)

### 6.1 Thay đổi mật khẩu admin
```bash
# Port-forward hoặc SSH vào Superset pod
kubectl exec -it -n serving deployment/superset -- bash

# Chạy lệnh
superset set-password admin new_password_here
```

### 6.2 Enable caching
Sửa `SUPERSET_CONFIG` để bật Redis caching:
```bash
# Thêm Redis pod (tuỳ chọn)
# Tập trung vào caching nếu cần performance optimization
```

### 6.3 Thêm users khác
```bash
kubectl exec -it -n serving deployment/superset -- bash
superset fab create-admin --username newuser --password password123 --firstname New --lastname User --email newuser@example.com
```

---

## 📊 Bước 7: Query các tables từ Trino

Sau khi connect, có thể query các tables từ Delta Lake:

```sql
-- Bronze layer
SELECT * FROM delta.bronze_nyc_taxi.trip_completed LIMIT 5

-- Silver lifecycle current-state table
SELECT * FROM delta.silver_nyc_taxi.trip_lifecycle WHERE status = 'completed' LIMIT 5

-- Gold layer
SELECT * FROM delta.gold_ml.features LIMIT 5
```

---

## ⚡ Troubleshooting

### Issue 1: "Cannot connect to Trino"
```bash
# Kiểm tra Trino đã chạy
kubectl get svc -n serving trino

# Test connection từ Superset pod
kubectl exec -it -n serving deployment/superset -- \
  curl -X GET http://trino.serving.svc.cluster.local:8080/v1/info
```

### Issue 2: "Database initialization failed"
```bash
# Kiểm tra PostgreSQL logs
kubectl logs -n serving deployment/postgres-superset

# Xóa và deploy lại
kubectl delete deployment postgres-superset -n serving
kubectl apply -f infra/k8s/superset/superset.yaml
```

### Issue 3: Init Job không tạo connection
```bash
# Xem logs của init job
kubectl logs -n serving job/superset-add-trino-db

# Tạo connection thủ công qua UI
# Admin Panel → Databases → + Database
# Database: trino
# SQLAlchemy URI: trino://trino.serving.svc.cluster.local:8080/delta
```

---

## 🎯 Next Steps

1. **Tạo Dashboards** cho metrics quan trọng
2. **Tạo Alerts** khi dữ liệu bất thường
3. **Share Dashboards** với team
4. **Tối ưu Queries** cho performance
5. **Cấu hình RBAC** cho multi-user access

---

## 📝 Cấu hình Superset chi tiết

File config: `infra/k8s/superset/superset.yaml`

### Các biến môi trường chính:
- `SQLALCHEMY_DATABASE_URI`: Kết nối PostgreSQL metadata
- `SUPERSET_SECRET_KEY`: Secret key (nên thay đổi!)
- `SUPERSET_LOAD_EXAMPLES`: Load ví dụ mẫu
- `TRINO_HOST/PORT`: Endpoint của Trino

### Tài nguyên (Resources):
- **CPU Request**: 500m, **Limit**: 1000m
- **Memory Request**: 512Mi, **Limit**: 1Gi

---

## 🔐 Production Checklist

- [ ] Thay đổi `SUPERSET_SECRET_KEY` thành một giá trị bảo mật
- [ ] Thay đổi mật khẩu admin
- [ ] Setup HTTPS/TLS cho Superset
- [ ] Cấu hình LDAP/OAuth nếu dùng SSO
- [ ] Setup backup cho PostgreSQL metadata
- [ ] Enable monitoring & logging
- [ ] Cấu hình ingress thay vì LoadBalancer

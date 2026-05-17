# Noi dung slide thuyet trinh: NYC Taxis BigData Pipeline

Tai lieu nay duoc viet de copy sang PowerPoint/Canva/Google Slides. Moi slide gom: noi dung hien thi, goi y hinh minh hoa, va loi thuyet trinh ngan.

## Slide 1. Tieu de

**NYC Taxis BigData Pipeline**  
Data Engineering + MLOps cho du lieu taxi New York

- Xu ly du lieu taxi theo ca batch va streaming
- Xay dung lakehouse Bronze -> Silver -> Gold
- Huan luyen va trien khai mo hinh du doan gia cuoc taxi
- Giam sat ket qua bang API, frontend va BI dashboard

**Goi y hinh:** anh taxi vang NYC hoac so do pipeline tong quan.  
**Loi thuyet trinh:** "Bai tap lon cua em khong chi dung o viec train model, ma xay mot pipeline end-to-end tu du lieu tho den du doan realtime va dashboard giam sat."

## Slide 2. Bai toan va muc tieu

**Bai toan:** Tu du lieu chuyen di taxi NYC, xay dung he thong co the:

- Nap du lieu lich su nam 2024 vao data lake
- Mo phong luong su kien realtime cua chuyen di taxi
- Lam sach, chuan hoa va tao bang phan tich
- Tao feature cho machine learning
- Train mo hinh XGBoost du doan `fare_amount`
- Phuc vu du doan qua streaming backend va REST API
- Giam sat chat luong model sau khi co actual fare

**Goi y hinh:** icons theo chuoi: raw data -> lakehouse -> ML -> API -> dashboard.  
**Loi thuyet trinh:** "Muc tieu la bien du lieu taxi thanh mot he thong san sang van hanh, co lineage, co model registry va co monitoring."

## Slide 3. Dataset

**Nguon du lieu trong project**

- NYC Yellow Taxi Trip Data nam 2024
- 12 file Parquet, tu `yellow_tripdata_2024-01.parquet` den `yellow_tripdata_2024-12.parquet`
- Tong so dong: **41,169,720 trips**
- Tong dung luong local: **~693 MB**
- Co bang `taxi_zone_lookup.csv` de tra cuu TLC zone

**Cac truong quan trong**

- Thoi gian don/tra: `tpep_pickup_datetime`, `tpep_dropoff_datetime`
- Vi tri: `PULocationID`, `DOLocationID`
- Thong tin chuyen di: `passenger_count`, `trip_distance`
- Thanh toan: `fare_amount`, `tip_amount`, `total_amount`, `payment_type`

**Goi y hinh:** bang dataset nho hoac bieu do 12 thang.  
**Loi thuyet trinh:** "Du lieu du lon de minh hoa Big Data, nhung van co the chay demo local bang sample nho."

## Slide 4. Kien truc tong quan

```text
Parquet 2024                  Replay Producer
    |                               |
    v                               v
Spark Batch -> Bronze Delta <- Kafka <- Avro events
                    |
                    v
          Silver clean tables + lifecycle
                    |
                    v
       Gold route estimates + ML features
                    |
                    v
       XGBoost + MLflow Model Registry
                    |
          +---------+----------+
          |                    |
 Streaming prediction      FastAPI / UI
          |                    |
          v                    v
 Gold predictions       User fare prediction
          |
          v
 Trino + Superset BI + model quality
```

**Goi y hinh:** ve lai so do nay bang flowchart.  
**Loi thuyet trinh:** "Kien truc di theo Medallion Architecture, tach ro raw data, clean data, ML features va serving."

## Slide 5. Cong nghe su dung

| Nhom | Cong nghe |
|---|---|
| Xu ly du lieu | Apache Spark, PySpark, Spark Structured Streaming |
| Lakehouse | Delta Lake, MinIO S3-compatible storage |
| Streaming | Apache Kafka, Strimzi, Avro |
| Machine Learning | XGBoost, scikit-learn |
| MLOps | MLflow Experiment Tracking + Model Registry |
| Serving | FastAPI, BentoML, Uvicorn |
| Frontend | React, Vite, lucide-react, xlsx |
| BI | Trino, Hive Metastore, Apache Superset |
| Trien khai | Docker, Kubernetes/K3s |

**Goi y hinh:** logo grid cong nghe.  
**Loi thuyet trinh:** "Stack duoc chon de gan voi kien truc production: storage rieng, compute rieng, model registry rieng va dashboard rieng."

## Slide 6. Medallion Architecture

**Bronze: raw event layer**

- Luu su kien `trip_started` va `trip_completed`
- Giu metadata: `event_id`, `trip_id`, `schema_version`, `ingest_mode`, `event_time`
- Du lieu duoc ghi dang Delta Lake tren MinIO

**Silver: clean business layer**

- Bang `trip_started`, `trip_completed`
- Bang `trip_lifecycle` de theo doi trang thai started/completed/expired
- Them cot phan tich: `trip_hour`, `year_month`, `payment_type_desc`

**Gold: ML va monitoring layer**

- `route_estimates`: lookup distance/duration theo tuyen
- `features`: bang feature training
- `predictions`: log du doan realtime
- `prediction_actuals`, `model_quality_daily`: danh gia model

**Goi y hinh:** 3/4 tang Bronze-Silver-Gold-Serving.  
**Loi thuyet trinh:** "Moi tang co vai tro rieng: Bronze de audit, Silver de business truth, Gold de ML truth."

## Slide 7. Ingestion: Batch va Streaming

**Batch ingestion**

- Doc historical Parquet tu `data/yellow_data/2024`
- Chuan hoa ten cot raw thanh contract Bronze
- Tao `trip_id` bang SHA-256 tu source file, pickup/dropoff, route va fare
- Tao `event_id` bang SHA-256 tu schema version, event type va trip id
- Ghi vao `s3a://lakehouse/bronze/nyc-taxi/trip_completed`

**Streaming ingestion**

- `replay_producer` doc Parquet theo thu tu thoi gian pickup
- Tao hai loai event: `trip_started` va `trip_completed`
- Serialize bang Avro, day vao Kafka topics:
  - `nyc-taxi-trip-started`
  - `nyc-taxi-trip-completed`
- Spark Structured Streaming doc Kafka, decode Avro va ghi Bronze Delta

**Goi y hinh:** 2 nhanh batch/stream cung chay vao Bronze.  
**Loi thuyet trinh:** "Diem hay la batch va streaming cung quy ve mot Bronze contract, nen cac buoc sau co the dung chung logic."

## Slide 8. Bronze -> Silver

**Lam sach du lieu**

- Loai event thieu `event_id`, `trip_id`, pickup/dropoff
- Kiem tra dropoff sau pickup
- Loai distance/fare/total khong hop le
- Kiem tra passenger count
- Deduplicate theo `event_id`
- Streaming co watermark 48 gio de xu ly duplicate/late event

**Lifecycle table**

- `trip_started` tao record status `started`
- `trip_completed` merge vao lifecycle va cap nhat status `completed`
- Trip qua TTL co the danh dau `expired`

**Gia tri tao ra**

- Silver giu du lieu sach cho BI va cho Gold ML
- Lifecycle giup noi event started/completed thanh mot hanh trinh day du

**Goi y hinh:** bang truoc/sau cleaning, hoac state machine started -> completed/expired.  
**Loi thuyet trinh:** "Silver la noi bien event roi rac thanh su that nghiep vu co the query duoc."

## Slide 9. Gold Feature Engineering

**Route estimates**

- Tao lookup bang median distance va duration
- Co 3 muc fallback:
  - `route_time`: pickup zone + dropoff zone + pickup hour + day of week
  - `route`: pickup zone + dropoff zone
  - `global`: fallback toan cuc khi tuyen it du lieu

**Feature contract gom 14 feature**

- Trip: passenger count, estimated distance, estimated duration, estimated speed
- Time: pickup hour, day of week, weekend
- Cyclical: hour sin/cos, day sin/cos
- Route proxy: distance_manhattan, location_cluster, temporal_cluster

**Diem quan trong**

- Feature training va feature serving duoc giu cung contract
- Tranh lech feature giua offline training va online inference

**Goi y hinh:** bang 14 feature, highlight distance/duration/speed.  
**Loi thuyet trinh:** "Model khong biet actual distance cua trip moi, nen he thong tao estimated distance va duration tu route history."

## Slide 10. Training va MLflow

**Mo hinh**

- Target: `fare_amount`
- Algorithm: XGBoost Regressor
- Default params: `n_estimators=100`, `max_depth=6`, `learning_rate=0.1`
- Train/test split: 80/20

**MLOps**

- Log params, metrics, feature importance vao MLflow
- Log model artifact va signature
- Register model ten `XGB_NYC_Fare`
- Tu dong promote model len `Production` neu Test R2 >= 0.70

**Goi y hinh:** screenshot MLflow experiment/model registry.  
**Loi thuyet trinh:** "MLflow giup minh biet model nao duoc train voi data nao, metric nao va version nao dang phuc vu production."

## Slide 11. Ket qua local pipeline

**Small local pipeline trong repo**

- Input sample: 10,000 rows tu file thang 01/2024
- Bronze completed: 10,000 rows
- Silver completed sau cleaning: 9,564 rows
- Route estimates: 6,796 rows
- Gold feature rows: 9,564 rows

**Model XGBoost local**

| Metric | Train | Test |
|---|---:|---:|
| R2 | 0.9017 | 0.8418 |
| RMSE | 4.54 | 5.19 |
| MAE | 2.14 | 2.54 |

**Feature quan trong nhat**

- `estimated_trip_distance`: ~56.97%
- `estimated_trip_duration_seconds`: ~23.14%
- `estimated_speed`: ~8.71%

**Goi y hinh:** bar chart metric + feature importance.  
**Loi thuyet trinh:** "Trong sample local, model dat R2 test khoang 0.84. Feature quan trong nhat van la khoang cach va thoi luong uoc tinh."

## Slide 12. Realtime Prediction Backend

**Flow realtime**

```text
silver/trip_started stream
  + gold/ml/route_estimates
  + MLflow Production model
  -> feature extraction
  -> XGBoost prediction
  -> gold/ml/predictions
```

**Cach xu ly**

- Model Production duoc load tu MLflow
- Broadcast model den Spark executors
- Dung `mapInPandas` de predict theo partition
- Ghi prediction log vao Delta, partition theo `year_month`
- Giu metadata: model name, model version, model stage, prediction timestamp

**Goi y hinh:** luong event realtime voi micro-batches.  
**Loi thuyet trinh:** "Day la phan chung minh model khong chi nam trong notebook, ma duoc dua vao backend xu ly stream."

## Slide 13. FastAPI va Frontend Demo

**FastAPI endpoints**

- `GET /health`: trang thai model va version
- `POST /predict`: du doan gia cuoc mot trip
- `GET /zones`: danh sach TLC zones cho UI
- `GET /stream-demo/data`: du lieu demo stream
- `GET /docs`: OpenAPI docs

**Frontend React**

- Man hinh Pricing: nhap passenger, distance, duration, pickup/dropoff zone
- Preset route: JFK -> Times Sq, Penn -> UES, SoHo -> FiDi
- Man hinh Streaming: replay file/Excel/CSV theo micro-batch
- Hien thi prediction log, MAE, bias va model version

**Goi y hinh:** screenshot UI Pricing va Streaming.  
**Loi thuyet trinh:** "Frontend giup nguoi xem demo truc tiep thay vi chi goi curl."

## Slide 14. BI va Monitoring

**BI stack**

```text
Delta Lake on MinIO -> Hive Metastore -> Trino -> Superset
```

**Dashboard duoc tao tu script**

- **NYC Taxi - Business Overview**
  - completed trips, revenue, average fare
  - demand theo gio, top routes, payment mix
- **NYC Taxi - Realtime Prediction Ops**
  - prediction volume, average predicted fare
  - estimate level mix, model version volume, route coverage
- **NYC Taxi - Model Quality**
  - MAE, RMSE, bias, label delay
  - predicted vs actual fare
  - route error hotspots

**Goi y hinh:** 3 o dashboard hoac screenshot Superset.  
**Loi thuyet trinh:** "Monitoring duoc tach thanh business truth va ML truth, de nhin ca hieu qua kinh doanh lan chat luong model."

## Slide 15. Deployment va Run Flow

**Moi truong production-style tren K3s**

- MinIO: object storage cho Bronze/Silver/Gold va MLflow artifacts
- Kafka/Strimzi: streaming broker
- Spark jobs: batch, streaming, feature engineering, training, prediction
- MLflow: tracking server va model registry
- FastAPI/BentoML: model serving
- Trino + Superset: SQL va BI dashboard

**Thu tu chay chuan**

1. Build Docker images
2. Apply infrastructure: MinIO, Kafka, MLflow
3. Batch ingestion historical 2024 -> Bronze
4. Bronze -> Silver completed + lifecycle
5. Gold route estimates -> Gold features
6. Train XGBoost -> MLflow Production
7. Streaming ingestion -> Silver started/completed
8. Stream prediction -> Gold predictions
9. Prediction actuals -> model quality daily
10. FastAPI/BentoML + BI dashboard

**Goi y hinh:** timeline run flow.  
**Loi thuyet trinh:** "Run flow nay cho thay project co kha nang demo tung phan va cung co duong chay end-to-end."

## Slide 16. Demo kich ban thuyet trinh

**Demo 1: MLflow**

- Mo MLflow UI
- Chi ra experiment `NYC_Taxi_Fare_Prediction`
- Chi model `XGB_NYC_Fare` version Production
- Noi ve metric R2, RMSE, MAE va feature importance

**Demo 2: FastAPI / Frontend**

- Mo UI Trip Fare Workspace
- Chon route preset, bam Estimate fare
- Giai thich response gom predicted fare, model name, version

**Demo 3: Streaming monitor**

- Mo tab Streaming
- Replay sample rows theo micro-batch
- Chi prediction log, MAE, bias, model version

**Demo 4: Superset**

- Mo dashboard Business Overview hoac Model Quality
- Noi ve query qua Trino tren Delta Lake

**Goi y hinh:** checklist demo.  
**Loi thuyet trinh:** "Khi thuyet trinh, nen demo theo thu tu: model registry -> API -> streaming -> dashboard."

## Slide 17. Diem manh cua project

- End-to-end: tu raw data den serving va monitoring
- Ho tro ca batch va streaming
- Dung Medallion Architecture ro rang
- Co event contract, Avro schema va idempotent event id
- Co lifecycle merge de noi started/completed trip
- Feature contract dong nhat giua training va serving
- Co MLflow registry va auto-promotion
- Co hai cach serving: FastAPI va BentoML
- Co BI dashboard cho business, prediction ops va model quality
- Co local demo runner de chay nhanh khi khong co K8s

**Goi y hinh:** danh sach diem manh theo 4 nhom: Data, ML, Serving, BI.  
**Loi thuyet trinh:** "Diem manh lon nhat la project co tinh he thong, khong phai mot script don le."

## Slide 18. Han che va huong phat trien

**Han che hien tai**

- Demo local dung sample nho nen metric chi mang tinh minh hoa
- Route feature dang dua vao TLC zone id proxy, chua dung toa do/ban do that
- MLflow backend trong dev dung SQLite, phu hop demo hon production lon
- Chua co orchestration scheduler nhu Airflow/Argo Workflows
- Chua co alert tu dong khi model drift hoac error vuot nguong

**Huong phat trien**

- Them Airflow/Argo de lap lich va retry pipeline
- Dung PostgreSQL cho MLflow backend store
- Them feature store online/offline
- Tinh geo distance bang taxi zone centroid hoac map service
- Them drift detection, alert Slack/Email
- CI/CD cho Docker image, Spark jobs va API

**Goi y hinh:** roadmap 3 buoc: productionize, improve model, automate monitoring.  
**Loi thuyet trinh:** "Neu tiep tuc phat trien, em se tap trung vao orchestration, feature store va monitoring tu dong."

## Slide 19. Ket luan

**Tong ket**

- Project xay dung thanh cong mot Big Data + MLOps pipeline cho NYC Taxi
- Du lieu duoc xu ly theo Bronze -> Silver -> Gold
- Model XGBoost duoc train, track va register bang MLflow
- He thong co realtime prediction backend, REST API, frontend va BI dashboard
- Pipeline co the chay local de demo va co manifest/script cho K8s

**Thong diep chinh**

> Tu du lieu taxi tho, project bien thanh mot he thong du doan gia cuoc co kha nang van hanh, giam sat va mo rong.

**Goi y hinh:** ket thuc bang architecture full pipeline hoac screenshot demo.  
**Loi thuyet trinh:** "Bai tap lon nay the hien ca data engineering, machine learning va trien khai ung dung."

## Phu luc. Cau lenh demo nhanh

**Chay local small pipeline**

```bash
python3 scripts/run_small_local_pipeline.py
```

**Chay streaming backend demo**

```bash
python3 scripts/demo_streaming_backend.py --events 20 --batch-size 5
```

**Chay FastAPI local**

```bash
cd apps/serving/fastapi
LOAD_MODEL_ON_STARTUP=false uvicorn app.main:app --host 0.0.0.0 --port 8000
```

**Chay BI stack local**

```bash
docker compose -f docker-compose.dev.yml up -d
python3 scripts/bi/create_dashboard.py
```

## Phu luc. Noi dung noi ngan trong 1 phut

"Project NYC Taxis BigData Pipeline cua em xay dung mot he thong end-to-end cho du lieu taxi New York. Du lieu lich su nam 2024 gom hon 41 trieu trip duoc nap vao Bronze bang Spark batch, dong thoi project co replay producer mo phong realtime qua Kafka va Avro. Sau do Spark xu ly Bronze sang Silver, lam sach du lieu va tao lifecycle cho trip started/completed. Tu Silver, he thong tao Gold route estimates va 14 feature dong nhat cho training va serving. Model XGBoost du doan fare amount duoc track bang MLflow, register thanh `XGB_NYC_Fare` va promote len Production khi dat nguong R2. Phan serving gom streaming inference ghi prediction log vao Gold, FastAPI cho du doan truc tiep, frontend React de demo va Superset dashboard de giam sat business cung model quality. Diem chinh cua project la ket hop Data Engineering, MLOps va BI thanh mot pipeline co the van hanh."

## Phu luc. Nguon da doc trong project

- `README.md`, `RUN_GUIDE.md`, `RUN_FLOW.md`, `BI_GUIDE.md`
- `apps/ingestion/...`: batch ingestion, replay producer, Kafka to Bronze, Avro schema
- `apps/processing/bronze_to_silver/...`: cleaning, lifecycle merge
- `apps/training/feature_engineering/...`: route estimates, features, monitoring actuals
- `apps/training/train_xgboost/...`: XGBoost + MLflow
- `apps/serving/stream_predict/...`: realtime inference
- `apps/serving/fastapi/...`: API, static UI, demo endpoints
- `apps/frontend/src/App.jsx`: React demo console
- `scripts/bi/...`: Superset datasets, charts, dashboards
- `_local_small_pipeline/summary.json`, `_local_streaming_demo/model_quality_summary.json`

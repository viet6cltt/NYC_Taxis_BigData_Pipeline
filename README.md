# NYC Taxis BigData Pipeline

## Project Overview
The **NYC Taxis BigData Pipeline** is a comprehensive, scalable data engineering + MLOps project that processes New York City Taxi trip data through a **4-Layer Medallion Architecture** (Bronze → Silver → Gold → ML Serving). It supports both **Batch** and **Streaming** ingestion, end-to-end ML training with experiment tracking, and real-time fare prediction — all deployed on Kubernetes.

## Full Architecture

```
                         ┌─────────────────────────────────────────────────────┐
                         │                 INGESTION LAYER                      │
  Parquet files ────────►│  Spark Batch Job (historical_to_bronze)             │
                         │                                    ┌─── Bronze ─────┐│
  NYC TLC data ─────────►│  Replay Producer → Kafka           │  Delta Lake    ││
  (simulated)            │  → Spark Streaming (kafka_to_bronze)│  (MinIO)      ││
                         └────────────────────────────────────┴─────────────────┘
                                              │
                         ┌────────────────────▼──────────────────────────────── ┐
                         │              PROCESSING LAYER                          │
                         │  bronze_to_silver: clean, filter, feature derive       │
                         └────────────────────┬──────────────────────────────────┘
                                              │ Silver Delta Lake
              ┌───────────────────────────────┴──────────────────────────────────┐
              │                       ML PIPELINE                                  │
              │                                                                     │
              │  [TRAINING — Batch/Scheduled]      [SERVING — Always-on]           │
              │                                                                     │
              │  feature_engineering               Kafka (new trip events)          │
              │  Silver → Gold features                   ↓                        │
              │        ↓                           stream_predict                  │
              │  train_xgboost                     Spark Streaming + XGBoost       │
              │  XGBoost + MLflow                         ↓                        │
              │        ↓                           Gold/predictions (Delta)         │
              │  MLflow Model Registry ──────────►        ↓                        │
              │                                    FastAPI  POST /predict           │
              └──────────────────────────────────────────────────────────────────── ┘
```

## Medallion Layers

| Layer | MinIO Path | Description |
|---|---|---|
| **Bronze** | `s3a://bronze/` | Raw events, Avro-decoded, no transformation |
| **Silver** | `s3a://silver/trips/` | Cleaned, validated, partitioned by `year_month` |
| **Gold (Features)** | `s3a://gold/features/` | Feature-engineered table ready for training |
| **Gold (Predictions)** | `s3a://gold/predictions/` | Real-time XGBoost predictions + model lineage |

## Technology Stack

| Category | Technology |
|---|---|
| Data Processing | Apache Spark (PySpark), Delta Lake |
| Streaming Broker | Apache Kafka |
| Data Serialization | Avro |
| Object Storage | MinIO (S3-compatible) |
| ML Framework | XGBoost, scikit-learn |
| ML Tracking | MLflow (Experiments + Model Registry) |
| Serving API | FastAPI + Uvicorn |
| Orchestration | Kubernetes (K8s), Spark-on-K8s |
| Language | Python 3.11 |

## Directory Structure

```text
NYC_Taxis_BigData_Pipeline/
├── apps/
│   ├── ingestion/
│   │   ├── batch/historical_to_bronze/    # Spark batch: parquet → Bronze
│   │   ├── common/                        # Shared contracts and normalizers
│   │   └── streaming/
│   │       ├── kafka_to_bronze/           # Spark Streaming: Kafka → Bronze
│   │       ├── replay_producer/           # Simulates live NYC taxi events
│   │       └── schemas/                   # Avro schemas (taxi_trip_event.avsc)
│   ├── processing/
│   │   └── bronze_to_silver/             # Spark job: Bronze → Silver
│   ├── training/
│   │   ├── feature_engineering/          # Spark batch: Silver → Gold features
│   │   └── train_xgboost/               # Train XGBoost + register in MLflow
│   └── serving/
│       ├── stream_predict/               # Spark Streaming: Kafka → XGBoost → Gold
│       └── fastapi/                      # FastAPI REST API: POST /predict
├── infra/k8s/
│   ├── common/                           # Namespaces, RBAC, NFS PV/PVCs
│   ├── ingestion/                        # Kafka cluster, topics, UI
│   ├── minio/                            # MinIO object storage
│   ├── mlflow/                           # MLflow Tracking Server
│   ├── serving/                          # FastAPI + stream_predict deployments
│   └── spark/                            # Spark History Server / UI
└── scripts/
    ├── images/                           # Docker build scripts (ingestion/training/serving)
    ├── ingestion/                        # Run batch + streaming ingestion
    ├── processing/                       # Run Bronze → Silver
    ├── training/                         # Run feature engineering + training
    └── serving/                          # Run streaming inference
```

## How to Run

### 1. Build Docker Images
```bash
bash scripts/images/build.sh           # Ingestion images
bash scripts/images/build_batch.sh     # Batch ingestion image
bash scripts/images/build_training.sh  # Training images (new)
bash scripts/images/build_serving.sh   # Serving images (new)
```

### 2. Infrastructure Setup
```bash
kubectl apply -f infra/k8s/common/
kubectl apply -f infra/k8s/minio/
kubectl apply -f infra/k8s/ingestion/kafka/
kubectl apply -f infra/k8s/mlflow/          # MLflow Tracking Server
```

### 3. Run Ingestion Jobs
```bash
bash scripts/ingestion/run_batch.sh        # Historical parquet → Bronze
bash scripts/ingestion/run_streaming.sh    # Kafka → Bronze (always-on)
```

### 4. Run Processing (Bronze → Silver)
```bash
bash scripts/processing/run_silver.sh
```

### 5. Run ML Training Pipeline
```bash
# Feature engineering: Silver → Gold
bash scripts/training/run_feature_engineering.sh

# Train XGBoost + register in MLflow Registry
bash scripts/training/run_training.sh
```

### 6. Run ML Serving Pipeline
```bash
# Deploy FastAPI prediction server
kubectl apply -f infra/k8s/serving/fastapi_deployment.yaml

# Start Spark Streaming inference (Kafka → XGBoost → Gold/predictions)
bash scripts/serving/run_stream_predict.sh
```

### 7. Call the Prediction API
```bash
curl -X POST http://<fastapi-service>:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "passenger_count": 2,
    "trip_distance": 3.5,
    "trip_duration_seconds": 900,
    "pickup_hour": 14,
    "pickup_day_of_week": 2,
    "pulocation_id": 161,
    "dolocation_id": 236
  }'
```

## ML Model Details

- **Target**: `fare_amount` (USD)
- **Algorithm**: XGBoost Regressor (`n_estimators=100`, `max_depth=6`, `lr=0.1`)
- **Features** (14):
  - Temporal: `pickup_hour`, `pickup_day_of_week`, `is_weekend`
  - Cyclical: `hour_sin`, `hour_cos`, `day_sin`, `day_cos`
  - Trip metrics: `passenger_count`, `trip_distance`, `trip_duration_seconds`, `speed`
  - Geo: `distance_manhattan`, `location_cluster`, `temporal_cluster`
- **Tracking**: MLflow — all runs, metrics, artifacts, and model versions logged
- **Auto-promotion**: Model is promoted to `Production` stage when Test R² ≥ 0.70

## Configuration

| Variable | Default | Description |
|---|---|---|
| `MINIO_ENDPOINT` | `http://minio-api.minio.svc.cluster.local:9000` | MinIO S3 endpoint |
| `MLFLOW_TRACKING_URI` | `http://mlflow.mlflow.svc.cluster.local:5000` | MLflow server |
| `MODEL_NAME` | `XGB_NYC_Fare` | Registered model name |
| `MODEL_STAGE` | `Production` | MLflow model stage to serve |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka-cluster-...:9092` | Kafka broker address |

All configs are also manageable via `.env` file in the repository root.

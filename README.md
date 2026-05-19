# NYC Taxis BigData Pipeline

A streaming lakehouse project for NYC taxi data that combines **data engineering, MLOps, realtime inference, and BI** on top of a 4-layer medallion architecture:

```text
Bronze -> Silver -> Gold -> Serving / Monitoring / BI
```

The pipeline supports:

- historical batch ingestion for model training
- realtime Kafka ingestion for `trip_started` and `trip_completed` events
- Delta Lake tables on MinIO
- XGBoost training with MLflow tracking + registry
- realtime fare prediction with Spark Streaming
- REST serving through FastAPI and BentoML
- model-quality monitoring tables
- BI dashboards through Trino + Superset

## Architecture

```text
                         +---------------- INGESTION ----------------+
Historical parquet ----> Spark batch -------------------------------> Bronze trip_completed
Realtime replay -------> Kafka -> Spark streaming ------------------> Bronze trip_started / trip_completed
                         +-------------------------------------------+
                                              |
                                              v
                         +---------------- PROCESSING ---------------+
                         | Bronze -> Silver cleaning + lifecycle     |
                         +-------------------------------------------+
                                              |
                         +--------------------+----------------------+
                         |                                           |
                         v                                           v
              Gold route estimates                         Silver realtime stream
                         |                                           |
                         v                                           v
              Gold training features                    Spark stream_predict
                         |                              + route lookup + model
                         v                                           |
              XGBoost training + MLflow Registry                    v
                         |                              Gold predictions
                         +--------------------+----------------------+
                                              |
                   +--------------------------+--------------------------+
                   |                         |                          |
                   v                         v                          v
              FastAPI / BentoML      prediction_actuals       Trino + Superset BI
                                             |
                                             v
                                  model_quality_daily
```

## Lakehouse Tables

| Layer | Path | Purpose |
|---|---|---|
| Bronze | `s3a://lakehouse/bronze/nyc-taxi/trip_started` | Raw realtime start events |
| Bronze | `s3a://lakehouse/bronze/nyc-taxi/trip_completed` | Raw completed events from batch + streaming |
| Silver | `s3a://lakehouse/silver/nyc-taxi/trip_started` | Clean started trips |
| Silver | `s3a://lakehouse/silver/nyc-taxi/trip_completed` | Clean completed trips |
| Silver | `s3a://lakehouse/silver/nyc-taxi/trip_lifecycle` | Joined trip lifecycle / business truth |
| Gold | `s3a://lakehouse/gold/ml/route_estimates` | Historical route priors for inference |
| Gold | `s3a://lakehouse/gold/ml/features` | Training features |
| Gold | `s3a://lakehouse/gold/ml/predictions` | Realtime prediction log |
| Gold | `s3a://lakehouse/gold/ml/prediction_actuals` | Prediction vs actual labels |
| Gold | `s3a://lakehouse/gold/monitoring/model_quality_daily` | Daily model-quality metrics |

## Stack

| Area | Technology |
|---|---|
| Processing | Apache Spark, PySpark, Delta Lake |
| Streaming | Apache Kafka, Avro |
| Storage | MinIO |
| ML | XGBoost, scikit-learn, MLflow |
| Serving | FastAPI, BentoML |
| Orchestration | Kubernetes, Spark-on-Kubernetes, Airflow |
| BI | Hive Metastore, Trino, Superset |
| Language | Python 3.11 |

## Repository Layout

```text
apps/
  ingestion/        batch + streaming producers/consumers
  processing/       bronze_to_silver jobs
  training/         feature engineering + XGBoost training
  serving/          stream_predict, FastAPI, BentoML
  orchestration/    Airflow DAGs
infra/k8s/          MinIO, Kafka, MLflow, Airflow, serving, Spark manifests
scripts/
  ingestion/        ingestion runners
  processing/       Silver + lifecycle runners
  training/         feature engineering + training runners
  serving/          streaming inference + local BentoML
  bi/               Trino/Superset setup and dashboard creation
```

## Quick Run Order

For the exact operational sequence, use [RUN_FLOW.md](RUN_FLOW.md). The short path is:

### 1. Build images

```bash
bash scripts/images/build.sh all
bash scripts/images/build_training.sh
bash scripts/images/build_serving.sh
bash scripts/images/build_airflow.sh
```

### 2. Prepare infrastructure

```bash
kubectl apply -f infra/k8s/minio/minio.yaml
kubectl apply -f infra/k8s/common/spark-rabc.yaml

kubectl create namespace kafka --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
kubectl apply -f infra/k8s/ingestion/kafka/kafka-cluster.yaml
kubectl apply -f infra/k8s/ingestion/kafka/kafka-topics.yaml
kubectl apply -f infra/k8s/mlflow/mlflow.yaml
```

### 3. Historical path for training

```bash
bash scripts/ingestion/run_batch.sh
bash scripts/processing/run_silver.sh completed batch
bash scripts/training/run_feature_engineering.sh route_estimates
bash scripts/training/run_feature_engineering.sh features
bash scripts/training/run_training.sh
```

### 4. Realtime path

```bash
bash scripts/ingestion/run_streaming.sh all
bash scripts/processing/run_silver.sh started streaming
bash scripts/processing/run_silver.sh completed streaming
bash scripts/serving/run_stream_predict.sh
```

### 5. Monitoring path

```bash
bash scripts/training/run_feature_engineering.sh prediction_actuals
bash scripts/training/run_feature_engineering.sh model_quality_daily
```

### 6. Serving APIs

```bash
kubectl apply -f infra/k8s/serving/fastapi_deployment.yaml
kubectl apply -f infra/k8s/serving/bentoml_deployment.yaml
```

### 7. Airflow lifecycle merge

```bash
kubectl apply -f infra/k8s/airflow/airflow.yaml
kubectl port-forward -n lakehouse svc/nyc-taxi-airflow 8081:8080
```

- UI: `http://localhost:8081`
- Login: `admin / admin`
- Main DAG: `nyc_taxi_lifecycle_merge`

### 8. BI stack

```bash
bash scripts/bi/setup_bi.sh
kubectl port-forward -n serving svc/superset 8088:8088
kubectl port-forward -n serving svc/trino 8080:8080
python3 scripts/bi/create_dashboard.py
```

See [BI_GUIDE.md](BI_GUIDE.md) for dashboard scope and local preview instructions.

## Prediction API Example

The API uses **estimated** distance and duration because inference happens at `trip_started`, before actual trip outcomes exist.

```bash
curl -X POST http://<service-host>:8000/predict \
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

## Model Notes

- target: `fare_amount`
- algorithm: XGBoost regressor
- registered model: `XGB_NYC_Fare`
- production stage: promoted when test `R² >= 0.70`
- feature families:
  - temporal + cyclical time features
  - passenger count
  - estimated distance, duration, speed
  - Manhattan distance and route/time clusters

## Core Configuration

| Variable | Default | Meaning |
|---|---|---|
| `MINIO_ENDPOINT` | `http://minio-api.storage.svc.cluster.local:9000` | MinIO endpoint |
| `MLFLOW_TRACKING_URI` | `http://mlflow.mlflow.svc.cluster.local:5000` | MLflow server |
| `MODEL_NAME` | `XGB_NYC_Fare` | registered model name |
| `MODEL_STAGE` | `Production` | model stage served by inference |
| `KAFKA_BOOTSTRAP_SERVERS` | cluster-specific | Kafka brokers |

Root-level `.env` can be used to override local defaults.

## Companion Docs

- [RUN_FLOW.md](RUN_FLOW.md) — full end-to-end execution order
- [RUN_GUIDE.md](RUN_GUIDE.md) — compact deployment guide
- [BI_GUIDE.md](BI_GUIDE.md) — Trino/Superset architecture and dashboards
- [SETUP_K3S.md](SETUP_K3S.md) — cluster setup notes

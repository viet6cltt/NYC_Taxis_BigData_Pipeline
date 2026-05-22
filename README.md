# NYC Taxis BigData Pipeline

This repository builds an end-to-end NYC Yellow Taxi lakehouse and fare-prediction demo on Spark and Kubernetes. It covers the data path from historical parquet files and replayed taxi events to Delta Lake tables, MLflow model versions, realtime fare predictions, BI dashboards, and operational benchmark signals.

```text
Bronze raw events -> Silver cleaned trips/lifecycle -> Gold ML + monitoring -> Serving / BI
```

## What The Project Does

### 1. Batch training path

The historical path backfills completed Yellow Taxi trips and prepares the model inputs:

```text
NYC Taxi parquet
  -> Spark historical_to_bronze
  -> Bronze trip_completed Delta table
  -> Spark Bronze-to-Silver cleaning
  -> Silver trip_completed
  -> Gold route estimates + Gold training features
  -> Spark XGBoost training
  -> MLflow tracking + Model Registry
```

The training target is `fare_amount`. The registered model name used by the repo is `XGB_NYC_Fare`.

### 2. Realtime lakehouse and prediction path

The streaming path replays taxi trips as Kafka Avro events and keeps start and completion events separate:

```text
Replay producer
  -> Kafka trip_started / trip_completed topics
  -> Spark Structured Streaming Kafka consumers
  -> Bronze started/completed Delta tables
  -> Silver started/completed Delta tables
```

Airflow periodically merges Silver start and completion events into a lifecycle table. Streaming inference reads `trip_started`, enriches it with route priors learned from historical data, loads the Production model from MLflow, and writes fare predictions to Gold:

```text
Silver trip_started
  + Gold route_estimates
  + MLflow Production model
  -> Gold predictions
```

When completed trips arrive later, feature jobs join predictions with actual fares and aggregate daily quality metrics:

```text
Gold predictions + Silver trip_completed
  -> Gold prediction_actuals
  -> Gold model_quality_daily
```

### 3. Serving, demo, analytics, and operations

The repo includes several surfaces around the lakehouse:

- FastAPI single-trip inference, health, taxi-zone, route-estimate, and local streaming-demo endpoints
- BentoML serving for the same fare prediction contract
- a React/Vite pricing and streaming workspace under `apps/frontend`
- Trino + Hive Metastore + Superset BI over Silver and Gold Delta tables
- Airflow DAGs for lifecycle merge and metric-gated MLOps retraining
- Prometheus, Grafana, and Loki manifests for Spark job and streaming benchmark visibility
- streaming benchmark scripts that collect Spark progress, Kafka lag, and pod resource samples
- lightweight local runners for a small laptop/workspace pipeline and backend demo

## Architecture

```text
              Historical parquet                         Replayed taxi events
                     |                                          |
                     v                                          v
          Spark batch ingestion                    Kafka Avro started/completed
                     |                                          |
                     +------------------+-----------------------+
                                        v
                         Bronze Delta tables on MinIO
                                        |
                                        v
                         Silver cleaning and normalization
                                        |
                    +-------------------+-------------------+
                    |                                       |
                    v                                       v
       Silver completed trips                  Silver started/completed streams
                    |                                       |
                    v                                       v
       Gold route estimates/features          Airflow lifecycle merge table
                    |                                       |
                    v                                       v
       XGBoost training + MLflow              Spark streaming prediction
                    |                          + route priors + MLflow model
                    |                                       |
                    +-------------------+-------------------+
                                        v
                 Gold predictions / delayed labels / quality metrics
                    +-------------------+-------------------+
                    |                   |                   |
                    v                   v                   v
             FastAPI/BentoML      Trino/Superset      Grafana/Loki
```

## Lakehouse Tables

| Layer | Path | Purpose |
|---|---|---|
| Bronze | `s3a://lakehouse/bronze/nyc-taxi/trip_started` | Raw realtime start events |
| Bronze | `s3a://lakehouse/bronze/nyc-taxi/trip_completed` | Raw completed events from batch and streaming |
| Silver | `s3a://lakehouse/silver/nyc-taxi/trip_started` | Clean started trips |
| Silver | `s3a://lakehouse/silver/nyc-taxi/trip_completed` | Clean completed trips |
| Silver | `s3a://lakehouse/silver/nyc-taxi/trip_lifecycle` | Merged started/completed trip state for BI and operations |
| Gold | `s3a://lakehouse/gold/ml/route_estimates` | Historical route and time priors used before actual trip outcomes exist |
| Gold | `s3a://lakehouse/gold/ml/features` | Training feature table |
| Gold | `s3a://lakehouse/gold/ml/predictions` | Realtime prediction log |
| Gold | `s3a://lakehouse/gold/ml/prediction_actuals` | Predictions joined with delayed actual fares |
| Gold | `s3a://lakehouse/gold/monitoring/model_quality_daily` | Daily model-quality aggregates |

## Main Components

| Area | Implementation |
|---|---|
| Storage | MinIO object storage with Delta Lake tables |
| Processing | Apache Spark, PySpark, Spark Structured Streaming |
| Streaming | Kafka, Strimzi manifests, Avro trip-event schema |
| ML | Spark XGBoost, scikit-learn-compatible MLflow artifacts, MLflow Registry |
| Serving | FastAPI, BentoML, Spark streaming predictor |
| Orchestration | Airflow DAGs and Spark-on-Kubernetes submit scripts |
| Analytics | Hive Metastore, Trino, Superset |
| Observability | Prometheus, Grafana, Loki, benchmark logs and summaries |
| UI | FastAPI static demo pages and React/Vite frontend |

## Repository Layout

```text
apps/
  frontend/         React/Vite pricing and streaming workspace
  ingestion/        historical batch ingestion, Kafka replay producer, Kafka-to-Bronze
  processing/       Bronze-to-Silver cleaning and lifecycle jobs
  training/         Gold feature engineering and XGBoost training
  serving/          FastAPI, BentoML, Spark streaming prediction
  orchestration/    Airflow DAGs for lifecycle merge and MLOps retrain
infra/
  k8s/              namespaces, MinIO, Kafka, Spark RBAC, MLflow, Airflow, serving, BI, monitoring
  docker/           local BI support for Docker Compose
scripts/
  ingestion/        Spark batch and streaming submit wrappers
  processing/       Silver and lifecycle runners
  training/         Gold feature and model-training runners
  serving/          streaming prediction and local BentoML helpers
  bi/               Trino/Superset setup and dashboard creation
  benchmark/        streaming, batch processing, and Delta pruning benchmark runners
```

## Kubernetes Run Flow

The primary flow is the K3s/Kubernetes path. See [RUN_FLOW.md](RUN_FLOW.md) for the full demo sequence, resource presets, benchmark steps, and port-forward commands.

### 1. Build application images

```bash
bash scripts/images/build.sh all
bash scripts/images/build_training.sh
bash scripts/images/build_serving.sh
bash scripts/images/build_airflow.sh
```

### 2. Apply the core manifests

```bash
kubectl apply -f infra/k8s/common/
kubectl apply -f infra/k8s/minio/
kubectl apply -f infra/k8s/ingestion/kafka/
kubectl apply -f infra/k8s/mlflow/
kubectl apply -f infra/k8s/airflow/airflow.yaml
```

The manifests and scripts use the `storage`, `ingestion`, `lakehouse`, `mlops`, `serving`, and `monitoring` namespaces. Kafka manifests expect the Strimzi operator to be available before the Kafka custom resources are applied.

### 3. Build training data and register a model

```bash
bash scripts/ingestion/run_batch.sh
bash scripts/processing/run_silver.sh completed batch
bash scripts/processing/run_lifecycle_merge.sh
bash scripts/training/run_feature_engineering.sh route_estimates
bash scripts/training/run_feature_engineering.sh features
bash scripts/training/run_training.sh
```

### 4. Run realtime ingestion and streaming prediction

```bash
bash scripts/ingestion/run_streaming.sh all
kubectl apply -f infra/k8s/ingestion/streaming/streaming_deployment.yaml

bash scripts/processing/run_silver.sh started streaming
bash scripts/processing/run_silver.sh completed streaming
bash scripts/serving/run_stream_predict.sh
```

Streaming inference emits JSON driver logs for each prediction microbatch and a bounded
sample of predicted trips. `PREDICTION_LOG_SAMPLE_ROWS=10` controls the per-batch sample
size; set it to `0` to keep only batch summaries. Promtail ships the pod stdout logs to
Loki, and the Grafana streaming dashboard includes prediction-log and sampled fare panels.

### 5. Produce delayed-label monitoring tables

```bash
bash scripts/training/run_feature_engineering.sh prediction_actuals
bash scripts/training/run_feature_engineering.sh model_quality_daily
```

### 6. Expose serving and BI surfaces

```bash
kubectl apply -f infra/k8s/serving/fastapi_deployment.yaml
kubectl apply -f infra/k8s/serving/bentoml_deployment.yaml

bash scripts/bi/setup_bi.sh
python3 scripts/bi/create_dashboard.py
```

For the compact deployment checklist, see [RUN_GUIDE.md](RUN_GUIDE.md).

## Airflow Workflows

| DAG | Role |
|---|---|
| `nyc_taxi_lifecycle_merge` | Every 5 minutes, merge Silver started and completed events into `trip_lifecycle`, then expire stale lifecycle records |
| `nyc_taxi_mlops_retrain` | Rebuild route estimates and features, train a candidate model, compare metrics against the current Production model, and promote only passing candidates |

The retrain DAG uses gates for test-row count, smoke-test success, train/test `R2` gap, candidate `R2` drop, and candidate RMSE ratio before changing the Production stage.

## Serving Contract

Prediction happens when a trip starts, so serving uses estimated route distance and duration rather than final trip values.

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

The default feature families are passenger count, estimated distance and duration, estimated speed, temporal features, weekend and cyclical encodings, and deterministic route/time cluster proxies.

## BI And Benchmarking

The BI setup registers Delta tables for these Trino schemas:

```text
delta.silver_nyc_taxi
delta.gold_ml
delta.gold_monitoring
```

Superset dashboards focus on trip lifecycle metrics, route estimates, fare prediction outputs, delayed labels, and model-quality trends. See [BI_GUIDE.md](BI_GUIDE.md).

Streaming benchmark runs are driven by:

```bash
bash scripts/benchmark/run_streaming_benchmark.sh
```

Each benchmark result folder can include scenario settings, replay logs, Spark driver logs, Kafka lag samples, pod resource samples, and a `summary.csv`.

Batch processing and Delta partition-pruning benchmark runs are driven by:

```bash
bash scripts/benchmark/run_processing_benchmark.sh
```

The processing runner measures isolated `Bronze completed -> Silver completed -> Gold route_estimates -> Gold features` trials from the full-year 2024 Bronze table. It writes benchmark Delta outputs under `s3a://lakehouse/benchmark/processing/<run_id>/`, compares lifecycle MERGE into `year_month`-partitioned and unpartitioned lifecycle targets fed by benchmark Silver completed data, then compares partitioned and unpartitioned copies of Gold features with Spark `year_month` filters. Result folders include `processing_*.csv`, `merge_*.csv`, `pruning_*.csv`, Spark physical plans, logs, and a short `report.md` under `benchmark_results/processing_<run_id>/`.

Useful overrides include `PROCESSING_BENCHMARK_YEAR`, `PROCESSING_BENCHMARK_TRIALS`, `PROCESSING_BENCHMARK_MERGE_TRIALS`, `PROCESSING_BENCHMARK_MERGE_INCLUDE_ALL_MONTHS`, `PROCESSING_BENCHMARK_ROOT`, `PROCESSING_BENCHMARK_BRONZE_PATH`, `PROCESSING_BENCHMARK_FILTER_MONTH`, and `PROCESSING_BENCHMARK_FILTER_QUARTER`. The standard report run requires all 12 months for its benchmark year; smoke runs against a partial Bronze input can set `PROCESSING_BENCHMARK_REQUIRE_FULL_YEAR=false`. MERGE smoke runs can set `PROCESSING_BENCHMARK_MERGE_INCLUDE_ALL_MONTHS=false` to skip the full-year control MERGE and keep the month-filtered partition comparison.

## Local Helpers

The local runner exercises a small version of the training contract without submitting Spark jobs to Kubernetes:

```bash
python3 scripts/run_small_local_pipeline.py
python3 run_fastapi_local.py
```

`docker-compose.dev.yml` provides a local MinIO, MLflow, Hive Metastore, Trino, and Superset stack for BI-oriented development. `scripts/demo_streaming_backend.py` can replay a small backend prediction demo from local pipeline outputs.

## Core Configuration

| Variable | Default | Meaning |
|---|---|---|
| `MINIO_ENDPOINT` | `http://minio-api.storage.svc.cluster.local:9000` | MinIO endpoint used inside cluster workloads |
| `MLFLOW_TRACKING_URI` | `http://mlflow.mlops.svc.cluster.local:5000` | MLflow tracking server |
| `MODEL_NAME` | `XGB_NYC_Fare` | Registry model name |
| `MODEL_STAGE` | `Production` | Registry stage used by serving |
| `KAFKA_BOOTSTRAP_SERVERS` | `my-kafka-cluster-kafka-bootstrap.ingestion.svc.cluster.local:9092` | Default Kafka broker address for streaming consumers |
| `REGISTRY` | `localhost:5000` | Image registry used by build and Spark submit scripts unless overridden |

Root-level `.env` can override script defaults.

## Companion Docs

- [RUN_FLOW.md](RUN_FLOW.md) - full end-to-end demo flow
- [RUN_GUIDE.md](RUN_GUIDE.md) - compact run checklist
- [BI_GUIDE.md](BI_GUIDE.md) - Trino and Superset setup
- [SETUP_K3S.md](SETUP_K3S.md) - cluster setup notes

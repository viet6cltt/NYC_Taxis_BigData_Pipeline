from __future__ import annotations

import math
import os
import shlex
import time
from datetime import datetime, timedelta

import mlflow
import mlflow.xgboost
import pandas as pd
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from mlflow.tracking import MlflowClient


REGISTRY = os.getenv("NYC_TAXI_IMAGE_REGISTRY", "localhost:5000")
IMAGE_TAG = os.getenv("NYC_TAXI_IMAGE_TAG", "v1.0")
FEATURE_IMAGE = os.getenv(
    "NYC_TAXI_FEATURE_IMAGE",
    f"{REGISTRY}/nyc-taxi-feature-engineering:{IMAGE_TAG}",
)
TRAIN_IMAGE = os.getenv(
    "NYC_TAXI_TRAIN_IMAGE",
    f"{REGISTRY}/nyc-taxi-train-xgboost:{IMAGE_TAG}",
)
SPARK_IMAGE_PULL_POLICY = os.getenv("NYC_TAXI_SPARK_IMAGE_PULL_POLICY", "IfNotPresent")

NAMESPACE = os.getenv("NYC_TAXI_SPARK_NAMESPACE", "lakehouse")
SERVICE_ACCOUNT = os.getenv("NYC_TAXI_SPARK_SERVICE_ACCOUNT", "spark-user")
SPARK_MASTER = os.getenv("NYC_TAXI_SPARK_MASTER", "k8s://https://kubernetes.default.svc")

MINIO_ENDPOINT = os.getenv(
    "NYC_TAXI_MINIO_ENDPOINT",
    "http://minio-api.storage.svc.cluster.local:9000",
)
MINIO_ACCESS_KEY = os.getenv("NYC_TAXI_MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("NYC_TAXI_MINIO_SECRET_KEY", "minioadmin")

MLFLOW_TRACKING_URI = os.getenv(
    "NYC_TAXI_MLFLOW_TRACKING_URI",
    "http://mlflow.mlops.svc.cluster.local:5000",
)
MLFLOW_S3_ENDPOINT_URL = os.getenv("NYC_TAXI_MLFLOW_S3_ENDPOINT_URL", MINIO_ENDPOINT)
EXPERIMENT_NAME = os.getenv("NYC_TAXI_EXPERIMENT_NAME", "NYC_Taxi_Fare_Prediction")
MODEL_NAME = os.getenv("NYC_TAXI_MODEL_NAME", "XGB_NYC_Fare")

SILVER_COMPLETED_PATH = os.getenv(
    "NYC_TAXI_SILVER_COMPLETED_PATH",
    "s3a://lakehouse/silver/nyc-taxi/trip_completed",
)
GOLD_ROUTE_ESTIMATES_PATH = os.getenv(
    "NYC_TAXI_GOLD_ROUTE_ESTIMATES_PATH",
    "s3a://lakehouse/gold/ml/route_estimates",
)
GOLD_FEATURES_PATH = os.getenv(
    "NYC_TAXI_GOLD_FEATURES_PATH",
    "s3a://lakehouse/gold/ml/features",
)
GOLD_PREDICTIONS_PATH = os.getenv(
    "NYC_TAXI_GOLD_PREDICTIONS_PATH",
    "s3a://lakehouse/gold/ml/predictions",
)
GOLD_PREDICTION_ACTUALS_PATH = os.getenv(
    "NYC_TAXI_GOLD_PREDICTION_ACTUALS_PATH",
    "s3a://lakehouse/gold/ml/prediction_actuals",
)
GOLD_MODEL_QUALITY_DAILY_PATH = os.getenv(
    "NYC_TAXI_GOLD_MODEL_QUALITY_DAILY_PATH",
    "s3a://lakehouse/gold/monitoring/model_quality_daily",
)

SPARK_FEATURE_DRIVER_MEMORY = os.getenv("NYC_TAXI_MLOPS_FEATURE_DRIVER_MEMORY", "2g")
SPARK_FEATURE_EXECUTOR_MEMORY = os.getenv("NYC_TAXI_MLOPS_FEATURE_EXECUTOR_MEMORY", "4g")
SPARK_TRAIN_DRIVER_MEMORY = os.getenv("NYC_TAXI_MLOPS_DRIVER_MEMORY", "2g")
SPARK_TRAIN_EXECUTOR_MEMORY = os.getenv("NYC_TAXI_MLOPS_EXECUTOR_MEMORY", "6g")
SPARK_TRAIN_DRIVER_MEMORY_OVERHEAD = os.getenv("NYC_TAXI_MLOPS_DRIVER_MEMORY_OVERHEAD", "1g")
SPARK_TRAIN_EXECUTOR_MEMORY_OVERHEAD = os.getenv("NYC_TAXI_MLOPS_EXECUTOR_MEMORY_OVERHEAD", "2g")
SPARK_FEATURE_EXECUTOR_INSTANCES = os.getenv("NYC_TAXI_MLOPS_FEATURE_EXECUTOR_INSTANCES", "1")
SPARK_TRAIN_EXECUTOR_INSTANCES = os.getenv("NYC_TAXI_MLOPS_EXECUTOR_INSTANCES", "3")
SPARK_FEATURE_EXECUTOR_CORES = os.getenv("NYC_TAXI_MLOPS_FEATURE_EXECUTOR_CORES", "1")
SPARK_TRAIN_EXECUTOR_CORES = os.getenv("NYC_TAXI_MLOPS_EXECUTOR_CORES", "2")
SPARK_SHUFFLE_PARTITIONS = os.getenv("NYC_TAXI_MLOPS_SHUFFLE_PARTITIONS", "6")
SPARK_DRIVER_DELETE_ON_TERMINATION = os.getenv(
    "NYC_TAXI_SPARK_DRIVER_DELETE_ON_TERMINATION",
    "true",
)
SPARK_EXECUTOR_DELETE_ON_TERMINATION = os.getenv(
    "NYC_TAXI_SPARK_EXECUTOR_DELETE_ON_TERMINATION",
    "true",
)
XGB_NUM_WORKERS = os.getenv("NYC_TAXI_XGB_NUM_WORKERS", "1")

MIN_TEST_ROWS = int(os.getenv("NYC_TAXI_MIN_TEST_ROWS", "10000"))
MAX_R2_DROP = float(os.getenv("NYC_TAXI_MAX_R2_DROP", "0.03"))
MAX_RMSE_RATIO = float(os.getenv("NYC_TAXI_MAX_RMSE_RATIO", "1.05"))
MAX_TRAIN_TEST_R2_GAP = float(os.getenv("NYC_TAXI_MAX_TRAIN_TEST_R2_GAP", "0.12"))

FEATURE_COLS = [
    "passenger_count",
    "estimated_trip_distance",
    "estimated_trip_duration_seconds",
    "estimated_speed",
    "pickup_hour",
    "pickup_day_of_week",
    "is_weekend",
    "hour_sin",
    "hour_cos",
    "day_sin",
    "day_cos",
    "distance_manhattan",
    "location_cluster",
    "temporal_cluster",
]

DEFAULT_ARGS = {
    "owner": "nyc-taxi",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def _quote(value: str | int | float) -> str:
    return shlex.quote(str(value))


def spark_submit_command(
    *,
    name: str,
    image: str,
    driver_env: dict[str, str | int | float],
    driver_memory: str,
    executor_memory: str,
    executor_instances: str,
    executor_cores: str,
    driver_memory_overhead: str | None = None,
    executor_memory_overhead: str | None = None,
    use_spark_node_selector: bool = True,
) -> str:
    env_conf = "\n".join(
        f"  --conf spark.kubernetes.driverEnv.{key}={_quote(value)} \\"
        for key, value in driver_env.items()
    )
    node_selector_conf = (
        "  --conf spark.kubernetes.driver.node.selector.workload=spark \\\n"
        "  --conf spark.kubernetes.executor.node.selector.workload=spark \\\n"
        if use_spark_node_selector
        else ""
    )
    overhead_conf = ""
    if driver_memory_overhead:
        overhead_conf += f"  --conf spark.driver.memoryOverhead={_quote(driver_memory_overhead)} \\\n"
    if executor_memory_overhead:
        overhead_conf += f"  --conf spark.executor.memoryOverhead={_quote(executor_memory_overhead)} \\\n"

    return f"""
set -euo pipefail

/opt/spark/bin/spark-submit \\
  --master {SPARK_MASTER} \\
  --deploy-mode cluster \\
  --name {name} \\
  --conf spark.kubernetes.namespace={NAMESPACE} \\
{node_selector_conf}\
  --conf spark.kubernetes.container.image={image} \\
  --conf spark.kubernetes.container.image.pullPolicy={SPARK_IMAGE_PULL_POLICY} \\
  --conf spark.kubernetes.authenticate.driver.serviceAccountName={SERVICE_ACCOUNT} \\
  --conf spark.kubernetes.submission.waitAppCompletion=true \\
  --conf spark.kubernetes.driver.deleteOnTermination={SPARK_DRIVER_DELETE_ON_TERMINATION} \\
  --conf spark.kubernetes.executor.deleteOnTermination={SPARK_EXECUTOR_DELETE_ON_TERMINATION} \\
  --conf spark.executor.instances={executor_instances} \\
  --conf spark.executor.cores={executor_cores} \\
  --conf spark.executor.memory={executor_memory} \\
  --conf spark.driver.memory={driver_memory} \\
{overhead_conf}\
  --conf spark.sql.shuffle.partitions={SPARK_SHUFFLE_PARTITIONS} \\
  --conf spark.sql.adaptive.enabled=true \\
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \\
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \\
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \\
  --conf spark.hadoop.fs.s3a.endpoint={MINIO_ENDPOINT} \\
  --conf spark.hadoop.fs.s3a.access.key={MINIO_ACCESS_KEY} \\
  --conf spark.hadoop.fs.s3a.secret.key={MINIO_SECRET_KEY} \\
  --conf spark.hadoop.fs.s3a.path.style.access=true \\
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \\
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \\
  --conf spark.hadoop.fs.s3a.attempts.maximum=3 \\
  --conf spark.kubernetes.driverEnv.PYTHONPATH=/opt/spark/work-dir \\
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/work-dir \\
{env_conf}
  local:///opt/spark/work-dir/app/main.py
"""


def _mlflow_client() -> MlflowClient:
    os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL", MLFLOW_S3_ENDPOINT_URL)
    os.environ.setdefault("AWS_ACCESS_KEY_ID", MINIO_ACCESS_KEY)
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", MINIO_SECRET_KEY)
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    return MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)


def _latest_candidate_run(client: MlflowClient, airflow_run_id: str):
    experiment = client.get_experiment_by_name(EXPERIMENT_NAME)
    if experiment is None:
        raise RuntimeError(f"MLflow experiment not found: {EXPERIMENT_NAME}")

    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        filter_string=(
            f"tags.airflow_dag_id = 'nyc_taxi_mlops_retrain' "
            f"and tags.airflow_run_id = '{airflow_run_id}'"
        ),
        order_by=["attributes.start_time DESC"],
        max_results=1,
    )
    if not runs:
        raise RuntimeError(f"No candidate MLflow run found for Airflow run_id={airflow_run_id}")
    return runs[0]


def _model_version_for_run(client: MlflowClient, run_id: str) -> str:
    latest = None
    for _ in range(12):
        versions = client.search_model_versions(f"name='{MODEL_NAME}' and run_id='{run_id}'")
        if versions:
            latest = max(versions, key=lambda version: int(version.version))
            if getattr(latest, "status", "READY") == "READY":
                return str(latest.version)
        time.sleep(5)

    if latest is not None:
        raise RuntimeError(
            f"Registered model version for run_id={run_id} is not READY "
            f"(status={getattr(latest, 'status', 'unknown')})."
        )
    raise RuntimeError(f"No registered model version found for run_id={run_id}")


def _production_context(client: MlflowClient) -> dict:
    versions = client.get_latest_versions(MODEL_NAME, stages=["Production"])
    if not versions:
        return {
            "current_version": None,
            "current_r2": None,
            "current_rmse": None,
            "has_production_model": False,
        }

    current = versions[0]
    run = client.get_run(current.run_id)
    return {
        "current_version": str(current.version),
        "current_r2": run.data.metrics.get("test_r2"),
        "current_rmse": run.data.metrics.get("test_rmse"),
        "has_production_model": True,
    }


def _smoke_test_candidate(candidate_version: str) -> bool:
    sample = pd.DataFrame(
        [
            {
                "passenger_count": 1,
                "estimated_trip_distance": 3.2,
                "estimated_trip_duration_seconds": 900.0,
                "estimated_speed": 12.8,
                "pickup_hour": 14,
                "pickup_day_of_week": 2,
                "is_weekend": 0,
                "hour_sin": -0.5,
                "hour_cos": -0.8660254038,
                "day_sin": 0.9749279122,
                "day_cos": -0.2225209340,
                "distance_manhattan": 75.0,
                "location_cluster": 2,
                "temporal_cluster": 2,
            }
        ],
        columns=FEATURE_COLS,
    )

    try:
        model = mlflow.xgboost.load_model(f"models:/{MODEL_NAME}/{candidate_version}")
        prediction = model.predict(sample)
        predicted_value = float(prediction[0])
        return math.isfinite(predicted_value)
    except Exception as exc:
        print(f"[mlops_retrain] Candidate smoke test failed: {exc}")
        return False


def collect_candidate_metrics(**context) -> dict:
    client = _mlflow_client()
    airflow_run_id = context["run_id"]
    candidate_run = _latest_candidate_run(client, airflow_run_id)
    candidate_version = _model_version_for_run(client, candidate_run.info.run_id)
    production = _production_context(client)

    metrics = candidate_run.data.metrics
    params = candidate_run.data.params
    candidate_r2 = metrics.get("test_r2")
    candidate_rmse = metrics.get("test_rmse")
    train_r2 = metrics.get("train_r2")
    test_rows = int(float(params.get("test_size", 0)))
    smoke_test_passed = _smoke_test_candidate(candidate_version)

    required = {
        "candidate_r2": candidate_r2,
        "candidate_rmse": candidate_rmse,
        "train_r2": train_r2,
    }
    missing = [name for name, value in required.items() if value is None]
    if missing:
        raise RuntimeError(f"Candidate run is missing required metrics: {missing}")

    has_production_model = production["has_production_model"]
    current_r2 = production["current_r2"]
    current_rmse = production["current_rmse"]
    if has_production_model and (current_r2 is None or current_rmse is None):
        raise RuntimeError(
            "Current Production model is missing test_r2/test_rmse metrics; "
            "refusing automated promotion."
        )

    r2_gate = True if not has_production_model else candidate_r2 >= current_r2 - MAX_R2_DROP
    rmse_gate = True if not has_production_model else candidate_rmse <= current_rmse * MAX_RMSE_RATIO
    overfit_gate = abs(train_r2 - candidate_r2) <= MAX_TRAIN_TEST_R2_GAP
    rows_gate = test_rows >= MIN_TEST_ROWS
    smoke_gate = smoke_test_passed is True

    gate_results = {
        "candidate_r2_vs_current": r2_gate,
        "candidate_rmse_vs_current": rmse_gate,
        "train_test_r2_gap": overfit_gate,
        "test_rows": rows_gate,
        "smoke_test": smoke_gate,
    }
    passed = all(gate_results.values())

    result = {
        **production,
        "candidate_run_id": candidate_run.info.run_id,
        "candidate_version": candidate_version,
        "candidate_r2": candidate_r2,
        "candidate_rmse": candidate_rmse,
        "train_r2": train_r2,
        "test_r2": candidate_r2,
        "test_rows": test_rows,
        "smoke_test_passed": smoke_test_passed,
        "max_r2_drop": MAX_R2_DROP,
        "max_rmse_ratio": MAX_RMSE_RATIO,
        "max_train_test_r2_gap": MAX_TRAIN_TEST_R2_GAP,
        "min_test_rows": MIN_TEST_ROWS,
        "gate_results": gate_results,
        "passed": passed,
    }
    print(f"[mlops_retrain] Candidate evaluation: {result}")
    return result


def choose_promotion_path(**context) -> str:
    evaluation = context["ti"].xcom_pull(task_ids="collect_candidate_metrics")
    if evaluation["passed"]:
        return "promote_candidate_to_production"
    return "skip_promotion"


def promote_candidate_to_production(**context) -> None:
    evaluation = context["ti"].xcom_pull(task_ids="collect_candidate_metrics")
    client = _mlflow_client()
    version = evaluation["candidate_version"]
    client.set_model_version_tag(MODEL_NAME, version, "promotion_gate", "passed")
    client.set_model_version_tag(MODEL_NAME, version, "airflow_run_id", context["run_id"])
    client.transition_model_version_stage(
        name=MODEL_NAME,
        version=version,
        stage="Production",
        archive_existing_versions=True,
    )
    print(f"[mlops_retrain] Promoted {MODEL_NAME} v{version} to Production.")


def mark_candidate_not_promoted(**context) -> None:
    evaluation = context["ti"].xcom_pull(task_ids="collect_candidate_metrics")
    client = _mlflow_client()
    version = evaluation["candidate_version"]
    failed_gates = [
        name
        for name, passed in evaluation["gate_results"].items()
        if not passed
    ]
    client.set_model_version_tag(MODEL_NAME, version, "promotion_gate", "failed")
    client.set_model_version_tag(MODEL_NAME, version, "failed_gates", ",".join(failed_gates))
    client.set_model_version_tag(MODEL_NAME, version, "airflow_run_id", context["run_id"])
    print(f"[mlops_retrain] Candidate {MODEL_NAME} v{version} not promoted. Failed gates: {failed_gates}")


feature_env = {
    "MINIO_ENDPOINT": MINIO_ENDPOINT,
    "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
    "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
    "SILVER_COMPLETED_PATH": SILVER_COMPLETED_PATH,
    "GOLD_ROUTE_ESTIMATES_PATH": GOLD_ROUTE_ESTIMATES_PATH,
    "GOLD_FEATURES_PATH": GOLD_FEATURES_PATH,
    "GOLD_PREDICTIONS_PATH": GOLD_PREDICTIONS_PATH,
    "GOLD_PREDICTION_ACTUALS_PATH": GOLD_PREDICTION_ACTUALS_PATH,
    "GOLD_MODEL_QUALITY_DAILY_PATH": GOLD_MODEL_QUALITY_DAILY_PATH,
    "WRITE_MODE": "overwrite",
}

training_env = {
    "MINIO_ENDPOINT": MINIO_ENDPOINT,
    "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
    "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
    "MLFLOW_TRACKING_URI": MLFLOW_TRACKING_URI,
    "MLFLOW_S3_ENDPOINT_URL": MLFLOW_S3_ENDPOINT_URL,
    "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
    "EXPERIMENT_NAME": EXPERIMENT_NAME,
    "MODEL_NAME": MODEL_NAME,
    "GOLD_FEATURES_PATH": GOLD_FEATURES_PATH,
    "XGB_NUM_WORKERS": XGB_NUM_WORKERS,
    "AUTO_PROMOTE": "false",
    "TRAINING_PIPELINE": "airflow_mlops_retrain",
    "AIRFLOW_DAG_ID": "nyc_taxi_mlops_retrain",
    "AIRFLOW_RUN_ID": "{{ run_id }}",
    "AIRFLOW_TASK_ID": "train_candidate_model",
    "AIRFLOW_DATA_INTERVAL_START": "{{ data_interval_start.isoformat() }}",
    "AIRFLOW_DATA_INTERVAL_END": "{{ data_interval_end.isoformat() }}",
}


with DAG(
    dag_id="nyc_taxi_mlops_retrain",
    description="Retrain NYC Taxi fare model, gate metrics in Airflow, and promote passing candidates.",
    schedule="30 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["nyc-taxi", "mlops", "retrain", "mlflow", "spark"],
) as dag:
    build_route_estimates = KubernetesPodOperator(
        task_id="build_route_estimates",
        name="nyc-taxi-gold-route-estimates",
        namespace=NAMESPACE,
        image=FEATURE_IMAGE,
        image_pull_policy=SPARK_IMAGE_PULL_POLICY,
        cmds=["/bin/bash", "-lc"],
        arguments=[
            spark_submit_command(
                name="nyc-taxi-gold-route-estimates",
                image=FEATURE_IMAGE,
                driver_env={**feature_env, "GOLD_JOB": "route_estimates"},
                driver_memory=SPARK_FEATURE_DRIVER_MEMORY,
                executor_memory=SPARK_FEATURE_EXECUTOR_MEMORY,
                executor_instances=SPARK_FEATURE_EXECUTOR_INSTANCES,
                executor_cores=SPARK_FEATURE_EXECUTOR_CORES,
            )
        ],
        service_account_name=SERVICE_ACCOUNT,
        in_cluster=True,
        get_logs=True,
        on_finish_action="delete_succeeded_pod",
    )

    build_training_features = KubernetesPodOperator(
        task_id="build_training_features",
        name="nyc-taxi-gold-features",
        namespace=NAMESPACE,
        image=FEATURE_IMAGE,
        image_pull_policy=SPARK_IMAGE_PULL_POLICY,
        cmds=["/bin/bash", "-lc"],
        arguments=[
            spark_submit_command(
                name="nyc-taxi-gold-features",
                image=FEATURE_IMAGE,
                driver_env={**feature_env, "GOLD_JOB": "features"},
                driver_memory=SPARK_FEATURE_DRIVER_MEMORY,
                executor_memory=SPARK_FEATURE_EXECUTOR_MEMORY,
                executor_instances=SPARK_FEATURE_EXECUTOR_INSTANCES,
                executor_cores=SPARK_FEATURE_EXECUTOR_CORES,
            )
        ],
        service_account_name=SERVICE_ACCOUNT,
        in_cluster=True,
        get_logs=True,
        on_finish_action="delete_succeeded_pod",
    )

    train_candidate_model = KubernetesPodOperator(
        task_id="train_candidate_model",
        name="nyc-taxi-train-candidate",
        namespace=NAMESPACE,
        image=TRAIN_IMAGE,
        image_pull_policy=SPARK_IMAGE_PULL_POLICY,
        cmds=["/bin/bash", "-lc"],
        arguments=[
            spark_submit_command(
                name="nyc-taxi-train-candidate",
                image=TRAIN_IMAGE,
                driver_env=training_env,
                driver_memory=SPARK_TRAIN_DRIVER_MEMORY,
                executor_memory=SPARK_TRAIN_EXECUTOR_MEMORY,
                executor_instances=SPARK_TRAIN_EXECUTOR_INSTANCES,
                executor_cores=SPARK_TRAIN_EXECUTOR_CORES,
                driver_memory_overhead=SPARK_TRAIN_DRIVER_MEMORY_OVERHEAD,
                executor_memory_overhead=SPARK_TRAIN_EXECUTOR_MEMORY_OVERHEAD,
                use_spark_node_selector=False,
            )
        ],
        service_account_name=SERVICE_ACCOUNT,
        in_cluster=True,
        get_logs=True,
        on_finish_action="delete_succeeded_pod",
    )

    collect_metrics = PythonOperator(
        task_id="collect_candidate_metrics",
        python_callable=collect_candidate_metrics,
    )

    quality_gate = BranchPythonOperator(
        task_id="quality_gate",
        python_callable=choose_promotion_path,
    )

    promote = PythonOperator(
        task_id="promote_candidate_to_production",
        python_callable=promote_candidate_to_production,
    )

    skip = PythonOperator(
        task_id="skip_promotion",
        python_callable=mark_candidate_not_promoted,
    )

    done = EmptyOperator(
        task_id="done",
        trigger_rule="none_failed_min_one_success",
    )

    (
        build_route_estimates
        >> build_training_features
        >> train_candidate_model
        >> collect_metrics
        >> quality_gate
        >> [promote, skip]
        >> done
    )

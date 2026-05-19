from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


REGISTRY = os.getenv("NYC_TAXI_IMAGE_REGISTRY", "localhost:5000")
IMAGE_TAG = os.getenv("NYC_TAXI_IMAGE_TAG", "v1.0")
SPARK_IMAGE = os.getenv(
    "NYC_TAXI_SILVER_IMAGE",
    f"{REGISTRY}/nyc-taxi-silver-consumer:{IMAGE_TAG}",
)
SPARK_IMAGE_PULL_POLICY = os.getenv("NYC_TAXI_SPARK_IMAGE_PULL_POLICY", "IfNotPresent")

NAMESPACE = os.getenv("NYC_TAXI_SPARK_NAMESPACE", "lakehouse")
SERVICE_ACCOUNT = os.getenv("NYC_TAXI_SPARK_SERVICE_ACCOUNT", "spark-user")
MINIO_ENDPOINT = os.getenv(
    "NYC_TAXI_MINIO_ENDPOINT",
    "http://minio-api.storage.svc.cluster.local:9000",
)
MINIO_ACCESS_KEY = os.getenv("NYC_TAXI_MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("NYC_TAXI_MINIO_SECRET_KEY", "minioadmin")

SILVER_STARTED_PATH = os.getenv(
    "NYC_TAXI_SILVER_STARTED_PATH",
    "s3a://lakehouse/silver/nyc-taxi/trip_started",
)
SILVER_COMPLETED_PATH = os.getenv(
    "NYC_TAXI_SILVER_COMPLETED_PATH",
    "s3a://lakehouse/silver/nyc-taxi/trip_completed",
)
LIFECYCLE_PATH = os.getenv(
    "NYC_TAXI_LIFECYCLE_PATH",
    "s3a://lakehouse/silver/nyc-taxi/trip_lifecycle",
)
LIFECYCLE_OVERLAP_MINUTES = int(os.getenv("NYC_TAXI_LIFECYCLE_OVERLAP_MINUTES", "10"))
LIFECYCLE_TTL_HOURS = int(os.getenv("NYC_TAXI_LIFECYCLE_TTL_HOURS", "48"))

SPARK_MASTER = os.getenv("NYC_TAXI_SPARK_MASTER", "k8s://https://kubernetes.default.svc")
SPARK_DRIVER_MEMORY = os.getenv("NYC_TAXI_LIFECYCLE_DRIVER_MEMORY", "1g")
SPARK_EXECUTOR_MEMORY = os.getenv("NYC_TAXI_LIFECYCLE_EXECUTOR_MEMORY", "2g")
SPARK_EXECUTOR_INSTANCES = os.getenv("NYC_TAXI_LIFECYCLE_EXECUTOR_INSTANCES", "1")
SPARK_SHUFFLE_PARTITIONS = os.getenv("NYC_TAXI_LIFECYCLE_SHUFFLE_PARTITIONS", "4")
SPARK_DRIVER_DELETE_ON_TERMINATION = os.getenv(
    "NYC_TAXI_SPARK_DRIVER_DELETE_ON_TERMINATION",
    "true",
)
SPARK_EXECUTOR_DELETE_ON_TERMINATION = os.getenv(
    "NYC_TAXI_SPARK_EXECUTOR_DELETE_ON_TERMINATION",
    "true",
)

DEFAULT_ARGS = {
    "owner": "nyc-taxi",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def spark_submit_command(silver_job: str, since_template: str = "", until_template: str = "") -> str:
    return f"""
set -euo pipefail

/opt/spark/bin/spark-submit \\
  --master {SPARK_MASTER} \\
  --deploy-mode cluster \\
  --name nyc-taxi-{silver_job} \\
  --conf spark.kubernetes.namespace={NAMESPACE} \\
  --conf spark.kubernetes.driver.node.selector.workload=spark \\
  --conf spark.kubernetes.executor.node.selector.workload=spark \\
  --conf spark.kubernetes.container.image={SPARK_IMAGE} \\
  --conf spark.kubernetes.container.image.pullPolicy={SPARK_IMAGE_PULL_POLICY} \\
  --conf spark.kubernetes.authenticate.driver.serviceAccountName={SERVICE_ACCOUNT} \\
  --conf spark.kubernetes.submission.waitAppCompletion=true \\
  --conf spark.kubernetes.driver.deleteOnTermination={SPARK_DRIVER_DELETE_ON_TERMINATION} \\
  --conf spark.kubernetes.executor.deleteOnTermination={SPARK_EXECUTOR_DELETE_ON_TERMINATION} \\
  --conf spark.executor.instances={SPARK_EXECUTOR_INSTANCES} \\
  --conf spark.executor.memory={SPARK_EXECUTOR_MEMORY} \\
  --conf spark.driver.memory={SPARK_DRIVER_MEMORY} \\
  --conf spark.sql.shuffle.partitions={SPARK_SHUFFLE_PARTITIONS} \\
  --conf spark.sql.adaptive.enabled=true \\
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \\
  --conf spark.kubernetes.driver.request.cores=0.5 \\
  --conf spark.kubernetes.driver.limit.cores=1.5 \\
  --conf spark.kubernetes.executor.request.cores=0.5 \\
  --conf spark.kubernetes.executor.limit.cores=1 \\
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
  --conf spark.kubernetes.driverEnv.SILVER_JOB={silver_job} \\
  --conf spark.kubernetes.driverEnv.PIPELINE_MODE=batch \\
  --conf spark.kubernetes.driverEnv.MINIO_ENDPOINT={MINIO_ENDPOINT} \\
  --conf spark.kubernetes.driverEnv.MINIO_ACCESS_KEY={MINIO_ACCESS_KEY} \\
  --conf spark.kubernetes.driverEnv.MINIO_SECRET_KEY={MINIO_SECRET_KEY} \\
  --conf spark.kubernetes.driverEnv.SILVER_STARTED_PATH={SILVER_STARTED_PATH} \\
  --conf spark.kubernetes.driverEnv.SILVER_COMPLETED_PATH={SILVER_COMPLETED_PATH} \\
  --conf spark.kubernetes.driverEnv.LIFECYCLE_PATH={LIFECYCLE_PATH} \\
  --conf spark.kubernetes.driverEnv.LIFECYCLE_TTL_HOURS={LIFECYCLE_TTL_HOURS} \\
  --conf spark.kubernetes.driverEnv.LIFECYCLE_MERGE_SINCE_TIMESTAMP="{since_template}" \\
  --conf spark.kubernetes.driverEnv.LIFECYCLE_MERGE_UNTIL_TIMESTAMP="{until_template}" \\
  local:///opt/spark/work-dir/app/main.py
"""


LIFECYCLE_SINCE = (
    "{{ (data_interval_start - macros.timedelta(minutes="
    + str(LIFECYCLE_OVERLAP_MINUTES)
    + ")).strftime('%Y-%m-%d %H:%M:%S') }}"
)
LIFECYCLE_UNTIL = "{{ data_interval_end.strftime('%Y-%m-%d %H:%M:%S') }}"


with DAG(
    dag_id="nyc_taxi_lifecycle_merge",
    description="Merge Silver started/completed taxi events into the lifecycle state table.",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["nyc-taxi", "lakehouse", "spark", "delta", "lifecycle"],
) as dag:
    merge_lifecycle = KubernetesPodOperator(
        task_id="submit_lifecycle_merge",
        name="nyc-taxi-lifecycle-merge",
        namespace=NAMESPACE,
        image=SPARK_IMAGE,
        image_pull_policy=SPARK_IMAGE_PULL_POLICY,
        cmds=["/bin/bash", "-lc"],
        arguments=[spark_submit_command("lifecycle", LIFECYCLE_SINCE, LIFECYCLE_UNTIL)],
        service_account_name=SERVICE_ACCOUNT,
        in_cluster=True,
        get_logs=True,
        on_finish_action="delete_succeeded_pod",
    )

    expire_lifecycle = KubernetesPodOperator(
        task_id="submit_expire_lifecycle",
        name="nyc-taxi-lifecycle-expire",
        namespace=NAMESPACE,
        image=SPARK_IMAGE,
        image_pull_policy=SPARK_IMAGE_PULL_POLICY,
        cmds=["/bin/bash", "-lc"],
        arguments=[spark_submit_command("expire")],
        service_account_name=SERVICE_ACCOUNT,
        in_cluster=True,
        get_logs=True,
        on_finish_action="delete_succeeded_pod",
    )

    merge_lifecycle >> expire_lifecycle

#!/usr/bin/env python3
"""
Local pipeline runner — chạy toàn bộ pipeline trực tiếp bằng PySpark local mode.
Kết nối MinIO (port-forwarded 9000) và MLflow (port-forwarded 5000).
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

MINIO_ENDPOINT   = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MLFLOW_URI       = "http://localhost:5000"

DATA_PATH  = "/teamspace/studios/this_studio/NYC_Taxis_BigData_Pipeline/data/yellow_data/2024"
SILVER_PATH = "s3a://silver/trips/"
GOLD_PATH   = "s3a://gold/features/"
PRED_PATH   = "s3a://gold/predictions/"


def build_spark(app_name: str):
    from pyspark.sql import SparkSession

    # Download Delta/S3A jars on first run
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.367")
        .config("spark.sql.extensions",         "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint",              MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",            MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",            MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",     "true")
        .config("spark.hadoop.fs.s3a.impl",                  "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled","false")
        .config("spark.hadoop.fs.s3a.attempts.maximum",      "3")
        .config("spark.driver.memory",                       "10g")
        .config("spark.driver.maxResultSize",                "4g")
        .config("spark.sql.shuffle.partitions",              "4")
        .config("spark.sql.adaptive.enabled",                "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark
# ==========================================================================
# STEP 1 — Batch Ingestion: parquet → Bronze Delta Lake
# ==========================================================================
def step1_batch_ingestion(spark):
    from pyspark.sql.functions import (
        lit, current_timestamp, input_file_name,
        to_date, col
    )
    from datetime import datetime
    import uuid

    print("\n" + "="*60)
    print("STEP 1: Batch Ingestion — parquet → Bronze")
    print("="*60)

    raw_df = spark.read.parquet(DATA_PATH)
    print(f"  Raw rows: {raw_df.count():,}")
    print(f"  Columns: {raw_df.columns[:5]}...")

    # Map NYC TLC 2024 columns → Bronze contract
    from pyspark.sql.functions import monotonically_increasing_id
    bronze_df = (
        raw_df
        .withColumn("event_id",            col("VendorID").cast("string") )
        .withColumn("event_type",          lit("taxi_trip"))
        .withColumn("schema_version",      lit("1.0"))
        .withColumn("source_file",         input_file_name())
        .withColumn("ingest_mode",         lit("batch"))
        .withColumn("ingest_timestamp",    current_timestamp())
        .withColumn("event_time",          col("tpep_pickup_datetime"))
        .withColumn("trip_date",           to_date(col("tpep_pickup_datetime")).cast("string"))
        .withColumn("vendor_id",           col("VendorID").cast("int"))
        .withColumn("pickup_datetime",     col("tpep_pickup_datetime"))
        .withColumn("dropoff_datetime",    col("tpep_dropoff_datetime"))
        .withColumn("passenger_count",     col("passenger_count").cast("long"))
        .withColumn("trip_distance",       col("trip_distance").cast("double"))
        .withColumn("rate_code_id",        col("RatecodeID").cast("long"))
        .withColumn("store_and_fwd_flag",  col("store_and_fwd_flag"))
        .withColumn("pulocation_id",       col("PULocationID").cast("int"))
        .withColumn("dolocation_id",       col("DOLocationID").cast("int"))
        .withColumn("payment_type",        col("payment_type").cast("int"))
        .withColumn("fare_amount",         col("fare_amount").cast("double"))
        .withColumn("extra",               col("extra").cast("double"))
        .withColumn("mta_tax",             col("mta_tax").cast("double"))
        .withColumn("tip_amount",          col("tip_amount").cast("double"))
        .withColumn("tolls_amount",        col("tolls_amount").cast("double"))
        .withColumn("improvement_surcharge", col("improvement_surcharge").cast("double"))
        .withColumn("total_amount",        col("total_amount").cast("double"))
        .withColumn("congestion_surcharge", col("congestion_surcharge").cast("double") if "congestion_surcharge" in raw_df.columns else lit(None).cast("double"))
        .withColumn("airport_fee",         col("Airport_fee").cast("double") if "Airport_fee" in raw_df.columns else lit(None).cast("double"))
        .select(
            "event_id","event_type","schema_version","source_file",
            "ingest_mode","ingest_timestamp","event_time","trip_date",
            "vendor_id","pickup_datetime","dropoff_datetime",
            "passenger_count","trip_distance","rate_code_id","store_and_fwd_flag",
            "pulocation_id","dolocation_id","payment_type",
            "fare_amount","extra","mta_tax","tip_amount","tolls_amount",
            "improvement_surcharge","total_amount","congestion_surcharge","airport_fee"
        )
    )

    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("trip_date")
        .save(BRONZE_PATH)
    )
    cnt = spark.read.format("delta").load(BRONZE_PATH).count()
    print(f"  ✅ Bronze rows written: {cnt:,}")
    return cnt


# ==========================================================================
# STEP 2 — Processing: Bronze → Silver
# ==========================================================================
def step2_bronze_to_silver(spark):
    from pyspark.sql.functions import (
        col, hour, date_format, unix_timestamp, when
    )

    print("\n" + "="*60)
    print("STEP 2: Processing — Bronze → Silver")
    print("="*60)

    bronze_df = spark.read.format("delta").load(BRONZE_PATH)

    silver_df = (
        bronze_df
        .withColumn("trip_hour",  hour(col("pickup_datetime")))
        .withColumn("year_month", date_format(col("pickup_datetime"), "yyyy-MM"))
        .withColumn("trip_duration_seconds",
                    unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime")))
        .withColumn("payment_type_desc",
                    when(col("payment_type") == 1, "credit_card")
                    .when(col("payment_type") == 2, "cash")
                    .when(col("payment_type") == 3, "no_charge")
                    .when(col("payment_type") == 4, "dispute")
                    .otherwise("other"))
        # Quality filters (same as bronze_to_silver/transform.py)
        .filter(col("event_id").isNotNull())
        .filter(col("pickup_datetime").isNotNull())
        .filter(col("dropoff_datetime").isNotNull())
        .filter(col("dropoff_datetime") > col("pickup_datetime"))
        .filter(col("trip_distance").isNotNull() & (col("trip_distance") > 0) & (col("trip_distance") < 1000))
        .filter(col("fare_amount").isNotNull() & (col("fare_amount") >= 0))
        .filter(col("total_amount").isNotNull() & (col("total_amount") >= 0))
        .filter(col("passenger_count").isNotNull() & (col("passenger_count") > 0) & (col("passenger_count") < 10))
    ).select(
        col("event_id").alias("trip_id"), "event_type","schema_version","ingest_mode","ingest_timestamp",
        "event_time","trip_date","trip_hour","year_month",
        "vendor_id","pickup_datetime","dropoff_datetime",
        "passenger_count","trip_distance","rate_code_id","store_and_fwd_flag",
        "pulocation_id","dolocation_id","payment_type","payment_type_desc",
        "fare_amount","extra","mta_tax","tip_amount","tolls_amount",
        "improvement_surcharge","total_amount","congestion_surcharge","airport_fee",
        "trip_duration_seconds"
    )

    (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("year_month")
        .save(SILVER_PATH)
    )
    cnt = spark.read.format("delta").load(SILVER_PATH).count()
    print(f"  ✅ Silver rows written: {cnt:,}")
    return cnt


# ==========================================================================
# STEP 3 — Feature Engineering: Silver → Gold
# ==========================================================================
def step3_feature_engineering(spark):
    import math
    from pyspark.sql.functions import (
        col, hour, dayofweek, sin, cos, lit, when,
        abs as spark_abs
    )

    print("\n" + "="*60)
    print("STEP 3: Feature Engineering — Silver → Gold")
    print("="*60)

    silver_df = (
        spark.read.format("delta").load(SILVER_PATH)
        .filter(col("fare_amount") > 0)
        .filter(col("trip_duration_seconds") > 0)
        .filter(col("trip_distance") > 0)
    )

    two_pi = 2.0 * math.pi
    df = (
        silver_df
        .withColumn("pickup_hour",        hour(col("pickup_datetime")))
        .withColumn("pickup_day_of_week", (dayofweek(col("pickup_datetime")) + 5) % 7)
        .withColumn("is_weekend",         when(col("pickup_day_of_week").isin(5, 6), 1).otherwise(0))
        .withColumn("hour_sin",           sin(col("pickup_hour")        * lit(two_pi / 24.0)))
        .withColumn("hour_cos",           cos(col("pickup_hour")        * lit(two_pi / 24.0)))
        .withColumn("day_sin",            sin(col("pickup_day_of_week") * lit(two_pi / 7.0)))
        .withColumn("day_cos",            cos(col("pickup_day_of_week") * lit(two_pi / 7.0)))
        .withColumn("distance_manhattan", spark_abs(col("dolocation_id").cast("double") - col("pulocation_id").cast("double")))
        .withColumn("speed",              when(col("trip_duration_seconds") > 0,
                                              col("trip_distance") / (col("trip_duration_seconds") / lit(3600.0))).otherwise(lit(0.0)))
        .withColumn("location_cluster",   ((col("pulocation_id") + col("dolocation_id")) % lit(5)).cast("int"))
        .withColumn("temporal_cluster",   (col("pickup_hour") / lit(6)).cast("int"))
    )

    gold_cols = [
        "trip_id","fare_amount",
        "passenger_count","trip_distance","trip_duration_seconds",
        "pulocation_id","dolocation_id",
        "pickup_hour","pickup_day_of_week","is_weekend",
        "hour_sin","hour_cos","day_sin","day_cos",
        "distance_manhattan","speed","location_cluster","temporal_cluster",
        "year_month"
    ]
    gold_df = df.select(*gold_cols)

    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("year_month")
        .save(GOLD_PATH)
    )
    cnt = spark.read.format("delta").load(GOLD_PATH).count()
    print(f"  ✅ Gold rows written: {cnt:,}")
    return cnt


# ==========================================================================
# STEP 4 — Train XGBoost + Log to MLflow
# ==========================================================================
def step4_train_xgboost(spark):
    import numpy as np
    import mlflow
    import mlflow.xgboost
    import xgboost as xgb
    from mlflow.models.signature import infer_signature
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import StandardScaler
    from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

    print("\n" + "="*60)
    print("STEP 4: Train XGBoost → MLflow")
    print("="*60)

    FEATURE_COLS = [
        "passenger_count","trip_distance","trip_duration_seconds","speed",
        "pickup_hour","pickup_day_of_week","is_weekend",
        "hour_sin","hour_cos","day_sin","day_cos",
        "distance_manhattan","location_cluster","temporal_cluster",
    ]
    TARGET_COL = "fare_amount"

    gold_df = spark.read.format("delta").load(GOLD_PATH).select(*FEATURE_COLS, TARGET_COL).dropna()
    # Sample 10% of 41M rows (~4M rows) to avoid OOM on single machine pandas conversion
    pdf = gold_df.sample(fraction=0.1, seed=42).toPandas()
    print(f"  Gold rows (sampled): {len(pdf):,}")

    X = pdf[FEATURE_COLS].fillna(0).replace([float("inf"), float("-inf")], 0)
    y = pdf[TARGET_COL]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    scaler = StandardScaler()
    Xs_train = scaler.fit_transform(X_train)
    Xs_test  = scaler.transform(X_test)

    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment("NYC_Taxi_Fare_Prediction")

    with mlflow.start_run(run_name="XGBoost_local") as run:
        xgb_params = dict(n_estimators=100, max_depth=6, learning_rate=0.1,
                          subsample=0.8, colsample_bytree=0.8,
                          random_state=42, n_jobs=-1, eval_metric="rmse")
        mlflow.log_params(xgb_params)

        model = xgb.XGBRegressor(**xgb_params)
        model.fit(Xs_train, y_train, eval_set=[(Xs_test, y_test)], verbose=False)

        y_pred_test  = model.predict(Xs_test)
        y_pred_train = model.predict(Xs_train)

        test_r2   = float(r2_score(y_test,  y_pred_test))
        test_rmse = float(np.sqrt(mean_squared_error(y_test, y_pred_test)))
        test_mae  = float(mean_absolute_error(y_test, y_pred_test))
        train_r2  = float(r2_score(y_train, y_pred_train))

        mlflow.log_metric("train_r2",  train_r2)
        mlflow.log_metric("test_r2",   test_r2)
        mlflow.log_metric("test_rmse", test_rmse)
        mlflow.log_metric("test_mae",  test_mae)

        sig = infer_signature(Xs_train, y_pred_train)
        mlflow.xgboost.log_model(model, artifact_path="model",
                                 signature=sig,
                                 registered_model_name="XGB_NYC_Fare")

        print(f"  Train R²:  {train_r2:.4f}")
        print(f"  Test  R²:  {test_r2:.4f}")
        print(f"  Test RMSE: {test_rmse:.4f}")
        print(f"  Test MAE:  {test_mae:.4f}")
        print(f"  ✅ MLflow Run ID: {run.info.run_id}")

    # Auto-promote
    client = mlflow.tracking.MlflowClient()
    versions = client.search_model_versions("name='XGB_NYC_Fare'")
    if versions:
        latest = max(versions, key=lambda v: int(v.version))
        client.set_registered_model_alias("XGB_NYC_Fare", "production", latest.version)
        print(f"  ✅ Model v{latest.version} → alias 'production'")

    return test_r2


# ==========================================================================
# MAIN
# ==========================================================================
if __name__ == "__main__":
    print("\n🚀 NYC Taxis BigData Pipeline — Full Local Run")
    print("=" * 60)
    spark = build_spark("NYC-Taxi-FullPipeline")
    try:
        step4_train_xgboost(spark)
        print("\n" + "=" * 60)
        print("✅ ALL STEPS COMPLETE!")
    finally:
        spark.stop()

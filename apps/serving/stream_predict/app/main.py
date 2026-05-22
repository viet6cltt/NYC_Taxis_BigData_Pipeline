"""
Spark Structured Streaming — Real-time Fare Prediction.

Flow:
  silver/trip_started Delta stream
    -> route_estimates lookup
    -> in-memory feature engineering
    -> XGBoost inference
    -> gold/ml/predictions Delta append log
"""

import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from app.config import (
    CHECKPOINT_LOCATION,
    FEATURE_COLS,
    GOLD_PREDICTIONS_PATH,
    GOLD_ROUTE_ESTIMATES_PATH,
    MAX_FILES_PER_TRIGGER,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    PREDICTION_LOG_SAMPLE_ROWS,
    SILVER_STARTED_PATH,
    STARTING_VERSION,
    TRIGGER_INTERVAL,
)
from app.feature_extractor import extract_features
from app.model_loader import get_model_version, load_production_model
from app.predictor import build_output_schema, make_predict_fn

PREDICTION_BATCH_PREFIX = "[stream_predict][prediction_batch]"
PREDICTION_PREFIX = "[stream_predict][prediction]"


def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("StreamPredict-RealTimeFarePrediction")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    hc = spark._jsc.hadoopConfiguration()
    hc.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    hc.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    hc.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    hc.set("fs.s3a.path.style.access", "true")
    hc.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hc.set("fs.s3a.connection.ssl.enabled", "false")
    hc.set("fs.s3a.attempts.maximum", "3")

    return spark


def read_started_stream(spark: SparkSession):
    reader = spark.readStream.format("delta")
    if STARTING_VERSION:
        reader = reader.option("startingVersion", STARTING_VERSION)
    if MAX_FILES_PER_TRIGGER:
        reader = reader.option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
    return reader.load(SILVER_STARTED_PATH)


def emit_json(prefix: str, payload: dict) -> None:
    print(f"{prefix} {json.dumps(payload, sort_keys=True, default=str)}", flush=True)


def log_predictions(predictions_df, batch_id: int, rows_in_batch: int, model_version: str) -> None:
    summary = (
        predictions_df
        .agg(
            F.count("*").alias("prediction_rows"),
            F.avg("predicted_fare_amount").alias("avg_predicted_fare_amount"),
            F.min("predicted_fare_amount").alias("min_predicted_fare_amount"),
            F.max("predicted_fare_amount").alias("max_predicted_fare_amount"),
        )
        .collect()[0]
        .asDict(recursive=True)
    )
    emit_json(
        PREDICTION_BATCH_PREFIX,
        {
            "batch_id": int(batch_id),
            "model_version": model_version,
            "rows_in_batch": int(rows_in_batch),
            **summary,
        },
    )

    if PREDICTION_LOG_SAMPLE_ROWS == 0:
        return

    log_columns = [
        column
        for column in [
            "event_id",
            "trip_id",
            "year_month",
            "pickup_datetime",
            "pulocation_id",
            "dolocation_id",
            "passenger_count",
            "estimate_level",
            "estimated_trip_distance",
            "estimated_trip_duration_seconds",
            "predicted_fare_amount",
            "model_name",
            "model_version",
            "model_stage",
            "prediction_timestamp",
        ]
        if column in predictions_df.columns
    ]
    for row in predictions_df.select(*log_columns).limit(PREDICTION_LOG_SAMPLE_ROWS).collect():
        emit_json(
            PREDICTION_PREFIX,
            {
                "batch_id": int(batch_id),
                **row.asDict(recursive=True),
            },
        )


def main() -> None:
    print("=== [stream_predict] Starting Real-time Fare Prediction ===")
    print(f"[stream_predict] Silver started path: {SILVER_STARTED_PATH}")
    print(f"[stream_predict] Route estimates path: {GOLD_ROUTE_ESTIMATES_PATH}")
    print(f"[stream_predict] Predictions path: {GOLD_PREDICTIONS_PATH}")

    model = load_production_model()
    model_version = get_model_version()
    print(f"[stream_predict] Model version: {model_version}")

    spark = build_spark_session()
    route_estimates_df = spark.read.format("delta").load(GOLD_ROUTE_ESTIMATES_PATH)
    started_df = read_started_stream(spark)
    feature_df = extract_features(started_df, route_estimates_df)
    out_schema = build_output_schema(feature_df.schema)
    predict_fn = make_predict_fn(
        spark.sparkContext.broadcast(model),
        spark.sparkContext.broadcast(FEATURE_COLS),
        spark.sparkContext.broadcast(model_version),
    )

    def process_batch(batch_df, batch_id):
        cached_batch = batch_df.persist()
        try:
            if cached_batch.isEmpty():
                return

            row_count = cached_batch.count()
            print(f"[stream_predict] Batch {batch_id} - {row_count} rows")
            predictions_df = (
                cached_batch.mapInPandas(predict_fn, schema=out_schema)
                .withColumn("prediction_timestamp", F.current_timestamp())
                .persist()
            )
            try:
                (
                    predictions_df
                    .write
                    .format("delta")
                    .mode("append")
                    .option("mergeSchema", "true")
                    .partitionBy("year_month")
                    .save(GOLD_PREDICTIONS_PATH)
                )
                log_predictions(predictions_df, batch_id, row_count, model_version)
            finally:
                predictions_df.unpersist()
        finally:
            cached_batch.unpersist()

    query = (
        feature_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    print(f"[stream_predict] Streaming query started. checkpoint={CHECKPOINT_LOCATION}")
    query.awaitTermination()


if __name__ == "__main__":
    main()

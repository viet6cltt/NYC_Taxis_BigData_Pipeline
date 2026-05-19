"""
Spark Structured Streaming — Real-time Fare Prediction.

Flow:
  silver/trip_started Delta stream
    -> route_estimates lookup
    -> in-memory feature engineering
    -> XGBoost inference
    -> gold/ml/predictions Delta append log
"""

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
    SILVER_STARTED_PATH,
    STARTING_VERSION,
    TRIGGER_INTERVAL,
)
from app.feature_extractor import extract_features
from app.model_loader import get_model_version, load_production_model
from app.predictor import build_output_schema, make_predict_fn


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
            )
            (
                predictions_df
                .write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .partitionBy("year_month")
                .save(GOLD_PREDICTIONS_PATH)
            )
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

"""
Spark Structured Streaming — Real-time Fare Prediction

Flow:
  Kafka (nyc-taxi-trips)
    → Decode Avro
    → Feature extraction (stateless)
    → XGBoost inference (mapInPandas + broadcast)
    → Write predictions to Gold Delta Lake
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col

from app.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    GOLD_PREDICTIONS_PATH,
    CHECKPOINT_LOCATION,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    TRIGGER_INTERVAL,
    FEATURE_COLS,
)
from app.model_loader import load_production_model, get_model_version
from app.feature_extractor import extract_features
from app.predictor import apply_predictions, build_output_schema


AVRO_SCHEMA_PATH = "schemas/taxi_trip_event.avsc"


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
    hc.set("fs.s3a.endpoint",               MINIO_ENDPOINT)
    hc.set("fs.s3a.access.key",             MINIO_ACCESS_KEY)
    hc.set("fs.s3a.secret.key",             MINIO_SECRET_KEY)
    hc.set("fs.s3a.path.style.access",      "true")
    hc.set("fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hc.set("fs.s3a.connection.ssl.enabled", "false")
    hc.set("fs.s3a.attempts.maximum",       "3")

    return spark


def load_avro_schema(path: str) -> str:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Avro schema not found: {path}")
    with open(path, "r") as f:
        return f.read()


def main() -> None:
    print("=== [stream_predict] Starting Real-time Fare Prediction ===")

    # Load model on driver (once)
    model         = load_production_model()
    model_version = get_model_version()
    print(f"[stream_predict] Model version: {model_version}")

    spark = build_spark_session()

    # ------------------------------------------------------------------
    # 1. Read Kafka stream
    # ------------------------------------------------------------------
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe",               KAFKA_TOPIC)
        .option("startingOffsets",         "latest")
        .option("failOnDataLoss",          "false")
        .load()
    )

    # ------------------------------------------------------------------
    # 2. Decode Avro
    # ------------------------------------------------------------------
    avro_schema_str = load_avro_schema(AVRO_SCHEMA_PATH)
    decoded_df = (
        kafka_df
        .select(
            from_avro(col("value"), avro_schema_str, {"mode": "PERMISSIVE"}).alias("event")
        )
        .filter(col("event").isNotNull())
        .select("event.metadata.*", "event.payload.*")
    )

    # ------------------------------------------------------------------
    # 3. Feature extraction (stateless)
    # ------------------------------------------------------------------
    feature_df = extract_features(decoded_df)

    # ------------------------------------------------------------------
    # 4. Build output schema and apply predictions per micro-batch
    # ------------------------------------------------------------------
    out_schema = build_output_schema(feature_df.schema)

    def process_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        print(f"[stream_predict] Batch {batch_id} — {batch_df.count()} rows")
        predictions_df = apply_predictions(batch_df, model, model_version, out_schema)
        (
            predictions_df
            .write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(GOLD_PREDICTIONS_PATH)
        )

    # ------------------------------------------------------------------
    # 5. Start streaming query
    # ------------------------------------------------------------------
    query = (
        feature_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    print(f"[stream_predict] Streaming query started. Writing to {GOLD_PREDICTIONS_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()

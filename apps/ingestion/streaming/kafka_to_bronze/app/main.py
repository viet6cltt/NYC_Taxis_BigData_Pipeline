import os 
import json
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col
from pyspark.sql.streaming import StreamingQueryListener
from app.config import (
    AVRO_SCHEMA_PATH,
    BENCHMARK_METRICS_ENABLED,
    CHECKPOINT_LOCATION,
    EVENT_KIND,
    KAFKA_GROUP_ID,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    MAX_OFFSETS_PER_TRIGGER,
    OUTPUT_PATH,
    TRIGGER_INTERVAL,
)
from app.transform import transform


def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("StreamingToBronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.attempts.maximum", "3")
    
    return spark


class BenchmarkProgressListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(
            json.dumps(
                {
                    "metric_type": "spark_query_started",
                    "query_id": str(event.id),
                    "run_id": str(event.runId),
                    "name": event.name,
                    "event_kind": EVENT_KIND,
                    "topic": KAFKA_TOPIC,
                },
                sort_keys=True,
            )
        )

    def onQueryProgress(self, event):
        progress = json.loads(event.progress.json)
        duration_ms = progress.get("durationMs", {})
        sources = progress.get("sources", [])
        source = sources[0] if sources else {}
        payload = {
            "metric_type": "spark_microbatch_progress",
            "event_kind": EVENT_KIND,
            "topic": KAFKA_TOPIC,
            "kafka_group_id": KAFKA_GROUP_ID,
            "batch_id": progress.get("batchId"),
            "timestamp": progress.get("timestamp"),
            "num_input_rows": progress.get("numInputRows", 0),
            "input_rows_per_second": progress.get("inputRowsPerSecond", 0.0),
            "processed_rows_per_second": progress.get("processedRowsPerSecond", 0.0),
            "trigger_execution_ms": duration_ms.get("triggerExecution", 0),
            "add_batch_ms": duration_ms.get("addBatch", 0),
            "get_batch_ms": duration_ms.get("getBatch", 0),
            "start_offset": source.get("startOffset"),
            "end_offset": source.get("endOffset"),
            "latest_offset": source.get("latestOffset"),
        }
        print("[benchmark][spark_progress] " + json.dumps(payload, sort_keys=True))

    def onQueryTerminated(self, event):
        print(
            json.dumps(
                {
                    "metric_type": "spark_query_terminated",
                    "query_id": str(event.id),
                    "run_id": str(event.runId),
                    "exception": event.exception,
                    "event_kind": EVENT_KIND,
                    "topic": KAFKA_TOPIC,
                },
                sort_keys=True,
            )
        )

    def onQueryIdle(self, event):
        return None


def load_avro_schema(file_path: str) -> str:
    """Đọc file .avsc và trả về chuỗi JSON"""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Không tìm thấy file schema tại: {file_path}")
    with open(file_path, "r") as f:
        return f.read()

def main():
    spark = build_spark_session()
    if BENCHMARK_METRICS_ENABLED:
        spark.streams.addListener(BenchmarkProgressListener())

    # =========================
    # Đọc stream từ Kafka
    # =========================
    print(f"--- Reading {EVENT_KIND} events from kafka topic={KAFKA_TOPIC} ---")
    kafka_reader = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)  
        .option("kafka.group.id", KAFKA_GROUP_ID)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
    )
    if MAX_OFFSETS_PER_TRIGGER:
        kafka_reader = kafka_reader.option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
    kafka_df = kafka_reader.load()
    
    # Read avro schema
    avro_schema_str = load_avro_schema(AVRO_SCHEMA_PATH)
    
    print("--- Decoding avro ---")
    # =========================
    # Decode Avro
    # =========================
    decoded_df = kafka_df.select(
        from_avro(
            col("value"),
            avro_schema_str,
            {"mode": "PERMISSIVE"},
        ).alias("event")
    ).filter(col("event").isNotNull())

    flat_df = decoded_df.select(
        "event.metadata.*",
        "event.payload.*"
    )
    
    bronze_df = transform(flat_df, EVENT_KIND)
    
    print(
        f"--- Writing {EVENT_KIND} events to Bronze path={OUTPUT_PATH}, "
        f"checkpoint={CHECKPOINT_LOCATION} ---"
    )
    # =========================
    # Ghi xuống BRONZE 
    # =========================
    query = (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()

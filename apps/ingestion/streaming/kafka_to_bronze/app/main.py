import os 
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col
from app.config import (
    AVRO_SCHEMA_PATH,
    CHECKPOINT_LOCATION,
    EVENT_KIND,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
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

def load_avro_schema(file_path: str) -> str:
    """Đọc file .avsc và trả về chuỗi JSON"""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Không tìm thấy file schema tại: {file_path}")
    with open(file_path, "r") as f:
        return f.read()

def main():
    spark = build_spark_session()
    # =========================
    # Đọc stream từ Kafka
    # =========================
    print(f"--- Reading {EVENT_KIND} events from kafka topic={KAFKA_TOPIC} ---")
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)  
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    
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


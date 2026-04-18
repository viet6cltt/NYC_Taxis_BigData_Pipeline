from pyspark.sql import SparkSession
import os
from app.config import (
    INPUT_PATH,
    OUTPUT_PATH,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    YEAR,
)
from app.transform import transform
from app.writer import write_to_bronze
from app.config import INPUT_PATH, OUTPUT_PATH, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
from app.transform import transform

def resolve_input_path(base_input_path: str, year: str) -> str:
    return os.path.join(base_input_path, year)


def validate_input_path(input_path: str) -> None:
    if not os.path.isdir(input_path):
        raise FileNotFoundError(f"Input directory not found: {input_path}")

    parquet_files = [name for name in os.listdir(input_path) if name.endswith(".parquet")]
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in: {input_path}")


def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("HistoricalToBronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    hadoop_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.attempts.maximum", "3")
    return spark

def main():
    input_path = resolve_input_path(INPUT_PATH, YEAR)
    validate_input_path(input_path)

    print(f"[batch] Reading historical parquet files from {input_path}")
    print(f"[batch] Writing bronze data to {OUTPUT_PATH}")
        
    # Transform the raw data to bronze format
    spark = build_spark_session()

    try:
        raw_df = spark.read.parquet(input_path)
        bronze_df = transform(raw_df)
        
        # Write the transformed data to the bronze layer
        write_to_bronze(bronze_df, OUTPUT_PATH)
        
    finally:
        spark.stop()
    


if __name__ == "__main__":
    main()
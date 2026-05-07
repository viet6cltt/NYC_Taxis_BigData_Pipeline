from pyspark.sql import SparkSession

from app.config import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
)
from app.reader import read_silver
from app.transform import transform
from app.writer import write_gold


def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("FeatureEngineering-SilverToGold")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    hc = spark._jsc.hadoopConfiguration()
    hc.set("fs.s3a.endpoint",             MINIO_ENDPOINT)
    hc.set("fs.s3a.access.key",           MINIO_ACCESS_KEY)
    hc.set("fs.s3a.secret.key",           MINIO_SECRET_KEY)
    hc.set("fs.s3a.path.style.access",    "true")
    hc.set("fs.s3a.impl",                 "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hc.set("fs.s3a.connection.ssl.enabled", "false")
    hc.set("fs.s3a.attempts.maximum",     "3")

    return spark


def main() -> None:
    print("=== [feature_engineering] Silver → Gold Feature Engineering ===")
    spark = build_spark_session()

    try:
        silver_df = read_silver(spark)
        gold_df   = transform(silver_df)
        write_gold(gold_df)
        print("=== [feature_engineering] Done ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

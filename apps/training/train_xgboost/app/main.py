from pyspark.sql import SparkSession

from app.config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
from app.reader import read_gold
from app.trainer import train_and_log


def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("TrainXGBoost-GoldToMLflow")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    hc = spark._jsc.hadoopConfiguration()
    hc.set("fs.s3a.endpoint",              MINIO_ENDPOINT)
    hc.set("fs.s3a.access.key",            MINIO_ACCESS_KEY)
    hc.set("fs.s3a.secret.key",            MINIO_SECRET_KEY)
    hc.set("fs.s3a.path.style.access",     "true")
    hc.set("fs.s3a.impl",                  "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hc.set("fs.s3a.connection.ssl.enabled","false")
    hc.set("fs.s3a.attempts.maximum",      "3")

    return spark


def main() -> None:
    print("=== [train_xgboost] Gold → XGBoost Training → MLflow ===")
    spark = build_spark_session()

    try:
        gold_df = read_gold(spark)
        train_and_log(gold_df)
        print("=== [train_xgboost] Done ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

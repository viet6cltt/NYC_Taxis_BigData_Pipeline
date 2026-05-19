from pyspark.sql import SparkSession

from app.config import (
    GOLD_JOB,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
)
from app.benchmark import benchmark_job
from app.reader import (
    read_prediction_actuals,
    read_predictions,
    read_route_estimates,
    read_silver_completed,
)
from app.transform import (
    build_model_quality_daily,
    build_prediction_actuals,
    build_route_estimates,
    build_training_features,
)
from app.writer import (
    write_features,
    write_model_quality_daily,
    write_prediction_actuals,
    write_route_estimates,
)


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
    print(f"=== [feature_engineering] Gold job: {GOLD_JOB} ===")
    spark = build_spark_session()

    try:
        with benchmark_job(
            prefix="[benchmark][spark_batch_job]",
            job_name=f"gold_{GOLD_JOB}",
            extra={"gold_job": GOLD_JOB},
        ):
            if GOLD_JOB == "route_estimates":
                completed_df = read_silver_completed(spark)
                route_estimates_df = build_route_estimates(completed_df)
                write_route_estimates(route_estimates_df)

            elif GOLD_JOB == "features":
                completed_df = read_silver_completed(spark)
                route_estimates_df = read_route_estimates(spark)
                features_df = build_training_features(completed_df, route_estimates_df)
                write_features(features_df)

            elif GOLD_JOB == "prediction_actuals":
                predictions_df = read_predictions(spark)
                completed_df = read_silver_completed(spark)
                prediction_actuals_df = build_prediction_actuals(predictions_df, completed_df)
                write_prediction_actuals(prediction_actuals_df)

            elif GOLD_JOB == "model_quality_daily":
                prediction_actuals_df = read_prediction_actuals(spark)
                quality_df = build_model_quality_daily(prediction_actuals_df)
                write_model_quality_daily(quality_df)

            else:
                raise ValueError(f"Unsupported GOLD_JOB: {GOLD_JOB}")

        print("=== [feature_engineering] Done ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

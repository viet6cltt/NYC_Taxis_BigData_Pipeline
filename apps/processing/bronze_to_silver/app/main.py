from app.config import PIPELINE_MODE, SILVER_JOB
from app.benchmark import benchmark_job
from app.reader import read_bronze_batch, read_bronze_streaming
from app.spark_session import build_spark_session
from app.transform import transform
from app.writer import (
    expire_lifecycle,
    merge_lifecycle_from_silver,
    write_clean_batch,
    write_clean_streaming,
)


def main() -> None:
    spark = build_spark_session(PIPELINE_MODE)

    try:
        if SILVER_JOB == "expire":
            with benchmark_job(
                prefix="[benchmark][spark_batch_job]",
                job_name="silver_lifecycle_expire",
                extra={"pipeline_mode": PIPELINE_MODE, "silver_job": SILVER_JOB},
            ):
                expire_lifecycle(spark)
            return

        if SILVER_JOB == "lifecycle":
            with benchmark_job(
                prefix="[benchmark][spark_batch_job]",
                job_name="silver_lifecycle_merge",
                extra={"pipeline_mode": PIPELINE_MODE, "silver_job": SILVER_JOB},
            ):
                merge_lifecycle_from_silver(spark)
            return

        if PIPELINE_MODE == "batch":
            with benchmark_job(
                prefix="[benchmark][spark_batch_job]",
                job_name=f"bronze_to_silver_{SILVER_JOB}",
                extra={"pipeline_mode": PIPELINE_MODE, "silver_job": SILVER_JOB},
            ):
                bronze_df = read_bronze_batch(spark)
                clean_df = transform(bronze_df, PIPELINE_MODE, SILVER_JOB)
                write_clean_batch(clean_df, SILVER_JOB)
            return

        bronze_df = read_bronze_streaming(spark)
        clean_df = transform(bronze_df, PIPELINE_MODE, SILVER_JOB)
        write_clean_streaming(clean_df, SILVER_JOB)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

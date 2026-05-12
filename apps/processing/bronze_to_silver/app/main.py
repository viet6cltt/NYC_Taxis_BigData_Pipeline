from app.config import LIFECYCLE_MERGE_ENABLED, PIPELINE_MODE, SILVER_JOB
from app.reader import read_bronze_batch, read_bronze_streaming
from app.spark_session import build_spark_session
from app.transform import transform
from app.writer import (
    ensure_lifecycle_table,
    expire_lifecycle,
    write_clean_and_lifecycle_batch,
    write_clean_and_lifecycle_streaming,
)


def main() -> None:
    spark = build_spark_session(PIPELINE_MODE)

    if SILVER_JOB == "expire":
        expire_lifecycle(spark)
        return

    if LIFECYCLE_MERGE_ENABLED:
        ensure_lifecycle_table(spark)
    if PIPELINE_MODE == "batch":
        bronze_df = read_bronze_batch(spark)
    else:
        bronze_df = read_bronze_streaming(spark)

    clean_df = transform(bronze_df, PIPELINE_MODE, SILVER_JOB)

    if PIPELINE_MODE == "batch":
        write_clean_and_lifecycle_batch(clean_df, SILVER_JOB)
    else:
        write_clean_and_lifecycle_streaming(clean_df, SILVER_JOB)


if __name__ == "__main__":
    main()

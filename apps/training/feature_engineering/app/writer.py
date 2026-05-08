from pyspark.sql import DataFrame
from app.config import GOLD_FEATURES_PATH, WRITE_MODE


def write_gold(df: DataFrame) -> None:
    """Write Gold feature table to Delta Lake, partitioned by year_month."""
    print(f"[feature_engineering] Writing Gold features to: {GOLD_FEATURES_PATH}")
    (
        df.write
          .format("delta")
          .mode(WRITE_MODE)
          .partitionBy("year_month")
          .save(GOLD_FEATURES_PATH)
    )
    print("[feature_engineering] Gold write complete.")

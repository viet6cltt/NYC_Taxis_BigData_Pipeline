from pyspark.sql import SparkSession, DataFrame
from app.config import SILVER_PATH


def read_silver(spark: SparkSession) -> DataFrame:
    """Read cleaned Silver Delta Lake table."""
    print(f"[feature_engineering] Reading Silver data from: {SILVER_PATH}")
    df = spark.read.format("delta").load(SILVER_PATH)
    print(f"[feature_engineering] Silver row count: {df.count()}")
    return df

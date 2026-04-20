from app.config import BRONZE_PATH
from pyspark.sql import SparkSession

def read_from_bronze(spark: SparkSession) -> SparkSession:
    return (
        spark.readStream
        .format("delta")
        .load(BRONZE_PATH)
    )
    
from app.config import BRONZE_PATH, STARTING_VERSION
from pyspark.sql import SparkSession

def read_bronze_batch(spark: SparkSession) -> SparkSession:
    return (
        spark.read
        .format("delta")
        .load(BRONZE_PATH)
    )
    
def read_bronze_streaming(spark: SparkSession) -> SparkSession:
    reader = spark.readStream.format("delta")
    if STARTING_VERSION:
        reader = reader.option("startingVersion", STARTING_VERSION)
    return reader.load(BRONZE_PATH)

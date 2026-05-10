from app.config import INPUT_PATH, STARTING_VERSION
from pyspark.sql import DataFrame, SparkSession

def read_bronze_batch(spark: SparkSession) -> DataFrame:
    return (
        spark.read
        .format("delta")
        .load(INPUT_PATH)
    )
    
def read_bronze_streaming(spark: SparkSession) -> DataFrame:
    reader = spark.readStream.format("delta")
    if STARTING_VERSION:
        reader = reader.option("startingVersion", STARTING_VERSION)
    return reader.load(INPUT_PATH)

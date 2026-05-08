from app.config import SILVER_CHECKPOINT_PATH, SILVER_PATH, TRIGGER_INTERVAL
from pyspark.sql import DataFrame

def write_to_silver_batch(df: DataFrame) -> None:
    (
        df.write
        .format("delta")
        .mode("append")
        .partitionBy("year_month")
        .save(SILVER_PATH)
    )
    
def write_to_silver_streaming(df: DataFrame) -> None:
    query = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", SILVER_CHECKPOINT_PATH)
        .option("optimizeWrite", "true")
        .option("path", SILVER_PATH)
        .partitionBy("year_month")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    
    query.awaitTermination()
    

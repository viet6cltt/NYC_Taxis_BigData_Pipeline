from app.config import SILVER_CHECKPOINT_PATH, SILVER_PATH, TRIGGER_INTERVAL
from pyspark.sql import DataFrame

def write_to_silver(df: DataFrame) -> None:
    query = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("path", SILVER_PATH)
        .partitionBy("year_month")
        .option("checkpointLocation", SILVER_CHECKPOINT_PATH)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    
    query.awaitTermination()
    
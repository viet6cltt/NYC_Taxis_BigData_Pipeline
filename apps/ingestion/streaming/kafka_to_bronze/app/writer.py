from pyspark.sql import DataFrame

def write_to_bronze(df: DataFrame, ouput_path: str, checkpoint_location: str) -> None:
    (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("path", ouput_path)
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime="10 seconds")
        .start()
    )    
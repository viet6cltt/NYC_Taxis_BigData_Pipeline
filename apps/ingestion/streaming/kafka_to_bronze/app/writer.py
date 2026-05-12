from pyspark.sql import DataFrame

def write_to_bronze(df: DataFrame, output_path: str, checkpoint_location: str):
    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime="10 seconds")
        .start()
    )

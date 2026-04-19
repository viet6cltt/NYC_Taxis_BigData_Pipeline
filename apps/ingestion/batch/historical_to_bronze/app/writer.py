from pyspark.sql import DataFrame


def write_to_bronze(df: DataFrame, output_path: str) -> None:
    (
        df.write
        .format("delta")
        .mode("append")
        .save(output_path)
    )
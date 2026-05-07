from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, lit, when

from app.config import GOLD_FEATURES_PATH, FEATURE_COLS, TARGET_COL


def read_gold(spark: SparkSession) -> DataFrame:
    """
    Read Gold feature table from Delta Lake.
    Returns a Spark DataFrame so training can stay distributed.
    """
    print(f"[train_xgboost] Reading Gold features from: {GOLD_FEATURES_PATH}")
    spark_df = spark.read.format("delta").load(GOLD_FEATURES_PATH)

    cols_needed = FEATURE_COLS + [TARGET_COL]
    selected_df = spark_df.select(*cols_needed)

    cleaned_df = selected_df.dropna(subset=[TARGET_COL])
    for name in cols_needed:
        value = col(name).cast("double")
        cleaned_df = cleaned_df.withColumn(
            name,
            when(
                value.isNull()
                | isnan(value)
                | (value == lit(float("inf")))
                | (value == lit(float("-inf"))),
                lit(0.0),
            ).otherwise(value),
        )

    row_count = cleaned_df.count()
    print(f"[train_xgboost] Gold rows loaded: {row_count:,}")
    return cleaned_df

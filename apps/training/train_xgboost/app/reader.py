import pandas as pd
from pyspark.sql import SparkSession
from app.config import GOLD_FEATURES_PATH, FEATURE_COLS, TARGET_COL


def read_gold(spark: SparkSession) -> pd.DataFrame:
    """
    Read Gold feature table from Delta Lake.
    Returns a Pandas DataFrame (training happens with sklearn/xgb, not Spark ML).
    """
    print(f"[train_xgboost] Reading Gold features from: {GOLD_FEATURES_PATH}")
    spark_df = spark.read.format("delta").load(GOLD_FEATURES_PATH)

    # Keep only feature + target columns; drop NaNs
    cols_needed = FEATURE_COLS + [TARGET_COL]
    spark_df = spark_df.select(*cols_needed).dropna()

    pdf = spark_df.toPandas()
    pdf[FEATURE_COLS] = pdf[FEATURE_COLS].replace([float("inf"), float("-inf")], 0).fillna(0)

    print(f"[train_xgboost] Gold rows loaded: {len(pdf):,}")
    return pdf

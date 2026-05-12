from pyspark.sql import SparkSession, DataFrame
from app.config import (
    GOLD_PREDICTIONS_PATH,
    GOLD_PREDICTION_ACTUALS_PATH,
    GOLD_ROUTE_ESTIMATES_PATH,
    SILVER_COMPLETED_PATH,
)


def _read_delta(spark: SparkSession, path: str, label: str) -> DataFrame:
    print(f"[feature_engineering] Reading {label} from: {path}")
    df = spark.read.format("delta").load(path)
    print(f"[feature_engineering] {label} row count: {df.count():,}")
    return df


def read_silver_completed(spark: SparkSession) -> DataFrame:
    return _read_delta(spark, SILVER_COMPLETED_PATH, "Silver completed")


def read_route_estimates(spark: SparkSession) -> DataFrame:
    return _read_delta(spark, GOLD_ROUTE_ESTIMATES_PATH, "Gold route estimates")


def read_predictions(spark: SparkSession) -> DataFrame:
    return _read_delta(spark, GOLD_PREDICTIONS_PATH, "Gold predictions")


def read_prediction_actuals(spark: SparkSession) -> DataFrame:
    return _read_delta(spark, GOLD_PREDICTION_ACTUALS_PATH, "Gold prediction_actuals")

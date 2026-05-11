from pyspark.sql import DataFrame
from app.config import (
    GOLD_FEATURES_PATH,
    GOLD_MODEL_QUALITY_DAILY_PATH,
    GOLD_PREDICTION_ACTUALS_PATH,
    GOLD_ROUTE_ESTIMATES_PATH,
    WRITE_MODE,
)


def _write_delta(df: DataFrame, path: str, label: str, partition_cols: list[str] | None = None) -> None:
    print(f"[feature_engineering] Writing {label} to: {path}")
    writer = (
        df.write
        .format("delta")
        .mode(WRITE_MODE)
    )
    if WRITE_MODE == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)
    print(f"[feature_engineering] {label} write complete.")


def write_route_estimates(df: DataFrame) -> None:
    _write_delta(df, GOLD_ROUTE_ESTIMATES_PATH, "Gold route_estimates")


def write_features(df: DataFrame) -> None:
    _write_delta(df, GOLD_FEATURES_PATH, "Gold features", ["year_month"])


def write_prediction_actuals(df: DataFrame) -> None:
    _write_delta(df, GOLD_PREDICTION_ACTUALS_PATH, "Gold prediction_actuals", ["year_month"])


def write_model_quality_daily(df: DataFrame) -> None:
    _write_delta(df, GOLD_MODEL_QUALITY_DAILY_PATH, "Gold model_quality_daily")

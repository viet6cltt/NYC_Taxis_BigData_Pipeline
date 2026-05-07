"""
Batch inference via mapInPandas.
Model is loaded once on the driver, broadcast to executors.
"""

import pandas as pd
import numpy as np
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType

from app.config import FEATURE_COLS, MODEL_NAME, MODEL_STAGE


def make_predict_fn(bc_model, bc_feature_cols, bc_model_version):
    """
    Returns a function compatible with mapInPandas.
    Each partition receives an iterator of Pandas DataFrames.
    """
    def _predict(iterator):
        model         = bc_model.value
        feature_cols  = bc_feature_cols.value
        model_version = bc_model_version.value

        for pdf in iterator:
            X = pdf[feature_cols].fillna(0).replace([np.inf, -np.inf], 0)
            pdf["predicted_fare"]  = model.predict(X).astype(float)
            pdf["model_name"]      = MODEL_NAME
            pdf["model_version"]   = model_version
            yield pdf

    return _predict


def apply_predictions(
    df: DataFrame,
    model,
    model_version: str,
    out_schema: StructType,
) -> DataFrame:
    """
    Apply broadcast XGBoost model predictions across all partitions.

    Args:
        df:            Spark DataFrame with all FEATURE_COLS present.
        model:         Loaded XGBoost model.
        model_version: Version string for lineage.
        out_schema:    Full output StructType (input schema + prediction columns).
    """
    sc = df.sparkSession.sparkContext
    bc_model         = sc.broadcast(model)
    bc_feature_cols  = sc.broadcast(FEATURE_COLS)
    bc_model_version = sc.broadcast(model_version)

    predict_fn = make_predict_fn(bc_model, bc_feature_cols, bc_model_version)
    return df.mapInPandas(predict_fn, schema=out_schema)


def build_output_schema(input_schema: StructType) -> StructType:
    """Extend input schema with prediction output columns."""
    return (
        input_schema
        .add(StructField("predicted_fare",  DoubleType(), True))
        .add(StructField("model_name",      StringType(), True))
        .add(StructField("model_version",   StringType(), True))
    )

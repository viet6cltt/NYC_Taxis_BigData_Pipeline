"""
Load XGBoost model from MLflow Model Registry.
Returns the unwrapped xgboost.Booster-compatible model for use inside
Spark executors via broadcast.
"""

import mlflow
import mlflow.xgboost
import xgboost as xgb

from app.config import MLFLOW_TRACKING_URI, MODEL_NAME, MODEL_STAGE


def load_production_model():
    """
    Pull the 'Production' stage model from MLflow Registry.
    Returns an mlflow.pyfunc model wrapper — supports .predict(pd.DataFrame).
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    model_uri = f"models:/{MODEL_NAME}/{MODEL_STAGE}"
    print(f"[stream_predict] Loading model from: {model_uri}")
    print(f"[stream_predict] XGBoost runtime version: {xgb.__version__}")
    model = mlflow.xgboost.load_model(model_uri)
    print(f"[stream_predict] Model loaded OK — {MODEL_NAME}@{MODEL_STAGE}")
    return model


def get_model_version() -> str:
    """Return the current Production model version string."""
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = mlflow.tracking.MlflowClient()
    versions = client.get_latest_versions(MODEL_NAME, stages=[MODEL_STAGE])
    if versions:
        return versions[0].version
    return "unknown"

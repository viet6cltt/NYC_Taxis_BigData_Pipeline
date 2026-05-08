"""
Global model state — loaded once at startup via FastAPI lifespan.
"""

import mlflow
import mlflow.xgboost

from app.config import MLFLOW_TRACKING_URI, MODEL_NAME, MODEL_STAGE

# Populated at startup
_model       = None
_model_version = "unknown"


def load_model() -> None:
    """Load XGBoost Production model from MLflow Registry."""
    global _model, _model_version

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    model_uri = f"models:/{MODEL_NAME}/{MODEL_STAGE}"
    print(f"[fastapi] Loading model from {model_uri} ...")
    _model = mlflow.xgboost.load_model(model_uri)

    client = mlflow.tracking.MlflowClient()
    versions = client.get_latest_versions(MODEL_NAME, stages=[MODEL_STAGE])
    _model_version = versions[0].version if versions else "unknown"

    print(f"[fastapi] Model ready — {MODEL_NAME} v{_model_version}")


def get_model():
    if _model is None:
        raise RuntimeError("Model not loaded. Call load_model() first.")
    return _model


def get_model_version() -> str:
    return _model_version

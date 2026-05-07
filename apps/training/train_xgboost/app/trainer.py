"""
XGBoost trainer with full MLflow tracking.
Mirrors the Kaggle notebook Sections 7, 8, 11.

Steps:
  1. Train / test split
  2. StandardScaler
  3. Train XGBoost
  4. Log params, metrics, scaler artifact, model to MLflow
  5. Register & auto-promote to Production if R² >= threshold
"""

import numpy as np
import pandas as pd
import mlflow
import mlflow.xgboost
import xgboost as xgb

from mlflow.models.signature import infer_signature
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

from app.config import (
    MLFLOW_TRACKING_URI,
    EXPERIMENT_NAME,
    MODEL_NAME,
    FEATURE_COLS,
    TARGET_COL,
    TEST_SIZE,
    RANDOM_STATE,
    XGB_PARAMS,
    PROMOTE_THRESHOLD_R2,
)


def _compute_metrics(y_true, y_pred) -> dict:
    return {
        "r2":   float(r2_score(y_true, y_pred)),
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "mae":  float(mean_absolute_error(y_true, y_pred)),
    }


def train_and_log(pdf: pd.DataFrame) -> None:
    """
    Train XGBoost model on the full Gold dataset and log everything to MLflow.
    Auto-promotes model to 'Production' stage if test R² ≥ threshold.
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    # -------------------------------------------------------------------
    # 1. Prepare data
    # -------------------------------------------------------------------
    X = pdf[FEATURE_COLS].fillna(0).replace([np.inf, -np.inf], 0)
    y = pdf[TARGET_COL]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE
    )

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled  = scaler.transform(X_test)

    print(f"[train_xgboost] Train: {X_train.shape[0]:,}  Test: {X_test.shape[0]:,}")

    # -------------------------------------------------------------------
    # 2. MLflow run
    # -------------------------------------------------------------------
    with mlflow.start_run(run_name="XGBoost") as run:
        # Params
        mlflow.log_param("model_type",  "XGBoost")
        mlflow.log_param("n_features",  len(FEATURE_COLS))
        mlflow.log_param("train_size",  len(X_train))
        mlflow.log_param("test_size",   len(X_test))
        mlflow.log_param("scaler",      "StandardScaler")
        mlflow.log_params(XGB_PARAMS)

        # -------------------------------------------------------------------
        # 3. Train
        # -------------------------------------------------------------------
        model = xgb.XGBRegressor(**XGB_PARAMS)
        model.fit(
            X_train_scaled, y_train,
            eval_set=[(X_test_scaled, y_test)],
            verbose=False,
        )

        # -------------------------------------------------------------------
        # 4. Metrics
        # -------------------------------------------------------------------
        train_metrics = _compute_metrics(y_train, model.predict(X_train_scaled))
        test_metrics  = _compute_metrics(y_test,  model.predict(X_test_scaled))

        mlflow.log_metric("train_r2",   train_metrics["r2"])
        mlflow.log_metric("test_r2",    test_metrics["r2"])
        mlflow.log_metric("test_rmse",  test_metrics["rmse"])
        mlflow.log_metric("test_mae",   test_metrics["mae"])

        print("[train_xgboost] Metrics:")
        print(f"  Train R²:  {train_metrics['r2']:.4f}")
        print(f"  Test  R²:  {test_metrics['r2']:.4f}")
        print(f"  Test RMSE: {test_metrics['rmse']:.4f}")
        print(f"  Test MAE:  {test_metrics['mae']:.4f}")

        # Feature importance artifact
        importance_df = pd.DataFrame({
            "feature":    FEATURE_COLS,
            "importance": model.feature_importances_,
        }).sort_values("importance", ascending=False)
        imp_path = "/tmp/xgb_feature_importance.csv"
        importance_df.to_csv(imp_path, index=False)
        mlflow.log_artifact(imp_path, artifact_path="feature_importance")

        # -------------------------------------------------------------------
        # 5. Log model + register
        # -------------------------------------------------------------------
        signature = infer_signature(X_train_scaled, model.predict(X_train_scaled))
        mlflow.xgboost.log_model(
            model,
            artifact_path="model",
            signature=signature,
            registered_model_name=MODEL_NAME,
        )

        run_id = run.info.run_id
        print(f"[train_xgboost] Run ID: {run_id}")

    # -------------------------------------------------------------------
    # 6. Auto-promote to Production if R² meets threshold
    # -------------------------------------------------------------------
    if test_metrics["r2"] >= PROMOTE_THRESHOLD_R2:
        client = mlflow.tracking.MlflowClient()
        # Get the latest version we just registered
        versions = client.get_latest_versions(MODEL_NAME, stages=["None"])
        if versions:
            latest_version = versions[0].version
            client.transition_model_version_stage(
                name=MODEL_NAME,
                version=latest_version,
                stage="Production",
                archive_existing_versions=True,
            )
            print(f"[train_xgboost] Model v{latest_version} promoted to Production "
                  f"(R²={test_metrics['r2']:.4f} ≥ {PROMOTE_THRESHOLD_R2})")
    else:
        print(f"[train_xgboost] R²={test_metrics['r2']:.4f} below threshold "
              f"{PROMOTE_THRESHOLD_R2} — model NOT promoted to Production.")

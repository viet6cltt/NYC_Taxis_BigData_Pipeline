"""
XGBoost trainer with full MLflow tracking.
Mirrors the Kaggle notebook Sections 7, 8, 11.

Steps:
  1. Train / test split
  2. Assemble Spark ML feature vectors
  3. Train XGBoost with xgboost.spark
  4. Log params, metrics, feature importance, model to MLflow
  5. Register & auto-promote to Production if R² >= threshold
"""

import pandas as pd
import mlflow
import mlflow.xgboost

from mlflow.models.signature import infer_signature
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame
from xgboost.spark import SparkXGBRegressor

from app.config import (
    MLFLOW_TRACKING_URI,
    EXPERIMENT_NAME,
    MODEL_NAME,
    FEATURE_COLS,
    TARGET_COL,
    TEST_SIZE,
    RANDOM_STATE,
    XGB_PARAMS,
    XGB_NUM_WORKERS,
    PROMOTE_THRESHOLD_R2,
)


FEATURES_COL = "features"
PREDICTION_COL = "prediction"


def _compute_metrics(predictions: DataFrame) -> dict:
    evaluator = RegressionEvaluator(
        labelCol=TARGET_COL,
        predictionCol=PREDICTION_COL,
    )
    return {
        "r2": float(evaluator.setMetricName("r2").evaluate(predictions)),
        "rmse": float(evaluator.setMetricName("rmse").evaluate(predictions)),
        "mae": float(evaluator.setMetricName("mae").evaluate(predictions)),
    }


def _resolve_num_workers(df: DataFrame) -> int:
    if XGB_NUM_WORKERS > 0:
        return XGB_NUM_WORKERS
    return max(1, df.sparkSession.sparkContext.defaultParallelism)


def _get_sklearn_model(spark_model):
    sklearn_model = getattr(spark_model, "_xgb_sklearn_model", None)
    if sklearn_model is None:
        raise RuntimeError(
            "SparkXGBRegressorModel does not expose _xgb_sklearn_model. "
            "Use mlflow.spark.log_model for this xgboost version, and update serving "
            "to load the Spark ML model."
        )
    return sklearn_model


def train_and_log(gold_df: DataFrame) -> None:
    """
    Train XGBoost model on the Gold dataset and log everything to MLflow.
    Auto-promotes model to 'Production' stage if test R² ≥ threshold.
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    # -------------------------------------------------------------------
    # 1. Prepare data
    # -------------------------------------------------------------------
    assembler = VectorAssembler(
        inputCols=FEATURE_COLS,
        outputCol=FEATURES_COL,
        handleInvalid="keep",
    )
    dataset = assembler.transform(gold_df).select(FEATURES_COL, TARGET_COL).cache()

    train_df, test_df = dataset.randomSplit(
        [1.0 - TEST_SIZE, TEST_SIZE],
        seed=RANDOM_STATE,
    )
    train_df = train_df.cache()
    test_df = test_df.cache()

    train_size = train_df.count()
    test_size = test_df.count()
    num_workers = _resolve_num_workers(gold_df)

    print(f"[train_xgboost] Train: {train_size:,}  Test: {test_size:,}")
    print(f"[train_xgboost] XGBoost Spark workers: {num_workers}")

    # -------------------------------------------------------------------
    # 2. MLflow run
    # -------------------------------------------------------------------
    with mlflow.start_run(run_name="XGBoost") as run:
        # Params
        mlflow.log_param("model_type",  "XGBoost")
        mlflow.log_param("n_features",  len(FEATURE_COLS))
        mlflow.log_param("train_size",  train_size)
        mlflow.log_param("test_size",   test_size)
        mlflow.log_param("scaler",      "none")
        mlflow.log_param("num_workers", num_workers)
        mlflow.log_params(XGB_PARAMS)

        # -------------------------------------------------------------------
        # 3. Train
        # -------------------------------------------------------------------
        estimator = SparkXGBRegressor(
            features_col=FEATURES_COL,
            label_col=TARGET_COL,
            prediction_col=PREDICTION_COL,
            num_workers=num_workers,
            **XGB_PARAMS,
        )
        spark_model = estimator.fit(train_df)
        sklearn_model = _get_sklearn_model(spark_model)

        # -------------------------------------------------------------------
        # 4. Metrics
        # -------------------------------------------------------------------
        train_predictions = spark_model.transform(train_df)
        test_predictions = spark_model.transform(test_df)
        train_metrics = _compute_metrics(train_predictions)
        test_metrics = _compute_metrics(test_predictions)

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
            "importance": sklearn_model.feature_importances_,
        }).sort_values("importance", ascending=False)
        imp_path = "/tmp/xgb_feature_importance.csv"
        importance_df.to_csv(imp_path, index=False)
        mlflow.log_artifact(imp_path, artifact_path="feature_importance")

        # -------------------------------------------------------------------
        # 5. Log model + register
        # -------------------------------------------------------------------
        input_example = gold_df.select(*FEATURE_COLS).limit(100).toPandas()
        signature = infer_signature(input_example, sklearn_model.predict(input_example))
        mlflow.xgboost.log_model(
            sklearn_model,
            artifact_path="model",
            signature=signature,
            registered_model_name=MODEL_NAME,
        )

        run_id = run.info.run_id
        print(f"[train_xgboost] Run ID: {run_id}")

    train_df.unpersist()
    test_df.unpersist()
    dataset.unpersist()

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

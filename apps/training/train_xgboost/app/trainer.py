"""
XGBoost trainer with full MLflow tracking.
Mirrors the Kaggle notebook Sections 7, 8, 11.

Steps:
  1. Train / test split
  2. Assemble Spark ML feature vectors
  3. Train XGBoost with xgboost.spark
  4. Log params, metrics, feature importance, model to MLflow
  5. Register model in MLflow
  6. Optionally auto-promote for manual runs
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
    XGB_MAX_TRAIN_ROWS,
    PROMOTE_THRESHOLD_R2,
    AUTO_PROMOTE,
    MLFLOW_RUN_TAGS,
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


def _cap_training_rows(gold_df: DataFrame) -> tuple[DataFrame, int, int | None]:
    source_rows = gold_df.count()
    if XGB_MAX_TRAIN_ROWS <= 0 or source_rows <= XGB_MAX_TRAIN_ROWS:
        return gold_df, source_rows, None

    fraction = XGB_MAX_TRAIN_ROWS / source_rows
    sampled_df = gold_df.sample(withReplacement=False, fraction=fraction, seed=RANDOM_STATE)
    sampled_rows = sampled_df.count()
    print(
        "[train_xgboost] Capping training input: "
        f"{source_rows:,} source rows -> {sampled_rows:,} sampled rows "
        f"(target max {XGB_MAX_TRAIN_ROWS:,}, fraction {fraction:.6f})"
    )
    return sampled_df, source_rows, sampled_rows


def train_and_log(gold_df: DataFrame) -> dict:
    """
    Train XGBoost model on the Gold dataset and log everything to MLflow.
    Airflow retrain runs set AUTO_PROMOTE=false so the DAG can evaluate the
    candidate against the current Production model before promotion.
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)
    training_input_df, source_rows, sampled_rows = _cap_training_rows(gold_df)

    # -------------------------------------------------------------------
    # 1. Prepare data
    # -------------------------------------------------------------------
    assembler = VectorAssembler(
        inputCols=FEATURE_COLS,
        outputCol=FEATURES_COL,
        handleInvalid="keep",
    )
    dataset = assembler.transform(training_input_df).select(FEATURES_COL, TARGET_COL)

    train_df, test_df = dataset.randomSplit(
        [1.0 - TEST_SIZE, TEST_SIZE],
        seed=RANDOM_STATE,
    )
    train_size = train_df.count()
    test_size = test_df.count()
    num_workers = _resolve_num_workers(gold_df)

    print(f"[train_xgboost] Train: {train_size:,}  Test: {test_size:,}")
    print(f"[train_xgboost] XGBoost Spark workers: {num_workers}")

    # -------------------------------------------------------------------
    # 2. MLflow run
    # -------------------------------------------------------------------
    with mlflow.start_run(run_name="XGBoost") as run:
        if MLFLOW_RUN_TAGS:
            mlflow.set_tags(MLFLOW_RUN_TAGS)

        # Params
        mlflow.log_param("model_type",  "XGBoost")
        mlflow.log_param("n_features",  len(FEATURE_COLS))
        mlflow.log_param("source_rows", source_rows)
        mlflow.log_param("max_train_rows", XGB_MAX_TRAIN_ROWS)
        if sampled_rows is not None:
            mlflow.log_param("sampled_rows", sampled_rows)
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
        
        print("[train_xgboost] Train prediction samples")
        train_predictions.select(
            TARGET_COL,
            PREDICTION_COL
        ).show(10, truncate=False)

        print("[train_xgboost] Test prediction samples")
        test_predictions.select(
            TARGET_COL,
            PREDICTION_COL
        ).show(10, truncate=False)
        
        train_metrics = _compute_metrics(train_predictions)
        test_metrics = _compute_metrics(test_predictions)

        mlflow.log_metric("train_r2",   train_metrics["r2"])
        mlflow.log_metric("train_rmse", train_metrics["rmse"])
        mlflow.log_metric("train_mae",  train_metrics["mae"])

        mlflow.log_metric("test_r2",    test_metrics["r2"])
        mlflow.log_metric("test_rmse",  test_metrics["rmse"])
        mlflow.log_metric("test_mae",   test_metrics["mae"])

        print("[train_xgboost] Metrics:")

        print(
            f"  Train -> "
            f"R²={train_metrics['r2']:.4f}  "
            f"RMSE={train_metrics['rmse']:.4f}  "
            f"MAE={train_metrics['mae']:.4f}"
        )

        print(
            f"  Test  -> "
            f"R²={test_metrics['r2']:.4f}  "
            f"RMSE={test_metrics['rmse']:.4f}  "
            f"MAE={test_metrics['mae']:.4f}"
        )

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

    if not AUTO_PROMOTE:
        print("[train_xgboost] AUTO_PROMOTE=false; candidate remains unpromoted for Airflow gate.")
        return {
            "run_id": run_id,
            "train_size": train_size,
            "test_size": test_size,
            "num_workers": num_workers,
            "train_r2": train_metrics["r2"],
            "train_rmse": train_metrics["rmse"],
            "train_mae": train_metrics["mae"],
            "test_r2": test_metrics["r2"],
            "test_rmse": test_metrics["rmse"],
            "test_mae": test_metrics["mae"],
            "promoted": False,
        }

    # -------------------------------------------------------------------
    # 6. Auto-promote to Production if R² meets threshold for manual runs
    # -------------------------------------------------------------------
    if test_metrics["r2"] >= PROMOTE_THRESHOLD_R2:
        promoted = False
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
            promoted = True
    else:
        promoted = False
        print(f"[train_xgboost] R²={test_metrics['r2']:.4f} below threshold "
              f"{PROMOTE_THRESHOLD_R2} — model NOT promoted to Production.")

    return {
        "run_id": run_id,
        "train_size": train_size,
        "test_size": test_size,
        "num_workers": num_workers,
        "train_r2": train_metrics["r2"],
        "train_rmse": train_metrics["rmse"],
        "train_mae": train_metrics["mae"],
        "test_r2": test_metrics["r2"],
        "test_rmse": test_metrics["rmse"],
        "test_mae": test_metrics["mae"],
        "promoted": promoted,
    }

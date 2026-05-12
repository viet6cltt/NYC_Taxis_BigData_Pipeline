#!/usr/bin/env python3
"""
Export trained XGBoost model from MLflow to BentoML
"""
import os
import mlflow
import bentoml

# Set AWS credentials for S3/MinIO
os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://localhost:9000"

MLFLOW_TRACKING_URI = "http://localhost:5000"
EXPERIMENT_NAME = "NYC_Taxi_Fare_Prediction"

try:
    # Connect to MLflow
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    print(f"✓ Connected to MLflow: {MLFLOW_TRACKING_URI}")
    
    # Get experiment
    experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
    if not experiment:
        print(f"❌ Experiment '{EXPERIMENT_NAME}' not found")
        exit(1)
    
    # Get all runs
    runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id])
    if runs.empty:
        print("❌ No runs found in experiment")
        exit(1)
    
    # Get latest run
    latest_run = runs.iloc[0]
    run_id = latest_run['run_id']
    
    print(f"\n✓ Found experiment: {EXPERIMENT_NAME}")
    print(f"✓ Latest run ID: {run_id}")
    
    # Load model from MLflow
    model_uri = f"runs:/{run_id}/model"
    model = mlflow.sklearn.load_model(model_uri)
    print(f"✓ Loaded model from MLflow")
    
    # Save to BentoML
    saved_model = bentoml.sklearn.save_model("nyc_taxi_xgb", model)
    print(f"\n✅ Model saved to BentoML:")
    print(f"   Model tag: {saved_model.tag}")
    print(f"   Model path: {saved_model.path}")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

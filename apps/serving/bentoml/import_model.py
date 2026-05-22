import os
import mlflow
import bentoml

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001")
MODEL_NAME = os.getenv("MODEL_NAME", "XGB_NYC_Fare")
MODEL_STAGE = os.getenv("MODEL_STAGE", "Production")
MODEL_ALIAS = os.getenv("MODEL_ALIAS", "")

def main():
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    client = mlflow.tracking.MlflowClient()
    if MODEL_ALIAS:
        try:
            client.get_model_version_by_alias(MODEL_NAME, MODEL_ALIAS)
        except Exception:
            raise RuntimeError(f"Could not find model alias {MODEL_NAME}@{MODEL_ALIAS} in MLflow.")
        model_uri = f"models:/{MODEL_NAME}@{MODEL_ALIAS}"
    else:
        versions = client.get_latest_versions(MODEL_NAME, stages=[MODEL_STAGE])
        if versions:
            model_uri = f"models:/{MODEL_NAME}/{MODEL_STAGE}"
        else:
            raise RuntimeError(f"Could not find model {MODEL_NAME} in MLflow stage {MODEL_STAGE}.")

    print(f"Importing model from MLflow: {model_uri}")

    # Import the model from MLflow into BentoML's local model store
    bento_model = bentoml.mlflow.import_model(
        MODEL_NAME,
        model_uri=model_uri
    )
    print(f"✅ Successfully imported to BentoML local store: {bento_model.tag}")

if __name__ == "__main__":
    main()

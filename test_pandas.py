import pandas as pd
import numpy as np
import mlflow
from deltalake import write_deltalake
from datetime import datetime, timezone

# 1. Đọc model
mlflow.set_tracking_uri("http://localhost:5001")
model = mlflow.xgboost.load_model("models:/XGB_NYC_Fare@production")

# 2. Tạo feature bằng Pandas (mô phỏng feature_extractor + avro decode)
df = pd.read_parquet("data/yellow_data/2024/yellow_tripdata_2024-01.parquet").head(10)
X = pd.DataFrame()
X["passenger_count"] = df["passenger_count"].fillna(1).astype(int)
X["trip_distance"] = df["trip_distance"].fillna(0.0)
X["trip_duration_seconds"] = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds()
X["speed"] = np.where(X["trip_duration_seconds"] > 0, X["trip_distance"] / (X["trip_duration_seconds"] / 3600), 0)
X["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
X["pickup_day_of_week"] = df["tpep_pickup_datetime"].dt.dayofweek
X["is_weekend"] = X["pickup_day_of_week"].isin([5, 6]).astype(int)
X["hour_sin"] = np.sin(X["pickup_hour"] * (2 * np.pi / 24))
X["hour_cos"] = np.cos(X["pickup_hour"] * (2 * np.pi / 24))
X["day_sin"] = np.sin(X["pickup_day_of_week"] * (2 * np.pi / 7))
X["day_cos"] = np.cos(X["pickup_day_of_week"] * (2 * np.pi / 7))
X["distance_manhattan"] = np.abs(df["DOLocationID"].fillna(0) - df["PULocationID"].fillna(0))
X["location_cluster"] = ((df["PULocationID"].fillna(0) + df["DOLocationID"].fillna(0)) % 5).astype(int)
X["temporal_cluster"] = (X["pickup_hour"] // 6).astype(int)

# 3. Predict
X = X.replace([np.inf, -np.inf], 0).fillna(0)
preds = model.predict(X)
df["predicted_fare"] = preds
df["model_name"] = "XGB_NYC_Fare"
df["model_version"] = "1"
df["event_id"] = [str(i) for i in range(10)]

# 4. Ghi Delta
write_deltalake("_local_delta_store/predictions", df, mode="append")
print("✅ Pandas predict & Delta write OK")
print(df[["event_id", "trip_distance", "predicted_fare"]])

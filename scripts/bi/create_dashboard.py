#!/usr/bin/env python3
"""Create the NYC Taxi Superset dashboard through the Superset REST API."""

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

# Config
K8S_NAMESPACE = os.getenv("K8S_NAMESPACE", "serving")
K8S_NODE_HOST = os.getenv("K8S_NODE_HOST", "34.143.191.48")
SUPERSET_NODE_PORT = os.getenv("SUPERSET_NODE_PORT", "30088")
TRINO_NODE_PORT = os.getenv("TRINO_NODE_PORT", "30091")

SUPERSET_URL  = os.getenv("SUPERSET_URL", f"http://{K8S_NODE_HOST}:{SUPERSET_NODE_PORT}")
# URL này chỉ để script chạy từ host kiểm tra Trino có lên chưa.
TRINO_URL     = os.getenv("TRINO_URL", f"http://{K8S_NODE_HOST}:{TRINO_NODE_PORT}")
ADMIN_USER    = os.getenv("SUPERSET_ADMIN_USER", "admin")
ADMIN_PASS    = os.getenv("SUPERSET_ADMIN_PASS", "admin")

# URI này được Superset dùng từ bên trong container/pod, không phải từ host.
# Superset dùng URI này từ trong container/pod. Mặc định trỏ service Trino
# trên K8s; Docker Compose/local vẫn override được bằng env nếu cần.
TRINO_URI = os.getenv(
    "TRINO_SQLALCHEMY_URI",
    f"trino://trino@trino.{K8S_NAMESPACE}.svc.cluster.local:8080/delta",
)

# HTTP helpers
_token: str = ""

def _req(method: str, path: str, body: Any = None, *, token: str = "") -> Any:
    url = f"{SUPERSET_URL}{path}"
    data = json.dumps(body).encode() if body is not None else None
    headers: dict = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        body_txt = e.read().decode(errors="replace")
        # 422 / 409 usually means "already exists"; keep the script idempotent.
        if e.code in (409, 422):
            return {"_http_error": e.code, "_body": body_txt}
        print(f"  HTTP {e.code} {method} {path}: {body_txt[:300]}")
        raise


def login() -> str:
    resp = _req("POST", "/api/v1/security/login", {
        "username": ADMIN_USER,
        "password": ADMIN_PASS,
        "provider": "db",
        "refresh": True,
    })
    return resp["access_token"]


def api(method: str, path: str, body: Any = None) -> Any:
    return _req(method, path, body, token=_token)


def filter_q(column: str, value: str) -> str:
    raw = f"(filters:!((col:{column},opr:eq,value:'{value}')))"
    return urllib.parse.quote(raw, safe="()!:,'")


# Wait helpers
def wait_http(url: str, label: str, retries: int = 40, delay: int = 5) -> bool:
    print(f"  Waiting for {label}...", end="", flush=True)
    for _ in range(retries):
        try:
            urllib.request.urlopen(url, timeout=4)
            print(" ready")
            return True
        except Exception:
            print(".", end="", flush=True)
            time.sleep(delay)
    print(" TIMEOUT")
    return False


# Step 1: Database connection
def ensure_database() -> int:
    """Create or return the Trino database connection id."""
    db_name = "Trino - NYC Taxi Lakehouse"
    resp = api("GET", "/api/v1/database/")
    for item in resp.get("result", []):
        if item.get("database_name") == db_name:
            db_id = item["id"]
            api("PUT", f"/api/v1/database/{db_id}", {
                "database_name": db_name,
                "sqlalchemy_uri": TRINO_URI,
                "expose_in_sqllab": True,
                "allow_run_async": True,
                "allow_ctas": False,
                "allow_cvas": False,
                "allow_dml": False,
                "extra": json.dumps({
                    "engine_params": {
                        "connect_args": {"http_scheme": "http"}
                    },
                    "cost_estimate_enabled": False,
                }),
            })
            print(f"  OK Database '{db_name}' already exists (id={db_id}); URI refreshed")
            return db_id

    payload = {
        "database_name": db_name,
        "sqlalchemy_uri": TRINO_URI,
        "expose_in_sqllab": True,
        "allow_run_async": True,
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_dml": False,
        "extra": json.dumps({
            "engine_params": {
                "connect_args": {"http_scheme": "http"}
            },
            "cost_estimate_enabled": False,
        }),
    }
    resp = api("POST", "/api/v1/database/", payload)
    if "_http_error" in resp:
        resp2 = api("GET", "/api/v1/database/")
        for item in resp2.get("result", []):
            if item.get("database_name") == db_name:
                return item["id"]
        raise RuntimeError(f"Cannot create database: {resp}")
    db_id = resp["id"]
    print(f"  OK Database '{db_name}' created (id={db_id})")
    return db_id


# Step 2: Datasets (virtual SQL)
DATASETS = {
    "business_monthly": {
        "dataset_name": "bi_business_monthly",
        "schema": "silver_nyc_taxi",
        "sql": """
SELECT
    year_month,
    COUNT(*) AS completed_trips,
    ROUND(SUM(total_amount), 0) AS total_revenue,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(AVG(trip_distance), 2) AS avg_distance_miles,
    ROUND(AVG(CAST(trip_duration_seconds AS DOUBLE)) / 60.0, 1) AS avg_duration_min
FROM delta.silver_nyc_taxi.trip_lifecycle
WHERE status = 'completed'
GROUP BY year_month
ORDER BY year_month
""",
        "description": "Monthly business overview from completed lifecycle trips",
    },
    "business_hourly": {
        "dataset_name": "bi_business_hourly",
        "schema": "silver_nyc_taxi",
        "sql": """
SELECT
    trip_hour AS pickup_hour,
    COUNT(*) AS completed_trips,
    ROUND(SUM(total_amount), 0) AS total_revenue,
    ROUND(AVG(fare_amount), 2) AS avg_fare
FROM delta.silver_nyc_taxi.trip_lifecycle
WHERE status = 'completed'
GROUP BY trip_hour
ORDER BY trip_hour
""",
        "description": "Completed trips and revenue by pickup hour",
    },
    "business_daily": {
        "dataset_name": "bi_business_daily",
        "schema": "silver_nyc_taxi",
        "sql": """
SELECT
    CAST(trip_date AS DATE) AS trip_day,
    CASE day_of_week(CAST(trip_date AS DATE))
        WHEN 1 THEN 'Mon'
        WHEN 2 THEN 'Tue'
        WHEN 3 THEN 'Wed'
        WHEN 4 THEN 'Thu'
        WHEN 5 THEN 'Fri'
        WHEN 6 THEN 'Sat'
        WHEN 7 THEN 'Sun'
    END AS weekday,
    COUNT(*) AS completed_trips,
    ROUND(SUM(total_amount), 0) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_total_amount,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(AVG(tip_amount), 2) AS avg_tip,
    ROUND(AVG(trip_distance), 2) AS avg_distance_miles,
    ROUND(AVG(CAST(trip_duration_seconds AS DOUBLE)) / 60.0, 1) AS avg_duration_min
FROM delta.silver_nyc_taxi.trip_lifecycle
WHERE status = 'completed'
  AND trip_date IS NOT NULL
GROUP BY 1, 2
ORDER BY 1
""",
        "description": "Daily NYC taxi demand, revenue, fare, tip, distance, and duration",
    },
    "business_weekday_hour": {
        "dataset_name": "bi_business_weekday_hour",
        "schema": "silver_nyc_taxi",
        "sql": """
SELECT
    CASE day_of_week(CAST(trip_date AS DATE))
        WHEN 1 THEN 'Mon'
        WHEN 2 THEN 'Tue'
        WHEN 3 THEN 'Wed'
        WHEN 4 THEN 'Thu'
        WHEN 5 THEN 'Fri'
        WHEN 6 THEN 'Sat'
        WHEN 7 THEN 'Sun'
    END AS weekday,
    day_of_week(CAST(trip_date AS DATE)) AS weekday_num,
    trip_hour AS pickup_hour,
    COUNT(*) AS completed_trips,
    ROUND(SUM(total_amount), 0) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_total_amount
FROM delta.silver_nyc_taxi.trip_lifecycle
WHERE status = 'completed'
  AND trip_date IS NOT NULL
  AND trip_hour IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY weekday_num, pickup_hour
""",
        "description": "Demand rhythm by weekday and pickup hour",
    },
    "business_routes": {
        "dataset_name": "bi_business_routes",
        "schema": "silver_nyc_taxi",
        "sql": """
SELECT
    pulocation_id,
    dolocation_id,
    CONCAT(CAST(pulocation_id AS VARCHAR), ' -> ', CAST(dolocation_id AS VARCHAR)) AS route_pair,
    COUNT(*) AS completed_trips,
    ROUND(SUM(total_amount), 0) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_total_amount,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(AVG(tip_amount), 2) AS avg_tip,
    ROUND(AVG(trip_distance), 2) AS avg_distance_miles,
    ROUND(AVG(CAST(trip_duration_seconds AS DOUBLE)) / 60.0, 1) AS avg_duration_min
FROM delta.silver_nyc_taxi.trip_lifecycle
WHERE status = 'completed'
GROUP BY pulocation_id, dolocation_id
""",
        "description": "Route-level business metrics",
    },
    "business_pickup_zones": {
        "dataset_name": "bi_business_pickup_zones",
        "schema": "silver_nyc_taxi",
        "sql": """
SELECT
    pulocation_id,
    COUNT(*) AS completed_trips,
    ROUND(SUM(total_amount), 0) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_total_amount,
    ROUND(AVG(trip_distance), 2) AS avg_distance_miles,
    ROUND(AVG(CAST(trip_duration_seconds AS DOUBLE)) / 60.0, 1) AS avg_duration_min
FROM delta.silver_nyc_taxi.trip_lifecycle
WHERE status = 'completed'
GROUP BY pulocation_id
""",
        "description": "Pickup zone demand and revenue hotspots",
    },
    "business_dropoff_zones": {
        "dataset_name": "bi_business_dropoff_zones",
        "schema": "silver_nyc_taxi",
        "sql": """
SELECT
    dolocation_id,
    COUNT(*) AS completed_trips,
    ROUND(SUM(total_amount), 0) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_total_amount,
    ROUND(AVG(trip_distance), 2) AS avg_distance_miles,
    ROUND(AVG(CAST(trip_duration_seconds AS DOUBLE)) / 60.0, 1) AS avg_duration_min
FROM delta.silver_nyc_taxi.trip_lifecycle
WHERE status = 'completed'
GROUP BY dolocation_id
""",
        "description": "Dropoff zone demand and revenue hotspots",
    },
    "business_fare_buckets": {
        "dataset_name": "bi_business_fare_buckets",
        "schema": "silver_nyc_taxi",
        "sql": """
SELECT
    CASE
        WHEN total_amount < 10 THEN '< $10'
        WHEN total_amount < 20 THEN '$10-20'
        WHEN total_amount < 40 THEN '$20-40'
        WHEN total_amount < 80 THEN '$40-80'
        ELSE '$80+'
    END AS fare_bucket,
    CASE
        WHEN total_amount < 10 THEN 1
        WHEN total_amount < 20 THEN 2
        WHEN total_amount < 40 THEN 3
        WHEN total_amount < 80 THEN 4
        ELSE 5
    END AS bucket_order,
    COUNT(*) AS completed_trips,
    ROUND(SUM(total_amount), 0) AS total_revenue,
    ROUND(AVG(trip_distance), 2) AS avg_distance_miles,
    ROUND(AVG(CAST(trip_duration_seconds AS DOUBLE)) / 60.0, 1) AS avg_duration_min
FROM delta.silver_nyc_taxi.trip_lifecycle
WHERE status = 'completed'
  AND total_amount IS NOT NULL
GROUP BY 1, 2
ORDER BY bucket_order
""",
        "description": "Trip distribution by total fare amount bucket",
    },
    "business_distance_buckets": {
        "dataset_name": "bi_business_distance_buckets",
        "schema": "silver_nyc_taxi",
        "sql": """
SELECT
    CASE
        WHEN trip_distance < 1 THEN '< 1 mi'
        WHEN trip_distance < 3 THEN '1-3 mi'
        WHEN trip_distance < 7 THEN '3-7 mi'
        WHEN trip_distance < 15 THEN '7-15 mi'
        ELSE '15+ mi'
    END AS distance_bucket,
    CASE
        WHEN trip_distance < 1 THEN 1
        WHEN trip_distance < 3 THEN 2
        WHEN trip_distance < 7 THEN 3
        WHEN trip_distance < 15 THEN 4
        ELSE 5
    END AS bucket_order,
    COUNT(*) AS completed_trips,
    ROUND(SUM(total_amount), 0) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_total_amount,
    ROUND(AVG(CAST(trip_duration_seconds AS DOUBLE)) / 60.0, 1) AS avg_duration_min
FROM delta.silver_nyc_taxi.trip_lifecycle
WHERE status = 'completed'
  AND trip_distance IS NOT NULL
GROUP BY 1, 2
ORDER BY bucket_order
""",
        "description": "Trip distribution by distance bucket",
    },
    "business_payments": {
        "dataset_name": "bi_business_payments",
        "schema": "silver_nyc_taxi",
        "sql": """
SELECT
    payment_type_desc,
    COUNT(*) AS completed_trips,
    ROUND(SUM(total_amount), 0) AS total_revenue,
    ROUND(AVG(tip_amount), 2) AS avg_tip
FROM delta.silver_nyc_taxi.trip_lifecycle
WHERE status = 'completed'
GROUP BY payment_type_desc
""",
        "description": "Payment mix for completed trips",
    },
    "business_status": {
        "dataset_name": "bi_business_status",
        "schema": "silver_nyc_taxi",
        "sql": """
SELECT status, COUNT(*) AS trip_count
FROM delta.silver_nyc_taxi.trip_lifecycle
GROUP BY status
""",
        "description": "Current lifecycle status distribution",
    },
    "prediction_daily": {
        "dataset_name": "bi_prediction_daily",
        "schema": "gold_ml",
        "sql": """
SELECT
    DATE(prediction_timestamp) AS prediction_date,
    model_name,
    model_version,
    estimate_level,
    COUNT(*) AS prediction_count,
    ROUND(AVG(predicted_fare_amount), 2) AS avg_predicted_fare,
    ROUND(AVG(estimated_trip_distance), 2) AS avg_estimated_distance,
    ROUND(AVG(CAST(estimated_trip_duration_seconds AS DOUBLE)) / 60.0, 1) AS avg_estimated_duration_min
FROM delta.gold_ml.predictions
GROUP BY 1, 2, 3, 4
""",
        "description": "Realtime prediction volume and estimate coverage by day",
    },
    "prediction_hourly": {
        "dataset_name": "bi_prediction_hourly",
        "schema": "gold_ml",
        "sql": """
SELECT
    pickup_hour,
    COUNT(*) AS prediction_count,
    ROUND(AVG(predicted_fare_amount), 2) AS avg_predicted_fare
FROM delta.gold_ml.predictions
GROUP BY pickup_hour
ORDER BY pickup_hour
""",
        "description": "Realtime predictions by pickup hour",
    },
    "route_coverage": {
        "dataset_name": "bi_route_coverage",
        "schema": "gold_ml",
        "sql": """
SELECT
    estimate_level,
    COUNT(*) AS lookup_rows,
    ROUND(AVG(sample_count), 1) AS avg_sample_count,
    ROUND(AVG(estimated_trip_distance), 2) AS avg_estimated_distance,
    ROUND(AVG(CAST(estimated_trip_duration_seconds AS DOUBLE)) / 60.0, 1) AS avg_estimated_duration_min
FROM delta.gold_ml.route_estimates
GROUP BY estimate_level
""",
        "description": "Coverage and support size for route estimate fallback levels",
    },
    "quality_daily": {
        "dataset_name": "bi_quality_daily",
        "schema": "gold_monitoring",
        "sql": """
SELECT
    metric_date,
    model_name,
    model_version,
    prediction_count,
    mae,
    rmse,
    bias,
    avg_predicted_fare,
    avg_actual_fare,
    avg_label_delay_seconds
FROM delta.gold_monitoring.model_quality_daily
""",
        "description": "Daily production model quality metrics",
    },
    "quality_by_estimate_level": {
        "dataset_name": "bi_quality_by_estimate_level",
        "schema": "gold_ml",
        "sql": """
SELECT
    estimate_level,
    COUNT(*) AS evaluated_predictions,
    ROUND(AVG(absolute_error), 2) AS mae,
    ROUND(SQRT(AVG(squared_error)), 2) AS rmse,
    ROUND(AVG(prediction_error), 2) AS bias,
    ROUND(AVG(label_delay_seconds), 1) AS avg_label_delay_seconds
FROM delta.gold_ml.prediction_actuals
GROUP BY estimate_level
""",
        "description": "Prediction quality split by route estimate fallback level",
    },
    "quality_route_hotspots": {
        "dataset_name": "bi_quality_route_hotspots",
        "schema": "gold_ml",
        "sql": """
SELECT
    pulocation_id,
    dolocation_id,
    COUNT(*) AS evaluated_predictions,
    ROUND(AVG(absolute_error), 2) AS mae,
    ROUND(AVG(prediction_error), 2) AS bias
FROM delta.gold_ml.prediction_actuals
GROUP BY pulocation_id, dolocation_id
HAVING COUNT(*) >= 1
""",
        "description": "Route-level error hotspots; preview keeps single-observation routes visible",
    },
}

BUSINESS_DATASETS = {
    "business_monthly",
    "business_daily",
    "business_hourly",
    "business_weekday_hour",
    "business_routes",
    "business_pickup_zones",
    "business_dropoff_zones",
    "business_fare_buckets",
    "business_distance_buckets",
    "business_payments",
    "business_status",
}
PREDICTION_DATASETS = {"prediction_daily", "prediction_hourly", "route_coverage"}
QUALITY_DATASETS = {
    "quality_daily",
    "quality_by_estimate_level",
    "quality_route_hotspots",
}


def has_datasets(ds: dict, required: set[str], label: str) -> bool:
    missing = sorted(required - set(ds))
    if missing:
        print(f"  WARN Skip {label}: missing datasets {', '.join(missing)}")
        return False
    return True


def ensure_dataset(db_id: int, key: str) -> int:
    cfg = DATASETS[key]
    name = cfg["dataset_name"]
    resp = api("GET", f"/api/v1/dataset/?q={filter_q('table_name', name)}")
    if resp.get("count", 0) > 0:
        ds_id = resp["result"][0]["id"]
        api("PUT", f"/api/v1/dataset/{ds_id}", {
            "database_id": db_id,
            "table_name": name,
            "sql": cfg["sql"].strip(),
            "schema": cfg["schema"],
            "is_managed_externally": False,
        })
        api("PUT", f"/api/v1/dataset/{ds_id}/refresh")
        print(f"  OK Dataset '{name}' exists (id={ds_id}); SQL and columns refreshed")
        return ds_id
    payload = {
        "database": db_id,
        "table_name": name,
        "sql": cfg["sql"].strip(),
        "schema": cfg["schema"],
        "is_managed_externally": False,
    }
    resp = api("POST", "/api/v1/dataset/", payload)
    if "_http_error" in resp:
        resp2 = api("GET", f"/api/v1/dataset/?q={filter_q('table_name', name)}")
        if resp2.get("count", 0) > 0:
            return resp2["result"][0]["id"]
        raise RuntimeError(f"Cannot create dataset '{name}': {resp}")
    ds_id = resp["id"]
    api("PUT", f"/api/v1/dataset/{ds_id}/refresh")
    print(f"  OK Dataset '{name}' created (id={ds_id}); columns refreshed")
    return ds_id


def make_chart(name: str, viz_type: str, ds_id: int, params: dict) -> int:
    resp = api("GET", f"/api/v1/chart/?q={filter_q('slice_name', name)}")
    if resp.get("count", 0) > 0:
        cid = resp["result"][0]["id"]
        api("PUT", f"/api/v1/chart/{cid}", {
            "slice_name": name,
            "viz_type": viz_type,
            "datasource_id": ds_id,
            "datasource_type": "table",
            "params": json.dumps(params),
        })
        print(f"  OK Chart '{name}' exists (id={cid}); params refreshed")
        return cid
    payload = {
        "slice_name": name,
        "viz_type": viz_type,
        "datasource_id": ds_id,
        "datasource_type": "table",
        "params": json.dumps(params),
        "description": params.get("_description", ""),
    }
    resp = api("POST", "/api/v1/chart/", payload)
    if "_http_error" in resp:
        resp2 = api("GET", f"/api/v1/chart/?q={filter_q('slice_name', name)}")
        if resp2.get("count", 0) > 0:
            return resp2["result"][0]["id"]
        raise RuntimeError(f"Cannot create chart '{name}': {resp}")
    cid = resp["id"]
    print(f"  OK Chart '{name}' created (id={cid})")
    return cid


def simple_metric(column: str, aggregate: str, label: str) -> dict:
    return {"expressionType": "SIMPLE", "column": {"column_name": column}, "aggregate": aggregate, "label": label}


def build_business_charts(ds: dict) -> dict:
    return {
        "completed_trips": make_chart("Business - Completed Trips", "big_number_total", ds["business_monthly"], {"metric": simple_metric("completed_trips", "SUM", "Completed Trips"), "subheader": "All time", "y_axis_format": ",.0f"}),
        "revenue": make_chart("Business - Total Revenue", "big_number_total", ds["business_monthly"], {"metric": simple_metric("total_revenue", "SUM", "Revenue"), "subheader": "All time", "y_axis_format": "$,.0f"}),
        "avg_fare": make_chart("Business - Avg Fare", "big_number_total", ds["business_monthly"], {"metric": simple_metric("avg_fare", "AVG", "Avg Fare"), "subheader": "Across months", "y_axis_format": "$,.2f"}),
        "avg_distance": make_chart("Business - Avg Distance", "big_number_total", ds["business_monthly"], {"metric": simple_metric("avg_distance_miles", "AVG", "Miles"), "subheader": "Average trip length", "y_axis_format": ",.2f"}),
        "monthly_trips": make_chart("Business - Monthly Trips", "echarts_timeseries_bar", ds["business_monthly"], {"x_axis": "year_month", "metrics": [simple_metric("completed_trips", "SUM", "Trips")], "groupby": [], "y_axis_format": ",.0f"}),
        "monthly_revenue": make_chart("Business - Monthly Revenue", "echarts_timeseries_line", ds["business_monthly"], {"x_axis": "year_month", "metrics": [simple_metric("total_revenue", "SUM", "Revenue")], "groupby": [], "y_axis_format": "$,.0f"}),
        "daily_demand": make_chart(
            "Business - Daily Revenue Trend",
            "echarts_timeseries_line",
            ds["business_daily"],
            {
                "x_axis": "trip_day",
                "metrics": [simple_metric("total_revenue", "SUM", "Revenue")],
                "groupby": [],
                "y_axis_format": "$,.0f",
                "adhoc_filters": [
                    {
                        "clause": "WHERE",
                        "expressionType": "SIMPLE",
                        "subject": "trip_day",
                        "operator": "TEMPORAL_RANGE",
                        "comparator": "2024-01-01 : now",
                    }
                ],
            }
        ),
        "daily_revenue": make_chart(
            "Business - Daily Revenue Trend",
            "echarts_timeseries_line",
            ds["business_daily"],
            {
                "x_axis": "trip_day",
                "metrics": [simple_metric("total_revenue", "SUM", "Revenue")],
                "groupby": [],
                "y_axis_format": "$,.0f",
                "adhoc_filters": [
                    {
                        "clause": "WHERE",
                        "expressionType": "SIMPLE",
                        "subject": "trip_day",
                        "operator": "TEMPORAL_RANGE",
                        "comparator": "2024-01-01 : now",
                    }
                ],
            }
        ),
        "hourly_demand": make_chart("Business - Hourly Demand", "echarts_timeseries_bar", ds["business_hourly"], {"x_axis": "pickup_hour", "metrics": [simple_metric("completed_trips", "SUM", "Trips")], "groupby": [], "y_axis_format": ",.0f"}),
        "weekday_hour": make_chart("Business - Weekday x Hour Demand", "table", ds["business_weekday_hour"], {"all_columns": ["weekday", "pickup_hour", "completed_trips", "total_revenue", "avg_total_amount"], "order_by_cols": [["completed_trips", False]], "row_limit": 24}),
        "top_pickups": make_chart("Business - Top Pickup Zones", "table", ds["business_pickup_zones"], {"all_columns": ["pulocation_id", "completed_trips", "total_revenue", "avg_total_amount", "avg_distance_miles", "avg_duration_min"], "order_by_cols": [["completed_trips", False]], "row_limit": 15}),
        "top_dropoffs": make_chart("Business - Top Dropoff Zones", "table", ds["business_dropoff_zones"], {"all_columns": ["dolocation_id", "completed_trips", "total_revenue", "avg_total_amount", "avg_distance_miles", "avg_duration_min"], "order_by_cols": [["completed_trips", False]], "row_limit": 15}),
        "top_routes": make_chart("Business - Top Route Pairs", "table", ds["business_routes"], {"all_columns": ["route_pair", "pulocation_id", "dolocation_id", "completed_trips", "total_revenue", "avg_total_amount", "avg_distance_miles", "avg_duration_min"], "order_by_cols": [["completed_trips", False]], "row_limit": 15}),
        "fare_buckets": make_chart("Business - Fare Distribution", "echarts_timeseries_bar", ds["business_fare_buckets"], {"x_axis": "fare_bucket", "metrics": [simple_metric("completed_trips", "SUM", "Trips")], "groupby": [], "y_axis_format": ",.0f"}),
        "distance_buckets": make_chart("Business - Distance Distribution", "echarts_timeseries_bar", ds["business_distance_buckets"], {"x_axis": "distance_bucket", "metrics": [simple_metric("completed_trips", "SUM", "Trips")], "groupby": [], "y_axis_format": ",.0f"}),
        "fare_by_distance": make_chart("Business - Avg Fare by Distance", "echarts_timeseries_bar", ds["business_distance_buckets"], {"x_axis": "distance_bucket", "metrics": [simple_metric("avg_total_amount", "AVG", "Avg Total Fare")], "groupby": [], "y_axis_format": "$,.2f"}),
        "payments": make_chart("Business - Payment Mix", "pie", ds["business_payments"], {"groupby": ["payment_type_desc"], "metric": simple_metric("completed_trips", "SUM", "Trips"), "donut": True, "show_labels": True}),
        "status": make_chart("Business - Lifecycle Status", "pie", ds["business_status"], {"groupby": ["status"], "metric": simple_metric("trip_count", "SUM", "Trips"), "donut": True, "show_labels": True}),
    }


def build_prediction_charts(ds: dict) -> dict:
    return {
        "prediction_count": make_chart("Prediction Ops - Total Predictions", "big_number_total", ds["prediction_daily"], {"metric": simple_metric("prediction_count", "SUM", "Predictions"), "subheader": "All time", "y_axis_format": ",.0f"}),
        "avg_predicted_fare": make_chart("Prediction Ops - Avg Predicted Fare", "big_number_total", ds["prediction_daily"], {"metric": simple_metric("avg_predicted_fare", "AVG", "Avg Predicted Fare"), "subheader": "Across days", "y_axis_format": "$,.2f"}),
        "daily_predictions": make_chart("Prediction Ops - Daily Volume", "echarts_timeseries_bar", ds["prediction_daily"], {"x_axis": "prediction_date", "metrics": [simple_metric("prediction_count", "SUM", "Predictions")], "groupby": [], "y_axis_format": ",.0f"}),
        "hourly_predictions": make_chart("Prediction Ops - Predictions by Hour", "echarts_timeseries_bar", ds["prediction_hourly"], {"x_axis": "pickup_hour", "metrics": [simple_metric("prediction_count", "SUM", "Predictions")], "groupby": [], "y_axis_format": ",.0f"}),
        "estimate_mix": make_chart("Prediction Ops - Estimate Level Mix", "pie", ds["prediction_daily"], {"groupby": ["estimate_level"], "metric": simple_metric("prediction_count", "SUM", "Predictions"), "donut": True, "show_labels": True}),
        "model_versions": make_chart("Prediction Ops - Volume by Model Version", "echarts_timeseries_bar", ds["prediction_daily"], {"x_axis": "model_version", "metrics": [simple_metric("prediction_count", "SUM", "Predictions")], "groupby": [], "y_axis_format": ",.0f"}),
        "route_coverage": make_chart("Prediction Ops - Route Coverage", "table", ds["route_coverage"], {"all_columns": ["estimate_level", "lookup_rows", "avg_sample_count", "avg_estimated_distance", "avg_estimated_duration_min"], "row_limit": 10}),
    }


def build_quality_charts(ds: dict) -> dict:
    return {
        "mae": make_chart("Quality - MAE", "big_number_total", ds["quality_daily"], {"metric": simple_metric("mae", "AVG", "MAE"), "subheader": "Daily average", "y_axis_format": "$,.2f"}),
        "bias": make_chart("Quality - Bias", "big_number_total", ds["quality_daily"], {"metric": simple_metric("bias", "AVG", "Bias"), "subheader": "Predicted - actual", "y_axis_format": "$,.2f"}),
        "label_delay": make_chart("Quality - Avg Label Delay", "big_number_total", ds["quality_daily"], {"metric": simple_metric("avg_label_delay_seconds", "AVG", "Seconds"), "subheader": "Delayed labels", "y_axis_format": ",.0f"}),
        "mae_trend": make_chart("Quality - MAE Trend", "echarts_timeseries_line", ds["quality_daily"], {"x_axis": "metric_date", "metrics": [simple_metric("mae", "AVG", "MAE"), simple_metric("rmse", "AVG", "RMSE")], "groupby": [], "y_axis_format": "$,.2f"}),
        "pred_vs_actual": make_chart("Quality - Predicted vs Actual Fare", "echarts_timeseries_line", ds["quality_daily"], {"x_axis": "metric_date", "metrics": [simple_metric("avg_predicted_fare", "AVG", "Predicted"), simple_metric("avg_actual_fare", "AVG", "Actual")], "groupby": [], "y_axis_format": "$,.2f"}),
        "quality_by_level": make_chart("Quality - Error by Estimate Level", "echarts_timeseries_bar", ds["quality_by_estimate_level"], {"x_axis": "estimate_level", "metrics": [simple_metric("mae", "AVG", "MAE"), simple_metric("rmse", "AVG", "RMSE")], "groupby": []}),
        "route_hotspots": make_chart("Quality - Route Error Hotspots", "table", ds["quality_route_hotspots"], {"all_columns": ["pulocation_id", "dolocation_id", "evaluated_predictions", "mae", "bias"], "order_by_cols": [["mae", False]], "row_limit": 10}),
    }


def chart_component(uid: str, chart_id: int, row_id: str, w: int = 6, h: int = 24) -> dict:
    return {
        "id": uid,
        "type": "CHART",
        "meta": {"chartId": chart_id, "width": w, "height": h, "sliceName": ""},
        "children": [],
        "parents": ["ROOT_ID", "GRID_ID", row_id],
    }


def row_component(uid: str, children: list) -> dict:
    return {"id": uid, "type": "ROW", "meta": {"background": "BACKGROUND_TRANSPARENT"}, "children": children, "parents": ["ROOT_ID", "GRID_ID"]}


def make_layout(rows: list[list[tuple[str, int, int, int]]]) -> dict:
    layout = {
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"], "parents": []},
        "GRID_ID": {"id": "GRID_ID", "type": "GRID", "children": [], "parents": ["ROOT_ID"]},
    }
    for idx, row in enumerate(rows, 1):
        row_id = f"ROW-{idx}"
        child_ids = []
        for slug, chart_id, width, height in row:
            cid = f"CHART-{slug}"
            child_ids.append(cid)
            layout[cid] = chart_component(cid, chart_id, row_id, width, height)
        layout[row_id] = row_component(row_id, child_ids)
        layout["GRID_ID"]["children"].append(row_id)
    return layout


def ensure_dashboard(title: str, slug: str, chart_ids: dict, rows: list[list[tuple[str, int, int, int]]]) -> int:
    resp = api("GET", f"/api/v1/dashboard/?q={filter_q('dashboard_title', title)}")
    layout = make_layout(rows)
    metadata = json.dumps({"positions": layout})
    payload = {
        "dashboard_title": title,
        "slug": slug,
        "published": True,
        "position_json": json.dumps(layout),
        "json_metadata": metadata,
    }
    if resp.get("count", 0) > 0:
        did = resp["result"][0]["id"]
        api("PUT", f"/api/v1/dashboard/{did}", {
            "position_json": payload["position_json"],
            "json_metadata": metadata,
            "published": True,
        })
        print(f"  OK Dashboard '{title}' exists (id={did}); layout updated")
    else:
        created = api("POST", "/api/v1/dashboard/", payload)
        if "_http_error" in created:
            resp2 = api("GET", f"/api/v1/dashboard/?q={filter_q('dashboard_title', title)}")
            if resp2.get("count", 0) == 0:
                raise RuntimeError(f"Cannot create dashboard '{title}': {created}")
            did = resp2["result"][0]["id"]
        else:
            did = created["id"]
        # Superset only materializes dashboard -> chart relations from
        # json_metadata.positions during the update path. A fresh POST stores the
        # layout but leaves dashboard_slices empty, so the UI renders orphaned
        # chart containers. Follow creation with the same update payload used for
        # existing dashboards to bind the slices immediately.
        api("PUT", f"/api/v1/dashboard/{did}", {
            "position_json": payload["position_json"],
            "json_metadata": metadata,
            "published": True,
        })
        print(f"  OK Dashboard '{title}' ready (id={did})")
    return did


def main():
    print("\n" + "=" * 60)
    print("  NYC Taxi - Superset Dashboard Creator")
    print("=" * 60)
    print("\n[0] Checking services...")
    if not wait_http(f"{SUPERSET_URL}/health", "Superset"):
        print("\n  ERROR Superset not reachable.")
        sys.exit(1)
    if not wait_http(f"{TRINO_URL}/v1/info", "Trino", retries=10, delay=3):
        print("  WARN Trino not running; metadata will be created but queries may fail")
    print("\n[1] Logging in to Superset...")
    global _token
    _token = login()
    print(f"  OK Authenticated as '{ADMIN_USER}'")
    print("\n[2] Setting up Trino database connection...")
    db_id = ensure_database()
    print("\n[3] Creating datasets...")
    ds_ids = {}
    for key in DATASETS:
        try:
            ds_ids[key] = ensure_dataset(db_id, key)
        except urllib.error.HTTPError:
            print(f"  WARN Dataset '{DATASETS[key]['dataset_name']}' skipped; source table/query is not ready")
        except Exception as ex:
            print(f"  WARN Dataset '{DATASETS[key]['dataset_name']}' skipped: {ex}")
    print("\n[4] Creating charts...")
    business = build_business_charts(ds_ids) if has_datasets(ds_ids, BUSINESS_DATASETS, "Business dashboard") else {}
    prediction = build_prediction_charts(ds_ids) if has_datasets(ds_ids, PREDICTION_DATASETS, "Prediction dashboard") else {}
    quality = build_quality_charts(ds_ids) if has_datasets(ds_ids, QUALITY_DATASETS, "Quality dashboard") else {}
    print("\n[5] Building dashboards...")
    if business:
        ensure_dashboard("NYC Taxi - Business Overview", "nyc-taxi-business-overview", business, [
            [("completed-trips", business["completed_trips"], 3, 14), ("revenue", business["revenue"], 3, 14), ("avg-fare", business["avg_fare"], 3, 14), ("avg-distance", business["avg_distance"], 3, 14)],
            [("daily-demand", business["daily_demand"], 6, 28), ("daily-revenue", business["daily_revenue"], 6, 28)],
            [("monthly-trips", business["monthly_trips"], 6, 28), ("monthly-revenue", business["monthly_revenue"], 6, 28)],
            [("hourly-demand", business["hourly_demand"], 6, 28), ("weekday-hour", business["weekday_hour"], 6, 28)],
            [("fare-buckets", business["fare_buckets"], 4, 28), ("distance-buckets", business["distance_buckets"], 4, 28), ("fare-by-distance", business["fare_by_distance"], 4, 28)],
            [("top-pickups", business["top_pickups"], 6, 32), ("top-dropoffs", business["top_dropoffs"], 6, 32)],
            [("top-routes", business["top_routes"], 8, 34), ("payments", business["payments"], 4, 34)],
            [("status", business["status"], 12, 24)],
        ])
    if prediction:
        ensure_dashboard("NYC Taxi - Realtime Prediction Ops", "nyc-taxi-realtime-prediction-ops", prediction, [
            [("prediction-count", prediction["prediction_count"], 6, 14), ("avg-predicted-fare", prediction["avg_predicted_fare"], 6, 14)],
            [("daily-predictions", prediction["daily_predictions"], 6, 28), ("hourly-predictions", prediction["hourly_predictions"], 6, 28)],
            [("estimate-mix", prediction["estimate_mix"], 6, 28), ("model-versions", prediction["model_versions"], 6, 28)],
            [("route-coverage", prediction["route_coverage"], 12, 30)],
        ])
    if quality:
        ensure_dashboard("NYC Taxi - Model Quality", "nyc-taxi-model-quality", quality, [
            [("mae", quality["mae"], 4, 14), ("bias", quality["bias"], 4, 14), ("label-delay", quality["label_delay"], 4, 14)],
            [("mae-trend", quality["mae_trend"], 6, 28), ("pred-vs-actual", quality["pred_vs_actual"], 6, 28)],
            [("quality-by-level", quality["quality_by_level"], 6, 28), ("route-hotspots", quality["route_hotspots"], 6, 30)],
        ])
    print("\n" + "=" * 60)
    print("  Dashboards ready!")
    print("=" * 60)
    print(f"\n  {SUPERSET_URL}/superset/dashboard/nyc-taxi-business-overview/")
    print(f"  {SUPERSET_URL}/superset/dashboard/nyc-taxi-realtime-prediction-ops/")
    print(f"  {SUPERSET_URL}/superset/dashboard/nyc-taxi-model-quality/")
    print()


if __name__ == "__main__":
    main()

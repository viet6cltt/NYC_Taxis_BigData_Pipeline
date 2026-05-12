import requests
import json

SUPERSET_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = "admin"
DATABASE_NAME = "Trino - NYC Taxi Gold"
SCHEMA = "gold"
TABLE_NAME = "features"


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def get_adhoc_metric(column, aggregate, label):
    return {
        "aggregate": aggregate,
        "column": {"column_name": column},
        "expressionType": "SIMPLE",
        "label": label,
    }


def get_adhoc_metric_avg(column, label):
    return get_adhoc_metric(column, "AVG", label)


def get_adhoc_metric_sum(column, label):
    return get_adhoc_metric(column, "SUM", label)


# ──────────────────────────────────────────────
# Auth + setup
# ──────────────────────────────────────────────

def authenticate():
    print("🔐 Authenticating...")
    resp = requests.post(f"{SUPERSET_URL}/api/v1/security/login", json={
        "username": USERNAME, "password": PASSWORD,
        "provider": "db", "refresh": True
    })
    resp.raise_for_status()
    token = resp.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


def get_database_id(headers):
    resp = requests.get(f"{SUPERSET_URL}/api/v1/database/", headers=headers)
    resp.raise_for_status()
    db_id = next(d["id"] for d in resp.json()["result"] if d["database_name"] == DATABASE_NAME)
    print(f"✅ Database ID: {db_id}")
    return db_id


def get_or_create_dataset(headers, db_id):
    resp = requests.get(f"{SUPERSET_URL}/api/v1/dataset/", headers=headers)
    resp.raise_for_status()
    existing = [d for d in resp.json()["result"] if d["table_name"] == TABLE_NAME]
    if existing:
        dataset_id = existing[0]["id"]
        print(f"✅ Dataset already exists, ID: {dataset_id}")
    else:
        resp2 = requests.post(f"{SUPERSET_URL}/api/v1/dataset/", headers=headers, json={
            "database": db_id, "schema": SCHEMA, "table_name": TABLE_NAME
        })
        resp2.raise_for_status()
        dataset_id = resp2.json()["id"]
        print(f"✅ Dataset created, ID: {dataset_id}")
    return dataset_id


def create_dashboard(headers, title="NYC Taxi Executive Dashboard"):
    print(f"\n📊 Creating dashboard: {title}")
    resp = requests.post(f"{SUPERSET_URL}/api/v1/dashboard/", headers=headers, json={
        "dashboard_title": title,
        "published": True
    })
    resp.raise_for_status()
    dashboard_id = resp.json()["id"]
    print(f"✅ Dashboard ID: {dashboard_id}")
    return dashboard_id


# ──────────────────────────────────────────────
# Chart definitions
# ──────────────────────────────────────────────

def get_adhoc_column(sql_expr, label):
    return {
        "sqlExpression": sql_expr,
        "label": label,
        "expressionType": "SQL"
    }

def get_chart_configs(dataset_id, dashboard_id):
    day_of_week_sql = "CASE pickup_day_of_week WHEN 0 THEN '1-Mon' WHEN 1 THEN '2-Tue' WHEN 2 THEN '3-Wed' WHEN 3 THEN '4-Thu' WHEN 4 THEN '5-Fri' WHEN 5 THEN '6-Sat' ELSE '7-Sun' END"
    
    return [
        # ── ROW 1: KPI Big Numbers ──────────────────────────────────────
        {
            "slice_name": "KPI: Tổng Số Chuyến",
            "viz_type": "big_number_total",
            "params": {
                "viz_type": "big_number_total",
                "metric": "count",
                "subheader": "Total Trips",
                "color_picker": {"r": 29, "g": 158, "b": 117, "a": 1},
            },
        },
        {
            "slice_name": "KPI: Tổng Doanh Thu",
            "viz_type": "big_number_total",
            "params": {
                "viz_type": "big_number_total",
                "metric": get_adhoc_metric_sum("fare_amount", "Total Revenue"),
                "subheader": "Total Fare Revenue (USD)",
            },
        },
        {
            "slice_name": "KPI: Quãng Đường Trung Bình",
            "viz_type": "big_number_total",
            "params": {
                "viz_type": "big_number_total",
                "metric": get_adhoc_metric_avg("trip_distance", "Avg Distance"),
                "subheader": "Avg Trip Distance (miles)",
            },
        },
        {
            "slice_name": "KPI: Thời Gian Chuyến TB",
            "viz_type": "big_number_total",
            "params": {
                "viz_type": "big_number_total",
                "metric": get_adhoc_metric_avg("trip_duration_seconds", "Avg Duration (sec)"),
                "subheader": "Avg Trip Duration (seconds)",
            },
        },

        # ── ROW 2: Xu hướng ─────────────────────────────────
        {
            "slice_name": "Doanh Thu Theo Giờ",
            "viz_type": "dist_bar",
            "params": {
                "viz_type": "dist_bar",
                "groupby": ["pickup_hour"],
                "metrics": [get_adhoc_metric_sum("fare_amount", "Total Revenue")],
                "color_scheme": "supersetColors",
                "show_legend": True,
            },
        },
        {
            "slice_name": "Heatmap: Giờ Cao Điểm (Giờ × Thứ)",
            "viz_type": "heatmap",
            "params": {
                "viz_type": "heatmap",
                "all_columns_x": "pickup_hour",
                "all_columns_y": get_adhoc_column(day_of_week_sql, "Day of Week"),
                "metric": "count",
                "normalize_across": "heatmap",
                "left_margin": "auto",
                "bottom_margin": "auto",
                "canvas_image_rendering": "pixelated",
                "sort_x_axis": "alpha_asc",
                "sort_y_axis": "alpha_asc",
            },
        },

        # ── ROW 3: Phân tích giờ / ngày ──────────────────────────────
        {
            "slice_name": "Trip Distribution by Hour",
            "viz_type": "dist_bar",
            "params": {
                "viz_type": "dist_bar",
                "groupby": ["pickup_hour"],
                "metrics": ["count"],
                "color_scheme": "supersetColors",
            },
        },
        {
            "slice_name": "Revenue by Day of Week",
            "viz_type": "pie",
            "params": {
                "viz_type": "pie",
                "groupby": [get_adhoc_column(day_of_week_sql, "Day of Week")],
                "metric": get_adhoc_metric_sum("fare_amount", "Total Fare"),
                "color_scheme": "googleCategory10c",
                "donut": True,
                "show_labels": True,
                "labels_outside": True,
            },
        },

        # ── ROW 4: Phân tích doanh thu / hành vi ─────────────────────
        {
            "slice_name": "Avg Distance by Hour",
            "viz_type": "dist_bar",
            "params": {
                "viz_type": "dist_bar",
                "groupby": ["pickup_hour"],
                "metrics": [get_adhoc_metric_avg("trip_distance", "Avg Distance")],
                "color_scheme": "supersetColors",
            },
        },
        {
            "slice_name": "Phân Phối Fare Amount (Khoảng 5$)",
            "viz_type": "dist_bar",
            "params": {
                "viz_type": "dist_bar",
                "groupby": [get_adhoc_column("CAST(fare_amount / 5 AS INTEGER) * 5", "Fare Bracket")],
                "metrics": ["count"],
                "color_scheme": "supersetColors",
                "x_axis_label": "Fare Amount Bracket (USD)",
                "y_axis_label": "Number of Trips",
            },
        },
        {
            "slice_name": "Doanh Thu TB Theo Giờ",
            "viz_type": "dist_bar",
            "params": {
                "viz_type": "dist_bar",
                "groupby": ["pickup_hour"],
                "metrics": [get_adhoc_metric_avg("fare_amount", "Avg Fare (USD)")],
                "color_scheme": "supersetColors",
                "show_legend": True,
                "x_axis_label": "Hour of Day",
                "y_axis_label": "Avg Fare (USD)",
            },
        },

        # ── ROW 5: Top locations & hành trình ────────────────────────
        {
            "slice_name": "Top 10 Pickup Locations",
            "viz_type": "table",
            "params": {
                "viz_type": "table",
                "groupby": ["pulocation_id"],
                "metrics": [
                    "count",
                    get_adhoc_metric_sum("fare_amount", "Total Fare"),
                    get_adhoc_metric_avg("trip_distance", "Avg Distance"),
                ],
                "query_mode": "aggregate",
                "order_by_cols": [json.dumps(["count", False])],
                "page_length": 10,
                "include_search": True,
            },
        },
        {
            "slice_name": "Top Hành Trình PU → DO",
            "viz_type": "table",
            "params": {
                "viz_type": "table",
                "groupby": ["pulocation_id", "dolocation_id"],
                "metrics": [
                    "count",
                    get_adhoc_metric_sum("fare_amount", "Total Fare"),
                ],
                "query_mode": "aggregate",
                "order_by_cols": [json.dumps(["count", False])],
                "page_length": 10,
                "include_search": True,
            },
        },
        {
            "slice_name": "Doanh Thu Theo Số Hành Khách",
            "viz_type": "dist_bar",
            "params": {
                "viz_type": "dist_bar",
                "groupby": ["passenger_count"],
                "metrics": [get_adhoc_metric_sum("fare_amount", "Total Fare")],
                "color_scheme": "supersetColors",
                "x_axis_label": "Passenger Count",
                "y_axis_label": "Total Revenue (USD)",
            },
        },
    ]


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def init_dashboard():
    headers = authenticate()
    db_id = get_database_id(headers)
    dataset_id = get_or_create_dataset(headers, db_id)
    dashboard_id = create_dashboard(headers)

    chart_configs = get_chart_configs(dataset_id, dashboard_id)

    print(f"\n🎨 Creating {len(chart_configs)} charts...\n")
    created, failed = [], []

    for cfg in chart_configs:
        payload = {
            "slice_name": cfg["slice_name"],
            "viz_type": cfg["viz_type"],
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "params": json.dumps(cfg["params"]),
            "dashboards": [dashboard_id],
        }
        resp = requests.post(f"{SUPERSET_URL}/api/v1/chart/", headers=headers, json=payload)
        if resp.ok:
            print(f"  ✅ {cfg['slice_name']}")
            created.append(cfg["slice_name"])
        else:
            print(f"  ❌ {cfg['slice_name']} — {resp.status_code}: {resp.text[:120]}")
            failed.append(cfg["slice_name"])

    print("\n" + "=" * 55)
    print(f"✅ Created : {len(created)} charts")
    if failed:
        print(f"❌ Failed  : {len(failed)} charts")
        for f in failed:
            print(f"   - {f}")
    print(f"📍 Dashboard: {SUPERSET_URL}/dashboard/list/")
    print("=" * 55)


if __name__ == "__main__":
    init_dashboard()

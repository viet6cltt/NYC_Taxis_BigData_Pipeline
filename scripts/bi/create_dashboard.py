#!/usr/bin/env python3
"""
create_dashboard.py
Tự động tạo toàn bộ dashboard "NYC Taxi Gold Analytics" trên Superset
thông qua Superset REST API v1.

Bao gồm:
  - Database connection: Trino → delta catalog
  - Dataset: delta.gold.features (virtual / SQL-based)
  - 8 Charts: bar, line, pie, big_number, scatter, heatmap...
  - 1 Dashboard: layout 2 cột

Chạy: python3 scripts/bi/create_dashboard.py
"""

import json
import sys
import time
import urllib.error
import urllib.request
from typing import Any

# ── Config ────────────────────────────────────────────────────────────────────
SUPERSET_URL  = "http://localhost:8088"
TRINO_URL     = "http://localhost:8080"
ADMIN_USER    = "admin"
ADMIN_PASS    = "admin"

# Trino SQLAlchemy URI — dùng "hive" user (Trino không cần auth)
TRINO_URI = "trino://hive@localhost:8080/delta"

# ── HTTP helpers ──────────────────────────────────────────────────────────────
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
        # 422 / 409 thường là "already exists" — không fatal
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


# ── Wait helpers ──────────────────────────────────────────────────────────────
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


# ── Step 1: Database connection ───────────────────────────────────────────────
def ensure_database() -> int:
    """Tạo hoặc lấy id của Trino database connection."""
    # Tìm xem đã tồn tại chưa
    resp = api("GET", "/api/v1/database/?q=(filters:!((col:database_name,opr:eq,val:'Trino+Gold')))")
    if resp.get("count", 0) > 0:
        db_id = resp["result"][0]["id"]
        print(f"  ✓ Database 'Trino Gold' already exists (id={db_id})")
        return db_id

    payload = {
        "database_name": "Trino Gold",
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
        # Thử lấy lại
        resp2 = api("GET", "/api/v1/database/?q=(filters:!((col:database_name,opr:eq,val:'Trino+Gold')))")
        if resp2.get("count", 0) > 0:
            return resp2["result"][0]["id"]
        raise RuntimeError(f"Cannot create database: {resp}")
    db_id = resp["id"]
    print(f"  ✓ Database 'Trino Gold' created (id={db_id})")
    return db_id


# ── Step 2: Datasets (virtual / SQL) ─────────────────────────────────────────
DATASETS = {
    "gold_features": {
        "dataset_name": "gold_features",
        "sql": "SELECT * FROM delta.gold.features",
        "description": "Gold layer — feature-engineered NYC taxi trips",
    },
    "monthly_summary": {
        "dataset_name": "gold_monthly_summary",
        "sql": """
SELECT
    year_month,
    COUNT(*)                                        AS total_trips,
    ROUND(AVG(fare_amount), 2)                      AS avg_fare,
    ROUND(SUM(fare_amount), 0)                      AS total_revenue,
    ROUND(AVG(trip_distance), 2)                    AS avg_distance_miles,
    ROUND(AVG(CAST(trip_duration_seconds AS DOUBLE)) / 60.0, 1) AS avg_duration_min,
    ROUND(AVG(speed), 1)                            AS avg_speed_mph
FROM delta.gold.features
GROUP BY year_month
ORDER BY year_month
""",
        "description": "Monthly aggregated summary",
    },
    "hourly_dist": {
        "dataset_name": "gold_hourly_distribution",
        "sql": """
SELECT
    pickup_hour,
    COUNT(*)                   AS trip_count,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(AVG(speed), 1)       AS avg_speed_mph
FROM delta.gold.features
GROUP BY pickup_hour
ORDER BY pickup_hour
""",
        "description": "Trip distribution by hour of day",
    },
    "dow_dist": {
        "dataset_name": "gold_dow_distribution",
        "sql": """
SELECT
    pickup_day_of_week,
    CASE pickup_day_of_week
        WHEN 0 THEN 'Mon' WHEN 1 THEN 'Tue' WHEN 2 THEN 'Wed'
        WHEN 3 THEN 'Thu' WHEN 4 THEN 'Fri' WHEN 5 THEN 'Sat'
        ELSE 'Sun'
    END                        AS day_name,
    COUNT(*)                   AS trip_count,
    ROUND(AVG(fare_amount), 2) AS avg_fare
FROM delta.gold.features
GROUP BY pickup_day_of_week
ORDER BY pickup_day_of_week
""",
        "description": "Trip distribution by day of week",
    },
    "fare_buckets": {
        "dataset_name": "gold_fare_buckets",
        "sql": """
SELECT
    CASE
        WHEN fare_amount < 5   THEN '< $5'
        WHEN fare_amount < 10  THEN '$5-$10'
        WHEN fare_amount < 20  THEN '$10-$20'
        WHEN fare_amount < 30  THEN '$20-$30'
        WHEN fare_amount < 50  THEN '$30-$50'
        ELSE '> $50'
    END                        AS fare_bucket,
    COUNT(*)                   AS trip_count
FROM delta.gold.features
GROUP BY 1
ORDER BY MIN(fare_amount)
""",
        "description": "Fare amount distribution buckets",
    },
    "location_clusters": {
        "dataset_name": "gold_location_clusters",
        "sql": """
SELECT
    location_cluster,
    COUNT(*)                         AS trip_count,
    ROUND(AVG(fare_amount), 2)       AS avg_fare,
    ROUND(AVG(trip_distance), 2)     AS avg_distance,
    ROUND(AVG(speed), 1)             AS avg_speed
FROM delta.gold.features
GROUP BY location_cluster
ORDER BY location_cluster
""",
        "description": "Location cluster analysis",
    },
    "weekend_vs_weekday": {
        "dataset_name": "gold_weekend_weekday",
        "sql": """
SELECT
    CASE is_weekend WHEN 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(*)                         AS trip_count,
    ROUND(AVG(fare_amount), 2)       AS avg_fare,
    ROUND(AVG(trip_distance), 2)     AS avg_distance,
    ROUND(AVG(speed), 1)             AS avg_speed_mph
FROM delta.gold.features
GROUP BY is_weekend
ORDER BY is_weekend
""",
        "description": "Weekend vs Weekday comparison",
    },
}


def ensure_dataset(db_id: int, key: str) -> int:
    cfg = DATASETS[key]
    name = cfg["dataset_name"]

    # Check existing
    resp = api("GET", f"/api/v1/dataset/?q=(filters:!((col:table_name,opr:eq,val:'{name}')))")
    if resp.get("count", 0) > 0:
        ds_id = resp["result"][0]["id"]
        print(f"  ✓ Dataset '{name}' exists (id={ds_id})")
        return ds_id

    payload = {
        "database": db_id,
        "table_name": name,
        "sql": cfg["sql"].strip(),
        "schema": "gold",
        "description": cfg.get("description", ""),
        "is_managed_externally": False,
    }
    resp = api("POST", "/api/v1/dataset/", payload)
    if "_http_error" in resp:
        resp2 = api("GET", f"/api/v1/dataset/?q=(filters:!((col:table_name,opr:eq,val:'{name}')))")
        if resp2.get("count", 0) > 0:
            return resp2["result"][0]["id"]
        raise RuntimeError(f"Cannot create dataset '{name}': {resp}")
    ds_id = resp["id"]
    print(f"  ✓ Dataset '{name}' created (id={ds_id})")
    return ds_id


# ── Step 3: Charts ────────────────────────────────────────────────────────────
def make_chart(name: str, viz_type: str, ds_id: int, params: dict) -> int:
    """Tạo chart, trả về id."""
    # Check existing
    resp = api("GET", f"/api/v1/chart/?q=(filters:!((col:slice_name,opr:eq,val:'{name}')))")
    if resp.get("count", 0) > 0:
        cid = resp["result"][0]["id"]
        print(f"  ✓ Chart '{name}' exists (id={cid})")
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
        resp2 = api("GET", f"/api/v1/chart/?q=(filters:!((col:slice_name,opr:eq,val:'{name}')))")
        if resp2.get("count", 0) > 0:
            return resp2["result"][0]["id"]
        raise RuntimeError(f"Cannot create chart '{name}': {resp}")
    cid = resp["id"]
    print(f"  ✓ Chart '{name}' created (id={cid})")
    return cid


def build_charts(ds_ids: dict) -> dict:
    """Tạo tất cả charts, trả về dict name→id."""
    charts = {}

    # 1. Big Number — Total Trips
    charts["total_trips"] = make_chart(
        "🚕 Total Trips",
        "big_number_total",
        ds_ids["monthly_summary"],
        {
            "metric": {"expressionType": "SIMPLE", "column": {"column_name": "total_trips"}, "aggregate": "SUM", "label": "Total Trips"},
            "subheader": "All time",
            "y_axis_format": ",.0f",
            "header_font_size": 0.4,
        },
    )

    # 2. Big Number — Avg Fare
    charts["avg_fare"] = make_chart(
        "💵 Avg Fare (USD)",
        "big_number_total",
        ds_ids["monthly_summary"],
        {
            "metric": {"expressionType": "SIMPLE", "column": {"column_name": "avg_fare"}, "aggregate": "AVG", "label": "Avg Fare"},
            "subheader": "Average across all months",
            "y_axis_format": "$,.2f",
            "header_font_size": 0.4,
        },
    )

    # 3. Big Number — Total Revenue
    charts["total_revenue"] = make_chart(
        "💰 Total Revenue (USD)",
        "big_number_total",
        ds_ids["monthly_summary"],
        {
            "metric": {"expressionType": "SIMPLE", "column": {"column_name": "total_revenue"}, "aggregate": "SUM", "label": "Total Revenue"},
            "subheader": "Sum of all fares",
            "y_axis_format": "$,.0f",
            "header_font_size": 0.4,
        },
    )

    # 4. Bar Chart — Monthly Trips
    charts["monthly_trips"] = make_chart(
        "📅 Monthly Trip Volume",
        "echarts_timeseries_bar",
        ds_ids["monthly_summary"],
        {
            "x_axis": "year_month",
            "metrics": [
                {"expressionType": "SIMPLE", "column": {"column_name": "total_trips"}, "aggregate": "SUM", "label": "Total Trips"}
            ],
            "groupby": [],
            "x_axis_title": "Month",
            "y_axis_title": "Number of Trips",
            "color_scheme": "supersetColors",
            "show_legend": False,
            "rich_tooltip": True,
            "y_axis_format": ",.0f",
        },
    )

    # 5. Line Chart — Monthly Avg Fare trend
    charts["fare_trend"] = make_chart(
        "📈 Monthly Avg Fare Trend",
        "echarts_timeseries_line",
        ds_ids["monthly_summary"],
        {
            "x_axis": "year_month",
            "metrics": [
                {"expressionType": "SIMPLE", "column": {"column_name": "avg_fare"}, "aggregate": "AVG", "label": "Avg Fare ($)"}
            ],
            "groupby": [],
            "x_axis_title": "Month",
            "y_axis_title": "Avg Fare (USD)",
            "color_scheme": "supersetColors",
            "show_legend": False,
            "rich_tooltip": True,
            "y_axis_format": "$,.2f",
            "smooth": True,
        },
    )

    # 6. Bar Chart — Trips by Hour
    charts["hourly_trips"] = make_chart(
        "🕐 Trips by Hour of Day",
        "echarts_bar",
        ds_ids["hourly_dist"],
        {
            "x": "pickup_hour",
            "metrics": [
                {"expressionType": "SIMPLE", "column": {"column_name": "trip_count"}, "aggregate": "SUM", "label": "Trips"}
            ],
            "groupby": [],
            "x_axis_title": "Hour (0–23)",
            "y_axis_title": "Number of Trips",
            "color_scheme": "bnbColors",
            "show_legend": False,
            "rich_tooltip": True,
            "y_axis_format": ",.0f",
        },
    )

    # 7. Bar Chart — Trips by Day of Week
    charts["dow_trips"] = make_chart(
        "📆 Trips by Day of Week",
        "echarts_bar",
        ds_ids["dow_dist"],
        {
            "x": "day_name",
            "metrics": [
                {"expressionType": "SIMPLE", "column": {"column_name": "trip_count"}, "aggregate": "SUM", "label": "Trips"}
            ],
            "groupby": [],
            "x_axis_title": "Day",
            "y_axis_title": "Number of Trips",
            "color_scheme": "bnbColors",
            "show_legend": False,
            "rich_tooltip": True,
            "y_axis_format": ",.0f",
        },
    )

    # 8. Pie Chart — Fare Distribution
    charts["fare_pie"] = make_chart(
        "🥧 Fare Distribution",
        "pie",
        ds_ids["fare_buckets"],
        {
            "groupby": ["fare_bucket"],
            "metric": {"expressionType": "SIMPLE", "column": {"column_name": "trip_count"}, "aggregate": "SUM", "label": "Trips"},
            "color_scheme": "supersetColors",
            "show_legend": True,
            "show_labels": True,
            "label_type": "key_percent",
            "donut": True,
            "innerRadius": 40,
            "outerRadius": 70,
        },
    )

    # 9. Bar Chart — Location Clusters
    charts["location_clusters"] = make_chart(
        "📍 Avg Fare by Location Cluster",
        "echarts_bar",
        ds_ids["location_clusters"],
        {
            "x": "location_cluster",
            "metrics": [
                {"expressionType": "SIMPLE", "column": {"column_name": "avg_fare"}, "aggregate": "AVG", "label": "Avg Fare ($)"},
                {"expressionType": "SIMPLE", "column": {"column_name": "trip_count"}, "aggregate": "SUM", "label": "Trips"},
            ],
            "groupby": [],
            "x_axis_title": "Location Cluster",
            "y_axis_title": "Value",
            "color_scheme": "supersetColors",
            "show_legend": True,
            "rich_tooltip": True,
            "y_axis_format": ",.2f",
        },
    )

    # 10. Bar Chart — Weekend vs Weekday
    charts["weekend_weekday"] = make_chart(
        "🗓️ Weekend vs Weekday",
        "echarts_bar",
        ds_ids["weekend_vs_weekday"],
        {
            "x": "day_type",
            "metrics": [
                {"expressionType": "SIMPLE", "column": {"column_name": "avg_fare"}, "aggregate": "AVG", "label": "Avg Fare ($)"},
                {"expressionType": "SIMPLE", "column": {"column_name": "avg_speed_mph"}, "aggregate": "AVG", "label": "Avg Speed (mph)"},
            ],
            "groupby": [],
            "x_axis_title": "Day Type",
            "y_axis_title": "Value",
            "color_scheme": "bnbColors",
            "show_legend": True,
            "rich_tooltip": True,
        },
    )

    return charts


# ── Step 4: Dashboard ─────────────────────────────────────────────────────────
def build_dashboard_layout(chart_ids: dict) -> dict:
    """
    Tạo layout JSON cho dashboard.
    Superset dùng cấu trúc cây: ROOT → GRID → ROW → CHART
    """
    cids = chart_ids  # shorthand

    # Mỗi CHART component cần id duy nhất
    def chart_component(uid: str, chart_id: int, w: int = 6, h: int = 8) -> dict:
        return {
            "id": uid,
            "type": "CHART",
            "meta": {
                "chartId": chart_id,
                "width": w,
                "height": h,
                "sliceName": "",
            },
            "children": [],
            "parents": [],
        }

    def row_component(uid: str, children: list) -> dict:
        return {
            "id": uid,
            "type": "ROW",
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
            "children": children,
            "parents": ["ROOT_ID", "GRID_ID"],
        }

    layout = {
        "ROOT_ID": {
            "id": "ROOT_ID",
            "type": "ROOT",
            "children": ["GRID_ID"],
            "parents": [],
        },
        "GRID_ID": {
            "id": "GRID_ID",
            "type": "GRID",
            "children": ["ROW-kpi", "ROW-monthly", "ROW-time", "ROW-dist", "ROW-cluster"],
            "parents": ["ROOT_ID"],
        },
        # Row 1: KPI big numbers (3 cards)
        "ROW-kpi": row_component("ROW-kpi", ["CHART-total-trips", "CHART-avg-fare", "CHART-revenue"]),
        "CHART-total-trips": chart_component("CHART-total-trips", cids["total_trips"],  w=4, h=5),
        "CHART-avg-fare":    chart_component("CHART-avg-fare",    cids["avg_fare"],     w=4, h=5),
        "CHART-revenue":     chart_component("CHART-revenue",     cids["total_revenue"],w=4, h=5),
        # Row 2: Monthly trends
        "ROW-monthly": row_component("ROW-monthly", ["CHART-monthly-trips", "CHART-fare-trend"]),
        "CHART-monthly-trips": chart_component("CHART-monthly-trips", cids["monthly_trips"], w=6, h=9),
        "CHART-fare-trend":    chart_component("CHART-fare-trend",    cids["fare_trend"],    w=6, h=9),
        # Row 3: Time distribution
        "ROW-time": row_component("ROW-time", ["CHART-hourly", "CHART-dow"]),
        "CHART-hourly": chart_component("CHART-hourly", cids["hourly_trips"], w=6, h=9),
        "CHART-dow":    chart_component("CHART-dow",    cids["dow_trips"],    w=6, h=9),
        # Row 4: Fare distribution + Weekend
        "ROW-dist": row_component("ROW-dist", ["CHART-fare-pie", "CHART-weekend"]),
        "CHART-fare-pie": chart_component("CHART-fare-pie", cids["fare_pie"],        w=6, h=9),
        "CHART-weekend":  chart_component("CHART-weekend",  cids["weekend_weekday"], w=6, h=9),
        # Row 5: Location clusters
        "ROW-cluster": row_component("ROW-cluster", ["CHART-location"]),
        "CHART-location": chart_component("CHART-location", cids["location_clusters"], w=12, h=9),
        # Header
        "HEADER_ID": {
            "id": "HEADER_ID",
            "type": "HEADER",
            "meta": {"text": "NYC Taxi Gold Analytics"},
        },
    }
    return layout


def ensure_dashboard(chart_ids: dict) -> int:
    title = "NYC Taxi Gold Analytics"

    # Check existing
    resp = api("GET", f"/api/v1/dashboard/?q=(filters:!((col:dashboard_title,opr:eq,val:'{title}')))")
    if resp.get("count", 0) > 0:
        did = resp["result"][0]["id"]
        print(f"  ✓ Dashboard '{title}' exists (id={did}) — updating charts...")
        # Update layout với chart ids mới nhất
        layout = build_dashboard_layout(chart_ids)
        api("PUT", f"/api/v1/dashboard/{did}", {
            "position_json": json.dumps(layout),
            "published": True,
        })
        return did

    layout = build_dashboard_layout(chart_ids)
    payload = {
        "dashboard_title": title,
        "slug": "nyc-taxi-gold",
        "published": True,
        "position_json": json.dumps(layout),
        "metadata": json.dumps({
            "color_scheme": "supersetColors",
            "refresh_frequency": 0,
            "expanded_slices": {},
            "default_filters": "{}",
        }),
    }
    resp = api("POST", "/api/v1/dashboard/", payload)
    if "_http_error" in resp:
        resp2 = api("GET", f"/api/v1/dashboard/?q=(filters:!((col:dashboard_title,opr:eq,val:'{title}')))")
        if resp2.get("count", 0) > 0:
            return resp2["result"][0]["id"]
        raise RuntimeError(f"Cannot create dashboard: {resp}")
    did = resp["id"]
    print(f"  ✓ Dashboard '{title}' created (id={did})")
    return did


def add_charts_to_dashboard(dashboard_id: int, chart_ids: dict):
    """Gắn tất cả charts vào dashboard."""
    ids = list(chart_ids.values())
    resp = api("PUT", f"/api/v1/dashboard/{dashboard_id}", {
        "charts": ids,
    })
    if "_http_error" not in resp:
        print(f"  ✓ {len(ids)} charts linked to dashboard")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("\n" + "═"*60)
    print("  NYC Taxi — Superset Dashboard Creator")
    print("═"*60)

    # 1. Wait for services
    print("\n[0] Checking services...")
    if not wait_http(f"{SUPERSET_URL}/health", "Superset"):
        print("\n  ✗ Superset not reachable.")
        print("    Start with: docker compose -f docker-compose.dev.yml up -d superset")
        sys.exit(1)

    trino_up = wait_http(f"{TRINO_URL}/v1/info", "Trino", retries=10, delay=3)
    if not trino_up:
        print("  ⚠  Trino not running — database connection will be created but queries may fail")
        print("     Start with: docker compose -f docker-compose.dev.yml up -d trino")

    # 2. Login
    print("\n[1] Logging in to Superset...")
    global _token
    for attempt in range(5):
        try:
            _token = login()
            print(f"  ✓ Authenticated as '{ADMIN_USER}'")
            break
        except Exception as e:
            if attempt == 4:
                print(f"  ✗ Login failed: {e}")
                sys.exit(1)
            print(f"  Retry {attempt+1}/5...")
            time.sleep(5)

    # 3. Database
    print("\n[2] Setting up Trino database connection...")
    db_id = ensure_database()

    # 4. Datasets
    print("\n[3] Creating datasets...")
    ds_ids = {}
    for key in DATASETS:
        ds_ids[key] = ensure_dataset(db_id, key)

    # 5. Charts
    print("\n[4] Creating charts...")
    chart_ids = build_charts(ds_ids)

    # 6. Dashboard
    print("\n[5] Building dashboard...")
    dashboard_id = ensure_dashboard(chart_ids)
    add_charts_to_dashboard(dashboard_id, chart_ids)

    # Done
    print("\n" + "═"*60)
    print("  ✅  Dashboard ready!")
    print("═"*60)
    print(f"\n  URL: {SUPERSET_URL}/superset/dashboard/nyc-taxi-gold/")
    print(f"  Or:  {SUPERSET_URL}/dashboard/list  → 'NYC Taxi Gold Analytics'")
    print(f"\n  Login: {SUPERSET_URL}  (admin / admin)")
    print()


if __name__ == "__main__":
    main()

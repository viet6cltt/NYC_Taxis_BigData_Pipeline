import { useEffect, useMemo, useRef, useState } from "react";
import * as XLSX from "xlsx";
import {
  Activity,
  ArrowLeftRight,
  BrainCircuit,
  Calculator,
  ClipboardPaste,
  Database,
  DatabaseZap,
  FileCode2,
  GitMerge,
  Pause,
  Play,
  RadioTower,
  RefreshCw,
  RotateCcw,
  Route,
  TableProperties,
  Upload,
} from "lucide-react";

const dayNames = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
const money = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" });
const mapWidth = 920;
const mapHeight = 620;
const mapPadding = 24;
const routeDistanceMultiplier = 1.35;
const routeAverageSpeedMph = 13.5;
const boroughColors = {
  Bronx: "#dbeafe",
  Brooklyn: "#dcfce7",
  EWR: "#f1f5f9",
  Manhattan: "#fef3c7",
  Queens: "#ede9fe",
  "Staten Island": "#fee2e2",
};

const initialTrip = {
  passenger_count: 2,
  estimated_trip_distance: 3.5,
  duration_minutes: 15,
  pickup_hour: 14,
  pickup_day_of_week: 2,
  pulocation_id: 161,
  dolocation_id: 236,
};

const presets = {
  airport: {
    passenger_count: 2,
    estimated_trip_distance: 16.2,
    duration_minutes: 48,
    pickup_hour: 17,
    pickup_day_of_week: 4,
    pulocation_id: 132,
    dolocation_id: 230,
  },
  midtown: {
    passenger_count: 1,
    estimated_trip_distance: 3.1,
    duration_minutes: 18,
    pickup_hour: 9,
    pickup_day_of_week: 1,
    pulocation_id: 186,
    dolocation_id: 236,
  },
  downtown: {
    passenger_count: 3,
    estimated_trip_distance: 2.2,
    duration_minutes: 12,
    pickup_hour: 21,
    pickup_day_of_week: 5,
    pulocation_id: 211,
    dolocation_id: 87,
  },
};

const sampleRows = `pickup_datetime,passenger_count,estimated_trip_distance,duration_minutes,pulocation_id,dolocation_id,actual_fare_amount
2024-01-01 08:05:00,1,3.4,16,161,236,18.2
2024-01-01 08:08:00,2,12.1,36,132,230,54.7
2024-01-01 08:13:00,1,2.2,11,211,87,11.8
2024-01-01 08:15:00,3,4.9,22,186,237,24.5
2024-01-01 08:20:00,1,1.8,9,234,68,10.1`;

const defaultInputColumns = [
  "pickup_datetime",
  "passenger_count",
  "estimated_trip_distance",
  "duration_minutes",
  "pulocation_id",
  "dolocation_id",
  "actual_fare_amount",
];

const knownInputColumns = new Set([
  "pickup_datetime",
  "pickup_time",
  "tpep_pickup_datetime",
  "passenger_count",
  "passengers",
  "estimated_trip_distance",
  "trip_distance",
  "distance",
  "duration_minutes",
  "estimated_duration_minutes",
  "minutes",
  "estimated_trip_duration_seconds",
  "duration_seconds",
  "trip_duration_seconds",
  "pickup_hour",
  "hour",
  "pickup_day_of_week",
  "day_of_week",
  "dow",
  "pulocation_id",
  "PULocationID",
  "pickup_zone_id",
  "dolocation_id",
  "DOLocationID",
  "dropoff_zone_id",
  "actual_fare_amount",
  "fare_amount",
  "actual_fare",
]);

function formatMoney(value) {
  const number = Number(value);
  return Number.isFinite(number) ? money.format(number) : "--";
}

function formatNumber(value, digits = 2) {
  const number = Number(value);
  return Number.isFinite(number) ? number.toFixed(digits) : "--";
}

function normalizeDateTime(value) {
  if (!value) return null;
  if (value instanceof Date) return value;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function pickupDayFromDate(value) {
  const date = normalizeDateTime(value);
  return date ? (date.getDay() + 6) % 7 : 0;
}

function pickupHourFromDate(value) {
  const date = normalizeDateTime(value);
  return date ? date.getHours() : 12;
}

function firstValue(row, keys) {
  for (const key of keys) {
    if (row[key] !== undefined && row[key] !== null && row[key] !== "") {
      return row[key];
    }
  }
  return undefined;
}

function toNumber(value, fallback = 0) {
  if (value === undefined || value === null || value === "") return fallback;
  const parsed = Number(String(value).replace(",", "."));
  return Number.isFinite(parsed) ? parsed : fallback;
}

function normalizeUploadRows(rows) {
  return rows
    .map((row, index) => {
      const pickupDatetime = firstValue(row, ["pickup_datetime", "pickup_time", "tpep_pickup_datetime"]);
      const pickupHour = toNumber(
        firstValue(row, ["pickup_hour", "hour"]),
        pickupHourFromDate(pickupDatetime),
      );
      const pickupDay = toNumber(
        firstValue(row, ["pickup_day_of_week", "day_of_week", "dow"]),
        pickupDayFromDate(pickupDatetime),
      );
      const durationSeconds = toNumber(
        firstValue(row, ["estimated_trip_duration_seconds", "duration_seconds", "trip_duration_seconds"]),
        toNumber(firstValue(row, ["duration_minutes", "estimated_duration_minutes", "minutes"]), 15) * 60,
      );

      return {
        source: "uploaded",
        row_number: index + 1,
        pickup_datetime: pickupDatetime || `row ${index + 1}`,
        passenger_count: Math.max(1, Math.min(6, toNumber(firstValue(row, ["passenger_count", "passengers"]), 1))),
        estimated_trip_distance: toNumber(firstValue(row, ["estimated_trip_distance", "trip_distance", "distance"]), 1),
        estimated_trip_duration_seconds: Math.max(60, durationSeconds),
        pickup_hour: Math.max(0, Math.min(23, pickupHour)),
        pickup_day_of_week: Math.max(0, Math.min(6, pickupDay)),
        pulocation_id: toNumber(firstValue(row, ["pulocation_id", "PULocationID", "pickup_zone_id"]), 161),
        dolocation_id: toNumber(firstValue(row, ["dolocation_id", "DOLocationID", "dropoff_zone_id"]), 236),
        actual_fare_amount: toNumber(firstValue(row, ["actual_fare_amount", "fare_amount", "actual_fare"]), NaN),
      };
    })
    .filter((row) => row.estimated_trip_distance > 0 && row.pulocation_id > 0 && row.dolocation_id > 0);
}

function rowsFromMatrix(matrix) {
  const cleanRows = matrix
    .map((row) => row.map((cell) => (cell === null || cell === undefined ? "" : String(cell).trim())))
    .filter((row) => row.some(Boolean));

  if (cleanRows.length === 0) return [];

  const firstRow = cleanRows[0].map((cell) => cell.trim());
  const hasHeader = firstRow.some((cell) => knownInputColumns.has(cell));
  const headers = hasHeader ? firstRow : defaultInputColumns;
  const dataRows = hasHeader ? cleanRows.slice(1) : cleanRows;

  return dataRows.map((row) =>
    Object.fromEntries(headers.map((header, index) => [header, row[index] ?? ""])),
  );
}

async function readWorkbook(file) {
  const data = await file.arrayBuffer();
  const workbook = XLSX.read(data, { type: "array", cellDates: true });
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  const matrix = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: "", raw: false });
  return rowsFromMatrix(matrix);
}

function readPastedTable(text) {
  const rows = text
    .trim()
    .split(/\r?\n/)
    .map((line) => line.split(line.includes("\t") ? "\t" : ","));
  return rowsFromMatrix(rows);
}

function buildPredictPayload(row) {
  return {
    passenger_count: Number(row.passenger_count),
    estimated_trip_distance: Number(row.estimated_trip_distance),
    estimated_trip_duration_seconds: Number(row.estimated_trip_duration_seconds),
    pickup_hour: Number(row.pickup_hour),
    pickup_day_of_week: Number(row.pickup_day_of_week),
    pulocation_id: Number(row.pulocation_id),
    dolocation_id: Number(row.dolocation_id),
  };
}

function apiErrorMessage(body) {
  if (!body) return "Prediction failed";
  if (typeof body.detail === "string") return body.detail;
  if (Array.isArray(body.detail)) {
    return body.detail
      .map((item) => {
        const loc = Array.isArray(item.loc) ? item.loc.join(".") : "field";
        return `${loc}: ${item.msg || "invalid value"}`;
      })
      .join("; ");
  }
  return body.message || "Prediction failed";
}

function computeQuality(records) {
  const actualRecords = records.filter((item) => Number.isFinite(Number(item.actual_fare_amount)));
  if (actualRecords.length === 0) {
    return { count: records.length, mae: null, rmse: null, bias: null };
  }
  const errors = actualRecords.map((item) => Number(item.predicted_fare_amount) - Number(item.actual_fare_amount));
  const abs = errors.map((value) => Math.abs(value));
  const squared = errors.map((value) => value * value);
  return {
    count: records.length,
    mae: abs.reduce((sum, value) => sum + value, 0) / abs.length,
    rmse: Math.sqrt(squared.reduce((sum, value) => sum + value, 0) / squared.length),
    bias: errors.reduce((sum, value) => sum + value, 0) / errors.length,
  };
}

function featureLocationId(feature) {
  return Number(feature?.properties?.locationid || feature?.properties?.LocationID || feature?.properties?.id);
}

function getGeometryRings(geometry) {
  if (!geometry) return [];
  if (geometry.type === "Polygon") return geometry.coordinates;
  if (geometry.type === "MultiPolygon") return geometry.coordinates.flat();
  return [];
}

function coordinateBounds(features) {
  const bounds = {
    minLon: Infinity,
    maxLon: -Infinity,
    minLat: Infinity,
    maxLat: -Infinity,
  };

  for (const feature of features) {
    for (const ring of getGeometryRings(feature.geometry)) {
      for (const [lon, lat] of ring) {
        bounds.minLon = Math.min(bounds.minLon, lon);
        bounds.maxLon = Math.max(bounds.maxLon, lon);
        bounds.minLat = Math.min(bounds.minLat, lat);
        bounds.maxLat = Math.max(bounds.maxLat, lat);
      }
    }
  }

  return bounds;
}

function createMapProjector(features) {
  const bounds = coordinateBounds(features);
  const lonRange = bounds.maxLon - bounds.minLon || 1;
  const latRange = bounds.maxLat - bounds.minLat || 1;
  const scale = Math.min((mapWidth - mapPadding * 2) / lonRange, (mapHeight - mapPadding * 2) / latRange);
  const renderedWidth = lonRange * scale;
  const renderedHeight = latRange * scale;
  const offsetX = (mapWidth - renderedWidth) / 2;
  const offsetY = (mapHeight - renderedHeight) / 2;

  return ([lon, lat]) => [
    offsetX + (lon - bounds.minLon) * scale,
    offsetY + (bounds.maxLat - lat) * scale,
  ];
}

function ringToPath(ring, project) {
  return ring
    .map((point, index) => {
      const [x, y] = project(point);
      return `${index === 0 ? "M" : "L"}${x.toFixed(1)} ${y.toFixed(1)}`;
    })
    .join(" ");
}

function featureToPath(feature, project) {
  return `${getGeometryRings(feature.geometry).map((ring) => ringToPath(ring, project)).join(" Z ")} Z`;
}

function featureCenter(feature, project) {
  const points = getGeometryRings(feature?.geometry).flat();
  if (points.length === 0) return null;

  const totals = points.reduce(
    (sum, [lon, lat]) => ({ lon: sum.lon + lon, lat: sum.lat + lat }),
    { lon: 0, lat: 0 },
  );
  return project([totals.lon / points.length, totals.lat / points.length]);
}

function featureGeoCenter(feature) {
  const points = getGeometryRings(feature?.geometry).flat();
  if (points.length === 0) return null;

  const totals = points.reduce(
    (sum, [lon, lat]) => ({ lon: sum.lon + lon, lat: sum.lat + lat }),
    { lon: 0, lat: 0 },
  );
  return [totals.lon / points.length, totals.lat / points.length];
}

function haversineMiles(start, end) {
  const toRadians = (degrees) => (degrees * Math.PI) / 180;
  const earthRadiusMiles = 3958.8;
  const [lon1, lat1] = start;
  const [lon2, lat2] = end;
  const dLat = toRadians(lat2 - lat1);
  const dLon = toRadians(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRadians(lat1)) * Math.cos(toRadians(lat2)) * Math.sin(dLon / 2) ** 2;
  return earthRadiusMiles * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function estimateFromZoneGeometry(features, pickupId, dropoffId) {
  const pickup = featureGeoCenter(features.find((feature) => featureLocationId(feature) === Number(pickupId)));
  const dropoff = featureGeoCenter(features.find((feature) => featureLocationId(feature) === Number(dropoffId)));
  if (!pickup || !dropoff) return null;

  const directDistance = haversineMiles(pickup, dropoff);
  const estimatedDistance = Math.max(0.3, directDistance * routeDistanceMultiplier + 0.2);
  const durationMinutes = Math.max(3, Math.round((estimatedDistance / routeAverageSpeedMph) * 60));

  return {
    distance: Number(estimatedDistance.toFixed(1)),
    durationMinutes,
  };
}

function TaxiZoneMap({ features, pickupId, dropoffId, activeEndpoint, onSelect }) {
  const { paths, pickupPoint, dropoffPoint } = useMemo(() => {
    if (!features.length) return { paths: [], pickupPoint: null, dropoffPoint: null };

    const project = createMapProjector(features);
    return {
      paths: features.map((feature) => ({
        feature,
        id: featureLocationId(feature),
        path: featureToPath(feature, project),
        center: featureCenter(feature, project),
      })),
      pickupPoint: featureCenter(features.find((feature) => featureLocationId(feature) === Number(pickupId)), project),
      dropoffPoint: featureCenter(features.find((feature) => featureLocationId(feature) === Number(dropoffId)), project),
    };
  }, [features, pickupId, dropoffId]);

  if (!features.length) {
    return <div className="map-empty">NYC taxi zone map loading</div>;
  }

  return (
    <svg className="taxi-zone-map" viewBox={`0 0 ${mapWidth} ${mapHeight}`} role="img" aria-label="NYC taxi zone selector">
      <rect className="map-water" x="0" y="0" width={mapWidth} height={mapHeight} />
      {paths.map(({ feature, id, path }) => {
        const selected = id === Number(pickupId) || id === Number(dropoffId);
        const title = `${feature.properties.borough} - ${feature.properties.zone}`;
        return (
          <path
            key={id}
            className={`zone-shape ${selected ? "selected" : ""} ${id === Number(pickupId) ? "pickup" : ""} ${id === Number(dropoffId) ? "dropoff" : ""}`}
            d={path}
            fill={boroughColors[feature.properties.borough] || "#e2e8f0"}
            onClick={() => onSelect(id)}
            tabIndex="0"
            role="button"
            aria-label={`${activeEndpoint === "pickup" ? "Set pickup" : "Set dropoff"}: ${title}`}
            onKeyDown={(event) => {
              if (event.key === "Enter" || event.key === " ") {
                event.preventDefault();
                onSelect(id);
              }
            }}
          >
            <title>{title}</title>
          </path>
        );
      })}
      {pickupPoint && dropoffPoint && (
        <line className="map-route-line" x1={pickupPoint[0]} y1={pickupPoint[1]} x2={dropoffPoint[0]} y2={dropoffPoint[1]} />
      )}
      {pickupPoint && (
        <g className="map-pin pickup" transform={`translate(${pickupPoint[0]} ${pickupPoint[1]})`}>
          <circle r="11" />
          <text y="4">PU</text>
        </g>
      )}
      {dropoffPoint && (
        <g className="map-pin dropoff" transform={`translate(${dropoffPoint[0]} ${dropoffPoint[1]})`}>
          <circle r="11" />
          <text y="4">DO</text>
        </g>
      )}
    </svg>
  );
}

function Sidebar({ page, setPage }) {
  return (
    <aside className="sidebar" aria-label="Workspace navigation">
      <div className="brand">
        <div className="brand-mark">TX</div>
        <div>
          <strong>TaxiOps</strong>
          <span>React Console</span>
        </div>
      </div>

      <nav className="nav-list" aria-label="Primary">
        <button className={`nav-item ${page === "pricing" ? "active" : ""}`} onClick={() => setPage("pricing")}>
          <Route />
          <span>Pricing</span>
        </button>
        <button className={`nav-item ${page === "streaming" ? "active" : ""}`} onClick={() => setPage("streaming")}>
          <Activity />
          <span>Streaming</span>
        </button>
        <a className="nav-item link-item" href="http://localhost:5000" target="_blank" rel="noreferrer">
          <Database />
          <span>MLflow</span>
        </a>
        <a className="nav-item link-item" href="/docs" target="_blank" rel="noreferrer">
          <FileCode2 />
          <span>API Docs</span>
        </a>
      </nav>

      <div className="sidebar-status">
        <span className="status-dot ok" />
        <div>
          <strong>Vite frontend</strong>
          <span>FastAPI proxy active</span>
        </div>
      </div>
    </aside>
  );
}

function PricingPage() {
  const [zones, setZones] = useState([]);
  const [mapFeatures, setMapFeatures] = useState([]);
  const [activeEndpoint, setActiveEndpoint] = useState("pickup");
  const [health, setHealth] = useState({ status: "pending", model_name: "XGB_NYC_Fare", model_version: "unknown" });
  const [trip, setTrip] = useState(initialTrip);
  const [result, setResult] = useState(null);
  const [message, setMessage] = useState("");
  const [latency, setLatency] = useState(null);
  const [history, setHistory] = useState([]);

  async function refreshHealth() {
    const response = await fetch("/health");
    setHealth(await response.json());
  }

  useEffect(() => {
    fetch("/zones")
      .then((response) => response.json())
      .then((payload) => setZones(payload.zones || []))
      .catch(() => setZones([]));
    fetch("/data/taxi_zones.geojson")
      .then((response) => response.json())
      .then((payload) => setMapFeatures(payload.features || []))
      .catch(() => setMapFeatures([]));
    refreshHealth().catch(() => setHealth({ status: "offline", model_name: "XGB_NYC_Fare", model_version: "unknown" }));
  }, []);

  const zoneName = (id) => zones.find((zone) => Number(zone.id) === Number(id))?.zone || `Zone ${id}`;
  const zoneLabel = (id) => zones.find((zone) => Number(zone.id) === Number(id))?.label || `Zone ${id}`;
  const speed = trip.estimated_trip_distance / (trip.duration_minutes / 60);
  const zoneDelta = Math.abs(Number(trip.dolocation_id) - Number(trip.pulocation_id));

  useEffect(() => {
    const pickupId = Number(trip.pulocation_id);
    const dropoffId = Number(trip.dolocation_id);
    const pickupHour = Number(trip.pickup_hour);
    const pickupDay = Number(trip.pickup_day_of_week);

    if (!pickupId || !dropoffId) return undefined;

    const controller = new AbortController();

    async function loadRouteEstimate() {
      let estimate = null;
      const params = new URLSearchParams({
        pulocation_id: String(pickupId),
        dolocation_id: String(dropoffId),
        pickup_hour: String(pickupHour),
        pickup_day_of_week: String(pickupDay),
      });

      try {
        const response = await fetch(`/route-estimate?${params.toString()}`, { signal: controller.signal });
        if (response.ok) {
          const body = await response.json();
          if (body.estimate_level !== "global" || mapFeatures.length === 0) {
            estimate = {
              distance: Number(body.estimated_trip_distance),
              durationMinutes: Number(body.duration_minutes || body.estimated_trip_duration_seconds / 60),
            };
          }
        }
      } catch (error) {
        if (error.name === "AbortError") return;
      }

      if (!estimate && mapFeatures.length > 0) {
        estimate = estimateFromZoneGeometry(mapFeatures, pickupId, dropoffId);
      }

      if (!estimate || !Number.isFinite(estimate.distance) || !Number.isFinite(estimate.durationMinutes)) {
        return;
      }

      const nextDistance = Number(estimate.distance.toFixed(1));
      const nextDuration = Math.max(1, Math.round(estimate.durationMinutes));

      setTrip((current) => {
        const sameRoute =
          Number(current.pulocation_id) === pickupId &&
          Number(current.dolocation_id) === dropoffId &&
          Number(current.pickup_hour) === pickupHour &&
          Number(current.pickup_day_of_week) === pickupDay;
        const alreadyCurrent =
          Number(current.estimated_trip_distance) === nextDistance &&
          Number(current.duration_minutes) === nextDuration;

        if (!sameRoute || alreadyCurrent) return current;

        return {
          ...current,
          estimated_trip_distance: nextDistance,
          duration_minutes: nextDuration,
        };
      });
    }

    loadRouteEstimate();
    return () => controller.abort();
  }, [trip.pulocation_id, trip.dolocation_id, trip.pickup_hour, trip.pickup_day_of_week, mapFeatures]);

  function updateTrip(key, value) {
    setTrip((current) => ({ ...current, [key]: value }));
    setResult(null);
  }

  function selectMapZone(zoneId) {
    const key = activeEndpoint === "pickup" ? "pulocation_id" : "dolocation_id";
    setTrip((current) => ({ ...current, [key]: zoneId }));
    setResult(null);
    if (activeEndpoint === "pickup") {
      setActiveEndpoint("dropoff");
    }
  }

  async function submit(event) {
    event.preventDefault();
    const started = performance.now();
    setMessage("Sending trip to model");
    const response = await fetch("/predict", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        passenger_count: Number(trip.passenger_count),
        estimated_trip_distance: Number(trip.estimated_trip_distance),
        estimated_trip_duration_seconds: Number(trip.duration_minutes) * 60,
        pickup_hour: Number(trip.pickup_hour),
        pickup_day_of_week: Number(trip.pickup_day_of_week),
        pulocation_id: Number(trip.pulocation_id),
        dolocation_id: Number(trip.dolocation_id),
      }),
    });
    const body = await response.json();
    if (!response.ok) {
      setMessage(body.detail || "Prediction failed");
      return;
    }
    const elapsed = Math.round(performance.now() - started);
    setResult(body);
    setLatency(elapsed);
    setMessage(`${body.model_name} v${body.model_version} returned ${formatMoney(body.predicted_fare)}`);
    setHealth((current) => ({ ...current, status: "ok", model_version: body.model_version }));
    setHistory((items) => [
      {
        time: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }),
        route: `${zoneName(trip.pulocation_id)} -> ${zoneName(trip.dolocation_id)}`,
        trip: `${Number(trip.estimated_trip_distance).toFixed(1)} mi, ${trip.duration_minutes} min`,
        fare: formatMoney(body.predicted_fare),
      },
      ...items,
    ].slice(0, 8));
  }

  return (
    <main className="main-area">
      <header className="topbar">
        <div>
          <p className="eyebrow">Production Pricing</p>
          <h1>Trip Fare Workspace</h1>
        </div>
        <div className="topbar-actions">
          <button className="ghost-button" type="button" onClick={refreshHealth}>
            <RefreshCw />
            <span>Refresh</span>
          </button>
        </div>
      </header>

      <section className="overview-grid">
        <div className="summary-tile">
          <span>Service</span>
          <strong>{health.status === "ok" ? "Online" : health.status}</strong>
        </div>
        <div className="summary-tile">
          <span>Model</span>
          <strong>{health.model_name}</strong>
        </div>
        <div className="summary-tile">
          <span>Version</span>
          <strong>{health.model_version}</strong>
        </div>
        <div className="summary-tile">
          <span>Last latency</span>
          <strong>{latency ? `${latency} ms` : "-- ms"}</strong>
        </div>
      </section>

      <section className="workspace-grid">
        <section className="panel trip-panel">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">Quote Builder</p>
              <h2>Trip details</h2>
            </div>
            <button className="icon-button" type="button" onClick={() => setTrip(initialTrip)} aria-label="Reset">
              <RotateCcw />
            </button>
          </div>

          <div className="preset-row">
            {Object.entries(presets).map(([key, preset]) => (
              <button className="preset-chip" type="button" key={key} onClick={() => setTrip(preset)}>
                {key === "airport" ? "JFK to Times Sq" : key === "midtown" ? "Penn to UES" : "SoHo to FiDi"}
              </button>
            ))}
          </div>

          <form className="trip-form" onSubmit={submit}>
            <div className="field-grid">
              <label className="field">
                <span>Passengers</span>
                <input type="number" min="1" max="6" value={trip.passenger_count} onChange={(event) => updateTrip("passenger_count", event.target.value)} />
              </label>
              <label className="field">
                <span>Pickup hour</span>
                <div className="range-field">
                  <input type="range" min="0" max="23" value={trip.pickup_hour} onChange={(event) => updateTrip("pickup_hour", event.target.value)} />
                  <strong>{String(trip.pickup_hour).padStart(2, "0")}:00</strong>
                </div>
              </label>
              <label className="field">
                <span>Day</span>
                <select value={trip.pickup_day_of_week} onChange={(event) => updateTrip("pickup_day_of_week", event.target.value)}>
                  {dayNames.map((day, index) => (
                    <option value={index} key={day}>{day}</option>
                  ))}
                </select>
              </label>
            </div>

            <div className="route-fields">
              <label className="field">
                <span>Pickup zone</span>
                <select value={trip.pulocation_id} onChange={(event) => updateTrip("pulocation_id", event.target.value)}>
                  {zones.map((zone) => <option key={zone.id} value={zone.id}>{zone.label}</option>)}
                </select>
              </label>
              <button className="swap-button" type="button" onClick={() => setTrip((current) => ({ ...current, pulocation_id: current.dolocation_id, dolocation_id: current.pulocation_id }))}>
                <ArrowLeftRight />
              </button>
              <label className="field">
                <span>Dropoff zone</span>
                <select value={trip.dolocation_id} onChange={(event) => updateTrip("dolocation_id", event.target.value)}>
                  {zones.map((zone) => <option key={zone.id} value={zone.id}>{zone.label}</option>)}
                </select>
              </label>
            </div>

            <div className="location-picker">
              <div className="location-picker-toolbar">
                <div className="segmented-control" aria-label="Map selection target">
                  <button className={activeEndpoint === "pickup" ? "active" : ""} type="button" onClick={() => setActiveEndpoint("pickup")}>
                    Pickup
                  </button>
                  <button className={activeEndpoint === "dropoff" ? "active" : ""} type="button" onClick={() => setActiveEndpoint("dropoff")}>
                    Dropoff
                  </button>
                </div>
                <div className="route-estimate-readout">
                  <span>Route estimate</span>
                  <strong>{formatNumber(trip.estimated_trip_distance, 1)} mi / {Math.round(Number(trip.duration_minutes) || 0)} min</strong>
                </div>
                <div className="selected-route">
                  <span>PU</span>
                  <strong>{zoneLabel(trip.pulocation_id)}</strong>
                  <span>DO</span>
                  <strong>{zoneLabel(trip.dolocation_id)}</strong>
                </div>
              </div>
              <TaxiZoneMap
                features={mapFeatures}
                pickupId={trip.pulocation_id}
                dropoffId={trip.dolocation_id}
                activeEndpoint={activeEndpoint}
                onSelect={selectMapZone}
              />
            </div>

            <div className="form-actions">
              <button className="primary-button" type="submit">
                <Calculator />
                <span>Estimate fare</span>
              </button>
              <p>{message}</p>
            </div>
          </form>
        </section>

        <section className="panel result-panel">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">Live Quote</p>
              <h2>Fare estimate</h2>
            </div>
            <span className="quote-badge">{result ? "Quoted" : "Ready"}</span>
          </div>
          <div className="fare-readout">
            <span>$</span>
            <strong>{result ? formatNumber(result.predicted_fare) : "--.--"}</strong>
          </div>
          <p className="fare-caption">{zoneName(trip.pulocation_id)} to {zoneName(trip.dolocation_id)}</p>
          <div className="signal-list">
            <div><span>Speed</span><strong>{formatNumber(speed, 1)} mph</strong></div>
            <div><span>Route delta</span><strong>{zoneDelta} zones</strong></div>
            <div><span>Day</span><strong>{dayNames[Number(trip.pickup_day_of_week)]}</strong></div>
          </div>
        </section>

        <section className="panel history-panel">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">Session Log</p>
              <h2>Recent estimates</h2>
            </div>
          </div>
          <div className="history-table-wrap">
            <table>
              <thead><tr><th>Time</th><th>Route</th><th>Trip</th><th>Fare</th></tr></thead>
              <tbody>
                {history.length === 0 ? (
                  <tr className="empty-row"><td colSpan="4">No estimates yet</td></tr>
                ) : history.map((item, index) => (
                  <tr key={`${item.time}-${index}`}><td>{item.time}</td><td>{item.route}</td><td>{item.trip}</td><td>{item.fare}</td></tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      </section>
    </main>
  );
}

function StreamingPage() {
  const [savedRecords, setSavedRecords] = useState([]);
  const [streamRecords, setStreamRecords] = useState([]);
  const [feed, setFeed] = useState([]);
  const [pasteText, setPasteText] = useState(sampleRows);
  const [inputDirty, setInputDirty] = useState(false);
  const [message, setMessage] = useState("Ready");
  const [isRunning, setIsRunning] = useState(false);
  const [currentBatch, setCurrentBatch] = useState("--");
  const [batchSize, setBatchSize] = useState(5);
  const stopRef = useRef(false);

  const quality = useMemo(() => computeQuality(streamRecords), [streamRecords]);

  async function loadSavedData() {
    const response = await fetch("/stream-demo/data");
    const payload = await response.json();
    const actualByTrip = new Map((payload.actuals || []).map((item) => [item.trip_id, item]));
    const rows = (payload.predictions || []).map((item, index) => ({
      ...item,
      source: "saved",
      row_number: index + 1,
      pickup_hour: pickupHourFromDate(item.pickup_datetime),
      pickup_day_of_week: pickupDayFromDate(item.pickup_datetime),
      actual_fare_amount: actualByTrip.get(item.trip_id)?.actual_fare_amount,
    }));
    setSavedRecords(rows);
    setStreamRecords(rows);
    setMessage(`${rows.length} saved backend predictions loaded`);
  }

  useEffect(() => {
    loadSavedData().catch(() => setMessage("No saved stream log"));
  }, []);

  async function parseFile(event) {
    const file = event.target.files?.[0];
    if (!file) return;
    const rows = await readWorkbook(file);
    const normalized = normalizeUploadRows(rows);
    setStreamRecords([]);
    setFeed([]);
    setSavedRecords(normalized);
    setInputDirty(false);
    setMessage(`${normalized.length} rows loaded from ${file.name}`);
  }

  function parsePaste() {
    const rows = readPastedTable(pasteText);
    const normalized = normalizeUploadRows(rows);
    setStreamRecords([]);
    setFeed([]);
    setSavedRecords(normalized);
    setInputDirty(false);
    setMessage(`${normalized.length} pasted rows loaded`);
  }

  async function predictOne(row) {
    const response = await fetch("/predict", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(buildPredictPayload(row)),
    });
    const body = await response.json();
    if (!response.ok) {
      throw new Error(apiErrorMessage(body));
    }
    return {
      ...row,
      predicted_fare_amount: body.predicted_fare,
      model_name: body.model_name,
      model_version: body.model_version,
      model_stage: "Production",
      prediction_timestamp: new Date().toISOString(),
    };
  }

  async function runUploadedStream() {
    let inputRows = savedRecords;
    if (inputDirty && pasteText.trim()) {
      inputRows = normalizeUploadRows(readPastedTable(pasteText));
      setSavedRecords(inputRows);
      setInputDirty(false);
    }

    if (inputRows.length === 0) {
      setMessage("No valid rows. Use CSV columns or values: pickup_datetime, passenger_count, distance, duration_minutes, pulocation_id, dolocation_id, actual_fare_amount");
      return;
    }

    stopRef.current = false;
    setIsRunning(true);
    setStreamRecords([]);
    setFeed([]);
    setMessage(`Streaming backend running ${inputRows.length} rows`);

    const effectiveBatchSize = Math.max(1, Math.min(100, Number(batchSize) || 1));
    const completed = [];
    try {
      for (let start = 0; start < inputRows.length; start += effectiveBatchSize) {
        if (stopRef.current) break;
        const batchNumber = Math.floor(start / effectiveBatchSize);
        setCurrentBatch(`batch ${String(batchNumber).padStart(3, "0")}`);
        const batch = inputRows.slice(start, start + effectiveBatchSize);
        const predictions = await Promise.all(batch.map((row) => predictOne(row)));
        completed.push(...predictions);
        setStreamRecords([...completed]);
        setFeed((items) => [...predictions.reverse(), ...items].slice(0, 10));
        await new Promise((resolve) => window.setTimeout(resolve, 650));
      }
      setMessage(stopRef.current ? "Stream paused" : "Stream complete");
    } catch (error) {
      setMessage(error.message || String(error));
    } finally {
      setIsRunning(false);
    }
  }

  function stopStream() {
    stopRef.current = true;
    setIsRunning(false);
  }

  return (
    <main className="main-area">
      <header className="topbar">
        <div>
          <p className="eyebrow">Production Backend</p>
          <h1>Streaming Model Monitor</h1>
        </div>
        <div className="topbar-actions">
          <button className="ghost-button" onClick={loadSavedData} type="button">
            <RefreshCw />
            <span>Refresh</span>
          </button>
        </div>
      </header>

      <section className="overview-grid">
        <div className="summary-tile"><span>Events</span><strong>{quality.count}</strong></div>
        <div className="summary-tile"><span>Model</span><strong>{streamRecords[0]?.model_version ? `v${streamRecords[0].model_version}` : "--"}</strong></div>
        <div className="summary-tile"><span>MAE</span><strong>{quality.mae === null ? "--" : formatMoney(quality.mae)}</strong></div>
        <div className="summary-tile"><span>Status</span><strong>{isRunning ? "Running" : String(message)}</strong></div>
      </section>

      <section className="stream-grid">
        <section className="panel stream-live-panel">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">Micro-Batches</p>
              <h2>Backend event replay</h2>
            </div>
            <div className="stream-controls">
              <label className="batch-size-control">
                <span>Batch size</span>
                <input
                  type="number"
                  min="1"
                  max="100"
                  value={batchSize}
                  disabled={isRunning}
                  onChange={(event) => setBatchSize(Math.max(1, Math.min(100, Number(event.target.value) || 1)))}
                />
              </label>
              <button className="primary-button compact" type="button" onClick={runUploadedStream} disabled={isRunning || savedRecords.length === 0}>
                <Play />
                <span>Replay</span>
              </button>
              <button className="ghost-button compact" type="button" onClick={stopStream}>
                <Pause />
                <span>Pause</span>
              </button>
            </div>
          </div>

          <div className="stream-pipeline">
            <div><RadioTower /><span>trip_started</span></div>
            <div><GitMerge /><span>feature build</span></div>
            <div><BrainCircuit /><span>MLflow model</span></div>
            <div><DatabaseZap /><span>prediction log</span></div>
          </div>

          <div className="live-readout">
            <div><span>Current batch</span><strong>{currentBatch}</strong></div>
            <div><span>Loaded rows</span><strong>{savedRecords.length}</strong></div>
            <div><span>Batch count</span><strong>{savedRecords.length ? Math.ceil(savedRecords.length / Math.max(1, Number(batchSize) || 1)) : "--"}</strong></div>
          </div>

          <div className="event-feed">
            {feed.length === 0 ? (
              <div className="feed-empty">No stream events yet</div>
            ) : feed.map((item, index) => (
              <div className="feed-item" key={`${item.row_number}-${index}`}>
                <div className="feed-icon"><span>{String(item.row_number).padStart(2, "0")}</span></div>
                <div>
                  <strong>{item.pulocation_id} -&gt; {item.dolocation_id} predicted {formatMoney(item.predicted_fare_amount)}</strong>
                  <p>{item.pickup_datetime} | model v{item.model_version || "--"}</p>
                </div>
              </div>
            ))}
          </div>
        </section>

        <section className="panel stream-quality-panel">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">Excel Input</p>
              <h2>Upload or paste rows</h2>
            </div>
            <span className="quote-badge">{savedRecords.length} rows</span>
          </div>

          <div className="upload-zone">
            <label className="upload-button">
              <Upload />
              <span>Upload Excel/CSV</span>
              <input type="file" accept=".xlsx,.xls,.csv" onChange={parseFile} />
            </label>
            <button className="ghost-button compact" type="button" onClick={parsePaste}>
              <ClipboardPaste />
              <span>Load pasted table</span>
            </button>
          </div>

          <textarea
            className="excel-textarea"
            value={pasteText}
            onChange={(event) => {
              setPasteText(event.target.value);
              setInputDirty(true);
              const count = normalizeUploadRows(readPastedTable(event.target.value)).length;
              setMessage(`${count} rows ready from textarea`);
            }}
            spellCheck="false"
          />

          <div className="quality-bars">
            <div>
              <span>Bias</span>
              <strong>{quality.bias === null ? "--" : formatMoney(quality.bias)}</strong>
              <div className="bar-track"><span style={{ width: `${Math.min(100, Math.max(10, Math.abs(quality.bias || 0) * 10))}%` }} /></div>
            </div>
          </div>
        </section>

        <section className="panel stream-table-panel">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">Prediction Log</p>
              <h2>Backend outputs</h2>
            </div>
            <div className="stream-controls">
              <button
                className="ghost-button compact"
                type="button"
                onClick={() => {
                  setPasteText(sampleRows);
                  setInputDirty(true);
                  setMessage("5 sample rows ready from textarea");
                }}
              >
                <TableProperties />
                <span>Sample</span>
              </button>
            </div>
          </div>
          <div className="history-table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Pickup</th><th>Route</th><th>Trip</th><th>Predicted</th><th>Actual</th><th>Error</th>
                </tr>
              </thead>
              <tbody>
                {streamRecords.length === 0 ? (
                  <tr className="empty-row"><td colSpan="6">No predictions loaded</td></tr>
                ) : streamRecords.map((item, index) => {
                  const error = Number(item.predicted_fare_amount) - Number(item.actual_fare_amount);
                  return (
                    <tr key={`${item.row_number}-${index}`}>
                      <td>{String(item.pickup_datetime).slice(0, 16)}</td>
                      <td>{item.pulocation_id} -&gt; {item.dolocation_id}</td>
                      <td>{formatNumber(item.estimated_trip_distance, 1)} mi, {formatNumber(item.estimated_trip_duration_seconds / 60, 0)} min</td>
                      <td>{formatMoney(item.predicted_fare_amount)}</td>
                      <td>{Number.isFinite(Number(item.actual_fare_amount)) ? formatMoney(item.actual_fare_amount) : "--"}</td>
                      <td>{Number.isFinite(error) ? formatMoney(error) : "--"}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </section>
      </section>
    </main>
  );
}

export default function App() {
  const [page, setPage] = useState(() => (window.location.pathname.includes("stream") ? "streaming" : "pricing"));

  function changePage(nextPage) {
    setPage(nextPage);
    window.history.replaceState(null, "", nextPage === "streaming" ? "/streaming" : "/");
  }

  return (
    <div className="app-shell">
      <Sidebar page={page} setPage={changePage} />
      {page === "streaming" ? <StreamingPage /> : <PricingPage />}
    </div>
  );
}

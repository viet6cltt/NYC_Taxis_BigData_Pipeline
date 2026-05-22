const FALLBACK_ZONES = [
  { id: 132, borough: "Queens", zone: "JFK Airport", service_zone: "Airports" },
  { id: 138, borough: "Queens", zone: "LaGuardia Airport", service_zone: "Airports" },
  { id: 161, borough: "Manhattan", zone: "Midtown Center", service_zone: "Yellow Zone" },
  { id: 186, borough: "Manhattan", zone: "Penn Station/Madison Sq West", service_zone: "Yellow Zone" },
  { id: 230, borough: "Manhattan", zone: "Times Sq/Theatre District", service_zone: "Yellow Zone" },
  { id: 236, borough: "Manhattan", zone: "Upper East Side North", service_zone: "Yellow Zone" },
  { id: 237, borough: "Manhattan", zone: "Upper East Side South", service_zone: "Yellow Zone" },
  { id: 211, borough: "Manhattan", zone: "SoHo", service_zone: "Yellow Zone" },
  { id: 87, borough: "Manhattan", zone: "Financial District North", service_zone: "Yellow Zone" },
  { id: 234, borough: "Manhattan", zone: "Union Sq", service_zone: "Yellow Zone" },
];

const MAP_WIDTH = 920;
const MAP_HEIGHT = 620;
const MAP_PADDING = 24;
const ROUTE_DISTANCE_MULTIPLIER = 1.35;
const ROUTE_AVERAGE_SPEED_MPH = 13.5;
const SVG_NS = "http://www.w3.org/2000/svg";
const BOROUGH_COLORS = {
  Bronx: "#dbeafe",
  Brooklyn: "#dcfce7",
  EWR: "#f1f5f9",
  Manhattan: "#fef3c7",
  Queens: "#ede9fe",
  "Staten Island": "#fee2e2",
};

const PRESETS = {
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

const HISTORY_KEY = "taxiops.recent-estimates";
const currency = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 2,
});

let zones = [];
let zoneById = new Map();
let zoneMapFeatures = [];
let mapFeaturePaths = [];
let activeMapEndpoint = "pickup";
let routeEstimateAbortController = null;
let history = [];
let latestFare = null;

const $ = (selector) => document.querySelector(selector);

const elements = {
  form: $("#prediction-form"),
  submitButton: $("#prediction-form button[type='submit']"),
  message: $("#form-message"),
  passengerCount: $("#passenger_count"),
  distance: $("#estimated_trip_distance"),
  duration: $("#duration_minutes"),
  pickupHour: $("#pickup_hour"),
  pickupHourLabel: $("#pickup-hour-label"),
  dayOfWeek: $("#pickup_day_of_week"),
  pickupZone: $("#pulocation_id"),
  dropoffZone: $("#dolocation_id"),
  fare: $("#predicted-fare"),
  fareCaption: $("#fare-caption"),
  quoteBadge: $("#quote-badge"),
  speed: $("#speed-signal"),
  zoneDelta: $("#zone-delta"),
  demandBand: $("#demand-band"),
  pickupZoneName: $("#pickup-zone-name"),
  dropoffZoneName: $("#dropoff-zone-name"),
  selectedPickupZone: $("#selected-pickup-zone"),
  selectedDropoffZone: $("#selected-dropoff-zone"),
  routeEstimateReadout: $("#route-estimate-readout"),
  mapModePickup: $("#map-mode-pickup"),
  mapModeDropoff: $("#map-mode-dropoff"),
  zoneMap: $("#taxi-zone-map"),
  mapEmpty: $("#map-empty"),
  canvas: $("#route-canvas"),
  serviceState: $("#service-state"),
  modelName: $("#model-name"),
  modelVersion: $("#model-version"),
  latency: $("#last-latency"),
  sidebarStatus: $("#sidebar-status"),
  sidebarVersion: $("#sidebar-version"),
  sidebarDot: $("#sidebar-status-dot"),
  historyBody: $("#history-body"),
  todayLabel: $("#today-label"),
  timeLabel: $("#time-label"),
};

function withLabel(zone) {
  return {
    ...zone,
    label: zone.label || `${zone.borough} - ${zone.zone}`,
  };
}

function zoneName(id) {
  const zone = zoneById.get(Number(id));
  return zone ? zone.zone : `Zone ${id}`;
}

function zoneLabel(id) {
  const zone = zoneById.get(Number(id));
  return zone ? zone.label : `Zone ${id}`;
}

async function loadZones() {
  try {
    const response = await fetch("/zones");
    if (!response.ok) {
      throw new Error("Zone lookup unavailable");
    }
    const payload = await response.json();
    zones = payload.zones.map(withLabel);
  } catch {
    zones = FALLBACK_ZONES.map(withLabel);
  }

  zones.sort((a, b) => a.borough.localeCompare(b.borough) || a.zone.localeCompare(b.zone));
  zoneById = new Map(zones.map((zone) => [Number(zone.id), zone]));
  populateZoneSelect(elements.pickupZone, 161);
  populateZoneSelect(elements.dropoffZone, 236);
  updateTripPreview();
}

function populateZoneSelect(select, selectedId) {
  select.replaceChildren();

  for (const zone of zones) {
    const option = document.createElement("option");
    option.value = zone.id;
    option.textContent = zone.label;
    select.appendChild(option);
  }

  if (zoneById.has(Number(selectedId))) {
    select.value = String(selectedId);
  } else if (select.options.length > 0) {
    select.selectedIndex = 0;
  }
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
  const scale = Math.min((MAP_WIDTH - MAP_PADDING * 2) / lonRange, (MAP_HEIGHT - MAP_PADDING * 2) / latRange);
  const renderedWidth = lonRange * scale;
  const renderedHeight = latRange * scale;
  const offsetX = (MAP_WIDTH - renderedWidth) / 2;
  const offsetY = (MAP_HEIGHT - renderedHeight) / 2;

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

function estimateFromZoneGeometry(pickupId, dropoffId) {
  const pickup = featureGeoCenter(zoneMapFeatures.find((feature) => featureLocationId(feature) === Number(pickupId)));
  const dropoff = featureGeoCenter(zoneMapFeatures.find((feature) => featureLocationId(feature) === Number(dropoffId)));
  if (!pickup || !dropoff) return null;

  const directDistance = haversineMiles(pickup, dropoff);
  const estimatedDistance = Math.max(0.3, directDistance * ROUTE_DISTANCE_MULTIPLIER + 0.2);
  const durationMinutes = Math.max(3, Math.round((estimatedDistance / ROUTE_AVERAGE_SPEED_MPH) * 60));

  return {
    distance: Number(estimatedDistance.toFixed(1)),
    durationMinutes,
  };
}

function updateRouteEstimateReadout() {
  const distance = Number(elements.distance.value);
  const duration = Number(elements.duration.value);
  elements.routeEstimateReadout.textContent = `${Number.isFinite(distance) ? distance.toFixed(1) : "--"} mi / ${Number.isFinite(duration) ? Math.round(duration) : "--"} min`;
}

async function refreshRouteEstimate() {
  const pickupId = Number(elements.pickupZone.value);
  const dropoffId = Number(elements.dropoffZone.value);
  const pickupHour = Number(elements.pickupHour.value);
  const pickupDay = Number(elements.dayOfWeek.value);

  if (!pickupId || !dropoffId) {
    updateRouteEstimateReadout();
    return;
  }

  routeEstimateAbortController?.abort();
  routeEstimateAbortController = new AbortController();

  let estimate = null;
  const params = new URLSearchParams({
    pulocation_id: String(pickupId),
    dolocation_id: String(dropoffId),
    pickup_hour: String(pickupHour),
    pickup_day_of_week: String(pickupDay),
  });

  try {
    const response = await fetch(`/route-estimate?${params.toString()}`, {
      signal: routeEstimateAbortController.signal,
    });
    if (response.ok) {
      const body = await response.json();
      if (body.estimate_level !== "global" || zoneMapFeatures.length === 0) {
        estimate = {
          distance: Number(body.estimated_trip_distance),
          durationMinutes: Number(body.duration_minutes || body.estimated_trip_duration_seconds / 60),
        };
      }
    }
  } catch (error) {
    if (error.name === "AbortError") return;
  }

  if (!estimate && zoneMapFeatures.length > 0) {
    estimate = estimateFromZoneGeometry(pickupId, dropoffId);
  }

  if (!estimate || !Number.isFinite(estimate.distance) || !Number.isFinite(estimate.durationMinutes)) {
    updateRouteEstimateReadout();
    return;
  }

  elements.distance.value = Number(estimate.distance).toFixed(1);
  elements.duration.value = String(Math.max(1, Math.round(estimate.durationMinutes)));
  latestFare = null;
  elements.fare.textContent = "--.--";
  elements.quoteBadge.textContent = "Ready";
  updateTripPreview();
}

function setMapEndpoint(endpoint) {
  activeMapEndpoint = endpoint;
  elements.mapModePickup.classList.toggle("active", endpoint === "pickup");
  elements.mapModeDropoff.classList.toggle("active", endpoint === "dropoff");
  for (const item of mapFeaturePaths) {
    const title = item.element.querySelector("title")?.textContent || `Zone ${item.id}`;
    item.element.setAttribute("aria-label", `Set ${endpoint}: ${title}`);
  }
}

function createSvgElement(tagName, attributes = {}) {
  const element = document.createElementNS(SVG_NS, tagName);
  for (const [key, value] of Object.entries(attributes)) {
    element.setAttribute(key, value);
  }
  return element;
}

function createMapPin(type, point) {
  const group = createSvgElement("g", {
    class: `map-pin ${type}`,
    transform: `translate(${point[0]} ${point[1]})`,
  });
  group.appendChild(createSvgElement("circle", { r: "11" }));
  const text = createSvgElement("text", { y: "4" });
  text.textContent = type === "pickup" ? "PU" : "DO";
  group.appendChild(text);
  return group;
}

function syncMapSelection() {
  elements.selectedPickupZone.textContent = zoneLabel(elements.pickupZone.value);
  elements.selectedDropoffZone.textContent = zoneLabel(elements.dropoffZone.value);

  if (mapFeaturePaths.length === 0) return;

  const pickupId = Number(elements.pickupZone.value);
  const dropoffId = Number(elements.dropoffZone.value);
  const centerById = new Map();

  for (const item of mapFeaturePaths) {
    item.element.classList.toggle("selected", item.id === pickupId || item.id === dropoffId);
    item.element.classList.toggle("pickup", item.id === pickupId);
    item.element.classList.toggle("dropoff", item.id === dropoffId);
    centerById.set(item.id, item.center);
  }

  elements.zoneMap.querySelector(".route-overlay")?.remove();

  const overlay = createSvgElement("g", { class: "route-overlay" });
  const pickupPoint = centerById.get(pickupId);
  const dropoffPoint = centerById.get(dropoffId);

  if (pickupPoint && dropoffPoint) {
    overlay.appendChild(createSvgElement("line", {
      class: "map-route-line",
      x1: pickupPoint[0],
      y1: pickupPoint[1],
      x2: dropoffPoint[0],
      y2: dropoffPoint[1],
    }));
  }
  if (pickupPoint) overlay.appendChild(createMapPin("pickup", pickupPoint));
  if (dropoffPoint) overlay.appendChild(createMapPin("dropoff", dropoffPoint));

  elements.zoneMap.appendChild(overlay);
}

function renderZoneMap() {
  if (!elements.zoneMap || zoneMapFeatures.length === 0) return;

  const project = createMapProjector(zoneMapFeatures);
  elements.zoneMap.replaceChildren();
  elements.zoneMap.appendChild(createSvgElement("rect", {
    class: "map-water",
    x: "0",
    y: "0",
    width: MAP_WIDTH,
    height: MAP_HEIGHT,
  }));

  mapFeaturePaths = zoneMapFeatures.map((feature) => {
    const id = featureLocationId(feature);
    const title = `${feature.properties.borough} - ${feature.properties.zone}`;
    const path = createSvgElement("path", {
      class: "zone-shape",
      d: featureToPath(feature, project),
      fill: BOROUGH_COLORS[feature.properties.borough] || "#e2e8f0",
      role: "button",
      tabindex: "0",
      "aria-label": `Set ${activeMapEndpoint === "pickup" ? "pickup" : "dropoff"}: ${title}`,
    });
    const titleElement = createSvgElement("title");
    titleElement.textContent = title;
    path.appendChild(titleElement);
    path.addEventListener("click", () => selectZoneFromMap(id));
    path.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        selectZoneFromMap(id);
      }
    });
    elements.zoneMap.appendChild(path);
    return {
      id,
      element: path,
      center: featureCenter(feature, project),
    };
  });

  elements.mapEmpty.classList.add("hidden");
  syncMapSelection();
}

async function loadZoneMap() {
  try {
    const response = await fetch("/static/data/taxi_zones.geojson");
    if (!response.ok) throw new Error("Taxi zone map unavailable");
    const payload = await response.json();
    zoneMapFeatures = payload.features || [];
    renderZoneMap();
  } catch {
    zoneMapFeatures = [];
    mapFeaturePaths = [];
    elements.mapEmpty.textContent = "NYC taxi zone map unavailable";
  }
}

function selectZoneFromMap(zoneId) {
  const target = activeMapEndpoint === "pickup" ? elements.pickupZone : elements.dropoffZone;
  if ([...target.options].some((option) => Number(option.value) === Number(zoneId))) {
    target.value = String(zoneId);
  }

  latestFare = null;
  elements.fare.textContent = "--.--";
  elements.quoteBadge.textContent = "Ready";
  if (activeMapEndpoint === "pickup") {
    setMapEndpoint("dropoff");
  }
  updateTripPreview();
  refreshRouteEstimate();
}

function setHealth(status, details = {}) {
  const ok = status === "ok";
  elements.serviceState.textContent = ok ? "Online" : "Offline";
  elements.sidebarStatus.textContent = ok ? "Prediction service online" : "Service unavailable";
  elements.modelName.textContent = details.model_name || "XGB_NYC_Fare";
  elements.modelVersion.textContent = details.model_version || "unknown";
  elements.sidebarVersion.textContent = `${details.model_stage || "Production"} model v${details.model_version || "unknown"}`;
  elements.sidebarDot.classList.toggle("ok", ok);
  elements.sidebarDot.classList.toggle("error", !ok);
}

async function refreshHealth() {
  try {
    const response = await fetch("/health");
    if (!response.ok) {
      throw new Error("Health check failed");
    }
    const body = await response.json();
    setHealth(body.status === "ok" ? "ok" : "error", body);
  } catch {
    setHealth("error");
  }
}

function collectPayload() {
  const durationMinutes = Number(elements.duration.value);

  return {
    passenger_count: Number(elements.passengerCount.value),
    estimated_trip_distance: Number(elements.distance.value),
    estimated_trip_duration_seconds: durationMinutes * 60,
    pickup_hour: Number(elements.pickupHour.value),
    pickup_day_of_week: Number(elements.dayOfWeek.value),
    pulocation_id: Number(elements.pickupZone.value),
    dolocation_id: Number(elements.dropoffZone.value),
  };
}

function updatePickupHourLabel() {
  elements.pickupHourLabel.textContent = `${String(elements.pickupHour.value).padStart(2, "0")}:00`;
}

function getDemandBand(hour, day) {
  const weekend = day === 5 || day === 6;
  const peak = (hour >= 7 && hour <= 10) || (hour >= 16 && hour <= 19);
  const late = hour >= 22 || hour <= 3;

  if (peak && !weekend) return "Peak";
  if (late) return "Late";
  if (weekend) return "Weekend";
  return "Standard";
}

function getRoutePoints(width, height) {
  const pickupId = Number(elements.pickupZone.value || 0);
  const dropoffId = Number(elements.dropoffZone.value || 0);
  const distance = Number(elements.distance.value || 1);

  const pickupShift = (pickupId % 11) / 10;
  const dropoffShift = (dropoffId % 13) / 12;
  const routeSpread = Math.min(0.22, distance / 120);

  return {
    start: {
      x: width * (0.18 + pickupShift * 0.14),
      y: height * (0.68 + pickupShift * 0.12),
    },
    end: {
      x: width * (0.67 + dropoffShift * 0.15),
      y: height * (0.18 + dropoffShift * 0.16),
    },
    controlA: {
      x: width * (0.35 + routeSpread),
      y: height * 0.38,
    },
    controlB: {
      x: width * 0.54,
      y: height * (0.78 - routeSpread),
    },
  };
}

function drawPoint(ctx, point, fill, label) {
  ctx.beginPath();
  ctx.arc(point.x, point.y, 8, 0, Math.PI * 2);
  ctx.fillStyle = fill;
  ctx.fill();
  ctx.lineWidth = 3;
  ctx.strokeStyle = "#ffffff";
  ctx.stroke();

  ctx.fillStyle = "#17202a";
  ctx.font = "700 11px Inter, system-ui, sans-serif";
  ctx.fillText(label, point.x + 13, point.y + 4);
}

function drawRoute() {
  const canvas = elements.canvas;
  const rect = canvas.getBoundingClientRect();

  if (rect.width === 0 || rect.height === 0) {
    return;
  }

  const dpr = window.devicePixelRatio || 1;
  canvas.width = Math.round(rect.width * dpr);
  canvas.height = Math.round(rect.height * dpr);

  const ctx = canvas.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, rect.width, rect.height);

  const { start, end, controlA, controlB } = getRoutePoints(rect.width, rect.height);

  ctx.lineWidth = 8;
  ctx.lineCap = "round";
  ctx.strokeStyle = "rgba(17, 24, 39, 0.14)";
  ctx.beginPath();
  ctx.moveTo(start.x, start.y);
  ctx.bezierCurveTo(controlA.x, controlA.y, controlB.x, controlB.y, end.x, end.y);
  ctx.stroke();

  ctx.lineWidth = 4;
  ctx.strokeStyle = "#f4c430";
  ctx.beginPath();
  ctx.moveTo(start.x, start.y);
  ctx.bezierCurveTo(controlA.x, controlA.y, controlB.x, controlB.y, end.x, end.y);
  ctx.stroke();

  drawPoint(ctx, start, "#16836f", "PU");
  drawPoint(ctx, end, "#2d67d3", "DO");
}

function updateTripPreview() {
  updatePickupHourLabel();

  const payload = collectPayload();
  const minutes = payload.estimated_trip_duration_seconds / 60;
  const speed = minutes > 0 ? payload.estimated_trip_distance / (minutes / 60) : 0;
  const delta = Math.abs((payload.dolocation_id || 0) - (payload.pulocation_id || 0));

  elements.speed.textContent = `${speed.toFixed(1)} mph`;
  elements.zoneDelta.textContent = `${delta} zones`;
  elements.demandBand.textContent = getDemandBand(payload.pickup_hour, payload.pickup_day_of_week);
  elements.pickupZoneName.textContent = zoneName(payload.pulocation_id);
  elements.dropoffZoneName.textContent = zoneName(payload.dolocation_id);
  updateRouteEstimateReadout();

  if (latestFare === null) {
    elements.fareCaption.textContent = `${payload.estimated_trip_distance.toFixed(1)} mi | ${minutes.toFixed(0)} min | ${zoneLabel(payload.pulocation_id)} to ${zoneLabel(payload.dolocation_id)}`;
  }

  syncMapSelection();
  drawRoute();
}

function setFormMessage(message, type = "default") {
  elements.message.textContent = message;
  elements.message.classList.toggle("error", type === "error");
}

function formatFare(value) {
  return currency.format(value).replace("$", "");
}

function saveHistory(record) {
  history = [record, ...history].slice(0, 8);
  localStorage.setItem(HISTORY_KEY, JSON.stringify(history));
  renderHistory();
}

function loadHistory() {
  try {
    history = JSON.parse(localStorage.getItem(HISTORY_KEY) || "[]");
  } catch {
    history = [];
  }
  renderHistory();
}

function renderHistory() {
  elements.historyBody.replaceChildren();

  if (history.length === 0) {
    const row = document.createElement("tr");
    row.className = "empty-row";
    const cell = document.createElement("td");
    cell.colSpan = 4;
    cell.textContent = "No estimates yet";
    row.appendChild(cell);
    elements.historyBody.appendChild(row);
    return;
  }

  for (const item of history) {
    const row = document.createElement("tr");
    for (const value of [item.time, item.route, item.trip, item.fare]) {
      const cell = document.createElement("td");
      cell.textContent = value;
      row.appendChild(cell);
    }
    elements.historyBody.appendChild(row);
  }
}

async function handlePredict(event) {
  event.preventDefault();

  if (!elements.form.reportValidity()) {
    return;
  }

  const payload = collectPayload();
  const startedAt = performance.now();

  elements.submitButton.disabled = true;
  elements.quoteBadge.textContent = "Running";
  setFormMessage("Sending trip to model");

  try {
    const response = await fetch("/predict", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    const body = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(body.detail || "Prediction failed");
    }

    const elapsed = Math.max(1, Math.round(performance.now() - startedAt));
    latestFare = Number(body.predicted_fare);

    elements.fare.textContent = formatFare(latestFare);
    elements.fareCaption.textContent = `${zoneLabel(payload.pulocation_id)} to ${zoneLabel(payload.dolocation_id)}`;
    elements.quoteBadge.textContent = "Quoted";
    elements.latency.textContent = `${elapsed} ms`;
    setFormMessage(`Model ${body.model_name} v${body.model_version} returned ${currency.format(latestFare)}`);

    saveHistory({
      time: new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }),
      route: `${zoneName(payload.pulocation_id)} -> ${zoneName(payload.dolocation_id)}`,
      trip: `${payload.estimated_trip_distance.toFixed(1)} mi, ${(payload.estimated_trip_duration_seconds / 60).toFixed(0)} min`,
      fare: currency.format(latestFare),
    });
  } catch (error) {
    latestFare = null;
    elements.quoteBadge.textContent = "Error";
    setFormMessage(error.message, "error");
  } finally {
    elements.submitButton.disabled = false;
    updateTripPreview();
  }
}

function applyPreset(name) {
  const preset = PRESETS[name];
  if (!preset) return;

  for (const [key, value] of Object.entries(preset)) {
    const element = $(`#${key}`);
    if (element) {
      element.value = String(value);
    }
  }

  latestFare = null;
  elements.fare.textContent = "--.--";
  elements.quoteBadge.textContent = "Ready";
  setFormMessage("Preset loaded");
  updateTripPreview();
  refreshRouteEstimate();
}

function resetForm() {
  elements.form.reset();
  elements.pickupZone.value = zoneById.has(161) ? "161" : elements.pickupZone.value;
  elements.dropoffZone.value = zoneById.has(236) ? "236" : elements.dropoffZone.value;
  latestFare = null;
  elements.fare.textContent = "--.--";
  elements.quoteBadge.textContent = "Ready";
  setFormMessage("");
  updateTripPreview();
  refreshRouteEstimate();
}

function swapZones() {
  const pickup = elements.pickupZone.value;
  elements.pickupZone.value = elements.dropoffZone.value;
  elements.dropoffZone.value = pickup;
  latestFare = null;
  elements.fare.textContent = "--.--";
  elements.quoteBadge.textContent = "Ready";
  updateTripPreview();
  refreshRouteEstimate();
}

function updateClock() {
  const now = new Date();
  elements.todayLabel.textContent = now.toLocaleDateString([], {
    weekday: "short",
    month: "short",
    day: "numeric",
  });
  elements.timeLabel.textContent = now.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    timeZoneName: "short",
  });
}

function bindEvents() {
  elements.form.addEventListener("submit", handlePredict);
  elements.pickupHour.addEventListener("input", () => {
    latestFare = null;
    updateTripPreview();
    refreshRouteEstimate();
  });

  for (const element of [
    elements.passengerCount,
    elements.dayOfWeek,
    elements.pickupZone,
    elements.dropoffZone,
  ]) {
    element.addEventListener("input", () => {
      latestFare = null;
      updateTripPreview();
      if (element !== elements.passengerCount) {
        refreshRouteEstimate();
      }
    });
    element.addEventListener("change", () => {
      latestFare = null;
      updateTripPreview();
      if (element !== elements.passengerCount) {
        refreshRouteEstimate();
      }
    });
  }

  $("#refresh-health").addEventListener("click", refreshHealth);
  $("#reset-form").addEventListener("click", resetForm);
  $("#swap-zones").addEventListener("click", swapZones);
  elements.mapModePickup.addEventListener("click", () => setMapEndpoint("pickup"));
  elements.mapModeDropoff.addEventListener("click", () => setMapEndpoint("dropoff"));
  $("#clear-history").addEventListener("click", () => {
    history = [];
    localStorage.removeItem(HISTORY_KEY);
    renderHistory();
  });

  document.querySelectorAll("[data-preset]").forEach((button) => {
    button.addEventListener("click", () => applyPreset(button.dataset.preset));
  });

  window.addEventListener("resize", drawRoute);
}

async function boot() {
  if (window.lucide) {
    window.lucide.createIcons();
  }

  bindEvents();
  loadHistory();
  updateClock();
  setInterval(updateClock, 30000);
  await loadZones();
  await loadZoneMap();
  await refreshRouteEstimate();
  await refreshHealth();
}

boot();

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

  if (latestFare === null) {
    elements.fareCaption.textContent = `${payload.estimated_trip_distance.toFixed(1)} mi | ${minutes.toFixed(0)} min | ${zoneLabel(payload.pulocation_id)} to ${zoneLabel(payload.dolocation_id)}`;
  }

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
}

function swapZones() {
  const pickup = elements.pickupZone.value;
  elements.pickupZone.value = elements.dropoffZone.value;
  elements.dropoffZone.value = pickup;
  latestFare = null;
  elements.fare.textContent = "--.--";
  elements.quoteBadge.textContent = "Ready";
  updateTripPreview();
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
  elements.pickupHour.addEventListener("input", updateTripPreview);

  for (const element of [
    elements.passengerCount,
    elements.distance,
    elements.duration,
    elements.dayOfWeek,
    elements.pickupZone,
    elements.dropoffZone,
  ]) {
    element.addEventListener("input", () => {
      latestFare = null;
      updateTripPreview();
    });
    element.addEventListener("change", () => {
      latestFare = null;
      updateTripPreview();
    });
  }

  $("#refresh-health").addEventListener("click", refreshHealth);
  $("#reset-form").addEventListener("click", resetForm);
  $("#swap-zones").addEventListener("click", swapZones);
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
  await refreshHealth();
}

boot();

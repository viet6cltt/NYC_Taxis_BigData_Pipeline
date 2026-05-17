const streamState = {
  records: [],
  index: 0,
  timer: null,
  playing: false,
};

const money = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 2,
});

const fmt = {
  num(value, digits = 2) {
    const n = Number(value);
    return Number.isFinite(n) ? n.toFixed(digits) : "--";
  },
  money(value) {
    const n = Number(value);
    return Number.isFinite(n) ? money.format(n) : "--";
  },
  time(value) {
    if (!value) return "--";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value).slice(0, 16);
    return date.toLocaleString([], {
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  },
};

const $ = (selector) => document.querySelector(selector);

const el = {
  statusDot: $("#stream-status-dot"),
  status: $("#stream-status"),
  source: $("#stream-source"),
  count: $("#metric-count"),
  model: $("#metric-model"),
  mae: $("#metric-mae"),
  r2: $("#metric-r2"),
  rmse: $("#quality-rmse"),
  avg: $("#quality-avg"),
  qualityStatus: $("#quality-status"),
  qualityBadge: $("#quality-badge"),
  currentBatch: $("#current-batch"),
  lastRoute: $("#last-route"),
  lastFare: $("#last-fare"),
  feed: $("#event-feed"),
  table: $("#prediction-table-body"),
  rmseBar: $("#rmse-bar"),
  avgBar: $("#avg-bar"),
  statusBar: $("#status-bar"),
};

function setStatus(ok, message, source) {
  el.statusDot.classList.toggle("ok", ok);
  el.statusDot.classList.toggle("error", !ok);
  el.status.textContent = message;
  el.source.textContent = source;
}

function mergeActuals(predictions, actuals) {
  const actualByTrip = new Map(actuals.map((item) => [item.trip_id, item]));
  return predictions.map((prediction, index) => ({
    ...prediction,
    index,
    batch: Math.floor(index / 5),
    actual: actualByTrip.get(prediction.trip_id) || {},
  }));
}

function renderMetrics(payload) {
  const quality = payload.quality || {};
  const records = streamState.records;
  const avgFare =
    records.reduce((total, item) => total + Number(item.predicted_fare_amount || 0), 0) /
    Math.max(records.length, 1);

  el.count.textContent = quality.prediction_count ?? records.length;
  el.model.textContent = `${quality.model_name || "XGB_NYC_Fare"} v${quality.model_version || "--"}`;
  el.mae.textContent = fmt.money(quality.mae);
  el.r2.textContent = fmt.num(quality.r2, 3);
  el.rmse.textContent = fmt.money(quality.rmse);
  el.avg.textContent = fmt.money(avgFare);
  el.qualityStatus.textContent = payload.available ? "Ready" : "Missing log";
  el.qualityBadge.textContent = payload.available ? "Production log" : "No data";

  el.rmseBar.style.width = `${Math.min(100, Math.max(8, Number(quality.rmse || 0) * 8))}%`;
  el.avgBar.style.width = `${Math.min(100, Math.max(8, avgFare * 1.4))}%`;
  el.statusBar.style.width = payload.available ? "100%" : "15%";
}

function renderTable() {
  el.table.replaceChildren();

  if (streamState.records.length === 0) {
    const row = document.createElement("tr");
    row.className = "empty-row";
    const cell = document.createElement("td");
    cell.colSpan = 6;
    cell.textContent = "No predictions loaded";
    row.appendChild(cell);
    el.table.appendChild(row);
    return;
  }

  for (const item of streamState.records) {
    const row = document.createElement("tr");
    const actualFare = item.actual.actual_fare_amount;
    const error = item.actual.prediction_error;
    const cells = [
      fmt.time(item.pickup_datetime),
      `${item.pulocation_id} -> ${item.dolocation_id}`,
      `${fmt.num(item.estimated_trip_distance, 1)} mi, ${fmt.num(Number(item.estimated_trip_duration_seconds) / 60, 0)} min`,
      fmt.money(item.predicted_fare_amount),
      fmt.money(actualFare),
      fmt.money(error),
    ];

    for (const value of cells) {
      const cell = document.createElement("td");
      cell.textContent = value;
      row.appendChild(cell);
    }
    el.table.appendChild(row);
  }
}

function makeFeedItem(item) {
  const node = document.createElement("div");
  node.className = "feed-item";
  node.innerHTML = `
    <div class="feed-icon"><span>${String(item.batch).padStart(2, "0")}</span></div>
    <div>
      <strong>${item.pulocation_id} -> ${item.dolocation_id} predicted ${fmt.money(item.predicted_fare_amount)}</strong>
      <p>${fmt.time(item.pickup_datetime)} | ${item.estimate_level} lookup | model v${item.model_version}</p>
    </div>
  `;
  return node;
}

function resetFeed() {
  streamState.index = 0;
  el.feed.replaceChildren();
  const empty = document.createElement("div");
  empty.className = "feed-empty";
  empty.textContent = "Replay ready";
  el.feed.appendChild(empty);
  el.currentBatch.textContent = "--";
  el.lastRoute.textContent = "--";
  el.lastFare.textContent = "--";
}

function pushNextEvent() {
  if (streamState.index >= streamState.records.length) {
    pauseReplay();
    el.qualityStatus.textContent = "Replay complete";
    return;
  }

  const item = streamState.records[streamState.index];
  if (streamState.index === 0) {
    el.feed.replaceChildren();
  }

  el.feed.prepend(makeFeedItem(item));
  while (el.feed.children.length > 8) {
    el.feed.removeChild(el.feed.lastElementChild);
  }

  el.currentBatch.textContent = `batch ${String(item.batch).padStart(3, "0")}`;
  el.lastRoute.textContent = `${item.pulocation_id} -> ${item.dolocation_id}`;
  el.lastFare.textContent = fmt.money(item.predicted_fare_amount);
  el.qualityStatus.textContent = "Streaming";

  streamState.index += 1;
}

function playReplay() {
  if (streamState.playing || streamState.records.length === 0) return;
  if (streamState.index >= streamState.records.length) {
    resetFeed();
  }
  streamState.playing = true;
  pushNextEvent();
  streamState.timer = window.setInterval(pushNextEvent, 650);
}

function pauseReplay() {
  streamState.playing = false;
  if (streamState.timer) {
    window.clearInterval(streamState.timer);
    streamState.timer = null;
  }
}

async function loadStreamData() {
  pauseReplay();
  setStatus(true, "Loading stream log", "Reading local backend output");

  try {
    const response = await fetch("/stream-demo/data");
    if (!response.ok) {
      throw new Error("Stream demo API failed");
    }
    const payload = await response.json();
    streamState.records = mergeActuals(payload.predictions || [], payload.actuals || []);
    renderMetrics(payload);
    renderTable();
    resetFeed();

    if (!payload.available) {
      setStatus(false, "No stream output yet", "Run scripts/demo_streaming_backend.py");
      return;
    }

    setStatus(true, "Stream log ready", `${streamState.records.length} predictions loaded`);
  } catch (error) {
    streamState.records = [];
    renderTable();
    resetFeed();
    setStatus(false, "Could not load stream log", error.message);
  }
}

function bind() {
  $("#refresh-stream").addEventListener("click", loadStreamData);
  $("#play-stream").addEventListener("click", playReplay);
  $("#pause-stream").addEventListener("click", pauseReplay);
}

async function boot() {
  if (window.lucide) {
    window.lucide.createIcons();
  }
  bind();
  await loadStreamData();
  playReplay();
}

boot();

"use strict";

const FETCH_TIMEOUT_MS = 5000;
const REFRESH_MS = 2000;
const Dashboard = window.StockAgentDashboard;
const $ = Dashboard.byId;
let activeController = null;
let requestSequence = 0;

const integer = new Intl.NumberFormat("zh-TW", {maximumFractionDigits: 0});
const decimal = new Intl.NumberFormat("zh-TW", {maximumFractionDigits: 2});
const percent = new Intl.NumberFormat("zh-TW", {style: "percent", maximumFractionDigits: 2});

function finite(value) { return Dashboard.finiteNumber(value); }
function number(value) { const n = finite(value); return n == null ? "—" : decimal.format(n); }
function count(value) { const n = finite(value); return n == null ? "—" : integer.format(n); }
function milliseconds(value) { const n = finite(value); return n == null ? "—" : `${decimal.format(n)} ms`; }
function ratio(value) { const n = finite(value); return n == null ? "—" : percent.format(n); }
function bytes(value) {
  return Dashboard.formatBytes(value, {maximumFractionDigits: 2});
}
function duration(value) {
  const seconds = finite(value);
  if (seconds == null) return "—";
  if (seconds < 60) return `${count(seconds)} 秒`;
  if (seconds < 3600) return `${number(seconds / 60)} 分鐘`;
  return `${number(seconds / 3600)} 小時`;
}
function localTime(value) {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "—" : date.toLocaleString("zh-TW", {timeZone: "Asia/Taipei", hour12: false});
}
function svgElement(name, attributes = {}) {
  return Dashboard.svgElement(name, attributes);
}

function renderChart(rows) {
  const grid = $("chart-grid"); const lines = $("chart-lines");
  grid.replaceChildren(); lines.replaceChildren();
  const samples = Array.isArray(rows) ? rows : [];
  const hasTraffic = samples.some((row) => Number(row.requests || 0) > 0);
  $("chart-empty").hidden = hasTraffic;
  const left = 54; const right = 906; const top = 22; const bottom = 232;
  const maxRps = Math.max(1, ...samples.map((row) => Number(row.requests_per_second || 0)));
  const maxLatency = Math.max(1, ...samples.map((row) => Number(row.latency_p95_ms || 0)));
  for (let index = 0; index <= 4; index += 1) {
    const y = top + (bottom - top) * index / 4;
    grid.append(svgElement("line", {x1: left, y1: y, x2: right, y2: y, class: "grid-line"}));
    const rpsLabel = svgElement("text", {x: left - 7, y: y + 4, class: "grid-label", "text-anchor": "end"});
    rpsLabel.textContent = number(maxRps * (4 - index) / 4);
    grid.append(rpsLabel);
    const latencyLabel = svgElement("text", {x: right + 7, y: y + 4, class: "grid-label"});
    latencyLabel.textContent = milliseconds(maxLatency * (4 - index) / 4);
    grid.append(latencyLabel);
  }
  if (!samples.length) return;
  const x = (index) => left + (right - left) * index / Math.max(1, samples.length - 1);
  const rpsY = (value) => bottom - Number(value || 0) / maxRps * (bottom - top);
  const latencyY = (value) => bottom - Number(value || 0) / maxLatency * (bottom - top);
  const rpsPoints = samples.map((row, index) => `${x(index).toFixed(2)},${rpsY(row.requests_per_second).toFixed(2)}`).join(" ");
  const latencyPoints = samples.map((row, index) => `${x(index).toFixed(2)},${latencyY(row.latency_p95_ms).toFixed(2)}`).join(" ");
  lines.append(svgElement("polyline", {points: rpsPoints, class: "rps-line"}));
  lines.append(svgElement("polyline", {points: latencyPoints, class: "latency-line"}));
  for (const [index, label] of [[0, samples[0]?.minute_utc], [samples.length - 1, samples.at(-1)?.minute_utc]]) {
    const text = svgElement("text", {x: x(index), y: 253, class: "grid-label", "text-anchor": index === 0 ? "start" : "end"});
    const date = new Date(label);
    text.textContent = Number.isNaN(date.getTime()) ? "—" : date.toLocaleTimeString("zh-TW", {timeZone: "Asia/Taipei", hour: "2-digit", minute: "2-digit", hour12: false});
    grid.append(text);
  }
}

function windowRow(label, row) {
  const tr = document.createElement("tr");
  const values = [label, count(row?.requests), number(row?.requests_per_second), milliseconds(row?.latency_p50_ms), milliseconds(row?.latency_p95_ms), milliseconds(row?.latency_p99_ms), bytes(row?.response_body_bytes), ratio(row?.error_ratio)];
  for (const value of values) { const td = document.createElement("td"); td.textContent = value; tr.append(td); }
  return tr;
}

function renderWindows(windows) {
  const fragment = document.createDocumentFragment();
  for (const [key, label] of [["1m", "1 分鐘"], ["5m", "5 分鐘"], ["1h", "1 小時"]]) fragment.append(windowRow(label, windows?.[key] || {}));
  $("window-rows").replaceChildren(fragment);
}

function renderRoutes(routes) {
  const rows = Array.isArray(routes) ? routes : [];
  const fragment = document.createDocumentFragment();
  for (const row of rows) {
    const tr = document.createElement("tr");
    const values = [row.route || "—", count(row.requests), ratio(row.share), milliseconds(row.latency_average_ms), milliseconds(row.latency_p95_ms), milliseconds(row.latency_max_ms), bytes(row.response_body_bytes), ratio(row.error_ratio)];
    values.forEach((value, index) => { const td = document.createElement("td"); td.textContent = value; if (index === 7 && Number(row.error_ratio || 0) > 0) td.className = "negative"; tr.append(td); });
    fragment.append(tr);
  }
  $("route-rows").replaceChildren(fragment);
  $("route-count").textContent = `${count(rows.length)} 組路由`;
}

function renderDefinitions(definitions) {
  const fragment = document.createDocumentFragment();
  for (const [term, definition] of Object.entries(definitions || {})) {
    const row = document.createElement("div"); const dt = document.createElement("dt"); const dd = document.createElement("dd");
    const labels = {request: "請求", response_body_bytes: "回應 body", latency: "延遲", percentile: "分位數", error_ratio: "錯誤率", visitor: "訪客隱私", cache_hit_ratio: "快取命中率", retention: "保留範圍"};
    dt.textContent = labels[term] || term; dd.textContent = String(definition || "—"); row.append(dt, dd); fragment.append(row);
  }
  $("definition-list").replaceChildren(fragment);
}

function render(data) {
  const minute = data.windows?.["1m"] || {};
  $("kpi-requests").textContent = count(minute.requests);
  $("kpi-rps").textContent = number(minute.requests_per_second);
  $("kpi-p50").textContent = milliseconds(minute.latency_p50_ms);
  $("kpi-p95").textContent = milliseconds(minute.latency_p95_ms);
  $("kpi-p99").textContent = milliseconds(minute.latency_p99_ms);
  $("kpi-concurrency").textContent = `${count(data.connections?.in_flight)}／${count(data.connections?.peak_in_flight)}`;
  $("cache-ratio").textContent = ratio(data.cache?.hit_ratio);
  $("cache-hits").textContent = count(data.cache?.hits);
  $("cache-builds").textContent = count(data.cache?.builds);
  $("uptime").textContent = duration(data.uptime_seconds);
  renderChart(data.trend);
  renderWindows(data.windows || {});
  renderRoutes(data.routes);
  renderDefinitions(data.definitions);
  $("updated-at").textContent = localTime(data.generated_at_utc);
  $("live-dot").className = "live-dot active";
  $("live-status").textContent = "即時觀察中";
}

function serverTiming(response) {
  const match = String(response.headers.get("Server-Timing") || "").match(/app;dur=([0-9.]+)/);
  return match ? Number(match[1]) : null;
}

function renderLocalTiming(timing) {
  $("local-server").textContent = milliseconds(timing.server);
  $("local-network").textContent = milliseconds(timing.headers);
  $("local-download").textContent = milliseconds(timing.download);
  $("local-parse").textContent = milliseconds(timing.parse);
  $("local-render").textContent = milliseconds(timing.render);
  requestAnimationFrame(() => { $("local-total").textContent = milliseconds(performance.now() - timing.started); });
}

async function refresh({manual = false} = {}) {
  if (activeController && !manual) return;
  if (activeController) activeController.abort();
  const controller = new AbortController(); activeController = controller;
  const sequence = ++requestSequence;
  const started = performance.now();
  try {
    const response = await Dashboard.fetchWithTimeout("api/status", {
      cache: "no-store",
      signal: controller.signal,
      timeoutMs: FETCH_TIMEOUT_MS,
    });
    const headersAt = performance.now();
    const text = await response.text();
    const downloadedAt = performance.now();
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const parsedAtStart = performance.now(); const data = JSON.parse(text); const parsedAt = performance.now();
    if (sequence !== requestSequence) return;
    const renderStarted = performance.now(); render(data); const renderedAt = performance.now();
    renderLocalTiming({started, server: serverTiming(response), headers: headersAt - started, download: downloadedAt - headersAt, parse: parsedAt - parsedAtStart, render: renderedAt - renderStarted});
  } catch (error) {
    if (error?.name === "AbortError") return;
    $("live-dot").className = "live-dot bad"; $("live-status").textContent = `讀取失敗：${error}`;
  } finally {
    if (activeController === controller) activeController = null;
  }
}

$("refresh-now").addEventListener("click", () => void refresh({manual: true}));
document.addEventListener("visibilitychange", () => { if (!document.hidden) void refresh({manual: true}); });
Dashboard.scheduleRefresh(refresh, {intervalMs: REFRESH_MS, refreshOnVisible: false});

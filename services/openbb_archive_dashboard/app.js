"use strict";

const REFRESH_MS = 60000;
const timeAxis = window.StockAgentTimeAxis;
const number = new Intl.NumberFormat("zh-TW");
const compact = new Intl.NumberFormat("zh-TW", {notation: "compact", maximumFractionDigits: 2});
const series = [
  {key: "accepted_percent", label: "已接受", color: "#ae8bff"},
  {key: "resolved_percent", label: "已判定", color: "#43d7ff"},
  {key: "unresolved_percent", label: "尚未解決", color: "#f5bd4f"},
];
const hiddenSeries = new Set();
let range = "all";
let refreshInFlight = false;

const $ = (id) => document.getElementById(id);
const finite = (value, fallback = 0) => Number.isFinite(Number(value)) ? Number(value) : fallback;
const pct = (value) => `${finite(value).toFixed(2)}%`;

function ageLabel(seconds) {
  const value = Number(seconds);
  if (!Number.isFinite(value)) return "無時間證據";
  if (value < 60) return `${Math.round(value)} 秒前`;
  if (value < 3600) return `${Math.round(value / 60)} 分鐘前`;
  if (value < 86400) return `${(value / 3600).toFixed(1)} 小時前`;
  return `${(value / 86400).toFixed(1)} 天前`;
}

function bytes(value) {
  let amount = Number(value);
  if (!Number.isFinite(amount)) return "—";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let unit = 0;
  while (amount >= 1024 && unit < units.length - 1) { amount /= 1024; unit += 1; }
  return `${amount.toFixed(amount >= 100 ? 0 : amount >= 10 ? 1 : 2)} ${units[unit]}`;
}

function healthLabel(health) {
  return ({active: "下載中", starting: "啟動／稽核中", complete: "封存完成", degraded: "活動逾時", stopped: "程序停止", unavailable: "無法取得"})[health] || "狀態未知";
}

async function fetchJson(path) {
  const response = await fetch(path, {cache: "no-store"});
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}

function renderStatus(data) {
  const archive = data.archive || {};
  const process = data.process || {};
  const recent = data.recent || {};
  const storage = data.storage || {};
  const health = String(data.health || "unavailable");
  const status = $("connection-status");
  status.className = `status ${health}`;
  status.lastChild.textContent = healthLabel(health);
  const phaseLabel = ({planning: "建立任務規劃", download: "下載", running: "下載", initializing: "初始化"})[process.phase] || String(process.phase || "未知階段");
  const phaseProgress = finite(process.phase_total) > 0
    ? ` ${number.format(finite(process.phase_completed))}/${number.format(finite(process.phase_total))}`
    : "";
  $("source-freshness").textContent = `${phaseLabel}${phaseProgress} · 程序活動 ${ageLabel(process.activity_age_seconds)} · 完整稽核 ${ageLabel(data.source_age_seconds)}（${data.snapshot_state === "current" ? "新鮮" : "逾時"}）`;
  $("archive-boundary").textContent = `${archive.start_date || "—"} 至 ${archive.end_date || "—"} · ${number.format(finite(archive.endpoint_count))} 個端點`;
  $("completion-percent").textContent = pct(archive.completion_percent);
  $("accepted-fraction").textContent = `${number.format(finite(archive.accepted_tasks))} / ${number.format(finite(archive.total_tasks))} 任務`;
  const width = Math.max(0, Math.min(100, finite(archive.completion_percent)));
  $("progress-fill").value = width;
  $("progress-fill").textContent = `${width}%`;
  $("success-rows").textContent = compact.format(finite(archive.success_rows));
  $("unresolved-tasks").textContent = number.format(finite(archive.unresolved_tasks));
  $("retryable-tasks").textContent = number.format(finite(archive.retryable_tasks));
  $("unavailable-tasks").textContent = number.format(finite(archive.unavailable_tasks));
  $("recent-throughput").textContent = `${finite(recent.tasks_per_minute_last_15m).toFixed(2)}/分`;
  $("recent-detail").textContent = `15 分鐘接受 ${number.format(finite(recent.accepted_tasks_last_15m))} 項`;
  $("disk-free").textContent = bytes(storage.free_bytes);
  $("disk-floor").textContent = `安全門檻 ${bytes(storage.minimum_free_bytes)}`;
  renderRuntimeAlert(data);
  renderCategories(data.categories || []);
  renderProviders(data.providers || []);
  renderAlerts(data.alerts || []);
}

function renderRuntimeAlert(data) {
  const panel = $("runtime-alert");
  const stopped = !["active", "starting", "complete"].includes(String(data.health));
  const stale = data.snapshot_state !== "current";
  panel.hidden = !stopped && !stale;
  if (panel.hidden) return;
  $("runtime-alert-title").textContent = stopped ? "長期程序目前未達可用狀態" : "完整稽核快照已逾時";
  $("runtime-alert-copy").textContent = stopped
    ? "頁面仍保留最後一次完整稽核結果，但不把舊 PID 或舊快照當成現在正在下載。"
    : "下載可能仍在執行；進度數字只代表最後一次完成的 manifest 掃描。";
}

function renderCategories(rows) {
  const root = $("category-grid");
  root.replaceChildren();
  for (const row of rows) {
    const card = document.createElement("article");
    card.className = "category-card";
    const head = document.createElement("div");
    head.className = "category-head";
    const name = document.createElement("strong");
    name.textContent = row.category;
    const value = document.createElement("span");
    value.textContent = pct(row.completion_percent);
    head.append(name, value);
    const track = document.createElement("progress");
    track.className = "category-track";
    track.max = 100;
    track.value = Math.max(0, Math.min(100, finite(row.completion_percent)));
    track.setAttribute("aria-label", `${row.category} 完成度`);
    const detail = document.createElement("p");
    detail.textContent = `已接受 ${number.format(finite(row.accepted_tasks))}／${number.format(finite(row.total_tasks))} · 未解決 ${number.format(finite(row.unresolved_tasks))} · ${compact.format(finite(row.success_rows))} 列`;
    card.append(head, track, detail);
    root.append(card);
  }
  if (!rows.length) root.textContent = "尚無分類快照。";
}

function cell(text, className = "") {
  const td = document.createElement("td");
  td.textContent = text;
  if (className) td.className = className;
  return td;
}

function renderProviders(rows) {
  const body = $("provider-body");
  body.replaceChildren();
  for (const row of rows) {
    const tr = document.createElement("tr");
    tr.append(cell(row.provider, "provider-name"));
    tr.append(cell(number.format(finite(row.accepted_tasks))));
    tr.append(cell(compact.format(finite(row.success_rows))));
    tr.append(cell(`${number.format(finite(row.eligible_backlog_tasks))} / ${number.format(finite(row.exclusive_backlog_tasks))}`));
    tr.append(cell(`${finite(row.recent_tasks_per_minute).toFixed(2)}/分`));
    tr.append(cell(`${finite(row.requests_per_second).toFixed(2)} / ${number.format(finite(row.configured_concurrency))}`));
    const state = document.createElement("td");
    const pill = document.createElement("span");
    pill.className = `state-pill${row.cooldown ? " cooldown" : ""}`;
    pill.textContent = row.cooldown ? "配額等待" : row.active ? `執行 ${row.active}` : "可排程";
    state.append(pill);
    tr.append(state);
    body.append(tr);
  }
  if (!rows.length) {
    const tr = document.createElement("tr");
    const td = cell("尚無供應商快照");
    td.colSpan = 7;
    tr.append(td);
    body.append(tr);
  }
}

function renderAlerts(rows) {
  const root = $("alert-list");
  root.replaceChildren();
  for (const row of rows) {
    const article = document.createElement("article");
    article.className = `alert-item ${row.severity || "warning"}`;
    const code = document.createElement("code");
    code.textContent = row.code;
    const message = document.createElement("p");
    message.textContent = row.message;
    article.append(code, message);
    root.append(article);
  }
  if (!rows.length) root.textContent = "目前沒有公開稽核警示。";
}

function svgElement(name, attributes = {}) {
  const node = document.createElementNS("http://www.w3.org/2000/svg", name);
  for (const [key, value] of Object.entries(attributes)) node.setAttribute(key, String(value));
  return node;
}

function renderLegend() {
  const root = $("chart-legend");
  root.replaceChildren();
  for (const item of series) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = hiddenSeries.has(item.key) ? "is-hidden" : "";
    button.setAttribute("aria-pressed", String(!hiddenSeries.has(item.key)));
    button.classList.add(`series-${item.key}`);
    const dot = document.createElement("i");
    dot.setAttribute("aria-hidden", "true");
    const label = document.createElement("span");
    label.textContent = item.label;
    button.append(dot, label);
    button.addEventListener("click", () => {
      if (hiddenSeries.has(item.key)) hiddenSeries.delete(item.key); else hiddenSeries.add(item.key);
      renderLegend();
      void loadHistory();
    });
    root.append(button);
  }
}

function renderChart(rows) {
  const lines = $("chart-lines");
  const grid = $("chart-grid");
  lines.replaceChildren();
  grid.replaceChildren();
  $("chart-empty").hidden = rows.length > 0;
  $("progress-chart").hidden = rows.length === 0;
  if (!rows.length) { $("chart-start").textContent = "—"; $("chart-end").textContent = "—"; return; }
  const left = 66, right = 980, top = 20, bottom = 255;
  const timestamps = rows.map((row) => new Date(row.checked_at).getTime());
  const axis = timeAxis.buildTimeAxis({range, timestamps});
  if (!axis) return;
  for (const value of [0, 25, 50, 75, 100]) {
    const y = bottom - (value / 100) * (bottom - top);
    grid.append(svgElement("line", {x1: left, y1: y, x2: right, y2: y, class: "grid-line"}));
    const label = svgElement("text", {x: 5, y: y + 6, class: "grid-label"});
    label.textContent = `${value}%`;
    grid.append(label);
  }
  const x = (value) => timeAxis.position(axis, new Date(value).getTime(), left, right);
  const y = (value) => bottom - Math.max(0, Math.min(100, finite(value))) * (bottom - top) / 100;
  for (const tick of axis.ticks) {
    const xx = timeAxis.position(axis, tick.timestamp, left, right);
    grid.append(svgElement("line", {x1: xx, y1: top, x2: xx, y2: bottom, class: "axis-time-grid"}));
    const attributes = {x: xx, y: 294, class: "axis-time-label", "text-anchor": tick.rotate ? "end" : "middle"};
    if (tick.rotate) attributes.transform = `rotate(-45 ${xx} 294)`;
    const label = svgElement("text", attributes);
    label.textContent = tick.label;
    grid.append(label);
  }
  for (const item of series) {
    if (hiddenSeries.has(item.key)) continue;
    const points = rows.map((row) => `${x(row.checked_at).toFixed(2)},${y(row[item.key]).toFixed(2)}`).join(" ");
    lines.append(svgElement("polyline", {points, class: "series-line", stroke: item.color}));
  }
  const formatTime = (value) => new Date(value).toLocaleString("zh-TW", {timeZone: "Asia/Taipei", hour12: false});
  $("chart-start").textContent = formatTime(axis.startMs);
  $("chart-end").textContent = formatTime(axis.endMs);
}

async function loadHistory() {
  try {
    const data = await fetchJson(`api/history?range=${encodeURIComponent(range)}`);
    renderChart(Array.isArray(data.history) ? data.history : []);
  } catch (_error) {
    renderChart([]);
  }
}

async function refresh() {
  if (document.hidden || refreshInFlight) return;
  refreshInFlight = true;
  try {
    renderStatus(await fetchJson("api/status"));
    await loadHistory();
  } catch (_error) {
    const status = $("connection-status");
    status.className = "status unavailable";
    status.lastChild.textContent = "無法取得";
    $("source-freshness").textContent = "公開狀態 API 暫時不可用";
  } finally {
    refreshInFlight = false;
  }
}

for (const button of document.querySelectorAll("[data-range]")) {
  button.setAttribute("aria-pressed", String(button.dataset.range === range));
  button.addEventListener("click", () => {
    range = button.dataset.range || "all";
    for (const item of document.querySelectorAll("[data-range]")) {
      const active = item.dataset.range === range;
      item.classList.toggle("active", active);
      item.setAttribute("aria-pressed", String(active));
    }
    void loadHistory();
  });
}
renderLegend();
document.addEventListener("visibilitychange", () => { if (!document.hidden) void refresh(); });
void refresh();
window.setInterval(() => void refresh(), REFRESH_MS);

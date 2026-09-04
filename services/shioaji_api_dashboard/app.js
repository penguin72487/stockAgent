"use strict";

const REFRESH_MS = 60000;
const Dashboard = window.StockAgentDashboard;
const fetchWithTimeout = Dashboard.createFetch({timeoutMs: 15000});
const SVG_NS = "http://www.w3.org/2000/svg";
const TIME_RANGES = {"1h": 3600e3, "1d": 86400e3, "1w": 7 * 86400e3, "1mo": 30 * 86400e3, "1q": 90 * 86400e3, "1y": 365 * 86400e3, all: Infinity};
const TIME_RANGE_LABELS = {"1h": "1 小時", "1d": "1 天", "1w": "1 週", "1mo": "1 月", "1q": "1 季", "1y": "1 年", all: "全部"};
const HIDDEN_TRAFFIC_SERIES_STORAGE_KEY = "shioaji-hidden-traffic-series";
const $ = Dashboard.byId;
let latestPipelines = [];
let activeFilter = "all";
let refreshInFlight = false;
let lastHeavyRevision = "";
let trafficTimeRange = "1d";
let storageTimeRange = "1mo";
let latestTrafficHistory = [];
let latestTrafficGuard = 0.9;
let latestStorageGrowth = [];
let hiddenTrafficSeries = new Set();

try {
  trafficTimeRange = localStorage.getItem("shioaji-traffic-time-range") || "1d";
  storageTimeRange = localStorage.getItem("shioaji-storage-time-range") || "1mo";
  const storedHiddenSeries = JSON.parse(localStorage.getItem(HIDDEN_TRAFFIC_SERIES_STORAGE_KEY) || "[]");
  if (Array.isArray(storedHiddenSeries)) hiddenTrafficSeries = new Set(storedHiddenSeries.map(String));
} catch (_error) { /* storage can be disabled */ }
if (!(trafficTimeRange in TIME_RANGES)) trafficTimeRange = "1d";
if (!(storageTimeRange in TIME_RANGES)) storageTimeRange = "1mo";

function number(value, digits = 0) {
  if (value == null || value === "") return "—";
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed.toLocaleString("zh-TW", {maximumFractionDigits: digits}) : "—";
}

function compact(value) {
  if (value == null || value === "") return "—";
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Dashboard.formatNumber(parsed, {notation: "compact", maximumFractionDigits: 2}) : "—";
}

function bytes(value) {
  return Dashboard.formatBytes(value);
}

function signedBytes(value) {
  return Dashboard.formatBytes(value, {showPositive: true});
}

function percent(value, digits = 1) {
  if (value == null || value === "") return "—";
  const parsed = Number(value);
  return Number.isFinite(parsed) ? `${(parsed * 100).toFixed(digits)}%` : "—";
}

function ageLabel(value) {
  return Dashboard.formatAge(value, {emptyLabel: "無更新時間", hourDigits: 0, dayDigits: 0});
}

function durationLabel(value) {
  const seconds = Number(value);
  if (!Number.isFinite(seconds) || seconds < 0) return "—";
  if (seconds < 60) return `${Math.max(0, Math.ceil(seconds))} 秒`;
  if (seconds < 3600) return `${Math.ceil(seconds / 60)} 分鐘`;
  if (seconds < 86400) {
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.ceil((seconds % 3600) / 60);
    return minutes > 0 ? `${hours} 小時 ${minutes} 分鐘` : `${hours} 小時`;
  }
  if (seconds < 365 * 86400) {
    const days = Math.floor(seconds / 86400);
    const hours = Math.round((seconds % 86400) / 3600);
    return hours > 0 ? `${days} 天 ${hours} 小時` : `${days} 天`;
  }
  const years = Math.floor(seconds / (365 * 86400));
  const months = Math.round((seconds % (365 * 86400)) / (30.4375 * 86400));
  return months > 0 ? `${years} 年 ${months} 個月` : `${years} 年`;
}

function etaPresentation(eta) {
  const item = eta && typeof eta === "object" ? eta : {};
  const confidence = ({high: "高信心", medium: "中信心", low: "低信心", none: "無樣本"})[item.confidence] || "";
  const state = String(item.state || "unknown");
  const duration = durationLabel(item.remaining_seconds);
  const staticValues = {
    complete: "已完成",
    up_to_date: "目前已更新",
    continuous: "持續擷取",
    on_demand: "按需執行",
    waiting_upstream: "等待上游",
    unknown: "尚無法估算",
  };
  let value = staticValues[state] || (duration === "—" ? "尚無法估算" : `約 ${duration}`);
  if (state === "paused" && duration !== "—") value = `約 ${duration}（執行時間）`;

  const details = [];
  if (item.estimated_complete_at_utc) {
    const prefix = item.assumption ? "情境完成" : "預計完成";
    details.push(`${prefix} ${localTime(item.estimated_complete_at_utc, {year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit"})}`);
  } else if (state === "paused" && duration !== "—") {
    details.push("恢復執行後開始計時，暫無固定完成日期");
  }
  if (Number.isFinite(Number(item.processing_seconds)) && Number(item.processing_seconds) >= 0 && item.assumption) {
    details.push(`純下載約 ${durationLabel(item.processing_seconds)}`);
  }
  if (Number.isFinite(Number(item.quota_windows_remaining)) && Number(item.quota_windows_remaining) > 0) {
    details.push(`約 ${number(item.quota_windows_remaining)} 個同等額度窗口`);
  }
  if (confidence) details.push(confidence);
  return {state, value, detail: details.join(" · ") || "—", basis: item.basis || "尚無估算依據"};
}

function localTime(value, options = {}) {
  const parsed = new Date(value);
  if (!value || Number.isNaN(parsed.getTime())) return "—";
  return parsed.toLocaleString("zh-TW", {timeZone: "Asia/Taipei", hour12: false, ...options});
}

function trailingRange(rows, rangeKey, timestampOf) {
  const timed = (Array.isArray(rows) ? rows : []).map((row) => [row, timestampOf(row)]).filter((item) => Number.isFinite(item[1]));
  if (!timed.length || rangeKey === "all") return timed.map((item) => item[0]);
  const anchor = Math.max(...timed.map((item) => item[1]));
  const cutoff = anchor - TIME_RANGES[rangeKey];
  return timed.filter((item) => item[1] >= cutoff).map((item) => item[0]);
}

function syncTimeRangeControl(id, selected) {
  $(id).querySelectorAll("button[data-range]").forEach((button) => {
    button.setAttribute("aria-pressed", String(button.dataset.range === selected));
  });
}

function setText(id, value) { Dashboard.setText(id, value); }

function healthPresentation(health) {
  const key = String(health || "unavailable");
  const labels = {active: "資料正常", waiting: "流量保護中", stale: "資料待更新", degraded: "部分管線需注意", unavailable: "暫時離線"};
  return {key, label: labels[key] || "狀態未知"};
}

function pipelineStatusLabel(status) {
  return ({active: "運行中", ready: "可使用", complete: "已完成", waiting: "等待中", partial: "部分完成", failed: "執行失敗", stopped: "已停止", unavailable: "無資料"})[status] || "待確認";
}

function categoryLabel(category) {
  return ({historical: "歷史下載", realtime: "即時訂閱", derived: "衍生資料", reference: "合約目錄", on_demand: "隨需行情"})[category] || "其他";
}

function quotaLabel(quota) {
  return ({historical: "計入歷史流量", realtime: "不扣歷史流量", none: "不呼叫 API"})[quota] || "配額待確認";
}

function storageClassLabel(storageClass) {
  return ({source: "永豐來源", derived: "本機衍生", reference: "合約目錄", operations: "狀態／稽核"})[storageClass] || "其他";
}

function usageStatusLabel(status) {
  return ({measured: "已量測", unattributed: "待新查詢事件", quota_exempt: "官方免計額度", local_only: "本機處理"})[status] || "待確認";
}

function metricValue(metric) {
  const value = metric?.value;
  const format = metric?.format;
  if (format === "bytes") return bytes(value);
  if (format === "compact") return compact(value);
  if (format === "percent") return percent(value, 1);
  if (format === "datetime") return localTime(value, {month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit"});
  const rendered = number(value);
  return metric?.unit && rendered !== "—" ? `${rendered} ${metric.unit}` : rendered;
}

function node(name, className = "", text = null) {
  const element = document.createElement(name);
  if (className) element.className = className;
  if (text != null) element.textContent = String(text);
  return element;
}

function pipelineCard(item) {
  const article = node("article", `pipeline-card state-${item.status || "unavailable"}`);
  article.dataset.category = item.category || "other";
  article.dataset.pipelineId = item.id || "";

  const header = node("header", "pipeline-card-header");
  const tags = node("div", "pipeline-tags");
  tags.append(node("span", "category-tag", categoryLabel(item.category)));
  tags.append(node("span", `pipeline-status ${item.status || "unavailable"}`, item.status_label || pipelineStatusLabel(item.status)));
  header.append(tags, node("span", "api-surface", item.api_surface || "—"));

  const title = node("h3", "", item.title || "未命名管線");
  const detail = node("p", "pipeline-detail", item.detail || "—");
  article.append(header, title, detail);

  if (item.coverage && Number.isFinite(Number(item.coverage.ratio))) {
    const ratio = Math.max(0, Math.min(1, Number(item.coverage.ratio)));
    const coverage = node("div", "mini-progress");
    const copy = node("div", "mini-progress-copy");
    copy.append(node("span", "", item.coverage.label || "覆蓋率"), node("strong", "", percent(ratio, 2)));
    const track = node("progress", "mini-progress-track");
    track.setAttribute("max", "100");
    track.setAttribute("value", String(ratio * 100));
    track.setAttribute("aria-label", `${item.coverage.label || "覆蓋率"} ${percent(ratio, 2)}`);
    coverage.append(copy, track, node("small", "", `${number(item.coverage.current)} / ${number(item.coverage.total)} ${item.coverage.unit || ""}`));
    article.append(coverage);
  }

  const etaView = etaPresentation(item.eta);
  const eta = node("div", `pipeline-eta eta-${etaView.state}`);
  eta.append(
    node("span", "eta-label", "預計還要"),
    node("strong", "eta-value", etaView.value),
    node("small", "eta-detail", etaView.detail),
    node("small", "eta-basis", etaView.basis),
  );
  article.append(eta);

  const metrics = node("dl", "pipeline-metrics");
  (Array.isArray(item.metrics) ? item.metrics : []).forEach((metric) => {
    const row = node("div");
    row.append(node("dt", "", metric.label || "—"), node("dd", "", metricValue(metric)));
    metrics.append(row);
  });
  article.append(metrics);

  const fields = node("div", "field-block");
  fields.append(node("span", "field-label", "包含資料"));
  const chips = node("div", "field-chips");
  (Array.isArray(item.fields) ? item.fields : []).forEach((field) => chips.append(node("span", "", field)));
  fields.append(chips); article.append(fields);

  const warnings = Array.isArray(item.warnings) ? item.warnings.filter(Boolean) : [];
  if (warnings.length) {
    const list = node("ul", "pipeline-warnings");
    warnings.forEach((warning) => list.append(node("li", "", warning)));
    article.append(list);
  }

  const footer = node("footer", "pipeline-footer");
  footer.append(node("span", `quota-tag quota-${item.quota || "unknown"}`, quotaLabel(item.quota)));
  const serviceText = item.service ? (item.service.active ? "服務運行" : item.service.state === "manual" ? "手動執行" : item.service.state === "unavailable" ? "無獨立服務" : "服務停止") : "資料產物";
  footer.append(node("span", "pipeline-age", `${serviceText} · 更新 ${ageLabel(item.latest_age_seconds)}`));
  article.append(footer);
  return article;
}

function renderPipelines(pipelines) {
  latestPipelines = Array.isArray(pipelines) ? pipelines : [];
  const visible = activeFilter === "all" ? latestPipelines : latestPipelines.filter((item) => item.category === activeFilter);
  const grid = $("pipeline-grid");
  grid.replaceChildren(...visible.map(pipelineCard));
  $("pipeline-empty").hidden = visible.length > 0;
}

function updatePipelineDynamics(pipelines) {
  const byId = new Map((Array.isArray(pipelines) ? pipelines : []).map((item) => [String(item.id || ""), item]));
  document.querySelectorAll(".pipeline-card[data-pipeline-id]").forEach((card) => {
    const item = byId.get(card.dataset.pipelineId || "");
    const target = card.querySelector(".pipeline-age");
    if (!item || !target) return;
    const serviceText = item.service ? (item.service.active ? "服務運行" : item.service.state === "manual" ? "手動執行" : item.service.state === "unavailable" ? "無獨立服務" : "服務停止") : "資料產物";
    target.textContent = `${serviceText} · 更新 ${ageLabel(item.latest_age_seconds)}`;
    const etaView = etaPresentation(item.eta);
    const etaNode = card.querySelector(".pipeline-eta");
    if (!etaNode) return;
    etaNode.className = `pipeline-eta eta-${etaView.state}`;
    const valueNode = etaNode.querySelector(".eta-value");
    const detailNode = etaNode.querySelector(".eta-detail");
    const basisNode = etaNode.querySelector(".eta-basis");
    if (valueNode) valueNode.textContent = etaView.value;
    if (detailNode) detailNode.textContent = etaView.detail;
    if (basisNode) basisNode.textContent = etaView.basis;
  });
}

function backfillLabel(state) {
  return ({downloading: "下載中", waiting_quota: "等待流量重置", waiting_market: "讓即時行情優先", complete: "全部完成", stopped: "服務停止"})[state] || "狀態未知";
}

function captureLabel(state) {
  return ({capturing: "持續寫入", quiet: "目前無新資料", starting: "啟動中", stopped: "服務停止"})[state] || "狀態未知";
}

function contractStatusLabel(state) {
  return ({complete: "完成", partial: "進行中"})[state] || "待確認";
}

function svgNode(name, attrs = {}, text = null) {
  const node = document.createElementNS(SVG_NS, name);
  Object.entries(attrs).forEach(([key, value]) => node.setAttribute(key, String(value)));
  if (text != null) node.textContent = String(text);
  return node;
}

function syncTrafficLegend() {
  document.querySelectorAll("#traffic-legend button[data-series]").forEach((button) => {
    const visible = !hiddenTrafficSeries.has(button.dataset.series);
    button.classList.toggle("is-hidden", !visible);
    button.setAttribute("aria-pressed", String(visible));
    button.setAttribute("aria-label", `${visible ? "隱藏" : "顯示"}${button.textContent.trim()}曲線`);
  });
}

function renderTrafficChart(history, guardFraction) {
  const svg = $("traffic-chart");
  while (svg.lastChild && !["title", "desc"].includes(svg.lastChild.localName)) svg.lastChild.remove();
  latestTrafficHistory = Array.isArray(history) ? history : [];
  latestTrafficGuard = guardFraction;
  const rows = trailingRange(latestTrafficHistory, trafficTimeRange, (row) => new Date(row.observed_at_utc || "").getTime()).filter((row) => Number(row.limit_bytes) > 0);
  const usageVisible = !hiddenTrafficSeries.has("usage");
  const guardVisible = !hiddenTrafficSeries.has("guard");
  const allHidden = !usageVisible && !guardVisible;
  const chartEmpty = $("chart-empty");
  chartEmpty.textContent = allHidden
    ? "所有曲線已隱藏；點選圖例色點可重新顯示。"
    : "累積兩筆觀測後會畫出趨勢；目前仍會顯示最新用量。";
  chartEmpty.hidden = allHidden ? false : !(usageVisible && rows.length <= 1);
  syncTrafficLegend();
  const width = 960, height = 300, left = 58, right = 22, top = 18, bottom = 42;
  const plotWidth = width - left - right, plotHeight = height - top - bottom;
  [0, 0.5, 1].forEach((ratio) => {
    const y = top + (1 - ratio) * plotHeight;
    svg.append(svgNode("line", {x1: left, y1: y, x2: width - right, y2: y, class: "grid-line"}));
    svg.append(svgNode("text", {x: left - 12, y: y + 4, class: "axis-label", "text-anchor": "end"}, `${ratio * 100}%`));
  });
  const guard = Number.isFinite(Number(guardFraction)) ? Number(guardFraction) : 0.9;
  if (guardVisible) {
    const guardY = top + (1 - guard) * plotHeight;
    svg.append(svgNode("line", {x1: left, y1: guardY, x2: width - right, y2: guardY, class: "guard-line"}));
    svg.append(svgNode("text", {x: width - right, y: guardY - 8, class: "guard-label", "text-anchor": "end"}, `安全上限 ${percent(guard, 0)}`));
  }
  if (!rows.length) { setText("traffic-range-note", `${TIME_RANGE_LABELS[trafficTimeRange]}內沒有保留觀測。`); return; }
  const points = rows.map((row, index) => {
    const x = rows.length === 1 ? left + plotWidth : left + (index / (rows.length - 1)) * plotWidth;
    const ratio = Math.min(1, Math.max(0, Number(row.used_bytes) / Number(row.limit_bytes)));
    return [x, top + (1 - ratio) * plotHeight, ratio];
  });
  if (usageVisible) {
    const linePath = points.map((point, index) => `${index ? "L" : "M"}${point[0].toFixed(2)},${point[1].toFixed(2)}`).join(" ");
    const areaPath = `${linePath} L${points.at(-1)[0]},${height - bottom} L${points[0][0]},${height - bottom} Z`;
    svg.append(svgNode("path", {d: areaPath, class: "usage-area"}));
    svg.append(svgNode("path", {d: linePath, class: "usage-line"}));
    const latest = points.at(-1);
    svg.append(svgNode("circle", {cx: latest[0], cy: latest[1], r: 5, class: "usage-point"}));
    svg.append(svgNode("text", {x: latest[0] - 8, y: latest[1] - 12, class: "latest-label", "text-anchor": "end"}, percent(latest[2], 1)));
  }
  const firstTime = localTime(rows[0].observed_at_utc, {hour: "2-digit", minute: "2-digit"});
  const lastTime = localTime(rows.at(-1).observed_at_utc, {hour: "2-digit", minute: "2-digit"});
  svg.append(svgNode("text", {x: left, y: height - 12, class: "axis-label"}, firstTime));
  svg.append(svgNode("text", {x: width - right, y: height - 12, class: "axis-label", "text-anchor": "end"}, lastTime));
  setText("traffic-range-note", `${TIME_RANGE_LABELS[trafficTimeRange]} · ${localTime(rows[0].observed_at_utc)} ～ ${localTime(rows.at(-1).observed_at_utc)} · ${number(rows.length)} 點；超出來源保留期的區間不會補造資料。`);
}

function appendCell(row, value, className = "") {
  const cell = document.createElement("td");
  cell.textContent = value;
  if (className) cell.className = className;
  row.append(cell);
}

function renderTrafficTable(history) {
  const body = $("traffic-body");
  body.replaceChildren();
  const rows = Array.isArray(history) ? history.slice(-8).reverse() : [];
  if (!rows.length) {
    const row = document.createElement("tr");
    const cell = document.createElement("td"); cell.colSpan = 5; cell.textContent = "尚無用量觀測"; row.append(cell); body.append(row); return;
  }
  rows.forEach((item) => {
    const row = document.createElement("tr");
    appendCell(row, localTime(item.observed_at_utc));
    appendCell(row, item.source_label || "歷史行情查詢");
    appendCell(row, bytes(item.used_bytes));
    appendCell(row, bytes(item.remaining_bytes));
    appendCell(row, percent(item.used_ratio, 2));
    body.append(row);
  });
}

function renderTrafficLedger(ledger) {
  const totals = ledger?.totals || {};
  const observed = Boolean(ledger?.updated_at_utc);
  setText("ledger-queries", number(observed ? totals.queries : null));
  setText("ledger-avoided", number(observed ? totals.avoided_queries : null));
  setText("ledger-bytes", bytes(observed ? totals.observed_usage_delta_bytes : null));
  setText("ledger-failures", number(observed ? totals.failures : null));
  setText("ledger-stream-ticks", compact(observed ? totals.stream_tick_events : null));
  setText("ledger-stream-books", compact(observed ? totals.stream_book_events : null));
  setText("ledger-stream-storage", bytes(observed ? totals.stream_stored_bytes : null));
  setText("ledger-stream-dropped", number(observed ? totals.stream_dropped_events : null));
  setText("ledger-observed", ledger?.updated_at_utc ? `更新 ${localTime(ledger.updated_at_utc)}` : "尚無紀錄");
  const body = $("ledger-body");
  body.replaceChildren();
  const rows = Array.isArray(ledger?.by_consumer) ? ledger.by_consumer : [];
  if (!rows.length) {
    const row = document.createElement("tr");
    const cell = document.createElement("td"); cell.colSpan = 8; cell.textContent = "尚無查詢或即時串流紀錄"; row.append(cell); body.append(row); return;
  }
  rows.sort((left, right) => Number(right.queries || 0) - Number(left.queries || 0)).forEach((item) => {
    const row = document.createElement("tr");
    appendCell(row, item.name || "unknown");
    appendCell(row, number(item.queries));
    appendCell(row, number(item.avoided_queries));
    appendCell(row, compact(item.stream_tick_events));
    appendCell(row, compact(item.stream_book_events));
    appendCell(row, bytes(item.stream_stored_bytes));
    appendCell(row, bytes(item.observed_usage_delta_bytes));
    appendCell(row, `${number(item.failures)} / ${number(item.stream_dropped_events)}`);
    body.append(row);
  });
}

function renderTrafficBreakdown(rows) {
  const body = $("traffic-breakdown-body");
  body.replaceChildren();
  const items = Array.isArray(rows) ? rows : [];
  if (!items.length) {
    const row = document.createElement("tr");
    const cell = document.createElement("td"); cell.colSpan = 8; cell.textContent = "尚無流量分類"; row.append(cell); body.append(row); return;
  }
  items.forEach((item) => {
    const row = document.createElement("tr");
    appendCell(row, item.title || "—");
    appendCell(row, item.api_surface || "—", "api-code");
    appendCell(row, item.price_label || "—");
    appendCell(row, quotaLabel(item.quota_class));
    appendCell(row, bytes(item.attributed_bytes));
    appendCell(row, `${number(item.queries)} / ${number(item.avoided_queries)}`);
    appendCell(row, compact(item.stream_events));
    appendCell(row, usageStatusLabel(item.usage_status), `usage-status ${item.usage_status || "unknown"}`);
    body.append(row);
  });
}

function renderStorageGrowthChart(rows) {
  const svg = $("storage-growth-chart");
  while (svg.lastChild && !["title", "desc"].includes(svg.lastChild.localName)) svg.lastChild.remove();
  latestStorageGrowth = Array.isArray(rows) ? rows : [];
  const items = trailingRange(latestStorageGrowth, storageTimeRange, (row) => new Date(`${row.date || ""}T00:00:00+08:00`).getTime()).filter((row) => Number.isFinite(Number(row.bytes)));
  const maximum = Math.max(0, ...items.map((row) => Number(row.bytes)));
  $("storage-chart-empty").hidden = items.length > 0 && maximum > 0;
  if (!items.length || maximum <= 0) { setText("storage-range-note", `${TIME_RANGE_LABELS[storageTimeRange]}內沒有完整日成長資料。`); return; }
  const width = 960, height = 300, left = 72, right = 18, top = 20, bottom = 44;
  const plotWidth = width - left - right, plotHeight = height - top - bottom;
  [0, 0.5, 1].forEach((ratio) => {
    const y = top + (1 - ratio) * plotHeight;
    svg.append(svgNode("line", {x1: left, y1: y, x2: width - right, y2: y, class: "grid-line"}));
    svg.append(svgNode("text", {x: left - 10, y: y + 4, class: "axis-label", "text-anchor": "end"}, bytes(maximum * ratio)));
  });
  const slot = plotWidth / items.length;
  items.forEach((item, index) => {
    const barHeight = (Number(item.bytes) / maximum) * plotHeight;
    svg.append(svgNode("rect", {
      x: left + index * slot + Math.max(1, slot * 0.14),
      y: top + plotHeight - barHeight,
      width: Math.max(2, slot * 0.72),
      height: Math.max(1, barHeight),
      rx: 2,
      class: "storage-bar",
    }));
  });
  svg.append(svgNode("text", {x: left, y: height - 13, class: "axis-label"}, items[0].date || "—"));
  svg.append(svgNode("text", {x: width - right, y: height - 13, class: "axis-label", "text-anchor": "end"}, items.at(-1).date || "—"));
  setText("storage-range-note", `${TIME_RANGE_LABELS[storageTimeRange]} · ${items[0].date || "—"} ～ ${items.at(-1).date || "—"} · ${number(items.length)} 日；來源目前保留的完整日才會顯示。`);
}

function renderStorageBars(datasets, totalBytes) {
  const container = $("storage-bars");
  container.replaceChildren();
  const items = Array.isArray(datasets) ? datasets : [];
  if (!items.length || !Number.isFinite(Number(totalBytes)) || Number(totalBytes) <= 0) {
    container.append(node("p", "empty", "背景容量掃描尚未完成"));
    return;
  }
  items.slice(0, 9).forEach((item) => {
    const ratio = Math.max(0, Math.min(1, Number(item.bytes || 0) / Number(totalBytes)));
    const row = node("div", "storage-bar-row");
    const copy = node("div", "storage-bar-copy");
    copy.append(node("span", "", item.title || "—"), node("strong", "", `${bytes(item.bytes)} · ${percent(ratio, 1)}`));
    const progress = node("progress", "storage-progress");
    progress.max = 100;
    progress.value = ratio * 100;
    progress.setAttribute("aria-label", `${item.title || "資料"}占總容量 ${percent(ratio, 1)}`);
    row.append(copy, progress);
    container.append(row);
  });
}

function renderStorageTable(datasets) {
  const body = $("storage-body");
  body.replaceChildren();
  const rows = Array.isArray(datasets) ? datasets : [];
  if (!rows.length) {
    const row = document.createElement("tr");
    const cell = document.createElement("td"); cell.colSpan = 9; cell.textContent = "背景容量掃描尚未完成"; row.append(cell); body.append(row); return;
  }
  rows.forEach((item) => {
    const row = document.createElement("tr");
    appendCell(row, item.title || "—");
    appendCell(row, storageClassLabel(item.storage_class));
    appendCell(row, quotaLabel(item.quota_class));
    appendCell(row, bytes(item.bytes));
    appendCell(row, number(item.files));
    appendCell(row, bytes(item.growth_window_bytes));
    appendCell(row, bytes(item.average_daily_growth_bytes));
    appendCell(row, `${number(item.active_growth_days)} / ${number(item.growth_window_days)}`);
    appendCell(row, localTime(item.latest_changed_at_utc));
    body.append(row);
  });
}

function storageDaysLabel(value) {
  if (value == null || value === "" || !Number.isFinite(Number(value))) return "—";
  const days = Number(value);
  if (days >= 365) return `${number(days / 365, 1)} 年`;
  return `${number(days, 0)} 天`;
}

function renderStorage(storage, renderHeavy) {
  const summary = storage?.summary || {};
  setText("storage-total", bytes(summary.total_bytes));
  setText("storage-files", `${number(summary.files)} 個實體檔案 · ${number(summary.datasets)} 個群組`);
  setText("storage-source", bytes(summary.source_bytes));
  setText("storage-derived", bytes(summary.derived_bytes));
  const hasObservedGrowth = Number(summary.observed_growth_days || 0) >= 7
    && Number.isFinite(Number(summary.observed_average_daily_net_growth_bytes));
  setText("storage-growth-label", hasObservedGrowth ? "實測日均淨成長" : "mtime 日均變動量");
  setText("storage-growth", hasObservedGrowth
    ? signedBytes(summary.observed_average_daily_net_growth_bytes)
    : bytes(summary.average_daily_growth_bytes));
  setText("storage-growth-window", hasObservedGrowth
    ? `${number(summary.observed_growth_days)} 個完整日的每日總量快照`
    : `樣本未滿 7 日；近 ${number(summary.growth_window_days)} 日變動檔案共 ${bytes(summary.growth_window_bytes)}`);
  setText("storage-free", bytes(summary.disk_free_bytes));
  setText("storage-disk-ratio", `磁碟已使用 ${percent(summary.disk_used_ratio, 1)} · 總容量 ${bytes(summary.disk_total_bytes)}`);
  setText("storage-days", storageDaysLabel(summary.estimated_days_remaining));
  setText("storage-observed", storage?.generated_at_utc ? `掃描 ${ageLabel(storage.age_seconds)} · 耗時 ${number(storage.scan_seconds, 1)} 秒` : "背景掃描尚未完成");
  if (renderHeavy) {
    renderStorageGrowthChart(storage?.daily_growth);
    renderStorageBars(storage?.datasets, summary.total_bytes);
    renderStorageTable(storage?.datasets);
  }
}

function renderContracts(rows) {
  const body = $("contracts-body");
  body.replaceChildren();
  if (!Array.isArray(rows) || !rows.length) {
    const row = document.createElement("tr");
    const cell = document.createElement("td"); cell.colSpan = 5; cell.textContent = "尚無回補 manifest"; row.append(cell); body.append(row); return;
  }
  rows.forEach((item) => {
    const row = document.createElement("tr");
    appendCell(row, item.contract || "—", "contract-code");
    appendCell(row, contractStatusLabel(item.status));
    appendCell(row, `${number(item.resolved_dates)} / ${number(item.expected_dates)}`);
    appendCell(row, number(item.rows));
    appendCell(row, bytes(item.stored_bytes));
    body.append(row);
  });
}

function renderAlert(data) {
  const alert = $("pipeline-alert");
  const state = data.backfill?.state;
  const failed = (Array.isArray(data.pipelines) ? data.pipelines : []).filter((item) => item.status === "failed");
  if (failed.length && state === "waiting_quota") {
    alert.hidden = false;
    setText("alert-title", `${failed.length} 條管線執行失敗，歷史下載也已進入流量保護`);
    setText("alert-copy", `${failed[0].title}：${failed[0].detail}。安全可用額度剩 ${bytes(data.traffic?.safe_remaining_bytes)}。`);
  } else if (failed.length) {
    alert.hidden = false;
    setText("alert-title", `${failed.length} 條資料管線需要處理`);
    setText("alert-copy", `${failed[0].title}：${failed[0].detail}。既有資料仍保留，公開面板不會自行重啟服務。`);
  } else if (state === "waiting_quota") {
    alert.hidden = false;
    setText("alert-title", "歷史下載已在安全閘門前暫停");
    setText("alert-copy", `帳面仍有 ${bytes(data.traffic?.remaining_bytes)}，但安全可用只剩 ${bytes(data.traffic?.safe_remaining_bytes)}；即時行情擷取不受影響。`);
  } else if (data.health === "degraded") {
    alert.hidden = false;
    setText("alert-title", "部分資料服務沒有運行");
    setText("alert-copy", "請看下方下載器與即時擷取狀態；公開面板不會自行啟動或重啟服務。");
  } else {
    alert.hidden = true;
  }
}

function render(data) {
  const heavyRevision = JSON.stringify([
    data.pipeline_summary,
    (data.pipelines || []).map((item) => [
      item.id, item.status, item.status_label, item.detail, item.coverage,
      item.latest_at_utc, item.fields, item.metrics, item.warnings, item.service,
      item.eta ? [item.eta.state, item.eta.remaining_seconds, item.eta.confidence,
        item.eta.basis, item.eta.processing_seconds, item.eta.sample_units,
        item.eta.sample_seconds, item.eta.units_per_hour,
        item.eta.quota_windows_remaining, item.eta.assumption] : null,
    ]),
    data.traffic?.history,
    data.traffic_ledger,
    data.traffic_breakdown,
    data.storage,
    data.traffic?.guard_fraction,
    data.backfill?.contracts,
  ]);
  const renderHeavy = heavyRevision !== lastHeavyRevision;
  const health = healthPresentation(data.health);
  const status = $("connection-status");
  status.className = `status ${health.key}`;
  status.lastChild.textContent = health.label;
  setText("source-freshness", `最新來源 ${ageLabel(data.source_age_seconds)} · ${localTime(data.generated_at_utc)}`);
  renderAlert(data);

  const summary = data.pipeline_summary || {};
  setText("pipeline-total", number(summary.total));
  setText("pipeline-scope", `${number(summary.historical)} 條歷史 · ${number(summary.realtime)} 條即時`);
  setText("pipeline-active", number(summary.active));
  setText("pipeline-ready", number(summary.ready));
  setText("pipeline-attention", number(summary.attention));
  if (renderHeavy) renderPipelines(data.pipelines);
  else updatePipelineDynamics(data.pipelines);

  const traffic = data.traffic || {};
  setText("traffic-used", `${bytes(traffic.used_bytes)} / ${bytes(traffic.limit_bytes)}`);
  setText("traffic-used-ratio", `${percent(traffic.used_ratio, 2)} 已使用`);
  setText("traffic-remaining", bytes(traffic.remaining_bytes));
  setText("traffic-safe-remaining", bytes(traffic.safe_remaining_bytes));
  setText("traffic-reset", traffic.reset_policy || "—");
  setText("traffic-price", traffic.pricing_evidence_label || "未取得費用欄位");
  setText("traffic-tier", bytes(traffic.limit_bytes));
  setText("traffic-attributed", bytes(traffic.attributed_bytes));
  setText("traffic-unattributed", bytes(traffic.unattributed_bytes));
  const trafficObserved = traffic.observed_at_utc ? (Date.now() - new Date(traffic.observed_at_utc).getTime()) / 1000 : null;
  setText("traffic-observed", `用量觀測 ${ageLabel(trafficObserved)}`);
  if (renderHeavy) {
    renderTrafficChart(traffic.history, traffic.guard_fraction);
    renderTrafficTable(traffic.history);
    renderTrafficLedger(data.traffic_ledger || {});
    renderTrafficBreakdown(data.traffic_breakdown || []);
  }

  renderStorage(data.storage || {}, renderHeavy);

  const backfill = data.backfill || {};
  const ratio = Number(backfill.progress_ratio) || 0;
  setText("fleet-progress-label", percent(ratio, 2));
  setText("fleet-progress-detail", `${number(backfill.resolved_contract_dates)} / ${number(backfill.expected_contract_dates)} 個合約交易日已有 receipt`);
  const fleetEta = etaPresentation(backfill.eta);
  setText("fleet-eta-label", fleetEta.value);
  setText("fleet-eta-detail", `${fleetEta.detail} · ${fleetEta.basis}`);
  $("fleet-progress-bar").value = Math.max(0, Math.min(100, ratio * 100));
  setText("contracts-complete", `${number(backfill.completed_contracts)} / ${number(backfill.inventory_contracts)}`);
  setText("contracts-started", `${number(backfill.started_contracts)} 個合約已開始`);
  setText("current-contract", backfill.current_contract || "—");
  const currentExpected = Number(backfill.current_contract_expected_dates) || 0;
  const currentResolved = Number(backfill.current_contract_resolved_dates) || 0;
  setText("current-contract-progress", `${number(currentResolved)} / ${number(currentExpected)} 日 · ${percent(currentExpected ? currentResolved / currentExpected : null, 1)}`);
  setText("backfill-rows", compact(backfill.rows));
  setText("backfill-storage", `${bytes(backfill.stored_bytes)} 已落盤`);
  setText("backfill-state", backfillLabel(backfill.state));
  setText("backfill-target", `目標截至 ${backfill.target_end_date || "—"}`);
  if (renderHeavy) renderContracts(backfill.contracts);

  const capture = data.capture || {};
  setText("capture-state", captureLabel(capture.state));
  setText("capture-freshness", `最新檔案 ${ageLabel(capture.latest_file_age_seconds)}`);
  setText("capture-session", `${capture.session === "night" ? "夜盤" : capture.session === "day" ? "日盤" : "—"} · ${capture.trade_date || "—"}`);
  setText("capture-workers", number(capture.workers));
  setText("capture-subscriptions", `${number(capture.contracts)} 合約 · ${number(capture.subscriptions)} 訂閱`);
  setText("capture-stop", capture.scheduled_stop_at_local ? localTime(capture.scheduled_stop_at_local, {month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit"}) : "—");
  $("capture-dot").className = capture.state === "capturing" ? "pulse active" : "pulse";
  lastHeavyRevision = heavyRevision;
}

async function refresh() {
  if (document.hidden || refreshInFlight) return;
  refreshInFlight = true;
  try {
    const response = await fetchWithTimeout("api/status", {cache: "no-store"});
    render(await Dashboard.readJsonResponse(response, {expectedRoot: "object"}));
  } catch (_error) {
    const status = $("connection-status");
    status.className = "status unavailable";
    status.lastChild.textContent = "暫時離線";
    setText("source-freshness", "無法取得公開監控資料");
  } finally {
    refreshInFlight = false;
  }
}

document.querySelectorAll(".filter").forEach((button) => {
  button.addEventListener("click", () => {
    activeFilter = button.dataset.filter || "all";
    document.querySelectorAll(".filter").forEach((candidate) => {
      const active = candidate === button;
      candidate.classList.toggle("active", active);
      candidate.setAttribute("aria-pressed", String(active));
    });
    renderPipelines(latestPipelines);
  });
});
$("traffic-time-range").addEventListener("click", (event) => {
  const button = event.target.closest("button[data-range]");
  if (!button || !(button.dataset.range in TIME_RANGES)) return;
  trafficTimeRange = button.dataset.range;
  try { localStorage.setItem("shioaji-traffic-time-range", trafficTimeRange); } catch (_error) { /* optional */ }
  syncTimeRangeControl("traffic-time-range", trafficTimeRange);
  renderTrafficChart(latestTrafficHistory, latestTrafficGuard);
});
$("traffic-legend").addEventListener("click", (event) => {
  const button = event.target.closest("button[data-series]");
  if (!button) return;
  const seriesId = button.dataset.series;
  if (hiddenTrafficSeries.has(seriesId)) hiddenTrafficSeries.delete(seriesId);
  else hiddenTrafficSeries.add(seriesId);
  try { localStorage.setItem(HIDDEN_TRAFFIC_SERIES_STORAGE_KEY, JSON.stringify([...hiddenTrafficSeries])); } catch (_error) { /* optional */ }
  renderTrafficChart(latestTrafficHistory, latestTrafficGuard);
});
$("storage-time-range").addEventListener("click", (event) => {
  const button = event.target.closest("button[data-range]");
  if (!button || !(button.dataset.range in TIME_RANGES)) return;
  storageTimeRange = button.dataset.range;
  try { localStorage.setItem("shioaji-storage-time-range", storageTimeRange); } catch (_error) { /* optional */ }
  syncTimeRangeControl("storage-time-range", storageTimeRange);
  renderStorageGrowthChart(latestStorageGrowth);
});
syncTimeRangeControl("traffic-time-range", trafficTimeRange);
syncTimeRangeControl("storage-time-range", storageTimeRange);
syncTrafficLegend();
Dashboard.scheduleRefresh(refresh, {intervalMs: REFRESH_MS});

"use strict";

const REFRESH_MS = 10000;
const SVG_NS = "http://www.w3.org/2000/svg";
const $ = (id) => document.getElementById(id);

function number(value, digits = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed.toLocaleString("zh-TW", {maximumFractionDigits: digits}) : "—";
}

function compact(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? new Intl.NumberFormat("zh-TW", {notation: "compact", maximumFractionDigits: 2}).format(parsed) : "—";
}

function bytes(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return "—";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let amount = Math.max(0, parsed);
  let unit = 0;
  while (amount >= 1024 && unit < units.length - 1) { amount /= 1024; unit += 1; }
  return `${amount.toLocaleString("zh-TW", {maximumFractionDigits: amount >= 100 ? 0 : 2})} ${units[unit]}`;
}

function percent(value, digits = 1) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? `${(parsed * 100).toFixed(digits)}%` : "—";
}

function ageLabel(value) {
  const seconds = Number(value);
  if (!Number.isFinite(seconds)) return "無更新時間";
  if (seconds < 60) return `${Math.max(0, Math.round(seconds))} 秒前`;
  if (seconds < 3600) return `${Math.round(seconds / 60)} 分鐘前`;
  return `${Math.round(seconds / 3600)} 小時前`;
}

function localTime(value, options = {}) {
  const parsed = new Date(value);
  if (!value || Number.isNaN(parsed.getTime())) return "—";
  return parsed.toLocaleString("zh-TW", {timeZone: "Asia/Taipei", hour12: false, ...options});
}

function setText(id, value) { $(id).textContent = value; }

function healthPresentation(health) {
  const key = String(health || "unavailable");
  const labels = {active: "資料正常", waiting: "流量保護中", stale: "資料待更新", degraded: "部分服務異常", unavailable: "暫時離線"};
  return {key, label: labels[key] || "狀態未知"};
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

function renderTrafficChart(history, guardFraction) {
  const svg = $("traffic-chart");
  while (svg.lastChild && !["title", "desc"].includes(svg.lastChild.localName)) svg.lastChild.remove();
  const rows = Array.isArray(history) ? history.filter((row) => Number(row.limit_bytes) > 0) : [];
  $("chart-empty").hidden = rows.length > 1;
  const width = 960, height = 300, left = 58, right = 22, top = 18, bottom = 42;
  const plotWidth = width - left - right, plotHeight = height - top - bottom;
  [0, 0.5, 1].forEach((ratio) => {
    const y = top + (1 - ratio) * plotHeight;
    svg.append(svgNode("line", {x1: left, y1: y, x2: width - right, y2: y, class: "grid-line"}));
    svg.append(svgNode("text", {x: left - 12, y: y + 4, class: "axis-label", "text-anchor": "end"}, `${ratio * 100}%`));
  });
  const guard = Number.isFinite(Number(guardFraction)) ? Number(guardFraction) : 0.9;
  const guardY = top + (1 - guard) * plotHeight;
  svg.append(svgNode("line", {x1: left, y1: guardY, x2: width - right, y2: guardY, class: "guard-line"}));
  svg.append(svgNode("text", {x: width - right, y: guardY - 8, class: "guard-label", "text-anchor": "end"}, `安全上限 ${percent(guard, 0)}`));
  if (!rows.length) return;
  const points = rows.map((row, index) => {
    const x = rows.length === 1 ? left + plotWidth : left + (index / (rows.length - 1)) * plotWidth;
    const ratio = Math.min(1, Math.max(0, Number(row.used_bytes) / Number(row.limit_bytes)));
    return [x, top + (1 - ratio) * plotHeight, ratio];
  });
  const linePath = points.map((point, index) => `${index ? "L" : "M"}${point[0].toFixed(2)},${point[1].toFixed(2)}`).join(" ");
  const areaPath = `${linePath} L${points.at(-1)[0]},${height - bottom} L${points[0][0]},${height - bottom} Z`;
  svg.append(svgNode("path", {d: areaPath, class: "usage-area"}));
  svg.append(svgNode("path", {d: linePath, class: "usage-line"}));
  const latest = points.at(-1);
  svg.append(svgNode("circle", {cx: latest[0], cy: latest[1], r: 5, class: "usage-point"}));
  svg.append(svgNode("text", {x: latest[0] - 8, y: latest[1] - 12, class: "latest-label", "text-anchor": "end"}, percent(latest[2], 1)));
  const firstTime = localTime(rows[0].observed_at_utc, {hour: "2-digit", minute: "2-digit"});
  const lastTime = localTime(rows.at(-1).observed_at_utc, {hour: "2-digit", minute: "2-digit"});
  svg.append(svgNode("text", {x: left, y: height - 12, class: "axis-label"}, firstTime));
  svg.append(svgNode("text", {x: width - right, y: height - 12, class: "axis-label", "text-anchor": "end"}, lastTime));
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
    const cell = document.createElement("td"); cell.colSpan = 4; cell.textContent = "尚無用量觀測"; row.append(cell); body.append(row); return;
  }
  rows.forEach((item) => {
    const row = document.createElement("tr");
    appendCell(row, localTime(item.observed_at_utc));
    appendCell(row, bytes(item.used_bytes));
    appendCell(row, bytes(item.remaining_bytes));
    appendCell(row, percent(item.used_ratio, 2));
    body.append(row);
  });
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
  if (state === "waiting_quota") {
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
  const health = healthPresentation(data.health);
  const status = $("connection-status");
  status.className = `status ${health.key}`;
  status.lastChild.textContent = health.label;
  setText("source-freshness", `最新來源 ${ageLabel(data.source_age_seconds)} · ${localTime(data.generated_at_utc)}`);
  renderAlert(data);

  const traffic = data.traffic || {};
  setText("traffic-used", `${bytes(traffic.used_bytes)} / ${bytes(traffic.limit_bytes)}`);
  setText("traffic-used-ratio", `${percent(traffic.used_ratio, 2)} 已使用`);
  setText("traffic-remaining", bytes(traffic.remaining_bytes));
  setText("traffic-safe-remaining", bytes(traffic.safe_remaining_bytes));
  setText("traffic-reset", traffic.reset_policy || "—");
  setText("traffic-observed", `用量觀測 ${ageLabel((Date.now() - new Date(traffic.observed_at_utc).getTime()) / 1000)}`);
  renderTrafficChart(traffic.history, traffic.guard_fraction);
  renderTrafficTable(traffic.history);

  const backfill = data.backfill || {};
  const ratio = Number(backfill.progress_ratio) || 0;
  setText("fleet-progress-label", percent(ratio, 2));
  setText("fleet-progress-detail", `${number(backfill.resolved_contract_dates)} / ${number(backfill.expected_contract_dates)} 個合約交易日已有 receipt`);
  $("fleet-progress-bar").style.width = `${Math.max(0, Math.min(100, ratio * 100))}%`;
  const track = $("fleet-progress-bar").parentElement;
  track.setAttribute("aria-valuenow", String(Math.max(0, Math.min(100, ratio * 100))));
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
  renderContracts(backfill.contracts);

  const capture = data.capture || {};
  setText("capture-state", captureLabel(capture.state));
  setText("capture-freshness", `最新檔案 ${ageLabel(capture.latest_file_age_seconds)}`);
  setText("capture-session", `${capture.session === "night" ? "夜盤" : capture.session === "day" ? "日盤" : "—"} · ${capture.trade_date || "—"}`);
  setText("capture-workers", number(capture.workers));
  setText("capture-subscriptions", `${number(capture.contracts)} 合約 · ${number(capture.subscriptions)} 訂閱`);
  setText("capture-stop", capture.scheduled_stop_at_local ? localTime(capture.scheduled_stop_at_local, {month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit"}) : "—");
  $("capture-dot").className = capture.state === "capturing" ? "pulse active" : "pulse";
}

async function refresh() {
  if (document.hidden) return;
  try {
    const response = await fetch("api/status", {cache: "no-cache"});
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    render(await response.json());
  } catch (_error) {
    const status = $("connection-status");
    status.className = "status unavailable";
    status.lastChild.textContent = "暫時離線";
    setText("source-freshness", "無法取得公開監控資料");
  }
}

document.addEventListener("visibilitychange", () => { if (!document.hidden) void refresh(); });
void refresh();
window.setInterval(() => void refresh(), REFRESH_MS);

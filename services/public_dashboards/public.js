"use strict";

const REFRESH_MS = 60000;
let refreshInFlight = false;

const Dashboard = window.StockAgentDashboard;
const $ = Dashboard.byId;
const fetchJson = Dashboard.createJsonFetcher({timeoutMs: 15000, cache: "no-store"});

function healthPresentation(value) {
  const health = String(value || "unavailable").toLowerCase();
  const labels = {
    active: "資料正常",
    ready: "資料正常",
    waiting: "休市監控",
    stale: "資料逾時",
    blocked: "策略阻擋",
    critical: "需要注意",
    degraded: "部分異常",
    starting: "啟動／稽核中",
    updating: "回補進行中",
    complete: "封存完成",
    stopped: "程序停止",
    unavailable: "暫時離線",
  };
  return {health: health === "ready" ? "active" : health, label: labels[health] || "狀態未知"};
}

function ageLabel(seconds) {
  return Dashboard.formatAge(seconds, {emptyLabel: "無更新時間", hourDigits: 0, dayDigits: 0});
}

function setHealth(prefix, value, overrideLabel = null) {
  const target = $(`${prefix}-health`);
  const {health, label} = healthPresentation(value);
  target.className = `health ${health}`;
  target.lastChild.textContent = overrideLabel || label;
}

function renderTaifex(data) {
    setHealth("taifex", data.health);
    $("taifex-freshness").textContent = ageLabel(data.source_age_seconds);
    const live = Number(data.live_strategies || 0);
    const coverage = Number(data.book_coverage_ratio);
    $("taifex-summary").textContent = Number.isFinite(coverage)
      ? `${live} 策略 · ${(coverage * 100).toFixed(0)}% 行情`
      : `${live} 個策略`;
}

function renderTw(data) {
    setHealth("tw", data.health);
    $("tw-freshness").textContent = ageLabel(data.source_age_seconds);
    const modes = Number(data.modes || 0);
    const positions = Number(data.open_positions || 0);
    $("tw-summary").textContent = `${modes} 模式 · ${positions} 個持倉`;
}

function bytes(value) {
  return Dashboard.formatBytes(value, {maximumFractionDigits: 1});
}

function renderShioaji(data) {
    setHealth("shioaji", data.health, data.health === "waiting" ? "流量保護" : null);
    $("shioaji-traffic").textContent = `${(Number(data.traffic_used_ratio || 0) * 100).toFixed(1)}% · 安全剩 ${bytes(data.safe_remaining_bytes)}`;
    $("shioaji-progress").textContent = `${Number(data.completed_contracts || 0)}/${Number(data.inventory_contracts || 0)} 合約 · ${(Number(data.progress_ratio || 0) * 100).toFixed(2)}%`;
}

function renderOpenbb(data) {
    setHealth("openbb", data.health, healthPresentation(data.health).label);
    const snapshot = data.snapshot_state === "current" ? "快照新鮮" : "快照逾時";
    $("openbb-freshness").textContent = `${snapshot} · ${ageLabel(data.source_age_seconds)}`;
    $("openbb-progress").textContent = `${Number(data.completion_percent || 0).toFixed(2)}% · ${Number(data.accepted_tasks || 0).toLocaleString("zh-TW")}/${Number(data.total_tasks || 0).toLocaleString("zh-TW")}`;
}

function renderDataMonitor(data) {
    const label = data.health === "active" ? "全部正常" : data.health === "updating" ? "回補進行中" : "有來源需處理";
    setHealth("data", data.health, label);
    $("data-registered").textContent = `${Number(data.registered_items || 0).toLocaleString("zh-TW")} 項`;
    const healthy = Number(data.healthy_or_progressing || 0);
    const attention = Number(data.attention_required || 0);
    $("data-progress").textContent = `${healthy.toLocaleString("zh-TW")} 正常 · ${attention.toLocaleString("zh-TW")} 待處理`;
}

function renderTraffic(data) {
    setHealth("traffic", "active", "即時觀察");
    $("traffic-requests").textContent = `${Number(data.requests_1m || 0).toLocaleString("zh-TW")} 次 · ${Number(data.requests_per_second_1m || 0).toLocaleString("zh-TW", {maximumFractionDigits: 2})} RPS`;
    const latency = Number(data.latency_p95_ms_1m);
    $("traffic-latency").textContent = Number.isFinite(latency) ? `${latency.toLocaleString("zh-TW", {maximumFractionDigits: 2})} ms` : "尚無樣本";
}

function renderUnavailable() {
  for (const prefix of ["taifex", "tw", "shioaji", "openbb", "data", "traffic"]) setHealth(prefix, "unavailable");
  $("taifex-freshness").textContent = "無法取得";
  $("tw-freshness").textContent = "無法取得";
  $("taifex-summary").textContent = "進入面板查看";
  $("tw-summary").textContent = "進入面板查看";
  $("shioaji-traffic").textContent = "無法取得";
  $("shioaji-progress").textContent = "進入面板查看";
  $("openbb-freshness").textContent = "無法取得";
  $("openbb-progress").textContent = "進入面板查看";
  $("data-registered").textContent = "無法取得";
  $("data-progress").textContent = "進入面板查看";
  $("traffic-requests").textContent = "無法取得";
  $("traffic-latency").textContent = "進入面板查看";
}

async function refresh() {
  if (document.hidden || refreshInFlight) return;
  refreshInFlight = true;
  try {
    const data = await fetchJson("api/overview");
    renderTaifex(data.taifex || {});
    renderTw(data.tw || {});
    renderShioaji(data.shioaji || {});
    renderOpenbb(data.openbb || {});
    renderDataMonitor(data.data_monitor || {});
    renderTraffic(data.traffic || {});
  } catch (_error) {
    renderUnavailable();
  } finally {
    refreshInFlight = false;
  }
}

Dashboard.scheduleRefresh(refresh, {intervalMs: REFRESH_MS});

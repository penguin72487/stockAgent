"use strict";

const REFRESH_MS = 60000;
let refreshInFlight = false;

const Dashboard = window.StockAgentDashboard;
const $ = Dashboard.byId;
const fetchJson = Dashboard.createJsonFetcher({timeoutMs: 15000, cache: "no-store", expectedRoot: "object"});

function count(value) {
  return Dashboard.formatNumber(value, {maximumFractionDigits: 0});
}

function percent(value, digits = 2, scale = 1) {
  const observed = Dashboard.finiteNumber(value);
  return observed == null ? "—" : `${Dashboard.formatNumber(observed * scale,
    {minimumFractionDigits: digits, maximumFractionDigits: digits})}%`;
}

function healthPresentation(value) {
  const health = String(value || "unavailable").toLowerCase();
  const labels = {
    active: "資料正常",
    ready: "資料正常",
    waiting: "等待資料／排程",
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
  const className = `health ${health}`;
  if (target.className !== className) target.className = className;
  Dashboard.setText(target.lastChild, overrideLabel || label);
}

function renderTaifex(data) {
    setHealth("taifex", data.health);
    $("taifex-freshness").textContent = ageLabel(data.source_age_seconds);
    const live = count(data.live_strategies);
    const coverage = Dashboard.finiteNumber(data.book_coverage_ratio);
    $("taifex-summary").textContent = coverage != null
      ? `${live} 策略 · ${percent(coverage, 0, 100)} 行情`
      : `${live} 個策略 · 行情未觀測`;
}

function renderTw(data) {
    setHealth("tw", data.health);
    $("tw-freshness").textContent = ageLabel(data.source_age_seconds);
    const modes = count(data.modes);
    const positions = count(data.open_positions);
    $("tw-summary").textContent = `${modes} 模式 · ${positions} 個持倉`;
}

function renderOvernight(data) {
    setHealth("overnight", data.health);
    $("overnight-freshness").textContent = ageLabel(data.source_age_seconds);
    const modes = count(data.modes);
    const positions = count(data.open_positions);
    $("overnight-summary").textContent = `${modes} 模式 · ${positions} 個隔夜持倉`;
}

function bytes(value) {
  return Dashboard.formatBytes(value, {maximumFractionDigits: 1});
}

function renderShioaji(data) {
    setHealth("shioaji", data.health);
    $("shioaji-traffic").textContent = `${percent(data.traffic_used_ratio, 1, 100)} · 安全剩 ${bytes(data.safe_remaining_bytes)}`;
    $("shioaji-progress").textContent = `${count(data.completed_contracts)}/${count(data.inventory_contracts)} 合約 · ${percent(data.progress_ratio, 2, 100)}`;
}

function renderFinlab(data) {
    const used = Number(data.used_mb), limit = Number(data.limit_mb);
    const hasQuota = data.used_mb != null && data.limit_mb != null && Number.isFinite(used) && Number.isFinite(limit);
    const age = data.quota_observed_at_utc ? Date.now() - Date.parse(data.quota_observed_at_utc) : Infinity;
    const freshQuota = hasQuota && Number.isFinite(age) && age >= 0 && age <= 15 * 60000;
    setHealth("finlab", freshQuota ? "active" : "degraded", freshQuota ? "配額觀測新鮮" : "配額觀測待更新");
    $("finlab-quota").textContent = hasQuota ? `${used.toFixed(0)}／${limit.toFixed(0)} MB` : "尚無帳號觀測";
    const downloaded = Number(data.downloaded), total = Number(data.catalog_total);
    $("finlab-progress").textContent = data.catalog_total != null && data.downloaded != null
      ? `${downloaded.toLocaleString("zh-TW")}／${total.toLocaleString("zh-TW")} 鍵`
      : "目錄待核實";
}

function renderFinmind(data) {
    setHealth("finmind", data.health === "updating" ? "updating" : data.health === "waiting" ? "waiting" : data.health);
    const used = data.observed_requests_60m;
    const limit = data.official_requests_per_hour;
    $("finmind-quota").textContent = used != null && limit != null
      ? `${data.quota_state === "complete_worker_window" ? "" : "≥"}${Number(used).toLocaleString("zh-TW")}／${Number(limit).toLocaleString("zh-TW")} 次`
      : "用量觀測未就緒";
    $("finmind-progress").textContent = data.complete != null && data.total != null
      ? `${Number(data.complete).toLocaleString("zh-TW")}／${Number(data.total).toLocaleString("zh-TW")} 日分區`
      : "日分區待核實";
}

function renderOpenbb(data) {
    setHealth("openbb", data.health, healthPresentation(data.health).label);
    const snapshot = data.snapshot_state === "current" ? "快照新鮮"
      : data.snapshot_state === "stale" ? "快照逾時" : "快照待核實";
    $("openbb-freshness").textContent = `${snapshot} · ${ageLabel(data.source_age_seconds)}`;
    $("openbb-progress").textContent = `${percent(data.completion_percent)} · ${count(data.accepted_tasks)}/${count(data.total_tasks)}`;
}

function renderTej(data) {
    setHealth("tej",data.state === "unavailable" ? "unavailable" : data.state === "running" ? "updating" : data.state === "needs_review" ? "degraded" : "waiting",
        data.state === "unavailable" ? "暫時無法讀取" : data.state === "running" ? "桌面回補中" : data.state === "needs_review" ? "下載需檢查" : "等待桌面工作");
    $("tej-catalog").textContent = data.tables != null && data.fields != null
        ? `${Number(data.tables).toLocaleString("zh-TW")} 表／${Number(data.fields).toLocaleString("zh-TW")} 欄` : "未核實";
    $("tej-progress").textContent = data.exported_rows != null
        ? `${Number(data.exported_rows).toLocaleString("zh-TW")} 查詢格點 · 非原生歷史完整率` : "尚無驗收匯出";
}

function renderDataMonitor(data) {
    const label = data.health === "active" ? "全部正常" : data.health === "updating" ? "回補進行中" : "有來源需處理";
    setHealth("data", data.health, label);
    $("data-registered").textContent = `${count(data.registered_items)} 項`;
    $("data-progress").textContent = `${count(data.healthy_or_progressing)} 正常 · ${count(data.attention_required)} 待處理`;
}

function renderTraffic(data) {
    const observed = Dashboard.finiteNumber(data.requests_1m) != null;
    setHealth("traffic", observed ? "active" : "waiting", observed ? "即時觀察" : "尚無觀測");
    $("traffic-requests").textContent = `${count(data.requests_1m)} 次 · ${Dashboard.formatNumber(data.requests_per_second_1m)} RPS`;
    const latency = Dashboard.finiteNumber(data.latency_p95_ms_1m);
    $("traffic-latency").textContent = latency != null ? `${Dashboard.formatNumber(latency)} ms` : "尚無樣本";
}

function renderUnavailable() {
  for (const prefix of ["taifex", "tw", "overnight", "shioaji", "finlab", "finmind", "tej", "openbb", "data", "traffic"]) setHealth(prefix, "unavailable");
  $("taifex-freshness").textContent = "無法取得";
  $("tw-freshness").textContent = "無法取得";
  $("overnight-freshness").textContent = "無法取得";
  $("taifex-summary").textContent = "進入面板查看";
  $("tw-summary").textContent = "進入面板查看";
  $("overnight-summary").textContent = "進入面板查看";
  $("shioaji-traffic").textContent = "無法取得";
  $("shioaji-progress").textContent = "進入面板查看";
  $("finlab-quota").textContent = "無法取得";
  $("finlab-progress").textContent = "進入面板查看";
  $("finmind-quota").textContent = "無法取得";
  $("finmind-progress").textContent = "進入面板查看";
  $("tej-catalog").textContent = "無法取得";
  $("tej-progress").textContent = "進入面板查看";
  $("openbb-freshness").textContent = "無法取得";
  $("openbb-progress").textContent = "進入面板查看";
  $("data-registered").textContent = "無法取得";
  $("data-progress").textContent = "進入面板查看";
  $("traffic-requests").textContent = "無法取得";
  $("traffic-latency").textContent = "進入面板查看";
}

let lastSuccessfulOverview = null;

async function refresh() {
  if (document.hidden || refreshInFlight) return;
  refreshInFlight = true;
  try {
    const data = await fetchJson("api/overview");
    renderTaifex(data.taifex || {});
    renderTw(data.tw || {});
    renderOvernight(data.overnight || {});
    renderShioaji(data.shioaji || {});
    renderFinlab(data.finlab || {});
    renderFinmind(data.finmind || {});
    renderTej(data.tej || {});
    renderOpenbb(data.openbb || {});
    renderDataMonitor(data.data_monitor || {});
    renderTraffic(data.traffic || {});
    lastSuccessfulOverview = data;
  } catch (_error) {
    if (!lastSuccessfulOverview) renderUnavailable();
    else {
      for (const prefix of ["taifex", "tw", "overnight", "shioaji", "finlab", "finmind", "tej", "openbb", "data", "traffic"]) {
        setHealth(prefix, "unavailable", "更新失敗 · 上次觀測");
      }
    }
  } finally {
    refreshInFlight = false;
  }
}

Dashboard.scheduleRefresh(refresh, {intervalMs: REFRESH_MS});

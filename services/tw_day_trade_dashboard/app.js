"use strict";

const PRICE_REFRESH_MS = 60000;
const FETCH_TIMEOUT_MS = 15000;
const SIGNAL_PAGE_SIZE = 250;
const POSITION_PAGE_SIZE = 250;
const EVENT_PAGE_SIZE = 250;
const COLORS = ["#37d3ff", "#5ee0a0", "#a98cff", "#f5bd4f", "#ff7ac8", "#73e6d1", "#ff9f68"];
const TIME_RANGE_LABELS = {"1h": "1 小時", "1d": "1 天", "1w": "1 週", "1mo": "1 月", "1q": "1 季", "1y": "1 年", all: "全部"};
const timeAxis = window.StockAgentTimeAxis;
const TW_STOCK_SESSIONS = [
  {label: "開", minute: 9 * 60},
  {label: "收", minute: 13 * 60 + 30},
];
const HIDDEN_EQUITY_SERIES_STORAGE_KEY = "tw-day-trade-hidden-equity-series";
const HISTORY_CLIENT_CACHE_MS = 45000;
let snapshot = null;
let chartHistory = null;
let chartRange = "1d";
let hiddenEquitySeries = new Set();
let chartHistoryCache = new Map();
let historyInFlight = false;
let lastFetchMs = null;
let refreshInFlight = false;
let refreshQueued = false;
let lastRenderedRevision = null;
let lastFilterRevision = null;
let signalRows = [];
let signalDirectionSummary = {};
let signalTotal = 0;
let signalHasMore = false;
let signalRecordCount = null;
let signalLoading = false;
let signalLoadError = "";
let signalRequestSequence = 0;
let signalFilterTimer = null;
let signalAbortController = null;
let eventRows = [];
let eventTotal = 0;
let eventOrderTotal = 0;
let eventFillTotal = 0;
let eventHasMore = false;
let eventRecordRevision = null;
let eventLoading = false;
let eventLoadError = "";
let eventRequestSequence = 0;
let eventAbortController = null;
let filterAnimationFrame = null;
let positionRows = [];
let positionTotal = 0;
let positionHasMore = false;
let positionLoading = false;
let positionLoadError = "";
let positionRequestSequence = 0;
let positionAbortController = null;
let availableDetailDates = [];

try {
  chartRange = localStorage.getItem("tw-day-trade-equity-time-range") || "1d";
  const storedHiddenSeries = JSON.parse(localStorage.getItem(HIDDEN_EQUITY_SERIES_STORAGE_KEY) || "[]");
  if (Array.isArray(storedHiddenSeries)) hiddenEquitySeries = new Set(storedHiddenSeries.map(String));
} catch (_error) { /* storage can be disabled */ }
if (!(chartRange in TIME_RANGE_LABELS)) chartRange = "1d";

const $ = (id) => document.getElementById(id);
async function fetchWithTimeout(path, options = {}) {
  const controller = new AbortController();
  const upstream = options.signal;
  const forwardAbort = () => controller.abort(upstream?.reason);
  if (upstream?.aborted) forwardAbort();
  else upstream?.addEventListener("abort", forwardAbort, {once: true});
  const timer = window.setTimeout(
    () => controller.abort(new DOMException("Request timed out", "TimeoutError")),
    FETCH_TIMEOUT_MS,
  );
  try { return await fetch(path, {...options, signal: controller.signal}); }
  finally {
    window.clearTimeout(timer);
    upstream?.removeEventListener("abort", forwardAbort);
  }
}
const esc = (value) => String(value ?? "—").replace(/[&<>"']/g, (c) => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"})[c]);
const number = (value, digits = 0) => {
  if (value == null || !Number.isFinite(Number(value))) return "—";
  const precision = Math.min(2, Math.max(0, Number(digits) || 0));
  const resolved = Math.abs(Number(value)) < .005 ? 0 : Number(value);
  return resolved.toLocaleString("zh-TW", {minimumFractionDigits: precision, maximumFractionDigits: precision});
};
// The ledger and API retain their source precision.  The public UI deliberately
// rounds every visible decimal to at most two places so dense comparisons stay
// scannable and binary floating-point tails never leak into the page.
const sourceNumber = (value) => {
  if (value == null || value === "") return "—";
  const resolved = Number(value);
  if (!Number.isFinite(resolved)) return "—";
  const displayValue = Math.abs(resolved) < .005 ? 0 : resolved;
  return displayValue.toLocaleString("zh-TW", {maximumFractionDigits: 2});
};
const monetaryNumber = (value) => {
  if (value == null || value === "") return "—";
  const resolved = Number(value);
  if (!Number.isFinite(resolved)) return "—";
  const displayValue = Math.abs(resolved) < .005 ? 0 : resolved;
  return displayValue.toLocaleString("zh-TW", {maximumFractionDigits: 2});
};
const summaryMoney = (value) => value == null || !Number.isFinite(Number(value))
  ? "—"
  : `NT$ ${Number(value).toLocaleString("zh-TW", {maximumFractionDigits: 2})}`;
const compactMoney = (value) => value == null || !Number.isFinite(Number(value))
  ? "—"
  : `NT$ ${Number(value).toLocaleString("zh-TW", {notation: "compact", maximumFractionDigits: 2})}`;
const displayPct = (value) => {
  if (value == null || !Number.isFinite(Number(value))) return "—";
  const resolved = Number(value);
  const displayValue = Math.abs(resolved) < .005 ? 0 : resolved;
  return `${displayValue.toLocaleString("zh-TW", {maximumFractionDigits: 2})}%`;
};
const money = (value) => value == null ? "—" : `NT$ ${monetaryNumber(value)}`;
const pct = (value) => value == null ? "—" : `${sourceNumber(Number(value) * 100)}%`;
const shortTime = (value) => value ? String(value).replace("T", " ").slice(5, 19) : "—";
const pnlClass = (value) => Number(value || 0) > 0 ? "positive" : Number(value || 0) < 0 ? "negative" : "";
const badge = (text, kind = "") => `<span class="badge ${kind}">${esc(text)}</span>`;
const clampRatio = (value) => Math.min(1, Math.max(0, Number(value) || 0));
const duration = (seconds) => {
  if (seconds == null || !Number.isFinite(Number(seconds))) return "—";
  const value = Math.max(0, Number(seconds));
  if (value < 1) return `${Math.round(value * 1000)} ms`;
  if (value < 60) return `${value.toFixed(value < 10 ? 1 : 0)} 秒`;
  const minutes = Math.floor(value / 60), rest = Math.round(value % 60);
  return `${minutes} 分 ${rest} 秒`;
};
const countdown = (value) => {
  if (!value) return "—";
  const remaining = Math.max(0, (new Date(value).getTime() - Date.now()) / 1000);
  return duration(remaining);
};
const progress = (ratio, kind = "") => {
  const bounded = clampRatio(ratio);
  const value = (bounded * 100).toFixed(2);
  return `<progress class="progress-track ${esc(kind)}" max="100" value="${value}" aria-label="${value}%"></progress>`;
};
const directionPair = (row = {}) => `多 ${pct(row.long_gross)} / 空 ${pct(row.short_gross)} · ${number(row.long_count)} / ${number(row.short_count)} 檔`;

function selectedMode() { return $("mode-filter").value || "all"; }
function selectedDetailStartDate() { return $("detail-start-date").value || ""; }
function selectedDetailEndDate() { return $("detail-end-date").value || ""; }
function selectedDate() {
  const boundary = selectedDetailEndDate();
  return availableDetailDates.find((value) => value <= boundary) || boundary;
}
function detailRangeKey() { return `${selectedDetailStartDate()}|${selectedDetailEndDate()}`; }
function selectedChartStartDate() { return $("equity-start-date").value || ""; }
function selectedChartEndDate() { return $("equity-end-date").value || ""; }
function hasCustomChartDates() { return Boolean(selectedChartStartDate() || selectedChartEndDate()); }
function chartWindowLabel() {
  const start = selectedChartStartDate();
  const end = selectedChartEndDate();
  if (!start && !end) return TIME_RANGE_LABELS[chartRange];
  return `${start || "最早資料"} ～ ${end || "最新資料"}`;
}
function chartRequestKey() {
  return JSON.stringify([
    hasCustomChartDates() ? "all" : chartRange,
    selectedChartStartDate(),
    selectedChartEndDate(),
  ]);
}
function textFilter() { return $("symbol-filter").value.trim().toLowerCase(); }
function matchesMode(row) { return selectedMode() === "all" || row.market === selectedMode(); }
function matchesSymbol(row) {
  const q = textFilter();
  return !q || String(row.symbol || "").toLowerCase().includes(q) || String(row.name || "").toLowerCase().includes(q);
}
function compareByAbsoluteWeight(a, b) {
  const aWeight = Number(a.target_weight);
  const bWeight = Number(b.target_weight);
  const aMagnitude = Number.isFinite(aWeight) ? Math.abs(aWeight) : -1;
  const bMagnitude = Number.isFinite(bWeight) ? Math.abs(bWeight) : -1;
  return bMagnitude - aMagnitude
    || (Number.isFinite(bWeight) ? bWeight : 0) - (Number.isFinite(aWeight) ? aWeight : 0)
    || String(a.market || "").localeCompare(String(b.market || ""), "zh-Hant")
    || String(a.symbol || "").localeCompare(String(b.symbol || ""), "zh-Hant");
}

function healthPresentation(value) {
  const health = String(value || "unavailable").toLowerCase();
  const labels = {
    active: "資料正常",
    ready: "資料正常",
    waiting: "等待資料",
    stale: "資料逾時",
    blocked: "策略阻擋",
    critical: "需要注意",
    unavailable: "暫時離線",
  };
  return {health: health === "ready" ? "active" : health, label: labels[health] || "狀態未知"};
}

function engineStatusLabel(value) {
  const labels = {
    active: "執行正常",
    ready: "已就緒",
    waiting: "等待時段",
    critical_unflattened_after_13_24: "13:24 市價重試後有殘餘，已轉 13:25 集合競價",
    blocked_missing_eligibility: "缺少當日當沖資格資料，已停止執行",
    blocked_missing_checkpoint: "缺少模型權重，已停止執行",
  };
  return labels[value] || String(value || "未知狀態").replaceAll("_", " ");
}

function engineStatusShortLabel(value) {
  const labels = {
    active: "執行正常",
    ready: "已就緒",
    critical_unflattened_after_13_24: "轉 13:25 集合競價",
    flat_directional_mix_unexecutable: "雙向整張不足・保持空倉",
    flat_no_executable_signal: "該日無可執行訊號",
    session_flat_after_exit: "該日已平倉",
    historical_session_complete: "歷史交易日已完成",
    historical_session_closed_with_residual: "歷史交易日有殘餘紀錄",
    historical_session_missed: "歷史交易日執行缺漏",
    historical_signal_blocked: "歷史訊號已安全阻擋",
  };
  return labels[value] || engineStatusLabel(value);
}

function executionStatusPresentation(value) {
  const status = String(value || "unknown");
  const presentations = {
    completed: {label: "該日已執行", kind: "good"},
    blocked: {label: "該日未執行・安全阻擋", kind: "bad"},
    starting: {label: "錯過後立即補跑中", kind: "warn"},
    waiting_09_00: {label: "等待 09:00", kind: ""},
    waiting_trading_day: {label: "等待交易日", kind: ""},
    missed: {label: "該日執行缺漏", kind: "bad"},
  };
  return presentations[status] || {label: status.replaceAll("_", " "), kind: "warn"};
}

function totalModeNetPnl(data) {
  const modes = Array.isArray(data?.modes) ? data.modes : [];
  const values = modes.map((mode) => {
    const initial = Number(mode.initial_capital_twd);
    const equity = Number(mode.total_equity_twd);
    return Number.isFinite(initial) && Number.isFinite(equity) ? equity - initial : null;
  }).filter((value) => value != null);
  return values.length === modes.length && values.length
    ? values.reduce((sum, value) => sum + value, 0)
    : null;
}

function resolvedPositionPnl(row = {}) {
  const signedShares = Number(row.signed_shares || 0);
  const realized = row.realized_net_pnl_twd ?? (signedShares === 0 ? row.net_pnl_twd : null);
  const unrealized = row.unrealized_net_pnl_twd ?? (signedShares === 0 ? 0 : row.last_complete_net_pnl_twd);
  const reconciled = realized != null && unrealized != null ? Number(realized) + Number(unrealized) : null;
  const total = row.reconciled_total_net_pnl_twd ?? reconciled ?? row.total_net_pnl_twd ?? row.net_pnl_twd ?? row.last_complete_net_pnl_twd;
  return {signedShares, realized, unrealized, total};
}

function renderOverview(data) {
  const modes = Array.isArray(data.modes) ? data.modes : [];
  const healthyModes = modes.filter((mode) => (
    mode.checkpoint_ready
    && !String(mode.engine_status || "").startsWith("critical")
    && !String(mode.engine_status || "").startsWith("blocked")
  )).length;
  const openPositions = (data.positions || []).filter((row) => Number(row.signed_shares || 0) !== 0);
  const openPositionCount = Number.isFinite(Number(data.open_position_count))
    ? Number(data.open_position_count)
    : openPositions.length;
  const stalePositions = Number.isFinite(Number(data.stale_position_count))
    ? Number(data.stale_position_count)
    : openPositions.filter((row) => row.valuation_stale).length;
  const sumModeField = (field) => {
    const values = modes
      .filter((mode) => mode[field] != null && Number.isFinite(Number(mode[field])))
      .map((mode) => Number(mode[field]));
    return values.length === modes.length && values.length
      ? values.reduce((sum, value) => sum + value, 0)
      : null;
  };
  const totalPnl = totalModeNetPnl(data);
  const realizedPnl = sumModeField("cumulative_realized_net_pnl_twd");
  const unrealizedPnl = sumModeField("open_net_liquidation_pnl_twd");
  const reconciliationDifference = [totalPnl, realizedPnl, unrealizedPnl].every((value) => value != null)
    ? totalPnl - realizedPnl - unrealizedPnl
    : null;
  const reconciled = reconciliationDifference != null && Math.abs(reconciliationDifference) <= .01;
  const returns = modes.map((mode) => Number(mode.return_pct)).filter(Number.isFinite);
  const best = returns.length ? Math.max(...returns) : null;
  const worst = returns.length ? Math.min(...returns) : null;
  const healthKind = healthyModes === modes.length ? "good" : healthyModes ? "warn" : "bad";
  const cards = [
    ["模式狀態", `${healthyModes}/${modes.length} 可解讀`, healthyModes === modes.length ? "所有 checkpoint 與執行狀態正常" : "有模式需要查看上方警示", healthKind],
    ["所選日持倉", `${number(openPositionCount)} 個`, stalePositions ? `${number(stalePositions)} 個估值延用` : "目前估值皆有新鮮報價", stalePositions ? "warn" : "good"],
    ["四模式已實現", realizedPnl == null ? "—" : `${realizedPnl >= 0 ? "+" : ""}${compactMoney(realizedPnl)}`, "已出場部分，已扣分攤後交易成本", pnlClass(realizedPnl)],
    ["四模式未實現", unrealizedPnl == null ? "—" : `${unrealizedPnl >= 0 ? "+" : ""}${compactMoney(unrealizedPnl)}`, stalePositions ? `含 ${number(stalePositions)} 個延用估值` : "以可清算 bid／ask 並扣剩餘成本", stalePositions ? "warn" : pnlClass(unrealizedPnl)],
    ["四模式總淨損益", totalPnl == null ? "—" : `${totalPnl >= 0 ? "+" : ""}${compactMoney(totalPnl)}`, reconciled ? "已實現＋未實現，已與總權益對帳" : reconciliationDifference == null ? "等待完整損益來源" : `對帳差異 ${summaryMoney(reconciliationDifference)}`, reconciled ? pnlClass(totalPnl) : "bad"],
    ["報酬範圍", best == null ? "—" : `${best >= 0 ? "+" : ""}${displayPct(best)} ～ ${worst >= 0 ? "+" : ""}${displayPct(worst)}`, "各模式以自己的初始資金為分母", best != null && worst < 0 ? "warn" : pnlClass(best)],
  ];
  $("overview-kpis").innerHTML = cards.map(([label, value, note, kind]) => `<div class="overview-kpi">
    <span>${esc(label)}</span><strong class="${esc(kind)}">${esc(value)}</strong><small class="${esc(kind)}">${esc(note)}</small>
  </div>`).join("");
  $("overview-kpis").setAttribute("aria-busy", "false");
}

function renderHeader(data) {
  const health = $("health");
  const presentation = healthPresentation(data.health);
  health.textContent = presentation.label;
  health.className = `pill ${presentation.health}`;
  $("connection-status").dataset.state = presentation.health;
  $("freshness").textContent = `來源 ${duration(data.source_age_seconds)}前 · ${shortTime(data.source_updated_at)}`;
  const alert = $("alert");
  const blockers = data.modes.filter((mode) => !mode.checkpoint_ready || String(mode.engine_status || "").startsWith("critical"));
  const catchUps = data.modes.filter((mode) => mode.today_execution_status === "starting");
  const missed = data.modes.filter((mode) => mode.today_execution_status === "missed");
  const hasReplay = data.modes.some((mode) => mode.counterfactual_open_replay || mode.simulation_replay);
  const hasBenchmarkReplay = (data.benchmarks || []).some((row) => row.counterfactual_open_replay);
  const signalMissingEligibility = new Map();
  const currentMissingEligibility = new Map();
  for (const mode of data.modes) {
    if (mode.signal_at) {
      for (const [venue, coverage] of Object.entries(mode.eligibility_coverage || {})) {
        if (!coverage.covered && !signalMissingEligibility.has(venue)) signalMissingEligibility.set(venue, coverage);
      }
    }
    for (const [venue, coverage] of Object.entries(mode.current_eligibility_coverage || {})) {
      if (!coverage.covered && !currentMissingEligibility.has(venue)) currentMissingEligibility.set(venue, coverage);
    }
  }
  if (hasReplay || hasBenchmarkReplay || data.health === "stale" || blockers.length || catchUps.length || missed.length || signalMissingEligibility.size || currentMissingEligibility.size) {
    const messages = [
      hasReplay ? "所選交易日使用實際開盤價做反事實重建；原始訊號時間保留，但這不是當時可成交報價、即時執行或券商成交。" : "",
      hasBenchmarkReplay ? "0050、2330 與台指期基準已補到實際開盤起點；補登區段是明確標示的回放，後續估值才接續記錄到的可成交 bid。" : "",
      data.health === "stale" ? "資料來源已逾時；畫面只能當歷史紀錄，不能視為現在行情。" : "",
      currentMissingEligibility.size ? `所選交易日當沖資格未完整覆蓋，後續訊號已停止執行：${[...currentMissingEligibility.entries()].map(([venue, row]) => `${venue.toUpperCase()} 需要 ${row.target_date || data.session_date || "所選日"}，最新僅到 ${row.latest_date || "無資料"}`).join("；")}` : "",
      !currentMissingEligibility.size && signalMissingEligibility.size ? "09:00 訊號產生時資格資料尚未到齊，因此已 fail-closed；較晚補齊的資料不會回填成假成交。" : "",
      catchUps.length ? `發現所選交易日執行缺漏，已立即啟動補跑：${catchUps.map((mode) => mode.label || mode.market).join("、")}` : "",
      missed.length ? `所選交易日進場時窗結束仍缺少執行紀錄：${missed.map((mode) => mode.label || mode.market).join("、")}` : "",
      ...blockers.map((mode) => `${mode.label || mode.market}：${engineStatusLabel(mode.engine_status)}${mode.checkpoint_ready ? "" : "；checkpoint 未就緒"}`),
    ].filter(Boolean);
    const signature = messages.join("|");
    alert.classList.remove("hidden");
    if (alert.dataset.signature !== signature) {
      alert.innerHTML = `<strong>需要注意</strong>${messages.map((message) => `<span>${esc(message)}</span>`).join("")}`;
      alert.dataset.signature = signature;
    }
  } else {
    alert.classList.add("hidden");
    alert.dataset.signature = "";
  }
}

function syncFilters(data) {
  const mode = $("mode-filter");
  const previousMode = mode.value;
  const startDate = $("detail-start-date");
  const endDate = $("detail-end-date");
  const previousStart = startDate.value;
  const previousEnd = endDate.value;
  const sourceDates = Array.isArray(data.available_session_dates) && data.available_session_dates.length
    ? data.available_session_dates
    : [data.session_date].filter(Boolean);
  availableDetailDates = [...sourceDates].map(String).sort((left, right) => right.localeCompare(left));
  const revision = JSON.stringify([data.modes.map((row) => [row.market, row.label]), availableDetailDates]);
  if (revision !== lastFilterRevision) {
    mode.innerHTML = `<option value="all">全部模式</option>` + data.modes.map((row) => `<option value="${esc(row.market)}">${esc(row.label || row.market)}</option>`).join("");
    if ([...mode.options].some((option) => option.value === previousMode)) mode.value = previousMode;
    const earliest = availableDetailDates.at(-1) || data.session_date || "";
    const latest = availableDetailDates[0] || data.session_date || "";
    startDate.min = earliest; startDate.max = latest;
    endDate.min = earliest; endDate.max = latest;
    const withinCoverage = (value) => Boolean(value && earliest <= value && value <= latest);
    startDate.value = withinCoverage(previousStart) ? previousStart : data.session_date;
    endDate.value = withinCoverage(previousEnd) ? previousEnd : data.session_date;
    lastFilterRevision = revision;
  }
  if (!endDate.value) endDate.value = data.session_date;
  if (!startDate.value) startDate.value = endDate.value;
}

function renderModes(data) {
  $("mode-cards").innerHTML = data.modes.map((mode) => {
    const initial = Number(mode.initial_capital_twd || 0);
    const equity = mode.total_equity_twd == null ? null : Number(mode.total_equity_twd);
    const pnl = equity == null ? null : equity - initial;
    const returnPct = mode.return_pct == null ? null : Number(mode.return_pct);
    const status = String(mode.engine_status || "unknown");
    const execution = executionStatusPresentation(mode.today_execution_status);
    const kind = status === "active" ? "good" : status.startsWith("blocked") || status.startsWith("critical") ? "bad" : "warn";
    const mix = mode.execution_projection || {};
    const offsetTicks = Number(mode.price_limit_offset_ticks || 0);
    const bracketPolicy = offsetTicks > 0
      ? `TP／SL 漲跌停內縮 ${sourceNumber(offsetTicks)} Tick（提高成交機率，非保證）`
      : "TP／SL 使用完整漲跌停價";
    return `<article class="panel mode-card">
      <header><h3>${esc(mode.label || mode.market)}</h3>${badge(engineStatusShortLabel(status), kind)}</header>
      <div class="equity ${pnlClass(returnPct)}">${returnPct == null ? "尚無估值" : `${returnPct >= 0 ? "+" : ""}${displayPct(returnPct)}`}</div>
      <div class="delta ${pnlClass(pnl)}">${pnl == null ? "尚無估值" : `總權益 ${summaryMoney(equity)} · 淨損益 ${pnl >= 0 ? "+" : ""}${summaryMoney(pnl)}`}</div>
      <div class="mode-glance">
        <div><span>該日策略執行</span><strong class="${esc(execution.kind)}">${esc(execution.label)}</strong></div>
        <div><span>持倉／缺價</span><strong>${number(mode.open_position_count)} / ${number(mode.stale_position_count)}</strong></div>
        <div><span>已實現淨損益</span><strong class="${pnlClass(mode.cumulative_realized_net_pnl_twd)}">${summaryMoney(mode.cumulative_realized_net_pnl_twd)}</strong></div>
        <div><span>未實現淨清算損益</span><strong class="${pnlClass(mode.open_net_liquidation_pnl_twd)}">${summaryMoney(mode.open_net_liquidation_pnl_twd)}</strong></div>
      </div>
      <details><summary>查看資金、訊號與曝險細節</summary><div class="metrics">
        <div><span>報酬率資金基準</span><strong>${money(initial)}</strong></div>
        <div><span>已賺手續費退佣</span><strong>${money(mode.cumulative_commission_rebate_accrued_twd)}</strong></div>
        <div><span>訊號時間</span><strong>${shortTime(mode.signal_at)}</strong></div>
        <div><span>13:24 市價重試後殘餘</span><strong class="${Number(mode.force_exit_failures || 0) ? "negative" : ""}">${number(mode.force_exit_failures || 0)}</strong></div>
        <div><span>13:30 帳務強平</span><strong>${number(mode.terminal_flatten_count || 0)}</strong></div>
        <div><span>強平價替代值</span><strong class="${Number(mode.terminal_flatten_degraded_count || 0) ? "negative" : ""}">${number(mode.terminal_flatten_degraded_count || 0)}</strong></div>
        ${mode.counterfactual_open_replay ? `<div class="wide"><span>開盤價重建</span><strong>實際開盤 ${shortTime(mode.signal_at)} · 原始訊號 ${shortTime(mode.source_signal_at)} · 非即時成交</strong></div>` : ""}
        <div class="wide"><span>方向總曝險：目標</span><strong>多 ${pct(mix.target_long_gross)} / 空 ${pct(mix.target_short_gross)}</strong></div>
        <div class="wide"><span>整張／深度後 → 平衡後</span><strong>多 ${pct(mix.pre_balance_long_gross)} / 空 ${pct(mix.pre_balance_short_gross)} → 多 ${pct(mix.post_balance_long_gross)} / 空 ${pct(mix.post_balance_short_gross)}</strong></div>
        <div class="wide"><span>停利停損價位</span><strong>${esc(bracketPolicy)}</strong></div>
      </div></details>
    </article>`;
  }).join("");
}

function renderBenchmarks(data) {
  const rows = Array.isArray(data.benchmarks) ? data.benchmarks : [];
  $("benchmark-cards").innerHTML = rows.map((row) => {
    const returnPct = row.return_pct == null ? null : Number(row.return_pct);
    const equity = row.total_equity_twd == null ? null : Number(row.total_equity_twd);
    const netPnl = row.net_pnl_twd == null ? null : Number(row.net_pnl_twd);
    const isTx = row.instrument_type === "continuous_long_future";
    const waitingRoll = String(row.valuation_source || "").includes("roll_waiting");
    const actionBlocked = !isTx && String(row.valuation_source || "").includes("corporate_action_reference_unavailable");
    const stale = Boolean(row.valuation_stale);
    const status = returnPct == null
      ? {label:"等待有效價格", kind:"bad"}
      : actionBlocked
        ? {label:"除權息資料未覆蓋", kind:"bad"}
      : waitingRoll
        ? {label:"等待雙邊換月報價", kind:"warn"}
        : stale
          ? {label:"延用前次估值", kind:"warn"}
          : {label:row.counterfactual_open_replay ? "實際開盤起算" : "可成交估值", kind:"good"};
    const holding = isTx
      ? `大台 1 口 · ${row.contract_code || "合約待確認"}`
      : `${number(row.quantity || 1000)} 股 · ${row.symbol || ""}`;
    const rollText = isTx
      ? `${number(row.roll_count || 0)} 次${row.last_roll_at ? ` · 最近 ${shortTime(row.last_roll_at)}` : " · 尚未換月"}`
      : row.corporate_action_status === "same_session_no_action_boundary"
        ? "同日進出未跨除權息界線 · 因子 1×"
      : row.total_return_contract
        ? `官方除權息因子 ${sourceNumber(row.corporate_action_factor ?? 1)}× · ${number(row.corporate_action_count || 0)} 次 · 覆蓋至 ${row.corporate_action_coverage_end || "—"}`
        : "等待下一個交易分鐘載入含息基準契約";
    const totalCosts = [row.fixed_fees_twd, row.transaction_tax_twd, row.liquidation_cost_twd]
      .map(Number).filter(Number.isFinite).reduce((sum, value) => sum + value, 0);
    return `<article class="panel benchmark-card">
      <header><h3>${esc(row.label || row.benchmark_id)}</h3>${badge(status.label, status.kind)}</header>
      <div class="equity ${pnlClass(returnPct)}">${returnPct == null ? "尚無估值" : `${returnPct >= 0 ? "+" : ""}${displayPct(returnPct)}`}</div>
      <div class="delta ${pnlClass(netPnl)}">${equity == null ? "等待完整來源" : `權益 ${summaryMoney(equity)} · 淨損益 ${netPnl >= 0 ? "+" : ""}${summaryMoney(netPnl)}`}</div>
      <div class="benchmark-facts">
        <div><span>持有標的</span><strong class="benchmark-contract">${esc(holding)}</strong></div>
        <div><span>報酬資金基準</span><strong>${money(row.initial_capital_twd)}</strong></div>
        <div><span>起算開盤</span><strong>${sourceNumber(row.entry_price)}<small>${shortTime(row.entry_at)}</small></strong></div>
        <div><span>目前可清算價</span><strong>${sourceNumber(row.last_mark_price)}<small>${shortTime(row.last_quote_at || row.last_mark_at)}</small></strong></div>
        <div><span>${isTx ? "自動換月" : "持有契約"}</span><strong>${esc(rollText)}</strong></div>
        <div><span>已發生＋清算成本</span><strong>${money(totalCosts)}</strong></div>
        ${isTx ? `<div class="wide"><span>換月規則</span><strong>舊約 bid 與新約 ask 必須同時存在；價差不列為報酬</strong></div>` : ""}
      </div>
    </article>`;
  }).join("") || `<article class="panel benchmark-card"><strong>基準尚未建立</strong><small>等待來源與起算價格通過稽核</small></article>`;
}

function renderOperations(data) {
  const warm = data.preopen || {};
  const session = data.session_progress || {};
  const execution = data.execution_records || {};
  const warmMarkets = warm.markets || [];
  const warmRatio = warmMarkets.length ? warmMarkets.reduce((sum, row) => sum + clampRatio(row.progress_ratio), 0) / warmMarkets.length : clampRatio(warm.progress_ratio);
  const totalModes = Number(warm.total_count || data.modes.length || 0);
  const readyModes = Number(warm.ready_count || 0);
  const sourceAge = Number(data.source_age_seconds || 0);
  const heartbeatKind = sourceAge <= 10 ? "good" : sourceAge <= 30 ? "warn" : "bad";
  const warmKind = warm.status === "ready" ? "good" : warm.status === "failed" ? "bad" : "warn";
  const phaseKind = ["active", "preopen"].includes(session.phase) ? "good" : session.phase === "force_exit" ? "bad" : "warn";
  const latency = data.today_latency || data.latency || {};
  const latencyStageLabels = {
    signal_pre_quote_prepare_ms: "訊號盤前骨架",
    signal_quote_fetch_ms: "訊號行情",
    signal_pre_inference_prepare_ms: "推論輸入準備",
    model_inference_ms: "模型推論",
    signal_post_inference_format_ms: "推論後格式化",
    signal_other_compute_ms: "其他訊號計算",
    artifact_publish_ms: "原子發布",
    artifact_discovery_ms: "消費端發現",
    eligibility_load_ms: "資格載入",
    executor_quote_fetch_ms: "執行行情",
    ledger_compute_persist_ms: "帳本落盤",
  };
  const latencyValue = (value) => value == null ? "—" : `${number(value, 1)} ms`;
  const noLatency = !Number(latency.sample_count || 0);
  const latencyEmptyLabel = "今日尚無開盤樣本";
  const latestBottleneck = latencyStageLabels[latency.latest_bottleneck_stage] || latency.latest_bottleneck_stage || "—";
  $("latency-kpis").innerHTML = [
    ["最新輸入→落盤", noLatency ? latencyEmptyLabel : latencyValue(latency.latest_ms), noLatency ? "不以舊日或估計值冒充今日速度" : `${esc(latency.latest_market || "—")} · ${shortTime(latency.latest_recorded_at)}`],
    ["P50", latencyValue(latency.p50_ms), `${number(latency.sample_count || 0)} 個成功模式樣本`],
    ["P95", latencyValue(latency.p95_ms), "訊號開始至模擬帳本 fsync 前後的牆鐘邊界"],
    ["最慢", latencyValue(latency.max_ms), "所選交易日成功樣本最大值"],
    ["最新瓶頸", noLatency ? "—" : latestBottleneck, noLatency ? "等待實測" : latencyValue(latency.latest_bottleneck_ms)],
  ].map(([label, value, note]) => `<div class="latency-kpi"><span>${esc(label)}</span><strong>${esc(value)}</strong><small>${esc(note)}</small></div>`).join("");
  $("operation-kpis").innerHTML = [
    ["所選日策略執行", `${number(execution.executed_count || 0)}/${number(execution.mode_count || data.modes.length || 0)} 完成`, execution.all_executed ? "全部策略均已進入所選日執行流程" : `${number(execution.blocked_count || 0)} 個被安全阻擋；解除後立即補跑`, execution.all_executed ? "good" : "bad"],
    ["盤前預熱", `${readyModes}/${totalModes || 0} READY`, warm.status || "pending", warmKind],
    ["目前階段", session.label || "—", `下一步 ${session.next_milestone_label || "—"} · ${countdown(session.next_milestone_at)}`, phaseKind],
    ["帳本心跳", `${sourceNumber(data.source_age_seconds)} 秒`, `目標每 ${number(session.decision_interval_seconds || 60)} 秒`, heartbeatKind],
    ["面板 API", lastFetchMs == null ? "—" : `${number(lastFetchMs, 1)} ms`, `行情與權益每 ${number(PRICE_REFRESH_MS / 1000)} 秒刷新`, lastFetchMs != null && lastFetchMs > 1000 ? "warn" : "good"],
  ].map(([label, value, note, kind]) => `<div class="operation-kpi"><span>${esc(label)}</span><strong>${esc(value)}</strong><small class="${esc(kind)}">${esc(note)}</small></div>`).join("");

  const workflowRows = [
    {label:"四模式預熱", value:warmRatio, count:`${number(warm.completed_count || 0)} / ${number(totalModes)} · ${number(warmRatio * 100, 1)}%`, note:`牆鐘 ${duration(warm.wall_elapsed_seconds)} · ${warm.modes_per_minute == null ? "—" : `${sourceNumber(warm.modes_per_minute)} 模式/分`}`, kind:warmKind},
    {label:"所選日策略執行", value:session.signal_progress_ratio, count:`${number(session.signal_completed_modes || 0)} / ${number(session.mode_count || 0)}`, note:"原子指標由 inotify 事件即時喚醒；0.1 秒只作備援，阻擋不算完成", kind:"good"},
    {label:"進場處理終態", value:session.entry_progress_ratio, count:`${number(session.entry_completed_modes || 0)} / ${number(session.mode_count || 0)}`, note:"完成後可能有成交或依真實限制保持空倉", kind:"good"},
    {label:"每分鐘權益紀錄", value:session.mark_progress_ratio, count:`${number(session.observed_mode_minutes || 0)} / ${number(session.expected_mode_minutes || 0)}`, note:`目前 ${sourceNumber(session.mark_rows_per_minute || 0)} 模式紀錄/分`, kind:""},
    {label:"13:20/13:24/13:25 退出", value:session.exit_progress_ratio, count:`${number(session.exit_started_modes || 0)} / ${number(session.mode_count || 0)}`, note:"先限價、再市價重試；殘餘於 13:25 以漲跌停價參與收盤集合競價", kind:"warn"},
  ];
  $("workflow-progress").innerHTML = workflowRows.map((row) => `<div class="progress-row">
    <div class="progress-title"><strong>${esc(row.label)}</strong><span>${esc(row.count)}</span></div>
    ${progress(row.value, row.kind)}
    <small>${esc(row.note)}</small>
  </div>`).join("");

  const preopenRows = warm.markets || [];
  $("preopen-progress").innerHTML = preopenRows.map((row) => {
    const status = String(row.status || "pending");
    const kind = status === "ready" ? "good" : status === "failed" ? "bad" : "warn";
    const stepText = row.step && row.total ? `${number(row.step)}/${number(row.total)} · ${number(row.progress_ratio * 100, 1)}%` : status.toUpperCase();
    const stepRate = row.step && row.elapsed_seconds ? Number(row.step) / Number(row.elapsed_seconds) : null;
    const eta = row.step && row.total && row.step < row.total && stepRate ? (Number(row.total) - Number(row.step)) / stepRate : null;
    const speed = row.symbols_per_second == null ? "—" : `${sourceNumber(row.symbols_per_second)} 股票/秒`;
    const inference = row.model_inference_ms == null ? "—" : `${duration(Number(row.model_inference_ms) / 1000)} 模型`;
    const limits = row.price_limit_requested ? `${number(row.price_limit_prepared)}/${number(row.price_limit_requested)} 漲跌停` : "漲跌停待準備";
    const eligibility = row.eligibility_ready ? `${row.eligibility_target_date || data.session_date || "所選日"} TWSE/TPEx 資格 READY` : "所選日資格待確認";
    const armed = row.final_arm_status === "ready" && row.final_arm_panel_cache_hit === true && row.final_arm_checkpoint_cache_hit === true && row.final_arm_model_cache_hit === true;
    const armText = armed
      ? `09:00 HOT READY ${shortTime(row.final_arm_completed_at)} · ${duration(row.final_arm_elapsed_seconds)} · ${number(row.final_arm_attempts || 1)} 次驗證`
      : row.final_arm_error || "08:55 最後武裝待驗證";
    const measuredDetail = `${duration(row.elapsed_seconds)} · ${speed} · ${inference} · ${limits} · ${eligibility} · ${armText}${eta == null ? "" : ` · ETA ${duration(eta)}`}`;
    const detail = row.error || (status === "running" && row.message) || measuredDetail;
    return `<div class="progress-row">
      <div class="progress-title"><strong>${esc(row.label || row.market)}</strong>${badge(stepText, kind)}</div>
      ${progress(row.progress_ratio, kind)}
      <small>${esc(detail)}</small>
    </div>`;
  }).join("") || `<div class="empty-inline">尚無所選日預熱紀錄</div>`;
  $("operation-source").textContent = warm.updated_at ? `預熱狀態 ${shortTime(warm.updated_at)}` : "預熱狀態尚未建立";
}

function renderChart(data) {
  const svg = $("equity-chart");
  const modes = selectedMode() === "all" ? data.modes.map((row) => row.market) : [selectedMode()];
  const historyRows = Array.isArray(chartHistory?.history) ? chartHistory.history : null;
  const modeRows = (historyRows || data.marks || []).filter((row) => row.series_type !== "benchmark" && modes.includes(row.market)).map((row) => ({...row, series_id: row.series_id || row.market}));
  const benchmarkRows = (historyRows || data.benchmark_marks || []).filter((row) => historyRows ? row.series_type === "benchmark" : true).map((row) => ({...row, series_id: row.series_id || row.benchmark_id}));
  const rows = modeRows.concat(benchmarkRows);
  const byMode = new Map();
  for (const row of rows) {
    if (!byMode.has(row.series_id)) byMode.set(row.series_id, []);
    byMode.get(row.series_id).push(row);
  }
  for (const values of byMode.values()) values.sort((a, b) => String(a.minute).localeCompare(String(b.minute)));
  const labels = new Map([
    ...data.modes.map((row) => [row.market, row.label || row.market]),
    ...(data.benchmarks || []).map((row) => [row.benchmark_id, row.label || row.benchmark_id]),
  ]);
  const series = [...byMode.entries()].map(([seriesId, values], index) => ({
    seriesId,
    values,
    index,
    valid: values.filter((row) => row.return_pct != null && Number.isFinite(Number(row.return_pct))),
  }));
  const allPoints = series.flatMap((item) => item.valid);
  const visibleSeries = series.filter((item) => !hiddenEquitySeries.has(item.seriesId));
  const points = visibleSeries.flatMap((item) => item.valid);
  $("chart-legend").innerHTML = series.map((item) => {
    const latest = item.valid.at(-1);
    const latestText = latest ? `${Number(latest.return_pct) >= 0 ? "+" : ""}${sourceNumber(latest.return_pct)}%` : "—";
    const label = labels.get(item.seriesId) || item.seriesId;
    const hidden = hiddenEquitySeries.has(item.seriesId);
    return `<button type="button" class="legend-toggle${hidden ? " is-hidden" : ""}" data-series-id="${esc(item.seriesId)}" aria-pressed="${String(!hidden)}" aria-label="${hidden ? "顯示" : "隱藏"}${esc(label)}曲線"><i class="series-${item.index % COLORS.length}" aria-hidden="true"></i>${esc(label)} <strong class="${pnlClass(latest?.return_pct)}">${esc(latestText)}</strong></button>`;
  }).join("");
  const empty = $("chart-empty");
  empty.textContent = allPoints.length ? "所有曲線已隱藏；點選圖例圓點可重新顯示。" : "目前尚無分鐘報酬率資料";
  empty.classList.toggle("hidden", points.length > 0);
  svg.classList.toggle("hidden", points.length === 0);
  if (!points.length) {
    svg.innerHTML = "";
    $("equity-range-note").textContent = allPoints.length
      ? `${chartWindowLabel()} · 目前 ${number(series.length)} 條曲線皆已隱藏。`
      : `${chartWindowLabel()}內沒有可繪製資料。`;
    return;
  }
  const width = 960, height = 360, left = 76, right = 22, top = 24, bottom = 70;
  const times = [...new Set(allPoints.map((row) => String(row.minute)))].sort();
  const axis = timeAxis.buildTimeAxis({
    range: hasCustomChartDates() ? "all" : chartRange,
    timestamps: times.map((value) => new Date(value).getTime()),
    sessions: TW_STOCK_SESSIONS,
    collapseEmptyIntervals: true,
  });
  if (!axis) return;
  let ymin = Math.min(0, ...points.map((row) => Number(row.return_pct)));
  let ymax = Math.max(0, ...points.map((row) => Number(row.return_pct)));
  const pad = Math.max(.01, (ymax - ymin) * .08); ymin -= pad; ymax += pad;
  const x = (minute) => timeAxis.position(axis, new Date(minute).getTime(), left, width - right);
  const y = (value) => top + (ymax - Number(value)) / (ymax - ymin) * (height - top - bottom);
  let html = "";
  for (let i = 0; i <= 4; i += 1) {
    const yy = top + i / 4 * (height - top - bottom);
    const value = ymax - i / 4 * (ymax - ymin);
    html += `<line class="axis" x1="${left}" y1="${yy}" x2="${width-right}" y2="${yy}"></line><text class="axis-text" x="6" y="${yy+4}">${esc(sourceNumber(value))}%</text>`;
  }
  for (const tick of axis.ticks) {
    const xx = timeAxis.position(axis, tick.timestamp, left, width - right);
    const tickClass = tick.kind === "session" ? "axis-time session" : "axis-time";
    const labelClass = tick.kind === "session" ? "axis-text axis-session-text" : "axis-text";
    const anchor = tick.rotate ? "end" : "middle";
    const labelY = tick.kind === "session" ? height - 34 : height - 8;
    const transform = tick.rotate ? ` transform="rotate(-45 ${xx} ${labelY})"` : "";
    html += `<line class="${tickClass}" x1="${xx}" y1="${top}" x2="${xx}" y2="${height-bottom}"></line><text class="${labelClass}" text-anchor="${anchor}" x="${xx}" y="${labelY}"${transform}>${esc(tick.label)}</text>`;
  }
  visibleSeries.forEach((item) => {
    const color = COLORS[item.index % COLORS.length];
    const path = item.valid.map((row, i) => `${i ? "L" : "M"}${x(row.minute).toFixed(1)},${y(row.return_pct).toFixed(1)}`).join(" ");
    html += `<path class="chart-line" stroke="${color}" d="${path}"></path>`;
    for (const row of item.valid.filter((value) => value.valuation_stale)) html += `<circle class="stale-dot" cx="${x(row.minute)}" cy="${y(row.return_pct)}" r="3"></circle>`;
  });
  svg.innerHTML = html;
  const start = new Date(times[0]).toLocaleString("zh-TW", {timeZone:"Asia/Taipei", hour12:false});
  const end = new Date(times.at(-1)).toLocaleString("zh-TW", {timeZone:"Asia/Taipei", hour12:false});
  const sampled = chartHistory?.downsampled ? `；已保留端點與區間極值縮圖（原 ${number(chartHistory.raw_points_in_range)} 點）` : "";
  $("equity-range-note").textContent = `${chartWindowLabel()} · ${start} ～ ${end} · 顯示 ${number(points.length)} 點、${number(visibleSeries.length)}/${number(series.length)} 條線；全體無資料的時間已壓縮${sampled}`;
}

function syncChartRangeControl() {
  $("equity-time-range").querySelectorAll("button[data-range]").forEach((button) => {
    button.setAttribute("aria-pressed", String(!hasCustomChartDates() && button.dataset.range === chartRange));
  });
}

function applyChartHistory(payload) {
  chartHistory = payload;
  const startInput = $("equity-start-date");
  const endInput = $("equity-end-date");
  startInput.min = payload.available_start_date || "";
  startInput.max = payload.available_end_date || "";
  endInput.min = payload.available_start_date || "";
  endInput.max = payload.available_end_date || "";
  if (snapshot) renderChart(snapshot);
}

async function loadChartHistory({preferCache = false} = {}) {
  if (document.hidden) return;
  const requestedRange = hasCustomChartDates() ? "all" : chartRange;
  const requestedStart = selectedChartStartDate();
  const requestedEnd = selectedChartEndDate();
  const requestedKey = chartRequestKey();
  const cached = chartHistoryCache.get(requestedKey);
  if (preferCache) {
    if (cached) applyChartHistory(cached.payload);
    else {
      chartHistory = null;
      if (snapshot) renderChart(snapshot);
    }
    if (cached && Date.now() - cached.receivedAt < HISTORY_CLIENT_CACHE_MS) return;
  }
  if (historyInFlight) return;
  historyInFlight = true;
  try {
    const params = new URLSearchParams({range: requestedRange});
    if (requestedStart) params.set("start_date", requestedStart);
    if (requestedEnd) params.set("end_date", requestedEnd);
    const response = await fetchWithTimeout(`api/history?${params.toString()}`, {cache:"default"});
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const payload = await response.json();
    if (requestedKey !== chartRequestKey()) return;
    chartHistoryCache.set(requestedKey, {payload, receivedAt: Date.now()});
    applyChartHistory(payload);
  } catch (error) {
    $("equity-range-note").textContent = `${chartWindowLabel()}歷史載入失敗：${error}`;
  } finally {
    historyInFlight = false;
    if (requestedKey !== chartRequestKey()) void loadChartHistory({preferCache: true});
  }
}

function renderPositions(data) {
  const rows = positionRows;
  $("position-count").textContent = positionLoadError
    ? `${rows.length} / ${positionTotal} 筆 · 等待重試`
    : `${rows.length} / ${positionTotal} 筆`;
  const loadMore = $("load-more-positions");
  loadMore.classList.toggle("hidden", !positionHasMore);
  loadMore.disabled = positionLoading;
  loadMore.textContent = positionLoading ? "載入中…" : "載入更多";
  const errorRow = positionLoadError
    ? `<tr><td colspan="6">${esc(positionLoadError)}</td></tr>`
    : "";
  const rowHtml = rows.map((row) => {
    const {signedShares, realized:realizedNet, unrealized:unrealizedNet, total:totalNet} = resolvedPositionPnl(row);
    return `<tr>
      <td><strong>${esc(row.market)}</strong> ${badge(row.side === "long" ? "多" : "空", row.side === "long" ? "good" : "bad")}<small>${esc(row.session_date)} · ${esc(row.symbol)} ${esc(row.name || "")}</small></td>
      <td><strong>${pct(row.target_weight)}</strong><small>成交 ${number(row.filled_shares)}／預計 ${number(row.requested_shares)} 股</small><small>剩餘 ${number(Math.abs(signedShares))} 股</small></td>
      <td><strong>進 ${money(row.entry_price)}</strong><small>${shortTime(row.entry_at)}${row.simulation_replay ? ` · 開盤價重建；原始訊號 ${shortTime(row.source_signal_at)}` : ""}</small><small>清算 ${money(row.last_mark_price)} · ${shortTime(row.last_quote_at)}</small></td>
      <td><strong>TP ${sourceNumber(row.take_profit_price)} · SL ${sourceNumber(row.stop_trigger_price)}</strong><small>${esc(row.stop_order_status)}</small><small>13:20 ${row.eod_limit_price == null ? "—" : sourceNumber(row.eod_limit_price)} · ${esc(row.eod_limit_order_status || "未到")} · ${shortTime(row.eod_limit_submitted_at)}</small><small>13:25 ${row.closing_auction_limit_price == null ? "—" : sourceNumber(row.closing_auction_limit_price)} · ${esc(row.closing_auction_order_status || "未到")}</small></td>
	    <td><strong>${row.exit_price == null ? (row.last_exit_price == null ? "持倉中" : `部分 ${money(row.last_exit_price)}`) : money(row.exit_price)}</strong><small>${esc(row.exit_reason || row.status || "—")}</small><small>${shortTime(row.exit_at || row.last_exit_at)}</small></td>
	    <td><strong class="${pnlClass(totalNet)}">總 ${money(totalNet)}</strong><small class="${pnlClass(realizedNet)}">已實現 ${money(realizedNet)}</small><small class="${pnlClass(unrealizedNet)}">未實現 ${money(unrealizedNet)} · ${row.valuation_stale ? badge("延用", "warn") : badge("新鮮", "good")}</small></td>
    </tr>`;
  }).join("");
  $("position-body").innerHTML = errorRow + (rowHtml || `<tr><td colspan="6">目前沒有符合篩選的持倉</td></tr>`);
}

function renderSignals() {
  $("signal-count").textContent = signalLoadError
    ? `${signalRows.length} / ${signalTotal} 筆 · 等待重試`
    : `${signalRows.length} / ${signalTotal} 筆`;
  const loadMore = $("load-more-signals");
  loadMore.classList.toggle("hidden", !signalHasMore);
  loadMore.disabled = signalLoading;
  loadMore.textContent = signalLoading ? "載入中…" : "載入更多";
  const target = signalDirectionSummary.target || {};
  const preBalance = signalDirectionSummary.pre_balance || {};
  const actual = signalDirectionSummary.actual || {};
  const positionMap = new Map(positionRows.map((row) => [`${row.session_date}\u0000${row.market}\u0000${row.symbol}`, row]));
  const modeMap = new Map((snapshot?.modes || []).map((mode) => [mode.market, mode]));
  $("signal-direction-summary").innerHTML = [
    ["區間訊號目標", target],
    ["整張／深度後", preBalance],
    ["方向平衡後", actual],
  ].map(([label, row]) => `<div><span>${esc(label)}</span><strong>${esc(directionPair(row))}</strong></div>`).join("");
  const errorRow = signalLoadError
    ? `<tr class="signal-load-error"><td colspan="6">${esc(signalLoadError)}</td></tr>`
    : "";
  const rowHtml = signalRows.map((row) => {
    const position = positionMap.get(`${row.session_date}\u0000${row.market}\u0000${row.symbol}`);
    const mode = modeMap.get(row.market);
    const positionPnl = position ? resolvedPositionPnl(position) : null;
    const hasPosition = Boolean(position && Number(position.filled_shares || 0) > 0);
    const isOpen = hasPosition && positionPnl.signedShares !== 0;
    const entryNotional = hasPosition ? Math.abs(Number(position.filled_shares || 0)) * Number(position.entry_price || 0) : null;
    const modeTotalEquity = row.session_date === snapshot?.session_date ? Number(mode?.total_equity_twd) : NaN;
    const equityImpactPct = hasPosition && positionPnl.total != null && Number.isFinite(modeTotalEquity) && Math.abs(modeTotalEquity) > .01
      ? Number(positionPnl.total) / modeTotalEquity * 100
      : null;
    const currentPrice = isOpen ? position.last_mark_price : position?.exit_price ?? position?.last_exit_price;
    const currentLabel = isOpen ? "現在可清算價" : hasPosition ? "已平倉價" : "未成交";
    const currentAt = isOpen ? position.last_quote_at : position?.exit_at ?? position?.last_exit_at;
    const eligibility = row.day_trade_eligible
      ? badge(row.sell_first_allowed ? "可雙向" : "僅買先", row.sell_first_allowed ? "good" : "warn")
      : badge("不可當沖", "bad");
    const result = badge(row.status, row.status === "ready" ? "good" : ["partial_depth", "partial_directional_mix"].includes(row.status) ? "warn" : row.status === "hold" ? "" : "bad");
    return `<tr>
      <td><strong>${esc(row.market)}</strong><small>${esc(row.session_date)} · ${shortTime(row.signal_at)}</small></td>
      <td><strong>${esc(row.symbol)}</strong> ${badge(row.side, row.side === "long" ? "good" : row.side === "short" ? "bad" : "")}<small>${esc(row.name || "")}</small><small>${eligibility} ${result}</small></td>
	    <td><strong>${sourceNumber(row.raw_score ?? row.score)}</strong><small>權重 ${pct(row.target_weight)}</small><small>${esc(row.reason || "")}</small></td>
	    <td>${hasPosition ? `<strong>進場價 ${money(position.entry_price)}</strong><small>進場名目 ${money(entryNotional)} · 費用 ${money(position.entry_fee_twd)}</small>` : `<strong>未成交・無進場成本</strong>`}<small>成交 ${number(row.filled_shares)}／${number(row.requested_shares)} 股 · L1 ${number(row.top_book_capacity_shares)}</small></td>
	    <td><strong>${currentLabel} ${hasPosition ? money(currentPrice) : "—"}</strong><small>${hasPosition ? shortTime(currentAt) : `訊號時 bid／ask ${sourceNumber(row.bid)}／${sourceNumber(row.ask)}`}</small><small class="${pnlClass(positionPnl?.total)}">該檔盈虧 ${hasPosition ? money(positionPnl.total) : "不計盈虧"}</small></td>
	    <td><strong class="${pnlClass(positionPnl?.total)}">佔該模式總權益 ${equityImpactPct == null ? "—" : `${equityImpactPct >= 0 ? "+" : ""}${displayPct(equityImpactPct)}`}</strong><small>模式總權益 ${Number.isFinite(modeTotalEquity) ? summaryMoney(modeTotalEquity) : "—"}</small><small>${hasPosition ? (position.valuation_stale ? badge("估值延用", "warn") : badge("估值新鮮", "good")) : "未成交不納入"}</small></td>
    </tr>`;
  }).join("");
  $("signal-body").innerHTML = errorRow + (rowHtml || `<tr><td colspan="6">目前沒有符合篩選的訊號</td></tr>`);
}

function renderEvents() {
  $("event-count").textContent = eventLoadError
    ? `${number(eventRows.length)} / ${number(eventTotal)} 筆 · 等待重試`
    : `${number(eventRows.length)} / ${number(eventTotal)} 筆（委託 ${number(eventOrderTotal)}／成交 ${number(eventFillTotal)}）`;
  const loadMore = $("load-more-events");
  loadMore.classList.toggle("hidden", !eventHasMore);
  loadMore.disabled = eventLoading;
  loadMore.textContent = eventLoading ? "載入中…" : "載入更多";
  const errorRow = eventLoadError
    ? `<tr><td colspan="6">${esc(eventLoadError)}</td></tr>`
    : "";
  const rowHtml = eventRows.map((row) => `<tr><td>${esc(row.session_date)}<small>${shortTime(row.fill_at || row.recorded_at)}</small></td><td>${esc(row.market)}<small>${esc(row.symbol)}</small></td><td>${esc(row.purpose)}</td><td>${esc(row.order_type || row.event_kind)}</td><td>${sourceNumber(row.price)} × ${number(row.quantity)}</td><td>${esc(row.status || row.event_kind)}</td></tr>`).join("");
  $("event-body").innerHTML = errorRow + (rowHtml || `<tr><td colspan="6">尚無委託／成交事件</td></tr>`);
}

function renderAudit(data) {
  const counts = data.record_counts || {};
  const items = [
    ["交易日", data.session_date], ["模擬模式", data.simulation_only ? "是，正式下單不可能" : "否"],
    ["完整帳本累積訊號／委託／成交", `${number(counts.signals)} / ${number(counts.orders)} / ${number(counts.fills)}`], ["策略／即時基準／補登基準 mark", `${number(counts.marks)} / ${number(counts.benchmark_marks)} / ${number(counts.benchmark_history_marks)}`],
    ["狀態 API 視窗", Object.entries(data.payload_window || {}).map(([key, value]) => `${key}:${number(value)}`).join(" · ") || "—"],
    ...data.modes.map((mode) => [`${mode.market} checkpoint`, mode.checkpoint_ready ? `READY · ${mode.checkpoint_fingerprint || "fingerprint pending"}` : "MISSING"]),
    ...data.modes.map((mode) => [`${mode.market} 資格資料`, Object.entries(mode.eligibility_coverage || {}).map(([venue, row]) => `${venue}:${row.covered ? row.target_date : `缺 ${row.target_date} / latest ${row.latest_date || "—"}`}`).join(" · ") || "尚未載入"]),
    ...data.modes.map((mode) => [`${mode.market} 目前資格來源`, Object.entries(mode.current_eligibility_coverage || {}).map(([venue, row]) => `${venue}:${row.covered ? `READY ${row.target_date}` : `缺 ${row.target_date} / latest ${row.latest_date || "—"}`}`).join(" · ") || "尚未檢查"]),
    ...(data.benchmarks || []).map((row) => [`${row.label || row.benchmark_id}`, row.return_pct == null ? `等待可成交報價 · ${row.valuation_source || "尚未進場"}` : `${row.return_pct >= 0 ? "+" : ""}${sourceNumber(row.return_pct)}% · ${shortTime(row.entry_at)} 起 · 資金 ${money(row.initial_capital_twd)} · ${row.contract_code || row.symbol || ""}`]),
  ];
  $("audit-grid").innerHTML = items.map(([label,value]) => `<div class="audit-item"><span>${esc(label)}</span><strong>${esc(value)}</strong></div>`).join("");
  const contract = data.source_contract || {};
  $("source-signal").textContent = contract.signal || "—"; $("source-replay").textContent = contract.replay || "—"; $("source-fill").textContent = contract.entry_fill || "—";
  $("source-fees").textContent = contract.fees || "—";
  $("source-comparison").textContent = `${contract.comparison || "—"}；${contract.benchmarks || "—"}；${contract.benchmark_history || "—"}`;
  $("source-eligibility").textContent = contract.eligibility || "—"; $("source-depth").textContent = `${contract.depth_limit || "—"}；${contract.bracket_fill || "—"}；${contract.terminal_flatten || "—"}`;
  $("source-exit").textContent = contract.exit_schedule || "—";
  $("source-latency").textContent = contract.latency || "—";
}

function revisionOf(data) {
  const counts = data.record_counts || {};
  return JSON.stringify([
    data.session_date,
    counts.orders, counts.fills, counts.marks, counts.benchmark_marks, counts.events,
    data.session_progress,
    data.preopen?.updated_at,
    data.modes.map((row) => [
      row.market, row.total_equity_twd, row.open_position_count,
      row.stale_position_count, row.force_exit_failures,
      row.terminal_flatten_count, row.terminal_flatten_degraded_count,
      row.engine_status,
    ]),
    (data.benchmarks || []).map((row) => [row.benchmark_id, row.return_pct, row.valuation_stale, row.contract_code, row.roll_count, row.last_roll_at]),
  ]);
}

function render({heavy = true} = {}) {
  if (!snapshot) return;
  renderHeader(snapshot);
  renderOverview(snapshot);
  if (!heavy) return;
  renderOperations(snapshot);
  renderModes(snapshot);
  renderBenchmarks(snapshot);
  renderChart(snapshot);
  renderPositions(snapshot);
  renderEvents();
  renderAudit(snapshot);
}

async function loadSignals({append = false} = {}) {
  if (!snapshot) return;
  if (signalAbortController) signalAbortController.abort();
  signalAbortController = new AbortController();
  const controller = signalAbortController;
  const sequence = ++signalRequestSequence;
  const requestRange = detailRangeKey();
  const params = new URLSearchParams({
    start_date: selectedDetailStartDate(),
    end_date: selectedDetailEndDate(),
    mode: selectedMode(),
    symbol: $("symbol-filter").value.trim(),
    status: $("status-filter").value,
    offset: String(append ? signalRows.length : 0),
    limit: String(SIGNAL_PAGE_SIZE),
  });
  signalLoading = true;
  renderSignals();
  try {
    const response = await fetchWithTimeout(`api/signals?${params.toString()}`, {cache: "no-store", signal: controller.signal});
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const page = await response.json();
    if (sequence !== signalRequestSequence) return;
    if (requestRange !== detailRangeKey()) return;
    signalLoadError = "";
    signalRows = append ? signalRows.concat(page.rows || []) : (page.rows || []);
    signalTotal = Number(page.total || 0);
    signalHasMore = Boolean(page.has_more);
    signalRecordCount = Number(page.record_count || 0);
    signalDirectionSummary = page.direction_summary || {};
  } catch (error) {
    if (sequence !== signalRequestSequence) return;
    if (error?.name === "AbortError") return;
    signalLoadError = `訊號明細暫時無法更新：${error}`;
    signalRecordCount = null;
  } finally {
    if (sequence === signalRequestSequence) {
      if (signalAbortController === controller) signalAbortController = null;
      signalLoading = false;
      renderSignals();
    }
  }
}

async function loadPositions({append = false} = {}) {
  if (!snapshot) return;
  if (positionAbortController) positionAbortController.abort();
  positionAbortController = new AbortController();
  const controller = positionAbortController;
  const sequence = ++positionRequestSequence;
  const requestRange = detailRangeKey();
  const params = new URLSearchParams({
    start_date: selectedDetailStartDate(),
    end_date: selectedDetailEndDate(),
    mode: selectedMode(),
    symbol: $("symbol-filter").value.trim(),
    status: $("status-filter").value,
    offset: String(append ? positionRows.length : 0),
    limit: String(POSITION_PAGE_SIZE),
  });
  positionLoading = true;
  renderPositions(snapshot);
  try {
    const response = await fetchWithTimeout(`api/positions?${params.toString()}`, {cache: "no-store", signal: controller.signal});
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const page = await response.json();
    if (sequence !== positionRequestSequence) return;
    if (requestRange !== detailRangeKey()) return;
    positionLoadError = "";
    positionRows = append ? positionRows.concat(page.rows || []) : (page.rows || []);
    positionTotal = Number(page.total || 0);
    positionHasMore = Boolean(page.has_more);
  } catch (error) {
    if (sequence !== positionRequestSequence) return;
    if (error?.name === "AbortError") return;
    positionLoadError = `持倉明細暫時無法更新：${error}`;
  } finally {
    if (sequence === positionRequestSequence) {
      if (positionAbortController === controller) positionAbortController = null;
      positionLoading = false;
      renderPositions(snapshot);
      renderSignals();
    }
  }
}

async function loadEvents({append = false} = {}) {
  if (!snapshot) return;
  if (eventAbortController) eventAbortController.abort();
  eventAbortController = new AbortController();
  const controller = eventAbortController;
  const sequence = ++eventRequestSequence;
  const requestRange = detailRangeKey();
  const params = new URLSearchParams({
    start_date: selectedDetailStartDate(),
    end_date: selectedDetailEndDate(),
    mode: selectedMode(),
    symbol: $("symbol-filter").value.trim(),
    offset: String(append ? eventRows.length : 0),
    limit: String(EVENT_PAGE_SIZE),
  });
  eventLoading = true;
  renderEvents();
  try {
    const response = await fetchWithTimeout(`api/events?${params.toString()}`, {cache: "no-store", signal: controller.signal});
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const page = await response.json();
    if (sequence !== eventRequestSequence) return;
    if (requestRange !== detailRangeKey()) return;
    eventLoadError = "";
    eventRows = append ? eventRows.concat(page.rows || []) : (page.rows || []);
    eventTotal = Number(page.total || 0);
    eventOrderTotal = Number(page.order_total || 0);
    eventFillTotal = Number(page.fill_total || 0);
    eventHasMore = Boolean(page.has_more);
    const counts = page.record_counts || {};
    eventRecordRevision = JSON.stringify([requestRange, Number(counts.orders || 0), Number(counts.fills || 0)]);
  } catch (error) {
    if (sequence !== eventRequestSequence) return;
    if (error?.name === "AbortError") return;
    eventLoadError = `事件明細暫時無法更新：${error}`;
    eventRecordRevision = null;
  } finally {
    if (sequence === eventRequestSequence) {
      if (eventAbortController === controller) eventAbortController = null;
      eventLoading = false;
      renderEvents();
    }
  }
}

async function refresh() {
  if (document.hidden) return;
  if (refreshInFlight) {
    refreshQueued = true;
    return;
  }
  refreshInFlight = true;
  try {
    const started = performance.now();
    const date = selectedDate();
    const response = await fetchWithTimeout(`api/status${date ? `?date=${encodeURIComponent(date)}` : ""}`, {cache: "no-store"});
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    snapshot = await response.json();
    lastFetchMs = performance.now() - started;
    syncFilters(snapshot);
    const revision = revisionOf(snapshot);
    const heavy = revision !== lastRenderedRevision;
    lastRenderedRevision = revision;
    render({heavy});
    const currentSignalCount = Number((snapshot.record_counts || {}).signals || 0);
    const detailLoads = [loadPositions()];
    if (signalRecordCount == null || currentSignalCount !== signalRecordCount) detailLoads.push(loadSignals());
    const counts = snapshot.record_counts || {};
    const currentEventRevision = JSON.stringify([detailRangeKey(), Number(counts.orders || 0), Number(counts.fills || 0)]);
    if (eventRecordRevision == null || currentEventRevision !== eventRecordRevision) detailLoads.push(loadEvents());
    await Promise.all(detailLoads);
  } catch (error) {
    const alert = $("alert"); alert.classList.remove("hidden"); alert.textContent = `面板讀取失敗：${error}`;
    $("health").textContent = "UNAVAILABLE"; $("health").className = "pill critical";
  } finally {
    refreshInFlight = false;
    if (refreshQueued) {
      refreshQueued = false;
      void refresh();
    }
  }
}

function renderFilteredDetails({includeChart = false} = {}) {
  if (!snapshot) return;
  if (includeChart) renderChart(snapshot);
  renderPositions(snapshot);
  renderEvents();
}

function filtersChanged({debounceSignals = false, includeChart = false, reloadEvents = true} = {}) {
  if (filterAnimationFrame != null) cancelAnimationFrame(filterAnimationFrame);
  if (debounceSignals) {
    filterAnimationFrame = requestAnimationFrame(() => {
      filterAnimationFrame = null;
      renderFilteredDetails({includeChart});
    });
  } else {
    renderFilteredDetails({includeChart});
  }
  window.clearTimeout(signalFilterTimer);
  if (debounceSignals) signalFilterTimer = window.setTimeout(() => {
    void loadPositions();
    void loadSignals();
    if (reloadEvents) void loadEvents();
  }, 180);
  else {
    void loadPositions();
    void loadSignals();
    if (reloadEvents) void loadEvents();
  }
}

$("mode-filter").addEventListener("change", () => filtersChanged({includeChart: true}));
function detailDateChanged(event) {
  const startInput = $("detail-start-date");
  const endInput = $("detail-end-date");
  if (startInput.value && endInput.value && startInput.value > endInput.value) {
    if (event.target === startInput) endInput.value = startInput.value;
    else startInput.value = endInput.value;
  }
  lastRenderedRevision = null;
  signalRecordCount = null;
  signalRows = [];
  eventRecordRevision = null;
  eventRows = [];
  positionRows = [];
  positionTotal = 0;
  positionHasMore = false;
  if (signalAbortController) signalAbortController.abort();
  if (eventAbortController) eventAbortController.abort();
  if (positionAbortController) positionAbortController.abort();
  void refresh();
}
$("detail-start-date").addEventListener("change", detailDateChanged);
$("detail-end-date").addEventListener("change", detailDateChanged);
$("status-filter").addEventListener("change", () => filtersChanged({reloadEvents: false}));
$("symbol-filter").addEventListener("input", () => filtersChanged({debounceSignals: true}));
$("reset-filters").addEventListener("click", () => {
  $("mode-filter").value = "all";
  $("symbol-filter").value = "";
  $("status-filter").value = "all";
  filtersChanged({includeChart: true});
});
$("load-more-signals").addEventListener("click", () => loadSignals({append: true}));
$("load-more-events").addEventListener("click", () => loadEvents({append: true}));
$("load-more-positions").addEventListener("click", () => loadPositions({append: true}));
$("equity-time-range").addEventListener("click", (event) => {
  const button = event.target.closest("button[data-range]");
  if (!button || !(button.dataset.range in TIME_RANGE_LABELS)) return;
  chartRange = button.dataset.range;
  $("equity-start-date").value = "";
  $("equity-end-date").value = "";
  try { localStorage.setItem("tw-day-trade-equity-time-range", chartRange); } catch (_error) { /* optional */ }
  syncChartRangeControl();
  void loadChartHistory({preferCache: true});
});
function chartDateChanged(event) {
  const startInput = $("equity-start-date");
  const endInput = $("equity-end-date");
  if (startInput.value && endInput.value && startInput.value > endInput.value) {
    if (event.target === startInput) endInput.value = startInput.value;
    else startInput.value = endInput.value;
  }
  syncChartRangeControl();
  chartHistory = null;
  if (snapshot) renderChart(snapshot);
  void loadChartHistory({preferCache: true});
}
$("equity-start-date").addEventListener("change", chartDateChanged);
$("equity-end-date").addEventListener("change", chartDateChanged);
$("clear-equity-dates").addEventListener("click", () => {
  $("equity-start-date").value = "";
  $("equity-end-date").value = "";
  syncChartRangeControl();
  void loadChartHistory({preferCache: true});
});
$("chart-legend").addEventListener("click", (event) => {
  const button = event.target.closest("button[data-series-id]");
  if (!button) return;
  const seriesId = button.dataset.seriesId;
  if (hiddenEquitySeries.has(seriesId)) hiddenEquitySeries.delete(seriesId);
  else hiddenEquitySeries.add(seriesId);
  try { localStorage.setItem(HIDDEN_EQUITY_SERIES_STORAGE_KEY, JSON.stringify([...hiddenEquitySeries])); } catch (_error) { /* optional */ }
  if (snapshot) renderChart(snapshot);
});
setInterval(() => { $("clock").textContent = new Date().toLocaleString("zh-TW", {timeZone:"Asia/Taipei", hour12:false}); }, 1000);
document.addEventListener("visibilitychange", () => { if (!document.hidden) { void refresh(); void loadChartHistory(); } });
syncChartRangeControl();
void refresh();
void loadChartHistory();
window.setInterval(() => { void refresh(); void loadChartHistory(); }, PRICE_REFRESH_MS);

"use strict";

const PRICE_REFRESH_MS = 60000;
const SERVICE_REVISION_REFRESH_MS = 1000;
const TW_PUBLIC_STATUS_REFRESH_MS = 30000;
const Dashboard = window.StockAgentDashboard;
const fetchWithTimeout = Dashboard.createFetch({timeoutMs: 15000});
const SIGNAL_PAGE_SIZE = 100;
const POSITION_PAGE_SIZE = 100;
const EVENT_PAGE_SIZE = 100;
const DATA_MONITOR_STATUS_PATHS = [
  "/data-monitor/api/status",
  "../data-monitor/api/status",
];
const COLORS = ["#37d3ff", "#5ee0a0", "#a98cff", "#f5bd4f", "#ff7ac8", "#73e6d1", "#ff9f68"];
const timeAxis = window.StockAgentTimeAxis;
const TW_STOCK_SESSIONS = [
  {label: "開", minute: 9 * 60},
  {label: "收", minute: 13 * 60 + 30},
];
const HIDDEN_EQUITY_SERIES_STORAGE_KEY = "tw-day-trade-hidden-equity-series";
const HISTORY_CLIENT_CACHE_MS = 45000;
let snapshot = null;
let chartHistory = null;
let hiddenEquitySeries = new Set();
let chartHistoryCache = new Map();
let historyInFlight = false;
let lastFetchMs = null;
let refreshInFlight = false;
let refreshQueued = false;
let refreshForceQueued = false;
let lastRenderedRevision = null;
let lastFilterRevision = null;
let lastSourceUpdatedAt = "";
let lastServiceRevision = "";
let revisionRefreshInFlight = false;
let signalRows = [];
let signalDirectionSummary = {};
let signalOpeningExecutionAudit = {};
let signalTotal = 0;
let signalHasMore = false;
let signalRecordCount = null;
let signalLoading = false;
let signalLoadError = "";
let signalRequestSequence = 0;
let signalFeatureDrivers = {};
let featurePanelSignalKey = "";
let featurePanelScopeText = "";
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
let positionRows = [];
let positionTotal = 0;
let positionHasMore = false;
let positionLoading = false;
let positionLoadError = "";
let positionRequestSequence = 0;
let positionAbortController = null;
let availableDetailDates = [];
let twPublicMonitorData = null;
let twPublicMonitorRefreshInFlight = false;
let twPublicMonitorLastFetchMs = null;
let twPublicMonitorLastUpdated = null;
let twPublicMonitorAbortController = null;

try {
  const storedHiddenSeries = JSON.parse(localStorage.getItem(HIDDEN_EQUITY_SERIES_STORAGE_KEY) || "[]");
  if (Array.isArray(storedHiddenSeries)) hiddenEquitySeries = new Set(storedHiddenSeries.map(String));
} catch (_error) { /* storage can be disabled */ }

const $ = Dashboard.byId;
const esc = Dashboard.escapeHtml;
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
const signalRowKey = (row = {}) => {
  const sessionDate = String(row.session_date || "");
  const market = String(row.market || "");
  const symbol = String(row.symbol || "");
  const signalAt = String(row.signal_at || "");
  if (!sessionDate || !market || !symbol) return "";
  return signalAt ? `${sessionDate}|${market}|${symbol}|${signalAt}` : `${sessionDate}|${market}|${symbol}`;
};
const selectedSignalRow = () => signalRows.find((row) => signalRowKey(row) === featurePanelSignalKey) || null;
const resolveSignalFeatureDrivers = (row = null, rowKey = "") => {
  const candidates = [];
  if (rowKey) candidates.push(rowKey);
  if (row) candidates.push(signalRowKey(row));
  candidates.push(featurePanelSignalKey);
  for (const candidate of candidates) {
    if (!candidate) continue;
    const value = signalFeatureDrivers[candidate];
    if (Array.isArray(value)) return value;
    if (value && Array.isArray(value.drivers)) return value.drivers;
  }
  return [];
};
const featureDriversSummaryText = () => featurePanelScopeText
  ? `特徵資料來源：${featurePanelScopeText}`
  : "目前只顯示本次訊號載入頁面可對應到的特徵資料。";

function sortedFeatureColumns(drivers) {
  const featureKey = "feature";
  const featured = new Set(["feature", "weighted_abs_value", "value", "abs_value", "importance", "score"]);
  const ordered = [];
  const rest = new Set();
  for (const driver of drivers) {
    if (!driver || typeof driver !== "object") continue;
    for (const key of Object.keys(driver)) {
      if (key === featureKey) continue;
      if (featured.has(key) && !ordered.includes(key)) {
        ordered.push(key);
      } else {
        rest.add(key);
      }
    }
  }
  return [
    featureKey,
    ...ordered,
    ...rest,
  ].filter((key, index, list) => list.indexOf(key) === index);
}

const formatFeatureValue = (value) => {
  if (value == null) return "—";
  if (typeof value === "number") {
    if (!Number.isFinite(value)) return "—";
    const absValue = Math.abs(value);
    return absValue < .005 ? "0" : Number(value).toLocaleString("zh-TW", {maximumFractionDigits: 2});
  }
  if (typeof value === "boolean") return value ? "是" : "否";
  if (typeof value === "string") return value || "—";
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "object") {
    try { return JSON.stringify(value); } catch (_error) { return "—"; }
  }
  return String(value);
};

function syncFeaturePanelSelection() {
  const exists = signalRows.some((row) => signalRowKey(row) === featurePanelSignalKey);
  if (!exists) featurePanelSignalKey = "";
}
const displayPct = (value) => {
  if (value == null || !Number.isFinite(Number(value))) return "—";
  const resolved = Number(value);
  const displayValue = Math.abs(resolved) < .005 ? 0 : resolved;
  return `${displayValue.toLocaleString("zh-TW", {maximumFractionDigits: 2})}%`;
};
const money = (value) => value == null ? "—" : `NT$ ${monetaryNumber(value)}`;
const pct = (value) => value == null ? "—" : `${sourceNumber(Number(value) * 100)}%`;
const shortTime = (value) => value ? String(value).replace("T", " ").slice(5, 19) : "—";
const shortDateTime = (value) => {
  const parsed = new Date(value || "");
  if (Number.isNaN(parsed.getTime())) return "—";
  return parsed.toLocaleString("zh-TW", {
    timeZone: "Asia/Taipei",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
};
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
const progressValue = (ratio) => {
  if (ratio == null || !Number.isFinite(Number(ratio))) return null;
  return clampRatio(Number(ratio));
};
const progress = (ratio, kind = "") => {
  const bounded = clampRatio(ratio);
  const value = (bounded * 100).toFixed(2);
  return `<progress class="progress-track ${esc(kind)}" max="100" value="${value}" aria-label="${value}%"></progress>`;
};
const twPublicProgress = (row = {}) => {
  const progressData = row.acquisition_progress || {};
  const ratio = progressValue(progressData.ratio);
  const state = String(progressData.state || "");
  const baseKind = (state === "complete" || state === "preparing_next_date") ? "good" :
    state === "acquiring" ? "warn" :
    state === "streaming" ? "good" : "warn";
  return {
    ratio,
    kind: baseKind,
    label: String(progressData.label || "尚無進度資訊"),
    current: progressData.current,
    total: progressData.total,
    unit: progressData.unit || "資料量",
    state: state || "unknown",
    dataThrough: progressData.data_through,
    preparingForDate: progressData.preparing_for_date,
    firstDataAt: progressData.first_data_at_utc,
    complete: Boolean(progressData.up_to_date || progressData.coverage_complete || progressData.batch_complete),
    firstDataObserved: Boolean(progressData.first_data_observed),
    basis: progressData.basis,
  };
};
const twPublicPublicationTime = (row = {}) => {
  const publication = row.publication || {};
  const value = publication.applied_at_utc || publication.detected_at_utc || publication.last_checked_at_utc || row.latest_at_utc;
  const observedValue = publication.detected_at_utc || publication.observed_at_utc || publication.last_checked_at_utc || row.latest_at_utc;
  const checkedValue = publication.last_checked_at_utc || publication.detected_at_utc || row.last_verified_at_utc;
  const parsed = value ? new Date(value) : null;
  const observedParsed = observedValue ? new Date(observedValue) : null;
  const checkedParsed = checkedValue ? new Date(checkedValue) : null;
  return {
    value: value ? String(value) : "",
    label: parsed && Number.isFinite(parsed.getTime())
      ? shortDateTime(parsed.toISOString())
      : "尚未確認",
    observed: observedValue ? String(observedValue) : "",
    observedLabel: observedParsed && Number.isFinite(observedParsed.getTime())
      ? shortDateTime(observedParsed.toISOString())
      : "—",
    checked: checkedValue ? String(checkedValue) : "",
    checkedLabel: checkedParsed && Number.isFinite(checkedParsed.getTime())
      ? shortDateTime(checkedParsed.toISOString())
      : "—",
    basis: String(publication.basis || "尚無發布依據"),
    schedule: String(publication.schedule_label || "未提供")
      .replace(/^來源未承諾固定發布時刻；/, "")
      .trim(),
    exact: Boolean(publication.exact_time_declared),
  };
};
const twPublicRowsSource = (payload = {}) => {
  const rows = Array.isArray(payload.sources)
    ? payload.sources
    : Array.isArray(payload.rows)
      ? payload.rows
      : [];
  const byId = new Map();
  for (const row of rows) {
    if (!row || typeof row !== "object" || !row.id) continue;
    byId.set(String(row.id), row);
  }
  return [...byId.values()];
};
const twPublicCompletionHint = (item = {}) => {
  const dataThrough = item.data_through || "—";
  const preparingFor = item.preparingForDate || "—";
  if (item.complete && item.preparingForDate) {
    return `已完成本日，下一資料日 ${preparingFor}`;
  }
  if (item.complete) {
    return "已完成本日，等待下次來源切換";
  }
  if (item.preparingForDate && item.firstDataObserved) {
    return `已到第一筆，預備 ${preparingFor}`;
  }
  if (item.firstDataObserved) {
    return "已收到第一筆，持續取得中";
  }
  return `資料截至 ${dataThrough}`;
};
const twPublicAvailabilityText = (value) => {
  if (value == null) return "未提供";
  if (Array.isArray(value)) {
    const compact = [...new Set(value.filter(Boolean).map((entry) => String(entry).trim()).filter(Boolean))]
      .slice(0, 5)
      .join(", ");
    return compact || "未提供";
  }
  if (typeof value === "object") {
    const values = Object.keys(value)
      .filter((key) => value[key])
      .map((key) => `${key}: ${value[key]}`)
      .slice(0, 5)
      .join(", ");
    return values || "未提供";
  }
  return String(value) || "未提供";
};
const twPublicCoverageText = (coverage) => {
  if (!coverage || typeof coverage !== "object") return "未提供";
  const current = coverage.current;
  const total = coverage.total;
  const ratio = coverage.ratio;
  const unit = coverage.unit || "資料單位";
  if (Number.isFinite(Number(current)) && Number.isFinite(Number(total))) {
    return `${number(current)} / ${number(total)} ${unit}`;
  }
  if (ratio != null && Number.isFinite(Number(ratio))) {
    return `${(Number(ratio) * 100).toFixed(ratio < 0.1 ? 2 : 1)}%`;
  }
  return String(coverage.label || coverage.basis || "尚未提供");
};
const twPublicAutomationText = (automation = {}, fallback) => {
  const mode = automation.mode ? `模式：${automation.mode}` : "";
  const hasScheduleText = Boolean(automation.schedule_label || automation.next_run_utc || automation.next_run_at_utc);
  const scheduleText = automation.next_run_at_utc && !automation.schedule_label
    ? shortDateTime(automation.next_run_at_utc)
    : (automation.schedule_label || "未提供");
  const schedule = hasScheduleText ? String(scheduleText || fallback || "未提供") : String(fallback || "未提供");
  const enabled = automation.automatic_update === true ? "已自動" : automation.automatic_update === false ? "非自動" : "";
  return [mode, enabled, schedule].filter(Boolean).join(" · ");
};
const twPublicExecutionLabel = (row = {}, state = {}) => {
  const stateText = String(state.operation || state.state || row.execution_state || "unknown");
  const opLabel = row.operation_label ? `狀態：${row.operation_label}` : `運行：${stateText}`;
  const opReason = row.operation_reason ? `原因：${row.operation_reason}` : "";
  return [opLabel, opReason].filter(Boolean).join(" · ");
};
const twPublicRowsValueText = (row = {}) => {
  const sourceRows = row.rows;
  if (sourceRows == null) return "—";
  if (typeof sourceRows === "number") return `${number(sourceRows)} 筆`;
  if (typeof sourceRows === "string") return sourceRows || "—";
  if (Array.isArray(sourceRows)) return `${number(sourceRows.length)} 筆`;
  if (typeof sourceRows === "object") return `欄位 ${number(Object.keys(sourceRows).length)} 個`;
  return "—";
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
function chartWindowLabel() {
  const start = selectedDetailStartDate();
  const end = selectedDetailEndDate();
  return `${start || "最早資料"} ～ ${end || "最新資料"}`;
}
function chartRequestKey() {
  return JSON.stringify(["all", selectedDetailStartDate(), selectedDetailEndDate()]);
}
function rangeSummaryFor(seriesId) {
  if (!chartHistory || chartHistory.start_date !== (selectedDetailStartDate() || null)
    || chartHistory.end_date !== (selectedDetailEndDate() || null)) return null;
  return (chartHistory.range_summary || []).find((row) => row.series_id === seriesId) || null;
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
    || String(b.session_date || "").localeCompare(String(a.session_date || ""))
    || (Number.isFinite(bWeight) ? bWeight : 0) - (Number.isFinite(aWeight) ? aWeight : 0)
    || String(a.market || "").localeCompare(String(b.market || ""), "zh-Hant")
    || String(a.symbol || "").localeCompare(String(b.symbol || ""), "zh-Hant");
}

function beginSilentTableUpdate(bodyId, loadMoreId, append) {
  const body = $(bodyId);
  body?.setAttribute("aria-busy", "true");
  if (!append) return;
  const loadMore = $(loadMoreId);
  if (!loadMore) return;
  loadMore.disabled = true;
  loadMore.textContent = "載入中…";
}

function healthPresentation(value) {
  const health = String(value || "unavailable").toLowerCase();
  const labels = {
    active: "資料正常",
    ready: "資料正常",
    waiting: "等待資料",
    degraded: "資料或執行異常",
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
    waiting_09_00: {label: "等待 09:00 即時訊號", kind: ""},
    waiting_trading_day: {label: "等待交易日", kind: ""},
    missed: {label: "該日執行缺漏", kind: "bad"},
  };
  return presentations[status] || {label: status.replaceAll("_", " "), kind: "warn"};
}

function fillOutcomePresentation(value) {
  const outcome = String(value || "pending");
  const presentations = {
    filled: {label: "目標股數已建立部位", kind: "good"},
    partial: {label: "僅部分建立部位", kind: "warn"},
    no_fill: {label: "零成交", kind: "bad"},
    no_order: {label: "無合法整張委託", kind: "warn"},
    blocked: {label: "遭守門阻擋・零成交", kind: "bad"},
    pending: {label: "等待成交結果", kind: "warn"},
  };
  return presentations[outcome] || {label: outcome.replaceAll("_", " "), kind: "warn"};
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
  const modeOpenPositions = modes.reduce((sum, mode) => sum + Number(mode.open_position_count || 0), 0);
  const modeStalePositions = modes.reduce((sum, mode) => sum + Number(mode.stale_position_count || 0), 0);
  const openPositionCount = Number.isFinite(Number(data.open_position_count))
    ? Number(data.open_position_count)
    : modes.length ? modeOpenPositions : openPositions.length;
  const stalePositions = Number.isFinite(Number(data.stale_position_count))
    ? Number(data.stale_position_count)
    : modes.length ? modeStalePositions : openPositions.filter((row) => row.valuation_stale).length;
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
  const returns = modes
    .map((mode) => rangeSummaryFor(mode.market)?.return_pct)
    .map(Number)
    .filter(Number.isFinite);
  const best = returns.length ? Math.max(...returns) : null;
  const worst = returns.length ? Math.min(...returns) : null;
  const healthKind = healthyModes === modes.length ? "good" : healthyModes ? "warn" : "bad";
  const cards = [
    ["模式狀態", `${healthyModes}/${modes.length} 可解讀`, healthyModes === modes.length ? "所有 checkpoint 與執行狀態正常" : "有模式需要查看上方警示", healthKind],
    ["所選日持倉", `${number(openPositionCount)} 個`, stalePositions ? `${number(stalePositions)} 個估值延用` : "目前估值皆有新鮮報價", stalePositions ? "warn" : "good"],
    ["各模式已實現", realizedPnl == null ? "—" : `${realizedPnl >= 0 ? "+" : ""}${compactMoney(realizedPnl)}`, "已出場部分，已扣分攤後交易成本", pnlClass(realizedPnl)],
    ["各模式未實現", unrealizedPnl == null ? "—" : `${unrealizedPnl >= 0 ? "+" : ""}${compactMoney(unrealizedPnl)}`, stalePositions ? `含 ${number(stalePositions)} 個延用估值` : "以可清算 bid／ask 並扣剩餘成本", stalePositions ? "warn" : pnlClass(unrealizedPnl)],
    ["各模式總淨損益", totalPnl == null ? "—" : `${totalPnl >= 0 ? "+" : ""}${compactMoney(totalPnl)}`, reconciled ? "已實現＋未實現，已與總權益對帳" : reconciliationDifference == null ? "等待完整損益來源" : `對帳差異 ${summaryMoney(reconciliationDifference)}`, reconciled ? pnlClass(totalPnl) : "bad"],
    ["篩選區間報酬", best == null ? "—" : `${best >= 0 ? "+" : ""}${displayPct(best)} ～ ${worst >= 0 ? "+" : ""}${displayPct(worst)}`, `${chartWindowLabel()}；各模式以上一交易日最後權益為基準`, best != null && worst < 0 ? "warn" : pnlClass(best)],
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
  const blockers = data.modes.filter((mode) => !mode.checkpoint_ready || String(mode.engine_status || "").startsWith("critical") || String(mode.engine_status || "").startsWith("blocked"));
  const catchUps = data.modes.filter((mode) => mode.today_execution_status === "starting");
  const missed = data.modes.filter((mode) => mode.today_execution_status === "missed");
  const hasReplay = data.modes.some((mode) => mode.counterfactual_open_replay || mode.simulation_replay);
  const hasBenchmarkReplay = (data.benchmarks || []).some((row) => row.counterfactual_open_replay);
  const operationalIssues = Array.isArray(data.operational_issues) ? data.operational_issues : [];
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
  if (operationalIssues.length || hasReplay || hasBenchmarkReplay || data.health === "stale" || blockers.length || catchUps.length || missed.length || signalMissingEligibility.size || currentMissingEligibility.size) {
    const messages = [
      hasReplay ? "所選交易日使用實際開盤價做反事實重建；原始訊號時間保留，但這不是當時可成交報價、即時執行或券商成交。" : "",
      hasBenchmarkReplay ? "0050、2330 與台指期基準已補到實際開盤起點；補登區段是明確標示的回放，後續估值使用 receipt 驗證的逐分鐘觀察價；缺分鐘只延續前一筆實際觀察值並明示，不插值。" : "",
      data.health === "stale" ? "資料來源已逾時；畫面只能當歷史紀錄，不能視為現在行情。" : "",
      currentMissingEligibility.size ? `所選交易日當沖資格未完整覆蓋，後續訊號已停止執行：${[...currentMissingEligibility.entries()].map(([venue, row]) => `${venue.toUpperCase()} 需要 ${row.target_date || data.session_date || "所選日"}，最新僅到 ${row.latest_date || "無資料"}`).join("；")}` : "",
      !currentMissingEligibility.size && signalMissingEligibility.size ? "09:00 訊號產生時資格資料尚未到齊，因此已 fail-closed；較晚補齊的資料不會回填成假成交。" : "",
      catchUps.length ? `發現所選交易日執行缺漏，已立即啟動補跑：${catchUps.map((mode) => mode.label || mode.market).join("、")}` : "",
      missed.length ? `所選交易日進場時窗結束仍缺少執行紀錄：${missed.map((mode) => mode.label || mode.market).join("、")}` : "",
      ...operationalIssues.map((issue) => `${issue.title || issue.code}${Number(issue.count || 1) > 1 ? `（${number(issue.count)} 筆）` : ""}：${issue.detail || "已記錄異常"}`),
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
    const rangeSummary = rangeSummaryFor(mode.market);
    const initial = rangeSummary?.baseline_equity_twd == null ? null : Number(rangeSummary.baseline_equity_twd);
    const equity = rangeSummary?.end_equity_twd == null ? null : Number(rangeSummary.end_equity_twd);
    const pnl = rangeSummary?.range_net_pnl_twd == null ? null : Number(rangeSummary.range_net_pnl_twd);
    const returnPct = rangeSummary?.return_pct == null ? null : Number(rangeSummary.return_pct);
    const status = String(mode.engine_status || "unknown");
    const execution = executionStatusPresentation(mode.today_execution_status);
    const fillOutcome = fillOutcomePresentation(mode.today_execution_outcome || mode.entry_fill_outcome);
    const kind = status === "active" ? "good" : status.startsWith("blocked") || status.startsWith("critical") ? "bad" : "warn";
    const offsetTicks = Number(mode.price_limit_offset_ticks || 0);
    const bracketPolicy = offsetTicks > 0
      ? `TP／SL 漲跌停內縮 ${sourceNumber(offsetTicks)} Tick（提高成交機率，非保證）`
      : "TP／SL 使用完整漲跌停價";
    const configuredEntryPolicy = mode.configured_entry_fill_policy || mode.entry_fill_policy;
    const configuredEntryOffset = Number(mode.configured_entry_price_offset_ticks || 0);
    const activeEntryPolicy = configuredEntryPolicy === "official_open_signal_0900_execute_0901_vwap"
      ? "目前規則：官方 09:00 開盤價供推論／張數，右標記 09:01 首分鐘成交 VWAP 供回補執行；任一缺價即阻擋"
      : configuredEntryPolicy === "official_open_at_09_01"
      ? "目前規則：09:01 以官方開盤價計算全部紙上買賣；缺價即阻擋，不使用 +1 Tick"
      : configuredEntryPolicy === "synthetic_open_tick"
      ? `目前規則：開盤價不利 ${number(configuredEntryOffset || 1)} Tick 合成成交`
      : configuredEntryPolicy === "market_at_best_quote_else_adverse_open_tick"
      ? `目前規則：紙上市價買進／回補取最佳 Ask、賣出／放空取最佳 Bid，完整模擬委託；缺報價才用開盤價不利 ${number(configuredEntryOffset || 1)} Tick`
      : "目前規則：09:00 訊號原子發布後，市價買進／回補取第一筆較晚最佳 Ask；市價賣出／放空取第一筆較晚最佳 Bid，且只吃可驗證一檔量；若錯過開盤，另以 09:00 官方開盤推論、09:01 首分鐘 VWAP 執行";
    const recordedEntryPolicy = mode.entry_fill_policy === "official_open_signal_0900_execute_0901_vwap"
      ? `所選交易日紀錄：09:00 官方開盤推論／張數，09:01 首分鐘 VWAP 執行 ${number(mode.entry_0901_vwap_fill_count || mode.entry_fill_count || 0)} 筆（反事實紙上估值，非交易所成交）`
      : mode.entry_fill_policy === "official_open_at_09_01"
      ? `所選交易日紀錄：09:01 官方開盤價計價 ${number(mode.entry_official_open_fill_count || mode.entry_fill_count || 0)} 筆（反事實紙上估值，非交易所成交）`
      : mode.entry_fill_policy === "synthetic_open_tick"
      ? `所選交易日紀錄：開盤價不利 ${number(mode.entry_price_offset_ticks || 1)} Tick 合成成交（不回寫成最佳報價）`
      : mode.entry_fill_policy === "causal_best_quote_else_adverse_open_tick"
      ? `所選交易日紀錄：歷史最佳 Bid／Ask ${number(mode.entry_best_quote_fill_count || 0)} 筆；缺報價才用開盤價不利 ${number(mode.entry_price_offset_ticks || 1)} Tick ${number(mode.entry_synthetic_fallback_fill_count || 0)} 筆`
      : mode.entry_fill_policy === "market_at_best_quote_else_adverse_open_tick"
      ? `所選交易日紀錄：紙上市價完整成交 ${number(mode.entry_paper_market_fill_count || mode.entry_fill_count || 0)} 筆；不宣稱交易所深度或排隊成交`
      : "所選交易日紀錄：因果最佳 Bid／Ask 與可驗證一檔量";
    const entryPolicy = configuredEntryPolicy === mode.entry_fill_policy
      ? activeEntryPolicy
      : `${activeEntryPolicy}；${recordedEntryPolicy}`;
    const reasonCounts = Object.entries(mode.signal_reason_counts || {})
      .filter(([, count]) => Number(count || 0) > 0)
      .sort((left, right) => Number(right[1]) - Number(left[1]))
      .slice(0, 4)
      .map(([reason, count]) => `${reason.replaceAll("_", " ")} ${number(count)}`)
      .join("、") || "無";
    return `<article class="panel mode-card">
      <header><h3>${esc(mode.label || mode.market)}</h3>${badge(engineStatusShortLabel(status), kind)}</header>
      <div class="equity ${pnlClass(returnPct)}">${returnPct == null ? "尚無估值" : `${returnPct >= 0 ? "+" : ""}${displayPct(returnPct)}`}</div>
      <div class="delta ${pnlClass(pnl)}">${pnl == null ? "篩選區間尚無估值" : `期末權益 ${summaryMoney(equity)} · 區間淨損益 ${pnl >= 0 ? "+" : ""}${summaryMoney(pnl)}`}</div>
      <div class="mode-glance">
        <div><span>該日策略執行</span><strong class="${esc(execution.kind)}">${esc(execution.label)}</strong></div>
        <div><span>實際成交結果</span><strong class="${esc(fillOutcome.kind)}">${esc(fillOutcome.label)}</strong></div>
        <div><span>持倉／缺價</span><strong>${number(mode.open_position_count)} / ${number(mode.stale_position_count)}</strong></div>
        <div><span>已實現淨損益</span><strong class="${pnlClass(mode.cumulative_realized_net_pnl_twd)}">${summaryMoney(mode.cumulative_realized_net_pnl_twd)}</strong></div>
        <div><span>未實現淨清算損益</span><strong class="${pnlClass(mode.open_net_liquidation_pnl_twd)}">${summaryMoney(mode.open_net_liquidation_pnl_twd)}</strong></div>
      </div>
      <details><summary>查看資金、訊號與曝險細節</summary><div class="metrics">
        <div><span>篩選區間報酬基準</span><strong>${money(initial)}</strong></div>
        <div><span>已賺手續費退佣</span><strong>${money(mode.cumulative_commission_rebate_accrued_twd)}</strong></div>
        <div><span>訊號時間</span><strong>${shortTime(mode.signal_at)}</strong></div>
        <div><span>要求／成交／未成交</span><strong>${number(mode.entry_requested_shares || 0)}／${number(mode.entry_filled_shares || 0)}／${number(mode.entry_unfilled_shares || 0)} 股</strong></div>
        <div><span>13:24 市價重試後殘餘</span><strong class="${Number(mode.force_exit_failures || 0) ? "negative" : ""}">${number(mode.force_exit_failures || 0)}</strong></div>
        <div><span>13:30 帳務強平</span><strong>${number(mode.terminal_flatten_count || 0)}</strong></div>
        <div><span>強平價替代值</span><strong class="${Number(mode.terminal_flatten_degraded_count || 0) ? "negative" : ""}">${number(mode.terminal_flatten_degraded_count || 0)}</strong></div>
        ${mode.counterfactual_open_replay ? `<div class="wide"><span>開盤價重建</span><strong>實際開盤 ${shortTime(mode.signal_at)} · 原始訊號 ${shortTime(mode.source_signal_at)} · 非即時成交</strong></div>` : ""}
        <div class="wide"><span>進場成交契約</span><strong>${esc(entryPolicy)}</strong></div>
        <div class="wide"><span>訊號結果原因</span><strong>${esc(reasonCounts)}</strong></div>
        <div class="wide"><span>停利停損價位</span><strong>${esc(bracketPolicy)}</strong></div>
      </div></details>
    </article>`;
  }).join("");
}

function renderBenchmarks(data) {
  const rows = Array.isArray(data.benchmarks) ? data.benchmarks : [];
  $("benchmark-cards").innerHTML = rows.map((row) => {
    const rangeSummary = rangeSummaryFor(row.benchmark_id);
    const returnPct = rangeSummary?.return_pct == null ? null : Number(rangeSummary.return_pct);
    const equity = rangeSummary?.end_equity_twd == null ? null : Number(rangeSummary.end_equity_twd);
    const netPnl = rangeSummary?.range_net_pnl_twd == null ? null : Number(rangeSummary.range_net_pnl_twd);
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
      <div class="delta ${pnlClass(netPnl)}">${equity == null ? "篩選區間等待完整來源" : `期末權益 ${summaryMoney(equity)} · 區間淨損益 ${netPnl >= 0 ? "+" : ""}${summaryMoney(netPnl)}`}</div>
      <div class="benchmark-facts">
        <div><span>持有標的</span><strong class="benchmark-contract">${esc(holding)}</strong></div>
        <div><span>篩選區間報酬基準</span><strong>${money(rangeSummary?.baseline_equity_twd)}</strong></div>
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
  const simulationWarm = warm.simulation || {};
  const simulationComponents = simulationWarm.components || {};
  const simulationQuoteWarm = simulationComponents.shioaji_quote || {};
  const simulationEligibilityWarm = simulationComponents.eligibility || {};
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
  const serviceSync = data.service_sync || {};
  const discordSync = serviceSync.discord || {};
  const syncKind = serviceSync.synchronized ? "good" : serviceSync.status === "catching_up" ? "warn" : "bad";
  const syncLabel = serviceSync.synchronized
    ? `rev ${number(serviceSync.state_revision)} 已同步`
    : serviceSync.status === "catching_up"
      ? `追趕中 · 差 ${number(serviceSync.revision_lag)} 版`
      : serviceSync.status === "discord_connecting"
        ? "Discord 連線中"
      : serviceSync.status === "engine_committed"
        ? `rev ${number(serviceSync.state_revision)} 已提交`
        : "Discord 狀態逾時";
  const syncNote = `engine ${duration(serviceSync.engine_age_seconds)}前 · Discord ${duration(discordSync.age_seconds)}前 · 版本探測每 ${number(SERVICE_REVISION_REFRESH_MS / 1000)} 秒`;
  const guardian = data.unattended_guardian || {};
  const guardianComponents = guardian.components || {};
  const guardianKind = guardian.status === "repairing"
    ? "warn"
    : guardian.ready
      ? "good"
      : "bad";
  const guardianLabel = guardian.status === "repairing"
    ? "自我修復中"
    : guardian.ready
      ? "長期守護 READY"
      : `長期守護 ${String(guardian.status || "MISSING").toUpperCase()}`;
  const guardianNote = `時間 ${guardianComponents.time_sync ? "OK" : "FAIL"} · 156來源 ${guardianComponents.source_events ? "OK" : "FAIL"} · 服務同步 ${guardianComponents.runtime_sync ? "OK" : "FAIL"} · 磁碟 ${guardianComponents.disk ? "OK" : "FAIL"} · ${duration(guardian.age_seconds)}前`;
  $("latency-kpis").innerHTML = [
    ["最新輸入→落盤", noLatency ? latencyEmptyLabel : latencyValue(latency.latest_ms), noLatency ? "不以舊日或估計值冒充今日速度" : `${esc(latency.latest_market || "—")} · ${shortTime(latency.latest_recorded_at)}`],
    ["P50", latencyValue(latency.p50_ms), `${number(latency.sample_count || 0)} 個成功模式樣本`],
    ["P95", latencyValue(latency.p95_ms), "訊號開始至模擬帳本 fsync 前後的牆鐘邊界"],
    ["最慢", latencyValue(latency.max_ms), "所選交易日成功樣本最大值"],
    ["最新瓶頸", noLatency ? "—" : latestBottleneck, noLatency ? "等待實測" : latencyValue(latency.latest_bottleneck_ms)],
  ].map(([label, value, note]) => `<div class="latency-kpi"><span>${esc(label)}</span><strong>${esc(value)}</strong><small>${esc(note)}</small></div>`).join("");
  $("operation-kpis").innerHTML = [
    ["所選日流程／成交", `${number(execution.executed_count || 0)} 已處理 · ${number(execution.filled_mode_count || 0)} 完整成交`, `${number(execution.partial_mode_count || 0)} 部分、${number(execution.zero_fill_mode_count || 0)} 零成交、${number(execution.no_order_mode_count || 0)} 無合法委託、${number(execution.failed_mode_count || 0)} 阻擋`, execution.all_modes_filled ? "good" : "bad"],
    ["盤前預熱", `${readyModes}/${totalModes || 0} 模型 READY`, `模擬執行 ${simulationWarm.status || "pending"}`, warmKind],
    ["目前階段", session.label || "—", `下一步 ${session.next_milestone_label || "—"} · ${countdown(session.next_milestone_at)}`, phaseKind],
    ["帳本心跳", `${sourceNumber(data.source_age_seconds)} 秒`, `目標每 ${number(session.decision_interval_seconds || 60)} 秒`, heartbeatKind],
    ["面板 API", lastFetchMs == null ? "—" : `${number(lastFetchMs, 1)} ms`, `行情與權益每 ${number(PRICE_REFRESH_MS / 1000)} 秒刷新`, lastFetchMs != null && lastFetchMs > 1000 ? "warn" : "good"],
    ["服務同步", syncLabel, syncNote, syncKind],
    ["無人維護守護", guardianLabel, guardianNote, guardianKind],
  ].map(([label, value, note, kind]) => `<div class="operation-kpi"><span>${esc(label)}</span><strong>${esc(value)}</strong><small class="${esc(kind)}">${esc(note)}</small></div>`).join("");

  const workflowRows = [
    {label:"啟用模式預熱", value:warmRatio, count:`${number(warm.completed_count || 0)} / ${number(totalModes)} · ${number(warmRatio * 100, 1)}%`, note:`牆鐘 ${duration(warm.wall_elapsed_seconds)} · ${warm.modes_per_minute == null ? "—" : `${sourceNumber(warm.modes_per_minute)} 模式/分`}`, kind:warmKind},
    {label:"所選日策略執行", value:session.signal_progress_ratio, count:`${number(session.signal_completed_modes || 0)} / ${number(session.mode_count || 0)}`, note:"原子指標由 inotify 事件即時喚醒；0.1 秒只作備援，阻擋不算完成", kind:"good"},
    {label:"進場處理終態", value:session.entry_progress_ratio, count:`${number(session.entry_completed_modes || 0)} / ${number(session.mode_count || 0)}`, note:"完成後可能有成交或依真實限制保持空倉", kind:"good"},
    {label:"每分鐘權益紀錄", value:session.mark_progress_ratio, count:`${number(session.observed_mode_minutes || 0)} / ${number(session.expected_mode_minutes || 0)}`, note:session.mark_tracking_complete ? "全部模式已平倉，估值追蹤完成" : `目前 ${sourceNumber(session.mark_rows_per_minute || 0)} 模式紀錄/分`, kind:""},
    {label:"13:20/13:24/13:25 退出", value:session.exit_progress_ratio, count:`${number(session.exit_started_modes || 0)} / ${number(session.mode_count || 0)}`, note:"先限價、再市價重試；殘餘於 13:25 以漲跌停價參與收盤集合競價", kind:"warn"},
  ];
  $("workflow-progress").innerHTML = workflowRows.map((row) => `<div class="progress-row">
    <div class="progress-title"><strong>${esc(row.label)}</strong><span>${esc(row.count)}</span></div>
    ${progress(row.value, row.kind)}
    <small>${esc(row.note)}</small>
  </div>`).join("");

  const preopenRows = warm.markets || [];
  const modelPreopenHtml = preopenRows.map((row) => {
    const status = String(row.status || "pending");
    const kind = status === "ready" ? "good" : status === "failed" ? "bad" : "warn";
    const statusLabel = status === "recovered_late" ? "延遲恢復" : status.toUpperCase();
    const stepText = status === "recovered_late"
      ? statusLabel
      : row.step && row.total ? `${number(row.step)}/${number(row.total)} · ${number(row.progress_ratio * 100, 1)}%` : statusLabel;
    const stepRate = row.step && row.elapsed_seconds ? Number(row.step) / Number(row.elapsed_seconds) : null;
    const eta = row.step && row.total && row.step < row.total && stepRate ? (Number(row.total) - Number(row.step)) / stepRate : null;
    const speed = row.symbols_per_second == null ? "—" : `${sourceNumber(row.symbols_per_second)} 股票/秒`;
    const inference = row.model_inference_ms == null ? "—" : `${duration(Number(row.model_inference_ms) / 1000)} 模型`;
    const limits = row.price_limit_requested ? `${number(row.price_limit_prepared)}/${number(row.price_limit_requested)} 漲跌停` : "漲跌停待準備";
    const eligibility = row.eligibility_ready ? `${row.eligibility_target_date || data.session_date || "所選日"} TWSE/TPEx 資格 READY` : "所選日資格待確認";
    const quoteRequested = Number(row.final_arm_quote_requested || 0);
    const quotePrimed = quoteRequested > 0
      && row.final_arm_quote_connection_scope === "process"
      && Number(row.final_arm_quote_primed || 0) === quoteRequested
      && Number(row.final_arm_quote_resolved || 0) === quoteRequested
      && Number(row.final_arm_quote_missing || 0) === 0;
    const armed = row.final_arm_contract_ready === true
      && (row.final_arm_current_process_required !== true || row.final_arm_hot_ready === true)
      && row.final_arm_panel_cache_hit === true
      && row.final_arm_checkpoint_cache_hit === true
      && row.final_arm_model_cache_hit === true
      && row.final_arm_quote_ready === true
      && quotePrimed;
    const quoteArm = row.final_arm_quote_ready === true
      ? `Shioaji 契約快取 ${number(row.final_arm_quote_primed)}/${number(row.final_arm_quote_requested)}（可解析 ${number(row.final_arm_quote_resolved)}）`
      : "Discord Shioaji 連線／契約待預熱";
    const armText = armed
      ? `09:00 HOT READY ${shortTime(row.final_arm_completed_at)} · ${duration(row.final_arm_elapsed_seconds)} · ${number(row.final_arm_attempts || 1)} 次驗證 · ${quoteArm}`
      : row.final_arm_public_error_message || row.final_arm_error || `08:45 起最後武裝待驗證 · ${quoteArm}`;
    const measuredDetail = `${duration(row.elapsed_seconds)} · ${speed} · ${inference} · ${limits} · ${eligibility} · ${armText}${eta == null ? "" : ` · ETA ${duration(eta)}`}`;
    const detail = row.public_error_message || row.error || (status === "running" && row.message) || measuredDetail;
    return `<div class="progress-row">
      <div class="progress-title"><strong>${esc(row.label || row.market)}</strong>${badge(stepText, kind)}</div>
      ${progress(row.progress_ratio, kind)}
      <small>${esc(detail)}</small>
    </div>`;
  }).join("") || `<div class="empty-inline">尚無所選日模型預熱紀錄</div>`;
  const simulationKind = simulationWarm.status === "ready" ? "good" : simulationWarm.status === "failed" ? "bad" : "warn";
  const simulationDetail = [
    `當日資格 ${simulationEligibilityWarm.status || "pending"}`,
    `Shioaji usage 探測 ${simulationQuoteWarm.status || "pending"}`,
    simulationQuoteWarm.checked_at ? shortTime(simulationQuoteWarm.checked_at) : "尚未驗證",
    simulationQuoteWarm.public_error_message || simulationEligibilityWarm.public_error_message || simulationQuoteWarm.error || simulationEligibilityWarm.error || "執行器與模型為獨立連線，兩者都必須 READY",
  ].join(" · ");
  const simulationHtml = `<div class="progress-row">
    <div class="progress-title"><strong>模擬執行器盤前守門</strong>${badge(String(simulationWarm.status || "pending").toUpperCase(), simulationKind)}</div>
    ${progress(simulationWarm.ready ? 1 : 0, simulationKind)}
    <small>${esc(simulationDetail)}</small>
  </div>`;
  $("preopen-progress").innerHTML = modelPreopenHtml + simulationHtml;
  $("operation-source").textContent = warm.updated_at ? `預熱狀態 ${shortTime(warm.updated_at)}` : "預熱狀態尚未建立";
}

function renderTwPublicMonitor(payload) {
  const root = $("tw-public-monitor-list");
  const summary = $("tw-public-monitor-summary");
  const fetchState = $("tw-public-monitor-fetch-state");
  if (!root || !summary) return;

  const rows = twPublicRowsSource(payload)
    .filter((row) => row?.parent_id === "group:tw-public");
  if (!rows.length) {
    root.innerHTML = `<div class="tw-public-empty">尚未取到台股官方公開資料來源明細。請確認 /data-monitor 是否有載入完成。</div>`;
    summary.textContent = "0 / 0 來源可顯示";
    if (fetchState) fetchState.textContent = "無法顯示來源；請稍後重試";
    return;
  }

  const prepared = rows
    .map((row) => {
      const state = twPublicProgress(row);
      const publication = twPublicPublicationTime(row);
      const automation = row.automation || {};
      const nextRun = row.publication
        ? (row.publication.next_check_at_utc || row.publication.next_acquisition_at_utc)
        : null;
      return {
        row,
        state,
        publication,
        automation,
        coverageText: twPublicCoverageText(row.coverage),
        availabilityText: twPublicAvailabilityText(row.availability),
        rowValueText: twPublicRowsValueText(row),
        nextRun,
        schedule: String((row.publication && row.publication.schedule_label) || "—"),
      };
    })
    .sort((left, right) => {
      const leftStatus = String(left.row.status || "");
      const rightStatus = String(right.row.status || "");
      const statusWeight = {current: 0, complete: 0, streaming: 1, stale: 2, waiting: 3, degraded: 4, updating: 5, unavailable: 6, blocked: 7};
      const weightLeft = statusWeight[leftStatus] ?? 9;
      const weightRight = statusWeight[rightStatus] ?? 9;
      if (weightLeft !== weightRight) return weightLeft - weightRight;
      const leftTitle = String(left.row.title || "");
      const rightTitle = String(right.row.title || "");
      return leftTitle.localeCompare(rightTitle, "zh-Hant");
    });

  const completed = prepared.filter((item) => item.state.complete).length;
  summary.textContent = `${number(completed)} / ${number(prepared.length)} 來源完成（含預備下一日）`;
  if (fetchState) {
    const generated = payload.generated_at_utc ? shortDateTime(payload.generated_at_utc) : "—";
    const fetchText = twPublicMonitorLastFetchMs == null
      ? "—"
      : `${number(twPublicMonitorLastFetchMs, 1)} ms`;
    fetchState.textContent = `資料鏡像 ${generated} · 輪詢 ${fetchText}`;
  }

  root.innerHTML = prepared.map((item) => {
    const row = item.row;
    const state = item.state;
    const ratio = state.ratio;
    const warnings = Array.isArray(row.warnings) ? row.warnings.filter(Boolean).map((line) => String(line)).filter(Boolean) : [];
    const warningText = warnings.length ? `注意：${warnings.join("；")}` : "無警報";
    const detailText = [row.detail || "—", row.category ? `類別：${row.category}` : "", row.scope ? `範疇：${row.scope}` : ""].filter(Boolean).join(" · ");
    const progressHtml = ratio == null
      ? `
        <div class="tw-public-progress-meta">取得進度：分母未提供</div>
        <progress class="progress-track ${esc(state.kind)}" max="100" value="0" aria-label="無法計算分母"></progress>
        <small>${esc(state.label)}</small>`
      : `
        <div class="tw-public-progress-meta">${esc(twPublicCompletionHint(state))}</div>
        ${progress(ratio, state.kind)}
        <small>${(ratio * 100).toFixed(ratio < 0.1 ? 2 : 1)}% · ${number(state.current)} / ${number(state.total)} ${esc(state.unit)}</small>`;
    const freshness = row.freshness && Number.isFinite(Number(row.freshness.age_seconds))
      ? `${number(Number(row.freshness.age_seconds))} 秒前更新`
      : "時效不明";
    const completionBadge = state.complete ? badge("完成", "good") : badge("進行中", "warn");
    const publicationTime = item.publication.label;
    const publicationBasis = item.publication.basis;
    const nextText = item.nextRun
      ? `下次檢查 ${shortDateTime(item.nextRun)}`
      : "下次檢查待定";
    return `<article class="tw-public-source-card">
      <header>
        <div>
          <h3>${esc(row.title || row.id)}</h3>
          <small>${esc(row.provider || "—")} · ${esc(row.cadence || "—")} · ${esc(item.schedule)}</small>
        </div>
        ${completionBadge}
      </header>
      <div class="tw-public-source-status">${esc(row.status_label || row.status || "未知")}</div>
        <div class="tw-public-meta">更新時間：${esc(publicationTime)}（${state.firstDataObserved ? "有首筆" : "未見首筆"}）${item.publication.exact ? " · 官方有明確時點" : " · 時點待測"}</div>
        <div class="tw-public-meta">官方觀測：${esc(item.publication.observedLabel)} / 套用：${esc(item.publication.value ? shortDateTime(item.publication.value) : "—")} / last_checked：${esc(item.publication.checkedLabel)}</div>
        <div class="tw-public-meta">證據依據：${esc(publicationBasis || "—")}；稽核：${esc(item.coverageText)}；資料筆數：${esc(item.rowValueText)}；可用：${esc(item.availabilityText)}</div>
        <div class="tw-public-meta">排程：${esc(twPublicAutomationText(item.automation, row.cadence))}；${esc(nextText)}；${esc(twPublicExecutionLabel(row, {state: row.execution_state, operation: row.operation_state}))}</div>
        <div class="tw-public-meta">下一資料日：${esc(state.preparingForDate || state.dataThrough || "—")} · ${esc(state.basis || "—")}</div>
        <div class="tw-public-meta">來源描述：${esc(detailText)}；${esc(warningText)}</div>
        <div class="tw-public-progress">${progressHtml}</div>
        <small class="tw-public-footer">freshness：${esc(freshness)}；最後官方/系統檢核：${esc(item.row.last_verified_at_utc || row.latest_at_utc || "—")}</small>
        <small class="tw-public-footer">資料截止：${esc(item.row.data_through || "—")}；最新快照：${shortTime(row.latest_at_utc)}</small>
    </article>`;
  }).join("");
}

async function loadTwPublicMonitorWithFallback(controller) {
  const failures = [];
  for (const candidate of DATA_MONITOR_STATUS_PATHS) {
    try {
      const response = await fetchWithTimeout(candidate, {
        cache: "no-store",
        signal: controller.signal,
      });
      if (!response.ok) {
        failures.push(`${candidate}: HTTP ${response.status}`);
        continue;
      }
      const payload = await response.json();
      if (payload == null || typeof payload !== "object") {
        failures.push(`${candidate}: 回傳資料異常`);
        continue;
      }
      return payload;
    } catch (error) {
      if (error?.name === "AbortError") throw error;
      failures.push(`${candidate}: ${error}`);
      continue;
    }
  }
  throw new Error(`資料監控端點全部失敗（${failures.join("；")}）`);
}

async function loadTwPublicMonitor() {
  if (document.hidden) return;
  if (twPublicMonitorRefreshInFlight) return;
  twPublicMonitorRefreshInFlight = true;
  if (twPublicMonitorAbortController) twPublicMonitorAbortController.abort();
  const controller = new AbortController();
  twPublicMonitorAbortController = controller;
  const started = performance.now();
  try {
    const payload = await loadTwPublicMonitorWithFallback(controller);
    twPublicMonitorData = payload;
    twPublicMonitorLastFetchMs = performance.now() - started;
    twPublicMonitorLastUpdated = payload.generated_at_utc || null;
    renderTwPublicMonitor(payload);
  } catch (error) {
    if (error?.name === "AbortError") return;
    const root = $("tw-public-monitor-list");
    if (root) root.innerHTML = `<div class="tw-public-empty">台股公開資料取得狀態讀取失敗：${esc(error)}</div>`;
    const summary = $("tw-public-monitor-summary");
    if (summary) summary.textContent = "讀取失敗";
  } finally {
    if (twPublicMonitorAbortController === controller) twPublicMonitorAbortController = null;
    twPublicMonitorRefreshInFlight = false;
  }
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
    range: "all",
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
    for (const row of item.valid.filter((value) => value.valuation_stale && (!value.historical_minute_replay || Number(value.missing_price_position_count || 0) > 0))) html += `<circle class="stale-dot" cx="${x(row.minute)}" cy="${y(row.return_pct)}" r="3"></circle>`;
  });
  svg.innerHTML = html;
  const start = new Date(times[0]).toLocaleString("zh-TW", {timeZone:"Asia/Taipei", hour12:false});
  const end = new Date(times.at(-1)).toLocaleString("zh-TW", {timeZone:"Asia/Taipei", hour12:false});
  const sampled = chartHistory?.downsampled ? `；已保留端點與區間極值縮圖（原 ${number(chartHistory.raw_points_in_range)} 點）` : "";
  const replayPoints = Number(chartHistory?.historical_minute_replay_points || 0);
  const replayMean = Number(chartHistory?.historical_minute_mean_fresh_trade_notional_coverage_ratio);
  const replayMissing = Number(chartHistory?.historical_minute_missing_price_points || 0);
  const replayQuality = replayPoints
    ? `；歷史分鐘 ${number(replayPoints)} 點，平均新成交名目覆蓋 ${Number.isFinite(replayMean) ? `${sourceNumber(replayMean * 100)}%` : "—"}，缺價 ${number(replayMissing)} 點（其餘無成交分鐘延用上一筆）`
    : "";
  $("equity-range-note").textContent = `${chartWindowLabel()} · 一分鐘曲線 · 報酬基準為起始日前最後一筆權益 · ${start} ～ ${end} · 顯示 ${number(points.length)} 點、${number(visibleSeries.length)}/${number(series.length)} 條線；全體無資料的時間已壓縮${sampled}${replayQuality}`;
}

function applyChartHistory(payload) {
  chartHistory = payload;
  if (snapshot) renderChart(snapshot);
}

async function loadChartHistory({preferCache = false} = {}) {
  if (document.hidden) return;
  const requestedRange = "all";
  const requestedStart = selectedDetailStartDate();
  const requestedEnd = selectedDetailEndDate();
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
  $("position-body").setAttribute("aria-busy", String(positionLoading));
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
  syncFeaturePanelSelection();
  $("signal-body").setAttribute("aria-busy", String(signalLoading));
  $("signal-count").textContent = signalLoadError
    ? `${signalRows.length} / ${signalTotal} 筆 · 等待重試`
    : `${signalRows.length} / ${signalTotal} 筆`;
  const loadMore = $("load-more-signals");
  loadMore.classList.toggle("hidden", !signalHasMore);
  loadMore.disabled = signalLoading;
  loadMore.textContent = signalLoading ? "載入中…" : "載入更多";
  const target = signalDirectionSummary.target || {};
  const actual = signalDirectionSummary.actual || {};
  const positionMap = new Map(positionRows.map((row) => [`${row.session_date}\u0000${row.market}\u0000${row.symbol}`, row]));
  const modeMap = new Map((snapshot?.modes || []).map((mode) => [mode.market, mode]));
  const directionHtml = [
    ["區間訊號目標", target],
    ["資格／整張／深度後實際成交", actual],
  ].map(([label, row]) => `<div><span>${esc(label)}</span><strong>${esc(directionPair(row))}</strong></div>`).join("");
  const openingAuditHtml = Object.entries(signalOpeningExecutionAudit).map(([market, row]) => {
    const missing = Number(row.opening_price_missing_count || 0);
    const reasons = Object.entries(row.unfilled_reason_counts || {}).slice(0, 3)
      .map(([reason, count]) => `${reason}:${number(count)}`).join(" · ");
    return `<div><span>${esc(market)} 開盤市價稽核</span><strong class="${missing ? "negative" : "positive"}">${number(row.opening_price_covered_count)}/${number(row.nonzero_signal_count)} 有開盤價 · 缺 ${number(missing)} · 成交 ${number(row.filled_signal_count)}</strong><small>${esc(reasons || "全部非零訊號皆已成交")}</small></div>`;
  }).join("");
  $("signal-direction-summary").innerHTML = directionHtml + openingAuditHtml;
  const errorRow = signalLoadError
    ? `<tr class="signal-load-error"><td colspan="6">${esc(signalLoadError)}</td></tr>`
    : "";
  const rowHtml = signalRows.map((row) => {
    const position = positionMap.get(`${row.session_date}\u0000${row.market}\u0000${row.symbol}`);
    const mode = modeMap.get(row.market);
    const positionPnl = position ? resolvedPositionPnl(position) : null;
    const hasFill = Number(row.filled_shares || 0) > 0;
    const hasPosition = Boolean(position && Number(position.filled_shares || 0) > 0);
    const isOpen = hasPosition && positionPnl.signedShares !== 0;
    const openingPrice = row.sizing_open_price ?? (row.counterfactual_open_replay ? row.execution_price : null);
    const executionPrice = row.execution_price ?? position?.entry_price;
    const hasTarget = Math.abs(Number(row.target_weight || 0)) > 0;
    const entryNotional = hasFill ? Math.abs(Number(row.filled_shares || 0)) * Number(executionPrice || 0) : null;
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
    const result = badge(row.status, row.status === "ready" ? "good" : row.status === "partial_depth" ? "warn" : row.status === "hold" ? "" : "bad");
    const selected = signalRowKey(row) === featurePanelSignalKey ? " is-selected" : "";
    return `<tr data-signal-key="${esc(signalRowKey(row))}" class="signal-row${selected}">
      <td><strong>${esc(row.market)}</strong><small>${esc(row.session_date)} · ${shortTime(row.signal_at)}</small></td>
      <td><strong>${esc(row.symbol)}</strong> ${badge(row.side, row.side === "long" ? "good" : row.side === "short" ? "bad" : "")}<small>${esc(row.name || "")}</small><small>${eligibility} ${result}</small></td>
	    <td><strong>${sourceNumber(row.raw_score ?? row.score)}</strong><small>持倉目標 ${pct(row.target_weight)}</small><small>${esc(row.reason || "")}</small></td>
	    <td>${hasTarget ? `<strong class="${openingPrice == null ? "negative" : ""}">開盤計價 ${money(openingPrice)}</strong>` : `<strong>零權重・不下單</strong>`}<small>${hasFill ? `市價成交 ${money(executionPrice)} · 名目 ${money(entryNotional)}` : hasTarget ? `未成交・${esc(row.reason || row.status || "受限")}` : "不需開盤計價"}</small><small>成交 ${number(row.filled_shares)}／${number(row.requested_shares)} 股 · L1 ${number(row.top_book_capacity_shares)}</small></td>
	    <td><strong>${currentLabel} ${hasPosition ? money(currentPrice) : "—"}</strong><small>${hasPosition ? shortTime(currentAt) : `訊號時 bid／ask ${sourceNumber(row.bid)}／${sourceNumber(row.ask)}`}</small><small class="${pnlClass(positionPnl?.total)}">該檔盈虧 ${hasPosition ? money(positionPnl.total) : "不計盈虧"}</small></td>
	    <td><strong class="${pnlClass(positionPnl?.total)}">佔該模式總權益 ${equityImpactPct == null ? "—" : `${equityImpactPct >= 0 ? "+" : ""}${displayPct(equityImpactPct)}`}</strong><small>模式總權益 ${Number.isFinite(modeTotalEquity) ? summaryMoney(modeTotalEquity) : "—"}</small><small>${hasPosition ? (position.valuation_stale ? badge("估值延用", "warn") : badge("估值新鮮", "good")) : "未成交不納入"}</small></td>
    </tr>`;
  }).join("");
  $("signal-body").innerHTML = errorRow + (rowHtml || `<tr><td colspan="6">目前沒有符合篩選的訊號</td></tr>`);
}

function renderSignalFeaturePanel() {
  const panel = $("signal-feature-panel");
  const body = $("signal-feature-body");
  const scope = $("signal-feature-scope");
  const empty = $("signal-feature-empty");
  if (!panel || !body || !scope || !empty) return;

  const row = selectedSignalRow();
  if (!row) {
    panel.classList.add("empty");
    body.innerHTML = "";
    scope.textContent = featureDriversSummaryText();
    empty.classList.remove("hidden");
    empty.textContent = "請點選「所有訊號」中的一列，載入該筆完整 feature 資料。";
    return;
  }

  const drivers = resolveSignalFeatureDrivers(row);
  if (!drivers.length) {
    panel.classList.add("empty");
    body.innerHTML = "";
    scope.textContent = `${featureDriversSummaryText()}（${esc(row.market)} ${esc(row.symbol)} ${esc(row.session_date)} 無可對應特徵）`;
    empty.classList.remove("hidden");
    empty.textContent = "本筆訊號尚未讀到 summary，請稍後重整。";
    return;
  }

  panel.classList.remove("empty");
  const columns = sortedFeatureColumns(drivers);
  scope.textContent = `${row.session_date} ${row.market} ${row.symbol} · 共 ${drivers.length} 個 feature，欄位 ${columns.length} 欄`;
  const featureRows = drivers
    .map((driver, index) => {
      const cells = [
        `<td>${index + 1}</td>`,
        ...columns.map((key) => `<td>${esc(formatFeatureValue(driver ? driver[key] : "—"))}</td>`),
      ];
      return `<tr>${cells.join("")}</tr>`;
    })
    .join("");
  const head = ["#", ...columns].map((column) => `<th>${esc(column === "feature" ? "Feature" : column)}</th>`).join("");
  body.innerHTML = `<div class="feature-table-scroll"><table class="compact-table feature-table">
    <thead><tr>${head}</tr></thead>
    <tbody>${featureRows}</tbody></table></div>`;
  empty.classList.add("hidden");
}

function renderEvents() {
  $("event-body").setAttribute("aria-busy", String(eventLoading));
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
    ["無人維護守護", `${data.unattended_guardian?.status || "missing"} · ${duration(data.unattended_guardian?.age_seconds)}前`],
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
    data.unattended_guardian?.observed_at_taipei,
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
  renderSignals();
  renderEvents();
  renderAudit(snapshot);
  renderSignalFeaturePanel();
}

function hydrateDefaultPositions(data) {
  const isDefaultFilter = selectedMode() === "all"
    && !textFilter()
    && $("status-filter").value === "all";
  const isSingleSnapshotDate = selectedDetailStartDate() === data.session_date
    && selectedDetailEndDate() === data.session_date;
  if (!isDefaultFilter || !isSingleSnapshotDate || !Array.isArray(data.positions)) return false;
  positionRows = data.positions;
  positionTotal = Number(data.payload_window?.positions ?? positionRows.length);
  positionHasMore = positionRows.length < positionTotal;
  positionLoadError = "";
  return true;
}

async function loadSignals({append = false, force = false} = {}) {
  if (!snapshot) return;
  if (signalAbortController) signalAbortController.abort();
  signalAbortController = new AbortController();
  const controller = signalAbortController;
  const sequence = ++signalRequestSequence;
  const requestRange = detailRangeKey();
  if (force) {
    signalLoadError = "";
  }
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
  beginSilentTableUpdate("signal-body", "load-more-signals", append);
  try {
    const response = await fetchWithTimeout(`api/signals?${params.toString()}`, {cache: "no-store", signal: controller.signal});
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const page = await response.json();
    if (sequence !== signalRequestSequence) return;
    if (requestRange !== detailRangeKey()) return;
    featurePanelScopeText = page.feature_drivers_scope
      ? `訊號頁面欄位 ${String(page.feature_drivers_scope)}`
      : "訊號頁面欄位 all_feature_drivers_if_available_else_top_feature_drivers";
    signalLoadError = "";
    signalRows = append ? signalRows.concat(page.rows || []) : (page.rows || []);
    signalRows.sort(compareByAbsoluteWeight);
    signalTotal = Number(page.total || 0);
    signalHasMore = Boolean(page.has_more);
    signalRecordCount = Number(page.record_count || 0);
    signalDirectionSummary = page.direction_summary || {};
    signalOpeningExecutionAudit = page.opening_execution_audit || {};
    const incomingDrivers = page.feature_drivers_by_signal || {};
    signalFeatureDrivers = append
      ? {...signalFeatureDrivers, ...incomingDrivers}
      : incomingDrivers;
  } catch (error) {
    if (sequence !== signalRequestSequence) return;
    if (error?.name === "AbortError") return;
    signalLoadError = `訊號明細暫時無法更新：${error}`;
    signalRecordCount = null;
  } finally {
    if (sequence === signalRequestSequence) {
      if (signalAbortController === controller) signalAbortController = null;
      signalLoading = false;
      syncFeaturePanelSelection();
      renderSignals();
      renderSignalFeaturePanel();
    }
  }
}

async function loadPositions({append = false, force = false} = {}) {
  if (!snapshot) return;
  if (positionAbortController) positionAbortController.abort();
  positionAbortController = new AbortController();
  const controller = positionAbortController;
  const sequence = ++positionRequestSequence;
  const requestRange = detailRangeKey();
  if (!append || force) positionLoadError = "";
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
  beginSilentTableUpdate("position-body", "load-more-positions", append);
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
      renderSignalFeaturePanel();
    }
  }
}

async function loadEvents({append = false, force = false} = {}) {
  if (!snapshot) return;
  if (eventAbortController) eventAbortController.abort();
  eventAbortController = new AbortController();
  const controller = eventAbortController;
  const sequence = ++eventRequestSequence;
  const requestRange = detailRangeKey();
  if (!append || force) eventLoadError = "";
  const params = new URLSearchParams({
    start_date: selectedDetailStartDate(),
    end_date: selectedDetailEndDate(),
    mode: selectedMode(),
    symbol: $("symbol-filter").value.trim(),
    offset: String(append ? eventRows.length : 0),
    limit: String(EVENT_PAGE_SIZE),
  });
  eventLoading = true;
  beginSilentTableUpdate("event-body", "load-more-events", append);
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

async function refresh({force = false} = {}) {
  if (document.hidden) return;
  if (refreshInFlight) {
    refreshQueued = true;
    refreshForceQueued = refreshForceQueued || force;
    return;
  }
  if (force) {
    if (signalAbortController) signalAbortController.abort();
    if (positionAbortController) positionAbortController.abort();
    if (eventAbortController) eventAbortController.abort();
  }
  refreshInFlight = true;
  try {
    const started = performance.now();
    const date = selectedDate();
    const response = await fetchWithTimeout(`api/status${date ? `?date=${encodeURIComponent(date)}` : ""}`, {cache: "no-store"});
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    snapshot = await response.json();
    lastServiceRevision = String(snapshot.service_sync?.revision_token || lastServiceRevision || "");
    lastFetchMs = performance.now() - started;
    const sourceUpdatedAt = String(snapshot.source_updated_at || "");
    const sourceHasChanged = sourceUpdatedAt && sourceUpdatedAt !== lastSourceUpdatedAt;
    if (sourceHasChanged || force) {
      lastSourceUpdatedAt = sourceUpdatedAt || `${String(Date.now())}`;
      signalLoadError = "";
      positionLoadError = "";
      eventLoadError = "";
      if (force) {
        signalRecordCount = null;
        eventRecordRevision = null;
      }
    }
    syncFilters(snapshot);
    await loadChartHistory({preferCache: !force});
    const positionsHydrated = hydrateDefaultPositions(snapshot);
    const revision = revisionOf(snapshot);
    const heavy = revision !== lastRenderedRevision;
    lastRenderedRevision = revision;
    render({heavy});
    const currentSignalCount = Number((snapshot.record_counts || {}).signals || 0);
    const detailLoads = [];
    const shouldReloadSignals = force || signalRecordCount == null || currentSignalCount !== signalRecordCount;
    if (shouldReloadSignals) detailLoads.push(loadSignals({force: true}));
    const counts = snapshot.record_counts || {};
    const currentEventRevision = JSON.stringify([detailRangeKey(), Number(counts.orders || 0), Number(counts.fills || 0)]);
    const shouldReloadEvents = force || eventRecordRevision == null || currentEventRevision !== eventRecordRevision;
    if (shouldReloadEvents) detailLoads.push(loadEvents({force: true}));
    const shouldReloadPositions = force || sourceHasChanged || !positionsHydrated;
    if (shouldReloadPositions) detailLoads.push(loadPositions());
    await Promise.all(detailLoads);
  } catch (error) {
    const alert = $("alert"); alert.classList.remove("hidden"); alert.textContent = `面板讀取失敗：${error}`;
    $("health").textContent = "UNAVAILABLE"; $("health").className = "pill critical";
  } finally {
    refreshInFlight = false;
    if (!document.hidden) void loadTwPublicMonitor();
    if (refreshQueued) {
      const queuedForce = refreshForceQueued;
      refreshQueued = false;
      refreshForceQueued = false;
      void refresh({force: queuedForce});
    }
  }
}

async function refreshServiceRevision() {
  if (document.hidden || revisionRefreshInFlight) return;
  revisionRefreshInFlight = true;
  try {
    const response = await fetchWithTimeout("api/revision", {cache: "no-store"});
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const serviceSync = await response.json();
    const revision = String(serviceSync.revision_token || "");
    const changed = Boolean(lastServiceRevision && revision && revision !== lastServiceRevision);
    if (revision) lastServiceRevision = revision;
    if (snapshot) {
      snapshot.service_sync = serviceSync;
      renderOperations(snapshot);
    }
    if (changed) void refresh();
  } catch (_error) {
    // The ordinary full refresh remains the fail-safe.  Do not replace the
    // last source-backed service state with an inferred client-side status.
  } finally {
    revisionRefreshInFlight = false;
  }
}

function filtersChanged({debounceSignals = false, includeChart = false, reloadEvents = true} = {}) {
  featurePanelSignalKey = "";
  featurePanelScopeText = "";
  signalFeatureDrivers = {};
  if (includeChart && snapshot) renderChart(snapshot);
  renderSignalFeaturePanel();
  beginSilentTableUpdate("position-body", "load-more-positions", false);
  beginSilentTableUpdate("signal-body", "load-more-signals", false);
  if (reloadEvents) beginSilentTableUpdate("event-body", "load-more-events", false);
  window.clearTimeout(signalFilterTimer);
  if (debounceSignals) signalFilterTimer = window.setTimeout(() => {
    void loadPositions();
    void loadSignals();
    if (reloadEvents) void loadEvents();
  }, 80);
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
  eventRecordRevision = null;
  featurePanelScopeText = "";
  featurePanelSignalKey = "";
  beginSilentTableUpdate("position-body", "load-more-positions", false);
  beginSilentTableUpdate("signal-body", "load-more-signals", false);
  beginSilentTableUpdate("event-body", "load-more-events", false);
  if (signalAbortController) signalAbortController.abort();
  if (eventAbortController) eventAbortController.abort();
  if (positionAbortController) positionAbortController.abort();
  chartHistory = null;
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
$("signal-body").addEventListener("click", (event) => {
  const row = event.target.closest("tr[data-signal-key]");
  if (!row) return;
  const nextKey = String(row.dataset.signalKey || "");
  if (!nextKey) return;
  featurePanelSignalKey = featurePanelSignalKey === nextKey ? "" : nextKey;
  renderSignals();
  renderSignalFeaturePanel();
});
const forceRefreshButton = $("force-refresh");
if (forceRefreshButton) {
  forceRefreshButton.addEventListener("click", () => void refresh({force: true}));
}
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
Dashboard.scheduleRefresh(() => {
  void refresh();
}, {intervalMs: PRICE_REFRESH_MS});
Dashboard.scheduleRefresh(refreshServiceRevision, {intervalMs: SERVICE_REVISION_REFRESH_MS});
Dashboard.scheduleRefresh(loadTwPublicMonitor, {intervalMs: TW_PUBLIC_STATUS_REFRESH_MS});

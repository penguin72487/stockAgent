"use strict";

const IS_OVERNIGHT = window.location.pathname.startsWith("/tw-overnight/");
const PRICE_REFRESH_MS = 60000;
const SERVICE_REVISION_REFRESH_MS = 1000; // fallback only; SSE owns normal delivery
const TW_PUBLIC_STATUS_REFRESH_MS = 30000;
const Dashboard = window.StockAgentDashboard;
const Presentation = window.StockAgentTwPresentation;
const Chart = window.StockAgentTwChart;
const strategyLabel = (value) => Presentation.strategyLabel(value, snapshot?.modes || []);
const fetchWithTimeout = Dashboard.createFetch({timeoutMs: 15000});
// A full multi-month minute history is intentionally lossless and larger than
// the summary APIs; cancellation still makes newer filter selections win.
const fetchMinuteHistory = Dashboard.createFetch({timeoutMs: 60000});
const SIGNAL_PAGE_SIZE = 100;
const POSITION_PAGE_SIZE = 100;
const EVENT_PAGE_SIZE = 100;
const DATA_MONITOR_STATUS_PATHS = [
  "api/public-data-status",
  "/data-monitor/api/status",
  "../data-monitor/api/status",
];
const COLORS = ["#37d3ff", "#5ee0a0", "#a98cff", "#f5bd4f", "#ff7ac8", "#73e6d1", "#ff9f68"];
const HIDDEN_EQUITY_SERIES_STORAGE_KEY = IS_OVERNIGHT
  ? "tw-overnight-hidden-equity-series"
  : "tw-day-trade-hidden-equity-series";
const HISTORY_CLIENT_CACHE_MAX_ENTRIES = 3;
const DATE_FILTER_DEBOUNCE_MS = 180;
let snapshot = null;
let appliedStatusRevision = null;
let appliedStatusPhase = "initial";
const revisionPaintObserver = Dashboard.createRevisionPaintObserver?.({
  getCurrentRevision: () => appliedStatusRevision,
});
let chartHistory = null;
let chartHistoryKey = "";
let hiddenEquitySeries = new Set();
let chartHistoryCache = new Map();
let historyInFlight = false;
let historyRequestKeyInFlight = "";
let historyRequestScopeInFlight = "";
let historyRequestSequence = 0;
let historyAbortController = null;
let historyLoadError = "";
let lastFetchMs = null;
let refreshInFlight = false;
let refreshQueued = false;
let refreshForceQueued = false;
let refreshPushedQueued = null;
let statusRecoveryTimer = null;
let statusRecoveryAttempts = 0;
let latestServiceNotice = null;
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
let signalDataRevision = null;
let signalSelectionKey = null;
let signalLoading = false;
let signalLoadError = "";
let signalLoadNotice = "";
let signalRequestSequence = 0;
let signalRequestRevisionInFlight = "";
let signalRequestScopeInFlight = "";
let signalFeatureDrivers = {};
let featurePanelSignalKey = "";
let featurePanelScopeText = "";
let signalFilterTimer = null;
let dateFilterTimer = null;
let followLatestSession = true;
let followPerformanceRange = false;
// A stale prior-year latest snapshot must not silently select the old year.
let latestSessionInPerformanceYear = true;
let sessionRolloverTimer = null;
let sessionRolloverDeadline = "";
let sessionRolloverDue = Infinity;
let signalAbortController = null;
let signalFallbackActive = false;
let signalFallbackVisible = SIGNAL_PAGE_SIZE;
let eventRows = [];
let eventTotal = 0;
let eventOrderTotal = 0;
let eventFillTotal = 0;
let eventHasMore = false;
let eventRecordRevision = null;
let eventSelectionKey = null;
let eventLoading = false;
let eventLoadError = "";
let eventLoadNotice = "";
let eventRequestSequence = 0;
let eventRequestRevisionInFlight = "";
let eventRequestScopeInFlight = "";
let eventAbortController = null;
let eventViewActivated = false;
let positionRows = [];
let positionTotal = 0;
let positionHasMore = false;
let positionLoading = false;
let positionLoadError = "";
let positionRequestSequence = 0;
let positionDataRevision = null;
let positionSelectionKey = null;
let positionRequestRevisionInFlight = "";
let positionRequestScopeInFlight = "";
let positionAbortController = null;
let positionFallbackActive = false;
let positionFallbackVisible = POSITION_PAGE_SIZE;
const EMPTY_SOURCE_ROWS = Object.freeze([]);
let priceSourceInputs = [];
let availableDetailDates = [];
let twPublicMonitorData = null;
let twPublicMonitorRefreshInFlight = false;
let twPublicMonitorLastFetchMs = null;
let twPublicMonitorLastUpdated = null;
let twPublicMonitorAbortController = null;
let twPublicMonitorActivated = false;

try {
  const storedHiddenSeries = JSON.parse(localStorage.getItem(HIDDEN_EQUITY_SERIES_STORAGE_KEY) || "[]");
  if (Array.isArray(storedHiddenSeries)) hiddenEquitySeries = new Set(storedHiddenSeries.map(String));
} catch (_error) { /* storage can be disabled */ }

const $ = Dashboard.byId;
const setHtml = Dashboard.setTrustedHtml;
const esc = Dashboard.escapeHtml;

function installProductCopy() {
  if (!IS_OVERNIGHT) return;
  document.title = "台股隔日沖模擬";
  const eyebrow = document.querySelector(".topbar .eyebrow");
  const title = document.querySelector(".topbar h1");
  const subtitle = document.querySelector(".topbar .subtitle");
  if (eyebrow) eyebrow.textContent = "TW STOCKS · CLOSE-TO-NEXT-OPEN · PUBLIC READ-ONLY";
  if (title) title.textContent = "台股隔日沖模擬";
  if (subtitle) subtitle.textContent = "13:00 切換並等待，13:20 開始計算且在 13:30 前送出模擬單；只在次一交易日實際開盤撮合後沖銷。";
  const followButton = document.querySelector("#follow-latest-session");
  if (followButton) followButton.textContent = "跟隨最新交易日（13:00 切換）";

  const operationHead = document.querySelector(".operations-panel .panel-head h2");
  const operationNote = document.querySelector(".operations-panel .panel-head p");
  if (operationHead) operationHead.textContent = "13:00 等待、13:20 計算、收盤進場與次日開盤沖銷進度";
  if (operationNote) operationNote.textContent = "試撮只供觀察；收盤與開盤都必須有交易所時間戳的非試撮價格才記為模擬成交";
  const operationColumns = document.querySelectorAll(".operation-body h3");
  ["跨日工作流程", "各模式收盤進場證據", "次日開盤沖銷證據", "模型與行情準備"].forEach((label, index) => {
    if (operationColumns[index]) operationColumns[index].textContent = label;
  });

  const performanceIntro = document.querySelector("#performance");
  if (performanceIntro) {
    performanceIntro.querySelector("h2").textContent = "隔日沖模式";
    performanceIntro.querySelector("p").textContent = "每個模式有獨立資金、委託、持倉與損益帳；不與當沖帳本混用。";
  }
  document.querySelector("#baselines")?.classList.add("hidden");
  document.querySelector("#benchmark-cards")?.classList.add("hidden");

  const detailsIntro = document.querySelector("#details");
  if (detailsIntro) detailsIntro.querySelector("p").textContent = "日期篩選同時控制集合競價事件曲線與明細；報酬以所選期間第一個事件重設為 0%，期末權益與累積損益不重設。";
  const chartPanel = document.querySelector(".chart-panel");
  if (chartPanel) {
    chartPanel.querySelector(".panel-head h2").textContent = "收盤到次日開盤事件報酬率（%）";
    chartPanel.querySelector(".panel-head p").textContent = "歷史每個完成交易日只呈現 09:00 官方開盤沖銷與 13:30 官方收盤進場／估值兩個反事實事件；不插值成盤中分鐘，也不宣稱交易所成交。";
    const rangeNote = chartPanel.querySelector(".time-range-control span");
    if (rangeNote) rangeNote.textContent = "日期範圍跟隨上方篩選；每條線以所選期間第一個集合競價事件為 0%。";
    const scroll = chartPanel.querySelector(".chart-scroll");
    if (scroll) scroll.setAttribute("aria-label", "台股隔日沖所選期間集合競價事件報酬曲線");
    const chart = chartPanel.querySelector("#equity-chart");
    if (chart) chart.setAttribute("aria-label", "收盤與次日開盤事件期間報酬率百分比曲線");
  }

  const timeline = document.querySelector(".timeline");
  if (timeline) setHtml(timeline, `
    <li><time>13:00</time><div><strong>切換至今日隔日沖並等待</strong><span>先驗證交易日、panel、checkpoint、模型與 CUDA cache；這個階段不產生正式訊號，也不送模擬單。</span></div></li>
    <li><time>13:20</time><div><strong>開始計算並送收盤集合競價限價單</strong><span>先沿用當沖 checkpoint，以計算當下最新行情決定整張目標；完成後立即以 LMT_ROD 送出，多單掛當日漲停買進、空單掛當日跌停賣出。</span></div></li>
    <li><time>13:20–13:30</time><div><strong>收盤試撮只觀察，不成交</strong><span>13:30 前必須完成模擬委託；simtrade 與預估成交價不是實際成交，沒有非試撮且帶交易所時間戳的收盤價就保持未成交。</span></div></li>
    <li><time>13:30／13:33</time><div><strong>按實際收盤撮合價建立隔夜部位</strong><span>一般股票 13:30 撮合；觸發延緩收市者最晚依 13:33 實際撮合價。全量成交只是紙上假設，不宣稱取得交易所排隊份額。</span></div></li>
    <li><time>隔夜</time><div><strong>逐分鐘依可清算 bid／ask 估值</strong><span>使用一般現股交易成本；多單以 bid、空單以 ask 評價，缺價時清楚標示估值延用。</span></div></li>
    <li><time>次日 08:30</time><div><strong>送開盤集合競價沖銷單</strong><span>多單掛當日跌停賣出、空單掛當日漲停回補；開盤前試撮只更新觀察，不記成交。</span></div></li>
    <li><time>次日 09:00</time><div><strong>只按實際開盤撮合價全數沖銷</strong><span>必須有非試撮、同一交易日且不早於 09:00 的實際 open；缺少證據就保留部位並顯示錯誤，不製造假成交。</span></div></li>`);

  const timelineHead = document.querySelector(".timeline-panel .panel-head");
  if (timelineHead) {
    timelineHead.querySelector("h2").textContent = "隔日沖執行時鐘與資料契約";
    timelineHead.querySelector("p").textContent = "收盤進場、次日開盤沖銷；模擬結果以帳本與實際撮合證據為準。";
  }
  const sourceTemplate = document.querySelector("#overnight-source-contract");
  const sourceFooter = $("execution-source-contract");
  if (sourceFooter && sourceTemplate) sourceFooter.append(sourceTemplate.content.cloneNode(true));

  const sourceTerms = document.querySelectorAll(".source-list dt");
  ["訊號", "試撮界線", "收盤／開盤成交", "一般交易成本", "百分比比較", "隔夜放空資格", "集合競價假設", "跨日退出時程", "延遲邊界"].forEach((label, index) => {
    if (sourceTerms[index]) sourceTerms[index].textContent = label;
  });
  const tablePanels = [...document.querySelectorAll("section.table-panel")];
  const positionPanel = tablePanels.find((panel) => panel.querySelector("#position-body"));
  const signalPanel = tablePanels.find((panel) => panel.querySelector("#signal-body"));
  if (positionPanel) {
    positionPanel.querySelector(".panel-head h2").textContent = "隔夜持倉完整生命週期";
    positionPanel.querySelector(".panel-head p").textContent = "依 |訊號權重| 排序；顯示收盤進場、隔夜估值與次日開盤沖銷證據。";
    const headers = positionPanel.querySelectorAll("thead th");
    ["標的／方向", "訊號／股數", "收盤進場／估值", "次日開盤委託", "開盤沖銷", "損益拆分／估值"].forEach((label, index) => {
      if (headers[index]) headers[index].textContent = label;
    });
  }
  if (signalPanel) {
    signalPanel.querySelector(".panel-head h2").textContent = "13:20 所有即時模型訊號";
    signalPanel.querySelector(".panel-head p").textContent = "隱藏目標為 0 的訊號，非零目標但 0 股未成交的股票仍保留；當沖 checkpoint 目前只作暫時權重轉接，不代表已針對隔夜風險訓練。";
    const headers = signalPanel.querySelectorAll("thead th");
    ["時間／模式", "股票／方向／結果", "分數／持倉 %", "決策計價／收盤委託", "收盤成交／目前估值", "損益／模式總權益"].forEach((label, index) => {
      if (headers[index]) headers[index].textContent = label;
    });
  }
}

installProductCopy();
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
const resolveSignalFeatureRecord = (row = null, rowKey = "") => {
  const candidates = [];
  if (rowKey) candidates.push(rowKey);
  if (row) candidates.push(signalRowKey(row), row.signal_id);
  candidates.push(featurePanelSignalKey);
  for (const candidate of candidates) {
    if (!candidate) continue;
    const value = signalFeatureDrivers[candidate];
    if (Array.isArray(value)) return {drivers: value};
    if (value && Array.isArray(value.drivers)) return value;
  }
  return {drivers: []};
};
const resolveSignalFeatureDrivers = (row = null, rowKey = "") => resolveSignalFeatureRecord(row, rowKey).drivers;
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
const shortTime = (value) => {
  if (!value) return "—";
  const compact = String(value).replace("T", " ").slice(5, 19);
  return /^[0-9 .:+/\-Z]+$/.test(compact) ? compact : "—";
};
const replayTimingText = (row = {}, fallbackAt = null) => {
  const rebuiltAt = row.open_reconstructed_at || fallbackAt || row.signal_at;
  if (row.replay_signal_origin === "current_model_counterfactual"
      || /-counterfactual-official-open-\d{4}-\d{2}-\d{2}$/.test(String(row.signal_id || ""))) {
    return `${shortTime(rebuiltAt)} · 目前模型反事實回放（非當時訊號）`;
  }
  const originalAt = row.source_signal_at;
  // An isolated replay envelope may repeat the effective ledger clock here.
  // Do not claim that duplicate is the immutable model generation time.
  const distinctOriginalAt = originalAt && originalAt !== rebuiltAt;
  return `${shortTime(rebuiltAt)} · 開盤價重建${distinctOriginalAt ? `；原始訊號 ${shortTime(originalAt)}` : ""}`;
};
const signalTimingText = (row = {}, fallbackAt = null) => row.counterfactual_open_replay
  ? replayTimingText(row, fallbackAt)
  : shortTime(row.signal_at || fallbackAt);
const signalReasonLabel = Presentation.signalReasonLabel;
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
const badge = (text, kind = "") => `<span class="badge ${esc(kind)}">${esc(text)}</span>`;
const detailComponents = window.StockAgentTwDetailComponents.create({
  Dashboard, strategyLabel, signalRowKey, resolvedPositionPnl,
  product: IS_OVERNIGHT ? "tw_overnight" : "tw_day_trade",
  format: {number, pct, money, sourceNumber, summaryMoney, displayPct, badge,
    pnlClass, shortTime, replayTimingText, signalTimingText, signalReasonLabel},
});
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
function selectedSignalRevisionScope() { return IS_OVERNIGHT ? "all" : ($("signal-revision-filter")?.value || "latest"); }
function selectedDetailStartDate() { return $("detail-start-date").value || ""; }
function selectedDetailEndDate() { return $("detail-end-date").value || ""; }
function textFilter() { return $("symbol-filter").value.trim().toLowerCase(); }
function selectedDate() {
  if (followLatestSession && latestSessionInPerformanceYear) return "";
  if (followLatestSession) return selectedDetailEndDate();
  const start = selectedDetailStartDate();
  const boundary = selectedDetailEndDate();
  return availableDetailDates.find((value) => (!start || value >= start) && value <= boundary) || boundary;
}
function detailRangeKey() { return `${selectedDetailStartDate()}|${selectedDetailEndDate()}`; }
function detailSelectionKey(kind) {
  return JSON.stringify([
    kind,
    detailRangeKey(),
    selectedMode(),
    textFilter(),
    kind === "events" ? "all" : $("status-filter").value,
    kind === "signals" ? selectedSignalRevisionScope() : null,
  ]);
}
function detailDataRevision(kind) {
  const service = snapshot?.service_sync || {};
  const history = snapshot?.historical_replay || {};
  return JSON.stringify([
    detailSelectionKey(kind),
    service.engine_generation ?? null,
    service.content_revision ?? null,
    history.generated_at || null,
    history.end_date || null,
  ]);
}
function detailReadScopeKey(kind) {
  return JSON.stringify([detailSelectionKey(kind), snapshot?.service_sync?.engine_generation ?? null]);
}
function chartReadScopeKey() {
  return JSON.stringify([detailRangeKey(), snapshot?.service_sync?.engine_generation ?? null]);
}
function chartWindowLabel() {
  const start = selectedDetailStartDate();
  const end = selectedDetailEndDate();
  return `${start || "最早資料"} ～ ${end || "最新資料"}`;
}
function chartRequestKey() {
  const service = snapshot?.service_sync || {};
  const history = snapshot?.historical_replay || {};
  // A status/content revision can advance without a new mark. The 14-MB chart
  // must reload only when its source fingerprint or selected dates change.
  // Older gateways without history_revision retain the conservative fallback.
  const sourceRevision = service.history_revision || null;
  return JSON.stringify([
    detailRangeKey(),
    service.engine_generation ?? null,
    sourceRevision || service.revision_token || service.content_revision || null,
    sourceRevision ? null : (history.generated_at || null),
    sourceRevision ? null : (history.end_date || null),
  ]);
}
function chartHistoryMatchesSelection() {
  return Boolean(
    chartHistory
    && Array.isArray(chartHistory.history)
    && chartHistory.start_date === (selectedDetailStartDate() || null)
    && chartHistory.end_date === (selectedDetailEndDate() || null)
  );
}
function rangeSummaryFor(seriesId) {
  if (!chartHistoryMatchesSelection() || chartHistoryKey !== chartRequestKey()) return null;
  return (chartHistory.range_summary || []).find((row) => row.series_id === seriesId) || null;
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

function validateDetailPage(page, {kind, offset, limit, expectedTotal = null}) {
  const count = page?.rows?.length;
  if (!Array.isArray(page?.rows) || !Number.isSafeInteger(page.total)
    || page.total < 0 || !Number.isSafeInteger(page.returned)
    || page.returned !== count || count > limit
    || page.total < offset + count || page.offset !== offset
    || typeof page.has_more !== "boolean"
    || page.has_more !== (offset + count < page.total)
    || (page.has_more && count === 0)
    || (expectedTotal !== null && page.total !== expectedTotal)
    || page.start_date !== selectedDetailStartDate()
    || page.end_date !== selectedDetailEndDate()) {
    throw new Error(`${kind}分頁範圍或筆數不完整`);
  }
  if (kind === "事件" && (!Number.isSafeInteger(page.order_total)
    || !Number.isSafeInteger(page.fill_total)
    || page.order_total < 0 || page.fill_total < 0
    || page.order_total + page.fill_total !== page.total)) {
    throw new Error("事件分頁統計不完整");
  }
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
  if (value === "model_ready_waiting_engine") return "模型訊號已發布；等待執行器登帳，尚未確認進場";
  const labels = {
    entry_partial_retrying: "進場部分成交・持續處理剩餘委託",
    waiting_closing_auction_evidence: "等待正式收盤撮合證據（含延後撮合）",
    critical_day_trade_exit_unresolved: "當沖沖銷失敗・不得自動轉為一般隔夜持倉",
    critical_adverse_limit_exception: "不利漲跌停無對手量・例外留倉待沖銷",
    critical_prior_inventory_liquidation: "優先清理前日殘餘・暫停新增曝險",
    critical_legacy_carry_requires_review: "舊規則殘餘持倉待核對・未改寫歷史成交",
    active: "執行正常",
    ready: "已就緒",
    waiting: "等待時段",
    waiting_open: "盤前準備中・等待開盤",
    waiting_signal: "等待當日訊號",
    waiting_13_00_switch: "等待 13:00 切換",
    armed_waiting_13_20_calculation: "已切換・等待 13:20 計算",
    critical_unflattened_after_13_24: "13:24 市價重試後有殘餘，已轉 13:25 集合競價",
    blocked_missing_eligibility: "缺少當日當沖資格資料，已停止執行",
    blocked_missing_checkpoint: "缺少模型權重，已停止執行",
    blocked_mode_configuration: "模式設定驗證失敗，已停止執行",
    waiting_13_25_signal: "等待舊版 13:25 訊號",
    waiting_close_auction_match: "等待實際收盤撮合",
    carrying_to_next_open: "持有至次一交易日開盤",
    flat_after_next_open: "次日開盤已沖銷",
    flat_close_orders_expired: "缺少實際收盤成交證據，委託已失效",
    flat_no_executable_signal: "無可執行隔日沖訊號",
    blocked_readiness: "隔日沖準備失敗",
  };
  return labels[value] || String(value || "未知狀態").replaceAll("_", " ");
}

function engineStatusShortLabel(value) {
  if (value === "model_ready_waiting_engine") return "訊號已產生・待登帳";
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
  if (IS_OVERNIGHT && modes.length && chartHistory?.range_summary?.length) {
    const historical = modes.map((mode) => rangeSummaryFor(mode.market)?.cumulative_net_pnl_twd);
    if (historical.every((value) => value != null && Number.isFinite(Number(value)))) {
      return historical.reduce((sum, value) => sum + Number(value), 0);
    }
  }
  const values = modes.map((mode) => {
    if (mode.initial_capital_twd == null || mode.total_equity_twd == null) return null;
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
  const healthyModes = modes.filter((mode) => Presentation.modeOperationallyReady(
    mode, data.operational_issues || [],
  )).length;
  const openPositions = (data.positions || []).filter((row) => Number(row.signed_shares || 0) !== 0);
  const modeOpenPositions = modes.reduce((sum, mode) => sum + Number(mode.open_position_count || 0), 0);
  const modeStalePositions = modes.reduce((sum, mode) => sum + Number(mode.stale_position_count || 0), 0);
  const indicativePositions = modes.reduce((sum, mode) => sum + Number(mode.indicative_valuation_position_count || 0), 0);
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
  const hasNontradeAccounting = modes.some((mode) => mode.cumulative_nontrade_settlement_net_pnl_twd != null
    || Number(mode.nontrade_settlement_count || 0) > 0);
  const unrealizedPnl = sumModeField("open_net_liquidation_pnl_twd");
  const adjustments = modes.map(Presentation.accountAdjustments);
  const carryCost = adjustments.reduce((sum, row) => sum + row.carryCost, 0);
  const corporateNet = adjustments.reduce((sum, row) => sum + row.corporateNet, 0);
  const reconciliationDifference = [totalPnl, realizedPnl, unrealizedPnl].every((value) => value != null)
    ? totalPnl - realizedPnl - unrealizedPnl - corporateNet + carryCost
    : null;
  const reconciled = reconciliationDifference != null && Math.abs(reconciliationDifference) <= .01;
  const returns = modes
    .map((mode) => rangeSummaryFor(mode.market)?.return_pct)
    .filter((value) => value != null && Number.isFinite(Number(value)))
    .map(Number);
  const best = returns.length ? Math.max(...returns) : null;
  const worst = returns.length ? Math.min(...returns) : null;
  const historicalRangeAvailable = Boolean(
    IS_OVERNIGHT
    && chartHistoryMatchesSelection()
    && modes.length
    && modes.every((mode) => rangeSummaryFor(mode.market)),
  );
  const healthKind = healthyModes === modes.length ? "good" : healthyModes ? "warn" : "bad";
  const valuationNote = [
    stalePositions ? `${number(stalePositions)} 個估值延用` : "",
    indicativePositions ? `${number(indicativePositions)} 個參考快照估值（非成交證據）` : "",
  ].filter(Boolean).join("；");
  const positionNote = openPositionCount === 0
    ? (IS_OVERNIGHT ? "尚未由實際收盤撮合建立隔夜部位" : hasNontradeAccounting ? "帳面無剩餘部位；部分為非成交假設結算" : "目前沒有未平倉部位")
    : valuationNote || "目前估值皆有新鮮報價";
  const cards = [
    ["模式狀態", `${healthyModes}/${modes.length} 無警示`, "依所選日訊號、估值與執行紀錄判定；詳細原因見上方警示", healthKind],
    [IS_OVERNIGHT ? "目前即時隔夜持倉" : "所選日持倉", `${number(openPositionCount)} 個`, positionNote, stalePositions || indicativePositions ? "warn" : openPositionCount ? "good" : ""],
    ...(historicalRangeAvailable ? [
      ["歷史反事實總淨損益", totalPnl == null ? "—" : `${totalPnl >= 0 ? "+" : ""}${compactMoney(totalPnl)}`, "各模式期末權益減初始資金；使用官方收盤／次日開盤，並非實際成交", pnlClass(totalPnl)],
    ] : [
      [hasNontradeAccounting ? "各模式帳面已結算" : "各模式已實現", realizedPnl == null ? "—" : `${realizedPnl >= 0 ? "+" : ""}${compactMoney(realizedPnl)}`, hasNontradeAccounting ? "包含非成交假設結算與假設成本" : "已出場部分，已扣分攤後交易成本", pnlClass(realizedPnl)],
      ["各模式未實現", unrealizedPnl == null ? "—" : `${unrealizedPnl >= 0 ? "+" : ""}${compactMoney(unrealizedPnl)}`, valuationNote || "以可清算 bid／ask 並扣剩餘成本", stalePositions || indicativePositions ? "warn" : pnlClass(unrealizedPnl)],
      ...(carryCost || corporateNet ? [
        ["持有成本／企業行動", summaryMoney(corporateNet - carryCost), `持有成本 ${summaryMoney(carryCost)}；企業行動淨額 ${summaryMoney(corporateNet)}`, pnlClass(corporateNet - carryCost)],
      ] : []),
      ["各模式總淨損益", totalPnl == null ? "—" : `${totalPnl >= 0 ? "+" : ""}${compactMoney(totalPnl)}`, reconciled ? (hasNontradeAccounting ? "帳面已結算（含假設）＋未實現＋企業行動－持有成本" : "已實現＋未實現＋企業行動－持有成本，已與總權益對帳") : reconciliationDifference == null ? "等待完整損益來源" : `對帳差異 ${summaryMoney(reconciliationDifference)}`, reconciled ? pnlClass(totalPnl) : "bad"],
    ]),
    ["各模式期間報酬", best == null ? "—" : `${best >= 0 ? "+" : ""}${displayPct(best)} ～ ${worst >= 0 ? "+" : ""}${displayPct(worst)}`, `${chartWindowLabel()}；每條線以所選期間第一個${IS_OVERNIGHT ? "集合競價事件" : "有效分鐘"}為 0%`, best != null && worst < 0 ? "warn" : pnlClass(best)],
  ];
  setHtml("overview-kpis", cards.map(([label, value, note, kind]) => `<div class="overview-kpi">
    <span>${esc(label)}</span><strong class="${esc(kind)}">${esc(value)}</strong><small class="${esc(kind)}">${esc(note)}</small>
  </div>`).join(""));
  $("overview-kpis").setAttribute("aria-busy", "false");
}

function renderHeader(data) {
  renderPriceSources(data);
  const health = $("health");
  const presentation = healthPresentation(data.health);
  health.textContent = presentation.label;
  health.className = `pill ${presentation.health}`;
  $("connection-status").dataset.state = presentation.health;
  $("freshness").textContent = IS_OVERNIGHT
    ? `來源 ${duration(data.source_age_seconds)}前 · ${shortTime(data.source_updated_at)}`
    : `來源更新 ${shortDateTime(data.source_updated_at)}`;
  if (data.service_sync?.snapshot_as_of_superseded === true) {
    $("freshness").textContent += " · 已核對的當時快照，較新版本準備中";
  }
  const alert = $("alert");
  const blockers = data.modes.filter((mode) => !mode.checkpoint_ready || String(mode.engine_status || "").startsWith("critical") || String(mode.engine_status || "").startsWith("blocked"));
  const catchUps = data.modes.filter((mode) => mode.today_execution_status === "starting");
  const missed = data.modes.filter((mode) => mode.today_execution_status === "missed");
  const openingWaits = pendingOpeningAttempts(data);
  const hasBenchmarkReplay = (data.benchmarks || []).some((row) => row.counterfactual_open_replay);
  const operationalIssues = operationalIssuesForDisplay(data);
  const overnightHistory = data.historical_replay || {};
  const overnightHistoryDegraded = IS_OVERNIGHT && (
    overnightHistory.status === "ready_with_stale_unresolved_position"
    || Number(overnightHistory.blocked_close_signal_count || 0) > 0
  );
  const localPaperExecution = !IS_OVERNIGHT
    && data.execution_evidence !== "shioaji_simulation_stock_deal_v1";
  const liveStaleModes = !IS_OVERNIGHT && selectedDetailEndDate() === data.session_date && data.session_progress?.phase === "active"
    ? data.modes.filter((mode) => mode.valuation_stale && Number(mode.open_position_count || 0) > 0)
    : [];
  const liveIndicativeModes = !IS_OVERNIGHT && selectedDetailEndDate() === data.session_date && data.session_progress?.phase === "active"
    ? data.modes.filter((mode) => Number(mode.indicative_valuation_position_count || 0) > 0)
    : [];
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
  if (localPaperExecution || openingWaits.length || operationalIssues.length || hasBenchmarkReplay || overnightHistoryDegraded || liveStaleModes.length || liveIndicativeModes.length || data.health === "stale" || blockers.length || catchUps.length || missed.length || signalMissingEligibility.size || currentMissingEligibility.size) {
    const messages = [
      ...openingWaits.map(row => `${strategyLabel(row)}：${openingWaitDescription(row)}`),
      localPaperExecution ? "目前當沖成交來自本地紙上帳本，Shioaji 只提供行情；尚未接入 Shioaji 模擬委託與 StockDeal 成交回報，不可將紙上成交當作券商 API 成交。" : "",
      liveStaleModes.length ? `盤中 ${number(liveStaleModes.length)} 個當沖模式的持倉估值正在延用舊價格；分鐘曲線仍記錄時間，但不是即時可成交行情，最新損益不可當成即時值。` : "",
      liveIndicativeModes.length ? "部分持倉使用當日參考快照買賣一檔估值；快照未提供非試撮證據，不用於成交、停損或停利。各模型卡片列出參考估值筆數。" : "",
      hasBenchmarkReplay ? "舊版市場基準歷史仍含開盤起算資料；新版會計契約尚未完成原子替換，該區段暫不視為 Buy & Hold 正式結果。" : "",
      overnightHistoryDegraded ? `隔日沖歷史有 ${number(overnightHistory.unresolved_prior_position_count || 0)} 個前期未解決持倉、${number(overnightHistory.blocked_close_signal_count || 0)} 次因前批未平而未新進場；估值延用與缺價均保留，沒有補造成交。` : "",
      data.health === "stale" ? "資料來源已逾時；畫面只能當歷史紀錄，不能視為現在行情。" : "",
      currentMissingEligibility.size ? `所選交易日當沖資格未完整覆蓋，後續訊號已停止執行：${[...currentMissingEligibility.entries()].map(([venue, row]) => `${venue.toUpperCase()} 需要 ${row.target_date || data.session_date || "所選日"}，最新僅到 ${row.latest_date || "無資料"}`).join("；")}` : "",
      !currentMissingEligibility.size && signalMissingEligibility.size ? "09:00 訊號產生時資格資料尚未到齊，因此已 fail-closed；較晚補齊的資料不會回填成假成交。" : "",
      catchUps.length ? `發現所選交易日執行缺漏，已立即啟動補跑：${catchUps.map((mode) => strategyLabel(mode)).join("、")}` : "",
      missed.length ? `所選交易日進場時窗結束仍缺少執行紀錄：${missed.map((mode) => strategyLabel(mode)).join("、")}` : "",
      ...operationalIssues.map((issue) => `${issue.title || issue.code}${Number(issue.count || 1) > 1 ? `（${number(issue.count)} 筆）` : ""}：${issue.detail || "已記錄異常"}`),
      ...blockers.map((mode) => `${strategyLabel(mode)}：${engineStatusLabel(mode.engine_status)}${mode.checkpoint_ready ? "" : "；checkpoint 未就緒"}`),
    ].filter(Boolean);
    const signature = messages.join("|");
    alert.classList.remove("hidden");
    if (alert.dataset.signature !== signature) {
      const [primaryMessage, ...remainingMessages] = messages;
      const remaining = remainingMessages.length
        ? `<details><summary>查看其餘 ${number(remainingMessages.length)} 項完整說明</summary><div>${remainingMessages.map((message) => `<span>${esc(message)}</span>`).join("")}</div></details>`
        : "";
      setHtml(alert, `<strong>需要注意 · ${number(messages.length)} 項</strong><span>${esc(primaryMessage)}</span>${remaining}`);
      alert.dataset.signature = signature;
    }
  } else {
    alert.classList.add("hidden");
    alert.dataset.signature = "";
  }
}

function performanceYear(data) {
  // Use the server observation clock, in the exchange's timezone, rather than
  // the visitor's local year or an old session's year after New Year's Day.
  const timestamp = data.generated_at_utc || data.service_sync?.generated_at_utc;
  const observed = timestamp ? new Date(timestamp) : null;
  if (observed && Number.isFinite(observed.getTime())) {
    return new Intl.DateTimeFormat("en-US", {timeZone: "Asia/Taipei", year: "numeric"}).format(observed);
  }
  // Test/offline snapshots without an observation clock still have a dated
  // session; missing both clocks is not evidence of a different market year.
  if (/^\d{4}-\d{2}-\d{2}$/.test(data.session_date || "")) return data.session_date.slice(0, 4);
  return new Intl.DateTimeFormat("en-US", {timeZone: "Asia/Taipei", year: "numeric"}).format(new Date());
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
  const year = performanceYear(data);
  const yearStart = `${year}-01-01`, yearEnd = `${year}-12-31`;
  availableDetailDates = [...sourceDates].map(String).sort((left, right) => right.localeCompare(left));
  const currentYearDates = availableDetailDates.filter((day) => yearStart <= day && day <= yearEnd);
  const sessionInYear = Boolean(data.session_date && yearStart <= data.session_date && data.session_date <= yearEnd);
  const latest = sessionInYear && (!currentYearDates[0] || data.session_date > currentYearDates[0])
    ? data.session_date : (currentYearDates[0] || "");
  latestSessionInPerformanceYear = sessionInYear;
  const revision = JSON.stringify([data.modes.map((row) => [row.market, row.label]), availableDetailDates, year, data.session_date]);
  if (revision !== lastFilterRevision) {
    setHtml(mode, `<option value="all">全部模式</option>` + data.modes.map((row) => `<option value="${esc(row.market)}">${esc(strategyLabel(row))}</option>`).join(""));
    if ([...mode.options].some((option) => option.value === previousMode)) mode.value = previousMode;
    // Keep the full source-date list for deciding whether an unbounded history
    // response is safe, but restrict the selectable period to the current year.
    // When the year has no observation, request the year's first day explicitly
    // instead of falling back to last year's data or an unbounded API request.
    const maximum = latest || yearStart;
    startDate.min = yearStart; startDate.max = maximum;
    endDate.min = yearStart; endDate.max = maximum;
    const withinCoverage = (value) => Boolean(value && yearStart <= value && value <= maximum);
    const defaultDay = sessionInYear ? data.session_date : maximum;
    startDate.value = withinCoverage(previousStart) ? previousStart : defaultDay;
    endDate.value = withinCoverage(previousEnd) ? previousEnd : defaultDay;
    lastFilterRevision = revision;
  }
  if (!endDate.value) endDate.value = latest || yearStart;
  if (!startDate.value) startDate.value = endDate.value;
  // A native date input may still hold manually typed values outside min/max.
  // Clamp on every synchronization, not only when source dates change.
  const clampDay = (value) => value < yearStart ? yearStart : value > (latest || yearStart) ? (latest || yearStart) : value;
  startDate.value = clampDay(startDate.value);
  endDate.value = clampDay(endDate.value);
  if (startDate.value > endDate.value) endDate.value = startDate.value;
  if (followPerformanceRange) {
    startDate.value = yearStart;
    endDate.value = sessionInYear ? data.session_date : (latest || yearStart);
  }
  if (followLatestSession) {
    startDate.value = sessionInYear ? data.session_date : (latest || yearStart);
    endDate.value = startDate.value;
  }
  const followButton = $("follow-latest-session");
  const performanceButton = $("performance-range");
  performanceButton?.setAttribute("aria-pressed", String(followPerformanceRange));
  if (performanceButton) performanceButton.textContent = `${year} 年至今`;
  if (followButton) {
    followButton.setAttribute("aria-pressed", String(followLatestSession));
    const rollover = data.service_sync?.session_clock?.rollover_local_time || (IS_OVERNIGHT ? "13:00" : "08:30");
    followButton.textContent = followLatestSession ? `跟隨最新交易日（${rollover} 切換）` : "回到最新交易日";
  }
}

function clearDateScopedViews() {
  for (const controller of [signalAbortController, eventAbortController, positionAbortController, historyAbortController]) controller?.abort();
  // Cancellation cannot stop a response whose JSON was already decoded.
  ++signalRequestSequence; ++eventRequestSequence; ++positionRequestSequence; ++historyRequestSequence;
  signalLoading = eventLoading = positionLoading = historyInFlight = false;
  historyRequestKeyInFlight = "";
  historyRequestScopeInFlight = "";
  signalRows = []; eventRows = []; positionRows = [];
  signalDirectionSummary = {}; signalOpeningExecutionAudit = {}; signalFeatureDrivers = {};
  signalTotal = eventTotal = eventOrderTotal = eventFillTotal = positionTotal = 0;
  signalHasMore = eventHasMore = positionHasMore = false;
  signalFallbackVisible = SIGNAL_PAGE_SIZE; positionFallbackVisible = POSITION_PAGE_SIZE;
  signalDataRevision = positionDataRevision = eventRecordRevision = lastRenderedRevision = null;
  signalSelectionKey = positionSelectionKey = eventSelectionKey = null;
  signalRequestRevisionInFlight = positionRequestRevisionInFlight = eventRequestRevisionInFlight = "";
  signalRequestScopeInFlight = positionRequestScopeInFlight = eventRequestScopeInFlight = "";
  featurePanelScopeText = featurePanelSignalKey = "";
  signalLoadError = signalLoadNotice = eventLoadError = eventLoadNotice = positionLoadError = historyLoadError = "";
  chartHistory = null;
  chartHistoryKey = "";
}

function scheduleSessionRollover(serviceSync) {
  // The day-trade page reconciles the session on its next minute tick.
  // Overnight retains its existing server-clock rollover observer.
  if (!IS_OVERNIGHT) return;
  if (!followLatestSession && !followPerformanceRange) return;
  const deadline = String(serviceSync?.session_clock?.next_rollover_at || "");
  const serverNow = Date.parse(serviceSync?.generated_at_utc || "");
  const remaining = Date.parse(deadline) - serverNow;
  const due = performance.now() + remaining;
  if (!deadline || !Number.isFinite(remaining) || remaining <= 0) return;
  // A cached status may have an old server timestamp. A fresh revision can
  // move the timer earlier; repeated heartbeats must never push it later.
  if (deadline === sessionRolloverDeadline && due >= sessionRolloverDue) return;
  window.clearTimeout(sessionRolloverTimer);
  sessionRolloverDeadline = deadline;
  sessionRolloverDue = due;
  // Relative server time avoids a mis-set browser wall clock. Visibility
  // reconciliation handles sleeping tabs; SSE still owns data updates.
  sessionRolloverTimer = window.setTimeout(() => {
    sessionRolloverDeadline = "";
    sessionRolloverDue = Infinity;
    if (followLatestSession || followPerformanceRange) void refresh();
  }, Math.min(remaining, 2147483647));
}

function renderLiveModeFallback() {
  if (IS_OVERNIGHT || snapshot) return;
  const shared = window.StockAgentTwLivePaper?.getSnapshot();
  const view = shared?.data;
  if (!view || !Array.isArray(view.modes)) return;
  // The compact projection has no independent server observation clock. At
  // a year boundary, fail closed if its as-of year differs from the browser's
  // Taipei year; an incorrect client clock may hide data, never relabel it.
  const currentYear = performanceYear({generated_at_utc: new Date().toISOString()});
  if (performanceYear({generated_at_utc: view.as_of}) !== currentYear) {
    setHtml("mode-cards", `<div class="panel">今年尚無已確認快照，或本機時鐘待確認；不顯示其他年度績效。</div>`);
    return;
  }
  const freshness = shared.error ? `；${shared.error}` : "";
  setHtml("mode-cards", view.modes.map((mode) => `<article class="panel mode-card" data-market="${esc(mode.market)}">
    <header><h3>${esc(mode.label)}</h3><span class="badge warn">已提交快照</span></header>
    <div class="metric-context">${esc(view.as_of)}${esc(freshness)}；完整面板、歷史與曲線尚未取得。</div>
    <div class="mode-glance">
      <div><span>模擬狀態</span><strong>${esc(engineStatusLabel(mode.engine_status))}</strong></div>
      <div><span>總權益</span><strong>${summaryMoney(mode.total_equity_twd)}</strong></div>
      <div><span>未平倉</span><strong>${number(mode.open_position_count)} 筆</strong></div>
      <div><span>本版已成交／未成交</span><strong>${number(mode.entry_filled_shares)}／${number(mode.entry_unfilled_shares)} 股</strong></div>
    </div>
  </article>`).join(""));
}

function renderModes(data) {
  if (IS_OVERNIGHT) {
    setHtml("mode-cards", data.modes.map((mode) => {
      const rangeSummary = rangeSummaryFor(mode.market);
      const equity = rangeSummary?.end_equity_twd ?? mode.total_equity_twd;
      const pnl = rangeSummary?.cumulative_net_pnl_twd ?? (
        Number(mode.cumulative_realized_net_pnl_twd || 0)
        + Number(mode.open_net_liquidation_pnl_twd || 0)
      );
      const returnPct = rangeSummary?.return_pct;
      const status = String(mode.engine_status || "waiting");
      const kind = status.startsWith("blocked") || status.startsWith("critical")
        ? "bad"
        : ["carrying_to_next_open", "flat_after_next_open"].includes(status) ? "good" : "warn";
      return `<article class="panel mode-card">
        <header><h3>${esc(strategyLabel(mode))}</h3>${badge(engineStatusShortLabel(status), kind)}</header>
        <div class="equity ${pnlClass(returnPct)}">${returnPct == null ? "等待跨日估值" : `${Number(returnPct) >= 0 ? "+" : ""}${displayPct(returnPct)}`}</div>
        <div class="metric-context">收盤進場 → 次一交易日開盤沖銷</div>
        <div class="delta ${pnlClass(pnl)}">總權益 ${summaryMoney(equity)} · 總淨損益 ${Number(pnl) >= 0 ? "+" : ""}${summaryMoney(pnl)}</div>
        <div class="mode-glance">
          <div><span>目前階段</span><strong class="${esc(kind)}">${esc(engineStatusLabel(status))}</strong></div>
          <div><span>隔夜持倉</span><strong>${number(mode.open_position_count || 0)}</strong></div>
          <div><span>目標／收盤成交</span><strong>${number(mode.entry_requested_shares || 0)}／${number(mode.entry_filled_shares || 0)} 股</strong></div>
          <div><span>已實現淨損益</span><strong class="${pnlClass(mode.cumulative_realized_net_pnl_twd)}">${summaryMoney(mode.cumulative_realized_net_pnl_twd)}</strong></div>
          <div><span>未實現淨清算損益</span><strong class="${pnlClass(mode.open_net_liquidation_pnl_twd)}">${summaryMoney(mode.open_net_liquidation_pnl_twd)}</strong></div>
        </div>
        <details><summary>查看執行契約</summary><div class="metrics">
          <div><span>資金基準</span><strong>${money(mode.initial_capital_twd)}</strong></div>
          <div><span>訊號時間</span><strong>${shortTime(mode.signal_at)}</strong></div>
          <div><span>收盤成交結果</span><strong>${esc(mode.entry_fill_outcome || "尚未執行")}</strong></div>
          <div><span>估值缺價</span><strong>${number(mode.stale_position_count || 0)}</strong></div>
          <div class="wide"><span>模型來源</span><strong>暫時沿用當沖 checkpoint；尚未針對隔夜風險訓練</strong></div>
          <div class="wide"><span>成交證據</span><strong>試撮不成交；只接受實際收盤與次日實際開盤撮合價格</strong></div>
        </div></details>
      </article>`;
    }).join(""));
    return;
  }
  setHtml("mode-cards", data.modes.map((mode) => {
    const rangeSummary = rangeSummaryFor(mode.market);
    const initial = rangeSummary?.initial_capital_twd == null ? null : Number(rangeSummary.initial_capital_twd);
    const equity = rangeSummary?.end_equity_twd == null ? null : Number(rangeSummary.end_equity_twd);
    const pnl = rangeSummary?.cumulative_net_pnl_twd == null ? null : Number(rangeSummary.cumulative_net_pnl_twd);
    const returnPct = rangeSummary?.return_pct == null ? null : Number(rangeSummary.return_pct);
    const account = mode.account_performance;
    const accountReturnPct = account?.status === "available" && account.return_pct != null
      ? Number(account.return_pct)
      : IS_OVERNIGHT && rangeSummary?.cumulative_return_pct != null
        ? Number(rangeSummary.cumulative_return_pct) : null;
    const status = mode.model_signal_status === "published_waiting_engine" && mode.engine_status === "waiting_signal"
      ? "model_ready_waiting_engine" : String(mode.engine_status || "unknown");
    const execution = executionStatusPresentation(mode.today_execution_status);
    const fillOutcome = fillOutcomePresentation(mode.today_execution_outcome || mode.entry_fill_outcome);
    // Absence is not zero: keep the real paper-order quantities visible and
    // separately attributable to this exact rendered mode revision.
    const entryCounts = [mode.entry_requested_shares, mode.entry_filled_shares, mode.entry_unfilled_shares];
    const entryCountsKnown = entryCounts.every((value) => Number.isSafeInteger(value) && value >= 0);
    const nontrade = Number(mode.nontrade_settlement_count || 0) > 0;
    const kind = nontrade ? "warn" : status === "active" ? "good" : status.startsWith("blocked") || status.startsWith("critical") ? "bad" : "warn";
    const offsetTicks = Number(mode.price_limit_offset_ticks || 0);
    const bracketPolicy = offsetTicks > 0
      ? `TP／SL 依價格界線內縮 ${sourceNumber(offsetTicks)} Tick（無幅限者為系統委託邊界，非漲跌停；成交不保證）`
      : "TP／SL 依完整價格界線；無幅限者為系統委託邊界，非漲跌停";
    const entryPolicy = Presentation.entryPolicy(mode, number);
    const reasonCounts = Object.entries(mode.signal_reason_counts || {})
      .filter(([, count]) => Number(count || 0) > 0)
      .sort((left, right) => Number(right[1]) - Number(left[1]))
      .slice(0, 4)
      .map(([reason, count]) => `${signalReasonLabel(reason)} ${number(count)}`)
      .join("、") || "無";
    return `<article class="panel mode-card" data-market="${esc(mode.market || "")}" data-public-signal-key="${esc(mode.public_signal_key || "")}">
      <header><h3>${esc(strategyLabel(mode))}</h3>${badge(nontrade ? "帳面結算（非成交）" : engineStatusShortLabel(status), kind)}</header>
      <div class="equity ${pnlClass(returnPct)}">${returnPct == null ? "尚無估值" : `${returnPct >= 0 ? "+" : ""}${displayPct(returnPct)}`}</div>
      <div class="metric-context">所選期間報酬 · 起始${IS_OVERNIGHT ? "集合競價事件" : "有效分鐘"} = 0%</div>
      ${mode.provisional_minute_source_coverage ? `<div class="mode-replay-warning" role="status">暫定回放：分鐘資料可能不完整；缺口估值延用，結果可能改變，並非券商成交。</div>` : ""}
      ${mode.cumulative_provisional_minute_source_coverage && !mode.provisional_minute_source_coverage ? `<div class="mode-replay-warning" role="status">累積績效包含分鐘資料可能不完整的暫定歷史；結果可能改變，並非券商成交。</div>` : ""}
      ${Number(mode.nontrade_settlement_count || 0) > 0 ? `<div class="mode-replay-warning" role="status">${number(mode.nontrade_settlement_count)} 筆為非成交假設帳面結算，包含假設成本。</div>` : ""}
      <div class="delta ${pnlClass(pnl)}">${pnl == null ? "所選日期尚無估值" : `期末權益 ${summaryMoney(equity)} · 累積淨損益 ${pnl >= 0 ? "+" : ""}${summaryMoney(pnl)}`}</div>
      <div class="mode-glance">
        <div><span>帳戶累積報酬（Discord 同口徑）</span><strong class="${pnlClass(accountReturnPct)}">${accountReturnPct == null ? "資料不可用" : `${accountReturnPct >= 0 ? "+" : ""}${displayPct(accountReturnPct)}`}</strong></div>
        <div><span>該日策略執行</span><strong class="${esc(execution.kind)}">${esc(execution.label)}</strong></div>
        <div><span>最新訊號紙上成交</span><strong class="${esc(fillOutcome.kind)}">${esc(fillOutcome.label)}</strong></div>
        <div><span>最新訊號目標／成交／缺口</span><strong${entryCountsKnown ? ` data-paper-entry-counts data-requested="${entryCounts[0]}" data-filled="${entryCounts[1]}" data-unfilled="${entryCounts[2]}"` : ""}>${entryCountsKnown ? `${number(entryCounts[0])}／${number(entryCounts[1])}／${number(entryCounts[2])} 股` : "等待成交資訊"}</strong></div>
        <div><span>持倉／缺價</span><strong>${number(mode.open_position_count)} / ${number(mode.stale_position_count)}</strong></div>
        ${Number(mode.indicative_valuation_position_count || 0) > 0 ? `<div><span>參考快照估值（非成交證據）</span><strong>${number(mode.indicative_valuation_position_count)} 個持倉</strong></div>` : ""}
        <div><span>${mode.cumulative_nontrade_settlement_net_pnl_twd != null || Number(mode.nontrade_settlement_count || 0) > 0 ? "帳面已結算（含假設）" : "已實現淨損益"}</span><strong class="${pnlClass(mode.cumulative_realized_net_pnl_twd)}">${summaryMoney(mode.cumulative_realized_net_pnl_twd)}</strong></div>
        <div><span>未實現淨清算損益</span><strong class="${pnlClass(mode.open_net_liquidation_pnl_twd)}">${summaryMoney(mode.open_net_liquidation_pnl_twd)}</strong></div>
        ${account?.margin_carry_contract ? `<div><span>累積現金權益（含減資退款）</span><strong>${summaryMoney(account.cumulative_corporate_action_net_twd || 0)}</strong></div><div><span>累積跨日成本</span><strong>${summaryMoney(account.cumulative_carry_cost_twd || 0)}</strong></div>` : ""}
      </div>
      <details><summary>查看資金、訊號與曝險細節</summary><div class="metrics">
        <div class="wide"><span>帳戶累積報酬基準</span><strong>原始帳戶資金至本次估值；與上方篩選區間報酬分開</strong></div>
        <div><span>帳戶估值時間</span><strong>${shortTime(account?.asof)}</strong></div>
        <div><span>帳本版本</span><strong>${esc(account?.state_revision ?? "歷史估值")}</strong></div>
        <div><span>原始資金基準</span><strong>${money(initial)}</strong></div>
        <div><span>已賺手續費退佣</span><strong>${money(mode.cumulative_commission_rebate_accrued_twd)}</strong></div>
        ${mode.counterfactual_open_replay
          ? `<div class="wide"><span>訊號／重建時間</span><strong>${esc(replayTimingText(mode))}</strong></div>`
          : `<div><span>訊號時間</span><strong>${shortTime(mode.signal_at)}</strong></div>`}
        <div><span>13:24 市價重試後殘餘</span><strong class="${Number(mode.force_exit_failures || 0) ? "negative" : ""}">${number(mode.force_exit_failures || 0)}</strong></div>
        <div><span>13:30 帳務強平</span><strong>${number(mode.terminal_flatten_count || 0)}</strong></div>
        <div><span>強平價替代值</span><strong class="${Number(mode.terminal_flatten_degraded_count || 0) ? "negative" : ""}">${number(mode.terminal_flatten_degraded_count || 0)}</strong></div>
        <div class="wide"><span>進場成交契約</span><strong>${esc(entryPolicy)}</strong></div>
        ${account?.odd_lot_execution_policy === "assumed_odd_lot_at_regular_board_price_v1"
          ? `<div class="wide"><span>零股執行假設</span><strong>按同時點一般盤價格模擬，非真實零股成交；停牌不成交</strong></div>` : ""}
        <div class="wide"><span>訊號結果原因</span><strong>${esc(reasonCounts)}</strong></div>
        <div class="wide"><span>停利停損價位</span><strong>${esc(bracketPolicy)}</strong></div>
      </div></details>
    </article>`;
  }).join(""));
}

function renderBenchmarks(data) {
  const rows = Array.isArray(data.benchmarks) ? data.benchmarks : [];
  setHtml("benchmark-cards", rows.map((row) => {
    const rangeSummary = rangeSummaryFor(row.benchmark_id);
    const returnPct = rangeSummary?.return_pct == null ? null : Number(rangeSummary.return_pct);
    const dailyReturnPct = row.daily_return_pct == null ? null : Number(row.daily_return_pct);
    const equity = rangeSummary?.end_equity_twd == null ? null : Number(rangeSummary.end_equity_twd);
    const netPnl = rangeSummary?.cumulative_net_pnl_twd == null ? null : Number(rangeSummary.cumulative_net_pnl_twd);
    const isTx = row.instrument_type === "continuous_long_future";
    const waitingRoll = String(row.valuation_source || "").includes("roll_waiting");
    const actionBlocked = !isTx && String(row.valuation_source || "").includes("corporate_action_reference_unavailable");
    const stale = Boolean(row.valuation_stale);
    const status = dailyReturnPct == null
      ? {label:"等待有效價格", kind:"bad"}
      : actionBlocked
        ? {label:"除權息資料未覆蓋", kind:"bad"}
      : waitingRoll
        ? {label:"等待雙邊換月報價", kind:"warn"}
        : stale
          ? {label:"延用前次估值", kind:"warn"}
          : {label:"跨日持有估值", kind:"good"};
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
    const totalCosts = Number(row.estimated_tracking_cost_twd);
    const priorCloseLabel = row.daily_return_previous_close_date
      ? `前收 ${row.daily_return_previous_close_date}`
      : "前一交易日收盤";
    const collateral = isTx
      ? `${money(row.current_contract_notional_twd)} · 覆蓋 ${sourceNumber(row.collateral_coverage_ratio)}×`
      : "—";
    const funding = isTx
      ? `補入 ${money(row.cumulative_margin_top_up_twd || 0)} · 提回 ${money(row.cumulative_margin_withdrawal_twd || 0)}`
      : "";
    const dailyReturnText = dailyReturnPct == null
      ? "今日相對昨收 —"
      : `今日相對昨收 ${dailyReturnPct >= 0 ? "+" : ""}${displayPct(dailyReturnPct)}`;
    return `<article class="panel benchmark-card">
      <header><h3>${esc(row.label || row.benchmark_id)}</h3>${badge(status.label, status.kind)}</header>
      <div class="equity ${pnlClass(dailyReturnPct)}">${dailyReturnPct == null ? "尚無前收基準" : `${dailyReturnPct >= 0 ? "+" : ""}${displayPct(dailyReturnPct)}`}</div>
      <div class="delta ${pnlClass(returnPct)}">${returnPct == null ? "所選日期等待完整來源" : `${dailyReturnText} · 所選期間 ${returnPct >= 0 ? "+" : ""}${displayPct(returnPct)} · 累積損益 ${netPnl >= 0 ? "+" : ""}${summaryMoney(netPnl)}`}</div>
      <div class="benchmark-facts">
        <div><span>持有標的</span><strong class="benchmark-contract">${esc(holding)}</strong></div>
        <div><span>${esc(priorCloseLabel)}</span><strong>${detailComponents.quoteText(row, ["daily_return_reference_price", "daily_return_previous_close"], sourceNumber)}</strong></div>
        <div><span>Buy & Hold 起點</span><strong>${detailComponents.quoteText(row, "entry_price", sourceNumber)}<small>${shortTime(row.entry_at)}</small></strong></div>
        <div><span>帳本清算估值</span><strong>${detailComponents.quoteText(row, "last_mark_price", sourceNumber)}<small>${shortTime(row.last_quote_at || row.last_mark_at)}</small></strong></div>
        <div><span>${isTx ? "自動換月" : "持有契約"}</span><strong>${esc(rollText)}</strong></div>
        <div><span>追蹤成本（不扣報酬）</span><strong>${Number.isFinite(totalCosts) ? money(totalCosts) : "—"}</strong></div>
        ${isTx ? `<div><span>1:1 名目資金</span><strong>${collateral}</strong></div><div><span>換月資金流</span><strong>${funding}</strong></div><div class="wide"><span>換月規則</span><strong>同一實體合約以前收計日報酬；換月價差只補入／提回現金，不列報酬，槓桿上限 1×</strong></div>` : ""}
      </div>
    </article>`;
  }).join("") || `<article class="panel benchmark-card"><strong>基準尚未建立</strong><small>等待來源與起算價格通過稽核</small></article>`);
}

function officialUniverseDescription(scope) {
  if (!scope || scope.status !== "ready") {
    return {value: "等待官方名冊", note: "不以模型歷史或成交量推定可交易標的", kind: "warn"};
  }
  return {
    value: `${number(scope.trading_count)} 檔`,
    note: `上市／上櫃股票與 ETF ${number(scope.listed_count)} 檔 − 停止交易 ${number(scope.halted_count)} 檔 · 觀測 ${shortTime(scope.observed_at_taipei)}；非當日原始訊號覆蓋率`,
    kind: "good",
  };
}

function operationalIssuesForDisplay(data) {
  const modes = Array.isArray(data.modes) ? data.modes : [];
  const byMarket = new Map(modes.map((mode) => [mode.market, mode]));
  const issues = (Array.isArray(data.operational_issues) ? data.operational_issues : []).map((issue) => {
    const mode = byMarket.get(issue.market);
    if (!mode || issue.code !== "entry_no_fill" || mode.today_execution_outcome !== "no_fill") return issue;
    const remaining = Math.max(0, Number(mode.entry_unfilled_shares) || 0);
    const positions = Math.max(0, Number(mode.open_position_count) || 0);
    const pendingText = remaining ? `，尚有 ${number(remaining)} 股未成交` : "";
    const positionsText = positions ? `；另有 ${number(positions)} 筆先前建立的未平模擬持倉` : "；目前沒有未平模擬持倉";
    return {...issue, detail: `本次訊號尚無新增成交${pendingText}${positionsText}。`};
  });
  for (const mode of modes) {
    const sourceAt = Date.parse(mode.source_signal_at || "");
    const replayAt = Date.parse(mode.signal_at || "");
    if (!mode.counterfactual_open_replay || !Number.isFinite(sourceAt) || !Number.isFinite(replayAt)
        || sourceAt <= replayAt || issues.some((issue) => issue.market === mode.market && issue.code === "replay_signal_after_simulated_time")) continue;
    issues.push({market: mode.market, code: "replay_signal_after_simulated_time", severity: "error",
      title: `${mode.label || mode.market} 回放訊號晚於模擬時間`,
      detail: `訊號資料時間為 ${mode.source_signal_at}，帳本模擬時間為 ${mode.signal_at}；不能視為當時已取得的訊號或即時成交，需重新核對來源並驗收回放。`});
  }
  return issues;
}

function pendingOpeningAttempts(data) {
  if (IS_OVERNIGHT) return [];
  const latency = data.opening_signal_latency || {};
  const ready = new Set(latency.observed_markets || []);
  return (latency.latest_failures || []).filter(row => !ready.has(row.market));
}

function openingWaitDescription(row) {
  if (row.reason_code === "tw_public_source_update_pending") {
    return "官方來源正在寫入相依資料；等待一致版本，不是訊號已完成";
  }
  if (row.reason_code === "tw_official_trading_universe_pending") {
    return "官方當日交易名冊尚未完整驗收；等待來源自動重試，不退回歷史模型清單";
  }
  if (row.reason_code === "tw_opening_input_coverage_pending") {
    const coverage = row.opening_input_coverage || {};
    const resolved = Number(coverage.opens || 0) + Number(coverage.no_prints || 0);
    return `開盤輸入 ${number(resolved)}／門檻 ${number(coverage.required || 0)}；有效開盤價 ${number(coverage.opens || 0)}、明確未成交 ${number(coverage.no_prints || 0)}；仍在取價，不以昨日價格替代`;
  }
  return `訊號尚未成功 · ${row.error_type || "來源未就緒"} · ${row.failure_stage || "等待重試"}`;
}

function renderOvernightOperations(data) {
  const modes = data.modes || [];
  const counts = data.record_counts || {};
  const sourceAge = Number(data.source_age_seconds);
  const openPositions = modes.reduce((sum, mode) => sum + Number(mode.open_position_count || 0), 0);
  const workingModes = modes.filter((mode) => mode.engine_status === "waiting_close_auction_match").length;
  const carriedModes = modes.filter((mode) => mode.engine_status === "carrying_to_next_open").length;
  const closedModes = modes.filter((mode) => mode.engine_status === "flat_after_next_open").length;
  const blockedModes = modes.filter((mode) => String(mode.engine_status || "").startsWith("blocked") || String(mode.engine_status || "").startsWith("critical")).length;
  setHtml("latency-kpis", [
    ["面板請求 → 顯示", lastFetchMs == null ? "—" : `${number(lastFetchMs, 1)} ms`, "同源唯讀 API；畫面局部更新、不整頁重載"],
    ["引擎心跳年齡", Number.isFinite(sourceAge) ? duration(sourceAge) : "—", "與最後完整帳本提交、市場價格新鮮度分開解讀"],
    ["策略切換時點", "13:00", "切換至當日交易日、完成預熱後等待"],
    ["模型計算時點", "13:20", "以當下行情特徵執行暫時的當沖模型轉接"],
    ["正式收盤撮合", "13:30／13:33", "試撮不列成交；延緩收市才可能到 13:33"],
    ["開盤委託時點", "次日 08:30", "以當日合法漲跌停價格參與集合競價"],
    ["正式開盤沖銷", "次日 09:00", "僅接受非試撮實際 open"],
    ["委託／成交紀錄", `${number(counts.orders || 0)}／${number(counts.fills || 0)}`, "append-only 紙上帳本"],
    ["安全邊界", "正式下單不可用", "不呼叫券商 order API"],
  ].map(([label, value, note]) => `<div class="latency-kpi"><span>${esc(label)}</span><strong>${esc(value)}</strong><small>${esc(note)}</small></div>`).join(""));
  setHtml("operation-kpis", [
    ["等待收盤撮合", `${number(workingModes)} 模式`, "已有 13:20 計算後送出的極限價委託，等待實際 close", workingModes ? "warn" : "good"],
    ["持有至次日開盤", `${number(carriedModes)} 模式 · ${number(openPositions)} 檔`, "已由實際收盤撮合價建立部位", carriedModes ? "good" : ""],
    ["次日已沖銷", `${number(closedModes)} 模式`, "實際開盤價已寫入成交帳", closedModes ? "good" : ""],
    ["阻擋／異常", `${number(blockedModes)} 模式`, blockedModes ? "詳見上方警示與稽核" : "沒有執行阻擋", blockedModes ? "bad" : "good"],
    ["面板刷新", `每 ${number(PRICE_REFRESH_MS / 1000)} 秒`, "價格與資料來源同為一分鐘週期", "good"],
  ].map(([label, value, note, kind]) => `<div class="operation-kpi"><span>${esc(label)}</span><strong>${esc(value)}</strong><small class="${esc(kind)}">${esc(note)}</small></div>`).join(""));

  const workflow = [
    ["13:20 計算與極限價委託", modes.filter((mode) => mode.signal_id).length, modes.length, "計算完成即送單；訊號與委託原子落盤"],
    ["實際收盤撮合", modes.filter((mode) => Number(mode.entry_filled_shares || 0) > 0).length, modes.length, "simtrade 不算成交"],
    ["隔夜持有", carriedModes, modes.length, "只允許一個未平 cohort，不重疊加倉"],
    ["次日實際開盤沖銷", closedModes, modes.length, "缺 actual open 則保持未平並揭露"],
  ];
  setHtml("workflow-progress", workflow.map(([label, completed, total, note]) => `<div class="progress-row"><div class="progress-title"><strong>${esc(label)}</strong><span>${number(completed)} / ${number(total)}</span></div>${progress(total ? completed / total : 0, completed === total && total ? "good" : "warn")}<small>${esc(note)}</small></div>`).join(""));
  setHtml("opening-stage-progress", modes.map((mode) => `<div class="progress-row"><div class="progress-title"><strong>${esc(strategyLabel(mode))}</strong>${badge(engineStatusShortLabel(mode.engine_status), String(mode.engine_status || "").startsWith("blocked") ? "bad" : "warn")}</div><small>訊號 ${shortTime(mode.signal_at)} · 目標 ${number(mode.entry_requested_shares || 0)} 股 · 收盤成交 ${number(mode.entry_filled_shares || 0)} 股 · ${esc(mode.entry_fill_outcome || "等待 13:20 計算")}</small></div>`).join("") || `<div class="empty-inline">尚無啟用模式。</div>`);
  setHtml("opening-latency-trend", `<div class="progress-row"><div class="progress-title"><strong>開盤試撮不算成交</strong>${badge("FAIL-CLOSED", "good")}</div><small>08:30–09:00 只更新預估價格；09:00 後必須讀到同日、非 simtrade 且帶交易所時間戳的 open。</small></div>`);
  setHtml("preopen-progress", modes.map((mode) => `<div class="progress-row"><div class="progress-title"><strong>${esc(strategyLabel(mode))}</strong>${badge(mode.checkpoint_ready ? "CHECKPOINT READY" : "CHECKPOINT MISSING", mode.checkpoint_ready ? "good" : "bad")}</div><small>${mode.model_trained_for_overnight === false ? "當沖模型暫時轉接，未針對隔夜報酬訓練" : "模型契約待確認"}</small></div>`).join(""));
  $("operation-source").textContent = `狀態 ${shortTime(data.source_updated_at)}`;
}

function renderOperations(data) {
  if (IS_OVERNIGHT) {
    renderOvernightOperations(data);
    return;
  }
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
  const latency = data.latency || data.today_latency || {};
  const openingLatency = data.opening_signal_latency || {};
  const latencyStageLabels = {
    scheduler_wake_ms: "排程喚醒",
    mode_dispatch_queue_ms: "前序模式排隊",
    preopen_catch_up_ms: "漏失盤前補備",
    realtime_prepare_ms: "即時狀態準備",
    realtime_prepare_wait_ms: "狀態準備剩餘等待",
    model_lock_queue_ms: "模型鎖等待",
    signal_pre_quote_prepare_ms: "訊號盤前骨架",
    signal_quote_fetch_ms: "訊號行情",
    signal_pre_inference_prepare_ms: "推論輸入準備",
    model_inference_ms: "模型推論",
    signal_post_inference_format_ms: "推論後格式化",
    signal_other_compute_ms: "其他訊號計算",
    artifact_publish_ms: "原子發布",
    artifact_discovery_ms: "發布至註冊（含等待／回補）",
    quote_usage_before_ms: "取價前流量查詢",
    quote_request_body_ms: "快照回傳與批次整理",
    quote_usage_after_ms: "取價後流量查詢",
    quote_traffic_record_ms: "流量紀錄寫入",
    quote_broker_queue_ms: "行情服務排隊",
    quote_response_encode_ms: "行情回應編碼",
    quote_lock_queue_ms: "行情連線鎖排隊",
    quote_contract_prepare_ms: "合約／批次準備",
    quote_submit_ms: "快照請求送出",
    quote_callback_wait_ms: "等待快照回傳",
    quote_callback_parse_ms: "快照解析",
    quote_output_assemble_ms: "行情向量組裝",
    quote_limit_resolve_ms: "合法價格界線整理",
    opening_signal_batch_wait_ms: "等待其他開盤模式",
    eligibility_load_ms: "資格載入",
    executor_quote_fetch_ms: "執行行情",
    ledger_compute_persist_ms: "帳本落盤",
  };
  const latencyValue = (value) => value == null ? "—" : `${number(value, 1)} ms`;
  const noLatency = !Number(latency.sample_count || 0);
  const noOpeningLatency = !Number(openingLatency.observed_mode_count || 0);
  const openingPartial = openingLatency.integrity?.state === "partial_corrupt";
  const latencyEmptyLabel = openingPartial ? "測速紀錄不完整" : "所選日尚無開盤樣本";
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
  const refreshNote = IS_OVERNIGHT ? `版本探測每 ${number(SERVICE_REVISION_REFRESH_MS / 1000)} 秒` : "網頁每 60 秒更新";
  const ageContext = IS_OVERNIGHT ? "" : "本次讀取時 ";
  const syncNote = `${ageContext}engine ${duration(serviceSync.engine_age_seconds)}前 · Discord ${duration(discordSync.age_seconds)}前 · ${refreshNote}`;
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
  const openingModes = openingLatency.modes || [];
  const firstOpeningMode = openingModes.reduce((best, row) => !best || Number(row.ready_from_0900_ms) < Number(best.ready_from_0900_ms) ? row : best, null);
  const openingChangeMs = openingLatency.change_vs_previous_ms == null
    ? Number.NaN
    : Number(openingLatency.change_vs_previous_ms);
  const openingChange = Number.isFinite(openingChangeMs)
    ? `${openingChangeMs < 0 ? "快" : openingChangeMs > 0 ? "慢" : "持平"} ${duration(Math.abs(openingChangeMs) / 1000)}`
    : "尚無可比前日";
  const openingGoalMs = Number(openingLatency.goal_ms || 1000);
  const latencyNumber = (value) => value == null ? Number.NaN : Number(value);
  const sourceReadyMs = latencyNumber(openingLatency.first_source_ready_ms);
  const controllableP50Ms = latencyNumber(openingLatency.source_ready_to_signal_p50_ms);
  const controllableMaxMs = latencyNumber(openingLatency.source_ready_to_signal_max_ms);
  const slowestOpeningMode = openingModes.reduce((best, row) => {
    const value = Number(row.bottleneck_ms);
    return Number.isFinite(value) && (!best || value > Number(best.bottleneck_ms)) ? row : best;
  }, null);
  const openingBottleneck = slowestOpeningMode
    ? `${latencyStageLabels[slowestOpeningMode.bottleneck_stage] || slowestOpeningMode.bottleneck_stage} · ${latencyValue(slowestOpeningMode.bottleneck_ms)}`
    : noLatency ? "—" : `${latestBottleneck} · ${latencyValue(latency.latest_bottleneck_ms)}`;
  setHtml("latency-kpis", [
    [openingPartial ? "09:00 → 可讀樣本最快" : "09:00 → 首個訊號", noOpeningLatency ? latencyEmptyLabel : latencyValue(openingLatency.first_ready_ms), openingPartial ? "原始測速紀錄有損壞，不能認定最早時間或達標" : firstOpeningMode ? `${strategyLabel(firstOpeningMode)} · 目標 ≤ ${number(openingGoalMs)} ms · ${openingLatency.first_signal_goal_met ? "達標" : "未達標"}` : "等待實測"],
    ["09:00 → 行情覆蓋", Number.isFinite(sourceReadyMs) ? latencyValue(sourceReadyMs) : "—", Number.isFinite(sourceReadyMs) ? "本機收到足夠 callback；不是交易所 RTT" : "舊樣本未記錄逐筆到達時間"],
    ["行情就緒 → 訊號", Number.isFinite(controllableP50Ms) ? `${latencyValue(controllableP50Ms)} / ${latencyValue(controllableMaxMs)}` : "—", "逐模式 P50 / 最慢；這是主要可控區段"],
    [openingPartial ? "09:00 → 可讀模式樣本" : "09:00 → 全部模式", noOpeningLatency ? "—" : latencyValue(openingLatency.final_ready_ms), `${number(openingLatency.observed_mode_count || 0)}/${number(openingLatency.expected_mode_count || 0)} 模式有可讀測速紀錄${openingPartial ? "；來源不完整，不能驗收" : openingLatency.all_modes_goal_met ? "，全數達標" : openingLatency.complete ? "，完成但未達 1 秒" : openingLatency.replay_without_live_measurement?.length ? `；${number(openingLatency.replay_without_live_measurement.length)} 個回補模式無當時即時測速` : "；仍缺測速紀錄"}`],
    ["相較前次開盤", openingChange, openingLatency.previous_session_date ? `${openingLatency.previous_session_date} 最後訊號 ${duration(Number(openingLatency.previous_final_ready_ms) / 1000)}` : "只比較 09:00 自動樣本"],
    ["09:00 → 全模式帳本落盤", openingLatency.final_ledger_ms == null ? "尚未完整觀測" : latencyValue(openingLatency.final_ledger_ms), `${number(openingLatency.ledger_observed_mode_count || 0)}/${number(openingLatency.expected_mode_count || 0)} 模式；含等待／回補，不是券商成交。輸入至帳本 P50/P95：${latencyValue(latency.p50_ms)} / ${latencyValue(latency.p95_ms)}`],
    ["開盤最大階段", openingBottleneck, slowestOpeningMode ? strategyLabel(slowestOpeningMode) : noLatency ? "等待實測" : "來自執行帳本樣本"],
    ["失敗嘗試", `${openingPartial ? "至少 " : ""}${number(openingLatency.failure_count || 0)}`, openingPartial ? "僅計可讀紀錄，損壞行可能包含其他失敗" : openingLatency.failure_count ? "錯誤類型已寫入測速帳本" : "所選日沒有測速失敗紀錄"],
  ].map(([label, value, note]) => `<div class="latency-kpi"><span>${esc(label)}</span><strong>${esc(value)}</strong><small>${esc(note)}</small></div>`).join(""));
  const officialUniverse = officialUniverseDescription(data.current_trading_universe);
  setHtml("operation-kpis", [
    ["所選日流程／成交", `${number(execution.executed_count || 0)} 已處理 · ${number(execution.filled_mode_count || 0)} 完整成交`, `${number(execution.partial_mode_count || 0)} 部分、${number(execution.zero_fill_mode_count || 0)} 零成交、${number(execution.no_order_mode_count || 0)} 無合法委託、${number(execution.failed_mode_count || 0)} 阻擋`, execution.all_modes_filled ? "good" : "bad"],
    ["當前官方可交易標的", officialUniverse.value, officialUniverse.note, officialUniverse.kind],
    ["盤前預熱", `${readyModes}/${totalModes || 0} 模型 READY`, `模擬執行 ${simulationWarm.status || "pending"}`, warmKind],
    ["目前階段", session.label || "—", `下一步 ${session.next_milestone_label || "—"} · ${countdown(session.next_milestone_at)}`, phaseKind],
    ["引擎心跳", `${sourceNumber(data.source_age_seconds)} 秒`, `最後完整提交 ${duration(data.source_commit_age_seconds)} 前`, heartbeatKind],
    ["面板 API", lastFetchMs == null ? "—" : `${number(lastFetchMs, 1)} ms`, `行情與權益每 ${number(PRICE_REFRESH_MS / 1000)} 秒刷新`, lastFetchMs != null && lastFetchMs > 1000 ? "warn" : "good"],
    ["服務同步", syncLabel, syncNote, syncKind],
    ["無人維護守護", guardianLabel, guardianNote, guardianKind],
  ].map(([label, value, note, kind]) => `<div class="operation-kpi"><span>${esc(label)}</span><strong>${esc(value)}</strong><small class="${esc(kind)}">${esc(note)}</small></div>`).join(""));

  const workflowRows = [
    {label:"啟用模式預熱", value:warmRatio, count:`${number(warm.completed_count || 0)} / ${number(totalModes)} · ${number(warmRatio * 100, 1)}%`, note:`牆鐘 ${duration(warm.wall_elapsed_seconds)} · ${warm.modes_per_minute == null ? "—" : `${sourceNumber(warm.modes_per_minute)} 模式/分`}`, kind:warmKind},
    {label:"所選日策略執行", value:session.signal_progress_ratio, count:`${number(session.signal_completed_modes || 0)} / ${number(session.mode_count || 0)}`, note:"原子指標由 inotify 事件即時喚醒；0.1 秒只作備援，阻擋不算完成", kind:"good"},
    {label:"進場處理終態", value:session.entry_progress_ratio, count:`${number(session.entry_completed_modes || 0)} / ${number(session.mode_count || 0)}`, note:"完成後可能有成交或依真實限制保持空倉", kind:"good"},
    {label:"每分鐘權益紀錄", value:session.mark_progress_ratio, count:`${number(session.observed_mode_minutes || 0)} / ${number(session.expected_mode_minutes || 0)}`, note:session.mark_tracking_complete ? "全部模式已平倉，估值追蹤完成" : `目前 ${sourceNumber(session.mark_rows_per_minute || 0)} 模式紀錄/分`, kind:""},
    {label:"13:20/13:24/13:25 退出", value:session.exit_progress_ratio, count:`${number(session.exit_started_modes || 0)} / ${number(session.mode_count || 0)}`, note:"先限價、再市價重試；殘餘於 13:25 以漲跌停價參與收盤集合競價", kind:"warn"},
  ];
  setHtml("workflow-progress", workflowRows.map((row) => `<div class="progress-row">
    <div class="progress-title"><strong>${esc(row.label)}</strong><span>${esc(row.count)}</span></div>
    ${progress(row.value, row.kind)}
    <small>${esc(row.note)}</small>
  </div>`).join(""));

  const openingStageHtml = openingModes.map((row) => {
    const stageEntries = Object.entries(row.stages || {})
      .filter(([, value]) => value != null)
      .map(([name, value]) => [name, Number(value)])
      .filter(([, value]) => Number.isFinite(value) && value >= 0)
      .sort((left, right) => right[1] - left[1]);
    const transport = row.quote_transport || {};
    const brokerParts = [
      ["服務排隊", transport.server_request_queue_ms],
      ["來源取價", transport.server_provider_fetch_ms],
      ["回應編碼", transport.server_snapshot_serialize_ms],
      ["呼叫往返", transport.client_round_trip_ms],
      ["首批回傳", transport.snapshot_first_callback_ms],
      ["末批回傳", transport.snapshot_last_callback_ms],
    ].filter(([, value]) => value != null && Number.isFinite(Number(value)));
    const stagesText = stageEntries.length
      ? stageEntries.map(([name, value]) => `${latencyStageLabels[name] || name} ${latencyValue(value)}`).join(" · ")
      : "舊樣本只有總延遲，明日開盤起提供完整分段";
    const brokerText = brokerParts.length
      ? `；報價通道（取價內含分段，不可相加）${brokerParts.map(([name, value]) => `${name} ${latencyValue(value)}`).join(" · ")}`
      : "";
    const publication = row.state_publication || {};
    const publicationFiles = Array.isArray(publication.files) ? publication.files : [];
    const publicationParts = [["完整發布", publication.total_ms], ["內容指紋", publication.projection_fingerprint_ms]];
    for (const [label, key] of [["JSON 編碼", "json_encode_ms"], ["檔案寫入／同步", "file_write_fsync_ms"], ["替換／目錄同步", "replace_directory_fsync_ms"]]) {
      if (publicationFiles.length && publicationFiles.every(file => file[key] != null && Number.isFinite(Number(file[key])) && Number(file[key]) >= 0)) {
        publicationParts.push([label, publicationFiles.reduce((sum, file) => sum + Number(file[key]), 0)]);
      }
    }
    const publicationText = publication.total_ms == null ? ""
      : `；帳本發布（帳本落盤內含分段，不可再相加）${publicationParts.filter(([, value]) => value != null && Number.isFinite(Number(value))).map(([label, value]) => `${label} ${latencyValue(value)}`).join(" · ")}`;
    const accountingNote = transport.snapshot_usage_observation === "not_sampled_latency_critical"
      ? "；即時取價不查流量；配額由獨立觀測更新，此筆流量未知，非免費／零流量"
      : "";
    const historyGuard = row.previous_signal_history_disabled === true
      ? "；歷史回補已禁止"
      : row.previous_signal_history_disabled === false
        ? "；警告：開盤路徑啟用了歷史回補"
        : "";
    const ledgerText = row.input_to_ledger_ms == null
      ? ""
      : `；09:00 到帳本 ${latencyValue(row.ledger_from_0900_ms)}；訊號輸入到帳本 ${latencyValue(row.input_to_ledger_ms)}（ready 後 ${latencyValue(row.ready_to_ledger_ms)}）`;
    const ratio = Math.min(1, Number(row.ready_from_0900_ms || 0) / Math.max(1, openingGoalMs));
    const kind = Number(row.ready_from_0900_ms) <= openingGoalMs ? "good" : "bad";
    return `<div class="progress-row opening-latency-row">
      <div class="progress-title"><strong>${esc(strategyLabel(row))}</strong>${badge(`${latencyValue(row.ready_from_0900_ms)} / ${latencyValue(openingGoalMs)}`, kind)}</div>
      ${progress(ratio, kind)}
      <small>${esc(`${row.delayed_recovery ? "延後恢復的排程訊號；未達開盤時限 · " : ""}行情覆蓋 ${latencyValue(row.source_ready_from_0900_ms)} · 行情就緒到訊號 ${latencyValue(row.source_ready_to_signal_ms)} · ${stagesText}${brokerText}${ledgerText}${publicationText}${historyGuard}${accountingNote}`)}</small>
    </div>`;
  }).join("") + pendingOpeningAttempts(data).map(row => {
    const coverage = row.opening_input_coverage || {};
    const required = Number(coverage.required || 0);
    const resolved = Number(coverage.opens || 0) + Number(coverage.no_prints || 0);
    return `<div class="progress-row opening-latency-row"><div class="progress-title"><strong>${esc(strategyLabel(row))}</strong>${badge("等待資料・自動重試", "warn")}</div>${required > 0 ? progress(Math.min(100, resolved / required * 100), "warn") : ""}<small>${esc(openingWaitDescription(row))} · ${esc(shortTime(row.recorded_at))}</small></div>`;
  }).join("");
  setHtml("opening-stage-progress", openingStageHtml || (openingPartial
    ? `<div class="empty-inline">測速紀錄有損壞，無法判定是否存在其他開盤樣本；原始紀錄已保留。</div>`
    : `<div class="empty-inline">所選日尚無 09:00 自動訊號測速；不以手動或回放資料冒充。</div>`));

  const openingTrendRows = (openingLatency.trend || []).slice(-7).reverse();
  setHtml("opening-latency-trend", openingTrendRows.map((row) => {
    const total = row.final_ready_ms == null ? "—" : latencyValue(row.final_ready_ms);
    const source = row.first_source_ready_ms == null ? "—" : latencyValue(row.first_source_ready_ms);
    const controllable = row.source_ready_to_signal_p50_ms == null ? "—" : latencyValue(row.source_ready_to_signal_p50_ms);
    const kind = row.all_modes_goal_met ? "good" : row.complete ? "bad" : "warn";
    const state = openingPartial ? "來源不完整" : row.all_modes_goal_met ? "≤1 秒" : row.complete ? "未達標" : "不完整";
    return `<div class="progress-row latency-trend-row">
      <div class="progress-title"><strong>${esc(row.session_date || "—")}</strong>${badge(state, kind)}</div>
      <small>${esc(`全部模式 ${total} · 行情覆蓋 ${source} · 可控 P50 ${controllable} · ${number(row.observed_mode_count || 0)}/${number(row.expected_mode_count || 0)} 模式 · 失敗 ${number(row.failure_count || 0)}`)}</small>
    </div>`;
  }).join("") || `<div class="empty-inline">尚無可比較的開盤日測速。</div>`);

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
      <div class="progress-title"><strong>${esc(strategyLabel(row))}</strong>${badge(stepText, kind)}</div>
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
  setHtml("preopen-progress", modelPreopenHtml + simulationHtml);
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
    setHtml(root, `<div class="tw-public-empty">尚未取到台股官方公開資料來源明細。請確認 /data-monitor 是否有載入完成。</div>`);
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

  setHtml(root, prepared.map((item) => {
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
  }).join(""));
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
        Dashboard.cancelResponse(response);
        failures.push(`${candidate}: HTTP ${response.status}`);
        continue;
      }
      const payload = await Dashboard.readJsonResponse(response, {expectedRoot: "object"});
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
  if (document.hidden || !twPublicMonitorActivated) return;
  if (twPublicMonitorRefreshInFlight) return;
  twPublicMonitorRefreshInFlight = true;
  if (twPublicMonitorAbortController) twPublicMonitorAbortController.abort();
  const controller = new AbortController();
  twPublicMonitorAbortController = controller;
  const started = performance.now();
  try {
    const payload = await loadTwPublicMonitorWithFallback(controller);
    if (!Array.isArray(payload.sources) && !Array.isArray(payload.rows)) {
      throw new Error("官方資料來源清單缺漏");
    }
    twPublicMonitorData = payload;
    twPublicMonitorLastFetchMs = performance.now() - started;
    twPublicMonitorLastUpdated = payload.generated_at_utc || null;
    renderTwPublicMonitor(payload);
  } catch (error) {
    if (error?.name === "AbortError") return;
    const root = $("tw-public-monitor-list");
    const summary = $("tw-public-monitor-summary");
    const fetchState = $("tw-public-monitor-fetch-state");
    const failure = `台股公開資料狀態更新失敗：${error}`;
    if (twPublicMonitorData) {
      const lastSuccess = twPublicMonitorLastUpdated
        ? shortDateTime(twPublicMonitorLastUpdated) : "時間不明";
      if (fetchState) fetchState.textContent = `${failure}；保留 ${lastSuccess} 的最後成功資料（可能已過期）`;
    } else {
      if (root) setHtml(root, `<div class="tw-public-empty">${esc(failure)}</div>`);
      if (summary) summary.textContent = "讀取失敗";
      if (fetchState) fetchState.textContent = failure;
    }
  } finally {
    if (twPublicMonitorAbortController === controller) twPublicMonitorAbortController = null;
    twPublicMonitorRefreshInFlight = false;
  }
}

function renderChart(data) {
  Chart.render({
    data,
    history: chartHistory,
    historyMatchesSelection: chartHistoryMatchesSelection(),
    historyPendingRevision: Boolean(chartHistory && chartHistoryKey !== chartRequestKey()),
    historyLoadError,
    historyInFlight,
    hiddenSeries: hiddenEquitySeries,
    selectedMode: selectedMode(),
    detailRangeKey: detailRangeKey(),
    isOvernight: IS_OVERNIGHT,
    strategyLabel,
    chartWindowLabel: chartWindowLabel(),
    formatNumber: sourceNumber,
    formatCount: number,
    pnlClass,
  });
}

function decodeChartHistory(payload) {
  return Chart.decodeHistory(payload);
}

function validateMinuteHistory(payload, {startDate, endDate, unbounded}) {
  const series = payload?.minute_series;
  if (payload?.history_encoding !== "minute_columns_v2"
    || !Array.isArray(payload.minute_axis)
    || !Array.isArray(payload.history)
    || !Array.isArray(payload.range_summary)
    || !Array.isArray(series)
    || !Number.isSafeInteger(payload.returned_points)
    || payload.returned_points < 0
    || (!unbounded && (payload.start_date !== startDate || payload.end_date !== endDate))
    || series.some((row) => !Array.isArray(row.minute_indexes)
      || !Array.isArray(row.return_pct)
      || !Array.isArray(row.cumulative_return_pct)
      || !Array.isArray(row.quality_flags)
      || row.minute_indexes.length !== row.return_pct.length
      || row.minute_indexes.length !== row.cumulative_return_pct.length
      || row.minute_indexes.length !== row.quality_flags.length)
    || series.reduce((sum, row) => sum + row.minute_indexes.length, 0) !== payload.returned_points) {
    throw new Error("分鐘曲線範圍或點數不完整");
  }
}

function applyChartHistory(payload, key) {
  chartHistory = payload;
  chartHistoryKey = key;
  historyLoadError = "";
  if (snapshot) {
    renderOverview(snapshot);
    renderModes(snapshot);
    renderBenchmarks(snapshot);
    renderChart(snapshot);
  }
}

async function loadChartHistory({preferCache = false} = {}) {
  if (document.hidden) return;
  const requestedRange = "all";
  const requestedStart = selectedDetailStartDate();
  const requestedEnd = selectedDetailEndDate();
  const requestedKey = chartRequestKey();
  const requestedScope = chartReadScopeKey();
  const cached = chartHistoryCache.get(requestedKey);
  if (preferCache) {
    if (cached && (chartHistory !== cached.payload || chartHistoryKey !== requestedKey)) {
      applyChartHistory(cached.payload, requestedKey);
    }
    else if (!chartHistoryMatchesSelection()) {
      chartHistory = null;
      chartHistoryKey = "";
      if (snapshot) renderChart(snapshot);
    } else if (snapshot) {
      // Keep the prior verified curve visible while the next minute arrives.
      // Its range summary is excluded from current accounting until refreshed.
      renderChart(snapshot);
    }
    // The key carries the exact ledger/history-source revision. An unchanged
    // key is immutable; a time-to-live only retransfers the same large curve.
    if (cached) return;
  }
  // New marks can arrive before a slow curve finishes. Coalesce that revision
  // into the next request; only another selection/owner or force cancels it.
  if (historyInFlight && historyRequestScopeInFlight === requestedScope && preferCache) return;
  if (historyAbortController) historyAbortController.abort();
  const controller = new AbortController();
  historyAbortController = controller;
  const sequence = ++historyRequestSequence;
  historyInFlight = true;
  historyRequestKeyInFlight = requestedKey;
  historyRequestScopeInFlight = requestedScope;
  let loaded = false;
  if (snapshot && !chartHistoryMatchesSelection()) renderChart(snapshot);
  try {
    const params = new URLSearchParams({range: requestedRange, resolution: "1m", encoding: "v2"});
    const coversAllAvailableDates = Boolean(
      requestedStart
      && requestedEnd
      && availableDetailDates.length
      && requestedStart <= availableDetailDates.at(-1)
      && requestedEnd >= availableDetailDates[0]
    );
    if (requestedStart && !coversAllAvailableDates) params.set("start_date", requestedStart);
    if (requestedEnd && !coversAllAvailableDates) params.set("end_date", requestedEnd);
    const response = await fetchMinuteHistory(`api/history?${params.toString()}`, {cache:"default", signal: controller.signal});
    if (!response.ok) { Dashboard.cancelResponse(response); throw new Error(`HTTP ${response.status}`); }
    const payload = await Dashboard.readJsonResponse(response, {expectedRoot: "object"});
    if (sequence !== historyRequestSequence) return;
    if (requestedScope !== chartReadScopeKey()) return;
    validateMinuteHistory(payload, {startDate: requestedStart, endDate: requestedEnd, unbounded: coversAllAvailableDates});
    if (!chartHistoryCache.has(requestedKey) && chartHistoryCache.size >= HISTORY_CLIENT_CACHE_MAX_ENTRIES) {
      chartHistoryCache.delete(chartHistoryCache.keys().next().value);
    }
    const decodedPayload = decodeChartHistory(payload);
    // A range enclosing all retained dates has exactly the unbounded points.
    // Reuse the startup-warmed projection without inventing earlier data;
    // retain the requested calendar boundaries and actual coverage separately.
    const decoded = coversAllAvailableDates
      ? {...decodedPayload, start_date: requestedStart, end_date: requestedEnd}
      : decodedPayload;
    chartHistoryCache.set(requestedKey, {payload: decoded});
    if (requestedKey === chartRequestKey() || chartHistoryKey !== chartRequestKey()) {
      applyChartHistory(decoded, requestedKey);
    }
    loaded = true;
  } catch (error) {
    if (sequence !== historyRequestSequence || requestedScope !== chartReadScopeKey()
        || (error?.name === "AbortError" && controller.signal.aborted)) return;
    historyLoadError = `歷史載入失敗：${error}`;
    if (snapshot) renderChart(snapshot);
    scheduleStatusRecovery();
  } finally {
    if (historyAbortController === controller) {
      historyAbortController = null;
      historyInFlight = false;
      historyRequestKeyInFlight = "";
      historyRequestScopeInFlight = "";
      if (loaded && requestedKey !== chartRequestKey()) void loadChartHistory({preferCache: true});
    }
  }
}

function renderPriceSources(data = snapshot) {
  if (!data || !$("price-source-list")) return;
  const positions = positionSelectionKey === detailSelectionKey("positions") ? positionRows : EMPTY_SOURCE_ROWS;
  const signals = signalSelectionKey === detailSelectionKey("signals") ? signalRows : EMPTY_SOURCE_ROWS;
  const events = eventSelectionKey === detailSelectionKey("events") ? eventRows : EMPTY_SOURCE_ROWS;
  const inputs = [data, positions, signals, events];
  if (inputs.every((input, index) => input === priceSourceInputs[index])) return;
  const view = detailComponents.priceSources({data, positions, signals, events});
  const quoteNotice = $("quote-display-notice");
  quoteNotice.textContent = detailComponents.priceNoticeText(data);
  quoteNotice.classList.toggle("hidden", !data.price_display_policy);
  setHtml("price-source-list", view.sourcesHtml);
  const referenceList = $("price-reference-list");
  const openKeys = new Set([...referenceList.querySelectorAll("li[data-reference-key]")]
    .filter(node => node.querySelector(".daily-reference-details[open]"))
    .map(node => node.dataset.referenceKey));
  setHtml(referenceList, view.referencesHtml);
  for (const node of referenceList.querySelectorAll("li[data-reference-key]")) {
    if (openKeys.has(node.dataset.referenceKey)) {
      const details = node.querySelector(".daily-reference-details");
      if (details) details.open = true;
    }
  }
  $("price-reference-count").textContent = number(view.referenceCount);
  priceSourceInputs = inputs;
}

function measureRevisionPaint(action, rows, rootId, stillCurrent = () => true) {
  if (!revisionPaintObserver || !appliedStatusRevision) return;
  const root = $(rootId);
  if (!root || !Dashboard.isElementVisible(root)) return;
  const seen = new Set();
  for (const row of rows) {
    const key = row.public_signal_key;
    if (!/^[a-f0-9]{32}$/.test(key || "") || seen.has(key)) continue;
    // Detail responses do not yet carry a committed ledger revision. Match
    // only the current logical signal and disclose that narrower proof; do
    // not label a historical/paginated row as this status commit's fills.
    const currentMode = (snapshot?.modes || []).find((mode) => mode.market === row.market);
    if (rootId !== "mode-cards" && currentMode?.public_signal_key !== key) continue;
    seen.add(key);
    const visibleNode = [...root.querySelectorAll(`[data-public-signal-key="${key}"]`)]
      .find((node) => Dashboard.isElementVisible(node));
    if (!visibleNode) continue;
    const countNode = rootId === "mode-cards" ? visibleNode.querySelector("[data-paper-entry-counts]") : null;
    const values = [row.entry_requested_shares, row.entry_filled_shares, row.entry_unfilled_shares];
    const countEvidenceIsCurrent = () => !!countNode && countNode.isConnected
      && Dashboard.isElementVisible(countNode)
      && values.every((value) => Number.isSafeInteger(value) && value >= 0)
      && [countNode.dataset.requested, countNode.dataset.filled, countNode.dataset.unfilled]
        .every((value, index) => value === String(values[index]));
    const entryCounts = countEvidenceIsCurrent() ? {
      entryCountsVisible: true, entryRequestedShares: values[0],
      entryFilledShares: values[1], entryUnfilledShares: values[2],
    } : null;
    revisionPaintObserver.applied({revision: appliedStatusRevision, action, market: row.market,
      sessionDate: row.session_date || snapshot.session_date, signalKey: key,
      updatePhase: appliedStatusPhase, entryCounts,
      viewEvidence: rootId === "mode-cards" ? "status_mode_snapshot" : "detail_signal_identity_only",
      stillCurrent: () => stillCurrent() && visibleNode.isConnected && Dashboard.isElementVisible(visibleNode)
        && (!entryCounts || countEvidenceIsCurrent())});
  }
}

function renderPositions() {
  const acceptedSelection = positionSelectionKey === detailSelectionKey("positions");
  const shared = !IS_OVERNIGHT ? window.StockAgentTwLivePaper?.getSnapshot() : null;
  const selection = {start: selectedDetailStartDate(), end: selectedDetailEndDate(),
    mode: selectedMode(), symbol: textFilter(), status: $("status-filter").value || "all"};
  const fallback = shared?.data && (!acceptedSelection || positionDataRevision !== detailDataRevision("positions"))
    ? detailComponents.livePositionFallback(shared?.data, selection) : null;
  positionFallbackActive = !!fallback;
  if (fallback) {
    const rows = fallback.rows.slice(0, positionFallbackVisible);
    const age = Math.max(0, Math.floor((Date.now() - Date.parse(shared.data.as_of)) / 1000));
    const freshness = `${shortTime(shared.data.as_of)} · 帳本版本 ${number(shared.data.state_revision)}`;
    const warning = shared.error || (age > 120 ? `快照已 ${number(age)} 秒未更新` : "");
    detailComponents.pagedTable({id: "position", rows, total: fallback.rows.length, loading: false,
      hasMore: rows.length < fallback.rows.length, error: positionLoadError,
      renderRow: detailComponents.positionRow,
      emptyText: fallback.note,
      countDetail: ` · ${fallback.note} · ${freshness}${warning ? ` · ${warning}` : ""}`});
    // Keep scope and clock visible even when the full-history request failed.
    $("position-count").textContent = `${number(rows.length)} / ${number(fallback.rows.length)} 筆 · ${fallback.note} · ${freshness}${warning ? ` · ${warning}` : ""}`;
    renderPriceSources();
    return;
  }
  detailComponents.pagedTable({
    id: "position", rows: acceptedSelection ? positionRows : [],
    total: acceptedSelection ? positionTotal : 0,
    loading: positionLoading || !acceptedSelection,
    hasMore: acceptedSelection && positionHasMore, error: positionLoadError,
    renderRow: detailComponents.positionRow,
    emptyText: acceptedSelection && positionDataRevision === detailDataRevision("positions")
      ? "目前沒有符合篩選的持倉" : "正在讀取符合篩選的持倉…",
  });
  if (!positionLoading && acceptedSelection && positionDataRevision === detailDataRevision("positions")) {
    const revision = positionDataRevision, selection = positionSelectionKey;
    measureRevisionPaint("positions_revision_paint", positionRows, "position-body",
      () => positionDataRevision === revision && positionSelectionKey === selection);
  }
  renderPriceSources();
}

function renderSignals() {
  syncFeaturePanelSelection();
  const acceptedSelection = signalSelectionKey === detailSelectionKey("signals");
  const shared = !IS_OVERNIGHT ? window.StockAgentPublishedSignals?.getSnapshot() : null;
  const selection = {start: selectedDetailStartDate(), end: selectedDetailEndDate(),
    mode: selectedMode(), symbol: textFilter(), status: $("status-filter").value || "all"};
  const ledgerReady = acceptedSelection && signalDataRevision === detailDataRevision("signals");
  const publishedAhead = shared?.data && ledgerReady
    && detailComponents.publishedSignalAhead(shared.data, selection, signalRows);
  const fallback = shared?.data
    ? detailComponents.publishedSignalFallback(shared.data, selection, {ledgerReady}) : null;
  signalFallbackActive = !!fallback;
  if (fallback) {
    const rows = fallback.rows.slice(0, signalFallbackVisible);
    const warning = shared.error ? ` · ${shared.error}` : "";
    setHtml("signal-direction-summary", `<div><span>最新已發布訊號</span><strong>${esc(fallback.note)}${esc(warning)}</strong></div>`);
    detailComponents.pagedTable({id: "signal", rows, total: fallback.rows.length, loading: false,
      hasMore: rows.length < fallback.rows.length, error: signalLoadError,
      renderRow: detailComponents.publishedSignalFallbackRow, emptyText: fallback.note,
      countDetail: ` · ${fallback.note}${warning}`});
    $("signal-count").textContent = `${number(rows.length)} / ${number(fallback.rows.length)} 筆 · ${fallback.note}${warning}`;
    renderPriceSources();
    return;
  }
  const visibleRows = acceptedSelection ? signalRows : [];
  const visibleSummary = acceptedSelection ? signalDirectionSummary : {};
  const visibleAudit = acceptedSelection ? signalOpeningExecutionAudit : {};
  const target = visibleSummary.target || {};
  const actual = visibleSummary.actual || {};
  const visiblePositions = positionSelectionKey === detailSelectionKey("positions") ? positionRows : [];
  const positionMap = new Map(visiblePositions.map((row) => [`${row.session_date}\u0000${row.market}\u0000${row.symbol}\u0000${row.signal_id || ""}`, row]));
  const modeMap = new Map((snapshot?.modes || []).map((mode) => [mode.market, mode]));
  const directionHtml = [
    ["區間訊號目標", target],
    [IS_OVERNIGHT ? "整張／隔夜放空守門後收盤成交" : "資格／整張／深度後實際成交", actual],
  ].map(([label, row]) => `<div><span>${esc(label)}</span><strong>${esc(directionPair(row))}</strong></div>`).join("");
  const openingAuditHtml = Object.entries(visibleAudit).map(([market, row]) => {
    const missing = Number(row.opening_price_missing_count || 0);
    const recorded = Number(row.model_signal_row_count || 0);
    const expected = Number(row.expected_model_signal_row_count || 0);
    const complete = row.model_signal_rows_complete === true;
    const belowLot = Number(row.below_one_board_lot_count || 0);
    const reasons = Object.entries(row.unfilled_reason_counts || {}).slice(0, 3)
      .map(([reason, count]) => `${signalReasonLabel(reason)}:${number(count)}`).join(" · ");
    const completeness = expected
      ? `模型決策列 ${number(recorded)}/${number(expected)}${complete ? " 完整" : " 缺漏"}`
      : `模型決策列 ${number(recorded)}`;
    return `<div><span>${esc(strategyLabel(market))} 完整訊號／執行稽核</span><strong class="${missing || (expected && !complete) ? "negative" : "positive"}">${completeness} · 非零訊號 ${number(row.nonzero_signal_count)} · 成交 ${number(row.filled_signal_count)}</strong><small>${belowLot ? `資金不足一張 ${number(belowLot)} 筆，完整訊號仍保留於下表` : esc(reasons || "全部非零訊號皆已成交")}</small></div>`;
  }).join("");
  const revisionHtml = (snapshot?.modes || []).filter((mode) =>
    mode.day_trade_signal_update_policy === "progressive_open_reference_rebalance_v1"
    && Number.isInteger(mode.opening_signal_revision?.index)).map((mode) => {
      const inventory = mode.inventory_rebalance;
      const progress = inventory?.contract_valid === true
        ? inventory.target_complete === true ? "實際庫存已達本版完整目標"
          : `庫存仍差 ${number(inventory.remaining_delta_shares)} 股 · ${number(inventory.unresolved_symbol_count)} 檔待取價`
        : "待核對完整庫存差額";
      return `<div><span>${esc(strategyLabel(mode.market))} 即時再平衡</span><strong>版本 ${number(mode.opening_signal_revision.index)} · ${esc(shortTime(mode.signal_at))}</strong><small>${progress}。新開盤價優先；缺價沿用可追溯前次成交價。僅交易目標差額，原成交保留。</small></div>`;
    }).join("");
  const publishedNoticeHtml = publishedAhead
    ? "<div><span>較新的模型預覽</span><strong>另列於上方最新訊號；此表保留已提交帳本版本，不把不同版本配成成交。</strong></div>"
    : "";
  setHtml("signal-direction-summary", directionHtml + openingAuditHtml + revisionHtml + publishedNoticeHtml);
  detailComponents.pagedTable({
    id: "signal", rows: visibleRows, total: acceptedSelection ? signalTotal : 0,
    loading: signalLoading || !acceptedSelection,
    hasMore: acceptedSelection && signalHasMore, error: signalLoadError,
    emptyText: acceptedSelection && signalDataRevision === detailDataRevision("signals")
      ? "目前沒有符合篩選的訊號" : "正在讀取符合篩選的訊號…",
    countDetail: acceptedSelection && signalLoadNotice ? ` · ${signalLoadNotice}` : "",
    renderRow: (row) => detailComponents.signalRow(row, {
      position: row.position || positionMap.get(`${row.session_date}\u0000${row.market}\u0000${row.symbol}\u0000${row.signal_id || ""}`),
      mode: modeMap.get(row.market), sessionDate: snapshot?.session_date,
      selectedKey: featurePanelSignalKey,
    }),
  });
  if (!signalLoading && acceptedSelection && signalDataRevision === detailDataRevision("signals")) {
    const revision = signalDataRevision, selection = signalSelectionKey;
    measureRevisionPaint("signals_revision_paint", signalRows, "signal-body",
      () => signalDataRevision === revision && signalSelectionKey === selection);
  }
  renderPriceSources();
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
    setHtml(body, "");
    scope.textContent = featureDriversSummaryText();
    empty.classList.remove("hidden");
    empty.textContent = "請點選「所有訊號」中的一列，載入該筆完整 feature 資料。";
    return;
  }

  const featureRecord = resolveSignalFeatureRecord(row);
  const drivers = featureRecord.drivers;
  if (featureRecord.feature_proxy_publication_allowed === false) {
    panel.classList.add("empty");
    setHtml(body, "");
    scope.textContent = `${strategyLabel(row)} ${row.symbol} ${row.session_date} · 僅公開模型衍生訊號`;
    empty.classList.remove("hidden");
    const researchNotice = featureRecord.research_only === true && featureRecord.historical_point_in_time_proven === false
      ? " 歷史資料未證明完整 point-in-time；回放績效不是可實現券商績效。" : "";
    empty.textContent = "此模式不公開私有原始訓練特徵或數值代理；模型分數、目標權重與模擬執行紀錄仍正常提供。這不是資料載入失敗。" + researchNotice;
    return;
  }
  if (!drivers.length) {
    panel.classList.add("empty");
    setHtml(body, "");
    scope.textContent = `${featureDriversSummaryText()}（${strategyLabel(row)} ${row.symbol} ${row.session_date} 無可對應特徵）`;
    empty.classList.remove("hidden");
    empty.textContent = "本筆訊號尚未讀到 summary，請稍後重整。";
    return;
  }

  panel.classList.remove("empty");
  const columns = sortedFeatureColumns(drivers);
  scope.textContent = `${row.session_date} ${strategyLabel(row)} ${row.symbol} · 共 ${drivers.length} 個 feature，欄位 ${columns.length} 欄`;
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
  setHtml(body, `<div class="feature-table-scroll"><table class="compact-table feature-table">
    <thead><tr>${head}</tr></thead>
    <tbody>${featureRows}</tbody></table></div>`);
  empty.classList.add("hidden");
}

function renderEvents() {
  const acceptedSelection = eventSelectionKey === detailSelectionKey("events");
  detailComponents.pagedTable({
    id: "event", rows: acceptedSelection ? eventRows : [],
    total: acceptedSelection ? eventTotal : 0,
    loading: eventLoading || (eventViewActivated && !acceptedSelection),
    hasMore: acceptedSelection && eventHasMore, error: eventLoadError,
    countDetail: acceptedSelection ? `（委託 ${number(eventOrderTotal)}／成交 ${number(eventFillTotal)}）${eventLoadNotice ? ` · ${eventLoadNotice}` : ""}` : "",
    renderRow: detailComponents.eventRow,
    emptyText: !eventViewActivated ? "捲動到本區時載入完整委託與成交事件。"
      : acceptedSelection && eventRecordRevision === detailDataRevision("events")
        ? "尚無委託／成交事件" : "正在讀取委託／成交事件…",
  });
  renderPriceSources();
}

function renderAudit(data) {
  const counts = data.record_counts || {};
  const items = IS_OVERNIGHT ? [
    ["進場交易日", data.session_date], ["模擬模式", data.simulation_only ? "是，正式下單不可能" : "否"],
    ["歷史反事實涵蓋", data.historical_replay?.start_date ? `${data.historical_replay.start_date} ～ ${data.historical_replay.end_date} · ${number(data.historical_replay.session_count)} 日` : "尚未發布"],
    ["13:25 缺值處理", data.historical_replay?.start_date ? `收盤替代 ${number(data.historical_replay.close_fallback_count)}／仍無計價 ${number(data.historical_replay.missing_1325_count)}` : "—"],
    ["歷史反事實訊號／事件／持倉", data.historical_replay?.start_date ? `${number(data.historical_replay.signal_count)} / ${number(data.historical_replay.event_count)} / ${number(data.historical_replay.position_count)}` : "—"],
    ["歷史訊號阻擋／前期未平", data.historical_replay?.start_date ? `${number(data.historical_replay.blocked_close_signal_count)} / ${number(data.historical_replay.unresolved_prior_position_count)}` : "—"],
    ["即時帳本訊號／委託／成交", [counts.signals, counts.orders, counts.fills].some((value) => value != null) ? `${number(counts.signals)} / ${number(counts.orders)} / ${number(counts.fills)}` : "明細按交易日讀取"],
    ["歷史反事實權益事件", number(data.historical_replay?.mark_count)],
    ...data.modes.map((mode) => [`${strategyLabel(mode)} checkpoint`, mode.checkpoint_ready ? `READY · ${mode.checkpoint_fingerprint || "fingerprint pending"}` : "MISSING"]),
    ...data.modes.map((mode) => [`${strategyLabel(mode)} 模型用途`, mode.model_trained_for_overnight === false ? "當沖 checkpoint 暫時轉接；非隔夜訓練" : "契約待確認"]),
    ...data.modes.map((mode) => [`${strategyLabel(mode)} 收盤／開盤狀態`, `${mode.entry_fill_outcome || "尚未進場"} · ${engineStatusLabel(mode.engine_status)}`]),
  ] : [
    ["交易日", data.session_date], ["模擬模式", data.simulation_only ? "是，正式下單不可能" : "否"],
    ["無人維護守護", `${data.unattended_guardian?.status || "missing"} · ${duration(data.unattended_guardian?.age_seconds)}前`],
    ["完整帳本累積訊號／委託／成交", `${number(counts.signals)} / ${number(counts.orders)} / ${number(counts.fills)}`], ["策略／即時基準／補登基準 mark", `${number(counts.marks)} / ${number(counts.benchmark_marks)} / ${number(counts.benchmark_history_marks)}`],
    ["狀態 API 視窗", Object.entries(data.payload_window || {}).map(([key, value]) => `${key}:${number(value)}`).join(" · ") || "—"],
    ...data.modes.map((mode) => [`${strategyLabel(mode)} checkpoint`, mode.checkpoint_ready ? `READY · ${mode.checkpoint_fingerprint || "fingerprint pending"}` : "MISSING"]),
    ...data.modes.map((mode) => [`${strategyLabel(mode)} 資格資料`, Object.entries(mode.eligibility_coverage || {}).map(([venue, row]) => `${venue}:${row.covered ? row.target_date : `缺 ${row.target_date} / latest ${row.latest_date || "—"}`}`).join(" · ") || "尚未載入"]),
    ...data.modes.map((mode) => [`${strategyLabel(mode)} 目前資格來源`, Object.entries(mode.current_eligibility_coverage || {}).map(([venue, row]) => `${venue}:${row.covered ? `READY ${row.target_date}` : `缺 ${row.target_date} / latest ${row.latest_date || "—"}`}`).join(" · ") || "尚未檢查"]),
    ...(data.benchmarks || []).map((row) => [`${row.label || row.benchmark_id}`, row.return_pct == null ? `等待可成交報價 · ${row.valuation_source || "尚未進場"}` : `${row.return_pct >= 0 ? "+" : ""}${sourceNumber(row.return_pct)}% · ${shortTime(row.entry_at)} 起 · 資金 ${money(row.initial_capital_twd)} · ${row.contract_code || row.symbol || ""}`]),
  ];
  setHtml("audit-grid", items.map(([label,value]) => `<div class="audit-item"><span>${esc(label)}</span><strong>${esc(value)}</strong></div>`).join(""));
  if (!IS_OVERNIGHT) return;
  const contract = data.source_contract || {};
  $("source-signal").textContent = contract.signal || "—"; $("source-replay").textContent = contract.replay || "—"; $("source-fill").textContent = contract.entry_fill || "—";
  $("source-fees").textContent = contract.fees || "—";
  $("source-comparison").textContent = `${contract.comparison || "—"}；${contract.benchmarks || "—"}；${contract.benchmark_history || "—"}`;
  $("source-eligibility").textContent = contract.eligibility || "—"; $("source-depth").textContent = `${contract.depth_limit || contract.queue || "—"}；${contract.bracket_fill || contract.simtrade || "—"}；${contract.terminal_flatten || "—"}`;
  $("source-exit").textContent = contract.exit_schedule || "—";
  $("source-latency").textContent = contract.latency || "—";
}

function revisionOf(data) {
  const counts = data.record_counts || {};
  const service = data.service_sync || {};
  return JSON.stringify([
    data.session_date,
    // Compact status deliberately omits full ledger counts. A new target can
    // leave equity and position counts unchanged (including no new fills).
    // Follow the canonical MATERIAL revision, never its heartbeat sequence,
    // so newly applied signals/rebalances cannot retain old mode-card DOM.
    service.engine_generation, service.content_revision,
    counts.signals, counts.orders, counts.fills, counts.marks, counts.benchmark_marks, counts.events,
    data.opening_signal_latency?.observed_mode_count,
    data.opening_signal_latency?.final_ready_ms,
    data.opening_signal_latency?.final_ledger_ms,
    data.opening_signal_latency?.ledger_observed_mode_count,
    data.opening_signal_latency?.first_source_ready_ms,
    data.opening_signal_latency?.source_ready_to_signal_p50_ms,
    data.opening_signal_latency?.failure_count,
    data.opening_signal_latency?.integrity,
    data.session_progress,
    data.preopen?.updated_at,
    data.unattended_guardian?.observed_at_taipei,
    data.modes.map((row) => [
      row.market, row.label, row.public_signal_key, row.opening_signal_revision?.index,
      row.total_equity_twd, row.open_position_count,
      row.stale_position_count, row.indicative_valuation_position_count, row.force_exit_failures,
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
  if (!heavy) {
    // A newer applied commit can cancel a pending double-rAF observation
    // without changing the cards' material content. Re-observe the existing
    // visible DOM; do not force heavy rendering or fabricate an SSE notice.
    measureRevisionPaint("status_revision_paint", snapshot.modes, "mode-cards");
    return;
  }
  renderOperations(snapshot);
  renderModes(snapshot);
  renderBenchmarks(snapshot);
  renderChart(snapshot);
  renderPositions(snapshot);
  renderSignals();
  renderEvents();
  renderAudit(snapshot);
  renderSignalFeaturePanel();
  measureRevisionPaint("status_revision_paint", snapshot.modes, "mode-cards");
}

async function loadSignals({append = false, force = false} = {}) {
  if (!snapshot) return;
  const requestRevision = detailDataRevision("signals");
  const requestSelection = detailSelectionKey("signals");
  const requestScope = detailReadScopeKey("signals");
  if (append && (signalSelectionKey !== requestSelection || signalDataRevision !== requestRevision)) append = false;
  if (!append && !force && (
    signalDataRevision === requestRevision
    || (signalLoading && signalRequestScopeInFlight === requestScope)
  )) return;
  if (signalAbortController) signalAbortController.abort();
  signalAbortController = new AbortController();
  const controller = signalAbortController;
  const sequence = ++signalRequestSequence;
  const requestRange = detailRangeKey();
  signalRequestRevisionInFlight = requestRevision;
  signalRequestScopeInFlight = requestScope;
  let loaded = false;
  const requestOffset = append ? signalRows.length : 0;
  const params = new URLSearchParams({
    start_date: selectedDetailStartDate(),
    end_date: selectedDetailEndDate(),
    mode: selectedMode(),
    symbol: $("symbol-filter").value.trim(),
    status: $("status-filter").value,
    hide_zero_targets: "1",
    revision_scope: selectedSignalRevisionScope(),
    offset: String(requestOffset),
    limit: String(SIGNAL_PAGE_SIZE),
  });
  signalLoading = true;
  beginSilentTableUpdate("signal-body", "load-more-signals", append);
  try {
    // A cross-session query may need to validate persistent historical
    // projections after a cold start. Keep the ordinary latest-day deadline
    // unchanged, while allowing the complete range to finish under contention.
    const timeoutMs = params.get("start_date") !== params.get("end_date") ? 30000 : 15000;
    const response = await fetchWithTimeout(`api/signals?${params.toString()}`, {cache: "no-store", signal: controller.signal, timeoutMs});
    const page = await Dashboard.readJsonResponse(response, {expectedRoot: "object"});
    if (sequence !== signalRequestSequence) return;
    if (requestRange !== detailRangeKey() || requestScope !== detailReadScopeKey("signals")) return;
    if (append && requestRevision !== detailDataRevision("signals")) { loaded = true; return; }
    validateDetailPage(page, {kind: "訊號", offset: requestOffset, limit: SIGNAL_PAGE_SIZE,
      expectedTotal: append ? signalTotal : null});
    if (page.revision_scope !== selectedSignalRevisionScope()) throw new Error("訊號版本範圍不符，保留前次結果");
    featurePanelScopeText = page.feature_drivers_scope
      ? `訊號頁面欄位 ${String(page.feature_drivers_scope)}`
      : "訊號頁面欄位 all_feature_drivers_if_available_else_top_feature_drivers";
    signalLoadError = "";
    signalLoadNotice = page.scan_limit_reached
      ? `已達 ${number(page.scan_limit)} 筆跨日掃描上限；縮小日期可查完整單日，事件曲線不受影響`
      : "";
    signalRows = append ? signalRows.concat(page.rows) : page.rows;
    signalRows.sort(compareByAbsoluteWeight);
    signalTotal = page.total;
    signalHasMore = page.has_more;
    signalDataRevision = requestRevision;
    signalSelectionKey = requestSelection;
    signalDirectionSummary = page.direction_summary || {};
    signalOpeningExecutionAudit = page.opening_execution_audit || {};
    const incomingDrivers = page.feature_drivers_by_signal || {};
    signalFeatureDrivers = append
      ? {...signalFeatureDrivers, ...incomingDrivers}
      : incomingDrivers;
    loaded = true;
  } catch (error) {
    if (sequence !== signalRequestSequence || requestScope !== detailReadScopeKey("signals")) return;
    // Only our own superseded selection is intentionally silent. A timeout
    // while reading the body may also surface as AbortError in browsers.
    if (error?.name === "AbortError" && controller.signal.aborted) return;
    signalLoadError = `訊號明細暫時無法更新：${error}`;
    signalLoadNotice = "";
    signalDataRevision = null;
    scheduleStatusRecovery();
  } finally {
    if (sequence === signalRequestSequence) {
      if (signalAbortController === controller) signalAbortController = null;
      signalRequestRevisionInFlight = "";
      signalRequestScopeInFlight = "";
      signalLoading = false;
      syncFeaturePanelSelection();
      renderSignals();
      renderSignalFeaturePanel();
      if (loaded && requestRevision !== detailDataRevision("signals")) void loadSignals();
    }
  }
}

async function loadPositions({append = false, force = false} = {}) {
  if (!snapshot) return;
  const requestRevision = detailDataRevision("positions");
  const requestSelection = detailSelectionKey("positions");
  const requestScope = detailReadScopeKey("positions");
  if (append && (positionSelectionKey !== requestSelection || positionDataRevision !== requestRevision)) append = false;
  if (!append && !force && (
    positionDataRevision === requestRevision
    || (positionLoading && positionRequestScopeInFlight === requestScope)
  )) return;
  if (positionAbortController) positionAbortController.abort();
  positionAbortController = new AbortController();
  const controller = positionAbortController;
  const sequence = ++positionRequestSequence;
  const requestRange = detailRangeKey();
  positionRequestRevisionInFlight = requestRevision;
  positionRequestScopeInFlight = requestScope;
  let loaded = false;
  const requestOffset = append ? positionRows.length : 0;
  const params = new URLSearchParams({
    start_date: selectedDetailStartDate(),
    end_date: selectedDetailEndDate(),
    mode: selectedMode(),
    symbol: $("symbol-filter").value.trim(),
    status: $("status-filter").value,
    offset: String(requestOffset),
    limit: String(POSITION_PAGE_SIZE),
  });
  positionLoading = true;
  beginSilentTableUpdate("position-body", "load-more-positions", append);
  try {
    const response = await fetchWithTimeout(`api/positions?${params.toString()}`, {cache: "no-store", signal: controller.signal});
    const page = await Dashboard.readJsonResponse(response, {expectedRoot: "object"});
    if (sequence !== positionRequestSequence) return;
    if (requestRange !== detailRangeKey() || requestScope !== detailReadScopeKey("positions")) return;
    if (append && requestRevision !== detailDataRevision("positions")) { loaded = true; return; }
    validateDetailPage(page, {kind: "持倉", offset: requestOffset, limit: POSITION_PAGE_SIZE,
      expectedTotal: append ? positionTotal : null});
    positionLoadError = "";
    positionRows = append ? positionRows.concat(page.rows) : page.rows;
    positionTotal = page.total;
    positionHasMore = page.has_more;
    positionDataRevision = requestRevision;
    positionSelectionKey = requestSelection;
    loaded = true;
  } catch (error) {
    if (sequence !== positionRequestSequence || requestScope !== detailReadScopeKey("positions")) return;
    if (error?.name === "AbortError" && controller.signal.aborted) return;
    positionLoadError = `持倉明細暫時無法更新：${error}`;
    positionDataRevision = null;
    scheduleStatusRecovery();
  } finally {
    if (sequence === positionRequestSequence) {
      if (positionAbortController === controller) positionAbortController = null;
      positionRequestRevisionInFlight = "";
      positionRequestScopeInFlight = "";
      positionLoading = false;
      renderPositions(snapshot);
      renderSignals();
      renderSignalFeaturePanel();
      if (loaded && requestRevision !== detailDataRevision("positions")) void loadPositions();
    }
  }
}

async function loadEvents({append = false, force = false} = {}) {
  if (!snapshot) return;
  const requestRevision = detailDataRevision("events");
  const requestSelection = detailSelectionKey("events");
  const requestScope = detailReadScopeKey("events");
  if (append && (eventSelectionKey !== requestSelection || eventRecordRevision !== requestRevision)) append = false;
  if (!append && !force && (
    eventRecordRevision === requestRevision
    || (eventLoading && eventRequestScopeInFlight === requestScope)
  )) return;
  if (eventAbortController) eventAbortController.abort();
  eventAbortController = new AbortController();
  const controller = eventAbortController;
  const sequence = ++eventRequestSequence;
  const requestRange = detailRangeKey();
  eventRequestRevisionInFlight = requestRevision;
  eventRequestScopeInFlight = requestScope;
  let loaded = false;
  const requestOffset = append ? eventRows.length : 0;
  const params = new URLSearchParams({
    start_date: selectedDetailStartDate(),
    end_date: selectedDetailEndDate(),
    mode: selectedMode(),
    symbol: $("symbol-filter").value.trim(),
    offset: String(requestOffset),
    limit: String(EVENT_PAGE_SIZE),
  });
  eventLoading = true;
  beginSilentTableUpdate("event-body", "load-more-events", append);
  try {
    const response = await fetchWithTimeout(`api/events?${params.toString()}`, {cache: "no-store", signal: controller.signal});
    const page = await Dashboard.readJsonResponse(response, {expectedRoot: "object"});
    if (sequence !== eventRequestSequence) return;
    if (requestRange !== detailRangeKey() || requestScope !== detailReadScopeKey("events")) return;
    if (append && requestRevision !== detailDataRevision("events")) { loaded = true; return; }
    validateDetailPage(page, {kind: "事件", offset: requestOffset, limit: EVENT_PAGE_SIZE,
      expectedTotal: append ? eventTotal : null});
    eventLoadError = "";
    eventRows = append ? eventRows.concat(page.rows) : page.rows;
    eventTotal = page.total;
    eventOrderTotal = page.order_total;
    eventFillTotal = page.fill_total;
    eventHasMore = page.has_more;
    eventLoadNotice = page.scan_limit_reached
      ? `僅統計最近每種帳本最多 ${number(page.scan_limit)} 筆，較早紀錄未計入` : "";
    eventRecordRevision = requestRevision;
    eventSelectionKey = requestSelection;
    loaded = true;
  } catch (error) {
    if (sequence !== eventRequestSequence || requestScope !== detailReadScopeKey("events")) return;
    if (error?.name === "AbortError" && controller.signal.aborted) return;
    eventLoadError = `事件明細暫時無法更新：${error}`;
    eventRecordRevision = null;
    scheduleStatusRecovery();
  } finally {
    if (sequence === eventRequestSequence) {
      if (eventAbortController === controller) eventAbortController = null;
      eventRequestRevisionInFlight = "";
      eventRequestScopeInFlight = "";
      eventLoading = false;
      renderEvents();
      if (loaded && requestRevision !== detailDataRevision("events")) void loadEvents();
    }
  }
}

async function refresh({force = false, pushedSnapshot = null} = {}) {
  if (document.hidden) return;
  if (refreshInFlight) {
    refreshQueued = true;
    refreshForceQueued = refreshForceQueued || force;
    if (pushedSnapshot) refreshPushedQueued = pushedSnapshot;
    return;
  }
  cancelStatusRecovery();
  if (force) {
    if (signalAbortController) signalAbortController.abort();
    if (positionAbortController) positionAbortController.abort();
    if (eventAbortController) eventAbortController.abort();
  }
  refreshInFlight = true;
  try {
    const started = performance.now();
    const date = selectedDate();
    let nextSnapshot = pushedSnapshot || await (async () => {
      const response = await fetchWithTimeout(`api/status${date ? `?date=${encodeURIComponent(date)}` : ""}`, {cache: "no-store"});
      return Dashboard.readJsonResponse(response, {expectedRoot: "object"});
    })();
    if (date !== selectedDate()) { refreshQueued = true; return; }
    // A pending HTTP response must not roll back a newer pushed commit. A
    // pushed latest-session DTO can never replace a user's historical scope.
    if (pushedSnapshot && date && date !== nextSnapshot.session_date) { refreshQueued = true; return; }
    const sync = nextSnapshot.service_sync || {};
    if (latestServiceNotice?.engine_generation && sync.engine_generation !== latestServiceNotice.engine_generation) {
      scheduleStatusRecovery();
      return;
    }
    if (appliedStatusRevision?.engine_generation && appliedStatusRevision.engine_generation === sync.engine_generation && (
      Number(sync.state_revision) < Number(appliedStatusRevision.state_revision) ||
      Number(sync.content_revision) < Number(appliedStatusRevision.content_revision))) {
      // A different status-query cache may predate an operational commit while
      // its complete financial/history/quote token is unchanged. Keep the newer
      // committed snapshot, but still fulfill a user's newly selected range.
      // Returning here strands the cleared tables/chart until the next minute.
      const retained = snapshot?.service_sync;
      if (retained && typeof sync.revision_token === "string" && sync.revision_token
          && retained.engine_generation === sync.engine_generation
          && retained.revision_token === sync.revision_token
          && Number.isSafeInteger(sync.content_revision)
          && retained.content_revision === sync.content_revision
          && snapshot.session_date === nextSnapshot.session_date
          && (!date || snapshot.session_date === date)) nextSnapshot = snapshot;
      else {
        scheduleStatusRecovery();
        return;
      }
    }
    if (!Array.isArray(nextSnapshot.modes)) throw new TypeError("Invalid dashboard mode snapshot");
    const previousRange = detailRangeKey();
    snapshot = nextSnapshot;
    if (appliedStatusRevision && ["revision_token", "engine_generation", "state_revision"]
      .some(key => appliedStatusRevision[key] !== nextSnapshot.service_sync?.[key])) appliedStatusPhase = "revision_change";
    appliedStatusRevision = {...nextSnapshot.service_sync};
    lastServiceRevision = String(snapshot.service_sync?.revision_token || lastServiceRevision || "");
    lastFetchMs = pushedSnapshot ? null : performance.now() - started;
    const sourceUpdatedAt = String(snapshot.source_updated_at || "");
    const sourceHasChanged = sourceUpdatedAt && sourceUpdatedAt !== lastSourceUpdatedAt;
    if (sourceHasChanged || force) {
      lastSourceUpdatedAt = sourceUpdatedAt || `${String(Date.now())}`;
      if (force) {
        signalDataRevision = null;
        eventRecordRevision = null;
        positionDataRevision = null;
      }
    }
    syncFilters(snapshot);
    if (detailRangeKey() !== previousRange) clearDateScopedViews();
    scheduleSessionRollover(snapshot.service_sync);
    const revision = revisionOf(snapshot);
    const heavy = revision !== lastRenderedRevision;
    lastRenderedRevision = revision;
    render({heavy});
    // A successful compact status is not recovery of an independently failed
    // table/history. Keep bounded backoff until those views succeed too.
    if (!signalLoadError && !positionLoadError && !historyLoadError
        && (!eventViewActivated || !eventLoadError)) statusRecoveryAttempts = 0;
    // First paint depends only on the compact status snapshot.  History and
    // the three detail tables are independent read-only views; loading them
    // after paint prevents a cold multi-session curve from holding the whole
    // dashboard blank.
    const shouldReloadSignals = force || signalDataRevision !== detailDataRevision("signals");
    const shouldReloadEvents = force || eventRecordRevision !== detailDataRevision("events");
    // The compact status intentionally omits position rows. Only the scoped
    // positions API owns the complete open + closed lifecycle table.
    const shouldReloadPositions = force || positionDataRevision !== detailDataRevision("positions");
    // Signals are the latency-critical view. A cold lossless minute history can
    // take seconds. Send signals first, then start the independent history;
    // neither a cold historical signal query nor a curve may gate the other.
    if (shouldReloadSignals) void loadSignals({force});
    if (shouldReloadPositions) void loadPositions({force});
    if (shouldReloadEvents && eventViewActivated) void loadEvents({force});
    void loadChartHistory({preferCache: !force});
  } catch (error) {
    const alert = $("alert"); alert.classList.remove("hidden");
    alert.textContent = `完整面板讀取失敗：${error}${IS_OVERNIGHT ? "" : "；上方訊號與持倉快照獨立顯示，歷史及曲線仍待恢復。"}`;
    $("health").textContent = "UNAVAILABLE"; $("health").className = "pill critical";
    renderLiveModeFallback();
    scheduleStatusRecovery();
  } finally {
    refreshInFlight = false;
    if (!document.hidden && twPublicMonitorActivated) void loadTwPublicMonitor();
    if (refreshQueued) {
      const queuedForce = refreshForceQueued;
      const queuedSnapshot = refreshPushedQueued;
      refreshQueued = false;
      refreshForceQueued = false;
      refreshPushedQueued = null;
      void refresh({force: queuedForce, pushedSnapshot: queuedSnapshot});
    }
  }
}

function cancelStatusRecovery() {
  if (statusRecoveryTimer === null) return;
  window.clearTimeout(statusRecoveryTimer);
  statusRecoveryTimer = null;
}

function scheduleStatusRecovery() {
  // One existing, coalesced recovery owner covers status and independent
  // detail/history failures, including an unchanged engine revision. Refresh
  // uses the CURRENT selection and reloads only invalid views; no page reload.
  // Failure recovery is separate from the minute-based price cadence. One
  // coalesced timer retries promptly, then backs off during prolonged faults.
  if (document.hidden || statusRecoveryTimer !== null) return;
  const delay = Math.min(250 * (2 ** Math.min(statusRecoveryAttempts++, 7)), 30000);
  statusRecoveryTimer = window.setTimeout(() => {
    statusRecoveryTimer = null;
    void refresh();
  }, delay);
}

function activateTwPublicMonitor() {
  if (twPublicMonitorActivated) return;
  twPublicMonitorActivated = true;
  const fetchState = $("tw-public-monitor-fetch-state");
  if (fetchState) fetchState.textContent = "正在載入台股公開資料狀態…";
  void loadTwPublicMonitor();
}

function installTwPublicMonitorActivation() {
  const target = $("tw-public-status") || $("tw-public-monitor-list");
  if (!target) return;
  if (typeof IntersectionObserver === "function") {
    const observer = new IntersectionObserver((entries) => {
      if (!entries.some((entry) => entry.isIntersecting)) return;
      observer.disconnect();
      activateTwPublicMonitor();
    }, {rootMargin: "700px 0px"});
    observer.observe(target);
    return;
  }
  const defer = window.requestIdleCallback
    ? (callback) => window.requestIdleCallback(callback, {timeout: 2500})
    : (callback) => window.setTimeout(callback, 1500);
  defer(activateTwPublicMonitor);
}

function activateEventView() {
  if (eventViewActivated) return;
  eventViewActivated = true;
  renderEvents();
  if (snapshot) void loadEvents();
}

function installEventViewActivation() {
  const target = $("event-body")?.closest("section");
  if (!target) return;
  if (typeof IntersectionObserver === "function") {
    const observer = new IntersectionObserver((entries) => {
      if (!entries.some((entry) => entry.isIntersecting)) return;
      observer.disconnect();
      activateEventView();
    }, {rootMargin: "600px 0px"});
    observer.observe(target);
    return;
  }
  const defer = window.requestIdleCallback
    ? (callback) => window.requestIdleCallback(callback, {timeout: 3000})
    : (callback) => window.setTimeout(callback, 1200);
  defer(activateEventView);
}

function acceptServiceRevision(serviceSync, {transport = "poll", statusExpected = false} = {}) {
  if (latestServiceNotice?.engine_generation && latestServiceNotice.engine_generation === serviceSync.engine_generation && (
    Number(serviceSync.state_revision) < Number(latestServiceNotice.state_revision) ||
    Number(serviceSync.content_revision) < Number(latestServiceNotice.content_revision))) return;
  latestServiceNotice = serviceSync;
  revisionPaintObserver?.notice(serviceSync, transport);
  scheduleSessionRollover(serviceSync);
  const revision = String(serviceSync.revision_token || "");
  // Compare against the last *applied status*, not the last notification.
  // The first request may legitimately get stale-while-rebuild; the ready
  // event (or fallback reconciliation) must still fetch its replacement.
  const changed = Boolean(lastServiceRevision && revision && revision !== lastServiceRevision);
  if (snapshot) {
    snapshot.service_sync = serviceSync;
    renderOperations(snapshot);
  }
  // A failed first status has no applied revision to compare. Recover from a
  // coherent notice too, without queueing a duplicate during its first fetch.
  const missingSnapshot = !snapshot && !refreshInFlight;
  // A verified-status stream may advertise a new commit before its projection
  // finishes or may lose that status frame. Keep one bounded recovery owner;
  // an arriving pushed snapshot cancels it in refresh().
  if ((changed || missingSnapshot) && statusExpected) scheduleStatusRecovery();
  if ((changed || missingSnapshot) && !statusExpected) void refresh();
}

function acceptServiceStatus(nextSnapshot) {
  if (document.hidden || !Array.isArray(nextSnapshot.modes) || !nextSnapshot.service_sync) return;
  if (nextSnapshot.service_sync.revision_token !== latestServiceNotice?.revision_token) return;
  const date = selectedDate();
  if (date && date !== nextSnapshot.session_date) { void refresh(); return; }
  void refresh({pushedSnapshot: nextSnapshot});
}

async function refreshServiceRevision() {
  if (document.hidden || revisionRefreshInFlight) return;
  revisionRefreshInFlight = true;
  try {
    const response = await fetchWithTimeout("api/revision", {cache: "no-store"});
    const serviceSync = await Dashboard.readJsonResponse(response, {expectedRoot: "object"});
    acceptServiceRevision(serviceSync);
  } catch (_error) {
    // The ordinary full refresh remains the fail-safe.  Do not replace the
    // last source-backed service state with an inferred client-side status.
  } finally {
    revisionRefreshInFlight = false;
  }
}

function filtersChanged({debounceSignals = false, includeChart = false, reloadEvents = true} = {}) {
  signalFallbackVisible = SIGNAL_PAGE_SIZE; positionFallbackVisible = POSITION_PAGE_SIZE;
  featurePanelSignalKey = "";
  featurePanelScopeText = "";
  signalFeatureDrivers = {};
  signalLoadError = positionLoadError = "";
  if (reloadEvents) eventLoadError = eventLoadNotice = "";
  if (includeChart && snapshot) renderChart(snapshot);
  renderSignalFeaturePanel();
  renderPositions();
  renderSignals();
  if (reloadEvents) renderEvents();
  window.clearTimeout(signalFilterTimer);
  if (debounceSignals) signalFilterTimer = window.setTimeout(() => {
    void loadPositions();
    void loadSignals();
    if (reloadEvents && eventViewActivated) void loadEvents();
  }, 80);
  else {
    void loadPositions();
    void loadSignals();
    if (reloadEvents && eventViewActivated) void loadEvents();
  }
}

$("mode-filter").addEventListener("change", () => filtersChanged({includeChart: true}));
function detailDateChanged(event) {
  followLatestSession = false;
  followPerformanceRange = false;
  window.clearTimeout(sessionRolloverTimer);
  sessionRolloverDeadline = "";
  const startInput = $("detail-start-date");
  const endInput = $("detail-end-date");
  if (startInput.value && endInput.value && startInput.value > endInput.value) {
    if (event.target === startInput) endInput.value = startInput.value;
    else startInput.value = endInput.value;
  }
  clearDateScopedViews();
  if (snapshot) syncFilters(snapshot);
  lastRenderedRevision = null;
  signalDataRevision = null;
  eventRecordRevision = null;
  positionDataRevision = null;
  featurePanelScopeText = "";
  featurePanelSignalKey = "";
  beginSilentTableUpdate("position-body", "load-more-positions", false);
  beginSilentTableUpdate("signal-body", "load-more-signals", false);
  beginSilentTableUpdate("event-body", "load-more-events", false);
  renderPositions();
  renderSignals();
  if (signalAbortController) signalAbortController.abort();
  if (eventAbortController) eventAbortController.abort();
  if (positionAbortController) positionAbortController.abort();
  if (historyAbortController) historyAbortController.abort();
  chartHistory = null;
  chartHistoryKey = "";
  historyLoadError = "";
  if (snapshot) renderChart(snapshot);
  // Native date pickers can commit the two boundaries separately. Coalesce
  // near-simultaneous edits so an obsolete range never starts a second set of
  // multi-ledger scans on the server.
  window.clearTimeout(dateFilterTimer);
  dateFilterTimer = window.setTimeout(() => {
    dateFilterTimer = null;
    void refresh();
  }, DATE_FILTER_DEBOUNCE_MS);
}
$("detail-start-date").addEventListener("change", detailDateChanged);
$("detail-end-date").addEventListener("change", detailDateChanged);
$("follow-latest-session")?.addEventListener("click", () => {
  followLatestSession = true;
  followPerformanceRange = false;
  lastFilterRevision = null;
  clearDateScopedViews();
  void refresh();
});
$("performance-range")?.addEventListener("click", () => {
  followLatestSession = false;
  followPerformanceRange = true;
  lastFilterRevision = null;
  clearDateScopedViews();
  void refresh();
});
$("status-filter").addEventListener("change", () => filtersChanged({reloadEvents: false}));
if (IS_OVERNIGHT) $("signal-revision-filter")?.closest("label")?.classList.add("hidden");
$("signal-revision-filter")?.addEventListener("change", () => filtersChanged({reloadEvents: false}));
$("symbol-filter").addEventListener("input", () => filtersChanged({debounceSignals: true}));
$("reset-filters").addEventListener("click", () => {
  $("mode-filter").value = "all";
  $("symbol-filter").value = "";
  $("status-filter").value = "all";
  if ($("signal-revision-filter")) $("signal-revision-filter").value = "latest";
  filtersChanged({includeChart: true});
});
$("load-more-signals").addEventListener("click", () => {
  if (signalFallbackActive) { signalFallbackVisible += SIGNAL_PAGE_SIZE; renderSignals(); }
  else void loadSignals({append: true});
});
$("load-more-events").addEventListener("click", () => loadEvents({append: true}));
$("load-more-positions").addEventListener("click", () => {
  if (positionFallbackActive) { positionFallbackVisible += POSITION_PAGE_SIZE; renderPositions(); }
  else void loadPositions({append: true});
});
if (!IS_OVERNIGHT) {
  // Projections already validated and fetched by the independent panels. A
  // failed full status or history request must not leave the tables blank.
  window.addEventListener("stockagent:published-signals", () => renderSignals());
  window.addEventListener("stockagent:live-paper", () => { renderPositions(); renderLiveModeFallback(); });
  if (window.StockAgentPublishedSignals?.getSnapshot().data) renderSignals();
  if (window.StockAgentTwLivePaper?.getSnapshot().data) { renderPositions(); renderLiveModeFallback(); }
}
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
function updateClock() {
  $("clock").textContent = new Date().toLocaleString("zh-TW", {timeZone:"Asia/Taipei", hour12:false});
}
Dashboard.scheduleRefresh(updateClock, {intervalMs: 1000});
if (IS_OVERNIGHT) {
  Dashboard.scheduleRefresh(() => refresh(), {intervalMs: PRICE_REFRESH_MS});
  Dashboard.scheduleRefresh(loadTwPublicMonitor, {intervalMs: TW_PUBLIC_STATUS_REFRESH_MS});
} else {
  // The user selected a one-minute page cadence. Consume the latest verified
  // version on that cadence; revision-by-revision full reloads can otherwise
  // race a busy writer and starve signals, positions and the minute curve.
  Dashboard.scheduleRefresh(() => refresh(), {
    intervalMs: PRICE_REFRESH_MS, cadenceFromLastStart: true, refreshOnVisible: "overdue",
  });
}
if (IS_OVERNIGHT) {
  Dashboard.subscribeRevisions("api/updates", acceptServiceRevision, refreshServiceRevision, {
    fallbackMs: SERVICE_REVISION_REFRESH_MS, onStatus: acceptServiceStatus,
  });
}
installTwPublicMonitorActivation();
installEventViewActivation();
for (const id of ["mode-cards", "signal-body", "position-body"]) {
  const target = $(id);
  if (target) Dashboard.observeVisibility(target, (visible) => {
    if (!visible || !snapshot) return;
    if (id === "mode-cards") measureRevisionPaint("status_revision_paint", snapshot.modes, id);
    else if (id === "signal-body") renderSignals();
    else renderPositions();
  });
}

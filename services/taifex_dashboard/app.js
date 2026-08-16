"use strict";

const PRICE_REFRESH_MS = 60000;
const FETCH_TIMEOUT_MS = 15000;
const money = new Intl.NumberFormat("zh-TW", { maximumFractionDigits: 0 });
const percent = new Intl.NumberFormat("zh-TW", { style: "percent", minimumFractionDigits: 2, maximumFractionDigits: 2 });
const ratio = new Intl.NumberFormat("zh-TW", { maximumFractionDigits: 2 });
const compactMoney = new Intl.NumberFormat("zh-TW", { notation: "compact", maximumFractionDigits: 2 });
const TIME_RANGES = {"1h": 3600e3, "1d": 86400e3, "1w": 7 * 86400e3, "1mo": 30 * 86400e3, "1q": 90 * 86400e3, "1y": 365 * 86400e3, all: Infinity};
const TIME_RANGE_LABELS = {"1h": "1 小時", "1d": "1 天", "1w": "1 週", "1mo": "1 月", "1q": "1 季", "1y": "1 年", all: "全部"};
const timeAxis = window.StockAgentTimeAxis;
const TAIFEX_SESSIONS = [
  {label: "夜收", minute: 5 * 60},
  {label: "日開", minute: 8 * 60 + 45},
  {label: "日收", minute: 13 * 60 + 45},
  {label: "夜開", minute: 15 * 60},
];
const HISTORY_CLIENT_CACHE_MS = 45000;
let selectedStrategy = "";
let selectedCategory = "";
let selectedDirectionalExposure = "";
let selectedVolatilityExposure = "";
let selectedHedgeType = "";
let selectedSort = "fixed_capital_return";
let sortDescending = true;
let lastSnapshot = null;
let cachedHistory = [];
let cachedHistoryMeta = null;
let curveVisibleCount = 12;
let guideVisibleCount = 12;
let strategySearch = "";
let lastStrategyCatalog = [];
let lastStrategyCounts = null;
let lastHeavyRevision = "";
let refreshInFlight = false;
let historyInFlight = false;
let lastHistoryEtag = "";
let historyPayloadCache = new Map();
let historyRetryTimer = null;
let strategySearchFrame = null;
let selectedTimeRange = "1d";

try { selectedTimeRange = localStorage.getItem("taifex-equity-time-range") || "1d"; } catch (_error) { /* storage can be disabled */ }
if (!(selectedTimeRange in TIME_RANGES)) selectedTimeRange = "1d";

function byId(id) { return document.getElementById(id); }
function setText(id, value) { byId(id).textContent = value ?? "—"; }
async function fetchWithTimeout(path, options = {}) {
  const controller = new AbortController();
  const timer = window.setTimeout(
    () => controller.abort(new DOMException("Request timed out", "TimeoutError")),
    FETCH_TIMEOUT_MS,
  );
  try { return await fetch(path, {...options, signal: controller.signal}); }
  finally { window.clearTimeout(timer); }
}
function formatTwd(value) {
  if (value == null || !Number.isFinite(Number(value))) return "—";
  return `${money.format(Number(value))} TWD`;
}
function formatCompactTwd(value) {
  if (value == null || !Number.isFinite(Number(value))) return "—";
  return `${compactMoney.format(Number(value))} TWD`;
}
function formatPercent(value) {
  if (value == null || !Number.isFinite(Number(value))) return "—";
  return percent.format(Number(value));
}
function formatRatio(value) {
  if (value == null || !Number.isFinite(Number(value))) return "—";
  return `${ratio.format(Number(value))}×`;
}
function localTime(value) {
  if (!value) return "—";
  return new Intl.DateTimeFormat("zh-TW", {
    month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit",
    hour12: false, timeZone: "Asia/Taipei"
  }).format(new Date(value));
}

function historyTimeMs(row) {
  const nanoseconds = Number(row?.decision_ts_ns);
  if (Number.isFinite(nanoseconds) && nanoseconds > 0) return nanoseconds / 1e6;
  const parsed = new Date(row?.recorded_at_utc || "").getTime();
  return Number.isFinite(parsed) ? parsed : null;
}

function filterHistoryByRange(history) {
  const rows = (Array.isArray(history) ? history : []).filter((row) => historyTimeMs(row) != null);
  if (!rows.length || selectedTimeRange === "all") return rows;
  const anchor = Math.max(...rows.map(historyTimeMs));
  const cutoff = anchor - TIME_RANGES[selectedTimeRange];
  return rows.filter((row) => historyTimeMs(row) >= cutoff);
}

function syncTimeRangeControl() {
  byId("equity-time-range").querySelectorAll("button[data-range]").forEach((button) => {
    button.setAttribute("aria-pressed", String(button.dataset.range === selectedTimeRange));
  });
}

function formatAge(value) {
  const seconds = Number(value);
  if (!Number.isFinite(seconds)) return "—";
  if (seconds < 60) return `${Math.max(0, seconds).toFixed(1)} 秒`;
  if (seconds < 3600) return `${ratio.format(seconds / 60)} 分鐘`;
  return `${ratio.format(seconds / 3600)} 小時`;
}

function engineLabel(value) {
  const labels = {
    intraday_active: "日內策略運作中",
    intraday_flat_for_day_close: "日盤已平倉",
    waiting_for_bootstrap: "等待新完整週期",
    waiting: "等待合法交易時段",
    active: "策略運作中",
  };
  return labels[value] || String(value || "未知狀態").replaceAll("_", " ");
}

function parityStateLabel(value) {
  const labels = {
    waiting_for_same_expiry_monthly_books: "等待同到期月五檔",
    waiting_for_engine_contract_v8: "等待夜盤引擎啟動",
    waiting_for_continuous_market: "等待連續交易",
    no_positive_edge_after_cost: "成本後尚無正套利",
    signal_pending_next_books: "等待訊號後新報價",
    signal_cancelled_after_next_book_recheck: "新報價複核後取消",
    locked_until_official_settlement: "套利已鎖定",
    entry_closed_for_expiry_settlement: "到期日前停止進場",
    settled_waiting_next_monthly_contract: "已結算，等待次月",
    blocked_missing_official_final_settlement: "等待官方結算價",
    forced_flat_until_next_monthly_contract: "強制平倉，等待次月",
    unsupported_underlying_product: "需使用 TX",
  };
  return labels[value] || String(value || "未知狀態").replaceAll("_", " ");
}

function signedContracts(value, label) {
  const quantity = Number(value);
  if (!Number.isFinite(quantity) || quantity === 0) return `${label} 0`;
  return `${label} ${quantity > 0 ? "多" : "空"} ${money.format(Math.abs(quantity))}`;
}

function healthPresentation(snapshot) {
  if (["active", "ready"].includes(snapshot.health)) return {label: "資料正常", state: "active"};
  if (snapshot.health === "degraded") return {label: "估值不完整", state: "degraded"};
  if (snapshot.health === "waiting" && !snapshot.source_fresh_expected) {
    return {label: "休市監控", state: "waiting"};
  }
  if (snapshot.health === "waiting") return {label: "等待資料", state: "waiting"};
  if (snapshot.health === "stale") return {label: "資料逾時", state: "blocked"};
  if (snapshot.health === "blocked") return {label: "策略阻擋", state: "blocked"};
  return {label: "狀態未知", state: "waiting"};
}

function healthMessage(snapshot) {
  if (snapshot.health === "blocked") return `引擎已阻擋：${snapshot.blocked_reason || "原因未記錄"}`;
  if (snapshot.health === "degraded") {
    const market = snapshot.market || {};
    return `行情仍在更新，但只有 ${market.strategy_timely_valuation_count || 0} / ${market.strategy_count || 0} 條策略具備 15 秒內的新鮮或明確 CARRIED 可成交估值；其餘曲線暫停在最後可信點。`;
  }
  if (snapshot.health === "stale") return `資料來源已逾時：最後一筆狀態距今 ${formatAge(snapshot.source_age_seconds)}。畫面仍可供歷史查閱，但不能當成現在行情。`;
  if (snapshot.health === "waiting" && !snapshot.source_fresh_expected) {
    return `排程監控中：${snapshot.current_market_phase} 為交易所休市空窗；下一個合法收單／撮合時段會自動恢復行情與策略。`;
  }
  if (snapshot.engine_status === "waiting_for_bootstrap") {
    return `正常等待：安全規則禁止補建已開始的週期，需等 ${snapshot.bootstrap_after_date} 結算後的新完整週期。`;
  }
  if (snapshot.health === "active") return "週期已開啟；策略、行情與理想帳持續更新。";
  return `引擎狀態正常：${engineLabel(snapshot.engine_status)}。`;
}

function renderHeader(snapshot) {
  const dot = byId("live-dot");
  dot.className = "live-dot";
  const presentation = healthPresentation(snapshot);
  const unhealthy = ["blocked", "degraded"].includes(presentation.state);
  dot.classList.add(presentation.state);
  byId("connection-status").dataset.state = presentation.state;
  setText("live-label", presentation.label);
  setText("last-refresh", `來源更新 ${localTime(snapshot.source_updated_at_utc)}`);
  const liveCount = snapshot.strategy_counts?.live_ideal ?? snapshot.strategies.length;
  setText("dashboard-title", `TAIFEX ${liveCount} 策略模擬交易即時面板`);
  setText("strategy-table-title", `${liveCount} 個獨立策略帳本`);
  const alert = byId("alert");
  alert.className = `alert ${presentation.state === "blocked" ? "blocked" : presentation.state === "degraded" ? "degraded" : "ready"}`;
  alert.textContent = healthMessage(snapshot);
}

function renderMetrics(snapshot) {
  const market = snapshot.market;
  const cycle = snapshot.active_cycle;
  const broker = snapshot.broker;
  setText("engine-status", engineLabel(snapshot.engine_status));
  byId("engine-status").title = snapshot.engine_status || "";
  setText("blocked-reason", snapshot.blocked_reason ? `阻擋原因：${snapshot.blocked_reason}` : "目前沒有阻擋");
  setText("book-coverage", percent.format(market.strategy_timely_valuation_coverage_ratio || 0));
  setText("book-count", `近期可用 ${market.strategy_timely_valuation_count || 0}/${market.strategy_count || 0} · fresh ${market.strategy_fresh_valuation_count || 0} + CARRIED≤15秒 ${market.strategy_recent_carried_valuation_count || 0} · 持倉腿 ${market.held_option_book_count || 0}/${market.held_option_contract_count || 0} · 一般訂閱 ${market.latest_book_count || 0}/${market.expected_book_count || 0}`);
  setText("cycle-status", cycle ? "週期進行中" : "目前空手");
  setText("cycle-expiry", cycle ? `到期 ${cycle.expiry_date || "—"}` : "目前無持倉週期");
  setText("broker-status", broker.order_failures === 0 ? "無失敗" : `${broker.order_failures} 次失敗`);
  setText("broker-detail", `${broker.inflight_order_count} 筆處理中 · 正式單=${broker.orders_enabled ? "啟用" : "停用"}`);
  setText("source-age", formatAge(snapshot.source_age_seconds));
  const safe = snapshot.simulation_only && !snapshot.production_order_possible;
  setText("safety-state", safe ? "只讀模擬" : "安全契約失敗");
}

function renderParity(snapshot) {
  const parity = snapshot.put_call_parity_tx || {};
  const state = parity.state || "waiting_for_same_expiry_monthly_books";
  const pill = byId("parity-state-pill");
  pill.className = "pill";
  if (state === "locked_until_official_settlement") pill.classList.add("valid");
  else if (state.startsWith("blocked_") || state.startsWith("forced_")) pill.classList.add("invalid");
  else pill.classList.add("stale");
  pill.textContent = parityStateLabel(state);

  const net = parity.locked_net_edge_after_estimated_cost_twd
    ?? parity.net_after_estimated_cost_twd;
  setText("parity-net-edge", formatTwd(net));
  setText("parity-threshold", `進場門檻 > ${formatTwd(parity.minimum_net_edge_twd ?? 0)} · 融資利率 ${(Number(parity.financing_interest_rate || 0) * 100).toFixed(2)}%`);
  setText("parity-gross-cost", `${formatTwd(parity.gross_locked_edge_twd)} / ${formatTwd(parity.total_estimated_cost_twd)}`);
  const directions = {
    sell_rich_synthetic_buy_tx: "賣貴的合成期貨／買 TX",
    buy_cheap_synthetic_sell_tx: "買便宜合成期貨／賣 TX",
  };
  setText("parity-direction", directions[parity.direction] || "尚無可成交候選");
  setText("parity-package", [
    signedContracts(parity.call_contracts, "Call"),
    signedContracts(parity.put_contracts, "Put"),
    signedContracts(parity.future_contracts, "TX"),
  ].join(" · "));
  const strike = parity.strike == null ? "—" : money.format(parity.strike);
  setText("parity-contracts", `${parity.series || "—"} · K ${strike} · ${parity.expiry_date || "—"}`);
  setText("parity-prices", `C ${parity.call_code || "—"}@${formatRatio(parity.call_price)} · P ${parity.put_code || "—"}@${formatRatio(parity.put_price)} · TX ${parity.future_code || "—"}@${formatRatio(parity.future_price)}`);
  setText("parity-causal-state", parityStateLabel(state));
  const counts = `掃描 ${parity.scanned_pair_count ?? 0} 組 · 可評估方向 ${parity.evaluable_direction_count ?? 0}`;
  const age = parity.maximum_book_age_ms == null ? "" : ` · 最老報價 ${ratio.format(Number(parity.maximum_book_age_ms))} ms`;
  const wait = parity.signal_wait_seconds == null ? "" : ` · 已等 ${formatAge(parity.signal_wait_seconds)}`;
  setText("parity-book-age", `${counts}${age}${wait}`);
}

function renderCycle(snapshot) {
  const market = snapshot.market;
  const cycle = snapshot.active_cycle || {};
  const marginSchedule = market.official_margin_schedule || {};
  const maintenance = marginSchedule.maintenance || {};
  const clearing = marginSchedule.clearing || {};
  const maintenanceTxo = maintenance.txo_risk_margin_twd || {};
  const clearingTxo = clearing.txo_risk_margin_twd || {};
  setText("underlying-contract", market.underlying_contract);
  setText("hedge-contract", `${market.hedge_product || "—"} · ${market.hedge_contract || "—"} · ×${market.hedge_multiplier_twd_per_point || "—"}`);
  setText("option-risk-margins", `${formatTwd(market.option_risk_margin_a_twd)} / ${formatTwd(market.option_risk_margin_b_twd)} / ${formatTwd(market.option_risk_margin_c_twd)} · ${market.option_risk_margin_effective_trading_date || "—"} 起 · C 僅作組合式參考，本帳逐腿裸賣計提`);
  setText("futures-initial-margin", `套利 ${market.underlying_product || "TX"} 每口 ${formatTwd(market.underlying_initial_margin_per_contract_twd)}；避險 ${market.hedge_product || "—"} 每口 ${formatTwd(market.futures_initial_margin_per_contract_twd)}`);
  setText("maintenance-clearing-margin", `${market.hedge_product || "—"} 維持 ${formatTwd(maintenance.futures_twd)}／結算 ${formatTwd(clearing.futures_twd)}；TXO 維持 A/B/C ${formatTwd(maintenanceTxo.A)} / ${formatTwd(maintenanceTxo.B)} / ${formatTwd(maintenanceTxo.C)}；結算 ${formatTwd(clearingTxo.A)} / ${formatTwd(clearingTxo.B)} / ${formatTwd(clearingTxo.C)}`);
  setText("capital-buffer-multiple", market.strategy_capital_buffer_multiple == null ? "—" : `×${market.strategy_capital_buffer_multiple}`);
  setText("cycle-series", cycle.series || "flat");
  setText("strategy-mode", snapshot.strategy_mode || "—");
  setText("catalog-entry-policy", snapshot.catalog_expansion_entry_policy || "next_cycle");
  setText("current-session", snapshot.current_market_phase || snapshot.current_session || "—");
  setText("runner-mode", snapshot.runner_mode || "—");
  setText("day-official-session", "08:30–08:45 盤前 · 08:45–13:45 連續");
  setText("intersession-gap", "13:45–14:50 休市監控");
  setText("night-official-session", "14:50–15:00 盤前 · 15:00–05:00 夜盤");
  setText("current-trading-date", snapshot.current_trading_date || "—");
  setText("decision-interval", snapshot.intraday_decision_interval_seconds == null ? "—" : `${snapshot.intraday_decision_interval_seconds} 秒`);
  setText("day-session-times", `${snapshot.intraday_entry_cutoff || "—"} / ${snapshot.intraday_flatten_time || "—"}`);
  setText("night-session-times", `${snapshot.night_entry_cutoff || "—"} / ${snapshot.night_flatten_time || "—"}`);
  setText("cycle-strike", cycle.strike == null ? "—" : money.format(cycle.strike));
  setText("detail-expiry", cycle.expiry_date || "—");
  setText("pending-targets", `${snapshot.pending_targets.length}`);
  setText("bootstrap-date", snapshot.bootstrap_after_date || "—");
  const receipt = snapshot.api_round_trip;
  setText("api-round-trip", receipt ? `${receipt.result} · ${receipt.logical_contract}→${receipt.resolved_contract} · final=${receipt.final_position} · ${localTime(receipt.finished_at_utc)}` : "尚無 receipt");
}

function renderStrategySelector(strategies) {
  const select = byId("strategy-select");
  const ids = strategies.map((row) => row.strategy_id);
  if (!selectedStrategy || !ids.includes(selectedStrategy)) selectedStrategy = ids[0] || "";
  if (select.options.length !== strategies.length) {
    select.replaceChildren();
    for (const row of strategies) {
      const option = document.createElement("option");
      option.value = row.strategy_id;
      option.textContent = row.label;
      select.appendChild(option);
    }
  }
  select.value = selectedStrategy;
}

function compareValues(left, right, key) {
  const a = left[key], b = right[key];
  if (a == null && b == null) return left.label.localeCompare(right.label, "zh-Hant");
  if (a == null) return 1;
  if (b == null) return -1;
  if (typeof a === "number" && typeof b === "number") return a - b;
  return String(a).localeCompare(String(b), "zh-Hant");
}

function sortedStrategies(strategies) {
  return [...strategies].sort((left, right) => {
    const compared = compareValues(left, right, selectedSort);
    return sortDescending ? -compared : compared;
  });
}

function matchesExposureFilters(row) {
  return (
    (!selectedDirectionalExposure || row.directional_exposure === selectedDirectionalExposure)
    && (!selectedVolatilityExposure || row.volatility_exposure === selectedVolatilityExposure)
    && (!selectedHedgeType || row.hedge_type === selectedHedgeType)
  );
}

function filteredStrategies(strategies) {
  return strategies.filter(matchesExposureFilters);
}

function populateFilter(id, rows, codeKey, labelKey, emptyLabel, selectedValue) {
  const select = byId(id);
  const labels = new Map();
  for (const row of rows) {
    if (row[codeKey]) labels.set(row[codeKey], row[labelKey] || row[codeKey]);
  }
  const options = [
    ["", emptyLabel],
    ...[...labels.entries()].sort((a, b) => a[1].localeCompare(b[1], "zh-Hant"))
  ];
  const signature = JSON.stringify(options);
  if (select.dataset.signature !== signature) {
    select.replaceChildren();
    for (const [value, label] of options) {
      const option = document.createElement("option");
      option.value = value;
      option.textContent = label;
      select.appendChild(option);
    }
    select.dataset.signature = signature;
  }
  select.value = labels.has(selectedValue) ? selectedValue : "";
}

function renderExposureControls(catalog) {
  populateFilter(
    "exposure-direction-filter", catalog, "directional_exposure",
    "directional_exposure_label", "全部方向曝險", selectedDirectionalExposure
  );
  populateFilter(
    "exposure-volatility-filter", catalog, "volatility_exposure",
    "volatility_exposure_label", "全部波動曝險", selectedVolatilityExposure
  );
  populateFilter(
    "exposure-hedge-filter", catalog, "hedge_type",
    "hedge_type_label", "全部避險型態", selectedHedgeType
  );
}

function renderExposureSummary(summary) {
  const grid = byId("exposure-summary");
  grid.replaceChildren();
  const dimensions = [
    ["directional_exposure", "方向曝險"],
    ["volatility_exposure", "波動曝險"],
    ["hedge_type", "避險型態"]
  ];
  for (const [key, label] of dimensions) {
    const block = document.createElement("div");
    const title = document.createElement("strong");
    title.textContent = label;
    const values = document.createElement("p");
    const rows = summary?.live?.[key] || [];
    values.textContent = rows.length
      ? rows.map((row) => `${row.label} ${row.count}`).join(" · ")
      : "尚無即時策略";
    block.append(title, values);
    grid.appendChild(block);
  }
}

function renderPerformanceSummary(summary) {
  summary = summary || {};
  setText("performance-count", `${summary.valid_return_count ?? 0} / ${summary.strategy_count ?? 0}`);
  setText("performance-reserve", formatCompactTwd(summary.independent_strategy_reserved_capital_twd));
  setText("performance-median", formatPercent(summary.median_fixed_capital_return));
  const best = summary.best_strategy;
  const worst = summary.worst_strategy;
  setText("performance-best", best ? formatPercent(best.fixed_capital_return) : "—");
  setText("performance-best-name", best?.label || "—");
  setText("performance-worst", worst ? formatPercent(worst.fixed_capital_return) : "—");
  setText("performance-worst-name", worst?.label || "—");
  setText("performance-cost", formatCompactTwd(summary.independent_strategy_explicit_cost_twd));
}

function appendTableMetric(cell, label, value, fullValue = "") {
  const line = document.createElement("span");
  line.className = "table-metric";
  const term = document.createElement("span");
  term.textContent = label;
  const detail = document.createElement("strong");
  detail.textContent = value;
  if (fullValue && fullValue !== value) detail.title = fullValue;
  line.append(term, detail);
  cell.appendChild(line);
}

function appendTableTwd(cell, label, value) {
  appendTableMetric(cell, label, formatCompactTwd(value), formatTwd(value));
}

function strategyStatusPill(row) {
  const pill = document.createElement("span");
  if (!row.alive) {
    pill.className = "pill invalid";
    pill.textContent = "RUINED";
    pill.title = `absorbing ruin · margin calls=${row.margin_call_count}`;
  } else if (row.forced_liquidation_pending || Number(row.margin_excess_twd) < 0) {
    pill.className = "pill invalid";
    pill.textContent = "MARGIN";
    pill.title = `保證金不足；margin calls=${row.margin_call_count}`;
  } else if (!row.valuation_available) {
    pill.className = "pill invalid";
    pill.textContent = "UNAVAILABLE";
  } else if (row.valuation_stale) {
    pill.className = "pill stale";
    pill.textContent = "CARRIED";
    pill.title = `缺報價，延用上一筆完整估值；age=${Number(row.valuation_age_seconds || 0).toFixed(1)}s`;
  } else {
    pill.className = "pill valid";
    pill.textContent = "FRESH";
  }
  return pill;
}

function renderTable(strategies) {
  const body = byId("strategy-body");
  body.replaceChildren();
  const visible = filteredStrategies(strategies);
  setText("strategy-table-title", `${visible.length} / ${strategies.length} 個獨立策略帳本`);
  for (const row of sortedStrategies(visible)) {
    const tr = document.createElement("tr");
    const title = document.createElement("td");
    title.className = "strategy-summary-cell";
    title.dataset.label = "策略／狀態";
    const titleLine = document.createElement("span");
    titleLine.className = "strategy-title-line";
    const name = document.createElement("strong");
    name.textContent = row.label;
    titleLine.append(name, strategyStatusPill(row));
    title.appendChild(titleLine);
    const category = document.createElement("span");
    category.className = "strategy-category";
    category.textContent = row.category || "—";
    title.appendChild(category);
    const id = document.createElement("span");
    id.className = "strategy-id";
    id.textContent = `${row.strategy_id} · ${row.implementation_level}`;
    title.appendChild(id);
    tr.appendChild(title);

    const exposure = document.createElement("td");
    exposure.className = "strategy-detail-cell exposure-cell";
    exposure.dataset.label = "曝險／口數比";
    appendTableMetric(exposure, "方向", row.directional_exposure_label || "—");
    appendTableMetric(exposure, "波動", row.volatility_exposure_label || "—");
    appendTableMetric(exposure, "避險", row.hedge_type_label || "—");
    appendTableMetric(exposure, "口數比", `設 ${row.design_option_ratio_label || "—"} · 實 ${row.live_option_ratio_label || "—"}`);
    tr.appendChild(exposure);

    const returns = document.createElement("td");
    returns.className = "strategy-detail-cell";
    returns.dataset.label = "報酬";
    appendTableMetric(returns, "固定", formatPercent(row.fixed_capital_return));
    appendTableMetric(returns, "複利*", formatPercent(row.compounded_return_to_live_mark));
    appendTableMetric(returns, "損益／成本", formatRatio(row.net_pnl_to_explicit_cost_ratio));
    tr.appendChild(returns);

    const pnl = document.createElement("td");
    pnl.className = "strategy-detail-cell";
    pnl.dataset.label = "損益／成本";
    appendTableTwd(pnl, "單位淨損益", row.one_unit_net_pnl_twd);
    appendTableTwd(pnl, "損益絕對值", row.one_unit_net_pnl_abs_twd);
    appendTableTwd(pnl, "顯性成本", row.explicit_cost_twd);
    tr.appendChild(pnl);

    const capital = document.createElement("td");
    capital.className = "strategy-detail-cell";
    capital.dataset.label = "資金／保證金";
    appendTableTwd(capital, "預留", row.reserved_capital_twd);
    appendTableTwd(capital, "總權益", row.total_equity_twd);
    appendTableTwd(capital, "保證金", row.margin_required_twd);
    appendTableMetric(capital, "占用", formatPercent(row.margin_utilization));
    appendTableTwd(capital, "餘裕", row.margin_excess_twd);
    tr.appendChild(capital);

    const position = document.createElement("td");
    position.className = "strategy-detail-cell";
    position.dataset.label = "部位／估值";
    appendTableTwd(position, "清算價值", row.open_liquidation_value_twd);
    appendTableMetric(position, "選擇權腿", money.format(row.option_position_count));
    appendTableMetric(position, "避險期貨", money.format(row.futures_position));
    appendTableMetric(position, "TX 套利", money.format(row.underlying_futures_position || 0));
    tr.appendChild(position);
    body.appendChild(tr);
  }
}

function appendGuideField(card, label, value) {
  const block = document.createElement("div");
  const term = document.createElement("strong");
  term.textContent = label;
  const detail = document.createElement("p");
  detail.textContent = value || "—";
  block.append(term, detail);
  card.appendChild(block);
}

function renderStrategyGuide(catalog, counts) {
  catalog = Array.isArray(catalog) ? catalog : [];
  counts = counts || {
    live_ideal: catalog.filter((row) => row.availability !== "blocked_contract").length,
    blocked_contract: catalog.filter((row) => row.availability === "blocked_contract").length,
    catalog_total: catalog.length
  };
  const filter = byId("strategy-category-filter");
  const categories = [...new Set(catalog.map((row) => row.category))].sort();
  const expected = ["", ...categories];
  if (filter.options.length !== expected.length) {
    filter.replaceChildren();
    for (const category of expected) {
      const option = document.createElement("option");
      option.value = category;
      option.textContent = category || "全部分類";
      filter.appendChild(option);
    }
  }
  filter.value = selectedCategory;
  const normalizedSearch = strategySearch.trim().toLocaleLowerCase("zh-Hant");
  const rows = catalog.filter((row) => (
    (!selectedCategory || row.category === selectedCategory)
    && matchesExposureFilters(row)
    && (!normalizedSearch || [
      row.label, row.category, row.family, row.summary, row.entry_rule,
      row.exit_rule, row.risk_note, row.directional_exposure_label,
      row.volatility_exposure_label, row.hedge_type_label,
    ].some((value) => String(value || "").toLocaleLowerCase("zh-Hant").includes(normalizedSearch)))
  ));
  const visibleRows = rows.slice(0, guideVisibleCount);
  const grid = byId("strategy-guide-grid");
  grid.replaceChildren();
  for (const row of visibleRows) {
    const card = document.createElement("article");
    card.className = "strategy-card";
    const head = document.createElement("div");
    head.className = "strategy-card-head";
    const title = document.createElement("h3");
    title.textContent = row.label;
    const badge = document.createElement("span");
    badge.className = `pill ${row.availability === "live_ideal" ? "valid" : "stale"}`;
    badge.textContent = row.availability === "live_ideal" ? "LIVE CURVE" : "CONTRACT GAP";
    head.append(title, badge);
    const meta = document.createElement("p");
    meta.className = "strategy-card-meta";
    meta.textContent = `${row.category} · ${row.family} · ${row.broker_monitoring}`;
    card.append(head, meta);
    const exposureTags = document.createElement("div");
    exposureTags.className = "exposure-tags";
    for (const value of [
      row.directional_exposure_label,
      row.volatility_exposure_label,
      row.hedge_type_label
    ]) {
      const tag = document.createElement("span");
      tag.textContent = value || "未分類";
      exposureTags.appendChild(tag);
    }
    card.appendChild(exposureTags);
    appendGuideField(card, "設計選擇權多空口數比", row.design_option_ratio_label);
    if (row.design_futures_target_index_equivalent != null) {
      const futureTarget = Number(row.design_futures_target_index_equivalent);
      const direction = futureTarget >= 0 ? "多" : "空";
      appendGuideField(
        card,
        "固定期貨目標",
        `${direction} ${Math.abs(futureTarget).toFixed(2)} 指數等價`
      );
    }
    appendGuideField(card, "核心做法", row.summary);
    appendGuideField(card, "進場", row.entry_rule);
    appendGuideField(card, "出場", row.exit_rule);
    appendGuideField(card, "風險與帳務", row.risk_note);
    if (row.blocker) appendGuideField(card, "尚缺契約", row.blocker);
    grid.appendChild(card);
  }
  if (!visibleRows.length) {
    const empty = document.createElement("p");
    empty.className = "empty-state";
    empty.textContent = "沒有符合搜尋與曝險條件的策略。";
    grid.appendChild(empty);
  }
  const loadMore = byId("guide-load-more");
  loadMore.hidden = visibleRows.length >= rows.length;
  setText("guide-visible-count", `顯示 ${visibleRows.length} / ${rows.length}`);
  setText(
    "strategy-guide-summary",
    `符合條件 ${rows.length} / ${counts.catalog_total}；${counts.live_ideal} 個實際理想帳曲線 · ${counts.blocked_contract} 個契約缺口 fail-closed`
  );
}

function svgNode(name, attrs = {}, text = "") {
  const node = document.createElementNS("http://www.w3.org/2000/svg", name);
  for (const [key, value] of Object.entries(attrs)) node.setAttribute(key, String(value));
  if (text) node.textContent = text;
  return node;
}

function renderCurveWall(strategies, history) {
  history = filterHistoryByRange(history);
  const grid = byId("curve-wall-grid");
  grid.replaceChildren();
  const filtered = sortedStrategies(filteredStrategies(strategies));
  const visible = filtered.slice(0, curveVisibleCount);
  const byStrategy = new Map(filtered.map((row) => [row.strategy_id, []]));
  for (const point of history) {
    const value = Number(point.fixed_capital_return);
    if (byStrategy.has(point.strategy_id) && Number.isFinite(value)) {
      byStrategy.get(point.strategy_id).push(value);
    }
  }
  const allValues = [...byStrategy.values()].flat();
  let minY = Math.min(0, ...allValues), maxY = Math.max(0, ...allValues);
  if (!Number.isFinite(minY) || !Number.isFinite(maxY) || minY === maxY) {
    minY = -0.001; maxY = 0.001;
  }
  const range = maxY - minY;
  minY -= range * 0.06;
  maxY += range * 0.06;
  const width = 240, height = 76, inset = 6;
  const y = (value) => inset + (maxY - value) * (height - inset * 2) / (maxY - minY);
  for (const row of visible) {
    const values = byStrategy.get(row.strategy_id) || [];
    const card = document.createElement("button");
    card.type = "button";
    card.className = "curve-card";
    card.title = `切換上方大圖：${row.label}`;
    card.addEventListener("click", () => {
      selectedStrategy = row.strategy_id;
      byId("strategy-select").value = selectedStrategy;
      if (lastSnapshot) renderChart(lastSnapshot.history);
      const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
      byId("equity-chart").scrollIntoView({ behavior: reducedMotion ? "auto" : "smooth", block: "center" });
    });
    const head = document.createElement("span");
    head.className = "curve-card-head";
    const title = document.createElement("strong");
    title.textContent = row.label;
    const current = document.createElement("b");
    current.className = Number(row.fixed_capital_return) >= 0 ? "positive" : "negative";
    current.textContent = formatPercent(row.fixed_capital_return);
    head.append(title, current);
    const meta = document.createElement("span");
    meta.className = "curve-card-meta";
    meta.textContent = `${row.directional_exposure_label} · ${row.volatility_exposure_label} · ${values.length} 點`;
    meta.title = `${row.hedge_type_label} · 設計 ${row.design_option_ratio_label} · 實際 ${row.live_exposure_label}`;
    const svg = svgNode("svg", { viewBox: `0 0 ${width} ${height}`, "aria-hidden": "true" });
    svg.appendChild(svgNode("line", { x1: inset, y1: y(0), x2: width - inset, y2: y(0), class: "mini-zero" }));
    if (values.length >= 2) {
      const x = (index) => inset + index * (width - inset * 2) / (values.length - 1);
      const points = values.map((value, index) => `${x(index).toFixed(2)},${y(value).toFixed(2)}`).join(" ");
      svg.appendChild(svgNode("polyline", {
        points,
        class: Number(values.at(-1)) >= 0 ? "mini-line positive" : "mini-line negative"
      }));
    }
    card.append(head, meta, svg);
    grid.appendChild(card);
  }
  const loadMore = byId("curve-load-more");
  loadMore.hidden = visible.length >= filtered.length;
  setText("curve-visible-count", `顯示 ${visible.length} / ${filtered.length}`);
  setText(
    "curve-wall-note",
    `${TIME_RANGE_LABELS[selectedTimeRange]} · 符合條件 ${filtered.length} / ${strategies.length} 條曲線；共用 Y 軸（${formatPercent(minY)} ～ ${formatPercent(maxY)}），以固定預留資金正規化；排序與下表一致。`
  );
}

function renderChart(history) {
  history = filterHistoryByRange(history);
  const svg = byId("equity-chart");
  svg.replaceChildren();
  const rows = history.filter((row) => (
    row.strategy_id === selectedStrategy && row.decision_ts_ns > 0
    && row.total_equity_twd != null
    && Number.isFinite(Number(row.total_equity_twd))
  ));
  if (rows.length < 2) {
    setText("chart-note", `${TIME_RANGE_LABELS[selectedTimeRange]}內資料點不足；至少累積兩個每分鐘 mark 後才繪圖。`);
    return;
  }
  const width = 900, height = 300, left = 76, right = 22, top = 22, bottom = 70;
  const values = rows.map((row) => Number(row.total_equity_twd));
  const baseline = Number(rows.at(-1).initial_capital_twd);
  const bounds = Number.isFinite(baseline) ? [...values, baseline] : values;
  let minY = Math.min(...bounds), maxY = Math.max(...bounds);
  if (minY === maxY) { minY -= 1; maxY += 1; }
  const pad = Math.max((maxY - minY) * 0.08, 1);
  minY -= pad; maxY += pad;
  const axis = timeAxis.buildTimeAxis({
    range: selectedTimeRange,
    timestamps: rows.map((row) => Number(row.decision_ts_ns) / 1e6),
    sessions: TAIFEX_SESSIONS,
  });
  if (!axis) return;
  const x = (row) => timeAxis.position(axis, Number(row.decision_ts_ns) / 1e6, left, width - right);
  const y = (value) => top + (maxY - value) * (height - top - bottom) / (maxY - minY);
  for (let i = 0; i <= 4; i += 1) {
    const value = minY + (maxY - minY) * i / 4;
    const yPos = y(value);
    svg.appendChild(svgNode("line", { x1: left, y1: yPos, x2: width - right, y2: yPos, class: "chart-grid" }));
    svg.appendChild(svgNode("text", { x: left - 10, y: yPos + 4, "text-anchor": "end", class: "chart-label" }, money.format(value)));
  }
  if (Number.isFinite(baseline)) svg.appendChild(svgNode("line", { x1: left, y1: y(baseline), x2: width - right, y2: y(baseline), class: "chart-baseline" }));
  for (const tick of axis.ticks) {
    const xPos = timeAxis.position(axis, tick.timestamp, left, width - right);
    const lineClass = tick.kind === "session" ? "chart-time-grid session" : "chart-time-grid";
    const labelClass = tick.kind === "session" ? "chart-label chart-session-label" : "chart-label";
    svg.appendChild(svgNode("line", { x1: xPos, y1: top, x2: xPos, y2: height - bottom, class: lineClass }));
    const labelY = tick.kind === "session" ? height - 34 : height - 8;
    const attributes = { x: xPos, y: labelY, "text-anchor": tick.rotate ? "end" : "middle", class: labelClass };
    if (tick.rotate) attributes.transform = `rotate(-45 ${xPos} ${labelY})`;
    svg.appendChild(svgNode("text", attributes, tick.label));
  }
  const points = rows.map((row) => `${x(row).toFixed(2)},${y(Number(row.total_equity_twd)).toFixed(2)}`).join(" ");
  svg.appendChild(svgNode("polyline", { points, class: "chart-line" }));
  const first = new Date(Number(rows[0].decision_ts_ns) / 1e6);
  const last = new Date(Number(rows[rows.length - 1].decision_ts_ns) / 1e6);
  const changed = Math.max(...values) !== Math.min(...values);
  const carried = rows.filter((row) => row.valuation_carried_forward).length;
  const pnl = rows.at(-1).cumulative_pnl_twd;
  setText("chart-note", changed
    ? `${TIME_RANGE_LABELS[selectedTimeRange]} · ${rows.length} 點（${localTime(first.toISOString())} ～ ${localTime(last.toISOString())}）${cachedHistoryMeta?.downsampled ? "；長區間已保留區間高低極值縮圖" : ""}；${carried} 點延用上一筆完整估值；最後總權益 ${formatTwd(values.at(-1))}，累積損益 ${formatTwd(pnl)}。`
    : `${TIME_RANGE_LABELS[selectedTimeRange]} · ${rows.length} 點（${localTime(first.toISOString())} ～ ${localTime(last.toISOString())}）${cachedHistoryMeta?.downsampled ? "；長區間已保留區間高低極值縮圖" : ""}；${carried} 點延用上一筆完整估值；總權益維持 ${formatTwd(values.at(-1))}。`);
}

function renderCounts(counts) {
  setText("count-trades", money.format(counts.ideal_trades));
  setText("count-marks", money.format(counts.marks));
  setText("count-calibrations", money.format(counts.calibrations));
  setText("count-events", money.format(counts.events));
}

function render(snapshot, {forceHeavy = false} = {}) {
  snapshot.history = cachedHistory;
  lastSnapshot = snapshot;
  renderHeader(snapshot); renderMetrics(snapshot); renderParity(snapshot); renderCycle(snapshot);
  renderPerformanceSummary(snapshot.portfolio_summary);
  const compatibilityCatalog = snapshot.strategy_catalog || snapshot.strategies.map((row) => ({
    strategy_id: row.strategy_id,
    label: row.label,
    family: row.family || "legacy_live_strategy",
    category: row.category || "既有即時策略",
    summary: row.summary || "既有即時策略帳本；完整說明會在下一個自然 capture 週期載入。",
    entry_rule: row.entry_rule || "沿用目前 active cycle 的既有因果進場契約。",
    exit_rule: row.exit_rule || "沿用目前 active cycle 的既有出場契約。",
    risk_note: row.risk_note || "目前為新舊 schema 自然切換期間。",
    broker_monitoring: row.broker_monitoring || "legacy_runtime",
    directional_exposure: row.directional_exposure || "adaptive",
    directional_exposure_label: row.directional_exposure_label || "動態／訊號決定",
    volatility_exposure: row.volatility_exposure || "signal_dependent",
    volatility_exposure_label: row.volatility_exposure_label || "多空波動由訊號決定",
    hedge_type: row.hedge_type || "contract_pending",
    hedge_type_label: row.hedge_type_label || "執行契約待完成",
    design_option_ratio_label: row.design_option_ratio_label || "尚未載入",
    availability: "live_ideal"
  }));
  const compatibilityCounts = snapshot.strategy_counts || {
    live_ideal: snapshot.strategies.length,
    blocked_contract: 0,
    catalog_total: compatibilityCatalog.length
  };
  lastStrategyCatalog = compatibilityCatalog;
  lastStrategyCounts = compatibilityCounts;
  const revision = JSON.stringify([
    snapshot.record_counts,
    snapshot.portfolio_summary,
    snapshot.exposure_summary,
    snapshot.strategies.map((row) => [
      row.strategy_id, row.total_equity_twd, row.fixed_capital_return,
      row.compounded_return_to_live_mark, row.margin_excess_twd,
      row.valuation_status, row.valuation_age_seconds,
      row.underlying_futures_position,
    ]),
    snapshot.put_call_parity_tx,
    compatibilityCatalog.length,
  ]);
  if (!forceHeavy && revision === lastHeavyRevision) return;
  lastHeavyRevision = revision;
  renderExposureControls(compatibilityCatalog);
  renderExposureSummary(snapshot.exposure_summary);
  renderStrategySelector(snapshot.strategies);
  renderChart(snapshot.history);
  renderCurveWall(snapshot.strategies, snapshot.history);
  renderTable(snapshot.strategies);
  renderCounts(snapshot.record_counts);
  renderStrategyGuide(compatibilityCatalog, compatibilityCounts);
}

async function refresh() {
  if (document.hidden || refreshInFlight) return;
  refreshInFlight = true;
  try {
    const response = await fetchWithTimeout("api/status", { cache: "no-store" });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
    render(payload);
  } catch (error) {
    const alert = byId("alert");
    alert.className = "alert blocked";
    alert.textContent = `面板資料讀取失敗：${error.message}`;
    byId("live-dot").className = "live-dot blocked";
    setText("live-label", "OFFLINE");
  } finally {
    refreshInFlight = false;
  }
}

function applyHistoryPayload(payload, etag = "") {
  lastHistoryEtag = etag;
  cachedHistory = Array.isArray(payload.history) ? payload.history : [];
  cachedHistoryMeta = payload;
  if (lastSnapshot) {
    lastSnapshot.history = cachedHistory;
    renderChart(cachedHistory);
    renderCurveWall(lastSnapshot.strategies, cachedHistory);
    renderCounts(payload.record_counts || lastSnapshot.record_counts);
  }
}

async function refreshHistory({preferCache = false} = {}) {
  if (document.hidden) return;
  const requestedRange = selectedTimeRange;
  const cached = historyPayloadCache.get(requestedRange);
  if (preferCache) {
    if (cached) applyHistoryPayload(cached.payload, cached.etag);
    else {
      cachedHistory = [];
      cachedHistoryMeta = null;
      lastHistoryEtag = "";
      if (lastSnapshot) { renderChart([]); renderCurveWall(lastSnapshot.strategies, []); }
    }
    if (cached && Date.now() - cached.receivedAt < HISTORY_CLIENT_CACHE_MS) return;
  }
  if (historyInFlight) return;
  historyInFlight = true;
  try {
    const response = await fetchWithTimeout(`api/history?range=${encodeURIComponent(requestedRange)}`, { cache: "default" });
    if (response.status === 429) {
      const retrySeconds = Math.min(30, Math.max(1, Number(response.headers.get("Retry-After")) || 5));
      setText("curve-wall-note", `${TIME_RANGE_LABELS[requestedRange]}歷史請求稍多，保留目前曲線並於 ${retrySeconds} 秒後自動重試。`);
      if (historyRetryTimer != null) clearTimeout(historyRetryTimer);
      historyRetryTimer = window.setTimeout(() => {
        historyRetryTimer = null;
        if (requestedRange === selectedTimeRange) void refreshHistory();
      }, retrySeconds * 1000);
      return;
    }
    const etag = response.headers.get("ETag") || "";
    if (requestedRange !== selectedTimeRange) return;
    if (etag && cached?.etag === etag) {
      cached.receivedAt = Date.now();
      if (historyRetryTimer != null) clearTimeout(historyRetryTimer);
      historyRetryTimer = null;
      return;
    }
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
    historyPayloadCache.set(requestedRange, {payload, etag, receivedAt: Date.now()});
    if (historyRetryTimer != null) clearTimeout(historyRetryTimer);
    historyRetryTimer = null;
    applyHistoryPayload(payload, etag);
  } catch (error) {
    setText("curve-wall-note", `曲線歷史暫時無法更新：${error.message}`);
  } finally {
    historyInFlight = false;
    if (requestedRange !== selectedTimeRange) void refreshHistory({preferCache: true});
  }
}

byId("strategy-select").addEventListener("change", (event) => {
  selectedStrategy = event.target.value;
  if (lastSnapshot) renderChart(lastSnapshot.history);
});
byId("strategy-category-filter").addEventListener("change", (event) => {
  selectedCategory = event.target.value;
  guideVisibleCount = 12;
  if (lastSnapshot) render(lastSnapshot, {forceHeavy: true});
});
byId("strategy-search").addEventListener("input", (event) => {
  strategySearch = event.target.value;
  guideVisibleCount = 12;
  if (strategySearchFrame != null) cancelAnimationFrame(strategySearchFrame);
  strategySearchFrame = requestAnimationFrame(() => {
    strategySearchFrame = null;
    if (lastSnapshot) renderStrategyGuide(lastStrategyCatalog, lastStrategyCounts);
  });
});
byId("exposure-direction-filter").addEventListener("change", (event) => {
  selectedDirectionalExposure = event.target.value;
  curveVisibleCount = 12;
  guideVisibleCount = 12;
  if (lastSnapshot) render(lastSnapshot, {forceHeavy: true});
});
byId("exposure-volatility-filter").addEventListener("change", (event) => {
  selectedVolatilityExposure = event.target.value;
  curveVisibleCount = 12;
  guideVisibleCount = 12;
  if (lastSnapshot) render(lastSnapshot, {forceHeavy: true});
});
byId("exposure-hedge-filter").addEventListener("change", (event) => {
  selectedHedgeType = event.target.value;
  curveVisibleCount = 12;
  guideVisibleCount = 12;
  if (lastSnapshot) render(lastSnapshot, {forceHeavy: true});
});
byId("exposure-filter-reset").addEventListener("click", () => {
  selectedDirectionalExposure = "";
  selectedVolatilityExposure = "";
  selectedHedgeType = "";
  curveVisibleCount = 12;
  guideVisibleCount = 12;
  if (lastSnapshot) render(lastSnapshot, {forceHeavy: true});
});
byId("strategy-sort").addEventListener("change", (event) => {
  selectedSort = event.target.value;
  if (lastSnapshot) {
    renderCurveWall(lastSnapshot.strategies, lastSnapshot.history);
    renderTable(lastSnapshot.strategies);
  }
});
byId("sort-direction").addEventListener("click", () => {
  sortDescending = !sortDescending;
  setText("sort-direction", sortDescending ? "由高到低 ↓" : "由低到高 ↑");
  if (lastSnapshot) {
    renderCurveWall(lastSnapshot.strategies, lastSnapshot.history);
    renderTable(lastSnapshot.strategies);
  }
});
byId("curve-load-more").addEventListener("click", () => {
  curveVisibleCount += 12;
  if (lastSnapshot) renderCurveWall(lastSnapshot.strategies, lastSnapshot.history);
});
byId("guide-load-more").addEventListener("click", () => {
  guideVisibleCount += 12;
  if (lastSnapshot) renderStrategyGuide(lastStrategyCatalog, lastStrategyCounts);
});
byId("equity-time-range").addEventListener("click", (event) => {
  const button = event.target.closest("button[data-range]");
  if (!button || !(button.dataset.range in TIME_RANGES)) return;
  selectedTimeRange = button.dataset.range;
  if (historyRetryTimer != null) clearTimeout(historyRetryTimer);
  historyRetryTimer = null;
  try { localStorage.setItem("taifex-equity-time-range", selectedTimeRange); } catch (_error) { /* optional */ }
  syncTimeRangeControl();
  void refreshHistory({preferCache: true});
});
document.addEventListener("visibilitychange", () => {
  if (!document.hidden) refreshMinuteSnapshot();
});

function refreshMinuteSnapshot() {
  void refresh();
  void refreshHistory();
}

syncTimeRangeControl();
refreshMinuteSnapshot();
window.setInterval(refreshMinuteSnapshot, PRICE_REFRESH_MS);

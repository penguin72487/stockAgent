"use strict";

const SUMMARY_REFRESH_MS = 5000;
const FULL_REFRESH_MS = 15000;
const SIGNAL_PAGE_SIZE = 250;
const POSITION_PAGE_SIZE = 250;
const MAX_EVENT_ROWS = 250;
const COLORS = ["#37d3ff", "#5ee0a0", "#a98cff", "#f5bd4f", "#ff7ac8", "#73e6d1", "#ff9f68"];
let snapshot = null;
let lastFetchMs = null;
let refreshInFlight = false;
let summaryInFlight = false;
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
let filterAnimationFrame = null;
let positionVisibleRows = POSITION_PAGE_SIZE;

const $ = (id) => document.getElementById(id);
const esc = (value) => String(value ?? "—").replace(/[&<>"']/g, (c) => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"})[c]);
const number = (value, digits = 0) => value == null || !Number.isFinite(Number(value)) ? "—" : Number(value).toLocaleString("zh-TW", {minimumFractionDigits: digits, maximumFractionDigits: digits});
// Source-backed values must not gain fake trailing zeroes or lose meaningful
// precision merely because a table cell previously chose a fixed width.  IEEE
// 754 doubles have about 15 trustworthy significant decimal digits; this also
// removes binary arithmetic tails such as 16.110149999999784 -> 16.11015.
const sourceNumber = (value) => {
  if (value == null || value === "") return "—";
  const resolved = Number(value);
  if (!Number.isFinite(resolved)) return "—";
  if (Object.is(resolved, -0)) return "0";
  return resolved.toLocaleString("zh-TW", {maximumSignificantDigits: 15});
};
const monetaryNumber = (value) => {
  if (value == null || value === "") return "—";
  const resolved = Number(value);
  if (!Number.isFinite(resolved)) return "—";
  if (Object.is(resolved, -0)) return "0";
  // Price has at most two decimals and the unrounded fee-rate contract can
  // produce up to eight economically meaningful TWD decimals.
  return resolved.toLocaleString("zh-TW", {maximumFractionDigits: 8});
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
  const digits = Math.abs(resolved) < .0001 && resolved !== 0 ? 6 : 4;
  return `${resolved.toLocaleString("zh-TW", {maximumFractionDigits: digits})}%`;
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
    critical_unflattened_after_13_24: "13:24 強平後仍有未平部位",
    blocked_missing_eligibility: "缺少當日當沖資格資料，已停止執行",
    blocked_missing_checkpoint: "缺少模型權重，已停止執行",
  };
  return labels[value] || String(value || "未知狀態").replaceAll("_", " ");
}

function engineStatusShortLabel(value) {
  const labels = {
    active: "執行正常",
    ready: "已就緒",
    critical_unflattened_after_13_24: "13:24 後未平",
    flat_no_executable_signal: "今日無可執行訊號",
    session_flat_after_exit: "今日已平倉",
  };
  return labels[value] || engineStatusLabel(value);
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
  const pnlValues = modes.map((mode) => {
    const initial = Number(mode.initial_capital_twd);
    const equity = Number(mode.total_equity_twd);
    return Number.isFinite(initial) && Number.isFinite(equity) ? equity - initial : null;
  }).filter((value) => value != null);
  const totalPnl = pnlValues.length ? pnlValues.reduce((sum, value) => sum + value, 0) : null;
  const returns = modes.map((mode) => Number(mode.return_pct)).filter(Number.isFinite);
  const best = returns.length ? Math.max(...returns) : null;
  const worst = returns.length ? Math.min(...returns) : null;
  const healthKind = healthyModes === modes.length ? "good" : healthyModes ? "warn" : "bad";
  const pnlKind = pnlClass(totalPnl);
  const cards = [
    ["模式狀態", `${healthyModes}/${modes.length} 可解讀`, healthyModes === modes.length ? "所有 checkpoint 與執行狀態正常" : "有模式需要查看上方警示", healthKind],
    ["今日持倉", `${number(openPositionCount)} 個`, stalePositions ? `${number(stalePositions)} 個估值延用` : "目前估值皆有新鮮報價", stalePositions ? "warn" : "good"],
    ["四模式淨損益", totalPnl == null ? "—" : `${totalPnl >= 0 ? "+" : ""}${compactMoney(totalPnl)}`, "四個獨立模擬帳本直接加總", pnlKind],
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
  if (data.health === "stale" || blockers.length || signalMissingEligibility.size || currentMissingEligibility.size) {
    const messages = [
      data.health === "stale" ? "資料來源已逾時；畫面只能當歷史紀錄，不能視為現在行情。" : "",
      currentMissingEligibility.size ? `今日當沖資格未完整覆蓋，後續訊號已停止執行：${[...currentMissingEligibility.entries()].map(([venue, row]) => `${venue.toUpperCase()} 需要 ${row.target_date || "今日"}，最新僅到 ${row.latest_date || "無資料"}`).join("；")}` : "",
      !currentMissingEligibility.size && signalMissingEligibility.size ? "09:00 訊號產生時資格資料尚未到齊，因此已 fail-closed；較晚補齊的資料不會回填成假成交。" : "",
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
  const previous = mode.value;
  const revision = JSON.stringify(data.modes.map((row) => [row.market, row.label]));
  if (revision !== lastFilterRevision) {
    mode.innerHTML = `<option value="all">全部模式</option>` + data.modes.map((row) => `<option value="${esc(row.market)}">${esc(row.label || row.market)}</option>`).join("");
    if ([...mode.options].some((option) => option.value === previous)) mode.value = previous;
    lastFilterRevision = revision;
  }
  const date = $("date-filter");
  if (date.value !== data.session_date) date.innerHTML = `<option value="${esc(data.session_date)}">${esc(data.session_date)}</option>`;
}

function renderModes(data) {
  $("mode-cards").innerHTML = data.modes.map((mode) => {
    const initial = Number(mode.initial_capital_twd || 0);
    const equity = mode.total_equity_twd == null ? null : Number(mode.total_equity_twd);
    const pnl = equity == null ? null : equity - initial;
    const returnPct = mode.return_pct == null ? null : Number(mode.return_pct);
    const status = String(mode.engine_status || "unknown");
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
        <div><span>持倉／缺價</span><strong>${number(mode.open_position_count)} / ${number(mode.stale_position_count)}</strong></div>
        <div><span>已實現淨損益</span><strong class="${pnlClass(mode.cumulative_realized_net_pnl_twd)}">${summaryMoney(mode.cumulative_realized_net_pnl_twd)}</strong></div>
        <div><span>13:24 未平</span><strong class="${Number(mode.force_exit_failures || 0) ? "negative" : ""}">${number(mode.force_exit_failures || 0)}</strong></div>
      </div>
      <details><summary>查看資金、訊號與曝險細節</summary><div class="metrics">
        <div><span>報酬率資金基準</span><strong>${money(initial)}</strong></div>
        <div><span>已賺手續費退佣</span><strong>${money(mode.cumulative_commission_rebate_accrued_twd)}</strong></div>
        <div><span>訊號時間</span><strong>${shortTime(mode.signal_at)}</strong></div>
        <div class="wide"><span>方向總曝險：目標</span><strong>多 ${pct(mix.target_long_gross)} / 空 ${pct(mix.target_short_gross)}</strong></div>
        <div class="wide"><span>整張／深度後 → 平衡後</span><strong>多 ${pct(mix.pre_balance_long_gross)} / 空 ${pct(mix.pre_balance_short_gross)} → 多 ${pct(mix.post_balance_long_gross)} / 空 ${pct(mix.post_balance_short_gross)}</strong></div>
        <div class="wide"><span>停利停損價位</span><strong>${esc(bracketPolicy)}</strong></div>
      </div></details>
    </article>`;
  }).join("");
}

function renderOperations(data) {
  const warm = data.preopen || {};
  const session = data.session_progress || {};
  const warmMarkets = warm.markets || [];
  const warmRatio = warmMarkets.length ? warmMarkets.reduce((sum, row) => sum + clampRatio(row.progress_ratio), 0) / warmMarkets.length : clampRatio(warm.progress_ratio);
  const totalModes = Number(warm.total_count || data.modes.length || 0);
  const readyModes = Number(warm.ready_count || 0);
  const sourceAge = Number(data.source_age_seconds || 0);
  const heartbeatKind = sourceAge <= 10 ? "good" : sourceAge <= 30 ? "warn" : "bad";
  const warmKind = warm.status === "ready" ? "good" : warm.status === "failed" ? "bad" : "warn";
  const phaseKind = ["active", "preopen"].includes(session.phase) ? "good" : session.phase === "force_exit" ? "bad" : "warn";
  $("operation-kpis").innerHTML = [
    ["盤前預熱", `${readyModes}/${totalModes || 0} READY`, warm.status || "pending", warmKind],
    ["目前階段", session.label || "—", `下一步 ${session.next_milestone_label || "—"} · ${countdown(session.next_milestone_at)}`, phaseKind],
    ["帳本心跳", `${sourceNumber(data.source_age_seconds)} 秒`, `目標每 ${number(session.decision_interval_seconds || 60)} 秒`, heartbeatKind],
    ["面板 API", lastFetchMs == null ? "—" : `${number(lastFetchMs, 1)} ms`, `摘要每 ${number(SUMMARY_REFRESH_MS / 1000)} 秒刷新`, lastFetchMs != null && lastFetchMs > 1000 ? "warn" : "good"],
  ].map(([label, value, note, kind]) => `<div class="operation-kpi"><span>${esc(label)}</span><strong>${esc(value)}</strong><small class="${esc(kind)}">${esc(note)}</small></div>`).join("");

  const workflowRows = [
    {label:"四模式預熱", value:warmRatio, count:`${number(warm.completed_count || 0)} / ${number(totalModes)} · ${number(warmRatio * 100, 1)}%`, note:`牆鐘 ${duration(warm.wall_elapsed_seconds)} · ${warm.modes_per_minute == null ? "—" : `${sourceNumber(warm.modes_per_minute)} 模式/分`}`, kind:warmKind},
    {label:"09:00 訊號完成", value:session.signal_progress_ratio, count:`${number(session.signal_completed_modes || 0)} / ${number(session.mode_count || 0)}`, note:"每個模式須有盤後特徵與開盤後報價", kind:"good"},
    {label:"市價進場處理", value:session.entry_progress_ratio, count:`${number(session.entry_completed_modes || 0)} / ${number(session.mode_count || 0)}`, note:"以較晚 best ask/bid 與一檔可見量處理", kind:"good"},
    {label:"每分鐘權益紀錄", value:session.mark_progress_ratio, count:`${number(session.observed_mode_minutes || 0)} / ${number(session.expected_mode_minutes || 0)}`, note:`目前 ${sourceNumber(session.mark_rows_per_minute || 0)} 模式紀錄/分`, kind:""},
    {label:"13:20/13:24 退出", value:session.exit_progress_ratio, count:`${number(session.exit_started_modes || 0)} / ${number(session.mode_count || 0)}`, note:"先限價，13:24 撤換市價；缺價不假成交", kind:"warn"},
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
    const eligibility = row.eligibility_ready ? `${row.eligibility_target_date || "今日"} TWSE/TPEx 資格 READY` : "今日資格待確認";
    const detail = row.error || row.message || `${duration(row.elapsed_seconds)} · ${speed} · ${inference} · ${limits} · ${eligibility}${eta == null ? "" : ` · ETA ${duration(eta)}`}`;
    return `<div class="progress-row">
      <div class="progress-title"><strong>${esc(row.label || row.market)}</strong>${badge(stepText, kind)}</div>
      ${progress(row.progress_ratio, kind)}
      <small>${esc(detail)}</small>
    </div>`;
  }).join("") || `<div class="empty-inline">尚無今日預熱紀錄</div>`;
  $("operation-source").textContent = warm.updated_at ? `預熱狀態 ${shortTime(warm.updated_at)}` : "預熱狀態尚未建立";
}

function renderChart(data) {
  const svg = $("equity-chart");
  const modes = selectedMode() === "all" ? data.modes.map((row) => row.market) : [selectedMode()];
  const modeRows = (data.marks || []).filter((row) => modes.includes(row.market)).map((row) => ({...row, series_id: row.market}));
  const benchmarkRows = (data.benchmark_marks || []).map((row) => ({...row, series_id: row.benchmark_id}));
  const rows = modeRows.concat(benchmarkRows);
  const byMode = new Map();
  for (const row of rows) {
    if (!byMode.has(row.series_id)) byMode.set(row.series_id, []);
    byMode.get(row.series_id).push(row);
  }
  for (const values of byMode.values()) values.sort((a, b) => String(a.minute).localeCompare(String(b.minute)));
  const points = [...byMode.values()].flat().filter((row) => row.return_pct != null && Number.isFinite(Number(row.return_pct)));
  $("chart-empty").classList.toggle("hidden", points.length > 0);
  svg.classList.toggle("hidden", points.length === 0);
  if (!points.length) { svg.innerHTML = ""; $("chart-legend").innerHTML = ""; return; }
  const width = 960, height = 360, left = 76, right = 22, top = 24, bottom = 42;
  const times = [...new Set(points.map((row) => String(row.minute)))].sort();
  let ymin = Math.min(0, ...points.map((row) => Number(row.return_pct)));
  let ymax = Math.max(0, ...points.map((row) => Number(row.return_pct)));
  const pad = Math.max(.01, (ymax - ymin) * .08); ymin -= pad; ymax += pad;
  const x = (minute) => left + (times.length <= 1 ? 0 : times.indexOf(String(minute)) / (times.length - 1)) * (width - left - right);
  const y = (value) => top + (ymax - Number(value)) / (ymax - ymin) * (height - top - bottom);
  let html = "";
  for (let i = 0; i <= 4; i += 1) {
    const yy = top + i / 4 * (height - top - bottom);
    const value = ymax - i / 4 * (ymax - ymin);
    html += `<line class="axis" x1="${left}" y1="${yy}" x2="${width-right}" y2="${yy}"></line><text class="axis-text" x="6" y="${yy+4}">${esc(Number(value).toLocaleString("zh-TW", {maximumFractionDigits:4}))}%</text>`;
  }
  [0, .25, .5, .75, 1].forEach((fraction) => {
    const idx = Math.min(times.length - 1, Math.round((times.length - 1) * fraction));
    const xx = x(times[idx]);
    html += `<text class="axis-text" text-anchor="middle" x="${xx}" y="${height-12}">${esc(times[idx].slice(11,16))}</text>`;
  });
  const labels = new Map([
    ...data.modes.map((row) => [row.market, row.label || row.market]),
    ...(data.benchmarks || []).map((row) => [row.benchmark_id, row.label || row.benchmark_id]),
  ]);
  const legend = [];
  [...byMode.entries()].forEach(([market, values], index) => {
    const color = COLORS[index % COLORS.length];
    const valid = values.filter((row) => row.return_pct != null && Number.isFinite(Number(row.return_pct)));
    const path = valid.map((row, i) => `${i ? "L" : "M"}${x(row.minute).toFixed(1)},${y(row.return_pct).toFixed(1)}`).join(" ");
    html += `<path class="chart-line" stroke="${color}" d="${path}"></path>`;
    for (const row of valid.filter((item) => item.valuation_stale)) html += `<circle class="stale-dot" cx="${x(row.minute)}" cy="${y(row.return_pct)}" r="3"></circle>`;
    const latest = valid.at(-1);
    const latestText = latest ? `${Number(latest.return_pct) >= 0 ? "+" : ""}${sourceNumber(latest.return_pct)}%` : "—";
    legend.push(`<span><i class="series-${index % COLORS.length}"></i>${esc(labels.get(market) || market)} <strong class="${pnlClass(latest?.return_pct)}">${esc(latestText)}</strong></span>`);
  });
  svg.innerHTML = html;
  $("chart-legend").innerHTML = legend.join("");
}

function renderPositions(data) {
  const status = $("status-filter").value;
  const rows = data.positions.filter(matchesMode).filter(matchesSymbol).filter((row) => {
    if (status === "all") return true;
    if (status === "open") return Number(row.signed_shares || 0) !== 0;
    if (status === "closed") return row.status === "closed";
    return false;
  }).sort(compareByAbsoluteWeight);
  const visible = rows.slice(0, positionVisibleRows);
  $("position-count").textContent = `${visible.length} / ${rows.length} 筆`;
  const loadMore = $("load-more-positions");
  loadMore.classList.toggle("hidden", visible.length >= rows.length);
  $("position-body").innerHTML = visible.map((row) => `<tr>
    <td><strong>${esc(row.market)}</strong><small>${esc(row.symbol)} ${esc(row.name || "")}</small></td>
    <td>${badge(row.side === "long" ? "多" : "空", row.side === "long" ? "good" : "bad")}</td>
    <td>${pct(row.target_weight)}</td>
	    <td>${number(row.filled_shares)} 股<small>剩餘 ${number(Math.abs(Number(row.signed_shares || 0)))} · requested ${number(row.requested_shares)}</small></td>
    <td>${money(row.entry_price)}<small>${shortTime(row.entry_at)}</small></td>
    <td>${money(row.last_mark_price)}<small>${shortTime(row.last_quote_at)}</small></td>
    <td>TP ${sourceNumber(row.take_profit_price)}<small>SL ${sourceNumber(row.stop_trigger_price)} · ${esc(row.stop_order_status)}</small></td>
    <td>${row.eod_limit_price == null ? "—" : sourceNumber(row.eod_limit_price)}<small>${esc(row.eod_limit_order_status || "未到")} · ${shortTime(row.eod_limit_submitted_at)}</small></td>
	    <td>${row.exit_price == null ? (row.last_exit_price == null ? "持倉中" : `部分 ${money(row.last_exit_price)}`) : money(row.exit_price)}<small>${esc(row.exit_reason || row.status || "—")} ${shortTime(row.exit_at || row.last_exit_at)}</small></td>
	    <td class="${pnlClass(row.total_net_pnl_twd ?? row.net_pnl_twd ?? row.last_complete_net_pnl_twd)}">${money(row.total_net_pnl_twd ?? row.net_pnl_twd ?? row.last_complete_net_pnl_twd)}</td>
    <td>${row.valuation_stale ? badge("延用", "warn") : badge("新鮮", "good")}</td>
  </tr>`).join("") || `<tr><td colspan="11">目前沒有符合篩選的持倉</td></tr>`;
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
  $("signal-direction-summary").innerHTML = [
    ["目前訊號目標", target],
    ["整張／深度後", preBalance],
    ["方向平衡後", actual],
  ].map(([label, row]) => `<div><span>${esc(label)}</span><strong>${esc(directionPair(row))}</strong></div>`).join("");
  const errorRow = signalLoadError
    ? `<tr class="signal-load-error"><td colspan="10">${esc(signalLoadError)}</td></tr>`
    : "";
  const rowHtml = signalRows.map((row) => `<tr>
    <td>${shortTime(row.signal_at)}</td><td>${esc(row.market)}</td><td><strong>${esc(row.symbol)}</strong><small>${esc(row.name || "")}</small></td>
	    <td>${esc(row.side)}</td><td>${sourceNumber(row.raw_score ?? row.score)}</td><td>${pct(row.target_weight)}</td><td>${number(row.filled_shares)} / ${number(row.requested_shares)}<small>L1 上限 ${number(row.top_book_capacity_shares)}</small></td>
	    <td>${sourceNumber(row.bid)} / ${sourceNumber(row.ask)}<small>${sourceNumber(row.bid_volume_lots)} / ${sourceNumber(row.ask_volume_lots)} 張 · ${shortTime(row.quote_at)}</small></td>
    <td>${row.day_trade_eligible ? badge(row.sell_first_allowed ? "可雙向" : "僅買先", row.sell_first_allowed ? "good" : "warn") : badge("不可當沖", "bad")}</td>
	    <td>${badge(row.status, row.status === "ready" ? "good" : ["partial_depth", "partial_directional_mix"].includes(row.status) ? "warn" : row.status === "hold" ? "" : "bad")}<small>${esc(row.reason || "")}</small></td>
  </tr>`).join("");
  $("signal-body").innerHTML = errorRow + (rowHtml || `<tr><td colspan="10">目前沒有符合篩選的訊號</td></tr>`);
}

function renderEvents(data) {
  const events = [...data.orders, ...data.fills.map((row) => ({...row, status: "fill", order_type: "FILL"}))]
    .filter(matchesMode).filter(matchesSymbol).sort((a,b) => String(b.recorded_at || b.fill_at).localeCompare(String(a.recorded_at || a.fill_at))).slice(0, MAX_EVENT_ROWS);
  $("event-body").innerHTML = events.map((row) => `<tr><td>${shortTime(row.fill_at || row.recorded_at)}</td><td>${esc(row.market)}<small>${esc(row.symbol)}</small></td><td>${esc(row.purpose)}</td><td>${esc(row.order_type)}</td><td>${sourceNumber(row.price)} × ${number(row.quantity)}</td><td>${esc(row.status)}</td></tr>`).join("") || `<tr><td colspan="6">尚無委託／成交事件</td></tr>`;
}

function renderAudit(data) {
  const counts = data.record_counts || {};
  const items = [
    ["交易日", data.session_date], ["模擬模式", data.simulation_only ? "是，正式下單不可能" : "否"],
    ["訊號／委託／成交", `${number(counts.signals)} / ${number(counts.orders)} / ${number(counts.fills)}`], ["策略／基準 mark", `${number(counts.marks)} / ${number(counts.benchmark_marks)}`],
    ["狀態 API 視窗", Object.entries(data.payload_window || {}).map(([key, value]) => `${key}:${number(value)}`).join(" · ") || "—"],
    ...data.modes.map((mode) => [`${mode.market} checkpoint`, mode.checkpoint_ready ? `READY · ${mode.checkpoint_fingerprint || "fingerprint pending"}` : "MISSING"]),
    ...data.modes.map((mode) => [`${mode.market} 資格資料`, Object.entries(mode.eligibility_coverage || {}).map(([venue, row]) => `${venue}:${row.covered ? row.target_date : `缺 ${row.target_date} / latest ${row.latest_date || "—"}`}`).join(" · ") || "尚未載入"]),
    ...data.modes.map((mode) => [`${mode.market} 目前資格來源`, Object.entries(mode.current_eligibility_coverage || {}).map(([venue, row]) => `${venue}:${row.covered ? `READY ${row.target_date}` : `缺 ${row.target_date} / latest ${row.latest_date || "—"}`}`).join(" · ") || "尚未檢查"]),
    ...(data.benchmarks || []).map((row) => [`${row.label || row.benchmark_id}`, row.return_pct == null ? `等待可成交報價 · ${row.valuation_source || "尚未進場"}` : `${row.return_pct >= 0 ? "+" : ""}${sourceNumber(row.return_pct)}% · ${shortTime(row.entry_at)} 起 · 資金 ${money(row.initial_capital_twd)} · ${row.contract_code || row.symbol || ""}`]),
  ];
  $("audit-grid").innerHTML = items.map(([label,value]) => `<div class="audit-item"><span>${esc(label)}</span><strong>${esc(value)}</strong></div>`).join("");
  const contract = data.source_contract || {};
  $("source-signal").textContent = contract.signal || "—"; $("source-fill").textContent = contract.entry_fill || "—";
  $("source-fees").textContent = contract.fees || "—";
  $("source-comparison").textContent = `${contract.comparison || "—"}；${contract.benchmarks || "—"}`;
  $("source-eligibility").textContent = contract.eligibility || "—"; $("source-depth").textContent = `${contract.depth_limit || "—"}；${contract.bracket_fill || "—"}`;
}

function revisionOf(data) {
  const counts = data.record_counts || {};
  return JSON.stringify([
    counts.orders, counts.fills, counts.marks, counts.benchmark_marks, counts.events,
    data.session_progress,
    data.preopen?.updated_at,
    data.modes.map((row) => [
      row.market, row.total_equity_twd, row.open_position_count,
      row.stale_position_count, row.force_exit_failures, row.engine_status,
    ]),
    (data.benchmarks || []).map((row) => [row.benchmark_id, row.return_pct, row.valuation_stale, row.contract_code]),
  ]);
}

function render({heavy = true} = {}) {
  if (!snapshot) return;
  renderHeader(snapshot);
  renderOverview(snapshot);
  if (!heavy) return;
  renderOperations(snapshot);
  renderModes(snapshot);
  renderChart(snapshot);
  renderPositions(snapshot);
  renderEvents(snapshot);
  renderAudit(snapshot);
}

async function loadSignals({append = false} = {}) {
  if (!snapshot) return;
  if (signalAbortController) signalAbortController.abort();
  signalAbortController = new AbortController();
  const controller = signalAbortController;
  const sequence = ++signalRequestSequence;
  const params = new URLSearchParams({
    mode: selectedMode(),
    symbol: $("symbol-filter").value.trim(),
    status: $("status-filter").value,
    offset: String(append ? signalRows.length : 0),
    limit: String(SIGNAL_PAGE_SIZE),
  });
  signalLoading = true;
  renderSignals();
  try {
    const response = await fetch(`api/signals?${params.toString()}`, {cache: "default", signal: controller.signal});
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const page = await response.json();
    if (sequence !== signalRequestSequence) return;
    signalLoadError = "";
    signalRows = append ? signalRows.concat(page.rows || []) : (page.rows || []);
    signalTotal = Number(page.total || 0);
    signalHasMore = Boolean(page.has_more);
    signalRecordCount = Number(page.record_count || 0);
    signalDirectionSummary = page.direction_summary || {};
  } catch (error) {
    if (sequence !== signalRequestSequence) return;
    if (error?.name === "AbortError") return;
    signalLoadError = String(error).includes("HTTP 429")
      ? "請求較密集，訊號明細會在下一輪自動重試；上方帳本摘要不受影響。"
      : `訊號明細暫時無法更新：${error}`;
    signalRecordCount = null;
  } finally {
    if (sequence === signalRequestSequence) {
      if (signalAbortController === controller) signalAbortController = null;
      signalLoading = false;
      renderSignals();
    }
  }
}

async function refresh() {
  if (document.hidden) return;
  if (refreshInFlight) return;
  refreshInFlight = true;
  try {
    const started = performance.now();
    const response = await fetch("api/status", {cache: "default"});
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    snapshot = await response.json();
    lastFetchMs = performance.now() - started;
    syncFilters(snapshot);
    const revision = revisionOf(snapshot);
    const heavy = revision !== lastRenderedRevision;
    lastRenderedRevision = revision;
    render({heavy});
    const currentSignalCount = Number((snapshot.record_counts || {}).signals || 0);
    if (signalRecordCount == null || currentSignalCount !== signalRecordCount) await loadSignals();
  } catch (error) {
    const alert = $("alert"); alert.classList.remove("hidden"); alert.textContent = `面板讀取失敗：${error}`;
    $("health").textContent = "UNAVAILABLE"; $("health").className = "pill critical";
  } finally {
    refreshInFlight = false;
  }
}

async function refreshSummary() {
  if (document.hidden || summaryInFlight || refreshInFlight || !snapshot) return;
  summaryInFlight = true;
  try {
    const response = await fetch("api/summary", {cache: "default"});
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    snapshot = {...snapshot, ...(await response.json())};
    renderHeader(snapshot);
    renderOverview(snapshot);
    const currentSignalCount = Number((snapshot.record_counts || {}).signals || 0);
    if (signalRecordCount == null || currentSignalCount !== signalRecordCount) await loadSignals();
  } catch (_error) {
    // The slower full refresh remains the visible availability authority.
  } finally {
    summaryInFlight = false;
  }
}

function renderFilteredDetails({includeChart = false} = {}) {
  if (!snapshot) return;
  if (includeChart) renderChart(snapshot);
  renderPositions(snapshot);
  renderEvents(snapshot);
}

function filtersChanged({debounceSignals = false, includeChart = false} = {}) {
  positionVisibleRows = POSITION_PAGE_SIZE;
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
  if (debounceSignals) signalFilterTimer = window.setTimeout(() => loadSignals(), 180);
  else loadSignals();
}

$("mode-filter").addEventListener("change", () => filtersChanged({includeChart: true}));
$("status-filter").addEventListener("change", () => filtersChanged());
$("symbol-filter").addEventListener("input", () => filtersChanged({debounceSignals: true}));
$("reset-filters").addEventListener("click", () => {
  $("mode-filter").value = "all";
  $("symbol-filter").value = "";
  $("status-filter").value = "all";
  filtersChanged({includeChart: true});
});
$("load-more-signals").addEventListener("click", () => loadSignals({append: true}));
$("load-more-positions").addEventListener("click", () => {
  positionVisibleRows += POSITION_PAGE_SIZE;
  renderPositions(snapshot);
});
setInterval(() => { $("clock").textContent = new Date().toLocaleString("zh-TW", {timeZone:"Asia/Taipei", hour12:false}); }, 1000);
document.addEventListener("visibilitychange", () => { if (!document.hidden) void refresh(); });
void refresh();
window.setInterval(() => void refreshSummary(), SUMMARY_REFRESH_MS);
window.setInterval(() => void refresh(), FULL_REFRESH_MS);

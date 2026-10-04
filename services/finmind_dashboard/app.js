"use strict";

const Dashboard = window.StockAgentDashboard;
const fetchJson = Dashboard.createJsonFetcher({timeoutMs: 15000, cache: "no-store", expectedRoot: "object"});
const $ = Dashboard.byId;
const SVG_NS = "http://www.w3.org/2000/svg";
const state = {latest: null, inFlight: false, filter: "all", quotaRange: "1d", retryTimer: null, retryDelayMs: 3000};
const integer = new Intl.NumberFormat("zh-TW", {maximumFractionDigits: 0});
const oneDecimal = new Intl.NumberFormat("zh-TW", {maximumFractionDigits: 1});
const statusLabels = {complete: "本輪已查驗", backfilling: "回補中", pending: "待取得", delegated: "由 Sponsor 主責", unavailable: "來源不可取", stale: "待追新", retry_exhausted: "重試耗盡，舊資料保留"};

function valueNumber(value) { if (value === null || value === undefined || value === "") return null; const parsed = Number(value); return Number.isFinite(parsed) ? parsed : null; }
function count(value) { const parsed = valueNumber(value); return parsed === null ? "—" : integer.format(parsed); }
function bytes(value) { return valueNumber(value) === null ? "—" : Dashboard.formatBytes(value, {maximumFractionDigits: 1}); }
function ratio(complete, total) { const left = valueNumber(complete), right = valueNumber(total); return left !== null && right > 0 ? Math.min(1, Math.max(0, left / right)) : null; }
function percent(value) { return value === null ? "—" : `${(value * 100).toFixed(2)}%`; }
function duration(seconds) { const value = valueNumber(seconds); if (value === null) return "未知"; if (value < 3600) return `${oneDecimal.format(value / 60)} 分鐘`; return `${oneDecimal.format(value / 3600)} 小時`; }
function datasetStatus(row) { return row.state === "delegated" && row.source_status === "delegated_to_complement_product_history" ? "由 Complement 主責" : statusLabels[row.state] || "待核實"; }
function historyEstimate(row) {
  return (state.latest?.acquisition?.completion_estimate?.stages || []).find(item => item.dataset === row.id);
}
function networkTimeLabel(row) {
  if (valueNumber(row.retry_exhausted_partitions) > 0 && row.state === "retry_exhausted") return "自動重試已停止，待人工修復";
  const estimate = historyEstimate(row);
  if (estimate) return completionEstimateView(estimate).scenarios[1].value;
  // Never reinterpret an older backend's partition-based legacy number as ETA.
  return row.state === "complete" ? "已查驗目前任務" : "未知（分割數不等於請求數）";
}
function networkTimeBasis(row) {
  if (valueNumber(row.retry_exhausted_partitions) > 0) return `${count(row.retry_exhausted_partitions)} 個任務重試達上限，保留 ${count(row.retained_rows)} 筆舊資料；不是完成，不占用自動下載估時。`;
  const estimate = historyEstimate(row);
  if (estimate) return `${completionEstimateView(estimate).scenarios[1].complete}；包含前面階段的等待，不是獨占額度。`;
  if (row.state === "delegated") return "由其他下載器承接；此舊 owner 不另估時間。";
  if (valueNumber(row.deferred_partitions) > 0) {
    return `仍有 ${count(row.deferred_partitions)} 個任務待重試；發出請求不代表已成功完成。完成估時請看上方階段，不能用剩餘分割數除以官方上限。`;
  }
  if (row.state === "complete") return "目前已建立任務已查驗；未來追新與未知歷史另計。";
  return "合批、未知代號、刷新、重試與共用額度均會改變耗時；沒有可信倒數。";
}
function timeLabel(value) { const date = new Date(value || ""); return Number.isNaN(date.getTime()) ? "—" : date.toLocaleString("zh-TW", {timeZone: "Asia/Taipei", hour12: false, year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit"}); }
function ageLabel(seconds) { return Dashboard.formatAge(seconds, {emptyLabel: "時間未核實", hourDigits: 0, dayDigits: 0}); }
function updateMonitorLines(info = {}) {
  const labels = {first_nonempty: "首次取得", first_empty: "來源尚空", unchanged: "未變更", new_data: "追加資料", revision: "內容修訂", coverage_changed: "覆蓋改變", first_nonempty_after_empty: "首次由空回轉有資料"};
  if (info.state === "derived_local") return [
    `本機衍生，不另呼叫 API · 源 ${info.derived_from}`,
    `最近本機寫入 ${timeLabel(info.local_materialized_at_utc)}`,
    `源資料最後檢查 ${timeLabel(info.checked_at_utc)} · ${labels[info.event] || "尚未觀測"}`,
    `下次源資料可排 ${timeLabel(info.next_check_at_utc)}（仍受配額／優先級限制）`];
  const lines = [info.release?.label || "發布規律待確認",
    `最後檢查 ${timeLabel(info.checked_at_utc)} · ${labels[info.event] || "尚未觀測"}`,
    `新內容首次看到 ${timeLabel(info.last_data_observed_at_utc)} · 修訂／追加 ${timeLabel(info.last_changed_at_utc)}`,
    `下次可排 ${timeLabel(info.next_check_at_utc)}（仍受配額／優先級限制）`];
  if (info.availability_by_utc) lines.push(`更新觀測區間 ${timeLabel(info.availability_after_utc)} → ${timeLabel(info.availability_by_utc)}`);
  if (valueNumber(info.response_rows) !== null) lines.push(`最近一 call 回應 ${count(info.response_rows)} 筆 · 網路 ${valueNumber(info.network_seconds) === null ? "未知" : `${oneDecimal.format(info.network_seconds)} 秒`}`);
  if (info.last_error_code) lines.push(`最近失敗 ${timeLabel(info.last_failed_check_at_utc)} · ${info.last_error_code}`);
  return lines;
}
function text(id, value) { $(id).textContent = value; }

function estimateDuration(seconds) {
  const value = valueNumber(seconds);
  if (value === null || value < 0) return "未知";
  if (value === 0) return "本輪可執行工作已查驗";
  if (value >= 172800) return `約 ${oneDecimal.format(value / 86400)} 天`;
  return `約 ${duration(value)}`;
}

function completionEstimateView(estimate, nowMs = Date.now()) {
  // This finite-backlog forecast is a separate contract from legacy partition projections.
  const info = estimate && typeof estimate === "object" ? estimate : {};
  const expiry = Date.parse(info.valid_until_utc || "");
  const expired = info.state === "stale" || (Number.isFinite(expiry) && expiry <= nowMs);
  const usable = !expired && (["estimated", "conditional", "current"].includes(info.state)
    || ([6, 7, 8, 9].includes(info.schema_version) && info.state === "warming_up"));
  const workload = info.workload || {};
  const exhaustedOnly = usable && workload.planned_requests === 0 && workload.retry_exhausted_tasks > 0;
  const scenarios = ["fastest", "central", "slowest"].map((key) => {
    const actual = info.scenarios?.[key] || {};
    const retryModel = !expired && info.retry_condition?.basis === "next_retry_succeeds_no_additional_failures"
      ? info.retry_condition.scenarios?.[key] : null;
    const conditional = actual.state !== "estimated" && retryModel?.state === "estimated";
    const scenario = conditional ? retryModel : actual;
    const seconds = valueNumber(scenario.remaining_seconds);
    const deadline = Date.parse(scenario.estimated_complete_at_utc || "");
    const overdue = !expired && (scenario.state === "overdue" || ((usable || conditional) && scenario.state === "estimated"
      && Number.isFinite(deadline) && deadline < nowMs && seconds > 0));
    const available = (usable || conditional) && !overdue && scenario.state === "estimated" && Number.isFinite(deadline) && seconds !== null && seconds >= 0;
    const retrying = !expired && info.state === "waiting_retry";
    const waiting = !expired && ["waiting_admission", "waiting_quota", "waiting_retry"].includes(info.state);
    const activeSeconds = valueNumber(scenario.active_work_seconds);
    const completedAt = available ? timeLabel(scenario.estimated_complete_at_utc) : "—";
    const requests = valueNumber(scenario.request_count);
    const rate = valueNumber(scenario.effective_requests_per_hour);
    return {
      key,
      conditional: available && conditional,
      value: expired ? "觀測已過期" : exhaustedOnly ? "重試耗盡，待人工修復" : overdue ? "已超過估計，尚未完成" : available ? `${conditional ? "條件試算 " : ""}${estimateDuration(seconds)}`
        : retrying ? "待重試，完成時間未定" : waiting && activeSeconds !== null ? `放行後 ${estimateDuration(activeSeconds)}`
        : usable && activeSeconds !== null ? `有效工時 ${estimateDuration(activeSeconds)}` : "未知",
      complete: exhaustedOnly || completedAt === "—" ? "完成日期尚無法估算" : `${conditional ? "若下次重試成功，" : ""}預計 ${completedAt}（台北）`,
      detail: conditional
        ? `仍有 ${count(info.retry_condition.retry_tasks)} 個任務待重試；假設已知冷卻後成功、沒有新增錯誤。不是可靠倒數，也不是已抓完。`
        : available
        ? `估計 ${count(requests)} 次請求 · ${info.rate_evidence?.overall_rate_basis === "stage_weighted" ? "全程加權" : "有效"} ${rate === null ? "—" : oneDecimal.format(rate)} 次／小時`
        : overdue ? "估計期限不是成功收據；等待背景重新盤點，不把超時當成完成"
        : retrying ? `仍有 ${count(workload.retry_tasks)} 個失敗／部分回應待重試；成功後工時 ${activeSeconds === null ? "未知" : estimateDuration(activeSeconds)}，不是倒數`
        : waiting ? "僅有效工時；尚不含未知等待時間" : usable && activeSeconds !== null ? "僅工時投影；超出可核實日曆範圍" : "等待可核實的工作量與速度",
      basis: typeof scenario.basis === "string" ? scenario.basis : "尚無此情境的估算依據",
    };
  });
  const stateLabel = expired ? "估算觀測已過期" : ({
    estimated: "三情境估算", conditional: "條件式三情境估算", current: "本輪可執行工作已查驗",
    warming_up: "等待估算樣本", unavailable: "估算暫不可用",
    waiting_admission: "等待必要下載完成／校驗放行", waiting_quota: "等待共用配額恢復",
    waiting_retry: "尚未完成：等待成功重試",
  })[info.state] || "尚無全域估算";
  const blockers = Array.isArray(info.blockers) ? info.blockers.filter((row) => row && typeof row.reason === "string") : [];
  const rates = info.rate_evidence || {};
  const gross = valueNumber(rates.gross_requests_per_hour), recurring = valueNumber(rates.future_recurring_requests_per_hour);
  const effective = valueNumber(rates.effective_requests_per_hour);
  const retryWait = valueNumber(info.retry_wait_seconds);
  const observedMs = Date.parse(info.observed_at_utc || "");
  const retryTiming = retryWait > 0 && Number.isFinite(observedMs)
    ? `取樣時已知重試冷卻最晚到 ${timeLabel(new Date(observedMs + retryWait * 1000).toISOString())}；屆時只是可排，不是完工。`
    : "依現有佇列排重試；不另外增加冷卻。";
  const rateBridge = expired ? "估算取樣已過期，等待背景重新取樣。"
    : info.state === "waiting_retry"
      ? `滾動 60 分鐘實發 ${count(rates.rolling_requests_60m)} 次，是請求發出數，不是成功完成數。仍有 ${count(workload.retry_tasks)} 個任務待重試。${retryTiming}`
    : rates.scheduling_basis === "release_clock_events" && rates.rolling_complete_window && gross !== null && effective !== null
      ? `${rates.stage_label || "目前階段"}：滾動 60 分鐘實發 ${count(rates.rolling_requests_60m)} 次；追新按發布／到期時點優先插入，未到期不扣容量。本階段平均回補速度 ${oneDecimal.format(effective)} 次／小時；此刻實際保留 ${count(rates.current_reserved_requests)} 次（含在途緩衝）。`
    : rates.rolling_complete_window && gross !== null && recurring !== null && effective !== null
      ? `${rates.stage_label || "目前階段"}：滾動 60 分鐘實發 ${count(rates.rolling_requests_60m)} 次 → 每小時 ${oneDecimal.format(gross)} 次，扣除未來追新模型 ${oneDecimal.format(recurring)} 次／小時 → 回補淨速度 ${oneDecimal.format(effective)} 次／小時（受共用限速上限約束）。`
      : "滾動一小時或追新負載證據不足；不以舊的活躍區間中位數替代。";
  return {
    stateLabel, scenarios, rateBridge,
    rateWindow: `估算流量窗 ${timeLabel(rates.rolling_window_start_at_utc)} → ${timeLabel(rates.rolling_window_end_at_utc)}（台北）。流量卡與估算各有每分鐘快照；對帳請比較各自取樣時間，官方帳號用量是另一項觀測。`,
    scope: typeof info.scope_label === "string" ? info.scope_label : "FinMind 已排程可執行工作（含次要校驗）",
    basis: typeof info.basis === "string" ? info.basis : "分割數不等於請求數；等待合批後工作量、共用額度與實際速度估算。",
    observed: `估算基準 ${timeLabel(info.observed_at_utc)}（台北）；依背景觀測更新，不以畫面倒數代替下載進度。`,
    workload: `必要 ${count(workload.required_requests)} 次 · 次要校驗 ${count(workload.validation_requests)} 次 · 合批後共 ${count(workload.planned_requests)} 次預估請求；其中歷史搜尋候選 ${count(workload.candidate_requests ?? 0)} 次（尚未建入佇列，非缺漏筆數）· 待重試 ${count(workload.retry_tasks ?? 0)} 個任務`,
    exclusions: `另列：重試耗盡 ${count(workload.retry_exhausted_tasks || 0)} 個（已停止自動重試、舊資料保留）· 等待日曆 ${count(workload.calendar_wait_tasks || 0)} 個 · 阻塞 ${count(workload.blocked_tasks)} 個任務 · 未排程 ${count(workload.unscheduled_datasets)} 類 · 佇列未能盤點 ${count(workload.unknown_datasets)} 類；這些不算完成，估時僅涵蓋可自動排程工作，未清點的歷史代號與未來新增工作另計。`,
    assumptions: Array.isArray(info.assumptions) ? info.assumptions.filter((item) => typeof item === "string") : [],
    blockers,
  };
}

function stageHasNoPendingWork(item, raw, nowMs = Date.now()) {
  return ["current", "estimated", "conditional"].includes(item.state)
    && Date.parse(item.valid_until_utc || "") > nowMs
    && item.workload?.planned_requests === 0 && raw.forecast_arrival_requests === 0
    && !item.workload?.inflight_tasks && !item.workload?.local_derived_tasks
    && !item.workload?.blocked_tasks && !item.workload?.unknown_datasets
    && !item.workload?.unscheduled_datasets && !item.workload?.retry_tasks && !item.workload?.retry_exhausted_tasks;
}

function renderCompletionEstimate(estimate) {
  const view = completionEstimateView(estimate);
  text("download-global-eta", view.stateLabel);
  text("download-global-eta-basis", view.scope);
  text("download-eta-observed", view.observed);
  text("download-eta-workload", view.workload);
  text("download-eta-exclusions", view.exclusions);
  text("download-eta-method", view.basis);
  text("download-eta-rate-bridge", view.rateBridge);
  text("download-eta-rate-window", view.rateWindow);
  for (const scenario of view.scenarios) {
    text(`download-eta-${scenario.key}`, scenario.value);
    text(`download-eta-${scenario.key}-complete`, scenario.complete);
    text(`download-eta-${scenario.key}-detail`, scenario.detail);
    text(`download-eta-${scenario.key}-basis`, scenario.basis);
  }
  const assumptions = $("download-eta-assumptions"); assumptions.replaceChildren();
  for (const item of view.assumptions) { const li = document.createElement("li"); li.textContent = item; assumptions.append(li); }
  const blockers = $("download-eta-blockers"); blockers.replaceChildren();
  for (const item of view.blockers) { const li = document.createElement("li"); li.textContent = `${item.reason}（${count(item.count)}）`; blockers.append(li); }
  blockers.hidden = view.blockers.length === 0;
  const milestones = $("download-eta-milestones");
  if (milestones) {
    milestones.replaceChildren();
    const stages = Array.isArray(estimate?.stages) ? estimate.stages :
      ["core", "non_tick", "all"].map(key => estimate?.milestones?.[key]).filter(Boolean);
    for (const [index, item] of stages.entries()) {
      const stage = completionEstimateView(item);
      const row = document.createElement("tr");
      const label = document.createElement("td");
      label.textContent = item.history_rank ? `歷史 ${item.history_rank}. ${stage.scope}` : stage.scope;
      const work = document.createElement("small");
      work.textContent = `剩餘 ${count(item.workload?.planned_requests)} 次（含 ${count(item.workload?.candidate_requests ?? 0)} 次未建候選）；待重試 ${count(item.workload?.retry_tasks ?? 0)} 個；累計 ${count(item.cumulative_planned_requests)} 次`;
      label.append(work); row.append(label);
      if (item.dataset) {
        const dataset = state.latest?.datasets?.find(value => value.id === item.dataset);
        const checked = dataset?.checked_partitions, total = dataset?.target_partitions;
        const completion = ratio(checked, total);
        const progress = document.createElement("progress");
        progress.className = "estimate-stage-progress"; progress.max = 1;
        progress.setAttribute("aria-label", `${stage.scope}候選查詢進度（非資料完整率）`);
        if (completion !== null) progress.value = completion;
        const copy = document.createElement("small");
        copy.textContent = `已查驗 ${count(checked)}／${count(total)} 候選 · ${percent(completion)}（非資料完整率）；非空 ${count(dataset?.complete_partitions)} · ${count(dataset?.rows)} 筆`;
        label.append(progress, copy);
        row.dataset.historyRank = String(item.history_rank);
        row.dataset.sourceDataset = item.dataset;
      }
      for (const scenario of stage.scenarios) {
        const raw = scenario.conditional
          ? item.retry_condition.scenarios[scenario.key] : item.scenarios?.[scenario.key] || {};
        const cell = document.createElement("td");
        const noWork = stageHasNoPendingWork(item, raw);
        cell.textContent = noWork ? "目前無已知待發請求" : `${scenario.value}；${scenario.complete}`;
        const detail = document.createElement("small");
        const canShow = raw.state === "estimated"
          && (scenario.conditional || !["stale", "unavailable", "waiting_retry"].includes(item.state))
          && Date.parse(item.valid_until_utc || "") > Date.now() && Date.parse(raw.estimated_complete_at_utc || "") >= Date.now();
        detail.textContent = noWork ? "此階段不增加目前排程時間；未來新增需求另計。" : canShow && raw.stage_start_at_utc
          ? `階段開始 ${timeLabel(raw.stage_start_at_utc)}；本階段 ${estimateDuration(raw.stage_duration_seconds)}`
          : "開始時間待前置階段／額度證據核實";
        const flow = document.createElement("small");
        flow.textContent = `本階段淨速度 ${count(raw.effective_requests_per_hour)} 次／小時；等待期間新增日分區模型 ${count(raw.forecast_arrival_requests)} 次`;
        cell.append(detail, flow); row.append(cell);
      }
      milestones.append(row);
    }
  }
}

function svgNode(name, attributes = {}) {
  const node = document.createElementNS(SVG_NS, name);
  for (const [key, value] of Object.entries(attributes)) node.setAttribute(key, String(value));
  return node;
}

function drawQuota(data) {
  const svg = $("quota-chart");
  svg.querySelectorAll(".drawn").forEach((node) => node.remove());
  document.querySelectorAll("#quota-time-range button").forEach((button) => {
    const active = button.dataset.range === state.quotaRange;
    button.classList.toggle("active", active);
    button.setAttribute("aria-pressed", String(active));
  });
  const earliest = Date.now() - (state.quotaRange === "1h" ? 3600e3 : 86400e3);
  const rows = (Array.isArray(data.history) ? data.history : []).map((row) => ({
    x: Date.parse(row.at_utc || ""), y: valueNumber(row.observed_requests_60m),
  })).filter((row) => Number.isFinite(row.x) && row.y !== null && row.x >= earliest);
  $("quota-chart-empty").hidden = rows.length >= 2;
  if (rows.length < 2) return;
  const limit = valueNumber(data.official_requests_per_hour) || 300;
  const left = 58, right = 926, top = 24, bottom = 220;
  const minX = rows[0].x, maxX = rows[rows.length - 1].x;
  if (minX >= maxX) return;
  const maxY = Math.max(limit, ...rows.map((row) => row.y), 1);
  const x = (value) => left + (value - minX) / (maxX - minX) * (right - left);
  const y = (value) => bottom - value / maxY * (bottom - top);
  for (const tickValue of [0, limit]) {
    svg.append(svgNode("line", {class: `drawn ${tickValue === limit ? "ceiling" : "axis"}`, x1: left, y1: y(tickValue), x2: right, y2: y(tickValue)}));
    const label = svgNode("text", {class: "drawn tick", x: 12, y: y(tickValue) + 4});
    label.textContent = count(tickValue);
    svg.append(label);
  }
  const firstLabel = svgNode("text", {class: "drawn tick", x: left, y: 250, "text-anchor": "start"});
  firstLabel.textContent = new Date(minX).toLocaleString("zh-TW", {timeZone: "Asia/Taipei", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit"});
  svg.append(firstLabel);
  const lastLabel = svgNode("text", {class: "drawn tick", x: right, y: 250, "text-anchor": "end"});
  lastLabel.textContent = new Date(maxX).toLocaleString("zh-TW", {timeZone: "Asia/Taipei", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit"});
  svg.append(lastLabel);
  const segments = [];
  let current = [], previous = null;
  for (const row of rows) {
    if (previous !== null && row.x - previous > 12 * 60e3) { if (current.length > 1) segments.push(current); current = []; }
    current.push(`${x(row.x).toFixed(2)},${y(row.y).toFixed(2)}`);
    previous = row.x;
  }
  if (current.length > 1) segments.push(current);
  for (const points of segments) svg.append(svgNode("polyline", {class: "drawn series", points: points.join(" ")}));
  const last = rows[rows.length - 1];
  svg.append(svgNode("circle", {class: "drawn point", cx: x(last.x), cy: y(last.y), r: 4}));
}

function renderQuota(info) {
  const used = valueNumber(info.observed_requests_60m);
  const limit = valueNumber(info.official_requests_per_hour);
  const completeWindow = info.state === "complete_worker_window";
  text("quota-used", used === null ? "—" : `${completeWindow ? "" : "≥"}${count(used)} 次`);
  text("quota-limit", limit === null ? "—" : `${count(limit)} 次／小時`);
  text("quota-headroom", completeWindow ? `${count(info.worker_headroom_60m)} 次` : "無法核定");
  const allocation = info.backfill_allocation;
  text("quota-backfill", allocation?.basis === "provider_observation_plus_local_starts"
    ? (allocation.schedule_verified === false ? "排程待核實" : allocation.priority_wait ? "追新優先" : `${count(Math.max(0, allocation.remaining - allocation.reserve))} 次`) : "無法核定");
  text("quota-reserved", allocation?.basis === "provider_observation_plus_local_starts"
    ? `本發布時段保留 ${count(allocation.reserve)} 次（含 2 次在途緩衝）；已到期 ${count(allocation.ready_incremental_requests)} 次優先派送；其餘時段供回補使用。排程取樣 ${timeLabel(allocation.observed_at_utc)}（每分鐘更新，非即時餘額）`
    : allocation?.snapshot_state === "stale" ? "排程取樣已過期；不能用舊餘額判定目前可抓，等待每分鐘更新。"
    : "帳號用量或本機請求帳本未核實；不能判定歷史回補是否放行");
  text("quota-ratio", used === null || !limit ? "—" : `${completeWindow ? "" : "≥"}${percent(Math.min(1, used / limit))}`);
  text("quota-basis", completeWindow ? "本站 worker 滾動 60 分鐘（每分鐘取樣）；非帳號總用量"
    : info.state === "partial_worker_window" ? "本站追蹤未滿一小時，為觀測下界"
    : info.state === "stale" ? "本站流量取樣已過期，不能當成目前用量" : "本站流量尚無可用取樣；不等於用量為零");
  text("quota-token", info.account_tier ? `官方帳號層級 ${info.account_tier}；最近帳號取樣 ${count(info.provider_used_in_hour)} 次` : "尚未驗證帳號層級");
  text("quota-observed", info.provider_observed_at_utc ? `帳號取樣 ${timeLabel(info.provider_observed_at_utc)}；本站流量取樣 ${timeLabel(info.observed_at_utc)}；紀錄始於 ${timeLabel(info.tracking_started_at_utc)}` : "尚無近期官方帳號取樣；本站請求不含其他應用");
  const progress = $("quota-progress");
  if (used === null || !limit) progress.removeAttribute("value"); else progress.value = Math.max(0, Math.min(1, used / limit));
  drawQuota(info);
}

function renderPipelines(data) {
  const rows = Array.isArray(data.datasets) ? data.datasets : [];
  text("pipeline-total", count(rows.length));
  text("pipeline-active", count(rows.filter((row) => row.state === "backfilling").length));
  text("pipeline-ready", count(rows.filter((row) => row.state === "complete").length));
  text("pipeline-attention", count(rows.filter((row) => !["backfilling", "complete"].includes(row.state)).length));
  const selected = rows.filter((row) => state.filter === "all" || (state.filter === "reference" ? ["reference", "snapshot"].includes(row.kind) : row.kind === state.filter));
  $("pipeline-empty").hidden = selected.length > 0;
  const grid = $("pipeline-grid"); grid.replaceChildren();
  for (const row of selected) {
    const visualState = row.state === "complete" ? "ready" : row.state === "backfilling" ? "active" : "waiting";
    const card = document.createElement("article"); card.className = `pipeline-card state-${visualState}`;
    card.innerHTML = '<div class="pipeline-card-header"><div class="pipeline-tags"><span class="category-tag"></span><span class="pipeline-status"></span></div><span class="api-surface"></span></div><h3></h3><p class="pipeline-detail"></p><div class="mini-progress"><div class="mini-progress-copy"><span>目前任務查驗率</span><strong></strong></div><progress class="mini-progress-track" max="1"></progress><small></small></div><div class="pipeline-eta"><span class="eta-label">完成時間估算</span><strong class="eta-value"></strong><small class="eta-basis"></small></div><div class="pipeline-footer"></div>';
    card.querySelector(".category-tag").textContent = row.kind.startsWith("sponsor_") ? "Sponsor 全市場" : ({session_history: "全市場盤中", snapshot: "主檔快照", reference: "交易日曆", global_history: "全市場／總經", symbol_history: "逐檔／固定指標", global_equity_history: "海外逐檔"})[row.kind] || "來源資料";
    const badge = card.querySelector(".pipeline-status"); badge.className += ` ${visualState}`; badge.textContent = datasetStatus(row);
    card.querySelector(".api-surface").textContent = row.id;
    card.querySelector("h3").textContent = row.history_rank ? `歷史 ${row.history_rank}. ${row.label}` : row.label;
    if (row.history_rank) card.dataset.historyRank = String(row.history_rank);
    const checked = row.checked_partitions ?? row.complete_partitions;
    const candidates = valueNumber(row.unseeded_partition_candidates) ?? 0;
    card.querySelector(".pipeline-detail").textContent = row.kind === "sponsor_unscheduled" ? `尚未排程：${row.source_status}` : `已查驗 ${count(checked)}／${count(row.target_partitions)} · 未建搜尋候選 ${count(candidates)} · 非空 ${count(row.complete_partitions)} · ${count(row.rows)} 筆 · ${bytes(row.local_bytes)} · 重試耗盡 ${count(row.retry_exhausted_partitions || 0)}（保留 ${count(row.retained_rows || 0)} 筆）· 空回 ${count(row.observed_empty_partitions || 0)}／待重試 ${count(row.deferred_partitions || 0)}／權限 ${count(row.not_entitled_partitions || 0)}／參數 ${count(row.invalid_request_partitions || 0)}`;
    card.querySelector(".mini-progress-copy span").textContent = candidates ? "候選查詢進度（非資料完整率）" : "已建任務查驗率";
    const completion = ratio(checked, row.target_partitions);
    const progress = card.querySelector("progress"); if (completion === null) progress.removeAttribute("value"); else progress.value = completion;
    card.querySelector(".mini-progress-copy strong").textContent = percent(completion);
    card.querySelector(".mini-progress small").textContent = `資料日期 ${row.first_data_date || "未取得"} → ${row.last_data_date || "未取得"}`;
    card.querySelector(".eta-value").textContent = networkTimeLabel(row);
    card.querySelector(".eta-basis").textContent = networkTimeBasis(row);
    const footer = card.querySelector(".pipeline-footer");
    footer.classList.add("update-monitor");
    for (const line of updateMonitorLines(row.update_monitor)) {
      const entry = document.createElement("p"); entry.textContent = line; footer.append(entry);
    }
    grid.append(card);
  }
}

function renderStorage(data) {
  text("storage-source", bytes(data.storage?.local_bytes));
  text("storage-free", bytes(data.storage?.filesystem_free_bytes));
  text("storage-total", data.storage?.estimated_total_bytes == null ? "未知" : bytes(data.storage.estimated_total_bytes));
  text("training-state", data.scope?.cold_published === true ? "已發版" : "未發版");
  renderStorageBars(Array.isArray(data.datasets) ? data.datasets : []);
}

function renderStorageBars(datasets) {
  const bars = $("storage-bars"); bars.replaceChildren();
  const rows = datasets.filter((row) => valueNumber(row.local_bytes) !== null);
  const total = rows.reduce((sum, row) => sum + Number(row.local_bytes), 0);
  for (const row of rows) {
    const wrap = document.createElement("div"), copy = document.createElement("div"), label = document.createElement("span"), size = document.createElement("strong"), progress = document.createElement("progress");
    copy.className = "storage-bar-copy"; label.textContent = row.label; size.textContent = bytes(row.local_bytes); copy.append(label, size);
    progress.className = "storage-progress"; progress.max = 1; progress.value = total > 0 ? Number(row.local_bytes) / total : 0;
    wrap.append(copy, progress); bars.append(wrap);
  }
  if (!rows.length) { const empty = document.createElement("p"); empty.className = "empty"; empty.textContent = "尚無可核實的來源容量"; bars.append(empty); }
}

function renderBackfill(data) {
  const info = data.acquisition || {}, total = valueNumber(info.total_tasks), complete = valueNumber(info.complete_tasks), checked = valueNumber(info.checked_tasks) ?? complete;
  const historyOrder = info.history_order || {};
  const sequence = $("history-sequence");
  if (sequence) sequence.textContent = `${(historyOrder.stages || []).map(item => item.label).join(" → ") || "順序觀測未載入"}。${historyOrder.applied ? "下載器已套用" : "等待下載器套用確認"}；到期追新與明確有限優先回補可插入，前階段未建候選不算抓完。`;
  const coverage = ratio(checked, total);
  text("download-progress-label", total === null ? "分母未核實" : `已查驗 ${count(checked)} / ${count(total)} · ${percent(coverage)}`);
  text("download-progress-detail", `已建任務 ${count(info.materialized_tasks)} · 未建歷史搜尋候選 ${count(info.unseeded_candidate_tasks)} · 非空 ${count(complete)}。${count(info.unknown_universe_datasets)} 類主檔待載入；此為候選搜尋進度，非全歷史完整率。`);
  renderCompletionEstimate(info.completion_estimate);
  text("download-count", `${count(checked)}／${count(total)}`);
  text("download-pending", `已建待辦 ${count(info.materialized_pending_tasks)} · 未建候選 ${count(info.unseeded_candidate_tasks)} · 重試耗盡 ${count(info.retry_exhausted_tasks || 0)} 個（停止自動重試，保留 ${count(info.retained_rows || 0)} 筆舊資料）· 空回 ${count(info.observed_empty_tasks)} · 權限 ${count(info.not_entitled_tasks)} · 參數 ${count(info.invalid_request_tasks)}`);
  text("download-deferred", count(info.retry_deferred_tasks));
  const last = info.latest_result;
  text("download-last", last?.partition || last?.date || "—");
  text("download-last-detail", last ? `${last.dataset} · ${last.data_id || "全市場"} · ${last.status || "未知"} · ${count(last.rows)} 筆` : "尚無可核實的最近結果");
  const progress = $("download-progress"); if (coverage === null) progress.removeAttribute("value"); else progress.value = coverage;
  renderDatasetRows(Array.isArray(data.datasets) ? data.datasets : []);
}

function renderDatasetRows(rows) {
  const body = $("dataset-rows"); body.replaceChildren();
  for (const row of rows) {
    const tr = document.createElement("tr");
    const grains = row.observed_grains || {};
    const grainLabel = row.kind === "session_history"
      ? [["1m", "1 分"], ["15s", "15 秒"], ["10s", "10 秒"], ["5s", "5 秒"]]
          .map(([key, label]) => `${count(grains[key] ?? 0)} 日 ${label}`).join(" · ")
      : `空 ${count(row.observed_empty_partitions || 0)} · 待重試 ${count(row.deferred_partitions || 0)} · 重試耗盡 ${count(row.retry_exhausted_partitions || 0)}（保留 ${count(row.retained_rows || 0)} 筆）· 權限 ${count(row.not_entitled_partitions || 0)} · 參數 ${count(row.invalid_request_partitions || 0)}`;
    const cells = [row.label, datasetStatus(row), `${count(row.checked_partitions ?? row.complete_partitions)}／${count(row.target_partitions)}（非空 ${count(row.complete_partitions)}）`, row.first_data_date || "—", row.last_data_date || "—", count(row.rows), bytes(row.local_bytes), grainLabel, networkTimeLabel(row), updateMonitorLines(row.update_monitor).join("\n")];
    for (const cell of cells) { const td = document.createElement("td"); td.textContent = cell; tr.append(td); }
    body.append(tr);
  }
  if (!body.children.length) { const tr = document.createElement("tr"), td = document.createElement("td"); td.colSpan = 10; td.textContent = "尚未取得管線觀測"; tr.append(td); body.append(tr); }
}

function renderCapture(data) {
  const info = data.acquisition || {};
  const running = data.health === "updating";
  const labels = {running: "正在抓取", backfilling: "持續回補", current: "目前任務已查驗", current_queue: "待下一輪查新", batch_complete: "本批結束，準備下一批", waiting: "等待下一輪排程", stale: "觀測已過期", unavailable: "尚無觀測", degraded: "來源需注意", protected_opening: "開盤保護暫停", waiting_retry: "等待短期重試", waiting_history_retry: "等待前階段歷史重試", rate_limited: "來源節流", disk_guard: "磁碟保留量不足", invalid_token: "Token 無效", waiting_necessary_acquisition: "等待必要下載完成後校驗", incremental_reserve: "預留追新配額"};
  text("capture-state", `整體 ${labels[info.state] || info.state || "待核實"} · Free ${labels[info.free_state] || info.free_state || "待核實"} · 補充 ${labels[info.complement_state] || info.complement_state || "待核實"} · Sponsor ${labels[info.sponsor_state] || info.sponsor_state || "待核實"}`);
  text("capture-freshness", `盤中 ${timeLabel(info.observed_at_utc)} · 補充 ${timeLabel(info.complement_observed_at_utc)} · Sponsor ${timeLabel(info.sponsor_observed_at_utc)}；最近觀測 ${ageLabel(data.status_age_seconds)}`);
  const workers = Object.entries(info.workers || {}).filter(([, worker]) => worker.alive);
  if (workers.length) {
    const names = {free: "盤中", complement: "補充", sponsor: "Sponsor"};
    $("capture-freshness").textContent += `；存活／下次檢查：${workers.map(([name, worker]) => `${names[name] || name} ${worker.state === "running" ? "正在執行" : timeLabel(worker.next_check_at_utc)}`).join(" · ")}（心跳不代表新資料）`;
  }
  text("capture-key", info.sponsor_active_tasks?.[0]?.dataset ? `${info.sponsor_active_tasks[0].dataset} · ${info.sponsor_active_tasks[0].partition}` : info.complement_active_task?.dataset ? `${info.complement_active_task.dataset} · ${info.complement_active_task.data_id || info.complement_active_task.partition || ""}` : info.active_task?.dataset ? `${info.active_task.dataset} · ${info.active_task.date || ""}` : running ? "執行中，等待下一次工作取樣" : "目前未觀測到進行中的請求");
  const last = info.latest_result;
  text("capture-last", last ? `${last.dataset} · ${last.data_id || last.partition || last.date || ""} · ${last.status || "未知"} · ${count(last.rows)} 筆${last.observed_at_utc ? ` · ${timeLabel(last.observed_at_utc)}` : ""}` : "尚無可核實的最近結果");
  const next = Array.isArray(info.queue_preview) ? info.queue_preview[0] : null;
  text("capture-next", info.complement_next_task?.dataset ? `${info.complement_next_task.dataset} · ${info.complement_next_task.data_id || info.complement_next_task.partition || ""}` : next?.dataset ? `${next.dataset} · ${next.date || ""}` : "無待補佇列／尚未清點");
  const universe = info.candidate_universe || {};
  text("capture-universe", `${count(universe.finmind_current_master_stock_ids)} 個現行主檔代號 + ${count(universe.additional_official_delisted_ids)} 個額外官方下市代號；歷史全域仍待驗證`);
  $("capture-dot").classList.toggle("active", running);
  const alert = $("pipeline-alert"); alert.hidden = data.health === "updating" || data.health === "waiting";
  if (!alert.hidden) { text("alert-title", data.health === "stale" ? "下載狀態逾時" : "資料管線需檢查"); text("alert-copy", "既有來源收據仍保留，但目前不能把服務存活或 API 200 當作全歷史完整。新聞已納入共用配額排程。"); }
}

function render(data) {
  if (data.read_only !== true || data.production_control_possible !== false) throw new Error("untrusted FinMind status");
  state.latest = data;
  renderQuota(data.quota || {});
  renderPipelines(data);
  renderStorage(data);
  renderBackfill(data);
  renderCapture(data);
  const badge = $("finmind-health"); badge.className = `status ${data.health || "unavailable"}`;
  badge.lastChild.textContent = ({updating: "歷史回補進行中", waiting: "等待排程／來源", degraded: "來源需注意", stale: "下載狀態逾時", unavailable: "資料尚未就緒"})[data.health] || "狀態待核實";
  text("finmind-freshness", `本機三支下載器最近觀測 ${ageLabel(data.status_age_seconds)} · 面板 ${timeLabel(data.generated_at_utc)}`);
}

function scheduleRetry() {
  if (state.retryTimer !== null) return;
  const wait = state.retryDelayMs;
  state.retryTimer = window.setTimeout(() => { state.retryTimer = null; refresh(); }, wait);
  state.retryDelayMs = Math.min(wait * 2, 30000);
}

async function refresh() {
  if (document.hidden || state.inFlight) return;
  state.inFlight = true;
  try {
    render(await fetchJson("api/status"));
    if (state.retryTimer !== null) window.clearTimeout(state.retryTimer);
    state.retryTimer = null; state.retryDelayMs = 3000;
  } catch (error) {
    // Failed refreshes must still expire old forecasts instead of leaving a live-looking ETA.
    renderCompletionEstimate(state.latest?.acquisition?.completion_estimate);
    const badge = $("finmind-health"); badge.className = `status ${state.latest ? "stale" : "unavailable"}`;
    badge.lastChild.textContent = state.latest ? "更新暫停，顯示上次資料" : "面板暫時無法讀取";
    text("finmind-freshness", state.latest ? `上次成功 ${timeLabel(state.latest.generated_at_utc)}；稍後重試。` : "稍後重試；不會呼叫 FinMind API 補畫面。");
    console.warn("FinMind status request failed", error);
    scheduleRetry();
  } finally { state.inFlight = false; }
}

document.querySelectorAll("[data-pipeline-filter]").forEach((button) => button.addEventListener("click", () => {
  state.filter = button.dataset.pipelineFilter;
  document.querySelectorAll("[data-pipeline-filter]").forEach((other) => { const active = other === button; other.classList.toggle("active", active); other.setAttribute("aria-pressed", String(active)); });
  if (state.latest) renderPipelines(state.latest);
}));
document.querySelectorAll("#quota-time-range button").forEach((button) => button.addEventListener("click", () => { state.quotaRange = button.dataset.range; if (state.latest) drawQuota(state.latest.quota || {}); }));
drawQuota = Dashboard.createDeferredRenderer("quota-chart", drawQuota);
renderPipelines = Dashboard.createDeferredRenderer("pipelines", renderPipelines);
renderStorageBars = Dashboard.createDeferredRenderer("storage", renderStorageBars);
renderDatasetRows = Dashboard.createDeferredRenderer("backfill", renderDatasetRows);
Dashboard.scheduleRefresh(refresh, {intervalMs: 30000});

"use strict";

const Dashboard = window.StockAgentDashboard;
const fetchJson = Dashboard.createJsonFetcher({timeoutMs: 15000, cache: "no-store", expectedRoot: "object"});
const $ = Dashboard.byId;
const SVG_NS = "http://www.w3.org/2000/svg";
const state = {datasets: [], filter: "all", group: "all", sort: "queue", groupSignature: "", pipelineFilter: "all", search: "", visible: 100, marketSearch: "", marketVisible: 100, inFlight: false, quotaRange: "1d", quotaHistory: [], latest: null, retryTimer: null, retryDelayMs: 3000};
const ranges = {"1h": 3600e3, "1d": 86400e3, "1w": 7 * 86400e3, "1mo": 30 * 86400e3, all: Infinity};
const integer = new Intl.NumberFormat("zh-TW", {maximumFractionDigits: 0});
const decimal = new Intl.NumberFormat("zh-TW", {maximumFractionDigits: 1});
const stateLabels = {downloaded: "已取得", pending: "待下載", deferred_resource: "資源暫緩", deferred_windowed: "需分日期", partial_windowed: "分日期回補中", resource_timeout: "查詢逾時", provider_error: "SDK 錯誤", provider_empty: "來源全空", vip_only: "權限待核", authentication_failed: "登入失敗", quota_wait: "額度不足"};

function n(value) { if (value == null || value === "") return null; const parsed = Number(value); return Number.isFinite(parsed) ? parsed : null; }
function count(value) { const valueNumber = n(value); return valueNumber === null ? "—" : integer.format(valueNumber); }
function mb(value) { const valueNumber = n(value); return valueNumber === null ? "—" : `${decimal.format(valueNumber)} MB`; }
function bytes(value) { const valueNumber = n(value); if (valueNumber === null) return "—"; if (valueNumber < 1024 * 1024) return `${decimal.format(valueNumber / 1024)} KiB`; if (valueNumber < 1024 ** 3) return `${decimal.format(valueNumber / 1024 ** 2)} MiB`; return `${decimal.format(valueNumber / 1024 ** 3)} GiB`; }
function pct(value) { const valueNumber = n(value); return valueNumber === null ? "—" : `${(valueNumber * 100).toFixed(1)}%`; }
function timeLabel(value) { const date = new Date(value || ""); return Number.isNaN(date.getTime()) ? "—" : date.toLocaleString("zh-TW", {timeZone: "Asia/Taipei", hour12: false, year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit"}); }
function ageLabel(value) { return Dashboard.formatAge(value, {emptyLabel: "時間未核實", hourDigits: 0, dayDigits: 0}); }
function durationLabel(value) { const seconds = n(value); if (seconds === null) return "—"; if (seconds < 60) return `${Math.ceil(seconds)} 秒`; if (seconds < 3600) return `${decimal.format(seconds / 60)} 分`; if (seconds < 86400) return `${decimal.format(seconds / 3600)} 小時`; return `${decimal.format(seconds / 86400)} 天`; }
function text(id, value) { $(id).textContent = value; }

function tickScenarioLabel(scenario) {
  if (scenario?.state === "complete") return "此候選工作已完成";
  if (scenario?.finish_at_utc) return timeLabel(scenario.finish_at_utc);
  return ({unscheduled: "未啟用排程", bounded_source_discovery: "每日有界探查，未承諾日期", waiting_required_general_data: "等待一般資料可執行缺口清完", waiting_higher_priority_minutes: "先完成分鐘回補", insufficient_samples: "完整批次樣本不足", quota_unverified: "配額觀測待更新", general_budget_unmeasured: "待完整配額週期", transfer_unmeasured: "計費流量待量測", capacity_constrained: "一般追新用完額度", horizon_exceeded: "超過十年估算上限"}[scenario?.state] || "等待估時資料");
}

function renderTickSummary(data) {
  const minuteFirst = data.intraday_market?.acquisition_mode === "minute_first_tick_residual";
  const worker = data.execution?.intraday || data.execution?.tick || {};
  const pipeline = ["bounded_range_process_pool", "rolling_range_process_pool"].includes(worker.processing_engine)
    ? `；${count(worker.pipeline_workers)} 個下載／驗證程序、授權最多 ${count(worker.authorization_slots ?? worker.pipeline_workers)} 路${n(worker.download_threads) > 1 ? `、每組最多 ${count(worker.download_threads)} 路檔案下載` : ""}、日期窗最多 ${count(worker.range_calendar_days)} 天${worker.http_keepalive ? "、重用 HTTP 連線" : ""}；在途 ${count(worker.inflight_partitions)} 個股票日${worker.processing_engine === "rolling_range_process_pool" ? "（空位立即派工）" : ""}` : "";
  text("minute-priority-now", worker.owner_alive && worker.state === "running"
    ? `目前 ${data.execution?.phase === "minute" ? "優先抓分鐘" : "抓剩餘配額 Tick"}：${worker.active_key || "選擇下個分區"} ${worker.active_trade_date || ""}；本批成功 ${count(worker.successful)}／已完成查詢 ${count(worker.attempted)}${pipeline}${worker.estimated_batch_finish_at_utc ? `；本批參考完成 ${timeLabel(worker.estimated_batch_finish_at_utc)}（非全歷史）` : ""}。`
    : worker.state === "source_rate_limited" ? `來源要求冷卻，${timeLabel(worker.source_backoff_until_utc)} 後再排；已取得資料保留，不重啟衝破來源限制。`
    : minuteFirst ? "分鐘優先；來源未上架保持等待，已存資料保留。Tick 不會搶分鐘回補或一般追新的預留配額。" : "等待分鐘優先排程的最新收據。");
  const info = (minuteFirst ? data.minute_progress : data.tick_progress) || {}, phases = info.phases || [];
  const recent = phases.find(row => row.id === "observed_history") || phases.find(row => row.id === "latest_session");
  text("market-eta", tickScenarioLabel(recent?.scenarios?.reference));
  text("tick-eta-summary", recent ? `${minuteFirst ? "分鐘優先" : "Tick"} · ${recent.start_date || "—"}–${recent.end_date || "—"}；若均已發布，剩約 ${durationLabel(recent.scenarios?.reference?.remaining_seconds)}。來源補檔等待另計。` : "等待分階段量測。");
  const measured = info.measurement || {}, rate = n(measured.seconds_per_acquired_partition);
  text("tick-estimation-basis", `${minuteFirst ? "直接分鐘" : "Tick"}下載樣本 ${count(measured.data_batches)} 個完整批次／${count(measured.data_partitions)} 個新取得股票日${rate > 0 ? `；約 ${decimal.format(3600 / rate)} 股票日／小時` : ""}。兩種粒度各自量測，不混用速度或計費。空回應探查樣本 ${count(measured.discovery_batches)} 批另計，不拿查無資料速度當下載速度。估時包含驗證、本機處理、批間等待，扣除 ${count(info.general_quota_cycles?.length)} 個完整每日週期的一般資料耗量；配額每天 08:00 重置。筆數與容量由已取得分區均值外推，未抓股票與年代可能不同。更早分鐘探索每天最多 16 個候選，不能沿用 Tick 的全速探索 ETA。清冊 ${timeLabel(info.inventory_at_utc)}${data.intraday_market?.fresh === false ? "（舊快照，等待更新）" : ""}；估計 ${timeLabel(info.generated_at_utc)}。`);
}

function renderQuota(data) {
  const quota = data.quota || {};
  const used = n(quota.used_mb);
  const limit = n(quota.limit_mb);
  const ratio = n(quota.used_ratio);
  const reserve = n(data.acquisition?.quota_reserve_mb) ?? 50;
  text("quota-used", mb(used));
  text("quota-limit", mb(limit));
  text("quota-remaining", mb(quota.remaining_mb));
  text("quota-safe-remaining", limit === null || used === null ? "—" : mb(Math.max(0, limit - used - reserve)));
  const incrementalExempt = data.workload?.state === "available" && data.workload?.incremental_quota_policy === "provider_enforced_no_local_reserve";
  text("quota-reserve-basis", incrementalExempt
    ? `歷史回補／整表保留 ${mb(reserve)}；已驗證 SDK 追新不受此本機門檻限制。帳號用量每分鐘取樣，官方實際限額仍有效。`
    : `歷史回補／整表保留 ${mb(reserve)}；追新規則待新鮮快照核實，官方帳號觀測另列。`);
  const intradayPolicy = data.intraday_quota_policy;
  if (intradayPolicy?.state === "available" && n(intradayPolicy.minute?.effective_reserve_mb) !== null) {
    const minute = intradayPolicy.minute;
    const minuteReserve = n(minute.effective_reserve_mb);
    text("quota-safe-remaining", n(quota.remaining_mb) === null ? "—" : mb(Math.max(0, n(quota.remaining_mb) - minuteReserve)));
    text("quota-reserve-basis", minute.mode === "pre_reset_drain"
      ? `重置前消化剩餘配額：分鐘線本機預留 ${mb(minuteReserve)}；仍先追新、遵守官方配額，來源沒資料不會重抓湊流量。帳號用量每分鐘取樣。`
      : `分鐘線保留 ${mb(minuteReserve)}；Tick 預留另計，不會卡住分鐘回補。${incrementalExempt ? "已驗證 SDK 追新不受此本機門檻限制。" : "一般追新優先。"}重置前最後 ${count(minute.drain_before_reset_minutes)} 分鐘解除本機預留；帳號用量每分鐘取樣。`);
  }
  text("quota-percent", ratio === null ? "無帳號觀測" : `${pct(ratio)} 的每日額度`);
  text("quota-ratio", pct(ratio));
  text("quota-reset", timeLabel(quota.reset_at_utc));
  text("quota-observed", quota.observed_at_utc
    ? `SDK 帳號觀測 ${timeLabel(quota.observed_at_utc)} · ${ageLabel(data.quota_age_seconds)}`
    : "尚無獨立帳號觀測；不以下載回執推算目前餘額");
  const progress = $("quota-progress");
  if (ratio === null) progress.removeAttribute("value");
  else progress.value = Math.min(1, Math.max(0, ratio));
  text("request-limit", data.quota_definition?.request_per_second_limit == null
    ? "官方未公布" : `${count(data.quota_definition.request_per_second_limit)} req/s`);
  state.quotaHistory = Array.isArray(data.quota_history) ? data.quota_history : [];
  drawQuotaHistory(state.quotaHistory);
  if (data.quota_history_state === "unavailable") {
    text("quota-chart-empty", "歷史用量觀測暫時無法讀取；目前額度仍依帳號最新回執顯示。");
    $("quota-chart-empty").hidden = false;
  } else {
    text("quota-chart-empty", "累積兩筆帳號觀測後顯示趨勢。");
  }
  renderQuotaRows(state.quotaHistory);
}

function renderQuotaRows(rows) {
  const body = $("quota-history-body");
  body.replaceChildren();
  for (const row of rows.slice(-30).reverse()) {
    const tr = document.createElement("tr");
    [timeLabel(row.observed_at_utc), mb(row.used_mb), mb(n(row.limit_mb) - n(row.used_mb)), pct(n(row.used_mb) / n(row.limit_mb))].forEach((value) => { const td = document.createElement("td"); td.textContent = value; tr.append(td); });
    body.append(tr);
  }
  if (!body.children.length) { const tr = document.createElement("tr"), td = document.createElement("td"); td.colSpan = 4; td.textContent = "尚無用量觀測"; tr.append(td); body.append(tr); }
}

function svgNode(name, attributes = {}) {
  const node = document.createElementNS(SVG_NS, name);
  for (const [key, value] of Object.entries(attributes)) node.setAttribute(key, String(value));
  return node;
}

function drawQuotaHistory(rows) {
  const svg = $("quota-chart");
  svg.querySelectorAll(".drawn").forEach((node) => node.remove());
  const now = Date.now();
  const points = (Array.isArray(rows) ? rows : []).map((row) => ({
    x: Date.parse(row.observed_at_utc || ""),
    ratio: n(row.limit_mb) > 0 ? n(row.used_mb) / n(row.limit_mb) : NaN,
  })).filter((row) => Number.isFinite(row.x) && Number.isFinite(row.ratio) && now - row.x <= ranges[state.quotaRange]);
  document.querySelectorAll("#quota-time-range button").forEach((button) => { const selected = button.dataset.range === state.quotaRange; button.classList.toggle("active", selected); button.setAttribute("aria-pressed", String(selected)); });
  $("quota-chart-empty").hidden = points.length >= 2;
  if (points.length < 2) return;
  const left = 58, right = 926, top = 24, bottom = 220;
  const first = points[0].x, last = points[points.length - 1].x;
  if (last <= first) return;
  const maxY = Math.max(1, Math.ceil(Math.max(...points.map((row) => row.ratio)) * 10) / 10);
  const x = (value) => left + ((value - first) / (last - first)) * (right - left);
  const y = (value) => bottom - (value / maxY) * (bottom - top);
  for (const value of [0, .5, 1]) {
    if (value > maxY) continue;
    svg.append(svgNode("line", {class: "drawn axis", x1: left, y1: y(value), x2: right, y2: y(value)}));
    const tick = svgNode("text", {class: "drawn tick", x: 11, y: y(value) + 4});
    tick.textContent = pct(value);
    svg.append(tick);
  }
  const endLabels = [[first, left, "start"], [last, right, "end"]];
  for (const [timestamp, position, anchor] of endLabels) {
    const tick = svgNode("text", {class: "drawn tick", x: position, y: 251, "text-anchor": anchor});
    tick.textContent = new Date(timestamp).toLocaleDateString("zh-TW", {timeZone: "Asia/Taipei", month: "2-digit", day: "2-digit"});
    svg.append(tick);
  }
  let segment = [];
  const flush = () => {
    if (segment.length > 1) svg.append(svgNode("polyline", {class: "drawn series", points: segment.join(" ")}));
    segment = [];
  };
  let previous = null;
  for (const point of points) {
    const expectedGap = ranges[state.quotaRange] > ranges["1d"] ? 30 : 20;
    if (previous !== null && point.x - previous > expectedGap * 60 * 1000) flush();
    segment.push(`${x(point.x).toFixed(2)},${y(point.ratio).toFixed(2)}`);
    previous = point.x;
  }
  flush();
  const latest = points[points.length - 1];
  svg.append(svgNode("circle", {class: "drawn point", cx: x(latest.x), cy: y(latest.ratio), r: 4}));
}

function renderDownloads(info) {
  const total = n(info.catalog_total), downloaded = n(info.downloaded), ratio = n(info.ratio);
  text("download-count", total === null ? `${count(downloaded)}／—` : `${count(downloaded)}／${count(total)}`);
  text("download-percent", total > 0 ? `${pct(ratio)} 個目錄鍵已取得` : "目錄分母未核實");
  text("download-pending", count(info.not_downloaded));
  text("download-recent", count(info.recent_downloaded_15m));
  text("download-last", info.last_receipt_at_utc ? `上次成功 ${timeLabel(info.last_receipt_at_utc)}` : "尚無成功收據");
  text("download-next", timeLabel(info.next_run_at_utc));
  text("download-state", ({running: "執行中", scheduled: "等待排程", waiting_quota: "等待額度重置", service_failed: "服務失敗", timer_disabled: "排程未啟用"})[info.state] || info.state || "待核實");
  const reasons = info.not_downloaded_by_reason || {};
  const accounted = Object.values(reasons).reduce((sum, value) => sum + (n(value) ?? 0), 0);
  const parts = [
    `尚未嘗試 ${count(reasons.pending)}`,
    `SDK 錯誤 ${count(reasons.provider_error)}`,
    `來源全空 ${count(reasons.provider_empty)}`,
    `查詢逾時 ${count(reasons.resource_timeout)}`,
    `資源暫緩 ${count(reasons.deferred_resource)}`,
    `需分日期 ${count(reasons.deferred_windowed)}`,
    `分日期回補中 ${count(reasons.partial_windowed)}`,
    `權限待核 ${count(reasons.vip_only)}`,
    `登入失敗 ${count(reasons.authentication_failed)}`,
    `額度不足 ${count(reasons.quota_wait)}`,
  ];
  text("download-breakdown", Object.keys(reasons).length
    ? `${parts.join(" · ")}；合計 ${count(accounted)}／未取得 ${count(info.not_downloaded)}。已下載但待追新另計，不代表歷史完整。`
    : `未取得 ${count(info.not_downloaded)}；細項分類待下一次監控快照。`);
}

function renderWorkload(info, tickProgress = {}, minuteProgress = {}) {
  const transfer = info.transfer || {}, valid = info.state === "available";
  const ratio = valid ? n(transfer.ratio) : null;
  text("volume-downloaded", bytes(transfer.completed_bytes));
  text("volume-remaining", n(transfer.remaining_bytes_estimate) === null ? "待量測" : `約 ${bytes(transfer.remaining_bytes_estimate)}`);
  text("volume-total", n(transfer.total_bytes_estimate) === null ? "待量測" : `約 ${bytes(transfer.total_bytes_estimate)}`);
  text("volume-progress-label", ratio === null ? "工作量觀測待更新" : `${pct(ratio)} · 一般表可估範圍（分鐘另列）`);
  const progress = $("volume-progress");
  if (ratio === null) progress.removeAttribute("value");
  else progress.value = Math.min(1, Math.max(0, ratio));
  text("workload-freshness", info.generated_at_utc
    ? `${timeLabel(info.generated_at_utc)} 觀測 · ${valid ? "每分鐘重算" : "快照過期或清冊未核實，暫停估時"} · 追新週期自 ${timeLabel(info.cycle_started_at_utc)}；已有歷史與本輪查新分開。`
    : "尚無工作量快照；流量、筆數與完成時間不以目錄張數代替。");
  text("volume-basis", `${transfer.basis || "來源檔傳輸大小尚未量測。"} 未知大小 ${count(info.unknown_transfer_keys)} 鍵。`);
  const workTime = info.work_time || {}, timeRatio = valid ? n(workTime.ratio) : null;
  text("workload-time-ratio", timeRatio === null ? "耗時樣本待補" : `${pct(timeRatio)} · 工作量加權`);
  const timeProgress = $("workload-time-progress");
  if (timeRatio === null) timeProgress.removeAttribute("value"); else timeProgress.value = Math.max(0, Math.min(1, timeRatio));
  text("workload-time-detail", `已查新 ${durationLabel(workTime.completed_seconds)}／可估總工作量 ${durationLabel(workTime.total_seconds_estimate)}；待辦約 ${durationLabel(workTime.remaining_seconds_estimate)}。不含配額等待，不是從啟動起計時；未知耗時 ${count(workTime.unknown_keys)} 鍵。`);
  const container = $("workload-records"); container.replaceChildren();
  for (const measure of info.records || []) {
    const article = document.createElement("article"), heading = document.createElement("div"), label = document.createElement("span"), value = document.createElement("strong"), bar = document.createElement("progress"), detail = document.createElement("p");
    const completeMeasurement = valid && n(info.unknown_record_keys) === 0;
    heading.className = "finlab-volume-heading";
    label.textContent = measure.label;
    value.textContent = `${completeMeasurement ? "本機已有" : "已量測至少"} ${count(measure.local_count)}`;
    heading.append(label, value); bar.max = 1; bar.className = "progress-track";
    bar.setAttribute("aria-label", `${measure.label}追新工作量`);
    if (completeMeasurement && n(measure.ratio) !== null) bar.value = Math.max(0, Math.min(1, measure.ratio));
    detail.textContent = `本輪已查新 ${count(measure.completed_count)}／可估總量 ${count(measure.total_estimate)}；約需重讀 ${count(measure.remaining_estimate)}。${completeMeasurement ? "重讀筆數不是缺少的歷史筆數。" : `尚有 ${count(info.unknown_record_keys)} 鍵未完成筆數量測。`}`;
    article.append(heading, bar, detail); container.append(article);
  }
  const scenarios = valid ? info.scenarios || {} : {}, reference = scenarios.reference || {};
  const eta = (scenario) => scenario?.state === "complete" ? "本次已查核" : scenario?.finish_at_utc ? timeLabel(scenario.finish_at_utc) : ({object_exceeds_daily_budget: "單表超過可用日額度", capacity_constrained: "新增追新排擠；容量受限", insufficient_samples: "耗時／流量樣本不足", scheduler_unverified: "排程未核實", quota_unverified: "額度觀測待更新", unscheduled_work: "有未排入的待辦", not_required: "目前沒有待辦", blocked: "來源條件未解決"}[scenario?.state] || "暫無可估時間");
  text("download-global-eta", valid && info.actionable_complete && info.blocked_count > 0
    ? `一般表本輪已查核；另有 ${count(info.blocked_count)} 個來源問題` : eta(reference));
  text("workload-time-left", reference.finish_at_utc ? `距現在約 ${durationLabel(reference.remaining_seconds)}；包含插隊的高優先新到期，不是所有未來追新或來源問題的完成日。` : "保留未知，不顯示假倒數。");
  text("workload-processing", durationLabel(reference.processing_seconds));
  text("workload-wait", durationLabel(reference.total_wait_seconds ?? reference.quota_opening_wait_seconds));
  text("workload-reset-count", reference.quota_resets == null ? "配額與開盤保護待核實" : `跨 ${count(reference.quota_resets)} 次重置；加入 ${count(reference.newly_due_checks)} 次新到期檢查。${reference.finish_at_utc ? "" : "未完成情境的耗時只代表模擬至停止點。"}`);
  text("workload-fast", eta(scenarios.fast)); text("workload-slow", eta(scenarios.slow));
  text("workload-eta-basis", `${info.eta_basis || "待取得同鍵的完整下載加轉存耗時，以及新鮮帳號配額。"}${n(info.overhead_seconds_per_key_estimate) === null ? " 尚無鍵間開銷樣本，耗時可能低估。" : ` 已加入相鄰完成收據推估的鍵間開銷：每鍵約 ${decimal.format(info.overhead_seconds_per_key_estimate)} 秒（${count(info.overhead_samples)} 個間隔樣本）。`}`);
  const budget = n(info.daily_budget_bytes), size = n(transfer.total_bytes_estimate);
  text("workload-budget", budget > 0 && size !== null
    ? `全量強制重抓約 ${bytes(size)}／每日保留額度後約 ${bytes(budget)}。${size > budget ? "一天配額不足以完整重抓一次；不能承諾所有表永遠在同一日內查新。" : "只代表容量能容納，不代表來源已發布或沒有其他消耗。"}`
    : "每日可用額度或整體傳輸量尚未量測。");
  const blocked = Array.isArray(info.blocked_keys) ? info.blocked_keys : [], validation = Array.isArray(info.validation_waiting_keys) ? info.validation_waiting_keys : [];
  text("workload-blockers", `無法承諾全資料完成日。${blocked.length ? `來源／權限阻塞：${blocked.join("、")}。` : ""}${validation.length ? `優先權等待：${validation.join("、")}。` : ""}全市場 Tick 另計且最低優先；已有 ${count(info.tick?.local_rows)} 筆（${timeLabel(info.tick?.observed_at_utc)} 快照）。近期回補條件完成日與舊候選首次探查 ETA 見下方逐筆表；來源供應等待另計。`);
  const stageContainer = $("workload-stages"); stageContainer.replaceChildren();
  const stageStates = {pending: "待執行", awaiting_release: "已查核 · 等下次到期", not_required: "本階段無待辦", blocked: "仍有來源問題", unmeasured: "未量測全市場", stale: "快照待更新"};
  const units = {wide_values: "非空數值格", event_rows: "事件／明細列", metadata_values: "標籤格"};
  const stages = (info.stages || []).flatMap(stage => stage.id === "tick" && minuteProgress.phases?.length
    ? [{id: "minute", label: "全市場分鐘優先回補", reason: "直接下載分鐘，已有 Tick 合成分鐘沿用；來源未發布保持缺口。"}, stage] : [stage]);
  for (const [index, stage] of stages.entries()) {
    const card = document.createElement("article"); card.className = "finlab-stage-card";
    const title = document.createElement("h4"), state = document.createElement("p"), bar = document.createElement("progress"), detail = document.createElement("p"), times = document.createElement("dl"), note = document.createElement("small");
    title.textContent = `${index + 1}. ${stage.label}`;
    const tickRecent = (stage.id === "minute" ? minuteProgress.phases : stage.id === "tick" ? tickProgress.phases : [])?.find(row => row.id === "observed_history");
    const familyLabel = stage.id === "minute" ? "分鐘" : "Tick";
    state.className = "finlab-stage-state"; state.textContent = tickRecent ? `${familyLabel}獨立量測與條件估計` : stageStates[stage.state] || "待核實";
    bar.className = "progress-track"; bar.max = 1;
    bar.setAttribute("aria-label", tickRecent ? `近期 ${familyLabel} 候選資料取得覆蓋，非來源完整率` : `${stage.label}來源工作量加權查核進度`);
    const stageRatio = tickRecent ? n(tickRecent.candidate_coverage_ratio) : valid ? n(stage.progress_ratio) : null;
    if (stageRatio !== null) bar.value = Math.max(0, Math.min(1, stageRatio));
    const records = (stage.records || []).map(r => `${count(r.count)} ${units[r.unit] || "待確認單位"}`).join("；");
    detail.textContent = tickRecent
      ? `近期已收 ${count(tickRecent.receipted_partitions)}/${count(tickRecent.candidate_partitions)} 個候選股票日（${pct(stageRatio)}）；預估剩餘 ${count(tickRecent.remaining_rows_estimate)} ${stage.id === "minute" ? "根K棒" : "成交列"}、下載 ${bytes(tickRecent.remaining_transfer_bytes_estimate)}。來源未上架等待另計，詳見下方 ${familyLabel} 進度表。`
      : `待辦 ${count(stage.pending_keys)} 鍵 · 預估下載 ${bytes(stage.remaining_bytes_estimate)} · 工作 ${durationLabel(stage.remaining_work_seconds_estimate)}${records ? `；需查核 ${records}` : ""}。${stageRatio === null ? "工作量無可信百分比。" : `來源大小加權 ${pct(stageRatio)}，不是帳號累計流量。`}`;
    times.className = "finlab-current-eta";
    for (const [name, label] of [["fast", "快速"], ["reference", "參考"], ["slow", "較慢"]]) {
      const dt = document.createElement("dt"), dd = document.createElement("dd");
      dt.textContent = label; dd.textContent = tickRecent ? tickScenarioLabel(tickRecent.scenarios?.[name]) : stage.state === "unmeasured" ? "等待 Tick 批次量測" : !valid ? "快照待更新" : eta(stage.scenarios?.[name]);
      times.append(dt, dd);
    }
    note.textContent = [stage.reason,
      stage.next_retry_at_utc ? `下次重試 ${timeLabel(stage.next_retry_at_utc)}；冷卻 ${count(stage.cooldown_keys)} 鍵。` : "",
      stage.next_check_at_utc ? `下次來源檢查 ${timeLabel(stage.next_check_at_utc)}；不是發布證明。` : "",
      !stage.reason && !stage.next_check_at_utc ? `未知傳輸量 ${count(stage.unknown_transfer_keys)} 鍵、未知耗時 ${count(stage.unknown_time_keys)} 鍵。` : ""].filter(Boolean).join(" ");
    if (tickRecent) note.textContent = "以上是近期候選均可取得的情境，不包含來源補檔等待；更早歷史的首次探查有獨立估時。";
    card.append(title, state, bar, detail, times, note); stageContainer.append(card);
    if (valid && stage.next_wave) {
      const wave = stage.next_wave, caption = document.createElement("p"), estimates = document.createElement("dl");
      caption.textContent = `下一波 ${timeLabel(wave.check_at_utc)} · ${count(wave.keys)} 鍵；以下只估這波查核，不是所有未來資料。`;
      estimates.className = "finlab-next-wave";
      for (const [name, label] of [["fast", "下一波快速"], ["reference", "下一波參考"], ["slow", "下一波較慢"]]) {
        const dt = document.createElement("dt"), dd = document.createElement("dd");
        dt.textContent = label; dd.textContent = eta(wave.scenarios?.[name]); estimates.append(dt, dd);
      }
      card.append(caption, estimates);
    }
  }
}

function renderInventory(data) {
  const info = data.history_inventory || {}, modes = info.mode_counts || {};
  const valid = info.state === "available", acquired = valid ? (n(modes.incremental) ?? 0) + (n(modes.incremental_with_history_review) ?? 0) : null;
  text("history-incremental", valid ? `${count(acquired)}／${count(info.general_total)} 鍵` : "歷史基線待核實");
  text("history-incremental-note", info.basis || "工作量快照尚未核實；已下載不等於逐股或PIT完整。");
  text("history-backfill", valid ? `${count(modes.history_backfill ?? 0)} 可回補 · ${count(modes.source_wait ?? 0)} 待來源` : "未知");
  text("history-backfill-note", valid ? `已有歷史但範圍縮減待核對 ${count(modes.incremental_with_history_review ?? 0)} 鍵；到期才追新，未到期不反覆整表抓取。` : "快照過期時不自動宣稱移入追新。");
  const market = data.intraday_market || {}, tick = market.by_kind?.tw_tick || {};
  text("history-intraday-range", tick.first_data_date ? `${tick.first_data_date}–${tick.last_data_date}` : "未確認");
  text("history-intraday-note", `Tick ${count(tick.rows)} 筆；${bytes(tick.parquet_bytes)}。這是已觀測界限，不是官方最早日期${market.fresh === false ? "；清冊已過期" : ""}。`);
  const groups = info.groups || [], signature = JSON.stringify(groups.map(row => [row.id, row.total_keys]));
  if (signature !== state.groupSignature) {
    const select = $("dataset-group"); select.replaceChildren();
    const option = document.createElement("option"); option.value = "all"; option.textContent = "全部分類"; select.append(option);
    for (const row of groups) {
      const item = document.createElement("option"); item.value = row.id;
      item.textContent = `${row.importance}. ${row.label}（${count(row.total_keys)}）`; select.append(item);
    }
    const intraday = document.createElement("option"); intraday.value = "intraday"; intraday.textContent = "10. 逐筆與合成分鐘"; select.append(intraday);
    if (state.group !== "all" && state.group !== "intraday" && !groups.some(row => row.id === state.group)) state.group = "all";
    select.value = state.group; state.groupSignature = signature;
  }
}

function renderStorage(info) {
  text("storage-source", bytes(info.raw_current_bytes));
  text("storage-files", `${count(info.raw_current_files)} 個目前收據檔`);
  text("storage-derived", bytes(info.research_table_bytes));
  text("storage-free", bytes(info.raw_filesystem_free_bytes));
  text("storage-growth", info.growth_bytes_per_day == null ? "尚無證據" : `${bytes(info.growth_bytes_per_day)}/日`);
  text("storage-basis", info.basis || "容量來源待核實");
  const bars = $("storage-bars"); bars.replaceChildren();
  const items = [["FinLab 目前來源檔", n(info.raw_current_bytes)], ["研究訓練表", n(info.research_table_bytes)]];
  const total = items.reduce((sum, item) => sum + Math.max(0, item[1] || 0), 0);
  for (const [label, value] of items) {
    const wrapper = document.createElement("div"), copy = document.createElement("div"), name = document.createElement("span"), size = document.createElement("strong"), track = document.createElement("progress");
    copy.className = "storage-bar-copy"; name.textContent = label; size.textContent = bytes(value); copy.append(name, size);
    track.className = "storage-progress"; track.max = 1; track.value = total > 0 ? Math.max(0, value || 0) / total : 0;
    wrapper.append(copy, track); bars.append(wrapper);
  }
}

function renderPipelines(data) {
  const gate = data.release_gate || {}, acquired = data.acquisition || {}, training = data.training || {};
  const pipelines = [
    {category: "reference", title: "FinLab SDK 目錄", api: "data.search()", status: gate.catalog_fresh ? "ready" : "waiting", detail: `${count(gate.catalog_total)} 個目錄鍵；目錄清點時間 ${timeLabel(gate.catalog_observed_at_utc)}`, progress: gate.catalog_fresh ? 1 : null, note: "目錄存在不代表帳號可下載。"},
    {category: "historical", title: "一般歷史追新", api: "SDK checked .recent merge / full fallback", status: data.execution?.phase === "general" ? "active" : gate.ready ? "ready" : "partial", detail: `已查新工作量 ${bytes(data.workload?.transfer?.completed_bytes)}；預估待辦傳輸 ${bytes(data.workload?.expected_pending_payload_bytes)}`, progress: data.workload?.state === "available" ? n(data.workload?.transfer?.ratio) : null, note: "按原始來源大小加權；實際增量與帳號計費另外觀測。"},
    {category: "historical", title: "全市場分鐘優先／Tick 剩餘配額", api: "tw_minute first; tw_tick residual", status: ["minute", "tick"].includes(data.execution?.phase) ? "active" : "partial", detail: `分鐘 ${count(data.intraday_market?.by_kind?.tw_minute?.rows)} 根；Tick ${count(data.intraday_market?.by_kind?.tw_tick?.rows)} 筆`, progress: null, note: "先補分鐘；直接與合成按股票日去重；Tick 最低優先；候選不是已發布總數。"},
    {category: "derived", title: "研究特徵建表", api: "local research overlay", status: training.local_rows ? (gate.ready ? "ready" : "partial") : "waiting", detail: `${count(training.local_rows)} 列，${count(training.local_finlab_channels)} 個 FinLab 欄位`, progress: training.local_rows ? 1 : 0, note: "只映射已驗證語義的來源，非嚴格歷史 PIT。"},
    {category: "derived", title: "私人冷庫版本", api: "stockagent-data publish", status: training.cold_publication === "verified_exact_release" ? (gate.ready ? "ready" : "partial") : "waiting", detail: training.cold_snapshot_id || "尚無可核驗 release", progress: training.cold_publication === "verified_exact_release" ? 1 : 0, note: gate.ready ? "精確版已驗證；遠端仍須 READY。" : "既有版本可保留，新版須全目錄追新才打包。"},
    {category: "derived", title: "遠端訓練準備", api: "exact release + READY", status: training.remote_materialization === "verified_ready" ? "ready" : "waiting", detail: training.remote_materialization === "verified_ready" ? "遠端 READY 已核驗" : "遠端 READY 未核驗", progress: training.remote_materialization === "verified_ready" ? 1 : null, note: "本機冷庫完成不等於遠端可訓練。"},
  ];
  text("pipeline-total", count(pipelines.length));
  text("pipeline-active", count(pipelines.filter((item) => item.status === "active").length));
  text("pipeline-ready", count(pipelines.filter((item) => item.status === "ready").length));
  text("pipeline-attention", count(pipelines.filter((item) => item.status === "partial" || item.status === "waiting").length));
  const grid = $("pipeline-grid"); grid.replaceChildren();
  const selected = pipelines.filter((item) => state.pipelineFilter === "all" || item.category === state.pipelineFilter);
  $("pipeline-empty").hidden = selected.length > 0;
  for (const item of selected) {
    const card = document.createElement("article"); card.className = `pipeline-card state-${item.status}`;
    card.innerHTML = '<div class="pipeline-card-header"><div class="pipeline-tags"><span class="category-tag"></span><span class="pipeline-status"></span></div><span class="api-surface"></span></div><h3></h3><p class="pipeline-detail"></p><div class="mini-progress"><div class="mini-progress-copy"><span>取得／核驗進度</span><strong></strong></div><progress class="mini-progress-track" max="1"></progress><small></small></div><div class="pipeline-eta"><span class="eta-label">預計完成</span><strong class="eta-value"></strong><small class="eta-basis"></small></div><div class="pipeline-footer"></div>';
    card.querySelector(".category-tag").textContent = {reference: "目錄與帳號", historical: "歷史下載", derived: "衍生資料"}[item.category];
    const status = card.querySelector(".pipeline-status"); status.className += ` ${item.status}`; status.textContent = {active: "執行中", ready: "就緒", partial: "未追新", waiting: "待核驗"}[item.status];
    card.querySelector(".api-surface").textContent = item.api;
    card.querySelector("h3").textContent = item.title;
    card.querySelector(".pipeline-detail").textContent = item.detail;
    const progress = card.querySelector("progress"); if (item.progress == null) progress.removeAttribute("value"); else progress.value = Math.max(0, Math.min(1, item.progress));
    card.querySelector(".mini-progress-copy strong").textContent = item.progress == null ? "不可量化" : pct(item.progress);
    card.querySelector(".mini-progress small").textContent = item.note;
    card.querySelector(".eta-value").textContent = item.status === "ready" ? "已完成目前核驗" : "無可信倒數";
    card.querySelector(".eta-basis").textContent = item.status === "ready" ? "後續新版本仍需增量檢查" : "共用配額、未知鍵大小或缺遠端證據";
    card.querySelector(".pipeline-footer").textContent = item.status === "active" ? `目前鍵：${data.current_fetch?.key || "等待收據"}` : item.note;
    grid.append(card);
  }
}

function renderCapture(data) {
  const acquired = data.acquisition || {}, gate = data.release_gate || {};
  const phase = data.execution?.phase, running = ["general", "minute", "tick"].includes(phase);
  text("capture-state", phase === "minute" ? "優先下載分鐘線" : phase === "tick" ? "剩餘配額抓 Tick" : phase === "general" ? "一般資料查核／抓取" : phase === "transition" ? "管線切換／後處理（非下載證明）" : acquired.state === "waiting_quota" ? "等待額度重置" : acquired.timer_active ? "等待排程" : "排程未啟用／未知");
  text("capture-freshness", running ? `開始 ${timeLabel(data.current_fetch?.started_at_utc)}` : phase === "transition" ? "服務仍活動，但沒有下載 worker 收據；不當成正在下載" : "無即時串流；依到期與重試排程查來源");
  text("capture-key", data.current_fetch?.key || "目前無單鍵請求");
  text("capture-last", timeLabel(acquired.last_receipt_at_utc));
  text("capture-next", timeLabel(acquired.next_run_at_utc));
  text("capture-gate", gate.ready ? "可進入發版雜湊核驗" : `${count(gate.verified)}／${count(gate.catalog_total)} 近 24 小時已查新`);
  text("capture-quota", data.quota?.observed_at_utc ? `${mb(data.quota.remaining_mb)} 剩餘 · ${timeLabel(data.quota.observed_at_utc)}` : "無新觀測");
  $("capture-dot").classList.toggle("active", running);
  const alert = $("pipeline-alert"); alert.hidden = Boolean(gate.ready);
  if (!gate.ready) { text("alert-title", "新私有包等待全目錄追新"); text("alert-copy", `未取得 ${count(gate.missing)}、查詢過期 ${count(gate.stale)}、收據異常 ${count(gate.invalid)}；既有冷庫版本不會被覆寫或冒充最新。`); }
}

function renderReleaseGate(gate) {
  if (!gate || !gate.catalog_total) {
    text("release-gate", "新包暫緩：目錄分母或來源查詢時間未核實；既有冷庫版本不代表目前最新。");
    return;
  }
  const state = gate.ready ? "全目錄已追至最近 24 小時，可進入雜湊驗證與私有打包" : "新包暫緩：尚未全目錄追至最新";
  text("release-gate", `${state}。近 24 小時查詢 ${count(gate.verified)}／${count(gate.catalog_total)}；未取得 ${count(gate.missing)}、過期 ${count(gate.stale)}、收據／檔案異常 ${count(gate.invalid)}。目錄本身${gate.catalog_fresh ? "已更新" : "未在 24 小時內更新"}。`);
}

function renderTraining(info, gate) {
  text("training-rows", count(info.local_rows));
  text("training-columns", count(info.local_finlab_channels));
  text("training-sources", `${count(info.local_source_keys)} 個已映射來源鍵；其餘下載鍵不自動混入模型`);
  text("training-cold", info.cold_publication === "verified_exact_release" ? (gate?.ready ? "私有冷庫已驗證" : "既有冷庫版；新包暫緩") : info.state === "staged_for_private_cold" ? "本機分發區已建" : "尚未建立分發區");
  text("training-remote", info.remote_materialization === "verified_ready" ? "已核驗 READY" : "尚未核驗");
  const baseState = info.base_cold_feature_match ? "正式底表冷庫雜湊相符" : "正式底表冷庫尚未核對相符";
  text("training-basis", `${baseState}；歷史 PIT：未驗證；正式／即時模型不可直接使用。${info.basis || ""}`);
}

function renderRows() {
  const search = state.search.toLocaleLowerCase("zh-TW");
  const matches = state.datasets.filter((row) => {
    const status = row.state || "pending";
    const selected = state.filter === "all" || status === state.filter
      || (state.filter === "incremental" && row.collection_mode?.startsWith("incremental"))
      || (state.filter === "backfill" && ["history_backfill", "windowed_backfill", "incremental_with_history_review"].includes(row.collection_mode))
      || (state.filter === "source_wait" && row.collection_mode === "source_wait")
      || (state.filter === "not_downloaded" && status !== "downloaded")
      || (state.filter === "attention" && !["downloaded", "pending"].includes(status));
    return selected && (state.group === "all" || row.feature_group === state.group)
      && (`${row.key} ${row.category || ""} ${row.feature_group_label || ""}`).toLocaleLowerCase("zh-TW").includes(search);
  });
  const queueRank = row => row.active ? -1 : ({history_backfill: 0, source_wait: 1, windowed_backfill: 2, incremental_with_history_review: 2, incremental: 3})[row.collection_mode] ?? 4;
  matches.sort((a, b) => {
    let difference = 0;
    if (state.sort === "queue") difference = queueRank(a) - queueRank(b) || (n(a.importance) ?? 99) - (n(b.importance) ?? 99);
    else if (state.sort === "importance") difference = (n(a.importance) ?? 99) - (n(b.importance) ?? 99);
    else if (state.sort === "size") difference = (n(b.local_bytes) ?? -1) - (n(a.local_bytes) ?? -1);
    else if (state.sort === "oldest_check") difference = (Date.parse(a.source_checked_at_utc) || 0) - (Date.parse(b.source_checked_at_utc) || 0);
    return difference || String(a.key).localeCompare(String(b.key), "zh-TW");
  });
  text("dataset-result-count", `${count(matches.length)}／${count(state.datasets.length)} 鍵`);
  const body = $("dataset-rows");
  body.replaceChildren();
  const fragment = document.createDocumentFragment();
  for (const row of matches.slice(0, state.visible)) {
    const tr = document.createElement("tr");
    const work = row.workload || {};
    const rowState = row.active ? "正在抓取" : row.collection_mode?.startsWith("incremental") ? work.needs_refresh ? "追新 · 已到期" : "追新 · 等來源到期" : row.collection_mode === "history_backfill" ? "歷史回補" : row.collection_mode === "source_wait" ? "來源待修復" : stateLabels[row.state] || row.state || "待核實";
    const low = n(row.estimated_fetch_seconds_low), high = n(row.estimated_fetch_seconds_high);
    const blocked = ["vip_only", "provider_empty", "deferred_resource", "deferred_windowed"].includes(row.state);
    const estimate = blocked ? "無可完成估時" : low === null ? "樣本不足" : low === high ? `約 ${durationLabel(low)}` : `${durationLabel(low)}–${durationLabel(high)}`;
    const finish = work.estimated_finish_at_utc ? `情境 ${timeLabel(work.estimated_finish_at_utc)}` : work.needs_refresh ? "配額／條件待確認" : row.state === "downloaded" && work.next_source_check_at_utc ? `下次查核 ${timeLabel(work.next_source_check_at_utc)}` : row.state === "downloaded" ? row.latest_check_within_24h ? "已查核來源" : "發布未核實；無下載 ETA" : row.estimated_finish_at_utc ? `約 ${timeLabel(row.estimated_finish_at_utc)}` : row.active ? low === null ? `已跑 ${durationLabel(row.running_elapsed_seconds)}；估時未知` : "已超過實測範圍" : blocked ? "待來源／授權條件；無 ETA" : "排隊／配額時間未知";
    const deferredReasons = {
      oversized_metadata: "來源標籤寬表超過記憶體預算；待有界擷取",
      oversized_table: "券商整表超過記憶體／額度預算；待分區介面",
      oversized_wide_refresh: "強制追新超過記憶體預算；舊版仍保留",
      requires_date_window: "必須指定起訖日期；整表下載器不適用",
    };
    const failureReasons = {
      provider_error: "SDK 查詢失敗；僅保留錯誤類別，根因未證實",
      provider_empty: "SDK 已回傳資料框，但沒有任何非空值",
      resource_timeout: "單鍵查詢逾時；需檢查分區能力",
      vip_only: "此鍵 SDK 回覆要求 VIP；需核對實際帳號與此鍵授權",
      authentication_failed: "本機登入驗證失敗；檢查 session",
      quota_wait: "帳號額度不足；重置後再試",
      pending: "尚未嘗試；依配額排隊",
    };
    let reason = row.state === "downloaded" ? row.latest_check_within_24h ? "來源已於 24 小時內查詢；歷史 PIT 未驗證" : "已下載，來源查詢超過 24 小時；不代表來源已發布新資料，等待版本核實"
      : deferredReasons[row.deferred_reason] || failureReasons[row.state] || "待核實";
    if (row.state === "downloaded" && work.needs_refresh) reason = "歷史已保存；依下載器本輪日額度週期待重新查源，不代表缺少這些歷史筆數";
    if (row.state === "partial_windowed") reason = `已保存 ${count(row.partition_receipts)}/${count(row.partition_total)} 個候選股票日分區；確認未上架 ${count(row.partition_not_ready)}、其他失敗 ${count(row.partition_other_failed)}；候選覆蓋不是來源可得完整率${row.source_kind === "derived_from_tw_tick" ? "；分鐘在本機合成、不另下載" : ""}`;
    if (row.collection_mode === "windowed_backfill" && row.partition_snapshot_fresh === false) reason += `；全市場快照已過期，保留 ${timeLabel(row.partition_observed_at_utc)} 的計數，不代表已追至最新`;
    if (row.collection_mode === "incremental_with_history_review") reason += "；來源歷史範圍縮減，保留舊版本待核對，不能稱為歷史完整";
    if (row.state === "provider_empty" && n(row.provider_rows) !== null) reason += `；來源 ${count(row.provider_rows)} 列／${count(row.provider_fields)} 欄`;
    if (row.state === "downloaded" && !row.latest_check_within_24h && row.attempt_status === "timed_out") reason += "；最近重查逾時";
    if (row.next_retry_at_utc && row.state !== "downloaded") reason += `；最早重試 ${timeLabel(row.next_retry_at_utc)}`;
    const units = {wide_values: "非空資料格", event_rows: "事件／明細列", metadata_values: "來源標籤資料格"};
    const cells = [row.key, rowState, row.feature_group_label ? `${row.importance}. ${row.feature_group_label}` : "未分類", reason, row.first || "—", row.last || "—", count(work.record_count ?? row.rows), row.collection_mode === "windowed_backfill" ? row.key.startsWith("tw_tick:") ? "逐筆成交列" : "分鐘K棒" : work.record_count == null ? "原收據列（非資料格）" : units[work.record_unit] || "待核實", bytes(row.local_bytes), work.needs_refresh ? bytes(work.expected_payload_bytes) : work.downloaded ? "本輪已查新" : "未知", n(work.fetch_seconds) === null ? estimate : `約 ${durationLabel(work.fetch_seconds)}`, finish, timeLabel(row.last_attempt_at_utc), timeLabel(row.source_checked_at_utc)];
    cells.forEach((value, index) => {
      const td = document.createElement("td");
      if (index === 1) {
        const badge = document.createElement("span");
        badge.className = `finlab-badge ${row.state === "downloaded" && row.latest_check_within_24h ? "downloaded" : row.state !== "downloaded" && row.state !== "pending" ? "attention" : ""}`;
        badge.textContent = value;
        td.append(badge);
      } else td.textContent = value;
      tr.append(td);
    });
    fragment.append(tr);
  }
  if (!matches.length) {
    const tr = document.createElement("tr"), td = document.createElement("td");
    td.colSpan = 14;
    td.textContent = "目前沒有符合條件的資料鍵。";
    tr.append(td); fragment.append(tr);
  }
  body.append(fragment);
  $("dataset-more").hidden = matches.length <= state.visible;
}

function renderMarket(data) {
  const market = data?.intraday_market || {};
  const rows = Array.isArray(market.symbols) ? market.symbols : [];
  text("market-symbols", count(market.universe_symbols));
  text("market-former", market.universe_symbols == null ? "尚無全市場清冊" : `普通股 ${count(market.ordinary_stock_symbols)}、另列 TDR ${count(market.tdr_symbols)}；現有 ${count(market.current_symbols)}、已下市 ${count(market.former_symbols)}；${count(market.missing_daily_bounds)} 檔缺本機日線界限，${count(market.pre_horizon_former_symbols)} 檔全在目前時間範圍之前`);
  text("market-basis", market.target_start_date
    ? `候選 ${market.target_start_date}–${market.target_end_date}，不是官方可得起點。排程：一般資料 → 分鐘回補與到期查新 → 剩餘配額 Tick。已有 Tick 可合成的分鐘沿用，不重抓；直接分鐘與合成分鐘依股票日去重。更早尚未確認的歷史每天最多 16 個候選探查，不堵住已可取的分鐘。分母是歷史候選股票×有觀測的市場交易日×粒度，不代表 FinLab 已上架。來源未上架不偽造已取得。`
    : "尚無全市場逐檔清冊；目前兩個 2330 範例鍵不代表全市場。 ");
  const daily = market.daily_price_coverage || {};
  text("market-daily", daily.state === "parquet_footer_nonnull_counts" ? `${count(daily.symbols_with_values)}/${count(market.universe_symbols)}` : "未驗證");
  text("market-daily-gap", daily.state === "parquet_footer_nonnull_counts" ? `FinLab 缺 ${count(daily.symbols_missing)}；其中 ${count(daily.missing_but_local_daily)} 檔本機另有日線，${count(daily.missing_both_sources)} 檔兩邊都缺` : "只依已下載價表的非空欄位判定");
  for (const [kind, name] of [["tw_minute", "minute"], ["tw_tick", "tick"]]) {
    const item = market.by_kind?.[kind] || {};
    const total = n(item.target_partitions), got = n(item.receipted_partitions);
    text(`market-${name}`, total === null ? "—" : `${count(got)}/${count(total)}`);
    text(`market-${name}-count`, `${count(item.rows)} ${kind === "tw_tick" ? "成交列" : "根K棒"} · ${bytes(item.parquet_bytes)}${kind === "tw_minute" ? `；直接 ${count(item.direct_api_partitions)}／合成 ${count(item.derived_partitions)} 股票日；直接最早 ${item.direct_api_first_data_date || "尚無證據"}` : ""}；已觀測 ${item.first_data_date || "—"}–${item.last_data_date || "—"}（非官方起點）。`);
    const progress = $(`market-${name}-progress`);
    if (market.fresh !== false && total > 0 && got !== null) progress.value = Math.max(0, Math.min(1, got / total));
    else progress.removeAttribute("value");
  }
  const worker = data?.execution?.tick || {};
  for (const [bodyId, info] of [["minute-phases", data.minute_progress], ["intraday-phases", data.tick_progress]]) {
  const phases = $(bodyId); phases.replaceChildren();
  for (const [index, item] of (info?.phases || []).entries()) {
    const tr = document.createElement("tr"); tr.dataset.phaseId = item.id;
    const cell = (main, detail = "") => { const td = document.createElement("td"), strong = document.createElement("strong"); strong.textContent = main; td.append(strong); if (detail) { const p = document.createElement("p"); p.textContent = detail; td.append(p); } tr.append(td); return td; };
    const discovery = item.estimate_scope === "first_pass_candidate_queries";
    cell(`${index + 1}. ${item.label}`, `${item.start_date || "無"}–${item.end_date || "無"}`);
    const ratio = discovery ? item.candidate_check_ratio : item.candidate_coverage_ratio;
    const progressCell = cell(`${count(item.receipted_partitions)}/${count(item.candidate_partitions)}`, discovery ? `已探查 ${pct(ratio)}；非取得率` : `已取得 ${pct(ratio)}；候選不是已發布清冊`);
    const bar = document.createElement("progress"); bar.className = "progress-track"; bar.max = 1;
    bar.setAttribute("aria-label", `${item.label}${discovery ? "首次候選探查進度，非下載完成率" : "候選資料取得進度，非全來源完整率"}`);
    if (market.fresh !== false && n(ratio) !== null) bar.value = Math.max(0, Math.min(1, ratio));
    progressCell.append(bar);
    cell(`未上架 ${count(item.provider_not_ready_partitions)}`, `待查 ${count(item.untested_candidate_partitions)}；失敗 ${count(item.other_failed_partitions)}`);
    cell(`已存 ${count(item.rows)} 筆`, discovery ? "來源範圍未定，無資料總筆數估計" : `剩餘約 ${count(item.remaining_rows_estimate)}；總計約 ${count(item.estimated_total_rows)} 筆`);
    cell(`已存 ${bytes(item.local_parquet_bytes)}`, discovery ? "查無資料不算零容量下載" : `還需約 ${bytes(item.remaining_local_bytes_estimate)}（Parquet）`);
    cell(discovery ? "資料量未定" : `約 ${bytes(item.remaining_transfer_bytes_estimate)}`, discovery ? "僅估首次查詢；若找到資料另加下載成本" : `情境 ${bytes(n(item.remaining_transfer_bytes_estimate) === null ? null : item.remaining_transfer_bytes_estimate * .75)}–${bytes(n(item.remaining_transfer_bytes_estimate) === null ? null : item.remaining_transfer_bytes_estimate * 1.5)}；帳號扣量推估，非本機容量`);
    const etaCell = document.createElement("td"), times = document.createElement("dl"); times.className = "tick-scenarios";
    for (const [name, label] of [["fast", "快速"], ["reference", "參考"], ["slow", "較慢"]]) {
      const dt = document.createElement("dt"), dd = document.createElement("dd"); dt.textContent = label; dd.textContent = tickScenarioLabel(item.scenarios?.[name]); times.append(dt, dd);
    }
    const detail = document.createElement("p"); detail.textContent = `${discovery ? "首次查完候選" : "若資料均已發布"}；參考剩 ${durationLabel(item.scenarios?.reference?.remaining_seconds)}。${item.provider_not_ready_partitions ? "來源等待另計。" : ""}`;
    etaCell.append(times, detail); tr.append(etaCell); phases.append(tr);
  }
  if (!phases.children.length) {
    const tr = document.createElement("tr"), td = document.createElement("td"); td.colSpan = 7; td.textContent = "等待逐筆分類與批次測量；下載器不受面板影響。"; tr.append(td); phases.append(tr);
  }
  }
  text("market-state", worker.state === "running" && worker.owner_alive
    ? `正在抓 ${worker.active_key || "下個分區"} ${worker.active_trade_date || ""}；本批成功 ${count(worker.successful)}／已試 ${count(worker.attempted)}，上限 ${count(worker.attempt_limit)}；${timeLabel(worker.observed_at_utc)} 更新。${worker.estimated_batch_finish_at_utc && worker.eta_measurement_contract_version === 2 ? `本批參考完成 ${timeLabel(worker.estimated_batch_finish_at_utc)}（含配額查詢與本機處理；非全歷史）。` : worker.batch_eta_state === "time_budget_limited" ? "本批受執行時間上限約束，剩餘下批續抓。" : worker.estimated_batch_finish_at_utc ? "舊版耗時尚未含配額查詢與本機處理，等待新版批次，不顯示偏短倒數。" : "本批耗時樣本尚不足或已超時；不顯示假倒數。"}`
    : worker.state === "interrupted" ? "上次 worker 已中斷；保留收據與游標，下次排程續抓。"
    : market.state ? `本輪 ${market.state}；成功 ${count(market.successes_this_run)}/${count(market.attempted_this_run)}；${timeLabel(market.observed_at_utc)} 更新` : "等待逐檔排程收據");
  const search = state.marketSearch.toLocaleLowerCase("zh-TW");
  const matches = rows.filter((row) => `${row.key || ""} ${row.market || ""}`.toLocaleLowerCase("zh-TW").includes(search));
  matches.sort((a, b) => {
    const aStatus = n(a.receipted_partitions) >= n(a.target_partitions) && n(a.target_partitions) > 0 ? 1 : 0;
    const bStatus = n(b.receipted_partitions) >= n(b.target_partitions) && n(b.target_partitions) > 0 ? 1 : 0;
    return aStatus - bStatus || String(a.key).localeCompare(String(b.key));
  });
  text("market-result-count", `${count(matches.length)}／${count(rows.length)} 動態鍵`);
  const body = $("market-rows");
  body.replaceChildren();
  const fragment = document.createDocumentFragment();
  for (const row of matches.slice(0, state.marketVisible)) {
    const tr = document.createElement("tr");
    const fields = [row.key, row.current ? `${row.market || "現有"} · ${row.security_type || "stock"}` : `${row.market || ""} · ${row.security_type || "stock"} · 下市 ${row.delisting_date || "日未核"}`,
      row.finlab_daily_close_rows === null || row.finlab_daily_close_rows === undefined ? "未驗證" : row.finlab_daily_close_rows > 0 ? `${count(row.finlab_daily_close_rows)} 日` : "FinLab 缺",
      count(row.target_partitions), count(row.receipted_partitions), `${count(row.provider_not_ready_partitions)}／${count(row.other_failed_partitions)}`,
      row.first_data_date || "—", row.last_data_date || "—", count(row.rows), bytes(row.parquet_bytes)];
    fields.forEach((value) => { const td = document.createElement("td"); td.textContent = value; tr.append(td); });
    fragment.append(tr);
  }
  if (!matches.length) { const tr = document.createElement("tr"), td = document.createElement("td"); td.colSpan = 10; td.textContent = rows.length ? "查無此代號" : "尚無全市場清冊"; tr.append(td); fragment.append(tr); }
  body.append(fragment);
  $("market-more").hidden = matches.length <= state.marketVisible;
}

function render(data) {
  if (data.datasets != null && (!Array.isArray(data.datasets)
      || data.datasets.some(row => !row || typeof row !== "object" || Array.isArray(row)))) {
    throw new Error("Invalid FinLab dataset metadata");
  }
  const health = data.health || "unavailable";
  const labels = {active: "資料觀測正常", waiting: "執行中但暫無新收據", stale: "面板快照逾時", degraded: "觀測需注意", unavailable: "暫時無資料"};
  renderQuota(data);
  renderDownloads(data.acquisition || {});
  renderTickSummary(data);
  renderWorkload(data.workload || {}, data.tick_progress || {}, data.minute_progress || {});
  renderInventory(data);
  renderReleaseGate(data.release_gate || {});
  renderTraining(data.training || {}, data.release_gate || {});
  renderStorage(data.storage || {});
  renderPipelines(data);
  renderCapture(data);
  state.datasets = Array.isArray(data.datasets) ? data.datasets : [];
  renderRows();
  renderMarket(data);
  state.latest = data;
  const badge = $("finlab-health");
  badge.className = `status ${health}`;
  badge.lastChild.textContent = labels[health] || "狀態待核實";
  text("finlab-freshness", `目錄快照 ${ageLabel(data.monitor_age_seconds)} · 帳號流量 ${ageLabel(data.quota_age_seconds)}`);
}

function scheduleRetry() {
  if (state.retryTimer !== null) return;
  const delay = state.retryDelayMs;
  state.retryTimer = window.setTimeout(() => {
    state.retryTimer = null;
    refresh();
  }, delay);
  state.retryDelayMs = Math.min(delay * 2, 30000);
}

async function refresh() {
  if (document.hidden || state.inFlight) return;
  state.inFlight = true;
  let data;
  try {
    data = await fetchJson("api/status");
  } catch (error) {
    const badge = $("finlab-health");
    badge.className = `status ${state.latest ? "stale" : "unavailable"}`;
    badge.lastChild.textContent = state.latest ? "更新暫停，顯示上次資料" : "面板暫時無法讀取";
    text("finlab-freshness", state.latest
      ? `上次成功快照 ${timeLabel(state.latest.generated_at_utc)}；${durationLabel(state.retryDelayMs / 1000)}後重試。`
      : `${durationLabel(state.retryDelayMs / 1000)}後重試；不會呼叫 FinLab API 來填補顯示。`);
    console.warn("FinLab status request failed", error);
    scheduleRetry();
    state.inFlight = false;
    return;
  }
  try {
    render(data);
    if (state.retryTimer !== null) window.clearTimeout(state.retryTimer);
    state.retryTimer = null;
    state.retryDelayMs = 3000;
  } catch (error) {
    const badge = $("finlab-health");
    badge.className = "status unavailable";
    badge.lastChild.textContent = "資料顯示失敗";
    text("finlab-freshness", "狀態資料已取得，但頁面顯示發生錯誤；請重新整理頁面。下載器不受影響。");
    console.error("FinLab status render failed", error);
  } finally {
    state.inFlight = false;
  }
}

$("dataset-search").addEventListener("input", (event) => { state.search = event.target.value.trim(); state.visible = 100; renderRows(); });
$("dataset-filter").addEventListener("change", (event) => { state.filter = event.target.value; state.visible = 100; renderRows(); });
$("dataset-group").addEventListener("change", (event) => { state.group = event.target.value; state.visible = 100; renderRows(); });
$("dataset-sort").addEventListener("change", (event) => { state.sort = event.target.value; state.visible = 100; renderRows(); });
$("dataset-more").addEventListener("click", () => { state.visible += 100; renderRows(); });
$("market-search").addEventListener("input", (event) => { state.marketSearch = event.target.value.trim(); state.marketVisible = 100; if (state.latest) renderMarket(state.latest); });
$("market-more").addEventListener("click", () => { state.marketVisible += 100; if (state.latest) renderMarket(state.latest); });
document.querySelectorAll("[data-pipeline-filter]").forEach((button) => button.addEventListener("click", () => { state.pipelineFilter = button.dataset.pipelineFilter; document.querySelectorAll("[data-pipeline-filter]").forEach((other) => { const active = other === button; other.classList.toggle("active", active); other.setAttribute("aria-pressed", String(active)); }); if (state.latest) renderPipelines(state.latest); }));
document.querySelectorAll("#quota-time-range button").forEach((button) => button.addEventListener("click", () => { state.quotaRange = button.dataset.range; drawQuotaHistory(state.quotaHistory); }));
drawQuotaHistory = Dashboard.createDeferredRenderer("quota-chart", drawQuotaHistory);
renderQuotaRows = Dashboard.createDeferredRenderer("quota-history-body", renderQuotaRows);
renderRows = Dashboard.createDeferredRenderer("dataset-rows", renderRows);
// Observe the entire section: its summary precedes the long table, and its
// filters must remain responsive when the header has scrolled out of view.
renderMarket = Dashboard.createDeferredRenderer("market", renderMarket);
renderPipelines = Dashboard.createDeferredRenderer("pipelines", renderPipelines);
Dashboard.scheduleRefresh(refresh, {intervalMs: 60000});

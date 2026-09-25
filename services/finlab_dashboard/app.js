"use strict";

const Dashboard = window.StockAgentDashboard;
const fetchJson = Dashboard.createJsonFetcher({timeoutMs: 15000, cache: "no-store", expectedRoot: "object"});
const $ = Dashboard.byId;
const SVG_NS = "http://www.w3.org/2000/svg";
const state = {datasets: [], filter: "all", pipelineFilter: "all", search: "", visible: 100, marketSearch: "", marketVisible: 100, inFlight: false, quotaRange: "1d", quotaHistory: [], latest: null, retryTimer: null, retryDelayMs: 3000};
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
function durationLabel(value) { const seconds = n(value); if (seconds === null) return "—"; if (seconds < 60) return `${Math.ceil(seconds)} 秒`; if (seconds < 3600) return `${decimal.format(seconds / 60)} 分`; return `${decimal.format(seconds / 3600)} 小時`; }
function text(id, value) { $(id).textContent = value; }

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
  text("quota-reserve-basis", `依下載器最近收據保留 ${mb(reserve)}；供應商可能另有單次請求成本`);
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
  const body = $("quota-history-body");
  body.replaceChildren();
  for (const row of state.quotaHistory.slice(-30).reverse()) {
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
  text("download-progress-label", total > 0 ? `${count(downloaded)} / ${count(total)} · ${pct(ratio)}` : "—");
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
  const progress = $("download-progress");
  if (ratio === null || total === null || total <= 0) progress.removeAttribute("value");
  else progress.value = Math.min(1, Math.max(0, ratio));
}

function renderVolume(info) {
  const measured = n(info.measured_bytes), total = n(info.estimated_total_bytes);
  const ratio = n(info.estimated_coverage_ratio);
  text("volume-downloaded", bytes(measured));
  text("volume-remaining", bytes(info.estimated_remaining_bytes));
  text("volume-total", total === null ? "樣本不足" : `約 ${bytes(total)}`);
  const fallbackKeys = n(info.global_estimated_keys) || 0;
  const smallSampleKeys = n(info.small_category_estimated_keys) || 0;
  text("volume-progress-label", ratio === null ? "無足夠樣本"
    : `${pct(ratio)} · ${fallbackKeys || smallSampleKeys ? "低信心情境" : "樣本推估"}`);
  const progress = $("volume-progress");
  if (ratio === null) progress.removeAttribute("value");
  else progress.value = Math.min(1, Math.max(0, ratio));
  const sampled = count(info.measured_files), totalKeys = count(info.catalog_keys);
  const fallback = count(info.global_estimated_keys), small = count(info.small_category_estimated_keys);
  const upper = n(info.high_scenario_total_bytes);
  text("volume-basis", info.catalog_verified === false
    ? `目錄 ${count(info.expected_catalog_keys)} 鍵與面板 ${totalKeys} 筆逐鍵清單不一致，或鍵名重複；已抓量仍為本機實測，總容量暫停估算。`
    : total === null
    ? `目前 ${sampled}／${totalKeys} 鍵有本機大小；沒有可外推的已下載檔案，總容量未知。`
    : `已抓量為 ${sampled} 個本機檔案實測；${fallback} 鍵無同類樣本、${small} 鍵同類樣本少於 3 個。較大樣本情境約 ${bytes(upper)}，不是上限；高容量鍵可能使總量大幅上修。此容量比例不是完成率，也不是 FinLab 帳號流量。`);
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
    {category: "historical", title: "全目錄歷史下載", api: "data.get(force_download=True)", status: acquired.service_active ? "active" : gate.ready ? "ready" : "partial", detail: `${count(acquired.downloaded)}／${count(acquired.catalog_total)} 鍵已取得；最近 24 小時查新 ${count(gate.verified)} 鍵`, progress: n(acquired.ratio), note: "資源暫緩與配額等候不列為完成；原始表不進冷庫。"},
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
  const running = acquired.service_active === true;
  text("capture-state", running ? "正在下載" : acquired.timer_active ? "等待排程" : "排程未啟用／未知");
  text("capture-freshness", running ? `開始 ${timeLabel(data.current_fetch?.started_at_utc)}` : "無即時串流；下次 timer 啟動才會查來源");
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
      || (state.filter === "not_downloaded" && status !== "downloaded")
      || (state.filter === "attention" && !["downloaded", "pending"].includes(status));
    return selected && (`${row.key} ${row.category || ""}`).toLocaleLowerCase("zh-TW").includes(search);
  });
  text("dataset-result-count", `${count(matches.length)}／${count(state.datasets.length)} 鍵`);
  const body = $("dataset-rows");
  body.replaceChildren();
  const fragment = document.createDocumentFragment();
  for (const row of matches.slice(0, state.visible)) {
    const tr = document.createElement("tr");
    const rowState = row.active ? "正在抓取" : row.state === "downloaded" && !row.latest_check_within_24h ? "待追新" : stateLabels[row.state] || row.state || "待核實";
    const low = n(row.estimated_fetch_seconds_low), high = n(row.estimated_fetch_seconds_high);
    const blocked = ["vip_only", "provider_empty", "deferred_resource", "deferred_windowed"].includes(row.state);
    const estimate = blocked ? "無可完成估時" : low === null ? "樣本不足" : low === high ? `約 ${durationLabel(low)}` : `${durationLabel(low)}–${durationLabel(high)}`;
    const finish = row.state === "downloaded" && row.latest_check_within_24h ? "已查最新" : row.estimated_finish_at_utc ? `約 ${timeLabel(row.estimated_finish_at_utc)}` : row.active ? low === null ? `已跑 ${durationLabel(row.running_elapsed_seconds)}；估時未知` : "已超過實測範圍" : blocked ? "待來源／授權條件；無 ETA" : "排隊／配額時間未知";
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
    let reason = row.state === "downloaded" ? row.latest_check_within_24h ? "來源已於 24 小時內查詢；歷史 PIT 未驗證" : "已下載，來源查詢超過 24 小時；需追新"
      : deferredReasons[row.deferred_reason] || failureReasons[row.state] || "待核實";
    if (row.state === "partial_windowed") reason = `已保存 ${count(row.partition_receipts)}/${count(row.partition_total)} 個所列期間工作日分區；確認未上架 ${count(row.partition_not_ready)}、其他失敗 ${count(row.partition_other_failed)}；更早歷史未證實`;
    if (row.state === "provider_empty" && n(row.provider_rows) !== null) reason += `；來源 ${count(row.provider_rows)} 列／${count(row.provider_fields)} 欄`;
    if (row.state === "downloaded" && !row.latest_check_within_24h && row.attempt_status === "timed_out") reason += "；最近重查逾時";
    if (row.next_retry_at_utc && row.state !== "downloaded") reason += `；最早重試 ${timeLabel(row.next_retry_at_utc)}`;
    const cells = [row.key, rowState, reason, row.first || "—", row.last || "—", count(row.rows), bytes(row.local_bytes), estimate, finish, timeLabel(row.last_attempt_at_utc), timeLabel(row.source_checked_at_utc)];
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
    td.colSpan = 11;
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
    ? `目標 ${market.target_start_date}–${market.target_end_date}；只下載 Tick，分鐘由本機合成；舊版直接分鐘收據 ${count(market.legacy_direct_minute_partitions)} 筆另存不計。分母是歷史候選股票×有觀測的市場交易日×粒度，不代表 FinLab 已上架。${market.session_basis || ""} 下市與轉板需依個別生命週期核對；來源未上架不偽造已取得。`
    : "尚無全市場逐檔清冊；目前兩個 2330 範例鍵不代表全市場。 ");
  const daily = market.daily_price_coverage || {};
  text("market-daily", daily.state === "parquet_footer_nonnull_counts" ? `${count(daily.symbols_with_values)}/${count(market.universe_symbols)}` : "未驗證");
  text("market-daily-gap", daily.state === "parquet_footer_nonnull_counts" ? `FinLab 缺 ${count(daily.symbols_missing)}；其中 ${count(daily.missing_but_local_daily)} 檔本機另有日線，${count(daily.missing_both_sources)} 檔兩邊都缺` : "只依已下載價表的非空欄位判定");
  for (const [kind, name] of [["tw_minute", "minute"], ["tw_tick", "tick"]]) {
    const item = market.by_kind?.[kind] || {};
    const total = n(item.target_partitions), got = n(item.receipted_partitions);
    text(`market-${name}`, total === null ? "—" : `${count(got)}/${count(total)}`);
    const progress = $(`market-${name}-progress`);
    if (total > 0 && got !== null) progress.value = Math.max(0, Math.min(1, got / total));
    else progress.removeAttribute("value");
  }
  text("market-eta", "無可信 ETA");
  text("market-state", market.state ? `本輪 ${market.state}；成功 ${count(market.successes_this_run)}/${count(market.attempted_this_run)}；${timeLabel(market.observed_at_utc)} 更新` : "等待逐檔排程收據");
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
  const health = data.health || "unavailable";
  const labels = {active: "資料觀測正常", waiting: "執行中但暫無新收據", stale: "面板快照逾時", degraded: "觀測需注意", unavailable: "暫時無資料"};
  renderQuota(data);
  renderDownloads(data.acquisition || {});
  renderVolume(data.volume_estimate || {});
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
$("dataset-more").addEventListener("click", () => { state.visible += 100; renderRows(); });
$("market-search").addEventListener("input", (event) => { state.marketSearch = event.target.value.trim(); state.marketVisible = 100; if (state.latest) renderMarket(state.latest); });
$("market-more").addEventListener("click", () => { state.marketVisible += 100; if (state.latest) renderMarket(state.latest); });
document.querySelectorAll("[data-pipeline-filter]").forEach((button) => button.addEventListener("click", () => { state.pipelineFilter = button.dataset.pipelineFilter; document.querySelectorAll("[data-pipeline-filter]").forEach((other) => { const active = other === button; other.classList.toggle("active", active); other.setAttribute("aria-pressed", String(active)); }); if (state.latest) renderPipelines(state.latest); }));
document.querySelectorAll("#quota-time-range button").forEach((button) => button.addEventListener("click", () => { state.quotaRange = button.dataset.range; drawQuotaHistory(state.quotaHistory); }));
Dashboard.scheduleRefresh(refresh, {intervalMs: 60000});

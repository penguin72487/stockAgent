"use strict";

const Dashboard = window.StockAgentDashboard;
const fetchJson = Dashboard.createJsonFetcher({timeoutMs: 15000, cache: "no-store", expectedRoot: "object"});
const $ = Dashboard.byId;
const SVG_NS = "http://www.w3.org/2000/svg";
const state = {latest: null, inFlight: false, filter: "all", quotaRange: "1d", retryTimer: null, retryDelayMs: 3000};
const integer = new Intl.NumberFormat("zh-TW", {maximumFractionDigits: 0});
const oneDecimal = new Intl.NumberFormat("zh-TW", {maximumFractionDigits: 1});
const statusLabels = {complete: "本輪已查驗", backfilling: "回補中", pending: "待取得", delegated: "由 Sponsor 主責", unavailable: "來源不可取", stale: "待追新"};

function valueNumber(value) { if (value === null || value === undefined || value === "") return null; const parsed = Number(value); return Number.isFinite(parsed) ? parsed : null; }
function count(value) { const parsed = valueNumber(value); return parsed === null ? "—" : integer.format(parsed); }
function bytes(value) { return valueNumber(value) === null ? "—" : Dashboard.formatBytes(value, {maximumFractionDigits: 1}); }
function ratio(complete, total) { const left = valueNumber(complete), right = valueNumber(total); return left !== null && right > 0 ? Math.min(1, Math.max(0, left / right)) : null; }
function percent(value) { return value === null ? "—" : `${(value * 100).toFixed(2)}%`; }
function duration(seconds) { const value = valueNumber(seconds); if (value === null) return "未知"; if (value < 3600) return `至少 ${oneDecimal.format(value / 60)} 分鐘`; return `至少 ${oneDecimal.format(value / 3600)} 小時`; }
function timeLabel(value) { const date = new Date(value || ""); return Number.isNaN(date.getTime()) ? "—" : date.toLocaleString("zh-TW", {timeZone: "Asia/Taipei", hour12: false, year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit"}); }
function ageLabel(seconds) { return Dashboard.formatAge(seconds, {emptyLabel: "時間未核實", hourDigits: 0, dayDigits: 0}); }
function text(id, value) { $(id).textContent = value; }

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
  text("quota-ratio", used === null || !limit ? "—" : `${completeWindow ? "" : "≥"}${percent(Math.min(1, used / limit))}`);
  text("quota-basis", completeWindow ? "本站 worker 滾動 60 分鐘；非帳號總用量" : "本站追蹤未滿一小時，為觀測下界");
  text("quota-token", info.account_tier ? `官方帳號層級 ${info.account_tier}；最近帳號取樣 ${count(info.provider_used_in_hour)} 次` : "尚未驗證帳號層級");
  text("quota-observed", info.provider_observed_at_utc ? `帳號取樣 ${timeLabel(info.provider_observed_at_utc)}；本站紀錄始於 ${timeLabel(info.tracking_started_at_utc)}` : "尚無近期官方帳號取樣；本站請求不含其他應用");
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
    card.innerHTML = '<div class="pipeline-card-header"><div class="pipeline-tags"><span class="category-tag"></span><span class="pipeline-status"></span></div><span class="api-surface"></span></div><h3></h3><p class="pipeline-detail"></p><div class="mini-progress"><div class="mini-progress-copy"><span>目前任務查驗率</span><strong></strong></div><progress class="mini-progress-track" max="1"></progress><small></small></div><div class="pipeline-eta"><span class="eta-label">最少剩餘時間</span><strong class="eta-value"></strong><small class="eta-basis"></small></div><div class="pipeline-footer"></div>';
    card.querySelector(".category-tag").textContent = row.kind.startsWith("sponsor_") ? "Sponsor 全市場" : ({session_history: "全市場盤中", snapshot: "主檔快照", reference: "交易日曆", global_history: "全市場／總經", symbol_history: "逐檔／固定指標", global_equity_history: "海外逐檔"})[row.kind] || "來源資料";
    const badge = card.querySelector(".pipeline-status"); badge.className += ` ${visualState}`; badge.textContent = statusLabels[row.state] || "待核實";
    card.querySelector(".api-surface").textContent = row.id;
    card.querySelector("h3").textContent = row.label;
    const checked = row.checked_partitions ?? row.complete_partitions;
    card.querySelector(".pipeline-detail").textContent = row.kind === "sponsor_unscheduled" ? `尚未排程：${row.source_status}` : `已查驗 ${count(checked)}／${count(row.target_partitions)} · 非空 ${count(row.complete_partitions)} · ${count(row.rows)} 筆 · ${bytes(row.local_bytes)} · 空回 ${count(row.observed_empty_partitions || 0)}／失敗 ${count(row.deferred_partitions || 0)}／權限 ${count(row.not_entitled_partitions || 0)}／參數 ${count(row.invalid_request_partitions || 0)}`;
    const completion = ratio(checked, row.target_partitions);
    const progress = card.querySelector("progress"); if (completion === null) progress.removeAttribute("value"); else progress.value = completion;
    card.querySelector(".mini-progress-copy strong").textContent = percent(completion);
    card.querySelector(".mini-progress small").textContent = `資料日期 ${row.first_data_date || "未取得"} → ${row.last_data_date || "未取得"}`;
    card.querySelector(".eta-value").textContent = row.state === "complete" ? "已取得目前任務" : duration(row.minimum_network_seconds_remaining);
    card.querySelector(".eta-basis").textContent = row.kind === "session_history" ? "與所有 FinMind 來源共用額度；非完工承諾" : "只含已建立任務；不代表歷史完整";
    card.querySelector(".pipeline-footer").textContent = `最後完成 ${timeLabel(row.last_receipt_at_utc)}`;
    grid.append(card);
  }
}

function renderStorage(data) {
  text("storage-source", bytes(data.storage?.local_bytes));
  text("storage-free", bytes(data.storage?.filesystem_free_bytes));
  text("storage-total", data.storage?.estimated_total_bytes == null ? "未知" : bytes(data.storage.estimated_total_bytes));
  text("training-state", data.scope?.cold_published === true ? "已發版" : "未發版");
  const bars = $("storage-bars"); bars.replaceChildren();
  const rows = (Array.isArray(data.datasets) ? data.datasets : []).filter((row) => valueNumber(row.local_bytes) !== null);
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
  const coverage = ratio(checked, total);
  text("download-progress-label", total === null ? "分母未核實" : `已查驗 ${count(checked)} / ${count(total)} · 非空 ${count(complete)} · ${percent(coverage)} · ${count(info.unknown_universe_datasets)} 類主檔待載入；歷史下市代號待稽核`);
  text("download-global-eta", duration(info.minimum_network_seconds_remaining));
  text("download-count", `${count(checked)}／${count(total)}`);
  text("download-pending", `${count(info.pending_tasks)} · 空回 ${count(info.observed_empty_tasks)} · 權限 ${count(info.not_entitled_tasks)} · 參數 ${count(info.invalid_request_tasks)}`);
  text("download-deferred", count(info.retry_deferred_tasks));
  text("download-last", info.sponsor_last_task?.partition || info.last_task?.date || info.complement_last_task?.partition || "—");
  text("download-last-detail", info.sponsor_last_task?.dataset ? `${info.sponsor_last_task.dataset} · ${info.sponsor_last_task.status || "未知"}` : info.complement_last_task?.dataset ? `${info.complement_last_task.dataset} · ${info.complement_last_task.status || "未知"}` : info.last_task?.dataset ? `${info.last_task.dataset} · ${info.last_task.status || "未知"}` : "尚無工作收據");
  const progress = $("download-progress"); if (coverage === null) progress.removeAttribute("value"); else progress.value = coverage;
  const body = $("dataset-rows"); body.replaceChildren();
  for (const row of (Array.isArray(data.datasets) ? data.datasets : [])) {
    const tr = document.createElement("tr");
    const grains = row.observed_grains || {};
    const grainLabel = row.kind === "session_history" ? `${count(grains["1m"] || 0)} 日 1 分 · ${count(grains["5s"] || 0)} 日 5 秒` : `空 ${count(row.observed_empty_partitions || 0)} · 失敗 ${count(row.deferred_partitions || 0)} · 權限 ${count(row.not_entitled_partitions || 0)} · 參數 ${count(row.invalid_request_partitions || 0)}`;
    const cells = [row.label, statusLabels[row.state] || "待核實", `${count(row.checked_partitions ?? row.complete_partitions)}／${count(row.target_partitions)}（非空 ${count(row.complete_partitions)}）`, row.first_data_date || "—", row.last_data_date || "—", count(row.rows), bytes(row.local_bytes), grainLabel, row.state === "complete" ? "—" : duration(row.minimum_network_seconds_remaining), timeLabel(row.last_receipt_at_utc)];
    for (const cell of cells) { const td = document.createElement("td"); td.textContent = cell; tr.append(td); }
    body.append(tr);
  }
  if (!body.children.length) { const tr = document.createElement("tr"), td = document.createElement("td"); td.colSpan = 10; td.textContent = "尚未取得管線觀測"; tr.append(td); body.append(tr); }
}

function renderCapture(data) {
  const info = data.acquisition || {};
  const running = data.health === "updating";
  const labels = {running: "正在抓取", backfilling: "持續回補", current: "目前已追新", current_queue: "待下一輪查新", protected_opening: "開盤保護暫停", waiting_retry: "等待短期重試", rate_limited: "來源節流", disk_guard: "磁碟保留量不足", invalid_token: "Token 無效"};
  text("capture-state", `盤中 ${labels[info.state] || info.state || "待核實"} · 補充 ${labels[info.complement_state] || info.complement_state || "待核實"} · Sponsor ${labels[info.sponsor_state] || info.sponsor_state || "待核實"}`);
  text("capture-freshness", `盤中 ${timeLabel(info.observed_at_utc)} · 補充 ${timeLabel(info.complement_observed_at_utc)} · Sponsor ${timeLabel(info.sponsor_observed_at_utc)}；最近觀測 ${ageLabel(data.status_age_seconds)}`);
  text("capture-key", info.sponsor_active_tasks?.[0]?.dataset ? `${info.sponsor_active_tasks[0].dataset} · ${info.sponsor_active_tasks[0].partition}` : info.complement_active_task?.dataset ? `${info.complement_active_task.dataset} · ${info.complement_active_task.data_id || info.complement_active_task.partition || ""}` : info.active_task?.dataset ? `${info.active_task.dataset} · ${info.active_task.date || ""}` : "目前無進行中的請求");
  text("capture-last", info.sponsor_last_task ? `${info.sponsor_last_task.dataset} · ${info.sponsor_last_task.status || "未知"} · ${count(info.sponsor_last_task.rows)} 筆` : info.complement_last_task ? `${info.complement_last_task.dataset} · ${info.complement_last_task.status || "未知"} · ${count(info.complement_last_task.rows)} 筆` : info.last_task ? `${info.last_task.status || "未知"} · ${count(info.last_task.rows)} 筆` : "—");
  const next = Array.isArray(info.queue_preview) ? info.queue_preview[0] : null;
  text("capture-next", info.complement_next_task?.dataset ? `${info.complement_next_task.dataset} · ${info.complement_next_task.data_id || info.complement_next_task.partition || ""}` : next?.dataset ? `${next.dataset} · ${next.date || ""}` : "無待補佇列／尚未清點");
  const universe = info.candidate_universe || {};
  text("capture-universe", `${count(universe.finmind_current_master_stock_ids)} 個現行主檔代號 + ${count(universe.additional_official_delisted_ids)} 個額外官方下市代號；歷史全域仍待驗證`);
  $("capture-dot").classList.toggle("active", running);
  const alert = $("pipeline-alert"); alert.hidden = data.health === "updating" || data.health === "waiting";
  if (!alert.hidden) { text("alert-title", data.health === "stale" ? "下載狀態逾時" : "資料管線需檢查"); text("alert-copy", "既有來源收據仍保留，但目前不能把服務存活或 API 200 當作全歷史完整。新聞維持停用。"); }
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
Dashboard.scheduleRefresh(refresh, {intervalMs: 30000});

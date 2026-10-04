"use strict";

const Dashboard = window.StockAgentDashboard;
const $ = Dashboard.byId;
const fetchJson = Dashboard.createJsonFetcher({timeoutMs: 15000, cache: "no-store", expectedRoot: "object"});
const DATE_FORMAT = new Intl.DateTimeFormat("zh-TW", {
  timeZone: "Asia/Taipei", hour12: false, year: "numeric", month: "2-digit", day: "2-digit",
  hour: "2-digit", minute: "2-digit",
});
const INTEGER_FORMAT = new Intl.NumberFormat("zh-TW", {maximumFractionDigits: 0});
const PAGE_SIZE = 30;
const MARKET_ORDER = {taiwan_equity: 0, taiwan_derivatives: 1, taiwan_public: 2,
  global_equity: 3, forex: 4, macro: 5, cross_market: 6, configuration: 7, crypto: 8};
const OPERATION_LABEL = {catching_up: "正在抓／未最新", streaming: "正在串流",
  complete: "已完成／已最新", unable: "無法完成", deferred: "已延後",
  control: "設定閘門", reference: "清冊參照"};
const DEDICATED = {FinLab: ["/finlab/", "/finlab/api/status"],
  FinMind: ["/finmind/", "/finmind/api/status"],
  TEJ: ["/tej/", "/tej/api/status"],
  "永豐 Shioaji": ["/shioaji/", "/shioaji/api/status"],
  OpenBB: ["/openbb/", null]};
const state = {provider: null, rows: [], matched: 0, nextOffset: 0, snapshotAt: null,
  loaded: false, inFlight: false, pendingRefresh: null, quotaInFlight: false, lastQuotaAt: 0};

function numeric(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}
function integer(value) { const n = numeric(value); return n === null ? "—" : INTEGER_FORMAT.format(Math.round(n)); }
function datetime(value) {
  const date = new Date(value || "");
  return Number.isNaN(date.getTime()) ? null : DATE_FORMAT.format(date);
}
function make(tag, className, value) {
  const element = document.createElement(tag);
  if (className) element.className = className;
  if (value !== undefined && value !== null) element.textContent = String(value);
  return element;
}
function showError(message) {
  $("provider-error").textContent = message;
  $("provider-error").hidden = false;
}
function clearError() { $("provider-error").hidden = true; }
function sourceCount(stats) {
  if (stats?.state === "not_applicable") return "不適用";
  const count = numeric(stats?.count);
  if (count !== null) return `${integer(count)} 筆`;
  if (numeric(stats?.invalid_files) > 0) return `總數未知 · ${integer(stats.invalid_files)} 檔異常`;
  if (stats?.state === "empty") return "尚無實存檔案";
  return "總數待核實";
}
function sourceDate(value, stats) {
  if (value) return String(value).replace("T", " ").replace(/\+00:00$/, " UTC");
  return stats?.state === "empty" ? "無資料" : "待核實";
}
function eta(estimate) {
  const remaining = numeric(estimate?.remaining_seconds);
  if (remaining !== null && remaining > 0) {
    if (remaining < 3600) return `約 ${Math.max(1, Math.round(remaining / 60))} 分鐘`;
    if (remaining < 86400) return `約 ${(remaining / 3600).toFixed(1)} 小時`;
    return `約 ${(remaining / 86400).toFixed(1)} 天`;
  }
  return {complete: "已到最新", continuous: "持續串流", deferred: "未啟用",
    waiting_quota: "等待配額", waiting_schedule: "待排程", running_unmeasured: "仍在量測",
    warming_up: "重新量測中", blocked: "阻擋中", reference: "清冊參照",
    not_applicable: "不適用"}[estimate?.state] || "沒有可信 ETA";
}
function progress(container, value) {
  if (["deferred", "reference", "blocked", "stale_complete_receipt", "not_applicable"].includes(value?.state)) {
    container.append(make("span", "cell-note", value.label || "進度不適用"));
    const evidence = value.evidence_coverage;
    if (numeric(evidence?.current) !== null && numeric(evidence?.total) !== null) {
      container.append(make("span", "cell-note", `舊收據證據 ${integer(evidence.current)}/${integer(evidence.total)} ${evidence.unit || ""}；不作目前完成率`));
    }
    return;
  }
  const ratio = numeric(value?.ratio);
  if (ratio !== null && numeric(value?.total) > 0) {
    const bar = make("progress");
    bar.max = 1;
    bar.value = Math.max(0, Math.min(1, ratio));
    bar.setAttribute("aria-label", value.label || "取得進度");
    container.append(bar, make("span", "cell-note", `${(ratio * 100).toFixed(1)}% · ${integer(value.current)}/${integer(value.total)} ${value.unit || ""}`));
  } else {
    container.append(make("span", "cell-note", value?.label || "分母未核實；不顯示百分比"));
  }
  if (value?.basis) container.append(make("span", "cell-note", value.basis));
}
function renderHeadline(data) {
  const summary = data.summary || {};
  const operations = summary.operation_state_counts || {};
  const activeOperations = summary.active_operation_state_counts || {};
  const inventory = summary.inventory_state_counts || {};
  $("provider-name").textContent = data.provider;
  $("breadcrumb-name").textContent = data.provider;
  document.title = `${data.provider} · StockAgent 全資料監控`;
  $("provider-observed").textContent = `監控快照：${datetime(data.generated_at_utc) || "時間未核實"}（臺北）`;
  $("provider-registered").textContent = integer(summary.registered);
  $("provider-active").textContent = integer(summary.active_endpoints);
  $("provider-catching").textContent = integer(operations.catching_up || 0);
  $("provider-unable").textContent = integer(operations.unable || 0);
  $("provider-verified").textContent = integer(inventory.verified || 0);
  $("provider-aliases").textContent = integer(summary.registry_aliases || 0);
  const active = numeric(summary.active_endpoints) || 0;
  const ratio = numeric(summary.active_current_ratio);
  const status = operations.unable ? "unable" : operations.catching_up ? "catching_up"
    : operations.streaming ? "streaming" : operations.complete ? "complete" : "reference";
  const badge = $("provider-health");
  badge.className = `status-pill ${status}`;
  badge.textContent = OPERATION_LABEL[status];
  if (active && ratio !== null) {
    $("provider-ratio").textContent = `${(ratio * 100).toFixed(1)}%`;
    $("provider-progress").value = Math.max(0, Math.min(1, ratio));
    $("provider-ratio-basis").textContent = `${integer((activeOperations.complete || 0) + (activeOperations.streaming || 0))} 個完成／串流中的主動端點，分母 ${integer(active)}；非歷史資料筆數完整率。`;
  } else {
    $("provider-ratio").textContent = "不適用";
    $("provider-progress").removeAttribute("value");
    $("provider-ratio-basis").textContent = "沒有主動端點分母；這些列可能是清冊參照、複合來源或設定閘門。";
  }
  $("provider-subtitle").textContent = summary.registry_aliases === summary.registered
    ? "此來源目前主要是清冊別名；實際取得進度以目錄收據與下方個別項目為準。"
    : "逐端點查看取得進度、發布與下次排程、實存首末資料及證據界線。";
  const markets = Object.entries(summary.market_categories || {}).sort((a, b) => b[1] - a[1]);
  $("provider-markets").replaceChildren(...markets.map(([name, count]) =>
    make("span", "market-category", `${name} · ${integer(count)}`)));
  const catalog = data.finlab_acquisition;
  $("provider-catalog-card").hidden = !catalog;
  if (catalog) {
    const total = numeric(catalog.catalog_total);
    const got = numeric(catalog.downloaded);
    const fraction = total > 0 && got !== null ? got / total : null;
    $("provider-catalog-progress").textContent = total === null
      ? `${integer(got)} 鍵已取得；分母待核實`
      : `${integer(got)}/${integer(total)} 個目錄鍵有收據`;
    if (fraction === null) $("provider-catalog-bar").removeAttribute("value");
    else $("provider-catalog-bar").value = Math.max(0, Math.min(1, fraction));
    $("provider-catalog-note").textContent = `未取得 ${integer(catalog.not_downloaded)} 鍵 · 上次成功 ${datetime(catalog.last_receipt_at_utc) || "尚無成功收據"} · 目錄鍵有收據不代表完整歷史。`;
  }
  const dedicated = DEDICATED[data.provider];
  $("provider-specialized").hidden = !dedicated;
  if (dedicated) $("provider-specialized").href = dedicated[0];
}
function renderMarkets(options) {
  const select = $("provider-market-filter");
  const previous = select.value;
  const choices = new Map(Object.entries(options || {}));
  select.replaceChildren(new Option("全部市場", "all"), ...[...choices].sort((a, b) =>
    (MARKET_ORDER[a[0]] ?? 99) - (MARKET_ORDER[b[0]] ?? 99)).map(([key, label]) => new Option(label || key, key)));
  if (choices.has(previous)) select.value = previous;
}
function rowNode(row) {
  const tr = make("tr");
  tr.dataset.operationState = row.operation_state || "reference";
  const identity = make("td");
  identity.dataset.label = "端點／責任";
  identity.append(make("strong", "source-name", row.title || row.id),
    make("span", "cell-note", row.endpoint_id || row.id),
    make("span", "cell-note", `${row.market_category_label || "其他"} · ${row.update_owner || "責任未指定"}`));
  if (row.registry_alias) identity.append(make("span", "cell-note", "清冊別名；不重複計入主動端點"));
  const activity = make("td");
  activity.dataset.label = "狀態與進度";
  activity.append(make("span", `status-pill ${row.operation_state || "reference"}`,
    row.operation_label || OPERATION_LABEL[row.operation_state] || "待判定"));
  activity.append(make("span", "cell-note", row.operation_reason || row.execution_state || "—"));
  progress(activity, row.acquisition_progress);
  const storage = make("td");
  storage.dataset.label = "實存資料";
  storage.append(make("strong", "source-name", sourceCount(row.record_stats)),
    make("span", "cell-note", `最早：${sourceDate(row.record_stats?.first, row.record_stats)}`),
    make("span", "cell-note", `最新：${sourceDate(row.record_stats?.last, row.record_stats)}`));
  if (numeric(row.record_stats?.files_total) !== null) storage.append(make("span", "cell-note",
    `檔案核實 ${integer(row.record_stats.files_inspected)}/${integer(row.record_stats.files_total)}`));
  const schedule = make("td");
  schedule.dataset.label = "發布、下次取得與 ETA";
  schedule.append(make("strong", "source-name", row.publication?.schedule_label || "來源未提供發布時間"));
  const observed = datetime(row.publication?.detected_at_utc || row.publication?.observed_at_utc);
  if (observed) schedule.append(make("span", "cell-note", `實測／觀測：${observed}（不冒充官方發布）`));
  schedule.append(make("span", "cell-note", `下次取得：${datetime(row.automation?.next_run_at_utc) || row.automation?.schedule_label || "未提供"}`),
    make("span", "cell-note", `預估完成：${eta(row.eta)}`));
  if (row.warnings?.length) schedule.append(make("span", "warnings", row.warnings.join("；")));
  tr.append(identity, activity, storage, schedule);
  return tr;
}
function renderRows() {
  $("provider-source-rows").replaceChildren(...state.rows.map(rowNode));
  $("provider-result-count").textContent = `顯示 ${integer(state.rows.length)}/${integer(state.matched)} 項符合結果`;
  $("provider-source-more").hidden = state.nextOffset >= state.matched;
  $("provider-source-empty").hidden = state.matched !== 0;
}
async function refreshQuota() {
  const dedicated = DEDICATED[state.provider];
  if (!dedicated?.[1] || state.quotaInFlight || document.hidden || Date.now() - state.lastQuotaAt < 60000) return;
  state.quotaInFlight = true;
  try {
    const data = await fetchJson(dedicated[1]);
    const observed = data.quota?.observed_at_utc || data.quota?.provider_observed_at_utc || data.traffic?.observed_at_utc;
    if (state.provider === "FinMind") {
      const quota = data.quota || {};
      $("provider-quota").textContent = numeric(quota.provider_used_in_hour) !== null
        ? `${integer(quota.provider_used_in_hour)}/${integer(quota.official_requests_per_hour)} 次／小時（帳號觀測）`
        : "帳號用量未核實";
      $("provider-quota-basis").textContent = `程序最近 60 分鐘 ${integer(quota.observed_requests_60m)} 次；帳號採樣 ${datetime(observed) || "時間未核實"}。兩種用量不相等。`;
    } else if (state.provider === "FinLab") {
      const quota = data.quota || {};
      $("provider-quota").textContent = numeric(quota.used_mb) !== null
        ? `${integer(quota.used_mb)}/${integer(quota.limit_mb)} MB（最近回執）` : "尚無額度回執";
      $("provider-quota-basis").textContent = `觀測 ${datetime(observed) || "時間未核實"}；不是瀏覽器呼叫 SDK。`;
    } else if (state.provider === "永豐 Shioaji") {
      const traffic = data.traffic || {};
      const used = numeric(traffic.used_bytes);
      const limit = numeric(traffic.limit_bytes);
      $("provider-quota").textContent = used !== null && limit !== null
        ? `${(used / 1048576).toFixed(1)} / ${(limit / 1048576).toFixed(1)} MiB` : "券商流量尚無核實值";
      $("provider-quota-basis").textContent = `觀測 ${datetime(observed) || "時間未核實"}；交易／行情限制分別遵守券商契約。`;
    }
    state.lastQuotaAt = Date.now();
  } catch (_error) {
    $("provider-quota-basis").textContent = "專用配額收據暫時無法讀取；未把未知值當作零。";
  } finally { state.quotaInFlight = false; }
}
async function refresh({reset = false, append = false} = {}) {
  if (document.hidden || !state.provider) return;
  if (state.inFlight) {
    state.pendingRefresh = {reset, append};
    return;
  }
  state.inFlight = true;
  const search = $("provider-source-search").value.trim();
  const operation = $("provider-status-filter").value;
  const market = $("provider-market-filter").value;
  const offset = append && !reset ? state.nextOffset : 0;
  const limit = append ? PAGE_SIZE : reset ? PAGE_SIZE : Math.max(PAGE_SIZE, state.rows.length);
  try {
    const params = new URLSearchParams({name: state.provider, offset: String(offset),
      limit: String(limit), q: search, state: operation, market});
    const data = await fetchJson(`/data-monitor/api/provider?${params}`);
    if (!Array.isArray(data.sources) || data.read_only !== true) throw new Error("Invalid provider snapshot");
    if (search !== $("provider-source-search").value.trim()
        || operation !== $("provider-status-filter").value
        || market !== $("provider-market-filter").value) {
      state.pendingRefresh = {reset: true};
      return;
    }
    if (append && state.snapshotAt && data.generated_at_utc !== state.snapshotAt) {
      state.pendingRefresh = {reset: true};
      return;
    }
    renderHeadline(data);
    if (append) {
      const seen = new Set(state.rows.map((row) => row.id));
      state.rows.push(...data.sources.filter((row) => !seen.has(row.id)));
    } else {
      state.rows = data.sources;
    }
    state.matched = numeric(data.page?.matched_total) || 0;
    state.nextOffset = offset + data.sources.length;
    state.snapshotAt = data.generated_at_utc || null;
    renderMarkets(data.summary?.market_category_options);
    renderRows();
    state.loaded = true;
    clearError();
    void refreshQuota();
  } catch (_error) {
    showError(state.loaded
      ? "更新暫時失敗；下方保留上次快照，不代表目前仍然最新。"
      : "來源頁面暫時無法讀取，或此來源標籤不存在。請回到全資料監控檢查。 ");
  } finally {
    state.inFlight = false;
    if (state.pendingRefresh) {
      const next = state.pendingRefresh;
      state.pendingRefresh = null;
      void refresh(next);
    }
  }
}

const pathMatch = /^\/data-monitor\/providers\/([^/]+)\/$/.exec(window.location.pathname);
try {
  state.provider = pathMatch ? decodeURIComponent(pathMatch[1]) : null;
} catch (_error) { state.provider = null; }
if (!state.provider) showError("網址沒有有效的來源標籤。請從全資料監控選擇來源。");
else void refresh();
let searchTimer = null;
$("provider-source-search").addEventListener("input", () => {
  window.clearTimeout(searchTimer);
  searchTimer = window.setTimeout(() => void refresh({reset: true}), 180);
});
for (const id of ["provider-status-filter", "provider-market-filter"]) {
  $(id).addEventListener("change", () => void refresh({reset: true}));
}
$("provider-source-more").addEventListener("click", () => void refresh({append: true}));
$("provider-filters").addEventListener("submit", (event) => event.preventDefault());
document.addEventListener("visibilitychange", () => { if (!document.hidden) void refresh(); });
Dashboard.scheduleRefresh(refresh, {intervalMs: 30000, immediate: false, refreshOnVisible: false});

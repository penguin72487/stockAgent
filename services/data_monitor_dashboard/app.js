"use strict";

const REFRESH_MS = 10000;
const FULL_REFRESH_TICKS = 6;
const SOURCE_PAGE_SIZE = 100;
const FETCH_TIMEOUT_MS = 15000;
const state = {
  data: null,
  refreshInFlight: false,
  refreshTick: 0,
  visibleRows: SOURCE_PAGE_SIZE,
  heavyRevision: "",
};
const $ = (id) => document.getElementById(id);
const DETAIL_LINKS = new Set(["../shioaji/", "../openbb/"]);

const STATUS_LABELS = {
  current: "正常", updating: "更新中", complete: "完成", waiting: "等待",
  stale: "需更新", degraded: "有缺口", blocked: "阻擋",
  unavailable: "不可用", deferred: "使用者延後", legacy: "封存", active: "正常",
};
const OPERATION_LABELS = {
  catching_up: "正在抓／還沒到最新",
  streaming: "正在串流",
  complete: "已完成／已到最新",
  unable: "無法完成",
};
const OPERATION_ORDER = {catching_up: 0, streaming: 1, complete: 2, unable: 3};
const EXECUTION_ORDER = {
  running: 0, streaming: 0, waiting_stream_window: 1, scheduled: 2,
  waiting_quota: 3, waiting: 4, idle_current: 5, on_demand: 6,
  deferred: 7, not_applicable: 8, not_configured: 9, failed: 10, blocked: 11, unknown: 12,
};

function number(value) {
  if (value == null || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function formatInteger(value) {
  const parsed = number(value);
  return parsed === null ? "—" : Math.round(parsed).toLocaleString("zh-TW");
}

function compact(value) {
  const parsed = number(value);
  if (parsed === null) return "—";
  return new Intl.NumberFormat("zh-TW", {notation: "compact", maximumFractionDigits: 2}).format(parsed);
}

function ageLabel(seconds) {
  const value = number(seconds);
  if (value === null) return "無更新時間";
  if (value < 60) return `${Math.max(0, Math.round(value))} 秒前`;
  if (value < 3600) return `${Math.round(value / 60)} 分鐘前`;
  if (value < 86400) return `${Math.round(value / 3600)} 小時前`;
  return `${Math.round(value / 86400)} 天前`;
}

function durationLabel(seconds) {
  const value = number(seconds);
  if (value === null) return null;
  if (value <= 0) return "已完成";
  if (value < 3600) return `約 ${Math.max(1, Math.round(value / 60))} 分鐘`;
  if (value < 86400) return `約 ${(value / 3600).toFixed(value < 36000 ? 1 : 0)} 小時`;
  if (value < 31557600) return `約 ${(value / 86400).toFixed(value < 864000 ? 1 : 0)} 天`;
  return `約 ${(value / 31557600).toFixed(1)} 年`;
}

function timeLabel(value) {
  const parsed = new Date(value || "");
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toLocaleString("zh-TW", {
    timeZone: "Asia/Taipei", hour12: false,
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit",
  });
}

function futureLabel(value) {
  const parsed = new Date(value || "");
  if (Number.isNaN(parsed.getTime())) return null;
  const seconds = (parsed.getTime() - Date.now()) / 1000;
  if (seconds <= 0) return "現在／已到時";
  return durationLabel(seconds)?.replace(/^約 /, "") || null;
}

function etaLabel(eta) {
  const duration = durationLabel(eta?.remaining_seconds);
  if (duration) return duration;
  const labels = {
    continuous: "持續串流，無完工日",
    on_demand: "按需查詢",
    waiting_quota: "等待配額，暫無 ETA",
    waiting_schedule: "待排程，暫無 ETA",
    running_unmeasured: "執行中，正在累積速率",
    phase_estimate: "執行階段 ETA",
    blocked: "阻擋中",
    unknown: "尚無有效速率",
  };
  return labels[String(eta?.state || "unknown")] || "暫無可靠 ETA";
}

function statusLabel(status) {
  return STATUS_LABELS[String(status || "unavailable")] || "未知";
}

function operationLabel(row) {
  const stateName = String(row?.operation_state || "unable");
  return row?.operation_label || OPERATION_LABELS[stateName] || "無法判定";
}

function scheduleLines(row) {
  const automation = row?.automation || {};
  const basisLabel = {
    systemd_timer: "systemd 實際 next elapse",
    declared_calendar: "已部署曆時契約",
    contract_only: "更新契約",
  }[automation.next_run_basis] || "更新契約";
  const window = automation.stream_window;
  if (window) {
    const start = timeLabel(window.starts_at_utc);
    const end = timeLabel(window.ends_at_utc);
    if (window.state === "open") {
      return {
        primary: row.operation_state === "streaming" ? "目前正在串流" : "串流時窗已開，等待落盤心跳",
        secondary: end ? `本時窗至 ${end}` : window.schedule_label,
      };
    }
    return {
      primary: start ? `下次串流 ${start}` : "等待下一個串流時窗",
      secondary: start ? `${futureLabel(window.starts_at_utc) || "—"}後 · ${window.schedule_label}` : window.schedule_label,
    };
  }
  const next = timeLabel(automation.next_run_at_utc);
  if (automation.job_running) {
    return {primary: "自動更新執行中", secondary: `${automation.schedule_label || row.cadence} · ${basisLabel}`};
  }
  if (next) {
    return {primary: `下次 ${next}`, secondary: `${futureLabel(automation.next_run_at_utc) || "—"}後 · ${automation.schedule_label || row.cadence} · ${basisLabel}`};
  }
  if (automation.schedule_state === "not_configured") {
    return {primary: "未註冊可執行排程", secondary: automation.schedule_label || row.cadence};
  }
  if (automation.schedule_state === "on_demand") {
    return {primary: "按需查詢", secondary: automation.schedule_label || row.cadence};
  }
  return {primary: automation.schedule_label || row.cadence || "未指定", secondary: automation.schedule_state || "排程契約"};
}

function setHealth(target, health, label) {
  const value = String(health || "unavailable");
  target.className = `health ${value}`;
  target.lastChild.textContent = label || STATUS_LABELS[value] || "需注意";
}

function make(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined && text !== null) node.textContent = String(text);
  return node;
}

function progressBlock(coverage, className = "mini-progress") {
  const wrap = make("div", className);
  if (!coverage || number(coverage.ratio) === null) {
    wrap.append(make("span", "", "尚無可加總的完整度單位"));
    return wrap;
  }
  const ratio = Math.min(1, Math.max(0, Number(coverage.ratio)));
  const bar = document.createElement("progress");
  bar.max = 1;
  bar.value = ratio;
  bar.setAttribute("aria-label", String(coverage.label || "完整度"));
  wrap.append(bar);
  wrap.append(make("span", "", `${(ratio * 100).toFixed(ratio < .1 ? 2 : 1)}% · ${formatInteger(coverage.current)}/${formatInteger(coverage.total)} ${coverage.unit || ""}`));
  return wrap;
}

function renderSummary(data) {
  const summary = data.summary || {};
  const healthLabel = data.health === "active"
    ? "所有來源正常"
    : data.health === "updating"
      ? "完整回補進行中"
      : "有來源需要處理";
  setHealth($("overall-health"), data.health, healthLabel);
  const generated = new Date(data.generated_at_utc || "");
  $("updated-at").textContent = Number.isNaN(generated.getTime())
    ? "無更新時間"
    : generated.toLocaleString("zh-TW", {timeZone: "Asia/Taipei", hour12: false});
  const ratio = Math.min(1, Math.max(0, number(summary.source_level_ratio) || 0));
  $("overall-percent").textContent = `${(ratio * 100).toFixed(1)}% 已完成或串流中`;
  $("overall-progress").value = ratio;
  $("registered-items").textContent = formatInteger(summary.registered_items);
  $("registered-detail").textContent = `${formatInteger(summary.storage_groups)} 群組 · ${formatInteger(summary.product_granularities)} 產品粒度 · ${formatInteger(summary.crypto_fact_families)} 加密事實 · ${formatInteger(summary.credential_gates)} 憑證閘門 · ${formatInteger(summary.logical_sources)} 逐來源`;
  $("catching-up-items").textContent = formatInteger(summary.catching_up);
  $("streaming-items").textContent = formatInteger(summary.streaming);
  $("completed-items").textContent = formatInteger(summary.completed);
  $("unable-items").textContent = formatInteger(summary.unable);
  $("known-rows").textContent = compact(summary.known_group_rows);
  if (data.definitions?.realtime_boundary) $("boundary-copy").textContent = data.definitions.realtime_boundary;
}

function groupCard(row) {
  const card = make("article", "group-card");
  const head = make("div", "group-head");
  const titleWrap = make("div");
  titleWrap.append(make("h3", "", row.title));
  titleWrap.append(make("span", "provider", row.provider));
  const badge = make("span", `status-pill ${row.operation_state}`, operationLabel(row));
  head.append(titleWrap, badge);
  card.append(head);
  card.append(make("p", "detail", row.detail));
  const meta = make("div", "group-meta");
  for (const [label, value] of [
    ["資料截至", row.data_through || "連續／未提供"],
    ["預估完成", etaLabel(row.eta)],
    ["下次更新", scheduleLines(row).primary],
  ]) {
    const item = make("div");
    item.append(make("span", "", label), make("strong", "", value));
    meta.append(item);
  }
  card.append(meta);
  const progress = progressBlock(row.coverage, "group-progress");
  const progressHead = make("div");
  progressHead.append(make("span", "", row.coverage?.label || "完整度"), make("span", "", ageLabel(row.freshness?.age_seconds)));
  progress.prepend(progressHead);
  card.append(progress);
  if (DETAIL_LINKS.has(String(row.detail_link || ""))) {
    const link = make("a", "detail-link", "查看專屬資料面板 →");
    link.href = row.detail_link;
    card.append(link);
  }
  return card;
}

function renderGroups(groups) {
  const container = $("group-grid");
  const fragment = document.createDocumentFragment();
  for (const row of sortedRows(groups || [])) fragment.append(groupCard(row));
  container.replaceChildren(fragment);
}

function populateProviders(rows) {
  const select = $("provider-filter");
  const selected = select.value;
  const providers = [...new Set((rows || []).map((row) => String(row.provider || "其他")))].sort((a, b) => a.localeCompare(b, "zh-Hant"));
  const fragment = document.createDocumentFragment();
  const all = document.createElement("option");
  all.value = "all";
  all.textContent = "全部供應商";
  fragment.append(all);
  for (const provider of providers) {
    const option = document.createElement("option");
    option.value = provider;
    option.textContent = provider;
    fragment.append(option);
  }
  select.replaceChildren(fragment);
  if (providers.includes(selected)) select.value = selected;
}

function tableRow(row) {
  const tr = document.createElement("tr");
  tr.dataset.operationState = String(row.operation_state || "unable");
  const name = document.createElement("td");
  name.dataset.label = "資料來源";
  name.append(make("strong", "source-name", row.title));
  const scopeLabel = row.scope === "storage_group"
    ? "資料群組"
    : row.scope === "product_granularity"
      ? `產品粒度：${row.granularity || "—"}`
      : row.scope === "credential_gate"
        ? "API 憑證閘門"
      : row.scope === "crypto_fact_family"
        ? "加密唯一事實"
      : "逐來源";
  name.append(make("span", "source-provider", `${row.provider} · ${scopeLabel}`));
  if (row.availability) name.append(make("span", "cell-note", `可用性：${row.availability}`));
  if (row.warnings?.length) name.append(make("span", "warnings", row.warnings.join("；")));

  const status = document.createElement("td");
  status.dataset.label = "排序狀態";
  status.append(make("span", `status-pill ${row.operation_state}`, operationLabel(row)));
  status.append(make("span", "cell-note", `${row.execution_state || "—"} · ${row.operation_reason || "—"}`));
  status.append(make("span", "raw-status", `原始：${statusLabel(row.status)} · ${row.status_label || "—"}`));
  if (row.scope === "crypto_fact_family" && row.credential_state !== "not_required") {
    status.append(make("span", "cell-note", `API：${row.credential_state || "unknown"} · ${row.credential_operational_state || "unknown"}`));
  }

  const coverage = document.createElement("td");
  coverage.dataset.label = "完整度";
  coverage.append(progressBlock(row.coverage));

  const schedule = document.createElement("td");
  schedule.dataset.label = "串流／下次更新";
  const timing = scheduleLines(row);
  schedule.append(make("span", "source-name", timing.primary));
  schedule.append(make("span", "cell-note", timing.secondary));

  const eta = document.createElement("td");
  eta.dataset.label = "預估完成";
  const etaText = make("span", `eta ${number(row.eta?.remaining_seconds) === null ? "unknown" : ""}`, etaLabel(row.eta));
  if (row.eta?.basis) etaText.title = String(row.eta.basis);
  eta.append(etaText);
  if (row.eta?.confidence) eta.append(make("span", "cell-note", `信心：${row.eta.confidence}`));
  const completionTime = timeLabel(row.eta?.estimated_complete_at_utc);
  if (completionTime) eta.append(make("span", "cell-note", `估計完成：${completionTime}`));

  const through = document.createElement("td");
  through.dataset.label = "最近驗證／資料截至";
  through.append(make("span", "source-name", timeLabel(row.last_verified_at_utc) || "無可驗證時間"));
  through.append(make("span", "cell-note", `資料截至：${row.data_through || "連續／未提供"} · ${ageLabel(row.freshness?.age_seconds)}`));
  if (number(row.rows) !== null) through.append(make("span", "cell-note", `${formatInteger(row.rows)} 列`));

  const owner = document.createElement("td");
  owner.dataset.label = "責任／證據";
  owner.append(make("span", "source-name", row.update_owner || "未指定"));
  owner.append(make("span", "cell-note", `端點：${row.endpoint_id || row.id || "—"}`));
  owner.append(make("span", "cell-note", `證據：${(row.automation?.evidence || ["未提供"]).join(" + ")}`));
  tr.append(name, status, coverage, schedule, eta, through, owner);
  return tr;
}

function sortedRows(rows) {
  return [...rows].sort((left, right) => {
    const stateDiff = (OPERATION_ORDER[left.operation_state] ?? 99) - (OPERATION_ORDER[right.operation_state] ?? 99);
    if (stateDiff) return stateDiff;
    const executionDiff = (EXECUTION_ORDER[left.execution_state] ?? 99) - (EXECUTION_ORDER[right.execution_state] ?? 99);
    if (executionDiff) return executionDiff;
    const leftEta = number(left.eta?.remaining_seconds) ?? Number.POSITIVE_INFINITY;
    const rightEta = number(right.eta?.remaining_seconds) ?? Number.POSITIVE_INFINITY;
    if (leftEta !== rightEta) return leftEta - rightEta;
    const leftNext = new Date(left.automation?.next_run_at_utc || "").getTime();
    const rightNext = new Date(right.automation?.next_run_at_utc || "").getTime();
    const safeLeftNext = Number.isFinite(leftNext) ? leftNext : Number.POSITIVE_INFINITY;
    const safeRightNext = Number.isFinite(rightNext) ? rightNext : Number.POSITIVE_INFINITY;
    if (safeLeftNext !== safeRightNext) return safeLeftNext - safeRightNext;
    return String(left.sort_index || left.endpoint_id || "").localeCompare(String(right.sort_index || right.endpoint_id || ""), "zh-Hant", {numeric: true});
  });
}

function filteredRows() {
  const rows = state.data?.sources || [];
  const query = $("search").value.trim().toLocaleLowerCase("zh-Hant");
  const provider = $("provider-filter").value;
  const status = $("status-filter").value;
  const scope = $("scope-filter").value;
  const granularity = $("granularity-filter").value;
  return sortedRows(rows.filter((row) => {
    if (provider !== "all" && row.provider !== provider) return false;
    if (status !== "all" && row.operation_state !== status) return false;
    if (scope === "storage_group" && row.scope !== "storage_group") return false;
    if (scope === "logical_source" && row.scope === "storage_group") return false;
    if (scope === "product_granularity" && row.scope !== "product_granularity") return false;
    if (scope === "credential_gate" && row.scope !== "credential_gate") return false;
    if (scope === "crypto_fact_family" && row.scope !== "crypto_fact_family") return false;
    if (granularity !== "all" && row.granularity !== granularity) return false;
    if (!query) return true;
    return [row.title, row.provider, row.update_owner, row.category, row.granularity, row.availability, row.detail]
      .some((value) => String(value || "").toLocaleLowerCase("zh-Hant").includes(query));
  }));
}

function renderRows({reset = false} = {}) {
  if (reset) state.visibleRows = SOURCE_PAGE_SIZE;
  const rows = filteredRows();
  const visible = rows.slice(0, state.visibleRows);
  const fragment = document.createDocumentFragment();
  for (const row of visible) fragment.append(tableRow(row));
  $("source-rows").replaceChildren(fragment);
  $("result-count").textContent = `顯示 ${formatInteger(visible.length)}／${formatInteger(rows.length)} 項符合結果 · 全部 ${formatInteger(state.data?.sources?.length || 0)} 項`;
  $("load-more").hidden = visible.length >= rows.length;
  $("empty-state").hidden = rows.length !== 0;
}

function heavyRevision(data) {
  return JSON.stringify([
    data.groups,
    (data.sources || []).map((row) => [
      row.endpoint_id, row.id, row.status, row.operation_state,
      row.execution_state, row.coverage, row.eta, row.data_through,
      row.rows, row.last_verified_at_utc, row.automation, row.warnings,
    ]),
  ]);
}

async function fetchJson(path) {
  const controller = new AbortController();
  const timer = window.setTimeout(
    () => controller.abort(new DOMException("Request timed out", "TimeoutError")),
    FETCH_TIMEOUT_MS,
  );
  let response;
  try { response = await fetch(path, {cache: "no-store", signal: controller.signal}); }
  finally { window.clearTimeout(timer); }
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}

async function refresh({details = false} = {}) {
  if (document.hidden || state.refreshInFlight) return;
  state.refreshInFlight = true;
  try {
    const data = await fetchJson(details ? "api/status" : "api/summary");
    renderSummary(data);
    if (details) {
      state.data = data;
      const revision = heavyRevision(data);
      if (revision !== state.heavyRevision) {
        state.heavyRevision = revision;
        renderGroups(data.groups || []);
        populateProviders(data.sources || []);
        renderRows({reset: true});
      }
    }
  } catch (_error) {
    setHealth($("overall-health"), "unavailable", "監控 API 暫時離線");
  } finally {
    state.refreshInFlight = false;
  }
}

for (const id of ["search", "provider-filter", "status-filter", "granularity-filter", "scope-filter"]) {
  $(id).addEventListener(id === "search" ? "input" : "change", () => renderRows({reset: true}));
}
$("load-more").addEventListener("click", () => {
  state.visibleRows += SOURCE_PAGE_SIZE;
  renderRows();
});
$("filters").addEventListener("submit", (event) => event.preventDefault());
document.addEventListener("visibilitychange", () => { if (!document.hidden) void refresh({details: true}); });
void refresh({details: true});
window.setInterval(() => {
  state.refreshTick += 1;
  void refresh({details: state.refreshTick % FULL_REFRESH_TICKS === 0});
}, REFRESH_MS);

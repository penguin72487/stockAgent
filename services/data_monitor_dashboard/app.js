"use strict";

const REFRESH_MS = 10000;
const FULL_REFRESH_TICKS = 6;
const SOURCE_PAGE_SIZE = 100;
const Dashboard = window.StockAgentDashboard;
const fetchJson = Dashboard.createJsonFetcher({timeoutMs: 15000, cache: "no-store", expectedRoot: "object"});
const state = {
  data: null,
  refreshInFlight: false,
  refreshTick: 0,
  visibleRows: SOURCE_PAGE_SIZE,
  heavyRevision: "",
};
const $ = Dashboard.byId;
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
  deferred: "已延後／未啟用",
  control: "設定／憑證閘門",
  reference: "清冊參照／不重複計算",
};
const OPERATION_ORDER = {catching_up: 0, streaming: 1, complete: 2, unable: 3, deferred: 4, control: 5, reference: 6};
const EXECUTION_ORDER = {
  running: 0, streaming: 0, waiting_stream_window: 1, scheduled: 2,
  waiting_quota: 3, waiting: 4, idle_current: 5, on_demand: 6,
  deferred: 7, control: 8, registry_alias: 9, not_applicable: 10, not_configured: 11, failed: 12, blocked: 13, unknown: 14,
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
  return Dashboard.formatNumber(parsed, {notation: "compact", maximumFractionDigits: 2});
}

function ageLabel(seconds) {
  return Dashboard.formatAge(seconds, {emptyLabel: "無更新時間", hourDigits: 0, dayDigits: 0});
}

function durationLabel(seconds) {
  const value = number(seconds);
  if (value === null) return null;
  if (value <= 0) return "不到 1 分鐘";
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
  const stateName = String(eta?.state || "unknown");
  if (stateName === "complete") return "已完成／已到最新";
  if (stateName === "deferred") return "未啟用，無完工倒數";
  if (stateName === "not_applicable") return "不適用";
  if (stateName === "reference") return "請見專用端點";
  const remaining = number(eta?.remaining_seconds);
  if (remaining !== null && remaining > 0) return durationLabel(remaining);
  const labels = {
    continuous: "持續串流，無完工日",
    on_demand: "按需查詢",
    waiting_quota: "等待配額，暫無 ETA",
    waiting_schedule: "待排程，暫無 ETA",
    running_unmeasured: "執行中，正在累積速率",
    warming_up: "執行中，重新量測 ETA",
    phase_estimate: "執行階段 ETA",
    blocked: "阻擋中",
    unknown: "尚無有效速率",
  };
  return labels[stateName] || "暫無可靠 ETA";
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

function progressBlock(progress, className = "mini-progress") {
  const wrap = make("div", className);
  if (!progress) {
    wrap.append(make("span", "", "尚無取得進度證據"));
    return wrap;
  }
  const progressState = String(progress.state || "");
  if (["deferred", "not_applicable", "reference", "blocked", "stale_complete_receipt"].includes(progressState)) {
    wrap.append(make("span", `progress-state ${progress.state}`, progress.label || "進度不適用"));
    if (progress.basis) wrap.append(make("span", "progress-basis", progress.basis));
    const evidence = progress.evidence_coverage;
    const evidenceRatio = number(evidence?.ratio);
    if (evidenceRatio !== null) {
      wrap.append(make("span", "progress-evidence", `舊收據證據：${(Math.min(1, Math.max(0, evidenceRatio)) * 100).toFixed(1)}% · ${formatInteger(evidence.current)}/${formatInteger(evidence.total)} ${evidence.unit || ""}`));
    }
    return wrap;
  }
  const ratioValue = number(progress.ratio);
  const bar = document.createElement("progress");
  bar.max = 1;
  bar.setAttribute("aria-label", String(progress.label || "取得進度"));
  if (ratioValue !== null) bar.value = Math.min(1, Math.max(0, ratioValue));
  else bar.className = "indeterminate";
  wrap.append(bar);
  if (ratioValue === null) {
    wrap.append(make("span", "", `${progress.label || "取得狀態未知"} · 分母未知`));
  } else {
    const ratio = Math.min(1, Math.max(0, ratioValue));
    wrap.append(make("span", "", `${(ratio * 100).toFixed(ratio < .1 ? 2 : 1)}% · ${formatInteger(progress.current)}/${formatInteger(progress.total)} ${progress.unit || ""}`));
    wrap.append(make("span", `progress-state ${progress.state || "unknown"}`, progress.label || "取得狀態未提供"));
  }
  if (progress.basis) wrap.append(make("span", "progress-basis", progress.basis));
  const evidence = progress.evidence_coverage;
  const evidenceRatio = number(evidence?.ratio);
  if (evidenceRatio !== null) {
    wrap.append(make("span", "progress-evidence", `舊收據證據：${(Math.min(1, Math.max(0, evidenceRatio)) * 100).toFixed(1)}% · ${formatInteger(evidence.current)}/${formatInteger(evidence.total)} ${evidence.unit || ""}`));
  }
  return wrap;
}

function publicationLines(row) {
  const publication = row?.publication || {};
  const detected = timeLabel(publication.detected_at_utc);
  const observed = timeLabel(publication.observed_at_utc);
  const applied = timeLabel(publication.applied_at_utc);
  const acquisition = scheduleLines(row);
  const evidence = detected
    ? `實測版本變更：${detected}`
    : observed
      ? `最近觀測：${observed}（非官方發布證明）`
      : "尚無實際發布／觀測時間";
  return {
    primary: publication.schedule_label || "來源未提供發布時間",
    evidence,
    applied: applied ? `完成套用：${applied}` : null,
    acquisition: `下次取得：${acquisition.primary}`,
  };
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
  const integrity = data.integrity_checks || {};
  $("overall-percent").textContent = `${(ratio * 100).toFixed(1)}% 已完成或串流中`;
  $("overall-progress").value = ratio;
  $("overall-denominator").textContent = `${formatInteger(summary.completed)} 完成 + ${formatInteger(summary.streaming)} 串流／${formatInteger(summary.active_data_endpoints)} 個主動資料端點；${formatInteger(summary.group_rollups)} 群組、${formatInteger(summary.deferred)} 延後、${formatInteger(summary.control_items)} 設定項、${formatInteger(summary.reference_items)} 清冊參照不進入分母。面板契約矛盾：${formatInteger(integrity.violations)}。`;
  $("registered-items").textContent = formatInteger(summary.registered_items);
  $("registered-detail").textContent = `${formatInteger(summary.storage_groups)} 群組 · ${formatInteger(summary.product_granularities)} 產品粒度 · ${formatInteger(summary.crypto_fact_families)} 加密事實 · ${formatInteger(summary.credential_gates)} 憑證閘門 · ${formatInteger(summary.reference_items)} 清冊參照 · ${formatInteger(summary.logical_sources)} 逐來源`;
  $("catching-up-items").textContent = formatInteger(summary.catching_up);
  $("streaming-items").textContent = formatInteger(summary.streaming);
  $("completed-items").textContent = formatInteger(summary.completed);
  $("unable-items").textContent = formatInteger(summary.unable);
  $("deferred-items").textContent = formatInteger(summary.deferred);
  $("control-items").textContent = formatInteger(summary.control_items);
  $("control-detail").textContent = `${formatInteger(summary.credential_ready)} 憑證就緒 · ${formatInteger(summary.credential_attention)} 待處理`;
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
  const publication = publicationLines(row);
  const acquisition = row.acquisition_progress || {};
  for (const [label, value] of [
    ["資料截至", row.data_through || "連續／未提供"],
    ["下一資料日", acquisition.preparing_for_date || "連續／未判定"],
    ["發布時間", publication.primary],
    ["預估完成", etaLabel(row.eta)],
    ["下次取得", scheduleLines(row).primary],
  ]) {
    const item = make("div");
    item.append(make("span", "", label), make("strong", "", value));
    meta.append(item);
  }
  card.append(meta);
  if (row.active_child_operation_counts) {
    const counts = Object.entries(row.active_child_operation_counts)
      .filter(([, count]) => Number(count) > 0)
      .map(([stateName, count]) => `${OPERATION_LABELS[stateName] || stateName} ${formatInteger(count)}`)
      .join(" · ");
    if (counts) card.append(make("p", "child-rollup", `必要子端點：${counts}`));
  }
  const progress = progressBlock(row.acquisition_progress, "group-progress");
  const progressHead = make("div");
  progressHead.append(make("span", "", row.acquisition_progress?.label || "取得進度"), make("span", "", ageLabel(row.freshness?.age_seconds)));
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
  coverage.dataset.label = "取得進度";
  coverage.append(progressBlock(row.acquisition_progress));
  if (row.acquisition_progress?.preparing_for_date) {
    coverage.append(make("span", "next-data-date", `下一資料日：${row.acquisition_progress.preparing_for_date}`));
  }

  const schedule = document.createElement("td");
  schedule.dataset.label = "發布／偵測／下次取得";
  const publication = publicationLines(row);
  schedule.append(make("span", "source-name publication-schedule", publication.primary));
  schedule.append(make("span", "cell-note", publication.evidence));
  if (publication.applied) schedule.append(make("span", "cell-note", publication.applied));
  schedule.append(make("span", "cell-note acquisition-schedule", publication.acquisition));

  const eta = document.createElement("td");
  eta.dataset.label = "預估完成";
  const etaText = make("span", `eta ${number(row.eta?.remaining_seconds) === null ? "unknown" : ""}`, etaLabel(row.eta));
  if (row.eta?.basis) etaText.title = String(row.eta.basis);
  eta.append(etaText);
  if (row.eta?.confidence) eta.append(make("span", "cell-note", `信心：${row.eta.confidence}`));
  const completionDate = new Date(row.eta?.estimated_complete_at_utc || "");
  const completionTime = timeLabel(row.eta?.estimated_complete_at_utc);
  if (completionTime && completionDate.getTime() > Date.now()) {
    eta.append(make("span", "cell-note", `估計完成：${completionTime}`));
  }

  const through = document.createElement("td");
  through.dataset.label = "最近驗證／資料截至";
  through.append(make("span", "source-name", timeLabel(row.last_verified_at_utc) || "無可驗證時間"));
  through.append(make("span", "cell-note", `資料截至：${row.data_through || "連續／未提供"} · ${ageLabel(row.freshness?.age_seconds)}`));
  if (row.acquisition_progress?.first_data_observed) through.append(make("span", "cell-note first-data", `首筆已收到${row.acquisition_progress?.first_data_at_utc ? `：${timeLabel(row.acquisition_progress.first_data_at_utc)}` : ""}`));
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
      row.rows, row.last_verified_at_utc, row.automation, row.publication,
      row.acquisition_progress, row.warnings,
    ]),
  ]);
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
Dashboard.scheduleRefresh(() => {
  state.refreshTick += 1;
  return refresh({details: state.refreshTick % FULL_REFRESH_TICKS === 0});
}, {intervalMs: REFRESH_MS, immediate: false, refreshOnVisible: false});

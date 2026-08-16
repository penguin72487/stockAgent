"use strict";

const REFRESH_MS = 10000;
const FETCH_TIMEOUT_MS = 15000;
const state = {data: null, refreshInFlight: false};
const $ = (id) => document.getElementById(id);
const DETAIL_LINKS = new Set(["../shioaji/", "../openbb/"]);

const STATUS_LABELS = {
  current: "正常", updating: "更新中", complete: "完成", waiting: "等待",
  stale: "需更新", degraded: "有缺口", blocked: "阻擋",
  unavailable: "不可用", legacy: "封存", active: "正常",
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
  $("overall-percent").textContent = `${(ratio * 100).toFixed(1)}% 已正常或處理中`;
  $("overall-progress").value = ratio;
  $("registered-items").textContent = formatInteger(summary.registered_items);
  $("registered-detail").textContent = `${formatInteger(summary.storage_groups)} 群組 · ${formatInteger(summary.logical_sources)} 逐來源`;
  $("healthy-items").textContent = formatInteger(summary.healthy_or_progressing);
  $("attention-items").textContent = formatInteger(summary.attention_required);
  $("known-rows").textContent = compact(summary.known_group_rows);
  if (data.definitions?.realtime_boundary) $("boundary-copy").textContent = data.definitions.realtime_boundary;
}

function groupCard(row) {
  const card = make("article", "group-card");
  const head = make("div", "group-head");
  const titleWrap = make("div");
  titleWrap.append(make("h3", "", row.title));
  titleWrap.append(make("span", "provider", row.provider));
  const badge = make("span", `status-pill ${row.status}`, statusLabel(row.status));
  head.append(titleWrap, badge);
  card.append(head);
  card.append(make("p", "detail", row.detail));
  const meta = make("div", "group-meta");
  for (const [label, value] of [
    ["資料到期", row.data_through || "未提供"],
    ["預估完成", etaLabel(row.eta)],
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
  for (const row of groups || []) fragment.append(groupCard(row));
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
  const name = document.createElement("td");
  name.dataset.label = "資料來源";
  name.append(make("strong", "source-name", row.title));
  name.append(make("span", "source-provider", `${row.provider} · ${row.scope === "storage_group" ? "資料群組" : "逐來源"}`));
  if (row.warnings?.length) name.append(make("span", "warnings", row.warnings.join("；")));

  const status = document.createElement("td");
  status.dataset.label = "狀態／新鮮度";
  status.append(make("span", `status-pill ${row.status}`, statusLabel(row.status)));
  status.append(make("span", "cell-note", `${row.status_label || "—"} · ${ageLabel(row.freshness?.age_seconds)}`));

  const coverage = document.createElement("td");
  coverage.dataset.label = "完整度";
  coverage.append(progressBlock(row.coverage));

  const eta = document.createElement("td");
  eta.dataset.label = "預估完成";
  const etaText = make("span", `eta ${number(row.eta?.remaining_seconds) === null ? "unknown" : ""}`, etaLabel(row.eta));
  if (row.eta?.basis) etaText.title = String(row.eta.basis);
  eta.append(etaText);
  if (row.eta?.confidence) eta.append(make("span", "cell-note", `信心：${row.eta.confidence}`));

  const through = document.createElement("td");
  through.dataset.label = "資料到期";
  through.append(make("span", "source-name", row.data_through || "連續／未提供"));
  if (number(row.rows) !== null) through.append(make("span", "cell-note", `${formatInteger(row.rows)} 列`));

  const owner = document.createElement("td");
  owner.dataset.label = "更新責任";
  owner.append(make("span", "source-name", row.update_owner || "未指定"));
  owner.append(make("span", "cell-note", row.cadence || "依來源排程"));
  tr.append(name, status, coverage, eta, through, owner);
  return tr;
}

function filteredRows() {
  const rows = state.data?.sources || [];
  const query = $("search").value.trim().toLocaleLowerCase("zh-Hant");
  const provider = $("provider-filter").value;
  const status = $("status-filter").value;
  const scope = $("scope-filter").value;
  return rows.filter((row) => {
    if (provider !== "all" && row.provider !== provider) return false;
    if (status !== "all" && row.status !== status) return false;
    if (scope !== "all" && row.scope !== scope) return false;
    if (!query) return true;
    return [row.title, row.provider, row.update_owner, row.category, row.detail]
      .some((value) => String(value || "").toLocaleLowerCase("zh-Hant").includes(query));
  });
}

function renderRows() {
  const rows = filteredRows();
  const fragment = document.createDocumentFragment();
  for (const row of rows) fragment.append(tableRow(row));
  $("source-rows").replaceChildren(fragment);
  $("result-count").textContent = `顯示 ${formatInteger(rows.length)}／${formatInteger(state.data?.sources?.length || 0)} 項`;
  $("empty-state").hidden = rows.length !== 0;
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

async function refresh() {
  if (document.hidden || state.refreshInFlight) return;
  state.refreshInFlight = true;
  try {
    const data = await fetchJson("api/status");
    state.data = data;
    renderSummary(data);
    renderGroups(data.groups || []);
    populateProviders(data.sources || []);
    renderRows();
  } catch (_error) {
    setHealth($("overall-health"), "unavailable", "監控 API 暫時離線");
  } finally {
    state.refreshInFlight = false;
  }
}

for (const id of ["search", "provider-filter", "status-filter", "scope-filter"]) {
  $(id).addEventListener(id === "search" ? "input" : "change", renderRows);
}
$("filters").addEventListener("submit", (event) => event.preventDefault());
document.addEventListener("visibilitychange", () => { if (!document.hidden) void refresh(); });
void refresh();
window.setInterval(() => void refresh(), REFRESH_MS);

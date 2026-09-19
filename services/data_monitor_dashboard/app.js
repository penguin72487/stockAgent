"use strict";

const REFRESH_MS = 10000;
const FULL_REFRESH_TICKS = 6;
const SOURCE_PAGE_SIZE = 25;
const MOBILE_SOURCE_PAGE_SIZE = 10;
const FEATURE_PAGE_SIZE = 80;
const MOBILE_FEATURE_PAGE_SIZE = 25;
const CATEGORY_ORDER = {
  taiwan_equity: 0, taiwan_derivatives: 1, taiwan_public: 2,
  global_equity: 3, forex: 4, macro: 5, cross_market: 6,
  configuration: 7, crypto: 8,
};
const Dashboard = window.StockAgentDashboard;
const fetchJson = Dashboard.createJsonFetcher({timeoutMs: 15000, cache: "no-store", expectedRoot: "object"});
const state = {
  data: null,
  sortedSources: [],
  refreshInFlight: false,
  refreshTick: 0,
  visibleRows: SOURCE_PAGE_SIZE,
  heavyRevision: "",
  groupRevision: "",
  categoryRevision: "",
  pendingCategory: null,
  detailsActivated: false,
  detailsQueued: false,
  featureRows: [],
  featureVisible: FEATURE_PAGE_SIZE,
  featureActivated: false,
  featureInFlight: false,
};
const $ = Dashboard.byId;
const DETAIL_LINKS = new Set(["../shioaji/", "../openbb/"]);
const INTEGER_FORMATTER = new Intl.NumberFormat("zh-TW", {maximumFractionDigits: 0});
const DATE_TIME_FORMATTER = new Intl.DateTimeFormat("zh-TW", {
  timeZone: "Asia/Taipei", hour12: false,
  year: "numeric", month: "2-digit", day: "2-digit",
  hour: "2-digit", minute: "2-digit",
});
const ROW_COLLATOR = new Intl.Collator("zh-Hant", {numeric: true});
const timeLabelCache = new Map();

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
  return parsed === null ? "—" : INTEGER_FORMATTER.format(Math.round(parsed));
}

function recordDate(value, stats) {
  if (stats?.state === "not_applicable") return "不適用";
  if (value) return String(value).replace("T", " ").replace(/\+00:00$/, " UTC");
  if (stats?.state === "empty" || (stats?.state === "verified" && stats.count === 0)) return "無資料";
  if (stats?.state === "verified") return "無可驗證時間界限";
  return "待驗證";
}

function recordCount(stats) {
  if (stats?.state === "not_applicable") return "不適用";
  if (number(stats?.count) !== null) return `${formatInteger(stats.count)} 筆`;
  if (number(stats?.invalid_files) > 0) {
    const lowerBound = number(stats?.verified_partial_count) !== null
      ? `已驗證部分 ${formatInteger(stats.verified_partial_count)} 筆；`
      : "";
    return `${lowerBound}總數未知（${formatInteger(stats.invalid_files)} 檔異常）`;
  }
  if (number(stats?.files_total) !== null && stats.files_total > 0) {
    const partial = number(stats?.verified_partial_count) !== null
      ? `已驗證部分 ${formatInteger(stats.verified_partial_count)} 筆；`
      : "";
    return `${partial}檢查中 ${formatInteger(stats.files_inspected)}/${formatInteger(stats.files_total)} 檔`;
  }
  if (stats?.state === "empty") return "尚無實存檔案";
  return "待驗證";
}

function recordFiles(stats) {
  if (number(stats?.files_total) === null) return "未建立檔案清冊";
  return `${formatInteger(stats.files_inspected)}/${formatInteger(stats.files_total)} 檔已核實${number(stats.invalid_files) > 0 ? ` · ${formatInteger(stats.invalid_files)} 檔異常` : ""}`;
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
  const key = String(value || "");
  if (timeLabelCache.has(key)) return timeLabelCache.get(key);
  const parsed = new Date(value || "");
  if (Number.isNaN(parsed.getTime())) return null;
  const label = DATE_TIME_FORMATTER.format(parsed);
  if (timeLabelCache.size >= 2048) timeLabelCache.clear();
  timeLabelCache.set(key, label);
  return label;
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
  $("verified-inventory").textContent = `${formatInteger(summary.physical_inventory_verified_items)}/${formatInteger(summary.physical_inventory_items)}`;
  const inventory = data.record_inventory_progress || {};
  $("inventory-detail").textContent = `${recordFiles({files_inspected: inventory.inspected_files, files_total: inventory.selected_files, invalid_files: inventory.invalid_files})}；${formatInteger(summary.physical_inventory_time_bounded_items)} 個資料集有首末時間，${formatInteger(summary.physical_inventory_invalid_items)} 個有異常。`;
  const categoryRevision = JSON.stringify(data.market_categories || []);
  if (categoryRevision !== state.categoryRevision) {
    state.categoryRevision = categoryRevision;
    renderMarketCategories(data.market_categories || []);
  }
  if (data.definitions?.realtime_boundary) $("boundary-copy").textContent = data.definitions.realtime_boundary;
  renderAcquisition(data.tw_public_acquisition);
}

function renderMarketCategories(categories) {
  const fragment = document.createDocumentFragment();
  for (const category of categories) {
    const button = make("button", "category-card");
    button.type = "button";
    button.append(
      make("span", "category-label", category.label),
      make("strong", "", `${formatInteger(category.items)} 項`),
      make("small", "", `筆數已核實 ${formatInteger(category.verified_items)} · 未核實 ${formatInteger(category.unverified_items)} · 異常 ${formatInteger(category.invalid_items)}`),
    );
    button.addEventListener("click", () => {
      state.pendingCategory = String(category.id);
      activateDetails();
      if (state.data?.sources) {
        $("category-filter").value = state.pendingCategory;
        state.pendingCategory = null;
        renderRows({reset: true});
      }
      $("source-list").scrollIntoView({behavior: "smooth", block: "start"});
    });
    fragment.append(button);
  }
  $("category-grid").replaceChildren(fragment);
}

function renderAcquisition(acquisition) {
  const gates = $("acquisition-gates");
  const datasets = $("acquisition-datasets");
  const next = $("acquisition-next");
  if (!acquisition) {
    gates.replaceChildren(make("p", "", "尚無臺灣公開資料擴充收據。"));
    datasets.replaceChildren();
    next.replaceChildren();
    return;
  }
  const observation = acquisition.observation || {};
  const batch = acquisition.full_batch || {};
  const cold = acquisition.cold_release || {};
  const training = acquisition.training_readiness || {};
  const history = acquisition.history_backfill || {};
  const backgroundCatalogs = acquisition.background_catalogs || {};
  const gcis = backgroundCatalogs.gcis || {};
  const fsc = backgroundCatalogs.fsc || {};
  const audit = acquisition.model_audit || {};
  const materialization = acquisition.materialization || {};
  const rows = acquisition.datasets || [];
  const liveCount = rows.filter((row) => row.live_downloaded).length;
  const coldCount = rows.filter((row) => row.in_cold_inventory).length;
  const observedCount = number(observation.observed);
  const registeredCount = number(observation.registered);
  const mofStatus = history.mof_status === "complete" ? "原稿已驗收"
    : history.mof_status === "backfilling" ? "回補中"
    : history.mof_status === "degraded" ? "待修復" : "未開始";
  const gateData = [
    ["商工開放資料", gcis.catalog_entries ? `${formatInteger(gcis.complete_entries)}/${formatInteger(gcis.catalog_entries)} 項` : gcis.checked_entries ? `已檢查 ${formatInteger(gcis.checked_entries)} 項` : "未開始",
      `原始 CSV ${formatInteger(gcis.saved_resources)} 份 · 無 CSV 連結 ${formatInteger(gcis.no_csv_entries)} 項 · 冷庫清冊列名 ${formatInteger(gcis.cold_inventory_raw_files)} 份 · 失敗 ${formatInteger(gcis.failed_entries)} 項${gcis.failed_names?.length ? `：${gcis.failed_names.join("、")}` : ""} · 檢查 ${timeLabel(gcis.last_poll_at_utc || gcis.last_catalog_check_utc) || "回補中／未檢查"}`],
    ["金管會公開統計", fsc.catalog_entries ? `${formatInteger(fsc.complete_entries)}/${formatInteger(fsc.catalog_entries)} 項` : fsc.checked_entries ? `已檢查 ${formatInteger(fsc.checked_entries)} 項` : "未開始",
      `原始 CSV ${formatInteger(fsc.saved_resources)} 份 · 冷庫清冊列名 ${formatInteger(fsc.cold_inventory_raw_files)} 份 · 失敗 ${formatInteger(fsc.failed_entries)} 項${fsc.failed_names?.length ? `：${fsc.failed_names.join("、")}` : ""} · 檢查 ${timeLabel(fsc.last_poll_at_utc || fsc.last_catalog_check_utc) || "回補中／未檢查"}`],
    ["官方歷史回補", `${formatInteger(history.official_daily_complete)}/${formatInteger(history.official_daily_total)} 組逐日`,
      `共同截至 ${history.official_daily_common_end || "未驗證"} · 財政部原稿檔 ${formatInteger(history.mof_raw_periods)}/${formatInteger(history.mof_indexed_releases)} · 正式驗收 ${formatInteger(history.mof_archived_releases)}/${formatInteger(history.mof_indexed_releases)} · 索引缺期 ${formatInteger(history.mof_index_period_gaps)} · ${mofStatus}`],
    ["來源觀測", observation.fresh === false ? "收據過期" : observedCount === null || registeredCount === null ? "—" : `${formatInteger(observedCount)}/${formatInteger(registeredCount)}`,
      `${formatInteger(observedCount)}/${formatInteger(registeredCount)} · 探測失敗 ${formatInteger(observation.failed_probes)} · 尚未套用 ${formatInteger(observation.unapplied_events)} · ${timeLabel(observation.updated_at_utc) || "無更新時間"}`],
    ["新增 live 檔案", `${liveCount}/${rows.length}`,
      "需有套用收據、原始檔、metadata 與 Parquet"],
    ["完整批次稽核", batch.coverage_complete ? (batch.current_versions_complete ? "通過" : "有後續異動") : "待更新",
      `${formatInteger(batch.dataset_count)}/${formatInteger(observation.registered)} 個來源 · 資料截至 ${batch.data_through || "—"} · 批次後異動 ${formatInteger(batch.changed_after_batch)}${batch.next_scheduled_at_utc ? ` · 下次排程 ${timeLabel(batch.next_scheduled_at_utc)}` : ""}`],
    ["最新冷庫清冊", cold.state === "inventory_verified" ? `${coldCount}/${rows.length}${cold.matches_full_batch ? " · 本批" : " · 舊批"}` : "未驗證",
      cold.published_at_utc ? `資料截至 ${cold.data_through || "—"} · 發布 ${timeLabel(cold.published_at_utc) || "—"}` : "缺少可驗證 head／manifest／inventory"],
    ["固定版解封", materialization.state === "ready_receipt" ? "READY" : materialization.state === "materializing" ? "還原中" : "未開始",
      `${cold.snapshot_id || "無固定 release"}${materialization.verified_at_utc ? ` · 驗證 ${timeLabel(materialization.verified_at_utc)}` : ""}`],
    ["全來源歷史訓練", training.all_source_history_ready ? "通過" : "未通過",
      `${formatInteger(training.current_snapshot_sources)} 個僅當期快照 · 主計總處原始公告 ${formatInteger(training.dgbas_saved_releases)}/${formatInteger(training.dgbas_registered_releases)} · 央行 ${training.cbc_complete ? "完整" : "待驗"}`],
    ["最近模型稽核", audit.state === "reported" ? (audit.model_safe ? (audit.matches_current_release ? "本版通過" : audit.pinned_release_ready ? "固定版通過" : "未綁版本") : "未通過") : "無有效收據",
      audit.state === "reported" ? `${formatInteger(audit.finding_counts?.critical)} 重大 · ${formatInteger(audit.finding_counts?.high)} 高風險 · ${formatInteger(audit.feature_count)} 欄${audit.research_only_same_close ? " · 同日收盤近似（研究）" : ""} · ${audit.config || "—"}${audit.audited_snapshot_id ? ` · ${audit.audited_snapshot_id}` : ""} · ${timeLabel(audit.observed_at_utc) || "—"}` : "需執行嚴格資料層稽核"],
  ];
  gates.replaceChildren(...gateData.map(([title, value, detail]) => {
    const card = make("article", "acquisition-gate");
    card.append(make("span", "", title), make("strong", "", value), make("small", "", detail));
    return card;
  }));
  datasets.replaceChildren(...rows.map((row) => {
    const item = make("div", "acquisition-row");
    const title = make("strong", "", row.title);
    const states = make("span", "", [
      row.observed ? "已觀測" : "未觀測",
      row.live_downloaded ? "live 已下載" : "live 待驗證",
      row.in_full_batch ? "納入完整批次" : "未納入完整批次",
      row.in_cold_inventory ? "冷庫已列名" : "冷庫未列名",
    ].join(" · "));
    item.append(title, states);
    return item;
  }));
  const nextLabels = {
    source_pilot_pending: "待驗證官方來源、版本與取得方式",
    on_demand_only: "僅按需查詢；不建立訓練資料鏡像",
  };
  next.replaceChildren(...(acquisition.next_work || []).map((item) => {
    const row = make("div", "acquisition-row");
    row.append(make("strong", "", item.title), make("span", "", nextLabels[item.state] || "狀態待確認"));
    return row;
  }));
  $("acquisition-boundary").textContent = `${acquisition.definitions?.history || ""} ${acquisition.definitions?.training || ""} ${acquisition.definitions?.model_audit || ""} ${acquisition.definitions?.cold_release || ""} ${acquisition.definitions?.materialization || ""}`;
}

function groupCard(row) {
  const card = make("article", "group-card");
  const head = make("div", "group-head");
  const titleWrap = make("div");
  titleWrap.append(make("h3", "", row.title));
  titleWrap.append(make("span", "provider", row.provider));
  titleWrap.append(make("span", "market-category", row.market_category_label || "跨市場／其他"));
  const badge = make("span", `status-pill ${row.operation_state}`, operationLabel(row));
  head.append(titleWrap, badge);
  card.append(head);
  card.append(make("p", "detail", row.detail));
  const meta = make("div", "group-meta");
  const publication = publicationLines(row);
  const acquisition = row.acquisition_progress || {};
  for (const [label, value] of [
    ["實存最早一筆", recordDate(row.record_stats?.first, row.record_stats)],
    ["實存最新一筆", recordDate(row.record_stats?.last, row.record_stats)],
    ["實存總筆數", recordCount(row.record_stats)],
    ["實存檔案", recordFiles(row.record_stats)],
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
  if (row.record_stats?.basis) {
    const evidence = make("p", "record-basis", row.record_stats.basis);
    card.append(evidence);
  }
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

function populateCategories(rows) {
  const select = $("category-filter");
  const selected = select.value;
  const categories = new Map((rows || []).map((row) => [String(row.market_category || "cross_market"), String(row.market_category_label || "跨市場／其他")]));
  const options = [...categories.entries()].sort((left, right) =>
    (CATEGORY_ORDER[left[0]] ?? 99) - (CATEGORY_ORDER[right[0]] ?? 99));
  const fragment = document.createDocumentFragment();
  const all = document.createElement("option");
  all.value = "all";
  all.textContent = "全部分類";
  fragment.append(all);
  for (const [category, label] of options) {
    const option = document.createElement("option");
    option.value = category;
    option.textContent = label;
    fragment.append(option);
  }
  select.replaceChildren(fragment);
  if (categories.has(selected)) select.value = selected;
}

function tableRow(row) {
  const tr = document.createElement("tr");
  tr.dataset.operationState = String(row.operation_state || "unable");
  const name = document.createElement("td");
  name.dataset.label = "資料來源";
  name.append(make("strong", "source-name", row.title));
  const scopeLabel = row.scope === "storage_group"
    ? "資料群組"
    : row.scope === "inventory_partition"
      ? "實體庫存分類（不重複計數）"
    : row.scope === "physical_inventory"
      ? "實體資料表（主表／衍生視圖分列）"
    : row.scope === "product_granularity"
      ? `產品粒度：${row.granularity || "—"}`
      : row.scope === "credential_gate"
        ? "API 憑證閘門"
      : row.scope === "crypto_fact_family"
        ? "加密唯一事實"
      : "逐來源";
  name.append(make("span", "source-provider", `${row.provider} · ${scopeLabel}`));
  name.append(make("span", "market-category", row.market_category_label || "跨市場／其他"));
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
  through.dataset.label = "實存首末筆／總筆數";
  through.append(make("span", "source-name", `最早：${recordDate(row.record_stats?.first, row.record_stats)}`));
  through.append(make("span", "cell-note", `最新：${recordDate(row.record_stats?.last, row.record_stats)}`));
  through.append(make("span", number(row.record_stats?.count) !== null ? "cell-note record-count" : "cell-note record-unknown", `總筆數：${recordCount(row.record_stats)}`));
  through.append(make("span", "cell-note", `檔案：${recordFiles(row.record_stats)}`));
  if (row.record_stats?.basis) {
    const basis = make("span", "cell-note record-basis", row.record_stats.basis);
    basis.title = String(row.record_stats.basis);
    through.append(basis);
  }
  through.append(make("span", "cell-note", `最近驗證：${timeLabel(row.last_verified_at_utc) || "無可驗證時間"}`));
  through.append(make("span", "cell-note", `資料截至：${row.data_through || "連續／未提供"} · ${ageLabel(row.freshness?.age_seconds)}`));
  if (row.acquisition_progress?.first_data_observed) through.append(make("span", "cell-note first-data", `首筆已收到${row.acquisition_progress?.first_data_at_utc ? `：${timeLabel(row.acquisition_progress.first_data_at_utc)}` : ""}`));
  if (number(row.rows) !== null && number(row.record_stats?.count) === null) through.append(make("span", "cell-note", `回執列數：${formatInteger(row.rows)}（不等於實存總筆數）`));

  const owner = document.createElement("td");
  owner.dataset.label = "責任／證據";
  owner.append(make("span", "source-name", row.update_owner || "未指定"));
  owner.append(make("span", "cell-note", `端點：${row.endpoint_id || row.id || "—"}`));
  owner.append(make("span", "cell-note", `證據：${(row.automation?.evidence || ["未提供"]).join(" + ")}`));
  tr.append(name, status, coverage, schedule, eta, through, owner);
  return tr;
}

function sortedRows(rows) {
  return rows.map((row) => ({
    row,
    category: CATEGORY_ORDER[row.market_category] ?? 99,
    provider: String(row.provider || ""),
    operation: OPERATION_ORDER[row.operation_state] ?? 99,
    execution: EXECUTION_ORDER[row.execution_state] ?? 99,
    eta: number(row.eta?.remaining_seconds) ?? Number.POSITIVE_INFINITY,
    next: (() => {
      const value = new Date(row.automation?.next_run_at_utc || "").getTime();
      return Number.isFinite(value) ? value : Number.POSITIVE_INFINITY;
    })(),
    label: String(row.sort_index || row.endpoint_id || ""),
  })).sort((left, right) => {
    const categoryDiff = left.category - right.category;
    if (categoryDiff) return categoryDiff;
    const providerDiff = ROW_COLLATOR.compare(left.provider, right.provider);
    if (providerDiff) return providerDiff;
    const stateDiff = left.operation - right.operation;
    if (stateDiff) return stateDiff;
    const executionDiff = left.execution - right.execution;
    if (executionDiff) return executionDiff;
    if (left.eta !== right.eta) return left.eta - right.eta;
    if (left.next !== right.next) return left.next - right.next;
    return ROW_COLLATOR.compare(left.label, right.label);
  }).map(({row}) => row);
}

function populateFeatureFilters(rows) {
  const category = $("feature-category");
  const source = $("feature-source");
  const selectedCategory = category.value;
  const selectedSource = source.value;
  const categories = new Map(rows.map((row) => [row.market_category, row.market_category_label]));
  const sources = new Map(rows.map((row) => [row.dataset_id, `${row.source_title} · ${row.provider}`]));
  category.replaceChildren(new Option("全部分類", "all"), ...[...categories].sort((a, b) =>
    (CATEGORY_ORDER[a[0]] ?? 99) - (CATEGORY_ORDER[b[0]] ?? 99)).map(([id, label]) => new Option(label, id)));
  source.replaceChildren(new Option("全部來源", "all"), ...[...sources].map(([id, label]) => new Option(label, id)));
  if (categories.has(selectedCategory)) category.value = selectedCategory;
  if (sources.has(selectedSource)) source.value = selectedSource;
}

function filteredFeatures() {
  const query = $("feature-search").value.trim().toLocaleLowerCase("zh-Hant");
  const category = $("feature-category").value;
  const source = $("feature-source").value;
  return state.featureRows.filter((row) => {
    if (category !== "all" && row.market_category !== category) return false;
    if (source !== "all" && row.dataset_id !== source) return false;
    return !query || [row.field, row.dataset_id, row.source_title, row.provider, row.market_category_label]
      .some((value) => String(value || "").toLocaleLowerCase("zh-Hant").includes(query));
  });
}

function featurePageSize() {
  return window.innerWidth <= 700 ? MOBILE_FEATURE_PAGE_SIZE : FEATURE_PAGE_SIZE;
}

function renderFeatures({reset = false} = {}) {
  if (reset) state.featureVisible = featurePageSize();
  const rows = filteredFeatures();
  const visible = rows.slice(0, state.featureVisible);
  const fragment = document.createDocumentFragment();
  let previousDataset = null;
  for (const row of visible) {
    if (row.dataset_id !== previousDataset) {
      const group = make("tr", "feature-group");
      const header = make("td", "", `${row.market_category_label} · ${row.provider} · ${row.source_title} (${row.dataset_id})`);
      header.colSpan = 5;
      group.append(header);
      fragment.append(group);
      previousDataset = row.dataset_id;
    }
    const tr = make("tr", "feature-item");
    const name = make("td");
    name.dataset.label = "欄位／Feature";
    name.append(make("strong", "source-name feature-name", row.field));
    const type = make("td");
    type.dataset.label = "型別／角色";
    type.append(make("span", "source-name", (row.types || []).join(" / ")));
    type.append(make("span", "cell-note", row.field_role === "key"
      ? "索引／識別欄位"
      : row.field_role === "conditional" ? "條件式證據欄位" : "資料／特徵欄位"));
    if (row.availability_note) {
      type.append(make("span", "cell-note", row.availability_note));
    }
    if ((row.types || []).length > 1) type.append(make("span", "warnings", "跨檔案型別漂移"));
    const nonNull = make("td");
    nonNull.dataset.label = "實際非空值";
    const exact = number(row.non_null_count);
    nonNull.append(make("strong", exact === null ? "record-unknown" : "record-count",
      exact === null ? "總數待驗證" : `${formatInteger(exact)} 筆`));
    if (exact === null && number(row.verified_partial_non_null) !== null) {
      nonNull.append(make("span", "cell-note", `已核實下界 ${formatInteger(row.verified_partial_non_null)} 筆`));
    }
    nonNull.append(make("span", "cell-note", `${formatInteger(row.non_null_known_files)}/${formatInteger(row.files_with_field)} 檔有 null 統計`));
    const files = make("td");
    files.dataset.label = "欄位所在檔案／列數";
    files.append(make("span", "source-name", `${formatInteger(row.files_with_field)}/${formatInteger(row.files_total)} 檔`));
    files.append(make("span", "cell-note", number(row.rows_with_field) === null
      ? `已核實部分 ${formatInteger(row.verified_partial_rows_with_field)} 列；總數待驗證`
      : `${formatInteger(row.rows_with_field)} 列具有此欄位`));
    const dates = make("td");
    dates.dataset.label = "資料集首末時間";
    dates.append(make("span", "source-name", `最早：${recordDate(row.dataset_first, {state: row.dataset_bounds_state})}`));
    dates.append(make("span", "cell-note", `最新：${recordDate(row.dataset_last, {state: row.dataset_bounds_state})}`));
    dates.append(make("span", "cell-note", "非欄位有效值／發布時間"));
    tr.append(name, type, nonNull, files, dates);
    fragment.append(tr);
  }
  $("feature-rows").replaceChildren(fragment);
  $("feature-empty").hidden = rows.length !== 0;
  $("feature-more").hidden = visible.length >= rows.length;
  const summary = state.featureSummary || {};
  $("feature-count").textContent = `顯示 ${formatInteger(visible.length)}/${formatInteger(rows.length)} 欄位 · 全部 ${formatInteger(summary.fields)} 欄位、${formatInteger(summary.datasets_with_schema)}/${formatInteger(summary.datasets_total)} 實體資料集已取得 schema · ${formatInteger(summary.files_with_schema)}/${formatInteger(summary.files_total)} 檔已取得欄位統計${summary.state === "complete" ? "" : "（仍有未核實檔案）"}`;
}

async function refreshFeatures() {
  if (!state.featureActivated || state.featureInFlight || document.hidden) return;
  state.featureInFlight = true;
  try {
    const data = await fetchJson("api/features");
    if (!Array.isArray(data.rows)) throw new Error("Invalid feature inventory");
    state.featureRows = data.rows;
    state.featureSummary = data.summary || {};
    populateFeatureFilters(state.featureRows);
    renderFeatures({reset: true});
  } catch (_error) {
    $("feature-count").textContent = "欄位清冊 API 暫時無法讀取；來源總覽仍可使用。";
  } finally {
    state.featureInFlight = false;
  }
}

function activateFeatures() {
  if (state.featureActivated) return;
  state.featureActivated = true;
  void refreshFeatures();
}

function filteredRows() {
  const rows = state.sortedSources;
  const query = $("search").value.trim().toLocaleLowerCase("zh-Hant");
  const provider = $("provider-filter").value;
  const category = $("category-filter").value;
  const status = $("status-filter").value;
  const inventory = $("inventory-filter").value;
  const scope = $("scope-filter").value;
  const granularity = $("granularity-filter").value;
  return rows.filter((row) => {
    if (provider !== "all" && row.provider !== provider) return false;
    if (category !== "all" && row.market_category !== category) return false;
    if (status !== "all" && row.operation_state !== status) return false;
    if (inventory !== "all") {
      const inventoryState = String(row.record_stats?.state || "unverified");
      if (inventory === "unverified" ? !["unverified", "scanning"].includes(inventoryState) : inventoryState !== inventory) return false;
    }
    if (scope === "storage_group" && row.scope !== "storage_group") return false;
    if (scope === "logical_source" && row.scope !== "logical_source") return false;
    if (scope === "physical_inventory" && row.scope !== "physical_inventory") return false;
    if (scope === "product_granularity" && row.scope !== "product_granularity") return false;
    if (scope === "credential_gate" && row.scope !== "credential_gate") return false;
    if (scope === "crypto_fact_family" && row.scope !== "crypto_fact_family") return false;
    if (granularity !== "all" && row.granularity !== granularity) return false;
    if (!query) return true;
    return [row.title, row.provider, row.update_owner, row.category, row.market_category_label, row.granularity, row.availability, row.detail]
      .some((value) => String(value || "").toLocaleLowerCase("zh-Hant").includes(query));
  });
}

function sourcePageSize() {
  return window.innerWidth <= 700 ? MOBILE_SOURCE_PAGE_SIZE : SOURCE_PAGE_SIZE;
}

function renderRows({reset = false} = {}) {
  if (reset) state.visibleRows = sourcePageSize();
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
  return `${data.schema_version || ""}|${data.generated_at_utc || ""}|${data.sources?.length || 0}`;
}

async function refresh({details = false} = {}) {
  if (document.hidden) return;
  if (state.refreshInFlight) {
    state.detailsQueued ||= details;
    return;
  }
  state.refreshInFlight = true;
  try {
    const data = await fetchJson(details ? "api/details" : "api/summary");
    if (!details) renderSummary(data);
    if (Array.isArray(data.groups)) {
      const groupRevision = JSON.stringify(data.groups);
      if (groupRevision !== state.groupRevision) {
        state.groupRevision = groupRevision;
        renderGroups(data.groups);
      }
    }
    if (details) {
      state.data = data;
      const revision = heavyRevision(data);
      if (revision !== state.heavyRevision) {
        state.heavyRevision = revision;
        state.sortedSources = sortedRows(data.sources || []);
        populateProviders(state.sortedSources);
        populateCategories(state.sortedSources);
        if (state.pendingCategory) {
          $("category-filter").value = state.pendingCategory;
          state.pendingCategory = null;
        }
        renderRows({reset: true});
      }
    }
  } catch (_error) {
    setHealth($("overall-health"), "unavailable", "監控 API 暫時離線");
  } finally {
    state.refreshInFlight = false;
    if (state.detailsQueued && !document.hidden) {
      state.detailsQueued = false;
      void refresh({details: true});
    }
  }
}

function activateDetails() {
  if (state.detailsActivated) return;
  state.detailsActivated = true;
  void refresh({details: true});
}

function installDetailsActivation() {
  const target = $("source-list");
  if ("IntersectionObserver" in window && target) {
    const activate = () => {
      observer.disconnect();
      window.removeEventListener("scroll", checkDistance);
      activateDetails();
    };
    const checkDistance = () => {
      const bounds = target.getBoundingClientRect();
      if (bounds.top <= window.innerHeight + 160 && bounds.bottom >= -160) activate();
    };
    const observer = new IntersectionObserver((entries) => {
      if (!entries.some((entry) => entry.isIntersecting)) return;
      activate();
    }, {rootMargin: "160px"});
    observer.observe(target);
    window.addEventListener("scroll", checkDistance, {passive: true});
    return;
  }
  if ("requestIdleCallback" in window) {
    window.requestIdleCallback(activateDetails, {timeout: 2000});
  } else {
    window.setTimeout(activateDetails, 1000);
  }
}

function installFeatureActivation() {
  const target = $("feature-list");
  if (!target) return;
  if ("IntersectionObserver" in window) {
    const observer = new IntersectionObserver((entries) => {
      if (!entries.some((entry) => entry.isIntersecting)) return;
      observer.disconnect();
      activateFeatures();
    }, {rootMargin: "220px"});
    observer.observe(target);
  } else {
    activateFeatures();
  }
}

for (const id of ["search", "provider-filter", "category-filter", "status-filter", "inventory-filter", "granularity-filter", "scope-filter"]) {
  $(id).addEventListener(id === "search" ? "input" : "change", () => renderRows({reset: true}));
}
$("load-more").addEventListener("click", () => {
  state.visibleRows += sourcePageSize();
  renderRows();
});
$("filters").addEventListener("submit", (event) => event.preventDefault());
for (const id of ["feature-search", "feature-category", "feature-source"]) {
  $(id).addEventListener(id === "feature-search" ? "input" : "change", () => renderFeatures({reset: true}));
}
$("feature-more").addEventListener("click", () => {
  state.featureVisible += featurePageSize();
  renderFeatures();
});
$("feature-filters").addEventListener("submit", (event) => event.preventDefault());
document.addEventListener("visibilitychange", () => {
  if (!document.hidden) {
    void refresh({details: state.detailsActivated});
    void refreshFeatures();
  }
});
// Install the viewport trigger only after summary groups have been painted.
// Otherwise the initially empty group container leaves the detail section in
// view and defeats the summary-first transfer boundary.
void refresh().finally(() => {
  installFeatureActivation();
  installDetailsActivation();
});
Dashboard.scheduleRefresh(() => {
  state.refreshTick += 1;
  if (state.refreshTick % FULL_REFRESH_TICKS === 0) void refreshFeatures();
  return refresh({details: state.detailsActivated && state.refreshTick % FULL_REFRESH_TICKS === 0});
}, {intervalMs: REFRESH_MS, immediate: false, refreshOnVisible: false});

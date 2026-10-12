"use strict";

const Dashboard = window.StockAgentDashboard;
const $ = Dashboard.byId;
const text = (id, value) => Dashboard.setText($(id), value);
const html = (id, value) => Dashboard.setTrustedHtml($(id), value);
const escape = Dashboard.escapeHtml;
const count = value => value == null ? "—" : Dashboard.formatNumber(value, {maximumFractionDigits: 0});
const decimal = value => value == null ? "—" : Dashboard.formatNumber(value, {maximumFractionDigits: 2});
const bytes = value => value == null ? "—" : Dashboard.formatBytes(value, {maximumFractionDigits: 2});
const labels = Object.freeze({
  running: "正在抓取", backfilling: "等待回補", inventorying: "清點新網址", idle: "等待到期工作", waiting: "等待來源",
  cooldown: "來源冷卻", known_queue_clear: "已知待辦已清", attention: "需注意", not_enabled: "未啟用／待確認",
  finite_complete: "固定批次正文已存", waiting_cohort: "新文章等待納入批次",
  authorized_api_pending: "授權 API 待接線",
  stopped: "下載器已停止", stale: "觀測已過期", unavailable: "尚無有效快照", invalid: "快照版本不相容", degraded: "快照數量不一致",
  draining: "正在排空保存", waiting_disk: "等待磁碟空間", waiting_restart: "設定交接，等待續跑",
});
const stateLabels = Object.freeze({
  complete: "已完成狀態", pending: "待回補", inflight: "處理中", retry: "待重試", retry_cooldown: "重試冷卻",
  awaiting_authorization: "未啟用／待確認", unsupported_article_path: "網址格式未支援", body_not_found: "缺正文",
  waiting_cohort: "新文章待納入，非抓取失敗",
  article_attribution_not_verified: "作者未驗證", article_license_not_verified: "授權標記未驗證", robots_disallowed: "robots 限制",
  article_identity_not_verified: "文章身分待驗證", article_rights_not_verified: "原創／來源署名待驗證", article_rights_or_scope_excluded: "原創／作者權利未確認或超出範圍",
  paywall_or_login: "付費／登入內容", access_challenge: "來源存取驗證", robots_access_challenge: "robots 存取驗證挑戰", robots_not_plain_rules: "robots 回應身分／規則待驗證", robots_redirect_refused: "robots 轉址待修",
  source_body_removed: "來源已主動撤下正文", source_not_found: "來源網址已不存在", source_body_image_only: "來源正文僅圖片，未做 OCR", source_body_embedded_only: "來源正文僅外部嵌入，未取得文字", source_article_empty_template: "來源目前回空白文章模板，未取得正文", pdf_image_only_or_body_not_found: "影像 PDF 待文字辨識",
  source_body_quote_only: "原站只提供行情片段，未取得新聞正文",
  source_not_allowed: "來源未允許", authorization_expired: "授權到期", paywall: "付費牆", not_found: "來源不存在",
  gone: "來源已移除", response_too_large: "回應過大", other: "其他待查原因",
});
const stages = Object.freeze({owner_initialization: "Owner 初始化", policy_reconciliation: "來源規則核對", metadata_inventory: "本機 metadata 清點",
  original_url_resolution: "既有轉址解碼／續跑", original_url_routing: "原站權限變更後續排",
  article_dispatch: "佇列派工", discovery_registration: "新網址註冊", batch_build: "批次建構", parquet_compress_fsync: "Parquet 壓縮／fsync",
  receipt_hash_fsync: "收據雜湊／fsync", acceptance_sqlite_commit: "耐久佇列提交", status_write: "快照寫入", idle_wait: "無工作等待",
  address_validation: "公開位址檢查", request_slot_wait: "限流等待", transport: "網路回應", article_extraction: "正文解析", discovery_extraction: "Feed／sitemap 解析"});
const capacityLabels = Object.freeze({warming: "累積樣本", ramping: "階梯升速", verifying: "退回後穩定驗證",
  recovering_transport: "暫時故障恢復驗證，非限速", endpoint_validation_failed: "端點／回應結構待修，非限速",
  waiting_source: "遵守來源暫停", insufficient_samples: "請求樣本不足", insufficient_useful_data: "無新增正文，不升速",
  official_or_learned_floor: "遵守來源／robots 間隔", stable_source_backoff: "限速後穩定設定",
  stable_pipeline_capacity: "管線穩定設定，非來源極限", engineering_ceiling_unverified: "客戶端資源邊界，來源極限未明"});
const reviewLabels = Object.freeze({permission_required: "需取得來源同意", review_incomplete: "來源審閱尚未完成",
  not_reviewed: "尚未審閱", private_public_candidate: "私人研究候選範圍，未自動啟用",
  reviewed_existing_scope: "已核對現有範圍", active_existing_policy: "依現有抓取規則",
  authorized_api_configuration_required: "授權 API 待設定", review_unverified: "審閱時間待驗證"});
const state = {latest: null, filter: "all", query: "", sort: "state", page: 0, inFlight: false, retry: null, retryDelay: 3000};
const PAGE_SIZE = 24;
const fetchJson = Dashboard.createJsonFetcher({expectedRoot: "object", cache: "no-store", timeoutMs: 15000});

function stamp(value) {
  if (!value) return "未知";
  const parsed = new Date(value);
  return Number.isFinite(parsed.getTime()) ? parsed.toLocaleString("zh-TW", {timeZone: "Asia/Taipei", hour12: false}) : "未知";
}
function duration(value) {
  if (value == null || !Number.isFinite(value)) return "未知";
  if (value === 0) return "已知可執行待辦已清";
  if (value >= 86400) return `約 ${decimal(value / 86400)} 天`;
  if (value >= 3600) return `約 ${decimal(value / 3600)} 小時`;
  if (value >= 60) return `約 ${decimal(value / 60)} 分鐘`;
  return `約 ${decimal(value)} 秒`;
}
function live(data) {
  const at = Date.parse(data?.observed_at_utc);
  return Number.isFinite(at) && Date.now() - at >= -5000 && Date.now() - at <= 180000
    && ["running", "idle", "waiting", "inventorying"].includes(data.health);
}
function eta(source, data) {
  return live(data) ? duration(source.conditional_eta_seconds) : "目前階段暫無有效估時";
}
function progress(id, ratio) {
  const bar = $(id);
  if (ratio == null) bar.removeAttribute("value");
  else bar.value = Math.max(0, Math.min(1, ratio));
}
function errorLabel(source) {
  const errors = Object.entries(source.http_errors || {}).map(([code, value]) => `HTTP ${code} × ${count(value)}`);
  if (source.transport_errors) errors.push(`連線錯誤 × ${count(source.transport_errors)}`);
  if (source.next_attempt_at_utc) errors.push(`整個來源最早再排 ${stamp(source.next_attempt_at_utc)}`);
  const pause = source.robots_origin_policy_pause;
  if (pause?.active_origin_count > 0) errors.push(`局部端點暫停 ${count(pause.active_origin_count)} 個 origin；最早重新核對 ${stamp(pause.next_policy_probe_at_utc)}；其他可用端點照常排程，到期不代表恢復存取`);
  else if (source.enabled && source.runtime_capacity_observed && pause == null) errors.push("局部端點暫停尚無有效觀測");
  const blocked = Object.entries(source.states || {}).filter(([key, value]) => value > 0 && !["pending", "complete", "retry", "retry_cooldown", "inflight"].includes(key))
    .map(([key, value]) => `${key === "awaiting_authorization" && source.authorized_api_setup ? "舊網頁管線未啟用" : stateLabels[key] || "其他待查結果"} × ${count(value)}`);
  if (blocked.length) errors.push(blocked.join("、"));
  const reason = source.source_policy_review === "permission_required" && !source.enabled ? "需取得來源同意；不以重試解決權限" : "";
  return [reason, errors.join("；")].filter(Boolean).join("；") || (!source.enabled ? "未啟用，尚未自動呼叫來源" : source.owner_requests ? "本程序未觀測拒絕" : "尚無本程序請求紀錄");
}

function renderSources(data) {
  const all = data.providers || [];
  let rows = all.filter(source => (!state.query || `${source.name} ${source.id}`.toLowerCase().includes(state.query))
    && (state.filter === "all" || (state.filter === "enabled" && source.enabled)
      || (state.filter === "not_enabled" && !source.enabled)
      || (state.filter === "permission" && source.source_policy_review === "permission_required")
      || (state.filter === "attention" && (["cooldown", "attention", "stale", "stopped", "invalid", "waiting_disk", "waiting_restart"].includes(source.state)
        || source.robots_origin_policy_pause?.active_origin_count > 0))));
  if (state.sort === "remaining") rows.sort((a, b) => (b.remaining_urls || 0) - (a.remaining_urls || 0));
  else if (state.sort === "complete") rows.sort((a, b) => (b.complete_urls || 0) - (a.complete_urls || 0));
  else if (state.sort === "name") rows.sort((a, b) => a.name.localeCompare(b.name, "zh-TW"));
  const pages = Math.max(1, Math.ceil(rows.length / PAGE_SIZE));
  state.page = Math.min(state.page, pages - 1);
  const start = state.page * PAGE_SIZE;
  const metadataOnlyCount = all.filter(source => source.original_metadata && source.known_urls === 0).length;
  text("source-results", `${count(rows.length)} 個符合來源／全部 ${count(all.length)} 個${metadataOnlyCount ? `（${count(metadataOnlyCount)} 個僅有原始網址線索）` : ""} · 每頁 ${PAGE_SIZE} 個，沒有截斷清冊`);
  text("source-page", `${state.page + 1} / ${pages}`);
  $("source-previous").disabled = state.page === 0;
  $("source-next").disabled = state.page + 1 >= pages;
  $("pipeline-empty").hidden = rows.length !== 0;
  html("pipeline-grid", rows.slice(start, start + PAGE_SIZE).map(source => {
    const ratio = source.progress_ratio == null ? "未知" : `${decimal(source.progress_ratio * 100)}%`;
    const statesText = Object.entries(source.states || {}).filter(([, value]) => value > 0)
      .map(([key, value]) => `${key === "awaiting_authorization" && source.authorized_api_setup ? "舊網頁管線未啟用" : stateLabels[key] || "其他"} ${count(value)}`).join(" · ");
    const finish = live(data) && source.conditional_finish_at_utc ? `條件完成 ${stamp(source.conditional_finish_at_utc)}` : "完成時點尚未知";
    const badge = ["running", "known_queue_clear", "finite_complete"].includes(source.state) ? "active" : ["cooldown", "attention", "waiting_disk", "waiting_restart"].includes(source.state) ? "waiting" : "";
    const cohort = source.finite_cohort;
    const metadata = source.original_metadata;
    const metadataOnly = metadata && source.known_urls === 0;
    const progressLabel = metadataOnly ? "正文工作尚未建立" : cohort ? "固定批次已存正文／網址" : "已完成狀態／已知網址";
    const progressCount = metadataOnly ? "未入佇列" : cohort ? `${count(cohort.stored_bodies)} / ${count(cohort.urls)}` : `${count(source.complete_urls)} / ${count(source.known_urls)}`;
    const metadataNote = metadata ? `<p class="news-provider-note news-provider-original-metadata">已解析原始媒體線索 <span data-news-original-links>${count(metadata.literal_metadata_links)}</span> 條 · ${count(metadata.original_hosts)} 個主機；可能多條指向同篇，不是正文筆數、不授予抓取或使用權限。</p>` : '';
    const supplement = source.finite_supplements;
    const outcomes = cohort && cohort.reviewed_nonbody_outside_cohort;
    const outcomeNote = outcomes && outcomes.urls ? `；已核對非正文 ${count(outcomes.urls)} 個網址（來源不存在 ${count(outcomes.states.source_not_found || 0)}，非文章路徑 ${count(outcomes.states.unsupported_article_path || 0)}），不計入已存正文` : "";
    const cohortNote = cohort ? `固定批次不重抓${supplement ? `；已核對追加 ${count(supplement.stored_bodies)} / ${count(supplement.urls)} 正文` : ""}${outcomeNote}；未核對新增 ${count(cohort.unreviewed_new_urls ?? cohort.new_urls_outside_cohort)} 個網址（待排 ${count(source.growth_waiting_urls)}）` : "";
    const apiNote = source.authorized_api_setup ? '<p class="news-provider-eta">已確認 API 授權；先設定 CNA_API_KEY 並核對實際工具、全文範圍與配額。目前未開始 API 全文下載，完成時間未知；舊網頁佇列保留，不算完成。</p>' : '';
    const work = source.body_work_progress;
    const forecast = source.completion_forecast;
    const displayedRate = forecast ? forecast.new_url_heads_per_second : source.owner_articles_per_second;
    const forecastLabels = { collecting_recent_samples: "正在累積目前程序的完整視窗，暫不推算完成時間", stale_recent_measurement: "近期量測已過期，暫不沿用舊速率估時", unavailable: "近期量測無法核對，完成時間未知", no_recent_new_heads: "近期完整視窗沒有新增正文，不以零速率推算完成時間" };
    const forecastNote = !forecast ? "" : `<p class="news-provider-note" data-news-forecast="${escape(forecast.state)}">${forecast.state === "recent_measured" ? `估時採近期 ${count(forecast.sample_count)} 個完整視窗，${decimal(forecast.sample_seconds)} 秒新增 ${count(forecast.new_url_heads)} 個網址正文，含失敗、等待與視窗間隔；不挑最快視窗。` : escape(forecastLabels[forecast.state] || "近期量測尚未知")}${forecast.ended_at_utc ? ` 最近視窗結束 ${escape(stamp(forecast.ended_at_utc))}。` : ""} 程序平均 ${decimal(source.owner_articles_per_second == null ? null : source.owner_articles_per_second * 60)}／分（含啟動等待，與估時速率分開）。</p>`;
    const workNote = source.id !== 'cnyes' ? '' : work
      ? `<div class="news-provider-work"><dl class="news-provider-facts"><div><dt>未找到已存主文／待分類</dt><dd data-news-work="missing">${count(work.no_known_primary_body_urls)}</dd></div><div><dt>主文已存，網址版本待驗證</dt><dd data-news-work="candidate">${count(work.existing_primary_body_candidates)}</dd></div><div><dt>本網址正文已存，待重查</dt><dd data-news-work="stored">${count(work.stored_body_recheck_urls)}</dd></div></dl><p class="news-provider-note">以上合計為待回補網址；同 ID 是排程提示，不代表正文相同或已完成。尚無主文優先，網址版本仍定期排入。</p></div>`
      : '<p class="news-provider-note news-provider-work">正文工作分類尚未就緒；不以未知數當成零或完成。</p>';
    return `<article class="pipeline-card"><header class="pipeline-card-header"><div class="news-provider-identity"><strong>${escape(source.name)}</strong><small>${escape(source.id)}</small></div><span class="pipeline-status ${badge}">${escape(labels[source.state] || "待觀測")}</span></header>${apiNote}
      <dl class="news-provider-facts"><div><dt>${progressLabel}</dt><dd>${progressCount}</dd></div><div><dt>待回補</dt><dd>${count(source.remaining_urls)}</dd></div><div><dt>正文 UTF-8</dt><dd>${bytes(source.text_bytes)}</dd></div><div><dt>${metadataOnly ? "新增正文速率尚未量測" : source.authorized_api_setup ? "API 新增正文／分鐘，未量測" : forecast ? "近期新增正文／分鐘" : source.forecast_rate_basis === "new_stored_url_heads" ? "本程序新增正文／分鐘" : "舊快照版本速率／分鐘"}</dt><dd data-news-rate="completion">${decimal(displayedRate == null ? null : displayedRate * 60)}</dd></div></dl>
      ${metadataNote}${workNote}
      <progress class="news-provider-progress mini-progress-track" max="1"${source.progress_ratio == null ? "" : ` value="${source.progress_ratio}"`} aria-label="${escape(source.name)} ${cohort ? '固定批次正文比例' : '已完成狀態占已知網址的比例'}"></progress><div class="news-provider-states">${cohort ? '固定批次' : '已知範圍'} ${ratio} · ${escape(statesText || (metadataOnly ? "已有原始網址線索，尚未建立正文工作" : "尚無已發現網址"))}${cohortNote ? `<p>${escape(cohortNote)}</p>` : ''}</div>
      <p class="news-provider-note">剩餘 ${escape(eta(source, data))} · ${escape(finish)}</p>${forecastNote}<p class="news-provider-note">${escape(reviewLabels[source.source_policy_review] || "來源審閱紀錄不可用")}${source.source_reviewed_at_utc ? ` · 核對 ${escape(stamp(source.source_reviewed_at_utc))}` : ""}</p><p class="news-provider-note">${source.authorized_api_setup ? "官方 API 能力檢查已實作，全文管線未啟用" : source.adapter_registered ? "共用下載器已接線" : "來源尚未註冊"} · ${source.adapter_acceptance === "committed_body_heads_observed" ? "已觀測正文入庫" : "尚無正文入庫證據"}${source.source_role === "redirector" ? " · 轉址來源，須找到原始媒體 URL" : ""}</p><p class="news-provider-note">${escape(errorLabel(source))}</p></article>`;
  }).join(""));
}
function renderTraffic(data) {
  const workloadLabels = { cnyes_bulk: "批次正文", wordpress_posts: "批次正文", rss_fulltext: "RSS 正文", article_pdf: "PDF", article_html: "單篇 HTML", mixed: "混合工作，不能當成同工作量測速", metadata: "目錄／索引", unclassified: "舊版未分類量測" };
  html("traffic-rows", (data.providers || []).filter(source => source.configured).map(source => `<tr><td>${escape(source.name)}<small>${escape(labels[source.state] || "待觀測")}${source.registered_bulk_endpoints ? `；${count(source.registered_bulk_endpoints)} 個分類／單次最多 ${count(source.observed_bulk_rows_per_request_cap)} 則` : ""}</small></td><td>${count(source.owner_requests)} 次／${bytes(source.owner_response_bytes)}</td><td>${decimal(source.client_min_interval_seconds)} 秒／${count(source.parallelism)} 並行<small>${escape(capacityLabels[source.capacity_state] || "等待執行中容量觀測")}；官方總配額未知</small></td><td>${decimal(source.minimum_observed_request_gap_seconds)} 秒<small>最近 ${decimal(source.last_window_seconds)} 秒：${decimal(source.last_window_requests_per_second)} 請求／秒、${decimal(source.last_window_new_heads_per_second)} 新正文／秒；${escape(workloadLabels[source.capacity_workload_family] || "工作分類待量測")}</small></td><td>${escape(errorLabel(source))}</td></tr>`).join(""));
}
function renderStages(data) {
  html("stage-rows", (data.stages || []).map(stage => `<tr><td>${escape(stages[stage.id] || "已量測階段")}</td><td>${count(stage.count)}</td><td>${decimal(stage.wall_seconds)}</td><td>${decimal(stage.cpu_seconds)}</td></tr>`).join("")
    || '<tr><td colspan="4">尚無此程序的階段量測</td></tr>');
}

function renderOriginalURLs(data) {
  const resolution = data.original_url_resolution;
  $("original-url-panel").hidden = !resolution;
  if (!resolution) return;
  const ratio = resolution.progress_ratio;
  text("original-url-summary", `${count(resolution.examined_urls)} / ${count(resolution.known_redirect_urls)}${ratio == null ? "" : ` · ${decimal(ratio * 100)}%`}`);
  text("original-url-count", `${count(resolution.examined_urls)} / ${count(resolution.known_redirect_urls)} 筆連結`);
  text("original-url-detail", resolution.snapshot_consistent
    ? `明示原站 ${count(resolution.literal_original_links)} · 不透明 token ${count(resolution.opaque_links)} · 其他未解 ${count(resolution.unresolved_links)} · 指向已入列目標的轉址 ${count(resolution.links_to_queued_targets)}（非唯一原文篇數）`
    : "轉址快照數量不一致，暫不提供比例或估時。正文進度另列。");
  text("original-url-eta", live(data) && resolution.snapshot_consistent ? duration(resolution.conditional_eta_seconds) : "目前暫無有效估時");
  progress("original-url-progress", ratio);
}

function render(data) {
  if (data.schema_version !== 1 || !Array.isArray(data.providers) || !data.acquisition || !data.worker) throw new Error("invalid news status contract");
  state.latest = data;
  const acquisition = data.acquisition;
  const badge = $("news-health");
  badge.className = `status ${data.health === "running" ? "updating" : data.health}`;
  badge.lastChild.textContent = labels[data.health] || "狀態待確認";
  text("news-freshness", `來源快照 ${stamp(data.observed_at_utc)} · 每 15 秒刷新唯讀快照`);
  text("body-count", count(acquisition.stored_body_articles));
  text("body-bytes", `正文 UTF-8 ${bytes(acquisition.text_bytes)} · 非壓縮容量`);
  text("known-count", count(acquisition.known_urls));
  text("remaining-count", count(acquisition.remaining_urls));
  text("remaining-note", `處理中 ${count(acquisition.inflight_urls)} · 待重試 ${count(acquisition.retry_urls)}`);
  text("restricted-count", count(acquisition.awaiting_authorization_urls));
  text("download-progress-label", `${count(acquisition.stored_body_articles)} / ${count(acquisition.known_urls)}${acquisition.progress_ratio == null ? "" : ` · ${decimal(acquisition.progress_ratio * 100)}%`}`);
  text("download-progress-detail", "正文／已清點唯一網址；探索仍在增加分母，未啟用或失敗的網址不算完成。");
  progress("download-progress", acquisition.progress_ratio);
  text("global-eta", "未知");
  text("global-eta-basis", "未啟用／被拒絕來源與未知歷史仍存在；不是成功倒數。");
  text("eligible-eta", live(data) ? duration(acquisition.eligible_eta_seconds) : "目前階段暫無有效估時");
  text("eligible-eta-basis", acquisition.eligible_eta_seconds == null ? "有來源缺有效落盤速率或正在冷卻，請看逐來源條件估時。" : "各來源並行，採最長條件工時；未含新探索或阻塞來源。");
  text("metadata-count", `${count(acquisition.metadata_partitions)} 個來源分區`);
  text("discovery-count", `到期清點 ${count(acquisition.discovery_due)} 個 · 清點錯誤 ${count(acquisition.discovery_errors)} 個 · 端點暫停 ${count(acquisition.temporarily_blocked_bulk_pages)} 頁`);
  text("retry-count", `${count(acquisition.retry_urls)} 個待重試`);
  text("error-count", `未啟用／其他不可執行 ${count(acquisition.blocked_urls)} · metadata 失敗 ${count(acquisition.metadata_errors)}`);
  text("request-count", count(data.worker.requests));
  text("response-bytes", bytes(data.worker.response_bytes));
  const measured = data.providers.filter(source => source.enabled && source.owner_articles_per_second != null);
  const exactHeads = measured.every(source => source.forecast_rate_basis === "new_stored_url_heads");
  text("capture-rate", live(data) && measured.length ? `${decimal(measured.reduce((sum, source) => sum + source.owner_articles_per_second * 60, 0))} ${exactHeads ? "新增正文" : "舊快照版本代理"}／分` : "等待有效量測");
  text("capture-cpu", data.worker.owner_elapsed_seconds > 0 && data.worker.owner_cpu_seconds != null ? `${decimal(data.worker.owner_cpu_seconds / data.worker.owner_elapsed_seconds * 100)}% 單核心` : "—");
  text("capture-elapsed", `程序經過 ${duration(data.worker.owner_elapsed_seconds)} · 不使用 GPU`);
  text("source-count", `完整保留 ${count(data.providers.length)} 個已配置／已發現來源；狀態、待辦與條件估時分開。`);
  const reviews = data.source_policy_review_counts || {};
  text("source-review-summary", data.source_reviews_state === "loaded"
    ? `需取得來源同意 ${count(reviews.permission_required || 0)} 個 · 尚未審閱 ${count(reviews.not_reviewed || 0)} 個 · 審閱未完成 ${count(reviews.review_incomplete || 0)} 個。這是來源審閱，不是抓取失敗或下載授權。`
    : "來源審閱紀錄暫無法讀取；既有下載狀態與數量仍保留，不因此啟用來源。");
  text("capture-next", live(data) ? "到期 feed 優先；就緒正文立即續抓，按來源限流／退避自動續排。" : `${labels[data.health] || "狀態待確認"}；不從瀏覽器啟動／重啟下載器。`);
  text("association-count", `${count(acquisition.metadata_association_rows)} 筆（不是唯一文章數）`);
  text("disabled-bulk", count(acquisition.disabled_bulk_pages));
  text("updated-at", `面板 ${stamp(data.generated_at_utc)} · 來源 ${stamp(data.observed_at_utc)}`);
  const blocked = data.providers.filter(source => source.enabled && (["cooldown", "attention"].includes(source.state)
    || source.robots_origin_policy_pause?.active_origin_count > 0));
  const stale = !live(data);
  $("pipeline-alert").hidden = !stale && blocked.length === 0;
  text("alert-title", stale ? labels[data.health] || "資料不可用" : `${count(blocked.length)} 個已啟用來源需要注意`);
  text("alert-copy", stale ? "保留既有數量；停止、排空、等待磁碟／交接或過期時不提供有效完成估時。" : blocked.map(source => `${source.name}：${errorLabel(source)}`).join("；"));
  renderSources(data); renderTraffic(data); renderStages(data); renderOriginalURLs(data);
}

async function refresh() {
  if (document.hidden || state.inFlight) return;
  state.inFlight = true;
  try {
    render(await fetchJson("api/status"));
    if (state.retry) clearTimeout(state.retry);
    state.retry = null; state.retryDelay = 3000;
  } catch (_error) {
    const badge = $("news-health"); badge.className = `status ${state.latest ? "stale" : "unavailable"}`;
    badge.lastChild.textContent = state.latest ? "更新暫停，顯示上次資料" : "面板暫時無法讀取";
    text("news-freshness", "稍後自動重試；不呼叫新聞來源補畫面。");
    if (state.latest) {
      state.latest = {...state.latest, health: "stale"};
      text("eligible-eta", "更新失敗，暫無估時"); renderSources(state.latest); renderOriginalURLs(state.latest);
    }
    $("pipeline-alert").hidden = false;
    text("alert-title", "快照更新失敗"); text("alert-copy", "保留最後成功資料與篩選；估時暫停，不把舊資料當成最新。");
    if (!state.retry) {
      state.retry = setTimeout(() => {state.retry = null; refresh();}, state.retryDelay);
      state.retryDelay = Math.min(30000, state.retryDelay * 2);
    }
  } finally { state.inFlight = false; }
}

document.querySelectorAll("[data-news-filter]").forEach(button => button.addEventListener("click", () => {
  state.filter = button.dataset.newsFilter; state.page = 0;
  document.querySelectorAll("[data-news-filter]").forEach(other => {const active = other === button; other.classList.toggle("active", active); other.setAttribute("aria-pressed", String(active));});
  if (state.latest) renderSources(state.latest);
}));
$("source-search").addEventListener("input", event => {state.query = event.target.value.trim().toLowerCase(); state.page = 0; if (state.latest) renderSources(state.latest);});
$("source-sort").addEventListener("change", event => {state.sort = event.target.value; state.page = 0; if (state.latest) renderSources(state.latest);});
$("source-previous").addEventListener("click", () => {state.page = Math.max(0, state.page - 1); if (state.latest) renderSources(state.latest);});
$("source-next").addEventListener("click", () => {state.page += 1; if (state.latest) renderSources(state.latest);});
renderSources = Dashboard.createDeferredRenderer("pipelines", renderSources);
renderTraffic = Dashboard.createDeferredRenderer("traffic", renderTraffic);
renderStages = Dashboard.createDeferredRenderer("capture", renderStages);
Dashboard.scheduleRefresh(refresh, {intervalMs: 15000});

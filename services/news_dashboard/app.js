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
  running: "正在抓取", backfilling: "等待回補", rechecking: "正文已存，等待重查", inventorying: "清點新網址", idle: "等待到期工作", waiting: "等待來源",
  cooldown: "等待下次請求", known_queue_clear: "已知待辦已清", attention: "需注意", not_enabled: "未啟用／待確認",
  finite_complete: "固定批次正文已存", waiting_cohort: "新文章等待納入批次",
  authorized_api_pending: "授權 API 待接線",
  metadata_polling: "RSS 資訊追新，非全文",
  stopped: "下載器已停止", stale: "觀測已過期", unavailable: "尚無有效快照", invalid: "快照版本不相容", degraded: "快照數量不一致",
  draining: "正在排空保存", waiting_disk: "等待磁碟空間", waiting_restart: "設定交接，等待續跑",
  waiting_local_database: "等待本機資料庫寫入鎖", waiting_local_database_stale: "本機鎖定觀測已過期",
  waiting_local_snapshot: "本機重試已結束，等待新快照",
  initializing: "啟動中，恢復本機資料", initializing_stale: "啟動中，進度觀測已過期",
});
const ownerFailurePhases = Object.freeze({queue_initialization: "初始化佇列", acceptance_recovery: "收據恢復",
  policy_reconciliation: "來源規則核對", awaiting_first_snapshot: "等待首份快照", snapshot_published: "運行",
  waiting_local_sqlite: "等待本機資料庫", local_sqlite_retry_finished: "本機資料庫重試", unknown: "階段未知"});
const ownerFailureReasons = Object.freeze({unknown: "原因未知", interrupted: "程序中斷", local_io: "本機 I/O 錯誤",
  io_error: "I/O 錯誤", missing_file: "檔案不存在", permission: "權限錯誤", invalid_json: "JSON 格式錯誤",
  validation: "資料或設定驗證錯誤", local_database: "本機資料庫錯誤", unexpected: "其他錯誤"});

function ownerFailureSummary(lifecycle) {
  const failure = lifecycle?.last_failure;
  if (!(lifecycle?.snapshot_owner_matches || lifecycle?.supersedes_snapshot)
      || !failure || failure.contract_version !== 1 || typeof failure.current_owner !== "boolean"
      || !Object.prototype.hasOwnProperty.call(ownerFailurePhases, failure.phase)
      || !Object.prototype.hasOwnProperty.call(ownerFailureReasons, failure.reason)) return "";
  const receipts = failure.phase === "acceptance_recovery" && Number.isSafeInteger(failure.receipts_decoded)
    && failure.receipts_decoded >= 0 ? `，上次記錄已檢查 ${count(failure.receipts_decoded)} 份收據` : "";
  return ` · ${failure.current_owner ? "本次失敗" : "上次失敗"} ${stamp(failure.observed_at_utc)}：${ownerFailurePhases[failure.phase]}／${ownerFailureReasons[failure.reason]}${receipts}`;
}
const discoveryKinds = Object.freeze({feed: "Feed", metadata_feed: "資訊 RSS", index: "索引", sitemap: "網站地圖",
  rss_fulltext: "全文 RSS", wordpress_posts: "WordPress 批次", cnyes_news: "鉅亨批次", other: "其他清點"});
const discoveryReasons = Object.freeze({http_401: "HTTP 401", http_403: "HTTP 403", http_404: "HTTP 404",
  http_410: "HTTP 410", http_429: "HTTP 429", http_451: "HTTP 451", http_other: "其他 HTTP 回應",
  transport: "連線錯誤", unregistered_endpoint: "端點已不在設定內", invalid_feed: "Feed 格式無法解析",
  invalid_feed_body: "Feed 正文格式異常", invalid_feed_identity: "Feed 文章身分無法確認",
  restricted_content: "來源內容受限", unusable_items: "批次項目不可用",
  pagination_no_progress: "分頁沒有前進", pagination_invalid: "分頁數量無法驗證", other: "其他待查"});
const stateLabels = Object.freeze({
  complete: "已完成狀態", pending: "待回補", inflight: "處理中", retry: "待重試", retry_cooldown: "重試冷卻",
  empty_http_response: "來源回傳零位元，等待有界重試",
  awaiting_authorization: "未啟用／待確認", unsupported_article_path: "網址格式未支援", body_not_found: "缺正文",
  redirect_not_article: "轉址未到文章，原網址待查",
  waiting_cohort: "新文章待納入，非抓取失敗",
  article_attribution_not_verified: "作者未驗證", article_title_not_verified: "標題未驗證", article_license_not_verified: "授權標記未驗證", robots_disallowed: "robots 限制",
  article_identity_not_verified: "文章身分待驗證", article_rights_not_verified: "原創／來源署名待驗證", article_rights_or_scope_excluded: "原創／作者權利未確認或超出範圍",
  invalid_bulk_publication_clock: "發布／異動時間未驗證",
  paywall_or_login: "付費／登入內容", access_challenge: "來源存取驗證", robots_access_challenge: "robots 存取驗證挑戰", robots_not_plain_rules: "robots 回應身分／規則待驗證", robots_redirect_refused: "robots 轉址待修",
  source_body_removed: "來源已主動撤下正文", source_not_found: "來源網址已不存在", source_body_image_only: "來源正文僅圖片，未做 OCR", source_body_embedded_only: "來源正文僅外部嵌入，未取得文字", source_article_empty_template: "來源目前回空白文章模板，未取得正文", pdf_image_only_or_body_not_found: "影像 PDF 待文字辨識",
  source_body_quote_only: "原站只提供行情片段，未取得新聞正文",
  source_article_test_only: "來源測試帳號內容，非新聞正文",
  source_document_landing_page: "文件索引頁，非新聞正文",
  source_not_allowed: "來源未允許", authorization_expired: "授權到期", paywall: "付費牆", not_found: "來源不存在",
  gone: "來源已移除", response_too_large: "回應過大", other: "其他待查原因",
});
const stages = Object.freeze({owner_initialization: "Owner 初始化", policy_reconciliation: "來源規則核對", metadata_inventory: "本機 metadata 清點",
  original_url_resolution: "既有轉址解碼／續跑", original_url_routing: "原站權限變更後續排",
  article_dispatch: "佇列派工", discovery_registration: "新網址註冊", batch_build: "批次建構", parquet_compress_fsync: "Parquet 壓縮／fsync",
  receipt_hash_fsync: "收據雜湊／fsync", acceptance_sqlite_commit: "耐久佇列提交", status_write: "快照寫入", idle_wait: "無工作等待",
  address_validation: "公開位址檢查", request_slot_wait: "限流等待", transport: "網路回應", article_extraction: "正文解析", discovery_extraction: "Feed／sitemap 解析"});
const capacityLabels = Object.freeze({warming: "累積樣本", ramping: "階梯升速", verifying: "退回後穩定驗證",
  metadata_polling: "RSS 更新排程，不進行額外測速請求",
  recovering_transport: "暫時故障恢復驗證，非限速", endpoint_validation_failed: "端點／回應結構待修，非限速",
  waiting_source: "遵守來源暫停", insufficient_samples: "請求樣本不足", insufficient_useful_data: "無新增正文，不升速",
  official_or_learned_floor: "遵守來源／robots 間隔", stable_source_backoff: "限速後穩定設定",
  stable_pipeline_capacity: "管線穩定設定，非來源極限", engineering_ceiling_unverified: "客戶端資源邊界，來源極限未明"});
const reviewLabels = Object.freeze({permission_required: "需取得來源同意", review_incomplete: "來源審閱尚未完成",
  not_reviewed: "尚未審閱", private_public_candidate: "私人研究候選範圍，未自動啟用",
  reviewed_existing_scope: "已核對現有範圍", active_existing_policy: "依現有抓取規則",
  authorized_api_configuration_required: "授權 API 待設定", review_unverified: "審閱時間待驗證"});
const followupLabels = Object.freeze({open: "有已記錄的後續問題",
  none_recorded: "目前紀錄未列後續問題，並非正文或歷史已完成", unknown: "後續問題紀錄未知"});
const outcomeLabels = Object.freeze({complete_state: "正文完成狀態", queued_or_retry: "可排程／重試",
  registration_required: "尚未接線", scope_or_access_waiting: "範圍／存取待核實", waiting_cohort: "新增待納入批次",
  repair_required: "解析／時間／端點待修", nontext_representation: "圖片／嵌入／PDF 待文字驗證",
  nonarticle_response: "行情／測試／文件索引", source_currently_empty: "目前空模板",
  origin_absent: "原站撤下／不存在", unknown_outcome: "其他待查"});
const state = {latest: null, filter: "all", query: "", sort: "state", page: 0, inFlight: false, retry: null, retryDelay: 3000, localWaitTimer: null, cnyesListOpen: false, outcomeOpen: new Set(), feedOpen: new Set(), discoveryErrorOpen: new Set()};
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
function currentSnapshot(data) {
  const at = Date.parse(data?.observed_at_utc);
  const age = Date.now() - at;
  return Number.isFinite(at) && age >= -5000 && age <= 180000
    && !["stale", "unavailable", "invalid", "degraded", "initializing", "initializing_stale",
      "waiting_local_database", "waiting_local_database_stale", "waiting_local_snapshot"].includes(data?.health);
}
function live(data) {
  return currentSnapshot(data) && ["running", "idle", "waiting", "inventorying"].includes(data.health);
}
function displaySources(data) {
  const sources = data.providers || [];
  if (!["stopped", "waiting_restart"].includes(data.health)) return sources;
  // Older API revisions can report a confirmed stopped owner globally but
  // mark its expired source snapshot stale. Do not alter the saved snapshot.
  return sources.map(source => source.enabled && source.state === "stale"
    ? {...source, state: data.health} : source);
}
function eta(source, data) {
  if (live(data) && source.conditional_eta_unavailable_reason === "source_request_scope_paused") return "等待來源重新核對，完成時間未知";
  if (live(data) && source.conditional_eta_unavailable_reason === "source_request_scope_unknown") return "缺文端點狀態尚未核對，完成時間未知";
  return live(data) ? duration(source.conditional_eta_seconds) : "目前階段暫無有效估時";
}
function progress(id, ratio) {
  const bar = $(id);
  if (ratio == null) bar.removeAttribute("value");
  else bar.value = Math.max(0, Math.min(1, ratio));
}

function outcomeRows(disposition, coverage, provider = null) {
  return Object.entries(disposition.categories || {}).filter(([, value]) => value > 0).map(([key, value]) => {
    const attribute = provider == null ? 'data-news-outcome-total' : 'data-news-outcome';
    const suffix = provider == null ? '-total' : '';
    return `<tr><td>${escape(outcomeLabels[key] || "其他待查")}</td><td ${attribute}="${escape(key)}">${count(value)}</td><td data-news-missing${suffix}="${escape(key)}">${count(coverage?.missing_body_outcomes?.categories?.[key])}</td><td data-news-retained${suffix}="${escape(key)}">${count(coverage?.retained_body_outcomes?.categories?.[key])}</td></tr>`;
  }).join('');
}
function errorLabel(source) {
  const errors = Object.entries(source.http_errors || {}).map(([code, value]) => `HTTP ${code} × ${count(value)}`);
  if (source.transport_errors) errors.push(`連線錯誤 × ${count(source.transport_errors)}`);
  if (source.owner_local_sqlite_busy_commit_events > 0) errors.push(`${live(state.latest) ? "本程序" : "上次快照"}：本機批次提交曾遇資料庫鎖定 ${count(source.owner_local_sqlite_busy_commit_events)} 次（涉及此來源，非來源限流）`);
  if (source.discovery_errors?.active_errors) errors.push(`上次清點異常 ${count(source.discovery_errors.active_errors)} 頁`);
  if (source.next_attempt_at_utc) errors.push(live(state.latest)
    ? `整個來源最早再排 ${stamp(source.next_attempt_at_utc)}`
    : `上次記錄來源冷卻最早截止 ${stamp(source.next_attempt_at_utc)}；不表示到時已執行`);
  const pause = source.robots_origin_policy_pause;
  if (pause?.active_origin_count > 0) errors.push(live(state.latest)
    ? `局部端點暫停 ${count(pause.active_origin_count)} 個 origin；最早重新核對 ${stamp(pause.next_policy_probe_at_utc)}；其他可用端點照常排程，到期不代表恢復存取`
    : `上次記錄局部端點暫停 ${count(pause.active_origin_count)} 個 origin；最早重新核對 ${stamp(pause.next_policy_probe_at_utc)}；目前狀態待確認`);
  else if (source.enabled && source.runtime_capacity_observed && pause == null) errors.push("局部端點暫停尚無有效觀測");
  const blocked = Object.entries(source.states || {}).filter(([key, value]) => value > 0 && !["pending", "complete", "retry", "retry_cooldown", "inflight"].includes(key))
    .map(([key, value]) => `${key === "awaiting_authorization" && source.authorized_api_setup ? "舊網頁管線未啟用" : stateLabels[key] || "其他待查結果"} × ${count(value)}`);
  if (blocked.length) errors.push(blocked.join("、"));
  const reason = source.source_policy_review === "permission_required" && !source.enabled ? "需取得來源同意；不以重試解決權限" : "";
  return [reason, errors.join("；")].filter(Boolean).join("；") || (!source.enabled ? "未啟用，尚未自動呼叫來源" : source.owner_requests ? "本程序未觀測拒絕" : "尚無本程序請求紀錄");
}

function discoveryErrorDetails(source, data) {
  const errors = source.discovery_errors;
  if (!errors) {
    const building = data.discovery_error_rollup?.state === 'building';
    return `<p class="news-provider-note" data-news-discovery-unknown>${building ? '逐來源清點異常摘要建立中' : '逐來源清點異常摘要尚無有效快照'}；未知不表示零。</p>`;
  }
  if (!errors.active_errors) return '<p class="news-provider-note" data-news-discovery-zero>上次快照：佇列清點異常 0 頁；不代表全站歷史已齊。</p>';
  const parts = (values, names) => Object.entries(values).sort((a, b) => b[1] - a[1])
    .map(([key, value]) => `${escape(names[key] || '其他')} ${count(value)} 頁`).join('、');
  const restrictedNote = errors.reasons.restricted_content
    ? '<p class="news-provider-note">含受限制文字的批次可能已登記允許的網址及後頁；須看各頁證據，不能只靠此原因判斷卡住。</p>' : '';
  return `<details class="news-provider-list" data-news-discovery-provider="${escape(source.id)}"${state.discoveryErrorOpen.has(source.id) ? ' open' : ''}><summary>上次快照：清點異常 ${count(errors.active_errors)} 頁</summary><p class="news-provider-note">類型：${parts(errors.kinds, discoveryKinds)}。</p><p class="news-provider-note">最後記錄原因：${parts(errors.reasons, discoveryReasons)}。</p>${restrictedNote}<p class="news-provider-note">這是佇列頁面的最後觀測，可能包含已移除端點；不代表整個來源停擺或文章篇數。實際重試仍依原排程與來源限制。</p></details>`;
}

function cnyesManifestPhase(phase) {
  if (!phase) return "";
  const firstRatio = phase.first_pages_observed / phase.planned_source_dates;
  const pageLabel = phase.totals_known
    ? `${count(phase.verified_pages)} / ${count(phase.source_reported_total_pages)} 頁（${decimal(phase.progress_ratio * 100)}%）`
    : `${count(phase.verified_pages)} 頁；總頁數未知`;
  const pageProgress = phase.totals_known
    ? `<progress class="news-provider-progress" aria-label="已知超限日期清單頁進度，非全文" max="1" value="${phase.progress_ratio}" data-news-manifest-progress="pages"></progress>` : "";
  // A browser transport loss does not mean the collector stopped. Preserve
  // verified counts, but withdraw forecasts until a fresh snapshot arrives.
  // Other providers' attention state must not suppress this provider's proof.
  const forecast = currentSnapshot(state.latest) ? phase.forecast : {state: "snapshot_stale"};
  let eta = "階段估時尚未取得有效觀測";
  if (forecast?.state === "recent_measured") {
    const slow = forecast.slowest_seconds == null ? "含零進展視窗，無有限較慢估時" : duration(forecast.slowest_seconds);
    eta = `近 ${count(forecast.sample_seconds / 60)} 分鐘實際保存 ${count(forecast.verified_pages)} 頁；情境最短 ${duration(forecast.fastest_seconds)}、基準 ${duration(forecast.middle_seconds)}、較慢 ${slow}。基準預計 ${stamp(forecast.middle_finish_at_utc)}；含等待與失敗，不是保證或統計信賴區間。`;
  } else if (forecast?.state === "snapshot_stale") eta = "畫面更新暫停，保留已驗證清單計數；暫不提供本階段估時。這不代表下載器停止。";
  else if (forecast?.state === "phase_complete") eta = "此階段來源回報清單頁與資料列已齊；正文仍需另外驗證";
  else if (forecast?.state === "no_recent_progress") eta = "近期沒有新增清單頁；暫停估時，不持續倒數";
  else if (forecast?.state === "provider_waiting") {
    const next = phase.source_gate?.state === "waiting" && phase.source_gate.next_probe_at_utc
      ? `清單端點最早 ${stamp(phase.source_gate.next_probe_at_utc)} 再檢查；屆時仍需驗證回應。` : "";
    eta = `此來源目前排程等待中（含清單端點、流量間隔或來源冷卻）；保留已驗證清單計數，暫不預估完成時間。${next}`;
  }
  else if (forecast?.state === "source_rechecking") eta = "清單來源等待期限已到，仍須成功驗證回應才確認恢復；暫不預估完成時間。";
  else if (forecast?.state === "source_scope_unknown") eta = "無法確認清單端點、主機或 robots 的完整狀態；暫不預估完成時間。";
  else if (forecast?.state === "last_page_waiting") {
    const next = phase.last_page_gate?.next_probe_at_utc ? `最早 ${stamp(phase.last_page_gate.next_probe_at_utc)} 再檢查；` : "";
    eta = `最後一頁目前等待重試，${next}成功驗證前暫不預估本階段完成時間。其他頁面的排程不受此狀態影響。`;
  }
  else if (forecast?.state === "last_page_rechecking") eta = "最後一頁已可重新檢查，仍須成功驗證回應；暫不預估本階段完成時間。";
  else if (forecast?.state === "last_page_scope_unknown") eta = "無法確認最後一頁的工作狀態；保留已驗證頁數，暫不預估本階段完成時間。";
  else if (forecast?.state === "partial_page_waiting") {
    const next = phase.partial_page_gate?.next_probe_at_utc ? `已知重試期限最晚 ${stamp(phase.partial_page_gate.next_probe_at_utc)}；` : "";
    eta = `有 ${count(phase.partial_page_gate?.failed_pages)} 頁尚未通過來源驗證，${next}成功前暫不預估本階段完成時間。其他頁可繼續排程。`;
  }
  else if (forecast?.state === "partial_page_rechecking") eta = "部分清單頁已可重新檢查，仍須成功驗證回應；其他頁可繼續排程，暫不預估本階段完成時間。";
  else if (forecast?.state === "partial_page_scope_unknown") eta = "無法確認部分清單頁的重試證據；保留已驗證頁數，暫不預估本階段完成時間。";
  else if (forecast?.state === "inactive") eta = "程序停止、暫停或快照過期；暫停估時";
  else if (!phase.totals_known) eta = "首頁尚未齊、來源總數異動或分頁內容核對中；不外推總量或完成時間";
  else if (forecast?.state === "warming") eta = "等待至少 3 個完整分鐘視窗與 10 頁有效觀測，再估此階段時間";
  return `<section class="news-manifest-phase" aria-label="鉅亨已知超限日期清單階段" data-news-manifest-display-contract="2" data-news-manifest-state="${forecast?.state || "unavailable"}"><p>階段 1 · 日期首頁：<span data-news-manifest-count="first_pages">${count(phase.first_pages_observed)}</span> / <span data-news-manifest-count="planned_dates">${count(phase.planned_source_dates)}</span> 日</p><progress class="news-provider-progress" aria-label="已知超限日期首頁盤點進度" max="1" value="${firstRatio}" data-news-manifest-progress="first_pages"></progress><p>階段 2 · 清單分頁：${pageLabel}</p>${pageProgress}<p class="news-provider-note">來源回報 ${count(phase.source_reported_total_metadata_rows)} 筆清單資料列，已觀測 ${count(phase.observed_metadata_rows)} 筆；只限已知超限日期，不是全庫或唯一新聞篇數。</p><p class="news-provider-note" data-news-manifest-eta>${eta}</p></section>`;
}

function cnyesListProgress(source, data) {
  if (source.id !== "cnyes") return "";
  const index = source.public_index_progress;
  const history = source.history_list_progress;
  const indexState = index ? "observed" : "unavailable";
  const summary = index ? `清單補漏：已驗證 ${count(index.verified_pages)} 頁 · ${count(index.metadata_rows)} 筆觀測` : "清單補漏：進度尚無有效觀測";
  const indexFacts = index
    ? `<dl class="news-provider-facts"><div><dt>公開清單已驗證頁數</dt><dd data-news-index-count="pages">${count(index.verified_pages)}</dd></div><div><dt>清單觀測資料列，非全文</dt><dd data-news-index-count="rows">${count(index.metadata_rows)}</dd></div><div><dt>已觀測發布日</dt><dd data-news-index-count="dates">${count(index.source_dates_observed)}</dd></div><div><dt>當下回報頁數已齊的發布日</dt><dd data-news-index-count="complete_dates">${count(index.dates_all_reported_pages_observed)}</dd></div><div><dt>正在核對分頁變動的發布日</dt><dd data-news-index-count="revision_dates">${count(index.dates_reconciling_page_membership)}</dd></div><div><dt>觀測到來源總數異動的發布日</dt><dd data-news-index-count="changed_dates">${count(index.dates_with_changed_source_total)}</dd></div></dl><p class="news-provider-note">發布日依來源清單的 UTC+8 日界。頁數已齊只限已觀測日期，不是全部歷史；觀測列可能重複收錄同篇，不能當唯一文章數。清單只註冊網址，接受正文 0 篇；全文仍須由原佇列取得並驗證。</p>`
    : '<p class="news-provider-note">舊快照、未啟用或數量無法核對時，清單進度顯示未知；不補零、不算完成。</p>';
  const indexRecovery = index?.local_recovery;
  const indexRecoveryFacts = index ? `<p class="news-provider-note" data-news-index-checkpoint>${live(data) ? '' : '上次快照：'}公開清單本機原件恢復：${indexRecovery == null ? '狀態未知' : indexRecovery.pending_originals === 0 ? '沒有已登記的待恢復原件' : `${count(indexRecovery.pending_originals)} 個原件待核驗，${indexRecovery.next_retry_ready ? '已到恢復時間，等待下載器處理' : `下次本機重試 ${stamp(indexRecovery.next_retry_at_utc)}`}`}。此數字不是缺少的文章數，也不代表歷史全文已完成。</p>` : '';
  const scopes = history?.current_source_contract_scope_states;
  const recovery = history?.overflow_retained_recovery;
  const recoveryLabel = {not_started: "尚未開始", scanning: "盤點中", complete: "盤點結束", local_retry_wait: "等待本機重試", unknown: "狀態未知"};
  const localCheckpointProblems = new Map([
    ['invalid_checkpoint', '盤點游標異常，重新盤點尚未完成。'],
    ['unsupported_version', '盤點游標版本不支援，原資料保留，該盤點暫停。'],
    ['audit_conflict', '盤點游標留存紀錄衝突，原資料保留，該盤點暫停。'],
  ]);
  const legacy = history?.legacy_metadata_scans;
  const legacyFacts = legacy
    ? `<p class="news-provider-note" data-news-legacy-local-scans>${live(data) ? '' : '上次快照：'}舊資料本機整理：${[['mismatch', '原件不符'], ['unavailable', '原件缺失'], ['empty', '空清單']].map(([key, label]) => `${label} ${recoveryLabel[legacy[key]?.scan_state] || '狀態未知'}${localCheckpointProblems.get(legacy[key]?.checkpoint_problem) || ''}`).join('；')}。這些是本機整理狀態，並不代表來源歷史或全文已抓齊。</p>`
    : history ? '<p class="news-provider-note" data-news-legacy-local-scans>舊資料本機整理：快照未提供，狀態未知。</p>' : '';
  const recoveryProblem = localCheckpointProblems.get(recovery?.checkpoint_problem) || '';
  const recoveryFacts = recovery
    ? `<p class="news-provider-note" data-news-overflow-recovery>${live(data) ? '' : '上次快照：'}分類舊原件本機盤點：${recoveryLabel[recovery.scan_state] || "狀態未知"}；已排入待核驗 ${count(recovery.pending_retained_pages)} 頁。${recoveryProblem}歷史清單原件待本機重試：${recovery.local_retry_pages == null ? "未知" : count(recovery.local_retry_pages)} 頁（其中可能包含前述待核驗頁）。只讀既存原件，不增加來源請求；未找到的頁面及分類缺口仍未完成。</p>`
    : "";
  const nonzero = history?.unobserved_nonzero_retained_scan;
  const nonzeroProblem = localCheckpointProblems.get(nonzero?.checkpoint_problem) || '';
  const nonzeroFacts = nonzero
    ? `<p class="news-provider-note" data-news-nonzero-local-recovery>${live(data) ? '' : '上次快照：'}非零歷史原件本機盤點：${recoveryLabel[nonzero.scan_state] || "狀態未知"}；本輪已檢視 ${count(nonzero.checked_steps)} 個區間或頁面步驟，發現 ${count(nonzero.queued_candidate_pages)} 頁待核驗候選。${nonzeroProblem}候選不是驗證完成，未發現原件的頁面仍需處理；總篇數與完成時間未知。</p>`
    : "";
  const accepted = history?.accepted_body_batch_scan;
  const acceptedProblem = localCheckpointProblems.get(accepted?.checkpoint_problem) || '';
  const invalidMigrationLabel = {not_started: "尚未開始", scanning: "補登中", complete: "補登結束", unknown: "狀態未知"};
  const frontierKnown = accepted && Number.isSafeInteger(accepted.scanned_batches)
    && Number.isSafeInteger(accepted.remaining_frontier_batches)
    && Number.isSafeInteger(accepted.estimated_frontier_batches)
    && accepted.scanned_batches >= 0 && accepted.remaining_frontier_batches >= 0
    && accepted.scanned_batches + accepted.remaining_frontier_batches === accepted.estimated_frontier_batches;
  const frontierFacts = accepted ? `<div class="news-provider-work" data-news-accepted-frontier>
    <p>${live(data) ? '' : '上次快照：'}已登記本機批次盤點：${frontierKnown ? `${count(accepted.scanned_batches)} / ${count(accepted.estimated_frontier_batches)} 已嘗試，剩餘 ${count(accepted.remaining_frontier_batches)} 個尚未嘗試` : '估計分母未知'}。</p>
    ${frontierKnown && accepted.estimated_frontier_batches > 0 ? `<progress class="news-provider-progress" aria-label="本輪已登記本機批次的已嘗試比例，非新聞下載完成率" max="${accepted.estimated_frontier_batches}" value="${accepted.scanned_batches}" data-news-accepted-frontier-progress></progress>` : ''}
    <p class="news-provider-note">分母為已嘗試批次，加上當時已登記且位於本輪預定範圍內的剩餘批次，可能因登記變動而改變；已嘗試包含缺檔及異常，待重試項目另列。這不是全文下載進度，完成時間未知。</p></div>` : '';
  const acceptedFacts = accepted
    ? `<p class="news-provider-note" data-news-accepted-batch-scan>${live(data) ? '' : '上次快照：當時'}已接受正文批次原件盤點：${recoveryLabel[accepted.scan_state] || "狀態未知"}。${acceptedProblem}本輪已嘗試盤點 ${count(accepted.scanned_batches)} 個批次，索引 ${count(accepted.indexed_candidates)} 個原件候選${accepted.local_retry_batches == null ? "；待本機重試批次數未知" : `；${count(accepted.local_retry_batches)} 個批次待本機重試`}${accepted.local_missing_retry_batches == null ? "；待再檢查缺檔批次數未知" : `；${count(accepted.local_missing_retry_batches)} 個缺檔批次待再檢查`}${accepted.local_invalid_retry_batches == null ? "；待復原異常批次數未知" : `；已盤點的全來源批次有 ${count(accepted.local_invalid_retry_batches)} 個異常批次等待本機復原`}；舊異常紀錄補登：${invalidMigrationLabel[accepted.local_invalid_migration_state] || "狀態未知"}。候選仍需核驗原請求、收據、授權與內容，並不代表全文或歷史已齊。</p>${frontierFacts}`
    : "";
  const integrity = history?.completed_integrity_scan;
  const integrityFacts = integrity
    ? `<p class="news-provider-note" data-news-integrity-scan>既有清單中繼資料巡檢：${recoveryLabel[integrity.scan_state] || "狀態未知"}；本輪已檢查 ${integrity.checked_scopes == null ? "未知" : count(integrity.checked_scopes)} 個範圍，發現 ${integrity.affected_scopes == null ? "未知" : count(integrity.affected_scopes)} 個受影響範圍（可能重複）。這不檢查原始檔或全文，也不代表所有歷史已抓齊。</p>`
    : "";
  const original = history?.original_envelope_audit;
  const originalProblem = localCheckpointProblems.get(original?.checkpoint_problem) || '';
  const originalPriority = history?.original_priority_admission;
  const priorityProblems = {source_unavailable: '鉅亨來源尚未啟用', plan_unavailable_or_invalid: '計畫檔遺失或校驗失敗', source_identity_changed: '來源或授權身分已變更', invalid_checkpoint: '本機游標異常', checkpoint_archive_conflict: '本機游標封存證據衝突'};
  const priorityFacts = originalPriority
    ? `<div class="news-provider-work" data-news-original-priority>${live(data) ? '' : '上次快照：'}優先處理已知原件問題：${originalPriority.state === 'blocked' ? escape(priorityProblems[originalPriority.problem] || '狀態未知') : `${count(originalPriority.processed_targets)} / ${count(originalPriority.targets)} 個紀錄已掃描；累計 ${count(originalPriority.enqueued_pages)} 個頁面交給本機核驗，${count(originalPriority.existing_pages)} 個頁面已有核驗或重試紀錄，${count(originalPriority.stale_targets)} 個紀錄在本輪掃描時暫無目前頁面。${originalPriority.state === 'scanning' && originalPriority.processed_targets === originalPriority.targets ? '仍有後續頁面變更待安排。' : ''}`}${originalPriority.state !== 'blocked' && originalPriority.targets > 0 ? `<progress class="news-provider-progress" aria-label="原件優先觀測掃描進度，非下載或修復完成率" max="${originalPriority.targets}" value="${originalPriority.processed_targets}"></progress>` : ''}<p class="news-provider-note">這是觀測掃描進度，不是已修復或下載完成；原件與來源仍須個別驗證，完成時間未知。</p></div>` : '';
  const priorityAudit = history?.original_priority_audit;
  const priorityAttempted = priorityAudit
    ? priorityAudit.verified_pages + priorityAudit.mismatched_pages + priorityAudit.unverified_pages + (priorityAudit.recovered_id_pages ?? 0) : 0;
  const priorityAuditFacts = priorityAudit
    ? `<div class="news-provider-work" data-news-original-priority-audit>${live(data) ? '' : '上次快照：'}優先清單本機核驗：追蹤 ${count(priorityAudit.tracked_pages)} 個頁面版本；等待 ${count(priorityAudit.awaiting_pages)}、最近核驗通過 ${count(priorityAudit.verified_pages)}、已建立頁面 ID 證據 ${count(priorityAudit.recovered_id_pages)}、請求或授權不符 ${count(priorityAudit.mismatched_pages)}、原件或資料無法驗證 ${count(priorityAudit.unverified_pages)}、不屬原件核驗 ${count(priorityAudit.outside_pages)}、頁首已變更 ${count(priorityAudit.changed_pages)}。${!priorityAudit.tracking_complete ? '計畫的追蹤紀錄不完整或不一致，這些數字僅涵蓋已登記部分。' : ''}${priorityAudit.tracking_complete && originalPriority?.state === 'complete' && priorityAudit.tracked_pages > 0 ? `<progress class="news-provider-progress" aria-label="優先頁面版本本機核驗進度，非修復完成率" max="${priorityAudit.tracked_pages}" value="${priorityAttempted}"></progress>` : ''}<p class="news-provider-note">這是最近一次本機核驗的結果；統計時不重讀原始檔或完整觀察，後續損毀須靠本機巡檢發現。舊版快照未分類的頁面 ID 證據顯示為未知。不符、無法驗證和頁首變更均不能當作已修復，已建立頁面 ID 證據也不代表全文完成；這些數字不是新聞篇數或全歷史完成率。</p></div>`
    : originalPriority && originalPriority.state !== 'blocked' ? '<p class="news-provider-note" data-news-original-priority-audit>優先清單的本機核驗結果尚無有效快照。</p>' : '';
  const originalNextAudit = original?.next_audit_at_utc
    ? `${live(data) ? '下次本機巡檢' : '上次快照預定下次本機巡檢'} ${escape(stamp(original.next_audit_at_utc))}；到期不代表已執行。`
    : '';
  const originalFacts = original
    ? `<p class="news-provider-note" data-news-original-envelope-audit>${live(data) ? '歷史清單原件身分盤點' : '上次快照的歷史清單原件身分盤點'}：${recoveryLabel[original.scan_state] || "狀態未知"}；${originalProblem}${original.scan_state === 'scanning' ? '目前這輪' : '最近一輪'}完整巡檢已檢視 ${original.checked_pages == null ? "未知" : count(original.checked_pages)} 個頁面記錄，其中 ${original.verified_pages == null ? "未知" : count(original.verified_pages)} 個原件核驗通過，${original.mismatched_pages == null ? "未知" : count(original.mismatched_pages)} 個請求或授權不符、${original.unverified_pages == null ? "未知" : count(original.unverified_pages)} 個中繼資料或本機原件當時無法驗證；另外 ${original.out_of_scope_pages == null ? "未知" : count(original.out_of_scope_pages)} 個未由這輪原件身分核驗（舊版結果可能包含中繼資料異常）。${original.retry_pending_pages == null ? '目前待重驗筆數未知。' : `目前已記錄 ${count(original.retry_pending_pages)} 個頁面待本機重驗；完整巡檢進行中時此數可能尚未包含全部缺件。`}${originalNextAudit}先嘗試既有原件，等待時保留來源的限速與失敗紀錄；這不是全文缺口或全部歷史的完成數。</p>`
    : '<p class="news-provider-note" data-news-original-envelope-audit>歷史清單原件身分盤點尚無有效快照；不代表原件全部正確。</p>';
  const historyFacts = history
    ? `<dl class="news-provider-facts"><div><dt>紀錄為完成的分類清單範圍</dt><dd data-news-history-count="verified">${count(history.verified_list_scopes)}</dd></div><div><dt>分類範圍等待清單證據</dt><dd data-news-history-count="waiting">${count(history.waiting_list_scopes ?? scopes.waiting_page_evidence)}</dd></div><div><dt>來源清單矛盾，非完成</dt><dd data-news-history-count="inconsistent">${count(history.inconsistent_source_scopes)}</dd></div><div><dt>原頁收據待核對</dt><dd data-news-history-count="reference">${history.original_page_reference_scopes == null ? "未知" : count(history.original_page_reference_scopes)}</dd></div><div><dt>本機頁面證據損壞的範圍</dt><dd data-news-history-count="invalid_evidence">${count(history.invalid_page_evidence_scopes)}</dd></div><div><dt>本機範圍證明待修復</dt><dd data-news-history-count="invalid_scope">${history.invalid_scope_evidence_scopes == null ? "未知" : count(history.invalid_scope_evidence_scopes)}</dd></div><div><dt>頁面上限需分區</dt><dd data-news-history-count="page_cap">${count(history.page_cap_exceeded_scopes)}</dd></div><div><dt>未解分類查詢範圍</dt><dd data-news-history-count="gaps">${count(history.unresolved_category_scopes)}</dd></div><div><dt>已核對退場的缺口收據</dt><dd data-news-history-count="retired">${count(history.retired_gap_receipts)}</dd></div></dl><p class="news-provider-note">完成狀態依最後一次核驗記錄；尚未巡檢的範圍無法保證目前仍有效。分類查詢範圍可能包含父子窗口與重疊分類，不是唯一日期或缺失篇數。來源矛盾、本機證據損壞、原頁重查及子窗口待證分開保留，不算清單完成。公開清單補漏不會自動刪掉分類缺口，也不代表該日全文已齊。</p>`
    : '<p class="news-provider-note">分類歷史驗證尚無有效快照；缺口數未知，不視為已補齊。</p>';
  return `<details class="news-provider-list" data-news-list-provider="cnyes" data-news-index-state="${indexState}"${state.cnyesListOpen ? " open" : ""}><summary>${summary}</summary>${cnyesManifestPhase(source.manifest_phase_progress)}${indexFacts}${indexRecoveryFacts}${historyFacts}${legacyFacts}${recoveryFacts}${acceptedFacts}${nonzeroFacts}${integrityFacts}${priorityFacts}${priorityAuditFacts}${originalFacts}<p class="news-provider-note" data-news-list-eta>全鉅亨歷史完成時間未知；清單階段估時不含全文，下方條件估時只涵蓋當前可回補正文網址，不包含尚未探索的清單或歷史缺口。</p></details>`;
}

function renderSources(data) {
  const grid = $("pipeline-grid");
  // A native details toggle event can be queued after a fast search/filter
  // input. Capture connected controls before replacing their DOM so the
  // pending event cannot be lost with the old element.
  const currentCnyesList = grid.querySelector('[data-news-list-provider="cnyes"]');
  if (currentCnyesList) state.cnyesListOpen = currentCnyesList.open;
  for (const [selector, key, open] of [
    ['[data-news-feed-provider]', 'newsFeedProvider', state.feedOpen],
    ['[data-news-discovery-provider]', 'newsDiscoveryProvider', state.discoveryErrorOpen],
    ['[data-news-outcome-provider]', 'newsOutcomeProvider', state.outcomeOpen],
  ]) {
    grid.querySelectorAll(selector).forEach(node => {
      if (node.open) open.add(node.dataset[key]); else open.delete(node.dataset[key]);
    });
  }
  const restoreListFocus = document.activeElement?.closest?.('[data-news-list-provider="cnyes"]') != null;
  const focusedDiscovery = document.activeElement?.closest?.('[data-news-discovery-provider]')?.dataset.newsDiscoveryProvider;
  const all = displaySources(data);
  let rows = all.filter(source => (!state.query || `${source.name} ${source.id}`.toLowerCase().includes(state.query))
    && (state.filter === "all" || (state.filter === "enabled" && source.enabled)
      || (state.filter === "not_enabled" && !source.enabled)
      || (state.filter === "permission" && source.source_policy_review === "permission_required")
      || (state.filter === "followup" && source.source_review_followup === "open")
      || (state.filter === "attention" && (["cooldown", "attention", "stale", "stopped", "invalid", "waiting_disk", "waiting_restart", "waiting_local_database", "waiting_local_database_stale", "waiting_local_snapshot"].includes(source.state)
        || source.robots_origin_policy_pause?.active_origin_count > 0 || source.discovery_errors?.active_errors > 0))));
  if (state.sort === "remaining") rows.sort((a, b) => (b.publisher_reference_coverage?.unresolved_url_heads ?? b.body_coverage?.missing_body_urls ?? b.remaining_urls ?? 0) - (a.publisher_reference_coverage?.unresolved_url_heads ?? a.body_coverage?.missing_body_urls ?? a.remaining_urls ?? 0));
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
    const hasStoredCounters = source.stored_body_counter_available === true;
    const bodyRatio = hasStoredCounters ? source.stored_body_progress_ratio : source.progress_ratio;
    const ratio = bodyRatio == null ? "未知" : `${decimal(bodyRatio * 100)}%`;
    const statesText = Object.entries(source.states || {}).filter(([, value]) => value > 0)
      .map(([key, value]) => `${key === "awaiting_authorization" && source.authorized_api_setup ? "舊網頁管線未啟用" : stateLabels[key] || "其他"} ${count(value)}`).join(" · ");
    const finish = live(data) && source.conditional_finish_at_utc ? `條件完成 ${stamp(source.conditional_finish_at_utc)}` : "完成時點尚未知";
    const badge = ["running", "known_queue_clear", "finite_complete"].includes(source.state) ? "active" : ["cooldown", "attention", "waiting_disk", "waiting_restart", "waiting_local_database", "waiting_local_database_stale", "waiting_local_snapshot"].includes(source.state) ? "waiting" : "";
    const cohort = source.finite_cohort;
    const metadata = source.original_metadata;
    const metadataOnly = metadata && source.known_urls === 0;
    const emptyStates = data.snapshot_consistent === false ? "快照狀態無法核對"
      : metadataOnly ? "已有原始網址線索，尚未建立正文工作" : "尚無已發現網址";
    const acceptanceText = source.adapter_acceptance == null ? "正文入庫狀態待核對"
      : source.adapter_acceptance === "committed_body_heads_observed" ? "已觀測正文入庫" : "尚無正文入庫證據";
    const progressLabel = metadataOnly ? "正文工作尚未建立" : hasStoredCounters ? "已存正文／全部已知網址" : cohort ? "固定批次已存正文／網址" : "已完成狀態／已知網址";
    const progressCount = metadataOnly ? "未入佇列" : hasStoredCounters ? `${count(source.stored_body_urls)} / ${count(source.known_urls)}` : cohort ? `${count(cohort.stored_bodies)} / ${count(cohort.urls)}` : `${count(source.complete_urls)} / ${count(source.known_urls)}`;
    const metadataNote = metadata ? `<p class="news-provider-note news-provider-original-metadata">已解析原始媒體線索 <span data-news-original-links>${count(metadata.literal_metadata_links)}</span> 條 · ${count(metadata.original_hosts)} 個主機；可能多條指向同篇，不是正文筆數、不授予抓取或使用權限。</p>` : '';
    const diagnostic = source.candidate_diagnostic_work;
    const diagnosticNote = diagnostic && Object.values(diagnostic).some(value => value > 0)
      ? `<p class="news-provider-note" data-news-candidate-work>來源診斷：待檢查提交 ${count(diagnostic.pending_unadmitted)} · 已排 ${count(diagnostic.ready)} · 處理中 ${count(diagnostic.reserved)} · 前次預留待確認 ${count(diagnostic.orphaned_reservation)} · 結果不確定 ${count(diagnostic.uncertain_outcome)}；這些不是已存正文或文章數。</p>` : '';
    const replay = source.local_replay_retries;
    const replayParts = [["api_pages", "API 原件", "頁"], ["metadata_batches", "手機欄位原件", "批"],
      ["diagnostic_originals", "診斷原件", "筆"]].map(([key, label, unit]) => {
      const item = replay?.entries?.[key];
      if (!item) return `${label}未知`;
      const due = item.records ? `（最早記錄期限 ${stamp(item.earliest_not_before_utc)}）` : '';
      return `${label} ${count(item.records)} ${unit}${due}`;
    });
    const localReplayNote = source.id !== 'cnyes' ? '' : replay
      ? `<p class="news-provider-note" data-news-local-retries>${live(data) ? '本機原件重試紀錄' : '上次快照的本機原件重試紀錄'}：${escape(replayParts.join(' · '))}；這些不是文章或正文完成數，記錄期限不代表已派發，仍受資料、權限與磁碟條件限制。</p>`
      : '<p class="news-provider-note" data-news-local-retries>本機原件重試紀錄未知；沒有有效快照不代表零或已完成。</p>';
    const resource = source.metadata_resource_waits;
    const resourceNote = source.id !== 'cnyes' ? '' : resource
      ? `<p class="news-provider-note" data-news-metadata-resource>${live(data) ? '手機欄位容量觀測紀錄' : '上次快照的手機欄位容量觀測紀錄'}：${count(resource.records)} 批，其中 ${count(resource.above_current_bound_records)} 批超過主迴圈單次讀取限制 ${bytes(resource.current_bound_bytes)}${resource.max_observed_required_bytes == null ? '' : `，最大觀測原件大小（壓縮或解壓）${bytes(resource.max_observed_required_bytes)}`}；紀錄可能過期，不是文章、正文或完成數。</p>`
      : '<p class="news-provider-note" data-news-metadata-resource>手機欄位容量觀測紀錄未知；沒有有效快照不代表零或已完成。</p>';
    const largeReplay = source.metadata_large_replay;
    const largeCauses = largeReplay?.retry_cause_counts;
    const largeCauseNote = !largeReplay?.retry_records ? '' : largeCauses
      ? `；其中無法觀測逾時 ${count(largeCauses.progress_unknown_timeout)}、計數器無變化逾時 ${count(largeCauses.counters_unchanged_timeout)}、其他 ${count(largeCauses.other)}`
      : '；重試原因分類未知';
    const largeReplayNote = source.id !== 'cnyes' ? '' : largeReplay
      ? `<p class="news-provider-note" data-news-metadata-large>${live(data) ? '大原件隔離讀取' : '上次快照的大原件隔離讀取'}：${largeReplay.assigned ? '已派發，等待回報' : '沒有已派發工作'}；本機重試紀錄 ${count(largeReplay.retry_records)} 筆${largeReplay.retry_records ? `（最早記錄期限 ${escape(stamp(largeReplay.earliest_not_before_utc))}）` : ''}${largeCauseNote}。重試紀錄可能過期，不是文章完成數或保證派發時間。</p>`
      : '<p class="news-provider-note" data-news-metadata-large>大原件隔離讀取狀態未知；沒有有效快照不代表零或已完成。</p>';
    const metadataIndex = source.metadata_index_progress;
    const indexState = {scanning: '基線核對中', draining: '處理核對期間的變動', complete: '基線核對完成'};
    const metadataIndexNote = source.id !== 'cnyes' ? '' : metadataIndex
      ? `<p class="news-provider-note" data-news-metadata-index>${live(data) ? '手機原件增量清冊' : '上次快照的手機原件增量清冊'}：${indexState[metadataIndex.state]}；累計掃描 ${count(metadataIndex.rows_seen)} 列，基線核對登記或更新 ${count(metadataIndex.registered_heads)} 個檢查點，處理變動 ${count(metadataIndex.dirty_processed)} 次${metadataIndex.cursor_resets ? `，重掃 ${count(metadataIndex.cursor_resets)} 次` : ''}；${metadataIndex.invalid_heads_observed == null ? '異常觀測數未知' : `曾觀測原件路徑缺失且尚未由接收流程清除 ${count(metadataIndex.invalid_heads_observed)} 筆`}。掃描可重複，這不是正文或欄位補件完成數，也無法推算全站完成時間。</p>`
      : '<p class="news-provider-note" data-news-metadata-index>手機原件增量清冊狀態未知；沒有有效快照不代表零或已完成。</p>';
    const feed = source.metadata_feed_progress;
    const feedDue = feed?.next_check_ready ? '已到期' : stamp(feed?.next_check_at_utc);
    const feedSchedule = !feed ? '' : live(data)
      ? `每 ${decimal(feed.poll_interval_seconds / 60)} 分鐘追新；最近核對 ${escape(stamp(feed.last_check_at_utc))}，下一次 ${escape(feed.next_check_ready ? '已到期，等待排程' : feedDue)}。`
      : `排程間隔 ${decimal(feed.poll_interval_seconds / 60)} 分鐘；最近核對 ${escape(stamp(feed.last_check_at_utc))}。${data.health === 'stopped' ? '下載器顯示已停止，恢復服務後才會繼續' : '目前階段無有效追新執行觀測'}；上次記錄排程 ${escape(feedDue)}，不表示已經執行。`;
    const feedRecovery = !feed ? '' : feed.local_recovery == null ? '本機恢復狀態未知。'
      : feed.local_recovery.pending_feeds === 0 ? '本機沒有待恢復的 RSS 原件。'
      : `${live(data) ? '' : '上次快照：'}本機待恢復 ${count(feed.local_recovery.pending_originals)} 筆原件、${count(feed.local_recovery.pending_feeds)} 個端點；${feed.local_recovery.next_retry_ready ? '已到恢復時間，等待下載器處理' : `最早恢復時間 ${escape(stamp(feed.local_recovery.next_retry_at_utc))}`}。同一端點會先恢復原件再追新。`;
    const legacy = feed?.legacy_original_index;
    const legacyNote = !feed ? '' : !legacy ? '舊 RSS 原件清點狀態未知。' :
      `${live(data) ? '' : '上次快照：'}舊原件目錄已檢查 ${count(legacy.examined_directories)} / ${count(legacy.expected_directories)}；發現待核驗原件 ${count(legacy.candidate_originals)} 份，已驗 ${count(legacy.verified_originals)}，待驗 ${count(legacy.pending_originals)}，重試 ${count(legacy.retry_originals)}，異常 ${count(legacy.invalid_originals + legacy.other_originals)}。${legacy.discovery_complete ? '最近一輪已掃描，之後新增的原件仍需巡檢。' : '仍在清點，候選原件數會增加。'}已索引原件不在候選數中；這些不是文章數或全文完成率。`;
    const feedNote = !feed ? '' : `<div class="news-provider-work" data-news-metadata-feed><dl class="news-provider-facts"><div><dt>已驗 RSS／已註冊</dt><dd>${count(feed.verified_feeds)} / ${count(feed.registered_feeds)}</dd></div><div><dt>最新 feed 資訊筆數</dt><dd>${count(feed.latest_feed_item_observations)}</dd></div><div><dt>最新 XML 流量</dt><dd>${bytes(feed.latest_feed_response_bytes)}</dd></div><div><dt>RSS 錯誤端點</dt><dd>${count(feed.feeds_with_errors)}</dd></div></dl><p class="news-provider-note">標題、摘要、連結與發布時間；不同 feed 可能包含同篇，不是去重文章或全文。${feedSchedule}${feedRecovery}${legacyNote}持續追新沒有全歷史完成時間；全文尚未啟用。</p></div>`;
    const supplement = source.finite_supplements;
    const outcomes = cohort && cohort.reviewed_nonbody_outside_cohort;
    const outcomeNote = outcomes && outcomes.urls ? `；已核對非正文 ${count(outcomes.urls)} 個網址（來源不存在 ${count(outcomes.states.source_not_found || 0)}，非文章路徑 ${count(outcomes.states.unsupported_article_path || 0)}），不計入已存正文` : "";
    const cohortNote = cohort ? `原固定批次 ${count(cohort.stored_bodies)} / ${count(cohort.urls)}，不重抓${supplement ? `；已核對追加 ${count(supplement.stored_bodies)} / ${count(supplement.urls)} 正文` : ""}${outcomeNote}；未核對新增 ${count(cohort.unreviewed_new_urls ?? cohort.new_urls_outside_cohort)} 個網址（待排 ${count(source.growth_waiting_urls)}）` : "";
    const disposition = source.outcome_progress;
    const coverage = source.body_coverage;
    const reference = source.publisher_reference_coverage;
    const referenceNote = !reference ? '' : `<div class="news-provider-work" data-news-publisher-references><dl class="news-provider-facts"><div><dt>已有正文的已驗別名</dt><dd data-news-verified-references>${count(reference.verified_reference_urls)}</dd></div><div><dt>未解析網址（扣除別名）</dt><dd data-news-unresolved-references>${count(reference.unresolved_url_heads)}</dd></div><div><dt>失效、待重驗對照</dt><dd data-news-inactive-references>${count(reference.inactive_reference_urls)}</dd></div><div><dt>正文或已驗對照／已知網址</dt><dd data-news-resolved-references>${count(reference.resolved_url_heads)} / ${count(source.known_urls)}</dd></div></dl><progress class="news-provider-progress mini-progress-track" data-news-reference-progress max="1"${reference.resolved_url_progress_ratio == null ? '' : ` value="${reference.resolved_url_progress_ratio}"`} aria-label="${escape(source.name)} 正文或已驗對照的網址解析比例"></progress><p class="news-provider-note">別名只連到已保存正文，不新增文章、不複製正文或原頁；各自的轉址與抓取時間保持分開。下方正文比例仍只計直接保存的 ${count(source.stored_body_urls)} 個網址；非全歷史完整率。</p></div>`;
    const coverageNote = coverage ? `<p class="news-provider-note" data-news-body-partition>待排缺文 <span data-news-queued-missing>${count(coverage.queued_missing_body_urls)}</span> · 已有正文待排重查 <span data-news-queued-retained>${count(coverage.queued_stored_body_recheck_urls)}</span>；已有正文、非完成狀態 <span data-news-retained-noncomplete>${count(coverage.retained_body_noncomplete_urls)}</span>，不算缺文。估時只計待排缺文。</p>` : '<p class="news-provider-note" data-news-body-partition-unknown>尚無可核對的正文／狀態交叉統計，不推測缺文與重查各有多少。</p>';
    const dispositionNote = !disposition ? "" : `<details class="news-provider-list" data-news-outcome-provider="${escape(source.id)}"${state.outcomeOpen.has(source.id) ? " open" : ""}><summary>缺口分類與下一步</summary><div class="table-scroll"><table><caption>全部網址狀態，拆分尚缺正文與已有正文</caption><thead><tr><th scope="col">分類</th><th scope="col">全部</th><th scope="col">缺正文</th><th scope="col">已有正文</th></tr></thead><tbody>${outcomeRows(disposition, coverage, source.id)}</tbody></table></div><p class="news-provider-note">缺正文與已有正文互斥，相加為全部網址；無交叉統計時顯示未知。不覆寫正文、收據或重試期限。原站不存在不代表所有可用替代都已找過；尚無全途徑不可取得／結案比例的驗收。</p></details>`;
    const apiNote = source.authorized_api_setup ? '<p class="news-provider-eta">已確認 API 授權；先設定 CNA_API_KEY 並核對實際工具、全文範圍與配額。目前未開始 API 全文下載，完成時間未知；舊網頁佇列保留，不算完成。</p>' : '';
    const work = source.body_work_progress;
    const scopeWork = source.body_request_scope_progress;
    const rechecks = scopeWork?.pending_source_rechecks;
    const recheckNote = !rechecks?.available ? '來源失敗後的恢復核對狀態尚未知，不能據此估算完成時間。' : rechecks.unverified_missing_body_urls
      ? `另有 <span data-news-scope-recheck-pending>${count(rechecks.unverified_missing_body_urls)}</span> 個缺文網址的來源失敗尚待成功核對，最近核對時其中 ${count(rechecks.due_missing_body_urls)} 個已到重新核對時間；到期不代表來源已恢復。`
      : '最近一次紀錄沒有仍待重新核對的來源失敗；這不代表已獲准存取或正文已完成。';
    const scopeProbe = scopeWork?.source_probe_ready
      ? (live(data) ? '已到期，等待排程' : '上次記錄已到期，實際執行情況待確認')
      : stamp(scopeWork?.next_source_probe_at_utc);
    const scopeExecution = live(data) ? '此分類不暫停追新與其他健康端點；是否正在執行依目前程序狀態判定。' : '此分類反映上次觀察；目前是否執行待確認。';
    const scopeNote = !scopeWork?.available ? '' : `<p class="news-provider-note" data-news-request-scope>最近核對：缺文端點暫緩 <span data-news-scope-paused>${count(scopeWork.source_paused_missing_body_urls)}</span> · 其餘待排／執行中 <span data-news-scope-other>${count(scopeWork.other_missing_body_urls)}</span>，合計 ${count(scopeWork.active_missing_body_urls)} 個缺文網址。${scopeWork.source_paused_missing_body_urls ? `最早重新核對 ${escape(scopeProbe)}；到期不代表來源恢復，不借用其他端點的新增速度倒數。` : '未受這兩類端點暫緩，不代表全部已到期或獲准存取。'}${recheckNote}${scopeExecution}</p>`;
    const forecast = source.completion_forecast;
    const displayedRate = forecast ? forecast.new_url_heads_per_second : source.owner_articles_per_second;
    const forecastLabels = { collecting_recent_samples: "正在累積目前程序的完整視窗，暫不推算完成時間", stale_recent_measurement: "近期量測已過期，暫不沿用舊速率估時", unavailable: "近期量測無法核對，完成時間未知", no_recent_new_heads: "近期完整視窗沒有新增正文，不以零速率推算完成時間" };
    const forecastNote = !forecast ? "" : `<p class="news-provider-note" data-news-forecast="${escape(forecast.state)}">${forecast.state === "recent_measured" ? `估時採近期 ${count(forecast.sample_count)} 個完整視窗，${decimal(forecast.sample_seconds)} 秒新增 ${count(forecast.new_url_heads)} 個網址正文，含失敗、等待與視窗間隔；不挑最快視窗。` : escape(forecastLabels[forecast.state] || "近期量測尚未知")}${forecast.ended_at_utc ? ` 最近視窗結束 ${escape(stamp(forecast.ended_at_utc))}。` : ""} 程序平均 ${decimal(source.owner_articles_per_second == null ? null : source.owner_articles_per_second * 60)}／分（含啟動等待，與估時速率分開）。</p>`;
    const workNote = source.id !== 'cnyes' ? '' : work
      ? `<div class="news-provider-work"><dl class="news-provider-facts"><div><dt>未找到已存主文／待分類</dt><dd data-news-work="missing">${count(work.no_known_primary_body_urls)}</dd></div><div><dt>主文已存，網址版本待驗證</dt><dd data-news-work="candidate">${count(work.existing_primary_body_candidates)}</dd></div><div><dt>本網址正文已存，待重查</dt><dd data-news-work="stored">${count(work.stored_body_recheck_urls)}</dd></div></dl><p class="news-provider-note">以上合計為待回補網址；同 ID 是排程提示，不代表正文相同或已完成。尚無主文優先，網址版本仍定期排入。</p></div>`
      : '<p class="news-provider-note news-provider-work">正文工作分類尚未就緒；不以未知數當成零或完成。</p>';
    const lookup = source.native_lookup_recheck_progress;
    const lookupNote = source.id !== 'cnyes' ? '' : lookup
      ? `<p class="news-provider-note" data-news-lookup-rechecks>${live(data) ? '最近紀錄' : '上次快照'}：本機已登記待核對的狀態變更 ${lookup.count_exact ? '' : '至少 '}${count(lookup.pending_records_lower_bound)} 筆；尚未包含舊資料掃描，這些不是正文完成數，也不代表已向來源查詢。</p>`
      : '<p class="news-provider-note" data-news-lookup-rechecks>本機候選補查狀態未知；沒有有效快照不代表沒有待查工作。</p>';
    return `<article class="pipeline-card"><header class="pipeline-card-header"><div class="news-provider-identity"><strong>${escape(source.name)}</strong><small>${escape(source.id)}</small></div><span class="pipeline-status ${badge}">${escape(labels[source.state] || "待觀測")}</span></header>${apiNote}
      <dl class="news-provider-facts"><div><dt>${progressLabel}</dt><dd>${progressCount}</dd></div><div><dt>${coverage ? "尚缺正文（含阻塞）" : "待排網址（含重查）"}</dt><dd data-news-missing-body-urls>${count(coverage ? coverage.missing_body_urls : source.remaining_urls)}</dd></div><div><dt>正文 UTF-8</dt><dd>${bytes(source.text_bytes)}</dd></div><div><dt>${metadataOnly ? "新增正文速率尚未量測" : source.authorized_api_setup ? "API 新增正文／分鐘，未量測" : forecast ? "近期新增正文／分鐘" : source.forecast_rate_basis === "new_stored_url_heads" ? "本程序新增正文／分鐘" : "舊快照版本速率／分鐘"}</dt><dd data-news-rate="completion">${decimal(displayedRate == null ? null : displayedRate * 60)}</dd></div></dl>
      ${referenceNote}${coverageNote}${feedNote}${metadataNote}${diagnosticNote}${localReplayNote}${resourceNote}${largeReplayNote}${metadataIndexNote}${workNote}${lookupNote}${scopeNote}${cnyesListProgress(source, data)}${fulltextFeedInventory(source)}${discoveryErrorDetails(source, data)}
      <progress class="news-provider-progress mini-progress-track" max="1"${bodyRatio == null ? "" : ` value="${bodyRatio}"`} aria-label="${escape(source.name)} ${hasStoredCounters ? '已存正文占全部已知網址的比例' : cohort ? '固定批次正文比例' : '已完成狀態占已知網址的比例'}"></progress><div class="news-provider-states">${hasStoredCounters ? '全部已知網址' : cohort ? '固定批次' : '已知範圍'} ${ratio} · ${escape(statesText || emptyStates)}${cohortNote ? `<p>${escape(cohortNote)}</p>` : ''}</div>${dispositionNote}
      <p class="news-provider-note">剩餘 ${escape(eta(source, data))} · ${escape(finish)}</p>${forecastNote}<p class="news-provider-note">${escape(reviewLabels[source.source_policy_review] || "來源審閱紀錄不可用")}${source.source_reviewed_at_utc ? ` · 核對 ${escape(stamp(source.source_reviewed_at_utc))}` : ""} · <span data-news-review-followup="${escape(source.source_review_followup || 'unknown')}">${escape(followupLabels[source.source_review_followup] || followupLabels.unknown)}</span></p><p class="news-provider-note">${source.authorized_api_setup ? "官方 API 能力檢查已實作，全文管線未啟用" : source.adapter_registered ? "共用下載器已接線" : "來源尚未註冊"} · ${acceptanceText}${source.source_role === "redirector" ? " · 轉址來源，須找到原始媒體 URL" : ""}</p><p class="news-provider-note">${escape(errorLabel(source))}</p></article>`;
  }).join(""));
  if (restoreListFocus) $("pipeline-grid").querySelector('[data-news-list-provider="cnyes"] > summary')?.focus({preventScroll: true});
  if (focusedDiscovery) Array.from($("pipeline-grid").querySelectorAll('[data-news-discovery-provider]'))
    .find(node => node.dataset.newsDiscoveryProvider === focusedDiscovery)?.querySelector('summary')?.focus({preventScroll: true});
}
function fulltextFeedInventory(source) {
  const feed = source.fulltext_feed_inventory;
  if (!feed) return "";
  const months = feed.first_registered_month && feed.last_registered_month ? `；登記月份 ${escape(feed.first_registered_month)}～${escape(feed.last_registered_month)}` : "";
  return `<details class="news-provider-list" data-news-feed-provider="${escape(source.id)}"${state.feedOpen.has(source.id) ? " open" : ""}><summary>RSS 正文端點與分頁</summary><dl class="news-provider-facts"><div><dt>追新根 feed</dt><dd data-news-feed-count="roots">${count(feed.registered_root_feeds)}</dd></div><div><dt>歷史月份 scope</dt><dd data-news-feed-count="months">${count(feed.registered_monthly_feeds)}</dd></div><div><dt>其他歷史 scope</dt><dd data-news-feed-count="other">${count(feed.registered_other_historical_feeds)}</dd></div><div><dt>本程序已結束 RSS 請求</dt><dd data-news-feed-count="requests">${count(feed.owner_terminal_requests)}</dd></div><div><dt>200 回應頁／304 未異動</dt><dd><span data-news-feed-count="pages">${count(feed.owner_200_pages)}</span> / <span data-news-feed-count="unchanged">${count(feed.owner_304_pages)}</span></dd></div><div><dt>RSS 已入庫新正文網址</dt><dd data-news-feed-count="new-bodies">${count(feed.owner_new_stored_url_heads)}</dd></div></dl><p class="news-provider-note">端點數是登記範圍，不代表歷史已抓齊${months}。請求包含根頁、歷史分頁與失敗；200 頁仍須驗證正文、作者與時間，不等於新增文章。只有已入庫新正文計入完成量，重疊文章、304 與既有正文不重複計入。缺少程序計數顯示未知，不補成零。</p></details>`;
}
function renderTraffic(data) {
  const workloadLabels = { cnyes_bulk: "批次正文", wordpress_posts: "批次正文", rss_fulltext: "RSS 正文", article_pdf: "PDF", article_html: "單篇 HTML", mixed: "混合工作，不能當成同工作量測速", metadata: "目錄／索引", unclassified: "舊版未分類量測" };
  html("traffic-rows", displaySources(data).filter(source => source.configured).map(source => `<tr><td>${escape(source.name)}<small>${escape(labels[source.state] || "待觀測")}${source.registered_bulk_endpoints ? `；${count(source.registered_bulk_endpoints)} 個分類／單次最多 ${count(source.observed_bulk_rows_per_request_cap)} 則` : ""}</small></td><td>${count(source.owner_requests)} 次／${bytes(source.owner_response_bytes)}</td><td>${decimal(source.client_min_interval_seconds)} 秒／${count(source.parallelism)} 並行<small>${escape(capacityLabels[source.capacity_state] || "等待執行中容量觀測")}；官方總配額未知</small></td><td>${decimal(source.minimum_observed_request_gap_seconds)} 秒<small>最近 ${decimal(source.last_window_seconds)} 秒：${decimal(source.last_window_requests_per_second)} 請求／秒、${decimal(source.last_window_new_heads_per_second)} 新正文／秒；${escape(workloadLabels[source.capacity_workload_family] || "工作分類待量測")}</small></td><td>${escape(errorLabel(source))}</td></tr>`).join(""));
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
  const routing = resolution.routing_work;
  const known = routing?.snapshot_consistent === true;
  text("original-routing-count", known
    ? `${count(routing.completed_hosts)} / ${count(routing.known_hosts)} 個主機` : "未知");
  text("original-routing-state", known
    ? ({pending: "基線尚未完成", complete: "目前基線已完成", empty: "無既有原站主機"}[routing.state] || "未知") : "等待有效快照");
  text("original-routing-detail", known
    ? `尚有 ${count(routing.pending_hosts)} 個主機待核對；此比例按主機計算，不是連結或正文完整率。新增來源或規則變更會另行續排。`
    : "目前快照未提供有效的來源路由進度；不由轉址解析百分比推算，也不當成零剩餘。");
  progress("original-routing-progress", known ? routing.host_progress_ratio : null);
}

function render(data) {
  if (data.schema_version !== 1 || !Array.isArray(data.providers) || !data.acquisition || !data.worker) throw new Error("invalid news status contract");
  if (data.health === "waiting_local_database" && localWaitRemainingMs(data) <= 0) data = staleLocalWait(data);
  state.latest = data;
  const acquisition = data.acquisition;
  const coverage = data.body_coverage;
  const badge = $("news-health");
  const localWait = ["waiting_local_database", "waiting_local_database_stale", "waiting_local_snapshot"].includes(data.health);
  badge.className = `status ${data.health === "running" ? "updating" : data.health === "initializing" ? "loading" : data.health === "initializing_stale" || localWait ? "stale" : data.health}`;
  badge.lastChild.textContent = labels[data.health] || "狀態待確認";
  const initializing = ["initializing", "initializing_stale"].includes(data.health);
  const lifecycle = data.worker.lifecycle;
  const freshness = localWait
    ? `本機狀態觀測 ${stamp(lifecycle?.observed_at_utc)} · 數量沿用來源快照 ${stamp(data.observed_at_utc)}`
    : initializing
    ? `啟動觀測 ${stamp(lifecycle?.observed_at_utc)} · 數量沿用前次快照 ${stamp(data.observed_at_utc)}`
    : `來源快照 ${stamp(data.observed_at_utc)} · 每 15 秒刷新唯讀快照`;
  text("news-freshness", freshness + ownerFailureSummary(lifecycle));
  text("body-count", count(acquisition.stored_body_articles));
  text("body-bytes", `正文 UTF-8 ${bytes(acquisition.text_bytes)} · 非壓縮容量`);
  text("known-count", count(acquisition.known_urls));
  text("remaining-label", coverage ? "待排缺正文" : "待排網址（含重查）");
  text("remaining-count", count(coverage ? coverage.queued_missing_body_urls : acquisition.remaining_urls));
  text("remaining-note", coverage ? `所有缺文 ${count(coverage.missing_body_urls)} · 已有正文待排重查 ${count(coverage.queued_stored_body_recheck_urls)}` : `處理中 ${count(acquisition.inflight_urls)} · 待重試 ${count(acquisition.retry_urls)}；缺文／重查尚未知`);
  text("restricted-count", count(acquisition.awaiting_authorization_urls));
  text("download-progress-label", `${count(acquisition.stored_body_articles)} / ${count(acquisition.known_urls)}${acquisition.progress_ratio == null ? "" : ` · ${decimal(acquisition.progress_ratio * 100)}%`}`);
  text("download-progress-detail", `正文／已清點唯一網址${coverage ? `；尚缺正文 ${count(coverage.missing_body_urls)}（含未接線、範圍限制與其他阻塞）` : ""}。探索仍在增加分母，不是全歷史完整率。`);
  progress("download-progress", acquisition.progress_ratio);
  const qualityAge = Date.now() - Date.parse(data.generated_at_utc);
  const qualitySnapshotCurrent = Number.isFinite(qualityAge) && qualityAge >= -5000 && qualityAge <= 45000;
  const quality = qualitySnapshotCurrent ? data.corpus_quality : null;
  const hasQuality = quality?.state === "known_issues" || quality?.state === "no_positive_in_scope";
  text("corpus-quality-title", quality?.state === "known_issues"
    ? `訓練資料集：已發現 ${count(quality.known_affected_versions)} 個有品質問題的全文版本`
    : quality?.state === "no_positive_in_scope" ? "訓練資料集：本次全文稽核範圍沒有確認受影響版本"
    : "訓練資料集全文品質：目前版本尚無可用稽核");
  text("corpus-quality-detail", hasQuality
    ? `核對時間 ${stamp(quality.observed_at_utc)}；正式受理 ${count(quality.owner_admitted_versions)}、待正式受理 ${count(quality.private_staged_versions)}。${quality.unconfirmed_targets ? `另有 ${count(quality.unconfirmed_targets)} 個稽核目標未確認。` : ""}僅限已核對的版本，其他全文品質仍未知；這不是下載佇列的缺文數，也不代表正式資料集已修復。`
    : quality?.state === "superseded" ? "資料集版本已變更，舊版稽核不可沿用；新版本品質尚未核對。"
    : !qualitySnapshotCurrent ? "面板資料已過期；等待重新讀取資料集品質。" : "稽核證據缺少或無法核對，不代表沒有品質問題。下載數量依舊顯示來源佇列的觀測。" );
  const qualityProviders = hasQuality && Array.isArray(quality.providers) && quality.providers.length > 0;
  $("corpus-quality-providers").hidden = !qualityProviders;
  text("corpus-quality-providers", qualityProviders
    ? `已確認的來源版本：${quality.providers.map(item => `${item.id} ${count(item.versions)}`).join(" · ")}` : "—");
  const headlineQuality = qualitySnapshotCurrent ? data.corpus_headline_quality : null;
  text("corpus-headline-quality", headlineQuality?.state === "tag_headlines_detected"
    ? `標題範圍：${count(headlineQuality.tag_headline_rows)} 筆 CMNews 標籤網址列為標題；稽核時的來源 head 均無正文指標，不能推定資料集當時的來源狀態。核對時間 ${stamp(headlineQuality.observed_at_utc)}；來源頁內容未核驗，其他標題品質仍未知。這不是全文版本數或下載佇列數，也不代表資料集已修復。`
    : headlineQuality?.state === "no_tag_headlines_in_scope" ? "標題範圍：本次稽核沒有發現 CMNews 標籤網址；其他標題品質仍未知。"
    : headlineQuality?.state === "superseded" ? "標題範圍：資料集版本已變更，舊版稽核不可沿用。"
    : "標題範圍：目前版本尚無可用稽核，不代表沒有問題。");
  const sortDay = qualitySnapshotCurrent ? data.sort_day_quality : null;
  const sortKnown = sortDay?.state === "historical_audit";
  text("sort-day-summary", sortKnown ? `· ${count(sortDay.source_keys_with_heads)} 個來源鍵` : "· 未知");
  text("sort-day-detail", sortKnown
    ? `歷史稽核 ${stamp(sortDay.observed_at_utc)}：${count(sortDay.known_heads)} 個已知網址中，${count(sortDay.present)} 個有排序日期；日曆格式無效 ${count(sortDay.invalid)}、早於 1900 年 ${count(sortDay.pre_1900)}、晚於核對當日 UTC ${count(sortDay.after_snapshot_utc)}。這些只是排程提示及待核對標記，不是來源發布時間、當前佇列狀態或歷史完整率。`
    : "稽核證據缺少、損壞或面板資料過期；排序日期品質未知。排序提示不是來源發布時間。");
  const flaggedSortSources = sortKnown && Array.isArray(sortDay.flagged_sources) && sortDay.flagged_sources.length > 0;
  $("sort-day-sources").hidden = !flaggedSortSources;
  text("sort-day-sources", flaggedSortSources
    ? `需核對的來源鍵：${sortDay.flagged_sources.map(item => `${item.id}（格式 ${count(item.invalid)}、早期 ${count(item.pre_1900)}、未來 ${count(item.after_snapshot_utc)}）`).join(" · ")}` : "—");
  text("global-eta", "未知");
  text("global-eta-basis", "未啟用／被拒絕來源與未知歷史仍存在；不是成功倒數。");
  const diskVerification = data.worker.disk_verification;
  const waitingForDiskVerification = data.health === "waiting_disk" && diskVerification;
  text("eligible-eta", live(data) ? duration(acquisition.eligible_eta_seconds) : "目前階段暫無有效估時");
  text("eligible-eta-basis", localWait ? "本機寫入重試及其後的快照未確認前，不以舊速率估時。" : waitingForDiskVerification ? "實體磁碟空間尚未確認，新增抓取暫停；目前沒有有效完成估時。" : data.health === "waiting_disk" ? "磁碟不足，新增抓取暫停；容量恢復前沒有有效完成估時。" : acquisition.eligible_eta_seconds == null ? "有來源缺有效落盤速率或正在冷卻，請看逐來源條件估時。" : "各來源並行，採最長條件工時；未含新探索或阻塞來源。");
  text("metadata-count", acquisition.metadata_selection_pending > 0
    ? `來源選取待核對 ${count(acquisition.metadata_selection_pending)} 個，分區數未知`
    : `${count(acquisition.metadata_partitions)} 個來源分區`);
  text("discovery-count", `到期清點 ${count(acquisition.discovery_due)} 個 · 清點錯誤 ${count(acquisition.discovery_errors)} 個 · 端點暫停 ${count(acquisition.temporarily_blocked_bulk_pages)} 頁`);
  text("retry-count", coverage ? `缺文待重試 ${count((coverage.missing_body_states.retry || 0) + (coverage.missing_body_states.retry_cooldown || 0))} · 已有正文重試 ${count((coverage.retained_body_states.retry || 0) + (coverage.retained_body_states.retry_cooldown || 0))}` : `${count(acquisition.retry_urls)} 個待重試（含重查）`);
  const outcomes = data.outcome_progress?.categories;
  const finmindCatalogError = acquisition.finmind_catalog_errors > 0
    ? ` · FinMind 新聞清單讀取異常${live(data) ? "" : "（上次快照）"}` : "";
  const finlabCatalogError = acquisition.finlab_catalog_errors > 0
    ? ` · FinLab 新聞清單讀取異常${live(data) ? "" : "（上次快照）"}` : "";
  text("error-count", (coverage ? `缺文待修 ${count(coverage.missing_body_outcomes.categories.repair_required)} · 已有正文、非完成狀態 ${count(coverage.retained_body_noncomplete_urls)} · metadata 失敗 ${count(acquisition.metadata_errors)}` : outcomes ? `全部網址待修狀態 ${count(outcomes.repair_required)}（可能已有正文）· metadata 失敗 ${count(acquisition.metadata_errors)}` : `未啟用／其他不可執行 ${count(acquisition.blocked_urls)} · metadata 失敗 ${count(acquisition.metadata_errors)}`) + finlabCatalogError + finmindCatalogError);
  html("outcome-rows", outcomes ? outcomeRows(data.outcome_progress, coverage) : '<tr><td colspan="4">尚無有效分類快照</td></tr>');
  text("request-count", count(data.worker.requests));
  text("response-bytes", bytes(data.worker.response_bytes));
  const measured = data.providers.filter(source => source.enabled && source.owner_articles_per_second != null);
  const exactHeads = measured.every(source => source.forecast_rate_basis === "new_stored_url_heads");
  text("capture-rate", live(data) && measured.length ? `${decimal(measured.reduce((sum, source) => sum + source.owner_articles_per_second * 60, 0))} ${exactHeads ? "新增正文" : "舊快照版本代理"}／分` : "等待有效量測");
  text("capture-cpu", data.worker.owner_elapsed_seconds > 0 && data.worker.owner_cpu_seconds != null ? `${decimal(data.worker.owner_cpu_seconds / data.worker.owner_elapsed_seconds * 100)}% 單核心` : "—");
  text("capture-elapsed", `程序經過 ${duration(data.worker.owner_elapsed_seconds)} · 不使用 GPU`);
  text("source-count", `完整保留 ${count(data.providers.length)} 個已配置／已發現來源；狀態、待辦與條件估時分開。`);
  const reviews = data.source_policy_review_counts || {};
  const followupCounts = data.source_review_followup_state_counts;
  const followupKeys = ["open", "none_recorded", "unknown"];
  const followupAvailable = data.source_review_followup_contract_version === 1 && data.source_reviews_state === "loaded"
    && followupCounts != null && typeof followupCounts === "object" && !Array.isArray(followupCounts)
    && Object.keys(followupCounts).every(key => followupKeys.includes(key))
    && followupKeys.every(key => Number.isSafeInteger(followupCounts[key] ?? 0) && (followupCounts[key] ?? 0) >= 0)
    && followupKeys.reduce((sum, key) => sum + (followupCounts[key] ?? 0), 0) === data.providers.length;
  document.querySelector('[data-news-filter="followup"]').disabled = !followupAvailable;
  if (!followupAvailable && state.filter === "followup") {
    state.filter = "all"; state.page = 0;
    document.querySelectorAll("[data-news-filter]").forEach(button => {
      const active = button.dataset.newsFilter === "all";
      button.classList.toggle("active", active); button.setAttribute("aria-pressed", String(active));
    });
  }
  text("source-review-summary", data.source_reviews_state === "loaded"
    ? `需取得來源同意 ${count(reviews.permission_required || 0)} 個 · 尚未審閱 ${count(reviews.not_reviewed || 0)} 個 · 審閱未完成 ${count(reviews.review_incomplete || 0)} 個。${followupAvailable ? `有已記錄後續問題 ${count(followupCounts.open || 0)} 個來源鍵；別名可能重複，未列出不代表完成。` : "後續問題紀錄暫無法讀取。"}這是來源審閱，不是抓取失敗或下載授權。`
    : "來源審閱紀錄暫無法讀取；既有下載狀態與數量仍保留，不因此啟用來源。");
  const disk = data.worker.disk_admission;
  const diskCheckReason = {pending: "正在查證實體磁碟空間", unavailable: "實體磁碟空間查證失敗", stale: "上次實體磁碟空間查證已過期"};
  text("capture-next", data.health === "waiting_local_database"
    ? "最近觀測：本機資料庫寫入受鎖阻擋，原程序當時正重試已保存的正文批次；既有來源請求可能仍在處理。這不是網站限流。"
    : data.health === "waiting_local_database_stale" ? "上次觀測為本機寫入鎖等待，但觀測已過期，現在是否仍在等待尚未確認。"
    : data.health === "waiting_local_snapshot" ? "本機寫入重試已結束；等待新的來源進度快照，不能據此判定正文提交成功。"
    : data.health === "waiting_disk" && disk?.available
    ? `磁碟可用 ${bytes(disk.free_bytes)}／保留線 ${bytes(disk.required_free_bytes)}，尚差 ${bytes(disk.shortfall_bytes)}；原服務約每 ${count(disk.automatic_recheck_seconds)} 秒重查，容量足夠才續抓。${data.worker.source_activity === "inventorying" ? "目前仍在整理清冊。" : ""}`
    : waitingForDiskVerification ? `${diskCheckReason[diskVerification.state]}；新增抓取暫停，${diskVerification.probe_inflight && diskVerification.state !== "pending" ? "正在重新查證" : "原服務會自動重查"}。`
    : live(data) ? "到期 feed 優先；就緒正文立即續抓，按來源限流／退避自動續排。" : `${labels[data.health] || "狀態待確認"}；不從瀏覽器啟動／重啟下載器。`);
  const inbox = data.url_metadata_inbox;
  text("url-inbox", inbox
    ? `已核對的收件匣有 ${count(inbox.targets_unexamined)} 項提交（${count(inbox.pending_requests)} 個請求）尚待 owner 檢查；可能與既有網址重複，尚未計入正文。${inbox.invalid_or_unreadable ? ` 另有 ${count(inbox.invalid_or_unreadable)} 個請求檔無法讀取或待查，項目數未知。` : ""}${inbox.completed_unarchived ? ` ${count(inbox.completed_unarchived)} 個已檢查請求待歸檔。` : ""}`
    : "收件匣進度未知；不代表沒有待辦。");
  const finlabPriority = data.finlab_cnyes_priority;
  const finlabPriorityState = finlabPriority ? ({
    unobserved: "已提交，尚無 owner 處理紀錄",
    processing: `owner 已檢查 ${count(finlabPriority.owner_checked)} / ${count(finlabPriority.targets)} 個`,
    checked: `owner 已檢查 ${count(finlabPriority.owner_checked)} / ${count(finlabPriority.targets)} 個`,
    unknown: "owner 收據不一致或無法核對，處理進度未知",
    not_applicable: "沒有符合條件的優先交接網址"
  }[finlabPriority.request_state] || "處理進度未知") : null;
  const finlabPriorityOutcomes = finlabPriority?.outcomes;
  text("finlab-cnyes-priority", finlabPriority
    ? `${live(data) ? "" : "上次快照："}最近一次交接紀錄 ${stamp(finlabPriority.recorded_at_utc)}：比對有 ${count(finlabPriority.raw_unique_delta)} 個原始網址差異，檢查 ${count(finlabPriority.raw_hints_examined)} 個，選出 ${count(finlabPriority.targets)} 個優先目標；${finlabPriorityState}。${finlabPriorityOutcomes ? ` 新增網址 ${count(finlabPriorityOutcomes.metadata_registered)}、保留既有 ${count(finlabPriorityOutcomes.existing_head_preserved)}、來源政策變動 ${count(finlabPriorityOutcomes.source_contract_changed)}、其他 ${count(finlabPriorityOutcomes.other)}${finlabPriority.unverified_origin > 0 ? `（其中 ${count(finlabPriority.unverified_origin)} 個來源檔案或原始列無法核對，已保留網址但未採用日期）` : ""}。` : ""}這是本機交接紀錄時間（中斷後可能為恢復時間）與網址進度，並非新聞發布時間、正文下載或完整歷史進度；完整來源匯入另行處理。`
    : "尚無可核對的交接紀錄；不代表沒有新網址。");
  const retainedHandoff = data.retained_link_handoff;
  const handoffPlan = retainedHandoff?.plan;
  const handoffRun = retainedHandoff?.runtime;
  const handoffReasons = {url_metadata_inbox_full: "網址收件匣已滿，會自動重試", handoff_parent_head_changed: "來源文章已變動，待重新核對",
    handoff_batch_changed: "原始批次或收據已變動，待重新核對", handoff_target_or_policy_changed: "來源政策已變動，待重新核對",
    handoff_proof_digest_changed: "原始驗證證據已變動，待重新核對", handoff_certificate_digest_changed: "交接憑證已變動，待重新核對",
    local_input_unavailable: "本機資料暫時無法讀取，會自動重試", other: "驗證未通過，待重新核對"};
  const handoffState = handoffRun ? ({validating: "正在核對來源", waiting_inbox: "等待收件匣容量", submitted: "已交給網址收件匣，後續由原排程檢查", owner_checked: `原排程已檢查：新增 ${count(handoffRun.outcomes?.metadata_registered)}、保留既有 ${count(handoffRun.outcomes?.existing_head_preserved)}、來源政策變動 ${count(handoffRun.outcomes?.source_contract_changed)}、其他 ${count(handoffRun.outcomes?.other)}`, deferred: "交接暫緩"}[handoffRun.state] || "狀態未知") : "尚無交接執行快照";
  text("retained-link-handoff", handoffPlan
    ? `本機已核對 ${count(handoffPlan.candidates)} 個候選網址、${count(handoffPlan.requests)} 批；${handoffRun && !live(data) ? "上次快照：" : ""}${handoffState}${handoffRun?.reason ? `（${handoffReasons[handoffRun.reason] || "待重新核對"}）` : ""}。候選不代表已取得正文，完成時間尚無可核實估計。`
    : retainedHandoff ? "尚無可核對的候選交接憑證；不代表沒有待辦。"
    : "面板服務尚未載入候選交接資訊；目前進度未知。");
  const diagnostics = data.candidate_diagnostic_progress;
  const localRetry = diagnostics?.local_retry;
  const localRetryCount = localRetry ? localRetry.request_io + localRetry.quarantine + localRetry.archive : 0;
  text("candidate-inbox", diagnostics
    ? `待檢查提交 ${count(diagnostics.pending_unadmitted)} 個 · 已排 ${count(diagnostics.jobs.ready)} · 處理中 ${count(diagnostics.jobs.reserved)} · 前次預留待確認 ${count(diagnostics.jobs.orphaned_reservation)} · 結果不確定 ${count(diagnostics.jobs.uncertain_outcome)}；診斷並非正文。${diagnostics.invalid_or_unreadable ? ` 另有 ${count(diagnostics.invalid_or_unreadable)} 個請求檔無法讀取或待查。` : ""}${localRetryCount ? ` 本機讀取 ${count(localRetry.request_io)}、隔離 ${count(localRetry.quarantine)}、封存 ${count(localRetry.archive)} 個待重新處理；這不是來源錯誤。${live(data) ? "" : "這是上次快照。"}` : ""}`
    : "診斷工作狀態未知；不代表沒有待辦。");
  text("association-count", `${count(acquisition.metadata_association_rows)} 筆（不是唯一文章數）`);
  text("disabled-bulk", count(acquisition.disabled_bulk_pages));
  text("updated-at", `面板 ${stamp(data.generated_at_utc)} · 來源 ${stamp(data.observed_at_utc)}`);
  const blocked = data.providers.filter(source => source.enabled && (["cooldown", "attention"].includes(source.state)
    || source.robots_origin_policy_pause?.active_origin_count > 0 || source.discovery_errors?.active_errors > 0));
  const stale = !live(data);
  $("pipeline-alert").hidden = !stale && blocked.length === 0;
  text("alert-title", stale ? labels[data.health] || "資料不可用" : `${count(blocked.length)} 個已啟用來源需要注意`);
  $("alert-details").hidden = stale || blocked.length === 0;
  text("alert-details-label", `查看 ${count(blocked.length)} 個來源的原因與重試時間`);
  text("alert-source-copy", blocked.map(source => `${source.name}：${errorLabel(source)}`).join("\n\n"));
  text("alert-copy", initializing
    ? `${data.health === "initializing_stale" ? "程序仍存在，但進度觀測已過期。" : "正在恢復本機資料。"}數量沿用前次快照，不使用前程序速度估時。${lifecycle?.receipts_decoded != null ? `本次已檢查 ${count(lifecycle.receipts_decoded)} 份收據；不是新增新聞篇數。` : ""}`
    : stale ? "保留既有數量；停止、排空、本機等待或觀測過期時不提供有效完成估時。" : "下載仍按各來源規則平行進行；來源暫停、資料待修與存取限制分開保留，展開可查完整原因。");
  renderSources(data); renderTraffic(data); renderStages(data); renderOriginalURLs(data);
  scheduleLocalWaitExpiry(data);
  return data;
}

function showSnapshotSections(visible) {
  // A first response may fail partway through rendering. Hide all copied and
  // original data until a complete snapshot can be rendered successfully.
  document.body.classList.toggle("news-snapshot-unavailable", !visible);
  for (const id of ["corpus-quality", "sort-day-quality", "acq-overview", "backfill", "traffic", "pipelines", "capture"]) {
    const section = $(id);
    if (section) section.hidden = !visible;
  }
}

function showRenderedSnapshot(data) {
  if (!document.body.classList.contains("news-snapshot-unavailable")) {
    showSnapshotSections(true);
    return;
  }
  // The acquisition hub mirrors source text on the next animation frame.
  // Keep it hidden until that copy has caught up, and do not reveal a
  // superseded snapshot if another refresh finished in the meantime.
  requestAnimationFrame(() => requestAnimationFrame(() => {
    if (state.latest === data) showSnapshotSections(true);
  }));
}

function staleSnapshot(data) {
  return {...data, health: "stale", providers: data.providers.map(source =>
    source.enabled ? {...source, state: "stale"} : source)};
}

function staleLocalWait(data) {
  return {...data, health: "waiting_local_database_stale", providers: data.providers.map(source =>
    source.state === "waiting_local_database" ? {...source, state: "waiting_local_database_stale"} : source)};
}

function localWaitRemainingMs(data) {
  const ttl = data.worker?.local_sqlite_wait_max_age_seconds;
  const age = Date.now() - Date.parse(data.worker?.lifecycle?.observed_at_utc);
  // An absent or invalid server contract cannot establish current waiting.
  if (!Number.isSafeInteger(ttl) || ttl < 1 || ttl > 3600 || !Number.isFinite(age) || age < -5000) return 0;
  return ttl * 1000 - age;
}

function scheduleLocalWaitExpiry(data) {
  if (state.localWaitTimer !== null) clearTimeout(state.localWaitTimer);
  state.localWaitTimer = null;
  if (data.health !== "waiting_local_database") return;
  // Expiration must not depend on the status fetch finishing. The identity
  // fence keeps a delayed timer from overwriting a newer server snapshot.
  state.localWaitTimer = setTimeout(() => {
    state.localWaitTimer = null;
    if (state.latest !== data) return;
    showRenderedSnapshot(render(data));
  }, Math.max(1, localWaitRemainingMs(data) + 1));
}

async function refresh() {
  if (document.hidden || state.inFlight) return;
  state.inFlight = true;
  let previous = state.latest;
  const previousFilter = state.filter, previousPage = state.page;
  try {
    // A tab may return after its observed live snapshot has expired. Withdraw
    // live claims before waiting for a potentially slow status response.
    if (previous && ["running", "idle", "waiting", "inventorying"].includes(previous.health)
        && !currentSnapshot(previous)) {
      previous = staleSnapshot(previous);
      showRenderedSnapshot(render(previous));
    }
    if (previous?.health === "waiting_local_database" && localWaitRemainingMs(previous) <= 0) {
      previous = staleLocalWait(previous);
      showRenderedSnapshot(render(previous));
    }
    if (previous && Date.now() - Date.parse(previous.generated_at_utc) > 45000) {
      showRenderedSnapshot(render(previous));
    }
    const data = await fetchJson("api/status");
    showRenderedSnapshot(render(data));
    if (state.retry) clearTimeout(state.retry);
    state.retry = null; state.retryDelay = 3000;
  } catch (_error) {
    state.filter = previousFilter; state.page = previousPage;
    if (previous) {
      // Re-render the whole last successful snapshot so every live-only field,
      // including the acquisition summary mirror, is withdrawn together.
      const stale = {...staleSnapshot(previous), corpus_quality: {state: "unavailable"},
        corpus_headline_quality: {state: "unavailable"}, sort_day_quality: {state: "unavailable"}};
      showRenderedSnapshot(render(stale));
      text("eligible-eta", "更新失敗，暫無估時");
      text("eligible-eta-basis", "快照更新失敗；保留上次數量，不以舊速率估算。");
    } else {
      state.latest = null;
      showSnapshotSections(false);
      text("updated-at", "尚未取得觀測");
    }
    const badge = $("news-health"); badge.className = `status ${previous ? "stale" : "unavailable"}`;
    badge.lastChild.textContent = previous ? "更新暫停，顯示上次資料" : "面板暫時無法讀取";
    text("news-freshness", "稍後自動重試；不呼叫新聞來源補畫面。");
    $("pipeline-alert").hidden = false;
    $("alert-details").hidden = true;
    text("alert-title", "快照更新失敗");
    text("alert-copy", previous ? "保留最後成功資料與篩選；估時暫停，不把舊資料當成最新。"
      : "尚無可顯示的有效資料；稍後會自動重試。");
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
$("pipeline-grid").addEventListener("toggle", event => {
  if (event.target.isConnected && event.target.matches?.('[data-news-list-provider="cnyes"]')) state.cnyesListOpen = event.target.open;
  if (event.target.isConnected && event.target.matches?.('[data-news-feed-provider]')) {
    const provider = event.target.dataset.newsFeedProvider;
    if (event.target.open) state.feedOpen.add(provider);
    else state.feedOpen.delete(provider);
  }
  if (event.target.isConnected && event.target.matches?.('[data-news-discovery-provider]')) {
    const provider = event.target.dataset.newsDiscoveryProvider;
    if (event.target.open) state.discoveryErrorOpen.add(provider);
    else state.discoveryErrorOpen.delete(provider);
  }
  if (event.target.isConnected && event.target.matches?.('[data-news-outcome-provider]')) {
    const provider = event.target.dataset.newsOutcomeProvider;
    if (event.target.open) state.outcomeOpen.add(provider);
    else state.outcomeOpen.delete(provider);
  }
}, true);
renderSources = Dashboard.createDeferredRenderer("pipelines", renderSources);
renderTraffic = Dashboard.createDeferredRenderer("traffic", renderTraffic);
renderStages = Dashboard.createDeferredRenderer("capture", renderStages);
Dashboard.scheduleRefresh(refresh, {intervalMs: 15000});

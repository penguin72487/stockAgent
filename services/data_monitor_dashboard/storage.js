/* Metadata-only storage explorer. No browser or API action controls a machine. */
(() => {
  "use strict";
  const section = document.getElementById("storage-inventory");
  if (!section) return;
  const core = window.StockAgentDashboard;
  const fetchJson = core.createJsonFetcher({timeoutMs: 12000, cache: "no-store", expectedRoot: "object"});
  const requests = core.createLatestRequest();
  const $ = id => section.querySelector(`#${id}`);
  function revealStorageAnchor() {
    if (!["#storage-inventory", "#storage-operations", "#acq-storage"].includes(window.location.hash)) return;
    const details = section.closest("details");
    if (details) details.open = true;
  }
  window.addEventListener("hashchange", revealStorageAnchor);
  document.addEventListener("DOMContentLoaded", revealStorageAnchor, {once: true});
  revealStorageAnchor();
  const make = (tag, className = "", text) => {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = String(text);
    return node;
  };
  const known = n => Number.isSafeInteger(n) && n >= 0;
  function bytes(n) {
    if (!known(n)) return "尚未量測";
    const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    const index = n ? Math.min(units.length - 1, Math.floor(Math.log(n) / Math.log(1024))) : 0;
    return `${new Intl.NumberFormat("zh-TW", {maximumFractionDigits: index ? 2 : 0}).format(n / 1024 ** index)} ${units[index]}`;
  }
  const measuredBytes = row => `${row.allocated_bytes_is_lower_bound ? "≥ " : ""}${bytes(row.allocated_bytes)}`;
  const clock = new Intl.DateTimeFormat("zh-TW", {timeZone: "Asia/Taipei", hour12: false,
    year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit"});
  const date = value => {
    const d = new Date(value || "");
    return Number.isNaN(d.getTime()) ? "尚未量測" : clock.format(d);
  };
  const states = {measured: "已實測", partial: "僅部分資訊", stale: "觀測已過期", unavailable: "本次無法讀取", unknown: "等待量測"};
  const kinds = {project: "專案", source: "來源資料", cache: "快取／工作副本", runtime: "執行服務",
    materialized: "還原副本", control: "控制與收據", cold: "封閉冷庫", results: "研究與訓練", other: "其他"};
  const state = {data: null, selected: null, path: [], visible: 12, loaded: false, inFlight: false};
  const operationList = {rows: [], open: new Set(), visible: 100};
  const volumes = () => (state.data?.nodes || []).flatMap(node => node.volumes);
  const percentage = (a, b) => known(a) && known(b) && b > 0 ? Math.min(100, Math.max(0, a / b * 100)) : null;
  function bar(amount, total, label) {
    const node = make("div", "storage-bar");
    const pct = percentage(amount, total);
    node.setAttribute("role", "img");
    node.setAttribute("aria-label", pct === null ? `${label}：尚未量測` : `${label}：${pct.toFixed(1)}%`);
    if (pct !== null) {
      const fill = make("span", "storage-bar-fill");
      fill.style.width = `${pct}%`;
      node.append(fill);
    } else node.classList.add("storage-bar-unknown");
    return node;
  }
  function card(volume) {
    const cap = volume.capacity;
    const article = make("article", "storage-disk");
    article.dataset.volume = volume.id;
    const pct = percentage(cap.used_bytes, cap.total_bytes);
    const writable = volume.writable_headroom;
    const host = volume.host_storage_accounting;
    const physical = writable?.state === "measured" && known(writable.available_bytes);
    const low = cap.complete && known(cap.available_bytes) && cap.available_bytes / cap.total_bytes < .1
      || physical && writable.available_bytes < 25 * 1024 ** 3;
    if (low) article.classList.add("storage-pressure");
    const heading = make("div", "storage-disk-heading");
    heading.append(make("h4", "", volume.label), make("span", `storage-state storage-state-${cap.state}`, states[cap.state] || "等待量測"));
    const size = make("p", "storage-size", writable ? bytes(cap.available_bytes)
      : cap.complete ? bytes(cap.used_bytes) : bytes(cap.available_bytes));
    size.append(make("small", "", writable ? " WSL 檔案系統可用"
      : cap.complete ? ` 已用／${bytes(cap.total_bytes)}` : " 可用；總容量待量測"));
    const fact = make("p", "storage-copy", cap.complete
      ? `可用 ${bytes(cap.available_bytes)} · 使用率 ${pct.toFixed(1)}%${known(cap.reserved_free_bytes) && cap.reserved_free_bytes > 0 ? ` · 系統保留 ${bytes(cap.reserved_free_bytes)}` : ""}`
      : "缺少整體容量資訊，暫不計算使用率。");
    const measure = make("p", "storage-clock", `容量量測 ${date(cap.observed_at_utc)}（台北）`);
    const relation = make("p", "storage-copy", volume.role);
    if (volume.contained_in) relation.append(make("span", "storage-containment", `包含於 ${volumes().find(v => v.id === volume.contained_in)?.label || "Windows 磁碟"}，不可加總`));
    if (writable) relation.append(make("span", "storage-containment", physical
      ? `主機新增配置的保守寫入餘量 ${bytes(writable.available_bytes)}；與 WSL 內部可用空間分開計算，未扣服務保留。`
      : "缺少近期 WSL 或 Windows 所在磁碟量測，保守寫入餘量未知。"));
    if (host?.state === "measured") {
      relation.append(make("span", "storage-containment",
        `WSL 已占實體空間 ${bytes(host.current_wsl_allocated_bytes)} · 其他 WSL ${bytes(host.other_wsl_allocated_bytes)}`));
      relation.append(make("span", "storage-containment",
        `Windows／其他檔案 ${bytes(host.windows_and_other_used_bytes)} · 主機剩餘 ${bytes(host.physical_free_bytes)}`));
      relation.append(make("span", "storage-containment",
        `主機可分配給此 WSL 的總空間 ${bytes(host.current_wsl_capacity_ceiling_bytes)}（含目前占用；不是額外可用空間）`));
    }
    if (["stale", "unavailable"].includes(cap.state) && cap.observed_at_utc) {
      measure.append(make("span", "storage-stale-note", "保留上次實測值，非目前即時值"));
    }
    const group = volume.classification;
    const button = make("button", "storage-open", group.tree.length ? `查看資料分類 · ${measuredBytes(group)}`
      : group.state === "scanning" ? "背景清點中 · 查看狀態" : "查看分類與量測狀態");
    button.type = "button";
    button.dataset.storageSelect = volume.id;
    button.setAttribute("aria-controls", "storage-explorer");
    button.setAttribute("aria-expanded", String(state.selected === volume.id));
    button.addEventListener("click", () => {
      state.selected = state.selected === volume.id ? null : volume.id;
      state.path = [];
      state.visible = 12;
      syncSelection();
      renderExplorer();
      if (state.selected) $("storage-explorer").scrollIntoView({block: "nearest", behavior: "auto"});
    });
    article.append(heading, size, bar(cap.used_bytes, cap.total_bytes, "磁碟使用率"), fact, relation, measure, button);
    return article;
  }
  function syncSelection() {
    section.querySelectorAll("[data-storage-select]").forEach(button => {
      const selected = button.dataset.storageSelect === state.selected;
      button.setAttribute("aria-expanded", String(selected));
      button.closest(".storage-disk").classList.toggle("storage-selected", selected);
    });
  }
  const operationStates = {running: "執行中", waiting: "等待中", blocked: "前置條件待處理", completed: "本範圍已驗收", unknown: "等待收據"};
  function prerequisiteLabel(row) {
    const labels = {
      repack_D_capacity: "D 空間待釋放", repack_exclusive_space: "可回收空間不足",
      retirement_pinned: "保留引用中", relief_pinned: "保留引用中",
      source_duplicate_bloat: "去重排程中", source_handoff_root_changed: "來源待固定",
      handoff_index_compared: "資料待合併", handoff_index_binance_union_required: "資料待合併",
      staged_cold_unresolved: "來源待辨識",
      rescue_original_manifest_missing: "原始版本缺件", rescue_registered_manifest_missing: "原始版本缺件",
      receiver_readiness: "等待接收端", receiver_readiness_lock_diagnosis: "等待接收端",
    };
    return labels[row.reason_code] || operationStates.blocked;
  }
  const number = n => known(n) ? new Intl.NumberFormat("zh-TW").format(n) : "未知";
  const duration = seconds => seconds < 60 ? "不到 1 分鐘" : seconds < 3600
    ? `約 ${Math.ceil(seconds / 60)} 分鐘` : seconds < 86400
      ? `約 ${(seconds / 3600).toFixed(1)} 小時` : `約 ${(seconds / 86400).toFixed(1)} 天`;
  function renderRuntime(value, failed = false) {
    const valid = value?.contract === "stockagent.storage_runtime.v1" && value.read_only === true;
    const rows = valid && Array.isArray(value.nodes) ? value.nodes : [
      {id: "penguin", label: "Penguin"}, {id: "lab203", label: "lab203／NAS"}, {id: "vastai1T", label: "Vast"}];
    const open = new Set([...section.querySelectorAll("[data-runtime-detail][open]")].map(node => node.dataset.runtimeDetail));
    const focused = document.activeElement?.closest("[data-runtime-detail]")?.dataset.runtimeDetail;
    const percent = n => typeof n === "number" && Number.isFinite(n) && n >= 0 && n <= 100 ? `${n.toFixed(1)}%` : "待回報";
    const cards = rows.map(row => {
      const article = make("article", "storage-runtime-card");
      article.dataset.runtimeNode = row.id;
      const current = failed ? "unavailable" : row.state || "unknown";
      const labels = {measured: row.id === "lab203" ? "回執在線" : "已連線取樣", stale: "觀測已過期", unavailable: "本次無法讀取", unknown: "等待觀測"};
      const heading = make("div", "storage-disk-heading");
      heading.append(make("h4", "", row.label), make("span", `storage-state storage-state-${current}`, labels[current] || labels.unknown));
      const m = row.metrics || {};
      const facts = [`CPU ${percent(m.cpu_percent)}${known(m.cpu_count) ? ` · ${number(m.cpu_count)} 核` : ""}`,
        `RAM ${known(m.memory_used_bytes) && known(m.memory_total_bytes) ? `${bytes(m.memory_used_bytes)}／${bytes(m.memory_total_bytes)}` : "待接收端回報"}`];
      if (row.id === "vastai1T") {
        facts.push(`GPU ${percent(m.gpu_percent)}${known(m.gpu_count) ? ` · ${number(m.gpu_count)} 張` : ""}`);
        if (typeof m.gpu_memory_used_mb === "number" && typeof m.gpu_memory_total_mb === "number") facts.push(`GPU 記憶體 ${number(Math.round(m.gpu_memory_used_mb))}／${number(Math.round(m.gpu_memory_total_mb))} MB（來源單位）`);
        if (known(m.supervised_running)) facts.push(`Supervisor 已登錄 StockAgent 程序：${number(m.supervised_running)} 執行中${known(m.supervised_waiting) ? ` · ${number(m.supervised_waiting)} 轉換中` : ""}${m.supervised_failed ? ` · ${number(m.supervised_failed)} 異常` : ""}`);
      }
      const details = make("details", "storage-operation-receipt");
      details.dataset.runtimeDetail = row.id;
      details.open = open.has(row.id);
      details.append(make("summary", "", row.id === "penguin" ? "服務與觀測範圍" : "觀測範圍"));
      for (const service of row.services || []) {
        const serviceLabels = {active: "執行中", activating: "啟動／本輪工作中", inactive: "本輪未執行", waiting: "暫緩；等待條件後重試", failed: "最近一輪失敗", deactivating: "停止中", unknown: "無法觀測"};
        details.append(make("p", "storage-copy", `${service.label}：${serviceLabels[service.state] || serviceLabels.unknown}`));
      }
      details.append(make("p", "storage-copy", row.scope || "尚未取得有效的背景觀測。"));
      const failures = (row.services || []).filter(service => service.state === "failed").length;
      article.append(heading, ...facts.map(fact => make("p", "storage-runtime-fact", fact)));
      if (failures) article.append(make("p", "storage-containment", `${number(failures)} 個服務最近一輪失敗，展開查看。`));
      article.append(make("p", "storage-clock", `觀測 ${date(row.observed_at_utc)}（台北）${current !== "measured" && row.observed_at_utc ? " · 保留上次量測，非目前即時值" : ""}`), details);
      return article;
    });
    $("storage-runtime").replaceChildren(...cards);
    if (focused) [...section.querySelectorAll("[data-runtime-detail]")].find(node => node.dataset.runtimeDetail === focused)
      ?.querySelector("summary")?.focus({preventScroll: true});
  }
  function operationCard(row) {
    const article = make("article", "storage-operation-card");
    article.dataset.operation = row.id;
    const heading = make("div", "storage-disk-heading");
    const label = row.complete_evidence ? operationStates.completed
      : row.stale ? "監測尚未更新"
        : row.historical_only === true ? "歷史維護待核對"
        : row.kind === "provider_originals" ? "待驗收回收"
          : !row.observed_at_utc ? "等待原始收據"
          : row.owner_state === "failed" ? "原執行者異常"
            : row.eta?.reason === "clock_ahead" ? "來源時鐘待核對"
            : row.state === "blocked" ? prerequisiteLabel(row)
              : row.owner_state === "inactive" ? "等待原排程"
                : row.progress_stale ? "等待新進度計數"
                  : operationStates[row.state] || operationStates.unknown;
    const operationLabel = row.kind === "workspace_e_resize" ? "E 擴容與 C 整理的原維護作業" : row.label;
    heading.append(make("h4", "", operationLabel), make("span", `storage-state storage-operation-${row.state}`, label));
    article.append(heading, make("p", "storage-operation-phase", row.phase_label));
    if (row.kind === "workspace_source_handoff" && row.phase === "source_handoff_unenrolled") {
      article.append(make("p", "storage-copy", "所列資料量是固定 E 複本的歷史工作量，可能包含已分批處理的子資料集；不等於目前 C 槽佔用或待回收量。"));
    }
    if (row.kind === "workspace_e_resize") {
      const current = volumes().find(volume => volume.id === "penguin-workspace")?.capacity;
      if (current?.state === "measured" && known(current.total_bytes) && known(current.available_bytes)) {
        article.append(make("p", "storage-copy",
          `目前 E 工作卷已掛載，檔案系統容量 ${bytes(current.total_bytes)}、可用 ${bytes(current.available_bytes)}；檔案系統容量與虛擬磁碟大小不同。這是 ${date(current.observed_at_utc)} 的獨立觀測。上次聯合維護的失敗收據仍保留，不代表目前工作卷未掛載。`));
      }
    }
    const binanceAfterClose = !row.stale && row.kind === "workspace_source_handoff"
      && row.state === "waiting" && row.next_attempt_at_utc
      && ["Binance 市場與衍生資料", "Binance 公開歷史"].some(label => row.label.endsWith(label));
    const displayedReason = row.reason && ["waiting", "blocked"].includes(row.state) && !row.complete_evidence;
    if (displayedReason) {
      article.append(make("p", "storage-copy storage-operation-reason", row.reason));
    } else if (binanceAfterClose) {
      article.append(make("p", "storage-copy storage-operation-reason", "大型來源核對配合當沖優先時段，盤後才開始。"));
    }
    if (row.failure_observed_at_utc && !row.complete_evidence) {
      article.append(make("p", "storage-clock", `例外狀態觀測 ${date(row.failure_observed_at_utc)}（台北）；不是新增工作進度或驗收。`));
    }
    if (!row.complete_evidence && row.previous_source_failure?.message) {
      article.append(make("p", "storage-copy storage-operation-reason", row.previous_source_failure.message));
    }
    if (row.diagnosis_at_utc && !row.complete_evidence) {
      const description = row.kind === "lake_relay"
        ? `最近一次驗證失敗 ${date(row.diagnosis_at_utc)}（台北）；不代表已重新通過。`
        : `最近一次鎖診斷 ${date(row.diagnosis_at_utc)}（台北）；不代表此刻仍持鎖。`;
      article.append(make("p", "storage-clock", description));
    }
    const p = row.progress || {};
    const m = row.metrics || {};
    const parts = [];
    if (row.kind === "foreign_head_handoff" && known(m.foreign_head_candidates)) {
      parts.push(`本輪候選 ${number(m.foreign_head_candidates)} 個（非全系統總數） · 累計 ${number(m.foreign_head_completed)} 個指標交接完成 · ${number(m.foreign_head_pending)} 個等待確認或復原`);
    }
    if (row.kind === "v3_reader_activation" && known(m.v3_candidates)) {
      parts.push(`本輪 ${number(m.v3_candidates)} 個 E 候選 · ${number(m.v3_activation_errors)} 個等待重試${known(m.v3_activated_last) ? ` · 最近一次啟用 ${number(m.v3_activated_last)} 個` : ""}`);
    }
    if (row.kind === "edge_reader_recovery" && known(m.edge_reader_last_recovered_epoch)) {
      parts.push(`最近一次程式恢復：${date(m.edge_reader_last_recovered_epoch * 1000)}`);
    }
    if (row.kind === "cold_scan_notification") {
      const waiting = known(m.cold_scan_pending) ? `${number(m.cold_scan_pending)} 個通知待處理` : "待處理數尚未量測";
      const last = known(m.cold_scan_last_acks) ? ` · 最近一輪接受 ${number(m.cold_scan_last_acks)} 個通知` : "";
      const elapsed = known(m.cold_scan_last_elapsed_seconds) ? `，耗時 ${number(m.cold_scan_last_elapsed_seconds)} 秒` : "";
      parts.push(`${waiting}${last}${elapsed}；新增封裝會增加佇列`);
    }
    if (row.kind === "remote_return" && known(m.remote_observed) && known(m.remote_eligible)
        && known(m.remote_published) && known(m.remote_cycle_deferred)) {
      parts.push(`最近完成一輪${known(m.remote_summary_epoch) ? `（${date(m.remote_summary_epoch * 1000)}）` : ""}：掃描 ${number(m.remote_observed)} 個成果，${number(m.remote_eligible)} 個符合該輪條件，發布 ${number(m.remote_published)} 個，${number(m.remote_cycle_deferred)} 個當時未輪到`);
      if (known(m.remote_pending_confirmation) && known(m.remote_confirmation_limit)) {
        parts.push(`待傳輸確認 ${number(m.remote_pending_confirmation)}／${number(m.remote_confirmation_limit)}；遠端保留與 NAS 驗收另計`);
      }
      if (known(m.remote_deferred_recovery)) {
        parts.push(`已與計算節點同步、暫緩額外還原 ${number(m.remote_deferred_recovery)} 項；固定身分保留、未執行遠端刪除，尚未完成還原驗收`);
      }
      if (known(m.remote_cache_available_bytes) && known(m.remote_cache_threshold_bytes)
          && known(m.remote_cache_observed_epoch) && known(m.remote_cache_reclaimed_bytes)) {
        parts.push(`Vast 暫存清理 ${date(m.remote_cache_observed_epoch * 1000)}：可用 ${bytes(m.remote_cache_available_bytes)}，低於 ${bytes(m.remote_cache_threshold_bytes)} 時檢查可安全清理的下載快取；本次清理 ${bytes(m.remote_cache_reclaimed_bytes)}`);
        if (m.remote_cache_profile_issues > 0) parts.push(`${number(m.remote_cache_profile_issues)} 類快取忙碌或無法檢查，等待下次排程`);
      }
      if (known(m.remote_cache_failure_epoch)) parts.push(`Vast 暫存清理 ${date(m.remote_cache_failure_epoch * 1000)} 未完成檢查；原排程會重試`);
    }
    if (row.kind === "staged_cold_attribution" && known(p.completed_items)) {
      if (known(p.total_items)) {
        parts.push(`既有清冊引用 ${number(p.completed_items)}／${number(p.total_items)} 個物件 · 對應 ${bytes(p.completed_bytes)}／${bytes(p.total_bytes)}；不代表已還原或可刪除`);
      } else {
        parts.push(`既有清冊引用 ${number(p.completed_items)} 個物件 · 對應 ${bytes(p.completed_bytes)}；總數待清點，不代表已還原或可刪除`);
      }
    } else {
      if (known(p.completed_bytes) && known(p.total_bytes)) parts.push(`${bytes(p.completed_bytes)}／${bytes(p.total_bytes)}`);
      if (!known(p.completed_bytes) && known(p.total_bytes)) parts.push(`總量 ${bytes(p.total_bytes)}，等待進度計數`);
      if (known(p.completed_bytes) && !known(p.total_bytes)) parts.push(`${row.phase === "censusing_source" ? "已清點" : "已處理"} ${bytes(p.completed_bytes)}（總量待清點）`);
      if (known(p.completed_bytes) && known(p.total_bytes) && p.completed_bytes < p.total_bytes) parts.push(`剩 ${bytes(p.total_bytes - p.completed_bytes)}`);
      if (known(p.completed_items) && known(p.total_items)) parts.push(`${number(p.completed_items)}／${number(p.total_items)} ${row.phase === "snapshotting_sqlite_state" ? "SQLite 頁" : row.phase === "source_local_dedup_packing" ? "份唯一內容" : ["cooperative", "retained_verification", "vast_spool"].includes(row.kind) ? "packs" : "項"}`);
    }
    if (!known(p.completed_items) && known(p.total_items)) parts.push(`共 ${number(p.total_items)} 項`);
    if (row.kind !== "staged_cold_attribution" && known(p.completed_items) && !known(p.total_items)) parts.push(`已處理 ${number(p.completed_items)} 項（總數待清點）`);
    if (known(m.namespace_pass) && known(m.namespace_passes_total)) parts.push(
      `第 ${number(m.namespace_pass)}／${number(m.namespace_passes_total)} 輪來源與副本清冊核對；估時僅含本輪`);
    if (known(m.sqlite_state_index) && known(m.sqlite_states_total)) parts.push(`SQLite 狀態 ${number(m.sqlite_state_index)}／${number(m.sqlite_states_total)}`);
    if (row.phase === "source_local_dedup_packing") parts.push("依固定清冊的 SHA 與長度，每份內容讀取一次；尚未發布 D 版本");
    if (m.dedup_native_fallbacks > 0) parts.push(`原生選取讀取曾退回既有方式 ${number(m.dedup_native_fallbacks)} 次；來源與已完成分包保留`);
    if (known(m.E_legacy_reclaimed_allocated_bytes)) parts.push(`已回收 E 舊封裝配置 ${bytes(m.E_legacy_reclaimed_allocated_bytes)}；與 WSL 或 D 槽回收分開計算`);
    if (row.kind === "staged_cold_attribution" && m.staged_deferred_batches > 0) {
      parts.push(`已清點 ${number(m.staged_known_subset_objects)} 個物件；另有 ${number(m.staged_deferred_batches)} 批未完成清點`);
    }
    if (row.kind === "staged_cold_attribution" && known(m.staged_index_matched_objects)
        && known(m.staged_index_new_objects) && known(m.staged_index_observed_epoch)) {
      parts.push(`${date(m.staged_index_observed_epoch * 1000)} 固定歷史傳輸索引比對：${number(m.staged_index_matched_objects)} 個物件相符，其中 ${number(m.staged_index_new_objects)} 個原本未由本機清冊引用；這是來源線索，不代表目前 NAS 讀回或可刪除${m.staged_index_unavailable === 1 ? "，最新索引檢查暫不可用" : ""}。`);
    }
    if (known(m.source_equal_items) && known(m.source_conflict_items)) {
      const status = row.phase === "handoff_index_public_latest_selected"
        ? "已選用較新來源；舊世代數值尚未逐筆比較"
        : row.complete_evidence ? "公開世代已整合，本機限定來源另保留" : "差異尚未合併";
      parts.push(`世代清冊：相同 ${number(m.source_equal_items)} 項、現行獨有 ${number(m.source_public_only_items || 0)} 項、舊來源獨有 ${number(m.source_private_only_items || 0)} 項、內容不同 ${number(m.source_conflict_items)} 項、本機限定 ${number(m.source_excluded_items || 0)} 項；${status}`);
    }
    if (known(m.uncompared_changed_members)) parts.push(`原世代有 ${number(m.uncompared_changed_members)} 個不同檔案尚未逐筆比較；舊資料的歷史值覆蓋範圍未知`);
    if (m.duplicate_encoded_bytes_pending > 0) parts.push(`另有 ${bytes(m.duplicate_encoded_bytes_pending)} 同內容封包仍是兩份實體檔，等待去重`);
    if (row.kind === "staged_cold_attribution" && known(m.archive_prefix_candidates)
        && known(m.archive_mixed_or_unknown)) {
      parts.push(`ZIP 名稱清點：單一路徑前綴候選 ${number(m.archive_prefix_candidates)} 個、混合或未知 ${number(m.archive_mixed_or_unknown)} 個；前綴不是來源證明`);
    }
    if (known(m.D_required_bytes) && known(m.D_observed_free_bytes) && known(m.D_reserve_bytes)
        && known(m.D_shortfall_bytes) && known(m.D_capacity_observed_epoch)) {
      parts.push(`${date(m.D_capacity_observed_epoch * 1000)} ${m.D_capacity_live === 1 ? "容量量測" : "上次嘗試"}：需寫入 ${bytes(m.D_required_bytes)}、D 可用 ${bytes(m.D_observed_free_bytes)}、保留 ${bytes(m.D_reserve_bytes)}，尚缺 ${bytes(m.D_shortfall_bytes)}；實際寫入前會重新檢查`);
    }
    if (known(m.D_exclusive_allocated_bytes) && known(m.D_relief_shortfall_bytes) && known(m.relief_observed_at_epoch))
      parts.push(`${date(m.relief_observed_at_epoch * 1000)} 檢查：本批符合條件的舊物件約可釋放 ${bytes(m.D_exclusive_allocated_bytes)}，當時需釋放 ${bytes(m.D_relief_shortfall_bytes)}；這是當次估計，會重新檢查`);
    if (m.D_old_head_temporarily_unavailable === 1)
      parts.push("舊 D 版本的部分物件已移除；資料暫存在 E，D 新封裝提交前舊版本可能無法完整讀取");
    if (known(m.D_bulk_carriers_reclaimed) && known(m.D_bulk_carrier_reclaimed_allocated_bytes))
      parts.push(`${m.D_bulk_carrier_summary_incomplete === 1 ? "至少 " : ""}${number(m.D_bulk_carriers_reclaimed)} 份舊回傳整包已回收，估計釋出 D 配置 ${bytes(m.D_bulk_carrier_reclaimed_allocated_bytes)}；並非整個 D 的淨增量，也不是 NAS 驗收`);
    if (known(m.D_bulk_landing_parts_reclaimed) && known(m.D_bulk_landing_parts_reclaimed_allocated_bytes))
      parts.push(`${m.D_bulk_carrier_summary_incomplete === 1 ? "至少 " : ""}${number(m.D_bulk_landing_parts_reclaimed)} 個舊回傳分段已回收，估計釋出 D 配置 ${bytes(m.D_bulk_landing_parts_reclaimed_allocated_bytes)}；不代表 NAS 驗收`);
    if (known(m.staging_e_write_bytes)) parts.push(`搬移程序累計向 E 寫入 ${bytes(m.staging_e_write_bytes)}（可能包含其他資料，尚未逐檔驗證）`);
    if (known(m.guest_e_capacity_bytes) && known(m.guest_e_capacity_observed_epoch))
      parts.push(`${date(m.guest_e_capacity_observed_epoch * 1000)} 量測：E 區塊裝置 ${bytes(m.guest_e_capacity_bytes)}${known(m.guest_e_filesystem_capacity_bytes) ? `，檔案系統 ${bytes(m.guest_e_filesystem_capacity_bytes)}` : ""}；先前 Windows 維護與服務恢復另計`);
    if (known(m.validation_cache_rebound))
      parts.push(`自動作業已續用 ${number(m.validation_cache_rebound)} 筆快取；原驗證器仍檢查檔案世代，分鐘資料總進度另計`);
    if (known(m.seed_transferred_bytes)) parts.push(`本次 rsync 傳送 ${bytes(m.seed_transferred_bytes)}${known(m.seed_source_logical_bytes) ? `；固定來源邏輯量 ${bytes(m.seed_source_logical_bytes)}` : ""}（傳送量不等於已驗證量）`);
    if (known(m.seed_checkpoint_files) && known(m.seed_checkpoint_total_files) && known(m.seed_checkpoint_epoch)) {
      parts.push(`重啟前持久檢查點 ${number(m.seed_checkpoint_files)}／${number(m.seed_checkpoint_total_files)} 檔${known(m.seed_checkpoint_bytes) && known(m.seed_checkpoint_total_bytes) ? ` · ${bytes(m.seed_checkpoint_bytes)}／${bytes(m.seed_checkpoint_total_bytes)}` : ""}（${date(m.seed_checkpoint_epoch * 1000)} 讀取；仍需本輪重新核對，不是完成驗收）`);
    }
    if (known(m.inventory_exact_files)) parts.push(`固定清冊：相同 ${number(m.inventory_exact_files)} 檔、變更 ${number(m.inventory_changed_files || 0)} 檔、僅 E ${number(m.inventory_e_only_files || 0)} 檔、僅 D ${number(m.inventory_d_only_files || 0)} 檔；還原驗收另計`);
    if (known(m.inventory_exact_symlinks)) parts.push(`連結目標：相同 ${number(m.inventory_exact_symlinks)} 個、不同 ${number(m.inventory_changed_symlinks || 0)} 個、僅 E ${number(m.inventory_e_only_symlinks || 0)} 個、僅 D ${number(m.inventory_d_only_symlinks || 0)} 個`);
    if (known(m.inventory_exact_bytes) && known(m.inventory_changed_bytes) && known(m.inventory_e_only_bytes)) {
      parts.push(`整檔邏輯量：相同 ${bytes(m.inventory_exact_bytes)}、變更 ${bytes(m.inventory_changed_bytes)}、僅 E ${bytes(m.inventory_e_only_bytes)}；去重後實際容量待估`);
    }
    if (known(m.cftc_raw_restored_files) && known(m.cftc_raw_restored_bytes) && known(m.cftc_raw_restored_epoch)) parts.push(`D 固定版本的原始 ZIP 已於 ${date(m.cftc_raw_restored_epoch * 1000)} 讀回 E：${number(m.cftc_raw_restored_files)} 檔／${bytes(m.cftc_raw_restored_bytes)}；NAS 驗收另計`);
    if (known(m.reclaimed_allocated_bytes)) parts.push(row.kind === "workspace_c_compaction"
      ? `C 槽 VHDX 實體配置減少 ${bytes(m.reclaimed_allocated_bytes)}`
      : `估計回收 WSL 配置 ${bytes(m.reclaimed_allocated_bytes)}；淨可用量仍受其他寫入影響`);
    if (known(m.estimated_reclaimed_allocated_bytes)) parts.push(`估計釋放 D ${bytes(m.estimated_reclaimed_allocated_bytes)}；仍有其他 hardlink 的物件不計入`);
    if (known(m.history_cumulative_reclaimed_bytes) && known(m.history_cumulative_versions))
      parts.push(`歷史回收已完成 ${number(m.history_cumulative_versions)} 個版本，累計估計釋放 D ${bytes(m.history_cumulative_reclaimed_bytes)}；這是已完成批次，非目前執行進度`);
    if (m.history_anchor_missing === 1)
      parts.push("舊回收工作的中繼版本已不存在；本次只確認現行清冊沒有其他歷史版本，未證明世代後繼或舊資料可還原");
    if (known(m.discarded_allocated_bytes) && row.kind === "local_testing_cleanup") parts.push(`已刪除本機測試副本，回收 WSL 配置 ${bytes(m.discarded_allocated_bytes)}`);
    if (known(m.discarded_allocated_bytes) && row.kind === "d_testing_cleanup") parts.push(`已刪除舊測試副本，回收 D 配置 ${bytes(m.discarded_allocated_bytes)}`);
    if (known(m.discarded_allocated_bytes) && row.kind === "vast_testing_cleanup") parts.push(`已刪除指定舊測試副本，回收 Vast 配置 ${bytes(m.discarded_allocated_bytes)}；不代表 D 或 NAS 驗收`);
    if (row.kind === "training_rescue_capture" && known(m.D_observed_free_bytes) && known(m.D_reserve_bytes))
      parts.push(`上次保全排程觀察 D 可用 ${bytes(m.D_observed_free_bytes)}，保留門檻 ${bytes(m.D_reserve_bytes)}`);
    if (row.kind === "training_rescue_references" && known(m.rescue_protected_bindings))
      parts.push(`現有 ${number(m.rescue_protected_bindings)} 個固定來源身分受保護${known(m.rescue_last_reconciled_epoch) ? `；上次完成盤點 ${date(m.rescue_last_reconciled_epoch * 1000)}` : ""}`);
    if (row.kind === "workspace_transport_comparison") {
      if (known(m.same_files)) parts.push(`一致 ${number(m.same_files)} 檔`);
      if (known(m.different_files)) parts.push(`內容不同 ${number(m.different_files)} 檔`);
      if (known(m.missing_files)) parts.push(`E 未找到 ${number(m.missing_files)} 檔`);
      if (known(m.unstable_files) && m.unstable_files) parts.push(`讀取中變動 ${number(m.unstable_files)} 檔`);
      if (known(m.unavailable_files) && m.unavailable_files) parts.push(`無法確認 ${number(m.unavailable_files)} 檔`);
    }
    if (row.kind === "workspace_transport" && known(m.stale_mount_namespaces) && m.stale_mount_namespaces) parts.push(`尚有 ${number(m.stale_mount_namespaces)} 個舊掛載命名空間`);
    if (row.kind === "workspace_partial_cold_readback" && known(m.partial_exact_full_files)
        && known(m.partial_empty_files) && known(m.partial_cold_bytes)) {
      parts.push(`D 正本已完整讀回 ${bytes(m.partial_cold_bytes)}；${number(m.partial_exact_full_files)} 個暫存與完整物件相同、${number(m.partial_empty_files)} 個空檔；暫存回收與 NAS 驗收另計`);
    }
    if (known(m.files_held) && m.files_held) parts.push(`保留 ${number(m.files_held)} 檔待核對`);
    if (row.kind === "packed_archive_placement" || row.kind === "packed_archive_total") {
      if (m.archive_selected_historical === 1) parts.push("本輪優先搬移仍需保留的舊封裝，原版本與內容不變");
      if (known(m.archive_placed_bytes) && known(m.archive_head_bytes)) parts.push(`本輪開始時，E 已有 ${bytes(m.archive_placed_bytes)}／${bytes(m.archive_head_bytes)}`);
      if (known(m.archive_D_reclaimed_bytes) && m.archive_D_reclaimed_bytes) parts.push(`已回收 D 配置空間 ${bytes(m.archive_D_reclaimed_bytes)}`);
      if (known(m.archive_shared_D_objects) && m.archive_shared_D_objects) parts.push(`${number(m.archive_shared_D_objects)} 個共用物件保留 D`);
    }
    if (row.kind === "ordinary_archive") {
      if (known(m.ordinary_history_versions_removed)) parts.push(`已回收 ${number(m.ordinary_history_versions_removed)} 個非研究舊版本`);
      if (known(m.ordinary_history_reclaimed_bytes)) parts.push(`D 已釋放配置空間 ${bytes(m.ordinary_history_reclaimed_bytes)}`);
      if (known(m.ordinary_history_coverage_protected) && m.ordinary_history_coverage_protected) parts.push(`${number(m.ordinary_history_coverage_protected)} 個舊版本仍含獨有原始路徑，繼續保留`);
    }
    if (row.kind === "provider_originals") {
      if (known(m.retained_original_roots)) parts.push(`C 檔案系統仍有 ${number(m.retained_original_roots)}／${number(m.total_original_roots)} 個回復入口`);
      if (known(m.selected_copy_bytes)) parts.push(`本輪 E 已驗證邏輯量 ${bytes(m.selected_copy_bytes)}；不是 C 可回收量`);
    }
    if (known(m.planned_reclaim_allocated_bytes)) parts.push(`清冊預計回收 ${bytes(m.planned_reclaim_allocated_bytes)}；尚未釋放`);
    if (known(m.planned_source_files)) parts.push(`清冊 ${number(m.planned_source_files)} 個檔案`);
    if (known(m.penguin_packs) && known(m.vast_packs)) parts.push(`Penguin ${number(m.penguin_packs)} · Vast ${number(m.vast_packs)}`);
    if (known(m.retained_packs)) parts.push(`沿用 ${number(m.retained_packs)} 份固定驗證證據`);
    if (known(m.pending_batches)) parts.push(`待掃描 ${number(m.pending_batches)} 批`);
    if (row.kind === "whole_root_refresh" && known(m.whole_root_registered)
        && known(m.whole_root_eligible) && known(m.whole_root_due) && known(m.whole_root_ambiguous)) {
      const closed = known(m.whole_root_closed) ? ` · 已封存歷史 ${number(m.whole_root_closed)} 個` : "";
      parts.push(`登錄 ${number(m.whole_root_registered)} 個來源根目錄 · 本輪可排 ${number(m.whole_root_eligible)} 個 · 已到期 ${number(m.whole_root_due)} 個 · 待釐清 ${number(m.whole_root_ambiguous)} 個${closed}；數字為本次嘗試前的觀測`);
    }
    if (row.kind === "catalog_export" && known(m.attempt_number)) parts.push(`第 ${number(m.attempt_number)} 次嘗試`);
    if (row.kind === "lake_relay" && known(m.attempt_number)) parts.push(`接收端已嘗試 ${number(m.attempt_number)} 次`);
    if (known(m.pending_bytes)) parts.push(`接收端待傳 ${bytes(m.pending_bytes)}`);
    if (known(m.active_jobs)) parts.push(`${row.kind === "vast_spool" ? "Vast 預驗證" : "NAS 管線"}目前 ${number(m.active_jobs)} 個工作`);
    if (row.kind === "vast_spool" && known(m.blocked_items) && m.blocked_items) parts.push(`${number(m.blocked_items)} 個封包暫緩並由原排程重試`);
    if (known(m.queued_jobs)) parts.push(`上次回報待排 ${number(m.queued_jobs)} 筆`);
    if (known(m.last_cycle_accepted_jobs)) parts.push(`該輪已驗收 ${number(m.last_cycle_accepted_jobs)} 筆`);
    if (known(m.retry_jobs) && m.retry_jobs) parts.push(`待重試 ${number(m.retry_jobs)} 筆`);
    if (known(m.rejected_jobs) && m.rejected_jobs) parts.push(row.kind === "vast_spool"
      ? "前次暫存回收結果待對帳" : `未通過 ${number(m.rejected_jobs)} 筆`);
    article.append(make("p", "storage-operation-amount", parts.join(" · ") || "等待原程序提供工作量"));
    if (row.kind === "vast_spool" && row.phase === "verify_primary_sources") {
      article.append(make("p", "storage-copy", "Vast 空間不足：正逐批完整核對已還原 D 固定版本的原始封包，通過後才檢查並回收對應的遠端可重建暫存。這不是刪除 D 原件或 NAS 驗收。"));
    }
    if (row.kind === "vast_spool" && row.phase === "unlink_verified_transport") {
      article.append(make("p", "storage-copy", "本批 D 原件已核對；正在檢查遠端物件鎖與程序引用，逐項回收符合條件的暫存。"));
    }
    if (row.kind === "vast_spool" && m.rejected_jobs) {
      article.append(make("p", "storage-copy storage-operation-capacity-wait", "上次遠端回收的結果未能完整確認，已停止自動回收並保留原紀錄，等待對帳。"));
    }
    if (row.kind === "vast_spool" && m.retry_jobs) {
      article.append(make("p", "storage-copy storage-operation-capacity-wait", "遠端同步狀態或掃描請求暫時無法確認，原排程會在空間允許時重試；掃描請求不代表資料已同步。"));
    }
    if (row.kind === "vast_spool" && known(m.discarded_allocated_bytes) && m.discarded_allocated_bytes > 0) {
      article.append(make("p", "storage-copy", `此佇列累計回收 ${bytes(m.discarded_allocated_bytes)} 的 Vast 可重建暫存；不等於目前淨空間，也不刪除 D 正本。`));
    }
    if (!row.complete_evidence && row.reason_code === "physical_capacity" && known(m.source_free_bytes) && known(m.source_reserve_bytes)) {
      article.append(make("p", "storage-copy storage-operation-capacity-wait",
        `${row.kind === "vast_spool" ? "Vast 暫存磁碟" : "來源磁碟"}可用 ${bytes(m.source_free_bytes)}，此管線保留 ${bytes(m.source_reserve_bytes)}；原排程先對帳、回收，空間恢復後重新清點並派送。`));
    }
    const lease = row.receiver_readiness;
    if (!row.complete_evidence && row.reason_code === "receiver_readiness" && lease?.observed_at_utc) {
      article.append(make("p", "storage-copy storage-operation-receiver-wait",
        `接收許可${lease.state === "future" ? "的來源時間異常" : "已過期"}：原量測 ${date(lease.observed_at_utc)}（台北）${lease.valid_until_utc ? `，有效至 ${date(lease.valid_until_utc)}` : ""}；等待 lab203 原執行者重新檢查。每分鐘狀態更新不會延長許可。`));
    }
    if (row.kind === "migration_publication" && row.phase === "packing_changed_small_files" && known(m.processed_source_files)) {
      article.append(make("p", "storage-clock", `來源檔已處理 ${number(m.processed_source_files)}${known(m.total_source_files) ? `／${number(m.total_source_files)}` : ""} 個${known(m.processed_source_bytes) ? ` · 原始內容 ${bytes(m.processed_source_bytes)}${known(m.total_source_bytes) ? `／${bytes(m.total_source_bytes)}` : ""}` : ""}；與上方階段計數重疊，不代表 NAS 傳輸或還原完成。`));
    }
    const amount = p.metric === "bytes" ? p.completed_bytes : p.completed_items;
    const total = p.metric === "bytes" ? p.total_bytes : p.total_items;
    if (row.phase !== "waiting_accepted_transport") article.append(bar(amount, total,
      row.phase === "workspace_seed_copy" && p.metric === "items" ? "rsync 清單檢查進度，尚未讀回驗收" : "本階段計量進度"));
    if (known(p.completed_bytes) && known(p.total_bytes) && p.total_bytes > 0
        && known(p.completed_items) && known(p.total_items) && p.total_items > 0) {
      article.append(make("p", "storage-clock storage-operation-two-counters",
        `資料量 ${bytes(p.completed_bytes)}／${bytes(p.total_bytes)}（${percentage(p.completed_bytes, p.total_bytes).toFixed(2)}%） · 項目 ${number(p.completed_items)}／${number(p.total_items)}（${percentage(p.completed_items, p.total_items).toFixed(2)}%）；兩者代表不同工作量。`));
    }
    if (typeof p.percent === "number") {
      let suffix = "";
      if (row.phase === "waiting_new_sealed_packs") suffix = " · 等候新封包";
      else if (!row.complete_evidence && p.percent >= 100) {
        suffix = ["source_local_E_prepared", "source_local_E_prepared_ready"].includes(row.phase)
          ? " · 等待寫入 D" : " · 等待正式驗收";
      }
      article.append(make("p", "storage-clock",
        `${row.phase === "waiting_new_sealed_packs" ? "目前候選" : "本階段"} ${p.percent.toFixed(2)}%${suffix}`));
    }
    const eta = row.eta || {};
    let estimate = row.complete_evidence && ["local_testing_cleanup", "d_testing_cleanup"].includes(row.kind)
      ? `固定測試清單清理完成 ${date(row.observed_at_utc)}（台北）；這是直接刪除回執，並非冷庫或 NAS 還原證明。`
      : row.complete_evidence ? `本範圍完成 ${date(row.observed_at_utc)}（台北）；已通過原程序驗收。`
      : row.phase === "waiting_accepted_transport" ? "目前沒有待回收的已驗收副本；新批次驗收後由原排程接續。"
        : `完成時間：${displayedReason && eta.message === row.reason ? "尚無可用估計" : eta.message || "等待工作量與速度取樣"}`;
    if (binanceAfterClose) {
      estimate = "完成時間待估：盤後開始核對後，才有實際速度與進度可供估算。";
    }
    if (eta.state === "estimated" && known(eta.remaining_seconds) && eta.completion_at_utc) {
      estimate = `${eta.scope === "remaining_source_content" ? "來源內容核對預估完成" : eta.scope === "rsync_filelist_estimate" ? "rsync 清單檢查粗估完成" : "本階段預估完成"} ${date(eta.completion_at_utc)}（台北；剩 ${duration(eta.remaining_seconds)}）`;
      if (known(eta.fastest_seconds)) estimate += ` · 最快 ${duration(eta.fastest_seconds)}`;
      if (known(eta.slowest_seconds)) estimate += `／較慢 ${duration(eta.slowest_seconds)}`;
      else if (known(eta.fastest_seconds)) estimate += "／較慢上界待估";
      if (typeof eta.rate_per_second === "number") estimate += ` · ${eta.unit === "bytes" ? bytes(Math.round(eta.rate_per_second)) : eta.rate_per_second.toFixed(2) + " 項"}/s`;
      if (eta.scope === "remaining_source_content") estimate += "；依組內逐檔進度估算，後續封存提交與還原另計。";
      if (eta.scope === "rsync_filelist_estimate") estimate += "；依近期檔案清單檢查速度粗估，不包含傳輸尾端、完整 SHA 讀回與路徑切換。";
    }
    article.append(make("p", "storage-operation-estimate", estimate));
    if (row.owner_deadline_at_utc && row.reason_code === "handoff_archive_legacy_timeout") {
      const deadline = Date.parse(row.owner_deadline_at_utc);
      article.append(make("p", "storage-clock storage-operation-capacity-wait", Number.isFinite(deadline) && deadline <= Date.now()
        ? `舊版本輪時限 ${date(row.owner_deadline_at_utc)}（台北）已到；等待原排程確認並續跑。`
        : `舊版本輪時限 ${date(row.owner_deadline_at_utc)}（台北）；到期後由原排程續跑，這不是完成時間。`));
    }
    for (const [key, label] of [["completion_forecast", "目前批次預估完成"], ["queue_forecast", "目前可執行佇列預估完成"]]) {
      const forecast = row[key] || {};
      const current = !row.stale && (!row.progress_stale || row.complete_evidence);
      let text;
      if (current && forecast.state === "estimated" && forecast.completion_at_utc) {
        text = `${label} ${date(forecast.completion_at_utc)}（台北；剩 ${duration(forecast.remaining_seconds)}）`;
        if (known(forecast.fastest_seconds) && known(forecast.slowest_seconds)) text += ` · 最快 ${duration(forecast.fastest_seconds)}／較慢 ${duration(forecast.slowest_seconds)}`;
        text += ` · 依近期 ${number(forecast.sample_count)} 批完整流程估算${forecast.confidence === "low" ? "，樣本少、可信度低" : ""}`;
        if (known(forecast.estimated_jobs)) text += ` · ${number(forecast.estimated_jobs)} 批`;
      } else if (current && forecast.state === "overdue") {
        text = `${label}：原估 ${date(forecast.previous_completion_at_utc)} 已超時；保留原估計，等待新進度，不延後重置倒數。`;
      } else if (key === "queue_forecast" && current && ["sampling", "partial"].includes(forecast.state)) {
        text = `${label}：${number(forecast.unknown_jobs)} 批尚缺工作量或同類完整耗時樣本，取樣後自動更新。`;
      } else if (key === "queue_forecast" && current && forecast.state === "idle") {
        text = forecast.retry_jobs ? "目前沒有可立即執行的回收批次；等待原失敗條件修復及重試。" : "目前可執行回收佇列已空；後續新驗收批次仍由原排程接續。";
      }
      if (text) {
        if (forecast.retry_jobs && forecast.state !== "idle") text += ` 另有 ${number(forecast.retry_jobs)} 批等待重試，完成時間取決於原阻擋解除。`;
        article.append(make("p", `storage-operation-estimate storage-operation-${key.replaceAll("_", "-")}`, text));
      }
    }
    article.append(make("p", "storage-clock storage-operation-checked",
      `本次檢查 ${date(row.checked_at_utc)}（台北） · ${row.complete_evidence ? "驗收完成" : "進度回報"} ${date(row.observed_at_utc)}`));
    if (!row.complete_evidence) {
      const follow = row.stale ? {} : row.continuation || {};
      const headHandoffBlocked = row.state === "blocked" &&
        ["repack_foreign_head_wait", "retirement_current_head"].includes(row.reason_code);
      const text = row.stale ? "本次尚未取得新觀測，無法確認自動接續狀況"
        : headHandoffBlocked && follow.automatic
          ? row.reason_code === "retirement_current_head"
            ? "符合條件且已有新 D 版本的公開資料，需檢查遠端狀態與交接；交接排程狀態另行顯示，尚未完成的版本仍保留。"
            : "E 封裝已準備，仍需完成新 D 版本及跨節點交接；目前不會直接回收舊版。"
        : follow.owner_running ? "原排程本輪工作中；各項條件通過後接續"
        : follow.automatic && follow.next_check_at_utc ? `自動接續已啟用 · 下次排程檢查 ${date(follow.next_check_at_utc)}（台北）`
          : follow.automatic && follow.check_due ? "已到原排程檢查時間；等待最新執行回報"
            : follow.automatic ? "自動接續已啟用；下一次執行時間待原排程回報"
              : "尚未確認自動接續排程";
      article.append(make("p", "storage-clock storage-operation-continuation", text));
      if (follow.window_guard_reason === "tw_day_trade_priority_window" && follow.work_not_before_utc)
        article.append(make("p", "storage-clock storage-operation-priority-window",
          `當沖優先時段內僅檢查啟動條件；最早 ${date(follow.work_not_before_utc)}（台北）可嘗試工作，仍須其他條件通過。`));
      if (row.next_attempt_at_utc && !row.stale) {
        const binanceWindow = binanceAfterClose;
        article.append(make("p", "storage-clock", binanceWindow
          ? `台股保護時段結束後，最早 ${date(row.next_attempt_at_utc)}（台北）由原排程開始檢查；不是完成時間。`
          : `原程序預定重試 ${date(row.next_attempt_at_utc)}（台北）；每分鐘資訊檢查不會提前重跑驗證。`));
      }
      if (row.next_observation_at_utc && !row.stale) article.append(make("p", "storage-clock storage-operation-next-observation",
        `${Date.parse(row.next_observation_at_utc) < Date.now() ? "已到資訊檢查時間" : "下次資訊檢查"} ${date(row.next_observation_at_utc)}（台北）；每 60 秒更新，與開始／完成時間分開。`));
    }
    const details = make("details", "storage-operation-receipt");
    details.dataset.operationDetail = row.id;
    const clockLabel = row.kind === "transport" && row.notification_observation === "retry"
      ? "通知重試觀測時間" : `原始${row.complete_evidence ? "驗收" : "進度"}時間`;
    details.append(make("summary", "", "量測時間與證據範圍"), make("p", "storage-copy", row.scope),
      make("p", "storage-clock", `${clockLabel} ${date(row.observed_at_utc)}（台北）${row.progress_stale && !row.complete_evidence ? " · 等待原工作回報新計數" : ""}`));
    if (row.kind === "transport" && row.notification_observation === "retry") details.append(make("p", "storage-clock",
      `上次實際同步通知${row.last_scan_observed_at_utc ? ` ${date(row.last_scan_observed_at_utc)}（台北）` : "時間未知"}；重試觀測不代表同步或 NAS 驗收。`));
    const ownerLabels = {active: "執行中", activating: "本輪工作中", inactive: "等待原排程", failed: "最近一輪失敗", deactivating: "停止中"};
    if (ownerLabels[row.owner_state] && !row.complete_evidence) details.append(make("p", "storage-clock", `本次原執行者觀測：${ownerLabels[row.owner_state]}；程序活動與進度計數分開顯示。`));
    if (row.activity_at_utc) details.append(make("p", "storage-clock", `程序活動 ${date(row.activity_at_utc)}（台北）；活動時間不替代進度收據。`));
    if (row.kind === "vast_spool") details.append(make("p", "storage-clock",
      `本輪候選清點${row.inventory_at_utc ? ` ${date(row.inventory_at_utc)}（台北）` : "時間尚未提供"}；來源持續新增封閉包，下輪清點會更新候選總量。`));
    if (row.reason && !row.complete_evidence) details.append(make("p", "storage-copy", row.reason));
    if (row.next_attempt_at_utc && row.kind !== "workspace_source_handoff") details.append(make("p", "storage-clock", `原佇列預定重試 ${date(row.next_attempt_at_utc)}（台北）；過期時間不代表正在執行。`));
    if (known(m.admission_available_bytes)) details.append(make("p", "storage-copy", `接收端可再接納 ${bytes(m.admission_available_bytes)}；不代表 NAS 已完成。`));
    if (known(m.lab_reclaimed_allocated_bytes)) details.append(make("p", "storage-copy", `lab203 累積回收 ${bytes(m.lab_reclaimed_allocated_bytes)}；與本機 WSL 分開計算。`));
    article.append(details);
    return article;
  }
  function rememberOpenOperations() {
    for (const detail of section.querySelectorAll("[data-operation-detail]")) {
      if (detail.open) operationList.open.add(detail.dataset.operationDetail);
      else operationList.open.delete(detail.dataset.operationDetail);
    }
  }
  function operationCards(rows) {
    return rows.map(row => {
      const card = operationCard(row);
      const detail = card.querySelector("[data-operation-detail]");
      if (detail) detail.open = operationList.open.has(detail.dataset.operationDetail);
      return card;
    });
  }
  function renderOtherOperations() {
    const target = $("storage-operation-other-tasks");
    if (!$("storage-operations-more").open) { target.replaceChildren(); return; }
    const cards = operationCards(operationList.rows.slice(0, operationList.visible));
    if (operationList.rows.length > operationList.visible) {
      const button = make("button", "", `再顯示 100 項（還有 ${number(operationList.rows.length - operationList.visible)} 項）`);
      button.type = "button";
      button.addEventListener("click", () => {
        rememberOpenOperations();
        operationList.visible += 100;
        renderOtherOperations();
      });
      cards.push(button);
    }
    target.replaceChildren(...cards);
  }
  function renderArchivePolicy(policy, observerStale) {
    const target = $("storage-archive-policy");
    if (!policy || policy.contract !== "stockagent.storage_archive_plan.v1") {
      target.replaceChildren(make("p", "storage-copy", "等待原觀測器提供儲存分工與增量設定。"));
      return;
    }
    const roles = make("div", "storage-archive-roles");
    for (const [label, text] of [
      ["C／WSL · 服務", "程式、網站、下載器、現行帳本與服務必要工作集。"],
      ["E · 增量與備份中轉", "下載、整理、解壓、舊包中轉；D 不足時保存已登錄封存。"],
      ["D · 壓縮備份", "同資料夾／資料集集中封裝，穩定內容重用，盡量循序讀寫。"],
    ]) {
      const item = make("div", "storage-archive-role");
      item.append(make("strong", "", label), make("p", "storage-copy", text));
      roles.append(item);
    }
    const window = policy.daily_merge_window;
    const period = `${String(window.start_hour).padStart(2, "0")}:00–${String(window.end_hour).padStart(2, "0")}:00`;
    const schedule = observerStale ? "設定快照已過期，等待本次觀測。"
      : `${policy.daily_window_open ? "目前在合併時段" : policy.next_daily_merge_start_at_utc ? `下一個允許開始時點 ${date(policy.next_daily_merge_start_at_utc)}（台北）` : "下次開始時點待確認"}；${policy.migration_catchup ? "此次遷移補整另由原排程持續輪轉。" : "日常合併依時段處理。"}`;
    const packing = `按資料集封裝，目標每包約 ${bytes(policy.dataset_pack_target_bytes)} 原始成員；舊包交接每批最多 ${bytes(policy.maximum_transfer_wave_bytes)}。這是工作量上限，並非完成時間。`;
    const versioning = policy.ordinary_history_retention === "current-and-research-pins"
      && policy.physical_version === "reuse_immutable_parts_and_sha256_chunks"
      ? "冷庫完整清冊重用既有物件，新增或修訂內容才增加封存負載；研究固定版本共用未變內容。普通中間版本由原引用回收器清理，舊版獨有資料與服務依賴繼續保留。"
      : "普通版本保留政策尚未取得有效設定。";
    const gap = policy.general_raw_retirement_enabled === false
      ? "尚待完成：一般原始工作檔的自動回收未全面啟用；FinLab／FinMind／TEJ 等仍需逐來源接妥按需讀取。使用中的 raw 先保留，不把它標成遷移完成。"
      : policy.general_raw_retirement_enabled == null ? "原始工作檔回收設定尚未確認；各來源仍依自己的讀取能力與原回收條件處理。"
        : "已宣告一般來源回收設定；實際就緒與刪除仍依逐來源收據判定。";
    target.replaceChildren(make("h4", "", "目標分工與目前設定"), roles,
      make("p", "storage-copy", versioning), make("p", "storage-copy", packing),
      make("p", "storage-clock", `每日 ${period}（台北）開始合併。${schedule}`),
      make("p", "storage-policy-gap", gap));
  }

  function renderOperations(value) {
    const valid = value?.contract === "stockagent.storage_operations.v1" && value.read_only === true && !!value.observed_at_utc;
    const observerStale = !valid || value.state === "stale";
    renderArchivePolicy(value?.archive_policy, observerStale);
    const summaryHeading = section.closest("details")?.querySelector(":scope > summary");
    if (summaryHeading) {
      let badge = summaryHeading.querySelector("[data-storage-progress-badge]");
      if (!badge) {
        badge = make("span", "storage-archive-badge");
        badge.dataset.storageProgressBadge = "true";
        summaryHeading.append(badge);
      }
      const reclaimed = valid && !observerStale && Array.isArray(value.tasks)
        ? value.tasks.filter(row => row.kind === "packed_archive_total")
          .reduce((sum, row) => sum + (row.metrics?.archive_D_reclaimed_bytes || 0), 0) : null;
      badge.textContent = known(reclaimed) ? `D 已回收 ${bytes(reclaimed)}` : "等待本次儲存進度";
    }
    $("storage-operations-observed").textContent = valid
      ? `備份與驗證快照 ${date(value.observed_at_utc)}（台北）${value.state === "stale" ? " · 觀測已過期，保留上次收據" : " · 背景與頁面每 60 秒更新"}`
      : "備份與驗證進度尚未取得；磁碟容量仍獨立顯示。";
    const summary = valid ? value.summary || {} : {};
    const discarded = valid && Array.isArray(value.tasks) ? value.tasks
      .filter(row => row.kind === "local_testing_cleanup"
        && row.complete_evidence && known(row.metrics?.discarded_allocated_bytes))
      .reduce((total, row) => total + row.metrics.discarded_allocated_bytes, 0) : 0;
    const dDiscarded = valid && Array.isArray(value.tasks) ? value.tasks
      .filter(row => row.kind === "d_testing_cleanup" && known(row.metrics?.discarded_allocated_bytes))
      .reduce((total, row) => total + row.metrics.discarded_allocated_bytes, 0) : 0;
    const operationStats = [
      ["執行中", summary.running], ["等待／前置待處理", known(summary.waiting) && known(summary.blocked) ? summary.waiting + summary.blocked : null],
      ["本範圍已驗收", summary.completed], ["累計回收 WSL 配置", bytes(known(summary.penguin_reclaimed_allocated_bytes)
        ? summary.penguin_reclaimed_allocated_bytes + discarded : null)],
      ["已釋放 D 舊測試", bytes(valid ? dDiscarded : null)]
    ];
    if (summary.historical_unresolved > 0) operationStats.push(["歷史紀錄待核對", summary.historical_unresolved]);
    $("storage-operations-summary").replaceChildren(...operationStats.map(([label, amount]) => {
      const card = make("div", "storage-operation-stat");
      card.append(make("span", "storage-copy", label), make("strong", "", typeof amount === "string" ? amount : number(amount)));
      return card;
    }));
    const overall = value?.local_migration_eta || value?.overall_eta || {};
    $("storage-operations-eta").textContent = !observerStale && overall.known_completion_at_utc
      ? `本機遷移可估步驟最晚 ${date(overall.known_completion_at_utc)}（台北；${number(overall.estimated_tasks)} 項已有估計、${number(overall.unestimated_tasks)} 項待估${overall.blocked_tasks ? `，其中 ${number(overall.blocked_tasks)} 項有阻擋` : ""}）。這不是全庫完成時間；NAS 與獨立還原另列，後續未知步驟繼續取樣。`
      : `預估完成時間：${observerStale ? "觀測中斷，暫停估算" : overall.message || "等待原程序提供完整工作量與速度"}。各卡片會顯示速度、剩餘時間及阻擋；取樣後每分鐘自動更新。`;
    const attention = observerStale ? [{level: "blocked", label: "目前無法更新運行與排程觀測",
      message: "保留上次收據與原時間；下次仍會重試讀取。以下舊排程及活動不能當作即時狀態。"}]
      : Array.isArray(value.attention) ? value.attention : [];
    $("storage-operation-attention").replaceChildren(...attention.map(row => {
      const entry = make("div", `storage-operation-notice ${row.level === "blocked" ? "storage-operation-notice-blocked" : ""}`);
      entry.append(make("strong", "", row.label), make("p", "storage-copy", row.message));
      const clocks = [];
      if (row.last_report_at_utc) clocks.push(`原工作回報 ${date(row.last_report_at_utc)}`);
      if (row.next_attempt_at_utc) clocks.push(`原排程重試 ${date(row.next_attempt_at_utc)}`);
      if (clocks.length) entry.append(make("small", "storage-clock", `${clocks.join(" · ")}（台北）`));
      return entry;
    }));
    if (!attention.length) $("storage-operation-attention").append(make("p", "storage-copy",
      valid && value.state !== "stale" ? "本次沒有已回報的阻擋；未提供的遠端排程仍維持未知。" : "等待有效的阻擋與排程觀測。"));
    rememberOpenOperations();
    const focused = document.activeElement?.closest("[data-operation-detail]")?.dataset.operationDetail;
    const priority = {running: 0, blocked: 1, waiting: 2, unknown: 3, completed: 5};
    const taskPriority = row => row.historical_only === true ? 4 : (priority[row.state] ?? 3);
    const rows = valid && Array.isArray(value.tasks) ? [...value.tasks].sort((a, b) =>
      taskPriority(a) - taskPriority(b)
      || Number(b.kind === "local_testing_cleanup") - Number(a.kind === "local_testing_cleanup")
      || Number(b.kind === "retirement") - Number(a.kind === "retirement")
      || Number(b.kind === "provider_originals") - Number(a.kind === "provider_originals")) : [];
    const publication = !observerStale && rows.find(row => row.kind === "migration_publication"
      && row.state === "running" && !row.stale);
    const source = publication?.metrics || {};
    const sourceCount = known(source.processed_source_files) && known(source.total_source_files)
      ? `；已處理 ${number(source.processed_source_files)}／${number(source.total_source_files)} 個來源檔`
      : "";
    const recoveryDeferred = rows.some(row => row.kind === "migration"
      && row.reason_code === "recovery_deferred_user_migration" && row.state === "waiting");
    $("storage-operations-focus").textContent = publication
      ? `目前來源封存主線：${publication.label}，${publication.phase_label}${sourceCount}。${recoveryDeferred
        ? "依目前遷移設定暫緩額外獨立還原；來源封裝及回收依各自的實際進度處理。"
        : "封閉包由原執行者逐批接續；來源提交、工作副本回收與 NAS 備份各自記錄。"}`
      : "各節點分工、目前階段與等待條件如下；預驗證完成量與可回收空間分開計算。";
    // Detailed source blockers belong after the six pipeline summaries.
    const primary = rows.filter(row => row.kind !== "migration_dependency").slice(0, 6);
    const primaryIds = new Set(primary.map(row => row.id));
    operationList.rows = rows.filter(row => !primaryIds.has(row.id));
    $("storage-operation-tasks").replaceChildren(...operationCards(primary));
    renderOtherOperations();
    if (focused) [...section.querySelectorAll("[data-operation-detail]")].find(node => node.dataset.operationDetail === focused)
      ?.querySelector("summary")?.focus({preventScroll: true});
    $("storage-operations-more").hidden = !operationList.rows.length;
    const omitted = Number(value.summary?.omitted_task_count) || 0;
    $("storage-operations-more-label").textContent = `其他工作與已完成階段（${number(operationList.rows.length)}）${omitted > 0 ? `；另有 ${number(omitted)} 項超過清單上限` : ""}`;
    const schedule = valid && Array.isArray(value.schedules) ? value.schedules : [];
    const flow = valid && Array.isArray(value.flow) ? value.flow : [];
    const migrationFlow = flow.filter(row => !row.branch || row.branch === "local_migration");
    $("storage-phase-timeline").replaceChildren(...migrationFlow.map(row => {
      const entry = make("tr");
      const heading = make("th", "", row.label);
      heading.scope = "row";
      let estimate;
      if (observerStale) estimate = "觀測已過期，暫停估算";
      else if (row.unassessed_source_count > 0) {
        const current = row.known_current_step_completion_at_utc
          ? `目前可估步驟最晚 ${date(row.known_current_step_completion_at_utc)}；` : "";
        estimate = `${current}另有 ${number(row.unassessed_source_count)} 個來源待確認交接；整階段待估`;
      }
      else if (row.state === "completed") estimate = "已完成此階段的已登錄範圍";
      else if (row.known_current_step_completion_at_utc) estimate = `目前可估步驟最晚 ${date(row.known_current_step_completion_at_utc)}；整階段及後續步驟待估`;
      else if (row.state === "blocked") estimate = "待解除阻擋；完成時間待估";
      else estimate = "等待本階段工作量與速度；完成時間待估";
      const time = make("td", "", estimate);
      if (!observerStale && row.next_check_at_utc) time.append(make("small", "storage-clock",
        `下次檢查 ${date(row.next_check_at_utc)}；不是完成時間`));
      entry.append(heading, make("td", "", observerStale ? "已過期" : operationStates[row.state] || "待觀測"), time);
      return entry;
    }));
    const stageLabels = new Map(flow.map(row => [row.id, row.label]));
    const coverageOpen = new Set([...section.querySelectorAll("details[data-stage-coverage][open]")]
      .map(node => node.dataset.stageCoverage));
    const coverageFocused = document.activeElement?.closest("[data-stage-coverage]")?.dataset.stageCoverage;
    $("storage-operation-flow").replaceChildren(...flow.map(row => {
      const entry = make("div", "storage-operation-stage");
      if (row.required_for_local_migration === false) entry.append(make("small", "storage-clock", "獨立流程，不阻擋本次本機遷移"));
      entry.append(make("strong", "", row.label), make("span", "storage-copy", observerStale ? "流程觀測已過期" : operationStates[row.state] || "等待觀測"),
        make("p", "storage-copy", row.condition));
      if (Array.isArray(row.depends_on) && row.depends_on.length) entry.append(make("small", "storage-clock",
        `逐固定版本接續：${row.depends_on.map(id => stageLabels.get(id) || "前一階段").join("、")}。其他版本可並行。`));
      if (!observerStale) {
        const pending = Number(row.pending_tasks) || 0;
        const done = Number(row.completed_tasks) || 0;
        const estimated = Number(row.estimated_current_steps) || 0;
        const missing = Number(row.unestimated_tasks) || 0;
        const parts = [`${row.id === "e_handoff" ? "交接器列管" : "已登錄"} ${number(done + pending)} 項 · 完成 ${number(done)} · 待處理 ${number(pending)}`];
        if (row.id === "e_handoff" && row.unassessed_source_count > 0) parts.push(
          `另有 ${number(row.unassessed_source_count)} 個已登錄來源未由此交接器列管；須逐一確認是否以其他方式切換，全部交接時間待估`);
        if (row.id === "retire" && row.unassessed_source_count > 0) parts.push(
          `另有 ${number(row.unassessed_source_count)} 個來源尚未完成交接審核，回收範圍待確認`);
        if (row.blocked_tasks) parts.push(`其中 ${number(row.blocked_tasks)} 項阻擋`);
        if (row.known_current_step_completion_at_utc) parts.push(`目前 ${number(estimated)} 項可估步驟最晚 ${date(row.known_current_step_completion_at_utc)}（台北）；${number(missing)} 項及後續步驟待估`);
        else if (pending || row.unassessed_source_count > 0) parts.push("完成時間待估：等待實際工作量、速度或前置條件");
        if (row.next_check_at_utc) parts.push(`下次排程檢查 ${date(row.next_check_at_utc)}（台北）`);
        entry.append(make("small", "storage-clock storage-stage-estimate", parts.join("；")));
        if (row.id === "e_handoff" && Array.isArray(row.unassessed_source_labels) && row.unassessed_source_labels.length) {
          const details = make("details", "storage-operation-details");
          details.dataset.stageCoverage = row.id;
          details.open = coverageOpen.has(row.id);
          details.append(make("summary", "", `查看未由此交接器列管的來源（${number(row.unassessed_source_labels.length)}）`),
            make("p", "storage-copy", "以下數量來自 E 已複製的固定世代；尚未確認服務交接，不代表 C 原件已可回收。"));
          const staging = Array.isArray(value.tasks) ? value.tasks.filter(item => item.kind === "workspace_source_staging") : [];
          const list = make("ul", "storage-handoff-sources");
          for (const label of row.unassessed_source_labels) {
            const copy = staging.find(item => item.label === `WSL → E · 已登錄原始資料複製 · ${label}`);
            const progress = copy?.progress || {};
            const copied = copy?.complete_evidence && known(progress.total_bytes) ? bytes(progress.total_bytes) : "尚未確認";
            const files = copy?.complete_evidence && known(progress.total_items) ? `${number(progress.total_items)} 檔` : "檔數待確認";
            list.append(make("li", "", `${label}：E 固定副本 ${copied}，${files}；來源交接待核對，完成時間待估。`));
          }
          details.append(list);
          entry.append(details);
        }
      }
      return entry;
    }));
    if (coverageFocused) [...section.querySelectorAll("[data-stage-coverage]")]
      .find(node => node.dataset.stageCoverage === coverageFocused)?.querySelector("summary")?.focus({preventScroll: true});
    $("storage-operation-schedule-list").replaceChildren(...schedule.map(row => {
      const entry = make("div", "storage-operation-schedule");
      entry.append(make("strong", "", row.label), make("span", "storage-copy", observerStale || row.stale ? "排程快照已過期"
        : !row.registered ? "目前無法讀取已安裝排程"
          : row.owner_state === "failed" ? "原服務失敗；需核對失敗收據及重試時間"
          : row.mode === "continuous" ? (row.owner_running ? "原生命週期常駐執行，依工作及相依條件接續" : "原常駐執行者未運行")
          : row.timer_active === false ? "排程目前未生效"
          : row.next_check_at_utc ? `下次檢查 ${date(row.next_check_at_utc)}（台北）`
            : row.check_due && row.scheduled_check_at_utc ? `已到預定檢查 ${date(row.scheduled_check_at_utc)}（台北）；等待最新執行回報`
            : row.owner_running === true ? "本輪正在執行；下次時間未排定" : "尚未提供下次檢查時間"));
      if (!observerStale && !row.stale && row.owner_state && row.owner_state !== "unknown") entry.append(make("small", "storage-clock",
        `執行者：${({active: "運行中", activating: "本輪執行中", inactive: "目前未執行", deactivating: "正在結束", failed: "失敗"})[row.owner_state] || "未知"}${known(row.exit_code) && row.exit_code !== 0 ? ` · 上次退出碼 ${row.exit_code}` : ""}`));
      if (!observerStale && !row.stale && row.window_guard_reason === "tw_day_trade_priority_window" && row.work_not_before_utc)
        entry.append(make("small", "storage-clock", `當沖優先：下次為條件檢查；最早 ${date(row.work_not_before_utc)}（台北）可嘗試工作，並非完成時間。`));
      else if (!observerStale && !row.stale && row.owner_result === "exec-condition")
        entry.append(make("small", "storage-clock", "上次啟動條件未通過；下次觸發會重新檢查，不保證開始工作。"));
      if (row.last_check_at_utc) entry.append(make("small", "storage-clock", `上次觸發 ${date(row.last_check_at_utc)}（台北）`));
      const window = state.data?.cold_tiers?.schedule;
      if (row.id === "cold_tiers" && window) entry.append(make("small", "storage-clock",
        `實際整理時段每日 ${String(window.start_hour).padStart(2, "0")}:00–${String(window.end_hour).padStart(2, "0")}:00（台北），其餘時間只檢查。`));
      return entry;
    }));
    $("storage-operation-remote-schedule").textContent = valid ? value.remote_schedule_message || "遠端排程尚未提供，開始時間維持未知。" : "";
  }
  function renderCards() {
    const focused = document.activeElement?.dataset.storageSelect;
    const nodes = state.data.nodes.map(node => {
      const group = make("div", "storage-node");
      group.append(make("h3", "", node.label));
      const grid = make("div", "storage-disk-grid");
      grid.append(...node.volumes.map(card));
      group.append(grid);
      return group;
    });
    $("storage-capacities").replaceChildren(...nodes);
    syncSelection();
    if (focused) [...section.querySelectorAll("[data-storage-select]")].find(b => b.dataset.storageSelect === focused)?.focus({preventScroll: true});
  }
  function treeNode(row) {
    const head = make("span", "storage-tree-line");
    head.append(make("span", "", `${row.label} · ${kinds[row.kind] || "其他"}`), make("strong", "", measuredBytes(row)));
    if (!row.children.length) {
      const leaf = make("div", "storage-tree-leaf");
      leaf.append(head);
      return leaf;
    }
    const details = make("details");
    details.dataset.storageNode = row.id;
    const summary = make("summary");
    summary.append(head);
    const children = make("div", "storage-tree-children");
    details.addEventListener("toggle", () => {
      if (details.open && !children.childNodes.length) children.append(...row.children.map(treeNode));
    });
    details.append(summary, children);
    return details;
  }
  function renderExplorer() {
    const volume = volumes().find(v => v.id === state.selected);
    $("storage-explorer").hidden = !volume;
    if (!volume) return;
    $("storage-selected-title").textContent = `${volume.label} · 資料分類`;
    const group = volume.classification;
    const basis = group.observed_at_utc ? `量測於 ${date(group.observed_at_utc)}（台北）` : "尚無完成的分類清點";
    const phases = {complete: "已登錄目錄已清點", partial: "部分目錄已量測，其餘目錄待清點", scanning: `背景清點中，開始 ${date(group.started_at_utc)}`,
      unavailable: "本次清點失敗；保留上次結果", not_scanned: "未取得分類，等待觀測端"};
    const coverage = known(group.roots_total) && known(group.roots_completed)
      ? `已完成 ${group.roots_completed}／${group.roots_total} 個根目錄。` : "";
    const deferred = {retirement_wave_active: "清點暫讓路給資料回收。", retirement_wave_pending: "清點暫讓路給資料回收。",
      retirement_wave_lock_unobservable: "清點暫緩，等待回收狀態可觀測。", dataset_version_owner_active: "清點暫讓路給冷庫寫入與驗證。",
      dataset_version_owner_unobservable: "清點暫緩，等待冷庫工作狀態可觀測。",
      transport_relocation_owner_active: "清點暫讓路給 D → E 驗證與空間回收；容量與節點狀態仍每分鐘更新。",
      source_local_migration_active: "目錄分類清點暫讓路給封裝與遷移，避免競爭磁碟 I/O；容量及工作進度仍每分鐘更新。",
      provider_workspace_owner_active: "目錄分類清點暫讓路給來源搬移與驗證，避免競爭 C／E 小檔 I/O；容量、節點及工作進度仍每分鐘更新。",
      provider_workspace_observation_unavailable: "來源搬移狀態暫不可讀，目錄分類清點稍後再試；容量與節點觀測照常。",
      tw_opening_guard_active: "目錄清點暫讓路給台股開盤準備。",
      tw_opening_guard_unknown: "開盤時段交易守護狀態無法確認，目錄清點暫緩。"};
    $("storage-classification-state").textContent = `${phases[group.state] || "等待清點"}。${basis}${group.stale ? "；此分類已過期" : ""}。${group.layout_changed ? "服務路徑已調整；以下為搬移前清冊，等待背景重新清點。" : ""}${deferred[group.deferred_reason] || ""}${coverage}${group.allocated_bytes_is_lower_bound ? "≥ 表示已完成子目錄的實測下限；比例僅比較已量測內容，尚未掃描不當作零。" : ""}只含已登錄專案目錄；總磁碟其餘用量包含系統及其他檔案。`;
    let rows = group.tree;
    let parentBytes = group.allocated_bytes;
    let direct = null;
    const crumb = (text, depth) => {
      const button = make("button", "", text);
      button.type = "button";
      button.addEventListener("click", () => { state.path = state.path.slice(0, depth); state.visible = 12; renderExplorer(); });
      return button;
    };
    const crumbs = [crumb("所有已登錄目錄", 0)];
    const validPath = [];
    for (const id of state.path) {
      const row = rows.find(r => r.id === id);
      if (!row) break;
      validPath.push(id);
      crumbs.push(make("span", "", "／"), crumb(row.label, validPath.length));
      rows = row.children;
      parentBytes = row.allocated_bytes;
      direct = row.direct_or_unexpanded_bytes;
    }
    state.path = validPath;
    $("storage-breadcrumbs").replaceChildren(...crumbs);
    const hexagons = rows.slice(0, state.visible).map(row => {
      const button = make("button", `storage-hex storage-kind-${row.kind}`);
      button.type = "button";
      const pct = percentage(row.allocated_bytes, parentBytes);
      const inside = make("span", "storage-hex-inner");
      inside.append(make("span", "storage-hex-kind", kinds[row.kind] || "其他"),
        make("strong", "storage-hex-title", row.label), make("span", "storage-hex-bytes", measuredBytes(row)),
        bar(row.allocated_bytes, parentBytes, "占本層比例"),
        make("span", "storage-hex-detail", `${pct === null ? "比例未知" : `${pct.toFixed(1)}%`}${row.children.length ? " · 點開" : " · 本層最深"}`));
      button.append(inside);
      button.disabled = !row.children.length;
      button.setAttribute("aria-label", `${row.label}，${measuredBytes(row)}${row.children.length ? "，展開分類" : "，目前清點最深層"}`);
      button.addEventListener("click", () => { state.path.push(row.id); state.visible = 12; renderExplorer(); $("storage-breadcrumbs button:last-child")?.focus({preventScroll: true}); });
      return button;
    });
    $("storage-honeycomb").replaceChildren(...hexagons);
    $("storage-more").hidden = rows.length <= state.visible;
    $("storage-more").textContent = `顯示更多分類（還有 ${Math.max(0, rows.length - state.visible)} 個）`;
    $("storage-direct").textContent = !group.tree.length
      ? "尚未取得目錄分類。NAS／lab203 需要接上觀測回執；Windows C 槽只量容量，不掃描個人檔案。"
      : known(direct) && direct > 0 ? `本層直接檔案、目錄本身或尚未展開：${bytes(direct)}。` : "空間以實際配置位元組計算；分類占用不等於可刪除空間。";
    if ($("storage-tree").parentElement.open && $("storage-tree").dataset.revision !== group.observed_at_utc) {
      $("storage-tree").replaceChildren(...group.tree.map(treeNode));
      $("storage-tree").dataset.revision = group.observed_at_utc || "unknown";
    }
  }
  function validate(data) {
    if (data.contract !== "stockagent.storage_monitor.v1" || data.read_only !== true || data.production_control_possible !== false
      || !Array.isArray(data.nodes) || data.nodes.length !== 4 || data.nodes.some(n => !Array.isArray(n.volumes))) throw new Error("Invalid storage metadata");
    const rows = data.nodes.flatMap(n => n.volumes);
    if (!rows.length || rows.length > 64 || new Set(rows.map(v => v.id)).size !== rows.length
      || rows.some(v => typeof v.id !== "string" || !v.id || !v.capacity || !v.classification
        || !Array.isArray(v.classification.tree)
        || v.contained_in && (v.contained_in === v.id || !rows.some(parent => parent.id === v.contained_in)))) throw new Error("Invalid storage volumes");
  }
  function renderDBackup(inventory, progress) {
    const details = $("storage-d-backup-details");
    if (!inventory || !Array.isArray(inventory.groups)) {
      details.hidden = true;
      const scanning = progress?.state === "scanning" && known(progress.processed_bytes) && known(progress.total_bytes);
      const pct = scanning ? percentage(progress.processed_bytes, progress.total_bytes) : null;
      $("storage-d-backup-summary").textContent = scanning
        ? `正在分類既有清冊${pct === null ? "" : ` · ${pct.toFixed(1)}%`}${progress.completion_at_utc ? ` · 此步驟預估 ${date(progress.completion_at_utc)} 完成` : ""}；此估時只包含清冊分類。`
        : progress?.state === "failed" || progress?.state === "stale"
          ? "清冊分類暫未完成，等待下次重試；磁碟容量與其他進度仍照常更新。"
          : "等待備份清冊分類；不會為顯示資訊重掃 D 槽原件。";
      return;
    }
    details.hidden = false;
    const source = progress?.state === "current" && progress.catalog_observed_at_utc
      ? progress.catalog_observed_at_utc : inventory.catalog_observed_at_utc;
    const sourceAge = source ? Date.now() - new Date(source).getTime() : null;
    const stale = !Number.isFinite(sourceAge) || sourceAge > 12 * 60 * 60 * 1000 || sourceAge < -300000;
    const scanPct = progress?.state === "scanning" ? percentage(progress.processed_bytes, progress.total_bytes) : null;
    const pending = progress?.state === "scanning"
      ? ` 新版本清冊分類中${scanPct === null ? "" : ` ${scanPct.toFixed(1)}%`}${progress.completion_at_utc ? `，此步驟預估 ${date(progress.completion_at_utc)} 完成` : ""}。` : "";
    const unreferenced = inventory.roles?.unreferenced_cold_object;
    $("storage-d-backup-summary").textContent = `已登錄 ${number(inventory.total_files)} 個封存物件／${bytes(inventory.total_bytes)}；清冊觀測 ${date(source)}（台北）${stale ? "，觀測已過期或未知" : ""}。${known(unreferenced?.files) ? `其中 ${number(unreferenced.files)} 個物件在此清冊未被版本引用（${bytes(unreferenced.bytes)}），不可據此判定可刪。` : ""}${pending}`;
    const container = $("storage-d-backup-groups");
    if (container.dataset.revision === inventory.observed_at_utc) return;
    const previouslyOpen = new Set([...container.querySelectorAll("details[open]")].map(d => d.dataset.group));
    const groups = inventory.groups.filter(group => Array.isArray(group.datasets) && group.datasets.length).map(group => {
      const entry = make("details", "storage-tree-details");
      entry.dataset.group = group.id;
      const summary = make("summary", "", `${group.label} · ${number(group.datasets.length)} 個資料集 · ${number(group.retained_versions)} 個歷史版本 · 相關物件 ${bytes(group.referenced_bytes)}`);
      const list = make("ul", "storage-copy");
      const populate = () => {
        if (entry.open && !list.childNodes.length) {
          list.append(...group.datasets.map(item => make("li", "", `${item.name} · ${number(item.versions)} 個版本`)));
        }
      };
      entry.addEventListener("toggle", populate);
      entry.append(summary, list);
      if (previouslyOpen.has(group.id)) { entry.open = true; populate(); }
      return entry;
    });
    container.replaceChildren(...groups);
    container.dataset.revision = inventory.observed_at_utc || "unknown";
  }
  function renderDPackTrial(trial) {
    const first = trial?.files?.[0];
    if (!first || !Array.isArray(first.candidates) || !first.candidates.length) {
      return;
    }
    const rows = first.candidates.map(c => `Zstd ${c.level}：${bytes(c.bytes)}、寫入 ${c.write_fsync_seconds.toFixed(2)} 秒`);
    $("storage-d-pack-trial").textContent = `E 槽樣本已完成欄位、數值與空值往返檢查：${first.name} 原始 ${bytes(first.source_bytes)}；${rows.join("；")}。這是單一小檔、熱快取與單次量測，未改動正式檔案，不能代表 D 槽效能或整批最佳格式。`;
  }
  async function refresh() {
    if (state.inFlight || document.hidden) return;
    state.inFlight = true;
    const request = requests.begin();
    try {
      const data = await fetchJson("/data-monitor/api/storage", {signal: request.signal});
      if (!request.isCurrent()) return;
      validate(data);
      state.data = data;
      state.loaded = true;
      const rows = volumes();
      const fresh = rows.filter(v => v.capacity.state === "measured").length;
      const partial = rows.filter(v => v.capacity.state === "partial").length;
      $("storage-observed").textContent = `${rows.length} 個磁碟位置 · ${fresh} 個有近期完整容量、${partial} 個只有部分容量資訊。快照 ${date(data.generated_at_utc)}（台北）；容量每分鐘更新，分類每六小時清點。`;
      $("storage-efficiency").hidden = !data.efficiency;
      if (data.efficiency) {
        const proof = data.efficiency;
        $("storage-efficiency").textContent = `空間維護 ${date(proof.observed_at_utc)}：本次已驗證回收 ${bytes(proof.reclaimed_allocated_bytes)}，保留使用中的 ${bytes(proof.protected_active_bytes)}；${known(proof.compression_sample_files) ? proof.compression_sample_files : "—"} 個壓縮樣本已核對原值${proof.production_codec_changed ? "" : "，正式資料維持現行壓縮設定"}。`;
      }
      const tiers = data.cold_tiers;
      $("storage-tiers").hidden = !tiers;
      if (tiers) {
        const labels = {outside_daily_window: "等待每日冷區整理時段", waiting_d_capacity: "等待 D 槽容量",
          waiting_publication_window: "等待原發布鎖", waiting_migration_priority: "優先處理 D 空間回收與資料遷移；之後自動繼續冷區整理", bounded_cycle_complete: "本輪冷區整理完成",
          partial_failures_preserved: "部分批次失敗，原檔保留", existing_tier_owner_busy: "冷區整理執行中"};
        const schedule = tiers.schedule;
        const window = schedule ? `；每日 ${String(schedule.start_hour).padStart(2, "0")}:00–${String(schedule.end_hour).padStart(2, "0")}:00（台北）處理穩定超過 ${schedule.stable_after_hours} 小時的增量` : "";
        const d = rows.find(row => row.id === "penguin-d")?.capacity;
        // The prior gateway generation omits this newly published policy field.
        // Show only a time-limited, explicitly dated observation until the
        // normal gateway reload; never silently treat it as a current policy.
        const policyClock = Date.parse("2026-11-05T02:50:14Z");
        const policyAge = Date.now() - policyClock;
        const directReserve = schedule && known(schedule.reserve_bytes);
        const reserveBytes = directReserve ? schedule.reserve_bytes : policyAge >= 0 && policyAge < 12 * 3600000 ? 64 * 1024 ** 3 : null;
        const reserve = known(reserveBytes) && d?.state === "measured" && known(d.available_bytes)
          ? `。${directReserve ? "原程序回報" : "11 月 5 日 10:50 設定快照"}：此冷區整理流程保留 ${bytes(reserveBytes)}，D 可用 ${bytes(d.available_bytes)}${d.available_bytes < reserveBytes ? "，依該設定尚不足，恢復時刻待估" : ""}；其他備份流程各用自己的准入條件` : "";
        const work = tiers.last_work_cycle;
        const last = work ? `。上次有工作 ${date(work.observed_at_utc)}：${known(work.objects_completed) ? work.objects_completed : "—"} 個物件、原值 ${bytes(work.logical_bytes_processed)}，配置空間減少 ${bytes(work.allocation_bytes_saved)}；此為單輪成果，非全庫壓縮率。` : "。尚無已登錄的正式整理成果。";
        $("storage-tiers").textContent = `D 增量→穩定冷區：${tiers.stale ? "觀測已過期 · " : ""}${labels[tiers.state] || "等待狀態"}${window}${reserve}${last}`;
      }
      renderCards();
      renderOperations(data.operations);
      renderRuntime(data.runtime);
      renderDBackup(data.d_backup_inventory, data.d_backup_inventory_progress);
      const layout = data.d_pack_layout;
      $("storage-pack-layout").hidden = !layout;
      if (layout) {
        const groups = layout.groups || [];
        const totals = groups.map(row => `${row.label} ${number(row.present_names)} 個、${bytes(row.present_name_bytes)}`).join("；");
        const placed = groups.reduce((sum, row) => sum + (row.placed_on_E_name_bytes || 0), 0);
        $("storage-pack-layout").textContent = `${layout.stale ? "舊觀測 · " : ""}${date(layout.observed_at_utc)} 新舊格式清點：${totals}；另有 ${bytes(placed)} 已定位 E。${layout.incomplete_datasets ? `${number(layout.incomplete_datasets)} 個資料集當時未能納入。` : ""}名稱長度含共用別名，不能相加作磁碟用量；最多每 6 小時重盤，網站每分鐘讀快照。`;
      }
      renderDPackTrial(data.d_pack_trial);
      renderExplorer();
      $("storage-error").hidden = true;
    } catch (error) {
      if (error.name === "AbortError" || !request.isCurrent()) return;
      $("storage-error").hidden = false;
      $("storage-error").textContent = state.loaded ? "本次儲存快照讀取失敗，保留上次結果；請留意各項量測時間。" : "儲存快照暫時無法讀取，稍後自動重試。";
      renderRuntime(state.data?.runtime, true);
      if (state.data?.operations) renderOperations({...state.data.operations, state: "stale",
        tasks: (state.data.operations.tasks || []).map(row => row.complete_evidence ? row : {...row, stale: true,
          eta: {state: "stale", message: "本次快照無法讀取，保留上次進度；完成時間暫停估算。"}})});
    } finally {
      if (request.isCurrent()) { state.inFlight = false; request.finish(); }
    }
  }
  $("storage-close").addEventListener("click", () => {
    const previous = state.selected;
    state.selected = null;
    state.path = [];
    syncSelection();
    renderExplorer();
    [...section.querySelectorAll("[data-storage-select]")].find(b => b.dataset.storageSelect === previous)?.focus();
  });
  $("storage-more").addEventListener("click", () => { state.visible += 12; renderExplorer(); });
  $("storage-operations-more").addEventListener("toggle", () => {
    rememberOpenOperations();
    renderOtherOperations();
  });
  $("storage-tree").parentElement.addEventListener("toggle", () => {
    if ($("storage-tree").parentElement.open) {
      const group = volumes().find(v => v.id === state.selected)?.classification;
      if (group) { $("storage-tree").replaceChildren(...group.tree.map(treeNode)); $("storage-tree").dataset.revision = group.observed_at_utc || "unknown"; }
    }
  });
  core.observeVisibility(section, visible => { if (visible) void refresh(); });
  document.addEventListener("visibilitychange", () => { if (!document.hidden) void refresh(); });
  setInterval(() => { void refresh(); }, 60000);
  void refresh();
})();

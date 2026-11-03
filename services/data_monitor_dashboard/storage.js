/* Metadata-only storage explorer. No browser or API action controls a machine. */
(() => {
  "use strict";
  const section = document.getElementById("storage-inventory");
  if (!section) return;
  const core = window.StockAgentDashboard;
  const fetchJson = core.createJsonFetcher({timeoutMs: 12000, cache: "no-store", expectedRoot: "object"});
  const requests = core.createLatestRequest();
  const $ = id => section.querySelector(`#${id}`);
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
        : !row.observed_at_utc ? "等待原始收據"
          : row.owner_state === "failed" ? "原執行者異常"
            : row.eta?.reason === "clock_ahead" ? "來源時鐘待核對"
            : row.state === "blocked" ? operationStates.blocked
              : row.owner_state === "inactive" ? "等待原排程"
                : row.progress_stale ? "等待新進度計數"
                  : operationStates[row.state] || operationStates.unknown;
    heading.append(make("h4", "", row.label), make("span", `storage-state storage-operation-${row.state}`, label));
    article.append(heading, make("p", "storage-operation-phase", row.phase_label));
    if (row.reason && ["waiting", "blocked"].includes(row.state) && !row.complete_evidence) {
      article.append(make("p", "storage-copy storage-operation-reason", row.reason));
    }
    if (!row.complete_evidence && row.previous_source_failure?.message) {
      article.append(make("p", "storage-copy storage-operation-reason", row.previous_source_failure.message));
    }
    if (row.diagnosis_at_utc && !row.complete_evidence) {
      article.append(make("p", "storage-clock", `最近一次鎖診斷 ${date(row.diagnosis_at_utc)}（台北）；不代表此刻仍持鎖。`));
    }
    const p = row.progress || {};
    const parts = [];
    if (known(p.completed_bytes) && known(p.total_bytes)) parts.push(`${bytes(p.completed_bytes)}／${bytes(p.total_bytes)}`);
    if (known(p.completed_bytes) && !known(p.total_bytes)) parts.push(`${row.phase === "censusing_source" ? "已清點" : "已處理"} ${bytes(p.completed_bytes)}（總量待清點）`);
    if (known(p.completed_bytes) && known(p.total_bytes) && p.completed_bytes < p.total_bytes) parts.push(`剩 ${bytes(p.total_bytes - p.completed_bytes)}`);
    if (known(p.completed_items) && known(p.total_items)) parts.push(`${number(p.completed_items)}／${number(p.total_items)} ${["cooperative", "retained_verification", "vast_spool"].includes(row.kind) ? "packs" : "項"}`);
    if (known(p.completed_items) && !known(p.total_items)) parts.push(`已處理 ${number(p.completed_items)} 項（總數待清點）`);
    const m = row.metrics || {};
    if (known(m.reclaimed_allocated_bytes)) parts.push(`已釋放 WSL ${bytes(m.reclaimed_allocated_bytes)}`);
    if (known(m.discarded_allocated_bytes)) parts.push(`已刪除測試副本、釋放 WSL ${bytes(m.discarded_allocated_bytes)}`);
    if (row.kind === "workspace_transport_comparison") {
      if (known(m.same_files)) parts.push(`一致 ${number(m.same_files)} 檔`);
      if (known(m.different_files)) parts.push(`內容不同 ${number(m.different_files)} 檔`);
      if (known(m.missing_files)) parts.push(`E 未找到 ${number(m.missing_files)} 檔`);
      if (known(m.unstable_files) && m.unstable_files) parts.push(`讀取中變動 ${number(m.unstable_files)} 檔`);
      if (known(m.unavailable_files) && m.unavailable_files) parts.push(`無法確認 ${number(m.unavailable_files)} 檔`);
    }
    if (row.kind === "workspace_transport" && known(m.stale_mount_namespaces) && m.stale_mount_namespaces) parts.push(`尚有 ${number(m.stale_mount_namespaces)} 個舊掛載命名空間`);
    if (known(m.files_held) && m.files_held) parts.push(`保留 ${number(m.files_held)} 檔待核對`);
    if (known(m.planned_reclaim_allocated_bytes)) parts.push(`清冊預計回收 ${bytes(m.planned_reclaim_allocated_bytes)}；尚未釋放`);
    if (known(m.planned_source_files)) parts.push(`清冊 ${number(m.planned_source_files)} 個檔案`);
    if (known(m.penguin_packs) && known(m.vast_packs)) parts.push(`Penguin ${number(m.penguin_packs)} · Vast ${number(m.vast_packs)}`);
    if (known(m.retained_packs)) parts.push(`沿用 ${number(m.retained_packs)} 份固定驗證證據`);
    if (known(m.pending_batches)) parts.push(`待掃描 ${number(m.pending_batches)} 批`);
    if (known(m.pending_bytes)) parts.push(`接收端待傳 ${bytes(m.pending_bytes)}`);
    if (known(m.active_jobs)) parts.push(`${row.kind === "vast_spool" ? "Vast 預驗證" : "NAS 管線"}目前 ${number(m.active_jobs)} 個工作`);
    if (known(m.queued_jobs)) parts.push(`上次回報待排 ${number(m.queued_jobs)} 筆`);
    if (known(m.last_cycle_accepted_jobs)) parts.push(`該輪已驗收 ${number(m.last_cycle_accepted_jobs)} 筆`);
    if (known(m.retry_jobs) && m.retry_jobs) parts.push(`待重試 ${number(m.retry_jobs)} 筆`);
    if (known(m.rejected_jobs) && m.rejected_jobs) parts.push(`未通過 ${number(m.rejected_jobs)} 筆`);
    article.append(make("p", "storage-operation-amount", parts.join(" · ") || "等待原程序提供工作量"));
    if (!row.complete_evidence && row.reason_code === "physical_capacity" && known(m.source_free_bytes) && known(m.source_reserve_bytes)) {
      article.append(make("p", "storage-copy storage-operation-capacity-wait",
        `來源磁碟可用 ${bytes(m.source_free_bytes)}，此管線保留 ${bytes(m.source_reserve_bytes)}；原排程先對帳、回收，空間恢復後重新清點並派送。`));
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
    if (row.phase !== "waiting_accepted_transport") article.append(bar(amount, total, "本階段計量進度"));
    if (typeof p.percent === "number") article.append(make("p", "storage-clock",
      `${row.phase === "waiting_new_sealed_packs" ? "目前候選" : "本階段"} ${p.percent.toFixed(2)}%${row.phase === "waiting_new_sealed_packs" ? " · 等候新封包" : !row.complete_evidence && p.percent >= 100 ? " · 等待正式驗收" : ""}`));
    const eta = row.eta || {};
    let estimate = row.complete_evidence && row.kind === "local_testing_cleanup"
      ? `固定測試清單清理完成 ${date(row.observed_at_utc)}（台北）；這是直接刪除回執，並非冷庫或 NAS 還原證明。`
      : row.complete_evidence ? `本範圍完成 ${date(row.observed_at_utc)}（台北）；已通過原程序驗收。`
      : row.phase === "waiting_accepted_transport" ? "目前沒有待回收的已驗收副本；新批次驗收後由原排程接續。"
        : `本階段預估完成：${eta.message || "等待工作量與速度取樣"}`;
    if (eta.state === "estimated" && known(eta.remaining_seconds) && eta.completion_at_utc) {
      estimate = `${eta.scope === "remaining_source_content" ? "來源內容核對預估完成" : "本階段預估完成"} ${date(eta.completion_at_utc)}（台北；剩 ${duration(eta.remaining_seconds)}）`;
      if (known(eta.fastest_seconds)) estimate += ` · 最快 ${duration(eta.fastest_seconds)}`;
      if (known(eta.slowest_seconds)) estimate += `／較慢 ${duration(eta.slowest_seconds)}`;
      else if (known(eta.fastest_seconds)) estimate += "／較慢上界待估";
      if (typeof eta.rate_per_second === "number") estimate += ` · ${eta.unit === "bytes" ? bytes(Math.round(eta.rate_per_second)) : eta.rate_per_second.toFixed(2) + " 項"}/s`;
      if (eta.scope === "remaining_source_content") estimate += "；依組內逐檔進度估算，後續封存提交與還原另計。";
    }
    article.append(make("p", "storage-operation-estimate", estimate));
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
      const text = row.stale ? "本次尚未取得新觀測，無法確認自動接續狀況"
        : follow.owner_running ? "原排程本輪工作中；各項條件通過後接續"
        : follow.automatic && follow.next_check_at_utc ? `自動接續已啟用 · 下次排程檢查 ${date(follow.next_check_at_utc)}（台北）`
          : follow.automatic && follow.check_due ? "已到原排程檢查時間；等待最新執行回報"
            : follow.automatic ? "自動接續已啟用；下一次執行時間待原排程回報"
              : "尚未確認自動接續排程";
      article.append(make("p", "storage-clock storage-operation-continuation", text));
      if (row.next_attempt_at_utc && !row.stale) article.append(make("p", "storage-clock",
        `原程序預定重試 ${date(row.next_attempt_at_utc)}（台北）；每分鐘資訊檢查不會提前重跑驗證。`));
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
    if (row.next_attempt_at_utc) details.append(make("p", "storage-clock", `原佇列預定重試 ${date(row.next_attempt_at_utc)}（台北）；過期時間不代表正在執行。`));
    if (known(m.admission_available_bytes)) details.append(make("p", "storage-copy", `接收端可再接納 ${bytes(m.admission_available_bytes)}；不代表 NAS 已完成。`));
    if (known(m.lab_reclaimed_allocated_bytes)) details.append(make("p", "storage-copy", `lab203 累積回收 ${bytes(m.lab_reclaimed_allocated_bytes)}；與本機 WSL 分開計算。`));
    article.append(details);
    return article;
  }
  function renderOperations(value) {
    const valid = value?.contract === "stockagent.storage_operations.v1" && value.read_only === true && !!value.observed_at_utc;
    const observerStale = !valid || value.state === "stale";
    $("storage-operations-observed").textContent = valid
      ? `備份與驗證快照 ${date(value.observed_at_utc)}（台北）${value.state === "stale" ? " · 觀測已過期，保留上次收據" : " · 背景與頁面每 60 秒更新"}`
      : "備份與驗證進度尚未取得；磁碟容量仍獨立顯示。";
    const summary = valid ? value.summary || {} : {};
    const discarded = valid && Array.isArray(value.tasks) ? value.tasks
      .filter(row => row.kind === "local_testing_cleanup" && known(row.metrics?.discarded_allocated_bytes))
      .reduce((total, row) => total + row.metrics.discarded_allocated_bytes, 0) : 0;
    $("storage-operations-summary").replaceChildren(...[
      ["執行中", summary.running], ["等待／前置待處理", known(summary.waiting) && known(summary.blocked) ? summary.waiting + summary.blocked : null],
      ["本範圍已驗收", summary.completed], ["已釋放 WSL", bytes(known(summary.penguin_reclaimed_allocated_bytes)
        ? summary.penguin_reclaimed_allocated_bytes + discarded : null)]
    ].map(([label, amount]) => {
      const card = make("div", "storage-operation-stat");
      card.append(make("span", "storage-copy", label), make("strong", "", typeof amount === "string" ? amount : number(amount)));
      return card;
    }));
    const overall = value?.overall_eta || {};
    $("storage-operations-eta").textContent = !observerStale && overall.known_completion_at_utc
      ? `可估工作目前最晚預估 ${date(overall.known_completion_at_utc)}（台北；${number(overall.estimated_tasks)} 項已有估計、${number(overall.unestimated_tasks)} 項待估${overall.blocked_tasks ? `，其中 ${number(overall.blocked_tasks)} 項有阻擋` : ""}）。各卡片分列目前階段、批次及可執行佇列；後續未知階段另計。`
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
    const open = new Set([...section.querySelectorAll("[data-operation-detail][open]")].map(node => node.dataset.operationDetail));
    const focused = document.activeElement?.closest("[data-operation-detail]")?.dataset.operationDetail;
    const priority = {running: 0, blocked: 1, waiting: 2, unknown: 3, completed: 4};
    const rows = valid && Array.isArray(value.tasks) ? [...value.tasks].sort((a, b) =>
      (priority[a.state] ?? 3) - (priority[b.state] ?? 3)
      || Number(b.kind === "local_testing_cleanup") - Number(a.kind === "local_testing_cleanup")
      || Number(b.kind === "retirement") - Number(a.kind === "retirement")) : [];
    const publication = !observerStale && rows.find(row => row.kind === "migration_publication"
      && row.state === "running" && !row.stale);
    const source = publication?.metrics || {};
    const sourceCount = known(source.processed_source_files) && known(source.total_source_files)
      ? `；已處理 ${number(source.processed_source_files)}／${number(source.total_source_files)} 個來源檔`
      : "";
    $("storage-operations-focus").textContent = publication
      ? `目前來源封存主線：${publication.label}，${publication.phase_label}${sourceCount}。Vast 可並行預驗證已封閉的包；整份來源提交後，原程序才進行完整還原與回收判定。`
      : "各節點分工、目前階段與等待條件如下；預驗證完成量與可回收空間分開計算。";
    // Detailed source blockers belong after the six pipeline summaries.
    const primary = rows.filter(row => row.kind !== "migration_dependency").slice(0, 6);
    const primaryIds = new Set(primary.map(row => row.id));
    const other = rows.filter(row => !primaryIds.has(row.id));
    const cards = [...primary, ...other].map(operationCard);
    for (const card of cards) {
      const detail = card.querySelector("[data-operation-detail]");
      detail.open = open.has(detail.dataset.operationDetail);
    }
    $("storage-operation-tasks").replaceChildren(...cards.slice(0, 6));
    $("storage-operation-other-tasks").replaceChildren(...cards.slice(6));
    if (focused) [...section.querySelectorAll("[data-operation-detail]")].find(node => node.dataset.operationDetail === focused)
      ?.querySelector("summary")?.focus({preventScroll: true});
    $("storage-operations-more").hidden = cards.length <= 6;
    $("storage-operations-more-label").textContent = `其他工作與已完成階段（${Math.max(0, cards.length - 6)}）`;
    const schedule = valid && Array.isArray(value.schedules) ? value.schedules : [];
    const flow = valid && Array.isArray(value.flow) ? value.flow : [];
    const stageLabels = new Map(flow.map(row => [row.id, row.label]));
    $("storage-operation-flow").replaceChildren(...flow.map(row => {
      const entry = make("div", "storage-operation-stage");
      entry.append(make("strong", "", row.label), make("span", "storage-copy", observerStale ? "流程觀測已過期" : operationStates[row.state] || "等待觀測"),
        make("p", "storage-copy", row.condition));
      if (Array.isArray(row.depends_on) && row.depends_on.length) entry.append(make("small", "storage-clock",
        `逐固定版本接續：${row.depends_on.map(id => stageLabels.get(id) || "前一階段").join("、")}。其他版本可並行。`));
      return entry;
    }));
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
          waiting_publication_window: "等待原發布鎖", bounded_cycle_complete: "本輪冷區整理完成",
          partial_failures_preserved: "部分批次失敗，原檔保留", existing_tier_owner_busy: "冷區整理執行中"};
        const schedule = tiers.schedule;
        const window = schedule ? `；每日 ${String(schedule.start_hour).padStart(2, "0")}:00–${String(schedule.end_hour).padStart(2, "0")}:00（台北）處理穩定超過 ${schedule.stable_after_hours} 小時的增量` : "";
        const work = tiers.last_work_cycle;
        const last = work ? `。上次有工作 ${date(work.observed_at_utc)}：${known(work.objects_completed) ? work.objects_completed : "—"} 個物件、原值 ${bytes(work.logical_bytes_processed)}，配置空間減少 ${bytes(work.allocation_bytes_saved)}；此為單輪成果，非全庫壓縮率。` : "。尚無已登錄的正式整理成果。";
        $("storage-tiers").textContent = `D 增量→穩定冷區：${tiers.stale ? "觀測已過期 · " : ""}${labels[tiers.state] || "等待狀態"}${window}${last}`;
      }
      renderCards();
      renderOperations(data.operations);
      renderRuntime(data.runtime);
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

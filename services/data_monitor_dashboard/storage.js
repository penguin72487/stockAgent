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
  const state = {data: null, selected: null, path: [], visible: 12, loaded: false, inFlight: false, inView: false};
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
    const low = cap.complete && known(cap.available_bytes) && cap.available_bytes / cap.total_bytes < .1;
    if (low) article.classList.add("storage-pressure");
    const heading = make("div", "storage-disk-heading");
    heading.append(make("h4", "", volume.label), make("span", `storage-state storage-state-${cap.state}`, states[cap.state] || "等待量測"));
    const size = make("p", "storage-size", cap.complete ? bytes(cap.used_bytes) : bytes(cap.available_bytes));
    size.append(make("small", "", cap.complete ? ` 已用／${bytes(cap.total_bytes)}` : " 可用；總容量待量測"));
    const fact = make("p", "storage-copy", cap.complete
      ? `可用 ${bytes(cap.available_bytes)} · 使用率 ${pct.toFixed(1)}%${known(cap.reserved_free_bytes) && cap.reserved_free_bytes > 0 ? ` · 系統保留 ${bytes(cap.reserved_free_bytes)}` : ""}`
      : "缺少整體容量資訊，暫不計算使用率。");
    const measure = make("p", "storage-clock", `容量量測 ${date(cap.observed_at_utc)}（台北）`);
    const relation = make("p", "storage-copy", volume.role);
    if (volume.contained_in) relation.append(make("span", "storage-containment", `包含於 ${volumes().find(v => v.id === volume.contained_in)?.label || "Windows 磁碟"}，不可加總`));
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
    const phases = {complete: "已登錄目錄已清點", partial: "部分目錄未成功或掃描期間有變動", scanning: `背景清點中，開始 ${date(group.started_at_utc)}`,
      unavailable: "本次清點失敗；保留上次結果", not_scanned: "未取得分類，等待觀測端"};
    const coverage = known(group.roots_total) && known(group.roots_completed)
      ? `已完成 ${group.roots_completed}／${group.roots_total} 個根目錄。` : "";
    $("storage-classification-state").textContent = `${phases[group.state] || "等待清點"}。${basis}${group.stale ? "；此分類已過期" : ""}。${coverage}${group.allocated_bytes_is_lower_bound ? "≥ 表示已完成子目錄的實測下限；比例僅比較已量測內容，尚未掃描不當作零。" : ""}只含已登錄專案目錄；總磁碟其餘用量包含系統及其他檔案。`;
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
    if (rows.length !== 6 || new Set(rows.map(v => v.id)).size !== 6 || rows.some(v => !v.capacity || !v.classification || !Array.isArray(v.classification.tree))) throw new Error("Invalid storage volumes");
  }
  async function refresh() {
    if (state.inFlight || document.hidden || (!state.inView && state.loaded)) return;
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
      $("storage-observed").textContent = `6 個磁碟位置 · ${fresh} 個有近期完整容量、${partial} 個只有部分容量資訊。快照 ${date(data.generated_at_utc)}（台北）；容量每分鐘更新，分類每六小時清點。`;
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
      renderExplorer();
      $("storage-error").hidden = true;
    } catch (error) {
      if (error.name === "AbortError" || !request.isCurrent()) return;
      $("storage-error").hidden = false;
      $("storage-error").textContent = state.loaded ? "本次儲存快照讀取失敗，保留上次結果；請留意各項量測時間。" : "儲存快照暫時無法讀取，稍後自動重試。";
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
  core.observeVisibility(section, visible => { state.inView = visible; if (visible) void refresh(); });
  document.addEventListener("visibilitychange", () => { if (!document.hidden) void refresh(); });
  setInterval(() => { void refresh(); }, 30000);
  void refresh();
})();

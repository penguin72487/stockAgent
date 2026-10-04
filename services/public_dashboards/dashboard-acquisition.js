/* Presentation only: reuse rendered, sanitized fields; never query a provider.
 * Keep original nodes/IDs/listeners and source-specific denominators intact. */
(function () {
  "use strict";

  const metric = (label, source, note, fallback = "") => ({label, source, note, fallback});
  const group = (key, label, hint, selectors) => ({key, label, hint, selectors});
  const profiles = Object.freeze({
    shioaji: {
      title: "永豐資料抓取", lead: "即時擷取、歷史回補與當日流量；只讀本站觀測。",
      metrics: [
        metric("即時擷取", "capture-state", "capture-session"),
        metric("期貨 tick 回補", "fleet-progress-label", "", "僅此回補範圍，不是全市場歷史完整率"),
        metric("當日已用流量", "traffic-used", "traffic-remaining"),
        metric("期貨 tick 剩餘估時", "fleet-eta-label", "fleet-eta-detail"),
      ],
      progress: ["fleet-progress-bar", "fleet-progress-label", "期貨 tick 回補進度", "fleet-progress-detail"],
      next: ["backfill-state", "回補狀態"],
      groups: [
        group("progress", "進度與執行排程", "回補合約、即時訂閱及執行狀態", ["#backfill", "#capture"]),
        group("quota", "配額與流量", "當日流量、曲線與請求對帳", ["#traffic"]),
        group("catalog", "完整資料清冊", "各管線、來源與篩選", ["#pipelines"]),
        group("storage", "儲存與交付", "磁碟、來源檔與冷庫", ["#storage"]),
        group("method", "統計口徑與限制", "證據、配額單位及可用性", ["#method"]),
      ],
    },
    finlab: {
      title: "FinLab 資料抓取", lead: "先看下載工作、估計資料量與完成時間，再展開完整目錄。",
      metrics: [
        metric("下載器狀態", "capture-state", "capture-key"),
        metric("估計下載量進度", "volume-progress-label", "volume-downloaded"),
        metric("當日已用流量", "quota-used", "quota-limit"),
        metric("本輪待辦剩餘估時", "download-global-eta", "", "參考情境；未知歷史與 tick 另列"),
      ],
      progress: ["volume-progress", "volume-progress-label", "估計下載量進度", "volume-basis"],
      next: ["download-next", "下次排程"],
      groups: [
        group("progress", "進度與執行排程", "分階段估時、工作量與執行摘要", ["#workload", "#capture"]),
        group("quota", "配額與流量", "每日用量、重置與曲線", ["#traffic"]),
        group("catalog", "完整資料清冊", "管線、全部資料鍵與全市場 tick", ["#pipelines", "#backfill"]),
        group("storage", "儲存與交付", "本機容量、冷庫與訓練守門", ["#storage"]),
        group("method", "統計口徑與限制", "估計依據及證據邊界", ["#method"]),
      ],
    },
    finmind: {
      title: "FinMind 資料抓取", lead: "先看目前工作、候選進度與估時；到期追新優先，美股分鐘最後。",
      metrics: [
        metric("下載器狀態", "finmind-health", "capture-key"),
        metric("候選搜尋進度", "download-progress-label", "", "包含待查候選，不是資料完整率"),
        metric("最近 60 分鐘本站請求", "quota-used", "quota-limit"),
        metric("剩餘工作估時", "download-global-eta", "", "條件式估算，不是成功倒數"),
      ],
      progress: ["download-progress", "download-progress-label", "候選搜尋進度 · 非資料完整率", "download-progress-detail"],
      next: ["capture-next", "下次排程"],
      groups: [
        group("progress", "進度與執行排程", "九類順序、分階段估時、工作量與下載明細", ["#backfill", "#capture"]),
        group("quota", "配額與流量", "滾動 60 分鐘本站請求及官方上限", ["#traffic"]),
        group("catalog", "完整資料清冊", "所有資料集、更新觀測與篩選", ["#pipelines"]),
        group("storage", "儲存與交付", "來源容量與訓練邊界", ["#storage"]),
        group("method", "統計口徑與限制", "來源空回、未知歷史與發布證據", ["#method"]),
      ],
    },
    tej: {
      title: "TEJ 資料抓取", lead: "先看桌面下載工作、目標範圍與估時；不操作 Excel 或 TEJ API。",
      metrics: [
        metric("目前工作步驟", "active-stage", "active-table"),
        metric("已查驗目標範圍", "scope-progress-label", "", "包含明確來源空回，不代表都有數值"),
        metric("最近 60 分鐘工作開始", "desktop-operations", "", "桌面工作，不是 HTTP 請求"),
        metric("剩餘工時 · 中間情境", "global-eta", "forecast-execution"),
      ],
      progress: ["scope-progress", "scope-progress-label", "已完成查詢範圍 · 含明確空回", "global-eta-basis"],
      next: ["tej-automation", "實際排程"], refresh: "refresh-now",
      groups: [
        group("progress", "進度與執行排程", "目前工作、讀回與分階段條件式估時", ["#activity", "#progress"]),
        group("quota", "配額與操作量", "桌面限額及本機操作量；不是 API 流量", ["#quota"]),
        group("catalog", "完整資料清冊", "帳號可見分類、表格與個別欄位", ["#overview", "#tables", "#features"]),
        group("method", "統計口徑與限制", "原生觀測、發布、匯出與授權邊界", ["#method"]),
      ],
    },
    openbb: {
      title: "OpenBB 資料抓取", lead: "先看封存進度、待辦與實際吞吐，再查看各供應商配額。",
      metrics: [
        metric("已接受目標進度", "completion-percent", "accepted-fraction"),
        metric("可處理待辦", "unresolved-tasks", "", "尚未接受且非永久不可用"),
        metric("已取得資料列", "success-rows", "", "實際寫入 Parquet 的列數"),
        metric("最近 15 分鐘吞吐", "recent-throughput", "recent-detail"),
      ],
      progress: ["progress-fill", "completion-percent", "固定目標已接受進度", "archive-boundary"],
      groups: [
        group("progress", "進度與執行排程", "固定邊界、待辦與歷史進度曲線", ["#overview", "#trend"]),
        group("quota", "配額與來源機制", "各供應商速率、並行及配額限制", ["#providers"]),
        group("catalog", "完整資料清冊", "資料分類與所有稽核警示", ["#categories", "#alerts"]),
        group("method", "統計口徑與限制", "已接受、不確定與永久不可用分開", ["#method"]),
      ],
    },
    "data-monitor": {
      title: "全資料抓取總覽", lead: "先看需處理的端點，再按來源與市場查看完整資料及特徵。",
      metrics: [
        metric("正在抓／未最新", "catching-up-items", "", "回補與追新端點"),
        metric("正在串流", "streaming-items", "", "即時訂閱端點"),
        metric("當期目標已完成", "completed-items", "", "不是全部歷史完整率"),
        metric("需處理端點", "unable-items", "", "阻擋、錯誤或尚有缺口"),
      ],
      progress: ["overall-progress", "overall-percent", "主動端點當期完成比例", "overall-denominator"],
      next: ["tw-calendar-next", "台股現貨下一時點"],
      groups: [
        group("progress", "進度與執行排程", "台股現貨日曆、協作守門與 FinLab 詳情", [".calendar-banner", ".acquisition", ".finlab-monitor"]),
        group("catalog", "所有來源與資料清冊", "全 provider、按市場分類及每個端點", ["#provider-directory", "section[aria-labelledby='category-title']", "#source-list", ".kpis"]),
        group("features", "所有特徵明細", "每個特徵獨立列出；依原清冊搜尋與分頁", ["#feature-list"]),
        group("storage", "儲存與證據", "實體群組、首末資料與可用性界線", ["section[aria-labelledby='groups-title']", ".boundary-note"]),
      ],
    },
    provider: {
      lead: "先看回補、需處理端點與配額，再展開每個資料集的歷史及更新時間。",
      metrics: [
        metric("正在抓／未最新", "provider-catching", "", "依現有收據判定"),
        metric("需處理端點", "provider-unable", "", "檢查阻擋或缺口"),
        metric("主動端點", "provider-active", "", "端點數，不是成交筆數"),
        metric("來源配額／已用", "provider-quota", "provider-quota-basis"),
      ],
      progress: ["provider-progress", "provider-ratio", "主動端點當期完成比例", "provider-ratio-basis"],
      groups: [
        group("quota", "配額與來源機制", "可驗證用量、市場範圍及專用面板", [".provider-context"]),
        group("catalog", "完整資料清冊", "所有端點、狀態、實存首末資料與更新時間", ["#provider-sources", ".provider-kpis"]),
        group("method", "統計口徑與限制", "取得、歷史完整性與可訓練性分開", [".boundary-note"]),
      ],
    },
    traffic: {
      title: "網站流量與延遲", lead: "本站閘道的流量與延遲；不是資料供應商的 API 配額。",
      metrics: [
        metric("最近 1 分鐘請求", "kpi-requests", "", "完成的 GET／HEAD"),
        metric("平均每秒請求", "kpi-rps", "", "最近 1 分鐘本站吞吐"),
        metric("延遲 p95", "kpi-p95", "", "閘道 wall time histogram 上界"),
        metric("最近 1 分鐘錯誤率", "kpi-errors", "", "本站已完成請求，不是供應商失敗率"),
      ], refresh: "refresh-now", groups: [],
    },
  });

  function node(tag, className, text) {
    const result = document.createElement(tag);
    if (className) result.className = className;
    if (text !== undefined) result.textContent = text;
    return result;
  }

  let navigationRevision = 0;
  // Also used by existing programmatic scroll controls, not just anchor clicks.
  function reveal(target) {
    const element = typeof target === "string" ? document.getElementById(target.replace(/^#/, "")) : target;
    if (!element) return null;
    navigationRevision += 1;
    if (element.tagName === "DETAILS") element.open = true;
    for (let ancestor = element.parentElement; ancestor; ancestor = ancestor.parentElement) {
      if (ancestor.tagName === "DETAILS") ancestor.open = true;
    }
    return element;
  }

  function createDisclosure(main, definition, elements, index) {
    const details = node("details", "acq-details");
    details.id = `acq-${definition.key}`;
    const summary = node("summary", "acq-disclosure-heading");
    summary.append(node("span", "acq-section-number", String(index + 1).padStart(2, "0")));
    const copy = node("span", "acq-disclosure-copy");
    copy.append(node("strong", "", definition.label), node("small", "", definition.hint));
    summary.append(copy);
    const content = node("div", "acq-detail-body");
    for (const element of elements) content.append(element);
    details.append(summary, content);
    main.append(details);
    details.addEventListener("toggle", () => {
      // Existing lazy loaders may have no IntersectionObserver on older clients.
      if (details.open) document.dispatchEvent(new CustomEvent("acquisition:reveal", {detail: {id: details.id}}));
    });
    return details;
  }

  function mountHub(main) {
    const cards = document.getElementById("dashboards");
    if (!cards) return;
    const trading = node("section", "cards acq-hub-trading");
    trading.id = "strategy-dashboards";
    trading.setAttribute("aria-label", "策略模擬面板");
    for (const route of ["taifex/", "tw-day-trade/", "tw-overnight/"]) {
      const card = cards.querySelector(`a[href='${route}']`);
      if (card) trading.append(card);
    }
    const directory = cards.querySelector("a[href='data-monitor/']");
    if (directory) cards.prepend(directory);
    cards.before(node("h2", "acq-hub-title", "資料抓取與來源總覽"));
    cards.after(node("h2", "acq-hub-title", "策略模擬"), trading);
  }

  function mount() {
    const kind = document.body.dataset.acquisitionPage;
    if (!kind) return;
    const main = document.querySelector("main");
    if (!main) return;
    if (kind === "overview") { mountHub(main); return; }
    const profile = profiles[kind];
    if (!profile) return;
    // Validate first, so a stale HTML asset leaves the original UI intact.
    const sources = profile.metrics.map((entry) => ({
      ...entry, element: document.getElementById(entry.source),
      noteElement: entry.note ? document.getElementById(entry.note) : null,
    }));
    const progressSource = profile.progress && document.getElementById(profile.progress[0]);
    if (sources.some((entry) => !entry.element || (entry.note && !entry.noteElement)) || (profile.progress && !progressSource)) {
      console.error("Acquisition summary field mismatch", kind);
      return;
    }
    const definitions = profile.groups.map((definition) => ({
      definition, elements: definition.selectors.map((selector) => main.querySelector(selector)),
    }));
    if (definitions.some(({elements}) => elements.some((element) => !element))) {
      console.error("Acquisition detail section mismatch", kind);
      return;
    }

    const hero = main.querySelector(".hero, .provider-hero");
    if (hero) {
      if (profile.title) hero.querySelector("h1").textContent = profile.title;
      const lead = hero.querySelector(".lead, .subtitle");
      if (lead) lead.textContent = profile.lead;
    }
    const overview = node("section", "acq-overview");
    overview.id = "acq-overview";
    overview.setAttribute("aria-labelledby", "acq-overview-title");
    const heading = node("header", "acq-overview-heading");
    heading.append(node("h2", "", "抓取摘要"));
    heading.firstChild.id = "acq-overview-title";
    heading.append(node("span", "", kind === "traffic" ? "網站觀測 · 非 provider 配額" : "詳細資料與清冊可於下方展開"));
    if (profile.refresh) {
      const refresh = document.getElementById(profile.refresh);
      if (refresh) heading.append(refresh); // Original handler and identity.
    }
    overview.append(heading);
    const metrics = node("div", "acq-metrics");
    const bindings = [];
    for (const entry of sources) {
      const card = node("article", "acq-metric");
      const value = node("strong", "acq-metric-value", "讀取中");
      value.dataset.acqSource = entry.source;
      const note = node("small", "acq-metric-note", entry.fallback);
      card.append(node("span", "acq-metric-label", entry.label), value, note);
      metrics.append(card);
      bindings.push([entry.element, value]);
      if (entry.noteElement) bindings.push([entry.noteElement, note]);
    }
    overview.append(metrics);
    if (profile.progress) {
      const [sourceId, labelId, caption, basisId] = profile.progress;
      const line = node("div", "acq-progress");
      const header = node("div", "acq-progress-header");
      const label = node("strong", "", "分母待確認");
      header.append(node("span", "", caption), label);
      const bar = node("progress", "acq-progress-bar");
      bar.dataset.acqSource = sourceId;
      bar.setAttribute("aria-label", caption);
      const basis = node("small", "acq-progress-basis");
      line.append(header, bar, basis);
      overview.append(line);
      if (document.getElementById(labelId)) bindings.push([document.getElementById(labelId), label]);
      if (basisId && document.getElementById(basisId)) bindings.push([document.getElementById(basisId), basis]);
      bindings.push([progressSource, bar]);
      // Health already carries ratio and the visible copy is here; no duplication.
      progressSource.closest(".hero-status, .provider-health-card")?.classList.add("acq-health-compact");
    }
    if (profile.next) {
      const next = document.getElementById(profile.next[0]);
      if (next) {
        const line = node("p", "acq-next");
        const value = node("span", "", "待觀測");
        line.append(node("strong", "", `${profile.next[1]}：`), value);
        overview.append(line);
        bindings.push([next, value]);
      }
    }
    // Alerts stay in front of the overview; grouped sections move behind it.
    const alert = main.querySelector("#pipeline-alert, #page-alert, #runtime-alert, #provider-error");
    if (alert) alert.after(overview);
    else if (hero) hero.after(overview);
    else main.prepend(overview);
    const disclosures = definitions.map(({definition, elements}, index) => createDisclosure(main, definition, elements, index));

    if (kind === "traffic") {
      // Group the unmodified sequence including each intro, all controls/tables.
      const elements = [...main.children].filter((element) => element !== hero && element !== overview);
      const buckets = [[], [], [], []];
      let bucket = 0;
      for (const element of elements) {
        if (element.classList.contains("section-intro")) {
          const title = element.querySelector("h2")?.textContent || "";
          if (title.includes("歷史") || title.includes("長期")) bucket = 2;
          else if (title.includes("瀏覽器") || title.includes("測速")) bucket = 1;
          else if (bucket >= 2) bucket = 3;
        }
        buckets[bucket].push(element);
      }
      [
        ["progress", "即時流量與趨勢", "最近一分鐘與最近一小時"],
        ["browser", "瀏覽器反應與測速", "本機互動、解析與繪製機會"],
        ["history", "歷史用量與延遲", "原時間範圍、曲線與路由明細"],
        ["method", "快取、請求與統計口徑", "完整明細與計量限制"],
      ].forEach(([key, label, hint], index) => {
        if (buckets[index].length) disclosures.push(createDisclosure(main, {key, label, hint}, buckets[index], index));
      });
    }
    let navigation = main.querySelector(":scope > .jump-nav");
    if (!navigation) navigation = node("nav", "jump-nav");
    navigation.setAttribute("aria-label", "摘要與完整明細");
    navigation.replaceChildren();
    const link = (href, label) => {
      const anchor = node("a", "", label);
      anchor.href = href;
      navigation.append(anchor);
    };
    link("#acq-overview", "抓取摘要");
    for (const details of disclosures) link(`#${details.id}`, details.querySelector("strong").textContent);
    overview.after(navigation);
    main.classList.add("acq-mounted");

    let queued = false;
    function sync() {
      queued = false;
      for (const [source, target] of bindings) {
        if (source.tagName === "PROGRESS") {
          const maximum = source.getAttribute("max") || "1";
          if (target.getAttribute("max") !== maximum) target.setAttribute("max", maximum);
          const raw = source.getAttribute("value");
          if (raw === null || !Number.isFinite(Number(raw))) target.removeAttribute("value");
          else if (target.getAttribute("value") !== raw) target.setAttribute("value", raw);
        } else {
          const text = source.textContent.trim();
          if (target.textContent !== text) target.textContent = text;
        }
      }
    }
    const observer = new MutationObserver(() => {
      if (queued) return;
      queued = true;
      window.requestAnimationFrame(sync);
    });
    for (const [source] of bindings) observer.observe(source, {
      childList: true, characterData: true, subtree: true,
      ...(source.tagName === "PROGRESS" ? {attributes: true, attributeFilter: ["value", "max"]} : {}),
    });
    sync();
    const activateHash = () => {
      let id;
      try { id = decodeURIComponent(window.location.hash.slice(1)); } catch { return; }
      const target = reveal(id);
      const revision = navigationRevision;
      if (target) window.requestAnimationFrame(() => {
        if (revision === navigationRevision) target.scrollIntoView({block: "start", behavior: "instant"});
      });
    };
    document.addEventListener("click", (event) => {
      const anchor = event.target.closest("a[href^='#']");
      if (!anchor) return;
      const id = anchor.getAttribute("href").slice(1);
      const target = reveal(id);
      if (target?.tagName === "DETAILS" && target.classList.contains("acq-details")) target.open = true;
    });
    window.addEventListener("hashchange", activateHash);
    if (window.location.hash) activateHash();
  }

  window.StockAgentAcquisition = Object.freeze({profiles, reveal});
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", mount, {once: true});
  else mount();
}());

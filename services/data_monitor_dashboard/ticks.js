/* Shared, snapshot-only Tick catalog for the overview and provider pages. */
(() => {
  "use strict";
  const section = document.getElementById("tick-inventory");
  if (!section) return;
  const core = window.StockAgentDashboard;
  const fetchJson = core.createJsonFetcher({timeoutMs: 15000, cache: "no-store", expectedRoot: "object"});
  const requests = core.createLatestRequest();
  const integer = new Intl.NumberFormat("zh-TW", {maximumFractionDigits: 0});
  const clock = new Intl.DateTimeFormat("zh-TW", {timeZone: "Asia/Taipei", hour12: false,
    year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit"});
  const markets = {taiwan_equity: "台股／指數", taiwan_derivatives: "臺灣期貨／選擇權",
    global_equity: "海外股票", forex: "外匯", crypto: "加密貨幣"};
  const labels = {receipt_index: "收據索引統計", parquet_footers: "實存列數已核實",
    unattributed_footers: "實存列數／未配對收據",
    capture_manifests: "已落盤清冊統計", partial: "統計未齊", unavailable: "統計暫不可讀",
    not_downloaded: "尚無登記資料", not_enabled: "未啟用", not_available: "來源不提供 tick"};
  const providerPath = location.pathname.match(/^\/data-monitor\/providers\/(.+)\/$/);
  let fixedProvider = "all";
  if (providerPath) {
    try { fixedProvider = decodeURIComponent(providerPath[1]); }
    catch { return; }
  }
  const state = {rows: [], revision: null, inFlight: false, loaded: false, inView: false};
  const make = (tag, className, text) => {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = String(text);
    return node;
  };
  const known = value => Number.isSafeInteger(value) && value >= 0;
  const count = value => known(value) ? integer.format(value) : "待核實";
  const date = value => typeof value === "string" && value ? value.slice(0, 10) : "待核實";

  const body = make("div", "tick-catalog-body");
  // Static template only. All source metadata below is inserted with textContent.
  body.innerHTML = `
    <p id="tick-observed" class="tick-observed">等待監控快照。</p>
    <form id="tick-filters" class="tick-filters" role="search" aria-label="篩選逐筆資料">
      <label>搜尋資料集／合約<input id="tick-search" type="search" maxlength="120" placeholder="例如 台股、TXO、JWFR2" autocomplete="off"></label>
      <label id="tick-provider-label">來源<select id="tick-provider"><option value="all">全部來源</option></select></label>
      <label>市場<select id="tick-market"><option value="all">全部市場</option></select></label>
      <label>資料種類<select id="tick-kind"><option value="all">全部種類</option><option value="trade">逐筆成交</option><option value="book">五檔委買賣事件</option><option value="book_snapshot">每秒五檔快照</option></select></label>
      <label>清冊層級<select id="tick-detail"><option value="0">依來源與商品分類</option><option value="1">展開連續期貨各合約</option></select></label>
    </form>
    <p id="tick-result-count" class="tick-result-count" role="status">讀取逐筆清冊中</p>
    <p id="tick-error" class="provider-error" role="alert" hidden></p>
    <div class="table-scroll tick-table-scroll" tabindex="0" role="region" aria-label="逐筆成交與五檔清冊，可水平捲動">
      <table class="tick-table"><thead><tr><th>市場／來源／資料集</th><th>筆數與涵蓋天數</th><th>最早／最新資料日</th><th>統計證據</th></tr></thead><tbody id="tick-rows"></tbody></table>
    </div>
    <p id="tick-empty" class="empty-state" hidden>沒有符合條件的資料集。</p>
    <button id="tick-more" class="load-more" type="button" hidden>載入更多資料集</button>
    <details class="tick-definitions"><summary>如何閱讀筆數、天數與資料日期</summary>
      <p>天數只計實際非空資料的相異日期，不以起訖日相減。FinMind 期權按事件日期，期交所與永豐期權按交易日，夜盤邊界不同。</p>
      <p>各來源、R1／R2 連續合約與實際交割合約可能重疊；期交所 TXO 是買賣側明細列，因此不加總成唯一成交數。每秒五檔快照是衍生資料，與逐筆成交分開。</p>
      <p>收據索引表示下載器登記的落盤數量，不代表重驗所有檔案、全部市場或歷史無缺口。沒有可驗證資料的產品保留登錄，未知值不當作零。</p>
    </details>`;
  section.append(body);
  const $ = id => section.querySelector(`#${id}`);
  for (const [key, label] of Object.entries(markets)) $("tick-market").append(new Option(label, key));
  if (fixedProvider !== "all") $("tick-provider-label").hidden = true;

  function filters() {
    return {provider: fixedProvider === "all" ? $("tick-provider").value : fixedProvider,
      market: $("tick-market").value, kind: $("tick-kind").value,
      detail: $("tick-detail").value, q: $("tick-search").value.trim()};
  }
  function rowNode(row) {
    const tr = make("tr");
    tr.dataset.tickId = row.id;
    const identity = make("td");
    identity.dataset.label = "市場／來源／資料集";
    identity.append(make("span", "cell-note", row.market_category_label), make("strong", "source-name", row.title),
      make("span", "cell-note", `${row.provider} · ${row.kind_label}`));
    const link = make("a", "detail-link", "來源頁面 →");
    link.href = `/data-monitor/providers/${encodeURIComponent(row.provider)}/#tick-inventory`;
    if (fixedProvider === "all") identity.append(link);
    const facts = make("td");
    facts.dataset.label = "筆數與涵蓋天數";
    const amount = known(row.count) ? `${count(row.count)} 筆`
      : row.statistics_state === "partial" && known(row.known_count) ? `已知至少 ${count(row.known_count)} 筆` : "筆數待核實";
    facts.append(make("strong", "source-name", amount),
      make("span", "cell-note", `有資料天數：${count(row.days)}${known(row.days) ? " 天" : ""}`));
    if (!known(row.days) && known(row.known_days)) facts.append(make("span", "cell-note", `已核實至少 ${count(row.known_days)} 天`));
    if (known(row.symbols)) facts.append(make("span", "cell-note", `有資料標的／視圖：${count(row.symbols)}`));
    const dates = make("td");
    dates.dataset.label = "最早／最新資料日";
    const prefix = row.statistics_state === "partial" ? "已知" : "";
    dates.append(make("span", "source-name", `${prefix}最早：${date(row.earliest_date ?? row.first)}`),
      make("span", "cell-note", `${prefix}最新：${date(row.latest_date ?? row.last)}`));
    const evidence = make("td");
    evidence.dataset.label = "統計證據";
    evidence.append(make("span", "tick-evidence-state", labels[row.statistics_state] || "待核實"));
    const detail = make("details", "tick-row-detail");
    detail.append(make("summary", "", "日期口徑與證據"), make("p", "", row.date_basis || "日期口徑待核實"),
      make("p", "", row.basis || "來源尚無可核對收據"));
    if (row.first_event && row.last_event) detail.append(make("p", "",
      `事件時間界限：${row.first_event} ～ ${row.last_event}`));
    if (known(row.dropped_events) && row.dropped_events > 0) detail.append(make("p", "", `接收端曾丟棄 ${count(row.dropped_events)} 個事件，收據筆數不代表完整行情。`));
    evidence.append(detail);
    tr.append(identity, facts, dates, evidence);
    return tr;
  }
  function validate(data, offset, limit, selected) {
    if (data.read_only !== true || data.production_control_possible !== false || !Array.isArray(data.rows)
        || data.offset !== offset || data.limit !== limit || !known(data.matching_total)
        || data.rows.length !== Math.min(limit, Math.max(0, data.matching_total - offset))
        || data.has_more !== (offset + data.rows.length < data.matching_total)
        || new Set(data.rows.map(row => row.id)).size !== data.rows.length
        || data.rows.some(row => !row || typeof row.id !== "string" || !markets[row.market_category]
          || (selected.provider !== "all" && row.provider !== selected.provider)
          || (selected.market !== "all" && row.market_category !== selected.market)
          || (selected.kind !== "all" && row.kind !== selected.kind))) throw new Error("Invalid Tick inventory page");
  }
  async function refresh({reset = false, append = false} = {}) {
    if (document.hidden || (!state.inView && !reset && !append) || (state.inFlight && !reset)) return;
    state.inFlight = true;
    const request = requests.begin();
    const selected = filters();
    const offset = append ? state.rows.length : 0;
    const limit = append || reset ? 30 : Math.min(100, Math.max(30, state.rows.length));
    $("tick-more").disabled = true;
    let reload = false;
    try {
      const data = await fetchJson(`/data-monitor/api/ticks?${new URLSearchParams({...selected,
        offset: String(offset), limit: String(limit)})}`, {signal: request.signal});
      if (!request.isCurrent() || JSON.stringify(filters()) !== JSON.stringify(selected)) return;
      validate(data, offset, limit, selected);
      if (append && state.revision !== data.revision) { reload = true; return; }
      const unchanged = state.revision && data.revision === state.revision;
      const previousIds = state.rows.map(row => row.id).join("|");
      if (append) state.rows = [...state.rows, ...data.rows];
      else state.rows = unchanged && !reset ? [...data.rows, ...state.rows.slice(limit)] : data.rows;
      if (new Set(state.rows.map(row => row.id)).size !== state.rows.length) throw new Error("Duplicate Tick inventory rows");
      if (!unchanged || reset || previousIds !== state.rows.map(row => row.id).join("|")) {
        $("tick-rows").replaceChildren(...state.rows.map(rowNode));
      }
      state.revision = data.revision;
      state.loaded = true;
      const providers = [...new Set(data.providers || [])];
      if (selected.provider !== "all" && !providers.includes(selected.provider)) providers.push(selected.provider);
      if (fixedProvider === "all" && JSON.stringify(providers) !== $("tick-provider").dataset.options) {
        $("tick-provider").replaceChildren(new Option("全部來源", "all"), ...providers.map(value => new Option(value, value)));
        $("tick-provider").value = selected.provider;
        $("tick-provider").dataset.options = JSON.stringify(providers);
      }
      const observed = new Date(data.observed_at_utc || "");
      $("tick-observed").textContent = `統計更新：${Number.isNaN(observed.getTime()) ? "待核實" : clock.format(observed)}（台北時間）；資料最新日請看各列。`;
      $("tick-result-count").textContent = data.state === "waiting_snapshot" ? "等待逐筆清冊快照；目前不代表零資料。"
        : `符合 ${count(data.matching_total)} 個資料集，已顯示 ${count(state.rows.length)} 個；各來源筆數分列。`;
      $("tick-empty").hidden = data.matching_total > 0 || data.state === "waiting_snapshot";
      $("tick-more").hidden = state.rows.length >= data.matching_total;
      $("tick-error").hidden = true;
    } catch (error) {
      if (!request.isCurrent() || error.name === "AbortError") return;
      $("tick-error").textContent = state.loaded ? "逐筆清冊更新失敗；保留上次觀測，請留意統計時間。" : "逐筆清冊暫時無法讀取，稍後自動重試。";
      $("tick-error").hidden = false;
    } finally {
      if (request.isCurrent()) {
        state.inFlight = false;
        $("tick-more").disabled = false;
        request.finish();
        if (reload) void refresh({reset: true});
      }
    }
  }
  function reset() {
    clearTimeout(searchTimer);
    state.rows = [];
    state.revision = null;
    state.loaded = false;
    $("tick-rows").replaceChildren();
    $("tick-more").hidden = true;
    $("tick-empty").hidden = true;
    $("tick-result-count").textContent = "篩選已變更；讀取相符資料集。";
    void refresh({reset: true});
  }
  let searchTimer;
  $("tick-search").addEventListener("input", () => {
    requests.abort();
    state.inFlight = false;
    clearTimeout(searchTimer);
    searchTimer = setTimeout(reset, 200);
  });
  $("tick-filters").addEventListener("submit", event => { event.preventDefault(); clearTimeout(searchTimer); reset(); });
  for (const id of ["tick-provider", "tick-market", "tick-kind", "tick-detail"]) $(id).addEventListener("change", reset);
  $("tick-more").addEventListener("click", () => void refresh({append: true}));
  core.observeVisibility(section, visible => {
    state.inView = visible;
    if (visible) void refresh();
  }, {marginPx: 100});
  setInterval(() => { if (state.inView) void refresh(); }, 15000);
})();

"use strict";

/* A completed model backtest is not a live signal or a paper account. */
(function (global) {
  function validate(data) {
    if (!data || data.schema_version !== 1 || !Array.isArray(data.candidates)) throw new Error("研究資料格式錯誤");
    for (const item of data.candidates) {
      if (item.status === "research_source_unavailable") continue;
      if (!(["research_only_not_enabled", "research_only_pending_paper"].includes(item.status))
          || item.paper_execution_enabled !== (item.status === "research_only_not_enabled" ? false : null)
          || item.live_signal_available !== false || item.simulation_only !== true || item.broker_fill !== false
          || typeof item.market !== "string" || !item.market || typeof item.label !== "string"
          || !Number.isFinite(item.initial_capital_twd) || item.initial_capital_twd <= 0
          || !Number.isInteger(item.year) || !Array.isArray(item.daily) || item.daily.length > 366) {
        throw new Error("研究候選契約錯誤");
      }
      if (item.research_status === "available") {
        if (!item.daily.length || item.session_count !== item.daily.length
            || item.start_date !== item.daily[0].date || item.end_date !== item.daily[item.daily.length - 1].date
            || !Number.isFinite(item.cumulative_return_pct)) throw new Error("研究績效不完整");
        let previous = "";
        for (const row of item.daily) {
          if (!row.date?.startsWith(`${item.year}-`) || row.date <= previous
              || !Number.isFinite(row.daily_return_pct) || !Number.isFinite(row.cumulative_return_pct)) {
            throw new Error("研究績效日期或數值錯誤");
          }
          previous = row.date;
        }
      } else if (item.research_status !== "no_current_year_data" || item.daily.length) {
        throw new Error("研究候選狀態錯誤");
      }
      const replay = item.provisional_replay;
      if (replay && replay.status !== "unavailable") {
        if (replay.status !== "available" || !Array.isArray(replay.daily) || !replay.daily.length
            || replay.session_count !== replay.daily.length || replay.minute_count !== replay.session_count * 270
            || replay.start_date !== replay.daily[0].date || replay.end_date !== replay.daily[replay.daily.length - 1].date
            || !Number.isFinite(replay.end_equity_twd) || !Number.isFinite(replay.cumulative_return_pct)
            || ![replay.carried_mark_minutes, replay.full_cross_assumptions, replay.nontrade_assumptions]
              .every(value => Number.isInteger(value) && value >= 0)
            || (replay.minute_series_available !== undefined && typeof replay.minute_series_available !== "boolean")) throw new Error("暫定回放契約錯誤");
        let previous = "";
        for (const row of replay.daily) {
          if (!row.date?.startsWith(`${item.year}-`) || row.date <= previous
              || ![row.end_equity_twd, row.daily_return_pct, row.cumulative_return_pct].every(Number.isFinite)
              || !Number.isInteger(row.carried_mark_minutes) || row.carried_mark_minutes < 0 || row.carried_mark_minutes > 270) {
            throw new Error("暫定回放數值錯誤");
          }
          previous = row.date;
        }
      }
      const signal = item.counterfactual_signal;
      if (signal && signal.status !== "unavailable") {
        if (signal.status !== "available" || signal.counterfactual !== true || signal.live_signal !== false
            || signal.broker_fill !== false || signal.execution_and_minute_coverage_verified !== false
            || typeof signal.session_date !== "string" || !signal.session_date.startsWith(`${item.year}-`)
            || typeof signal.generated_at !== "string" || !Array.isArray(signal.signals)
            || !Number.isInteger(signal.signal_count) || signal.signal_count !== signal.signals.length
            || signal.signals.length > 10000) throw new Error("反事實訊號契約錯誤");
        const seen = new Set();
        let previous = Infinity;
        for (const row of signal.signals) {
          if (!row || typeof row.symbol !== "string" || !/^[A-Z0-9]{1,12}$/.test(row.symbol)
              || seen.has(row.symbol) || typeof row.name !== "string" || row.name.length > 80
              || !Number.isFinite(row.target_weight_pct) || Math.abs(row.target_weight_pct) <= 0
              || Math.abs(row.target_weight_pct) > 100 || Math.abs(row.target_weight_pct) > previous
              || ![row.tradable, row.can_buy, row.can_sell].every(value => typeof value === "boolean")) {
            throw new Error("反事實訊號數值錯誤");
          }
          seen.add(row.symbol);
          previous = Math.abs(row.target_weight_pct);
        }
      }
    }
    return data;
  }

  function validateMinute(data, market, day) {
    if (!data || data.schema_version !== 1 || data.status !== "available" || data.market !== market
        || data.session_date !== day || data.first_minute !== `${day}T09:01:00+08:00`
        || data.interval_seconds !== 60 || data.simulation_only !== true || data.provisional !== true
        || data.broker_fill !== false || data.exchange_fill !== false || data.source_completeness_verified !== false
        || !Array.isArray(data.equity_twd) || data.equity_twd.length !== 270
        || !data.equity_twd.every(value => Number.isFinite(value) && value > 0)
        || !Array.isArray(data.valuation_stale) || data.valuation_stale.length !== 270
        || !data.valuation_stale.every(value => typeof value === "boolean")) {
      throw new Error("每分鐘回放尚未通過核對");
    }
    return data;
  }

  function render(data, escape) {
    return data.candidates.map((item) => {
      if (item.status === "research_source_unavailable") return "<p>研究來源暫時無法核對，沒有顯示績效，也沒有啟用模擬。</p>";
      const pct = (value) => `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
      const money = new Intl.NumberFormat("zh-TW", {maximumFractionDigits: 2}).format(item.initial_capital_twd);
      const paperStatus = item.status === "research_only_not_enabled" ? "尚未啟用紙上執行" : "已設定，紙上帳戶狀態尚待核對";
      const heading = `<h4>${escape(item.label)}</h4><p>初始資金 NT$ ${money} · ${paperStatus} · 此處未提供即時訊號或成交</p>`;
      const signal = item.counterfactual_signal;
      const counterfactualSignal = !signal ? "" : signal.status !== "available"
        ? "<p>盤後重建訊號暫時無法核對，不顯示訊號。</p>"
        : `<h5>盤後重建訊號（非即時訊號）</h5>
          <p>交易日 ${escape(signal.session_date)} · 實際產生 ${escape(signal.generated_at.replace("T", " ").replace(/\+08:00$/, " 台北時間"))} · ${signal.signal_count} 筆非零目標。這是收盤後依官方開盤價重算，不是當時已發布的訊號；尚未確認該日分鐘回放與模擬成交，亦非券商成交。</p>
          <details><summary>查看全部重建訊號</summary><div class="table-scroll short" tabindex="0"><table class="compact-table research-return-table"><thead><tr><th>標的</th><th>方向</th><th>目標權重</th><th>模型資格</th></tr></thead><tbody>${signal.signals.map(row => `<tr><td>${escape(row.symbol)} ${escape(row.name)}</td><td>${row.target_weight_pct > 0 ? "多" : "空"}</td><td>${pct(row.target_weight_pct)}</td><td>${row.tradable && (row.target_weight_pct > 0 ? row.can_buy : row.can_sell) ? "符合" : "不符合"}</td></tr>`).join("")}</tbody></table></div></details>`;
      const replay = item.provisional_replay;
      const provisional = !replay ? "" : replay.status !== "available"
        ? "<p>暫定回放摘要暫時無法核對，不顯示其績效。</p>"
        : `<h5>今年暫定反事實回放（非即時帳本）</h5>
          <p>${escape(replay.start_date)}～${escape(replay.end_date)} · ${replay.session_count} 個交易日／${replay.minute_count} 個分鐘點 · 帳面資產 NT$ ${new Intl.NumberFormat("zh-TW", {maximumFractionDigits: 2}).format(replay.end_equity_twd)} · 累積 ${pct(replay.cumulative_return_pct)}</p>
          <p>資料可能不完整、結果可能改變，未成交目標的逐筆原因尚未全面獨立核對；${replay.carried_mark_minutes} 個分鐘點含延用估值，${replay.full_cross_assumptions} 筆穿價全數出場與 ${replay.nontrade_assumptions} 筆非成交帳面結算是模擬假設。這是後來重建的每日一次訊號，不是當時已發布、盤中逐版再平衡、交易所或券商成交。</p>
          <details><summary>查看暫定回放每日報酬</summary><div class="table-scroll short" tabindex="0"><table class="compact-table research-return-table"><thead><tr><th>交易日</th><th>當日報酬</th><th>累積報酬</th></tr></thead><tbody>${replay.daily.map(row => `<tr><td>${escape(row.date)}</td><td>${pct(row.daily_return_pct)}</td><td>${pct(row.cumulative_return_pct)}</td></tr>`).join("")}</tbody></table></div></details>
          ${replay.minute_series_available !== true ? "" : `<details data-research-minutes data-market="${escape(item.market)}"><summary>查看暫定每分鐘資產曲線</summary><label>交易日 <select data-research-day>${replay.daily.map((row, index) => `<option value="${escape(row.date)}"${index === replay.daily.length - 1 ? " selected" : ""}>${escape(row.date)}</option>`).join("")}</select></label><p data-research-minute-status role="status">展開後讀取資料。</p><div class="chart-host" data-research-minute-chart role="img" aria-label="暫定回放每分鐘報酬率曲線"></div><p>所選交易日第一分鐘的報酬率設為 0%，資產金額仍為累積帳面資產；延用估值不代表當分鐘有成交。</p></details>`}`;
      if (item.research_status !== "available") return `${heading}${counterfactualSignal}<p>今年尚無可核對的原模型研究回測。</p>${provisional}`;
      const rows = item.daily.map((row) => `<tr><td>${escape(row.date)}</td><td>${pct(row.daily_return_pct)}</td><td>${pct(row.cumulative_return_pct)}</td></tr>`).join("");
      return `${heading}${counterfactualSignal}<p>原模型研究回測 ${escape(item.start_date)}～${escape(item.end_date)} · ${item.session_count} 個交易日 · 累積報酬 <strong>${pct(item.cumulative_return_pct)}</strong></p>
        <p>每天一次決策的歷史研究結果，含研究結算假設；歷史資料的當時可取得性尚未驗證，不是盤中逐版再平衡績效。最新穿價全數出場規則未反映在此原回測；${escape(item.end_date)} 之後尚未呈現，回測不能保證未來表現。</p>
        <details><summary>查看研究回測每日報酬</summary><div class="table-scroll short" tabindex="0"><table class="compact-table research-return-table"><thead><tr><th>交易日</th><th>當日報酬</th><th>累積報酬</th></tr></thead><tbody>${rows}</tbody></table></div></details>${provisional}`;
    }).join("");
  }

  function init() {
    const panel = global.document?.getElementById("research-candidates");
    const Dashboard = global.StockAgentDashboard;
    if (!panel || !Dashboard || global.location?.pathname.startsWith("/tw-overnight/")) return;
    const get = Dashboard.createFetch({timeoutMs: 6000});
    let previous = null;
    let renderKey = null, plot = null, resize = null, minuteRequest = 0;
    function destroyPlot() {
      if (plot) plot.destroy();
      if (resize) resize.disconnect();
      plot = null; resize = null;
    }
    async function showMinutes(section) {
      const market = section.dataset.market, day = section.querySelector("[data-research-day]")?.value;
      const status = section.querySelector("[data-research-minute-status]");
      const host = section.querySelector("[data-research-minute-chart]");
      if (!market || !day || !status || !host || !section.open) return;
      const sequence = ++minuteRequest;
      destroyPlot();
      status.textContent = "讀取已核對的暫定分鐘回放…";
      try {
        const response = await get(`api/research-minute?${new URLSearchParams({market, date: day})}`, {cache: "no-store"});
        if (!response.ok) throw new Error("分鐘資料讀取失敗");
        const data = validateMinute(await Dashboard.readJsonResponse(response, {expectedRoot: "object"}), market, day);
        if (sequence !== minuteRequest || !section.isConnected || !section.open) return;
        if (typeof global.uPlot !== "function") throw new Error("曲線繪圖元件不可用");
        const first = data.equity_twd[0], x = data.equity_twd.map((_v, index) => index);
        const y = data.equity_twd.map(value => (value / first - 1) * 100);
        const time = value => { const minutes = 9 * 60 + 1 + Math.round(value); return `${String(Math.floor(minutes / 60)).padStart(2, "0")}:${String(minutes % 60).padStart(2, "0")}`; };
        const options = {width: Math.max(200, host.clientWidth), height: Math.max(200, host.clientHeight),
          legend: {show: false}, scales: {x: {time: false}},
          axes: [{stroke: "#72899e", splits: chart => chart.width < 500 ? [0, 134, 269] : [0, 59, 119, 179, 239, 269],
                  values: (_chart, splits) => splits.map(time)},
                 {stroke: "#72899e", size: 65, values: (_chart, splits) => splits.map(value => `${value.toFixed(2)}%`)}],
          series: [{}, {stroke: "#38bdf8", width: 2, points: {show: false}}]};
        plot = new global.uPlot(options, [x, y], host);
        if (typeof global.ResizeObserver === "function") {
          resize = new global.ResizeObserver(() => { if (plot && host.clientWidth) plot.setSize({width: host.clientWidth, height: Math.max(200, host.clientHeight)}); });
          resize.observe(host);
        }
        const amount = new Intl.NumberFormat("zh-TW", {maximumFractionDigits: 2}).format(data.equity_twd.at(-1));
        status.textContent = `09:01～13:30，共 270 點 · 收盤帳面資產 NT$ ${amount} · ${data.valuation_stale.filter(Boolean).length} 點含延用估值 · 暫定回放，非實際成交`;
      } catch (_error) {
        if (sequence === minuteRequest && section.isConnected) status.textContent = "每分鐘回放暫時無法核對或繪製；不顯示未驗證資料。";
      }
    }
    panel.addEventListener("toggle", event => {
      const section = event.target;
      if (!section?.matches?.("[data-research-minutes]")) return;
      if (section.open) showMinutes(section); else { minuteRequest++; destroyPlot(); }
    }, true);
    panel.addEventListener("change", event => {
      if (event.target?.matches?.("[data-research-day]")) showMinutes(event.target.closest("[data-research-minutes]"));
    });
    async function refresh() {
      try {
        const response = await get("api/research-candidates", {cache: "no-store"});
        if (!response.ok) throw new Error("無法取得研究來源");
        const data = validate(await Dashboard.readJsonResponse(response, {expectedRoot: "object"}));
        previous = data;
        const refreshStatus = Dashboard.byId("research-candidates-refresh-status");
        if (refreshStatus) refreshStatus.textContent = "";
        panel.classList.toggle("hidden", data.candidates.length === 0);
        const key = JSON.stringify(data.candidates);
        if (renderKey !== key) {
          minuteRequest++; destroyPlot();
          Dashboard.setTrustedHtml("research-candidates-content", render(data, Dashboard.escapeHtml));
          renderKey = key;
        }
      } catch (_error) {
        panel.classList.remove("hidden");
        const refreshStatus = Dashboard.byId("research-candidates-refresh-status");
        if (refreshStatus && previous) refreshStatus.textContent = "研究資料更新失敗，保留上次成功讀取的結果。";
        if (!previous) Dashboard.setTrustedHtml("research-candidates-content", "研究來源暫時無法取得，沒有啟用模擬。");
      }
    }
    const scheduler = Dashboard.scheduleRefresh(refresh, {intervalMs: 60000, cadenceFromLastStart: true, refreshOnVisible: "overdue"});
    Dashboard.byId("force-refresh")?.addEventListener("click", () => scheduler.run());
  }
  global.StockAgentResearchCandidates = Object.freeze({validate, validateMinute, render, init});
  init();
})(globalThis);

"use strict";

/* A single acknowledged engine projection, not a historical ledger page. */
(function installLivePaper(global) {
  // This projection is restricted to committed open positions, not history.
  let sharedSnapshot = null, sharedError = "";
  function getSnapshot() { return {data: sharedSnapshot, error: sharedError}; }
  function notify() {
    if (typeof global.dispatchEvent === "function" && typeof global.CustomEvent === "function") {
      global.dispatchEvent(new global.CustomEvent("stockagent:live-paper"));
    }
  }
  async function retryTransientRead(fetcher, url, options, pause = ms => new Promise(resolve => global.setTimeout(resolve, ms))) {
    const delays = [250, 750, 1500];
    for (let attempt = 0; ; attempt++) {
      const response = await fetcher(url, options);
      if ((response.status !== 503 && response.status !== 504) || attempt === delays.length) return response;
      // A live writer can cross several commit boundaries in succession. Only
      // retry transient server failures, close each failed body, and bound the
      // recovery burst; normal successful refreshes remain one per minute.
      try { await response.body?.cancel?.(); } catch (_error) { /* response already closed */ }
      await pause(delays[attempt]);
    }
  }

  function validate(data) {
    if (!data || data.schema_version !== 1 || data.simulation_only !== true
        || data.production_order_possible !== false || data.scope !== "committed_open_positions_only"
        || !Number.isSafeInteger(data.state_revision) || data.state_revision <= 0
        || typeof data.as_of !== "string" || !/(Z|[+-]\d\d:\d\d)$/.test(data.as_of)
        || !Number.isFinite(Date.parse(data.as_of)) || !Array.isArray(data.modes) || !Array.isArray(data.rows)) {
      throw new Error("模擬快照格式錯誤");
    }
    const modes = new Map(), counts = new Map();
    for (const mode of data.modes) {
      if (!mode || typeof mode.market !== "string" || !mode.market || modes.has(mode.market)
          || typeof mode.label !== "string" || !Number.isSafeInteger(mode.open_position_count)
          || mode.open_position_count < 0) throw new Error("模擬模式格式錯誤");
      modes.set(mode.market, mode);
    }
    for (const row of data.rows) {
      if (!row || !modes.has(row.market) || typeof row.symbol !== "string" || !row.symbol
          || typeof row.signed_shares !== "number" || !Number.isFinite(row.signed_shares)
          || row.signed_shares === 0) throw new Error("未平倉資料不一致");
      counts.set(row.market, (counts.get(row.market) || 0) + 1);
    }
    for (const mode of data.modes) {
      if ((counts.get(mode.market) || 0) !== mode.open_position_count) throw new Error("未平倉筆數不一致");
    }
    return data;
  }

  function init() {
    const panel = global.document?.getElementById("live-paper"), Dashboard = global.StockAgentDashboard;
    if (global.location?.pathname.startsWith("/tw-overnight/")) {
      global.document?.querySelector('a[href="#live-paper"]')?.classList.add("hidden");
      return;
    }
    if (!panel || !Dashboard || typeof detailComponents === "undefined") return;
    panel.classList.remove("hidden");
    const $ = Dashboard.byId, esc = Dashboard.escapeHtml, setHtml = Dashboard.setTrustedHtml;
    // A cold gateway may still be verifying the prior official daily-price
    // source. Do not abort a valid committed snapshot before that bounded
    // read can finish; this does not change the one-minute refresh cadence.
    const fetchSnapshot = Dashboard.createFetch({timeoutMs: 10000});
    const clock = new Intl.DateTimeFormat("zh-TW", {timeZone: "Asia/Taipei", hour12: false,
      month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit"});
    const number = value => typeof value === "number" && Number.isFinite(value)
      ? value.toLocaleString("zh-TW", {maximumFractionDigits: 2}) : "—";
    let current = null, etag = "", error = "", visible = 50;
    function render() {
      const modes = current?.modes || [], rows = current?.rows || [];
      const modeMap = new Map(modes.map(mode => [mode.market, mode]));
      const age = current ? Math.max(0, (Date.now() - Date.parse(current.as_of)) / 1000) : null;
      const freshness = current ? `${clock.format(new Date(current.as_of))} · 版本 ${current.state_revision}` : "尚未取得";
      const warning = error || (age != null && age > 120 ? `帳本快照已 ${Math.floor(age)} 秒未更新` : "");
      const summary = modes.map(mode => {
        const nontrade = Number(mode.nontrade_settlement_count || 0) > 0;
        const booked = mode.cumulative_nontrade_settlement_net_pnl_twd != null || nontrade;
        const flags = [nontrade ? "含非成交假設結算" : mode.engine_status || "狀態未知", `${nontrade ? "帳面未平" : "未平"} ${number(mode.open_position_count)} 筆`,
          `估值延用 ${number(mode.stale_position_count)} 筆`, `權益 NT$ ${number(mode.total_equity_twd)}`,
          `${booked ? "累積帳面已結算（含假設）" : "累積已實現"} NT$ ${number(mode.cumulative_realized_net_pnl_twd)}`,
          `未平倉估值 NT$ ${number(mode.open_net_liquidation_pnl_twd)}`];
        if (nontrade) flags.push(`${number(mode.nontrade_settlement_count)} 筆／${number(mode.nontrade_settlement_shares)} 股為非成交帳面結算`);
        if (Number(mode.entry_unfilled_shares) > 0) flags.push(`本版未成交 ${number(mode.entry_unfilled_shares)} 股`);
        if (Number(mode.unresolved_exit_count) > 0) flags.push(`待處理出場 ${number(mode.unresolved_exit_count)} 筆`);
        if (mode.readiness_error) flags.push("模式準備有誤");
        if (mode.execution_evidence_complete === false) flags.push("成交證據未完整");
        const counts = mode.signal_counts || {};
        for (const [key, title] of [["blocked", "阻擋"], ["waiting_source", "等待來源"], ["partial_depth", "部分成交"]]) {
          if (Number(counts[key]) > 0) flags.push(`${title} ${number(counts[key])} 筆`);
        }
        return `<div><span>${esc(mode.label)}</span><strong>${esc(flags.join(" · "))}</strong></div>`;
      }).join("");
      setHtml("live-paper-status", `<div><span>已確認快照</span><strong>${esc(freshness)}${warning ? ` · ${esc(warning)}` : ""}</strong></div>${summary}`);
      $("live-paper-count").textContent = current ? `${Math.min(visible, rows.length)} / ${rows.length} 筆未平倉` : "尚未取得持倉";
      $("live-paper-more").classList.toggle("hidden", rows.length <= visible);
      const selected = rows.slice().sort((a, b) => Math.abs(Number(b.target_weight) || 0) - Math.abs(Number(a.target_weight) || 0));
      const html = selected.slice(0, visible).map(row => detailComponents.positionRow({...row, label: modeMap.get(row.market)?.label})).join("");
      setHtml("live-paper-body", html || `<tr><td colspan="6">${esc(error || (current ? "此快照沒有未平倉" : "正在讀取模擬帳本快照…"))}</td></tr>`);
    }
    async function refresh() {
      try {
        const response = await retryTransientRead(fetchSnapshot, "api/live-snapshot", {cache: "no-store", headers: etag ? {"If-None-Match": etag} : {}});
        if (response.status === 304 && current) {
          error = ""; sharedSnapshot = current; sharedError = error; render(); notify(); return;
        }
        current = validate(await Dashboard.readJsonResponse(response, {expectedRoot: "object"}));
        etag = response.headers.get("etag") || "";
        error = "";
      } catch (_error) {
        error = current ? "更新失敗，保留上次已確認快照" : "模擬快照暫時無法核對，將自動重試";
      }
      sharedSnapshot = current; sharedError = error;
      render();
      notify();
    }
    $("live-paper-more").addEventListener("click", () => { visible += 50; render(); });
    const scheduler = Dashboard.scheduleRefresh(refresh, {
      intervalMs: 60000, cadenceFromLastStart: true, refreshOnVisible: "overdue",
    });
    $("force-refresh")?.addEventListener("click", () => scheduler.run());
  }
  function validateEvents(data) {
    if (!data || data.schema_version !== 1 || data.simulation_only !== true
        || data.production_order_possible !== false || data.scope !== "recent_committed_events_only"
        || !Number.isSafeInteger(data.state_revision) || data.state_revision <= 0
        || typeof data.as_of !== "string" || !/(Z|[+-]\d\d:\d\d)$/.test(data.as_of)
        || !Number.isFinite(Date.parse(data.as_of)) || !Array.isArray(data.rows)
        || data.rows.some(row => !row || !["order", "fill", "notice"].includes(row.event_kind)
          || typeof row.replay !== "boolean")) throw new Error("模擬紀錄格式錯誤");
    return data;
  }

  function initEvents() {
    const panel = global.document?.getElementById("live-events"), Dashboard = global.StockAgentDashboard;
    if (global.location?.pathname.startsWith("/tw-overnight/")) {
      global.document?.querySelector('a[href="#live-events"]')?.classList.add("hidden");
      return;
    }
    if (!panel || !Dashboard || typeof detailComponents === "undefined") return;
    panel.classList.remove("hidden");
    const $ = Dashboard.byId, esc = Dashboard.escapeHtml, setHtml = Dashboard.setTrustedHtml;
    const fetchEvents = Dashboard.createFetch({timeoutMs: 6000});
    const clock = new Intl.DateTimeFormat("zh-TW", {timeZone: "Asia/Taipei", hour12: false,
      month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit"});
    let current = null, etag = "", error = "";
    function render() {
      const rows = current?.rows || [];
      const age = current ? Math.max(0, (Date.now() - Date.parse(current.as_of)) / 1000) : null;
      const warning = error || (age != null && age > 120 ? `帳本已 ${Math.floor(age)} 秒未更新` : "");
      const stamp = current ? `${clock.format(new Date(current.as_of))} · 版本 ${current.state_revision}` : "尚未取得";
      setHtml("live-events-status", `<div><span>已確認紀錄</span><strong>${esc(stamp)}${warning ? ` · ${esc(warning)}` : ""}</strong></div>`);
      $("live-events-count").textContent = current ? `最近 ${rows.length} 筆 · 非完整歷史` : "尚未取得紀錄";
      const html = rows.map(row => detailComponents.eventRow({...row,
        order_type: row.event_kind === "fill" ? (row.replay ? "回放紙上成交" : "紙上成交")
          : row.event_kind === "notice" ? "流程" : row.order_type || "紙上委託"})).join("");
      setHtml("live-events-body", html || `<tr><td colspan="6">${esc(error || (current ? "目前沒有最近模擬紀錄" : "正在讀取模擬紀錄…"))}</td></tr>`);
    }
    async function refresh() {
      try {
        const response = await retryTransientRead(fetchEvents, "api/live-events", {cache: "no-store", headers: etag ? {"If-None-Match": etag} : {}});
        if (response.status === 304 && current) { error = ""; render(); return; }
        current = validateEvents(await Dashboard.readJsonResponse(response, {expectedRoot: "object"}));
        etag = response.headers.get("etag") || "";
        error = "";
      } catch (_error) {
        error = current ? "更新失敗，保留上次已確認紀錄" : "模擬紀錄暫時無法核對，將自動重試";
      }
      render();
    }
    const scheduler = Dashboard.scheduleRefresh(refresh, {
      intervalMs: 60000, cadenceFromLastStart: true, refreshOnVisible: "overdue",
    });
    $("force-refresh")?.addEventListener("click", () => scheduler.run());
  }
  global.StockAgentTwLivePaper = Object.freeze({validate, init, validateEvents, initEvents, retryTransientRead, getSnapshot});
  init();
  initEvents();
})(globalThis);

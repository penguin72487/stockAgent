"use strict";

/* The producer's model target is a different fact from a paper fill.  This
 * view never depends on the status/position APIs and never renders fills. */
(function installPublishedSignals(global) {
  function validate(data) {
    if (!data || data.schema_version !== 1 || data.simulation_only !== true
        || data.production_order_possible !== false
        || !/^\d{4}-\d{2}-\d{2}$/.test(data.session_date || "")
        || !Array.isArray(data.modes) || !Array.isArray(data.rows)) throw new Error("訊號格式不完整");
    const modes = new Map();
    for (const mode of data.modes) {
      if (!mode || typeof mode.market !== "string" || !mode.market || modes.has(mode.market)
          || typeof mode.label !== "string" || !["published", "waiting", "unavailable"].includes(mode.status)) {
        throw new Error("訊號模式格式錯誤");
      }
      if (mode.status === "published" && (!Number.isSafeInteger(mode.target_count) || mode.target_count < 0
          || !/^[a-f0-9]{32}$/.test(mode.public_signal_key || "") || !mode.signal_ready_at)) {
        throw new Error("訊號版本不完整");
      }
      modes.set(mode.market, mode);
    }
    const counts = new Map(), seen = new Set();
    for (const row of data.rows) {
      const mode = modes.get(row?.market);
      const key = `${row?.market}\u0000${row?.symbol}`;
      if (!mode || mode.status !== "published" || row.public_signal_key !== mode.public_signal_key
          || typeof row.symbol !== "string" || !row.symbol || seen.has(key)
          || typeof row.target_weight !== "number" || !Number.isFinite(row.target_weight)
          || row.target_weight === 0 || row.side !== (row.target_weight > 0 ? "long" : "short")) {
        throw new Error("訊號資料不一致");
      }
      seen.add(key);
      counts.set(row.market, (counts.get(row.market) || 0) + 1);
    }
    for (const mode of data.modes) {
      if (mode.status === "published" && (counts.get(mode.market) || 0) !== mode.target_count) {
        throw new Error("訊號筆數不完整");
      }
    }
    return data;
  }

  function merge(previous, incoming) {
    validate(incoming);
    const sameSession = previous?.session_date === incoming.session_date;
    const oldModes = new Map((sameSession ? previous.modes : []).map((mode) => [mode.market, mode]));
    const newRows = new Map(), oldRows = new Map();
    for (const row of incoming.rows) {
      if (!newRows.has(row.market)) newRows.set(row.market, []);
      newRows.get(row.market).push(row);
    }
    for (const row of sameSession ? previous.rows : []) {
      if (!oldRows.has(row.market)) oldRows.set(row.market, []);
      oldRows.get(row.market).push(row);
    }
    const modes = [], rows = [];
    for (const mode of incoming.modes) {
      if (mode.status === "published") {
        modes.push({...mode, stale: false});
        rows.push(...(newRows.get(mode.market) || []));
      } else {
        const old = oldModes.get(mode.market);
        const canRetain = old && (old.status === "published" || old.stale === true);
        modes.push(canRetain ? {...old, label: mode.label, status: mode.status, stale: true} : {...mode, stale: false});
        if (canRetain) rows.push(...(oldRows.get(mode.market) || []));
      }
    }
    rows.sort((a, b) => Math.abs(b.target_weight) - Math.abs(a.target_weight)
      || a.market.localeCompare(b.market) || a.symbol.localeCompare(b.symbol));
    return {session_date: incoming.session_date, modes, rows};
  }

  function init() {
    const panel = global.document?.getElementById("published-signals");
    const Dashboard = global.StockAgentDashboard;
    if (global.location?.pathname.startsWith("/tw-overnight/")) {
      global.document?.querySelector('a[href="#published-signals"]')?.classList.add("hidden");
      return;
    }
    if (!panel || !Dashboard) return;
    panel.classList.remove("hidden");
    const $ = Dashboard.byId, esc = Dashboard.escapeHtml, setHtml = Dashboard.setTrustedHtml;
    const fetchSnapshot = Dashboard.createFetch({timeoutMs: 4000});
    let current = null, etag = "", error = "", visible = 100;
    const displayTime = (value) => {
      const parsed = new Date(value);
      return Number.isFinite(parsed.getTime())
        ? new Intl.DateTimeFormat("zh-TW", {timeZone: "Asia/Taipei", hour12: false,
          hour: "2-digit", minute: "2-digit", second: "2-digit"}).format(parsed) : "時間未確認";
    };
    const percent = (value) => {
      const magnitude = Math.abs(value) * 100;
      if (magnitude > 0 && magnitude < 0.01) return value < 0 ? "−<0.01%" : "+<0.01%";
      return `${value < 0 ? "−" : "+"}${magnitude.toFixed(2)}%`;
    };
    function render() {
      const modes = current?.modes || [], rows = current?.rows || [];
      const byMarket = new Map(modes.map((mode) => [mode.market, mode]));
      const summary = modes.map((mode) => {
        const version = mode.revision_index == null ? "" : `版本 ${mode.revision_index} · `;
        const text = mode.status === "published" ? `已發布 · ${version}${displayTime(mode.signal_ready_at)} · ${mode.target_count} 筆`
          : mode.stale ? `更新失敗，延用上一版 · ${version}${displayTime(mode.signal_ready_at)}`
          : mode.status === "waiting" ? "等待今日訊號" : "訊號暫時無法核對";
        return `<div><span>${esc(mode.label)}</span><strong>${esc(text)}</strong></div>`;
      }).join("");
      setHtml("published-signal-status", (error ? `<div><span>訊號更新</span><strong>${esc(error)}</strong></div>` : "")
        + summary || "<div>等待訊號資料</div>");
      $("published-signal-count").textContent = current
        ? `${current.session_date} · ${Math.min(visible, rows.length)} / ${rows.length} 筆`
        : "尚未取得訊號";
      const button = $("published-signal-more");
      button.classList.toggle("hidden", rows.length <= visible);
      const html = rows.slice(0, visible).map((row) => {
        const mode = byMarket.get(row.market);
        const revision = mode.revision_index == null ? "" : `版本 ${mode.revision_index} · `;
        return `<tr data-public-signal-key="${esc(row.public_signal_key)}"><td><strong>${esc(mode.label)}</strong><small>${mode.stale ? "上一版 · " : ""}${esc(revision)}${esc(displayTime(mode.signal_ready_at))}</small></td><td><strong>${esc(row.symbol)}</strong><small>${esc(row.name || "")} · ${row.side === "long" ? "多" : "空"}</small></td><td><strong>${esc(percent(row.target_weight))}</strong></td></tr>`;
      }).join("");
      setHtml("published-signal-body", html || `<tr><td colspan="3">${esc(error || (current ? "目前沒有可顯示的當日非零訊號" : "正在讀取已發布訊號…"))}</td></tr>`);
    }
    async function refresh() {
      try {
        const headers = etag ? {"If-None-Match": etag} : {};
        const response = await fetchSnapshot("api/published-signals", {cache: "no-store", headers});
        if (response.status === 304 && current) {
          error = "";
          render();
          return;
        }
        const data = await Dashboard.readJsonResponse(response, {expectedRoot: "object"});
        const next = merge(current, data);
        if (current && current.session_date !== next.session_date) visible = 100;
        current = next;
        etag = response.headers.get("etag") || "";
        error = "";
      } catch (_error) {
        error = current ? "更新失敗，保留上次成功讀取的訊號（日期與版本如下）" : "訊號暫時無法取得，將自動重試";
      }
      render();
    }
    $("published-signal-more").addEventListener("click", () => { visible += 100; render(); });
    const scheduler = Dashboard.scheduleRefresh(refresh, {
      intervalMs: 60000, cadenceFromLastStart: true, refreshOnVisible: "overdue",
    });
    $("force-refresh")?.addEventListener("click", () => scheduler.run());
  }

  global.StockAgentPublishedSignals = Object.freeze({validate, merge, init});
  init();
})(globalThis);

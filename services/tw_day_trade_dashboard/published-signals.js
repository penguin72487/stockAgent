"use strict";

/* The producer's model target is a different fact from a paper fill.  This
 * view never depends on the status/position APIs and never renders fills. */
(function installPublishedSignals(global) {
  // Share the already validated projection with the detail view without a
  // second request. This is a model target, never execution evidence.
  let sharedSnapshot = null, sharedError = "";
  function getSnapshot() { return {data: sharedSnapshot, error: sharedError}; }
  function notify() {
    if (typeof global.dispatchEvent === "function" && typeof global.CustomEvent === "function") {
      global.dispatchEvent(new global.CustomEvent("stockagent:published-signals"));
    }
  }
  function validate(data) {
    if (!data || data.schema_version !== 1 || data.simulation_only !== true
        || data.production_order_possible !== false
        || !(data.session_date === null || /^\d{4}-\d{2}-\d{2}$/.test(data.session_date || ""))
        || !Array.isArray(data.modes) || !Array.isArray(data.rows)) throw new Error("訊號格式不完整");
    const modes = new Map();
    for (const mode of data.modes) {
      if (!mode || typeof mode.market !== "string" || !mode.market || modes.has(mode.market)
          || typeof mode.label !== "string" || !["published", "waiting", "unavailable", "calendar_unverified"].includes(mode.status)
          || (data.session_date === null && mode.status !== "calendar_unverified")
          || (mode.completed_session_calculation !== undefined && typeof mode.completed_session_calculation !== "boolean")) {
        throw new Error("訊號模式格式錯誤");
      }
      if (mode.status === "published" && (!Number.isSafeInteger(mode.target_count) || mode.target_count < 0
          || !/^[a-f0-9]{32}$/.test(mode.public_signal_key || "") || !mode.signal_ready_at)) {
        throw new Error("訊號版本不完整");
      }
      modes.set(mode.market, mode);
    }
    const counts = new Map(), seen = new Set();
    if (data.session_date === null && data.rows.length) throw new Error("交易日尚未驗收");
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
    const reference = data.preopen_reference || {modes: [], rows: []};
    if (!Array.isArray(reference.modes) || !Array.isArray(reference.rows)) throw new Error("盤前參考格式錯誤");
    const referenceModes = new Map(), referenceCounts = new Map(), referenceSeen = new Set();
    for (const mode of reference.modes) {
      if (!mode || !modes.has(mode.market) || modes.get(mode.market).status !== "waiting"
          || referenceModes.has(mode.market) || typeof mode.label !== "string"
          || !/^\d{4}-\d{2}-\d{2}$/.test(mode.price_data_date || "")
          || !Number.isSafeInteger(mode.target_count) || mode.target_count < 0
          || !/^[a-f0-9]{32}$/.test(mode.public_signal_key || "") || !mode.signal_ready_at) {
        throw new Error("盤前參考版本錯誤");
      }
      referenceModes.set(mode.market, mode);
    }
    for (const row of reference.rows) {
      const mode = referenceModes.get(row?.market), key = `${row?.market}\u0000${row?.symbol}`;
      if (!mode || row.public_signal_key !== mode.public_signal_key || referenceSeen.has(key)
          || typeof row.symbol !== "string" || !row.symbol || typeof row.target_weight !== "number"
          || !Number.isFinite(row.target_weight) || row.target_weight === 0
          || row.side !== (row.target_weight > 0 ? "long" : "short")) throw new Error("盤前參考資料錯誤");
      referenceSeen.add(key);
      referenceCounts.set(row.market, (referenceCounts.get(row.market) || 0) + 1);
    }
    for (const mode of reference.modes) {
      if ((referenceCounts.get(mode.market) || 0) !== mode.target_count) throw new Error("盤前參考筆數不完整");
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
    return {session_date: incoming.session_date, modes, rows,
      preopen_reference: incoming.preopen_reference || {modes: [], rows: []}};
  }

  function emptyMessage(current, error) {
    if (error) return error;
    if (!current) return "正在讀取已發布訊號…";
    if (!current.session_date) return "交易日尚未驗收，暫無可顯示的訊號";
    if (!current.modes.length) return "目前沒有啟用的訊號模式";
    const published = current.modes.filter((mode) => mode.status === "published" && !mode.stale).length;
    if (published === current.modes.length) return "已發布的模式目前沒有非零目標";
    if (published) return "已發布的模式沒有非零目標；其餘模式尚未可用";
    return "本交易日的訊號尚未產生或無法核對，將自動重試";
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
    let scheduler = null, retryTimer = null, retryAttempts = 0;
    function scheduleRecovery() {
      const incomplete = !!error || (current?.modes || []).some((mode) => mode.status === "unavailable");
      if (!incomplete) {
        retryAttempts = 0;
        if (retryTimer !== null && typeof global.clearTimeout === "function") global.clearTimeout(retryTimer);
        retryTimer = null;
        return;
      }
      if (retryTimer !== null || retryAttempts >= 3 || typeof global.setTimeout !== "function") return;
      retryTimer = global.setTimeout(() => {
        retryTimer = null;
        retryAttempts += 1;
        scheduler?.run();
      }, 2000);
    }
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
      const modes = current?.modes || [], reference = current?.preopen_reference || {modes: [], rows: []};
      const references = new Map(reference.modes.map((mode) => [mode.market, mode]));
      const rows = [...(current?.rows || []).map((row) => ({...row, preopenReference: false})),
        ...reference.rows.map((row) => ({...row, preopenReference: true}))]
        .sort((a, b) => Math.abs(b.target_weight) - Math.abs(a.target_weight));
      const byMarket = new Map(modes.map((mode) => [mode.market, mode]));
      const summary = modes.map((mode) => {
        const version = mode.revision_index == null ? "" : `版本 ${mode.revision_index} · `;
        const phase = mode.completed_session_calculation ? "盤後計算 · " : "";
        const ref = references.get(mode.market);
        const text = mode.status === "published" ? `已發布 · ${phase}${version}${displayTime(mode.signal_ready_at)} · ${mode.target_count} 筆`
          : mode.stale ? `更新失敗，延用上一版 · ${phase}${version}${displayTime(mode.signal_ready_at)}`
          : ref ? `盤前參考 · ${ref.price_data_date} 收盤 · ${ref.target_count} 筆；等待今日訊號`
          : mode.status === "waiting" ? "等待今日訊號"
          : mode.status === "calendar_unverified" ? "交易日曆尚未驗收" : "訊號暫時無法核對";
        return `<div><span>${esc(mode.label)}</span><strong>${esc(text)}</strong></div>`;
      }).join("");
      setHtml("published-signal-status", (error ? `<div><span>訊號更新</span><strong>${esc(error)}</strong></div>` : "")
        + summary || "<div>等待訊號資料</div>");
      $("published-signal-count").textContent = current
        ? `${current.session_date || "交易日尚未驗收"} · ${Math.min(visible, rows.length)} / ${rows.length} 筆`
        : "尚未取得訊號";
      const button = $("published-signal-more");
      button.classList.toggle("hidden", rows.length <= visible);
      const html = rows.slice(0, visible).map((row) => {
        const mode = row.preopenReference ? references.get(row.market) : byMarket.get(row.market);
        if (row.preopenReference) return `<tr><td><strong>${esc(mode.label)}</strong><small>盤前參考 · ${esc(mode.price_data_date)} 收盤 · 非今日訊號</small></td><td><strong>${esc(row.symbol)}</strong><small>${esc(row.name || "")} · ${row.side === "long" ? "多" : "空"}</small></td><td><strong>${esc(percent(row.target_weight))}</strong></td></tr>`;
        const revision = (mode.completed_session_calculation ? "盤後計算 · " : "")
          + (mode.revision_index == null ? "" : `版本 ${mode.revision_index} · `);
        return `<tr data-public-signal-key="${esc(row.public_signal_key)}"><td><strong>${esc(mode.label)}</strong><small>${mode.stale ? "上一版 · " : ""}${esc(revision)}${esc(displayTime(mode.signal_ready_at))}</small></td><td><strong>${esc(row.symbol)}</strong><small>${esc(row.name || "")} · ${row.side === "long" ? "多" : "空"}</small></td><td><strong>${esc(percent(row.target_weight))}</strong></td></tr>`;
      }).join("");
      setHtml("published-signal-body", html || `<tr><td colspan="3">${esc(emptyMessage(current, error))}</td></tr>`);
    }
    async function refresh() {
      try {
        const headers = etag ? {"If-None-Match": etag} : {};
        const response = await fetchSnapshot("api/published-signals", {cache: "no-store", headers});
        if (response.status === 304 && current) {
          error = "";
          sharedSnapshot = current; sharedError = error;
          render();
          notify();
          scheduleRecovery();
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
      sharedSnapshot = current; sharedError = error;
      render();
      notify();
      scheduleRecovery();
    }
    $("published-signal-more").addEventListener("click", () => { visible += 100; render(); });
    scheduler = Dashboard.scheduleRefresh(refresh, {
      intervalMs: 60000, cadenceFromLastStart: true, refreshOnVisible: "overdue",
    });
    $("force-refresh")?.addEventListener("click", () => scheduler.run());
  }

  global.StockAgentPublishedSignals = Object.freeze({validate, merge, init, getSnapshot, emptyMessage});
  init();
})(globalThis);

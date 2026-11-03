import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import vm from "node:vm";

function pendingDetailHarness(kind) {
  const app = readFileSync(new URL("../services/tw_day_trade_dashboard/app.js", import.meta.url), "utf8");
  const requests = [];
  let release;
  const page = {rows: [{symbol: "2330", target_weight: .1}], total: 1, has_more: false};
  const context = vm.createContext({
    snapshot: {service_sync: {engine_generation: "owner-a", content_revision: 1}},
    selection: "scope-a", URLSearchParams, AbortController,
    Dashboard: {readJsonResponse: async response => response},
    fetchWithTimeout: async (url, options) => {
      requests.push({url, options});
      if (requests.length === 1) return new Promise(resolve => { release = () => resolve(page); });
      return page;
    },
    $: () => ({value: ""}),
    detailSelectionKey: () => context.selection, detailRangeKey: () => "range",
    selectedDetailStartDate: () => "2026-11-02", selectedDetailEndDate: () => "2026-11-02",
    selectedMode: () => "all", SIGNAL_PAGE_SIZE: 100, POSITION_PAGE_SIZE: 100, EVENT_PAGE_SIZE: 100,
    beginSilentTableUpdate() {}, validateDetailPage() {}, compareByAbsoluteWeight: () => 0,
    syncFeaturePanelSelection() {}, renderSignals() {}, renderPositions() {}, renderEvents() {},
    renderSignalFeaturePanel() {}, scheduleStatusRecovery() {},
  });
  for (const prefix of ["signal", "position", "event"]) {
    Object.assign(context, {[`${prefix}Loading`]: false, [`${prefix}Rows`]: [],
      [`${prefix}RequestRevisionInFlight`]: "", [`${prefix}RequestScopeInFlight`]: "",
      [`${prefix}RequestSequence`]: 0, [`${prefix}AbortController`]: null,
      [`${prefix}DataRevision`]: null, [`${prefix}SelectionKey`]: null,
      [`${prefix}LoadError`]: "", [`${prefix}Total`]: 0, [`${prefix}HasMore`]: false});
  }
  Object.assign(context, {eventRecordRevision: null, signalFeatureDrivers: {},
    signalDirectionSummary: {}, signalOpeningExecutionAudit: {}});
  vm.runInContext(app.slice(app.indexOf("function detailDataRevision("), app.indexOf("function chartWindowLabel(")), context);
  vm.runInContext(app.slice(app.indexOf("async function loadSignals("), app.indexOf("async function refresh(")), context);
  const loader = {signals: "loadSignals", positions: "loadPositions", events: "loadEvents"}[kind];
  return {context, requests, release: () => release(), load: () => context[loader]()};
}

for (const [kind, prefix] of [["signals", "signal"], ["positions", "position"], ["events", "event"]]) {
  test(`${kind}: new revisions coalesce without aborting a valid in-flight selection`, async () => {
    const h = pendingDetailHarness(kind);
    const first = h.load();
    h.context.snapshot.service_sync.content_revision = 2;
    await h.load();
    assert.equal(h.requests.length, 1);
    assert.equal(h.requests[0].options.signal.aborted, false);
    h.release();
    await first;
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(h.requests.length, 2, "one catch-up request follows successful older data");
    assert.equal(h.context[`${prefix}Rows`].length, 1);
    const revision = prefix === "event" ? "eventRecordRevision" : `${prefix}DataRevision`;
    assert.equal(h.context[revision], h.context.detailDataRevision(kind));
    assert.equal(h.context[`${prefix}LoadError`], "");
  });
  for (const change of ["selection", "owner"]) {
    test(`${kind}: ${change} changes still cancel and fence stale responses`, async () => {
      const h = pendingDetailHarness(kind);
      const first = h.load();
      if (change === "selection") h.context.selection = "scope-b";
      else h.context.snapshot.service_sync.engine_generation = "owner-b";
      await h.load();
      assert.equal(h.requests.length, 2);
      assert.equal(h.requests[0].options.signal.aborted, true);
      const rows = h.context[`${prefix}Rows`];
      h.release();
      await first;
      assert.equal(h.context[`${prefix}Rows`], rows);
      assert.equal(h.context[`${prefix}LoadError`], "");
    });
  }
}

test("minute history finishes a same-selection read before fetching the latest revision", async () => {
  const app = readFileSync(new URL("../services/tw_day_trade_dashboard/app.js", import.meta.url), "utf8");
  const calls = [], applied = [];
  let release;
  const context = vm.createContext({
    document: {hidden: false}, snapshot: {service_sync: {engine_generation: "owner-a"}},
    historyRevision: "history-a", chartHistory: null, chartHistoryKey: "", chartHistoryCache: new Map(),
    historyInFlight: false, historyRequestKeyInFlight: "", historyRequestScopeInFlight: "",
    historyRequestSequence: 0, historyAbortController: null, historyLoadError: "",
    availableDetailDates: ["2026-11-02"], HISTORY_CLIENT_CACHE_MAX_ENTRIES: 3,
    selectedDetailStartDate: () => "2026-11-02", selectedDetailEndDate: () => "2026-11-02",
    detailRangeKey: () => "range", chartRequestKey: () => context.historyRevision,
    chartHistoryMatchesSelection: () => true, AbortController, URLSearchParams,
    fetchMinuteHistory: async (url, options) => {
      calls.push({url, options});
      if (calls.length === 1) return new Promise(resolve => { release = () => resolve({ok: true}); });
      return {ok: true};
    },
    Dashboard: {readJsonResponse: async () => ({history: []}), cancelResponse() {}},
    decodeChartHistory: value => value, validateMinuteHistory() {}, renderChart() {}, scheduleStatusRecovery() {},
    applyChartHistory(payload, key) { context.chartHistory = payload; context.chartHistoryKey = key; applied.push(key); },
  });
  vm.runInContext(app.slice(app.indexOf("function chartReadScopeKey("), app.indexOf("function chartWindowLabel(")), context);
  vm.runInContext(app.slice(app.indexOf("async function loadChartHistory("), app.indexOf("function renderPriceSources(")), context);
  const pending = context.loadChartHistory({preferCache: true});
  context.historyRevision = "history-b";
  await context.loadChartHistory({preferCache: true});
  assert.equal(calls.length, 1);
  assert.equal(calls[0].options.signal.aborted, false);
  release();
  await pending;
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(calls.length, 2);
  assert.deepEqual(applied, ["history-a", "history-b"]);
  assert.equal(context.chartHistoryKey, "history-b");
  assert.equal(context.historyLoadError, "");
});

test("counterfactual prefix clocks are not presented as original historical signals", () => {
  const app = readFileSync(new URL("../services/tw_day_trade_dashboard/app.js", import.meta.url), "utf8");
  const context = vm.createContext({});
  vm.runInContext(app.slice(app.indexOf("const shortTime ="), app.indexOf("const signalReasonLabel ="))
    + ";globalThis.timing = replayTimingText;", context);
  const at = "2026-01-02T09:01:00+08:00";
  const replay = {signal_id: "tw_day_trade_100m-counterfactual-official-open-2026-01-02",
    signal_at: at, source_signal_at: at, open_reconstructed_at: at};
  assert.match(context.timing(replay), /目前模型反事實回放（非當時訊號）/);
  assert.doesNotMatch(context.timing(replay), /原始訊號/);
  assert.match(context.timing({...replay, signal_id: undefined,
    replay_signal_origin: "current_model_counterfactual"}), /目前模型反事實回放（非當時訊號）/);
  assert.doesNotMatch(context.timing({...replay, signal_id: "retained"}), /原始訊號/);
  assert.match(context.timing({...replay, signal_id: "retained", source_signal_at: "2026-01-02T09:00:12+08:00"}),
    /原始訊號 01-02 09:00:12/);
});

test("minute chart axis labels scale to phone width without removing source points", () => {
  const renderer = readFileSync(new URL("../services/tw_day_trade_dashboard/chart-renderer.js", import.meta.url), "utf8");
  const context = vm.createContext({});
  vm.runInContext(renderer.slice(renderer.indexOf("  function taipeiDayNumber("),
    renderer.indexOf("  function formatMinute(")), context);
  const first = Date.UTC(2026, 1, 25, 1, 1) / 60000;
  const minutes = Array.from({length: 156 * 270}, (_, index) => (
    first + Math.floor(index / 270) * 1440 + index % 270
  ));
  const phone = context.tickPlan(minutes, 3);
  const desktop = context.tickPlan(minutes, 10);
  assert.equal(phone.positions.length, 3);
  assert.equal(desktop.positions.length, 10);
  assert.equal(phone.positions[0], 0);
  assert.equal(phone.positions.at(-1), 155 * 270);
  assert.equal(minutes.length, 42120);
  const oneSession = context.tickPlan(minutes.slice(0, 270), 3);
  assert.equal(oneSession.positions.length, 3);
  assert.equal(oneSession.positions.at(-1), 269);
});

function components(options = {}) {
  const context = vm.createContext({});
  for (const file of ["presentation.js", "detail-components.js"]) {
    vm.runInContext(readFileSync(new URL(`../services/tw_day_trade_dashboard/${file}`, import.meta.url), "utf8"), context);
  }
  const presentation = context.StockAgentTwPresentation;
  const modes = [{market: "old_account_id", label: "LayerNorm v12 當沖"}];
  const escapeHtml = (value) => String(value ?? "—").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll('"', "&quot;");
  const nodes = new Map();
  const byId = (id) => {
    if (!nodes.has(id)) nodes.set(id, {textContent: "", html: "", attrs: {}, classes: {},
      setAttribute(k, v) { this.attrs[k] = v; },
      classList: {toggle(k, v) { nodes.get(id).classes[k] = v; }},
    });
    return nodes.get(id);
  };
  const format = Object.fromEntries(["number", "pct", "money", "sourceNumber", "summaryMoney", "displayPct", "shortTime", "replayTimingText", "signalTimingText"].map((key) => [key, () => "0"]));
  format.badge = (text) => `<span>${escapeHtml(text)}</span>`;
  format.pnlClass = () => "positive";
  format.signalReasonLabel = presentation.signalReasonLabel;
  Object.assign(format, options.formatOverrides || {});
  const view = context.StockAgentTwDetailComponents.create({
    Dashboard: {byId, escapeHtml, setTrustedHtml: (id, html) => { byId(id).html = html; }},
    strategyLabel: (row) => presentation.strategyLabel(row, modes),
    signalRowKey: (row) => `stable-key-${row.symbol}`,
    resolvedPositionPnl: () => ({signedShares: 1000, realized: 0, unrealized: 100, total: 100}),
    format,
    product: options.product || "tw_day_trade",
  });
  return {view, presentation, modes, byId};
}

test("fallback targets never claim a fill and respect date, mode, symbol and status", () => {
  const {view} = components();
  const key = "a".repeat(32);
  const data = {session_date: "2026-10-29", modes: [
    {market: "m", label: "模型", revision_index: 2, signal_ready_at: "2026-10-29T09:00:04+08:00"},
    {market: "n", label: "另一模式", stale: true, revision_index: 1}], rows: [
    {market: "m", symbol: "2330", name: "台積電", target_weight: 0.1, side: "long", public_signal_key: key},
    {market: "n", symbol: "2317", name: "鴻海", target_weight: -0.2, side: "short", public_signal_key: key}]};
  const selection = {start: "2026-10-29", end: "2026-10-29", mode: "all", symbol: "", status: "all"};
  assert.equal(view.publishedSignalFallback(data, {...selection, end: "2026-10-28"}), null);
  assert.equal(view.publishedSignalFallback(data, {...selection, mode: "m"}).rows.length, 1);
  assert.equal(view.publishedSignalFallback(data, {...selection, symbol: "鴻海"}).rows.length, 1);
  const filtered = view.publishedSignalFallback(data, {...selection, status: "closed"});
  assert.equal(filtered.rows.length, 0);
  assert.match(filtered.note, /無法套用/);
  const fallback = view.publishedSignalFallback(data, selection);
  assert.match(fallback.note, /非完整交易紀錄/);
  const html = view.publishedSignalFallbackRow(fallback.rows[0]);
  assert.match(html, /模擬委託及成交待核對/);
  assert.doesNotMatch(html, /0 股|已成交|不可當沖/);
  assert.match(view.publishedSignalFallbackRow(fallback.rows[1]), /上一版/);
});

test("newer published identity is not falsely paired with an older committed signal", () => {
  const {view} = components();
  const key = "a".repeat(32), old = "b".repeat(32);
  const data = {session_date: "2026-10-29", modes: [{market: "m", status: "published", stale: false,
    public_signal_key: key, signal_ready_at: "2026-10-29T09:00:06+08:00"}], rows: []};
  const selection = {start: "2026-10-29", end: "2026-10-29", mode: "all", symbol: "", status: "all"};
  const row = {session_date: "2026-10-29", market: "m", symbol: "2330", public_signal_key: old,
    signal_at: "2026-10-29T09:00:05+08:00"};
  assert.equal(view.publishedSignalAhead(data, selection, [row]), true);
  assert.equal(view.publishedSignalFallback(data, selection, {ledgerReady: true}), null,
    "a newer model preview cannot hide successful committed ledger details");
  assert.ok(view.publishedSignalFallback(data, selection, {ledgerReady: false}),
    "keep the independent target fallback while the ledger is unavailable");
  assert.equal(view.publishedSignalAhead(data, selection, [{...row, public_signal_key: key}]), false);
  assert.equal(view.publishedSignalAhead(data, selection, [{...row, signal_at: "2026-10-29T09:00:07+08:00"}]), false);
  assert.equal(view.publishedSignalAhead(data, selection, [{...row, signal_at: null}]), false);
  assert.equal(view.publishedSignalAhead({...data, modes: [{...data.modes[0], stale: true}]}, selection, [row]), false);
  assert.equal(view.publishedSignalAhead(data, {...selection, status: "closed"}, [row]), false);
  assert.equal(view.publishedSignalAhead(data, {...selection, end: "2026-10-28"}, [row]), false);
});

test("fallback positions are scoped to a committed open snapshot, never closed history", () => {
  const {view} = components();
  const data = {as_of: "2026-10-29T02:01:00Z", state_revision: 7, modes: [{market: "m", label: "模型"}],
    rows: [{market: "m", symbol: "2330", name: "台積電", session_date: "2026-10-29", signed_shares: 1000, target_weight: 0.1},
      {market: "m", symbol: "2317", session_date: "2026-10-28", signed_shares: -1000, target_weight: -0.3}]};
  const selection = {start: "2026-10-29", end: "2026-10-29", mode: "all", symbol: "", status: "all"};
  const current = view.livePositionFallback(data, selection);
  assert.equal(current.rows.length, 1);
  assert.equal(current.rows[0].symbol, "2330");
  assert.equal(current.rows[0].label, "模型");
  assert.match(current.note, /不含完整生命週期/);
  assert.equal(view.livePositionFallback(data, {...selection, end: "2026-10-28"}), null);
  assert.equal(view.livePositionFallback(data, {...selection, status: "open"}).rows.length, 1);
  const closed = view.livePositionFallback(data, {...selection, status: "closed"});
  assert.equal(closed.rows.length, 0);
  assert.match(closed.note, /無法查詢已平倉/);
});

test("entry bracket rejections are explicit and never called a fill", () => {
  const {presentation} = components();
  for (const [reason, boundary] of [
    ["entry_price_at_or_beyond_take_profit", "TP"],
    ["entry_price_at_or_beyond_stop_loss", "SL"],
  ]) {
    const label = presentation.signalReasonLabel(reason);
    assert.match(label, new RegExp(boundary));
    assert.match(label, /不進場/);
    assert.doesNotMatch(label, /已成交|已補齊/);
  }
});

test("shared day-trade and overnight signal requests hide zero targets on the server before pagination", () => {
  const app = readFileSync(new URL("../services/tw_day_trade_dashboard/app.js", import.meta.url), "utf8");
  const signalLoader = app.slice(app.indexOf("async function loadSignals("), app.indexOf("async function loadPositions("));
  const positionLoader = app.slice(app.indexOf("async function loadPositions("), app.indexOf("async function loadEvents("));
  assert.match(signalLoader, /hide_zero_targets: "1"/);
  assert.doesNotMatch(positionLoader, /hide_zero_targets/);
  assert.match(app, /product: IS_OVERNIGHT \? "tw_overnight" : "tw_day_trade"/);
  const html = readFileSync(new URL("../services/tw_day_trade_dashboard/index.html", import.meta.url), "utf8");
  assert.match(html, /隱藏目標為 0 的訊號/);
  assert.match(html, /非零目標但 0 股未成交的股票仍保留/);
  assert.match(app, /隱藏目標為 0 的訊號，非零目標但 0 股未成交的股票仍保留/);
  assert.doesNotMatch(app, /保留所有模型目標/);
});

test("owner prices are labelled private observations without claiming public-feed permission", () => {
  const {view} = components();
  const data = {price_display_policy: {private_view: true, message: "本人私人檢視"}};
  const row = {symbol: "2330", last_mark_price: 123.456789,
    public_price_display: {withheld_fields: [], visible_fields: {
      last_mark_price: {source: "本人私人檢視", publication_basis: "owner_private_view"},
    }}};
  assert.equal(view.priceNoticeText(data), "本人私人檢視");
  const html = view.priceSources({data, positions: [row]}).sourcesHtml;
  assert.match(html, /本人私人檢視/);
  assert.doesNotMatch(html, /已驗證可公開行情/);
});

test("withheld public quotes remain pending without inventing zero notional or unfilled trades", () => {
  const {view} = components({formatOverrides: {money: (value) => value == null ? "—" : `NT$ ${value}`}});
  const hidden = {withheld_fields: ["entry_price", "execution_price", "sizing_open_price", "last_mark_price", "bid", "ask"]};
  const html = view.signalRow({symbol: "2330", target_weight: 0.1, requested_shares: 1000, filled_shares: 1000,
    execution_price: null, sizing_open_price: null, public_price_display: hidden,
    position: {filled_shares: 1000, signed_shares: 1000, entry_price: null, last_mark_price: null,
      public_price_display: hidden}}, {});
  assert.match(html, /等待可公開來源/);
  assert.match(html, /名目 —/);
  assert.doesNotMatch(html, /名目 NT\$ 0|未成交不納入|0 股紙上成交/);
  assert.doesNotMatch(html, /class="negative">開盤計價/);
});

test("progressive targets show actual inventory without attributing old fills to the new decision", () => {
  const {view} = components({formatOverrides: {number: (value) => String(value ?? 0)}});
  const row = {symbol: "2330", target_weight: .2, requested_shares: 0, filled_shares: 0,
    reduction_filled_shares: 2000, current_target_inventory: {
      signed_shares: 2000, target_signed_shares: 2000, signal_revision: 3, cohort_count: 1}};
  const html = view.signalRow(row);
  assert.match(html, /版本 3 · 庫存 2000 股／目標 2000 股/);
  assert.match(html, /本版增倉成交 0 股 · 減倉成交 2000 股/);
  assert.match(html, /本版無增倉成交；既有成交與費用另行保留/);
  assert.doesNotMatch(html, /未成交不納入|0 股紙上成交/);
  assert.match(view.signalRow({...row, target_weight: 0}), /零目標・減倉至零/);
  assert.match(view.signalRow({...row, current_target_inventory: {
    ...row.current_target_inventory, target_signed_shares: null}}), /目標 待取價/);
});

test("progressive completion follows inventory deltas rather than addition-only fill counts", () => {
  const app = readFileSync(new URL("../services/tw_day_trade_dashboard/app.js", import.meta.url), "utf8");
  const start = app.indexOf("  const revisionHtml =");
  const source = app.slice(start, app.indexOf('  setHtml("signal-direction-summary"', start));
  const row = {market: "unit", day_trade_signal_update_policy: "progressive_open_reference_rebalance_v1",
    opening_signal_revision: {index: 2}, inventory_rebalance: {
      contract_valid: true, target_complete: false, remaining_delta_shares: 2000, unresolved_symbol_count: 1}};
  const context = vm.createContext({snapshot: {modes: [row]}, publishedAhead: false, esc: String, strategyLabel: String,
    number: String, shortTime: String});
  vm.runInContext(`function render() { ${source}\n return revisionHtml; }`, context);
  assert.match(context.render(), /庫存仍差 2000 股 · 1 檔待取價/);
  row.inventory_rebalance.target_complete = "not-a-boolean";
  assert.doesNotMatch(context.render(), /實際庫存已達本版完整目標/);
  row.inventory_rebalance.target_complete = true;
  assert.match(context.render(), /實際庫存已達本版完整目標/);
});

test("price withholding does not turn a closed day-trade or overnight position into an open position", () => {
  for (const product of ["tw_day_trade", "tw_overnight"]) {
    const {view} = components({product});
    const html = view.positionRow({symbol: "2330", status: "closed", filled_shares: 1000,
      entry_price: null, exit_price: null, last_exit_price: null,
      public_price_display: {withheld_fields: ["entry_price", "exit_price", "last_exit_price"]}});
    assert.match(html, /已平倉・等待可公開來源/);
    assert.doesNotMatch(html, /持倉中|等待實際開盤/);
  }
});

test("public source clocks are escaped in the footer, not repeated beside event prices", () => {
  const {view} = components({formatOverrides: {sourceNumber: (value) => String(value ?? "—")}});
  const row = {price: 112.5, public_price_display: {visible_fields: {
    price: {source: '<img onerror="x">', observed_at: "2026-10-13T09:00:01+08:00"}}}};
  const visible = view.eventRow(row);
  assert.match(visible, /112.5/);
  assert.doesNotMatch(visible, /公開行情|onerror/);
  const footer = view.priceSources({events: [row]}).sourcesHtml;
  assert.match(footer, /&lt;img/);
  assert.match(footer, /10-13 09:00:01/);
  assert.doesNotMatch(footer, /<img/);
  assert.doesNotMatch(visible, /<img/);
  const hidden = view.eventRow({price: 12345.67, public_price_display: {withheld_fields: ["price"]}});
  assert.match(hidden, /等待可公開來源/);
  assert.doesNotMatch(hidden, /12345.67/);
});

test("all detail rows use the active strategy label, not the immutable account ID", () => {
  const {view, modes} = components();
  const row = {market: "old_account_id", symbol: "2330", name: "<script>alert(1)</script>", status: "hold"};
  for (const render of [view.signalRow, view.positionRow, view.eventRow]) {
    const html = render(row);
    assert.match(html, /LayerNorm v12/);
    assert.doesNotMatch(html, /old_account_id|<script>/);
  }
  modes[0].label = "下一版策略";
  assert.match(view.signalRow(row), /下一版策略/);
  assert.doesNotMatch(view.signalRow(row), /LayerNorm/);
});

test("unknown strategy or policy never silently claims another deployment", () => {
  const {presentation} = components();
  assert.equal(presentation.strategyLabel("missing", []), "策略名稱未提供");
  assert.match(presentation.entryPolicy({entry_fill_policy: "new_policy"}), /未辨識成交規則/);
  assert.match(presentation.entryPolicy({entry_fill_policy: "constructor"}), /未辨識成交規則/);
  assert.match(presentation.entryPolicy({entry_fill_policy: "official_open_signal_0900_execute_0901_vwap"}), /09:01/);
});

test("every signal row has an explicit dated futures badge, including unknown", () => {
  const {view, presentation} = components();
  for (const [status, label] of [["listed", "有期貨"], ["not_listed", "無期貨"], ["unknown", "期貨未確認"]]) {
    const html = view.signalRow({symbol: "2330", stock_futures: {status, products: ["CD"], as_of: "2026-09-06T13:00:00Z"}});
    assert.match(html, new RegExp(label));
    assert.match(html, /2026-09-06/);
    assert.match(html, new RegExp(`data-futures-status="${status}"`));
  }
  assert.match(view.signalRow({symbol: "1101"}), /期貨未確認/);
  assert.match(view.futuresBadge({status: "listed", products: ['<img src=x onerror="alert(1)">']}), /&lt;img/);
  assert.equal(presentation.futuresPresentation({status: "constructor"}).status, "unknown");
});

test("missed opening replay is not labelled as a live market-order fill", () => {
  const {view} = components();
  const html = view.signalRow({
    symbol: "2330", market: "old_account_id", side: "long", target_weight: 0.1,
    requested_shares: 17000, filled_shares: 1000,
    counterfactual_0901_price_fill: true,
    entry_fill_policy: "official_open_signal_0900_execute_0901_vwap",
    minute_kbar_capacity_shares: 1000, execution_price: 100,
  });
  assert.match(html, /09:01 首分鐘價反事實模擬/);
  assert.match(html, /容量限制未建立委託/);
  assert.doesNotMatch(html, /市價成交/);
});

test("filled signals use their exact embedded valuation independently of position pagination", () => {
  const {view} = components();
  const html = view.signalRow({symbol: "2330", filled_shares: 1000,
    position: {filled_shares: 1000, signed_shares: 1000, last_mark_price: 103}}, {});
  assert.match(html, /現在可清算價/);
  assert.match(html, /估值新鮮/);
  assert.doesNotMatch(html, /未成交不納入|估值資料未取得/);
});

test("missing valuation never rewrites a recorded paper fill as unfilled", () => {
  const {view} = components();
  const filled = view.signalRow({symbol: "2330", filled_shares: 1000});
  assert.match(filled, /紙上已成交・估值未取得/);
  assert.match(filled, /估值資料未取得/);
  assert.doesNotMatch(filled, /未成交不納入|不計盈虧/);
  const unfilled = view.signalRow({symbol: "2330", filled_shares: 0});
  assert.match(unfilled, /未成交不納入/);
  assert.doesNotMatch(unfilled, /紙上已成交/);
});

test("Snapshot valuation is not described as an executable liquidation quote", () => {
  const {view} = components();
  const html = view.signalRow({symbol: "2330", filled_shares: 1000,
    position: {filled_shares: 1000, signed_shares: 1000, last_mark_price: 103,
      last_mark_quote_source: "shioaji:stock_snapshot+prepared_limits"}});
  assert.match(html, /參考快照估值（非成交證據）/);
  assert.doesNotMatch(html, /現在可清算價/);
});

test("one paged-table shell preserves rows during errors and owns loading/count state", () => {
  const {view, byId} = components();
  view.pagedTable({id: "signal", rows: [1], total: 30, loading: true, hasMore: true, error: "<bad>",
    renderRow: () => "<tr>existing row</tr>", emptyText: "empty"});
  assert.equal(byId("signal-body").attrs["aria-busy"], "true");
  assert.match(byId("signal-body").html, /existing row/);
  assert.match(byId("signal-body").html, /&lt;bad>/);
  assert.equal(byId("load-more-signals").disabled, true);
  assert.equal(byId("load-more-signals").classes.hidden, false);
  view.pagedTable({id: "signal", rows: [], total: 0, loading: false, hasMore: false, error: "讀取失敗",
    renderRow: () => "", emptyText: "目前沒有訊號"});
  assert.match(byId("signal-body").html, /讀取失敗/);
  assert.doesNotMatch(byId("signal-body").html, /目前沒有訊號/);
});

test("mode card keeps filtered returns separate from the shared Discord account DTO", () => {
  const app = readFileSync(new URL("../services/tw_day_trade_dashboard/app.js", import.meta.url), "utf8");
  const source = app.slice(app.indexOf("function renderModes(data)"), app.indexOf("function renderBenchmarks(data)"));
  let output = "";
  let selectedReturn = 2;
  const formatters = Object.fromEntries(["esc", "strategyLabel", "engineStatusShortLabel", "badge", "pnlClass", "summaryMoney", "number", "money", "shortTime", "sourceNumber"].map((key) => [key, String]));
  const context = vm.createContext({...formatters, IS_OVERNIGHT: false,
    rangeSummaryFor: () => ({return_pct: selectedReturn, end_equity_twd: 1250, cumulative_net_pnl_twd: 250, initial_capital_twd: 1000}),
    executionStatusPresentation: () => ({kind: "good", label: "done"}),
    fillOutcomePresentation: () => ({kind: "good", label: "done"}),
    Presentation: {entryPolicy: () => "causal"}, displayPct: (value) => `${Number(value).toFixed(2)}%`,
    setHtml: (_id, html) => { output = html; },
  });
  vm.runInContext(source, context);
  const mode = {market: "unit", account_performance: {status: "available", return_pct: 25, asof: "2026-09-09", state_revision: 10}};
  context.renderModes({modes: [mode]});
  assert.match(output, />\+2\.00%</);
  assert.match(output, /帳戶累積報酬（Discord 同口徑）/);
  assert.match(output, />\+25\.00%</);
  selectedReturn = 3;
  context.renderModes({modes: [mode]});
  assert.match(output, />\+3\.00%</);
  assert.match(output, />\+25\.00%</);
  mode.account_performance = {status: "unavailable"};
  context.renderModes({modes: [mode]});
  assert.match(output, /資料不可用/);
  assert.doesNotMatch(output, />\+25\.00%</);
  mode.indicative_valuation_position_count = 7;
  context.renderModes({modes: [mode]});
  assert.match(output, /參考快照估值（非成交證據）/);
  assert.match(output, />7 個持倉</);
  assert.match(output, />\+3\.00%</);
  mode.indicative_valuation_position_count = 0;
  context.renderModes({modes: [mode]});
  assert.doesNotMatch(output, /參考快照估值（非成交證據）/);
  assert.match(output, /模擬目標／成交／缺口/);
  assert.match(output, /等待成交資訊/);
  assert.doesNotMatch(output, /data-paper-entry-counts/);
  Object.assign(mode, {entry_requested_shares: 1000, entry_filled_shares: 500, entry_unfilled_shares: 500});
  context.renderModes({modes: [mode]});
  assert.match(output, /data-paper-entry-counts data-requested="1000" data-filled="500" data-unfilled="500"/);
  assert.match(output, /1000／500／500 股/);
  mode.entry_filled_shares = null;
  context.renderModes({modes: [mode]});
  assert.match(output, /等待成交資訊/);
  assert.doesNotMatch(output, /data-paper-entry-counts/);
});

test("both TW charts key large history by date and source revision only", () => {
  const app = readFileSync(new URL("../services/tw_day_trade_dashboard/app.js", import.meta.url), "utf8");
  const source = app.slice(app.indexOf("function chartRequestKey()"), app.indexOf("function chartHistoryMatchesSelection()"));
  const context = vm.createContext({
    snapshot: {service_sync: {content_revision: 7, history_revision: "history-a"}},
    detailRangeKey: () => "2026-09-01|2026-09-22",
  });
  vm.runInContext(source, context);
  const first = context.chartRequestKey();
  assert.equal(first, context.chartRequestKey());
  assert.doesNotMatch(source, /selectedMode|textFilter|status-filter/);
  context.snapshot.service_sync.content_revision = 8;
  context.snapshot.service_sync.revision_token = "status-only-change";
  context.snapshot.historical_replay = {generated_at: "status-only-change"};
  assert.equal(context.chartRequestKey(), first);
  context.snapshot.service_sync.history_revision = "history-b";
  assert.notEqual(context.chartRequestKey(), first);
  context.snapshot.service_sync.history_revision = "history-a";
  context.detailRangeKey = () => "2026-09-03|2026-09-22";
  assert.notEqual(context.chartRequestKey(), first);
});

test("a previous curve stays visible but cannot supply current-period accounting", () => {
  const app = readFileSync(new URL("../services/tw_day_trade_dashboard/app.js", import.meta.url), "utf8");
  const source = app.slice(app.indexOf("function chartHistoryMatchesSelection()"), app.indexOf("function compareByAbsoluteWeight("));
  const context = vm.createContext({
    chartHistory: {
      history: [], start_date: "2026-09-01", end_date: "2026-09-22",
      range_summary: [{series_id: "strategy", cumulative_net_pnl_twd: 100}],
    },
    chartHistoryKey: "old-source",
    chartRequestKey: () => "new-source",
    selectedDetailStartDate: () => "2026-09-01",
    selectedDetailEndDate: () => "2026-09-22",
  });
  vm.runInContext(source, context);
  assert.equal(context.chartHistoryMatchesSelection(), true);
  assert.equal(context.rangeSummaryFor("strategy"), null);
  context.chartHistoryKey = "new-source";
  assert.equal(context.rangeSummaryFor("strategy").cumulative_net_pnl_twd, 100);
  const renderer = readFileSync(new URL("../services/tw_day_trade_dashboard/chart-renderer.js", import.meta.url), "utf8");
  assert.match(renderer, /資料更新中，暫顯示上一份已驗證曲線/);
});

test("overnight missing history cannot become a zero PnL", () => {
  const app = readFileSync(new URL("../services/tw_day_trade_dashboard/app.js", import.meta.url), "utf8");
  const source = app.slice(app.indexOf("function totalModeNetPnl("), app.indexOf("function resolvedPositionPnl("));
  const context = vm.createContext({
    IS_OVERNIGHT: true,
    chartHistory: {range_summary: [{series_id: "one"}]},
    rangeSummaryFor: () => null,
  });
  vm.runInContext(source, context);
  assert.equal(context.totalModeNetPnl({modes: [{market: "one"}]}), null);
  context.rangeSummaryFor = () => ({cumulative_net_pnl_twd: 125});
  assert.equal(context.totalModeNetPnl({modes: [{market: "one"}]}), 125);
});

test("opening telemetry shows durable publication subphases without treating them as extra stages", () => {
  const app = readFileSync(new URL("../services/tw_day_trade_dashboard/app.js", import.meta.url), "utf8");
  const source = app.slice(app.indexOf("function renderOperations(data)"), app.indexOf("function renderTwPublicMonitor(payload)"));
  const nodes = new Map();
  const byId = id => {
    if (!nodes.has(id)) nodes.set(id, {textContent: "", html: ""});
    return nodes.get(id);
  };
  const context = vm.createContext({
    IS_OVERNIGHT: false, lastFetchMs: null, SERVICE_REVISION_REFRESH_MS: 1000, PRICE_REFRESH_MS: 60000,
    clampRatio: value => Math.max(0, Math.min(1, Number(value || 0))),
    number: (value, digits = 0) => Number(value || 0).toFixed(digits),
    duration: String, sourceNumber: String, shortTime: String, countdown: () => "—",
    esc: value => String(value ?? "—").replaceAll("<", "&lt;"),
    strategyLabel: () => "unit", badge: String, progress: () => "",
    $: byId, setHtml: (id, html) => { byId(id).html = html; },
  });
  vm.runInContext(app.slice(app.indexOf("function officialUniverseDescription("),
    app.indexOf("function renderOvernightOperations(")), context);
  vm.runInContext(source, context);
  const row = {market: "unit", ready_from_0900_ms: 2000, stages: {},
    state_publication: {total_ms: 12.3, projection_fingerprint_ms: 1.2, files: [
      {json_encode_ms: 1, file_write_fsync_ms: 2, replace_directory_fsync_ms: 0.5},
      {json_encode_ms: 2, file_write_fsync_ms: 3, replace_directory_fsync_ms: 1},
    ]}};
  const data = {modes: [], opening_signal_latency: {modes: [row]}};
  context.renderOperations(data);
  const html = byId("opening-stage-progress").html;
  assert.match(html, /帳本發布（帳本落盤內含分段，不可再相加）/);
  assert.match(html, /完整發布 12.3 ms/);
  assert.match(html, /JSON 編碼 3.0 ms/);
  assert.match(html, /檔案寫入／同步 5.0 ms/);
  assert.match(html, /替換／目錄同步 1.5 ms/);
  delete row.state_publication.files[1].json_encode_ms;
  context.renderOperations(data);
  assert.doesNotMatch(byId("opening-stage-progress").html, /JSON 編碼/);
  delete row.state_publication;
  context.renderOperations(data);
  assert.doesNotMatch(byId("opening-stage-progress").html, /帳本發布|JSON 編碼/);
});

test("corrupt opening diagnostic is visibly incomplete and never reported as a clean zero", () => {
  const app = readFileSync(new URL("../services/tw_day_trade_dashboard/app.js", import.meta.url), "utf8");
  const source = app.slice(app.indexOf("function renderOperations(data)"), app.indexOf("function renderTwPublicMonitor(payload)"));
  const nodes = new Map();
  const byId = id => {
    if (!nodes.has(id)) nodes.set(id, {textContent: "", html: ""});
    return nodes.get(id);
  };
  const context = vm.createContext({
    IS_OVERNIGHT: false, lastFetchMs: null, SERVICE_REVISION_REFRESH_MS: 1000, PRICE_REFRESH_MS: 60000,
    clampRatio: value => Math.max(0, Math.min(1, Number(value || 0))),
    number: (value, digits = 0) => Number(value || 0).toFixed(digits),
    duration: String, sourceNumber: String, shortTime: String, countdown: () => "—",
    esc: value => String(value ?? "—").replaceAll("<", "&lt;"),
    strategyLabel: () => "unit", badge: String, progress: () => "",
    $: byId, setHtml: (id, html) => { byId(id).html = html; },
  });
  vm.runInContext(app.slice(app.indexOf("function officialUniverseDescription("),
    app.indexOf("function renderOvernightOperations(")), context);
  vm.runInContext(source, context);
  context.renderOperations({modes: [], opening_signal_latency: {
    modes: [], failure_count: 0, integrity: {state: "partial_corrupt", malformed_lines: 1},
  }});
  assert.match(byId("latency-kpis").html, /測速紀錄不完整/);
  assert.match(byId("latency-kpis").html, /至少 0/);
  assert.doesNotMatch(byId("latency-kpis").html, /所選日沒有測速失敗紀錄/);
  assert.match(byId("opening-stage-progress").html, /測速紀錄有損壞/);
});

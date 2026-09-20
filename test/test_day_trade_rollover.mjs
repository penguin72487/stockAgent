import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import vm from "node:vm";

const app = readFileSync(new URL("../services/tw_day_trade_dashboard/app.js", import.meta.url), "utf8");
function harness({overnight = false} = {}) {
  const nodes = new Map();
  const timers = new Map();
  let timerId = 0;
  const byId = (id) => {
    if (!nodes.has(id)) nodes.set(id, {value: "", options: [], setAttribute() {}, classList: {remove() {}}});
    return nodes.get(id);
  };
  const context = vm.createContext({
    IS_OVERNIGHT: overnight, $, performance: {now: () => 0}, document: {hidden: false},
    window: {setTimeout: (fn, ms) => { timers.set(++timerId, {fn, ms}); return timerId; }, clearTimeout: (id) => timers.delete(id)},
    setHtml: () => {}, esc: String, strategyLabel: (row) => row.label,
    Date, Map, Set, AbortController, console,
  });
  function $(id) { return byId(id); }
  vm.runInContext(app.slice(app.indexOf("let snapshot = null;"), app.indexOf("try {\n  const storedHiddenSeries")), context);
  vm.runInContext(app.slice(app.indexOf("function selectedMode()"), app.indexOf("function chartWindowLabel()")), context);
  vm.runInContext(app.slice(app.indexOf("function validateDetailPage"), app.indexOf("function healthPresentation")), context);
  vm.runInContext(app.slice(app.indexOf("function syncFilters(data)"), app.indexOf("function renderModes(data)")), context);
  const run = (code) => vm.runInContext(code, context);
  const data = (day) => ({session_date: day, available_session_dates: [day, "2026-09-08"], modes: []});
  return {context, run, byId, timers, data};
}

test("automatic day follows status, manual history stays pinned", () => {
  const {context, run, byId, data} = harness();
  context.data = data("2026-09-08"); run("syncFilters(data)");
  assert.equal(run("selectedDate()"), "");
  context.data = data("2026-09-09"); run("syncFilters(data)");
  assert.equal(byId("detail-start-date").value, "2026-09-09");
  assert.equal(byId("detail-end-date").value, "2026-09-09");
  run("followLatestSession = false");
  byId("detail-start-date").value = byId("detail-end-date").value = "2026-09-08";
  context.data = data("2026-09-11"); run("syncFilters(data)");
  assert.equal(run("selectedDate()"), "2026-09-08");
  run("followLatestSession = true; syncFilters(data)");
  assert.equal(byId("detail-end-date").value, "2026-09-11");
});

test("date transition clears stale rows and invalidates decoded old requests", () => {
  const {run} = harness();
  run("signalRows = [{symbol:'yesterday'}]; positionRows = [{}]; chartHistory = {}; signalDataRevision = 'old'; positionDataRevision = 'old'; clearDateScopedViews()");
  assert.equal(run("signalRows.length + positionRows.length"), 0);
  assert.equal(run("signalDataRevision"), null);
  assert.equal(run("positionDataRevision"), null);
  assert.equal(run("chartHistory"), null);
  assert.equal(run("signalRequestSequence"), 1);
  assert.equal(run("historyRequestSequence"), 1);
});

test("08:30 timer uses server clock, fresh revisions advance a stale timer", () => {
  const {context, run, timers} = harness();
  let refreshed = 0;
  context.refresh = () => { refreshed++; };
  context.sync = {generated_at_utc: "2026-09-09T00:29:00Z", session_clock: {next_rollover_at: "2026-09-09T08:30:00+08:00"}};
  run("scheduleSessionRollover(sync)");
  assert.equal([...timers.values()][0].ms, 60000);
  context.sync.generated_at_utc = "2026-09-09T00:29:59Z";
  run("scheduleSessionRollover(sync)");
  assert.equal(timers.size, 1);
  assert.equal([...timers.values()][0].ms, 1000);
  [...timers.values()][0].fn();
  assert.equal(refreshed, 1);
  run("followLatestSession = false; scheduleSessionRollover(sync)");
  assert.equal(timers.size, 1);
});

test("a never-finishing history cannot delay core details and events stay deferred", async () => {
  const {context, run, data} = harness();
  const started = [];
  Object.assign(context, {
    fetchWithTimeout: async () => ({}),
    Dashboard: {readJsonResponse: async () => data("2026-09-09")},
    revisionOf: () => "new", render: () => {},
    loadSignals: () => { started.push("signals"); return new Promise(() => {}); },
    loadPositions: () => { started.push("positions"); return new Promise(() => {}); },
    loadEvents: () => { started.push("events"); return new Promise(() => {}); },
    loadChartHistory: () => { started.push("history"); return new Promise(() => {}); },
  });
  run(app.slice(app.indexOf("async function refresh({"), app.indexOf("function activateTwPublicMonitor()")));
  await run("refresh()");
  assert.deepEqual(started, ["signals", "positions", "history"]);
  assert.equal(run("refreshInFlight"), false);

  run("eventViewActivated = true");
  await run("refresh({force: true})");
  assert.deepEqual(started, [
    "signals", "positions", "history",
    "signals", "positions", "events", "history",
  ]);
});

test("empty compact status cannot erase a loaded position page or suppress its refresh", async () => {
  const {context, run, data} = harness();
  const day = "2026-09-16";
  context.initial = {...data(day), service_sync: {content_revision: 1}, positions: []};
  run("snapshot = initial; syncFilters(snapshot); positionRows = [{symbol: '2330'}]; positionTotal = 95; positionDataRevision = detailDataRevision('positions')");
  const started = [];
  Object.assign(context, {
    fetchWithTimeout: async () => ({}),
    Dashboard: {readJsonResponse: async () => ({...data(day), service_sync: {content_revision: 2}, positions: [], payload_window: {positions: 0}})},
    revisionOf: () => "new", render: () => {},
    loadSignals: () => {},
    loadPositions: () => { started.push("positions"); },
    loadEvents: () => {},
    loadChartHistory: () => {},
  });
  run(app.slice(app.indexOf("async function refresh({"), app.indexOf("function activateTwPublicMonitor()")));
  await run("refresh()");
  assert.equal(run("positionRows.length"), 1);
  assert.equal(run("positionTotal"), 95);
  assert.deepEqual(started, ["positions"]);
});

test("position table distinguishes pending data from an authoritative empty page", () => {
  const {context, run} = harness();
  const rendered = [];
  context.detailComponents = {pagedTable: (options) => rendered.push(options), positionRow: () => ""};
  run(app.slice(app.indexOf("function renderPositions()"), app.indexOf("function renderSignals()")));
  run("renderPositions()");
  assert.equal(rendered.at(-1).emptyText, "正在讀取符合篩選的持倉…");
  run("positionSelectionKey = detailSelectionKey('positions'); positionDataRevision = detailDataRevision('positions'); renderPositions()");
  assert.equal(rendered.at(-1).emptyText, "目前沒有符合篩選的持倉");
});

test("position page replaces rows only after its scope and counts validate", async () => {
  const {context, run, data} = harness();
  context.initial = {...data("2026-09-16"), service_sync: {content_revision: 2}};
  run("snapshot = initial; syncFilters(snapshot); positionRows = [{symbol: 'old'}]; positionTotal = 95");
  let page = {start_date: "2026-09-16", end_date: "2026-09-16", offset: 0,
    returned: 1, total: 95, has_more: true, rows: [{symbol: "2330"}]};
  Object.assign(context, {
    POSITION_PAGE_SIZE: 100,
    URLSearchParams,
    fetchWithTimeout: async () => ({}),
    Dashboard: {readJsonResponse: async () => page},
    beginSilentTableUpdate: () => {},
    renderPositions: () => {}, renderSignals: () => {}, renderSignalFeaturePanel: () => {},
  });
  run(app.slice(app.indexOf("async function loadPositions({"), app.indexOf("async function loadEvents({")));
  await run("loadPositions({force: true})");
  assert.equal(run("positionRows[0].symbol"), "2330");
  assert.equal(run("positionTotal"), 95);
  assert.equal(run("positionDataRevision === detailDataRevision('positions')"), true);
  assert.equal(run("positionSelectionKey === detailSelectionKey('positions')"), true);

  page = {...page, rows: [], returned: 0, total: 0};
  await run("loadPositions({force: true})");
  assert.equal(run("positionRows[0].symbol"), "2330");
  assert.match(run("positionLoadError"), /持倉分頁範圍或筆數不完整/);
});

test("signal and event pages keep prior rows when an incomplete page arrives", async () => {
  for (const overnight of [false, true]) {
    const {context, run, data} = harness({overnight});
    context.initial = {...data("2026-09-16"), service_sync: {content_revision: 2}};
    run("snapshot = initial; syncFilters(snapshot); signalRows = [{symbol: 'old'}]; eventRows = [{symbol: 'old'}]");
    let page = {start_date: "2026-09-16", end_date: "2026-09-16", offset: 0,
      returned: 0, total: 0, has_more: false, rows: []};
    Object.assign(context, {
      SIGNAL_PAGE_SIZE: 100, EVENT_PAGE_SIZE: 100, URLSearchParams,
      fetchWithTimeout: async () => ({}),
      Dashboard: {readJsonResponse: async () => page},
      beginSilentTableUpdate: () => {}, compareByAbsoluteWeight: () => 0,
      syncFeaturePanelSelection: () => {}, renderSignals: () => {},
      renderSignalFeaturePanel: () => {}, renderEvents: () => {},
    });
    run(app.slice(app.indexOf("async function loadSignals({"), app.indexOf("async function loadPositions({")));
    run(app.slice(app.indexOf("async function loadEvents({"), app.indexOf("async function refresh({")));

    page = {...page, rows: undefined};
    await run("loadSignals({force: true})");
    assert.equal(run("signalRows[0].symbol"), "old");
    assert.match(run("signalLoadError"), /訊號分頁範圍或筆數不完整/);
    page = {...page, rows: []};
    await run("loadSignals({force: true})");
    assert.equal(run("signalRows.length"), 0);
    assert.equal(run("signalDataRevision === detailDataRevision('signals')"), true);

    page = {...page, order_total: 1, fill_total: 0};
    await run("loadEvents({force: true})");
    assert.equal(run("eventRows[0].symbol"), "old");
    assert.match(run("eventLoadError"), /事件分頁統計不完整/);
    page = {...page, order_total: 0};
    await run("loadEvents({force: true})");
    assert.equal(run("eventRows.length"), 0);
    assert.equal(run("eventRecordRevision === detailDataRevision('events')"), true);
  }
});

test("changed filters hide old rows and reject a late response from the old scope", async () => {
  const {context, run, data, byId} = harness();
  context.initial = {...data("2026-09-16"), service_sync: {content_revision: 2}};
  run("snapshot = initial; syncFilters(snapshot); positionRows = [{symbol: '2330'}]; positionTotal = 1; positionSelectionKey = detailSelectionKey('positions'); positionDataRevision = detailDataRevision('positions')");
  const rendered = [];
  context.detailComponents = {pagedTable: (options) => rendered.push(options), positionRow: () => ""};
  run(app.slice(app.indexOf("function renderPositions()"), app.indexOf("function renderSignals()")));
  run("renderPositions()");
  assert.equal(rendered.at(-1).rows.length, 1);
  byId("symbol-filter").value = "0050";
  run("renderPositions()");
  assert.equal(rendered.at(-1).rows.length, 0);
  assert.match(rendered.at(-1).emptyText, /正在讀取/);

  let release;
  Object.assign(context, {
    POSITION_PAGE_SIZE: 100, URLSearchParams,
    fetchWithTimeout: () => new Promise((resolve) => { release = resolve; }),
    Dashboard: {readJsonResponse: async () => ({start_date: "2026-09-16", end_date: "2026-09-16",
      offset: 0, returned: 1, total: 1, has_more: false, rows: [{symbol: "2330"}]})},
    beginSilentTableUpdate: () => {}, renderSignals: () => {}, renderSignalFeaturePanel: () => {},
  });
  run(app.slice(app.indexOf("async function loadPositions({"), app.indexOf("async function loadEvents({")));
  const pending = run("loadPositions({force: true})");
  byId("symbol-filter").value = "1101";
  release({});
  await pending;
  assert.equal(run("positionRows[0].symbol"), "2330");
  assert.notEqual(run("positionSelectionKey"), run("detailSelectionKey('positions')"));
  run("renderPositions()");
  assert.equal(rendered.at(-1).rows.length, 0);
  assert.equal(run("positionLoadError"), "");
});

test("minute history rejects an incomplete curve instead of replacing the displayed one", () => {
  const {context, run} = harness();
  context.Chart = {decodeHistory: (payload) => payload};
  run(app.slice(app.indexOf("function decodeChartHistory("), app.indexOf("function applyChartHistory(")));
  const page = {history_encoding: "minute_columns_v2", start_date: "2026-09-16", end_date: "2026-09-16",
    minute_axis: [1], history: [], range_summary: [], returned_points: 1,
    minute_series: [{minute_indexes: [0], return_pct: [0], cumulative_return_pct: [0], quality_flags: [0]}]};
  context.page = page;
  run("validateMinuteHistory(page, {startDate: '2026-09-16', endDate: '2026-09-16', unbounded: false})");
  context.badPage = {...page, minute_series: [{...page.minute_series[0], return_pct: []}]};
  assert.throws(() => run("validateMinuteHistory(badPage, {startDate: '2026-09-16', endDate: '2026-09-16', unbounded: false})"), /分鐘曲線範圍或點數不完整/);
  context.badPage = {...page, start_date: "2026-09-15"};
  assert.throws(() => run("validateMinuteHistory(badPage, {startDate: '2026-09-16', endDate: '2026-09-16', unbounded: false})"), /分鐘曲線範圍或點數不完整/);
});

test("all detail tables reject mixed page totals during append", () => {
  const {context, run, data} = harness();
  context.initial = data("2026-09-16");
  run("snapshot = initial; syncFilters(snapshot)");
  const page = {start_date: "2026-09-16", end_date: "2026-09-16", offset: 1,
    returned: 1, total: 3, has_more: true, rows: [{symbol: "2330"}],
    order_total: 2, fill_total: 1};
  context.page = page;
  for (const kind of ["訊號", "持倉", "事件"]) {
    context.kind = kind;
    assert.throws(() => run("validateDetailPage(page, {kind, offset: 1, limit: 100, expectedTotal: 2})"), /分頁範圍或筆數不完整/);
  }
});

test("official source monitor retains last success and marks a failed refresh stale", async () => {
  const {context, run, byId} = harness();
  byId("tw-public-monitor-list").html = "<article>既有官方來源</article>";
  byId("tw-public-monitor-summary").textContent = "1 / 1 來源完成";
  Object.assign(context, {
    loadTwPublicMonitorWithFallback: async () => ({}),
    shortDateTime: () => "09/16 20:00",
    setHtml: (id, html) => { byId(id).html = html; },
    renderTwPublicMonitor: () => { throw new Error("unexpected render"); },
  });
  run("twPublicMonitorActivated = true; twPublicMonitorData = {sources: [{id: 'twse'}]}; twPublicMonitorLastUpdated = '2026-09-16T12:00:00Z'");
  run(app.slice(app.indexOf("async function loadTwPublicMonitor()"), app.indexOf("function renderChart(data)")));
  await run("loadTwPublicMonitor()");
  assert.equal(byId("tw-public-monitor-list").html, "<article>既有官方來源</article>");
  assert.equal(byId("tw-public-monitor-summary").textContent, "1 / 1 來源完成");
  assert.match(byId("tw-public-monitor-fetch-state").textContent, /更新失敗.*最後成功資料/);
});

test("chart note exposes refresh failure while keeping the last valid selected history", () => {
  const chartCode = readFileSync(new URL("../services/tw_day_trade_dashboard/chart-renderer.js", import.meta.url), "utf8");
  const nodes = new Map();
  const byId = (id) => {
    if (!nodes.has(id)) nodes.set(id, {
      textContent: "", classList: {add() {}, remove() {}, toggle() {}},
      replaceChildren() {}, setAttribute() {},
    });
    return nodes.get(id);
  };
  const context = vm.createContext({document: {getElementById: byId}, performance: {now: () => 0}});
  vm.runInContext(chartCode, context);
  const history = {minute_axis: [], minute_series: [], range_summary: []};
  const input = {data: {modes: [], benchmarks: []}, history, historyMatchesSelection: true,
    historyLoadError: "", historyInFlight: false, hiddenSeries: new Set(), selectedMode: "all",
    detailRangeKey: "2026-09-16|2026-09-16", isOvernight: false,
    strategyLabel: String, chartWindowLabel: "09/16", formatNumber: String,
    formatCount: String, pnlClass: () => ""};
  context.input = input;
  context.StockAgentTwChart.render(input);
  assert.match(byId("equity-range-note").textContent, /沒有可繪製資料/);
  input.historyLoadError = "更新請求失敗";
  context.StockAgentTwChart.render(input);
  assert.match(byId("equity-range-note").textContent, /更新請求失敗.*保留上次成功資料/);
});

test("minute chart labels carried valuation instead of presenting it as fresh", () => {
  const chartCode = readFileSync(new URL("../services/tw_day_trade_dashboard/chart-renderer.js", import.meta.url), "utf8");
  const nodes = new Map();
  const byId = (id) => {
    if (!nodes.has(id)) nodes.set(id, {
      textContent: "", classList: {add() {}, remove() {}, toggle() {}},
      replaceChildren() {}, setAttribute() {}, getBoundingClientRect: () => ({width: 500}),
    });
    return nodes.get(id);
  };
  const context = vm.createContext({
    document: {
      getElementById: byId,
      createElement: () => ({dataset: {}, append() {}, setAttribute() {}}),
      createTextNode: (value) => value,
      hidden: false,
    },
    performance: {now: () => 0},
    matchMedia: () => ({matches: false}),
    requestAnimationFrame: (callback) => callback(),
    uPlot: class {
      constructor(options) { this.width = options.width; this.height = options.height; }
      destroy() {}
    },
  });
  vm.runInContext(chartCode, context);
  const history = {
    minute_axis: [29_828_220, 29_828_221],
    minute_series: [{series_id: "m", series_type: "strategy", minute_indexes: [0, 1],
      return_pct: [0, 1], cumulative_return_pct: [0, 1], quality_flags: [0, 1]}],
    range_summary: [{series_id: "m", point_count: 2, expected_minute_points: 2, minute_coverage_ratio: 1}],
  };
  context.StockAgentTwChart.render({
    data: {modes: [{market: "m", label: "M"}], benchmarks: []}, history,
    historyMatchesSelection: true, historyLoadError: "", historyInFlight: false,
    hiddenSeries: new Set(), selectedMode: "all", detailRangeKey: "today",
    isOvernight: false, strategyLabel: (row) => row.label,
    chartWindowLabel: "今日", formatNumber: String, formatCount: String, pnlClass: () => "",
  });
  assert.match(byId("equity-range-note").textContent, /1 個顯示點使用延用估值或缺價，非即時可成交報價/);
});

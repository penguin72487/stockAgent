import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import vm from "node:vm";

const app = readFileSync(new URL("../services/tw_day_trade_dashboard/app.js", import.meta.url), "utf8");
function harness() {
  const nodes = new Map();
  const timers = new Map();
  let timerId = 0;
  const byId = (id) => {
    if (!nodes.has(id)) nodes.set(id, {value: "", options: [], setAttribute() {}, classList: {remove() {}}});
    return nodes.get(id);
  };
  const context = vm.createContext({
    IS_OVERNIGHT: false, $, performance: {now: () => 0}, document: {hidden: false},
    window: {setTimeout: (fn, ms) => { timers.set(++timerId, {fn, ms}); return timerId; }, clearTimeout: (id) => timers.delete(id)},
    setHtml: () => {}, esc: String, strategyLabel: (row) => row.label,
    Date, Map, Set, AbortController, console,
  });
  function $(id) { return byId(id); }
  vm.runInContext(app.slice(app.indexOf("let snapshot = null;"), app.indexOf("try {\n  const storedHiddenSeries")), context);
  vm.runInContext(app.slice(app.indexOf("function selectedMode()"), app.indexOf("function chartWindowLabel()")), context);
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
    hydrateDefaultPositions: () => false, revisionOf: () => "new", render: () => {},
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

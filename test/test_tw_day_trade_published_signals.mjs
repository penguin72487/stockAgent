import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import vm from "node:vm";

const source = readFileSync(new URL("../services/tw_day_trade_dashboard/published-signals.js", import.meta.url), "utf8");
const key = "a".repeat(32);
const snapshot = (overrides = {}) => ({schema_version: 1, simulation_only: true,
  production_order_possible: false, session_date: "2026-10-28",
  modes: [{market: "m", label: "模型", status: "published", signal_ready_at: "2026-10-28T09:00:01+08:00",
    revision_index: 0, target_count: 1, public_signal_key: key}],
  rows: [{market: "m", symbol: "2330", name: "台積電", target_weight: 0.1, side: "long", public_signal_key: key}],
  ...overrides});

function contracts() {
  const context = vm.createContext({});
  vm.runInContext(source, context);
  return context.StockAgentPublishedSignals;
}

test("only independently published signal identities with complete target counts are accepted", () => {
  const {validate} = contracts();
  assert.equal(validate(snapshot()).rows.length, 1);
  for (const bad of [
    snapshot({simulation_only: false}), snapshot({production_order_possible: true}),
    snapshot({rows: []}), snapshot({rows: [{...snapshot().rows[0], public_signal_key: "b".repeat(32)}]}),
    snapshot({rows: [{...snapshot().rows[0], target_weight: 0}]}),
    snapshot({rows: [{...snapshot().rows[0], side: "short"}]}),
  ]) assert.throws(() => validate(bad));
});

test("a failed mode keeps its explicitly stale last good version; a new day never carries it", () => {
  const {merge} = contracts();
  const previous = merge(null, snapshot());
  const failed = snapshot({modes: [{market: "m", label: "模型", status: "unavailable"}], rows: []});
  const retained = merge(previous, failed);
  assert.equal(retained.modes[0].stale, true);
  assert.equal(retained.modes[0].status, "unavailable");
  assert.equal(retained.rows[0].symbol, "2330");
  const nextDay = merge(previous, {...failed, session_date: "2026-10-29"});
  assert.equal(nextDay.modes[0].stale, false);
  assert.equal(nextDay.rows.length, 0);
});

test("new versions and genuine zero-target revisions replace rather than invent old targets", () => {
  const {merge} = contracts();
  const previous = merge(null, snapshot());
  const updated = snapshot({modes: [{...snapshot().modes[0], revision_index: 1, public_signal_key: "b".repeat(32)}],
    rows: [{...snapshot().rows[0], symbol: "2317", public_signal_key: "b".repeat(32)}]});
  const next = merge(previous, updated);
  assert.equal(next.rows[0].symbol, "2317");
  assert.equal(next.modes[0].revision_index, 1);
  const empty = snapshot({modes: [{...snapshot().modes[0], target_count: 0}], rows: []});
  assert.equal(merge(next, empty).rows.length, 0);
});

test("website paints published signals while no status or fill API has responded, then updates in place", async () => {
  const nodes = new Map(), requests = [], scheduled = [], optionsSeen = [];
  function node(id) {
    if (!nodes.has(id)) nodes.set(id, {textContent: "", html: "", hidden: true, listeners: {},
      classList: {remove() {}, add() {}, toggle() {}}, addEventListener(k, fn) { this.listeners[k] = fn; }});
    return nodes.get(id);
  }
  let data = snapshot(), etag = '"first"', fail = false;
  const dashboard = {
    byId: node,
    escapeHtml: value => String(value ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll('"', "&quot;"),
    setTrustedHtml: (id, html) => { node(id).html = html; },
    createFetch: () => async (url, options) => {
      requests.push({url, headers: options.headers});
      if (fail) throw new Error("offline");
      if (options.headers["If-None-Match"] === etag) return {status: 304};
      return {status: 200, headers: {get: () => etag}, payload: data};
    },
    readJsonResponse: async response => response.payload,
    scheduleRefresh: (callback, options) => { optionsSeen.push(options); scheduled.push(callback); return {run: callback}; },
  };
  const context = vm.createContext({StockAgentDashboard: dashboard,
    document: {getElementById: node}, location: {pathname: "/tw-day-trade/"}, Intl, Date});
  vm.runInContext(source, context);
  assert.equal(scheduled.length, 1);
  assert.equal(optionsSeen[0].intervalMs, 60000);
  assert.equal(optionsSeen[0].cadenceFromLastStart, true);
  assert.equal(optionsSeen[0].refreshOnVisible, "overdue");
  await scheduled[0]();
  assert.match(node("published-signal-body").html, /2330/);
  assert.doesNotMatch(node("published-signal-body").html, /成交/);
  assert.deepEqual(requests.map(r => r.url), ["api/published-signals"]);
  await scheduled[0]();
  assert.equal(requests[1].headers["If-None-Match"], '"first"');
  data = snapshot({modes: [{...snapshot().modes[0], revision_index: 1, public_signal_key: "b".repeat(32)}],
    rows: [{...snapshot().rows[0], symbol: "2317", public_signal_key: "b".repeat(32)}]});
  etag = '"second"';
  await scheduled[0]();
  assert.match(node("published-signal-body").html, /2317/);
  assert.doesNotMatch(node("published-signal-body").html, /2330/);
  fail = true;
  await scheduled[0]();
  assert.match(node("published-signal-body").html, /2317/);
  assert.match(node("published-signal-status").html, /更新失敗/);
});

test("signal-first panel and detailed signals precede the lifecycle table", () => {
  const html = readFileSync(new URL("../services/tw_day_trade_dashboard/index.html", import.meta.url), "utf8");
  assert.ok(html.indexOf('id="published-signals"') < html.indexOf('id="signal-body"'));
  assert.ok(html.indexOf('id="signal-body"') < html.indexOf('id="position-body"'));
  assert.match(html, /不代表模擬委託已送出或已成交/);
});

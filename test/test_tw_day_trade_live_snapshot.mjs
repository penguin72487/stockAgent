import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import vm from "node:vm";

const source = readFileSync(new URL("../services/tw_day_trade_dashboard/live-snapshot.js", import.meta.url), "utf8");
const sample = (overrides = {}) => ({schema_version: 1, simulation_only: true, production_order_possible: false,
  scope: "committed_open_positions_only", state_revision: 7, as_of: "2026-10-28T09:00:01+08:00",
  modes: [{market: "m", label: "模型", open_position_count: 1, engine_status: "active"}],
  rows: [{market: "m", symbol: "2330", signed_shares: 1000, target_weight: 0.1}], ...overrides});

test("temporary commit conflicts get one bounded retry, never a fabricated snapshot", async () => {
  const context = vm.createContext({Date});
  vm.runInContext(source, context);
  const {retryTransientRead} = context.StockAgentTwLivePaper;
  const options = {cache: "no-store"};
  for (const status of [503, 504]) {
    const responses = [{status}, {status: 200}], calls = [], delays = [];
    const response = await retryTransientRead(async (url, passed) => {
      calls.push([url, passed]);
      return responses.shift();
    }, "api/live-snapshot", options, async ms => delays.push(ms));
    assert.equal(response.status, 200);
    assert.equal(calls.length, 2);
    assert.equal(calls[1][1], options);
    assert.deepEqual(delays, [250]);
  }
  let calls = 0;
  const unavailable = await retryTransientRead(async () => { calls++; return {status: 503}; },
    "api/live-events", options, async () => {});
  assert.equal(unavailable.status, 503);
  assert.equal(calls, 2);
  calls = 0;
  assert.equal((await retryTransientRead(async () => { calls++; return {status: 404}; },
    "api/live-events", options, async () => {})).status, 404);
  assert.equal(calls, 1);
});

test("only complete, simulated committed open-position views are accepted", () => {
  const context = vm.createContext({Date});
  vm.runInContext(source, context);
  const {validate} = context.StockAgentTwLivePaper;
  assert.equal(validate(sample()).rows.length, 1);
  for (const bad of [sample({simulation_only: false}), sample({production_order_possible: true}),
    sample({rows: []}), sample({rows: [{...sample().rows[0], signed_shares: 0}]}),
    sample({as_of: "2026-10-28T09:00:01"}), sample({scope: "all_history"})]) {
    assert.throws(() => validate(bad));
  }
});

test("live view updates without status API, retains last committed version on failure", async () => {
  const nodes = new Map(), requests = [], scheduled = [], optionsSeen = [];
  function node(id) {
    if (!nodes.has(id)) nodes.set(id, {textContent: "", html: "", listeners: {}, classList: {remove() {}, add() {}, toggle() {}},
      addEventListener(k, fn) { this.listeners[k] = fn; }});
    return nodes.get(id);
  }
  let data = sample(), etag = '"one"', fail = false;
  const Dashboard = {
    byId: node, escapeHtml: value => String(value ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;"),
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
  const context = vm.createContext({StockAgentDashboard: Dashboard, Date, Intl,
    detailComponents: {positionRow: row => `<tr><td>${row.symbol}</td></tr>`},
    document: {getElementById: node}, location: {pathname: "/tw-day-trade/"}});
  vm.runInContext(source, context);
  assert.deepEqual(optionsSeen.map(option => option.intervalMs), [60000, 60000]);
  assert.ok(optionsSeen.every(option => option.cadenceFromLastStart && option.refreshOnVisible === "overdue"));
  await scheduled[0]();
  assert.match(node("live-paper-body").html, /2330/);
  assert.deepEqual(requests.map(row => row.url), ["api/live-snapshot"]);
  await scheduled[0]();
  assert.equal(requests[1].headers["If-None-Match"], '"one"');
  data = sample({state_revision: 8, rows: [{...sample().rows[0], symbol: "2317"}]});
  etag = '"two"';
  await scheduled[0]();
  assert.match(node("live-paper-body").html, /2317/);
  assert.doesNotMatch(node("live-paper-body").html, /2330/);
  fail = true;
  await scheduled[0]();
  assert.match(node("live-paper-body").html, /2317/);
  assert.match(node("live-paper-status").html, /更新失敗，保留上次已確認快照/);
});

test("one-minute display cadence does not falsely mark a recent source stale", async () => {
  const nodes = new Map(), scheduled = [];
  function node(id) {
    if (!nodes.has(id)) nodes.set(id, {textContent: "", html: "", classList: {remove() {}, add() {}, toggle() {}},
      addEventListener() {}});
    return nodes.get(id);
  }
  const sourceAt = Date.parse(sample().as_of);
  let currentTime = sourceAt + 60000;
  class ClockDate extends Date { static now() { return currentTime; } }
  const Dashboard = {byId: node, escapeHtml: value => String(value ?? ""),
    setTrustedHtml: (id, html) => { node(id).html = html; },
    createFetch: () => async () => ({status: 200, headers: {get: () => ""}, payload: sample()}),
    readJsonResponse: async response => response.payload,
    scheduleRefresh: callback => { scheduled.push(callback); return {run: callback}; }};
  const context = vm.createContext({StockAgentDashboard: Dashboard, Date: ClockDate, Intl,
    detailComponents: {positionRow: () => "", eventRow: () => ""},
    document: {getElementById: node}, location: {pathname: "/tw-day-trade/"}});
  vm.runInContext(source, context);
  await scheduled[0]();
  assert.doesNotMatch(node("live-paper-status").html, /秒未更新/);
  currentTime = sourceAt + 121000;
  await scheduled[0]();
  assert.match(node("live-paper-status").html, /121 秒未更新/);
});

test("event projection rejects malformed and non-paper records", () => {
  const context = vm.createContext({Date});
  vm.runInContext(source, context);
  const {validateEvents} = context.StockAgentTwLivePaper;
  const events = {schema_version: 1, simulation_only: true, production_order_possible: false,
    scope: "recent_committed_events_only", state_revision: 7, as_of: "2026-10-28T09:00:01+08:00",
    rows: [{event_kind: "fill", replay: true}]};
  assert.equal(validateEvents(events).rows[0].replay, true);
  for (const bad of [{...events, production_order_possible: true}, {...events, scope: "all_history"},
    {...events, rows: [{event_kind: "fill"}]}, {...events, rows: [{event_kind: "broker", replay: false}]}]) {
    assert.throws(() => validateEvents(bad));
  }
});

test("recent event updates independently and labels replay fills", async () => {
  const nodes = new Map(), scheduled = [];
  function node(id) {
    if (!nodes.has(id)) nodes.set(id, {textContent: "", html: "", classList: {remove() {}, add() {}, toggle() {}},
      addEventListener() {}});
    return nodes.get(id);
  }
  let fail = false;
  const data = {schema_version: 1, simulation_only: true, production_order_possible: false,
    scope: "recent_committed_events_only", state_revision: 7, as_of: "2026-10-28T09:00:01+08:00",
    rows: [{event_kind: "fill", replay: true, symbol: "2330"}]};
  const Dashboard = {byId: node, escapeHtml: v => String(v ?? ""),
    setTrustedHtml: (id, html) => {node(id).html = html;},
    createFetch: () => async () => {if (fail) throw Error("offline"); return {status: 200, headers: {get: () => ""}, payload: data};},
    readJsonResponse: async response => response.payload,
    scheduleRefresh: callback => {scheduled.push(callback); return {run: callback};}};
  const context = vm.createContext({StockAgentDashboard: Dashboard, Date, Intl,
    detailComponents: {eventRow: row => `<tr>${row.order_type} ${row.symbol}</tr>`, positionRow: () => ""},
    document: {getElementById: node}, location: {pathname: "/tw-day-trade/"}});
  vm.runInContext(source, context);
  await scheduled[1]();
  assert.match(node("live-events-body").html, /回放紙上成交 2330/);
  fail = true;
  await scheduled[1]();
  assert.match(node("live-events-body").html, /2330/);
  assert.match(node("live-events-status").html, /更新失敗，保留上次已確認紀錄/);
});

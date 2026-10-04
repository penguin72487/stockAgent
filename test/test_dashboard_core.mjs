import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import vm from "node:vm";

const SOURCE = readFileSync(
  new URL("../services/public_dashboards/dashboard-core.js", import.meta.url),
  "utf8",
);

function fakeDocument(current = "tw-day-trade") {
  const listeners = new Map();
  const nav = {
    dataset: {dashboardNav: current},
    children: [],
    replaceChildren(fragment) { this.children = [...fragment.children]; },
  };
  const document = {
    documentElement: {dataset: {}},
    hidden: false,
    getElementById() { return null; },
    querySelectorAll(selector) { return selector === "nav[data-dashboard-nav]" ? [nav] : []; },
    createDocumentFragment() {
      return {children: [], append(node) { this.children.push(node); }};
    },
    createElement(tagName) {
      return {
        tagName,
        attributes: {},
        href: "",
        textContent: "",
        setAttribute(name, value) { this.attributes[name] = String(value); },
      };
    },
    createElementNS(_namespace, tagName) {
      return this.createElement(tagName);
    },
    addEventListener(name, listener) {
      if (!listeners.has(name)) listeners.set(name, new Set());
      listeners.get(name).add(listener);
    },
    removeEventListener(name, listener) { listeners.get(name)?.delete(listener); },
    dispatchEvent(event) {
      for (const listener of listeners.get(event.type) || []) listener(event);
      return true;
    },
  };
  return {document, nav, listeners};
}

function loadCore({current = "tw-day-trade", fetchImpl, localStorage, globals = {}, beforeInstall} = {}) {
  const {document, nav, listeners} = fakeDocument(current);
  const requests = [];
  const sandbox = {
    AbortController,
    DOMException,
    URL,
    clearInterval,
    clearTimeout,
    document,
    fetch: fetchImpl || (async (input, options) => {
      requests.push({input, options});
      return {ok: true, status: 200, json: async () => ({ok: true})};
    }),
    location: {href: "https://dashboard.example/tw-day-trade/", origin: "https://dashboard.example", pathname: "/tw-day-trade/"},
    localStorage,
    setInterval,
    setTimeout,
    ...globals,
  };
  beforeInstall?.(sandbox);
  vm.createContext(sandbox);
  vm.runInContext(SOURCE, sandbox, {filename: "dashboard-core.js"});
  return {core: sandbox.StockAgentDashboard, document, listeners, nav, requests, sandbox};
}

test("deferred rendering keeps only the latest hidden snapshot and flushes on reveal without fetching", async () => {
  let closed = true;
  let top = 20;
  const target = {closest: () => closed ? {} : null, getClientRects: () => closed ? [] : [{}],
    getBoundingClientRect: () => ({top, bottom: top + 100, left: 0, right: 200})};
  const {core, document, requests, listeners} = loadCore({globals: {innerHeight: 768, innerWidth: 1366}});
  const rendered = [];
  const render = core.createDeferredRenderer(target, value => rendered.push(value));
  render("old"); render("latest");
  assert.deepEqual(rendered, []);
  closed = false;
  document.dispatchEvent({type: "toggle"}); await new Promise(setImmediate);
  assert.deepEqual(rendered, ["latest"]);
  top = 2000;
  document.dispatchEvent({type: "acquisition:reveal"}); await new Promise(setImmediate);
  render("offscreen"); render("newest");
  assert.equal(rendered.length, 1);
  top = 20;
  document.dispatchEvent({type: "acquisition:reveal"}); await new Promise(setImmediate);
  assert.deepEqual(rendered, ["latest", "newest"]);
  document.hidden = true;
  document.dispatchEvent({type: "visibilitychange"}); await new Promise(setImmediate);
  render("foreground");
  document.hidden = false;
  document.dispatchEvent({type: "visibilitychange"}); await new Promise(setImmediate);
  assert.deepEqual(rendered, ["latest", "newest", "foreground"]);
  render.dispose();
  core.observeVisibility(target, () => {});
  assert.equal(listeners.get("toggle").size, 1, "visibility event handling is shared");
  assert.equal(requests.length, 0);
});

test("intersection hints cannot override a closed ancestor, and loading DOM waits until ready", async () => {
  let hint;
  const target = {closest: () => null, getClientRects: () => [{}],
    getBoundingClientRect: () => ({top: 20, bottom: 100, left: 0, right: 200})};
  const {core, document} = loadCore({globals: {innerHeight: 768, innerWidth: 1366,
    IntersectionObserver: class {constructor(callback) { hint = callback; } observe() {} disconnect() {}}}});
  document.readyState = "loading";
  const changes = [];
  const observer = core.observeVisibility(target, visible => changes.push(visible));
  hint([{isIntersecting: true}]);
  assert.deepEqual(changes, [false]);
  document.readyState = "complete";
  document.dispatchEvent({type: "DOMContentLoaded"}); await new Promise(setImmediate);
  assert.deepEqual(changes, [false, true]);
  target.closest = () => ({});
  hint([{isIntersecting: true}]);
  assert.deepEqual(changes, [false, true, false]);
  observer.dispose();
});

test("one broken disclosure callback cannot prevent other visibility consumers updating", async () => {
  let closed = true;
  const target = {closest:() => closed ? {} : null, getClientRects:() => [{}],
    getBoundingClientRect:() => ({top:20,bottom:100,left:0,right:200})};
  const errors = [], changes = [];
  const {core, document} = loadCore({globals:{innerHeight:768,innerWidth:1366,
    console:{error:value => errors.push(value)}}});
  core.observeVisibility(target, visible => {if (visible) throw new Error("private details must not leak");});
  core.observeVisibility(target, visible => changes.push(visible));
  closed=false;document.dispatchEvent({type:"toggle"});await new Promise(setImmediate);
  assert.deepEqual(changes, [false,true]);
  assert.deepEqual(errors, ["Dashboard visibility callback failed"]);
});

test("shared navigation renders one canonical route list and current page", () => {
  const {core, nav} = loadCore({current: "tw-day-trade"});
  assert.equal(core.NAV_ITEMS.length, 11);
  assert.equal(nav.children.length, 11);
  assert.deepEqual(nav.children.map((link) => link.textContent), [
    "總覽", "TAIFEX", "台股當沖", "隔日沖", "永豐 API", "FinLab", "FinMind", "TEJ", "OpenBB", "全資料", "流量",
  ]);
  assert.equal(nav.children[0].href, "/");
  assert.equal(nav.children[2].href, "/tw-day-trade/");
  assert.equal(nav.children[2].attributes["aria-current"], "page");
  assert.equal(nav.children[6].href, "/finmind/");
  assert.equal(nav.children[9].href, "/data-monitor/");
  assert.equal(nav.dataset.dashboardNavMounted, "true");
  assert.ok(Object.isFrozen(core));
  assert.ok(Object.isFrozen(core.NAV_ITEMS));
});

test("provider detail navigation resolves to canonical root routes, not nested siblings", () => {
  const {core, nav, sandbox} = loadCore({current: "data-monitor", beforeInstall(context) {
    context.location.href = "https://dashboard.example/data-monitor/providers/finmind/";
    context.location.pathname = "/data-monitor/providers/finmind/";
  }});
  const resolved = nav.children.map(link => new URL(link.href, sandbox.location.href).pathname);
  assert.deepEqual(resolved, Array.from(core.NAV_ITEMS, item => item.slug ? `/${item.slug}/` : "/"));
  assert.equal(nav.children[9].attributes["aria-current"], "page");
});

test("Escape closes the expanded menu when focus remains on its toggle", () => {
  const headerEvents = new Map(), navEvents = new Map();
  let toggle, focused = false;
  const {nav} = loadCore({beforeInstall(sandbox) {
    const target = sandbox.document.querySelectorAll("nav[data-dashboard-nav]")[0];
    target.addEventListener = (name, listener) => navEvents.set(name, listener);
    target.parentElement = {
      querySelector() { return null; },
      addEventListener(name, listener) { headerEvents.set(name, listener); },
      insertBefore(node) {
        toggle = node;
        node.focus = () => { focused = true; };
        node.addEventListener = (name, listener) => { node[name] = listener; };
      },
    };
  }});
  toggle.click();
  assert.equal(nav.dataset.expanded, "true");
  headerEvents.get("keydown")?.({key: "Escape", target: toggle});
  assert.equal(nav.dataset.expanded, "false");
  assert.equal(toggle.attributes["aria-expanded"], "false");
  assert.equal(focused, true);
});

test("shared formatters cap visible precision and escape unsafe strings", () => {
  const {core} = loadCore();
  assert.equal(core.formatNumber(1234.5678), "1,234.57");
  assert.equal(core.formatNumber(-0.001), "0");
  assert.equal(core.formatBytes(1536), "1.5 KiB");
  assert.equal(core.formatAge(90), "2 分鐘前");
  assert.equal(core.escapeHtml(`<img src=x onerror="alert(1)">`), "&lt;img src=x onerror=&quot;alert(1)&quot;&gt;");
});

test("shared DOM writers skip unchanged content", () => {
  const {core} = loadCore();
  let textWrites = 0;
  let htmlWrites = 0;
  let textValue = "ready";
  let htmlValue = "";
  const node = {
    get textContent() { return textValue; },
    set textContent(value) { textWrites += 1; textValue = value; },
    get innerHTML() { return htmlValue; },
    set innerHTML(value) { htmlWrites += 1; htmlValue = value; },
  };
  assert.equal(core.setText(node, "ready"), false);
  assert.equal(core.setText(node, "updated"), true);
  assert.equal(core.setText(node, "updated"), false);
  assert.equal(textWrites, 1);
  assert.equal(core.setTrustedHtml(node, "<b>safe</b>"), true);
  assert.equal(core.setTrustedHtml(node, "<b>safe</b>"), false);
  assert.equal(htmlWrites, 1);
});

test("shared JSON fetch stays same-origin and carries secure defaults", async () => {
  const {core, requests} = loadCore();
  assert.deepEqual(await core.fetchJson("/api/status", {cache: "no-store"}), {ok: true});
  assert.equal(requests.length, 1);
  assert.equal(requests[0].input, "/api/status");
  assert.equal(requests[0].options.cache, "no-store");
  assert.equal(requests[0].options.credentials, "same-origin");
  assert.ok(requests[0].options.signal instanceof AbortSignal);
  await assert.rejects(
    core.fetchJson("https://attacker.example/collect"),
    /must stay on the same origin/,
  );
  assert.equal(requests.length, 1);
});

test("shared JSON reader rejects invalid roots and preserves API errors", async () => {
  const {core} = loadCore();
  assert.throws(() => core.validateJsonRoot([], "object"), /root must be an object/);
  assert.throws(() => core.validateJsonRoot({}, "array"), /root must be an array/);
  await assert.rejects(
    core.readJsonResponse({ok: false, status: 503, json: async () => ({error: "source waiting"})}, {expectedRoot: "object"}),
    /source waiting/,
  );
});

test("conditional refresh consumes 304 without parsing or recording an error", async () => {
  const {core} = loadCore({fetchImpl: async () => ({
    ok: false, status: 304, headers: {get: () => null},
    text: async () => "", json: async () => assert.fail("304 has no JSON body"),
  })});
  const response = await core.fetchWithTimeout("/data-monitor/api/features", {
    cache: "no-store", headers: {"If-None-Match": '"known"'},
  });
  assert.equal(response.status, 304);
  assert.equal(await core.readTextResponse(response), "");
  assert.equal(core.performanceSnapshot().at(-1).outcome, "ok");
});

test("latest-request guard cancels and invalidates superseded work", () => {
  const {core} = loadCore();
  const latest = core.createLatestRequest();
  const first = latest.begin();
  assert.equal(first.isCurrent(), true);
  const second = latest.begin();
  assert.equal(first.signal.aborted, true);
  assert.equal(first.isCurrent(), false);
  assert.equal(second.isCurrent(), true);
  second.finish();
  assert.equal(second.isCurrent(), false);
});

test("shared fetch propagates caller cancellation", async () => {
  const fetchImpl = (_input, options) => new Promise((_resolve, reject) => {
    options.signal.addEventListener("abort", () => reject(options.signal.reason), {once: true});
  });
  const {core} = loadCore({fetchImpl});
  const controller = new AbortController();
  const request = core.fetchWithTimeout("/api/status", {
    signal: controller.signal,
    timeoutMs: 1000,
  });
  controller.abort(new DOMException("superseded", "AbortError"));
  await assert.rejects(request, (error) => error?.name === "AbortError");
});

test("revision subscription uses SSE, validates messages, and releases hidden streams", () => {
  const {core, sandbox} = loadCore();
  const streams = [], received = [], listeners = new Map();
  sandbox.document.addEventListener = (name, fn) => {
    if (!listeners.has(name)) listeners.set(name, []);
    listeners.get(name).push(fn);
  };
  sandbox.EventSource = class {
    constructor(url) { this.url = url; streams.push(this); }
    addEventListener(name, fn) { this[name] = fn; }
    close() { this.closed = true; }
  };
  let fallbacks = 0;
  const subscription = core.subscribeRevisions("api/updates", (payload) => received.push(payload), () => { fallbacks++; });
  assert.equal(streams.length, 1);
  streams[0].onopen();
  streams[0].revision({data: '{"revision_token":"2"}'});
  streams[0].revision({data: "[]"});
  assert.equal(received.length, 1);
  assert.equal(received[0].revision_token, "2");
  assert.equal(fallbacks, 1);
  sandbox.document.hidden = true;
  listeners.get("visibilitychange").forEach((fn) => fn());
  assert.equal(streams[0].closed, true);
  sandbox.document.hidden = false;
  listeners.get("visibilitychange").forEach((fn) => fn());
  assert.equal(streams.length, 2);
  subscription.dispose();
  assert.equal(streams[1].closed, true);
  assert.throws(() => core.subscribeRevisions("https://evil.example/events", () => {}, () => {}), /same origin/);
});

test("revision subscription keeps polling when EventSource is unavailable", () => {
  const {core} = loadCore();
  let fallbacks = 0;
  const subscription = core.subscribeRevisions("api/updates", () => assert.fail("no stream"), () => { fallbacks++; });
  assert.equal(fallbacks, 1);
  subscription.dispose();
});

function stalledBodyFetch(_input, {signal}) {
  return Promise.resolve({ok: true, status: 200, json: () => new Promise((_resolve, reject) => {
    if (signal.aborted) reject(signal.reason);
    else signal.addEventListener("abort", () => reject(signal.reason), {once: true});
  })});
}

test("body timeout remains armed after headers arrive", async () => {
  const {core} = loadCore({fetchImpl: stalledBodyFetch});
  await assert.rejects(core.fetchJson("/api/status", {timeoutMs: 20}), e => e.name === "TimeoutError");
  assert.equal(core.performanceSnapshot().at(-1).outcome, "TimeoutError");
});

test("superseding a request aborts body consumption after headers", async () => {
  const {core} = loadCore({fetchImpl: stalledBodyFetch});
  const controller = new AbortController();
  const response = await core.fetchWithTimeout("/api/status", {signal: controller.signal});
  const pending = core.readJsonResponse(response);
  controller.abort(new DOMException("superseded", "AbortError"));
  await assert.rejects(pending, e => e.name === "AbortError");
});

test("JSON completion cleans up cancellation and captures bounded non-query metrics", async () => {
  let signal;
  const {core} = loadCore({fetchImpl: async (_input, options) => {
    signal = options.signal;
    return {ok: true, status: 200, json: async () => ({}), text: async () => '{"ok":true}'};
  }});
  const controller = new AbortController();
  for (let n=0; n<140; n++) await core.fetchJson("/api/status?private=secret", {signal: controller.signal});
  controller.abort();
  assert.equal(signal.aborted, false);
  const metrics = core.performanceSnapshot();
  assert.equal(metrics.length, 128);
  assert.equal(metrics[0].path, "/api/status");
  assert.ok(metrics[0].headersMs >= 0 && metrics[0].bodyMs >= 0 && metrics[0].parseMs >= 0);
  metrics[0].path = "changed";
  assert.equal(core.performanceSnapshot()[0].path, "/api/status");
});

test("same-origin protection also rejects URL objects and blocks redirects", async () => {
  const {core, requests} = loadCore();
  await assert.rejects(core.fetchJson(new URL("https://evil.example/")), /same origin/);
  await core.fetchJson(new URL("https://dashboard.example/api/status"));
  assert.equal(requests[0].options.redirect, "error");
});

test("browser performance history attributes actions without persisting private values", async () => {
  const stored = new Map();
  const localStorage = {
    getItem(key) { return stored.get(key) ?? null; },
    setItem(key, value) { stored.set(key, String(value)); },
    removeItem(key) { stored.delete(key); },
  };
  const {core, document} = loadCore({localStorage});
  const target = {
    tagName: "BUTTON",
    id: "force-refresh",
    value: "DO-NOT-STORE",
    dataset: {},
    getAttribute() { return null; },
    closest(selector) { return selector === "[data-performance-ignore]" ? null : this; },
  };
  document.dispatchEvent({type: "click", target, timeStamp: Date.now()});
  await core.fetchJson("/api/status?private=secret");
  await new Promise((resolve) => setTimeout(resolve, 250));

  const history = core.performanceHistorySnapshot();
  assert.ok(history.some((row) => row.kind === "interaction" && row.action === "button:force-refresh"));
  assert.ok(history.some((row) => row.kind === "api" && row.action === "button:force-refresh" && row.requestPath === "/api/status"));
  const serialized = JSON.stringify([...stored.values()]);
  assert.doesNotMatch(serialized, /DO-NOT-STORE|private=secret/);
  history[0].route = "/changed/";
  assert.notEqual(core.performanceHistorySnapshot()[0].route, "/changed/");

  core.clearPerformanceHistory();
  assert.equal(core.performanceHistorySnapshot().length, 0);
  assert.equal(stored.size, 0);
});

test("browser performance history bounds a noisy metric series", async () => {
  const {core} = loadCore();
  for (let index = 0; index < 40; index += 1) {
    await core.fetchJson("/api/status");
  }
  await new Promise((resolve) => setTimeout(resolve, 20));
  const apiRows = core.performanceHistorySnapshot().filter((row) => row.kind === "api");
  assert.equal(apiRows.length, 32);
  assert.equal(core.PERFORMANCE_HISTORY_LIMIT, 256);
  assert.equal(core.PERFORMANCE_SCHEMA_VERSION, 1);
});

test("keyboard control latency is measured without persisting the pressed key or field value", async () => {
  const {core, document} = loadCore();
  const target = {tagName: "BUTTON", id: "menu", dataset: {}, value: "DO-NOT-STORE",
    getAttribute() { return null; },
    closest(selector) { return selector === "[data-performance-ignore]" ? null : this; }};
  document.dispatchEvent({type: "keydown", key: "Escape", target, timeStamp: Date.now()});
  document.dispatchEvent({type: "keydown", key: "private-character", target, timeStamp: Date.now()});
  await new Promise(resolve => setTimeout(resolve, 10));
  const history = core.performanceHistorySnapshot();
  assert.equal(history.length, 1);
  assert.equal(history[0].eventType, "keydown");
  assert.equal(history[0].action, "button:menu");
  assert.equal("key" in history[0], false);
  assert.doesNotMatch(JSON.stringify(history), /DO-NOT-STORE|private-character|Escape/);
});

test("explicit chart rendering metrics preserve bounded phase and size evidence", () => {
  const {core} = loadCore();
  const metric = core.recordPerformanceMetric({
    kind: "render",
    action: "equity_chart",
    eventType: "history_update",
    durationMs: 22.1254,
    prepareMs: 15.4,
    drawMs: 6.7,
    paintMs: 31.2,
    pointCount: 300304,
    seriesCount: 8,
    privateValue: "must-not-survive",
  });
  assert.equal(metric.kind, "render");
  assert.equal(metric.action, "equity_chart");
  assert.equal(metric.durationMs, 22.125);
  assert.equal(metric.pointCount, 300304);
  assert.equal(metric.seriesCount, 8);
  assert.equal("privateValue" in metric, false);
  const recorded = core.performanceHistorySnapshot();
  assert.equal(recorded.length, 1);
  assert.equal(recorded[0].kind, metric.kind);
  assert.equal(recorded[0].pointCount, metric.pointCount);
});

test("missing performance observations stay unknown, while actual zeros and full point counts survive", () => {
  const {core} = loadCore();
  const metric = core.recordPerformanceMetric({
    kind: "api", action: "unknown_phases", durationMs: 0, headersMs: 0,
    serverMs: null, inputDelayMs: null, fcpMs: "", lcpMs: false,
    bodyMs: "   ", parseMs: [], paintMs: {}, pointCount: 1200000, seriesCount: 8,
  });
  for (const field of ["serverMs", "inputDelayMs", "fcpMs", "lcpMs", "bodyMs", "parseMs", "paintMs"]) {
    assert.equal(field in metric, false, `${field} is not an observation`);
  }
  assert.equal(metric.durationMs, 0);
  assert.equal(metric.headersMs, 0);
  assert.equal(metric.pointCount, 1200000);
});

test("API history without Server-Timing does not invent a zero server duration", async () => {
  const {core} = loadCore();
  await core.fetchJson("/api/status");
  await new Promise(resolve => setTimeout(resolve, 10));
  const metric = core.performanceHistorySnapshot().find(row => row.kind === "api");
  assert.ok(metric);
  assert.equal("serverMs" in metric, false);
  assert.equal(core.performanceSnapshot().at(-1).serverMs, null);
});

test("refresh scheduler catches synchronous errors and remains usable", async () => {
  const errors = [];
  let callback, calls = 0;
  const {core} = loadCore({globals: {
    setInterval(fn) { callback = fn; return 1; }, clearInterval() {},
  }});
  let scheduler;
  assert.doesNotThrow(() => {
    scheduler = core.scheduleRefresh(() => {
      calls += 1;
      if (calls === 1) throw new Error("synthetic refresh failure");
    }, {intervalMs: 1000, onError: error => errors.push(error.message)});
  });
  await new Promise(resolve => setTimeout(resolve, 0));
  await callback();
  assert.equal(calls, 2);
  assert.deepEqual(errors, ["synthetic refresh failure"]);
  scheduler.dispose();
});

test("refresh scheduler coalesces slow ticks without delaying the initial invocation", async () => {
  let tick, release, calls = 0, clears = 0;
  const {core} = loadCore({globals: {
    setInterval(fn) { tick = fn; return 1; }, clearInterval() { clears += 1; },
  }});
  const scheduler = core.scheduleRefresh(() => {
    calls += 1;
    return new Promise(resolve => { release = resolve; });
  }, {intervalMs: 1000});
  assert.equal(calls, 1);
  tick(); tick(); tick();
  assert.equal(calls, 1);
  release();
  await new Promise(resolve => setTimeout(resolve, 0));
  tick();
  assert.equal(calls, 2);
  release();
  scheduler.dispose();
  tick();
  assert.equal(calls, 2);
  assert.equal(clears, 1);
});

function responsiveFixture(rows = 100, columns = 4) {
  const counts = {reads: 0, writes: 0};
  const headers = Array.from({length: columns}, (_, i) => ({textContent: `Header ${i}`}));
  const table = {
    nodeType: 1, dataset: {}, classList: {contains() { return false; }, add() {}},
    matches(selector) { return selector === "table"; },
    closest(selector) { return selector === ".table-scroll,.table-wrap" ? {} : null; },
    querySelectorAll(selector) {
      if (selector === "thead th") return headers;
      if (selector === "tbody tr") return this.rows;
      return [];
    },
  };
  table.rows = Array.from({length: rows}, () => {
    const row = {
      nodeType: 1, tagName: "TR", closest(selector) {
        return selector === "table" ? table : selector === "tr" ? this : null;
      },
      matches(selector) { return selector === "tr"; }, querySelectorAll() { return []; },
    };
    row.children = Array.from({length: columns}, () => ({
      tagName: "TD", nodeType: 1, attributes: {},
      closest(selector) { return selector === "table" ? table : selector === "tr" ? row : null; },
      getAttribute(name) { counts.reads += 1; return this.attributes[name] ?? null; },
      setAttribute(name, value) { counts.writes += 1; this.attributes[name] = value; },
    }));
    return row;
  });
  return {table, headers, counts};
}

test("responsive labels are idempotent for unchanged table headers and rows", () => {
  const {core} = loadCore();
  const {table, counts} = responsiveFixture();
  assert.equal(core.enhanceResponsiveTable(table), true);
  assert.equal(counts.writes, 400);
  counts.writes = 0;
  core.enhanceResponsiveTable(table);
  assert.equal(counts.writes, 0);
});

test("a single-cell mutation visits only its responsive row, while a header change updates all rows", () => {
  let observe;
  const {core} = loadCore({
    globals: {MutationObserver: class {
      constructor(fn) { observe = fn; } observe() {}
    }},
    beforeInstall(sandbox) { sandbox.document.body = {}; },
  });
  const {table, headers, counts} = responsiveFixture();
  core.enhanceResponsiveTable(table);
  counts.reads = counts.writes = 0;
  const cell = table.rows[5].children[1];
  observe([{target: cell, addedNodes: [{nodeType: 3}]}]);
  assert.equal(counts.reads, 4);
  assert.equal(counts.writes, 0);
  counts.reads = counts.writes = 0;
  headers[0].textContent = "Changed header";
  const thead = {closest(selector) { return selector === "table" ? table : selector === "thead" ? this : null; }};
  observe([{target: thead, addedNodes: []}]);
  assert.equal(counts.reads, 400);
  assert.equal(counts.writes, 100);
  assert.equal(table.rows[99].children[0].attributes["data-label"], "Changed header");
});

test("responsive observer labels inserted rows without revisiting the existing tbody", () => {
  let observe;
  const {core} = loadCore({
    globals: {MutationObserver: class { constructor(fn) { observe = fn; } observe() {} }},
    beforeInstall(sandbox) { sandbox.document.body = {}; },
  });
  const {table, counts} = responsiveFixture();
  core.enhanceResponsiveTable(table);
  const inserted = table.rows[99];
  for (const cell of inserted.children) cell.attributes = {};
  counts.reads = counts.writes = 0;
  const tbody = {closest(selector) { return selector === "table" ? table : null; }};
  observe([{target: tbody, addedNodes: [inserted]}]);
  assert.equal(counts.reads, 4);
  assert.equal(counts.writes, 4);
});

test("one refresh queued by visibility resumes after the active request, and disposal cancels it", async () => {
  const {core, document} = loadCore({globals: {setInterval() { return 1; }, clearInterval() {}}});
  let calls = 0, release;
  const scheduler = core.scheduleRefresh(() => {
    calls += 1;
    return new Promise(resolve => { release = resolve; });
  }, {intervalMs: 1000});
  document.hidden = true;
  document.dispatchEvent({type: "visibilitychange"});
  document.hidden = false;
  document.dispatchEvent({type: "visibilitychange"});
  document.dispatchEvent({type: "visibilitychange"});
  assert.equal(calls, 1);
  release();
  await new Promise(resolve => setTimeout(resolve, 0));
  assert.equal(calls, 2);
  document.dispatchEvent({type: "visibilitychange"});
  scheduler.dispose();
  release();
  await new Promise(resolve => setTimeout(resolve, 0));
  assert.equal(calls, 2);
});

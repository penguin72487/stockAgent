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

function loadCore({current = "tw-day-trade", fetchImpl, localStorage} = {}) {
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
  };
  vm.createContext(sandbox);
  vm.runInContext(SOURCE, sandbox, {filename: "dashboard-core.js"});
  return {core: sandbox.StockAgentDashboard, document, listeners, nav, requests, sandbox};
}

test("shared navigation renders one canonical route list and current page", () => {
  const {core, nav} = loadCore({current: "tw-day-trade"});
  assert.equal(core.NAV_ITEMS.length, 8);
  assert.equal(nav.children.length, 8);
  assert.deepEqual(nav.children.map((link) => link.textContent), [
    "總覽", "TAIFEX", "台股當沖", "隔日沖", "永豐 API", "OpenBB", "全資料", "流量",
  ]);
  assert.equal(nav.children[0].href, "../");
  assert.equal(nav.children[2].href, "./");
  assert.equal(nav.children[2].attributes["aria-current"], "page");
  assert.equal(nav.children[6].href, "../data-monitor/");
  assert.equal(nav.dataset.dashboardNavMounted, "true");
  assert.ok(Object.isFrozen(core));
  assert.ok(Object.isFrozen(core.NAV_ITEMS));
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

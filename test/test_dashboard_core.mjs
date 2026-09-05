import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import vm from "node:vm";

const SOURCE = readFileSync(
  new URL("../services/public_dashboards/dashboard-core.js", import.meta.url),
  "utf8",
);

function fakeDocument(current = "tw-day-trade") {
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
    addEventListener() {},
    removeEventListener() {},
  };
  return {document, nav};
}

function loadCore({current = "tw-day-trade", fetchImpl} = {}) {
  const {document, nav} = fakeDocument(current);
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
    location: {href: "https://dashboard.example/tw-day-trade/", origin: "https://dashboard.example"},
    setInterval,
    setTimeout,
  };
  vm.createContext(sandbox);
  vm.runInContext(SOURCE, sandbox, {filename: "dashboard-core.js"});
  return {core: sandbox.StockAgentDashboard, nav, requests};
}

test("shared navigation renders one canonical route list and current page", () => {
  const {core, nav} = loadCore({current: "tw-day-trade"});
  assert.equal(core.NAV_ITEMS.length, 7);
  assert.equal(nav.children.length, 7);
  assert.deepEqual(nav.children.map((link) => link.textContent), [
    "總覽", "TAIFEX", "台股當沖", "永豐 API", "OpenBB", "全資料", "流量",
  ]);
  assert.equal(nav.children[0].href, "../");
  assert.equal(nav.children[2].href, "./");
  assert.equal(nav.children[2].attributes["aria-current"], "page");
  assert.equal(nav.children[5].href, "../data-monitor/");
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

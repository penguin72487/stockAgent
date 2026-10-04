import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import vm from "node:vm";

const core = readFileSync(new URL("../services/public_dashboards/dashboard-core.js", import.meta.url), "utf8");
const overview = readFileSync(new URL("../services/public_dashboards/public.js", import.meta.url), "utf8");

function loadOverview() {
  const nodes = new Map();
  const document = {
    hidden: true, documentElement: {dataset: {}},
    addEventListener() {}, removeEventListener() {}, querySelectorAll() { return []; },
    getElementById(id) {
      if (!nodes.has(id)) nodes.set(id, {textContent: "", className: "", lastChild: {textContent: ""}});
      return nodes.get(id);
    },
  };
  const sandbox = {document, URL, AbortController, DOMException, setTimeout, clearTimeout,
    setInterval() { return 1; }, clearInterval() {},
    location: {href: "https://dashboard.example/", origin: "https://dashboard.example", pathname: "/"}};
  sandbox.window = sandbox;
  vm.createContext(sandbox);
  vm.runInContext(core, sandbox);
  vm.runInContext(overview, sandbox);
  return {sandbox, value: id => nodes.get(id)?.textContent,
    health: prefix => nodes.get(`${prefix}-health`)?.lastChild.textContent};
}

test("overview preserves unknown TAIFEX coverage and actual zero strategies", () => {
  const {sandbox, value} = loadOverview();
  sandbox.renderTaifex({health: "blocked", live_strategies: 0, book_coverage_ratio: null});
  assert.match(value("taifex-summary"), /0.*策略/);
  assert.doesNotMatch(value("taifex-summary"), /0%/);
  sandbox.renderTaifex({health: "active", live_strategies: 2, book_coverage_ratio: 0});
  assert.match(value("taifex-summary"), /0%/);
});

test("overview traffic without latency samples does not display zero milliseconds", () => {
  const {sandbox, value} = loadOverview();
  sandbox.renderTraffic({requests_1m: 0, requests_per_second_1m: 0, latency_p95_ms_1m: null});
  assert.equal(value("traffic-latency"), "尚無樣本");
  sandbox.renderTraffic({requests_1m: 1, requests_per_second_1m: 1, latency_p95_ms_1m: 0});
  assert.equal(value("traffic-latency"), "0 ms");
});

test("overview does not turn unavailable quota and completion ratios into zero progress", () => {
  const {sandbox, value} = loadOverview();
  sandbox.renderShioaji({health: "waiting", traffic_used_ratio: null, progress_ratio: null});
  assert.doesNotMatch(value("shioaji-traffic"), /0%|0\.0%/);
  assert.doesNotMatch(value("shioaji-progress"), /0\.00%/);
  sandbox.renderOpenbb({health: "stopped", completion_percent: null, snapshot_state: null});
  assert.doesNotMatch(value("openbb-progress"), /0\.00%/);
  assert.doesNotMatch(value("openbb-freshness"), /快照逾時/);
});

test("quota freshness is not reported as complete FinLab data health or accepted from a future clock", () => {
  const {sandbox, health} = loadOverview();
  sandbox.renderFinlab({used_mb: 10, limit_mb: 100,
    quota_observed_at_utc: new Date(Date.now() - 1000).toISOString()});
  assert.match(health("finlab"), /配額.*觀測/);
  assert.doesNotMatch(health("finlab"), /資料正常/);
  sandbox.renderFinlab({used_mb: 10, limit_mb: 100,
    quota_observed_at_utc: new Date(Date.now() + 86400000).toISOString()});
  assert.match(health("finlab"), /待更新|待核實/);
});

test("a generic waiting state is not proof that the market is closed or the API quota exhausted", () => {
  const {sandbox, health} = loadOverview();
  sandbox.renderTw({health: "waiting", modes: 0, open_positions: 0});
  assert.match(health("tw"), /等待/);
  assert.doesNotMatch(health("tw"), /休市/);
  sandbox.renderShioaji({health: "waiting"});
  assert.doesNotMatch(health("shioaji"), /流量保護/);
});

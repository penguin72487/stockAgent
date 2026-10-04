"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

const appPath = path.resolve(__dirname, "../../services/tw_day_trade_dashboard/app.js");
const source = fs.readFileSync(appPath, "utf8");
const boot = source.indexOf('\n$("mode-filter").addEventListener');
assert.ok(boot > 0);

function loadApp(cancelledBySelection) {
  let controller;
  const elements = new Map();
  const dashboard = {
    byId(id) {
      if (!elements.has(id)) elements.set(id, {
        value: id.includes("date") ? "2026-09-24" : id === "symbol-filter" ? "" : "all",
        setAttribute() {},
      });
      return elements.get(id);
    },
    createFetch() { return async () => ({ok: true, json() {}}); },
    async readJsonResponse() {
      if (cancelledBySelection) controller.abort();
      // Browser body readers can throw AbortError even when the fetch
      // helper's private timeout controller caused the cancellation.
      throw new DOMException("The user aborted a request.", "AbortError");
    },
  };
  const context = vm.createContext({
    window: {
      location: {pathname: "/tw-day-trade/"},
      StockAgentDashboard: dashboard,
      StockAgentTwPresentation: {},
      StockAgentTwDetailComponents: {create: () => ({})},
      StockAgentTwChart: {},
    },
    document: {hidden: false}, URLSearchParams,
    AbortController: class extends AbortController {
      constructor() { super(); controller = this; }
    },
  });
  vm.runInContext(source.slice(0, boot), context, {filename: appPath});
  vm.runInContext(`
    snapshot = {service_sync: {content_revision: 1}};
    renderSignals = renderPositions = renderEvents = renderSignalFeaturePanel = renderChart = () => {};
  `, context);
  return context;
}

for (const [loader, errorField, loadingField] of [
  ["loadSignals", "signalLoadError", "signalLoading"],
  ["loadPositions", "positionLoadError", "positionLoading"],
  ["loadEvents", "eventLoadError", "eventLoading"],
  ["loadChartHistory", "historyLoadError", "historyInFlight"],
]) {
  for (const cancelled of [false, true]) {
    test(`${loader}: ${cancelled ? "superseded selection is silent" : "body timeout is not silent cancellation"}`, async () => {
      const context = loadApp(cancelled);
      await vm.runInContext(`${loader}()`, context);
      assert.equal(Boolean(vm.runInContext(errorField, context)), !cancelled);
      assert.equal(vm.runInContext(loadingField, context), false);
    });
  }
}

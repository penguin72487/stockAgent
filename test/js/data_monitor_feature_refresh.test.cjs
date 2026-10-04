"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

const appPath = path.resolve(__dirname, "../../services/data_monitor_dashboard/app.js");

test("feature pages stay viewport-bounded, searchable, and preserve expanded rows", async () => {
  const app = fs.readFileSync(appPath, "utf8");
  const boot = app.indexOf('\nfor (const id of ["search",');
  assert.ok(boot > 0, "application boot boundary must be found");
  let now = 1_000_000;
  let requests = 0;
  let failures = 0;
  let revision = "a".repeat(32);
  let observer;
  const urls = [];
  const elements = new Map();
  const sourceRows = Array.from({length: 200}, (_, index) => ({
    field: `field-${index}`, dataset_id: "sample", market_category: "taiwan_equity",
    source_title: "Sample", provider: "TWSE", market_category_label: "台股",
  }));
  const dashboard = {
    byId(id) {
      if (!elements.has(id)) elements.set(id, {value: id === "feature-search" ? "" : "all"});
      return elements.get(id);
    },
    createJsonFetcher() { return async () => ({}); },
    async fetchWithTimeout(url) {
      requests += 1;
      urls.push(url);
      if (failures) { failures -= 1; throw new Error("temporary network failure"); }
      const params = new URLSearchParams(url.split("?")[1]);
      const source = params.get("source");
      const query = params.get("q") || "";
      const filtered = source !== "all" ? [] :
        query ? sourceRows.filter((row) => row.field.includes(query)) : sourceRows;
      const offset = Number(params.get("offset"));
      const limit = Number(params.get("limit"));
      const resetRequired = params.has("revision") && params.get("revision") !== revision;
      const responseOffset = resetRequired ? 0 : offset;
      return {payload: {
        schema_version: 1, read_only: true, production_control_possible: false,
        revision, reset_required: resetRequired,
        offset: responseOffset,
        limit, matching_total: filtered.length,
        rows: filtered.slice(responseOffset, responseOffset + limit),
        summary: {fields: sourceRows.length},
        filters: {categories: [{id: "taiwan_equity", label: "台股"}],
          sources: [{id: "sample", label: "Sample · TWSE"}]},
      }};
    },
    async readJsonResponse(response) { return response.payload; },
  };
  class Observer {
    constructor(callback) { this.callback = callback; observer = this; }
    observe() {}
  }
  const context = vm.createContext({
    window: {StockAgentDashboard: dashboard, IntersectionObserver: Observer, innerWidth: 1200},
    document: {hidden: false}, IntersectionObserver: Observer,
    performance: {now: () => now}, Option: class {}, URLSearchParams,
  });
  vm.runInContext(app.slice(0, boot), context, {filename: appPath});
  let renderCount = 0;
  context.captureRender = () => { renderCount += 1; };
  vm.runInContext("renderFeatures = captureRender; populateFeatureFilters = () => {};", context);
  const state = vm.runInContext("state", context);

  vm.runInContext("installFeatureActivation()", context);
  assert.equal(requests, 0);
  observer.callback([{isIntersecting: true}]);
  await new Promise(setImmediate);
  assert.equal(requests, 1);
  assert.match(urls[0], /api\/features\/page\?/);
  assert.equal(state.featureRows.length, 80);
  assert.equal(state.featureMatchingTotal, 200);
  assert.equal(renderCount, 1);

  await vm.runInContext("refreshFeatures({force:true,append:true})", context);
  assert.equal(state.featureRows.length, 160);
  assert.equal(state.featureVisible, 160);
  assert.match(urls[1], /offset=80/);
  observer.callback([{isIntersecting: false}]);
  now += 61_000;
  await vm.runInContext("refreshFeatures()", context);
  assert.equal(requests, 2, "hidden feature list must not poll");
  observer.callback([{isIntersecting: true}]);
  await new Promise(setImmediate);
  assert.equal(requests, 3, "re-entry catches up after refresh interval");
  assert.equal(state.featureRows.length, 160);
  assert.equal(state.featureVisible, 160);

  revision = "b".repeat(32);
  now += 61_000;
  await vm.runInContext("refreshFeatures()", context);
  assert.equal(requests, 4, "new snapshot generation must be fetched");
  assert.equal(state.featureRows.length, 160);
  assert.equal(state.featureVisible, 160, "ordinary refresh preserves the expanded row count");

  failures = 1;
  now += 61_000;
  await vm.runInContext("refreshFeatures()", context);
  assert.equal(requests, 5);
  now += 9_000;
  await vm.runInContext("refreshFeatures()", context);
  assert.equal(requests, 5, "failed refresh is briefly backed off");
  now += 1_000;
  await vm.runInContext("refreshFeatures()", context);
  assert.equal(requests, 6);
  assert.equal(state.featureVisible, 160);

  dashboard.byId("feature-source").value = "other";
  await vm.runInContext("refreshFeatures({force:true})", context);
  assert.equal(state.featureMatchingTotal, 0);
  assert.equal(state.featureRows.length, 0);
  assert.equal(state.featureSummary.fields, 200, "empty filter must not erase full coverage");
  assert.match(urls.at(-1), /source=other/);

  context.document.hidden = true;
  now += 61_000;
  await vm.runInContext("refreshFeatures()", context);
  assert.equal(requests, 7, "background browser tab must not poll");
});

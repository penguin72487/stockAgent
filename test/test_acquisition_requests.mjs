import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import vm from "node:vm";

// Coordinator tests deliberately replace renderers. Real DOM, disclosures,
// source parity and request visibility are covered by browser acceptance.
function application(directory, boot) {
  const nodes = new Map(), requests = [], rendered = [];
  const byId = id => {
    if (!nodes.has(id)) nodes.set(id, {value: id.includes("filter") || id === "feature-category"
      || id === "feature-source" ? "all" : id === "history-range" ? "24h" : "",
      textContent: "", hidden:false, lastChild:{textContent:""}, replaceChildren() {},
      setAttribute() {}, removeAttribute() {}});
    return nodes.get(id);
  };
  const request = (url, options) => new Promise((resolve, reject) => requests.push({url, options, resolve, reject}));
  let readCount = 0;
  const context = vm.createContext({
    window: {innerWidth:1366, StockAgentDashboard: {
      byId, createJsonFetcher:() => request, fetchWithTimeout:request,
      async readJsonResponse(response) { readCount++; if (response.ok === false) throw new Error("HTTP failed"); return response; },
      finiteNumber:value => value == null ? null : Number(value),
      createLatestRequest() {
        let sequence = 0, controller = null;
        return {begin() {
          controller?.abort(); controller = new AbortController();
          const current = ++sequence, own = controller;
          return {signal:own.signal, isCurrent:() => current === sequence && controller === own,
            finish() {if (controller === own) controller = null;}};
        }, abort() {sequence++; controller?.abort(); controller = null;}};
      }, isElementVisible:() => context.visible !== false,
    }}, document:{hidden:false}, Intl, Date, URLSearchParams, AbortController,
    performance:{now:() => Date.now()}, console,
  });
  const source = readFileSync(new URL(`../services/${directory}/app.js`, import.meta.url), "utf8");
  if (directory === 'data_monitor_dashboard') {
    context.globalThis = context.window;
    for (const file of ['feature-page-constraints.js','feature-page-contract.js']) {
      vm.runInContext(readFileSync(new URL(`../services/public_dashboards/${file}`, import.meta.url),'utf8'),context);
    }
  }
  const marker = source.indexOf(boot);
  assert.ok(marker > 0, "real bootstrap boundary must exist");
  vm.runInContext(source.slice(0, marker), context);
  context.record = value => rendered.push(value);
  return {context, byId, requests, rendered, run:source => vm.runInContext(source, context), reads:() => readCount};
}

function provider() {
  // Provider is a second entrypoint in the canonical data-monitor app.
  const source = readFileSync(new URL("../services/data_monitor_dashboard/provider.js", import.meta.url), "utf8");
  const d = application("openbb_archive_dashboard", '\nfor (const button of document.querySelectorAll("[data-range]"))');
  const base = d.context.window.StockAgentDashboard;
  const context = vm.createContext({window:{StockAgentDashboard:base},document:{hidden:false},
    Intl, Date, URLSearchParams, AbortController, record:d.context.record});
  vm.runInContext(source.slice(0, source.indexOf("\nconst pathMatch")), context);
  vm.runInContext('state.provider="P"; renderHeadline=record; renderMarkets=()=>{}; renderRows=()=>{}; refreshQuota=async()=>{};', context);
  return {...d, context, run:source => vm.runInContext(source, context)};
}

function providerPage(offset=0, limit=30, generation="one", total=61) {
  const sources = Array.from({length:Math.min(limit, Math.max(0,total-offset))}, (_, i) => ({id:`row-${offset+i}`}));
  return {provider:"P", read_only:true, production_control_possible:false, generated_at_utc:generation,
    summary:{}, sources, page:{offset, limit, matched_total:total, has_more:offset+sources.length<total}};
}

test("provider pagination validates identity, bounds, safety flags and duplicate IDs", async () => {
  for (const patch of [{provider:"another"}, {read_only:false}, {production_control_possible:true},
    {page:{offset:30, limit:30, matched_total:61, has_more:true}},
    {sources:Array.from({length:30}, () => ({id:"duplicate"}))}]) {
    const d=provider(), pending=d.run("refresh()");
    d.requests[0].resolve({...providerPage(), ...patch}); await pending;
    assert.equal(d.rendered.length, 0);
    assert.equal(d.run("state.loaded"), false);
  }
});

test("provider generation changes refresh the loaded prefix instead of dropping back to page one", async () => {
  const d=provider();
  let pending=d.run("refresh()"); d.requests[0].resolve(providerPage()); await pending;
  pending=d.run("refresh({append:true})"); d.requests[1].resolve(providerPage(30)); await pending;
  assert.equal(d.run("state.rows.length"), 60);
  pending=d.run("refresh({append:true})"); d.requests[2].resolve(providerPage(60,30,"two")); await pending;
  assert.equal(d.requests.length, 4);
  const query = new URLSearchParams(d.requests[3].url.split("?")[1]);
  assert.equal(query.get("offset"), "0"); assert.equal(query.get("limit"), "60");
  d.requests[3].resolve(providerPage(0,60,"two")); await new Promise(setImmediate);
  assert.equal(d.run("state.rows.length"), 60);
  assert.equal(d.run("state.snapshotAt"), "two");
});

test("provider responses and errors cannot commit to a newly selected scope before debounce", async () => {
  for (const failure of [false,true]) {
    const d=provider(), pending=d.run("refresh()");
    d.byId("provider-source-search").value="changed";
    if (failure) d.requests[0].reject(new Error("old request failed"));
    else d.requests[0].resolve(providerPage());
    await pending;
    assert.equal(d.rendered.length, 0);
    assert.equal(d.byId("provider-error").textContent, "");
    if (!failure) {
      d.requests[1].resolve(providerPage(0,30,"one",0)); await new Promise(setImmediate);
      assert.equal(d.rendered.length, 1);
    }
  }
});

test("OpenBB loads history only when visible, preserves same-range success and clears a changed range", async () => {
  const d=application("openbb_archive_dashboard", '\nfor (const button of document.querySelectorAll("[data-range]"))');
  d.run("renderChart=record"); d.context.visible=false;
  await d.run("loadHistory()"); assert.equal(d.requests.length, 0);
  d.context.visible=true;
  let pending=d.run("loadHistory()"); d.requests[0].resolve({history:[{checked_at:"one"}]}); await pending;
  assert.equal(d.rendered.at(-1)[0].checked_at, "one");
  pending=d.run("loadHistory()"); d.requests[1].reject(new Error("temporary failure")); await pending;
  assert.equal(d.rendered.at(-1)[0].checked_at, "one");
  assert.match(d.byId("history-status").textContent, /保留同範圍/);
  pending=d.run('range="all"; loadHistory()');
  assert.equal(d.rendered.at(-1).length, 0);
  d.requests[2].reject(new Error("temporary failure")); await pending;
  assert.equal(d.rendered.at(-1).length, 0);
  assert.match(d.byId("chart-empty").textContent, /失敗不等於來源沒有資料/);
});

test("OpenBB slow history does not block summary refresh completion", async () => {
  const d=application("openbb_archive_dashboard", '\nfor (const button of document.querySelectorAll("[data-range]"))');
  d.run("renderStatus=record;renderChart=()=>{}");
  const pending=d.run("refresh()");
  d.requests[0].resolve({health:"active"}); await pending;
  assert.equal(d.run("refreshInFlight"), false);
  assert.equal(d.rendered[0].health, "active");
  d.requests[1].resolve({history:[]}); await new Promise(setImmediate);
});

test("traffic consumes non-2xx bodies and ignores superseded history errors", async () => {
  const d=application("traffic_dashboard", '\nrenderChart = Dashboard.createDeferredRenderer');
  d.run("renderHistory=record");
  const old=d.run("refreshHistory()"), current=d.run('refreshHistory({manual:true})');
  d.requests[1].resolve({trend:[]}); await current;
  const label=d.byId("history-status").textContent;
  d.requests[0].resolve({ok:false}); await old;
  assert.equal(d.reads(), 2);
  assert.equal(d.byId("history-status").textContent, label);
  d.context.visible=false; await d.run("refreshHistory()");
  assert.equal(d.requests.length, 2);
});

test("data-monitor rejects mismatched envelopes and responses from an obsolete filter", async () => {
  const d=application("data_monitor_dashboard", '\nfor (const id of ["search",');
  d.run("state.featureActivated=true;state.featureInView=true;renderFeatures=record;populateFeatureFilters=()=>{}");
  const payload = {schema_version:1, generated_at_utc:null, read_only:true, production_control_possible:false, rows:[], filters:{}, summary:{},
    matching_total:0, offset:0, limit:80, has_more:false, reset_required:false, revision:"a".repeat(32)};
  let pending=d.run("refreshFeatures({force:true})");
  d.requests[0].resolve({...payload, production_control_possible:true}); await pending;
  assert.equal(d.run("state.featureRevision"), null);
  pending=d.run("refreshFeatures({force:true})");
  d.byId("feature-search").value="new";
  d.requests[1].resolve(payload); await pending;
  assert.equal(d.run("state.featureRevision"), null);
  pending=d.run("refreshFeatures({force:true})");
  d.requests[2].resolve({...payload, offset:80}); await pending;
  assert.equal(d.run("state.featureRevision"), null);
  pending=d.run("refreshFeatures({force:true})");
  d.requests[3].resolve(payload); await pending;
  assert.equal(d.run("state.featureRevision"), payload.revision);
});

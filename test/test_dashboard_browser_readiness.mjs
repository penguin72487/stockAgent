import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import test from "node:test";

const source = fs.readFileSync(new URL("../scripts/audit_public_dashboards_browser.mjs", import.meta.url), "utf8");
const helper = source.slice(source.indexOf("async function waitUntilReady("), source.indexOf("const expression ="));
function readiness() {
  let time = 0;
  const context = {Date: {now: () => time}, setTimeout: (callback, ms) => {time += ms; callback();}};
  vm.runInNewContext(helper, context);
  return context.waitUntilReady;
}

test("continuous live polling cannot starve initial API and paint acceptance", async () => {
  const pending = new Map([["initial", "/api/status"]]);
  let checks = 0, paints = 0;
  const send = async (_method, params) => {
    if (params.awaitPromise) { paints += 1; return {}; }
    checks += 1;
    if (checks > 1) pending.delete("initial");
    if (checks > 1) pending.delete(`poll-${checks - 1}`);
    pending.set(`poll-${checks}`, "/api/status");
    return {result: {value: {state: "complete", requiredApiReady: checks > 1}}};
  };
  await readiness()(send, pending, {requiredApiPath: "/api/status"});
  assert.equal(checks, 2);
  assert.equal(paints, 1);
  assert.equal(pending.size, 1, "ongoing polls remain active");
});

test("an unresolved initial API still times out instead of claiming readiness", async () => {
  const pending = new Map([["initial", "/api/signals"]]);
  const send = async () => ({result: {value: {state: "complete", requiredApiReady: true}}});
  await assert.rejects(readiness()(send, pending), /pending APIs: \/api\/signals/);
});

test("a failed or canceled required status is not successful page acceptance", async () => {
  const pending = new Map();
  const send = async () => ({result: {value: {state: "complete", requiredApiReady: false}}});
  await assert.rejects(readiness()(send, pending, {requiredApiPath: "/api/status"}), /Page readiness deadline/);
});

test("document loading cannot pass merely because the network is idle", async () => {
  const send = async () => ({result: {value: {state: "loading", requiredApiReady: true}}});
  await assert.rejects(readiness()(send, new Map()), /Page readiness deadline/);
});

const modeSource = fs.readFileSync(new URL("../scripts/benchmark_dashboard_browser.mjs", import.meta.url), "utf8");
const modeHelper = modeSource.slice(modeSource.indexOf("async function revealModeCardsForObservation("),
  modeSource.indexOf("\ntry {\n  for (const name"));
function modeObservation({hidden = false, exists = true, disconnected = false, visible = true,
    key = "a".repeat(32), loseOnFrame = null} = {}) {
  let frames = 0;
  const calls = [], card = {isConnected:!disconnected, dataset:{publicSignalKey:key},
    scrollIntoView: options => calls.push({scroll:options}),
    getBoundingClientRect: () => ({toJSON: () => ({top:100, bottom:500})})};
  const context = {document:{hidden, querySelector:() => exists ? card : null},
    window:{scrollY:2500}, innerWidth:1366, innerHeight:768,
    StockAgentDashboard:{isElementVisible:node => node === card && visible && frames >= 2},
    requestAnimationFrame: callback => {
      frames++;
      if (frames === loseOnFrame) card.isConnected = false;
      callback();
    },
    performance:{now:() => frames * 16}, render:options => calls.push({render:options})};
  vm.runInNewContext(modeHelper, context);
  return {run:context.revealModeCardsForObservation, calls, getFrames:() => frames};
}

test("mode paint probe observes an actually visible card in a separate instant-scroll phase", async () => {
  const {run, calls, getFrames} = modeObservation();
  const result = await run();
  assert.equal(calls[0].scroll.behavior, "instant");
  assert.equal(calls[0].scroll.block, "center");
  assert.equal(calls.length, 2);
  assert.equal(calls[1].render.heavy, false, "do not rebuild or fetch to manufacture readiness");
  assert.equal(getFrames(), 4);
  assert.equal(result.startedAtMs, 0);
  assert.equal(result.visibleAtMs, 32);
  assert.equal(result.completedAtMs, 64);
  assert.match(result.boundary, /not initial paint/);
  assert.match(result.boundary, /full six-mode display/);
});

test("hidden, missing, invalid, offscreen and disconnected cards cannot fabricate paint proof", async () => {
  for (const options of [{hidden:true}, {exists:false}, {key:"not-a-public-identity"},
    {visible:false}, {disconnected:true}, {loseOnFrame:4}]) {
    await assert.rejects(modeObservation(options).run(), /foreground|identity|visible/);
  }
});

function scheduledModeObservation({appearAt = Infinity, key = "b".repeat(32), hidden = false} = {}) {
  let elapsed = 0, reveals = 0, scrolls = 0;
  const container = {scrollIntoView:() => {scrolls++;}};
  const card = {dataset:{publicSignalKey:key}};
  const context = {
    document:{hidden, querySelector:selector => selector === "#mode-cards" ? container :
      elapsed >= appearAt ? card : null},
    performance:{now:() => elapsed},
    setTimeout:(callback, ms) => {elapsed += ms; callback();},
  };
  vm.runInNewContext(modeHelper, context);
  return {run:() => context.watchModeCardsForObservation(500,
      async () => {reveals++; return {signalKey:key};}),
    counts:() => ({reveals, scrolls})};
}

test("opening watch arms before a signal and observes only an eventual real identity", async () => {
  const probe = scheduledModeObservation({appearAt:200});
  const result = await probe.run();
  assert.equal(result.state, "observed");
  assert.equal(result.waitMs, 200);
  assert.equal(result.signalKey, "b".repeat(32));
  assert.equal(probe.counts().reveals, 1);
  assert.equal(probe.counts().scrolls, 1);
});

test("opening watch retains unavailable evidence when identity never arrives", async () => {
  for (const options of [{}, {appearAt:0, key:"invalid"}, {hidden:true}]) {
    const probe = scheduledModeObservation(options);
    const result = await probe.run();
    assert.equal(result.state, "unavailable");
    assert.equal(probe.counts().reveals, 0);
  }
});

test("viewport failure remains a failed canonical browser probe and never bypasses identity verification", () => {
  assert.match(modeSource, /stage = "mode_viewport_observation"/);
  assert.match(modeSource, /paintVerification\.state!=='observed'/);
  assert.match(modeSource, /modeViewportObservation,fullHistory/);
});

const detailHelper = modeSource.slice(modeSource.indexOf("async function observeScopedDetails("),
  modeSource.indexOf("\ntry {\n  for (const name"));
function detailObservation(overrides = {}, onWait = () => {}) {
  let elapsed = 0;
  const context = {
    performance: {now: () => elapsed},
    setTimeout: callback => { elapsed += 100; onWait(context, elapsed); callback(); },
    detailRangeKey: () => "2026-01-02|2026-10-22",
    detailSelectionKey: kind => `${kind}:range`, detailDataRevision: kind => `${kind}:revision`,
    signalRows: [{symbol:"2330"}], signalTotal: 1, signalLoading: false, signalLoadError: "",
    signalSelectionKey: "signals:range", signalDataRevision: "signals:revision",
    positionRows: [], positionTotal: 0, positionLoading: false, positionLoadError: "",
    positionSelectionKey: "positions:range", positionDataRevision: "positions:revision",
    ...overrides,
  };
  vm.runInNewContext(detailHelper, context);
  return options => context.observeScopedDetails({timeoutMs:250, ...options});
}

test("range acceptance waits for both independent current-scope detail pages", async () => {
  const run = detailObservation({signalRows:[], signalLoading:true, signalSelectionKey:null}, (state, elapsed) => {
    if (elapsed === 200) Object.assign(state, {signalRows:[{symbol:"2330"}],
      signalLoading:false, signalSelectionKey:"signals:range"});
  });
  const result = await run({minimumSignals:1});
  assert.equal(result.waitMs, 200);
  assert.equal(result.signals.rows, 1);
  assert.match(result.boundary, /not all pages/);
});

test("cleared, stale, pending and failed pages cannot masquerade as successful browser acceptance", async () => {
  for (const options of [
    {signalRows:[], signalTotal:0, signalSelectionKey:null},
    {signalDataRevision:"old-revision"}, {signalLoading:true},
    {positionLoading:true}, {positionSelectionKey:"old-range"},
  ]) await assert.rejects(detailObservation(options)(), /scoped details deadline/);
  await assert.rejects(detailObservation({signalLoadError:"timeout"})(), /scoped signals failed: timeout/);
  await assert.rejects(detailObservation({positionLoadError:"incomplete page"})(), /scoped positions failed/);
});

test("an accepted empty page is legitimate but expanding a nonempty range must not lose all signals", async () => {
  const run = detailObservation({signalRows:[], signalTotal:0});
  assert.equal((await run()).signals.rows, 0);
  await assert.rejects(run({minimumSignals:1}), /expanded range lost previously observed signals/);
});

test("both initial and expanded-range manual probes require scoped detail completion", () => {
  assert.match(modeSource, /const initialDetails = watchMs \?/);
  assert.match(modeSource, /const rangeDetails = watchMs \?/);
  assert.equal(modeSource.match(/observeScopedDetails\.toString\(\)/g).length, 2);
  assert.match(modeSource, /initial,signalAndChartReadyMs,initialDetails,modeViewportObservation,fullHistory/);
});

const networkHelper = modeSource.slice(modeSource.indexOf("function captureApiNetworkEvent("),
  modeSource.indexOf("async function revealModeCardsForObservation("));
function networkRecorder() {
  const context = {URL, URLSearchParams};
  vm.runInNewContext(networkHelper, context);
  const requests = new Map();
  return {requests, record:(method, params, maximum) => context.captureApiNetworkEvent(
    {method, params}, requests, "https://example.test", maximum)};
}

test("API timing distinguishes server, response, download and timeout without recording secrets", () => {
  const {requests, record} = networkRecorder();
  record("Network.requestWillBeSent", {requestId:"signal", request:{url:"https://example.test/tw-day-trade/api/signals?offset=0&revision_scope=latest&token=secret", method:"GET", headers:{Authorization:"secret"}}, timestamp:10, wallTime:20, type:"Fetch"});
  record("Network.responseReceived", {requestId:"signal", timestamp:11,
    response:{status:200, protocol:"h2", headers:{"Server-Timing":"app;dur=200", "Set-Cookie":"secret", "Content-Length":"42"}, timing:{receiveHeadersEnd:200}}});
  record("Network.loadingFinished", {requestId:"signal", timestamp:12, encodedDataLength:42});
  const entry = requests.get("signal");
  assert.equal(entry.startSeconds, 10);
  assert.equal(entry.responseSeconds, 11);
  assert.equal(entry.finishedSeconds, 12);
  assert.equal(entry.headers["server-timing"], "app;dur=200");
  assert.equal(new URL(entry.path,"https://example.test").searchParams.get("revision_scope"),"latest");
  assert.equal(JSON.stringify(entry).includes("secret"), false);
  record("Network.loadingFailed", {requestId:"signal", timestamp:25, errorText:"net::ERR_ABORTED", canceled:true});
  assert.equal(entry.failedSeconds, 25);
  assert.equal(entry.canceled, true);
});

test("API trace is origin scoped and bounded, without mistaking dropped requests for complete coverage", () => {
  const {requests, record} = networkRecorder();
  const start = (requestId, address, maximum) => record("Network.requestWillBeSent", {
    requestId, request:{url:address, method:"GET"}, timestamp:1}, maximum);
  assert.equal(start("external", "https://elsewhere.test/api/private"), 0);
  assert.equal(start("asset", "https://example.test/app.js"), 0);
  assert.equal(requests.size, 0);
  start("first", "https://example.test/api/status", 1);
  assert.equal(start("second", "https://example.test/api/signals", 1), 1);
  record("Network.loadingFinished", {requestId:"first", timestamp:2, encodedDataLength:100});
  assert.equal(requests.get("first").finishedSeconds, 2);
  assert.equal(requests.size, 1);
});

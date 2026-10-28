#!/usr/bin/env node
// Read-only real-browser acceptance. Optional full-history selection changes
// only this temporary tab, never the engine, receipts, or execution ledger.
import fs from "node:fs";

const port = Number(process.argv[2] || 9224);
const url = process.argv[3] || "https://penguin72487.dpdns.org/tw-day-trade/";
const output = process.argv[4];
const watchOption = process.argv.slice(5).find(value => value.startsWith("--watch-ms="));
const watchMs = watchOption ? Number(watchOption.split("=")[1]) : 0;
if (!Number.isSafeInteger(watchMs) || watchMs < 0 || watchMs > 900000) throw new Error("watch-ms must be 0-900000");
if (output && (fs.existsSync(output) || fs.existsSync(`${output}.updates.jsonl`))) throw new Error("benchmark receipts must be new");
if (output) fs.writeFileSync(`${output}.updates.jsonl`, "", {flag:"wx"});
const target = await (await fetch(`http://127.0.0.1:${port}/json/new?about:blank`, {method:"PUT"})).json();
const socket = new WebSocket(target.webSocketDebuggerUrl);
await new Promise((resolve) => socket.addEventListener("open", resolve, {once:true}));
let id = 0;
const pending = new Map(), errors = [], messages = [], revisionPaint = [];
const apiNetwork = new Map();
const pageOrigin = new URL(url).origin;
let droppedApiRequests = 0;
let droppedPaintSamples = 0;
let mainFrame = null, stage = "connect";
// Node and the local paper engine use the same host wall clock. The paired
// monotonic brackets expose clock steps; these samples do not assert that the
// host clock is synchronized with the exchange or remote browser clients.
function observerClockSample() {
  const monotonicBeforeNs = process.hrtime.bigint();
  const wallUnixMs = Date.now();
  const monotonicAfterNs = process.hrtime.bigint();
  return {wallUnixMs, monotonicBeforeNs:String(monotonicBeforeNs),
    monotonicAfterNs:String(monotonicAfterNs)};
}
const appendClockAnchor = () => {
  if (output) fs.appendFileSync(`${output}.updates.jsonl`, JSON.stringify({
    contract:"observer_clock_anchor_v1", ...observerClockSample()})+"\n");
};
if (output && watchMs) appendClockAnchor();
const clockTimer = output && watchMs ? setInterval(() => {
  try { appendClockAnchor(); }
  catch (error) { errors.push({stage:"observer_clock_anchor",error:String(error)}); clearInterval(clockTimer); }
}, 1000) : null;
clockTimer?.unref();
socket.addEventListener("message", (event) => {
  const message = JSON.parse(event.data);
  droppedApiRequests += captureApiNetworkEvent(message, apiNetwork, pageOrigin);
  if (message.id) {
    pending.get(message.id)?.(message);
    pending.delete(message.id);
  } else if (message.method === "Runtime.exceptionThrown") errors.push(message.params.exceptionDetails.text);
  else if (message.method === "Runtime.bindingCalled" && message.params.name === "stockagentBenchmarkPaint") {
    try {
      const metric = JSON.parse(message.params.payload);
      if (metric.eventType !== "revision_applied" || !metric.correlation) return;
      const observerClock = observerClockSample();
      const sample = {browserRecord:metric,receivedAtUnixMs:observerClock.wallUnixMs,observerClock};
      if (revisionPaint.length < 512) revisionPaint.push(sample); else droppedPaintSamples += 1;
      if (output) fs.appendFileSync(`${output}.updates.jsonl`, JSON.stringify(sample)+"\n");
    } catch (error) { errors.push({stage:"paint_binding",error:String(error)}); }
  }
  else if (message.method === "Page.frameNavigated" && !message.params.frame.parentId) mainFrame = message.params.frame;
  else if (message.method === "Network.responseReceived" && message.params.response.status >= 400) errors.push({url:message.params.response.url,status:message.params.response.status});
  else if (message.method === "Network.eventSourceMessageReceived") messages.push({receivedAt:Date.now(),data:JSON.parse(message.params.data)});
});
const send = (method, params = {}) => new Promise((resolve) => {
  pending.set(++id, resolve);
  socket.send(JSON.stringify({id,method,params}));
});
const evaluate = async (expression, awaitPromise = false) => {
  const reply = await send("Runtime.evaluate", {expression, returnByValue:true, awaitPromise});
  if (reply.error || reply.result?.exceptionDetails) throw new Error(JSON.stringify(reply));
  return reply.result.result.value;
};
const waitForDocument = async () => {
  const deadline = Date.now() + 60000;
  while (Date.now() < deadline) {
    const reply = await send("Runtime.evaluate", {expression:"({href:location.href,state:document.readyState})",returnByValue:true});
    const document = reply.result?.result?.value;
    // Page.navigate acknowledges the navigation, not its final document.
    // A real WWW/HTTPS redirect may replace the execution context. Poll CDP
    // outside the page so no readiness promise remains in about:blank.
    if (mainFrame && document?.href === mainFrame.url && document.href !== "about:blank"
        && ["interactive", "complete"].includes(document.state)) return document;
    await new Promise(resolve=>setTimeout(resolve,50));
  }
  throw new Error("navigation document deadline");
};

function captureApiNetworkEvent(message, requests, origin, maximum = 4096) {
  const params = message.params || {};
  if (message.method === "Network.requestWillBeSent") {
    const address = new URL(params.request.url);
    if (address.origin !== origin || !address.pathname.includes("/api/")) return 0;
    if (!requests.has(params.requestId) && requests.size >= maximum) return 1;
    const prior = requests.get(params.requestId);
    const query = new URLSearchParams();
    for (const [key, value] of address.searchParams) {
      if (["date", "start_date", "end_date", "mode", "symbol", "status", "hide_zero_targets",
        "offset", "limit", "range", "resolution", "encoding"].includes(key)) query.append(key, value);
    }
    requests.set(params.requestId, {requestId:params.requestId, path:address.pathname + (query.size ? `?${query}` : ""),
      method:params.request.method, type:params.type, startSeconds:params.timestamp,
      wallTimeSeconds:params.wallTime, redirects:(prior?.redirects || 0) + (prior ? 1 : 0)});
  } else if (requests.has(params.requestId)) {
    const entry = requests.get(params.requestId);
    if (message.method === "Network.responseReceived") {
      const response = params.response;
      const headers = {};
      for (const [key, value] of Object.entries(response.headers || {})) {
        const name = key.toLowerCase();
        if (["server-timing", "content-length", "content-encoding", "cf-cache-status"].includes(name)) headers[name] = value;
      }
      Object.assign(entry, {status:response.status, protocol:response.protocol,
        responseSeconds:params.timestamp, headers, timing:response.timing || null});
    } else if (message.method === "Network.loadingFinished") {
      Object.assign(entry, {finishedSeconds:params.timestamp, encodedBytes:params.encodedDataLength});
    } else if (message.method === "Network.loadingFailed") {
      Object.assign(entry, {failedSeconds:params.timestamp, canceled:params.canceled || false,
        error:params.errorText, blockedReason:params.blockedReason || null});
    }
  }
  return 0;
}

async function revealModeCardsForObservation() {
  // This is a separate, automated viewport action, NOT initial-page paint.
  // CSS smooth scrolling can outlive two rAFs and leave the cards offscreen.
  // Do not change the site's CSS or count an offscreen identity as displayed.
  const startedAt = performance.now();
  if (document.hidden) throw new Error("mode observation needs a foreground document");
  const firstCard = document.querySelector("#mode-cards [data-public-signal-key]");
  if (!firstCard || !/^[a-f0-9]{32}$/.test(firstCard.dataset.publicSignalKey || "")) {
    throw new Error("mode observation needs a real public signal identity");
  }
  const frame = () => new Promise(resolve => requestAnimationFrame(resolve));
  firstCard.scrollIntoView({block:"center", behavior:"instant"});
  await frame();
  await frame();
  if (document.hidden || !firstCard.isConnected || !StockAgentDashboard.isElementVisible(firstCard)) {
    throw new Error("mode observation card is not currently visible");
  }
  const visibleAt = performance.now();
  // Rearm only the existing observer against the real, applied status. This
  // does not rebuild DOM, change filters, fetch data, or invent an SSE notice.
  render({heavy:false});
  await frame();
  await frame();
  if (document.hidden || !firstCard.isConnected || !StockAgentDashboard.isElementVisible(firstCard)) {
    throw new Error("mode observation lost its visible card");
  }
  return {startedAtMs:startedAt, visibleAtMs:visibleAt, completedAtMs:performance.now(),
    signalKey:firstCard.dataset.publicSignalKey, scrollY:window.scrollY,
    cardRect:firstCard.getBoundingClientRect().toJSON(),
    viewportWidth:innerWidth, viewportHeight:innerHeight,
    boundary:"Separate instant viewport action and foreground double-rAF opportunity; not initial paint, a matched SSE update, or full six-mode display"};
}

async function watchModeCardsForObservation(timeoutMs, reveal) {
  // Arm the viewport before the opening even when today's signal does not yet
  // exist. Only reveal() can certify a real, visible signal identity.
  const started = performance.now();
  const container = document.querySelector("#mode-cards");
  if (document.hidden || !container) {
    return {state:"unavailable", reason:"foreground_mode_container_unavailable"};
  }
  container.scrollIntoView({block:"center", behavior:"instant"});
  for (;;) {
    const card = document.querySelector("#mode-cards [data-public-signal-key]");
    if (card && /^[a-f0-9]{32}$/.test(card.dataset.publicSignalKey || "")) {
      const observation = await reveal();
      return {...observation, state:"observed", waitMs:performance.now() - started};
    }
    const remaining = timeoutMs - (performance.now() - started);
    if (remaining <= 0) {
      return {state:"unavailable", reason:"no_public_signal_identity_during_watch",
        waitMs:performance.now() - started,
        boundary:"No signal paint or native opening success inferred from an armed viewport"};
    }
    await new Promise(resolve => setTimeout(resolve, Math.min(100, remaining)));
  }
}

async function observeScopedDetails({timeoutMs = 20000, minimumSignals = 0} = {}) {
  // A chart finishing does not prove that the independent detail requests did.
  // In particular, do not pass a range change while its signals are empty,
  // pending, stale, or have timed out. Zero rows are valid only for an accepted
  // empty page, not because clearDateScopedViews just emptied the client.
  const started = performance.now();
  for (;;) {
    const state = {
      range: detailRangeKey(),
      signals: {rows: signalRows.length, total: signalTotal, loading: signalLoading,
        error: signalLoadError, selectionMatches: signalSelectionKey === detailSelectionKey("signals"),
        revisionMatches: signalDataRevision === detailDataRevision("signals")},
      positions: {rows: positionRows.length, total: positionTotal, loading: positionLoading,
        error: positionLoadError, selectionMatches: positionSelectionKey === detailSelectionKey("positions"),
        revisionMatches: positionDataRevision === detailDataRevision("positions")},
    };
    for (const [kind, page] of Object.entries({signals: state.signals, positions: state.positions})) {
      if (page.error) throw new Error(`scoped ${kind} failed: ${page.error}; ${JSON.stringify(state)}`);
    }
    if ([state.signals, state.positions].every(page => !page.loading && page.selectionMatches
        && page.revisionMatches && Number.isSafeInteger(page.total) && page.total >= page.rows)) {
      if (state.signals.rows < minimumSignals) {
        throw new Error(`expanded range lost previously observed signals: ${JSON.stringify(state)}`);
      }
      return {...state, readyMs: performance.now(), waitMs: performance.now() - started,
        boundary: "Accepted current-scope first pages, not all pages or proof of execution"};
    }
    if (performance.now() - started >= timeoutMs) {
      throw new Error(`scoped details deadline: ${JSON.stringify(state)}`);
    }
    await new Promise(resolve => setTimeout(resolve, 100));
  }
}

try {
  for (const name of ["Page", "Runtime", "Network"]) await send(`${name}.enable`);
  if (watchMs) {
    // This is an explicitly disclosed 2K desktop observer, not a claim about
    // a laptop/phone's viewport or every user's network and compositor.
    await send("Emulation.setDeviceMetricsOverride", {width:2560,height:1440,
      deviceScaleFactor:1,mobile:false,screenWidth:2560,screenHeight:1440});
  }
  await send("Runtime.addBinding", {name:"stockagentBenchmarkPaint"});
  await send("Page.addScriptToEvaluateOnNewDocument", {source:`
    document.addEventListener('stockagent-performance-recorded', event => {
      if (event.detail?.eventType === 'revision_applied' && event.detail?.correlation)
        window.stockagentBenchmarkPaint(JSON.stringify(event.detail));
    });`});
  await send("Network.setCacheDisabled", {cacheDisabled:true});
  stage = "navigation";
  const navigation = await send("Page.navigate", {url});
  if (navigation.result?.errorText) throw new Error(navigation.result.errorText);
  await waitForDocument();
  stage = "initial_view";
  await evaluate(`new Promise((resolve, reject) => {
    const deadline = performance.now() + 60000;
    const check = () => {
      const alert = document.getElementById('alert')?.textContent || '';
      if (alert.startsWith('面板讀取失敗')) return reject(new Error(alert));
      if (${watchMs > 0} && typeof snapshot !== 'undefined' && snapshot?.modes?.length
          && snapshot?.service_sync?.engine_generation && snapshot.service_sync.state_revision>0) return resolve(true);
      if (typeof signalRows !== 'undefined' && signalRows.length && chartHistoryMatchesSelection() && !historyInFlight) return resolve(true);
      if (performance.now() > deadline) return reject(new Error('initial view deadline'));
      setTimeout(check, 100);
    }; check();
  })`, true);
  const signalAndChartReadyMs = await evaluate("performance.now()");
  stage = "initial_view_details";
  const initialDetails = watchMs ? {skipped:true, boundary:"Opening watch waits for status identity, not detail completion"}
    : await evaluate(`(${observeScopedDetails.toString()})({minimumSignals:1})`, true);
  const initial = await evaluate(`({
    signals:signalRows.length, points:chartHistory?.returned_points ?? chartHistory?.history?.length ?? null,
    publicSignalKeys:(snapshot?.modes || []).map(mode=>({market:mode.market,sessionDate:mode.session_date,signalKey:mode.public_signal_key || null})),
    revisionPaintCapability:typeof window.StockAgentDashboard?.createRevisionPaintObserver==='function',
    revisionIdentityReady:/^[a-f0-9]{32}$/.test(snapshot?.service_sync?.engine_generation || '') && Number.isSafeInteger(snapshot?.service_sync?.state_revision) && snapshot.service_sync.state_revision>0,
    series:chartHistory?.minute_series?.length ?? null, chartCanvases:document.querySelectorAll('#equity-chart canvas').length, revision:lastServiceRevision,
    readyMs:performance.now(),
    navigation:performance.getEntriesByType('navigation').map(x=>({type:x.type,domContentLoadedMs:x.domContentLoadedEventEnd,loadMs:x.loadEventEnd,responseEndMs:x.responseEnd,transferBytes:x.transferSize,
      redirectCount:x.redirectCount,redirectStartMs:x.redirectStart,redirectEndMs:x.redirectEnd,fetchStartMs:x.fetchStart,
      dnsStartMs:x.domainLookupStart,dnsEndMs:x.domainLookupEnd,connectStartMs:x.connectStart,secureConnectionStartMs:x.secureConnectionStart,connectEndMs:x.connectEnd,
      requestStartMs:x.requestStart,responseStartMs:x.responseStart,protocol:x.nextHopProtocol,encodedBytes:x.encodedBodySize,decodedBytes:x.decodedBodySize})),
    assetResources:performance.getEntriesByType('resource').filter(x=>!x.name.includes('/api/')).map(x=>({path:x.name.replace(location.origin,''),initiator:x.initiatorType,startMs:x.startTime,
      dnsStartMs:x.domainLookupStart,dnsEndMs:x.domainLookupEnd,connectStartMs:x.connectStart,secureConnectionStartMs:x.secureConnectionStart,connectEndMs:x.connectEnd,
      requestStartMs:x.requestStart,responseStartMs:x.responseStart,endMs:x.responseEnd,durationMs:x.duration,transferBytes:x.transferSize,encodedBytes:x.encodedBodySize,protocol:x.nextHopProtocol})),
    paint:performance.getEntriesByType('paint').map(x=>({name:x.name,startMs:x.startTime})),
    resources:performance.getEntriesByType('resource').filter(x=>x.name.includes('/api/')).map(x=>({path:x.name.replace(location.origin,''),startMs:x.startTime,endMs:x.responseEnd,durationMs:x.duration})),
    renderMs:(()=>{const t=performance.now();render({heavy:true});return performance.now()-t;})(),
    memoizedChartMs:(()=>{const t=performance.now();renderChart(snapshot);return performance.now()-t;})()
  })`);
  stage = "mode_viewport_observation";
  const watchStarted = performance.now();
  const modeViewportObservation = watchMs
    ? await evaluate(`(${watchModeCardsForObservation.toString()})(${watchMs}, ${revealModeCardsForObservation.toString()})`, true)
    : await evaluate(`(${revealModeCardsForObservation.toString()})()`, true);
  stage = watchMs ? "watch_updates" : "full_history";
  const fullHistory = watchMs ? {skipped:true,boundary:"Opening observation never forces an all-history workload"} : await evaluate(`new Promise((resolve,reject)=>{
    const select = document.getElementById('detail-start-date');
    const dates = [...availableDetailDates].filter(Boolean).sort();
    select.value = dates[0];
    const started = performance.now();
    select.dispatchEvent(new Event('change'));
    const check = async () => {
      if (chartHistoryMatchesSelection() && !historyInFlight) {
        await new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)));
        const bitmap = ()=>[...document.querySelectorAll('#equity-chart canvas')].map(c=>c.toDataURL()).join('|');
        const before = bitmap();
        const hot = performance.now();renderChart(snapshot);const memoizedMs=performance.now()-hot;
        const savedHistory=chartHistory;
        chartHistory={...savedHistory};
        const cold=performance.now();renderChart(snapshot);const rebuildMs=performance.now()-cold;
        await new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)));
        const identicalCanvas=before===bitmap();
        chartHistory=savedHistory;
        renderChart(snapshot);
        return resolve({start:dates[0],points:chartHistory.returned_points ?? chartHistory.history.length,series:chartHistory.minute_series?.length ?? 0,chartCanvases:document.querySelectorAll('#equity-chart canvas').length,chartVisible:!document.getElementById('equity-chart').classList.contains('hidden'),loadMs:performance.now()-started,memoizedMs,rebuildMs,identicalCanvas,historyError:historyLoadError});
      }
      if (performance.now()-started>60000) return reject(new Error('full history deadline'));
      setTimeout(check,100);
    }; setTimeout(check,250);
  })`, true);
  stage = watchMs ? "watch_updates" : "full_history_details";
  const rangeDetails = watchMs ? {skipped:true, boundary:"Opening watch does not force a range change"}
    : await evaluate(`(${observeScopedDetails.toString()})({minimumSignals:${initial.signals > 0 ? 1 : 0}})`, true);
  const remainingWatchMs = watchMs ? Math.max(0, watchMs - (performance.now() - watchStarted)) : 3500;
  await new Promise((resolve)=>setTimeout(resolve, remainingWatchMs));
  const final = await evaluate(`({signals:signalRows.length,signalLoading,signalError:signalLoadError,positions:positionRows.length,positionLoading,positionError:positionLoadError,revision:lastServiceRevision,sourceRevision:snapshot.service_sync.revision_token,hidden:document.hidden,overflow:document.documentElement.scrollWidth-document.documentElement.clientWidth,resources:performance.getEntriesByType('resource').filter(x=>x.name.includes('/api/')&&!x.name.endsWith('/revision')).map(x=>({path:x.name.replace(location.origin,''),startMs:x.startTime,endMs:x.responseEnd,durationMs:x.duration}))})`);
  const performanceRecords = await evaluate(`window.StockAgentDashboard?.performanceHistorySnapshot?.() || []`);
  if (droppedApiRequests) errors.push({stage:"api_trace_capacity", droppedRequests:droppedApiRequests});
  const initialKeys = new Set(initial.publicSignalKeys.filter(row=>row.signalKey).map(row=>row.signalKey));
  const initialMarkets = new Set(initial.publicSignalKeys.map(row=>row.market));
  const visibleIdentitySamples = revisionPaint.filter(row=>
    row.browserRecord.correlation.viewEvidence==='status_mode_snapshot'
      && (watchMs ? initialMarkets.has(row.browserRecord.correlation.market)
                  : initialKeys.has(row.browserRecord.correlation.signalKey)));
  const paintVerification = {capability:initial.revisionPaintCapability,
    backendIdentity:initial.revisionIdentityReady,visibleIdentitySamples:visibleIdentitySamples.length,
    state:initial.revisionPaintCapability && initial.revisionIdentityReady && visibleIdentitySamples.length ? 'observed' : 'unavailable'};
  if (paintVerification.state!=='observed') errors.push({stage:'visible_revision_identity',
    error:'Current foreground data display was not measured; SSE and page reachability alone are insufficient',
    ...paintVerification});
  const result = {contract:"read_only_dashboard_browser_timing_v1",url,initial,signalAndChartReadyMs,initialDetails,modeViewportObservation,fullHistory,rangeDetails,final,performanceRecords,sseMessages:messages.length,errors,
    apiNetwork:{requests:[...apiNetwork.values()],droppedRequests:droppedApiRequests,
      boundary:"CDP request/response/download/cancel clocks plus allowlisted response timing headers; no cookies, authorization headers or bodies"},
    revisionPaint:{samples:revisionPaint,droppedFromSummary:droppedPaintSamples,watchMs,verification:paintVerification,
      matchedNoticeSamples:revisionPaint.filter(row=>row.browserRecord.correlation.trigger!=="initial_or_unmatched").length,
      matchedRevisionUpdateSamples:revisionPaint.filter(row=>row.browserRecord.correlation.trigger!=="initial_or_unmatched" && row.browserRecord.correlation.updatePhase==="revision_change").length,
      boundary:"Foreground double-rAF opportunity only; status mode snapshot and detail identity proofs differ. Initial/unmatched records are not SSE latency. Cross-host clocks are uncalibrated; no full-target fill assertion.",
      durableUpdatesPath:output ? `${output}.updates.jsonl` : null},
    boundary:"Fresh temporary tab with browser HTTP cache disabled; server and OS caches uncontrolled; SVG/2D browser profile is recorded by the launcher; no execution actions.",
    networkTimingBoundary:"Browser Resource/Navigation Timing, not server compute or kernel packet traces. Zero phases may represent reused connections or cross-origin timing redaction, not zero real network cost."};
  console.log(JSON.stringify(result,null,2));
  if (output) fs.writeFileSync(output,JSON.stringify(result,null,2)+"\n");
  if (errors.length || !messages.length || (!fullHistory.skipped && (!fullHistory.identicalCanvas || !fullHistory.points || !fullHistory.chartCanvases || !fullHistory.chartVisible || fullHistory.historyError)) || final.overflow) process.exitCode=1;
} catch (error) {
  let diagnostic = null;
  try {
    diagnostic = await evaluate(`({health:document.getElementById('health')?.textContent,
      alert:document.getElementById('alert')?.textContent?.slice(0,2000),
      signalCount:typeof signalRows==='undefined'?null:signalRows.length,
      modeCount:typeof snapshot==='undefined'?null:snapshot?.modes?.length,
      revision:typeof appliedStatusRevision==='undefined'?null:appliedStatusRevision,
      signalError:typeof signalLoadError==='undefined'?null:signalLoadError,
      positionError:typeof positionLoadError==='undefined'?null:positionLoadError,
      historyError:typeof historyLoadError==='undefined'?null:historyLoadError,
      historyMatches:typeof chartHistoryMatchesSelection==='function'?chartHistoryMatchesSelection():null})`);
  } catch (_diagnosticError) { /* Navigation may have lost its context. */ }
  const failure = {contract:"read_only_dashboard_browser_timing_v1",status:"failed",url,stage,
    diagnostic,
    documentUrl:mainFrame?.url ?? null,error:String(error),errors,sseMessages:messages.length,
    apiNetwork:{requests:[...apiNetwork.values()],droppedRequests:droppedApiRequests},
    revisionPaint:{samples:revisionPaint,droppedFromSummary:droppedPaintSamples,watchMs},
    boundary:"Native navigation/readiness failure retained; not a successful or zero-latency sample."};
  console.log(JSON.stringify(failure,null,2));
  if (output) fs.writeFileSync(output,JSON.stringify(failure,null,2)+"\n");
  process.exitCode=1;
} finally {
  if (clockTimer) clearInterval(clockTimer);
  socket.close();
  await fetch(`http://127.0.0.1:${port}/json/close/${target.id}`);
}

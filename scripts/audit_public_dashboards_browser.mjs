#!/usr/bin/env node
/** Audit the deployed dashboards in a real Chromium viewport through CDP. */

import fs from "node:fs";
import path from "node:path";

const port = Number(process.argv[2] || 9222);
const baseUrl = String(process.argv[3] || "https://penguin72487.ddnsgeek.com").replace(/\/$/, "");
const outputDir = String(process.argv[4] || "/tmp/stockagent-dashboard-browser-audit");
const width = Number(process.argv[5] || 1366);
const height = Number(process.argv[6] || 768);
const deviceScaleFactor = Math.max(1, Math.min(4, Number(process.argv[8] || 1)));
const emulateMobile = ["1", "true", "mobile"].includes(String(process.argv[9] || "").toLowerCase());
const allPages = [
  ["overview", "/"],
  ["taifex", "/taifex/"],
  ["tw-day-trade", "/tw-day-trade/"],
  ["tw-overnight", "/tw-overnight/"],
  ["shioaji", "/shioaji/"],
  ["openbb", "/openbb/"],
  ["data-monitor", "/data-monitor/"],
  ["traffic", "/traffic/"],
];
const requestedPages = new Set(String(process.argv[7] || "").split(",").filter(Boolean));
const pages = requestedPages.size
  ? allPages.filter(([name]) => requestedPages.has(name))
  : allPages;
const representativeControls = {
  taifex: {selector: "button[data-range='1h']", apiPath: "/taifex/api/history"},
  shioaji: {selector: "button[data-filter='historical']"},
  openbb: {selector: "button[data-range='1h']", apiPath: "/openbb/api/history"},
  "data-monitor": {selector: "#status-filter", value: "complete"},
  traffic: {selector: "#browser-kind-filter", value: "interaction"},
};

fs.mkdirSync(outputDir, {recursive: true});

async function openTarget(url) {
  const response = await fetch(
    `http://127.0.0.1:${port}/json/new?${encodeURIComponent(url)}`,
    {method: "PUT", signal: AbortSignal.timeout(10000)},
  );
  if (!response.ok) throw new Error(`cannot create Chrome target: HTTP ${response.status}`);
  return response.json();
}

async function connect(webSocketUrl) {
  const socket = new WebSocket(webSocketUrl);
  await new Promise((resolve, reject) => {
    const timeout = setTimeout(() => { socket.close(); reject(new Error("CDP connection timeout")); }, 10000);
    socket.addEventListener("open", () => { clearTimeout(timeout); resolve(); }, {once: true});
    socket.addEventListener("error", error => { clearTimeout(timeout); reject(error); }, {once: true});
  });
  let sequence = 0;
  const pending = new Map();
  const consoleErrors = [];
  const pendingApi = new Map();
  const failedApi = [];
  socket.addEventListener("close", () => {
    for (const {reject, timer} of pending.values()) {
      clearTimeout(timer); reject(new Error("CDP disconnected"));
    }
    pending.clear();
  });
  socket.addEventListener("message", (event) => {
    const message = JSON.parse(event.data);
    if (message.id && pending.has(message.id)) {
      const {resolve, reject, timer} = pending.get(message.id);
      clearTimeout(timer);
      pending.delete(message.id);
      if (message.error) reject(new Error(JSON.stringify(message.error)));
      else resolve(message.result);
      return;
    }
    if (message.method === "Network.requestWillBeSent") {
      const url = new URL(message.params.request.url);
      if (url.pathname.includes("/api/") && !url.pathname.endsWith("/updates")) pendingApi.set(message.params.requestId, url.pathname);
    }
    if (message.method === "Network.loadingFinished" || message.method === "Network.loadingFailed") {
      const apiPath = pendingApi.get(message.params.requestId);
      if (apiPath && message.method === "Network.loadingFailed" && !message.params.canceled) failedApi.push({path:apiPath, error:message.params.errorText});
      pendingApi.delete(message.params.requestId);
    }
    if (message.method === "Runtime.exceptionThrown") {
      consoleErrors.push(
        message.params?.exceptionDetails?.exception?.description
        || message.params?.exceptionDetails?.text
        || "runtime exception",
      );
    }
    if (message.method === "Log.entryAdded" && message.params?.entry?.level === "error") {
      consoleErrors.push(message.params.entry.text || "console error");
    }
  });
  const send = (method, params = {}) => new Promise((resolve, reject) => {
    const id = ++sequence;
    const timer = setTimeout(() => { pending.delete(id); reject(new Error(`CDP timeout: ${method}`)); }, 60000);
    pending.set(id, {resolve, reject, timer});
    try { socket.send(JSON.stringify({id, method, params})); }
    catch (error) { clearTimeout(timer); pending.delete(id); reject(error); }
  });
  return {socket, send, consoleErrors, pendingApi, failedApi};
}

async function waitUntilReady(send, pendingApi) {
  const deadline = Date.now() + 30000;
  let quietSince = null;
  while (Date.now() < deadline) {
    const result = await send("Runtime.evaluate", {
      expression: "document.readyState",
      returnByValue: true,
    });
    if (result.result?.value === "complete" && pendingApi.size === 0) {
      quietSince ??= Date.now();
      if (Date.now() - quietSince >= 1000) return;
    } else quietSince = null;
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(`Page readiness deadline; pending APIs: ${[...pendingApi.values()].join(", ")}`);
}

const expression = `(() => {
  const visible = (element) => {
    const style = getComputedStyle(element);
    const rect = element.getBoundingClientRect();
    return style.display !== "none" && style.visibility !== "hidden" && rect.width > 0 && rect.height > 0;
  };
  const insideIntentionalScroller = (element) => {
    for (let parent = element.parentElement; parent; parent = parent.parentElement) {
      const overflowX = getComputedStyle(parent).overflowX;
      if (["auto", "scroll"].includes(overflowX) && parent.scrollWidth > parent.clientWidth + 1) return true;
    }
    return false;
  };
  const horizontalOverflow = Math.max(0, document.documentElement.scrollWidth - document.documentElement.clientWidth);
  const offenders = [...document.querySelectorAll("body *")]
    .filter((element) => visible(element) && !insideIntentionalScroller(element))
    .map((element) => ({
      tag: element.tagName.toLowerCase(),
      id: element.id || null,
      className: typeof element.className === "string" ? element.className.slice(0, 80) : "",
      right: Math.round(element.getBoundingClientRect().right),
    }))
    .filter((row) => row.right > innerWidth + 1)
    .slice(0, 12);
  const scrollRegions = [...document.querySelectorAll(".table-scroll,.table-wrap,.chart-scroll")]
    .filter(visible)
    .map((element) => ({
      label: element.getAttribute("aria-label") || element.className,
      clientWidth: element.clientWidth,
      scrollWidth: element.scrollWidth,
      overflow: Math.max(0, element.scrollWidth - element.clientWidth),
    }));
  const navigationRegions = [...document.querySelectorAll("nav")]
    .filter(visible)
    .map((element) => ({
      label: element.getAttribute("aria-label") || element.className || "navigation",
      clientWidth: element.clientWidth,
      scrollWidth: element.scrollWidth,
      overflow: Math.max(0, element.scrollWidth - element.clientWidth),
    }));
  const navigation = performance.getEntriesByType("navigation")[0];
  const resources = performance.getEntriesByType("resource");
  const decimalSamples = (document.body.innerText.match(/[-+]?\\d+\\.\\d{3,}/g) || []).slice(0, 12);
  const interactive = [...document.querySelectorAll("a,button,input,select,summary,[tabindex]")]
    .filter(visible);
  const smallTargets = interactive.filter((element) => !element.classList.contains("skip-link")).map((element) => {
    const rect = element.getBoundingClientRect();
    return {
      tag: element.tagName.toLowerCase(),
      label: (element.getAttribute("aria-label") || element.textContent || element.value || "").trim().slice(0, 60),
      width: Math.round(rect.width),
      height: Math.round(rect.height),
    };
  }).filter((row) => row.width < 24 || row.height < 24);
  const touchTargetRisks = interactive.filter((element) => !element.classList.contains("skip-link")).map((element) => {
    const rect = element.getBoundingClientRect();
    return {
      tag: element.tagName.toLowerCase(),
      label: (element.getAttribute("aria-label") || element.textContent || element.value || "").trim().slice(0, 60),
      width: Math.round(rect.width),
      height: Math.round(rect.height),
    };
  }).filter((row) => row.width < 40 || row.height < 40);
  const clippedInteractive = interactive.map((element) => {
    const rect = element.getBoundingClientRect();
    return {
      tag: element.tagName.toLowerCase(),
      label: (element.getAttribute("aria-label") || element.textContent || element.value || "").trim().slice(0, 60),
      left: Math.round(rect.left), right: Math.round(rect.right),
    };
  }).filter((row) => row.left < -1 || row.right > innerWidth + 1).slice(0, 20);
  const viewportTargets = interactive.map((element) => ({element, rect:element.getBoundingClientRect()}))
    .filter(({rect}) => rect.bottom > 0 && rect.top < innerHeight && rect.right > 0 && rect.left < innerWidth);
  const overlappingTargets = [];
  for (let leftIndex = 0; leftIndex < viewportTargets.length; leftIndex += 1) {
    const left = viewportTargets[leftIndex];
    for (let rightIndex = leftIndex + 1; rightIndex < viewportTargets.length; rightIndex += 1) {
      const right = viewportTargets[rightIndex];
      if (left.element.contains(right.element) || right.element.contains(left.element)) continue;
      const overlapWidth = Math.min(left.rect.right, right.rect.right) - Math.max(left.rect.left, right.rect.left);
      const overlapHeight = Math.min(left.rect.bottom, right.rect.bottom) - Math.max(left.rect.top, right.rect.top);
      if (overlapWidth <= 2 || overlapHeight <= 2) continue;
      overlappingTargets.push({
        first:(left.element.getAttribute("aria-label") || left.element.textContent || left.element.tagName).trim().slice(0,40),
        second:(right.element.getAttribute("aria-label") || right.element.textContent || right.element.tagName).trim().slice(0,40),
        overlapWidth:Math.round(overlapWidth), overlapHeight:Math.round(overlapHeight),
      });
      if (overlappingTargets.length >= 12) break;
    }
    if (overlappingTargets.length >= 12) break;
  }
  const unnamedTargets = interactive.filter((element) => !(
    element.getAttribute("aria-label")
    || element.getAttribute("title")
    || element.textContent?.trim()
    || element.value?.trim()
    || (element.id && document.querySelector('label[for="' + CSS.escape(element.id) + '"]'))
    || element.closest("label")
  )).map((element) => ({tag: element.tagName.toLowerCase(), id: element.id || null}));
  const idCounts = new Map();
  for (const element of document.querySelectorAll("[id]")) {
    idCounts.set(element.id, (idCounts.get(element.id) || 0) + 1);
  }
  const duplicateIds = [...idCounts.entries()]
    .filter(([, count]) => count > 1)
    .map(([id, count]) => ({id, count}));
  const globalNavigation = document.querySelector("header nav[aria-label*='公開面板']");
  const unlabeledFields = [...document.querySelectorAll("input,select,textarea")]
    .filter(visible)
    .filter((element) => !(
      element.getAttribute("aria-label")
      || element.closest("label")
      || (element.id && document.querySelector('label[for="' + CSS.escape(element.id) + '"]'))
    ))
    .map((element) => ({tag: element.tagName.toLowerCase(), id: element.id || null}));
  const inaccessibleTableRegions = [...document.querySelectorAll("table")]
    .filter(visible)
    .filter((table) => {
      const region = table.closest(".table-scroll,.table-wrap");
      return region && (region.tabIndex < 0 || region.getAttribute("role") !== "region");
    })
    .map((table) => table.id || table.className || "unnamed-table");
  const tinyText = [...document.querySelectorAll("body *")]
    .filter(visible)
    .filter((element) => element.children.length === 0 && element.textContent.trim())
    .map((element) => ({
      tag: element.tagName.toLowerCase(),
      text: element.textContent.trim().slice(0, 50),
      size: Number.parseFloat(getComputedStyle(element).fontSize),
    }))
    .filter((row) => Number.isFinite(row.size) && row.size < 10)
    .slice(0, 20);
  const chartAudit = [...document.querySelectorAll("[role='img']")]
    .filter(visible)
    .map((chart) => {
      const rect = chart.getBoundingClientRect();
      const canvas = chart.matches("canvas") ? chart : chart.querySelector("canvas");
      const svg = chart.matches("svg") ? chart : chart.querySelector("svg");
      return {
        id: chart.id || null,
        renderer: canvas ? "canvas" : svg ? "svg" : "unknown",
        width: Math.round(rect.width),
        height: Math.round(rect.height),
        pixelWidth: canvas?.width || null,
        pixelHeight: canvas?.height || null,
        markCount: svg?.querySelectorAll("path,polyline,circle,rect").length || 0,
        textCount: svg?.querySelectorAll("text").length || 0,
      };
    });
  const viewportMeta = document.querySelector('meta[name="viewport"]')?.content || "";
  const menuButton = document.querySelector(".dashboard-nav-toggle");
  return {
    title: document.title,
    url: location.href,
    readyState: document.readyState,
    viewport: {width: innerWidth, height: innerHeight, devicePixelRatio},
    viewportMeta,
    horizontalOverflow,
    offenders,
    scrollRegions,
    navigationRegions,
    navigationOverflow: navigationRegions.filter(row => row.overflow > 1),
    laptopTableOverflow: innerWidth >= 1024 ? scrollRegions.filter(row => row.overflow > 1) : [],
    decimalSamples,
    interactiveCount: interactive.length,
    smallTargets,
    touchTargetRisks,
    clippedInteractive,
    overlappingTargets,
    unnamedTargets,
    lang: document.documentElement.lang,
    mainCount: document.querySelectorAll("main").length,
    h1Count: document.querySelectorAll("h1").length,
    duplicateIds,
    globalNavLinkCount: globalNavigation?.querySelectorAll("a").length || 0,
    currentPageLinkCount: globalNavigation?.querySelectorAll('[aria-current="page"]').length || 0,
    unlabeledFields,
    inaccessibleTableRegions,
    tinyText,
    chartAudit,
    mobileMenu: menuButton ? {
      visible: visible(menuButton),
      expanded: menuButton.getAttribute("aria-expanded"),
      controls: menuButton.getAttribute("aria-controls"),
    } : null,
    requestTimings: window.StockAgentDashboard?.performanceSnapshot?.() || [],
    browserPerformanceHistory: window.StockAgentDashboard?.performanceHistorySnapshot?.() || [],
    chartPerformance: (window.StockAgentDashboard?.performanceHistorySnapshot?.() || [])
      .filter(row => row.kind === "render" && row.action === "equity_chart"),
    apiTimingErrors: (window.StockAgentDashboard?.performanceSnapshot?.() || []).filter(row =>
      row.outcome !== 'AbortError' && (row.outcome !== 'ok' || row.status >= 400)),
    apiResources: resources.filter(row => row.name.includes('/api/') && !row.name.includes('/updates')).map(row => ({
      path: new URL(row.name).pathname, startMs: row.startTime, headersMs: row.responseStart - row.startTime,
      bodyMs: row.responseEnd - row.responseStart, totalMs: row.duration,
      transferBytes: row.transferSize, decodedBytes: row.decodedBodySize,
      serverTiming: row.serverTiming?.map(x => ({name:x.name,duration:x.duration,description:x.description})),
    })),
    paintTimings: performance.getEntriesByType('paint').map(row => ({name:row.name,ms:row.startTime})),
    longTasks: window.__dashboardAuditLongTasks || [],
    domContentLoadedMs: navigation ? Math.round(navigation.domContentLoadedEventEnd) : null,
    loadMs: navigation ? Math.round(navigation.loadEventEnd) : null,
    resourceTransferBytes: resources.reduce((sum, row) => sum + Number(row.transferSize || 0), 0),
    resourceDecodedBytes: resources.reduce((sum, row) => sum + Number(row.decodedBodySize || 0), 0),
    visibleTextLength: document.body.innerText.length,
  };
})()`;

const results = [];
for (const [name, suffix] of pages) {
  let target, client;
  let consoleErrors = [];
  try {
  target = await openTarget("about:blank");
  client = await connect(target.webSocketDebuggerUrl);
  const {send, pendingApi, failedApi} = client;
  consoleErrors = client.consoleErrors;
  await send("Page.enable");
  await send("Runtime.enable");
  await send("Log.enable");
  await send("Network.enable");
  await send("Network.setCacheDisabled", {cacheDisabled: true});
  await send("Page.addScriptToEvaluateOnNewDocument", {source: `
    window.__dashboardAuditLongTasks = [];
    try { new PerformanceObserver(list => { for (const row of list.getEntries()) {
      if (window.__dashboardAuditLongTasks.length < 128) window.__dashboardAuditLongTasks.push({startMs:row.startTime,durationMs:row.duration});
    }}).observe({type:'longtask', buffered:true}); } catch (_) {}
  `});
  await send("Emulation.setDeviceMetricsOverride", {
    width,
    height,
    deviceScaleFactor,
    mobile: emulateMobile,
  });
  await send("Emulation.setTouchEmulationEnabled", {
    enabled: emulateMobile,
    maxTouchPoints: emulateMobile ? 5 : 1,
  });
  await send("Page.navigate", {url: `${baseUrl}${suffix}`});
  await waitUntilReady(send, pendingApi);
  const initialApiResources = await send("Runtime.evaluate", {
    expression: `performance.getEntriesByType('resource')
      .filter(row=>row.name.includes('/api/')&&!row.name.includes('/updates'))
      .map(row=>({path:new URL(row.name).pathname,startMs:row.startTime,totalMs:row.duration,decodedBytes:row.decodedBodySize}))`,
    returnByValue: true,
  });
  let interactionLatency = null;
  if (name === "traffic") {
    const interaction = await send("Runtime.evaluate", {
      expression: `new Promise((resolve) => {
        const output = document.getElementById("local-total");
        const button = document.getElementById("refresh-now");
        if (!output || !button) { resolve({error: "missing traffic controls"}); return; }
        const started = performance.now();
        const observer = new MutationObserver(() => {
          observer.disconnect();
          resolve({milliseconds: performance.now() - started, displayed: output.textContent});
        });
        observer.observe(output, {childList: true, characterData: true, subtree: true});
        button.click();
        setTimeout(() => { observer.disconnect(); resolve({error: "timeout"}); }, 5000);
      })`,
      awaitPromise: true,
      returnByValue: true,
    });
    interactionLatency = interaction.result?.value || null;
    const performancePanel = await send("Runtime.evaluate", {
      expression: `new Promise(resolve => {
        const started=performance.now();
        const check=()=>{
          const rows=document.querySelectorAll('#browser-action-rows tr').length;
          const history=StockAgentDashboard.performanceHistorySnapshot();
          const crossPageRoutes=[...new Set(history.map(row=>row.route))];
          if(rows>0&&history.length>0) resolve({browserMetricRows:rows,browserMetricCount:history.length,crossPageRoutes});
          else if(performance.now()-started>5000) resolve({error:'browser performance panel deadline',browserMetricRows:rows,browserMetricCount:history.length,crossPageRoutes});
          else setTimeout(check,25);
        }; check();
      })`,
      awaitPromise: true,
      returnByValue: true,
    });
    interactionLatency = {...interactionLatency, ...(performancePanel.result?.value || {error:"performance panel evaluation failed"})};
    await send("Runtime.evaluate", {
      expression: `new Promise(resolve => {
        document.getElementById('browser-performance-title')?.scrollIntoView({behavior:'instant',block:'start'});
        requestAnimationFrame(()=>requestAnimationFrame(resolve));
      })`,
      awaitPromise: true,
    });
    const performanceScreenshot = await send("Page.captureScreenshot", {
      format: "png",
      captureBeyondViewport: false,
    });
    fs.writeFileSync(
      path.join(outputDir, `traffic-performance-${width}x${height}.png`),
      Buffer.from(performanceScreenshot.data, "base64"),
    );
    await send("Runtime.evaluate", {expression: "scrollTo({top:0,behavior:'instant'})"});
  }
  if (name === "data-monitor") {
    const initialScreenshot = await send("Page.captureScreenshot", {
      format: "png",
      captureBeyondViewport: false,
    });
    fs.writeFileSync(
      path.join(outputDir, `data-monitor-initial-${width}x${height}.png`),
      Buffer.from(initialScreenshot.data, "base64"),
    );
    const interaction = await send("Runtime.evaluate", {
      expression: `new Promise(resolve => {
        const target=document.getElementById('source-list');
        if (!target) return resolve({error:'missing source registry'});
        const resources=performance.getEntriesByType('resource');
        const initial={
          decodedBytes:resources.reduce((sum,row)=>sum+Number(row.decodedBodySize||0),0),
          apiPaths:resources.filter(row=>row.name.includes('/api/')&&!row.name.includes('/updates')).map(row=>new URL(row.name).pathname),
          detailRows:document.querySelectorAll('#source-rows tr').length,
        };
        const start=performance.now();
        target.scrollIntoView({behavior:'instant',block:'start'});
        const check=()=>{
          const rows=document.querySelectorAll('#source-rows tr').length;
          const samples=StockAgentDashboard.performanceSnapshot();
          const detail=samples.find(row=>row.path.endsWith('/api/details')&&row.outcome==='ok');
          if(rows>0&&detail) requestAnimationFrame(()=>requestAnimationFrame(()=>resolve({
            initial, detailRows:rows, revealToPaintMs:performance.now()-start, detailRequest:detail,
          })));
          else if(performance.now()-start>30000) resolve({
            error:'detail activation deadline', initial, detailRows:rows,
            scrollY, sourceTop:target.getBoundingClientRect().top,
            samples:StockAgentDashboard.performanceSnapshot(),
          });
          else setTimeout(check,25);
        }; check();
      })`, awaitPromise:true, returnByValue:true,
    });
    interactionLatency = interaction.result?.value || {error:"evaluation failed"};
    await waitUntilReady(send, pendingApi);
  }
  if (["tw-day-trade", "tw-overnight"].includes(name)) {
    const interaction = await send("Runtime.evaluate", {
      expression: `new Promise(resolve => {
        const button=document.getElementById('force-refresh');
        if (!button) return resolve({error:'missing refresh button'});
        const start=performance.now(); const prior=StockAgentDashboard.performanceSnapshot().length;
        button.click();
        const check=()=>{
          const samples=StockAgentDashboard.performanceSnapshot();
          if (samples.length>prior && samples.slice(prior).some(x=>x.path.endsWith('/api/status'))) {
            requestAnimationFrame(()=>requestAnimationFrame(()=>resolve({requestToPaintOpportunityMs:performance.now()-start,samples:samples.slice(prior),navigationCount:performance.getEntriesByType('navigation').length})));
          } else if(performance.now()-start>20000) resolve({error:'refresh deadline'});
          else setTimeout(check,20);
        }; check();
      })`, awaitPromise:true, returnByValue:true,
    });
    interactionLatency = interaction.result?.value || {error:"evaluation failed"};
    const eventActivation = await send("Runtime.evaluate", {
      expression: `new Promise(resolve => {
        const target=document.getElementById('event-body')?.closest('section');
        if (!target) return resolve({error:'missing event section'});
        const before=StockAgentDashboard.performanceSnapshot();
        const priorApi=before.find(row=>row.path.endsWith('/api/events'));
        if(priorApi) {
          target.scrollIntoView({behavior:'instant',block:'center'});
          return requestAnimationFrame(()=>requestAnimationFrame(()=>resolve({
            milliseconds:0,alreadyLoaded:true,api:priorApi,
            eventRows:document.querySelectorAll('#event-body tr').length,
          })));
        }
        const started=performance.now(); const prior=before.length;
        target.scrollIntoView({behavior:'instant',block:'center'});
        const check=()=>{
          const rows=StockAgentDashboard.performanceSnapshot().slice(prior);
          const api=rows.find(row=>row.path.endsWith('/api/events'));
          if(api) requestAnimationFrame(()=>requestAnimationFrame(()=>resolve({milliseconds:performance.now()-started,api,eventRows:document.querySelectorAll('#event-body tr').length})));
          else if(performance.now()-started>10000) resolve({error:'event activation deadline',rows,eventRows:document.querySelectorAll('#event-body tr').length});
          else setTimeout(check,20);
        }; check();
      })`, awaitPromise:true, returnByValue:true,
    });
    interactionLatency = {...interactionLatency, eventActivation:eventActivation.result?.value || {error:"evaluation failed"}};
    const eventScreenshot = await send("Page.captureScreenshot", {format:"png",captureBeyondViewport:false});
    fs.writeFileSync(path.join(outputDir, `${name}-events-${width}x${height}.png`), Buffer.from(eventScreenshot.data,"base64"));
    await send("Runtime.evaluate", {expression:"scrollTo({top:0,behavior:'instant'})"});
    if (name === "tw-day-trade") {
      const fullHistory = await send("Runtime.evaluate", {
        expression: `new Promise(resolve => {
          const startInput=document.getElementById('detail-start-date');
          const endInput=document.getElementById('detail-end-date');
          if(!startInput||!endInput||!startInput.min||!endInput.max) {
            resolve({error:'missing full-history date boundaries'}); return;
          }
          const startedAt=Date.now(); const started=performance.now();
          const priorRequests=StockAgentDashboard.performanceSnapshot().length;
          startInput.value=startInput.min; endInput.value=endInput.max;
          endInput.dispatchEvent(new Event('change',{bubbles:true}));
          const check=()=>{
            const requests=StockAgentDashboard.performanceSnapshot().slice(priorRequests);
            const api=requests.find(row=>row.path.endsWith('/api/history')&&row.outcome==='ok');
            const render=StockAgentDashboard.performanceHistorySnapshot()
              .filter(row=>row.observedAt>=startedAt&&row.kind==='render'&&row.action==='equity_chart'&&row.pointCount>10000)
              .at(-1);
            if(api&&render) requestAnimationFrame(()=>requestAnimationFrame(()=>resolve({
              milliseconds:performance.now()-started, api, render,
              canvasCount:document.querySelectorAll('#equity-chart canvas').length,
              svgPathCount:document.querySelectorAll('#equity-chart path').length,
              startDate:startInput.value, endDate:endInput.value,
            })));
            else if(performance.now()-started>30000) resolve({
              error:'full history render deadline', requests,
              recentRenders:StockAgentDashboard.performanceHistorySnapshot().filter(row=>row.observedAt>=startedAt&&row.kind==='render'),
            });
            else setTimeout(check,25);
          }; check();
        })`, awaitPromise:true, returnByValue:true,
      });
      interactionLatency = {
        ...interactionLatency,
        fullHistory: fullHistory.result?.value || {error:"evaluation failed"},
      };
      await waitUntilReady(send, pendingApi);
    }
  }
  let representativeAction = null;
  const representative = representativeControls[name];
  if (representative) {
    const tested = await send("Runtime.evaluate", {
      expression: `new Promise(resolve => {
        let target=document.querySelector(${JSON.stringify(representative.selector)});
        if(!target) return resolve({error:'missing representative control'});
        if(target.matches('button[data-range]')&&target.getAttribute('aria-pressed')==='true') {
          target=[...target.parentElement.querySelectorAll('button[data-range]')]
            .find(button=>button.getAttribute('aria-pressed')!=='true')||target;
        }
        const startedAt=Date.now(); const started=performance.now();
        ${representative.value
          ? `target.value=${JSON.stringify(representative.value)}; target.dispatchEvent(new Event('change',{bubbles:true}));`
          : "target.click();"}
        const check=()=>{
          const rows=StockAgentDashboard.performanceHistorySnapshot().filter(row=>row.observedAt>=startedAt-5);
          const interaction=rows.find(row=>row.kind==='interaction');
          const api=${representative.apiPath
            ? `rows.find(row=>row.kind==='api'&&row.requestPath===${JSON.stringify(representative.apiPath)})`
            : "true"};
          if(interaction&&api) resolve({milliseconds:performance.now()-started,interaction,api:${representative.apiPath ? "api" : "null"}});
          else if(performance.now()-started>10000) resolve({error:'representative action deadline',rows});
          else setTimeout(check,20);
        }; check();
      })`,
      awaitPromise: true,
      returnByValue: true,
    });
    representativeAction = tested.result?.value || {error:"representative action evaluation failed"};
    await waitUntilReady(send, pendingApi);
  }
  const evaluated = await send("Runtime.evaluate", {
    expression,
    returnByValue: true,
  });
  const screenshot = await send("Page.captureScreenshot", {
    format: "png",
    captureBeyondViewport: false,
  });
  fs.writeFileSync(
    path.join(outputDir, `${name}-${width}x${height}.png`),
    Buffer.from(screenshot.data, "base64"),
  );
  results.push({...evaluated.result.value,
    initialApiResources:initialApiResources.result?.value || [],
    interactionLatency, representativeAction, consoleErrors, failedApi,
    pendingApi:[...pendingApi.values()],
  });
  } catch(error) {
    results.push({url:`${baseUrl}${suffix}`,auditError:String(error),consoleErrors});
  } finally {
    client?.socket.close();
    if (target) await fetch(`http://127.0.0.1:${port}/json/close/${target.id}`, {signal:AbortSignal.timeout(5000)}).catch(() => {});
  }
}

const reportPath = path.join(outputDir, `report-${width}x${height}.json`);
fs.writeFileSync(reportPath, `${JSON.stringify(results, null, 2)}\n`);
console.log(JSON.stringify({reportPath, results:results.map(row=>({url:row.url,horizontalOverflow:row.horizontalOverflow,
  navigationOverflow:row.navigationOverflow,touchTargetRisks:row.touchTargetRisks?.length,
  apiRequests:row.requestTimings?.length,interactionLatency:row.interactionLatency,
  representativeAction:row.representativeAction,consoleErrors:row.consoleErrors,auditError:row.auditError}))}, null, 2));
if(results.some(row=>row.auditError || row.horizontalOverflow || row.laptopTableOverflow?.length
  || row.navigationOverflow?.length || row.smallTargets?.length || row.clippedInteractive?.length
  || row.overlappingTargets?.length || row.consoleErrors.length || row.failedApi?.length
  || row.apiTimingErrors?.length || row.interactionLatency?.error
  || (emulateMobile && row.touchTargetRisks?.length)
  || row.interactionLatency?.eventActivation?.error || row.representativeAction?.error)) process.exitCode=1;

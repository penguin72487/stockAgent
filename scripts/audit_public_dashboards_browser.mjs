#!/usr/bin/env node
/** Audit the deployed dashboards in a real Chromium viewport through CDP. */

import fs from "node:fs";
import path from "node:path";

const port = Number(process.argv[2] || 9222);
const baseUrl = String(process.argv[3] || "https://penguin72487.ddnsgeek.com").replace(/\/$/, "");
const outputDir = String(process.argv[4] || "/tmp/stockagent-dashboard-browser-audit");
const width = Number(process.argv[5] || 1366);
const height = Number(process.argv[6] || 768);
const pages = [
  ["overview", "/"],
  ["taifex", "/taifex/"],
  ["tw-day-trade", "/tw-day-trade/"],
  ["shioaji", "/shioaji/"],
  ["openbb", "/openbb/"],
  ["data-monitor", "/data-monitor/"],
  ["traffic", "/traffic/"],
];

fs.mkdirSync(outputDir, {recursive: true});

async function openTarget(url) {
  const response = await fetch(
    `http://127.0.0.1:${port}/json/new?${encodeURIComponent(url)}`,
    {method: "PUT"},
  );
  if (!response.ok) throw new Error(`cannot create Chrome target: HTTP ${response.status}`);
  return response.json();
}

async function connect(webSocketUrl) {
  const socket = new WebSocket(webSocketUrl);
  await new Promise((resolve, reject) => {
    socket.addEventListener("open", resolve, {once: true});
    socket.addEventListener("error", reject, {once: true});
  });
  let sequence = 0;
  const pending = new Map();
  const consoleErrors = [];
  socket.addEventListener("message", (event) => {
    const message = JSON.parse(event.data);
    if (message.id && pending.has(message.id)) {
      const {resolve, reject} = pending.get(message.id);
      pending.delete(message.id);
      if (message.error) reject(new Error(JSON.stringify(message.error)));
      else resolve(message.result);
      return;
    }
    if (message.method === "Runtime.exceptionThrown") {
      consoleErrors.push(message.params?.exceptionDetails?.text || "runtime exception");
    }
    if (message.method === "Log.entryAdded" && message.params?.entry?.level === "error") {
      consoleErrors.push(message.params.entry.text || "console error");
    }
  });
  const send = (method, params = {}) => new Promise((resolve, reject) => {
    const id = ++sequence;
    pending.set(id, {resolve, reject});
    socket.send(JSON.stringify({id, method, params}));
  });
  return {socket, send, consoleErrors};
}

async function waitUntilReady(send) {
  const deadline = Date.now() + 30000;
  while (Date.now() < deadline) {
    const result = await send("Runtime.evaluate", {
      expression: "document.readyState",
      returnByValue: true,
    });
    if (result.result?.value === "complete") break;
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  await new Promise((resolve) => setTimeout(resolve, 3500));
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
  return {
    title: document.title,
    url: location.href,
    readyState: document.readyState,
    viewport: {width: innerWidth, height: innerHeight},
    horizontalOverflow,
    offenders,
    scrollRegions,
    decimalSamples,
    interactiveCount: interactive.length,
    smallTargets,
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
    domContentLoadedMs: navigation ? Math.round(navigation.domContentLoadedEventEnd) : null,
    loadMs: navigation ? Math.round(navigation.loadEventEnd) : null,
    resourceTransferBytes: resources.reduce((sum, row) => sum + Number(row.transferSize || 0), 0),
    resourceDecodedBytes: resources.reduce((sum, row) => sum + Number(row.decodedBodySize || 0), 0),
    visibleTextLength: document.body.innerText.length,
  };
})()`;

const results = [];
for (const [name, suffix] of pages) {
  const target = await openTarget(`${baseUrl}${suffix}`);
  const {socket, send, consoleErrors} = await connect(target.webSocketDebuggerUrl);
  await send("Page.enable");
  await send("Runtime.enable");
  await send("Log.enable");
  await send("Network.enable");
  await send("Network.setCacheDisabled", {cacheDisabled: true});
  await send("Emulation.setDeviceMetricsOverride", {
    width,
    height,
    deviceScaleFactor: 1,
    mobile: false,
  });
  await send("Page.navigate", {url: `${baseUrl}${suffix}`});
  await waitUntilReady(send);
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
  results.push({...evaluated.result.value, interactionLatency, consoleErrors});
  socket.close();
}

const reportPath = path.join(outputDir, `report-${width}x${height}.json`);
fs.writeFileSync(reportPath, `${JSON.stringify(results, null, 2)}\n`);
console.log(JSON.stringify({reportPath, results}, null, 2));

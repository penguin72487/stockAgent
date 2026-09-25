#!/usr/bin/env node
// Real Chromium request-to-paint benchmark for the large feature inventory.
import {writeFileSync} from "node:fs";

const port = Number(process.argv[2] || 9229);
const url = process.argv[3] || "https://penguin72487.ddnsgeek.com/data-monitor/";
const output = process.argv[4] || "";
const target = await (await fetch(
  `http://127.0.0.1:${port}/json/new?about:blank`,
  {method: "PUT", signal: AbortSignal.timeout(10000)},
)).json();
const socket = new WebSocket(target.webSocketDebuggerUrl);
await new Promise((resolve, reject) => {
  socket.addEventListener("open", resolve, {once: true});
  socket.addEventListener("error", reject, {once: true});
});
let sequence = 0;
const pending = new Map();
const errors = [];
socket.addEventListener("message", (event) => {
  const message = JSON.parse(event.data);
  if (message.id && pending.has(message.id)) {
    pending.get(message.id)(message);
    pending.delete(message.id);
  } else if (message.method === "Runtime.exceptionThrown") {
    errors.push(message.params.exceptionDetails.text);
  }
});
const send = (method, params = {}) => new Promise((resolve) => {
  const id = ++sequence;
  pending.set(id, resolve);
  socket.send(JSON.stringify({id, method, params}));
});
const evaluate = async (expression, awaitPromise = false) => {
  const reply = await send("Runtime.evaluate", {expression, returnByValue: true, awaitPromise});
  if (reply.error || reply.result?.exceptionDetails) throw new Error(JSON.stringify(reply));
  return reply.result.result.value;
};
try {
  await send("Page.enable");
  await send("Runtime.enable");
  await send("Page.navigate", {url});
  const result = await evaluate(`new Promise((resolve, reject) => {
    const started = performance.now();
    const deadline = started + 30000;
    const ready = () => {
      if (typeof activateFeatures === "function") {
        activateFeatures();
        return waitForRows();
      }
      if (performance.now() > deadline) return reject(new Error("dashboard script unavailable"));
      setTimeout(ready, 50);
    };
    const waitForRows = () => {
      if (state.featureRows.length && !state.featureInFlight) {
        return measure();
      }
      if (performance.now() > deadline) return reject(new Error("feature inventory unavailable"));
      setTimeout(waitForRows, 50);
    };
    const measure = async () => {
      const loadedMs = performance.now() - started;
      const input = document.getElementById("feature-search");
      const samples = [];
      for (const query of ["t", "tw", "twpub", "price", "台股"]) {
        const begin = performance.now();
        input.value = query;
        input.dispatchEvent(new Event("input", {bubbles: true}));
        const syncMs = performance.now() - begin;
        await new Promise((done) => requestAnimationFrame(() => requestAnimationFrame(done)));
        samples.push({query, syncMs, paintOpportunityMs: performance.now() - begin});
      }
      const exactSearchParity = ["t", "tw", "twpub", "price", "台股", "\\0"].every((query) => {
        input.value = query;
        const actual = filteredFeatures();
        const lowerQuery = query.trim().toLowerCase();
        const expected = state.featureRows.filter((row) =>
          [row.field, row.dataset_id, row.source_title, row.provider, row.market_category_label]
            .some((value) => String(value || "").toLowerCase().includes(lowerQuery)));
        return actual.length === expected.length && actual.every((row, index) => row === expected[index]);
      });
      resolve({rows: state.featureRows.length,
        indexedRows: Array.isArray(state.featureSearchIndex) ? state.featureSearchIndex.length : 0,
        loadedMs, samples, exactSearchParity,
        horizontalOverflow: document.documentElement.scrollWidth - document.documentElement.clientWidth,
        api: performance.getEntriesByType("resource")
          .filter((entry) => entry.name.includes("/api/features"))
          .map((entry) => ({durationMs: entry.duration, transferSize: entry.transferSize,
            decodedBodySize: entry.decodedBodySize}))});
    };
    ready();
  })`, true);
  const conditionalRefresh = await evaluate(`(async () => {
    const rows = state.featureRows;
    const index = state.featureSearchIndex;
    const firstRow = document.querySelector("#feature-rows tr");
    const count = document.getElementById("feature-count").textContent;
    const etagPresent = Boolean(state.featureETag);
    await refreshFeatures();
    const timing = Dashboard.performanceSnapshot().filter((row) =>
      row.path === "/data-monitor/api/features").at(-1);
    return {etagPresent, status: timing?.status ?? null,
      outcome: timing?.outcome ?? null,
      reusedRows: rows === state.featureRows,
      reusedSearchIndex: index === state.featureSearchIndex,
      preservedFirstRow: firstRow === document.querySelector("#feature-rows tr"),
      preservedCount: count === document.getElementById("feature-count").textContent};
  })()`, true);
  const receipt = {url, observedAtUtc: new Date().toISOString(), ...result,
    conditionalRefresh, consoleErrors: errors};
  console.log(JSON.stringify(receipt, null, 2));
  if (output) writeFileSync(output, `${JSON.stringify(receipt, null, 2)}\n`);
  if (errors.length || result.horizontalOverflow || result.rows < 1 || result.indexedRows !== result.rows || !result.exactSearchParity ||
      !conditionalRefresh.etagPresent || conditionalRefresh.status !== 304 || conditionalRefresh.outcome !== "ok" ||
      !conditionalRefresh.reusedRows || !conditionalRefresh.reusedSearchIndex ||
      !conditionalRefresh.preservedFirstRow || !conditionalRefresh.preservedCount) process.exitCode = 1;
} finally {
  socket.close();
  await fetch(`http://127.0.0.1:${port}/json/close/${target.id}`);
}

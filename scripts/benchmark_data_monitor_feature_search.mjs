#!/usr/bin/env node
// Read-only, repeatable CPU benchmark for the public feature inventory filter.
import {writeFileSync} from "node:fs";
import {performance} from "node:perf_hooks";

const endpoint = process.argv[2] || "http://127.0.0.1:8770/data-monitor/api/features";
const output = process.argv[3] || "";
const response = await fetch(endpoint);
if (!response.ok) throw new Error(`feature inventory HTTP ${response.status}`);
const decoded = await response.text();
const parseStarted = performance.now();
const payload = JSON.parse(decoded);
const parseMs = performance.now() - parseStarted;
if (!Array.isArray(payload.rows)) throw new Error("feature rows are missing");

const rows = payload.rows;
const fields = ["field", "dataset_id", "source_title", "provider", "market_category_label"];
const queries = ["t", "tw", "twpub", "price", "台股"];
let sourceCasingDifferences = 0;
for (const row of rows) {
  for (const field of fields) {
    const value = String(row[field] || "");
    if (value.toLowerCase() !== value.toLocaleLowerCase("zh-Hant")) sourceCasingDifferences += 1;
  }
}
if (sourceCasingDifferences) {
  throw new Error(`${sourceCasingDifferences} source strings change under default casing`);
}
const select = (lower) => queries.map((query) => {
  const selected = [];
  rows.forEach((row, index) => {
    if (fields.some((field) => lower(String(row[field] || "")).includes(query))) {
      selected.push(index);
    }
  });
  return selected;
});
const benchmark = (lower) => {
  const started = performance.now();
  const selected = select(lower);
  return {elapsedMs: performance.now() - started, selected};
};

const previous = benchmark((value) => value.toLocaleLowerCase("zh-Hant"));
const current = benchmark((value) => value.toLowerCase());
const indexStarted = performance.now();
const searchIndex = rows.map((row) => fields.map((field) => String(row[field] || "").toLowerCase()).join("\0"));
const indexBuildMs = performance.now() - indexStarted;
const indexedStarted = performance.now();
const indexed = queries.map((query) => {
  const selected = [];
  searchIndex.forEach((haystack, index) => {
    if (haystack.includes(query)) selected.push(index);
  });
  return selected;
});
const indexedSearchMs = performance.now() - indexedStarted;
for (let queryIndex = 0; queryIndex < queries.length; queryIndex += 1) {
  const before = previous.selected[queryIndex];
  const after = current.selected[queryIndex];
  if (before.length !== after.length || before.some((value, index) => value !== after[index])) {
    throw new Error(`search result mismatch for ${queries[queryIndex]}`);
  }
  if (before.length !== indexed[queryIndex].length || before.some((value, index) => value !== indexed[queryIndex][index])) {
    throw new Error(`indexed search result mismatch for ${queries[queryIndex]}`);
  }
}
const result = {
  endpoint,
  observedAtUtc: new Date().toISOString(),
  rows: rows.length,
  decodedBytes: Buffer.byteLength(decoded),
  parseMs,
  queries,
  matched: previous.selected.map((selected) => selected.length),
  sourceCasingDifferences,
  previousLocaleLowercaseMs: previous.elapsedMs,
  currentDefaultLowercaseMs: current.elapsedMs,
  indexBuildMs,
  indexedSearchMs,
  exactResultsEqual: true,
  scope: "Node CPU filter only; not browser paint, network p95, or backend rebuild",
};
console.log(JSON.stringify(result, null, 2));
if (output) writeFileSync(output, `${JSON.stringify(result, null, 2)}\n`);

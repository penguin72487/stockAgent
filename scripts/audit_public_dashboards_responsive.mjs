#!/usr/bin/env node
/** Run the real-browser dashboard audit across the supported CSS-pixel matrix. */

import {spawnSync} from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import {fileURLToPath} from "node:url";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const auditScript = path.join(scriptDir, "audit_public_dashboards_browser.mjs");
const port = String(process.argv[2] || "9222");
const baseUrl = String(process.argv[3] || "https://penguin72487.ddnsgeek.com");
const outputRoot = String(process.argv[4] || "/tmp/stockagent-dashboard-responsive-audit");
const requestedPages = String(process.argv[5] || "");

const profiles = Object.freeze([
  {id: "phone-compact-portrait", width: 320, height: 568, dpr: 2, mobile: true},
  {id: "phone-portrait", width: 390, height: 844, dpr: 3, mobile: true},
  {id: "phone-landscape", width: 844, height: 390, dpr: 2, mobile: true},
  {id: "tablet-portrait", width: 768, height: 1024, dpr: 2, mobile: true},
  {id: "tablet-landscape", width: 1024, height: 768, dpr: 2, mobile: true},
  {id: "laptop-720p", width: 1280, height: 720, dpr: 1, mobile: false},
  {id: "laptop-standard", width: 1366, height: 768, dpr: 1, mobile: false},
  {id: "laptop-hidpi", width: 1280, height: 800, dpr: 2, mobile: false},
  {id: "desktop-1080p", width: 1920, height: 1080, dpr: 1, mobile: false},
  {id: "desktop-2k", width: 2560, height: 1440, dpr: 1, mobile: false},
  {id: "desktop-ultrawide", width: 2560, height: 1080, dpr: 1, mobile: false},
]);

fs.mkdirSync(outputRoot, {recursive: true});
const results = [];
let failed = false;

for (const profile of profiles) {
  const outputDir = path.join(outputRoot, profile.id);
  const child = spawnSync(process.execPath, [
    auditScript,
    port,
    baseUrl,
    outputDir,
    String(profile.width),
    String(profile.height),
    requestedPages,
    String(profile.dpr),
    String(profile.mobile),
  ], {encoding: "utf8", maxBuffer: 16 * 1024 * 1024});
  const reportPath = path.join(outputDir, `report-${profile.width}x${profile.height}.json`);
  let rows = [];
  if (fs.existsSync(reportPath)) rows = JSON.parse(fs.readFileSync(reportPath, "utf8"));
  const gates = {
    auditErrors: rows.filter((row) => row.auditError).length,
    documentOverflow: rows.filter((row) => row.horizontalOverflow > 0).length,
    navigationOverflow: rows.filter((row) => row.navigationOverflow?.length).length,
    laptopTableOverflow: rows.filter((row) => row.laptopTableOverflow?.length).length,
    smallTargets: rows.reduce((sum, row) => sum + Number(row.smallTargets?.length || 0), 0),
    mobileTouchTargetRisks: profile.mobile
      ? rows.reduce((sum, row) => sum + Number(row.touchTargetRisks?.length || 0), 0)
      : 0,
    clippedControls: rows.reduce((sum, row) => sum + Number(row.clippedInteractive?.length || 0), 0),
    overlappingControls: rows.reduce((sum, row) => sum + Number(row.overlappingTargets?.length || 0), 0),
    consoleErrors: rows.reduce((sum, row) => sum + Number(row.consoleErrors?.length || 0), 0),
    failedApis: rows.reduce((sum, row) => sum + Number(row.failedApi?.length || 0), 0),
    timingErrors: rows.reduce((sum, row) => sum + Number(row.apiTimingErrors?.length || 0), 0),
  };
  const status = child.status ?? 1;
  if (status !== 0) failed = true;
  results.push({...profile, status, reportPath, pages: rows.length, gates});
  process.stdout.write(`${profile.id} ${profile.width}x${profile.height}@${profile.dpr} status=${status}\n`);
  if (status !== 0 && child.stderr) process.stderr.write(child.stderr.slice(-4000));
}

const receipt = {
  schemaVersion: 1,
  generatedAt: new Date().toISOString(),
  baseUrl,
  requestedPages: requestedPages ? requestedPages.split(",").filter(Boolean) : "all",
  profiles: results,
  passed: !failed,
};
const receiptPath = path.join(outputRoot, "responsive-audit.json");
fs.writeFileSync(receiptPath, `${JSON.stringify(receipt, null, 2)}\n`);
process.stdout.write(`${receiptPath}\n`);
if (failed) process.exitCode = 1;

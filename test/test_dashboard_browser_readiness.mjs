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

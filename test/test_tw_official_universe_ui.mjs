import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import test from "node:test";

const app = fs.readFileSync(new URL("../services/tw_day_trade_dashboard/app.js", import.meta.url), "utf8");
const context = {number: value => String(value), shortTime: value => value};
vm.runInNewContext(app.slice(app.indexOf("function officialUniverseDescription("), app.indexOf("function pendingOpeningAttempts(")), context);

test("official live membership is dated and is not relabelled original signal coverage", () => {
  const description = context.officialUniverseDescription({status: "ready", trading_count: 2293, listed_count: 2311, halted_count: 18, observed_at_taipei: "2026-10-13T09:51:11+08:00"});
  assert.equal(description.value, "2293 檔");
  assert.match(description.note, /2311 檔 − 停止交易 18 檔/);
  assert.match(description.note, /09:51:11/);
  assert.match(description.note, /非當日原始訊號覆蓋率/);
});

test("missing official receipt never falls back to 2303 or claims ready", () => {
  for (const scope of [null, {}, {status: "waiting_source", trading_count: 2303}]) {
    assert.equal(context.officialUniverseDescription(scope).value, "等待官方名冊");
    assert.equal(context.officialUniverseDescription(scope).kind, "warn");
  }
});

import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import test from "node:test";

const source = fs.readFileSync(new URL("../services/tw_day_trade_dashboard/app.js", import.meta.url), "utf8");
const helpers = source.slice(source.indexOf("function pendingOpeningAttempts("), source.indexOf("function renderOvernightOperations("));
function context(overnight = false) {
  const state = {IS_OVERNIGHT: overnight, number: value => Number(value).toLocaleString("en-US")};
  vm.runInNewContext(helpers, state);
  return state;
}

test("current published models are not shown as still waiting for input", () => {
  const ui = context();
  const pending = ui.pendingOpeningAttempts({opening_signal_latency: {
    observed_markets: ["ready"], latest_failures: [{market: "ready"}, {market: "waiting"}],
  }});
  assert.equal(pending.length, 1);
  assert.equal(pending[0].market, "waiting");
});

test("opening coverage message distinguishes real opens from no-print proof", () => {
  const message = context().openingWaitDescription({reason_code: "tw_opening_input_coverage_pending",
    opening_input_coverage: {opens: 1889, no_prints: 1, required: 2061}});
  assert.match(message, /1,890／門檻 2,061/);
  assert.match(message, /有效開盤價 1,889、明確未成交 1/);
  assert.match(message, /不以昨日價格替代/);
});

test("shared overnight UI does not borrow opening waits from day trade", () => {
  assert.equal(context(true).pendingOpeningAttempts({opening_signal_latency: {
    latest_failures: [{market: "waiting"}],
  }}).length, 0);
});

test("source write lease remains a wait, not a successful signal", () => {
  assert.match(context().openingWaitDescription({reason_code: "tw_public_source_update_pending"}), /不是訊號已完成/);
});

import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import test from "node:test";

const source = fs.readFileSync(new URL("../services/tw_day_trade_dashboard/app.js", import.meta.url), "utf8");
const helpers = source.slice(source.indexOf("function operationalIssuesForDisplay("), source.indexOf("function renderOvernightOperations("));
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

test("latest unfilled revision does not deny earlier open positions", () => {
  const data = {modes: [{market: "mgda", today_execution_outcome: "no_fill", entry_unfilled_shares: 88000, open_position_count: 12}],
    operational_issues: [{market: "mgda", code: "entry_no_fill", detail: "沒有建立任何成交部位"}]};
  const issue = context().operationalIssuesForDisplay(data)[0];
  assert.match(issue.detail, /88,000 股未成交/);
  assert.match(issue.detail, /12 筆先前建立的未平模擬持倉/);
  assert.doesNotMatch(issue.detail, /沒有建立任何成交部位/);
  assert.equal(data.operational_issues[0].detail, "沒有建立任何成交部位");
});

test("late replay input is disclosed once and is not called an actual fill", () => {
  const mode = {market: "a", label: "A", counterfactual_open_replay: true,
    source_signal_at: "2026-10-28T09:10:03+08:00", signal_at: "2026-10-28T09:01:00+08:00"};
  const issue = context().operationalIssuesForDisplay({modes: [mode]})[0];
  assert.equal(issue.code, "replay_signal_after_simulated_time");
  assert.match(issue.detail, /不能視為當時已取得的訊號或即時成交/);
  assert.equal(context().operationalIssuesForDisplay({modes: [mode], operational_issues: [issue]}).length, 1);
  assert.equal(context().operationalIssuesForDisplay({modes: [{...mode, source_signal_at: "2026-10-28T09:00:12+08:00"}]}).length, 0);
});

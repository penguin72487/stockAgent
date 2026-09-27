const assert = require("node:assert/strict");
const {test} = require("node:test");
require("../../services/tw_day_trade_dashboard/presentation.js");
const {modeOperationallyReady, accountAdjustments} = globalThis.StockAgentTwPresentation;
const complete = {
  market: "mode_a", checkpoint_ready: true, total_equity_twd: 10_000_000,
  engine_status: "historical_session_complete", today_execution_status: "completed",
  today_execution_outcome: "filled",
};

test("a loaded checkpoint does not make missed execution healthy", () => {
  assert.equal(modeOperationallyReady({...complete, engine_status: "historical_session_missed"}), false);
  assert.equal(modeOperationallyReady({...complete, today_execution_status: "missed"}), false);
  assert.equal(modeOperationallyReady({...complete, total_equity_twd: null}), false);
});

test("residuals and unfilled requests stay visible", () => {
  assert.equal(modeOperationallyReady({...complete, engine_status: "historical_session_closed_with_residual"}), false);
  assert.equal(modeOperationallyReady({...complete, today_execution_outcome: "partial"}), false);
  assert.equal(modeOperationallyReady({...complete, today_execution_outcome: "no_fill"}), false);
});

test("only the mode's own operational warnings affect its badge", () => {
  assert.equal(modeOperationallyReady(complete), true);
  assert.equal(modeOperationallyReady(complete, [{market: "mode_a", severity: "warning"}]), false);
  assert.equal(modeOperationallyReady(complete, [{market: "mode_b", severity: "error"}]), true);
  assert.equal(modeOperationallyReady({...complete, today_execution_outcome: "no_order"}), true);
});

test("account reconciliation includes carry costs and corporate actions once", () => {
  assert.deepEqual(accountAdjustments({account_performance: {
    cumulative_carry_cost_twd: 300, cumulative_corporate_action_net_twd: 40,
    // Receivables are already included in corporate-action net PnL.
    corporate_action_receivable_twd: 20,
  }}), {carryCost: 300, corporateNet: 40, net: -260});
  assert.deepEqual(accountAdjustments({}), {carryCost: 0, corporateNet: 0, net: 0});
});

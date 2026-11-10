import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import vm from "node:vm";

const source = readFileSync(new URL("../services/tw_day_trade_dashboard/research-candidates.js", import.meta.url), "utf8");
const data = () => ({schema_version: 1, candidates: [{market: "2m", label: "研究候選", initial_capital_twd: 2_000_000,
  status: "research_only_not_enabled", paper_execution_enabled: false, live_signal_available: false, simulation_only: true,
  broker_fill: false, year: 2026, research_status: "available", start_date: "2026-01-02", end_date: "2026-01-05",
  session_count: 2, cumulative_return_pct: -12, daily: [{date: "2026-01-02", daily_return_pct: 10, cumulative_return_pct: 10},
    {date: "2026-01-05", daily_return_pct: -20, cumulative_return_pct: -12}]}]});
function api() { const context = vm.createContext({Intl}); vm.runInContext(source, context); return context.StockAgentResearchCandidates; }

test("research preview remains distinct from enabled accounts and live signals", () => {
  const {validate, render} = api();
  validate(data());
  const html = render(data(), x => x);
  assert.match(html, /尚未啟用紙上執行/);
  assert.match(html, /未提供即時訊號或成交/);
  assert.match(html, /2026-01-02～2026-01-05/);
  assert.match(html, /-12\.00%/);
  assert.match(html, /不是盤中逐版再平衡績效/);
  assert.match(html, /最新穿價全數出場規則未反映在此原回測/);
  for (const [field, value] of [["paper_execution_enabled", true], ["broker_fill", true], ["live_signal_available", true]]) {
    const changed = data(); changed.candidates[0][field] = value;
    assert.throws(() => validate(changed));
  }
});

test("configured but unconfirmed is not called an active or disabled account", () => {
  const {validate, render} = api();
  const pending = data();
  pending.candidates[0].status = "research_only_pending_paper";
  pending.candidates[0].paper_execution_enabled = null;
  validate(pending);
  const html = render(pending, x => x);
  assert.match(html, /紙上帳戶狀態尚待核對/);
  assert.doesNotMatch(html, /尚未啟用紙上執行/);
});

test("rejects invalid date and incomplete daily values", () => {
  const {validate} = api();
  const future = data(); future.candidates[0].daily[1].date = "2025-01-05";
  assert.throws(() => validate(future));
  const missing = data(); missing.candidates[0].daily.pop();
  assert.throws(() => validate(missing));
});

test("provisional replay is separate from live execution and discloses assumptions", () => {
  const {validate, render} = api();
  const value = data();
  value.candidates[0].provisional_replay = {status: "available", start_date: "2026-01-02", end_date: "2026-01-02",
    session_count: 1, minute_count: 270, carried_mark_minutes: 12, full_cross_assumptions: 2,
    nontrade_assumptions: 1, end_equity_twd: 2_100_000, cumulative_return_pct: 5,
    daily: [{date: "2026-01-02", end_equity_twd: 2_100_000, daily_return_pct: 5,
             cumulative_return_pct: 5, carried_mark_minutes: 12}]};
  validate(value);
  const html = render(value, x => x);
  assert.match(html, /暫定反事實回放（非即時帳本）/);
  assert.match(html, /資料可能不完整、結果可能改變/);
  assert.match(html, /12 個分鐘點含延用估值/);
  assert.match(html, /不是當時已發布、盤中逐版再平衡、交易所或券商成交/);
  assert.throws(() => validate({...value, candidates: [{...value.candidates[0],
    provisional_replay: {...value.candidates[0].provisional_replay, minute_count: 269}}]}));
});

test("minute view accepts only fully disclosed, matching 270-point provisional sessions", () => {
  const {validateMinute} = api();
  const row = {schema_version: 1, status: "available", market: "2m", session_date: "2026-01-02",
    first_minute: "2026-01-02T09:01:00+08:00", interval_seconds: 60, simulation_only: true,
    provisional: true, broker_fill: false, exchange_fill: false, source_completeness_verified: false,
    equity_twd: Array(270).fill(2_000_000), valuation_stale: Array(270).fill(false)};
  assert.equal(validateMinute(row, "2m", "2026-01-02"), row);
  for (const changed of [
    {...row, equity_twd: row.equity_twd.slice(1)}, {...row, broker_fill: true},
    {...row, valuation_stale: Array(270).fill(0)}, {...row, source_completeness_verified: true},
    {...row, equity_twd: [NaN, ...row.equity_twd.slice(1)]}, {...row, first_minute: "2026-01-02T09:00:00+08:00"},
  ]) assert.throws(() => validateMinute(changed, "2m", "2026-01-02"));
  assert.throws(() => validateMinute(row, "different", "2026-01-02"));
});

test("minute selector is only offered for an independently verified archive", () => {
  const {validate, render} = api();
  const value = data();
  value.candidates[0].provisional_replay = {status: "available", start_date: "2026-01-02", end_date: "2026-01-02",
    session_count: 1, minute_count: 270, carried_mark_minutes: 0, full_cross_assumptions: 0,
    nontrade_assumptions: 0, end_equity_twd: 2_100_000, cumulative_return_pct: 5,
    daily: [{date: "2026-01-02", end_equity_twd: 2_100_000, daily_return_pct: 5,
             cumulative_return_pct: 5, carried_mark_minutes: 0}]};
  assert.doesNotMatch(render(value, x => x), /data-research-minutes/);
  value.candidates[0].provisional_replay.minute_series_available = true;
  validate(value);
  assert.match(render(value, x => x), /查看暫定每分鐘資產曲線/);
  value.candidates[0].provisional_replay.minute_series_available = "true";
  assert.throws(() => validate(value));
});

test("counterfactual signals remain separate from live positions and retain absolute-weight order", () => {
  const {validate, render} = api();
  const value = data();
  value.candidates[0].counterfactual_signal = {status: "available", session_date: "2026-11-09",
    generated_at: "2026-11-09T14:35:05+08:00", signal_count: 2, counterfactual: true,
    live_signal: false, broker_fill: false, execution_and_minute_coverage_verified: false,
    signals: [{symbol: "6488", name: "乙", target_weight_pct: -20, tradable: true, can_buy: true, can_sell: true},
      {symbol: "2330", name: "甲", target_weight_pct: 10, tradable: true, can_buy: true, can_sell: true}]};
  validate(value);
  const html = render(value, x => x);
  assert.match(html, /盤後重建訊號（非即時訊號）/);
  assert.match(html, /不是當時已發布的訊號/);
  assert.match(html, /尚未確認該日分鐘回放與模擬成交/);
  assert.match(html, /2026-11-09 14:35:05 台北時間/);
  assert.ok(html.indexOf("6488") < html.indexOf("2330"));
  value.candidates[0].counterfactual_signal.live_signal = true;
  assert.throws(() => validate(value));
});

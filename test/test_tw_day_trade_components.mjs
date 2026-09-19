import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import vm from "node:vm";

function components() {
  const context = vm.createContext({});
  for (const file of ["presentation.js", "detail-components.js"]) {
    vm.runInContext(readFileSync(new URL(`../services/tw_day_trade_dashboard/${file}`, import.meta.url), "utf8"), context);
  }
  const presentation = context.StockAgentTwPresentation;
  const modes = [{market: "old_account_id", label: "LayerNorm v12 當沖"}];
  const escapeHtml = (value) => String(value ?? "—").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll('"', "&quot;");
  const nodes = new Map();
  const byId = (id) => {
    if (!nodes.has(id)) nodes.set(id, {textContent: "", html: "", attrs: {}, classes: {},
      setAttribute(k, v) { this.attrs[k] = v; },
      classList: {toggle(k, v) { nodes.get(id).classes[k] = v; }},
    });
    return nodes.get(id);
  };
  const format = Object.fromEntries(["number", "pct", "money", "sourceNumber", "summaryMoney", "displayPct", "shortTime", "replayTimingText", "signalTimingText"].map((key) => [key, () => "0"]));
  format.badge = (text) => `<span>${escapeHtml(text)}</span>`;
  format.pnlClass = () => "positive";
  format.signalReasonLabel = presentation.signalReasonLabel;
  const view = context.StockAgentTwDetailComponents.create({
    Dashboard: {byId, escapeHtml, setTrustedHtml: (id, html) => { byId(id).html = html; }},
    strategyLabel: (row) => presentation.strategyLabel(row, modes),
    signalRowKey: (row) => `stable-key-${row.symbol}`,
    resolvedPositionPnl: () => ({signedShares: 1000, realized: 0, unrealized: 100, total: 100}),
    format,
  });
  return {view, presentation, modes, byId};
}

test("all detail rows use the active strategy label, not the immutable account ID", () => {
  const {view, modes} = components();
  const row = {market: "old_account_id", symbol: "2330", name: "<script>alert(1)</script>", status: "hold"};
  for (const render of [view.signalRow, view.positionRow, view.eventRow]) {
    const html = render(row);
    assert.match(html, /LayerNorm v12/);
    assert.doesNotMatch(html, /old_account_id|<script>/);
  }
  modes[0].label = "下一版策略";
  assert.match(view.signalRow(row), /下一版策略/);
  assert.doesNotMatch(view.signalRow(row), /LayerNorm/);
});

test("unknown strategy or policy never silently claims another deployment", () => {
  const {presentation} = components();
  assert.equal(presentation.strategyLabel("missing", []), "策略名稱未提供");
  assert.match(presentation.entryPolicy({entry_fill_policy: "new_policy"}), /未辨識成交規則/);
  assert.match(presentation.entryPolicy({entry_fill_policy: "constructor"}), /未辨識成交規則/);
  assert.match(presentation.entryPolicy({entry_fill_policy: "official_open_signal_0900_execute_0901_vwap"}), /09:01/);
});

test("every signal row has an explicit dated futures badge, including unknown", () => {
  const {view, presentation} = components();
  for (const [status, label] of [["listed", "有期貨"], ["not_listed", "無期貨"], ["unknown", "期貨未確認"]]) {
    const html = view.signalRow({symbol: "2330", stock_futures: {status, products: ["CD"], as_of: "2026-09-06T13:00:00Z"}});
    assert.match(html, new RegExp(label));
    assert.match(html, /2026-09-06/);
    assert.match(html, new RegExp(`data-futures-status="${status}"`));
  }
  assert.match(view.signalRow({symbol: "1101"}), /期貨未確認/);
  assert.match(view.futuresBadge({status: "listed", products: ['<img src=x onerror="alert(1)">']}), /&lt;img/);
  assert.equal(presentation.futuresPresentation({status: "constructor"}).status, "unknown");
});

test("missed opening replay is not labelled as a live market-order fill", () => {
  const {view} = components();
  const html = view.signalRow({
    symbol: "2330", market: "old_account_id", side: "long", target_weight: 0.1,
    requested_shares: 17000, filled_shares: 1000,
    counterfactual_0901_price_fill: true,
    entry_fill_policy: "official_open_signal_0900_execute_0901_vwap",
    minute_kbar_capacity_shares: 1000, execution_price: 100,
  });
  assert.match(html, /09:01 首分鐘價反事實模擬/);
  assert.match(html, /容量限制未建立委託/);
  assert.doesNotMatch(html, /市價成交/);
});

test("one paged-table shell preserves rows during errors and owns loading/count state", () => {
  const {view, byId} = components();
  view.pagedTable({id: "signal", rows: [1], total: 30, loading: true, hasMore: true, error: "<bad>",
    renderRow: () => "<tr>existing row</tr>", emptyText: "empty"});
  assert.equal(byId("signal-body").attrs["aria-busy"], "true");
  assert.match(byId("signal-body").html, /existing row/);
  assert.match(byId("signal-body").html, /&lt;bad>/);
  assert.equal(byId("load-more-signals").disabled, true);
  assert.equal(byId("load-more-signals").classes.hidden, false);
  view.pagedTable({id: "signal", rows: [], total: 0, loading: false, hasMore: false, error: "讀取失敗",
    renderRow: () => "", emptyText: "目前沒有訊號"});
  assert.match(byId("signal-body").html, /讀取失敗/);
  assert.doesNotMatch(byId("signal-body").html, /目前沒有訊號/);
});

test("mode card keeps filtered returns separate from the shared Discord account DTO", () => {
  const app = readFileSync(new URL("../services/tw_day_trade_dashboard/app.js", import.meta.url), "utf8");
  const source = app.slice(app.indexOf("function renderModes(data)"), app.indexOf("function renderBenchmarks(data)"));
  let output = "";
  let selectedReturn = 2;
  const formatters = Object.fromEntries(["esc", "strategyLabel", "engineStatusShortLabel", "badge", "pnlClass", "summaryMoney", "number", "money", "shortTime", "sourceNumber"].map((key) => [key, String]));
  const context = vm.createContext({...formatters, IS_OVERNIGHT: false,
    rangeSummaryFor: () => ({return_pct: selectedReturn, end_equity_twd: 1250, cumulative_net_pnl_twd: 250, initial_capital_twd: 1000}),
    executionStatusPresentation: () => ({kind: "good", label: "done"}),
    fillOutcomePresentation: () => ({kind: "good", label: "done"}),
    Presentation: {entryPolicy: () => "causal"}, displayPct: (value) => `${Number(value).toFixed(2)}%`,
    setHtml: (_id, html) => { output = html; },
  });
  vm.runInContext(source, context);
  const mode = {market: "unit", account_performance: {status: "available", return_pct: 25, asof: "2026-09-09", state_revision: 10}};
  context.renderModes({modes: [mode]});
  assert.match(output, />\+2\.00%</);
  assert.match(output, /帳戶累積報酬（Discord 同口徑）/);
  assert.match(output, />\+25\.00%</);
  selectedReturn = 3;
  context.renderModes({modes: [mode]});
  assert.match(output, />\+3\.00%</);
  assert.match(output, />\+25\.00%</);
  mode.account_performance = {status: "unavailable"};
  context.renderModes({modes: [mode]});
  assert.match(output, /資料不可用/);
  assert.doesNotMatch(output, />\+25\.00%</);
});

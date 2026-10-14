import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import vm from "node:vm";

function view(product = "tw_day_trade") {
  const context = vm.createContext({});
  vm.runInContext(readFileSync(new URL("../services/tw_day_trade_dashboard/detail-components.js", import.meta.url), "utf8"), context);
  const esc = (value) => String(value ?? "—").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll('"', "&quot;");
  const format = Object.fromEntries(["number", "pct", "money", "sourceNumber", "summaryMoney", "displayPct", "shortTime", "replayTimingText", "signalTimingText", "signalReasonLabel"].map((key) => [key, (value) => value == null ? "—" : esc(value)]));
  Object.assign(format, {badge: esc, pnlClass: () => ""});
  // Counts use integer formatting; quote display must not accidentally reuse it.
  format.number = (value) => value == null ? "—" : String(Math.round(Number(value)));
  return context.StockAgentTwDetailComponents.create({Dashboard: {escapeHtml: esc},
    strategyLabel: () => "模擬", signalRowKey: () => "key",
    resolvedPositionPnl: () => ({signedShares: 0, realized: 123, unrealized: 0, total: 123}),
    format, product});
}

const daily = {session_date: "2026-09-21", open: 2445, close: 2480,
  received_at: "2026-09-22T09:13:47Z", public_price_display: {visible_fields: {
    open: {source: "臺灣證券交易所", grain: "daily", session_date: "2026-09-21"},
    close: {source: "臺灣證券交易所", grain: "daily", session_date: "2026-09-21"},
  }}};

for (const product of ["tw_day_trade", "tw_overnight"]) {
  test(`${product} shows daily references separately without inventing the private fill`, () => {
    const html = view(product).positionRow({symbol: "2330", status: "closed",
      entry_price: null, exit_price: null, official_daily_quote: daily,
      public_price_display: {withheld_fields: ["entry_price", "exit_price"]}});
    assert.match(html, /官方日行情 2026-09-21 · 開 2445／收 2480/);
    assert.match(html, /取得 09-22 17:13:47（臺北） · 非分鐘成交/);
    assert.match(html, /等待可公開來源/);
    assert.match(html, /已平倉/);
  });
}

test("daily source labels escape markup and hidden daily facts do not render", () => {
  const render = view();
  assert.equal(render.dailyReference({official_daily_quote: {...daily, open: null, close: null}}), "");
  const html = render.dailyReference({official_daily_quote: {...daily,
    public_price_display: {visible_fields: {open: {source: "<script>alert(1)</script>"}}}}});
  assert.ok(!html.includes("<script>"));
  assert.ok(html.includes("&lt;script>"));
  assert.ok(!html.includes("收 2480"), "one field's proof cannot expose another field");
});

for (const product of ["tw_day_trade", "tw_overnight"]) {
  test(`${product} labels official after-close prices and next-session bounds distinctly`, () => {
    const values = {last_bid: 96.1, last_ask: 96.2, average_price: 96.15,
      next_reference_price: 96.2, next_upper_limit: 105.5, next_lower_limit: 86.6};
    const quote = {...daily, ...values, open: null, close: null, volume_shares: 0, trade_count: 0,
      public_price_display: {visible_fields: Object.fromEntries(Object.keys(values)
        .map(key => [key, {source: "證券櫃檯買賣中心", grain: "daily"}]))}};
    const html = view(product).dailyReference({official_daily_quote: quote});
    assert.match(html, /盤後最後買 96\.1／盤後最後賣 96\.2/);
    assert.match(html, /次一交易日參考 96\.2／次一交易日申報上界 105\.5／下界 86\.6/);
    assert.match(html, /成交股數 0股／成交筆數 0筆/);
    assert.match(html, /<summary>完整官方日資料／次日界線<\/summary>/);
    assert.ok(!html.includes("當日漲停"));
    assert.ok(!html.includes("undefined"));
    quote.next_upper_limit = 9999.95;
    quote.next_lower_limit = 0.01;
    assert.match(view(product).dailyReference({official_daily_quote: quote}), /上界 9999\.95／下界 0\.01/);
  });
}

test("daily fields require their own proof and unknown proof keys do not create rows", () => {
  const render = view();
  const quote = {...daily, last_bid: 91.2, next_upper_limit: 120};
  const html = render.dailyReference({official_daily_quote: quote});
  assert.ok(!html.includes("91.2"));
  assert.ok(!html.includes("120"));
  quote.public_price_display = {visible_fields: {arbitrary: {source: "not a price"}}};
  quote.arbitrary = 1;
  assert.equal(render.dailyReference({official_daily_quote: quote}), "");
});

test("prior official references cannot be mistaken for today's prices", () => {
  const render = view();
  const quote = {...daily, prior_session_reference_only: true, requested_session_date: "2026-09-22"};
  assert.match(render.dailyReference({official_daily_quote: quote}), /2026-09-22 日行情待發布，非當日報價/);
  const notice = render.priceNoticeText({official_price_reference: {session_date: "2026-09-21",
    requested_session_date: "2026-09-22", quotes: [{symbol: "2330", official_daily_quote: quote}]}});
  assert.match(notice, /2026-09-22 日行情待發布或待驗收/);
  assert.match(notice, /官方日行情 2026-09-21：2330 開 2445／收 2480/);
});

test("daily proof shows market day instead of pretending the acquisition clock is a tick", () => {
  const html = view().quoteText({sizing_open_price: 2445, public_price_display: {visible_fields: {
    sizing_open_price: {source: "證交所", grain: "daily", session_date: "2026-09-21", observed_at: "2026-09-22T09:13:47Z"},
  }}}, "sizing_open_price");
  assert.match(html, /官方日價核對 2026-09-21/);
  assert.ok(!html.includes("2026-09-22T09:13:47Z"));
});

test("an empty strategy can show independent official quotes without a readiness claim", () => {
  const text = view().priceNoticeText({price_display_policy: {message: "fallback"},
    official_price_reference: {session_date: "2026-09-21", quotes: [{symbol: "2330", official_daily_quote: daily}]}});
  assert.match(text, /2330 開 2445／收 2480/);
  assert.match(text, /日行情不是分鐘成交或即時買賣價/);
  assert.ok(!text.includes("即時正常"));
  assert.equal(view().priceNoticeText({price_display_policy: {message: "source pending"}}), "source pending");
});

test("price notice preserves decimal prices instead of using whole-share formatting", () => {
  const text = view().priceNoticeText({official_price_reference: {session_date: "2026-10-13",
    quotes: [{symbol: "0050", official_daily_quote: {...daily, open: 112.55, close: 112.4}}]}});
  assert.match(text, /0050 開 112\.55／收 112\.4/);
});

for (const product of ["tw_day_trade", "tw_overnight"]) {
  test(`${product} distinguishes permission pending from missing prices`, () => {
    const row = {symbol: "2330", status: "closed", entry_price: null, exit_price: null,
      official_daily_quote: daily, public_price_display: {
        withheld_fields: ["entry_price", "exit_price"], withheld_reasons: {
          entry_price: "public_display_permission_unverified", exit_price: "public_display_permission_unverified",
        }}};
    const render = view(product);
    assert.match(render.quoteText(row, "entry_price"), /公開展示授權待確認/);
    assert.match(render.positionRow(row), /已平倉・公開展示授權待確認/);
    assert.ok(!render.positionRow(row).includes("等待可公開來源"));
    assert.match(render.positionRow(row), /官方日行情 2026-09-21/);
    const invalid = {entry_price: null, public_price_display: {withheld_fields: ["entry_price"],
      withheld_reasons: {entry_price: "public_observation_not_verified"}}};
    assert.match(render.quoteText(invalid, "entry_price"), /公開價格核對未通過/);
  });
}

test("an independently verified exit remains visible if another exit field is withheld", () => {
  const html = view().positionRow({symbol: "2330", status: "closed", exit_price: 2480,
    last_exit_price: null, public_price_display: {visible_fields: {
      exit_price: {source: "證交所", grain: "daily", session_date: "2026-09-21"},
    }, withheld_fields: ["last_exit_price"]}});
  assert.match(html, /2480<small>官方日價核對/);
  assert.ok(!html.includes("已平倉・等待可公開來源"));
});

test("header states unverified broker display rights without calling data acquisition failed", () => {
  const text = view().priceNoticeText({price_display_policy: {broker_public_display_permission: "unverified"},
    official_price_reference: {session_date: "2026-09-21", quotes: [{symbol: "2330", official_daily_quote: daily}]}});
  assert.match(text, /永豐分鐘／即時價格公開展示授權待確認/);
  assert.ok(!text.includes("行情失敗"));
});

for (const product of ["tw_day_trade", "tw_overnight"]) {
  test(`${product} distinguishes own paper fill reports from licensed quotes and broker execution`, () => {
    const proof = {source: "使用者公開的模擬帳本", publication_basis: "owner_paper_transaction_report_v1",
      evidence_kind: "paper_fill", broker_execution_verified: false, recorded_at: "2026-10-13T09:01:00+08:00"};
    const row = {symbol: "2330", status: "closed", entry_price: 112.12345, exit_price: 115.5,
      public_price_display: {visible_fields: {entry_price: proof, exit_price: proof}}};
    const render = view(product);
    assert.match(render.positionRow(row), /112\.12345<small>模擬帳本成交/);
    assert.match(render.positionRow(row), /115\.5<small>模擬帳本成交/);
    assert.match(render.quoteText(row, "entry_price"), /10-13 09:01:00（臺北） · 非券商成交/);
    assert.ok(!render.quoteText(row, "entry_price").includes("公開行情"));
    assert.ok(!render.quoteText(row, "entry_price").includes("授權"));
  });
}

test("paper report header is explicit and does not imply a raw-feed grant", () => {
  const text = view().priceNoticeText({price_display_policy: {paper_transaction_reports: true,
    broker_public_display_permission: "unverified"}, official_price_reference: {session_date: "2026-09-21",
    quotes: [{symbol: "2330", official_daily_quote: daily}]}});
  assert.match(text, /進出場價為使用者公開的模擬帳本成交，非券商成交/);
  assert.match(text, /永豐分鐘／即時價格公開展示授權待確認/);
});

test("a closed position with only a last fill price is not described as partly closed", () => {
  const html = view().positionRow({symbol: "2330", status: "closed", last_exit_price: 115.5});
  assert.match(html, /115\.5/);
  assert.ok(!html.includes("部分 115.5"));
});

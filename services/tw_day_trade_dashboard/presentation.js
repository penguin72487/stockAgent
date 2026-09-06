"use strict";

/* Pure presentation contracts. Account keys are opaque ledger identities, not
 * strategy names. No model names, stock universes or deployment IDs live here. */
(function installPresentation(global) {
  function strategyLabel(value, modes = []) {
    const market = typeof value === "object" ? value?.market : value;
    const mode = modes.find((item) => item.market === market);
    return mode?.label || (typeof value === "object" && value?.label) || "策略名稱未提供";
  }

  function futuresPresentation(membership = {}) {
    const states = {
      listed: {label: "有期貨", kind: "good"},
      not_listed: {label: "無期貨（名單未列）", kind: ""},
      unknown: {label: "期貨未確認", kind: "warn"},
    };
    const reasons = {
      stale_catalog: "名單過期", future_catalog: "名單時間異常",
      missing_or_invalid_catalog: "缺少有效名單", invalid_symbol: "證券代號未確認",
    };
    const status = Object.hasOwn(states, membership?.status) ? membership.status : "unknown";
    const date = membership?.as_of ? String(membership.as_of).slice(0, 10) : "無日期";
    const products = Array.isArray(membership?.products) ? membership.products.join("、") : "";
    return {...states[status], status,
      detail: [products, `名單 ${date}`, reasons[membership?.reason]].filter(Boolean).join(" · "),
      title: "最新期交所名單；不是訊號當日的期貨資格，也不代表目前可下單或有流動性",
    };
  }

  // Each execution contract owns its current-policy and recorded-policy copy.
  // Unknown contracts are visible instead of silently described as Bid/Ask.
  const ENTRY_POLICIES = {
    official_open_signal_0900_execute_0901_vwap: {
      current: () => "官方 09:00 開盤價供推論／張數，右標記 09:01 首分鐘價供回補執行（VWAP，否則同根 K 棒 Close）；缺 tick 不阻擋，缺整根 K 棒才阻擋",
      recorded: (m, n) => `09:00 官方開盤推論／張數，09:01 首分鐘價執行 ${n(m.entry_0901_minute_price_fill_count || m.entry_0901_vwap_fill_count || m.entry_fill_count || 0)} 筆（VWAP ${n(m.entry_0901_vwap_fill_count || 0)}／K 棒 Close ${n(m.entry_0901_close_fill_count || 0)}；反事實紙上估值，非交易所成交）`,
    },
    official_open_at_09_01: {
      current: () => "09:01 以官方開盤價計算全部紙上買賣；缺價即阻擋，不使用 +1 Tick",
      recorded: (m, n) => `09:01 官方開盤價計價 ${n(m.entry_official_open_fill_count || m.entry_fill_count || 0)} 筆（反事實紙上估值，非交易所成交）`,
    },
    synthetic_open_tick: {
      current: (m, n) => `開盤價不利 ${n(m.configured_entry_price_offset_ticks || 1)} Tick 合成成交`,
      recorded: (m, n) => `開盤價不利 ${n(m.entry_price_offset_ticks || 1)} Tick 合成成交（不回寫成最佳報價）`,
    },
    market_at_best_quote_else_adverse_open_tick: {
      current: (m, n) => `紙上市價買進／回補取最佳 Ask、賣出／放空取最佳 Bid，完整模擬委託；缺報價才用開盤價不利 ${n(m.configured_entry_price_offset_ticks || 1)} Tick`,
      recorded: (m, n) => `紙上市價完整成交 ${n(m.entry_paper_market_fill_count || m.entry_fill_count || 0)} 筆；不宣稱交易所深度或排隊成交`,
    },
    causal_best_quote_else_adverse_open_tick: {
      current: (m, n) => `因果最佳 Bid／Ask；缺報價才用開盤價不利 ${n(m.configured_entry_price_offset_ticks || 1)} Tick`,
      recorded: (m, n) => `歷史最佳 Bid／Ask ${n(m.entry_best_quote_fill_count || 0)} 筆；缺報價才用開盤價不利 ${n(m.entry_price_offset_ticks || 1)} Tick ${n(m.entry_synthetic_fallback_fill_count || 0)} 筆`,
    },
    causal_best_quote: {
      current: () => "09:00 訊號原子發布後，市價買進／回補取第一筆較晚最佳 Ask；市價賣出／放空取第一筆較晚最佳 Bid，且只吃可驗證一檔量；若錯過開盤，另以 09:00 官方開盤推論，並用 09:01 首分鐘價執行（VWAP 優先，否則同根 K 棒 Close）",
      recorded: () => "因果最佳 Bid／Ask 與可驗證一檔量",
    },
  };

  function entryPolicy(mode, number = String) {
    const configured = mode.configured_entry_fill_policy || mode.entry_fill_policy;
    const describe = (id, phase) => Object.hasOwn(ENTRY_POLICIES, id)
      ? ENTRY_POLICIES[id][phase](mode, number) : `未辨識成交規則（${id || "未提供"}）`;
    const active = `目前規則：${describe(configured, "current")}`;
    return configured === mode.entry_fill_policy ? active
      : `${active}；所選交易日紀錄：${describe(mode.entry_fill_policy, "recorded")}`;
  }

const SIGNAL_REASON_LABELS = {
  below_one_board_lot: "資金不足一張（完整訊號已保留）",
  zero_target_weight: "零權重，不下單",
  model_tradable_mask_false: "模型交易遮罩不允許",
  cannot_buy_open: "不可買進開倉",
  cannot_sell_open: "不可賣出開倉",
  exact_session_eligibility_missing: "當日當沖資格資料缺漏",
  not_day_trade_eligible: "不具當日當沖資格",
  sell_first_suspended: "暫停先賣後買",
  official_session_no_trade_print: "官方當日無成交價",
  official_open_price_unavailable: "官方開盤價不可用",
  observed_09_01_minute_vwap_unavailable: "09:01 首分鐘成交價不可用",
  observed_09_01_minute_price_unavailable: "09:01 首分鐘成交價不可用",
  price_limit_unavailable: "合法漲跌停價缺漏",
  no_executable_best_quote: "最佳一檔報價不可用",
  quote_not_after_signal: "報價未晚於訊號",
  quote_after_local_observation: "報價時間超前本機觀測",
  marketable_depth_unavailable: "可成交深度不足",
  marketable_depth_exhausted: "可成交深度僅部分足夠",
  counterfactual_official_open_price_fill_at_09_01: "09:01 開盤價重建",
  counterfactual_observed_09_01_minute_vwap_fill: "09:01 首分鐘價重建",
  counterfactual_observed_09_01_minute_price_fill: "09:01 首分鐘價重建",
};
const signalReasonLabel = (value) => SIGNAL_REASON_LABELS[String(value || "")] || String(value || "").replaceAll("_", " ") || "未提供原因";

  global.StockAgentTwPresentation = Object.freeze({strategyLabel, futuresPresentation, entryPolicy, signalReasonLabel});
})(globalThis);

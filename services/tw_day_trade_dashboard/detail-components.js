"use strict";

/* Stateless detail-row renderers and one shared paged-table shell. Network,
 * filters, cancellation and selected rows remain owned by app.js. */
(function installDetailComponents(global) {
  function create({Dashboard, strategyLabel, signalRowKey, resolvedPositionPnl, format, product = "tw_day_trade"}) {
    const {escapeHtml: esc, byId: $, setTrustedHtml: setHtml} = Dashboard;
    const {number, pct, money, sourceNumber, summaryMoney, displayPct, badge,
      pnlClass, shortTime, replayTimingText, signalTimingText, signalReasonLabel} = format;
    const sourceClockFormat = new Intl.DateTimeFormat("sv-SE", {timeZone: "Asia/Taipei",
      month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23"});
    const sourceClockCache = new Map();
    function sourceClock(value) {
      if (typeof value !== "string" || value.length > 64 || !/(Z|[+-]\d{2}:\d{2})$/.test(value)) return "—";
      if (sourceClockCache.has(value)) return sourceClockCache.get(value);
      const date = new Date(value);
      if (!Number.isFinite(date.getTime())) return "—";
      const parts = Object.fromEntries(sourceClockFormat.formatToParts(date).map((part) => [part.type, part.value]));
      const text = `${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second}`;
      if (sourceClockCache.size >= 128) sourceClockCache.delete(sourceClockCache.keys().next().value);
      sourceClockCache.set(value, text);
      return text;
    }

    const withheld = (row, field) => row?.public_price_display?.withheld_fields?.includes(field) === true;
    function pendingQuoteText(row, fields) {
      const keys = Array.isArray(fields) ? fields : [fields];
      const reasons = keys.filter((key) => withheld(row, key)).map((key) => row?.public_price_display?.withheld_reasons?.[key]);
      if (reasons.includes("public_display_permission_unverified")) return "公開展示授權待確認";
      if (reasons.includes("public_observation_not_verified")) return "公開價格核對未通過";
      return "等待可公開來源";
    }
    function quoteText(row, fields, formatter = money) {
      const keys = Array.isArray(fields) ? fields : [fields];
      const key = keys.find((field) => row?.[field] != null && !withheld(row, field));
      if (!key) return keys.some((field) => withheld(row, field)) ? pendingQuoteText(row, keys) : formatter(null);
      return formatter(row[key]);
    }

    function priceDistanceText(row, fields) {
      const keys = Array.isArray(fields) ? fields : [fields];
      const key = keys.find((field) => row?.[field] != null && !withheld(row, field));
      const metric = key && row?.entry_price_distances?.[key];
      if (!metric || metric.contract !== "public_entry_price_distance_v1"
        || row?.entry_price == null || withheld(row, "entry_price")) return "";
      const signed = (value) => {
        if (value == null || !Number.isFinite(Number(value))) return "—";
        const rounded = Math.round((Number(value) + Number.EPSILON) * 100) / 100;
        return `${rounded > 0 ? "+" : ""}${sourceNumber(Object.is(rounded, -0) ? 0 : rounded)}`;
      };
      return `<small class="price-entry-distance" data-price-field="${esc(key)}">距進場 ${signed(metric.change_pct)}% · ${metric.approximate_ticks ? "約 " : ""}${signed(metric.ticks)} tick</small>`;
    }

    function liquidation(row, signedShares) {
      const closed = row.status === "closed" || (Number(row.filled_shares) > 0 && signedShares === 0);
      return {fields: closed ? ["exit_price", "last_exit_price"] : "last_mark_price",
        label: closed ? "清算（已平倉）" : "清算",
        at: closed ? row.exit_exchange_at || row.exit_at || row.last_exit_at : row.last_quote_at};
    }

    function dailyReference(row) {
      const daily = row?.official_daily_quote;
      if (!daily) return "";
      const proofs = daily.public_price_display?.visible_fields || {};
      const visible = (key) => daily[key] != null && proofs[key] && !withheld(daily, key);
      const groups = [
        [["open", "開"], ["close", "收"]],
        [["high", "高"], ["low", "低"], ["average_price", "均"]],
        [["last_bid", "盤後最後買"], ["last_ask", "盤後最後賣"]],
        [["next_reference_price", "次一交易日參考"], ["next_upper_limit", "次一交易日申報上界"], ["next_lower_limit", "下界"]],
      ];
      const source = groups.flat().filter(([key]) => visible(key)).map(([key]) => proofs[key])[0];
      if (!source) return "";
      const lines = groups.map(group => group.filter(([key]) => visible(key))
        .map(([key, label]) => `${label} ${sourceNumber(daily[key])}`).join("／")).filter(Boolean);
      const measures = [["volume_shares", "成交股數", "股"], ["trade_count", "成交筆數", "筆"], ["turnover", "成交金額", "（來源單位）"]]
        .filter(([key]) => daily[key] != null && Number.isFinite(Number(daily[key])) && Number(daily[key]) >= 0)
        .map(([key, label, unit]) => `${label} ${key === "turnover" ? sourceNumber(daily[key]) : number(daily[key])}${unit}`);
      if (measures.length) lines.push(measures.join("／"));
      const prior = daily.prior_session_reference_only
        ? ` · ${esc(daily.requested_session_date)} 日行情待發布，非當日報價` : "";
      const overview = `<small class="public-daily-quote" data-daily-session="${esc(daily.session_date)}">官方日行情 ${esc(daily.session_date)} · ${lines.shift()}${prior}</small>`;
      const details = lines.length
        ? `<details class="daily-reference-details"><summary>完整官方日資料${visible("next_upper_limit") || visible("next_lower_limit") ? "／次日界線" : ""}</summary>${lines.map(line => `<small>${line}</small>`).join("")}</details>` : "";
      return `${overview}${details}<small>來源 ${esc(source.source)} · 取得 ${sourceClock(daily.received_at)}（臺北） · 非分鐘成交</small>`;
    }

    function priceNoticeText(data) {
      if (data.price_display_policy?.private_view === true) return data.price_display_policy.message || "本人私人檢視";
      const reference = data.official_price_reference;
      const rows = (reference?.quotes || []).filter((row) => row.official_daily_quote?.open != null
        || row.official_daily_quote?.close != null);
      if (!rows.length) return data.price_display_policy?.message || "";
      const values = rows.map((row) => `${row.symbol} 開 ${sourceNumber(row.official_daily_quote.open)}／收 ${sourceNumber(row.official_daily_quote.close)}`);
      const sources = [...new Set(rows.map((row) => {
        const proofs = row.official_daily_quote.public_price_display?.visible_fields;
        return (proofs?.open || proofs?.close)?.source;
      }).filter(Boolean))];
      const permission = data.price_display_policy?.broker_public_display_permission === "unverified"
        ? "永豐分鐘／即時價格公開展示授權待確認。" : "";
      const reports = data.price_display_policy?.paper_transaction_reports === true
        ? "進出場價為使用者公開的模擬帳本成交，非券商成交。" : "";
      const strategy = data.price_display_policy?.paper_strategy_reports === true
        ? "TP／SL 與委託價是使用者公開的策略設定，非行情／非成交。" : "";
      const prior = reference.requested_session_date && reference.requested_session_date !== reference.session_date
        ? `${reference.requested_session_date} 日行情待發布或待驗收，以下為前一份已取得的官方資料。` : "";
      return `${strategy}${reports}${prior}官方日行情 ${reference.session_date}：${values.join("；")}。來源：${sources.join("／")}。${permission}日行情不是分鐘成交或即時買賣價；模型與帳本原值不變。`;
    }

    function priceSources({data = {}, positions = [], signals = [], events = []} = {}) {
      const rows = [...(data.official_price_reference?.quotes || []), ...(data.benchmarks || []),
        ...positions, ...signals, ...signals.map(row => row.position).filter(Boolean), ...events];
      const sources = new Map();
      const references = new Map();
      const roles = {entry_price: "進場", execution_price: "進場", exit_price: "出場", last_exit_price: "出場",
        last_mark_price: "清算估值", take_profit_price: "TP", stop_trigger_price: "SL", eod_limit_price: "13:20 委託",
        closing_auction_limit_price: "收盤委託", opening_exit_limit_price: "開盤沖銷委託", order_limit_price: "策略委託",
        sizing_open_price: "開盤計價", open: "日開", close: "日收", high: "日高", low: "日低", price: "事件價格",
        average_price: "日均", last_bid: "盤後最後買", last_ask: "盤後最後賣",
        next_reference_price: "次日參考", next_upper_limit: "次日申報上界", next_lower_limit: "次日申報下界",
        daily_return_reference_price: "前收基準", daily_return_previous_close: "前收"};
      function collect(row) {
        for (const [field, proof] of Object.entries(row?.public_price_display?.visible_fields || {})) {
          if (row[field] == null || withheld(row, field)) continue;
          const key = [proof.source, proof.dataset, proof.grain, proof.publication_basis].join("|");
          if (!sources.has(key)) sources.set(key, {proof, roles: new Set(), dates: new Set(), clocks: new Set()});
          const item = sources.get(key);
          item.roles.add(roles[field] || field);
          if (proof.session_date || row.session_date) item.dates.add(String(proof.session_date || row.session_date));
          const clock = proof.received_at || row.received_at || proof.recorded_at || proof.observed_at;
          if (sourceClock(clock) !== "—") item.clocks.add(String(clock));
        }
      }
      for (const row of rows) {
        collect(row);
        const daily = row?.official_daily_quote;
        if (!daily) continue;
        collect(daily);
        const key = [row.symbol || row.benchmark_id, daily.session_date, daily.requested_session_date].join("|");
        if (!references.has(key) && dailyReference(row)) references.set(key, row);
      }
      const html = [...sources.values()].map(({proof, roles, dates, clocks}) => {
        const sortedDates = [...dates].sort();
        const sortedClocks = [...clocks].sort((a, b) => Date.parse(a) - Date.parse(b));
        const range = values => values.length ? `${values[0]}${values.length > 1 && values[0] !== values.at(-1) ? ` ～ ${values.at(-1)}` : ""}` : "—";
        const basis = proof.publication_basis === "owner_paper_transaction_report_v1" ? "模擬帳本成交 · 非券商成交"
          : proof.publication_basis === "owner_paper_strategy_report_v1" ? "策略設定 · 非行情／非成交"
          : proof.publication_basis === "owner_private_view" ? "本人私人檢視 · 原始觀測／帳本價格"
          : proof.grain === "daily" ? "官方日資料 · 非分鐘／即時行情" : "已驗證可公開行情";
        return `<li><strong>${esc(proof.source || "來源未提供")}</strong><span>${esc(basis)} · ${esc([...roles].join("／"))}</span><small>資料日 ${esc(range(sortedDates))} · ${proof.grain === "daily" ? "取得" : "紀錄"} ${esc(range(sortedClocks.map(sourceClock)))}（臺北）</small></li>`;
      }).join("");
      const referenceHtml = [...references.entries()].map(([key, row]) =>
        `<li data-reference-key="${esc(key)}"><strong>${esc(row.symbol || row.benchmark_id)} ${esc(row.name || row.label || "")}</strong>${dailyReference(row)}</li>`).join("");
      return {sourcesHtml: html || (data.price_display_policy?.private_view
        ? "<li>目前已載入資料尚無價格來源。</li>" : "<li>目前已載入資料尚無可公開價格來源。</li>"), referencesHtml: referenceHtml,
        referenceCount: references.size};
    }

    function futuresBadge(membership) {
      const view = global.StockAgentTwPresentation.futuresPresentation(membership);
      return `<small class="stock-futures" data-futures-status="${esc(view.status)}" title="${esc(view.title)}">${badge(view.label, view.kind)}<span>${esc(view.detail)}</span></small>`;
    }

    function positionRow(row) {
      const {signedShares, realized:realizedNet, unrealized:unrealizedNet, total:totalNet} = resolvedPositionPnl(row);
      const mark = liquidation(row, signedShares);
      if (product === "tw_overnight") {
        const openStatus = row.opening_exit_order_status || "尚未送出";
        return `<tr>
          <td><strong>${esc(strategyLabel(row))}</strong> ${badge(row.side === "long" ? "多" : "空", row.side === "long" ? "good" : "bad")}<small>${esc(row.session_date)} · ${esc(row.symbol)} ${esc(row.name || "")}</small></td>
          <td><strong>${pct(row.target_weight)}</strong><small>收盤成交 ${number(row.filled_shares)}／目標 ${number(row.requested_shares)} 股</small><small>剩餘 ${number(Math.abs(signedShares))} 股</small></td>
          <td><strong>收盤進 ${quoteText(row, "entry_price")}</strong><small>${shortTime(row.entry_exchange_at || row.entry_at)}</small><small>${mark.label} ${quoteText(row, mark.fields)} · ${shortTime(mark.at)}</small>${priceDistanceText(row, mark.fields)}</td>
          <td><strong>${quoteText(row, "opening_exit_limit_price")}</strong><small>LMT_ROD · ${esc(openStatus)}</small><small>${shortTime(row.opening_exit_submitted_at)} · ${esc(row.opening_exit_order_session_date || row.exit_session_date || "次一交易日")}</small></td>
          <td><strong>${withheld(row, "exit_price") ? `${row.status === "closed" || signedShares === 0 ? "已平倉・" : ""}${pendingQuoteText(row, "exit_price")}` : row.exit_price == null ? "等待實際開盤" : quoteText(row, "exit_price")}</strong>${priceDistanceText(row, "exit_price")}<small>${esc(row.exit_reason || row.status || "—")}</small><small>${shortTime(row.exit_exchange_at || row.exit_at || row.last_exit_at)}</small></td>
          <td><strong class="${pnlClass(totalNet)}">總 ${money(totalNet)}</strong><small class="${pnlClass(realizedNet)}">已實現 ${money(realizedNet)}</small><small class="${pnlClass(unrealizedNet)}">未實現 ${money(unrealizedNet)} · ${row.valuation_stale ? badge("延用", "warn") : badge("新鮮", "good")}</small></td>
        </tr>`;
      }
      return `<tr>
        <td><strong>${esc(strategyLabel(row))}</strong> ${badge(row.side === "long" ? "多" : "空", row.side === "long" ? "good" : "bad")}<small>${esc(row.session_date)} · ${esc(row.symbol)} ${esc(row.name || "")}</small></td>
        <td><strong>${pct(row.target_weight)}</strong><small>成交 ${number(row.filled_shares)}／預計 ${number(row.requested_shares)} 股</small><small>未平倉 ${number(Math.abs(signedShares))} 股${Number(row.target_unsubmitted_shares || 0) > 0 ? ` · 目標缺口 ${number(row.target_unsubmitted_shares)} 股` : ""}</small></td>
        <td><strong>進 ${quoteText(row, "entry_price")}</strong><small>${esc(row.counterfactual_open_replay ? replayTimingText(row, row.entry_at) : shortTime(row.entry_at))}</small><small>${mark.label} ${quoteText(row, mark.fields)} · ${shortTime(mark.at)}</small>${priceDistanceText(row, mark.fields)}</td>
        <td><strong>TP ${quoteText(row, "take_profit_price", sourceNumber)} · SL ${quoteText(row, "stop_trigger_price", sourceNumber)}</strong><small>${esc(row.stop_order_status)}</small><small>13:20 ${quoteText(row, "eod_limit_price", sourceNumber)} · ${esc(row.eod_limit_order_status || "未到")} · ${shortTime(row.eod_limit_submitted_at)}</small><small>13:25 ${quoteText(row, "closing_auction_limit_price", sourceNumber)} · ${esc(row.closing_auction_order_status || "未到")}</small></td>
        <td><strong>${row.exit_price != null && !withheld(row, "exit_price") ? quoteText(row, "exit_price") : row.last_exit_price != null && !withheld(row, "last_exit_price") ? `${row.status === "closed" || signedShares === 0 ? "" : "部分 "}${quoteText(row, "last_exit_price")}` : withheld(row, "exit_price") || withheld(row, "last_exit_price") ? `${row.status === "closed" || signedShares === 0 ? "已平倉・" : "部分出場・"}${pendingQuoteText(row, ["exit_price", "last_exit_price"])}` : "持倉中"}</strong>${priceDistanceText(row, ["exit_price", "last_exit_price"])}<small>${esc(row.exit_reason || row.status || "—")}</small><small>${shortTime(row.exit_at || row.last_exit_at)}</small></td>
        <td><strong class="${pnlClass(totalNet)}">總 ${money(totalNet)}</strong><small class="${pnlClass(realizedNet)}">已實現 ${money(realizedNet)}</small><small class="${pnlClass(unrealizedNet)}">未實現 ${money(unrealizedNet)} · ${row.valuation_stale ? badge("延用", "warn") : badge("新鮮", "good")}</small></td>
      </tr>`;

    }

    function signalRow(row, {position, mode, sessionDate, selectedKey = ""} = {}) {
      position = row.position || position;
      const positionPnl = position ? resolvedPositionPnl(position) : null;
      const hasFill = product === "tw_overnight"
        ? Number(row.filled_shares || position?.filled_shares || 0) > 0
        : Number(row.filled_shares || 0) > 0;
      const hasPosition = Boolean(position && Number(position.filled_shares || 0) > 0);
      const isOpen = hasPosition && positionPnl.signedShares !== 0;
      const indicative = String(position?.last_mark_quote_source || "").includes("stock_snapshot");
      const openingPrice = row.sizing_open_price ?? (row.counterfactual_open_replay ? row.execution_price : null);
      const executionPrice = !withheld(row, "execution_price") && row.execution_price != null
        ? row.execution_price : !withheld(position, "entry_price") ? position?.entry_price : null;
      const hasTarget = Math.abs(Number(row.target_weight || 0)) > 0;
      const entryNotional = hasFill && executionPrice != null && Number.isFinite(Number(executionPrice))
        ? Math.abs(Number(row.filled_shares || 0)) * Number(executionPrice) : null;
      const minuteReplay = row.counterfactual_0901_price_fill === true
        || String(row.entry_fill_policy || "").startsWith("official_open_signal_0900_execute_0901");
      const fillLabel = minuteReplay ? "09:01 首分鐘價反事實模擬" : "因果報價紙上模擬";
      const targetGap = Math.max(0, Number(row.requested_shares || 0) - Number(row.filled_shares || 0));
      const modeTotalEquity = row.session_date === sessionDate ? Number(mode?.total_equity_twd) : NaN;
      const equityImpactPct = hasPosition && positionPnl.total != null && Number.isFinite(modeTotalEquity) && Math.abs(modeTotalEquity) > .01
        ? Number(positionPnl.total) / modeTotalEquity * 100
        : null;
      const currentPrice = isOpen ? position.last_mark_price : position?.exit_price ?? position?.last_exit_price;
      const currentLabel = isOpen ? (indicative ? "參考快照估值（非成交證據）" : "現在可清算價") : hasPosition ? "已平倉價" : hasFill ? "紙上已成交・估值未取得" : "未成交";
      const missingPnlLabel = hasFill ? "估值資料未取得" : "不計盈虧";
      const currentAt = isOpen ? position.last_quote_at : position?.exit_at ?? position?.last_exit_at;
      const reasonText = signalReasonLabel(row.reason || row.status);
      const eligibility = row.day_trade_eligible
        ? badge(row.sell_first_allowed ? "可雙向" : "僅買先", row.sell_first_allowed ? "good" : "warn")
        : badge("不可當沖", "bad");
      const result = badge(row.status, row.status === "ready" ? "good" : row.status === "partial_depth" ? "warn" : row.status === "hold" ? "" : "bad");
      const selected = signalRowKey(row) === selectedKey ? " is-selected" : "";
      if (product === "tw_overnight") {
        const historical = row.counterfactual_overnight_replay === true;
        const sizingLabel = row.sizing_price_at_decision != null ? "13:20 計價" : "13:25 歷史計價";
        const eligibility = row.side === "short"
          ? badge(row.status === "working" || hasPosition ? "隔夜放空已通過" : "隔夜放空未通過", row.status === "working" || hasPosition ? "good" : "bad")
          : badge("現股買進", "good");
        const result = badge(hasFill ? (historical ? "歷史收盤近似" : "收盤已成交") : row.status || "等待", hasFill ? "good" : row.status === "working" ? "warn" : row.status === "hold" ? "" : "bad");
        const currentAt = isOpen ? position.last_quote_at : position?.exit_exchange_at ?? position?.exit_at;
        const modeTotalEquity = historical
          ? Number(row.sizing_capital_twd)
          : row.session_date === sessionDate ? Number(mode?.total_equity_twd) : NaN;
        const equityImpactPct = hasPosition && positionPnl.total != null && Number.isFinite(modeTotalEquity) && Math.abs(modeTotalEquity) > .01
          ? Number(positionPnl.total) / modeTotalEquity * 100
          : null;
        return `<tr data-signal-key="${esc(signalRowKey(row))}" class="signal-row${selected}">
          <td><strong>${esc(strategyLabel(row))}</strong><small>${shortTime(row.signal_at)}</small></td>
          <td><strong>${esc(row.symbol)}</strong> ${badge(row.side, row.side === "long" ? "good" : row.side === "short" ? "bad" : "")}<small>${esc(row.name || "")}</small><small>${eligibility} ${result}</small>${futuresBadge(row.stock_futures)}</td>
          <td><strong>${sourceNumber(row.raw_score ?? row.score)}</strong><small>持倉目標 ${pct(row.target_weight)}</small><small>${esc(signalReasonLabel(row.reason || row.status))}</small></td>
          <td><strong>${sizingLabel} ${quoteText(row, ["sizing_price_at_decision", "sizing_price_at_13_25", "sizing_open_price"])}</strong><small>${row.side === "long" ? "漲停買進" : row.side === "short" ? "跌停賣空" : "零權重"} · ${quoteText(row, "order_limit_price")}</small><small>目標 ${number(row.requested_shares)} 股 · ${row.simtrade === true ? "目前為試撮" : "等待／已見正式行情"}</small></td>
          <td><strong>${hasFill ? `${historical ? "官方收盤反事實" : "收盤成交"} ${position ? quoteText(position, "entry_price") : quoteText(row, "execution_price")}` : historical ? "歷史反事實未成交" : "尚無實際收盤成交"}</strong><small>${hasPosition ? `${isOpen ? (historical ? "期末估值" : "可清算") : (historical ? "官方開盤反事實沖銷" : "開盤沖銷")} ${quoteText(position, isOpen ? "last_mark_price" : ["exit_price", "last_exit_price"])} · ${shortTime(currentAt)}` : hasFill ? "紙上已成交；對應持倉估值未取得" : historical ? "不補造歷史成交" : `訊號時 bid／ask ${quoteText(row, "bid", sourceNumber)}／${quoteText(row, "ask", sourceNumber)}`}</small>${hasPosition ? priceDistanceText(position, isOpen ? "last_mark_price" : ["exit_price", "last_exit_price"]) : ""}<small class="${pnlClass(positionPnl?.total)}">該檔盈虧 ${hasPosition ? money(positionPnl.total) : missingPnlLabel}</small></td>
          <td><strong class="${pnlClass(positionPnl?.total)}">佔模式總權益 ${equityImpactPct == null ? "—" : `${equityImpactPct >= 0 ? "+" : ""}${displayPct(equityImpactPct)}`}</strong><small>${historical ? "進場前權益" : "模式總權益"} ${Number.isFinite(modeTotalEquity) ? summaryMoney(modeTotalEquity) : "—"}</small><small>${historical ? "歷史反事實 · 無交易所成交證據" : "當沖模型暫時轉接 · 非隔夜訓練"}</small></td>
        </tr>`;
      }
      return `<tr data-signal-key="${esc(signalRowKey(row))}" class="signal-row${selected}">
        <td><strong>${esc(strategyLabel(row))}</strong><small>${esc(signalTimingText(row))}</small></td>
        <td><strong>${esc(row.symbol)}</strong> ${badge(row.side, row.side === "long" ? "good" : row.side === "short" ? "bad" : "")}<small>${esc(row.name || "")}</small><small>${eligibility} ${result}</small>${futuresBadge(row.stock_futures)}</td>
        <td><strong>${sourceNumber(row.raw_score ?? row.score)}</strong><small>持倉目標 ${pct(row.target_weight)}</small><small>${esc(reasonText)}</small></td>
        <td>${hasTarget ? `<strong class="${openingPrice == null && !withheld(row, "sizing_open_price") && !withheld(row, "execution_price") ? "negative" : ""}">開盤計價 ${quoteText(row, row.counterfactual_open_replay ? ["sizing_open_price", "execution_price"] : "sizing_open_price")}</strong>` : `<strong>零權重・不下單</strong>`}<small>${hasFill ? `${fillLabel} ${row.execution_price != null || withheld(row, "execution_price") ? quoteText(row, "execution_price") : quoteText(position, "entry_price")} · 名目 ${money(entryNotional)}` : hasTarget ? `完整模型訊號已保留 · 0 股紙上成交` : "不需開盤計價"}</small><small>${hasTarget && !hasFill ? `${esc(reasonText)} · ` : ""}模型目標 ${number(row.requested_shares)}／紙上成交 ${number(row.filled_shares)}／${minuteReplay ? "容量限制未建立委託" : "尚未成交"} ${number(targetGap)} 股${minuteReplay ? ` · 09:01 容量 ${number(row.minute_kbar_capacity_shares)}` : ` · L1 ${number(row.top_book_capacity_shares)}`}</small></td>
        <td><strong>${currentLabel} ${hasPosition ? quoteText(position, isOpen ? "last_mark_price" : ["exit_price", "last_exit_price"]) : "—"}</strong><small>${hasPosition ? shortTime(currentAt) : `訊號時 bid／ask ${quoteText(row, "bid", sourceNumber)}／${quoteText(row, "ask", sourceNumber)}`}</small>${hasPosition ? priceDistanceText(position, isOpen ? "last_mark_price" : ["exit_price", "last_exit_price"]) : ""}<small class="${pnlClass(positionPnl?.total)}">該檔盈虧 ${hasPosition ? money(positionPnl.total) : missingPnlLabel}</small></td>
        <td><strong class="${pnlClass(positionPnl?.total)}">佔該模式總權益 ${equityImpactPct == null ? "—" : `${equityImpactPct >= 0 ? "+" : ""}${displayPct(equityImpactPct)}`}</strong><small>模式總權益 ${Number.isFinite(modeTotalEquity) ? summaryMoney(modeTotalEquity) : "—"}</small><small>${hasPosition ? (position.valuation_stale ? badge("估值延用", "warn") : badge("估值新鮮", "good")) : hasFill ? "已成交；待取得對應持倉估值" : "未成交不納入"}</small></td>
      </tr>`;

    }

    function eventRow(row) {
      return `<tr><td>${esc(row.session_date)}<small>${shortTime(row.fill_at || row.recorded_at)}</small></td><td>${esc(strategyLabel(row))}<small>${esc(row.symbol)}</small></td><td>${esc(row.purpose)}</td><td>${esc(row.order_type || row.event_kind)}</td><td>${quoteText(row, "price", sourceNumber)} × ${number(row.quantity)}</td><td>${esc(row.status || row.event_kind)}</td></tr>`;
    }

    function pagedTable({id, rows, total, loading, hasMore, error, renderRow, emptyText, countDetail = ""}) {
      $(`${id}-body`).setAttribute("aria-busy", String(loading));
      $(`${id}-count`).textContent = `${number(rows.length)} / ${number(total)} 筆${error ? " · 更新失敗" : countDetail}`;
      const button = $(`load-more-${id}s`);
      button.classList.toggle("hidden", !hasMore);
      button.disabled = loading;
      button.textContent = loading ? "載入中…" : "載入更多";
      const message = (text) => `<tr><td colspan="6">${esc(text)}</td></tr>`;
      const markup = rows.map(renderRow).join("");
      setHtml(`${id}-body`, (error ? message(error) : "") + (markup || (error ? "" : message(emptyText))));
    }

    return Object.freeze({positionRow, signalRow, eventRow, futuresBadge, pagedTable, quoteText, dailyReference, priceNoticeText, priceDistanceText, priceSources});
  }
  global.StockAgentTwDetailComponents = Object.freeze({create});
})(globalThis);

"use strict";

/* Stateless detail-row renderers and one shared paged-table shell. Network,
 * filters, cancellation and selected rows remain owned by app.js. */
(function installDetailComponents(global) {
  function create({Dashboard, strategyLabel, signalRowKey, resolvedPositionPnl, format, product = "tw_day_trade"}) {
    const {escapeHtml: esc, byId: $, setTrustedHtml: setHtml} = Dashboard;
    const {number, pct, money, sourceNumber, summaryMoney, displayPct, badge,
      pnlClass, shortTime, replayTimingText, signalTimingText, signalReasonLabel} = format;

    function futuresBadge(membership) {
      const view = global.StockAgentTwPresentation.futuresPresentation(membership);
      return `<small class="stock-futures" data-futures-status="${esc(view.status)}" title="${esc(view.title)}">${badge(view.label, view.kind)}<span>${esc(view.detail)}</span></small>`;
    }

    function positionRow(row) {
      const {signedShares, realized:realizedNet, unrealized:unrealizedNet, total:totalNet} = resolvedPositionPnl(row);
      if (product === "tw_overnight") {
        const openStatus = row.opening_exit_order_status || "尚未送出";
        return `<tr>
          <td><strong>${esc(strategyLabel(row))}</strong> ${badge(row.side === "long" ? "多" : "空", row.side === "long" ? "good" : "bad")}<small>${esc(row.session_date)} · ${esc(row.symbol)} ${esc(row.name || "")}</small></td>
          <td><strong>${pct(row.target_weight)}</strong><small>收盤成交 ${number(row.filled_shares)}／目標 ${number(row.requested_shares)} 股</small><small>剩餘 ${number(Math.abs(signedShares))} 股</small></td>
          <td><strong>收盤進 ${money(row.entry_price)}</strong><small>${shortTime(row.entry_exchange_at || row.entry_at)} · ${esc(row.entry_price_source || "等待實際收盤")}</small><small>清算 ${money(row.last_mark_price)} · ${shortTime(row.last_quote_at)}</small></td>
          <td><strong>${money(row.opening_exit_limit_price)}</strong><small>LMT_ROD · ${esc(openStatus)}</small><small>${shortTime(row.opening_exit_submitted_at)} · ${esc(row.opening_exit_order_session_date || row.exit_session_date || "次一交易日")}</small></td>
          <td><strong>${row.exit_price == null ? "等待實際開盤" : money(row.exit_price)}</strong><small>${esc(row.exit_reason || row.status || "—")}</small><small>${shortTime(row.exit_exchange_at || row.exit_at || row.last_exit_at)}</small></td>
          <td><strong class="${pnlClass(totalNet)}">總 ${money(totalNet)}</strong><small class="${pnlClass(realizedNet)}">已實現 ${money(realizedNet)}</small><small class="${pnlClass(unrealizedNet)}">未實現 ${money(unrealizedNet)} · ${row.valuation_stale ? badge("延用", "warn") : badge("新鮮", "good")}</small></td>
        </tr>`;
      }
      return `<tr>
        <td><strong>${esc(strategyLabel(row))}</strong> ${badge(row.side === "long" ? "多" : "空", row.side === "long" ? "good" : "bad")}<small>${esc(row.session_date)} · ${esc(row.symbol)} ${esc(row.name || "")}</small></td>
        <td><strong>${pct(row.target_weight)}</strong><small>成交 ${number(row.filled_shares)}／預計 ${number(row.requested_shares)} 股</small><small>剩餘 ${number(Math.abs(signedShares))} 股</small></td>
        <td><strong>進 ${money(row.entry_price)}</strong><small>${esc(row.counterfactual_open_replay ? replayTimingText(row, row.entry_at) : shortTime(row.entry_at))}</small><small>清算 ${money(row.last_mark_price)} · ${shortTime(row.last_quote_at)}</small></td>
        <td><strong>TP ${sourceNumber(row.take_profit_price)} · SL ${sourceNumber(row.stop_trigger_price)}</strong><small>${esc(row.stop_order_status)}</small><small>13:20 ${row.eod_limit_price == null ? "—" : sourceNumber(row.eod_limit_price)} · ${esc(row.eod_limit_order_status || "未到")} · ${shortTime(row.eod_limit_submitted_at)}</small><small>13:25 ${row.closing_auction_limit_price == null ? "—" : sourceNumber(row.closing_auction_limit_price)} · ${esc(row.closing_auction_order_status || "未到")}</small></td>
        <td><strong>${row.exit_price == null ? (row.last_exit_price == null ? "持倉中" : `部分 ${money(row.last_exit_price)}`) : money(row.exit_price)}</strong><small>${esc(row.exit_reason || row.status || "—")}</small><small>${shortTime(row.exit_at || row.last_exit_at)}</small></td>
        <td><strong class="${pnlClass(totalNet)}">總 ${money(totalNet)}</strong><small class="${pnlClass(realizedNet)}">已實現 ${money(realizedNet)}</small><small class="${pnlClass(unrealizedNet)}">未實現 ${money(unrealizedNet)} · ${row.valuation_stale ? badge("延用", "warn") : badge("新鮮", "good")}</small></td>
      </tr>`;

    }

    function signalRow(row, {position, mode, sessionDate, selectedKey = ""} = {}) {
      const positionPnl = position ? resolvedPositionPnl(position) : null;
      const hasFill = product === "tw_overnight"
        ? Boolean(position && Number(position.filled_shares || 0) > 0)
        : Number(row.filled_shares || 0) > 0;
      const hasPosition = Boolean(position && Number(position.filled_shares || 0) > 0);
      const isOpen = hasPosition && positionPnl.signedShares !== 0;
      const openingPrice = row.sizing_open_price ?? (row.counterfactual_open_replay ? row.execution_price : null);
      const executionPrice = row.execution_price ?? position?.entry_price;
      const hasTarget = Math.abs(Number(row.target_weight || 0)) > 0;
      const entryNotional = hasFill ? Math.abs(Number(row.filled_shares || 0)) * Number(executionPrice || 0) : null;
      const modeTotalEquity = row.session_date === sessionDate ? Number(mode?.total_equity_twd) : NaN;
      const equityImpactPct = hasPosition && positionPnl.total != null && Number.isFinite(modeTotalEquity) && Math.abs(modeTotalEquity) > .01
        ? Number(positionPnl.total) / modeTotalEquity * 100
        : null;
      const currentPrice = isOpen ? position.last_mark_price : position?.exit_price ?? position?.last_exit_price;
      const currentLabel = isOpen ? "現在可清算價" : hasPosition ? "已平倉價" : "未成交";
      const currentAt = isOpen ? position.last_quote_at : position?.exit_at ?? position?.last_exit_at;
      const reasonText = signalReasonLabel(row.reason || row.status);
      const eligibility = row.day_trade_eligible
        ? badge(row.sell_first_allowed ? "可雙向" : "僅買先", row.sell_first_allowed ? "good" : "warn")
        : badge("不可當沖", "bad");
      const result = badge(row.status, row.status === "ready" ? "good" : row.status === "partial_depth" ? "warn" : row.status === "hold" ? "" : "bad");
      const selected = signalRowKey(row) === selectedKey ? " is-selected" : "";
      if (product === "tw_overnight") {
        const historical = row.counterfactual_overnight_replay === true;
        const sizingPrice = row.sizing_price_at_13_25 ?? row.sizing_open_price;
        const orderPrice = row.order_limit_price;
        const eligibility = row.side === "short"
          ? badge(row.status === "working" || hasPosition ? "隔夜放空已通過" : "隔夜放空未通過", row.status === "working" || hasPosition ? "good" : "bad")
          : badge("現股買進", "good");
        const result = badge(hasFill ? (historical ? "歷史收盤近似" : "收盤已成交") : row.status || "等待", hasFill ? "good" : row.status === "working" ? "warn" : row.status === "hold" ? "" : "bad");
        const currentPrice = isOpen ? position.last_mark_price : position?.exit_price ?? position?.last_exit_price;
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
          <td><strong>13:25 計價 ${money(sizingPrice)}</strong><small>${row.side === "long" ? "漲停買進" : row.side === "short" ? "跌停賣空" : "零權重"} · ${money(orderPrice)}</small><small>目標 ${number(row.requested_shares)} 股 · ${row.simtrade === true ? "目前為試撮" : "等待／已見正式行情"}</small></td>
          <td><strong>${hasFill ? `${historical ? "官方收盤反事實" : "收盤成交"} ${money(position.entry_price)}` : historical ? "歷史反事實未成交" : "尚無實際收盤成交"}</strong><small>${hasPosition ? `${isOpen ? (historical ? "期末估值" : "可清算") : (historical ? "官方開盤反事實沖銷" : "開盤沖銷")} ${money(currentPrice)} · ${shortTime(currentAt)}` : historical ? "不補造歷史成交" : `訊號時 bid／ask ${sourceNumber(row.bid)}／${sourceNumber(row.ask)}`}</small><small class="${pnlClass(positionPnl?.total)}">該檔盈虧 ${hasPosition ? money(positionPnl.total) : "不計盈虧"}</small></td>
          <td><strong class="${pnlClass(positionPnl?.total)}">佔模式總權益 ${equityImpactPct == null ? "—" : `${equityImpactPct >= 0 ? "+" : ""}${displayPct(equityImpactPct)}`}</strong><small>${historical ? "進場前權益" : "模式總權益"} ${Number.isFinite(modeTotalEquity) ? summaryMoney(modeTotalEquity) : "—"}</small><small>${historical ? "歷史反事實 · 無交易所成交證據" : "當沖模型暫時轉接 · 非隔夜訓練"}</small></td>
        </tr>`;
      }
      return `<tr data-signal-key="${esc(signalRowKey(row))}" class="signal-row${selected}">
        <td><strong>${esc(strategyLabel(row))}</strong><small>${esc(signalTimingText(row))}</small></td>
        <td><strong>${esc(row.symbol)}</strong> ${badge(row.side, row.side === "long" ? "good" : row.side === "short" ? "bad" : "")}<small>${esc(row.name || "")}</small><small>${eligibility} ${result}</small>${futuresBadge(row.stock_futures)}</td>
        <td><strong>${sourceNumber(row.raw_score ?? row.score)}</strong><small>持倉目標 ${pct(row.target_weight)}</small><small>${esc(reasonText)}</small></td>
        <td>${hasTarget ? `<strong class="${openingPrice == null ? "negative" : ""}">開盤計價 ${money(openingPrice)}</strong>` : `<strong>零權重・不下單</strong>`}<small>${hasFill ? `市價成交 ${money(executionPrice)} · 名目 ${money(entryNotional)}` : hasTarget ? `完整模型訊號已保留 · 0 股未成交` : "不需開盤計價"}</small><small>${hasTarget && !hasFill ? `${esc(reasonText)} · ` : ""}成交 ${number(row.filled_shares)}／${number(row.requested_shares)} 股 · L1 ${number(row.top_book_capacity_shares)}</small></td>
        <td><strong>${currentLabel} ${hasPosition ? money(currentPrice) : "—"}</strong><small>${hasPosition ? shortTime(currentAt) : `訊號時 bid／ask ${sourceNumber(row.bid)}／${sourceNumber(row.ask)}`}</small><small class="${pnlClass(positionPnl?.total)}">該檔盈虧 ${hasPosition ? money(positionPnl.total) : "不計盈虧"}</small></td>
        <td><strong class="${pnlClass(positionPnl?.total)}">佔該模式總權益 ${equityImpactPct == null ? "—" : `${equityImpactPct >= 0 ? "+" : ""}${displayPct(equityImpactPct)}`}</strong><small>模式總權益 ${Number.isFinite(modeTotalEquity) ? summaryMoney(modeTotalEquity) : "—"}</small><small>${hasPosition ? (position.valuation_stale ? badge("估值延用", "warn") : badge("估值新鮮", "good")) : "未成交不納入"}</small></td>
      </tr>`;

    }

    function eventRow(row) {
      return `<tr><td>${esc(row.session_date)}<small>${shortTime(row.fill_at || row.recorded_at)}</small></td><td>${esc(strategyLabel(row))}<small>${esc(row.symbol)}</small></td><td>${esc(row.purpose)}</td><td>${esc(row.order_type || row.event_kind)}</td><td>${sourceNumber(row.price)} × ${number(row.quantity)}</td><td>${esc(row.status || row.event_kind)}</td></tr>`;
    }

    function pagedTable({id, rows, total, loading, hasMore, error, renderRow, emptyText, countDetail = ""}) {
      $(`${id}-body`).setAttribute("aria-busy", String(loading));
      $(`${id}-count`).textContent = `${number(rows.length)} / ${number(total)} 筆${error ? " · 等待重試" : countDetail}`;
      const button = $(`load-more-${id}s`);
      button.classList.toggle("hidden", !hasMore);
      button.disabled = loading;
      button.textContent = loading ? "載入中…" : "載入更多";
      const message = (text) => `<tr><td colspan="6">${esc(text)}</td></tr>`;
      const markup = rows.map(renderRow).join("");
      setHtml(`${id}-body`, (error ? message(error) : "") + (markup || message(emptyText)));
    }

    return Object.freeze({positionRow, signalRow, eventRow, futuresBadge, pagedTable});
  }
  global.StockAgentTwDetailComponents = Object.freeze({create});
})(globalThis);

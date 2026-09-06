"use strict";

/* Stateless detail-row renderers and one shared paged-table shell. Network,
 * filters, cancellation and selected rows remain owned by app.js. */
(function installDetailComponents(global) {
  function create({Dashboard, strategyLabel, signalRowKey, resolvedPositionPnl, format}) {
    const {escapeHtml: esc, byId: $, setTrustedHtml: setHtml} = Dashboard;
    const {number, pct, money, sourceNumber, summaryMoney, displayPct, badge,
      pnlClass, shortTime, replayTimingText, signalTimingText, signalReasonLabel} = format;

    function futuresBadge(membership) {
      const view = global.StockAgentTwPresentation.futuresPresentation(membership);
      return `<small class="stock-futures" data-futures-status="${esc(view.status)}" title="${esc(view.title)}">${badge(view.label, view.kind)}<span>${esc(view.detail)}</span></small>`;
    }

    function positionRow(row) {
      const {signedShares, realized:realizedNet, unrealized:unrealizedNet, total:totalNet} = resolvedPositionPnl(row);
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
      const hasFill = Number(row.filled_shares || 0) > 0;
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

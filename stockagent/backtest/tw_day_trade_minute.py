"""Differentiable daily-policy execution on historical one-minute K bars."""

from __future__ import annotations

from dataclasses import dataclass
import os

import torch

from stockagent.backtest.tw_commission_rebate import (
    normalize_commission_rebate_timing,
)
from stockagent.data.tw_day_trade_execution import (
    DAY_TRADE_FULL_SESSION_FIELDS,
    DAY_TRADE_FULL_SESSION_MINUTES,
    DAY_TRADE_EXECUTION_FIELD_COUNT,
    DayTradeExecutionField as F,
    FULL_SESSION_PRICE,
    FULL_SESSION_VOLUME_SHARES,
)


MINUTE_VOLUME_PARTICIPATION = 0.50
BOARD_LOT_SHARES = 1_000.0
MARGIN_FINANCING_PRINCIPAL_RATIO = 0.60
MARGIN_FINANCING_ANNUAL_RATE = 0.16
MARGIN_FINANCING_ONE_DAY_RATE = (
    MARGIN_FINANCING_PRINCIPAL_RATIO * MARGIN_FINANCING_ANNUAL_RATE / 365.0
)
# The TWSE educational schedule gives a 0.08%--0.10% one-time margin-short
# handling fee and an annual borrow rate ceiling of 20%.  This stress contract
# deliberately selects both upper endpoints while assuming inventory is
# unlimited.  It is not the separate day-trade securities-shortfall borrowing
# mechanism, whose one-day fee can be much larger.
MARGIN_SHORT_HANDLING_FEE_RATE = 0.0010
MARGIN_SHORT_ANNUAL_BORROW_RATE = 0.20
MARGIN_SHORT_ONE_DAY_BORROW_RATE = MARGIN_SHORT_ANNUAL_BORROW_RATE / 365.0
COMPILED_BLOCK_ROWS = 4


_COMPILED_DAY_CACHE: dict[tuple[object, ...], object] = {}
_COMPILED_DAY_FAILED: set[tuple[object, ...]] = set()
_COMPILE_STATS = {
    "compile_constructors": 0,
    "compiled_day_calls": 0,
    "eager_fallback_calls": 0,
}


@dataclass(frozen=True)
class TaiwanDayTradeMinuteResult:
    """Exact intraday fills plus the T+2 net-cash claim ledger.

    Economic returns are recognized when the intraday position closes, while
    cash changes only when the net claim settles.  Iteration preserves the
    historical four-value ABI for callers that only consume returns,
    turnover, weights, and equity.
    """

    strategy_returns: torch.Tensor
    turnovers: torch.Tensor
    weights_history: torch.Tensor
    equity_scale_history: torch.Tensor
    net_claims: torch.Tensor
    cash_history: torch.Tensor
    payables_history: torch.Tensor
    receivables_history: torch.Tensor
    final_cash: torch.Tensor
    final_payables: torch.Tensor
    final_receivables: torch.Tensor
    final_equity_scale: torch.Tensor

    def __iter__(self):
        yield self.strategy_returns
        yield self.turnovers
        yield self.weights_history
        yield self.equity_scale_history


def _env_truthy(name: str, default: str) -> bool:
    return os.environ.get(name, default).strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
        "",
    }


def _strict_no_fallback_enabled() -> bool:
    return _env_truthy("STOCKAGENT_STRICT_NO_FALLBACK", "0")


def get_tw_day_trade_minute_compile_stats(*, reset: bool = False) -> dict[str, int]:
    """Return process-local daily-kernel compile audit counters."""

    snapshot = dict(_COMPILE_STATS)
    if reset:
        for name in _COMPILE_STATS:
            _COMPILE_STATS[name] = 0
    return snapshot


def _ste_floor_lots(shares: torch.Tensor) -> torch.Tensor:
    """Exact whole-lot forward value with a straight-through gradient."""

    exact = torch.floor(shares.clamp_min(0.0) / BOARD_LOT_SHARES) * BOARD_LOT_SHARES
    return shares + (exact - shares).detach()


def _capacity(volume: torch.Tensor) -> torch.Tensor:
    clean = torch.where(
        torch.isfinite(volume) & (volume > 0.0), volume, torch.zeros_like(volume)
    )
    return (
        torch.floor(clean * MINUTE_VOLUME_PARTICIPATION / BOARD_LOT_SHARES)
        * BOARD_LOT_SHARES
    )


def _capacity_at_participation(
    volume: torch.Tensor,
    maximum_volume_participation: float,
) -> torch.Tensor:
    clean = torch.where(
        torch.isfinite(volume) & (volume > 0.0), volume, torch.zeros_like(volume)
    )
    return (
        torch.floor(
            clean * float(maximum_volume_participation) / BOARD_LOT_SHARES
        )
        * BOARD_LOT_SHARES
    )


def _sequential_market_fill(
    desired_shares: torch.Tensor,
    prices: torch.Tensor,
    volumes: torch.Tensor,
    *,
    maximum_volume_participation: float,
) -> tuple[torch.Tensor, torch.Tensor]:
    """Fill a persistent whole-lot order against chronological minute volume."""

    valid_price = torch.isfinite(prices) & (prices > 0.0)
    safe_prices = torch.where(valid_price, prices, torch.zeros_like(prices))
    capacity = _capacity_at_participation(
        torch.where(valid_price, volumes, torch.zeros_like(volumes)),
        maximum_volume_participation,
    )
    cumulative = torch.cumsum(capacity, dim=-1)
    before = cumulative - capacity
    quantities = torch.minimum(
        capacity,
        (desired_shares.unsqueeze(-1) - before).clamp_min(0.0),
    )
    return quantities.sum(dim=-1), (quantities * safe_prices).sum(dim=-1)


def _settle_day_trade_claim(
    *,
    pnl: torch.Tensor,
    traded_notional: torch.Tensor,
    entry_weights: torch.Tensor,
    cash: torch.Tensor,
    payables: torch.Tensor,
    receivables: torch.Tensor,
    equity_scale: torch.Tensor,
    capital0: torch.Tensor,
    state_advance: torch.Tensor,
) -> tuple[
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
]:
    """Apply the shared economic-NAV and T+2 net-claim recurrence."""

    nav_twd = capital0 * equity_scale.clamp_min(1.0e-12)
    simple_return = (pnl / nav_twd.clamp_min(1.0e-12)).clamp_min(-1.0 + 1.0e-6)
    simple_return = torch.where(
        state_advance, simple_return, torch.zeros_like(simple_return)
    )
    net_claim = equity_scale * simple_return

    due_payable = payables[0]
    due_receivable = receivables[0]
    settled_cash = cash + due_receivable - due_payable
    queue_zero = torch.zeros_like(due_payable).unsqueeze(0)
    shifted_payables = torch.cat((payables[1:], queue_zero), dim=0)
    shifted_receivables = torch.cat((receivables[1:], queue_zero), dim=0)
    cash = torch.where(state_advance, settled_cash, cash)
    payables = torch.where(state_advance, shifted_payables, payables)
    receivables = torch.where(state_advance, shifted_receivables, receivables)
    negative_claim = (-net_claim).clamp_min(0.0)
    positive_claim = net_claim.clamp_min(0.0)
    insertion = torch.nn.functional.one_hot(
        torch.tensor(
            int(payables.numel()) - 1,
            device=payables.device,
        ),
        num_classes=int(payables.numel()),
    ).to(dtype=payables.dtype)
    payables = payables + insertion * negative_claim
    receivables = receivables + insertion * positive_claim
    next_equity_scale = cash + receivables.sum() - payables.sum()
    turnover = traded_notional.sum() / nav_twd.clamp_min(1.0e-12)
    return (
        torch.log1p(simple_return),
        turnover,
        entry_weights,
        next_equity_scale,
        net_claim,
        cash,
        payables,
        receivables,
    )


def _run_tw_day_trade_full_session_volume_day(
    target_weights: torch.Tensor,
    execution_row: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_short_open_mask: torch.Tensor,
    day_trade_eligible_mask: torch.Tensor,
    day_trade_can_buy_open_mask: torch.Tensor,
    day_trade_can_sell_open_mask: torch.Tensor,
    buy_fee_rates: torch.Tensor,
    sell_fee_rates: torch.Tensor,
    normal_sell_fee_rates: torch.Tensor,
    cash: torch.Tensor,
    payables: torch.Tensor,
    receivables: torch.Tensor,
    equity_scale: torch.Tensor,
    capital0: torch.Tensor,
    state_advance: torch.Tensor,
    maximum_volume_participation: float,
) -> tuple[
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
]:
    """Execute one 09:00 target through persistent minute market orders."""

    official_open = execution_row[:, 0, FULL_SESSION_PRICE]
    valid_open = torch.isfinite(official_open) & (official_open > 0.0)
    safe_official_open = torch.where(
        valid_open, official_open, torch.ones_like(official_open)
    )
    request = torch.where(
        state_advance, target_weights, torch.zeros_like(target_weights)
    )
    permission = (
        tradable_mask.bool()
        & day_trade_eligible_mask.bool()
        & torch.where(
            request < 0.0,
            can_short_open_mask.bool() & day_trade_can_sell_open_mask.bool(),
            day_trade_can_buy_open_mask.bool(),
        )
        & valid_open
    )
    request = torch.where(permission, request, torch.zeros_like(request))
    deployable_twd = capital0 * cash.clamp_min(0.0)
    nav_twd = capital0 * equity_scale.clamp_min(1.0e-12)
    desired = _ste_floor_lots(
        request.abs() * deployable_twd / safe_official_open.clamp_min(1.0e-12)
    )
    # The opening order is live from 09:00. Bars 09:01..13:19 represent
    # chronological fills; at 13:20 any unfilled entry remainder is cancelled.
    entry_shares, entry_notional = _sequential_market_fill(
        desired,
        execution_row[:, 1:260, FULL_SESSION_PRICE],
        execution_row[:, 1:260, FULL_SESSION_VOLUME_SHARES],
        maximum_volume_participation=maximum_volume_participation,
    )
    direction = torch.sign(request)
    entry_average = torch.where(
        entry_shares > 0.0,
        entry_notional / entry_shares.clamp_min(1.0),
        torch.zeros_like(entry_shares),
    )

    # A close decision made after the completed 13:20 bar can execute only in
    # the right-labelled 13:21..13:30 bars. Non-trading minutes naturally
    # contribute zero capacity; the 13:30 row is the closing auction.
    exit_prices = execution_row[:, 261:271, FULL_SESSION_PRICE]
    exit_volumes = execution_row[:, 261:271, FULL_SESSION_VOLUME_SHARES]
    exit_shares, exit_notional = _sequential_market_fill(
        entry_shares,
        exit_prices,
        exit_volumes,
        maximum_volume_participation=maximum_volume_participation,
    )
    remaining = (entry_shares - exit_shares).clamp_min(0.0)

    close_price = execution_row[:, 270, FULL_SESSION_PRICE]
    valid_close = torch.isfinite(close_price) & (close_price > 0.0)
    safe_close = torch.where(valid_close, close_price, entry_average)
    marked_remaining = torch.where(valid_close, remaining, torch.zeros_like(remaining))
    residual_exit_notional = marked_remaining * safe_close
    gross_pnl = direction * (
        exit_notional + residual_exit_notional - entry_notional
    )
    entry_fees = entry_notional * torch.where(
        direction > 0.0, buy_fee_rates, sell_fee_rates
    )
    exit_fees = exit_notional * torch.where(
        direction > 0.0, sell_fee_rates, buy_fee_rates
    )
    residual_exit_fees = residual_exit_notional * torch.where(
        direction > 0.0, normal_sell_fee_rates, buy_fee_rates
    )
    normal_sell_tax_adjustment = (
        normal_sell_fee_rates - sell_fee_rates
    ).clamp_min(0.0)
    residual_entry_notional = marked_remaining * entry_average
    margin_cost = torch.where(
        direction > 0.0,
        residual_exit_notional * MARGIN_FINANCING_ONE_DAY_RATE,
        residual_exit_notional * MARGIN_SHORT_ONE_DAY_BORROW_RATE
        + residual_entry_notional
        * (MARGIN_SHORT_HANDLING_FEE_RATE + normal_sell_tax_adjustment),
    )
    missing_close_loss = torch.where(
        (remaining > 0.0) & ~valid_close,
        remaining * entry_average,
        torch.zeros_like(remaining),
    )
    pnl = (
        gross_pnl.sum()
        - entry_fees.sum()
        - exit_fees.sum()
        - residual_exit_fees.sum()
        - margin_cost.sum()
        - missing_close_loss.sum()
    )
    traded_notional = entry_notional + exit_notional + residual_exit_notional
    entry_weights = direction * entry_notional / nav_twd.clamp_min(1.0e-12)
    return _settle_day_trade_claim(
        pnl=pnl,
        traded_notional=traded_notional,
        entry_weights=entry_weights,
        cash=cash,
        payables=payables,
        receivables=receivables,
        equity_scale=equity_scale,
        capital0=capital0,
        state_advance=state_advance,
    )


def _run_tw_day_trade_minute_day(
    target_weights: torch.Tensor,
    execution_row: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_short_open_mask: torch.Tensor,
    day_trade_eligible_mask: torch.Tensor,
    day_trade_can_buy_open_mask: torch.Tensor,
    day_trade_can_sell_open_mask: torch.Tensor,
    buy_fee_rates: torch.Tensor,
    sell_fee_rates: torch.Tensor,
    normal_sell_fee_rates: torch.Tensor,
    cash: torch.Tensor,
    payables: torch.Tensor,
    receivables: torch.Tensor,
    equity_scale: torch.Tensor,
    capital0: torch.Tensor,
    state_advance: torch.Tensor,
    maximum_volume_participation: float,
) -> tuple[
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
]:
    """Fixed-shape one-session kernel used by the chronological executor."""

    if execution_row.ndim == 3:
        return _run_tw_day_trade_full_session_volume_day(
            target_weights,
            execution_row,
            tradable_mask,
            can_short_open_mask,
            day_trade_eligible_mask,
            day_trade_can_buy_open_mask,
            day_trade_can_sell_open_mask,
            buy_fee_rates,
            sell_fee_rates,
            normal_sell_fee_rates,
            cash,
            payables,
            receivables,
            equity_scale,
            capital0,
            state_advance,
            maximum_volume_participation,
        )

    request = torch.where(
        state_advance, target_weights, torch.zeros_like(target_weights)
    )
    official_open = execution_row[:, F.OFFICIAL_OPEN]
    direction = torch.sign(request)
    official_open = execution_row[:, F.OFFICIAL_OPEN]
    is_daily_proxy = execution_row[:, F.DAILY_PROXY_FLAG] > 0.5
    minute_entry_price = execution_row[:, F.ENTRY_VWAP_0901]
    proxy_entry_price = torch.where(
        direction < 0.0,
        execution_row[:, F.DAILY_PROXY_SHORT_ENTRY_PRICE],
        execution_row[:, F.DAILY_PROXY_LONG_ENTRY_PRICE],
    )
    valid_minute_entry = (
        ~is_daily_proxy
        & torch.isfinite(official_open)
        & (official_open > 0.0)
        & torch.isfinite(minute_entry_price)
        & (minute_entry_price > 0.0)
    )
    valid_proxy_entry = (
        is_daily_proxy
        & torch.isfinite(official_open)
        & (official_open > 0.0)
        & torch.isfinite(proxy_entry_price)
        & (proxy_entry_price > 0.0)
    )
    valid_entry = valid_minute_entry | valid_proxy_entry
    safe_official_open = torch.where(
        torch.isfinite(official_open) & (official_open > 0.0),
        official_open,
        torch.ones_like(official_open),
    )
    entry_price = torch.where(
        valid_entry,
        torch.where(is_daily_proxy, proxy_entry_price, minute_entry_price),
        safe_official_open,
    )
    permission = (
        tradable_mask.bool()
        & day_trade_eligible_mask.bool()
        & torch.where(
            request < 0.0,
            can_short_open_mask.bool() & day_trade_can_sell_open_mask.bool(),
            day_trade_can_buy_open_mask.bool(),
        )
        & valid_entry
    )
    request = torch.where(permission, request, torch.zeros_like(request))
    # Orders are submitted before T+2 close settlement.  Pending gains are
    # economic NAV but are not deployable cash; they can size a new order only
    # from the following session (T+3 in decision-time language).
    deployable_twd = capital0 * cash.clamp_min(0.0)
    nav_twd = capital0 * equity_scale.clamp_min(1.0e-12)
    desired = _ste_floor_lots(
        request.abs() * deployable_twd / safe_official_open.clamp_min(1.0e-12)
    )
    entry_shares = torch.minimum(
        desired,
        _capacity(
            torch.where(
                is_daily_proxy,
                execution_row[:, F.DAILY_PROXY_VOLUME],
                execution_row[:, F.ENTRY_VOLUME_0901],
            )
        ),
    )
    signed_entry = torch.sign(request) * entry_shares
    # ``abs(0)`` has zero derivative. Multiplying by the requested direction
    # keeps the exact fail-closed forward value while retaining its STE signal.
    remaining = signed_entry * direction
    gross_pnl = torch.zeros_like(remaining)
    fees = (
        remaining
        * entry_price
        * torch.where(direction > 0.0, buy_fee_rates, sell_fee_rates)
    )
    traded_notional = remaining * entry_price

    limit_price = execution_row[:, F.LIMIT_PRICE_1320]
    valid_limit_price = torch.isfinite(limit_price) & (limit_price > 0.0)
    safe_limit_price = torch.where(valid_limit_price, limit_price, entry_price)
    for high_field in (F.HIGH_1321, F.HIGH_1322, F.HIGH_1323):
        high = execution_row[:, high_field]
        low = execution_row[:, int(high_field) + 1]
        volume = execution_row[:, int(high_field) + 2]
        # Strict penetration is intentional: equality has unknown queue
        # priority and therefore cannot be claimed as a historical fill.
        crossed = torch.where(direction > 0.0, high > limit_price, low < limit_price)
        valid_limit = crossed & valid_limit_price & ~is_daily_proxy
        quantity = torch.minimum(remaining, _capacity(volume))
        quantity = torch.where(valid_limit, quantity, torch.zeros_like(quantity))
        gross_pnl = gross_pnl + direction * quantity * (safe_limit_price - entry_price)
        fees = fees + quantity * safe_limit_price * torch.where(
            direction > 0.0, sell_fee_rates, buy_fee_rates
        )
        traded_notional = traded_notional + quantity * safe_limit_price
        remaining = remaining - quantity

    for price_field, volume_field in (
        (F.MARKET_VWAP_1324, F.MARKET_VOLUME_1324),
        (F.MARKET_VWAP_1325, F.MARKET_VOLUME_1325),
        (F.AUCTION_PRICE_1330, F.AUCTION_VOLUME_1330),
    ):
        price = execution_row[:, price_field]
        valid_price = (
            torch.isfinite(price) & (price > 0.0) & ~is_daily_proxy
        )
        safe_price = torch.where(valid_price, price, entry_price)
        quantity = torch.minimum(remaining, _capacity(execution_row[:, volume_field]))
        quantity = torch.where(valid_price, quantity, torch.zeros_like(quantity))
        gross_pnl = gross_pnl + direction * quantity * (safe_price - entry_price)
        fees = fees + quantity * safe_price * torch.where(
            direction > 0.0, sell_fee_rates, buy_fee_rates
        )
        traded_notional = traded_notional + quantity * safe_price
        remaining = remaining - quantity

    # The pre-minute-data daily proxy is deliberately one observation per
    # session, not a fabricated 13:20/13:24/13:30 path.  Its entry and exit
    # both obey the same 50%-of-estimated-minute-volume whole-lot cap.  Since
    # entry shares already cannot exceed that capacity, every admitted proxy
    # position can close here without inventing extra liquidity.
    proxy_exit_price = torch.where(
        direction < 0.0,
        execution_row[:, F.DAILY_PROXY_SHORT_EXIT_PRICE],
        execution_row[:, F.DAILY_PROXY_LONG_EXIT_PRICE],
    )
    valid_proxy_exit = (
        is_daily_proxy
        & torch.isfinite(proxy_exit_price)
        & (proxy_exit_price > 0.0)
    )
    safe_proxy_exit = torch.where(valid_proxy_exit, proxy_exit_price, entry_price)
    proxy_exit_quantity = torch.minimum(
        remaining,
        _capacity(execution_row[:, F.DAILY_PROXY_VOLUME]),
    )
    proxy_exit_quantity = torch.where(
        valid_proxy_exit,
        proxy_exit_quantity,
        torch.zeros_like(proxy_exit_quantity),
    )
    gross_pnl = (
        gross_pnl
        + direction * proxy_exit_quantity * (safe_proxy_exit - entry_price)
    )
    fees = fees + proxy_exit_quantity * safe_proxy_exit * torch.where(
        direction > 0.0, sell_fee_rates, buy_fee_rates
    )
    traded_notional = traded_notional + proxy_exit_quantity * safe_proxy_exit
    remaining = remaining - proxy_exit_quantity

    observed_auction_close = torch.where(
        is_daily_proxy,
        proxy_exit_price,
        execution_row[:, F.AUCTION_PRICE_1330],
    )
    official_close = execution_row[:, F.OFFICIAL_CLOSE]
    # A missing 13:30 minute row means that no auction liquidity can be
    # claimed; it does not make the security worthless.  Value the residual at
    # the official daily close and route it through the existing unlimited
    # financing/borrow conversion.  The normal tax/fee and highest financing
    # costs below still apply, so this is not a free or fabricated fill.
    close_price = torch.where(
        torch.isfinite(observed_auction_close) & (observed_auction_close > 0.0),
        observed_auction_close,
        official_close,
    )
    valid_close = torch.isfinite(close_price) & (close_price > 0.0)
    safe_close = torch.where(valid_close, close_price, entry_price)
    # Residual securities are marked at the official close and assumed to
    # obtain unlimited margin financing/short inventory.  The daily stock book
    # is flattened after charging the close-side fee and margin costs; only the
    # resulting net cash difference enters the T+2 queue below.
    marked_remaining = torch.where(valid_close, remaining, torch.zeros_like(remaining))
    gross_pnl = gross_pnl + direction * marked_remaining * (safe_close - entry_price)
    residual_exit_fees = (
        marked_remaining
        * safe_close
        * torch.where(direction > 0.0, normal_sell_fee_rates, buy_fee_rates)
    )
    normal_sell_tax_adjustment = (normal_sell_fee_rates - sell_fee_rates).clamp_min(0.0)
    margin_cost = torch.where(
        direction > 0.0,
        marked_remaining * safe_close * MARGIN_FINANCING_ONE_DAY_RATE,
        marked_remaining
        * (
            safe_close * MARGIN_SHORT_ONE_DAY_BORROW_RATE
            + entry_price
            * (MARGIN_SHORT_HANDLING_FEE_RATE + normal_sell_tax_adjustment)
        ),
    )
    traded_notional = traded_notional + marked_remaining * safe_close
    # If both the auction observation and official daily close are absent,
    # retain the conservative fail-closed total loss.  A source-data failure
    # must never become a free stale mark.
    missing_close_loss = torch.where(
        (remaining > 0.0) & ~valid_close,
        remaining * entry_price,
        torch.zeros_like(remaining),
    )
    pnl = (
        gross_pnl.sum()
        - fees.sum()
        - residual_exit_fees.sum()
        - margin_cost.sum()
        - missing_close_loss.sum()
    )
    entry_weights = signed_entry * entry_price / nav_twd.clamp_min(1.0e-12)
    return _settle_day_trade_claim(
        pnl=pnl,
        traded_notional=traded_notional,
        entry_weights=entry_weights,
        cash=cash,
        payables=payables,
        receivables=receivables,
        equity_scale=equity_scale,
        capital0=capital0,
        state_advance=state_advance,
    )


def _run_tw_day_trade_minute_block(
    target_weights: torch.Tensor,
    execution_tape: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_short_open_mask: torch.Tensor,
    day_trade_eligible_mask: torch.Tensor,
    day_trade_can_buy_open_mask: torch.Tensor,
    day_trade_can_sell_open_mask: torch.Tensor,
    buy_fee_rates: torch.Tensor,
    sell_fee_rates: torch.Tensor,
    normal_sell_fee_rates: torch.Tensor,
    cash: torch.Tensor,
    payables: torch.Tensor,
    receivables: torch.Tensor,
    equity_scale: torch.Tensor,
    capital0: torch.Tensor,
    state_advance: torch.Tensor,
    maximum_volume_participation: float,
) -> tuple[
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
]:
    """Execute one fixed four-session block without breaking NAV recurrence."""

    log_rows: list[torch.Tensor] = []
    turnover_rows: list[torch.Tensor] = []
    entry_weight_rows: list[torch.Tensor] = []
    equity_rows: list[torch.Tensor] = []
    claim_rows: list[torch.Tensor] = []
    cash_rows: list[torch.Tensor] = []
    payable_rows: list[torch.Tensor] = []
    receivable_rows: list[torch.Tensor] = []
    for idx in range(COMPILED_BLOCK_ROWS):
        (
            log_return,
            turnover,
            entry_weights,
            equity_scale,
            net_claim,
            cash,
            payables,
            receivables,
        ) = _run_tw_day_trade_minute_day(
            target_weights[idx],
            execution_tape[idx],
            tradable_mask[idx],
            can_short_open_mask[idx],
            day_trade_eligible_mask[idx],
            day_trade_can_buy_open_mask[idx],
            day_trade_can_sell_open_mask[idx],
            buy_fee_rates,
            sell_fee_rates,
            normal_sell_fee_rates,
            cash,
            payables,
            receivables,
            equity_scale,
            capital0,
            state_advance[idx],
            maximum_volume_participation,
        )
        log_rows.append(log_return)
        turnover_rows.append(turnover)
        entry_weight_rows.append(entry_weights)
        equity_rows.append(equity_scale)
        claim_rows.append(net_claim)
        cash_rows.append(cash)
        payable_rows.append(payables)
        receivable_rows.append(receivables)
    return (
        torch.stack(log_rows),
        torch.stack(turnover_rows),
        torch.stack(entry_weight_rows),
        torch.stack(equity_rows),
        torch.stack(claim_rows),
        torch.stack(cash_rows),
        torch.stack(payable_rows),
        torch.stack(receivable_rows),
    )


def _block_runner_for(
    weights: torch.Tensor,
    execution_tape: torch.Tensor,
    settlement_lag_sessions: int,
    maximum_volume_participation: float,
):
    """Return one reusable four-session kernel for a fixed symbol axis."""

    if (
        weights.device.type != "cuda"
        or not hasattr(torch, "compile")
        or not _env_truthy("STOCKAGENT_BACKTEST_COMPILE", "1")
        or torch.compiler.is_compiling()
    ):
        return _run_tw_day_trade_minute_block, None
    key = (
        weights.device.type,
        weights.device.index,
        weights.dtype,
        int(weights.size(1)),
        tuple(int(value) for value in execution_tape.shape[2:]),
        int(settlement_lag_sessions),
        float(maximum_volume_participation),
    )
    if key in _COMPILED_DAY_FAILED:
        if _strict_no_fallback_enabled():
            raise RuntimeError(
                "TW day-trade minute block kernel was previously marked failed; "
                "strict_no_fallback=true disables eager fallback"
            )
        return _run_tw_day_trade_minute_block, None
    cached = _COMPILED_DAY_CACHE.get(key)
    if cached is None:
        cached = torch.compile(
            _run_tw_day_trade_minute_block,
            fullgraph=True,
            dynamic=False,
            options={"triton.cudagraphs": False},
        )
        _COMPILED_DAY_CACHE[key] = cached
        _COMPILE_STATS["compile_constructors"] += 1
    return cached, key


def run_tw_day_trade_minute_execution(
    target_weights: torch.Tensor,
    execution_tape: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_short_open_mask: torch.Tensor,
    day_trade_eligible_mask: torch.Tensor,
    day_trade_can_buy_open_mask: torch.Tensor,
    day_trade_can_sell_open_mask: torch.Tensor,
    buy_fee_rates: torch.Tensor,
    sell_fee_rates: torch.Tensor,
    normal_sell_fee_rates: torch.Tensor,
    commission_rebate_rates: torch.Tensor | None = None,
    commission_rebate_timing: str = "daily_close",
    *,
    initial_capital_twd: float,
    maximum_volume_participation: float = MINUTE_VOLUME_PARTICIPATION,
    settlement_lag_sessions: int = 2,
    state_advance_mask: torch.Tensor | None = None,
    initial_cash: torch.Tensor | None = None,
    initial_payables: torch.Tensor | None = None,
    initial_receivables: torch.Tensor | None = None,
    initial_equity_scale: torch.Tensor | None = None,
) -> TaiwanDayTradeMinuteResult:
    """Execute one daily target through the fixed intraday order schedule.

    Returns exact fills plus a T+2 ledger containing only the signed net cash
    difference of each completed day trade.  The stock position is flat after
    every session; gross buy and sell consideration is never carried twice.
    The forward pass uses exact board-lot quantities.  Since exact rounding has
    zero derivative almost everywhere, only its backward derivative uses the
    straight-through estimator; reported loss/NAV always uses the exact forward
    lots and fills.  Residuals have unlimited financing/short inventory and are
    accounting-closed at the official close.  That close still creates a net
    T+2 cash claim; it creates neither a carried stock position nor a default.
    A daily-close broker commission rebate is netted inside that same claim;
    tax is never rebated. Deferred rebate timing fails closed because this
    executor intentionally carries no separate rebate-receivable state.
    """

    if target_weights.ndim != 2:
        raise ValueError("daily target weights must have shape [T,S]")
    scheduled_shape = (*target_weights.shape, DAY_TRADE_EXECUTION_FIELD_COUNT)
    full_session_shape = (
        *target_weights.shape,
        DAY_TRADE_FULL_SESSION_MINUTES,
        DAY_TRADE_FULL_SESSION_FIELDS,
    )
    if tuple(execution_tape.shape) not in {scheduled_shape, full_session_shape}:
        raise ValueError(
            "execution_tape must have scheduled shape "
            f"{scheduled_shape} or full-session shape {full_session_shape}"
        )
    for name, value in (
        ("tradable_mask", tradable_mask),
        ("can_short_open_mask", can_short_open_mask),
        ("day_trade_eligible_mask", day_trade_eligible_mask),
        ("day_trade_can_buy_open_mask", day_trade_can_buy_open_mask),
        ("day_trade_can_sell_open_mask", day_trade_can_sell_open_mask),
    ):
        if tuple(value.shape) != tuple(target_weights.shape):
            raise ValueError(f"{name} must match target_weights")
    if not float(initial_capital_twd) > 0.0:
        raise ValueError("initial_capital_twd must be positive")
    participation = float(maximum_volume_participation)
    if not 0.0 < participation <= 1.0:
        raise ValueError("maximum_volume_participation must be in (0, 1]")
    if tuple(execution_tape.shape) == scheduled_shape and abs(
        participation - MINUTE_VOLUME_PARTICIPATION
    ) > 1.0e-12:
        raise ValueError(
            "scheduled event tape preserves its 50% capacity contract; use the "
            "full-session tape for another participation rate"
        )
    if (
        isinstance(settlement_lag_sessions, bool)
        or int(settlement_lag_sessions) != settlement_lag_sessions
        or int(settlement_lag_sessions) <= 0
    ):
        raise ValueError("settlement_lag_sessions must be a positive integer")
    lag = int(settlement_lag_sessions)
    device = target_weights.device
    dtype = torch.float32
    tape = execution_tape.to(device=device, dtype=dtype)
    weights = target_weights.float()
    gross_buy_fees = buy_fee_rates.to(device=device, dtype=dtype).reshape(-1)
    gross_sell_fees = sell_fee_rates.to(device=device, dtype=dtype).reshape(-1)
    gross_normal_sell_fees = normal_sell_fee_rates.to(
        device=device, dtype=dtype
    ).reshape(-1)
    rebate_timing = normalize_commission_rebate_timing(
        commission_rebate_timing
    )
    rebate_fees = (
        torch.zeros_like(gross_buy_fees)
        if commission_rebate_rates is None
        else commission_rebate_rates.to(device=device, dtype=dtype).reshape(-1)
    )
    if (
        gross_buy_fees.numel() != weights.size(1)
        or gross_sell_fees.numel() != weights.size(1)
        or gross_normal_sell_fees.numel() != weights.size(1)
        or rebate_fees.numel() != weights.size(1)
    ):
        raise ValueError("fee and commission-rebate vectors must have shape [S]")
    if rebate_timing != "daily_close" and bool((rebate_fees > 0.0).any().item()):
        raise ValueError(
            "exact-minute tw_day_trade currently supports commission rebates "
            "only at daily_close; deferred rebates require an explicit "
            "cross-session rebate ledger"
        )
    buy_fees = (gross_buy_fees - rebate_fees).clamp_min(0.0)
    sell_fees = (gross_sell_fees - rebate_fees).clamp_min(0.0)
    normal_sell_fees = (gross_normal_sell_fees - rebate_fees).clamp_min(0.0)
    if bool((normal_sell_fees < sell_fees).any().item()):
        raise ValueError(
            "normal_sell_fee_rates must be at least day-trade sell_fee_rates"
        )
    advance = (
        torch.ones(weights.size(0), device=device, dtype=torch.bool)
        if state_advance_mask is None
        else state_advance_mask.to(device=device, dtype=torch.bool)
    )
    equity_scale = (
        weights.new_tensor(1.0)
        if initial_equity_scale is None
        else initial_equity_scale.to(device=device, dtype=dtype)
    )

    def initial_queue(value: torch.Tensor | None, name: str) -> torch.Tensor:
        queue = (
            torch.zeros(lag, device=device, dtype=dtype)
            if value is None
            else value.to(device=device, dtype=dtype).reshape(-1)
        )
        if int(queue.numel()) != lag:
            raise ValueError(f"{name} must have shape [{lag}]")
        if not bool((torch.isfinite(queue) & (queue >= 0.0)).all().item()):
            raise ValueError(f"{name} must be finite and non-negative")
        return queue

    payables = initial_queue(initial_payables, "initial_payables")
    receivables = initial_queue(initial_receivables, "initial_receivables")
    cash = (
        equity_scale - receivables.sum() + payables.sum()
        if initial_cash is None
        else initial_cash.to(device=device, dtype=dtype).reshape(())
    )
    if not bool(torch.isfinite(cash).item()):
        raise ValueError("initial_cash must be finite")
    accounting_equity = cash + receivables.sum() - payables.sum()
    tolerance = 2.0e-5 * max(1.0, abs(float(equity_scale.item())))
    if abs(float((accounting_equity - equity_scale).item())) > tolerance:
        raise ValueError(
            "initial minute T+2 cash/claim state does not reconcile to "
            "initial_equity_scale"
        )
    log_rows: list[torch.Tensor] = []
    turnover_rows: list[torch.Tensor] = []
    entry_weight_rows: list[torch.Tensor] = []
    equity_rows: list[torch.Tensor] = []
    claim_rows: list[torch.Tensor] = []
    cash_rows: list[torch.Tensor] = []
    payable_rows: list[torch.Tensor] = []
    receivable_rows: list[torch.Tensor] = []
    capital0 = weights.new_tensor(float(initial_capital_twd))
    block_runner, compiled_key = _block_runner_for(
        weights,
        tape,
        lag,
        participation,
    )
    total_rows = int(weights.size(0))

    for idx in range(0, total_rows, COMPILED_BLOCK_ROWS):
        valid_block_rows = min(COMPILED_BLOCK_ROWS, total_rows - idx)
        if valid_block_rows == COMPILED_BLOCK_ROWS:
            block_selector: slice | torch.Tensor = slice(idx, idx + COMPILED_BLOCK_ROWS)
            block_advance = advance[block_selector]
        else:
            # Pad the final ABI by repeating its last physical row, then mask
            # the repeated decisions. This is exact because state_advance=False
            # forces zero return and zero action-gradient for padded rows.
            block_selector = torch.arange(
                idx,
                idx + COMPILED_BLOCK_ROWS,
                device=device,
                dtype=torch.long,
            ).clamp_max(total_rows - 1)
            block_advance = advance.index_select(0, block_selector) & (
                torch.arange(COMPILED_BLOCK_ROWS, device=device) < valid_block_rows
            )
        try:
            block_result = block_runner(
                weights[block_selector],
                tape[block_selector],
                tradable_mask[block_selector],
                can_short_open_mask[block_selector],
                day_trade_eligible_mask[block_selector],
                day_trade_can_buy_open_mask[block_selector],
                day_trade_can_sell_open_mask[block_selector],
                buy_fees,
                sell_fees,
                normal_sell_fees,
                cash,
                payables,
                receivables,
                equity_scale,
                capital0,
                block_advance,
                participation,
            )
        except Exception as exc:
            if compiled_key is None or _strict_no_fallback_enabled():
                raise
            _COMPILED_DAY_CACHE.pop(compiled_key, None)
            _COMPILED_DAY_FAILED.add(compiled_key)
            _COMPILE_STATS["eager_fallback_calls"] += 1
            compiled_key = None
            block_runner = _run_tw_day_trade_minute_block
            print(
                "[TW day-trade minute compile] four-session kernel failed; "
                f"falling back to eager: {type(exc).__name__}: {exc}"
            )
            block_result = block_runner(
                weights[block_selector],
                tape[block_selector],
                tradable_mask[block_selector],
                can_short_open_mask[block_selector],
                day_trade_eligible_mask[block_selector],
                day_trade_can_buy_open_mask[block_selector],
                day_trade_can_sell_open_mask[block_selector],
                buy_fees,
                sell_fees,
                normal_sell_fees,
                cash,
                payables,
                receivables,
                equity_scale,
                capital0,
                block_advance,
                participation,
            )
        else:
            if compiled_key is not None:
                _COMPILE_STATS["compiled_day_calls"] += COMPILED_BLOCK_ROWS
        (
            block_log,
            block_turnover,
            block_weights,
            block_equity,
            block_claims,
            block_cash,
            block_payables,
            block_receivables,
        ) = block_result
        log_rows.append(block_log[:valid_block_rows])
        turnover_rows.append(block_turnover[:valid_block_rows])
        entry_weight_rows.append(block_weights[:valid_block_rows])
        equity_rows.append(block_equity[:valid_block_rows])
        claim_rows.append(block_claims[:valid_block_rows])
        cash_rows.append(block_cash[:valid_block_rows])
        payable_rows.append(block_payables[:valid_block_rows])
        receivable_rows.append(block_receivables[:valid_block_rows])
        equity_scale = block_equity[valid_block_rows - 1]
        cash = block_cash[valid_block_rows - 1]
        payables = block_payables[valid_block_rows - 1]
        receivables = block_receivables[valid_block_rows - 1]

    strategy_returns = torch.cat(log_rows)
    turnovers = torch.cat(turnover_rows)
    weights_history = torch.cat(entry_weight_rows)
    equity_scale_history = torch.cat(equity_rows)
    net_claims = torch.cat(claim_rows)

    return TaiwanDayTradeMinuteResult(
        strategy_returns=strategy_returns,
        turnovers=turnovers,
        weights_history=weights_history,
        equity_scale_history=equity_scale_history,
        net_claims=net_claims,
        cash_history=torch.cat(cash_rows),
        payables_history=torch.cat(payable_rows),
        receivables_history=torch.cat(receivable_rows),
        final_cash=cash,
        final_payables=payables,
        final_receivables=receivables,
        final_equity_scale=equity_scale_history[-1],
    )


__all__ = [
    "COMPILED_BLOCK_ROWS",
    "TaiwanDayTradeMinuteResult",
    "get_tw_day_trade_minute_compile_stats",
    "run_tw_day_trade_minute_execution",
]

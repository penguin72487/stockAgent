"""Daily Taiwan day-trade execution with a net T+2-close cash ledger.

The policy makes one signed decision per session.  Daily open-to-close labels
are the only price path used: no minute K bar is read.  A normal close-side
permission executes the ordinary day-trade round trip.  If the close side is
blocked but the official close mark exists, the residual is assumed to obtain
unlimited margin financing/short inventory and is accounting-closed at that
mark with conservative margin costs.  Stock positions never carry overnight,
but the fee-adjusted net cash difference settles after T+2 orders and is first
available to size a new order at T+3 open.
"""

from __future__ import annotations

from dataclasses import dataclass
import os

import torch


# Direct API callers keep the historical conservative fallback.  Training sets
# STOCKAGENT_TW_CONTINUOUS_COMPILE_CHUNK_ROWS from its validated config (32 for
# the active TW day-trade contract), so the same performance knob controls all
# Taiwan settlement executors without adding a second semantic setting.
COMPILED_BLOCK_ROWS = 4

_COMPILED_BLOCK_CACHE: dict[tuple[object, ...], object] = {}
_COMPILED_BLOCK_FAILED: set[tuple[object, ...]] = set()
_COMPILE_STATS: dict[str, int] = {
    "compile_constructors": 0,
    "compiled_block_calls": 0,
    "compiled_day_calls": 0,
    "eager_fallback_calls": 0,
}


@dataclass(frozen=True)
class TaiwanDayTradeDailyResult:
    """Open-to-close fills plus the T+2 net-cash claim ledger.

    Economic NAV recognizes the claim on T.  Deployable cash changes only
    after T+2 orders have executed, so the first new sizing decision that can
    consume the settlement is T+3 open.  Iteration preserves the historical
    four-output ABI used by older internal callers.
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


def compiled_block_rows() -> int:
    """Return the process-local fixed block size used by the daily ledger.

    Zero deliberately disables the compiled block runner.  Positive values are
    executor-only: they alter graph granularity, never the recurrence or its
    gradient horizon.
    """

    raw = os.environ.get(
        "STOCKAGENT_TW_CONTINUOUS_COMPILE_CHUNK_ROWS",
        str(COMPILED_BLOCK_ROWS),
    )
    try:
        return max(0, int(raw))
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "STOCKAGENT_TW_CONTINUOUS_COMPILE_CHUNK_ROWS must be an integer"
        ) from exc


def _assert_tensor_condition(condition: torch.Tensor, message: str) -> None:
    """Validate CUDA values without forcing a host synchronization.

    The public CPU behavior remains an immediate Python exception.  CUDA can
    enqueue a device-side assertion and surface the same failure at the next
    natural synchronization instead of stalling every optimizer batch on
    ``Tensor.item()``.
    """

    predicate = condition.reshape(())
    if predicate.device.type == "cuda" and hasattr(torch, "_assert_async"):
        torch._assert_async(predicate, message)
        return
    if not bool(predicate.item()):
        raise ValueError(message)


def get_tw_day_trade_daily_compile_stats(
    *, reset: bool = False
) -> dict[str, int]:
    """Return process-local compile audit counters for the daily executor."""

    snapshot = dict(_COMPILE_STATS)
    if reset:
        for name in _COMPILE_STATS:
            _COMPILE_STATS[name] = 0
    return snapshot


def _cap_from_reference_notional(
    requested: torch.Tensor,
    reference_cap: torch.Tensor,
    equity_scale: torch.Tensor,
) -> torch.Tensor:
    valid = torch.isfinite(reference_cap) & (reference_cap >= 0.0)
    binds = valid & (reference_cap < requested * equity_scale)
    safe_scale = torch.where(binds, equity_scale, torch.ones_like(equity_scale))
    capped = torch.where(binds, reference_cap / safe_scale, requested)
    return torch.where(valid, capped, torch.zeros_like(capped))


def _preserve_requested_side_mix(
    requested: torch.Tensor,
    executable: torch.Tensor,
) -> torch.Tensor:
    """Reduce only the better-filled side of a two-sided request."""

    eps = requested.new_tensor(1.0e-12)
    requested_long = requested.clamp_min(0.0).sum()
    requested_short = (-requested.clamp_max(0.0)).sum()
    executable_long = executable.clamp_min(0.0).sum()
    executable_short = (-executable.clamp_max(0.0)).sum()
    both_requested = (requested_long > eps) & (requested_short > eps)
    either_missing = (executable_long <= eps) | (executable_short <= eps)
    long_fill = executable_long / requested_long.clamp_min(eps)
    short_fill = executable_short / requested_short.clamp_min(eps)
    common_fill = torch.minimum(long_fill, short_fill)
    long_scale = torch.minimum(
        torch.ones_like(common_fill), common_fill / long_fill.clamp_min(eps)
    )
    short_scale = torch.minimum(
        torch.ones_like(common_fill), common_fill / short_fill.clamp_min(eps)
    )
    scaled = torch.where(
        executable > 0.0,
        executable * long_scale,
        executable * short_scale,
    )
    fail_closed = executable + (torch.zeros_like(executable) - executable).detach()
    scaled = torch.where(both_requested & either_missing, fail_closed, scaled)
    return torch.where(both_requested, scaled, executable)


def _run_day(
    target_weights: torch.Tensor,
    intraday_log_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_close_mask: torch.Tensor,
    can_sell_close_mask: torch.Tensor,
    can_short_open_mask: torch.Tensor,
    day_trade_eligible_mask: torch.Tensor,
    can_buy_open_mask: torch.Tensor,
    can_sell_open_mask: torch.Tensor,
    buy_fee_rates: torch.Tensor,
    day_trade_sell_fee_rates: torch.Tensor,
    normal_sell_fee_rates: torch.Tensor,
    commission_rebate_rates: torch.Tensor,
    volume_limit_weights: torch.Tensor,
    cash: torch.Tensor,
    payables: torch.Tensor,
    receivables: torch.Tensor,
    equity_scale: torch.Tensor,
    max_turnover_ratio: torch.Tensor,
    margin_financing_ratio: torch.Tensor,
    margin_financing_annual_rate: torch.Tensor,
    margin_short_handling_fee_rate: torch.Tensor,
    margin_short_annual_borrow_rate: torch.Tensor,
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
    raw_log_return = intraday_log_returns
    finite_log = torch.isfinite(raw_log_return)
    simple_unchecked = torch.expm1(
        torch.where(finite_log, raw_log_return, torch.zeros_like(raw_log_return))
    )
    valid_return = finite_log & torch.isfinite(simple_unchecked)
    simple_asset_return = torch.where(
        valid_return, simple_unchecked, torch.zeros_like(simple_unchecked)
    )

    raw_request = torch.where(
        state_advance, target_weights, torch.zeros_like(target_weights)
    )
    # The model target is a fraction of deployable cash.  Pending receivables
    # are already economic NAV but cannot fund orders; pending payables have
    # not left cash yet.  Convert the request to an economic-NAV weight before
    # applying the fixed-reference notional capacity below.
    deployable_scale = cash.clamp_min(0.0) / equity_scale.clamp_min(1.0e-12)
    request = raw_request * deployable_scale
    entry_permission = (
        tradable_mask.bool()
        & day_trade_eligible_mask.bool()
        & valid_return
        & torch.where(
            raw_request < 0.0,
            can_short_open_mask.bool() & can_sell_open_mask.bool(),
            can_buy_open_mask.bool(),
        )
    )
    executed = torch.where(entry_permission, request, torch.zeros_like(request))

    # The causal daily volume input is a fixed-reference notional budget.  A
    # round trip consumes the same shares twice, so at most half can be opened.
    cap = volume_limit_weights * 0.5
    magnitude = _cap_from_reference_notional(
        executed.abs(), cap, equity_scale.clamp_min(1.0e-12)
    )
    executed = torch.sign(executed) * magnitude
    raw_turnover = executed.abs().sum() * 2.0
    turnover_scale = torch.where(
        max_turnover_ratio > 0.0,
        torch.minimum(
            torch.ones_like(raw_turnover),
            max_turnover_ratio / raw_turnover.clamp_min(1.0e-12),
        ),
        torch.ones_like(raw_turnover),
    )
    executed = executed * turnover_scale
    executed = _preserve_requested_side_mix(request, executed)

    close_available = torch.where(
        executed < 0.0,
        can_buy_close_mask.bool(),
        can_sell_close_mask.bool(),
    )
    residual = (executed != 0.0) & ~close_available
    long_notional = executed.clamp_min(0.0)
    short_notional = (-executed.clamp_max(0.0))
    close_factor = (1.0 + simple_asset_return).clamp_min(0.0)
    buy_notional = long_notional + short_notional * close_factor
    sell_notional = long_notional * close_factor + short_notional
    effective_sell_fee = torch.where(
        residual, normal_sell_fee_rates, day_trade_sell_fee_rates
    )
    fees = (
        buy_notional * buy_fee_rates
        + sell_notional * effective_sell_fee
    )
    rebates = (buy_notional + sell_notional) * commission_rebate_rates
    one_day_financing_rate = (
        margin_financing_ratio * margin_financing_annual_rate / 365.0
    )
    one_day_short_borrow_rate = margin_short_annual_borrow_rate / 365.0
    margin_cost = torch.where(
        residual & (executed > 0.0),
        long_notional * close_factor * one_day_financing_rate,
        torch.where(
            residual & (executed < 0.0),
            short_notional * margin_short_handling_fee_rate
            + short_notional * close_factor * one_day_short_borrow_rate,
            torch.zeros_like(executed),
        ),
    )
    pnl_ratio = (
        executed * simple_asset_return - fees + rebates - margin_cost
    ).sum()
    pnl_ratio = torch.where(
        state_advance,
        pnl_ratio.clamp_min(-1.0 + 1.0e-6),
        torch.zeros_like(pnl_ratio),
    )
    # Same-quantity securities legs offset on T.  Only the fee-adjusted net
    # cash difference survives as a normalized claim; gross buy and sell
    # consideration must never be entered in the settlement queue.
    net_claim = equity_scale * pnl_ratio

    # Settle the queue front only after today's orders.  Therefore a claim
    # created on T posts to cash after T+2 execution and first affects sizing
    # at T+3 open.
    due_payable = payables[0]
    due_receivable = receivables[0]
    settled_cash = cash + due_receivable - due_payable
    queue_zero = torch.zeros_like(due_payable).unsqueeze(0)
    shifted_payables = torch.cat((payables[1:], queue_zero), dim=0)
    shifted_receivables = torch.cat((receivables[1:], queue_zero), dim=0)
    cash = torch.where(state_advance, settled_cash, cash)
    payables = torch.where(state_advance, shifted_payables, payables)
    receivables = torch.where(state_advance, shifted_receivables, receivables)
    insertion = torch.nn.functional.one_hot(
        torch.tensor(
            int(payables.numel()) - 1,
            device=payables.device,
        ),
        num_classes=int(payables.numel()),
    ).to(dtype=payables.dtype)
    payables = payables + insertion * (-net_claim).clamp_min(0.0)
    receivables = receivables + insertion * net_claim.clamp_min(0.0)
    next_equity_scale = cash + receivables.sum() - payables.sum()
    turnover = torch.where(
        state_advance,
        (buy_notional + sell_notional).sum(),
        torch.zeros_like(pnl_ratio),
    )
    return (
        torch.log1p(pnl_ratio),
        turnover,
        executed,
        next_equity_scale,
        net_claim,
        cash,
        payables,
        receivables,
    )


def _run_block(
    target_weights: torch.Tensor,
    intraday_log_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_close_mask: torch.Tensor,
    can_sell_close_mask: torch.Tensor,
    can_short_open_mask: torch.Tensor,
    day_trade_eligible_mask: torch.Tensor,
    can_buy_open_mask: torch.Tensor,
    can_sell_open_mask: torch.Tensor,
    buy_fee_rates: torch.Tensor,
    day_trade_sell_fee_rates: torch.Tensor,
    normal_sell_fee_rates: torch.Tensor,
    commission_rebate_rates: torch.Tensor,
    volume_limit_weights: torch.Tensor,
    cash: torch.Tensor,
    payables: torch.Tensor,
    receivables: torch.Tensor,
    equity_scale: torch.Tensor,
    max_turnover_ratio: torch.Tensor,
    margin_financing_ratio: torch.Tensor,
    margin_financing_annual_rate: torch.Tensor,
    margin_short_handling_fee_rate: torch.Tensor,
    margin_short_annual_borrow_rate: torch.Tensor,
    state_advance: torch.Tensor,
    block_rows: int,
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
    log_rows: list[torch.Tensor] = []
    turnover_rows: list[torch.Tensor] = []
    weight_rows: list[torch.Tensor] = []
    equity_rows: list[torch.Tensor] = []
    claim_rows: list[torch.Tensor] = []
    cash_rows: list[torch.Tensor] = []
    payable_rows: list[torch.Tensor] = []
    receivable_rows: list[torch.Tensor] = []
    for idx in range(int(block_rows)):
        (
            log_return,
            turnover,
            executed,
            equity_scale,
            net_claim,
            cash,
            payables,
            receivables,
        ) = _run_day(
            target_weights[idx],
            intraday_log_returns[idx],
            tradable_mask[idx],
            can_buy_close_mask[idx],
            can_sell_close_mask[idx],
            can_short_open_mask[idx],
            day_trade_eligible_mask[idx],
            can_buy_open_mask[idx],
            can_sell_open_mask[idx],
            buy_fee_rates,
            day_trade_sell_fee_rates,
            normal_sell_fee_rates,
            commission_rebate_rates,
            volume_limit_weights[idx],
            cash,
            payables,
            receivables,
            equity_scale,
            max_turnover_ratio,
            margin_financing_ratio,
            margin_financing_annual_rate,
            margin_short_handling_fee_rate,
            margin_short_annual_borrow_rate,
            state_advance[idx],
        )
        log_rows.append(log_return)
        turnover_rows.append(turnover)
        weight_rows.append(executed)
        equity_rows.append(equity_scale)
        claim_rows.append(net_claim)
        cash_rows.append(cash)
        payable_rows.append(payables)
        receivable_rows.append(receivables)
    return (
        torch.stack(log_rows),
        torch.stack(turnover_rows),
        torch.stack(weight_rows),
        torch.stack(equity_rows),
        torch.stack(claim_rows),
        torch.stack(cash_rows),
        torch.stack(payable_rows),
        torch.stack(receivable_rows),
    )


def _block_runner_for(
    weights: torch.Tensor,
    settlement_lag_sessions: int,
    block_rows: int,
):
    if (
        int(block_rows) <= 0
        or weights.device.type != "cuda"
        or not hasattr(torch, "compile")
        or not _env_truthy("STOCKAGENT_BACKTEST_COMPILE", "1")
        or torch.compiler.is_compiling()
    ):
        return _run_block, None
    key = (
        weights.device.type,
        weights.device.index,
        weights.dtype,
        int(weights.size(1)),
        int(settlement_lag_sessions),
        int(block_rows),
    )
    if key in _COMPILED_BLOCK_FAILED:
        if _env_truthy("STOCKAGENT_STRICT_NO_FALLBACK", "0"):
            raise RuntimeError("compiled daily T+2-close kernel previously failed")
        return _run_block, None
    compiled = _COMPILED_BLOCK_CACHE.get(key)
    if compiled is None:
        compiled = torch.compile(
            _run_block,
            fullgraph=True,
            dynamic=False,
            options={"triton.cudagraphs": False},
        )
        _COMPILED_BLOCK_CACHE[key] = compiled
        _COMPILE_STATS["compile_constructors"] += 1
    return compiled, key


def run_tw_day_trade_daily_execution(
    target_weights: torch.Tensor,
    intraday_log_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_close_mask: torch.Tensor,
    can_sell_close_mask: torch.Tensor,
    can_short_open_mask: torch.Tensor,
    day_trade_eligible_mask: torch.Tensor,
    can_buy_open_mask: torch.Tensor,
    can_sell_open_mask: torch.Tensor,
    buy_fee_rates: torch.Tensor,
    day_trade_sell_fee_rates: torch.Tensor,
    normal_sell_fee_rates: torch.Tensor,
    commission_rebate_rates: torch.Tensor,
    volume_limit_weights: torch.Tensor | None,
    *,
    max_turnover_ratio: float,
    margin_financing_ratio: float,
    margin_financing_annual_rate: float,
    margin_short_handling_fee_rate: float,
    margin_short_annual_borrow_rate: float,
    settlement_lag_sessions: int = 2,
    state_advance_mask: torch.Tensor | None = None,
    initial_cash: torch.Tensor | None = None,
    initial_payables: torch.Tensor | None = None,
    initial_receivables: torch.Tensor | None = None,
    initial_equity_scale: torch.Tensor | None = None,
) -> TaiwanDayTradeDailyResult:
    """Execute daily open-to-close labels with a net T+2-close ledger."""

    if target_weights.ndim != 2:
        raise ValueError("target_weights must have shape [T,S]")
    if (
        isinstance(settlement_lag_sessions, bool)
        or int(settlement_lag_sessions) != settlement_lag_sessions
        or int(settlement_lag_sessions) <= 0
    ):
        raise ValueError("settlement_lag_sessions must be a positive integer")
    lag = int(settlement_lag_sessions)
    shape = tuple(target_weights.shape)
    for name, value in (
        ("intraday_log_returns", intraday_log_returns),
        ("tradable_mask", tradable_mask),
        ("can_buy_close_mask", can_buy_close_mask),
        ("can_sell_close_mask", can_sell_close_mask),
        ("can_short_open_mask", can_short_open_mask),
        ("day_trade_eligible_mask", day_trade_eligible_mask),
        ("can_buy_open_mask", can_buy_open_mask),
        ("can_sell_open_mask", can_sell_open_mask),
    ):
        if tuple(value.shape) != shape:
            raise ValueError(f"{name} must match target_weights")
    weights = target_weights.float()
    device = weights.device
    fee_vectors = [
        value.to(device=device, dtype=torch.float32).reshape(-1)
        for value in (
            buy_fee_rates,
            day_trade_sell_fee_rates,
            normal_sell_fee_rates,
            commission_rebate_rates,
        )
    ]
    if any(int(value.numel()) != int(weights.size(1)) for value in fee_vectors):
        raise ValueError("fee vectors must have shape [S]")
    _assert_tensor_condition(
        ~(fee_vectors[2] < fee_vectors[1]).any(),
        "normal sell fees must not be below day-trade sell fees",
    )
    volume = (
        torch.full_like(weights, torch.finfo(torch.float32).max)
        if volume_limit_weights is None
        else volume_limit_weights.to(device=device, dtype=torch.float32)
    )
    if tuple(volume.shape) != shape:
        raise ValueError("volume_limit_weights must match target_weights")
    advance = (
        torch.ones(weights.size(0), device=device, dtype=torch.bool)
        if state_advance_mask is None
        else state_advance_mask.to(device=device, dtype=torch.bool)
    )
    equity = (
        weights.new_tensor(1.0)
        if initial_equity_scale is None
        else initial_equity_scale.to(device=device, dtype=torch.float32)
    )

    def initial_queue(value: torch.Tensor | None, name: str) -> torch.Tensor:
        queue = (
            torch.zeros(lag, device=device, dtype=torch.float32)
            if value is None
            else value.to(device=device, dtype=torch.float32).reshape(-1)
        )
        if int(queue.numel()) != lag:
            raise ValueError(f"{name} must have shape [{lag}]")
        _assert_tensor_condition(
            (torch.isfinite(queue) & (queue >= 0.0)).all(),
            f"{name} must be finite and non-negative",
        )
        return queue

    payables = initial_queue(initial_payables, "initial_payables")
    receivables = initial_queue(initial_receivables, "initial_receivables")
    cash = (
        equity - receivables.sum() + payables.sum()
        if initial_cash is None
        else initial_cash.to(device=device, dtype=torch.float32).reshape(())
    )
    _assert_tensor_condition(torch.isfinite(cash), "initial_cash must be finite")
    accounting_equity = cash + receivables.sum() - payables.sum()
    tolerance = equity.abs().clamp_min(1.0) * 2.0e-5
    _assert_tensor_condition(
        torch.isfinite(equity)
        & torch.isfinite(accounting_equity)
        & ((accounting_equity - equity).abs() <= tolerance),
        "initial daily T+2 cash/claim state does not reconcile to "
        "initial_equity_scale",
    )
    scalar_args = [
        weights.new_tensor(float(value))
        for value in (
            max_turnover_ratio,
            margin_financing_ratio,
            margin_financing_annual_rate,
            margin_short_handling_fee_rate,
            margin_short_annual_borrow_rate,
        )
    ]
    configured_block_rows = compiled_block_rows()
    block_rows = max(1, int(configured_block_rows))
    block_runner, compiled_key = _block_runner_for(
        weights,
        lag,
        configured_block_rows,
    )
    outputs: list[list[torch.Tensor]] = [[], [], [], [], [], [], [], []]
    total_rows = int(weights.size(0))
    for start in range(0, total_rows, block_rows):
        valid_rows = min(block_rows, total_rows - start)
        if valid_rows == block_rows:
            row_slice = slice(start, start + block_rows)
            block_tensors = (
                weights[row_slice],
                intraday_log_returns[row_slice],
                tradable_mask[row_slice],
                can_buy_close_mask[row_slice],
                can_sell_close_mask[row_slice],
                can_short_open_mask[row_slice],
                day_trade_eligible_mask[row_slice],
                can_buy_open_mask[row_slice],
                can_sell_open_mask[row_slice],
                volume[row_slice],
            )
            block_advance = advance[row_slice]
        else:
            # Only the final partial block needs gather-based duplicate
            # padding.  Full blocks use zero-copy views and avoid eleven GPU
            # index-select kernels per recurrent launch.
            selector = torch.arange(
                start,
                start + block_rows,
                device=device,
                dtype=torch.long,
            ).clamp_max(total_rows - 1)
            block_tensors = (
                weights.index_select(0, selector),
                intraday_log_returns.index_select(0, selector),
                tradable_mask.index_select(0, selector),
                can_buy_close_mask.index_select(0, selector),
                can_sell_close_mask.index_select(0, selector),
                can_short_open_mask.index_select(0, selector),
                day_trade_eligible_mask.index_select(0, selector),
                can_buy_open_mask.index_select(0, selector),
                can_sell_open_mask.index_select(0, selector),
                volume.index_select(0, selector),
            )
            block_advance = advance.index_select(0, selector) & (
                torch.arange(block_rows, device=device) < valid_rows
            )
        args = (
            *block_tensors[:9],
            *fee_vectors,
            block_tensors[9],
            cash,
            payables,
            receivables,
            equity,
            *scalar_args,
            block_advance,
            block_rows,
        )
        try:
            block = block_runner(*args)
        except Exception as exc:
            if compiled_key is None or _env_truthy(
                "STOCKAGENT_STRICT_NO_FALLBACK", "0"
            ):
                raise
            _COMPILED_BLOCK_CACHE.pop(compiled_key, None)
            _COMPILED_BLOCK_FAILED.add(compiled_key)
            _COMPILE_STATS["eager_fallback_calls"] += 1
            compiled_key = None
            block_runner = _run_block
            print(
                "[TW daily T+2-close compile] falling back to eager: "
                f"{type(exc).__name__}: {exc}"
            )
            block = block_runner(*args)
        else:
            if compiled_key is not None:
                _COMPILE_STATS["compiled_block_calls"] += 1
                _COMPILE_STATS["compiled_day_calls"] += valid_rows
        for bucket, value in zip(outputs, block, strict=True):
            bucket.append(value[:valid_rows])
        equity = block[3][valid_rows - 1]
        cash = block[5][valid_rows - 1]
        payables = block[6][valid_rows - 1]
        receivables = block[7][valid_rows - 1]
    (
        strategy_returns,
        turnovers,
        weights_history,
        equity_scale_history,
        net_claims,
        cash_history,
        payables_history,
        receivables_history,
    ) = (torch.cat(bucket) for bucket in outputs)
    return TaiwanDayTradeDailyResult(
        strategy_returns=strategy_returns,
        turnovers=turnovers,
        weights_history=weights_history,
        equity_scale_history=equity_scale_history,
        net_claims=net_claims,
        cash_history=cash_history,
        payables_history=payables_history,
        receivables_history=receivables_history,
        final_cash=cash,
        final_payables=payables,
        final_receivables=receivables,
        final_equity_scale=equity_scale_history[-1],
    )


__all__ = [
    "COMPILED_BLOCK_ROWS",
    "TaiwanDayTradeDailyResult",
    "compiled_block_rows",
    "get_tw_day_trade_daily_compile_stats",
    "run_tw_day_trade_daily_execution",
]

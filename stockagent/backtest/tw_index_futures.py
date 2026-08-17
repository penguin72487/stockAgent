"""Canonical day-session execution for TX/MTX/TMF across E1..E6.

The current contract uses 18 direct signed capital fractions and converts each
one to whole contracts without adding unrequested lots.  Exact per-contract
fees, tax, slippage, and the recurrent equity path are shared by the loss
forward and integer audit.  A scalar front-month basket path remains only for
the previous caller ABI and regression comparisons.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from numbers import Real
import os
from typing import Callable, Final
import warnings

import numpy as np
import torch

from stockagent.data.tw_index_futures import (
    TAIFEX_INDEX_FUTURES_ACTION_COUNT,
    TAIFEX_INDEX_FUTURES_MULTIPLIERS,
    TAIFEX_INDEX_FUTURES_PRODUCTS,
    TAIFEX_INDEX_FUTURES_TENOR_SLOTS,
    TaiwanIndexFuturesDaySession,
)


# v7 is the direct 18-action, exact whole-contract recurrent-equity contract.
# It applies the statutory rate to both transactions of every daily-flat round
# trip and keeps the same forward ledger in eager, fixed-block compiled loss,
# chunked evaluation, and the final integer artifact.
TW_INDEX_FUTURES_DAY_BACKTEST_CONTRACT_VERSION: Final[int] = 7
# v2 preserves directional PnL gradients at exactly zero requested exposure.
# Forward whole-contract counts, costs, returns, and equity are unchanged, but
# optimizer trajectories from v1 must not resume silently under this gradient.
TW_INDEX_FUTURES_TRAINING_GRADIENT_CONTRACT_VERSION: Final[int] = 2
TW_INDEX_FUTURES_TRANSACTION_TAX_RATE: Final[float] = 0.00002
# Compatibility exports for downstream imports.  Both names now denote the
# per-transaction rate, not a one-sided round-trip rate.
TW_INDEX_FUTURES_SELL_TAX_RATE: Final[float] = (
    TW_INDEX_FUTURES_TRANSACTION_TAX_RATE
)
TW_INDEX_FUTURES_TAX_RATE: Final[float] = TW_INDEX_FUTURES_TRANSACTION_TAX_RATE
# Current TAIFEX exchange + clearing charges per contract per side.  A broker's
# negotiated commission is separate and defaults to zero below.
TAIFEX_FIXED_FEES_PER_SIDE_TWD: Final[dict[str, float]] = {
    "TX": 20.0,
    "MTX": 12.5,
    "TMF": 8.0,
}
TW_INDEX_FUTURES_COMPILED_BLOCK_ROWS: Final[int] = 32
_COMPILED_ALL_TENOR_BLOCKS: dict[
    tuple[object, ...], Callable[..., tuple[torch.Tensor, ...]]
] = {}
_FAILED_ALL_TENOR_BLOCKS: set[tuple[object, ...]] = set()
_ALL_TENOR_COMPILE_STATS: dict[str, int] = {
    "compiled_block_calls": 0,
    "compiled_day_calls": 0,
    "eager_fallback_calls": 0,
}


def get_tw_index_futures_compile_stats() -> dict[str, int]:
    """Return process-local fixed-block usage counters."""

    return dict(_ALL_TENOR_COMPILE_STATS)


@dataclass(frozen=True, slots=True)
class FuturesCostSchedule:
    # Tax, fixed fees, and slippage apply to both transactions in a daily-flat
    # round trip. ``tax_rate`` is the statutory per-transaction rate.
    tax_rate: float = TW_INDEX_FUTURES_TRANSACTION_TAX_RATE
    exchange_and_clearing_fee_per_side_twd: tuple[float, ...] = (20.0, 12.5, 8.0)
    broker_fee_per_side_twd: tuple[float, ...] = (0.0, 0.0, 0.0)
    slippage_points_per_side: tuple[float, ...] = (0.0, 0.0, 0.0)
    basket_fee_penalty: float = 1.0

    def __post_init__(self) -> None:
        if (
            not isinstance(self.tax_rate, Real)
            or isinstance(self.tax_rate, bool)
            or not math.isfinite(float(self.tax_rate))
            or float(self.tax_rate) < 0.0
        ):
            raise ValueError("tax_rate must be a finite non-negative real")
        for name in (
            "exchange_and_clearing_fee_per_side_twd",
            "broker_fee_per_side_twd",
            "slippage_points_per_side",
        ):
            values = tuple(getattr(self, name))
            if len(values) != len(TAIFEX_INDEX_FUTURES_PRODUCTS):
                raise ValueError(f"{name} must contain TX, MTX, and TMF values")
            if any(
                isinstance(value, bool)
                or not isinstance(value, Real)
                or not math.isfinite(float(value))
                or float(value) < 0.0
                for value in values
            ):
                raise ValueError(f"{name} must contain finite non-negative values")
        if (
            not isinstance(self.basket_fee_penalty, Real)
            or isinstance(self.basket_fee_penalty, bool)
            or not math.isfinite(float(self.basket_fee_penalty))
            or float(self.basket_fee_penalty) < 0.0
        ):
            raise ValueError(
                "basket_fee_penalty must be a finite non-negative real"
            )

    @property
    def fixed_fee_per_side_twd(self) -> np.ndarray:
        return np.asarray(
            self.exchange_and_clearing_fee_per_side_twd,
            dtype=np.float64,
        ) + np.asarray(self.broker_fee_per_side_twd, dtype=np.float64)


@dataclass(frozen=True, slots=True)
class FuturesContinuousBacktest:
    strategy_returns: torch.Tensor
    gross_returns: torch.Tensor
    cost_returns: torch.Tensor
    requested_exposure: torch.Tensor
    executed_exposure: torch.Tensor
    turnovers: torch.Tensor
    tradable_mask: torch.Tensor
    executed_actions: torch.Tensor | None = None
    equity_scale_history: torch.Tensor | None = None
    final_equity_scale: torch.Tensor | None = None
    final_alive: torch.Tensor | None = None


@dataclass(frozen=True, slots=True)
class FuturesContractBasket:
    quantities: np.ndarray
    target_notional: float
    actual_notional: float
    tracking_error: float
    estimated_round_trip_cost: float


@dataclass(frozen=True, slots=True)
class FuturesIntegerBacktest:
    dates: np.ndarray
    products: tuple[str, ...]
    requested_exposure: np.ndarray
    executed_exposure: np.ndarray
    contract_quantities: np.ndarray
    gross_pnl_twd: np.ndarray
    fees_twd: np.ndarray
    tax_twd: np.ndarray
    slippage_twd: np.ndarray
    net_pnl_twd: np.ndarray
    strategy_returns: np.ndarray
    turnovers: np.ndarray
    equity: np.ndarray
    alive: np.ndarray
    contract_months: np.ndarray


def build_tw_index_futures_day_execution_tensor(
    market: TaiwanIndexFuturesDaySession,
    *,
    cost_schedule: FuturesCostSchedule | None = None,
) -> np.ndarray:
    """Return exact per-opening-notional long/short outcomes and notionals.

    Channels are ``[long_net_simple_return, short_net_simple_return,
    one_contract_open_notional_twd]`` in the same stable product-major 18-slot
    order as the model.  Fees, tax, and slippage are charged on both legs.
    Invalid executor rows remain NaN and are never interpreted as zero-return
    trades.
    """

    schedule = FuturesCostSchedule() if cost_schedule is None else cost_schedule
    (
        _months,
        opens,
        _highs,
        _lows,
        closes,
        _volumes,
        _log_returns,
        tradable,
    ) = market.flattened_tenor_panel()
    multipliers = np.repeat(
        np.asarray(market.multipliers, dtype=np.float64),
        TAIFEX_INDEX_FUTURES_TENOR_SLOTS,
    )
    product_index = np.repeat(
        np.arange(len(market.products), dtype=np.int64),
        TAIFEX_INDEX_FUTURES_TENOR_SLOTS,
    )
    fixed = schedule.fixed_fee_per_side_twd[product_index]
    slippage = np.asarray(
        schedule.slippage_points_per_side, dtype=np.float64
    )[product_index]
    valid = (
        np.asarray(tradable, dtype=bool)
        & np.isfinite(opens)
        & (opens > 0.0)
        & np.isfinite(closes)
        & (closes > 0.0)
    )
    contract_notional = opens * multipliers[None, :]
    round_trip_cost = (
        2.0 * fixed[None, :]
        + (opens + closes)
        * multipliers[None, :]
        * float(schedule.tax_rate)
        + 2.0 * slippage[None, :] * multipliers[None, :]
    )
    gross_long = (closes - opens) * multipliers[None, :]
    gross_short = -gross_long
    long_return = (gross_long - round_trip_cost) / contract_notional
    short_return = (gross_short - round_trip_cost) / contract_notional
    output = np.stack((long_return, short_return, contract_notional), axis=-1)
    output[~valid] = np.nan
    return output.astype(np.float32, copy=False)


def _run_tw_index_futures_all_tenors_day_torch_impl(
    actions: torch.Tensor,
    execution_tensor: torch.Tensor,
    *,
    initial_capital: float,
    max_abs_exposure: float = 1.0,
    initial_equity_scale: torch.Tensor | None = None,
    initial_alive: torch.Tensor | None = None,
) -> FuturesContinuousBacktest:
    """Execute direct 18-slot actions with exact-forward whole contracts.

    Contract counts are floored independently, so the executor never adds
    risk the model did not request and never forces a minimum one-contract
    position.  A straight-through executed fraction preserves useful action
    gradients while the forward values, costs, PnL, and equity path are the
    exact integer ledger represented by ``execution_tensor``.
    """

    capital = _finite_nonnegative("initial_capital", initial_capital)
    limit = _finite_nonnegative("max_abs_exposure", max_abs_exposure)
    if capital <= 0.0 or limit <= 0.0:
        raise ValueError("initial_capital and max_abs_exposure must be positive")
    clean = torch.nan_to_num(actions.float(), nan=0.0, posinf=0.0, neginf=0.0)
    gross_requested = clean.abs().sum(dim=-1, keepdim=True)
    exact_scale = torch.clamp(
        clean.new_tensor(float(limit))
        / gross_requested.clamp_min(torch.finfo(clean.dtype).eps),
        max=1.0,
    )
    # The model normally emits gross exactly at the limit. Different reduction
    # trees may place that value on opposite sides of clamp's derivative kink.
    # Keep the exact capped forward value while using the identity derivative
    # inside a one-ppm boundary band; outside the band retain the true
    # normalization derivative.
    stable_scale = torch.where(
        gross_requested
        <= clean.new_tensor(float(limit) * (1.0 + 1e-6)),
        torch.ones_like(exact_scale),
        exact_scale,
    )
    scale = stable_scale + (exact_scale - stable_scale).detach()
    clean = clean * scale
    valid = torch.isfinite(execution_tensor).all(dim=-1)
    long_returns = torch.nan_to_num(execution_tensor[..., 0].float(), nan=0.0)
    short_returns = torch.nan_to_num(execution_tensor[..., 1].float(), nan=0.0)
    notionals = torch.nan_to_num(
        execution_tensor[..., 2].float(), nan=0.0, posinf=0.0, neginf=0.0
    )
    requested_history: list[torch.Tensor] = []
    executed_history: list[torch.Tensor] = []
    return_history: list[torch.Tensor] = []
    turnover_history: list[torch.Tensor] = []
    starting_scale = (
        clean.new_ones(())
        if initial_equity_scale is None
        else initial_equity_scale.to(device=clean.device, dtype=clean.dtype)
    )
    equity = clean.new_tensor(float(capital)) * starting_scale
    alive = (
        torch.ones((), dtype=torch.bool, device=clean.device)
        if initial_alive is None
        else initial_alive.to(device=clean.device, dtype=torch.bool)
    )
    equity_scale_history: list[torch.Tensor] = []
    for row in range(int(clean.size(0))):
        requested = torch.where(valid[row] & alive, clean[row], torch.zeros_like(clean[row]))
        target = requested.abs() * equity.detach()
        contract_ratio = (
            target
            / notionals[row].clamp_min(torch.finfo(notionals.dtype).tiny)
        )
        # Model actions may arrive through BF16/FP32.  Treat ratios within one
        # part per million of an integer as that exact integer so representable
        # 1% requests do not spuriously lose a whole contract.
        counts = torch.floor(contract_ratio + 1e-6)
        counts = torch.where(valid[row] & alive, counts, torch.zeros_like(counts))
        actual_abs = counts * notionals[row] / equity.detach().clamp_min(1e-12)
        # Forward equals the integer fraction.  The signed straight-through
        # value retains the directional PnL derivative even at requested=0;
        # using only abs(requested) plus a long/short branch would make cash an
        # artificial absorbing point because d(abs(x))/dx is zero at x=0.
        executed_abs = requested.abs() + (actual_abs - requested.abs()).detach()
        actual_signed = torch.copysign(actual_abs, requested)
        signed_executed = requested + (actual_signed - requested).detach()
        directional_returns = 0.5 * (long_returns[row] - short_returns[row])
        round_trip_cost_returns = -0.5 * (
            long_returns[row] + short_returns[row]
        )
        strategy_return = torch.sum(
            signed_executed * directional_returns
            - executed_abs * round_trip_cost_returns
        )
        next_equity = equity * (1.0 + strategy_return)
        row_alive = torch.isfinite(next_equity) & (next_equity > 0.0)
        equity = torch.where(row_alive, next_equity, torch.zeros_like(next_equity))
        alive = alive & row_alive
        equity_scale_history.append(equity / float(capital))
        requested_history.append(requested)
        executed_history.append(signed_executed)
        return_history.append(strategy_return)
        turnover_history.append(2.0 * actual_abs.sum())
    if return_history:
        strategy_returns = torch.stack(return_history)
        executed_actions = torch.stack(executed_history)
        turnovers = torch.stack(turnover_history)
        requested_actions = torch.stack(requested_history)
        equity_scales = torch.stack(equity_scale_history)
    else:
        strategy_returns = clean.new_empty((0,))
        executed_actions = clean.new_empty((0, TAIFEX_INDEX_FUTURES_ACTION_COUNT))
        turnovers = clean.new_empty((0,))
        requested_actions = executed_actions
        equity_scales = clean.new_empty((0,))
    return FuturesContinuousBacktest(
        strategy_returns=strategy_returns,
        gross_returns=strategy_returns,
        cost_returns=torch.zeros_like(strategy_returns),
        requested_exposure=requested_actions.abs().sum(dim=-1),
        executed_exposure=executed_actions.abs().sum(dim=-1),
        turnovers=turnovers,
        tradable_mask=valid.any(dim=-1),
        executed_actions=executed_actions,
        equity_scale_history=equity_scales,
        final_equity_scale=(
            equity_scales[-1] if equity_scales.numel() else starting_scale
        ),
        final_alive=alive,
    )


def _futures_result_tuple(
    result: FuturesContinuousBacktest,
) -> tuple[torch.Tensor, ...]:
    if (
        result.executed_actions is None
        or result.equity_scale_history is None
        or result.final_equity_scale is None
        or result.final_alive is None
    ):
        raise RuntimeError("direct futures result is missing recurrent state")
    return (
        result.strategy_returns,
        result.requested_exposure,
        result.executed_exposure,
        result.turnovers,
        result.tradable_mask,
        result.executed_actions,
        result.equity_scale_history,
        result.final_equity_scale,
        result.final_alive,
    )


def _compiled_all_tenor_block(
    actions: torch.Tensor,
    *,
    block_rows: int,
    initial_capital: float,
    max_abs_exposure: float,
) -> tuple[tuple[object, ...], Callable[..., tuple[torch.Tensor, ...]]]:
    device_index = (
        actions.device.index
        if actions.device.index is not None
        else torch.cuda.current_device()
    )
    key: tuple[object, ...] = (
        int(device_index),
        str(actions.dtype),
        int(block_rows),
        float(initial_capital),
        float(max_abs_exposure),
    )
    compiled = _COMPILED_ALL_TENOR_BLOCKS.get(key)
    if compiled is not None:
        return key, compiled

    def block(
        block_actions: torch.Tensor,
        block_execution: torch.Tensor,
        equity_scale: torch.Tensor,
        alive: torch.Tensor,
    ) -> tuple[torch.Tensor, ...]:
        return _futures_result_tuple(
            _run_tw_index_futures_all_tenors_day_torch_impl(
                block_actions,
                block_execution,
                initial_capital=initial_capital,
                max_abs_exposure=max_abs_exposure,
                initial_equity_scale=equity_scale,
                initial_alive=alive,
            )
        )

    compiled = torch.compile(
        block,
        fullgraph=True,
        dynamic=False,
        options={"triton.cudagraphs": False},
    )
    _COMPILED_ALL_TENOR_BLOCKS[key] = compiled
    return key, compiled


def _futures_result_from_tuple(
    values: tuple[torch.Tensor, ...],
) -> FuturesContinuousBacktest:
    return FuturesContinuousBacktest(
        strategy_returns=values[0],
        gross_returns=values[0],
        cost_returns=torch.zeros_like(values[0]),
        requested_exposure=values[1],
        executed_exposure=values[2],
        turnovers=values[3],
        tradable_mask=values[4],
        executed_actions=values[5],
        equity_scale_history=values[6],
        final_equity_scale=values[7],
        final_alive=values[8],
    )


def _slice_futures_result_rows(
    result: FuturesContinuousBacktest,
    rows: int,
) -> FuturesContinuousBacktest:
    """Discard inert padding rows while preserving the block terminal state."""

    return FuturesContinuousBacktest(
        strategy_returns=result.strategy_returns[:rows],
        gross_returns=result.gross_returns[:rows],
        cost_returns=result.cost_returns[:rows],
        requested_exposure=result.requested_exposure[:rows],
        executed_exposure=result.executed_exposure[:rows],
        turnovers=result.turnovers[:rows],
        tradable_mask=result.tradable_mask[:rows],
        executed_actions=(
            None
            if result.executed_actions is None
            else result.executed_actions[:rows]
        ),
        equity_scale_history=(
            None
            if result.equity_scale_history is None
            else result.equity_scale_history[:rows]
        ),
        final_equity_scale=result.final_equity_scale,
        final_alive=result.final_alive,
    )


def run_tw_index_futures_all_tenors_day_torch(
    actions: torch.Tensor,
    execution_tensor: torch.Tensor,
    *,
    initial_capital: float,
    max_abs_exposure: float = 1.0,
    initial_equity_scale: torch.Tensor | None = None,
    initial_alive: torch.Tensor | None = None,
) -> FuturesContinuousBacktest:
    """Run the exact ledger in reusable fixed-size CUDA blocks.

    The eager implementation remains the semantic oracle.  Compiling an
    entire training batch unrolls the equity recurrence once per row and makes
    compile latency proportional to batch size.  A fixed block is compiled
    once and reused while its differentiable equity/alive state is chained
    across calls.  A non-aligned tail is padded with inert, non-tradable rows
    and sent through the same fixed graph, then sliced back to the true length.
    This keeps every real row on the compiled exact ledger without introducing
    a second tail formula or a length-specific graph.
    """

    if actions.ndim != 2 or int(actions.size(1)) != TAIFEX_INDEX_FUTURES_ACTION_COUNT:
        raise ValueError("actions must have shape [T,18]")
    if tuple(execution_tensor.shape) != (
        int(actions.size(0)),
        TAIFEX_INDEX_FUTURES_ACTION_COUNT,
        3,
    ):
        raise ValueError("execution_tensor must have shape [T,18,3]")

    try:
        block_rows = int(
            os.environ.get(
                "STOCKAGENT_TW_CONTINUOUS_COMPILE_CHUNK_ROWS",
                str(TW_INDEX_FUTURES_COMPILED_BLOCK_ROWS),
            )
        )
    except ValueError:
        block_rows = 0
    compile_blocks = bool(
        block_rows > 0
        and actions.device.type == "cuda"
        and hasattr(torch, "compile")
        and os.environ.get("STOCKAGENT_BACKTEST_COMPILE", "1")
        .strip()
        .lower()
        in {"1", "true", "on", "yes"}
        and not torch.compiler.is_compiling()
        and int(actions.size(0)) >= block_rows
    )
    if not compile_blocks:
        return _run_tw_index_futures_all_tenors_day_torch_impl(
            actions,
            execution_tensor,
            initial_capital=initial_capital,
            max_abs_exposure=max_abs_exposure,
            initial_equity_scale=initial_equity_scale,
            initial_alive=initial_alive,
        )

    capital = _finite_nonnegative("initial_capital", initial_capital)
    limit = _finite_nonnegative("max_abs_exposure", max_abs_exposure)
    if capital <= 0.0 or limit <= 0.0:
        raise ValueError("initial_capital and max_abs_exposure must be positive")
    if actions.ndim != 2 or int(actions.size(1)) != TAIFEX_INDEX_FUTURES_ACTION_COUNT:
        raise ValueError("actions must have shape [T,18]")
    if tuple(execution_tensor.shape) != (
        int(actions.size(0)),
        TAIFEX_INDEX_FUTURES_ACTION_COUNT,
        3,
    ):
        raise ValueError("execution_tensor must have shape [T,18,3]")

    equity_scale = (
        actions.new_ones((), dtype=torch.float32)
        if initial_equity_scale is None
        else initial_equity_scale.to(device=actions.device, dtype=torch.float32)
    )
    alive = (
        torch.ones((), dtype=torch.bool, device=actions.device)
        if initial_alive is None
        else initial_alive.to(device=actions.device, dtype=torch.bool)
    )
    try:
        key, compiled_block = _compiled_all_tenor_block(
            actions,
            block_rows=block_rows,
            initial_capital=capital,
            max_abs_exposure=limit,
        )
    except Exception:
        if os.environ.get("STOCKAGENT_STRICT_NO_FALLBACK", "0").strip().lower() in {
            "1",
            "true",
            "on",
            "yes",
        }:
            raise
        _ALL_TENOR_COMPILE_STATS["eager_fallback_calls"] += 1
        return _run_tw_index_futures_all_tenors_day_torch_impl(
            actions,
            execution_tensor,
            initial_capital=capital,
            max_abs_exposure=limit,
            initial_equity_scale=equity_scale,
            initial_alive=alive,
        )
    if key in _FAILED_ALL_TENOR_BLOCKS:
        if os.environ.get("STOCKAGENT_STRICT_NO_FALLBACK", "0").strip().lower() in {
            "1",
            "true",
            "on",
            "yes",
        }:
            raise RuntimeError("compiled futures block previously failed")
        _ALL_TENOR_COMPILE_STATS["eager_fallback_calls"] += 1
        return _run_tw_index_futures_all_tenors_day_torch_impl(
            actions,
            execution_tensor,
            initial_capital=capital,
            max_abs_exposure=limit,
            initial_equity_scale=equity_scale,
            initial_alive=alive,
        )

    chunks: list[FuturesContinuousBacktest] = []
    total_rows = int(actions.size(0))
    full_stop = total_rows - total_rows % block_rows

    def call_compiled_block(
        block_actions: torch.Tensor,
        block_execution: torch.Tensor,
        block_equity_scale: torch.Tensor,
        block_alive: torch.Tensor,
    ) -> tuple[torch.Tensor, ...]:
        # PyTorch Dynamo probes ``.grad`` while wrapping non-leaf tensor
        # inputs.  Pytest's repository-wide warnings-as-errors policy can turn
        # that internal, non-semantic probe into an InternalTorchDynamoError.
        # Keep the suppression limited to this exact upstream warning; the
        # compiled graph, gradients, and strict no-fallback behavior remain
        # unchanged.
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message=r"The \.grad attribute of a Tensor that is not a leaf Tensor.*",
                category=UserWarning,
            )
            warnings.filterwarnings(
                "ignore",
                message=r"`torch\.jit\.script_method` is deprecated\..*",
                category=DeprecationWarning,
            )
            return compiled_block(
                block_actions,
                block_execution,
                block_equity_scale,
                block_alive,
            )

    try:
        for start in range(0, full_stop, block_rows):
            stop = start + block_rows
            result = _futures_result_from_tuple(
                call_compiled_block(
                    actions[start:stop],
                    execution_tensor[start:stop],
                    equity_scale,
                    alive,
                )
            )
            chunks.append(result)
            _ALL_TENOR_COMPILE_STATS["compiled_block_calls"] += 1
            _ALL_TENOR_COMPILE_STATS["compiled_day_calls"] += block_rows
            if result.final_equity_scale is None or result.final_alive is None:
                raise RuntimeError("compiled futures block omitted terminal state")
            equity_scale = result.final_equity_scale
            alive = result.final_alive
        if full_stop < total_rows:
            tail_rows = total_rows - full_stop
            pad_rows = block_rows - tail_rows
            padded_actions = torch.cat(
                (
                    actions[full_stop:],
                    actions.new_zeros((pad_rows, TAIFEX_INDEX_FUTURES_ACTION_COUNT)),
                ),
                dim=0,
            )
            padded_execution = torch.cat(
                (
                    execution_tensor[full_stop:],
                    execution_tensor.new_full(
                        (pad_rows, TAIFEX_INDEX_FUTURES_ACTION_COUNT, 3),
                        float("nan"),
                    ),
                ),
                dim=0,
            )
            padded_tail = _futures_result_from_tuple(
                call_compiled_block(
                    padded_actions,
                    padded_execution,
                    equity_scale,
                    alive,
                )
            )
            tail = _slice_futures_result_rows(padded_tail, tail_rows)
            chunks.append(tail)
            _ALL_TENOR_COMPILE_STATS["compiled_block_calls"] += 1
            _ALL_TENOR_COMPILE_STATS["compiled_day_calls"] += tail_rows
    except Exception:
        _FAILED_ALL_TENOR_BLOCKS.add(key)
        if os.environ.get("STOCKAGENT_STRICT_NO_FALLBACK", "0").strip().lower() in {
            "1",
            "true",
            "on",
            "yes",
        }:
            raise
        _ALL_TENOR_COMPILE_STATS["eager_fallback_calls"] += 1
        return _run_tw_index_futures_all_tenors_day_torch_impl(
            actions,
            execution_tensor,
            initial_capital=capital,
            max_abs_exposure=limit,
            initial_equity_scale=initial_equity_scale,
            initial_alive=initial_alive,
        )

    def concatenate(name: str) -> torch.Tensor:
        return torch.cat([getattr(chunk, name) for chunk in chunks], dim=0)

    final = chunks[-1]
    return FuturesContinuousBacktest(
        strategy_returns=concatenate("strategy_returns"),
        gross_returns=concatenate("gross_returns"),
        cost_returns=concatenate("cost_returns"),
        requested_exposure=concatenate("requested_exposure"),
        executed_exposure=concatenate("executed_exposure"),
        turnovers=concatenate("turnovers"),
        tradable_mask=concatenate("tradable_mask"),
        executed_actions=concatenate("executed_actions"),
        equity_scale_history=concatenate("equity_scale_history"),
        final_equity_scale=final.final_equity_scale,
        final_alive=final.final_alive,
    )


def _finite_nonnegative(name: str, value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"{name} must be a finite non-negative real")
    result = float(value)
    if not math.isfinite(result) or result < 0.0:
        raise ValueError(f"{name} must be a finite non-negative real")
    return result


def run_tw_index_futures_day_continuous(
    exposure: torch.Tensor,
    reference_log_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    *,
    round_trip_cost_rate: float = 0.00004,
    max_abs_exposure: float = 1.0,
) -> FuturesContinuousBacktest:
    """Run the differentiable same-session exposure contract.

    A row is flat before the open and after the close, so turnover is twice the
    absolute executed exposure.  Missing or non-finite reference returns are an
    executor-only no-trade fact and never become a zero-return open position.
    """

    cost_rate = _finite_nonnegative(
        "round_trip_cost_rate",
        round_trip_cost_rate,
    )
    exposure_limit = _finite_nonnegative("max_abs_exposure", max_abs_exposure)
    if exposure_limit <= 0.0:
        raise ValueError("max_abs_exposure must be positive")
    if exposure.ndim != 1:
        raise ValueError(f"exposure must have shape [T], got {tuple(exposure.shape)}")
    if tuple(reference_log_returns.shape) != tuple(exposure.shape):
        raise ValueError("reference_log_returns must have shape [T]")
    if tuple(tradable_mask.shape) != tuple(exposure.shape):
        raise ValueError("tradable_mask must have shape [T]")

    requested = torch.nan_to_num(
        exposure,
        nan=0.0,
        posinf=exposure_limit,
        neginf=-exposure_limit,
    ).clamp(min=-exposure_limit, max=exposure_limit)
    valid = (
        tradable_mask.to(device=exposure.device, dtype=torch.bool)
        & torch.isfinite(reference_log_returns)
    )
    executed = torch.where(valid, requested, torch.zeros_like(requested))
    simple_returns = torch.expm1(
        torch.where(
            valid,
            reference_log_returns.to(
                device=exposure.device,
                dtype=torch.float32,
            ),
            torch.zeros_like(executed, dtype=torch.float32),
        )
    )
    executed_f32 = executed.to(dtype=torch.float32)
    gross = executed_f32 * simple_returns
    costs = executed_f32.abs() * float(cost_rate)
    strategy = gross - costs
    return FuturesContinuousBacktest(
        strategy_returns=strategy,
        gross_returns=gross,
        cost_returns=costs,
        requested_exposure=requested,
        executed_exposure=executed,
        turnovers=executed_f32.abs() * 2.0,
        tradable_mask=valid,
    )


def tw_index_futures_log_utility_loss(
    exposure: torch.Tensor,
    reference_log_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    *,
    round_trip_cost_rate: float = 0.00004,
    max_abs_exposure: float = 1.0,
) -> torch.Tensor:
    result = run_tw_index_futures_day_continuous(
        exposure,
        reference_log_returns,
        tradable_mask,
        round_trip_cost_rate=round_trip_cost_rate,
        max_abs_exposure=max_abs_exposure,
    )
    valid = result.tradable_mask
    if not bool(valid.any().detach().cpu().item()):
        return exposure.sum() * 0.0
    safe_returns = torch.clamp(
        result.strategy_returns[valid],
        min=-1.0 + 1e-7,
    )
    return -torch.log1p(safe_returns).mean()


def _candidate_counts(
    target_notional: float,
    contract_notionals: np.ndarray,
    available: np.ndarray,
) -> list[tuple[int, ...]]:
    """Enumerate a bounded exact-sizing neighborhood from large to small."""

    notionals = np.asarray(contract_notionals, dtype=np.float64)
    tradable = np.asarray(available, dtype=bool)
    if notionals.shape != (3,) or tradable.shape != (3,):
        raise ValueError("contract_notionals and available must have shape [3]")
    candidates: list[tuple[int, ...]] = []

    def around(value: float, radius: int) -> list[int]:
        floor_value = max(0, int(math.floor(value)))
        values = {0}
        for candidate in range(
            max(0, floor_value - radius),
            floor_value + radius + 2,
        ):
            values.add(candidate)
        return sorted(values)

    tx_counts = (
        around(target_notional / notionals[0], 2)
        if tradable[0]
        else [0]
    )
    for tx_count in tx_counts:
        tx_notional = tx_count * notionals[0]
        residual_after_tx = max(0.0, target_notional - tx_notional)
        mtx_counts = (
            around(residual_after_tx / notionals[1], 5)
            if tradable[1]
            else [0]
        )
        for mtx_count in mtx_counts:
            used = tx_notional + mtx_count * notionals[1]
            residual = max(0.0, target_notional - used)
            tmf_counts = (
                around(residual / notionals[2], 3)
                if tradable[2]
                else [0]
            )
            for tmf_count in tmf_counts:
                candidates.append((tx_count, mtx_count, tmf_count))
    return candidates


def select_tw_index_futures_contract_basket(
    target_notional: float,
    open_prices: np.ndarray,
    tradable_mask: np.ndarray,
    *,
    cost_schedule: FuturesCostSchedule | None = None,
    max_notional: float | None = None,
) -> FuturesContractBasket:
    """Choose non-negative TX/MTX/TMF counts for an absolute target notional."""

    target = _finite_nonnegative("target_notional", target_notional)
    maximum = target if max_notional is None else _finite_nonnegative(
        "max_notional",
        max_notional,
    )
    schedule = FuturesCostSchedule() if cost_schedule is None else cost_schedule
    opens = np.asarray(open_prices, dtype=np.float64)
    tradable = np.asarray(tradable_mask, dtype=bool)
    if opens.shape != (3,) or tradable.shape != (3,):
        raise ValueError("open_prices and tradable_mask must have shape [3]")
    multipliers = np.asarray(
        [
            TAIFEX_INDEX_FUTURES_MULTIPLIERS[product]
            for product in TAIFEX_INDEX_FUTURES_PRODUCTS
        ],
        dtype=np.float64,
    )
    valid = tradable & np.isfinite(opens) & (opens > 0.0)
    notionals = opens * multipliers
    if target == 0.0 or not bool(valid.any()):
        return FuturesContractBasket(
            quantities=np.zeros(3, dtype=np.int64),
            target_notional=target,
            actual_notional=0.0,
            tracking_error=target,
            estimated_round_trip_cost=0.0,
        )
    # Invalid products are already excluded by ``valid`` in the enumerator.
    # Give them a finite zero placeholder rather than infinity: the unavailable
    # branch has an exact count of zero, and ``0 * inf`` would otherwise emit a
    # warning/NaN before that candidate is rejected.
    safe_notionals = np.where(valid, notionals, 0.0)
    fixed = schedule.fixed_fee_per_side_twd
    slippage = np.asarray(
        schedule.slippage_points_per_side,
        dtype=np.float64,
    )

    best_key: tuple[float, float, int, int, int] | None = None
    best_counts = np.zeros(3, dtype=np.int64)
    best_notional = 0.0
    best_cost = 0.0
    for raw_counts in _candidate_counts(target, safe_notionals, valid):
        counts = np.asarray(raw_counts, dtype=np.int64)
        if bool(np.any((counts > 0) & ~valid)):
            continue
        actual = float(np.dot(counts, np.where(valid, notionals, 0.0)))
        if actual > maximum + 1e-9:
            continue
        fixed_round_trip = float(np.dot(counts, fixed * 2.0))
        # Direction and closing prices are not known while choosing the basket
        # at the open.  Use twice the opening notional as the round-trip tax
        # estimate; the ledger below replaces the second leg with its actual
        # close price.
        tax_round_trip = float(
            np.dot(
                counts,
                np.where(valid, notionals, 0.0)
                * (2.0 * float(schedule.tax_rate)),
            )
        )
        slippage_round_trip = float(np.dot(counts, slippage * multipliers * 2.0))
        estimated_cost = (
            fixed_round_trip + tax_round_trip + slippage_round_trip
        )
        tracking_error = abs(actual - target)
        objective = tracking_error + float(
            schedule.basket_fee_penalty
        ) * estimated_cost
        # Ties prefer lower costs/fewer contracts, then larger contracts.
        key = (
            objective,
            estimated_cost,
            int(counts.sum()),
            -int(counts[0]),
            -int(counts[1]),
        )
        if best_key is None or key < best_key:
            best_key = key
            best_counts = counts
            best_notional = actual
            best_cost = estimated_cost
    return FuturesContractBasket(
        quantities=best_counts,
        target_notional=target,
        actual_notional=best_notional,
        tracking_error=abs(best_notional - target),
        estimated_round_trip_cost=best_cost,
    )


def run_tw_index_futures_day_integer(
    exposure: np.ndarray,
    market: TaiwanIndexFuturesDaySession,
    *,
    initial_capital: float,
    max_abs_exposure: float = 1.0,
    cost_schedule: FuturesCostSchedule | None = None,
) -> FuturesIntegerBacktest:
    """Execute direct E1..E6 actions or the legacy scalar front basket."""

    initial_equity = _finite_nonnegative("initial_capital", initial_capital)
    if initial_equity <= 0.0:
        raise ValueError("initial_capital must be positive")
    exposure_limit = _finite_nonnegative("max_abs_exposure", max_abs_exposure)
    if exposure_limit <= 0.0:
        raise ValueError("max_abs_exposure must be positive")
    raw_exposure = np.asarray(exposure, dtype=np.float64)
    if raw_exposure.ndim == 2:
        if raw_exposure.shape != (
            int(market.dates.size),
            TAIFEX_INDEX_FUTURES_ACTION_COUNT,
        ):
            raise ValueError("direct futures actions must have shape [T,18]")
        schedule = FuturesCostSchedule() if cost_schedule is None else cost_schedule
        execution = build_tw_index_futures_day_execution_tensor(
            market, cost_schedule=schedule
        ).astype(np.float64, copy=False)
        months, opens, _highs, _lows, closes, _volumes, _returns, tradable = (
            market.flattened_tenor_panel()
        )
        rows = int(market.dates.size)
        clean = np.nan_to_num(
            raw_exposure,
            nan=0.0,
            posinf=0.0,
            neginf=0.0,
        )
        requested_gross = np.abs(clean).sum(axis=-1, keepdims=True)
        clean *= np.minimum(
            1.0,
            exposure_limit / np.maximum(requested_gross, 1e-12),
        )
        quantities = np.zeros_like(clean, dtype=np.int64)
        executed = np.zeros(rows, dtype=np.float64)
        gross_pnl = np.zeros(rows, dtype=np.float64)
        fixed_fees = np.zeros(rows, dtype=np.float64)
        taxes = np.zeros(rows, dtype=np.float64)
        slippage_costs = np.zeros(rows, dtype=np.float64)
        net_pnl = np.zeros(rows, dtype=np.float64)
        strategy_returns = np.zeros(rows, dtype=np.float64)
        turnovers = np.zeros(rows, dtype=np.float64)
        equity = np.zeros(rows, dtype=np.float64)
        alive = np.ones(rows, dtype=bool)
        multipliers = np.repeat(
            np.asarray(market.multipliers, dtype=np.float64),
            TAIFEX_INDEX_FUTURES_TENOR_SLOTS,
        )
        product_index = np.repeat(
            np.arange(len(market.products), dtype=np.int64),
            TAIFEX_INDEX_FUTURES_TENOR_SLOTS,
        )
        fixed_per_side = schedule.fixed_fee_per_side_twd[product_index]
        slip_points = np.asarray(
            schedule.slippage_points_per_side, dtype=np.float64
        )[product_index]
        current_equity = float(initial_equity)
        still_alive = True
        valid_all = np.asarray(tradable, dtype=bool) & np.isfinite(execution).all(axis=-1)
        for row in range(rows):
            if not still_alive:
                equity[row] = current_equity
                alive[row] = False
                continue
            requested = np.where(valid_all[row], clean[row], 0.0)
            notionals = execution[row, :, 2]
            contract_ratio = np.divide(
                    np.abs(requested) * current_equity,
                    notionals,
                    out=np.zeros_like(requested),
                    where=valid_all[row] & (notionals > 0.0),
                )
            counts_abs = np.floor(contract_ratio + 1e-6).astype(np.int64)
            signed_counts = counts_abs * np.sign(requested).astype(np.int64)
            quantities[row] = signed_counts
            active = signed_counts != 0
            if not bool(active.any()):
                equity[row] = current_equity
                continue
            active_abs = counts_abs[active].astype(np.float64)
            active_signed = signed_counts[active].astype(np.float64)
            active_multipliers = multipliers[active]
            active_opens = opens[row, active]
            active_closes = closes[row, active]
            gross = float(
                np.dot(
                    active_signed * active_multipliers,
                    active_closes - active_opens,
                )
            )
            fees = float(np.dot(active_abs, 2.0 * fixed_per_side[active]))
            tax = float(
                np.dot(
                    active_abs * active_multipliers,
                    (active_opens + active_closes) * float(schedule.tax_rate),
                )
            )
            slip = float(
                np.dot(
                    active_abs,
                    2.0 * slip_points[active] * active_multipliers,
                )
            )
            net = gross - fees - tax - slip
            actual_abs_notional = float(
                np.dot(active_abs, active_opens * active_multipliers)
            )
            gross_pnl[row] = gross
            fixed_fees[row] = fees
            taxes[row] = tax
            slippage_costs[row] = slip
            net_pnl[row] = net
            strategy_returns[row] = net / current_equity
            executed[row] = actual_abs_notional / current_equity
            turnovers[row] = 2.0 * executed[row]
            next_equity = current_equity + net
            if not math.isfinite(next_equity) or next_equity <= 0.0:
                current_equity = 0.0
                still_alive = False
            else:
                current_equity = next_equity
            equity[row] = current_equity
            alive[row] = still_alive
        return FuturesIntegerBacktest(
            dates=np.asarray(market.dates, dtype="datetime64[D]").copy(),
            products=market.tenor_action_symbols(),
            requested_exposure=clean,
            executed_exposure=executed,
            contract_quantities=quantities,
            gross_pnl_twd=gross_pnl,
            fees_twd=fixed_fees,
            tax_twd=taxes,
            slippage_twd=slippage_costs,
            net_pnl_twd=net_pnl,
            strategy_returns=strategy_returns,
            turnovers=turnovers,
            equity=equity,
            alive=alive,
            contract_months=months.copy(),
        )
    requested = np.nan_to_num(
        raw_exposure,
        nan=0.0,
        posinf=exposure_limit,
        neginf=-exposure_limit,
    )
    requested = np.clip(requested, -exposure_limit, exposure_limit)
    rows = int(market.dates.size)
    if requested.shape != (rows,):
        raise ValueError(f"exposure must have shape [{rows}]")
    if market.products != TAIFEX_INDEX_FUTURES_PRODUCTS:
        raise ValueError(
            "integer execution requires products ordered as TX, MTX, TMF"
        )
    schedule = FuturesCostSchedule() if cost_schedule is None else cost_schedule
    multipliers = market.multipliers.astype(np.float64, copy=False)
    if tuple(multipliers.tolist()) != (200.0, 50.0, 10.0):
        raise ValueError("market multipliers must be TX=200, MTX=50, TMF=10")

    quantities = np.zeros((rows, 3), dtype=np.int64)
    executed = np.zeros(rows, dtype=np.float64)
    gross_pnl = np.zeros(rows, dtype=np.float64)
    fixed_fees = np.zeros(rows, dtype=np.float64)
    taxes = np.zeros(rows, dtype=np.float64)
    slippage_costs = np.zeros(rows, dtype=np.float64)
    net_pnl = np.zeros(rows, dtype=np.float64)
    strategy_returns = np.zeros(rows, dtype=np.float64)
    turnovers = np.zeros(rows, dtype=np.float64)
    equity = np.zeros(rows, dtype=np.float64)
    alive = np.ones(rows, dtype=bool)
    current_equity = float(initial_equity)
    still_alive = True
    fixed_per_side = schedule.fixed_fee_per_side_twd
    slip_points = np.asarray(
        schedule.slippage_points_per_side,
        dtype=np.float64,
    )

    for row in range(rows):
        if not still_alive or requested[row] == 0.0:
            equity[row] = current_equity
            alive[row] = still_alive
            continue
        valid_products = np.asarray(market.tradable_mask[row], dtype=bool)
        if not bool(valid_products.any()):
            equity[row] = current_equity
            alive[row] = still_alive
            continue
        target_abs = abs(float(requested[row])) * current_equity
        basket = select_tw_index_futures_contract_basket(
            target_abs,
            market.open_prices[row],
            valid_products,
            cost_schedule=schedule,
            max_notional=exposure_limit * current_equity,
        )
        direction = 1 if requested[row] > 0.0 else -1
        signed_counts = basket.quantities * direction
        quantities[row] = signed_counts
        if not bool(basket.quantities.any()):
            equity[row] = current_equity
            alive[row] = still_alive
            continue

        opens = market.open_prices[row]
        closes = market.close_prices[row]
        # A product with zero selected contracts has no cash flow. Restrict
        # every price-dependent calculation to active legs so unavailable
        # products cannot leak NaN through IEEE ``0 * NaN`` arithmetic. An
        # active leg with a broken price is instead a hard data-contract error;
        # silently converting it to zero PnL would fabricate an executable
        # round trip.
        active = signed_counts != 0
        active_prices_valid = (
            np.isfinite(opens[active])
            & np.isfinite(closes[active])
            & (opens[active] > 0.0)
            & (closes[active] > 0.0)
        )
        if not bool(active_prices_valid.all()):
            invalid_products = [
                market.products[index]
                for index in np.flatnonzero(active)
                if not (
                    math.isfinite(float(opens[index]))
                    and math.isfinite(float(closes[index]))
                    and float(opens[index]) > 0.0
                    and float(closes[index]) > 0.0
                )
            ]
            raise ValueError(
                "selected futures contracts require finite positive open/close "
                f"prices on {market.dates[row]}: {', '.join(invalid_products)}"
            )
        active_counts = signed_counts[active].astype(np.float64)
        active_absolute_counts = np.abs(active_counts)
        active_multipliers = multipliers[active]
        active_opens = opens[active]
        active_closes = closes[active]
        gross = float(
            np.dot(
                active_counts * active_multipliers,
                active_closes - active_opens,
            )
        )
        fees = float(
            np.dot(active_absolute_counts, fixed_per_side[active] * 2.0)
        )
        # Opening and closing are separate taxable transactions for both long
        # and short daily-flat positions.
        tax = float(
            np.dot(
                active_absolute_counts * active_multipliers,
                (active_opens + active_closes) * float(schedule.tax_rate),
            )
        )
        slip = float(
            np.dot(
                active_absolute_counts,
                slip_points[active] * active_multipliers * 2.0,
            )
        )
        net = gross - fees - tax - slip
        gross_pnl[row] = gross
        fixed_fees[row] = fees
        taxes[row] = tax
        slippage_costs[row] = slip
        net_pnl[row] = net
        strategy_returns[row] = net / current_equity
        executed[row] = direction * basket.actual_notional / current_equity
        turnovers[row] = 2.0 * abs(executed[row])
        next_equity = current_equity + net
        if not math.isfinite(next_equity) or next_equity <= 0.0:
            current_equity = 0.0
            still_alive = False
        else:
            current_equity = next_equity
        equity[row] = current_equity
        alive[row] = still_alive

    return FuturesIntegerBacktest(
        dates=np.asarray(market.dates, dtype="datetime64[D]").copy(),
        products=market.products,
        requested_exposure=requested,
        executed_exposure=executed,
        contract_quantities=quantities,
        gross_pnl_twd=gross_pnl,
        fees_twd=fixed_fees,
        tax_twd=taxes,
        slippage_twd=slippage_costs,
        net_pnl_twd=net_pnl,
        strategy_returns=strategy_returns,
        turnovers=turnovers,
        equity=equity,
        alive=alive,
        contract_months=market.contract_months.copy(),
    )


__all__ = [
    "FuturesContinuousBacktest",
    "FuturesContractBasket",
    "FuturesCostSchedule",
    "FuturesIntegerBacktest",
    "TAIFEX_FIXED_FEES_PER_SIDE_TWD",
    "TW_INDEX_FUTURES_DAY_BACKTEST_CONTRACT_VERSION",
    "TW_INDEX_FUTURES_TRAINING_GRADIENT_CONTRACT_VERSION",
    "TW_INDEX_FUTURES_COMPILED_BLOCK_ROWS",
    "TW_INDEX_FUTURES_SELL_TAX_RATE",
    "TW_INDEX_FUTURES_TAX_RATE",
    "TW_INDEX_FUTURES_TRANSACTION_TAX_RATE",
    "build_tw_index_futures_day_execution_tensor",
    "get_tw_index_futures_compile_stats",
    "run_tw_index_futures_all_tenors_day_torch",
    "run_tw_index_futures_day_continuous",
    "run_tw_index_futures_day_integer",
    "select_tw_index_futures_contract_basket",
    "tw_index_futures_log_utility_loss",
]

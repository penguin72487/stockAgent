"""Physical FIFO extension of the canonical backtest archive, not a writer.

The caller owns source acceptance and the exact ordered universe. Never infer
either from normalized weights, a filename, or a moving release head. Archive
encoding uses the same state validation as the safe Torch checkpoint contract.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import date
from typing import Mapping

import numpy as np
import torch

from stockagent.backtest.simulator import (
    BacktestResult,
    _portfolio_simple_returns_to_log_torch,
)
from stockagent.backtest.tw_day_trade_carry import DayTradeCarryState
from stockagent.backtest.tw_day_trade_inventory import DayTradeInventoryState


PHYSICAL_CARRY_ARCHIVE_SCHEMA = 8


@dataclass(frozen=True)
class DayTradeCarryArtifactContext:
    universe: tuple[str, ...]
    release_id: str  # exact composite source identity accepted by the caller
    initial_capital: float  # account denomination, not the segment's rebased NAV
    initial_nav: float  # NAV immediately BEFORE the first archived session


_HISTORY_FIELDS = {
    "strategy_returns", "benchmark_returns", "turnovers", "weights_history",
    "requested_weights_history", "shares_history", "settlement_default",
    "equity_scale_history", "final_weights", "final_alive", "final_equity_scale",
    "minute_nav",
}
_INVENTORY_FIELDS = tuple(DayTradeInventoryState.__dataclass_fields__)
_INVENTORY_METADATA = ("abi", "carry_contract", "odd_lot_contract")
_STATE_METADATA = ("abi", "initial_capital", "last_session_day", "last_nav", "alive")


def carry_artifact_payload(
    result: BacktestResult, dates: np.ndarray, context: DayTradeCarryArtifactContext,
) -> dict[str, np.ndarray]:
    """Validate a complete segment; reject future terminal state on a prefix."""
    if result.day_trade_carry_state is None or result.minute_nav is None:
        raise ValueError("physical FIFO archive requires both inventory and minute NAV")
    if result.execution_mode != "tw_day_trade" or result.settlement_ledger_unit != "currency":
        raise ValueError("physical FIFO archive requires the currency day-trade ledger")
    unsupported = [f.name for f in fields(result)
        if f.name not in _HISTORY_FIELDS | {
            "execution_mode", "settlement_ledger_unit", "day_trade_carry_state"}
        and getattr(result, f.name) is not None]
    if unsupported:
        raise ValueError(f"physical FIFO archive cannot discard other ledger fields: {unsupported}")
    if not isinstance(context, DayTradeCarryArtifactContext):
        raise ValueError("physical FIFO archive requires an explicit source context")
    if (isinstance(context.initial_nav, bool) or not np.isfinite(context.initial_nav)
            or context.initial_nav < 0):
        raise ValueError("physical FIFO segment initial NAV must be finite and nonnegative")
    if (isinstance(context.initial_capital, bool) or not np.isfinite(context.initial_capital)
            or context.initial_capital <= 0
            or context.initial_capital != result.day_trade_carry_state.initial_capital):
        raise ValueError("physical FIFO archive differs from pinned account capital")
    state = result.day_trade_carry_state.detached(device="cpu")
    checkpoint = state.checkpoint_state(universe=context.universe, release_id=context.release_id)
    original_dates = np.asarray(dates)
    if (original_dates.ndim != 1 or not len(original_dates)
            or original_dates.dtype.kind != "M"):
        raise ValueError("physical FIFO dates must be a nonempty datetime session vector")
    dates = original_dates.astype("datetime64[D]")
    if (np.isnat(dates).any() or not np.array_equal(dates, original_dates)
            or np.any(dates[1:] <= dates[:-1])):
        raise ValueError("physical FIFO dates must be exact strictly chronological sessions")
    last_day = int(dates[-1].astype(np.int64)) + date(1970, 1, 1).toordinal()
    if last_day != state.last_session_day:
        raise ValueError("physical FIFO prefix cannot retain a different terminal session state")
    rows, symbols = len(dates), len(context.universe)
    shapes = {
        **{n: (rows,) for n in ("strategy_returns", "benchmark_returns", "turnovers",
                                "settlement_default", "equity_scale_history")},
        **{n: (rows, symbols) for n in ("weights_history", "shares_history", "requested_weights_history")},
        "final_weights": (symbols,), "final_alive": (), "final_equity_scale": (),
        "minute_nav": (rows, 270),
    }
    payload = {}
    for name in sorted(_HISTORY_FIELDS):
        value = np.asarray(getattr(result, name))
        dtype = np.dtype(bool if name in {"settlement_default", "final_alive"} else np.float64)
        if value.shape != shapes[name] or value.dtype != dtype or not np.isfinite(value).all():
            raise ValueError(f"physical FIFO {name} must be finite {dtype} with shape {shapes[name]}")
        payload[name] = value
    nav = payload["minute_nav"]
    close_nav = nav[:, -1]
    if np.any(nav < 0) or np.any(payload["turnovers"] < 0):
        raise ValueError("physical FIFO NAV and turnover cannot be negative")
    flat_nav = nav.reshape(-1)
    if np.any((flat_nav == 0) & np.maximum.accumulate((flat_nav > 0)[::-1])[::-1]):
        raise ValueError("physical FIFO default must be absorbing within minute history")
    prior = np.concatenate(([context.initial_nav], close_nav[:-1]))
    if context.initial_nav == 0 and np.any(nav):
        raise ValueError("physical FIFO segment cannot revive a defaulted account")
    expected_returns = _portfolio_simple_returns_to_log_torch(
        torch.from_numpy(close_nav / np.maximum(prior, 1e-30) - 1)).numpy()
    expected_returns = np.where(prior > 0, expected_returns, 0.)
    shares = payload["shares_history"]
    if np.any(np.abs(shares - shares.round()) > 1e-8):
        raise ValueError("physical FIFO shares history must contain physical integer shares")
    comparisons = (
        (payload["strategy_returns"], expected_returns, "daily return / minute NAV"),
        (payload["equity_scale_history"], close_nav / state.initial_capital, "equity scale"),
        (payload["final_equity_scale"], close_nav[-1] / state.initial_capital, "final equity scale"),
        (payload["final_weights"], payload["weights_history"][-1], "terminal weights"),
        (shares[-1], state.inventory.shares.numpy(), "terminal physical shares"),
        (close_nav[-1], state.last_nav.item(), "terminal NAV"),
    )
    for actual, expected, label in comparisons:
        if not np.allclose(actual, expected, rtol=1e-12, atol=1e-10):
            raise ValueError(f"physical FIFO inconsistent {label}")
    if (not np.array_equal(payload["settlement_default"], close_nav == 0)
            or bool(payload["final_alive"]) != bool(state.alive)
            or bool(state.alive) != (close_nav[-1] > 0)):
        raise ValueError("physical FIFO inconsistent financial default state")
    payload.update(
        artifact_schema_version=np.asarray(PHYSICAL_CARRY_ARCHIVE_SCHEMA, dtype=np.int64),
        execution_mode=np.asarray(result.execution_mode),
        settlement_ledger_unit=np.asarray(result.settlement_ledger_unit),
        dates=dates,
        carry_universe=np.asarray(context.universe),
        carry_release_id=np.asarray(context.release_id),
        carry_segment_initial_nav=np.asarray(context.initial_nav, dtype=np.float64),
    )
    for name in _STATE_METADATA:
        value = checkpoint[name]
        payload[f"carry_{name}"] = value.numpy() if isinstance(value, torch.Tensor) else np.asarray(value)
    for name in (*_INVENTORY_FIELDS, *_INVENTORY_METADATA):
        value = checkpoint["inventory"][name]
        payload[f"carry_inventory_{name}"] = value.numpy() if isinstance(value, torch.Tensor) else np.asarray(value)
    return payload


def carry_artifact_from_payload(
    payload: Mapping[str, np.ndarray], context: DayTradeCarryArtifactContext,
) -> tuple[BacktestResult, np.ndarray]:
    """No pickle, dtype coercion, inferred source identity or legacy queue state."""
    required = _HISTORY_FIELDS | {
        "artifact_schema_version", "execution_mode", "settlement_ledger_unit", "dates",
        "carry_universe", "carry_release_id", "carry_segment_initial_nav",
        *(f"carry_{n}" for n in _STATE_METADATA),
        *(f"carry_inventory_{n}" for n in (*_INVENTORY_FIELDS, *_INVENTORY_METADATA)),
    }
    if set(payload) != required:
        raise ValueError(f"physical FIFO archive fields differ: missing={sorted(required - set(payload))} "
                         f"unknown={sorted(set(payload) - required)}")
    if not isinstance(context, DayTradeCarryArtifactContext):
        raise ValueError("physical FIFO load requires the caller's pinned source context")
    def scalar(name, kind):
        value = np.asarray(payload[name])
        if value.shape or value.dtype.kind not in kind:
            raise ValueError(f"physical FIFO invalid scalar {name}")
        return value.item()
    if scalar("artifact_schema_version", "iu") != PHYSICAL_CARRY_ARCHIVE_SCHEMA:
        raise ValueError("incompatible physical FIFO archive schema")
    universe = np.asarray(payload["carry_universe"])
    if (universe.ndim != 1 or universe.dtype.kind != "U"
            or universe.tolist() != list(context.universe)
            or scalar("carry_release_id", "U") != context.release_id
            or scalar("carry_segment_initial_nav", "f") != context.initial_nav):
        raise ValueError("physical FIFO archive differs from pinned universe/release/initial NAV")
    checkpoint = {
        "abi": scalar("carry_abi", "U"),
        "initial_capital": scalar("carry_initial_capital", "f"),
        "last_session_day": scalar("carry_last_session_day", "iu"),
        "last_nav": torch.from_numpy(np.array(payload["carry_last_nav"], copy=True)),
        "alive": torch.from_numpy(np.array(payload["carry_alive"], copy=True)),
        "inventory": {
            **{n: scalar(f"carry_inventory_{n}", "U") for n in _INVENTORY_METADATA},
            "universe": universe.tolist(), "release_id": context.release_id,
            **{n: torch.from_numpy(np.array(payload[f"carry_inventory_{n}"], copy=True))
               for n in _INVENTORY_FIELDS},
        },
    }
    state = DayTradeCarryState.from_checkpoint_state(checkpoint,
        universe=context.universe, release_id=context.release_id,
        initial_capital=context.initial_capital)
    result = BacktestResult(**{n: np.array(payload[n], copy=True) for n in _HISTORY_FIELDS},
        execution_mode=scalar("execution_mode", "U"),
        settlement_ledger_unit=scalar("settlement_ledger_unit", "U"), day_trade_carry_state=state)
    dates = np.array(payload["dates"], copy=True)
    carry_artifact_payload(result, dates, context)  # same semantic acceptance on write/read
    return result, dates

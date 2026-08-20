from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np

from stockagent.backtest.tw_execution import normalize_execution_mode
from stockagent.data.panel import PanelData
from stockagent.data.walkforward import WalkForwardFold, normalize_lookback_context


@dataclass(frozen=True, slots=True)
class DayTradeExecutionCoverage:
    actionable_cells_by_row: np.ndarray
    eligible_cells_by_row: np.ndarray
    first_actionable_date: np.datetime64 | None


def _required_bool_panel(
    panel: PanelData,
    name: str,
) -> np.ndarray:
    value = getattr(panel, name, None)
    if value is None:
        raise ValueError(f"tw_day_trade requires PanelData.{name}")
    result = np.asarray(value, dtype=bool)
    if result.shape != panel.tradable_mask.shape:
        raise ValueError(
            f"PanelData.{name} shape {result.shape} must match "
            f"tradable_mask shape {panel.tradable_mask.shape}"
        )
    return result


def measure_tw_day_trade_execution_coverage(
    panel: PanelData,
    *,
    long_only: bool,
    chunk_rows: int = 64,
) -> DayTradeExecutionCoverage:
    """Measure realized round-trip support without exposing it to the model.

    This is an offline preflight only.  Current-session open/close masks remain
    executor-only and are never added to the model's selection mask.
    """

    if panel.intraday_returns is None:
        raise ValueError("tw_day_trade requires PanelData.intraday_returns")
    intraday_returns = np.asarray(panel.intraday_returns)
    if intraday_returns.shape != panel.tradable_mask.shape:
        raise ValueError(
            "PanelData.intraday_returns must match tradable_mask shape"
        )

    eligible = _required_bool_panel(panel, "day_trade_eligible_mask")
    buy_open = _required_bool_panel(panel, "day_trade_can_buy_open_mask")
    sell_open = _required_bool_panel(panel, "day_trade_can_sell_open_mask")
    buy_close = _required_bool_panel(panel, "can_buy_mask")
    sell_close = _required_bool_panel(panel, "can_sell_mask")
    short_open = (
        np.zeros_like(eligible, dtype=bool)
        if panel.day_trade_can_short_open_mask is None
        else _required_bool_panel(panel, "day_trade_can_short_open_mask")
    )
    force_exit = (
        np.zeros_like(eligible, dtype=bool)
        if panel.force_exit_mask is None
        else _required_bool_panel(panel, "force_exit_mask")
    )

    row_count = int(panel.num_dates)
    eligible_cells_by_row = np.zeros(row_count, dtype=np.int64)
    actionable_cells_by_row = np.zeros(row_count, dtype=np.int64)
    block_rows = max(1, int(chunk_rows))
    for start in range(0, row_count, block_rows):
        end = min(row_count, start + block_rows)
        row_slice = slice(start, end)
        finite_return = np.isfinite(intraday_returns[row_slice])
        entry_eligible = eligible[row_slice] & ~force_exit[row_slice]
        long_round_trip = (
            entry_eligible
            & buy_open[row_slice]
            & sell_close[row_slice]
            & finite_return
        )
        if long_only:
            actionable = long_round_trip
        else:
            short_round_trip = (
                entry_eligible
                & sell_open[row_slice]
                & short_open[row_slice]
                & buy_close[row_slice]
                & finite_return
            )
            actionable = long_round_trip | short_round_trip
        eligible_cells_by_row[row_slice] = np.count_nonzero(
            entry_eligible,
            axis=1,
        )
        actionable_cells_by_row[row_slice] = np.count_nonzero(
            actionable,
            axis=1,
        )

    actionable_rows = np.flatnonzero(actionable_cells_by_row > 0)
    first_actionable_date = (
        None
        if actionable_rows.size == 0
        else np.asarray(panel.dates)[int(actionable_rows[0])]
    )
    return DayTradeExecutionCoverage(
        actionable_cells_by_row=actionable_cells_by_row,
        eligible_cells_by_row=eligible_cells_by_row,
        first_actionable_date=first_actionable_date,
    )


def _valid_split_indices(
    date_indices: np.ndarray,
    *,
    lookback: int,
    lookback_context: str = "panel_history",
) -> np.ndarray:
    indices = np.sort(np.asarray(date_indices, dtype=np.int64))
    if indices.size == 0:
        return indices
    # tw_day_trade commits the session-t order from close-complete rows through
    # t-1 (plus the opt-in open[t] gap stored on that final row), so a
    # lookback-L target uses only already-observed rows.  The canonical
    # panel-history policy owns the split's first target whenever enough causal
    # history exists in the panel; split-only is retained only for legacy runs.
    history_origin = (
        0
        if normalize_lookback_context(lookback_context) == "panel_history"
        else int(indices[0])
    )
    first_valid = history_origin + int(lookback)
    return indices[indices >= first_valid]


def validate_training_execution_coverage(
    panel: PanelData,
    folds: Iterable[WalkForwardFold],
    *,
    execution_mode: str,
    long_only: bool,
    lookback: int,
    lookback_context: str = "panel_history",
) -> DayTradeExecutionCoverage | None:
    """Reject folds whose objective is mathematically constant in model output."""

    if normalize_execution_mode(execution_mode) != "tw_day_trade":
        return None
    fold_list = list(folds)
    if not fold_list:
        return None

    coverage = measure_tw_day_trade_execution_coverage(
        panel,
        long_only=long_only,
    )
    for fold in fold_list:
        for split_name, years, raw_indices in (
            ("train", fold.train_years, fold.train_indices),
            ("validation", fold.val_years, fold.val_indices),
        ):
            indices = _valid_split_indices(
                raw_indices,
                lookback=lookback,
                lookback_context=lookback_context,
            )
            eligible_cells = int(
                coverage.eligible_cells_by_row[indices].sum()
            )
            actionable_cells = int(
                coverage.actionable_cells_by_row[indices].sum()
            )
            if actionable_cells > 0:
                continue
            earliest = (
                "none"
                if coverage.first_actionable_date is None
                else str(
                    np.asarray(coverage.first_actionable_date).astype(
                        "datetime64[D]"
                    )
                )
            )
            raise ValueError(
                "tw_day_trade execution-coverage preflight failed: "
                f"fold={int(fold.fold_id)} split={split_name} years={list(years)} "
                f"has zero executable round trips after lookback={int(lookback)} "
                f"(eligible_cells={eligible_cells}, actionable_cells=0). "
                "The canonical loss is therefore constant in model output and "
                "all model gradients would be exactly zero. "
                f"Earliest executable panel date={earliest}. Set data.panel_start_date "
                "and walk_forward.expected_first_year to an honestly supported year; "
                "never project current eligibility backward."
            )
    return coverage


__all__ = [
    "DayTradeExecutionCoverage",
    "measure_tw_day_trade_execution_coverage",
    "validate_training_execution_coverage",
]

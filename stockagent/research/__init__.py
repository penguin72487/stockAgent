"""Isolated research workflows that are not live-order adapters."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from stockagent.research.tw_minute_kbars import (
        MinuteKbarBacktestConfig,
        MinuteKbarBacktestResult,
        MinuteRebalanceBacktester,
        add_minute_strategy_scores,
        chronological_date_splits,
        run_minute_rebalance_backtest,
        run_minute_round_trip_backtest,
    )

__all__ = [
    "MinuteKbarBacktestConfig",
    "MinuteKbarBacktestResult",
    "MinuteRebalanceBacktester",
    "add_minute_strategy_scores",
    "chronological_date_splits",
    "run_minute_rebalance_backtest",
    "run_minute_round_trip_backtest",
]


def __getattr__(name: str) -> Any:
    if name in __all__:
        from stockagent.research.tw_minute_kbars import (
            MinuteKbarBacktestConfig,
            MinuteKbarBacktestResult,
            MinuteRebalanceBacktester,
            add_minute_strategy_scores,
            chronological_date_splits,
            run_minute_rebalance_backtest,
            run_minute_round_trip_backtest,
        )

        return {
            "MinuteKbarBacktestConfig": MinuteKbarBacktestConfig,
            "MinuteKbarBacktestResult": MinuteKbarBacktestResult,
            "MinuteRebalanceBacktester": MinuteRebalanceBacktester,
            "add_minute_strategy_scores": add_minute_strategy_scores,
            "chronological_date_splits": chronological_date_splits,
            "run_minute_rebalance_backtest": run_minute_rebalance_backtest,
            "run_minute_round_trip_backtest": run_minute_round_trip_backtest,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted((*globals(), *__all__))

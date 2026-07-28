from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import time
from typing import Any

import numpy as np
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.research.tw_minute_kbars import (  # noqa: E402
    STRATEGY_SCORE_COLUMNS,
    MinuteKbarBacktestConfig,
    MinuteRebalanceBacktester,
    add_minute_strategy_scores,
    chronological_date_splits,
    run_minute_round_trip_backtest,
)

RESEARCH_COLUMNS = (
    "date",
    "ts",
    "symbol",
    "minutes_from_open",
    "feature_valid",
    "log_close_return_1m",
    "intrabar_log_return",
    "close_location",
    "relative_volume_20",
    "gap_log_return",
    "label_valid_1m",
    "execution_open_next_1m",
    "exit_close_next_1m",
    "future_volume_shares_next_1m",
    "session_exit_valid",
    "session_close",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate transparent one-minute Kbar day-trade baselines with "
            "chronological train/validation/test reporting."
        )
    )
    parser.add_argument(
        "--dataset-root",
        type=Path,
        default=Path("data_tw_minute/research_dataset"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("artifacts/research/tw_minute_kbars"),
    )
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument(
        "--holding-mode",
        choices=("minute_rebalance", "next_minute", "session_close"),
        default="minute_rebalance",
    )
    parser.add_argument(
        "--first-decision-minute",
        "--entry-minute",
        dest="first_decision_minute",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--last-decision-minute",
        type=int,
        default=269,
    )
    parser.add_argument("--slippage-bps-per-side", type=float, default=2.0)
    parser.add_argument("--maximum-volume-participation", type=float, default=0.01)
    parser.add_argument("--maximum-order-notional", type=float, default=1_000_000.0)
    parser.add_argument(
        "--selection-hysteresis-multiplier",
        type=float,
        default=2.0,
    )
    return parser.parse_args()


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _stateful_split_metrics(
    result: Any,
    *,
    split_dates: tuple[Any, ...],
    symbols: int,
) -> dict[str, Any]:
    curve = result.equity_curve.filter(pl.col("date").is_in(split_dates))
    trades = result.trades.filter(pl.col("date").is_in(split_dates))
    if curve.is_empty():
        raise RuntimeError("stateful split has no equity events")
    first_equity = float(curve["equity"][0])
    first_return = float(curve["strategy_return"][0])
    starting_equity = first_equity / (1.0 + first_return)
    ending_equity = float(curve["equity"][-1])
    equity_values = np.r_[starting_equity, curve["equity"].to_numpy()]
    maximum_drawdown = float(
        np.min(equity_values / np.maximum.accumulate(equity_values) - 1.0)
    )
    daily = curve.group_by("date", maintain_order=True).agg(
        pl.col("equity").last().alias("ending_equity")
    )
    daily_equity = daily["ending_equity"].to_numpy()
    daily_prior = np.r_[starting_equity, daily_equity[:-1]]
    daily_returns = daily_equity / daily_prior - 1.0
    daily_std = float(np.std(daily_returns, ddof=1)) if daily_returns.size > 1 else 0.0
    daily_sharpe = (
        float(np.mean(daily_returns) / daily_std * np.sqrt(252.0))
        if daily_std > 0
        else None
    )
    return {
        "dates": len(split_dates),
        "symbols": symbols,
        "decisions": curve.filter(pl.col("event_type") == "minute_decision").height,
        "total_return": ending_equity / starting_equity - 1.0,
        "maximum_drawdown": maximum_drawdown,
        "daily_sharpe": daily_sharpe,
        "positive_day_rate": float(np.mean(daily_returns > 0)),
        "trades": trades.height,
        "total_executed_notional": float(curve["executed_notional"].sum()),
        "total_explicit_fees": float(curve["explicit_fees"].sum()),
        "total_slippage_cost": float(curve["slippage_cost"].sum()),
        "mean_actual_gross_exposure": float(curve["actual_gross_exposure"].mean()),
        "forced_exit_over_capacity_notional": float(
            curve["forced_exit_over_capacity_notional"].sum()
        ),
        "stale_forced_exit_notional": float(curve["stale_forced_exit_notional"].sum()),
    }


def _run_stateful_minute_research(
    args: argparse.Namespace,
    paths: list[Path],
) -> None:
    dates = sorted(
        datetime.strptime(path.parent.name.split("=", 1)[1], "%Y-%m-%d").date()
        for path in paths
    )
    splits = chronological_date_splits(dates)
    config = MinuteKbarBacktestConfig(
        top_n=int(args.top_n),
        holding_mode="minute_rebalance",
        first_decision_minute=int(args.first_decision_minute),
        last_decision_minute=int(args.last_decision_minute),
        slippage_bps_per_side=float(args.slippage_bps_per_side),
        maximum_volume_participation=float(args.maximum_volume_participation),
        maximum_order_notional=float(args.maximum_order_notional),
        selection_hysteresis_multiplier=float(args.selection_hysteresis_multiplier),
    )
    full_engines = {
        score: MinuteRebalanceBacktester(score_column=score, config=config)
        for score in STRATEGY_SCORE_COLUMNS
    }
    total_rows = 0
    symbols: set[str] = set()
    load_seconds = 0.0
    benchmark_seconds = 0.0
    for index, path in enumerate(paths, start=1):
        load_started = time.perf_counter()
        frame = pl.read_parquet(path, columns=RESEARCH_COLUMNS)
        scored = add_minute_strategy_scores(frame)
        load_seconds += time.perf_counter() - load_started
        total_rows += scored.height
        symbols.update(str(value) for value in scored["symbol"].unique().to_list())
        trade_date = scored["date"][0]
        benchmark_started = time.perf_counter()
        for score_index, score in enumerate(STRATEGY_SCORE_COLUMNS):
            full_engines[score].process_day(
                scored,
                validate_keys=score_index == 0,
            )
        benchmark_seconds += time.perf_counter() - benchmark_started
        if index == 1 or index % 25 == 0 or index == len(paths):
            print(
                f"[tw-minute-research] partition={index}/{len(paths)} "
                f"date={trade_date} rows={scored.height}",
                flush=True,
            )

    full_results = {score: engine.finalize() for score, engine in full_engines.items()}
    rows: list[dict[str, Any]] = []
    for score in STRATEGY_SCORE_COLUMNS:
        for split_name, split_dates in splits.items():
            if not split_dates:
                continue
            summary = _stateful_split_metrics(
                full_results[score],
                split_dates=split_dates,
                symbols=len(symbols),
            )
            rows.append(
                {
                    "strategy": score,
                    "split": split_name,
                    "start_date": min(split_dates).isoformat(),
                    "end_date": max(split_dates).isoformat(),
                    **{
                        key: summary[key]
                        for key in (
                            "dates",
                            "symbols",
                            "decisions",
                            "total_return",
                            "maximum_drawdown",
                            "daily_sharpe",
                            "positive_day_rate",
                            "trades",
                            "total_executed_notional",
                            "total_explicit_fees",
                            "total_slippage_cost",
                            "mean_actual_gross_exposure",
                            "forced_exit_over_capacity_notional",
                            "stale_forced_exit_notional",
                        )
                    },
                }
            )
    results_frame = pl.DataFrame(rows).sort(["strategy", "split"])
    args.output_dir.mkdir(parents=True, exist_ok=True)
    results_frame.write_csv(args.output_dir / "strategy_split_metrics.csv")
    validation = results_frame.filter(pl.col("split") == "validation")
    research_ready = (
        len(dates) >= config.minimum_research_days
        and len(symbols) >= config.minimum_research_symbols
    )
    selected_strategy = None
    if research_ready and validation.height:
        finite = validation.filter(pl.col("daily_sharpe").is_not_null())
        if finite.height:
            selected_strategy = str(
                finite.sort("daily_sharpe", descending=True)["strategy"][0]
            )
    for score, result in full_results.items():
        result.equity_curve.write_parquet(
            args.output_dir / f"{score}_equity.parquet",
            compression="zstd",
        )
        result.trades.write_parquet(
            args.output_dir / f"{score}_trades.parquet",
            compression="zstd",
        )
    _atomic_json(
        args.output_dir / "summary.json",
        {
            "schema_version": 2,
            "status": "ok",
            "source": "shioaji_kbars_1m",
            "holding_mode": "minute_rebalance",
            "timing_contract": (
                "every completed right-labelled bar t -> decide target -> "
                "execute at next bar open -> carry state -> force flat by close"
            ),
            "dates": len(dates),
            "symbols": len(symbols),
            "rows": total_rows,
            "research_ready": research_ready,
            "minimum_research_days": config.minimum_research_days,
            "minimum_research_symbols": config.minimum_research_symbols,
            "selected_strategy_from_validation": selected_strategy,
            "selection_suppressed_reason": (
                None
                if research_ready
                else "pilot coverage is below the configured day/symbol research gate"
            ),
            "splits": {
                key: {
                    "dates": len(value),
                    "start": min(value).isoformat() if value else None,
                    "end": max(value).isoformat() if value else None,
                }
                for key, value in splits.items()
            },
            "configuration": asdict(config),
            "split_capital_semantics": "continuous_full_path",
            "load_and_score_seconds": load_seconds,
            "all_strategy_backtests_seconds": benchmark_seconds,
            "rows_per_second": (
                total_rows * len(STRATEGY_SCORE_COLUMNS) / benchmark_seconds
                if benchmark_seconds > 0
                else None
            ),
            "written_at_utc": datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat(),
        },
    )
    print(
        f"[tw-minute-research] dates={len(dates)} symbols={len(symbols)} "
        f"rows={total_rows} ready={research_ready} "
        f"backtest_seconds={benchmark_seconds:.3f} output={args.output_dir}",
        flush=True,
    )


def main() -> None:
    args = parse_args()
    paths = sorted(args.dataset_root.glob("trade_date=*/data.parquet"))
    if not paths:
        raise RuntimeError(f"no minute research partitions under {args.dataset_root}")
    if args.holding_mode == "minute_rebalance":
        _run_stateful_minute_research(args, paths)
        return
    load_started = time.perf_counter()
    scan = pl.scan_parquet([str(path) for path in paths]).select(RESEARCH_COLUMNS)
    if args.holding_mode == "session_close":
        # The session strategy makes exactly one decision per symbol/day.
        # Predicate pushdown avoids materializing the other ~269 bars for a
        # full-market multi-year study.
        scan = scan.filter(
            pl.col("minutes_from_open") == int(args.first_decision_minute)
        )
    frame = scan.collect(engine="streaming")
    scored = add_minute_strategy_scores(frame)
    load_seconds = time.perf_counter() - load_started
    dates = sorted(set(scored["date"].to_list()))
    splits = chronological_date_splits(dates)
    config = MinuteKbarBacktestConfig(
        top_n=int(args.top_n),
        holding_mode=str(args.holding_mode),
        first_decision_minute=int(args.first_decision_minute),
        last_decision_minute=int(args.last_decision_minute),
        slippage_bps_per_side=float(args.slippage_bps_per_side),
        maximum_volume_participation=float(args.maximum_volume_participation),
        maximum_order_notional=float(args.maximum_order_notional),
        selection_hysteresis_multiplier=float(args.selection_hysteresis_multiplier),
    )
    rows: list[dict[str, Any]] = []
    full_results = {}
    benchmark_started = time.perf_counter()
    for score_column in STRATEGY_SCORE_COLUMNS:
        full_result = run_minute_round_trip_backtest(
            scored,
            score_column=score_column,
            config=config,
        )
        full_results[score_column] = full_result
        for split_name, split_dates in splits.items():
            if not split_dates:
                continue
            split_frame = scored.filter(pl.col("date").is_in(split_dates))
            result = run_minute_round_trip_backtest(
                split_frame,
                score_column=score_column,
                config=config,
            )
            rows.append(
                {
                    "strategy": score_column,
                    "split": split_name,
                    "start_date": min(split_dates).isoformat(),
                    "end_date": max(split_dates).isoformat(),
                    **{
                        key: result.summary[key]
                        for key in (
                            "dates",
                            "symbols",
                            "total_return",
                            "maximum_drawdown",
                            "daily_sharpe",
                            "positive_day_rate",
                            "trades",
                            "total_executed_notional",
                            "total_explicit_fees",
                            "total_slippage_cost",
                            "mean_actual_gross_exposure",
                        )
                    },
                }
            )
    benchmark_seconds = time.perf_counter() - benchmark_started
    results_frame = pl.DataFrame(rows).sort(["strategy", "split"])
    args.output_dir.mkdir(parents=True, exist_ok=True)
    results_frame.write_csv(args.output_dir / "strategy_split_metrics.csv")

    validation = results_frame.filter(pl.col("split") == "validation")
    selected_strategy = None
    if (
        validation.height
        and scored["symbol"].n_unique() >= config.minimum_research_symbols
    ):
        finite = validation.filter(pl.col("daily_sharpe").is_not_null())
        if finite.height:
            selected_strategy = str(
                finite.sort("daily_sharpe", descending=True)["strategy"][0]
            )
    for score_column, result in full_results.items():
        result.equity_curve.write_parquet(
            args.output_dir / f"{score_column}_equity.parquet",
            compression="zstd",
        )
        result.trades.write_parquet(
            args.output_dir / f"{score_column}_trades.parquet",
            compression="zstd",
        )
    research_ready = (
        len(dates) >= config.minimum_research_days
        and scored["symbol"].n_unique() >= config.minimum_research_symbols
    )
    _atomic_json(
        args.output_dir / "summary.json",
        {
            "schema_version": 1,
            "status": "ok",
            "source": "shioaji_kbars_1m",
            "holding_mode": config.holding_mode,
            "timing_contract": (
                "completed_bar_t -> open_t_plus_1 -> session_close"
                if config.holding_mode == "session_close"
                else "completed_bar_t -> open_t_plus_1 -> close_t_plus_1"
            ),
            "dates": len(dates),
            "symbols": scored["symbol"].n_unique(),
            "rows": scored.height,
            "research_ready": research_ready,
            "minimum_research_days": config.minimum_research_days,
            "minimum_research_symbols": config.minimum_research_symbols,
            "selected_strategy_from_validation": (
                selected_strategy if research_ready else None
            ),
            "selection_suppressed_reason": (
                None
                if research_ready
                else "pilot coverage is below the configured day/symbol research gate"
            ),
            "splits": {
                key: {
                    "dates": len(value),
                    "start": min(value).isoformat() if value else None,
                    "end": max(value).isoformat() if value else None,
                }
                for key, value in splits.items()
            },
            "configuration": asdict(config),
            "load_and_score_seconds": load_seconds,
            "all_strategy_backtests_seconds": benchmark_seconds,
            "rows_per_second": (
                scored.height * len(STRATEGY_SCORE_COLUMNS) * 4 / benchmark_seconds
                if benchmark_seconds > 0
                else None
            ),
            "written_at_utc": datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat(),
        },
    )
    print(
        f"[tw-minute-research] dates={len(dates)} symbols={scored['symbol'].n_unique()} "
        f"rows={scored.height} ready={research_ready} "
        f"backtest_seconds={benchmark_seconds:.3f} output={args.output_dir}",
        flush=True,
    )


if __name__ == "__main__":
    main()

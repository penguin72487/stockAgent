from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
import math
from typing import Any

import numpy as np
import polars as pl

from stockagent.backtest.tw_execution import (
    TaiwanFeeSchedule,
    effective_fee_rate_vectors,
)


STRATEGY_SCORE_COLUMNS = (
    "score_momentum",
    "score_reversal",
    "score_candle_pressure",
    "score_volume_breakout",
    "score_blend",
)


@dataclass(frozen=True, slots=True)
class MinuteKbarBacktestConfig:
    """Causal minute-decision assumptions for Kbar research."""

    initial_equity: float = 10_000_000.0
    gross_exposure: float = 0.90
    top_n: int = 10
    maximum_name_weight: float = 0.15
    slippage_bps_per_side: float = 2.0
    maximum_volume_participation: float = 0.01
    maximum_order_notional: float = 1_000_000.0
    selection_hysteresis_multiplier: float = 2.0
    holding_mode: str = "minute_rebalance"
    first_decision_minute: int = 1
    last_decision_minute: int = 269
    minimum_score: float = 0.0
    minimum_research_days: int = 120
    minimum_research_symbols: int = 20
    fee_schedule: TaiwanFeeSchedule = TaiwanFeeSchedule()

    def __post_init__(self) -> None:
        for name in ("initial_equity", "maximum_order_notional"):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be positive and finite")
        for name in (
            "gross_exposure",
            "maximum_name_weight",
            "maximum_volume_participation",
        ):
            value = float(getattr(self, name))
            if not math.isfinite(value) or not 0 < value <= 1:
                raise ValueError(f"{name} must be in (0, 1]")
        if self.top_n <= 0:
            raise ValueError("top_n must be positive")
        if (
            not math.isfinite(self.selection_hysteresis_multiplier)
            or self.selection_hysteresis_multiplier < 1.0
        ):
            raise ValueError(
                "selection_hysteresis_multiplier must be finite and at least 1"
            )
        if not 0 <= self.first_decision_minute <= self.last_decision_minute <= 269:
            raise ValueError("decision minutes must be between 0 and 269")
        if self.holding_mode not in {
            "minute_rebalance",
            "next_minute",
            "session_close",
        }:
            raise ValueError(
                "holding_mode must be minute_rebalance, next_minute, or session_close"
            )
        if (
            not math.isfinite(self.slippage_bps_per_side)
            or self.slippage_bps_per_side < 0
        ):
            raise ValueError("slippage_bps_per_side must be finite and nonnegative")
        if not math.isfinite(self.minimum_score):
            raise ValueError("minimum_score must be finite")


@dataclass(slots=True)
class MinuteKbarBacktestResult:
    summary: dict[str, Any]
    equity_curve: pl.DataFrame
    trades: pl.DataFrame


def _rank_unit(raw: pl.Expr) -> pl.Expr:
    valid_raw = pl.when(pl.col("feature_valid") & raw.is_finite()).then(raw)
    count = valid_raw.is_not_null().sum().over("ts")
    rank = valid_raw.rank(method="average").over("ts")
    return (
        pl.when(count > 1).then(2.0 * (rank - 1.0) / (count - 1.0) - 1.0).otherwise(0.0)
    )


def add_minute_strategy_scores(frame: pl.DataFrame) -> pl.DataFrame:
    """Add transparent cross-sectional scores from completed Kbars only."""

    required = {
        "ts",
        "feature_valid",
        "log_close_return_1m",
        "intrabar_log_return",
        "close_location",
        "relative_volume_20",
        "gap_log_return",
    }
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"minute score inputs are missing: {sorted(missing)}")
    momentum = _rank_unit(pl.col("log_close_return_1m"))
    reversal = _rank_unit(-pl.col("log_close_return_1m"))
    candle_pressure = _rank_unit(
        pl.col("intrabar_log_return") + 0.001 * (pl.col("close_location") - 0.5)
    )
    volume_breakout = _rank_unit(
        pl.col("log_close_return_1m")
        * pl.col("relative_volume_20").clip(0.0, 10.0).log1p()
    )
    gap = _rank_unit(pl.col("gap_log_return"))
    return frame.with_columns(
        momentum.alias("score_momentum"),
        reversal.alias("score_reversal"),
        candle_pressure.alias("score_candle_pressure"),
        volume_breakout.alias("score_volume_breakout"),
        (
            0.30 * momentum
            + 0.25 * candle_pressure
            + 0.25 * volume_breakout
            + 0.20 * gap
        ).alias("score_blend"),
    )


def chronological_date_splits(
    dates: list[date],
    *,
    train_fraction: float = 0.60,
    validation_fraction: float = 0.20,
) -> dict[str, tuple[date, ...]]:
    """Split complete dates chronologically; never randomize minute rows."""

    unique = sorted(set(dates))
    if not unique:
        return {"train": (), "validation": (), "test": ()}
    if not 0 < train_fraction < 1:
        raise ValueError("train_fraction must be in (0, 1)")
    if not 0 < validation_fraction < 1:
        raise ValueError("validation_fraction must be in (0, 1)")
    if train_fraction + validation_fraction >= 1:
        raise ValueError("train plus validation fraction must be below 1")
    train_end = max(1, int(len(unique) * train_fraction))
    validation_end = max(
        train_end + 1, int(len(unique) * (train_fraction + validation_fraction))
    )
    validation_end = min(validation_end, len(unique))
    return {
        "train": tuple(unique[:train_end]),
        "validation": tuple(unique[train_end:validation_end]),
        "test": tuple(unique[validation_end:]),
    }


def _maximum_drawdown(equity: np.ndarray) -> float:
    if equity.size == 0:
        return 0.0
    return float(np.min(equity / np.maximum.accumulate(equity) - 1.0))


def run_minute_round_trip_backtest(
    frame: pl.DataFrame,
    *,
    score_column: str = "score_blend",
    config: MinuteKbarBacktestConfig | None = None,
) -> MinuteKbarBacktestResult:
    """Enter at next Kbar open and exit at that same Kbar close.

    Selection is completed before inspecting next-bar label/fill fields. A
    missing label or insufficient future bar volume reduces actual exposure; it
    never causes replacement by a lower-ranked symbol.
    """

    config = config or MinuteKbarBacktestConfig()
    if config.holding_mode == "minute_rebalance":
        return run_minute_rebalance_backtest(
            frame,
            score_column=score_column,
            config=config,
        )
    label_column = (
        "session_exit_valid"
        if config.holding_mode == "session_close"
        else "label_valid_1m"
    )
    close_column = (
        "session_close"
        if config.holding_mode == "session_close"
        else "exit_close_next_1m"
    )
    required = {
        "date",
        "ts",
        "symbol",
        "minutes_from_open",
        "feature_valid",
        label_column,
        "execution_open_next_1m",
        close_column,
        "future_volume_shares_next_1m",
        score_column,
    }
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"minute backtest inputs are missing: {sorted(missing)}")
    duplicates = frame.group_by("ts", "symbol").len().filter(pl.col("len") > 1).height
    if duplicates:
        raise ValueError("minute backtest requires unique timestamp/symbol rows")

    symbols = sorted(str(value) for value in frame["symbol"].unique().to_list())
    buy_rates, sell_rates = effective_fee_rate_vectors(
        symbols,
        "tw_day_trade",
        fee_schedule=config.fee_schedule,
    )
    fee_map = pl.DataFrame(
        {
            "symbol": symbols,
            "buy_fee_rate": buy_rates,
            "sell_fee_rate": sell_rates,
        }
    )
    decision_filter = (
        pl.col("minutes_from_open") == config.first_decision_minute
        if config.holding_mode == "session_close"
        else pl.col("minutes_from_open").is_between(
            config.first_decision_minute,
            config.last_decision_minute,
            closed="both",
        )
    )
    candidates = (
        frame.filter(
            pl.col("feature_valid")
            & pl.col(score_column).is_not_null()
            & pl.col(score_column).is_finite()
            & (pl.col(score_column) > config.minimum_score)
            & decision_filter
        )
        .with_columns(
            pl.col(score_column)
            .rank(method="ordinal", descending=True)
            .over("ts")
            .alias("selection_rank")
        )
        .filter(pl.col("selection_rank") <= config.top_n)
        .with_columns(pl.len().over("ts").alias("selected_count"))
        .join(fee_map, on="symbol", how="left", validate="m:1")
        .sort(["ts", "selection_rank"])
    )

    equity = float(config.initial_equity)
    slippage = config.slippage_bps_per_side / 10_000.0
    curve_columns: dict[str, list[Any]] = {
        name: []
        for name in (
            "decision_ts",
            "date",
            "equity",
            "strategy_return",
            "desired_notional",
            "executed_notional",
            "actual_gross_exposure",
            "selected_names",
            "filled_names",
            "explicit_fees",
            "slippage_cost",
        )
    }
    trade_columns: dict[str, list[Any]] = {
        name: []
        for name in (
            "decision_ts",
            "date",
            "symbol",
            "selection_rank",
            "score",
            "requested_notional",
            "executed_notional",
            "volume_participation",
            "entry_price_proxy",
            "exit_price_proxy",
            "gross_return",
            "net_return",
        )
    }
    if candidates.height:
        timestamp_ns = candidates["ts"].cast(pl.Int64).to_numpy()
        group_starts = np.r_[
            0,
            np.flatnonzero(timestamp_ns[1:] != timestamp_ns[:-1]) + 1,
        ]
        group_ends = np.r_[group_starts[1:], candidates.height]
        dates = candidates["date"].to_numpy()
        symbol_values = candidates["symbol"].to_numpy()
        ranks = candidates["selection_rank"].to_numpy()
        scores = candidates[score_column].to_numpy()
        label_valid = candidates[label_column].to_numpy()
        open_prices = (
            candidates["execution_open_next_1m"]
            .cast(pl.Float64)
            .fill_null(np.nan)
            .to_numpy()
        )
        close_prices = (
            candidates[close_column].cast(pl.Float64).fill_null(np.nan).to_numpy()
        )
        future_volumes = (
            candidates["future_volume_shares_next_1m"]
            .cast(pl.Float64)
            .fill_null(np.nan)
            .to_numpy()
        )
        buy_fees_all = candidates["buy_fee_rate"].to_numpy()
        sell_fees_all = candidates["sell_fee_rate"].to_numpy()
        selected_counts = candidates["selected_count"].to_numpy()
        for start, end in zip(group_starts, group_ends, strict=True):
            equity_before = equity
            selected_count = int(selected_counts[start])
            target_weight = min(
                config.gross_exposure / selected_count,
                config.maximum_name_weight,
            )
            requested = min(
                equity_before * target_weight,
                config.maximum_order_notional,
            )
            desired_notional = requested * selected_count
            scope = slice(start, end)
            opens = open_prices[scope]
            closes = close_prices[scope]
            volumes = future_volumes[scope]
            valid = (
                label_valid[scope]
                & np.isfinite(opens)
                & np.isfinite(closes)
                & np.isfinite(volumes)
                & (opens > 0)
                & (closes > 0)
                & (volumes > 0)
            )
            capacity = volumes * opens * config.maximum_volume_participation
            notionals = np.where(valid, np.minimum(requested, capacity), 0.0)
            filled = notionals > 0
            buy_prices = opens * (1.0 + slippage)
            sell_prices = closes * (1.0 - slippage)
            buy_fees = buy_fees_all[scope]
            sell_fees = sell_fees_all[scope]
            net_returns = (
                sell_prices * (1.0 - sell_fees) / (buy_prices * (1.0 + buy_fees)) - 1.0
            )
            gross_returns = closes / opens - 1.0
            minute_return = float(
                np.sum(
                    np.where(
                        filled,
                        notionals / equity_before * net_returns,
                        0.0,
                    )
                )
            )
            explicit_fees = float(
                np.sum(
                    np.where(
                        filled,
                        notionals * (buy_fees + sell_fees * (closes / opens)),
                        0.0,
                    )
                )
            )
            slippage_cost = float(
                np.sum(
                    np.where(
                        filled,
                        notionals * (slippage + slippage * (closes / opens)),
                        0.0,
                    )
                )
            )
            executed_notional = float(np.sum(notionals))
            equity *= 1.0 + minute_return
            timestamp = int(timestamp_ns[start])
            curve_columns["decision_ts"].append(timestamp)
            curve_columns["date"].append(str(dates[start]))
            curve_columns["equity"].append(equity)
            curve_columns["strategy_return"].append(minute_return)
            curve_columns["desired_notional"].append(desired_notional)
            curve_columns["executed_notional"].append(executed_notional)
            curve_columns["actual_gross_exposure"].append(
                executed_notional / equity_before
            )
            curve_columns["selected_names"].append(selected_count)
            curve_columns["filled_names"].append(int(np.count_nonzero(filled)))
            curve_columns["explicit_fees"].append(explicit_fees)
            curve_columns["slippage_cost"].append(slippage_cost)

            filled_indices = np.flatnonzero(filled)
            for local_index in filled_indices:
                absolute = start + int(local_index)
                notional = float(notionals[local_index])
                trade_columns["decision_ts"].append(timestamp)
                trade_columns["date"].append(str(dates[absolute]))
                trade_columns["symbol"].append(symbol_values[absolute])
                trade_columns["selection_rank"].append(int(ranks[absolute]))
                trade_columns["score"].append(float(scores[absolute]))
                trade_columns["requested_notional"].append(requested)
                trade_columns["executed_notional"].append(notional)
                trade_columns["volume_participation"].append(
                    notional / (volumes[local_index] * opens[local_index])
                )
                trade_columns["entry_price_proxy"].append(
                    float(buy_prices[local_index])
                )
                trade_columns["exit_price_proxy"].append(
                    float(sell_prices[local_index])
                )
                trade_columns["gross_return"].append(float(gross_returns[local_index]))
                trade_columns["net_return"].append(float(net_returns[local_index]))

    curve = (
        pl.DataFrame(
            curve_columns,
            schema_overrides={"decision_ts": pl.Int64, "date": pl.String},
            strict=False,
        ).with_columns(
            pl.col("decision_ts").cast(pl.Datetime("ns")),
            pl.col("date").str.to_date(),
        )
        if curve_columns["decision_ts"]
        else pl.DataFrame()
    )
    trades = (
        pl.DataFrame(
            trade_columns,
            schema_overrides={"decision_ts": pl.Int64, "date": pl.String},
            strict=False,
        ).with_columns(
            pl.col("decision_ts").cast(pl.Datetime("ns")),
            pl.col("date").str.to_date(),
        )
        if trade_columns["decision_ts"]
        else pl.DataFrame()
    )
    equity_values = (
        curve["equity"].to_numpy()
        if curve.height
        else np.asarray([config.initial_equity])
    )
    daily = (
        curve.group_by("date", maintain_order=True).agg(
            pl.col("equity").last().alias("ending_equity")
        )
        if curve.height
        else pl.DataFrame(schema={"date": pl.Date, "ending_equity": pl.Float64})
    )
    if daily.height:
        daily = daily.with_columns(
            (
                pl.col("ending_equity")
                / pl.col("ending_equity").shift(1).fill_null(config.initial_equity)
                - 1.0
            ).alias("daily_return")
        )
        daily_returns = daily["daily_return"].to_numpy()
    else:
        daily_returns = np.asarray([], dtype=np.float64)
    daily_std = float(np.std(daily_returns, ddof=1)) if daily_returns.size > 1 else 0.0
    sharpe = (
        float(np.mean(daily_returns) / daily_std * math.sqrt(252.0))
        if daily_std > 0
        else None
    )
    unique_dates = frame["date"].n_unique()
    research_ready = (
        unique_dates >= config.minimum_research_days
        and len(symbols) >= config.minimum_research_symbols
    )
    summary: dict[str, Any] = {
        "schema_version": 1,
        "execution_mode": "tw_minute_kbar_research",
        "score_column": score_column,
        "holding_mode": config.holding_mode,
        "timing_contract": (
            "completed_bar_t -> open_t_plus_1 -> session_close"
            if config.holding_mode == "session_close"
            else "completed_bar_t -> open_t_plus_1 -> close_t_plus_1"
        ),
        "symbols": len(symbols),
        "dates": unique_dates,
        "research_ready": research_ready,
        "minimum_research_days": config.minimum_research_days,
        "minimum_research_symbols": config.minimum_research_symbols,
        "initial_equity": config.initial_equity,
        "final_equity": float(equity_values[-1]),
        "total_return": float(equity_values[-1] / config.initial_equity - 1.0),
        "maximum_drawdown": _maximum_drawdown(equity_values),
        "daily_sharpe": sharpe,
        "positive_day_rate": (
            float(np.mean(daily_returns > 0)) if daily_returns.size else None
        ),
        "trades": trades.height,
        "total_executed_notional": (
            float(curve["executed_notional"].sum()) if curve.height else 0.0
        ),
        "total_explicit_fees": (
            float(curve["explicit_fees"].sum()) if curve.height else 0.0
        ),
        "total_slippage_cost": (
            float(curve["slippage_cost"].sum()) if curve.height else 0.0
        ),
        "mean_actual_gross_exposure": (
            float(curve["actual_gross_exposure"].mean()) if curve.height else 0.0
        ),
        "configuration": {
            key: value for key, value in asdict(config).items() if key != "fee_schedule"
        },
        "fee_schedule": asdict(config.fee_schedule),
        "limitations": [
            "one-minute OHLC has no bid/ask, queue position, or partial-fill path",
            "next-bar open/close plus configured slippage are execution proxies",
            "future bar volume is used only to cap a preselected order and never to replace it",
            "long-only until point-in-time sell-first eligibility and borrow inventory are joined",
        ],
    }
    return MinuteKbarBacktestResult(summary=summary, equity_curve=curve, trades=trades)


class MinuteRebalanceBacktester:
    """Stateful long-only portfolio that receives a decision every minute."""

    def __init__(
        self,
        *,
        score_column: str = "score_blend",
        config: MinuteKbarBacktestConfig | None = None,
    ) -> None:
        self.config = config or MinuteKbarBacktestConfig()
        if self.config.holding_mode != "minute_rebalance":
            raise ValueError(
                "MinuteRebalanceBacktester requires holding_mode=minute_rebalance"
            )
        self.score_column = str(score_column)
        self.initial_equity = float(self.config.initial_equity)
        self.equity = self.initial_equity
        self._last_date: date | None = None
        self._dates: list[date] = []
        self._daily_equity: list[float] = []
        self._symbols: set[str] = set()
        self._curve: dict[str, list[Any]] = {
            name: []
            for name in (
                "decision_ts",
                "date",
                "event_type",
                "equity",
                "strategy_return",
                "desired_notional",
                "executed_notional",
                "turnover",
                "actual_gross_exposure",
                "selected_names",
                "filled_names",
                "explicit_fees",
                "slippage_cost",
                "forced_exit_over_capacity_notional",
                "stale_forced_exit_notional",
            )
        }
        self._trades: dict[str, list[Any]] = {
            name: []
            for name in (
                "decision_ts",
                "date",
                "symbol",
                "side",
                "reason",
                "selection_rank",
                "score",
                "requested_notional",
                "executed_notional",
                "volume_participation",
                "reference_price",
                "execution_price_proxy",
                "explicit_fee",
                "slippage_cost",
            )
        }

    def process_frame(self, frame: pl.DataFrame) -> None:
        """Process one or more complete trading dates in chronological order."""

        if frame.is_empty():
            return
        for day in frame.sort(["date", "ts", "symbol"]).partition_by(
            "date", maintain_order=True
        ):
            self._process_day(day, validate_keys=True)

    def process_day(
        self,
        frame: pl.DataFrame,
        *,
        validate_keys: bool = True,
    ) -> None:
        """Fast path for one already isolated trading-date partition."""

        if frame.is_empty():
            return
        self._process_day(frame, validate_keys=validate_keys)

    def _append_curve(
        self,
        *,
        timestamp_ns: int,
        trade_date: date,
        event_type: str,
        previous_equity: float,
        desired_notional: float,
        executed_notional: float,
        gross_notional: float,
        selected_names: int,
        filled_names: int,
        explicit_fees: float,
        slippage_cost: float,
        forced_exit_over_capacity_notional: float = 0.0,
        stale_forced_exit_notional: float = 0.0,
    ) -> None:
        self._curve["decision_ts"].append(timestamp_ns)
        self._curve["date"].append(trade_date.isoformat())
        self._curve["event_type"].append(event_type)
        self._curve["equity"].append(self.equity)
        self._curve["strategy_return"].append(
            self.equity / previous_equity - 1.0 if previous_equity > 0 else -1.0
        )
        self._curve["desired_notional"].append(desired_notional)
        self._curve["executed_notional"].append(executed_notional)
        self._curve["turnover"].append(
            executed_notional / previous_equity if previous_equity > 0 else 0.0
        )
        self._curve["actual_gross_exposure"].append(
            gross_notional / self.equity if self.equity > 0 else 0.0
        )
        self._curve["selected_names"].append(selected_names)
        self._curve["filled_names"].append(filled_names)
        self._curve["explicit_fees"].append(explicit_fees)
        self._curve["slippage_cost"].append(slippage_cost)
        self._curve["forced_exit_over_capacity_notional"].append(
            forced_exit_over_capacity_notional
        )
        self._curve["stale_forced_exit_notional"].append(stale_forced_exit_notional)

    def _append_trade(
        self,
        *,
        timestamp_ns: int,
        trade_date: date,
        symbol: str,
        side: str,
        reason: str,
        selection_rank: int,
        score: float,
        requested_notional: float,
        executed_notional: float,
        volume_participation: float,
        reference_price: float,
        execution_price_proxy: float,
        explicit_fee: float,
        slippage_cost: float,
    ) -> None:
        self._trades["decision_ts"].append(timestamp_ns)
        self._trades["date"].append(trade_date.isoformat())
        self._trades["symbol"].append(symbol)
        self._trades["side"].append(side)
        self._trades["reason"].append(reason)
        self._trades["selection_rank"].append(selection_rank)
        self._trades["score"].append(score)
        self._trades["requested_notional"].append(requested_notional)
        self._trades["executed_notional"].append(executed_notional)
        self._trades["volume_participation"].append(volume_participation)
        self._trades["reference_price"].append(reference_price)
        self._trades["execution_price_proxy"].append(execution_price_proxy)
        self._trades["explicit_fee"].append(explicit_fee)
        self._trades["slippage_cost"].append(slippage_cost)

    def _process_day(
        self,
        frame: pl.DataFrame,
        *,
        validate_keys: bool,
    ) -> None:
        config = self.config
        frame = frame.with_columns(
            pl.col("ts").cast(pl.Datetime("ns")),
            pl.col("date").cast(pl.Date),
        )
        required = {
            "date",
            "ts",
            "symbol",
            "minutes_from_open",
            "feature_valid",
            "label_valid_1m",
            "execution_open_next_1m",
            "exit_close_next_1m",
            "future_volume_shares_next_1m",
            "session_close",
            self.score_column,
        }
        missing = required - set(frame.columns)
        if missing:
            raise ValueError(f"minute rebalance inputs are missing: {sorted(missing)}")
        if validate_keys:
            duplicates = (
                frame.group_by("ts", "symbol").len().filter(pl.col("len") > 1).height
            )
            if duplicates:
                raise ValueError(
                    "minute rebalance requires unique timestamp/symbol rows"
                )
        dates = frame["date"].unique().to_list()
        if len(dates) != 1:
            raise ValueError("minute rebalance day frame must contain one date")
        trade_date = dates[0]
        if self._last_date is not None and trade_date <= self._last_date:
            raise ValueError("minute rebalance dates must be strictly increasing")
        self._last_date = trade_date
        self._dates.append(trade_date)

        symbols = sorted(str(value) for value in frame["symbol"].unique().to_list())
        self._symbols.update(symbols)
        symbol_to_index = {symbol: index for index, symbol in enumerate(symbols)}
        buy_rates, sell_rates = effective_fee_rate_vectors(
            symbols,
            "tw_day_trade",
            fee_schedule=config.fee_schedule,
        )
        shares = np.zeros(len(symbols), dtype=np.float64)
        last_prices = np.full(len(symbols), np.nan, dtype=np.float64)
        cash = float(self.equity)
        slippage = config.slippage_bps_per_side / 10_000.0

        session_rows = (
            frame.sort(["symbol", "ts"])
            .group_by("symbol", maintain_order=True)
            .agg(
                pl.col("session_close").last().alias("session_close"),
                pl.col("ts").max().alias("session_last_ts"),
            )
            .with_columns(
                pl.col("symbol")
                .replace_strict(symbol_to_index, return_dtype=pl.Int32)
                .alias("symbol_index")
            )
            .sort("symbol_index")
        )
        session_close = (
            session_rows["session_close"].cast(pl.Float64).fill_null(np.nan).to_numpy()
        )
        session_last_ns = session_rows["session_last_ts"].cast(pl.Int64).to_numpy()
        force_capacity_shares = np.zeros(len(symbols), dtype=np.float64)

        decision_filter = pl.col("minutes_from_open").is_between(
            config.first_decision_minute,
            config.last_decision_minute,
            closed="both",
        )
        working = frame.filter(decision_filter).sort(["ts", "symbol"])
        if working.is_empty():
            self._daily_equity.append(self.equity)
            return

        timestamp_ns = working["ts"].cast(pl.Int64).to_numpy()
        starts = np.r_[0, np.flatnonzero(timestamp_ns[1:] != timestamp_ns[:-1]) + 1]
        ends = np.r_[starts[1:], working.height]
        symbol_values = working["symbol"].to_numpy()
        symbol_indices = np.searchsorted(
            np.asarray(symbols, dtype=object),
            symbol_values,
        )
        feature_valid = working["feature_valid"].to_numpy()
        scores = (
            working[self.score_column].cast(pl.Float64).fill_null(np.nan).to_numpy()
        )
        valid_labels = working["label_valid_1m"].to_numpy()
        opens = (
            working["execution_open_next_1m"]
            .cast(pl.Float64)
            .fill_null(np.nan)
            .to_numpy()
        )
        closes = (
            working["exit_close_next_1m"].cast(pl.Float64).fill_null(np.nan).to_numpy()
        )
        volumes = (
            working["future_volume_shares_next_1m"]
            .cast(pl.Float64)
            .fill_null(np.nan)
            .to_numpy()
        )
        minutes = working["minutes_from_open"].to_numpy()

        for start, end in zip(starts, ends, strict=True):
            scope = slice(start, end)
            indices = symbol_indices[scope]
            event_opens = opens[scope]
            event_closes = closes[scope]
            event_volumes = volumes[scope]
            valid = (
                valid_labels[scope]
                & np.isfinite(event_opens)
                & np.isfinite(event_closes)
                & np.isfinite(event_volumes)
                & (event_opens > 0)
                & (event_closes > 0)
                & (event_volumes > 0)
            )
            valid_indices = indices[valid]
            last_prices[valid_indices] = event_opens[valid]
            held_at_open = shares > 0
            equity_open = cash + float(
                np.dot(shares[held_at_open], last_prices[held_at_open])
            )
            previous_equity = float(self.equity)
            if equity_open <= 0:
                raise RuntimeError("minute rebalance equity became nonpositive")

            event_scores = scores[scope]
            eligible_locations = np.flatnonzero(
                feature_valid[scope]
                & np.isfinite(event_scores)
                & (event_scores > config.minimum_score)
            )
            if eligible_locations.size:
                order = np.argsort(
                    -event_scores[eligible_locations],
                    kind="stable",
                )
                ordered_locations = eligible_locations[order]
            else:
                ordered_locations = np.asarray([], dtype=np.int64)
            current_shares = shares[indices]
            retention_rank = max(
                config.top_n,
                int(math.ceil(config.top_n * config.selection_hysteresis_multiplier)),
            )
            retention_candidates = ordered_locations[:retention_rank]
            retained_locations = retention_candidates[
                current_shares[retention_candidates] > 0
            ][: config.top_n]
            retained_set = set(int(value) for value in retained_locations)
            fill_locations = [
                int(value)
                for value in ordered_locations
                if int(value) not in retained_set
            ][: config.top_n - len(retained_locations)]
            selected_locations = np.asarray(
                [*retained_locations.tolist(), *fill_locations],
                dtype=np.int64,
            )
            selected_local = np.zeros(end - start, dtype=bool)
            selected_local[selected_locations] = True
            ranks_local = np.zeros(end - start, dtype=np.int32)
            ranks_local[ordered_locations] = np.arange(
                1,
                len(ordered_locations) + 1,
                dtype=np.int32,
            )
            selected_count = len(selected_locations)
            target_notional = (
                min(
                    equity_open * config.gross_exposure / selected_count,
                    equity_open * config.maximum_name_weight,
                    config.maximum_order_notional,
                )
                if selected_count > 0
                else 0.0
            )
            desired_notional = target_notional * selected_count
            safe_opens = np.where(valid, event_opens, 0.0)
            target_shares = np.zeros(end - start, dtype=np.float64)
            selected_and_valid = selected_local & valid
            target_shares[selected_and_valid] = (
                target_notional / safe_opens[selected_and_valid]
            )
            delta = target_shares - current_shares
            capacity_shares = np.where(
                valid,
                event_volumes * config.maximum_volume_participation,
                0.0,
            )
            event_buy_rates = buy_rates[indices]
            event_sell_rates = sell_rates[indices]

            sell_requested = np.maximum(-delta, 0.0)
            sell_quantities = np.minimum(sell_requested, capacity_shares)
            sell_quantities = np.minimum(sell_quantities, current_shares)
            sell_reference = safe_opens
            sell_execution = sell_reference * (1.0 - slippage)
            sell_fees = sell_quantities * sell_execution * event_sell_rates
            sell_slippage = sell_quantities * sell_reference * slippage
            cash += float(np.sum(sell_quantities * sell_execution - sell_fees))
            shares[indices] -= sell_quantities

            buy_requested = np.maximum(delta, 0.0)
            buy_quantities = np.minimum(buy_requested, capacity_shares)
            buy_reference = safe_opens
            buy_execution = buy_reference * (1.0 + slippage)
            buy_unit_cost = buy_execution * (1.0 + event_buy_rates)
            total_buy_cost = float(np.sum(buy_quantities * buy_unit_cost))
            buy_scale = (
                min(1.0, max(cash, 0.0) / total_buy_cost) if total_buy_cost > 0 else 0.0
            )
            buy_quantities *= buy_scale
            buy_fees = buy_quantities * buy_execution * event_buy_rates
            buy_slippage = buy_quantities * buy_reference * slippage
            cash -= float(np.sum(buy_quantities * buy_execution + buy_fees))
            if -1e-7 < cash < 0:
                cash = 0.0
            shares[indices] += buy_quantities

            executed_notional = float(
                np.sum(
                    sell_quantities * sell_reference + buy_quantities * buy_reference
                )
            )
            explicit_fees = float(np.sum(sell_fees) + np.sum(buy_fees))
            slippage_cost = float(np.sum(sell_slippage) + np.sum(buy_slippage))
            filled_mask = (sell_quantities > 0) | (buy_quantities > 0)

            for local in np.flatnonzero(sell_quantities > 0):
                absolute = start + int(local)
                quantity = float(sell_quantities[local])
                volume = float(event_volumes[local])
                self._append_trade(
                    timestamp_ns=int(timestamp_ns[start]),
                    trade_date=trade_date,
                    symbol=str(symbol_values[absolute]),
                    side="sell",
                    reason="minute_rebalance",
                    selection_rank=int(ranks_local[local]),
                    score=float(scores[absolute]),
                    requested_notional=float(
                        sell_requested[local] * sell_reference[local]
                    ),
                    executed_notional=float(quantity * sell_reference[local]),
                    volume_participation=quantity / volume,
                    reference_price=float(sell_reference[local]),
                    execution_price_proxy=float(sell_execution[local]),
                    explicit_fee=float(sell_fees[local]),
                    slippage_cost=float(sell_slippage[local]),
                )
            for local in np.flatnonzero(buy_quantities > 0):
                absolute = start + int(local)
                quantity = float(buy_quantities[local])
                volume = float(event_volumes[local])
                self._append_trade(
                    timestamp_ns=int(timestamp_ns[start]),
                    trade_date=trade_date,
                    symbol=str(symbol_values[absolute]),
                    side="buy",
                    reason="minute_rebalance",
                    selection_rank=int(ranks_local[local]),
                    score=float(scores[absolute]),
                    requested_notional=float(
                        buy_requested[local] * buy_reference[local]
                    ),
                    executed_notional=float(quantity * buy_reference[local]),
                    volume_participation=quantity / volume,
                    reference_price=float(buy_reference[local]),
                    execution_price_proxy=float(buy_execution[local]),
                    explicit_fee=float(buy_fees[local]),
                    slippage_cost=float(buy_slippage[local]),
                )

            last_prices[valid_indices] = event_closes[valid]
            if int(minutes[start]) == config.last_decision_minute:
                force_capacity_shares[indices] = capacity_shares
            held_at_close = shares > 0
            gross_notional = float(
                np.dot(shares[held_at_close], last_prices[held_at_close])
            )
            self.equity = cash + gross_notional
            self._append_curve(
                timestamp_ns=int(timestamp_ns[start]),
                trade_date=trade_date,
                event_type="minute_decision",
                previous_equity=previous_equity,
                desired_notional=desired_notional,
                executed_notional=executed_notional,
                gross_notional=gross_notional,
                selected_names=selected_count,
                filled_names=int(np.count_nonzero(filled_mask)),
                explicit_fees=explicit_fees,
                slippage_cost=slippage_cost,
            )

        held = shares > 0
        if np.any(held):
            if np.any(~np.isfinite(session_close[held]) | (session_close[held] <= 0)):
                missing_symbols = [
                    symbols[index]
                    for index in np.flatnonzero(
                        held & (~np.isfinite(session_close) | (session_close <= 0))
                    )
                ]
                raise RuntimeError(
                    "minute rebalance cannot force-close symbols without a "
                    f"session price: {missing_symbols[:10]}"
                )
            previous_equity = float(self.equity)
            held_indices = np.flatnonzero(held)
            held_shares = shares[held_indices]
            reference = session_close[held_indices]
            execution = reference * (1.0 - slippage)
            fees = held_shares * execution * sell_rates[held_indices]
            slippage_values = held_shares * reference * slippage
            executed_notional = float(np.sum(held_shares * reference))
            capacity = force_capacity_shares[held_indices]
            over_capacity = float(
                np.sum(np.maximum(held_shares - capacity, 0.0) * reference)
            )
            close_minute_ns = 13 * 60 + 30
            session_minutes = (session_last_ns[held_indices] // 60_000_000_000) % (
                24 * 60
            )
            stale_mask = session_minutes != close_minute_ns
            stale_notional = float(
                np.sum(held_shares[stale_mask] * reference[stale_mask])
            )
            cash += float(np.sum(held_shares * execution - fees))
            for local, symbol_index in enumerate(held_indices):
                self._append_trade(
                    timestamp_ns=int(np.max(session_last_ns)),
                    trade_date=trade_date,
                    symbol=symbols[int(symbol_index)],
                    side="sell",
                    reason="forced_session_close",
                    selection_rank=0,
                    score=float("nan"),
                    requested_notional=float(held_shares[local] * reference[local]),
                    executed_notional=float(held_shares[local] * reference[local]),
                    volume_participation=(
                        float(held_shares[local] / capacity[local])
                        if capacity[local] > 0
                        else float("inf")
                    ),
                    reference_price=float(reference[local]),
                    execution_price_proxy=float(execution[local]),
                    explicit_fee=float(fees[local]),
                    slippage_cost=float(slippage_values[local]),
                )
            shares.fill(0.0)
            self.equity = cash
            self._append_curve(
                timestamp_ns=int(np.max(session_last_ns)),
                trade_date=trade_date,
                event_type="forced_session_close",
                previous_equity=previous_equity,
                desired_notional=0.0,
                executed_notional=executed_notional,
                gross_notional=0.0,
                selected_names=0,
                filled_names=len(held_indices),
                explicit_fees=float(np.sum(fees)),
                slippage_cost=float(np.sum(slippage_values)),
                forced_exit_over_capacity_notional=over_capacity,
                stale_forced_exit_notional=stale_notional,
            )

        self._daily_equity.append(float(self.equity))

    def finalize(self) -> MinuteKbarBacktestResult:
        curve = (
            pl.DataFrame(
                self._curve,
                schema_overrides={
                    "decision_ts": pl.Int64,
                    "date": pl.String,
                },
                strict=False,
            ).with_columns(
                pl.col("decision_ts").cast(pl.Datetime("ns")),
                pl.col("date").str.to_date(),
            )
            if self._curve["decision_ts"]
            else pl.DataFrame()
        )
        trades = (
            pl.DataFrame(
                self._trades,
                schema_overrides={
                    "decision_ts": pl.Int64,
                    "date": pl.String,
                },
                strict=False,
            ).with_columns(
                pl.col("decision_ts").cast(pl.Datetime("ns")),
                pl.col("date").str.to_date(),
            )
            if self._trades["decision_ts"]
            else pl.DataFrame()
        )
        daily_equity = np.asarray(self._daily_equity, dtype=np.float64)
        if daily_equity.size:
            prior = np.r_[self.initial_equity, daily_equity[:-1]]
            daily_returns = daily_equity / prior - 1.0
        else:
            daily_returns = np.asarray([], dtype=np.float64)
        daily_std = (
            float(np.std(daily_returns, ddof=1)) if daily_returns.size > 1 else 0.0
        )
        sharpe = (
            float(np.mean(daily_returns) / daily_std * math.sqrt(252.0))
            if daily_std > 0
            else None
        )
        equity_values = (
            curve["equity"].to_numpy()
            if curve.height
            else np.asarray([self.initial_equity])
        )
        research_ready = (
            len(self._dates) >= self.config.minimum_research_days
            and len(self._symbols) >= self.config.minimum_research_symbols
        )
        summary: dict[str, Any] = {
            "schema_version": 2,
            "execution_mode": "tw_minute_kbar_research",
            "score_column": self.score_column,
            "holding_mode": "minute_rebalance",
            "timing_contract": (
                "every completed right-labelled bar t -> decide target -> "
                "execute at next bar open -> carry state -> force flat by close"
            ),
            "symbols": len(self._symbols),
            "dates": len(self._dates),
            "decisions": (
                int(curve.filter(pl.col("event_type") == "minute_decision").height)
                if curve.height
                else 0
            ),
            "research_ready": research_ready,
            "minimum_research_days": self.config.minimum_research_days,
            "minimum_research_symbols": self.config.minimum_research_symbols,
            "initial_equity": self.initial_equity,
            "final_equity": float(self.equity),
            "total_return": float(self.equity / self.initial_equity - 1.0),
            "maximum_drawdown": _maximum_drawdown(equity_values),
            "daily_sharpe": sharpe,
            "positive_day_rate": (
                float(np.mean(daily_returns > 0)) if daily_returns.size else None
            ),
            "trades": trades.height,
            "total_executed_notional": (
                float(curve["executed_notional"].sum()) if curve.height else 0.0
            ),
            "total_explicit_fees": (
                float(curve["explicit_fees"].sum()) if curve.height else 0.0
            ),
            "total_slippage_cost": (
                float(curve["slippage_cost"].sum()) if curve.height else 0.0
            ),
            "mean_actual_gross_exposure": (
                float(curve["actual_gross_exposure"].mean()) if curve.height else 0.0
            ),
            "forced_exit_over_capacity_notional": (
                float(curve["forced_exit_over_capacity_notional"].sum())
                if curve.height
                else 0.0
            ),
            "stale_forced_exit_notional": (
                float(curve["stale_forced_exit_notional"].sum())
                if curve.height
                else 0.0
            ),
            "configuration": {
                key: value
                for key, value in asdict(self.config).items()
                if key != "fee_schedule"
            },
            "fee_schedule": asdict(self.config.fee_schedule),
            "limitations": [
                "one-minute OHLC has no bid/ask, queue position, or partial-fill path",
                "every decision uses completed bars and executes only on the next bar",
                "future volume caps a preselected rebalance and never changes ranking",
                "forced close above observed capacity is reported explicitly",
                "long-only until point-in-time sell-first eligibility and borrow inventory are joined",
            ],
        }
        return MinuteKbarBacktestResult(
            summary=summary,
            equity_curve=curve,
            trades=trades,
        )


def run_minute_rebalance_backtest(
    frame: pl.DataFrame,
    *,
    score_column: str = "score_blend",
    config: MinuteKbarBacktestConfig | None = None,
) -> MinuteKbarBacktestResult:
    backtester = MinuteRebalanceBacktester(
        score_column=score_column,
        config=config,
    )
    backtester.process_frame(frame)
    return backtester.finalize()

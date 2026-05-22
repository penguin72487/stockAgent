from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import torch


INT64_MIN_FLOAT_SAFE = np.nextafter(float(np.iinfo(np.int64).min), 0.0)
INT64_MAX_FLOAT_SAFE = np.nextafter(float(np.iinfo(np.int64).max + 1), 0.0)
CASH_SYMBOL_NAMES = {"CASH", "現金"}


def _clip_to_int64_storage_bounds(values: np.ndarray | float, *, non_negative: bool = False) -> np.ndarray:
    """Clip numeric values to safe float bounds that can be cast to int64."""
    arr = np.nan_to_num(
        np.asarray(values, dtype=np.float64),
        nan=0.0,
        posinf=INT64_MAX_FLOAT_SAFE,
        neginf=0.0 if non_negative else INT64_MIN_FLOAT_SAFE,
    )
    lower = 0.0 if non_negative else INT64_MIN_FLOAT_SAFE
    return np.clip(arr, lower, INT64_MAX_FLOAT_SAFE)


def _floor_to_int64(values: np.ndarray | float, *, non_negative: bool = False) -> np.ndarray:
    """Floor and cast to int64 after clipping strictly to int64 storage bounds."""
    clipped = _clip_to_int64_storage_bounds(values, non_negative=non_negative)
    return np.floor(clipped).astype(np.int64)


def _build_cash_symbol_mask(symbols: list[str]) -> np.ndarray:
    """Return a boolean mask for symbols that represent cash and must not be traded."""
    normalized = [str(symbol).strip().upper() for symbol in symbols]
    return np.array([name in CASH_SYMBOL_NAMES for name in normalized], dtype=bool)


def _fee_with_floor(notional: float, rate: float, minimum_fee: float) -> float:
    if notional <= 0.0:
        return 0.0
    return float(max(notional * rate, minimum_fee))


@dataclass(slots=True)
class BacktestResult:
    """Container for a single backtest simulation run."""

    strategy_returns: np.ndarray   # [T] net daily returns after costs
    benchmark_returns: np.ndarray  # [T] universe-average daily returns
    turnovers: np.ndarray          # [T] total absolute weight change per day
    weights_history: np.ndarray    # [T, S] realised portfolio weights


@dataclass(slots=True)
class BacktestResultTensor:
    """Torch tensor container for a single backtest simulation run."""

    strategy_returns: torch.Tensor   # [T]
    benchmark_returns: torch.Tensor  # [T]
    turnovers: torch.Tensor          # [T]
    weights_history: torch.Tensor    # [T, S]

    def to_numpy(self) -> BacktestResult:
        return BacktestResult(
            strategy_returns=self.strategy_returns.detach().cpu().numpy().astype(np.float32),
            benchmark_returns=self.benchmark_returns.detach().cpu().numpy().astype(np.float32),
            turnovers=self.turnovers.detach().cpu().numpy().astype(np.float32),
            weights_history=self.weights_history.detach().cpu().numpy().astype(np.float32),
        )


@dataclass(slots=True)
class HoldingsRecord:
    """Single holding record for one date/symbol, sorted by holding ratio."""

    date: str
    symbol: str
    shares: int
    price: float
    market_value: float
    holding_ratio: float
    is_cash: bool
    traded_notional: float = 0.0
    buy_fee: float = 0.0
    sell_fee: float = 0.0


def _vectorized_backtest(
    weights: np.ndarray,
    future_returns: np.ndarray,
    tradable_mask: np.ndarray,
    fee_per_side: float,
    buy_fee_rate: float | None = None,
    sell_fee_rate: float | None = None,
    cash_symbol_mask: np.ndarray | None = None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    if buy_fee_rate is None:
        buy_fee_rate = fee_per_side
    if sell_fee_rate is None:
        sell_fee_rate = fee_per_side

    weights_history = np.asarray(weights, dtype=np.float32).copy()
    weights_history[~tradable_mask.astype(bool)] = 0.0

    if cash_symbol_mask is None:
        cash_mask = np.zeros(weights_history.shape[1], dtype=bool)
    else:
        cash_mask = np.asarray(cash_symbol_mask, dtype=bool)
        if cash_mask.shape != (weights_history.shape[1],):
            raise ValueError(
                "cash_symbol_mask shape must match num_symbols: "
                f"expected {(weights_history.shape[1],)}, got {cash_mask.shape}"
            )

    weight_sums = np.abs(weights_history).sum(axis=1, keepdims=True)
    nonzero = weight_sums.squeeze(1) > 0
    weights_history[nonzero] /= weight_sums[nonzero]

    prev = np.concatenate([
        np.zeros((1, weights_history.shape[1]), dtype=np.float32),
        weights_history[:-1],
    ], axis=0)
    delta = weights_history - prev
    buy_turnover = np.maximum(delta, 0.0)
    sell_turnover = np.maximum(-delta, 0.0)
    if np.any(cash_mask):
        buy_turnover[:, cash_mask] = 0.0
        sell_turnover[:, cash_mask] = 0.0
    turnovers = (buy_turnover + sell_turnover).sum(axis=1).astype(np.float32)

    gross = np.einsum("ts,ts->t", weights_history, future_returns, dtype=np.float32)
    strategy_returns = gross - buy_fee_rate * buy_turnover.sum(axis=1) - sell_fee_rate * sell_turnover.sum(axis=1)
    return strategy_returns.astype(np.float32), turnovers, weights_history


def _vectorized_backtest_torch(
    weights: torch.Tensor,
    future_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    fee_per_side: float,
    buy_fee_rate: float | None = None,
    sell_fee_rate: float | None = None,
    cash_symbol_mask: torch.Tensor | None = None,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    if buy_fee_rate is None:
        buy_fee_rate = fee_per_side
    if sell_fee_rate is None:
        sell_fee_rate = fee_per_side

    weights_history = weights.float().clone()
    
    # Step 1: Mask non-tradable symbols
    weights_history = weights_history.masked_fill(~tradable_mask.bool(), 0.0)
    
    # Step 2: Normalize by gross exposure so long/short portfolios remain comparable.
    weight_sums = weights_history.abs().sum(dim=1, keepdim=True).clamp_min(1e-12)
    weights_history = weights_history / weight_sums
    
    # Step 3: Compute turnover
    prev = torch.cat(
        [torch.zeros_like(weights_history[:1]), weights_history[:-1]],
        dim=0,
    )
    delta = weights_history - prev
    buy_turnover = torch.relu(delta)
    sell_turnover = torch.relu(-delta)
    if cash_symbol_mask is not None:
        cash_mask = cash_symbol_mask.to(device=delta.device, dtype=torch.bool)
        if cash_mask.ndim != 1 or cash_mask.numel() != delta.size(1):
            raise ValueError(
                "cash_symbol_mask shape must match num_symbols: "
                f"expected ({delta.size(1)},), got {tuple(cash_mask.shape)}"
            )
        if bool(cash_mask.any().item()):
            buy_turnover = buy_turnover.masked_fill(cash_mask.unsqueeze(0), 0.0)
            sell_turnover = sell_turnover.masked_fill(cash_mask.unsqueeze(0), 0.0)
    turnovers = (buy_turnover + sell_turnover).sum(dim=1)

    # Step 4: Compute strategy returns
    gross = (weights_history * future_returns.float()).sum(dim=1)
    strategy_returns = gross - buy_fee_rate * buy_turnover.sum(dim=1) - sell_fee_rate * sell_turnover.sum(dim=1)
    return strategy_returns.float(), turnovers.float(), weights_history.float()


def run_backtest(
    weights: np.ndarray,
    future_returns: np.ndarray,
    tradable_mask: np.ndarray,
    benchmark_returns: np.ndarray,
    fee_per_side: float,
    buy_fee_rate: float | None = None,
    sell_fee_rate: float | None = None,
    cash_symbol_mask: np.ndarray | None = None,
) -> BacktestResult:
    """Simulate daily portfolio execution from model weights."""
    strategy_returns, turnovers, weights_history = _vectorized_backtest(
        weights,
        future_returns,
        tradable_mask,
        fee_per_side,
        buy_fee_rate,
        sell_fee_rate,
        cash_symbol_mask,
    )

    return BacktestResult(
        strategy_returns=strategy_returns,
        benchmark_returns=benchmark_returns.astype(np.float32),
        turnovers=turnovers,
        weights_history=weights_history,
    )


def run_backtest_torch(
    weights: torch.Tensor,
    future_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    benchmark_returns: torch.Tensor,
    fee_per_side: float,
    buy_fee_rate: float | None = None,
    sell_fee_rate: float | None = None,
    cash_symbol_mask: torch.Tensor | None = None,
) -> BacktestResultTensor:
    """Simulate daily portfolio execution from model weights in torch."""
    strategy_returns, turnovers, weights_history = _vectorized_backtest_torch(
        weights,
        future_returns,
        tradable_mask,
        fee_per_side,
        buy_fee_rate,
        sell_fee_rate,
        cash_symbol_mask,
    )

    return BacktestResultTensor(
        strategy_returns=strategy_returns,
        benchmark_returns=benchmark_returns.float(),
        turnovers=turnovers,
        weights_history=weights_history,
    )


def run_backtest_integer_shares(
    weights: np.ndarray,
    future_returns: np.ndarray,
    tradable_mask: np.ndarray,
    benchmark_returns: np.ndarray,
    *,
    initial_capital: float = 1_000_000.0,
    buy_fee_rate: float | None = None,
    sell_fee_rate: float | None = None,
    lot_size: int | None = None,
    backtest_rule: str = "day_trade",
    min_fee_per_side: float | None = None,
    open_prices: np.ndarray | None = None,
    close_prices: np.ndarray | None = None,
    symbols: list[str] | None = None,
    dates: np.ndarray | None = None,
) -> tuple[BacktestResult, list[HoldingsRecord]]:
    """Run a rule-based integer-share backtest.

    Supported rules:
    - day_trade: buy at open, sell at close, whole lots only.
    - basic: buy/sell at close with immediate settlement, odd lots allowed.
    - overnight: buy/sell at close with T+2 settlement, odd lots allowed.
    """

    @dataclass(slots=True)
    class _RuleSpec:
        rule: str
        lot_size: int
        buy_fee_rate: float
        sell_fee_rate: float
        min_fee_per_side: float
        buy_settlement_lag_days: int
        sell_settlement_lag_days: int

    def _normalize_rule_name(rule_name: str) -> str:
        return str(rule_name).strip().lower().replace("-", "_")

    def _resolve_rule_spec() -> _RuleSpec:
        rule_name = _normalize_rule_name(backtest_rule)
        defaults = {
            "day_trade": _RuleSpec("day_trade", 1000, 0.001425, 0.002925, 20.0, 0, 0),
            "basic": _RuleSpec("basic", 1, 0.001425, 0.004425, 20.0, 0, 0),
            "overnight": _RuleSpec("overnight", 1, 0.001425, 0.004425, 20.0, 2, 2),
        }
        if rule_name not in defaults:
            raise ValueError(
                "Unknown backtest_rule. Expected one of day_trade, basic, overnight; "
                f"got {backtest_rule!r}"
            )
        preset = defaults[rule_name]
        return _RuleSpec(
            rule=rule_name,
            lot_size=int(lot_size if lot_size is not None else preset.lot_size),
            buy_fee_rate=float(buy_fee_rate if buy_fee_rate is not None else preset.buy_fee_rate),
            sell_fee_rate=float(sell_fee_rate if sell_fee_rate is not None else preset.sell_fee_rate),
            min_fee_per_side=float(min_fee_per_side if min_fee_per_side is not None else preset.min_fee_per_side),
            buy_settlement_lag_days=preset.buy_settlement_lag_days,
            sell_settlement_lag_days=preset.sell_settlement_lag_days,
        )

    spec = _resolve_rule_spec()

    w = np.asarray(weights, dtype=np.float64)
    _ = np.asarray(future_returns, dtype=np.float64)  # Kept for API compatibility.
    m = np.asarray(tradable_mask, dtype=bool)
    b = np.nan_to_num(np.asarray(benchmark_returns, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0)

    t_len, n_symbols = w.shape
    if spec.lot_size <= 0:
        raise ValueError(f"lot_size must be positive, got {spec.lot_size}")

    if symbols is None:
        symbols = [f"SYM_{idx:04d}" for idx in range(n_symbols)]
    cash_symbol_mask = _build_cash_symbol_mask(symbols)

    if dates is None:
        date_text = [f"t{idx:04d}" for idx in range(t_len)]
    else:
        date_text = [str(np.datetime_as_string(np.asarray(d, dtype="datetime64[D]"), unit="D")) for d in dates]

    if open_prices is None:
        open_matrix = None
    else:
        open_matrix = np.nan_to_num(np.asarray(open_prices, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0)
        if open_matrix.shape != (t_len, n_symbols):
            raise ValueError(
                "open_prices shape must match (num_days, num_symbols): "
                f"expected {(t_len, n_symbols)}, got {open_matrix.shape}"
            )

    if close_prices is None:
        close_matrix = None
    else:
        close_matrix = np.nan_to_num(np.asarray(close_prices, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0)
        if close_matrix.shape != (t_len, n_symbols):
            raise ValueError(
                "close_prices shape must match (num_days, num_symbols): "
                f"expected {(t_len, n_symbols)}, got {close_matrix.shape}"
            )

    if open_matrix is None and close_matrix is None:
        raise ValueError("At least one of open_prices or close_prices must be provided.")

    if open_matrix is None:
        open_matrix = close_matrix.copy()
    if close_matrix is None:
        close_matrix = open_matrix.copy()

    prev_open = np.where(open_matrix[0] > 1e-12, open_matrix[0], 1.0)
    prev_close = np.where(close_matrix[0] > 1e-12, close_matrix[0], prev_open)
    open_matrix = np.where(open_matrix > 1e-12, open_matrix, prev_open[None, :])
    close_matrix = np.where(close_matrix > 1e-12, close_matrix, prev_close[None, :])

    if spec.rule == "day_trade":
        return _simulate_day_trade_backtest(
            weights=w,
            tradable_mask=m,
            benchmark_returns=b,
            spec=spec,
            initial_capital=initial_capital,
            open_matrix=open_matrix,
            close_matrix=close_matrix,
            cash_symbol_mask=cash_symbol_mask,
            symbols=symbols,
            date_text=date_text,
        )
    if spec.rule == "basic":
        return _simulate_basic_backtest(
            weights=w,
            tradable_mask=m,
            benchmark_returns=b,
            spec=spec,
            initial_capital=initial_capital,
            close_matrix=close_matrix,
            cash_symbol_mask=cash_symbol_mask,
            symbols=symbols,
            date_text=date_text,
        )
    return _simulate_overnight_backtest(
        weights=w,
        tradable_mask=m,
        benchmark_returns=b,
        spec=spec,
        initial_capital=initial_capital,
        close_matrix=close_matrix,
        cash_symbol_mask=cash_symbol_mask,
        symbols=symbols,
        date_text=date_text,
    )


def _simulate_day_trade_backtest(
    *,
    weights: np.ndarray,
    tradable_mask: np.ndarray,
    benchmark_returns: np.ndarray,
    spec: object,
    initial_capital: float,
    open_matrix: np.ndarray,
    close_matrix: np.ndarray,
    cash_symbol_mask: np.ndarray,
    symbols: list[str],
    date_text: list[str],
) -> tuple[BacktestResult, list[HoldingsRecord]]:
    t_len, n_symbols = weights.shape
    strategy_returns = np.zeros(t_len, dtype=np.float32)
    turnovers = np.zeros(t_len, dtype=np.float32)
    stock_weights_history = np.zeros((t_len, n_symbols), dtype=np.float32)
    records: list[HoldingsRecord] = []
    cash = float(initial_capital)

    for t in range(t_len):
        day_open = open_matrix[t]
        day_close = close_matrix[t]

        day_mask = tradable_mask[t] & (day_open > 1e-12) & (day_close > 1e-12) & (~cash_symbol_mask)
        target_w = np.nan_to_num(weights[t], nan=0.0, posinf=0.0, neginf=0.0)
        target_w[~day_mask] = 0.0

        gross_exposure = float(np.abs(target_w).sum())
        if gross_exposure > 1.0:
            target_w /= gross_exposure

        equity_before = max(float(cash), 1e-12)
        desired_value = equity_before * target_w

        lot_notional = day_open * float(spec.lot_size)
        long_mask = desired_value > 0.0
        short_mask = desired_value < 0.0

        long_lot_cost = lot_notional * (1.0 + spec.buy_fee_rate)
        short_lot_proceeds = lot_notional * (1.0 - spec.sell_fee_rate)

        long_target_lots = np.zeros(n_symbols, dtype=np.int64)
        short_target_lots = np.zeros(n_symbols, dtype=np.int64)

        valid_long = long_mask & (long_lot_cost > 1e-12)
        if np.any(valid_long):
            raw_long_lots = np.zeros(n_symbols, dtype=np.float64)
            raw_long_lots[valid_long] = desired_value[valid_long] / long_lot_cost[valid_long]
            long_target_lots = _floor_to_int64(raw_long_lots, non_negative=True)

        valid_short = short_mask & (short_lot_proceeds > 1e-12)
        if np.any(valid_short):
            raw_short_lots = np.zeros(n_symbols, dtype=np.float64)
            raw_short_lots[valid_short] = (-desired_value[valid_short]) / short_lot_proceeds[valid_short]
            short_target_lots = _floor_to_int64(raw_short_lots, non_negative=True)

        shares = (long_target_lots - short_target_lots) * np.int64(spec.lot_size)
        if np.any(cash_symbol_mask):
            shares[cash_symbol_mask] = 0
        long_shares = np.maximum(shares, 0)
        short_shares = np.minimum(shares, 0)

        long_buy_notional = float(np.dot(long_shares.astype(np.float64), day_open))
        short_open_notional = float(np.dot((-short_shares).astype(np.float64), day_open))
        long_buy_fee = _fee_with_floor(long_buy_notional, spec.buy_fee_rate, spec.min_fee_per_side)
        short_sell_fee = _fee_with_floor(short_open_notional, spec.sell_fee_rate, spec.min_fee_per_side)
        open_cash_flow = -long_buy_notional - long_buy_fee + short_open_notional - short_sell_fee

        if cash + open_cash_flow < 0.0 and (long_buy_notional > 0.0 or short_open_notional > 0.0):
            scale = max(0.0, cash / max(long_buy_notional + long_buy_fee - (short_open_notional - short_sell_fee), 1e-12))
            scaled_long_lots = _floor_to_int64(long_target_lots.astype(np.float64) * scale, non_negative=True)
            scaled_short_lots = _floor_to_int64(short_target_lots.astype(np.float64) * scale, non_negative=True)
            shares = (scaled_long_lots - scaled_short_lots) * np.int64(spec.lot_size)
            if np.any(cash_symbol_mask):
                shares[cash_symbol_mask] = 0
            long_shares = np.maximum(shares, 0)
            short_shares = np.minimum(shares, 0)
            long_buy_notional = float(np.dot(long_shares.astype(np.float64), day_open))
            short_open_notional = float(np.dot((-short_shares).astype(np.float64), day_open))
            long_buy_fee = _fee_with_floor(long_buy_notional, spec.buy_fee_rate, spec.min_fee_per_side)
            short_sell_fee = _fee_with_floor(short_open_notional, spec.sell_fee_rate, spec.min_fee_per_side)
            open_cash_flow = -long_buy_notional - long_buy_fee + short_open_notional - short_sell_fee

        cash_after_open = cash + open_cash_flow

        long_close_notional = float(np.dot(long_shares.astype(np.float64), day_close))
        short_cover_notional = float(np.dot((-short_shares).astype(np.float64), day_close))
        long_sell_fee = _fee_with_floor(long_close_notional, spec.sell_fee_rate, spec.min_fee_per_side)
        short_buy_fee = _fee_with_floor(short_cover_notional, spec.buy_fee_rate, spec.min_fee_per_side)
        cash_end = cash_after_open + long_close_notional - long_sell_fee - short_cover_notional - short_buy_fee
        cash_end = max(cash_end, 0.0)

        open_market_value = shares.astype(np.float64) * day_open
        stock_weights_history[t] = (open_market_value / equity_before).astype(np.float32)
        turnovers[t] = float((long_buy_notional + short_open_notional + long_close_notional + short_cover_notional) / equity_before)
        strategy_returns[t] = np.float32(np.log(max(cash_end, 1e-12) / equity_before))

        records.append(
            HoldingsRecord(
                date=date_text[t],
                symbol="CASH",
                shares=int(_floor_to_int64(cash_end, non_negative=True).item()),
                price=1.0,
                market_value=float(cash_end),
                holding_ratio=1.0 if cash_end > 0.0 else 0.0,
                is_cash=True,
                traded_notional=0.0,
                buy_fee=0.0,
                sell_fee=0.0,
            )
        )

        traded_idx = np.flatnonzero(shares != 0)
        if traded_idx.size > 0:
            denom = max(equity_before, 1e-12)
            for idx in traded_idx.tolist():
                shares_i = int(shares[idx])
                open_notional_i = float(abs(shares_i) * day_open[idx])
                close_notional_i = float(abs(shares_i) * day_close[idx])
                if shares_i > 0:
                    buy_fee_i = float(_fee_with_floor(open_notional_i, spec.buy_fee_rate, spec.min_fee_per_side))
                    sell_fee_i = float(_fee_with_floor(close_notional_i, spec.sell_fee_rate, spec.min_fee_per_side))
                else:
                    buy_fee_i = float(_fee_with_floor(close_notional_i, spec.buy_fee_rate, spec.min_fee_per_side))
                    sell_fee_i = float(_fee_with_floor(open_notional_i, spec.sell_fee_rate, spec.min_fee_per_side))
                open_value_i = float(open_market_value[idx])
                records.append(
                    HoldingsRecord(
                        date=date_text[t],
                        symbol=symbols[idx],
                        shares=shares_i,
                        price=float(day_open[idx]),
                        market_value=open_value_i,
                        holding_ratio=float(open_value_i / denom),
                        is_cash=False,
                        traded_notional=float(open_notional_i + close_notional_i),
                        buy_fee=buy_fee_i,
                        sell_fee=sell_fee_i,
                    )
                )

        cash = cash_end

    return (
        BacktestResult(
            strategy_returns=strategy_returns,
            benchmark_returns=benchmark_returns.astype(np.float32),
            turnovers=turnovers,
            weights_history=stock_weights_history,
        ),
        records,
    )


def _simulate_basic_backtest(
    *,
    weights: np.ndarray,
    tradable_mask: np.ndarray,
    benchmark_returns: np.ndarray,
    spec: object,
    initial_capital: float,
    close_matrix: np.ndarray,
    cash_symbol_mask: np.ndarray,
    symbols: list[str],
    date_text: list[str],
) -> tuple[BacktestResult, list[HoldingsRecord]]:
    t_len, n_symbols = weights.shape
    positions = np.zeros(n_symbols, dtype=np.int64)
    cash = float(initial_capital)
    strategy_returns = np.zeros(t_len, dtype=np.float32)
    turnovers = np.zeros(t_len, dtype=np.float32)
    stock_weights_history = np.zeros((t_len, n_symbols), dtype=np.float32)
    records: list[HoldingsRecord] = []
    prev_day_equity = float(initial_capital)

    for t in range(t_len):
        close_price = close_matrix[t]
        day_mask = tradable_mask[t] & (close_price > 1e-12) & (~cash_symbol_mask)
        target_w = np.nan_to_num(weights[t], nan=0.0, posinf=0.0, neginf=0.0)
        target_w[~day_mask] = 0.0
        target_w = np.clip(target_w, 0.0, None)

        gross_exposure = float(np.abs(target_w).sum())
        if gross_exposure > 1.0:
            target_w /= gross_exposure

        current_value = positions.astype(np.float64) * close_price
        equity_before_trade = max(float(cash + current_value.sum()), 1e-12)
        desired_value = equity_before_trade * target_w

        target_shares = np.zeros(n_symbols, dtype=np.int64)
        valid = (close_price > 1e-12) & (desired_value > 0.0)
        if np.any(valid):
            raw_shares = np.zeros(n_symbols, dtype=np.float64)
            raw_shares[valid] = desired_value[valid] / close_price[valid]
            target_shares = _floor_to_int64(raw_shares, non_negative=True)

        delta_shares = target_shares - positions
        buy_shares = np.maximum(delta_shares, 0)
        sell_shares = np.maximum(-delta_shares, 0)
        buy_notional = float(np.dot(buy_shares.astype(np.float64), close_price))
        sell_notional = float(np.dot(sell_shares.astype(np.float64), close_price))
        buy_fee = _fee_with_floor(buy_notional, spec.buy_fee_rate, spec.min_fee_per_side)
        sell_fee = _fee_with_floor(sell_notional, spec.sell_fee_rate, spec.min_fee_per_side)

        cash = cash - buy_notional - buy_fee + sell_notional - sell_fee
        positions = target_shares
        market_value = positions.astype(np.float64) * close_price
        nav_end = max(float(cash + market_value.sum()), 1e-12)

        stock_weights_history[t] = (market_value / nav_end).astype(np.float32)
        turnovers[t] = float((buy_notional + sell_notional) / max(prev_day_equity, 1e-12))
        strategy_returns[t] = np.float32(np.log(nav_end / max(prev_day_equity, 1e-12)))
        prev_day_equity = nav_end

        records.append(
            HoldingsRecord(
                date=date_text[t],
                symbol="CASH",
                shares=int(_floor_to_int64(cash, non_negative=True).item()),
                price=1.0,
                market_value=float(cash),
                holding_ratio=float(cash / nav_end) if cash > 0.0 else 0.0,
                is_cash=True,
            )
        )

        traded_idx = np.flatnonzero(delta_shares != 0)
        if traded_idx.size > 0:
            denom = max(nav_end, 1e-12)
            for idx in traded_idx.tolist():
                shares_i = int(delta_shares[idx])
                notional_i = float(abs(shares_i) * close_price[idx])
                buy_fee_i = float(_fee_with_floor(notional_i, spec.buy_fee_rate, spec.min_fee_per_side)) if shares_i > 0 else 0.0
                sell_fee_i = float(_fee_with_floor(notional_i, spec.sell_fee_rate, spec.min_fee_per_side)) if shares_i < 0 else 0.0
                records.append(
                    HoldingsRecord(
                        date=date_text[t],
                        symbol=symbols[idx],
                        shares=shares_i,
                        price=float(close_price[idx]),
                        market_value=float(market_value[idx]),
                        holding_ratio=float(market_value[idx] / denom),
                        is_cash=False,
                        traded_notional=notional_i,
                        buy_fee=buy_fee_i,
                        sell_fee=sell_fee_i,
                    )
                )

    return (
        BacktestResult(
            strategy_returns=strategy_returns,
            benchmark_returns=benchmark_returns.astype(np.float32),
            turnovers=turnovers,
            weights_history=stock_weights_history,
        ),
        records,
    )


def _simulate_overnight_backtest(
    *,
    weights: np.ndarray,
    tradable_mask: np.ndarray,
    benchmark_returns: np.ndarray,
    spec: object,
    initial_capital: float,
    close_matrix: np.ndarray,
    cash_symbol_mask: np.ndarray,
    symbols: list[str],
    date_text: list[str],
) -> tuple[BacktestResult, list[HoldingsRecord]]:
    t_len, n_symbols = weights.shape
    positions = np.zeros(n_symbols, dtype=np.int64)
    cash = float(initial_capital)
    pending_buy_liability = 0.0
    pending_sell_receivable = 0.0
    max_lag = max(int(spec.buy_settlement_lag_days), int(spec.sell_settlement_lag_days))
    buy_settlement_schedule = np.zeros(t_len + max_lag + 1, dtype=np.float64)
    sell_settlement_schedule = np.zeros(t_len + max_lag + 1, dtype=np.float64)
    strategy_returns = np.zeros(t_len, dtype=np.float32)
    turnovers = np.zeros(t_len, dtype=np.float32)
    stock_weights_history = np.zeros((t_len, n_symbols), dtype=np.float32)
    records: list[HoldingsRecord] = []
    bankrupt = False

    for t in range(t_len):
        due_buy = float(buy_settlement_schedule[t])
        if due_buy > 0.0:
            cash -= due_buy
            pending_buy_liability -= due_buy
        if cash < 0.0:
            bankrupt = True

        close_price = close_matrix[t]
        current_value = positions.astype(np.float64) * close_price
        equity_before = max(float(cash + current_value.sum() - pending_buy_liability + pending_sell_receivable), 1e-12)

        if bankrupt:
            strategy_returns[t] = np.float32(np.log(1e-12 / equity_before))
            turns = 0.0
            turnovers[t] = np.float32(turns)
            stock_weights_history[t] = 0.0
            records.append(
                HoldingsRecord(
                    date=date_text[t],
                    symbol="CASH",
                    shares=0,
                    price=1.0,
                    market_value=0.0,
                    holding_ratio=0.0,
                    is_cash=True,
                )
            )
            positions[:] = 0
            cash = 0.0
            pending_buy_liability = 0.0
            pending_sell_receivable = 0.0
            continue

        day_mask = tradable_mask[t] & (close_price > 1e-12) & (~cash_symbol_mask)
        target_w = np.nan_to_num(weights[t], nan=0.0, posinf=0.0, neginf=0.0)
        target_w[~day_mask] = 0.0
        target_w = np.clip(target_w, 0.0, None)

        gross_exposure = float(np.abs(target_w).sum())
        if gross_exposure > 1.0:
            target_w /= gross_exposure

        desired_value = equity_before * target_w
        target_shares = np.zeros(n_symbols, dtype=np.int64)
        valid = (close_price > 1e-12) & (desired_value > 0.0)
        if np.any(valid):
            raw_shares = np.zeros(n_symbols, dtype=np.float64)
            raw_shares[valid] = desired_value[valid] / close_price[valid]
            target_shares = _floor_to_int64(raw_shares, non_negative=True)

        delta_shares = target_shares - positions
        buy_shares = np.maximum(delta_shares, 0)
        sell_shares = np.maximum(-delta_shares, 0)
        buy_notional = float(np.dot(buy_shares.astype(np.float64), close_price))
        sell_notional = float(np.dot(sell_shares.astype(np.float64), close_price))
        buy_fee = _fee_with_floor(buy_notional, spec.buy_fee_rate, spec.min_fee_per_side)
        sell_fee = _fee_with_floor(sell_notional, spec.sell_fee_rate, spec.min_fee_per_side)

        cash -= buy_fee + sell_fee
        if cash < 0.0:
            bankrupt = True

        positions = target_shares
        if buy_notional > 0.0:
            due_day = t + int(spec.buy_settlement_lag_days)
            if due_day < buy_settlement_schedule.size:
                buy_settlement_schedule[due_day] += buy_notional
            pending_buy_liability += buy_notional
        if sell_notional > 0.0:
            due_day = t + int(spec.sell_settlement_lag_days)
            if due_day < sell_settlement_schedule.size:
                sell_settlement_schedule[due_day] += sell_notional
            pending_sell_receivable += sell_notional

        due_sell = float(sell_settlement_schedule[t])
        if due_sell > 0.0:
            cash += due_sell
            pending_sell_receivable -= due_sell

        nav_end = max(cash + (positions.astype(np.float64) * close_price).sum() - pending_buy_liability + pending_sell_receivable, 1e-12)
        strategy_returns[t] = np.float32(np.log(nav_end / equity_before))
        turnovers[t] = float((buy_notional + sell_notional) / equity_before)
        stock_weights_history[t] = (positions.astype(np.float64) * close_price / equity_before).astype(np.float32)

        records.append(
            HoldingsRecord(
                date=date_text[t],
                symbol="CASH",
                shares=int(_floor_to_int64(cash, non_negative=True).item()),
                price=1.0,
                market_value=float(cash),
                holding_ratio=1.0 if cash > 0.0 else 0.0,
                is_cash=True,
            )
        )

        traded_idx = np.flatnonzero(delta_shares != 0)
        if traded_idx.size > 0:
            denom = max(equity_before, 1e-12)
            for idx in traded_idx.tolist():
                shares_i = int(delta_shares[idx])
                notional_i = float(abs(shares_i) * close_price[idx])
                buy_fee_i = float(_fee_with_floor(notional_i, spec.buy_fee_rate, spec.min_fee_per_side)) if shares_i > 0 else 0.0
                sell_fee_i = float(_fee_with_floor(notional_i, spec.sell_fee_rate, spec.min_fee_per_side)) if shares_i < 0 else 0.0
                records.append(
                    HoldingsRecord(
                        date=date_text[t],
                        symbol=symbols[idx],
                        shares=shares_i,
                        price=float(close_price[idx]),
                        market_value=float(positions[idx] * close_price[idx]),
                        holding_ratio=float((positions[idx] * close_price[idx]) / denom),
                        is_cash=False,
                        traded_notional=notional_i,
                        buy_fee=buy_fee_i,
                        sell_fee=sell_fee_i,
                    )
                )

        if bankrupt:
            positions[:] = 0
            cash = 0.0
            pending_buy_liability = 0.0
            pending_sell_receivable = 0.0
            buy_settlement_schedule[:] = 0.0
            sell_settlement_schedule[:] = 0.0
            break

    return (
        BacktestResult(
            strategy_returns=strategy_returns,
            benchmark_returns=benchmark_returns.astype(np.float32),
            turnovers=turnovers,
            weights_history=stock_weights_history,
        ),
        records,
    )
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import torch


INT64_MIN_FLOAT_SAFE = np.nextafter(float(np.iinfo(np.int64).min), 0.0)
INT64_MAX_FLOAT_SAFE = np.nextafter(float(np.iinfo(np.int64).max + 1), 0.0)


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


def _vectorized_backtest(
    weights: np.ndarray,
    future_returns: np.ndarray,
    tradable_mask: np.ndarray,
    fee_per_side: float,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    weights_history = np.asarray(weights, dtype=np.float32).copy()
    weights_history[~tradable_mask.astype(bool)] = 0.0

    weight_sums = weights_history.sum(axis=1, keepdims=True)
    nonzero = weight_sums.squeeze(1) > 0
    weights_history[nonzero] /= weight_sums[nonzero]

    prev = np.concatenate([
        np.zeros((1, weights_history.shape[1]), dtype=np.float32),
        weights_history[:-1],
    ], axis=0)
    turnovers = np.abs(weights_history - prev).sum(axis=1).astype(np.float32)

    gross = np.einsum("ts,ts->t", weights_history, future_returns, dtype=np.float32)
    strategy_returns = gross - fee_per_side * turnovers
    return strategy_returns.astype(np.float32), turnovers, weights_history


def _vectorized_backtest_torch(
    weights: torch.Tensor,
    future_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    fee_per_side: float,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    # ✅ FIXED: Simplified and more numerically stable weight normalization
    weights_history = weights.float().clone()
    
    # Step 1: Mask non-tradable symbols
    weights_history = weights_history.masked_fill(~tradable_mask.bool(), 0.0)
    
    # Step 2: Normalize weights (simplified logic)
    weight_sums = weights_history.sum(dim=1, keepdim=True).clamp_min(1e-12)
    weights_history = weights_history / weight_sums  # Direct broadcast normalization
    
    # Step 3: Compute turnover
    prev = torch.cat(
        [torch.zeros_like(weights_history[:1]), weights_history[:-1]],
        dim=0,
    )
    turnovers = (weights_history - prev).abs().sum(dim=1)

    # Step 4: Compute strategy returns
    gross = (weights_history * future_returns.float()).sum(dim=1)
    strategy_returns = gross - fee_per_side * turnovers
    return strategy_returns.float(), turnovers.float(), weights_history.float()


def run_backtest(
    weights: np.ndarray,
    future_returns: np.ndarray,
    tradable_mask: np.ndarray,
    benchmark_returns: np.ndarray,
    fee_per_side: float,
) -> BacktestResult:
    """Simulate daily portfolio execution from model weights."""
    strategy_returns, turnovers, weights_history = _vectorized_backtest(
        weights,
        future_returns,
        tradable_mask,
        fee_per_side,
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
) -> BacktestResultTensor:
    """Simulate daily portfolio execution from model weights in torch."""
    strategy_returns, turnovers, weights_history = _vectorized_backtest_torch(
        weights,
        future_returns,
        tradable_mask,
        fee_per_side,
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
    buy_fee_rate: float = 0.001425,
    sell_fee_rate: float = 0.002925,
    lot_size: int = 1000,
    open_prices: np.ndarray | None = None,
    close_prices: np.ndarray | None = None,
    symbols: list[str] | None = None,
    dates: np.ndarray | None = None,
) -> tuple[BacktestResult, list[HoldingsRecord]]:
    """Day-trade backtest: buy at open and force liquidate at same-day close.

    Trading assumptions:
    - Initial capital is cash only.
    - Shares are bought in integer lots (1 lot = ``lot_size`` shares).
    - Buy and sell fees are charged separately by buy_fee_rate/sell_fee_rate.
    - All positions are closed at the same day's close (no overnight holdings).
    """
    w = np.asarray(weights, dtype=np.float64)
    _ = np.asarray(future_returns, dtype=np.float64)  # Kept for API compatibility.
    m = np.asarray(tradable_mask, dtype=bool)
    b = np.nan_to_num(np.asarray(benchmark_returns, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0)

    t_len, n_symbols = w.shape
    if lot_size <= 0:
        raise ValueError(f"lot_size must be positive, got {lot_size}")

    if symbols is None:
        symbols = [f"SYM_{idx:04d}" for idx in range(n_symbols)]
    if dates is None:
        date_text = [f"t{idx:04d}" for idx in range(t_len)]
    else:
        date_text = [str(np.datetime_as_string(np.asarray(d, dtype="datetime64[D]"), unit="D")) for d in dates]

    strategy_returns = np.zeros(t_len, dtype=np.float32)
    turnovers = np.zeros(t_len, dtype=np.float32)
    stock_weights_history = np.zeros((t_len, n_symbols), dtype=np.float32)

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

    cash = float(initial_capital)
    records: list[HoldingsRecord] = []

    for t in range(t_len):
        day_open = open_matrix[t]
        day_close = close_matrix[t]

        day_mask = m[t] & (day_open > 1e-12) & (day_close > 1e-12)
        target_w = np.nan_to_num(w[t], nan=0.0, posinf=0.0, neginf=0.0)
        target_w = np.clip(target_w, 0.0, None)
        target_w[~day_mask] = 0.0

        total_target = float(target_w.sum())
        if total_target > 1.0:
            target_w /= total_target

        equity_before = max(float(cash), 1e-12)
        desired_value = equity_before * target_w

        lot_notional = day_open * float(lot_size)
        lot_total_cost = lot_notional * (1.0 + buy_fee_rate)
        valid_lots = lot_total_cost > 1e-12

        target_lots = np.zeros(n_symbols, dtype=np.int64)
        if np.any(valid_lots):
            raw_target_lots = np.zeros(n_symbols, dtype=np.float64)
            raw_target_lots[valid_lots] = desired_value[valid_lots] / lot_total_cost[valid_lots]
            target_lots = _floor_to_int64(raw_target_lots, non_negative=True)

        shares = target_lots * np.int64(lot_size)
        buy_notional = float(np.dot(shares.astype(np.float64), day_open))
        buy_fee = buy_fee_rate * buy_notional
        total_buy_cost = buy_notional + buy_fee

        if total_buy_cost > cash + 1e-9 and buy_notional > 0.0:
            scale = max(0.0, cash / total_buy_cost)
            scaled_lots = _floor_to_int64(target_lots.astype(np.float64) * scale, non_negative=True)
            shares = scaled_lots * np.int64(lot_size)
            buy_notional = float(np.dot(shares.astype(np.float64), day_open))
            buy_fee = buy_fee_rate * buy_notional
            total_buy_cost = buy_notional + buy_fee

        if total_buy_cost > cash + 1e-6:
            shares.fill(0)
            buy_notional = 0.0
            buy_fee = 0.0
            total_buy_cost = 0.0

        cash_after_buy = cash - total_buy_cost

        sell_notional = float(np.dot(shares.astype(np.float64), day_close))
        sell_fee = sell_fee_rate * sell_notional
        cash_end = cash_after_buy + sell_notional - sell_fee
        cash_end = max(cash_end, 0.0)

        open_market_value = shares.astype(np.float64) * day_open
        stock_weights_history[t] = (open_market_value / equity_before).astype(np.float32)
        turnovers[t] = float((buy_notional + sell_notional) / equity_before)
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
            )
        )

        cash = cash_end

    return (
        BacktestResult(
            strategy_returns=strategy_returns,
            benchmark_returns=b.astype(np.float32),
            turnovers=turnovers,
            weights_history=stock_weights_history,
        ),
        records,
    )
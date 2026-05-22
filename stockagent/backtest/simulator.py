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
    cash_symbol_mask = _build_cash_symbol_mask(symbols)
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

        day_mask = m[t] & (day_open > 1e-12) & (day_close > 1e-12) & (~cash_symbol_mask)
        target_w = np.nan_to_num(w[t], nan=0.0, posinf=0.0, neginf=0.0)
        target_w[~day_mask] = 0.0

        gross_exposure = float(np.abs(target_w).sum())
        if gross_exposure > 1.0:
            target_w /= gross_exposure

        equity_before = max(float(cash), 1e-12)
        desired_value = equity_before * target_w

        lot_notional = day_open * float(lot_size)
        long_mask = desired_value > 0.0
        short_mask = desired_value < 0.0

        long_lot_cost = lot_notional * (1.0 + buy_fee_rate)
        short_lot_proceeds = lot_notional * (1.0 - sell_fee_rate)

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

        shares = (long_target_lots - short_target_lots) * np.int64(lot_size)
        if np.any(cash_symbol_mask):
            shares[cash_symbol_mask] = 0
        long_shares = np.maximum(shares, 0)
        short_shares = np.minimum(shares, 0)

        long_buy_notional = float(np.dot(long_shares.astype(np.float64), day_open))
        short_open_notional = float(np.dot((-short_shares).astype(np.float64), day_open))
        long_buy_fee = buy_fee_rate * long_buy_notional
        short_sell_fee = sell_fee_rate * short_open_notional
        open_cash_flow = -long_buy_notional - long_buy_fee + short_open_notional - short_sell_fee

        if cash + open_cash_flow < 0.0 and (long_buy_notional > 0.0 or short_open_notional > 0.0):
            scale = max(0.0, cash / max(long_buy_notional + long_buy_fee - (short_open_notional - short_sell_fee), 1e-12))
            scaled_long_lots = _floor_to_int64(long_target_lots.astype(np.float64) * scale, non_negative=True)
            scaled_short_lots = _floor_to_int64(short_target_lots.astype(np.float64) * scale, non_negative=True)
            shares = (scaled_long_lots - scaled_short_lots) * np.int64(lot_size)
            if np.any(cash_symbol_mask):
                shares[cash_symbol_mask] = 0
            long_shares = np.maximum(shares, 0)
            short_shares = np.minimum(shares, 0)
            long_buy_notional = float(np.dot(long_shares.astype(np.float64), day_open))
            short_open_notional = float(np.dot((-short_shares).astype(np.float64), day_open))
            long_buy_fee = buy_fee_rate * long_buy_notional
            short_sell_fee = sell_fee_rate * short_open_notional
            open_cash_flow = -long_buy_notional - long_buy_fee + short_open_notional - short_sell_fee

        cash_after_open = cash + open_cash_flow

        long_close_notional = float(np.dot(long_shares.astype(np.float64), day_close))
        short_cover_notional = float(np.dot((-short_shares).astype(np.float64), day_close))
        long_sell_fee = sell_fee_rate * long_close_notional
        short_buy_fee = buy_fee_rate * short_cover_notional
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

        # Record intraday traded symbols so holdings.csv reflects actual buy/sell activity.
        traded_idx = np.flatnonzero(shares != 0)
        if traded_idx.size > 0:
            denom = max(equity_before, 1e-12)
            for idx in traded_idx.tolist():
                shares_i = int(shares[idx])
                open_notional_i = float(abs(shares_i) * day_open[idx])
                close_notional_i = float(abs(shares_i) * day_close[idx])
                if shares_i > 0:
                    buy_fee_i = float(buy_fee_rate * open_notional_i)
                    sell_fee_i = float(sell_fee_rate * close_notional_i)
                else:
                    buy_fee_i = float(buy_fee_rate * close_notional_i)
                    sell_fee_i = float(sell_fee_rate * open_notional_i)
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
            benchmark_returns=b.astype(np.float32),
            turnovers=turnovers,
            weights_history=stock_weights_history,
        ),
        records,
    )
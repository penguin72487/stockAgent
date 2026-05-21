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
    sell_fee_rate: float = 0.004425,
    close_prices: np.ndarray | None = None,
    symbols: list[str] | None = None,
    dates: np.ndarray | None = None,
) -> tuple[BacktestResult, list[HoldingsRecord]]:
    """Daily backtest with integer shares, virtual cash, and daily fee settlement.

    Trading assumptions:
    - Initial capital is cash only.
    - Stock shares are integer lots: floor(target_value / current_price).
    - Buy and sell fees are charged separately by buy_fee_rate/sell_fee_rate.
    - Cash is a virtual asset with 0 daily return.
    """
    w = np.asarray(weights, dtype=np.float64)
    r = np.nan_to_num(np.asarray(future_returns, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0)
    m = np.asarray(tradable_mask, dtype=bool)
    b = np.nan_to_num(np.asarray(benchmark_returns, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0)

    t_len, n_symbols = w.shape
    if symbols is None:
        symbols = [f"SYM_{idx:04d}" for idx in range(n_symbols)]
    if dates is None:
        date_text = [f"t{idx:04d}" for idx in range(t_len)]
    else:
        date_text = [str(np.datetime_as_string(np.asarray(d, dtype="datetime64[D]"), unit="D")) for d in dates]

    strategy_returns = np.zeros(t_len, dtype=np.float32)
    turnovers = np.zeros(t_len, dtype=np.float32)
    stock_weights_history = np.zeros((t_len, n_symbols), dtype=np.float32)

    if close_prices is not None:
        price_matrix = np.nan_to_num(np.asarray(close_prices, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0)
        if price_matrix.shape != (t_len, n_symbols):
            raise ValueError(
                "close_prices shape must match (num_days, num_symbols): "
                f"expected {(t_len, n_symbols)}, got {price_matrix.shape}"
            )
        current_prices = np.where(price_matrix[0] > 1e-12, price_matrix[0], 1.0)
    else:
        price_matrix = None
        current_prices = np.ones(n_symbols, dtype=np.float64)
    shares = np.zeros(n_symbols, dtype=np.int64)
    cash = float(initial_capital)
    cash_hold_mode = False

    records: list[HoldingsRecord] = []

    for t in range(t_len):
        if cash_hold_mode:
            strategy_returns[t] = 0.0
            turnovers[t] = 0.0
            stock_weights_history[t] = 0.0
            records.append(
                HoldingsRecord(
                    date=date_text[t],
                    symbol="CASH",
                    shares=int(_floor_to_int64(cash, non_negative=True).item()),
                    price=1.0,
                    market_value=float(cash),
                    holding_ratio=1.0 if cash > 0 else 0.0,
                    is_cash=True,
                )
            )
            continue

        if price_matrix is not None:
            current_prices = np.where(price_matrix[t] > 1e-12, price_matrix[t], current_prices)

        day_mask = m[t]
        target_w = np.nan_to_num(w[t], nan=0.0, posinf=0.0, neginf=0.0)
        target_w = np.clip(target_w, 0.0, None)
        target_w[~day_mask] = 0.0

        total_target = float(target_w.sum())
        if total_target > 1.0:
            target_w /= total_target

        equity_before = float(cash + np.dot(shares.astype(np.float64), current_prices))
        equity_before = max(equity_before, 1e-12)

        desired_value = equity_before * target_w
        safe_prices = np.where(current_prices > 1e-12, current_prices, np.inf)
        raw_target_shares = desired_value / safe_prices
        desired_shares = _floor_to_int64(raw_target_shares, non_negative=True)

        # Non-tradable symbols keep existing shares.
        desired_shares[~day_mask] = shares[~day_mask]

        delta = desired_shares - shares
        sell_qty = np.clip(-delta, 0, None)
        buy_qty = np.clip(delta, 0, None)

        sell_notional = float(np.dot(sell_qty.astype(np.float64), current_prices))
        buy_notional = float(np.dot(buy_qty.astype(np.float64), current_prices))

        available_cash = cash + sell_notional - sell_notional * sell_fee_rate
        max_affordable_buy = available_cash / (1.0 + buy_fee_rate) if buy_fee_rate >= 0.0 else available_cash

        if buy_notional > max_affordable_buy + 1e-9 and buy_notional > 0.0:
            scale = max(0.0, max_affordable_buy / buy_notional)
            scaled_buy_qty = buy_qty.astype(np.float64) * scale
            buy_qty = _floor_to_int64(scaled_buy_qty, non_negative=True)
            desired_shares = shares - sell_qty + buy_qty
            delta = desired_shares - shares
            sell_qty = np.clip(-delta, 0, None)
            buy_qty = np.clip(delta, 0, None)
            sell_notional = float(np.dot(sell_qty.astype(np.float64), current_prices))
            buy_notional = float(np.dot(buy_qty.astype(np.float64), current_prices))

        # Cash-hold rule: if strategy wants stock exposure but cannot buy even 1 share,
        # stop trading and keep current cash through the remaining dates.
        wanted_stock = bool(np.any(target_w > 0.0))
        has_any_share = bool(np.any(desired_shares > 0))
        if wanted_stock and not has_any_share:
            tradable_target = (day_mask & (target_w > 0.0))
            candidate_prices = current_prices[tradable_target]
            candidate_prices = candidate_prices[np.isfinite(candidate_prices) & (candidate_prices > 1e-12)]
            if candidate_prices.size > 0:
                min_buy_cost = float(candidate_prices.min() * (1.0 + buy_fee_rate))
                if max_affordable_buy + 1e-12 < min_buy_cost:
                    strategy_returns[t] = 0.0
                    turnovers[t] = 0.0
                    stock_weights_history[t] = 0.0
                    shares.fill(0)
                    cash_hold_mode = True
                    records.append(
                        HoldingsRecord(
                            date=date_text[t],
                            symbol="CASH",
                            shares=int(_floor_to_int64(cash, non_negative=True).item()),
                            price=1.0,
                            market_value=float(cash),
                            holding_ratio=1.0 if cash > 0 else 0.0,
                            is_cash=True,
                        )
                    )
                    continue

        buy_fee = buy_fee_rate * buy_notional
        sell_fee = sell_fee_rate * sell_notional
        fee = buy_fee + sell_fee
        traded_notional = buy_notional + sell_notional

        shares = desired_shares
        cash = cash + sell_notional - sell_fee - buy_notional - buy_fee
        if cash < 0 and abs(cash) < 1e-7:
            cash = 0.0

        stock_market_values = shares.astype(np.float64) * current_prices
        equity_after_trade = float(cash + stock_market_values.sum())
        equity_after_trade = max(equity_after_trade, 1e-12)

        stock_weights_history[t] = (stock_market_values / equity_after_trade).astype(np.float32)
        turnovers[t] = float(traded_notional / equity_before)

        cash_ratio = float(cash / equity_after_trade)
        day_rows: list[HoldingsRecord] = []
        day_rows.append(
            HoldingsRecord(
                date=date_text[t],
                symbol="CASH",
                shares=int(_floor_to_int64(cash, non_negative=True).item()),
                price=1.0,
                market_value=float(cash),
                holding_ratio=cash_ratio,
                is_cash=True,
            )
        )
        nonzero = np.flatnonzero(shares > 0)
        for idx in nonzero.tolist():
            mv = float(stock_market_values[idx])
            day_rows.append(
                HoldingsRecord(
                    date=date_text[t],
                    symbol=symbols[idx],
                    shares=int(shares[idx]),
                    price=float(current_prices[idx]),
                    market_value=mv,
                    holding_ratio=float(mv / equity_after_trade),
                    is_cash=False,
                )
            )
        day_rows.sort(key=lambda item: item.holding_ratio, reverse=True)
        records.extend(day_rows)

        if price_matrix is not None and (t + 1) < t_len:
            next_prices = np.where(price_matrix[t + 1] > 1e-12, price_matrix[t + 1], current_prices)
        else:
            next_prices = current_prices * np.exp(r[t])
            next_prices = np.where(np.isfinite(next_prices) & (next_prices > 1e-12), next_prices, current_prices)
        equity_end = float(cash + np.dot(shares.astype(np.float64), next_prices))
        equity_end = max(equity_end, 1e-12)

        strategy_returns[t] = np.float32(np.log(equity_end / equity_before))
        current_prices = next_prices

    return (
        BacktestResult(
            strategy_returns=strategy_returns,
            benchmark_returns=b.astype(np.float32),
            turnovers=turnovers,
            weights_history=stock_weights_history,
        ),
        records,
    )
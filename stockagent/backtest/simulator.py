from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import torch

try:
    import cupy as cp
except Exception:  # pragma: no cover - optional GPU dependency
    cp = None


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
    target_weights = np.asarray(weights, dtype=np.float32).copy()
    target_weights[~tradable_mask.astype(bool)] = 0.0

    weight_sums = target_weights.sum(axis=1, keepdims=True)
    nonzero = weight_sums.squeeze(1) > 0
    target_weights[nonzero] /= weight_sums[nonzero]

    # Execute with one-day delay: signal at t is executed at t+1.
    weights_history = np.concatenate(
        [np.zeros((1, target_weights.shape[1]), dtype=np.float32), target_weights[:-1]],
        axis=0,
    )

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
    # Build target weights from model signal.
    target_weights = weights.float().clone()
    
    # Step 1: Mask non-tradable symbols
    target_weights = target_weights.masked_fill(~tradable_mask.bool(), 0.0)
    
    # Step 2: Normalize weights (simplified logic)
    weight_sums = target_weights.sum(dim=1, keepdim=True).clamp_min(1e-12)
    target_weights = target_weights / weight_sums

    # Step 3: Execute with one-day delay (signal at t executes at t+1)
    weights_history = torch.cat(
        [torch.zeros_like(target_weights[:1]), target_weights[:-1]],
        dim=0,
    )
    
    # Step 4: Compute turnover
    prev = torch.cat(
        [torch.zeros_like(weights_history[:1]), weights_history[:-1]],
        dim=0,
    )
    turnovers = (weights_history - prev).abs().sum(dim=1)

    # Step 5: Compute strategy returns
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


def run_backtest_cupy(
    weights,
    future_returns,
    tradable_mask,
    benchmark_returns,
    fee_per_side: float,
) -> BacktestResult:
    """GPU backtest core with CuPy, returned as numpy BacktestResult."""
    if cp is None:
        return run_backtest(
            np.asarray(weights),
            np.asarray(future_returns),
            np.asarray(tradable_mask),
            np.asarray(benchmark_returns),
            fee_per_side,
        )

    w = cp.asarray(weights, dtype=cp.float32).copy()
    r = cp.asarray(future_returns, dtype=cp.float32)
    m = cp.asarray(tradable_mask).astype(cp.bool_)
    b = cp.asarray(benchmark_returns, dtype=cp.float32)

    w = cp.where(m, w, cp.zeros_like(w))
    denom = cp.clip(w.sum(axis=1, keepdims=True), 1e-12, None)
    w = w / denom

    weights_history = cp.concatenate([cp.zeros_like(w[:1]), w[:-1]], axis=0)
    prev = cp.concatenate([cp.zeros_like(weights_history[:1]), weights_history[:-1]], axis=0)
    turnovers = cp.abs(weights_history - prev).sum(axis=1)
    gross = (weights_history * r).sum(axis=1)
    strategy_returns = gross - np.float32(fee_per_side) * turnovers

    return BacktestResult(
        strategy_returns=cp.asnumpy(strategy_returns.astype(cp.float32)),
        benchmark_returns=cp.asnumpy(b.astype(cp.float32)),
        turnovers=cp.asnumpy(turnovers.astype(cp.float32)),
        weights_history=cp.asnumpy(weights_history.astype(cp.float32)),
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
    min_fee: float = 20.0,
    execution_mode: str = "overnight_tplus2",
    lot_size: int = 1000,
    settlement_delay_days: int = 2,
    open_prices: np.ndarray | None = None,
    close_prices: np.ndarray | None = None,
    symbols: list[str] | None = None,
    dates: np.ndarray | None = None,
) -> tuple[BacktestResult, list[HoldingsRecord]]:
    """Integer-share backtest with mode switch for intraday vs overnight execution."""
    w = np.asarray(weights, dtype=np.float64)
    m = np.asarray(tradable_mask, dtype=bool)
    b = np.nan_to_num(np.asarray(benchmark_returns, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0)
    t_len, n_symbols = w.shape

    if lot_size <= 0:
        raise ValueError(f"lot_size must be positive, got {lot_size}")
    if settlement_delay_days < 0:
        raise ValueError(f"settlement_delay_days must be >= 0, got {settlement_delay_days}")
    if execution_mode not in {"intraday_next_open", "overnight_tplus2"}:
        raise ValueError(f"Unsupported execution_mode: {execution_mode}")

    if symbols is None:
        symbols = [f"SYM_{idx:04d}" for idx in range(n_symbols)]
    if dates is None:
        date_text = [f"t{idx:04d}" for idx in range(t_len)]
    else:
        date_text = [str(np.datetime_as_string(np.asarray(d, dtype="datetime64[D]"), unit="D")) for d in dates]

    close_matrix = None
    if close_prices is not None:
        close_matrix = np.nan_to_num(np.asarray(close_prices, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0)
        if close_matrix.shape != (t_len, n_symbols):
            raise ValueError(
                "close_prices shape must match (num_days, num_symbols): "
                f"expected {(t_len, n_symbols)}, got {close_matrix.shape}"
            )

    open_matrix = None
    if open_prices is not None:
        open_matrix = np.nan_to_num(np.asarray(open_prices, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0)
        if open_matrix.shape != (t_len, n_symbols):
            raise ValueError(
                "open_prices shape must match (num_days, num_symbols): "
                f"expected {(t_len, n_symbols)}, got {open_matrix.shape}"
            )

    if execution_mode == "intraday_next_open" and open_matrix is None:
        raise ValueError("intraday_next_open mode requires open_prices")

    strategy_returns = np.zeros(t_len, dtype=np.float32)
    turnovers = np.zeros(t_len, dtype=np.float32)
    weights_history = np.zeros((t_len, n_symbols), dtype=np.float32)
    records: list[HoldingsRecord] = []

    def _normalize_target(raw_target: np.ndarray, tradable: np.ndarray) -> np.ndarray:
        target = np.nan_to_num(raw_target, nan=0.0, posinf=0.0, neginf=0.0)
        target = np.clip(target, 0.0, None)
        target[~tradable] = 0.0
        total = float(target.sum())
        if total > 1.0:
            target /= total
        return target

    def _commission_per_symbol(notional_by_symbol: np.ndarray, fee_rate: float) -> float:
        valid = notional_by_symbol > 0.0
        if not np.any(valid):
            return 0.0
        fees = np.maximum(float(min_fee), notional_by_symbol[valid] * fee_rate)
        return float(fees.sum())

    cash = float(initial_capital)

    bankrupt = False
    bankruptcy_day = None
    # 只要破產，後續全部歸零
    def _force_bankrupt(t):
        nonlocal cash, close_holdings, pending_settlement
        cash = 0.0
        close_holdings[:] = 0
        pending_settlement[:] = 0.0
        return t

    if execution_mode == "intraday_next_open":
        pending_target_w = np.zeros(n_symbols, dtype=np.float64)
        for t in range(t_len):
            open_t = open_matrix[t]
            close_t = close_matrix[t] if close_matrix is not None else open_t
            tradable_today = m[t] & np.isfinite(open_t) & (open_t > 1e-12) & np.isfinite(close_t) & (close_t > 1e-12)

            equity_before = max(float(cash), 1e-12)
            target_w = _normalize_target(pending_target_w, tradable_today)

            desired_value = equity_before * target_w
            per_lot_cost = open_t * float(lot_size)
            lots = np.floor(np.divide(desired_value, per_lot_cost, out=np.zeros_like(desired_value), where=per_lot_cost > 0)).astype(np.int64)
            shares = lots * int(lot_size)

            buy_notional_by_symbol = shares.astype(np.float64) * open_t
            buy_notional = float(buy_notional_by_symbol.sum())
            buy_fee = _commission_per_symbol(buy_notional_by_symbol, buy_fee_rate)

            if buy_notional + buy_fee > cash and buy_notional > 0.0:
                scale = max(0.0, (cash - buy_fee) / max(buy_notional, 1e-12))
                scaled_lots = np.floor(lots.astype(np.float64) * scale).astype(np.int64)
                shares = scaled_lots * int(lot_size)
                buy_notional_by_symbol = shares.astype(np.float64) * open_t
                buy_notional = float(buy_notional_by_symbol.sum())
                buy_fee = _commission_per_symbol(buy_notional_by_symbol, buy_fee_rate)

            sell_notional_by_symbol = shares.astype(np.float64) * close_t
            sell_notional = float(sell_notional_by_symbol.sum())
            sell_fee = _commission_per_symbol(sell_notional_by_symbol, sell_fee_rate)

            cash = cash - buy_notional - buy_fee + sell_notional - sell_fee
            cash = max(cash, 0.0)

            traded_notional = buy_notional + sell_notional
            turnovers[t] = float(traded_notional / equity_before)
            strategy_returns[t] = np.float32(np.log(max(cash, 1e-12) / equity_before))
            weights_history[t] = 0.0

            records.append(
                HoldingsRecord(
                    date=date_text[t],
                    symbol="CASH",
                    shares=0,
                    price=1.0,
                    market_value=float(cash),
                    holding_ratio=1.0,
                    is_cash=True,
                )
            )

            pending_target_w = _normalize_target(w[t], m[t])

    else:
        close_holdings = np.zeros(n_symbols, dtype=np.int64)
        pending_settlement = np.zeros(t_len + settlement_delay_days + 2, dtype=np.float64)
        total_equity_prev = float(initial_capital)


        for t in range(t_len):
            if bankrupt:
                strategy_returns[t] = -np.inf
                turnovers[t] = 0.0
                weights_history[t] = 0.0
                # 只記錄現金歸零
                records.append(
                    HoldingsRecord(
                        date=date_text[t],
                        symbol="CASH",
                        shares=0,
                        price=1.0,
                        market_value=0.0,
                        holding_ratio=1.0,
                        is_cash=True,
                    )
                )
                continue

            close_t = close_matrix[t] if close_matrix is not None else np.ones(n_symbols, dtype=np.float64)
            tradable_today = m[t] & np.isfinite(close_t) & (close_t > 1e-12)

            if pending_settlement[t] > 0.0:
                cash += float(pending_settlement[t])

            # 破產判斷：T日開盤前現金<0，或任何已到期交割金額未能支付
            # 允許交割日前一天補足現金，只要交割當天開盤前現金>=0就不算破產
            if cash < -1e-6:
                bankrupt = True
                bankruptcy_day = t
                _force_bankrupt(t)
                strategy_returns[t] = -np.inf
                turnovers[t] = 0.0
                weights_history[t] = 0.0
                records.append(
                    HoldingsRecord(
                        date=date_text[t],
                        symbol="CASH",
                        shares=0,
                        price=1.0,
                        market_value=0.0,
                        holding_ratio=1.0,
                        is_cash=True,
                    )
                )
                continue

            sellable = (close_holdings > 0) & tradable_today
            sell_shares = np.where(sellable, close_holdings, 0)
            sell_notional_by_symbol = sell_shares.astype(np.float64) * close_t
            sell_notional = float(sell_notional_by_symbol.sum())
            sell_fee = _commission_per_symbol(sell_notional_by_symbol, sell_fee_rate)
            net_sell_cash = max(0.0, sell_notional - sell_fee)
            due_idx = min(t + settlement_delay_days, pending_settlement.shape[0] - 1)
            pending_settlement[due_idx] += net_sell_cash
            close_holdings[sellable] = 0

            buy_target = _normalize_target(w[t], tradable_today)
            buy_budget = max(cash, 0.0)
            desired_value = buy_budget * buy_target
            per_lot_cost = close_t * float(lot_size)
            lots = np.floor(np.divide(desired_value, per_lot_cost, out=np.zeros_like(desired_value), where=per_lot_cost > 0)).astype(np.int64)
            buy_shares = lots * int(lot_size)

            buy_notional_by_symbol = buy_shares.astype(np.float64) * close_t
            buy_notional = float(buy_notional_by_symbol.sum())
            buy_fee = _commission_per_symbol(buy_notional_by_symbol, buy_fee_rate)
            if buy_notional + buy_fee > cash and buy_notional > 0.0:
                scale = max(0.0, (cash - buy_fee) / max(buy_notional, 1e-12))
                scaled_lots = np.floor(lots.astype(np.float64) * scale).astype(np.int64)
                buy_shares = scaled_lots * int(lot_size)
                buy_notional_by_symbol = buy_shares.astype(np.float64) * close_t
                buy_notional = float(buy_notional_by_symbol.sum())
                buy_fee = _commission_per_symbol(buy_notional_by_symbol, buy_fee_rate)

            close_holdings += buy_shares
            cash = cash - buy_notional - buy_fee
            cash = max(cash, 0.0)

            holdings_mv = float(np.dot(close_holdings.astype(np.float64), close_t))
            receivable = float(pending_settlement[t + 1 :].sum())
            total_equity_now = max(cash + holdings_mv + receivable, 1e-12)
            strategy_returns[t] = np.float32(np.log(total_equity_now / max(total_equity_prev, 1e-12)))
            total_equity_prev = total_equity_now

            traded_notional = buy_notional + sell_notional
            turnovers[t] = float(traded_notional / max(total_equity_now, 1e-12))
            weights_history[t] = (close_holdings.astype(np.float64) * close_t / total_equity_now).astype(np.float32)

            day_rows: list[HoldingsRecord] = []
            day_rows.append(
                HoldingsRecord(
                    date=date_text[t],
                    symbol="CASH",
                    shares=0,
                    price=1.0,
                    market_value=float(cash),
                    holding_ratio=float(cash / total_equity_now),
                    is_cash=True,
                )
            )
            if receivable > 0.0:
                day_rows.append(
                    HoldingsRecord(
                        date=date_text[t],
                        symbol="RECEIVABLE",
                        shares=0,
                        price=1.0,
                        market_value=receivable,
                        holding_ratio=float(receivable / total_equity_now),
                        is_cash=True,
                    )
                )
            nonzero = np.flatnonzero(close_holdings > 0)
            for idx in nonzero.tolist():
                mv = float(close_holdings[idx] * close_t[idx])
                day_rows.append(
                    HoldingsRecord(
                        date=date_text[t],
                        symbol=symbols[idx],
                        shares=int(close_holdings[idx]),
                        price=float(close_t[idx]),
                        market_value=mv,
                        holding_ratio=float(mv / total_equity_now),
                        is_cash=False,
                    )
                )
            day_rows.sort(key=lambda item: item.holding_ratio, reverse=True)
            records.extend(day_rows)

    return (
        BacktestResult(
            strategy_returns=strategy_returns,
            benchmark_returns=b.astype(np.float32),
            turnovers=turnovers,
            weights_history=weights_history,
        ),
        records,
    )
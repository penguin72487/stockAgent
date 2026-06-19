from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(slots=True)
class DriftResult:
    weights: np.ndarray
    simple_return: float
    log_return: float
    nav_ratio: float
    valid_price_count: int


def estimate_drifted_weights(
    previous_weights: np.ndarray,
    base_prices: np.ndarray,
    current_prices: np.ndarray,
) -> DriftResult:
    """Mark previous signed portfolio weights to current prices."""
    prev = np.nan_to_num(np.asarray(previous_weights, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0)
    base = np.asarray(base_prices, dtype=np.float64)
    current = np.asarray(current_prices, dtype=np.float64)

    ratio = np.ones_like(prev, dtype=np.float64)
    valid = np.isfinite(base) & np.isfinite(current) & (base > 0.0) & (current > 0.0)
    ratio[valid] = current[valid] / base[valid]

    simple_returns = ratio - 1.0
    portfolio_simple_return = float(np.sum(prev * simple_returns, dtype=np.float64))
    nav_ratio = 1.0 + portfolio_simple_return
    if not np.isfinite(nav_ratio) or abs(nav_ratio) <= 1e-12:
        drifted = prev.copy()
        nav_ratio = 1.0
        portfolio_simple_return = 0.0
    else:
        drifted = prev * ratio / nav_ratio

    log_return = float(np.log1p(portfolio_simple_return)) if portfolio_simple_return > -1.0 else float("nan")
    return DriftResult(
        weights=drifted.astype(np.float64, copy=False),
        simple_return=portfolio_simple_return,
        log_return=log_return,
        nav_ratio=float(nav_ratio),
        valid_price_count=int(valid.sum()),
    )


def estimate_benchmark_return(
    symbols: list[str],
    benchmark_name: str | None,
    base_prices: np.ndarray,
    current_prices: np.ndarray,
    tradable_mask: np.ndarray | None = None,
) -> float:
    """Estimate a simple benchmark return from current prices."""
    base = np.asarray(base_prices, dtype=np.float64)
    current = np.asarray(current_prices, dtype=np.float64)
    valid = np.isfinite(base) & np.isfinite(current) & (base > 0.0) & (current > 0.0)
    simple = np.zeros_like(base, dtype=np.float64)
    simple[valid] = current[valid] / base[valid] - 1.0

    key = (benchmark_name or "").strip().upper()
    aliases = {key, key.replace(".", ""), key.replace("-", ""), key.replace("_", "")}
    for idx, symbol in enumerate(symbols):
        normalized = str(symbol).strip().upper().replace(".", "").replace("-", "").replace("_", "")
        if normalized in aliases and valid[idx]:
            return float(simple[idx])

    if tradable_mask is not None:
        valid &= np.asarray(tradable_mask, dtype=bool)
    if not bool(valid.any()):
        return 0.0
    return float(np.mean(simple[valid], dtype=np.float64))


def build_rebalance_rows(
    symbols: list[str],
    current_weights: np.ndarray,
    target_weights: np.ndarray,
    current_prices: np.ndarray,
    base_prices: np.ndarray,
    *,
    symbol_names: dict[str, str] | None = None,
    min_abs_delta: float = 0.0,
) -> list[dict[str, float | str]]:
    current = np.asarray(current_weights, dtype=np.float64)
    target = np.asarray(target_weights, dtype=np.float64)
    prices = np.asarray(current_prices, dtype=np.float64)
    base = np.asarray(base_prices, dtype=np.float64)
    delta = target - current
    rows: list[dict[str, float | str]] = []
    for idx, symbol in enumerate(symbols):
        abs_delta = abs(float(delta[idx]))
        if abs_delta < float(min_abs_delta):
            continue
        base_price = float(base[idx]) if np.isfinite(base[idx]) else float("nan")
        current_price = float(prices[idx]) if np.isfinite(prices[idx]) else float("nan")
        simple_return = (
            current_price / base_price - 1.0
            if np.isfinite(base_price) and np.isfinite(current_price) and base_price > 0.0
            else float("nan")
        )
        rows.append(
            {
                "symbol": str(symbol),
                "name": str((symbol_names or {}).get(str(symbol), "")),
                "current_weight": float(current[idx]),
                "target_weight": float(target[idx]),
                "delta_weight": float(delta[idx]),
                "abs_delta_weight": abs_delta,
                "trade_price": current_price,
                "current_price": current_price,
                "base_price": base_price,
                "price_return": float(simple_return),
            }
        )
    rows.sort(key=lambda row: float(row["abs_delta_weight"]), reverse=True)
    return rows


def top_weight_rows(
    symbols: list[str],
    weights: np.ndarray,
    current_prices: np.ndarray,
    *,
    symbol_names: dict[str, str] | None = None,
    top_n: int,
) -> list[dict[str, float | str]]:
    arr = np.asarray(weights, dtype=np.float64)
    prices = np.asarray(current_prices, dtype=np.float64)
    order = np.argsort(-np.abs(arr))
    rows: list[dict[str, float | str]] = []
    for idx in order[: max(0, int(top_n))]:
        rows.append(
            {
                "symbol": str(symbols[int(idx)]),
                "name": str((symbol_names or {}).get(str(symbols[int(idx)]), "")),
                "weight": float(arr[int(idx)]),
                "abs_weight": abs(float(arr[int(idx)])),
                "current_price": float(prices[int(idx)]) if np.isfinite(prices[int(idx)]) else float("nan"),
            }
        )
    return rows

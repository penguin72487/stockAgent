from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt

from stockagent.backtest.simulator import BacktestResult


def _safe_expm1(log_sum: float) -> float:
    """expm1 that returns inf instead of raising overflow for very large log sums."""
    if log_sum >= 709.78:   # math.log(float_max) ≈ 709.78
        return math.inf
    if log_sum <= -745.13:  # underflow → -1
        return -1.0
    return math.expm1(log_sum)



def compute_god_mode_returns(
    future_log_returns: np.ndarray,
    tradable_mask: np.ndarray,
) -> np.ndarray:
    """Theoretical maximum: each day pick the single tradable stock with highest return.

    Args:
        future_log_returns: [T, S] log returns
        tradable_mask:       [T, S] bool mask

    Returns:
        god_returns: [T] log returns of perfect daily selection
    """
    masked = np.where(tradable_mask.astype(bool), future_log_returns, -np.inf)
    god = np.max(masked, axis=1).astype(np.float32)  # [T]
    god = np.where(np.isneginf(god), 0.0, god)  # no tradable stocks → 0
    return god


def compute_metrics(result: BacktestResult) -> dict[str, float]:
    """Compute portfolio performance metrics from a BacktestResult.

    Returns a dict with:
        cumulative_return, annualized_return, sharpe, max_drawdown,
        turnover, daily_hit_rate, excess_return_vs_universe_average,
        cumulative_benchmark
    """
    r = np.nan_to_num(result.strategy_returns, nan=0.0)
    b = np.nan_to_num(result.benchmark_returns, nan=0.0)

    # r and b are log returns; cumulative = exp(sum) - 1
    cum_r = _safe_expm1(float(r.sum()))
    cum_b = _safe_expm1(float(b.sum()))

    avg = float(r.mean())
    std = float(r.std(ddof=0))
    ann_r = _safe_expm1(float(avg * 252.0))
    sharpe = float(avg / std * math.sqrt(252.0)) if std > 0 else 0.0

    equity = np.exp(np.cumsum(r.astype(np.float64)))
    running_max = np.maximum.accumulate(equity)
    dd = equity / np.clip(running_max, 1e-12, None) - 1.0
    max_dd = float(dd.min(initial=0.0))

    return {
        "cumulative_return": cum_r,
        "annualized_return": ann_r,
        "sharpe": sharpe,
        "max_drawdown": max_dd,
        "turnover": float(result.turnovers.mean()) if result.turnovers.size else 0.0,
        "daily_hit_rate": float((r > 0).mean()) if r.size else 0.0,
        "excess_return_vs_universe_average": cum_r - cum_b,
        "cumulative_benchmark": cum_b,
    }


def compute_metrics_by_year(
    result: BacktestResult,
    dates: np.ndarray,
    god_returns: np.ndarray | None = None,
) -> dict[int, dict[str, float]]:
    """Compute annual performance metrics.

    Args:
        result: BacktestResult containing daily returns
        dates: numpy array of datetime64 (shape [T])
        god_returns: optional [T] log returns for theoretical max (god mode)

    Returns:
        dict mapping year -> annual metrics
    """
    r = np.nan_to_num(result.strategy_returns, nan=0.0)
    b = np.nan_to_num(result.benchmark_returns, nan=0.0)
    g = np.nan_to_num(god_returns, nan=0.0) if god_returns is not None else None

    # Extract year from dates
    years = np.asarray(dates, dtype='datetime64[D]').astype(object)
    years = np.array([d.year for d in years])

    annual_metrics = {}
    for year in np.unique(years):
        mask = years == year
        r_year = r[mask]
        b_year = b[mask]
        turnover_year = result.turnovers[mask]

        cum_r = _safe_expm1(float(r_year.sum()))
        cum_b = _safe_expm1(float(b_year.sum()))
        avg = float(r_year.mean())
        std = float(r_year.std(ddof=0)) + 1e-8
        ann_r = _safe_expm1(float(avg * 252.0))
        sharpe = float(avg / std * math.sqrt(252.0))

        equity = np.exp(np.cumsum(r_year.astype(np.float64)))
        running_max = np.maximum.accumulate(equity)
        dd = equity / np.clip(running_max, 1e-12, None) - 1.0
        max_dd = float(dd.min(initial=0.0))

        entry: dict[str, float] = {
            "cumulative_return": cum_r,
            "annualized_return": ann_r,
            "sharpe": sharpe,
            "max_drawdown": max_dd,
            "turnover": float(turnover_year.mean()),
            "daily_hit_rate": float((r_year > 0).mean()),
            "excess_return_vs_universe_average": cum_r - cum_b,
            "cumulative_benchmark": cum_b,
        }
        if g is not None:
            entry["god_cumulative_return"] = _safe_expm1(float(g[mask].sum()))
        annual_metrics[int(year)] = entry

    return annual_metrics


def generate_annual_report(
    result: BacktestResult,
    dates: np.ndarray,
    output_path: str | None = None,
    god_returns: np.ndarray | None = None,
) -> str:
    """Generate a text report of annual performance.

    Args:
        result: BacktestResult
        dates: datetime64 array [T]
        output_path: optional file path to save report
        god_returns: optional [T] log returns for theoretical max (god mode)

    Returns:
        formatted report string
    """
    annual_metrics = compute_metrics_by_year(result, dates, god_returns=god_returns)
    has_god = god_returns is not None

    # Column widths: Year(8) Strategy(12) Baseline(12) Excess(12) Sharpe(10) MaxDD(10) Turnover(10) [GodMode(14)]
    width = 97 if not has_god else 112
    lines = ["Annual Performance Report", "=" * width]
    header = (
        f"{'Year':<8} {'Strategy':>12} {'Baseline':>12} {'Excess':>12} "
        f"{'Sharpe':>10} {'Max DD':>10} {'Turnover':>10}"
    )
    if has_god:
        header += f" {'GodMode':>14}"
    lines.append(header)
    lines.append("-" * width)

    # Accumulate totals for summary row
    strat_log_sum = 0.0
    bench_log_sum = 0.0
    god_log_sum = 0.0

    r_all = np.nan_to_num(result.strategy_returns, nan=0.0)
    b_all = np.nan_to_num(result.benchmark_returns, nan=0.0)
    g_all = np.nan_to_num(god_returns, nan=0.0) if has_god else None

    for year in sorted(annual_metrics.keys()):
        m = annual_metrics[year]
        row = (
            f"{year:<8} {m['cumulative_return']:>11.2%} {m['cumulative_benchmark']:>12.2%} "
            f"{m['excess_return_vs_universe_average']:>12.2%} "
            f"{m['sharpe']:>10.3f} {m['max_drawdown']:>10.2%} {m['turnover']:>10.4f}"
        )
        if has_god:
            row += f" {m.get('god_cumulative_return', float('nan')):>13.2%}"
        lines.append(row)

    # --- Summary row (full-period) ---
    lines.append("=" * width)
    cum_r_total = _safe_expm1(float(r_all.sum()))
    cum_b_total = _safe_expm1(float(b_all.sum()))
    avg = float(r_all.mean())
    std = float(r_all.std(ddof=0)) + 1e-8
    sharpe_total = float(avg / std * math.sqrt(252.0))
    equity_total = np.exp(np.cumsum(r_all.astype(np.float64)))
    dd_total = equity_total / np.maximum.accumulate(equity_total).clip(1e-12) - 1.0
    max_dd_total = float(dd_total.min(initial=0.0))
    turnover_total = float(result.turnovers.mean()) if result.turnovers.size else 0.0

    summary_row = (
        f"{'TOTAL':<8} {cum_r_total:>11.2%} {cum_b_total:>12.2%} "
        f"{cum_r_total - cum_b_total:>12.2%} "
        f"{sharpe_total:>10.3f} {max_dd_total:>10.2%} {turnover_total:>10.4f}"
    )
    if has_god:
        cum_g_total = _safe_expm1(float(g_all.sum()))
        summary_row += f" {cum_g_total:>13.2%}"
    lines.append(summary_row)

    report = "\n".join(lines)

    if output_path:
        with open(output_path, "w") as f:
            f.write(report)

    return report


def plot_annual_performance(
    result: BacktestResult,
    dates: np.ndarray,
    output_path: str | Path | None = None,
) -> None:
    """Plot annual performance comparison: strategy vs benchmark.
    
    Args:
        result: BacktestResult
        dates: datetime64 array [T]
        output_path: optional file path to save figure
    """
    annual_metrics = compute_metrics_by_year(result, dates)
    years = sorted(annual_metrics.keys())
    
    strategy_returns = [annual_metrics[y]["cumulative_return"] for y in years]
    benchmark_returns = [annual_metrics[y]["cumulative_benchmark"] for y in years]
    sharpe_ratios = [annual_metrics[y]["sharpe"] for y in years]
    
    fig, axes = plt.subplots(2, 1, figsize=(12, 8))
    
    # Panel 1: Annual returns
    ax = axes[0]
    x = np.arange(len(years))
    width = 0.35
    ax.bar(x - width/2, strategy_returns, width, label="Strategy", alpha=0.8)
    ax.bar(x + width/2, benchmark_returns, width, label="Benchmark", alpha=0.8)
    ax.set_xlabel("Year")
    ax.set_ylabel("Annual Return")
    ax.set_title("Annual Returns: Strategy vs Benchmark")
    ax.set_xticks(x)
    ax.set_xticklabels(years)
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    ax.axhline(y=0, color="black", linestyle="-", linewidth=0.5)
    
    # Panel 2: Sharpe ratio by year
    ax = axes[1]
    ax.plot(years, sharpe_ratios, marker="o", linewidth=2, markersize=8, label="Sharpe Ratio")
    ax.set_xlabel("Year")
    ax.set_ylabel("Sharpe Ratio")
    ax.set_title("Annual Sharpe Ratio")
    ax.grid(True, alpha=0.3)
    ax.axhline(y=0, color="red", linestyle="--", linewidth=1)
    ax.legend()
    
    plt.tight_layout()
    
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved plot to {output_path}")
    plt.close()


def plot_equity_curve(
    result: BacktestResult,
    dates: np.ndarray,
    output_path: str | Path | None = None,
    god_returns: np.ndarray | None = None,
) -> None:
    """Plot cumulative equity curve for strategy and benchmark.

    Args:
        result: BacktestResult
        dates: datetime64 array [T]
        output_path: optional file path to save figure
        god_returns: optional [T] log returns for theoretical max (god mode)
    """
    r = np.nan_to_num(result.strategy_returns, nan=0.0)
    b = np.nan_to_num(result.benchmark_returns, nan=0.0)

    strategy_equity = np.exp(np.cumsum(r.astype(np.float64)))
    benchmark_equity = np.exp(np.cumsum(b.astype(np.float64)))

    fig, ax = plt.subplots(figsize=(14, 7))
    ax.plot(dates, strategy_equity, label="Strategy", linewidth=2, alpha=0.8)
    ax.plot(dates, benchmark_equity, label="Benchmark", linewidth=2, alpha=0.8)
    if god_returns is not None:
        g = np.nan_to_num(god_returns, nan=0.0)
        god_equity = np.exp(np.cumsum(g.astype(np.float64)))
        ax.plot(dates, god_equity, label="God Mode (theoretical max)", linewidth=1.5,
                linestyle="--", color="gold", alpha=0.9)
    ax.set_xlabel("Date")
    ax.set_ylabel("Cumulative Value (starting at 1.0)")
    ax.set_title("Strategy vs Benchmark Equity Curve")
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved plot to {output_path}")
    plt.close()


def plot_equity_curve_log(
    result: BacktestResult,
    dates: np.ndarray,
    output_path: str | Path | None = None,
    god_returns: np.ndarray | None = None,
) -> None:
    """Plot cumulative equity curve for strategy and benchmark (log scale Y-axis).

    Args:
        result: BacktestResult
        dates: datetime64 array [T]
        output_path: optional file path to save figure
        god_returns: optional [T] log returns for theoretical max (god mode)
    """
    r = np.nan_to_num(result.strategy_returns, nan=0.0)
    b = np.nan_to_num(result.benchmark_returns, nan=0.0)

    strategy_equity = np.exp(np.cumsum(r.astype(np.float64)))
    benchmark_equity = np.exp(np.cumsum(b.astype(np.float64)))

    fig, ax = plt.subplots(figsize=(14, 7))
    ax.plot(dates, strategy_equity, label="Strategy", linewidth=2, alpha=0.8)
    ax.plot(dates, benchmark_equity, label="Benchmark", linewidth=2, alpha=0.8)
    if god_returns is not None:
        g = np.nan_to_num(god_returns, nan=0.0)
        god_equity = np.exp(np.cumsum(g.astype(np.float64)))
        ax.plot(dates, god_equity, label="God Mode (theoretical max)", linewidth=1.5,
                linestyle="--", color="gold", alpha=0.9)
    ax.set_xlabel("Date")
    ax.set_ylabel("Cumulative Value (log scale, starting at 1.0)")
    ax.set_yscale("log")
    ax.set_title("Strategy vs Benchmark Equity Curve (Log Scale)")
    ax.legend()
    ax.grid(True, alpha=0.3, which="both")
    
    plt.tight_layout()
    
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved plot to {output_path}")
    plt.close()


def plot_fold_first_year_returns(
    fold_ids: list[int],
    first_years: list[int],
    strategy_first_year_log_returns: list[float],
    baseline_first_year_log_returns: list[float],
    output_path: str | Path | None = None,
) -> None:
    """Plot single-chart yearly cumulative return (chained) from fold first test years."""
    if not fold_ids:
        return

    per_year: dict[int, tuple[float, float]] = {}
    for _, year, strat_log, base_log in sorted(
        zip(fold_ids, first_years, strategy_first_year_log_returns, baseline_first_year_log_returns),
        key=lambda item: item[1],
    ):
        # If multiple folds map to the same first-year, keep the latest one in sorted order.
        per_year[int(year)] = (float(strat_log), float(base_log))

    years = sorted(per_year.keys())
    strat_logs = np.array([per_year[y][0] for y in years], dtype=np.float64)
    base_logs = np.array([per_year[y][1] for y in years], dtype=np.float64)

    strat_nav = np.exp(np.cumsum(strat_logs))
    base_nav = np.exp(np.cumsum(base_logs))

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(years, strat_nav, marker="o", linewidth=2.2, label="Strategy")
    ax.plot(years, base_nav, marker="o", linewidth=2.2, label="Baseline")
    ax.set_xticks(years)
    ax.set_xlabel("Year")
    ax.set_ylabel("Cumulative NAV (log scale, start=1)")
    ax.set_title("Walkforward First-Test-Year Cumulative NAV (Chained by Year)")
    ax.set_yscale("log")
    ax.axhline(y=1.0, color="black", linewidth=0.8)
    ax.grid(True, alpha=0.3, which="both")
    ax.legend()

    plt.tight_layout()
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved plot to {output_path}")
    plt.close()

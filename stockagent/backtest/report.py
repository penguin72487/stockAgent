from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt

from stockagent.backtest.simulator import BacktestResult


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
    cum_r = float(np.expm1(r.sum()))
    cum_b = float(np.expm1(b.sum()))

    avg = float(r.mean())
    std = float(r.std(ddof=0))
    ann_r = float(np.expm1(avg * 252.0))
    sharpe = float(avg / std * math.sqrt(252.0)) if std > 0 else 0.0

    equity = np.exp(np.cumsum(r))
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
) -> dict[int, dict[str, float]]:
    """Compute annual performance metrics.
    
    Args:
        result: BacktestResult containing daily returns
        dates: numpy array of datetime64 (shape [T])
        
    Returns:
        dict mapping year -> annual metrics
    """
    r = np.nan_to_num(result.strategy_returns, nan=0.0)
    b = np.nan_to_num(result.benchmark_returns, nan=0.0)
    
    # Extract year from dates
    years = np.asarray(dates, dtype='datetime64[D]').astype(object)
    years = np.array([d.year for d in years])
    
    annual_metrics = {}
    for year in np.unique(years):
        mask = years == year
        r_year = r[mask]
        b_year = b[mask]
        turnover_year = result.turnovers[mask]
        
        cum_r = float(np.expm1(r_year.sum()))
        cum_b = float(np.expm1(b_year.sum()))
        avg = float(r_year.mean())
        std = float(r_year.std(ddof=0)) + 1e-8
        ann_r = float(np.expm1(avg * 252.0))
        sharpe = float(avg / std * math.sqrt(252.0))
        
        equity = np.exp(np.cumsum(r_year))
        running_max = np.maximum.accumulate(equity)
        dd = equity / np.clip(running_max, 1e-12, None) - 1.0
        max_dd = float(dd.min(initial=0.0))
        
        annual_metrics[int(year)] = {
            "cumulative_return": cum_r,
            "annualized_return": ann_r,
            "sharpe": sharpe,
            "max_drawdown": max_dd,
            "turnover": float(turnover_year.mean()),
            "daily_hit_rate": float((r_year > 0).mean()),
            "excess_return_vs_universe_average": cum_r - cum_b,
            "cumulative_benchmark": cum_b,
        }
    
    return annual_metrics


def generate_annual_report(
    result: BacktestResult,
    dates: np.ndarray,
    output_path: str | None = None,
) -> str:
    """Generate a text report of annual performance.
    
    Args:
        result: BacktestResult
        dates: datetime64 array [T]
        output_path: optional file path to save report
        
    Returns:
        formatted report string
    """
    annual_metrics = compute_metrics_by_year(result, dates)
    
    lines = ["Annual Performance Report", "=" * 80]
    lines.append(f"{'Year':<8} {'Return':>12} {'Sharpe':>10} {'Max DD':>10} {'Turnover':>10} {'Excess':>12}")
    lines.append("-" * 80)
    
    for year in sorted(annual_metrics.keys()):
        m = annual_metrics[year]
        lines.append(
            f"{year:<8} {m['cumulative_return']:>11.2%} {m['sharpe']:>10.3f} "
            f"{m['max_drawdown']:>10.2%} {m['turnover']:>10.4f} {m['excess_return_vs_universe_average']:>12.2%}"
        )
    
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
) -> None:
    """Plot cumulative equity curve for strategy and benchmark.
    
    Args:
        result: BacktestResult
        dates: datetime64 array [T]
        output_path: optional file path to save figure
    """
    r = np.nan_to_num(result.strategy_returns, nan=0.0)
    b = np.nan_to_num(result.benchmark_returns, nan=0.0)
    
    strategy_equity = np.exp(np.cumsum(r))
    benchmark_equity = np.exp(np.cumsum(b))
    
    fig, ax = plt.subplots(figsize=(14, 7))
    ax.plot(dates, strategy_equity, label="Strategy", linewidth=2, alpha=0.8)
    ax.plot(dates, benchmark_equity, label="Benchmark", linewidth=2, alpha=0.8)
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
) -> None:
    """Plot cumulative equity curve for strategy and benchmark (log scale Y-axis).
    
    Args:
        result: BacktestResult
        dates: datetime64 array [T]
        output_path: optional file path to save figure
    """
    r = np.nan_to_num(result.strategy_returns, nan=0.0)
    b = np.nan_to_num(result.benchmark_returns, nan=0.0)
    
    strategy_equity = np.exp(np.cumsum(r))
    benchmark_equity = np.exp(np.cumsum(b))
    
    fig, ax = plt.subplots(figsize=(14, 7))
    ax.plot(dates, strategy_equity, label="Strategy", linewidth=2, alpha=0.8)
    ax.plot(dates, benchmark_equity, label="Benchmark", linewidth=2, alpha=0.8)
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

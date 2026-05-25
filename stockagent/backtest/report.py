from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

from stockagent.backtest.simulator import BacktestResult


_PLOT_LOG_MIN = -745.0
_PLOT_LOG_MAX = 600.0


def _safe_expm1(log_sum: float) -> float:
    """expm1 that returns inf instead of raising overflow for very large log sums."""
    if log_sum >= 709.78:   # math.log(float_max) ≈ 709.78
        return math.inf
    if log_sum <= -745.13:  # underflow → -1
        return -1.0
    return math.expm1(log_sum)


def _safe_equity_for_plot(log_returns: np.ndarray) -> np.ndarray:
    """Build a plot-safe equity curve from log returns."""
    clean = np.nan_to_num(log_returns, nan=0.0).astype(np.float64)
    cum_log = np.cumsum(clean)
    # Keep plotting range finite to avoid matplotlib overflow warnings.
    return np.exp(np.clip(cum_log, _PLOT_LOG_MIN, _PLOT_LOG_MAX))


def _configure_log_y_axis(ax: plt.Axes, *series: np.ndarray) -> None:
    """Configure a stable log-scale Y axis for very wide equity ranges."""
    positive = [
        np.asarray(values, dtype=np.float64)[np.asarray(values, dtype=np.float64) > 0.0]
        for values in series
    ]
    positive = [values[np.isfinite(values)] for values in positive if values.size]
    if not positive:
        return

    all_positive = np.concatenate(positive)
    y_min = float(all_positive.min(initial=1.0))
    y_max = float(all_positive.max(initial=1.0))
    if not np.isfinite(y_min) or not np.isfinite(y_max) or y_min <= 0.0 or y_max <= 0.0:
        return

    ax.set_yscale("log")
    ax.set_ylim(y_min, y_max)

    min_exp = int(np.floor(np.log10(y_min)))
    max_exp = int(np.ceil(np.log10(y_max)))
    span = max_exp - min_exp
    step = max(1, int(np.ceil(span / 12)))
    exponents = np.arange(min_exp, max_exp + 1, step, dtype=np.int32)
    ticks = np.power(10.0, exponents.astype(np.float64))

    ax.yaxis.set_major_locator(mticker.FixedLocator(ticks))
    ax.yaxis.set_major_formatter(mticker.LogFormatterSciNotation(base=10.0))
    ax.yaxis.set_minor_locator(mticker.NullLocator())


def _max_drawdown_from_log_returns(log_returns: np.ndarray) -> float:
    """Compute max drawdown in log-space without unstable exp/divide operations."""
    clean = np.nan_to_num(log_returns, nan=0.0).astype(np.float64)
    cum_log = np.cumsum(clean)
    running_max_log = np.maximum.accumulate(cum_log)
    dd = np.expm1(np.clip(cum_log - running_max_log, _PLOT_LOG_MIN, 0.0))
    return float(dd.min(initial=0.0))


def compute_metrics(result: BacktestResult) -> dict[str, float]:
    """Compute portfolio performance metrics from a BacktestResult.

    Returns a dict with:
        cumulative_return, annualized_return, sharpe, baseline_sharpe, max_drawdown,
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
    avg_b = float(b.mean())
    std_b = float(b.std(ddof=0))
    ann_r = _safe_expm1(float(avg * 252.0))
    sharpe = float(avg / std * math.sqrt(252.0)) if std > 0 else 0.0
    baseline_sharpe = float(avg_b / std_b * math.sqrt(252.0)) if std_b > 0 else 0.0

    max_dd = _max_drawdown_from_log_returns(r)

    return {
        "cumulative_return": cum_r,
        "annualized_return": ann_r,
        "sharpe": sharpe,
        "baseline_sharpe": baseline_sharpe,
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

        cum_r = _safe_expm1(float(r_year.sum()))
        cum_b = _safe_expm1(float(b_year.sum()))
        avg = float(r_year.mean())
        std = float(r_year.std(ddof=0)) + 1e-8
        avg_b = float(b_year.mean())
        std_b = float(b_year.std(ddof=0)) + 1e-8
        ann_r = _safe_expm1(float(avg * 252.0))
        sharpe = float(avg / std * math.sqrt(252.0))
        baseline_sharpe = float(avg_b / std_b * math.sqrt(252.0))

        max_dd = _max_drawdown_from_log_returns(r_year)

        entry: dict[str, float] = {
            "cumulative_return": cum_r,
            "annualized_return": ann_r,
            "sharpe": sharpe,
            "baseline_sharpe": baseline_sharpe,
            "max_drawdown": max_dd,
            "turnover": float(turnover_year.mean()),
            "daily_hit_rate": float((r_year > 0).mean()),
            "excess_return_vs_universe_average": cum_r - cum_b,
            "cumulative_benchmark": cum_b,
        }
        annual_metrics[int(year)] = entry

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

    # Column widths: Year(8) Strategy(12) Baseline(12) Excess(12) Sharpe(10) BaseSharpe(11) MaxDD(10) Turnover(10)
    width = 109
    lines = ["Annual Performance Report", "=" * width]
    header = (
        f"{'Year':<8} {'Strategy':>12} {'Baseline':>12} {'Excess':>12} "
        f"{'Sharpe':>10} {'BaseShrp':>11} {'Max DD':>10} {'Turnover':>10}"
    )
    lines.append(header)
    lines.append("-" * width)

    r_all = np.nan_to_num(result.strategy_returns, nan=0.0)
    b_all = np.nan_to_num(result.benchmark_returns, nan=0.0)

    for year in sorted(annual_metrics.keys()):
        m = annual_metrics[year]
        row = (
            f"{year:<8} {m['cumulative_return']:>11.2%} {m['cumulative_benchmark']:>12.2%} "
            f"{m['excess_return_vs_universe_average']:>12.2%} "
            f"{m['sharpe']:>10.3f} {m['baseline_sharpe']:>11.3f} {m['max_drawdown']:>10.2%} {m['turnover']:>10.4f}"
        )
        lines.append(row)

    # --- Summary row (full-period) ---
    lines.append("=" * width)
    cum_r_total = _safe_expm1(float(r_all.sum()))
    cum_b_total = _safe_expm1(float(b_all.sum()))
    avg = float(r_all.mean())
    std = float(r_all.std(ddof=0)) + 1e-8
    avg_b = float(b_all.mean())
    std_b = float(b_all.std(ddof=0)) + 1e-8
    sharpe_total = float(avg / std * math.sqrt(252.0))
    baseline_sharpe_total = float(avg_b / std_b * math.sqrt(252.0))
    max_dd_total = _max_drawdown_from_log_returns(r_all)
    turnover_total = float(result.turnovers.mean()) if result.turnovers.size else 0.0

    summary_row = (
        f"{'TOTAL':<8} {cum_r_total:>11.2%} {cum_b_total:>12.2%} "
        f"{cum_r_total - cum_b_total:>12.2%} "
        f"{sharpe_total:>10.3f} {baseline_sharpe_total:>11.3f} {max_dd_total:>10.2%} {turnover_total:>10.4f}"
    )
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
    baseline_sharpes = [annual_metrics[y]["baseline_sharpe"] for y in years]
    
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
    ax.plot(years, sharpe_ratios, marker="o", linewidth=2, markersize=8, label="Strategy Sharpe")
    ax.plot(years, baseline_sharpes, marker="s", linewidth=2, markersize=6, label="Baseline Sharpe")
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

    strategy_equity = _safe_equity_for_plot(r)
    benchmark_equity = _safe_equity_for_plot(b)

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

    strategy_equity = _safe_equity_for_plot(r)
    benchmark_equity = _safe_equity_for_plot(b)

    fig, ax = plt.subplots(figsize=(14, 7))
    ax.plot(dates, strategy_equity, label="Strategy", linewidth=2, alpha=0.8)
    ax.plot(dates, benchmark_equity, label="Benchmark", linewidth=2, alpha=0.8)
    ax.set_xlabel("Date")
    ax.set_ylabel("Cumulative Value (log scale, starting at 1.0)")
    _configure_log_y_axis(ax, strategy_equity, benchmark_equity)
    ax.set_title("Strategy vs Benchmark Equity Curve (Log Scale)")
    ax.legend()
    ax.grid(True, alpha=0.3, which="both")
    
    plt.tight_layout()
    
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved plot to {output_path}")
    plt.close()


def plot_fold_first_year_returns(
    all_first_year_dates: list[np.ndarray],
    all_first_year_strategy_log: list[np.ndarray],
    all_first_year_baseline_log: list[np.ndarray],
    output_path: str | Path | None = None,
) -> None:
    """Plot all folds' first test-year daily cumulative NAV as a single line per strategy/baseline, X axis is date, Y axis is log scale."""
    import pandas as pd
    if not all_first_year_dates:
        return

    # Concatenate all folds' first-year daily returns and dates
    all_dates = np.concatenate(all_first_year_dates)
    all_strategy = np.concatenate(all_first_year_strategy_log)
    all_baseline = np.concatenate(all_first_year_baseline_log)

    # Sort by date
    order = np.argsort(all_dates)
    dates_sorted = all_dates[order]
    strat_sorted = all_strategy[order]
    base_sorted = all_baseline[order]

    # Compute cumulative NAV
    strat_nav = _safe_equity_for_plot(strat_sorted)
    base_nav = _safe_equity_for_plot(base_sorted)

    # Convert dates to pandas for better x-axis formatting
    date_pd = pd.to_datetime(dates_sorted)

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(date_pd, strat_nav, label="Strategy", linewidth=2.2)
    ax.plot(date_pd, base_nav, label="Baseline", linewidth=2.2)
    ax.set_xlabel("Date")
    ax.set_ylabel("Cumulative NAV (log scale, start=1)")
    ax.set_title("Walkforward First-Test-Year Daily Cumulative NAV (All Folds)")
    _configure_log_y_axis(ax, strat_nav, base_nav)
    ax.axhline(y=1.0, color="black", linewidth=0.8)
    ax.grid(True, alpha=0.3, which="both")
    ax.legend()

    fig.autofmt_xdate()
    plt.tight_layout()
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved plot to {output_path}")
    plt.close()

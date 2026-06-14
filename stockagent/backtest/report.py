from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

from stockagent.backtest.simulator import BacktestResult


_PLOT_LOG_MIN = -745.0
_PLOT_LOG_MAX = 600.0


def _format_risk_metrics_text(result: BacktestResult) -> str:
    """Build a compact text block for on-chart risk metrics."""
    met = compute_metrics(result)
    return (
        f"CAGR: {met['cagr']:.2%}\n"
        f"Sharpe: {met['sharpe']:.3f}\n"
        f"Sortino: {met['sortino']:.3f}\n"
        f"Calmar: {met['calmar']:.3f}\n"
        f"MDD: {met['max_drawdown']:.2%}"
    )


def _add_metrics_box(ax: plt.Axes, text: str) -> None:
    """Render risk metrics box in a consistent style across plots."""
    ax.text(
        0.01,
        0.99,
        text,
        transform=ax.transAxes,
        va="top",
        ha="left",
        fontsize=10,
        bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.75, "edgecolor": "0.7"},
    )


def _build_plot_result(
    strategy_log_returns: np.ndarray,
    benchmark_log_returns: np.ndarray,
) -> BacktestResult:
    """Build a minimal BacktestResult for plotting-only metrics overlays."""
    strategy = np.nan_to_num(np.asarray(strategy_log_returns, dtype=np.float64), nan=0.0)
    benchmark = np.nan_to_num(np.asarray(benchmark_log_returns, dtype=np.float64), nan=0.0)
    rows = int(strategy.shape[0])
    return BacktestResult(
        strategy_returns=strategy,
        benchmark_returns=benchmark,
        turnovers=np.zeros(rows, dtype=np.float64),
        weights_history=np.zeros((rows, 1), dtype=np.float64),
    )


def _safe_expm1(log_sum: float) -> float:
    """expm1 that returns inf instead of raising overflow for very large log sums."""
    if log_sum >= 709.78:   # math.log(float_max) ≈ 709.78
        return math.inf
    if log_sum <= -745.13:  # underflow → -1
        return -1.0
    return math.expm1(log_sum)


def _total_log_return(log_returns: np.ndarray) -> float:
    """Return the finite cumulative log return for summary plots."""
    clean = np.nan_to_num(np.asarray(log_returns, dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0)
    return float(clean.sum())


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


def _annotate_max_drawdown_segment(
    ax: plt.Axes,
    dates: np.ndarray,
    equity_curve: np.ndarray,
    color: str = "tab:red",
) -> None:
    """Connect MDD peak-to-trough with a dashed line and label the drawdown percent."""
    equity = np.nan_to_num(np.asarray(equity_curve, dtype=np.float64), nan=0.0)
    x_values = np.asarray(dates)
    if equity.size < 2 or x_values.size != equity.size:
        return

    running_max = np.maximum.accumulate(equity)
    safe_running_max = np.maximum(running_max, 1e-12)
    drawdowns = equity / safe_running_max - 1.0
    trough_idx = int(np.argmin(drawdowns))
    mdd = float(drawdowns[trough_idx])
    if mdd >= 0.0:
        return

    peak_idx = int(np.argmax(equity[: trough_idx + 1]))
    x_peak = x_values[peak_idx]
    x_trough = x_values[trough_idx]
    y_peak = float(equity[peak_idx])
    y_trough = float(equity[trough_idx])

    ax.plot(
        [x_peak, x_trough],
        [y_peak, y_trough],
        linestyle="--",
        color=color,
        linewidth=1.8,
        alpha=0.9,
        label="_nolegend_",
        zorder=5,
    )
    ax.scatter([x_peak, x_trough], [y_peak, y_trough], color=color, s=22, zorder=6, label="_nolegend_")
    ax.annotate(
        f"{mdd:.2%}",
        xy=(x_trough, y_trough),
        xytext=(8, -10),
        textcoords="offset points",
        color=color,
        fontsize=9,
        fontweight="bold",
        bbox={"boxstyle": "round,pad=0.2", "facecolor": "white", "alpha": 0.7, "edgecolor": "none"},
    )


def compute_metrics(result: BacktestResult) -> dict[str, float]:
    """Compute portfolio performance metrics from a BacktestResult.

    Returns a dict with:
        cumulative_return, annualized_return, cagr, sharpe, baseline_sharpe, sortino, baseline_sortino,
        max_drawdown, calmar, turnover, daily_hit_rate, excess_return_vs_universe_average,
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
    downside = np.minimum(r, 0.0)
    downside_b = np.minimum(b, 0.0)
    downside_dev = float(np.sqrt(np.mean(np.square(downside))))
    downside_dev_b = float(np.sqrt(np.mean(np.square(downside_b))))
    sortino = float(avg / downside_dev * math.sqrt(252.0)) if downside_dev > 0 else 0.0
    baseline_sortino = float(avg_b / downside_dev_b * math.sqrt(252.0)) if downside_dev_b > 0 else 0.0

    max_dd = _max_drawdown_from_log_returns(r)
    calmar = ann_r / abs(max_dd) if max_dd < 0.0 else 0.0

    return {
        "cumulative_return": cum_r,
        "annualized_return": ann_r,
        "cagr": ann_r,
        "sharpe": sharpe,
        "baseline_sharpe": baseline_sharpe,
        "sortino": sortino,
        "baseline_sortino": baseline_sortino,
        "max_drawdown": max_dd,
        "calmar": calmar,
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
        downside = np.minimum(r_year, 0.0)
        downside_b = np.minimum(b_year, 0.0)
        downside_dev = float(np.sqrt(np.mean(np.square(downside))))
        downside_dev_b = float(np.sqrt(np.mean(np.square(downside_b))))
        sortino = float(avg / downside_dev * math.sqrt(252.0)) if downside_dev > 0 else 0.0
        baseline_sortino = float(avg_b / downside_dev_b * math.sqrt(252.0)) if downside_dev_b > 0 else 0.0

        max_dd = _max_drawdown_from_log_returns(r_year)
        calmar = ann_r / abs(max_dd) if max_dd < 0.0 else 0.0

        entry: dict[str, float] = {
            "cumulative_return": cum_r,
            "annualized_return": ann_r,
            "cagr": ann_r,
            "sharpe": sharpe,
            "baseline_sharpe": baseline_sharpe,
            "sortino": sortino,
            "baseline_sortino": baseline_sortino,
            "max_drawdown": max_dd,
            "calmar": calmar,
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
    downside_total = np.minimum(r_all, 0.0)
    downside_total_b = np.minimum(b_all, 0.0)
    downside_total_dev = float(np.sqrt(np.mean(np.square(downside_total))))
    downside_total_dev_b = float(np.sqrt(np.mean(np.square(downside_total_b))))
    sortino_total = float(avg / downside_total_dev * math.sqrt(252.0)) if downside_total_dev > 0 else 0.0
    baseline_sortino_total = float(avg_b / downside_total_dev_b * math.sqrt(252.0)) if downside_total_dev_b > 0 else 0.0
    max_dd_total = _max_drawdown_from_log_returns(r_all)
    turnover_total = float(result.turnovers.mean()) if result.turnovers.size else 0.0

    summary_row = (
        f"{'TOTAL':<8} {cum_r_total:>11.2%} {cum_b_total:>12.2%} "
        f"{cum_r_total - cum_b_total:>12.2%} "
        f"{sharpe_total:>10.3f} {baseline_sharpe_total:>11.3f} {max_dd_total:>10.2%} {turnover_total:>10.4f}"
    )
    lines.append(summary_row)
    lines.append(
        f"{'':<8} {'':>12} {'':>12} {'':>12} "
        f"{'Sortino':>10} {'BaseSrt':>11} {'':>10} {'':>10}"
    )
    lines.append(
        f"{'':<8} {'':>12} {'':>12} {'':>12} "
        f"{sortino_total:>10.3f} {baseline_sortino_total:>11.3f} {'':>10} {'':>10}"
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
    risk_text = _format_risk_metrics_text(result)
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
    _add_metrics_box(ax, risk_text)
    
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
    _annotate_max_drawdown_segment(ax, dates, strategy_equity)
    ax.set_xlabel("Date")
    ax.set_ylabel("Cumulative Value (starting at 1.0)")
    ax.set_title("Strategy vs Benchmark Equity Curve")
    ax.legend()
    ax.grid(True, alpha=0.3)
    _add_metrics_box(ax, _format_risk_metrics_text(result))

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
    _annotate_max_drawdown_segment(ax, dates, strategy_equity)
    ax.set_xlabel("Date")
    ax.set_ylabel("Cumulative Value (log scale, starting at 1.0)")
    _configure_log_y_axis(ax, strategy_equity, benchmark_equity)
    ax.set_title("Strategy vs Benchmark Equity Curve (Log Scale)")
    ax.legend()
    ax.grid(True, alpha=0.3, which="both")
    _add_metrics_box(ax, _format_risk_metrics_text(result))
    
    plt.tight_layout()
    
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved plot to {output_path}")
    plt.close()


def plot_leverage_curve(
    result: BacktestResult,
    dates: np.ndarray,
    output_path: str | Path | None = None,
    target_gross_leverage: float = 1.0,
) -> None:
    """Plot realised daily gross leverage from weights history.

    Gross leverage is defined as sum(abs(weights)) each day.
    """
    weights = np.asarray(result.weights_history, dtype=np.float64)
    if weights.ndim != 2 or weights.size == 0:
        return

    leverage_series = np.sum(np.abs(np.nan_to_num(weights, nan=0.0)), axis=1)
    if leverage_series.size == 0:
        return

    fig, ax = plt.subplots(figsize=(14, 5))
    ax.plot(dates, leverage_series, label="Realized Gross Leverage", linewidth=1.8, alpha=0.9)
    ax.axhline(y=float(target_gross_leverage), color="tab:red", linestyle="--", linewidth=1.2, label=f"Target {float(target_gross_leverage):.2f}x")
    ax.set_xlabel("Date")
    ax.set_ylabel("Gross Leverage")
    ax.set_title("Daily Gross Leverage")
    ax.grid(True, alpha=0.3)
    ax.legend()
    _add_metrics_box(ax, _format_risk_metrics_text(result))

    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved plot to {output_path}")
    plt.close()


def plot_configured_leverage_equity_curve(
    result: BacktestResult,
    dates: np.ndarray,
    output_path: str | Path | None = None,
    configured_gross_leverage: float = 1.0,
) -> None:
    """Plot equity curve from strategy returns generated under configured leverage."""
    r = np.nan_to_num(result.strategy_returns, nan=0.0)
    b = np.nan_to_num(result.benchmark_returns, nan=0.0)

    strategy_equity = _safe_equity_for_plot(r)
    benchmark_equity = _safe_equity_for_plot(b)

    fig, ax = plt.subplots(figsize=(14, 7))
    ax.plot(
        dates,
        strategy_equity,
        label=f"Strategy (configured leverage={float(configured_gross_leverage):.2f}x)",
        linewidth=2.2,
        alpha=0.9,
    )
    ax.plot(dates, benchmark_equity, label="Benchmark", linewidth=1.8, alpha=0.8)
    _annotate_max_drawdown_segment(ax, dates, strategy_equity)
    ax.set_xlabel("Date")
    ax.set_ylabel("Cumulative Value (starting at 1.0)")
    ax.set_title("Equity Curve (Configured Leverage Run)")
    ax.legend()
    ax.grid(True, alpha=0.3)

    # These are the realised risk metrics from the configured-leverage run.
    risk_text = _format_risk_metrics_text(result)
    info_text = f"{risk_text}\nConfigured Leverage: {float(configured_gross_leverage):.2f}x"
    _add_metrics_box(ax, info_text)

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

    date_values = np.asarray(dates_sorted, dtype="datetime64[ns]")

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(date_values, strat_nav, label="Strategy", linewidth=2.2)
    ax.plot(date_values, base_nav, label="Baseline", linewidth=2.2)
    _annotate_max_drawdown_segment(ax, date_values, strat_nav)
    ax.set_xlabel("Date")
    ax.set_ylabel("Cumulative NAV (log scale, start=1)")
    ax.set_title("Walkforward First-Test-Year Daily Cumulative NAV (All Folds)")
    _configure_log_y_axis(ax, strat_nav, base_nav)
    ax.axhline(y=1.0, color="black", linewidth=0.8)
    ax.grid(True, alpha=0.3, which="both")
    ax.legend()
    combined_result = _build_plot_result(strat_sorted, base_sorted)
    _add_metrics_box(ax, _format_risk_metrics_text(combined_result))

    fig.autofmt_xdate()
    plt.tight_layout()
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved plot to {output_path}")
    plt.close()


def plot_first_year_fold_metric_bars(
    fold_ids: list[int],
    all_first_year_strategy_log: list[np.ndarray],
    all_first_year_baseline_log: list[np.ndarray],
    output_path: str | Path | None = None,
) -> None:
    """Plot per-fold first-test-year log-return/risk metrics."""
    if not fold_ids:
        return

    labels = [f"F{int(fold_id):02d}" for fold_id in fold_ids]
    rows: list[dict[str, float]] = []
    for strategy, baseline in zip(all_first_year_strategy_log, all_first_year_baseline_log, strict=False):
        strategy_log_return = _total_log_return(strategy)
        baseline_log_return = _total_log_return(baseline)
        result = _build_plot_result(strategy, baseline)
        metrics = compute_metrics(result)
        rows.append(
            {
                "strategy_return": strategy_log_return,
                "baseline_return": baseline_log_return,
                "excess_return": strategy_log_return - baseline_log_return,
                "sharpe": float(metrics["sharpe"]),
                "sortino": float(metrics["sortino"]),
                "max_drawdown": float(metrics["max_drawdown"]),
            }
        )
    if not rows:
        return

    x = np.arange(len(rows), dtype=np.float64)
    width = 0.36
    fig, axes = plt.subplots(2, 2, figsize=(15, 9))

    strategy_return = np.array([row["strategy_return"] for row in rows], dtype=np.float64)
    baseline_return = np.array([row["baseline_return"] for row in rows], dtype=np.float64)
    excess_return = np.array([row["excess_return"] for row in rows], dtype=np.float64)
    sharpe = np.array([row["sharpe"] for row in rows], dtype=np.float64)
    sortino = np.array([row["sortino"] for row in rows], dtype=np.float64)
    max_drawdown = np.array([row["max_drawdown"] for row in rows], dtype=np.float64)

    ax = axes[0, 0]
    ax.bar(x - width / 2, strategy_return, width, label="Strategy")
    ax.bar(x + width / 2, baseline_return, width, label="Baseline")
    ax.axhline(0.0, color="black", linewidth=0.8)
    ax.set_title("First Test Year Cumulative Log Return")
    ax.set_ylabel("Log return")
    ax.legend()

    ax = axes[0, 1]
    ax.bar(x, excess_return, color=np.where(excess_return >= 0.0, "tab:green", "tab:red"))
    ax.axhline(0.0, color="black", linewidth=0.8)
    ax.set_title("First Test Year Excess Log Return")
    ax.set_ylabel("Strategy - Baseline")

    ax = axes[1, 0]
    ax.plot(x, sharpe, marker="o", label="Sharpe")
    ax.plot(x, sortino, marker="o", label="Sortino")
    ax.axhline(0.0, color="black", linewidth=0.8)
    ax.set_title("First Test Year Risk-Adjusted Return")
    ax.set_ylabel("Ratio")
    ax.legend()

    ax = axes[1, 1]
    ax.bar(x, max_drawdown, color="tab:orange")
    ax.axhline(0.0, color="black", linewidth=0.8)
    ax.set_title("First Test Year Max Drawdown")
    ax.set_ylabel("Drawdown")

    for ax in axes.ravel():
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=45, ha="right")
        ax.grid(True, axis="y", alpha=0.3)

    plt.tight_layout()
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved plot to {output_path}")
    plt.close()


def plot_first_year_turnover_concentration(
    fold_ids: list[int],
    all_first_year_turnovers: list[np.ndarray],
    all_first_year_weights: list[np.ndarray],
    output_path: str | Path | None = None,
) -> None:
    """Plot first-test-year turnover and concentration by fold."""
    if not fold_ids:
        return

    labels = [f"F{int(fold_id):02d}" for fold_id in fold_ids]
    mean_turnover: list[float] = []
    mean_max_abs_weight: list[float] = []
    mean_hhi: list[float] = []
    for turnovers, weights in zip(all_first_year_turnovers, all_first_year_weights, strict=False):
        turnover_arr = np.nan_to_num(np.asarray(turnovers, dtype=np.float64), nan=0.0)
        weight_arr = np.nan_to_num(np.asarray(weights, dtype=np.float64), nan=0.0)
        mean_turnover.append(float(turnover_arr.mean()) if turnover_arr.size else 0.0)
        if weight_arr.size == 0:
            mean_max_abs_weight.append(0.0)
            mean_hhi.append(0.0)
            continue
        abs_weights = np.abs(weight_arr)
        gross = abs_weights.sum(axis=1, keepdims=True)
        shares = np.divide(abs_weights, np.maximum(gross, 1e-12))
        hhi = np.square(shares).sum(axis=1)
        mean_max_abs_weight.append(float(abs_weights.max(axis=1).mean()))
        mean_hhi.append(float(hhi.mean()))

    x = np.arange(len(labels), dtype=np.float64)
    fig, axes = plt.subplots(3, 1, figsize=(14, 10), sharex=True)

    axes[0].bar(x, np.array(mean_turnover, dtype=np.float64), color="tab:blue")
    axes[0].set_title("First Test Year Mean Turnover")
    axes[0].set_ylabel("Turnover")

    axes[1].bar(x, np.array(mean_max_abs_weight, dtype=np.float64), color="tab:purple")
    axes[1].set_title("First Test Year Mean Max Absolute Single-Name Weight")
    axes[1].set_ylabel("Weight")

    axes[2].bar(x, np.array(mean_hhi, dtype=np.float64), color="tab:brown")
    axes[2].set_title("First Test Year Mean Weight HHI")
    axes[2].set_ylabel("HHI")
    axes[2].set_xticks(x)
    axes[2].set_xticklabels(labels, rotation=45, ha="right")

    for ax in axes:
        ax.grid(True, axis="y", alpha=0.3)

    plt.tight_layout()
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved plot to {output_path}")
    plt.close()


def plot_first_test_year_only(
    dates: np.ndarray,
    strategy_log_returns: np.ndarray,
    baseline_log_returns: np.ndarray,
    output_path: str | Path | None = None,
) -> None:
    """Plot only the earliest first test year in walkforward evaluation."""
    if len(dates) == 0:
        return

    strategy = np.nan_to_num(np.asarray(strategy_log_returns, dtype=np.float64), nan=0.0)
    baseline = np.nan_to_num(np.asarray(baseline_log_returns, dtype=np.float64), nan=0.0)
    date_values = np.asarray(dates, dtype="datetime64[ns]")
    strategy_nav = _safe_equity_for_plot(strategy)
    baseline_nav = _safe_equity_for_plot(baseline)
    result = _build_plot_result(strategy, baseline)

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(date_values, strategy_nav, label="Strategy", linewidth=2.2)
    ax.plot(date_values, baseline_nav, label="Baseline", linewidth=2.2)
    _annotate_max_drawdown_segment(ax, date_values, strategy_nav)
    ax.set_xlabel("Date")
    ax.set_ylabel("Cumulative NAV (log scale, start=1)")
    ax.set_title("Walkforward First Test Year Only")
    _configure_log_y_axis(ax, strategy_nav, baseline_nav)
    ax.axhline(y=1.0, color="black", linewidth=0.8)
    ax.grid(True, alpha=0.3, which="both")
    ax.legend()
    _add_metrics_box(ax, _format_risk_metrics_text(result))

    fig.autofmt_xdate()
    plt.tight_layout()
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved plot to {output_path}")
    plt.close()

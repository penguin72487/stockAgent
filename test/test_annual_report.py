from __future__ import annotations

import numpy as np
import pytest

from stockagent.backtest.report import (
    _max_drawdown_from_log_returns,
    compute_metrics,
    compute_metrics_by_year,
    generate_annual_report,
)
from stockagent.backtest.simulator import BacktestResult


def _backtest_result(
    strategy_simple_returns: list[float],
    *,
    benchmark_simple_returns: list[float] | None = None,
    turnovers: list[float] | None = None,
) -> BacktestResult:
    rows = len(strategy_simple_returns)
    benchmark = (
        benchmark_simple_returns
        if benchmark_simple_returns is not None
        else [0.0] * rows
    )
    turnover_values = turnovers if turnovers is not None else [0.0] * rows
    return BacktestResult(
        strategy_returns=np.log1p(np.asarray(strategy_simple_returns, dtype=np.float64)),
        benchmark_returns=np.log1p(np.asarray(benchmark, dtype=np.float64)),
        turnovers=np.asarray(turnover_values, dtype=np.float64),
        weights_history=np.zeros((rows, 1), dtype=np.float64),
    )


def _report_row(report: str, label: str) -> str:
    return next(line for line in report.splitlines() if line.startswith(label))


def test_annual_metrics_and_report_include_partial_2026_ytd() -> None:
    result = _backtest_result(
        [0.10, 0.02, -0.03],
        benchmark_simple_returns=[0.01, 0.005, -0.002],
        turnovers=[0.1, 0.2, 0.4],
    )
    dates = np.asarray(["2025-12-31", "2026-01-02", "2026-07-10"], dtype="datetime64[D]")

    annual = compute_metrics_by_year(result, dates)
    report = generate_annual_report(result, dates)

    assert sorted(annual) == [2025, 2026]
    assert annual[2026]["cumulative_return"] == pytest.approx((1.02 * 0.97) - 1.0)
    assert annual[2026]["cumulative_benchmark"] == pytest.approx(
        (1.005 * 0.998) - 1.0
    )
    assert annual[2026]["turnover"] == pytest.approx(0.3)
    assert _report_row(report, "2026").split()[0] == "2026"


def test_annual_report_does_not_fabricate_2026_without_dated_rows() -> None:
    result = _backtest_result([0.01, 0.02])
    dates = np.asarray(["2024-12-31", "2025-12-31"], dtype="datetime64[D]")

    annual = compute_metrics_by_year(result, dates)
    report = generate_annual_report(result, dates)

    assert sorted(annual) == [2024, 2025]
    assert not any(line.startswith("2026") for line in report.splitlines())


def test_annual_report_total_uses_same_aligned_valid_rows_as_years() -> None:
    result = _backtest_result(
        [0.10, 0.90, 0.20, 0.80],
        benchmark_simple_returns=[0.05, 0.90, 0.10, 0.80],
        turnovers=[0.1, 9.0, 0.3, 7.0],
    )
    # The NaT row and the unmatched fourth return must be excluded from both
    # annual rows and TOTAL.  The two valid dated returns compound to 32%.
    dates = np.asarray(["2024-12-31", "NaT", "2025-01-02"], dtype="datetime64[D]")

    report = generate_annual_report(result, dates)
    total_fields = _report_row(report, "TOTAL").split()

    assert float(total_fields[1].removesuffix("%")) / 100.0 == pytest.approx(0.32)
    assert float(total_fields[2].removesuffix("%")) / 100.0 == pytest.approx(0.155)
    assert float(total_fields[-1]) == pytest.approx(0.2)


def test_max_drawdown_includes_initial_nav_before_first_day_loss() -> None:
    result = _backtest_result([-0.10, 0.01, 0.01])
    dates = np.asarray(["2026-01-02", "2026-01-05", "2026-01-06"], dtype="datetime64[D]")

    assert _max_drawdown_from_log_returns(result.strategy_returns) == pytest.approx(
        -0.10
    )
    assert compute_metrics(result)["max_drawdown"] == pytest.approx(-0.10)
    assert compute_metrics_by_year(result, dates)[2026]["max_drawdown"] == pytest.approx(-0.10)

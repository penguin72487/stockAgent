import numpy as np
import warnings

from stockagent.backtest import report


def test_first_year_fold_metric_bars_uses_log_return_scale(monkeypatch):
    original_close = report.plt.close
    monkeypatch.setattr(report.plt, "close", lambda *args, **kwargs: None)
    fig = None
    try:
        report.plot_first_year_fold_metric_bars(
            [1],
            [np.array([10.0], dtype=np.float64)],
            [np.array([0.0], dtype=np.float64)],
        )
        fig = report.plt.gcf()
        return_axis = fig.axes[0]
        bar_heights = [patch.get_height() for patch in return_axis.patches]

        assert return_axis.get_ylabel() == "Log return"
        assert return_axis.get_title() == "First Test Year Cumulative Log Return"
        assert bar_heights == [10.0, 0.0]
    finally:
        monkeypatch.setattr(report.plt, "close", original_close)
        if fig is not None:
            original_close(fig)


def test_backtest_report_plots_handle_nonfinite_extremes_without_runtime_warning(tmp_path):
    dates = np.arange("2026-01-01", "2026-01-09", dtype="datetime64[D]")
    strategy_returns = np.array([-2.0, np.nan, np.inf, -np.inf, 0.1, -0.2, 1.0, -1.5], dtype=np.float64)
    benchmark_returns = np.array([0.05, 0.0, 0.2, -0.1, np.nan, np.inf, -np.inf, 0.03], dtype=np.float64)
    weights = np.array(
        [
            [1.0, -1.0],
            [np.nan, 0.5],
            [np.inf, -np.inf],
            [0.0, 0.0],
            [0.8, -0.1],
            [0.2, 0.2],
            [-0.4, 0.7],
            [0.1, -0.3],
        ],
        dtype=np.float64,
    )
    result = report.BacktestResult(
        strategy_returns=strategy_returns,
        benchmark_returns=benchmark_returns,
        turnovers=np.array([0.0, 0.1, np.nan, np.inf, -np.inf, 0.2, 0.3, 0.4], dtype=np.float64),
        weights_history=weights,
    )

    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        report.plot_annual_performance(result, dates, tmp_path / "annual.png")
        report.plot_equity_curve(result, dates, tmp_path / "equity.png")
        report.plot_equity_curve_log(result, dates, tmp_path / "equity_log.png")
        report.plot_leverage_curve(result, dates, tmp_path / "leverage.png")
        report.plot_configured_leverage_equity_curve(result, dates, tmp_path / "configured_leverage.png")
        report.plot_fold_first_year_returns([dates], [strategy_returns], [benchmark_returns], tmp_path / "fold.png")
        report.plot_first_year_fold_metric_bars([1], [strategy_returns], [benchmark_returns], tmp_path / "bars.png")
        report.plot_first_year_turnover_concentration([1], [result.turnovers], [weights], tmp_path / "turnover.png")
        report.plot_first_test_year_only(dates, strategy_returns, benchmark_returns, tmp_path / "first_year.png")

    assert (tmp_path / "annual.png").exists()
    assert (tmp_path / "equity.png").exists()
    assert (tmp_path / "first_year.png").exists()

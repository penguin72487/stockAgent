import warnings

import numpy as np
from matplotlib.colors import to_rgba

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
        report.plot_fold_first_year_returns_log10(
            [dates],
            [strategy_returns],
            [benchmark_returns],
            tmp_path / "fold_log10.png",
        )
        report.plot_first_year_fold_metric_bars([1], [strategy_returns], [benchmark_returns], tmp_path / "bars.png")
        report.plot_first_year_turnover_concentration([1], [result.turnovers], [weights], tmp_path / "turnover.png")
        report.plot_first_test_year_only(dates, strategy_returns, benchmark_returns, tmp_path / "first_year.png")

    assert (tmp_path / "annual.png").exists()
    assert (tmp_path / "equity.png").exists()
    assert (tmp_path / "fold_log10.png").exists()
    assert (tmp_path / "first_year.png").exists()


def test_first_year_fold_metric_bars_labels_scope_and_benchmark(monkeypatch):
    original_close = report.plt.close
    monkeypatch.setattr(report.plt, "close", lambda *args, **kwargs: None)
    fig = None
    try:
        report.plot_first_year_fold_metric_bars(
            [1],
            [np.array([0.2], dtype=np.float64)],
            [np.array([0.1], dtype=np.float64)],
            scope_label="Fold Test First Year",
            benchmark_label="Benchmark (2330)",
            experimental_fold_ids={1},
        )
        fig = report.plt.gcf()
        assert fig.axes[0].get_title() == "Fold Test First Year Cumulative Log Return"
        assert [text.get_text() for text in fig.axes[0].get_legend().get_texts()] == [
            "Strategy",
            "Benchmark (2330)",
        ]
        assert fig.axes[1].get_ylabel() == "Strategy - Benchmark (2330)"
        assert fig.axes[0].get_xticklabels()[0].get_text() == "F01*"
    finally:
        monkeypatch.setattr(report.plt, "close", original_close)
        if fig is not None:
            original_close(fig)


def test_fold_first_year_cumulative_plot_deduplicates_overlapping_dates(monkeypatch):
    original_close = report.plt.close
    monkeypatch.setattr(report.plt, "close", lambda *args, **kwargs: None)
    fig = None
    try:
        repeated_date = np.asarray(["2026-01-02"], dtype="datetime64[D]")
        report.plot_fold_first_year_returns(
            [repeated_date, repeated_date],
            [np.asarray([0.1]), np.asarray([0.9])],
            [np.asarray([0.0]), np.asarray([0.0])],
        )
        fig = report.plt.gcf()
        strategy_line = fig.axes[0].lines[0]
        assert len(strategy_line.get_ydata()) == 1
        assert np.isclose(strategy_line.get_ydata()[0], np.exp(0.1))
    finally:
        monkeypatch.setattr(report.plt, "close", original_close)
        if fig is not None:
            original_close(fig)


def test_fold_metric_plot_compares_benchmark_risk_and_overlays_mdd(monkeypatch):
    original_close = report.plt.close
    monkeypatch.setattr(report.plt, "close", lambda *args, **kwargs: None)
    fig = None
    try:
        report.plot_first_year_fold_metric_bars(
            [1],
            [np.asarray([0.10, -0.20, 0.05], dtype=np.float64)],
            [np.asarray([0.02, -0.05, 0.01], dtype=np.float64)],
            benchmark_label="Benchmark (2330)",
        )
        fig = report.plt.gcf()

        risk_axis = fig.axes[2]
        assert [line.get_label() for line in risk_axis.lines[:4]] == [
            "Strategy Sharpe",
            "Strategy Sortino",
            "Benchmark (2330) Sharpe",
            "Benchmark (2330) Sortino",
        ]
        assert [line.get_linestyle() for line in risk_axis.lines[:4]] == [
            "-",
            "-",
            "--",
            "--",
        ]
        assert [line.get_color().upper() for line in risk_axis.lines[:4]] == [
            "#1F77B4",
            "#6BAED6",
            "#FF7F0E",
            "#FFBB78",
        ]

        drawdown_axis = fig.axes[3]
        assert len(drawdown_axis.patches) == 2
        assert abs(drawdown_axis.patches[0].get_height()) >= abs(
            drawdown_axis.patches[1].get_height()
        )
        assert drawdown_axis.patches[0].get_width() > (
            drawdown_axis.patches[1].get_width()
        )
        assert {
            patch.get_facecolor() for patch in drawdown_axis.patches
        } == {to_rgba("#1F77B4"), to_rgba("#FF7F0E")}
        assert [
            text.get_text()
            for text in drawdown_axis.get_legend().get_texts()
        ] == ["Strategy MDD", "Benchmark (2330) MDD"]
    finally:
        monkeypatch.setattr(report.plt, "close", original_close)
        if fig is not None:
            original_close(fig)

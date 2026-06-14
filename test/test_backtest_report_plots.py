import numpy as np

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

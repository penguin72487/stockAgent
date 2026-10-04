from dataclasses import replace
from types import SimpleNamespace

import numpy as np
import pytest

from stockagent.backtest import report
from stockagent.backtest.simulator import BacktestResult
from stockagent.config import load_config
from stockagent.training import trainer


@pytest.mark.parametrize(
    ("mode", "benchmark", "weight_unit"),
    [
        ("general", "TX front-month, 1x gross, no fees/tax", "signed_entry_contract_quantities"),
        ("intraday", "TWD cash, 0% interest", "signed_entry_contract_quantities"),
        ("margin", "TWD cash, 0% interest", "signed_entry_contract_quantities"),
    ],
)
def test_futures_reporting_uses_execution_benchmark_without_changing_stock_data(
    mode, benchmark, weight_unit,
):
    config = load_config(f"configs/markets/tw_futures_v8_{mode}.yaml")
    assert config.data.benchmark_name == "2330"
    assert trainer._benchmark_plot_label(config) == f"Benchmark ({benchmark})"
    contract = trainer._mode_artifact_contract_for_config(config)
    assert contract["mode_details"]["benchmark_name"] == benchmark
    assert contract["weight_snapshot_contract"] == weight_unit
    assert config.data.benchmark_name == "2330"


def test_verified_margin_tx_roll_reports_its_actual_benchmark():
    config = load_config(
        "configs/markets/tw_futures_v8_margin_verified_2011_capital100m_tx_front_roll_v2.yaml"
    )
    details = trainer._benchmark_reporting_details(config)
    contract = trainer._mode_artifact_contract_for_config(config)
    assert details["benchmark_name"] == "TX front-month rolling buy-and-hold, 1x gross"
    assert contract["benchmark_contract"] == "tx_front_month_rolling_buy_hold_1x_gross_same_contract_close_v1"


def _contracts(*, margin=False):
    return BacktestResult(
        strategy_returns=np.array([0.01, -0.02], dtype=np.float32),
        benchmark_returns=np.array([0.02, -0.01], dtype=np.float32),
        turnovers=np.array([0.3, 0.4], dtype=np.float32),
        weights_history=np.array([[26.0, -3.0], [12.0, -5.0]], dtype=np.float32),
        requested_weights_history=np.array([[0.4, -0.1], [0.3, -0.2]], dtype=np.float32),
        execution_mode="tw_stock_context_futures_portfolio",
        settlement_ledger_unit="contract_quantity",
        futures_margin_audit=np.ones((2, 1)) if margin else None,
    )


@pytest.mark.parametrize("margin", [False, True])
def test_contract_counts_cannot_be_reported_as_weight_or_leverage(margin):
    result = _contracts(margin=margin)
    before = result.weights_history.copy()
    weights, label = report.reporting_weight_history(result)
    prefix = "requested_initial_margin_budget" if margin else "requested_notional_exposure"
    assert label.lower().replace(" ", "_") == prefix
    np.testing.assert_array_equal(weights, result.requested_weights_history)
    symbols, table = trainer._reporting_weight_table_payload(result, ["TX", "MTX"])
    assert symbols == [f"{prefix}:TX", f"{prefix}:MTX"]
    np.testing.assert_array_equal(table, weights)
    np.testing.assert_array_equal(result.weights_history, before)
    with pytest.raises(ValueError, match="signed contract counts are not leverage"):
        report.plot_leverage_curve(result, np.array(["2026-01-02", "2026-01-05"]))
    with pytest.raises(ValueError, match="requires requested weight history"):
        report.reporting_weight_history(replace(result, requested_weights_history=None))


def test_walkforward_concentration_labels_requests_and_preserves_scope(tmp_path, monkeypatch):
    fold_dir = tmp_path / "fold_10"
    fold_dir.mkdir()
    trainer._backtest_path(fold_dir).touch()
    trainer._deployment_backtest_path(fold_dir).touch()
    reset = _contracts()
    deployed = replace(reset, requested_weights_history=reset.requested_weights_history / 2)
    dates = np.array(["2026-01-02", "2026-01-05"], dtype="datetime64[D]")
    monkeypatch.setattr(
        trainer, "_load_backtest_artifact",
        lambda path: (deployed if path.name.startswith("deployment") else reset, dates),
    )
    monkeypatch.setattr(trainer, "_write_summary", lambda *_args: None)
    for name in ("plot_fold_first_year_returns", "plot_fold_first_year_returns_log10", "plot_first_year_fold_metric_bars"):
        monkeypatch.setattr(trainer, name, lambda *_args, **_kwargs: None)
    calls = []
    monkeypatch.setattr(trainer, "plot_first_year_turnover_concentration", lambda *args, **kwargs: calls.append((args, kwargs)))
    trainer._refresh_walkforward_artifacts(tmp_path, [SimpleNamespace(fold_id=10)])
    assert len(calls) == 2
    for (args, kwargs), expected in zip(calls, [reset, deployed], strict=True):
        np.testing.assert_array_equal(args[2][0], expected.requested_weights_history)
        assert kwargs["weight_label"] == "Requested Notional Exposure"
    assert calls[0][1]["scope_label"] == "Fold Test First Year (Reset State)"
    assert calls[1][1]["scope_label"] == "Owned Stitched Deployment Segment"


def test_concentration_chart_discloses_requested_allocation(monkeypatch):
    report.plt.switch_backend("Agg")
    result = _contracts()
    weights, label = report.reporting_weight_history(result)
    close = report.plt.close
    monkeypatch.setattr(report.plt, "close", lambda *_args: None)
    try:
        report.plot_first_year_turnover_concentration(
            [10], [result.turnovers], [weights], weight_label=label,
        )
        figure = report.plt.gcf()
        assert "Requested Notional Exposure" in figure.axes[1].get_title()
        assert "Requested Notional Exposure" in figure.axes[2].get_title()
        assert figure.axes[1].patches[0].get_height() == pytest.approx(0.35)
    finally:
        close("all")

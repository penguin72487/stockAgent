from dataclasses import replace
import hashlib
import json
from types import SimpleNamespace

import numpy as np
import pytest

from stockagent.backtest import report
from stockagent.backtest.simulator import BacktestResult
from stockagent.config import load_config
from stockagent.data.tw_futures_decision_clock import clock_dataset, decision_clock_contract
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


@pytest.mark.parametrize(
    ("decision_time", "scope", "minutes", "quarantine", "label"),
    [
        ("08:45", "stock_only", 1, False, "post_decision_minute_vwap_label_527"),
        ("09:00", "twd_equity", 30, True, "post_decision_30min_vwap_end_label_571"),
        ("08:45", "all_twd", 300, True, "post_decision_300min_vwap_end_label_826"),
    ],
)
def test_margin_reporting_reads_actual_entry_scope_and_window(
    tmp_path, decision_time, scope, minutes, quarantine, label,
):
    config = load_config("configs/markets/tw_futures_v8_margin.yaml")
    config = replace(config, trading=replace(
        config.trading, tw_futures_portfolio_decision_time=decision_time,
        tw_futures_portfolio_entry_clock_path=str(tmp_path / "entries.parquet"),
        tw_futures_portfolio_opening_reference_path=None,
    ))
    options = {"window_minutes": minutes, "quarantine_unknown": quarantine}
    clock = decision_clock_contract(decision_time, asset_scope=scope, **options)
    manifest = {"clock": clock, "dataset": clock_dataset(scope, **options), "status": "complete"}
    raw = json.dumps(manifest).encode()
    (tmp_path / "manifest.json").write_bytes(raw)
    contract = trainer._mode_artifact_contract_for_config(config)
    assert contract["decision_clock"] == f"{decision_time}_completed_prior_session_features"
    assert contract["execution_clock"] == label
    assert contract["mode_details"]["entry_clock_reporting_contract_version"] == 2
    assert contract["mode_details"]["entry_clock"] == clock
    assert contract["mode_details"]["entry_clock_manifest_sha256"] == hashlib.sha256(raw).hexdigest()


def test_margin_reporting_never_invents_an_absent_entry_source(tmp_path):
    config = load_config("configs/markets/tw_futures_v8_margin.yaml")
    config = replace(config, trading=replace(
        config.trading, tw_futures_portfolio_decision_time="09:00",
        tw_futures_portfolio_entry_clock_path=str(tmp_path / "entries.parquet"),
        tw_futures_portfolio_opening_reference_path=None,
    ))
    contract = trainer._mode_artifact_contract_for_config(config)
    assert contract["execution_clock"] == "post_decision_window_vwap_source_metadata_unavailable"
    assert contract["mode_details"]["entry_clock"] == {"decision_time": "09:00"}
    assert contract["mode_details"]["entry_clock_manifest_sha256"] is None


@pytest.mark.parametrize("damage", ["wrong_time", "wrong_dataset", "incomplete", "wrong_clock", "not_object"])
def test_margin_reporting_rejects_unverified_clock_manifest(tmp_path, damage):
    config = load_config("configs/markets/tw_futures_v8_margin.yaml")
    config = replace(config, trading=replace(
        config.trading, tw_futures_portfolio_decision_time="09:00",
        tw_futures_portfolio_entry_clock_path=str(tmp_path / "entries.parquet"),
        tw_futures_portfolio_opening_reference_path=None,
    ))
    options = {"window_minutes": 30, "quarantine_unknown": True}
    clock = decision_clock_contract("09:00", asset_scope="all_twd", **options)
    manifest = {"clock": clock, "dataset": clock_dataset("all_twd", **options), "status": "complete"}
    if damage == "wrong_time":
        clock["decision_time"] = "08:45"
    elif damage == "wrong_dataset":
        manifest["dataset"] = "different_source"
    elif damage == "incomplete":
        manifest["status"] = "incomplete"
    elif damage == "wrong_clock":
        clock["window_end_minute"] += 1
    else:
        manifest = []
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="futures report entry clock manifest"):
        trainer._mode_artifact_contract_for_config(config)


@pytest.mark.parametrize("scope,minutes,quarantine", [
    ("stock_only", 1, False), ("twd_equity", 30, True), ("all_twd", 300, True),
])
def test_checkpoint_records_actual_clock_and_binds_its_entry_bytes(tmp_path, scope, minutes, quarantine):
    from stockagent.training.checkpoint_contract import _trading_checkpoint_contract

    config = load_config("configs/markets/tw_futures_v8_margin.yaml")
    entry = tmp_path / "entries.parquet"
    entry.write_bytes(b"observed source bytes")
    config = replace(config, trading=replace(
        config.trading, tw_futures_portfolio_decision_time="08:45",
        tw_futures_portfolio_entry_clock_path=str(entry),
        tw_futures_portfolio_opening_reference_path=None,
    ))
    options = {"window_minutes": minutes, "quarantine_unknown": quarantine}
    clock = decision_clock_contract("08:45", asset_scope=scope, **options)
    manifest = {"clock": clock, "dataset": clock_dataset(scope, **options), "status": "complete",
                "entries_sha256": hashlib.sha256(entry.read_bytes()).hexdigest()}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    checkpoint_clock = _trading_checkpoint_contract(config)["futures_entry_clock"]
    assert all(checkpoint_clock[key] == value for key, value in clock.items())
    assert checkpoint_clock["source_sha256"] == manifest["entries_sha256"]
    entry.write_bytes(b"changed source bytes")
    with pytest.raises(ValueError, match="source differs from its manifest"):
        _trading_checkpoint_contract(config)


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

from dataclasses import replace
import json
from types import SimpleNamespace

import numpy as np
import pytest
import torch

from stockagent.config import load_config
from stockagent.data import tw_futures_margin as margin
from stockagent.data.tw_futures_account_calendar import build_futures_account_calendar
from stockagent.training.dataset import CrossSectionalDataset
from stockagent.training.futures_account_artifact import (
    bind_futures_account_result_context, fresh_futures_account_context_for_dates,
    slice_futures_account_ledger,
)
from stockagent.training import trainer
from stockagent.training.windowed import dataset_to_windowed_tensors
from test_futures_causal_account_policy import packet
from test_tw_futures_account_evaluator import _case
from test_tw_futures_margin import rule_panel, tape


def test_fresh_context_requires_exact_contiguous_source_and_flat_state():
    source, _, _ = _case(prepared_execution=True)
    dates = source.calendar.decision_dates
    context = fresh_futures_account_context_for_dates(
        source, dates, execution_release_fingerprint=source.execution_release_fingerprint,
        initial_capital=1000.,
    )
    assert context.expected_calendar_fingerprint == source.calendar.fingerprint
    assert context.initial_equity_scale == 1 and context.initial_alive
    assert not any(context.initial_quantities)
    with pytest.raises(ValueError, match="execution release differs"):
        fresh_futures_account_context_for_dates(source, dates,
            execution_release_fingerprint="a" * 64, initial_capital=1000.)
    for bad in (dates[::-1], dates.astype("datetime64[ns]"),
                np.array(["2016-02-03", "2016-02-16"], dtype="datetime64[D]")):
        with pytest.raises(ValueError):
            fresh_futures_account_context_for_dates(source, bad,
                execution_release_fingerprint=source.execution_release_fingerprint, initial_capital=1000.)


def test_stitched_futures_replays_carried_account_and_holiday_event(tmp_path, monkeypatch):
    monkeypatch.setenv("STOCKAGENT_EVAL_BACKTEST_COMPILE", "0")
    for name in ("plot_equity_curve", "plot_equity_curve_log", "plot_annual_performance"):
        monkeypatch.setattr(trainer, name, lambda *args, **kwargs: None)
    panel, _ = rule_panel(tmp_path)
    panel.dates = np.asarray(["2026-01-02", "2026-01-03", "2026-01-05"], dtype="datetime64[D]")
    calendar = build_futures_account_calendar(
        panel.dates, ["2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05"],
        [False, False, True, False],
    )
    decisions, events = tape(rows=3, slots=1936), tape(rows=1, slots=1936)
    decisions[:, 1:] = 0
    events[:, 1:] = 0
    events[..., 1] = events[..., 8] = 0
    events[0, 0, 2] = events[0, 0, margin.CASH_SETTLEMENT] = 1
    events[0, 0, margin.TERMINAL_MARK] = 1100
    daily = panel.stock_context_futures_portfolio_daily
    panel.stock_context_futures_portfolio_daily = replace(
        daily, dates=panel.dates, integer_execution=decisions.numpy(),
        margin_contract_version=1, futures_account_calendar=calendar,
        account_only_integer_execution=events.numpy(),
    )
    config = load_config("configs/markets/tw_stock_context_all_futures_portfolio_multi_basis_projection_l1.yaml")
    config.trading.tw_futures_portfolio_integer_contracts = True
    config.trading.tw_futures_portfolio_integer_initial_capital = 1000.
    config.trading.volume_participation_equity = 1000.
    config.trading.max_volume_participation = 0.
    config.trading.long_only = False
    config.trading.portfolio_activation = "pre_normalized"
    config.training.financial_transformer.futures_causal_account_policy = True
    config.training.financial_transformer.futures_net_notional_cap = None
    config.training.financial_transformer.futures_beta_notional_cap = None
    runtime = trainer._build_execution_runtime(panel, config, torch.device("cpu"))
    source = runtime.futures_account_source
    with pytest.raises(ValueError, match="contiguous source slice"):
        fresh_futures_account_context_for_dates(
            source, panel.dates[[0, 2]], execution_release_fingerprint=source.execution_release_fingerprint,
            initial_capital=1000.,
        )
    small = packet(rows=2, slots=1, feedback=-.3)
    policies = torch.zeros((2, 1936, small.shape[-1]), dtype=torch.float32)
    policies[:, :1] = small

    def evaluate(indices, requested):
        ds = CrossSectionalDataset(panel, np.asarray(indices), lookback=1,
            execution_mode=config.trading.execution_mode, futures_account_source=source)
        split = dataset_to_windowed_tensors(ds)
        actual = split._valid_indices_cpu
        assert actual.tolist() == list(indices)
        result, _ = trainer._run_eval_backtest_from_weight_buffers(
            requested, ds.future_log_returns_t[actual], ds.tradable_mask_t[actual],
            ds.can_buy_mask_t[actual], ds.can_sell_mask_t[actual],
            ds.can_short_open_mask_t[actual], ds.force_short_cover_mask_t[actual],
            ds.force_exit_mask_t[actual], ds.benchmark_t[actual],
            device=torch.device("cpu"), non_blocking=False, long_only=False,
            buy_fee_rate=config.trading.buy_fee_rate, sell_fee_rate=config.trading.sell_fee_rate,
            max_turnover_ratio=config.trading.max_turnover_ratio, gross_leverage=1.,
            min_trade_weight=config.trading.min_trade_weight, backtest_chunk_rows=1,
            portfolio_activation="pre_normalized", compute_metrics_summary=False,
            return_weights_history=True, profile_timing=False, progress_label=None,
            timing=trainer.TimingBreakdown(), reset_at_rows=None, execution_runtime=runtime,
            physical_split=split,
        )
        context = bind_futures_account_result_context(
            result.futures_account_calendar_result, source, split,
            execution_release_fingerprint=runtime.futures_execution_release_fingerprint,
            initial_capital=runtime.futures_initial_capital,
        )
        return result.to_numpy(futures_account_context=context), context

    for fold_id, index in ((1, 1), (2, 2)):
        fold_dir = tmp_path / f"fold_{fold_id:02d}"
        fold_dir.mkdir()
        # The first model's full test overlaps the next fold; deployment owns
        # only its prefix. Replay must take the original policy prefix.
        indices = [1, 2] if fold_id == 1 else [2]
        policy = policies if fold_id == 1 else policies[1:]
        fresh, context = evaluate(indices, policy)
        dates = panel.dates[indices]
        trainer._save_backtest_artifact(fold_dir / "test_backtest.npz", fresh, dates,
                                        futures_account_context=context)
        (fold_dir / "test_backtest_symbols.json").write_text(json.dumps(list(source.universe)))
        prefix, prefix_context = trainer._slice_backtest_rows(
            fresh, 0, 1, futures_account_context=context, return_futures_context=True,
        )
        trainer._save_deployment_test_artifacts(fold_dir, prefix, dates[:1], symbols=source.universe,
                                               futures_account_context=prefix_context)
    expected, full_context = evaluate([1, 2], policies)
    assert expected.futures_account_ledger.account_only_mask.tolist() == [False, True, False]
    assert expected.futures_account_ledger.opening_quantities[1, 0] != 0
    results = [SimpleNamespace(fold_id=1), SimpleNamespace(fold_id=2)]
    replayed = trainer._replay_taiwan_stitched_deployment(tmp_path, results, panel=panel, config=config)
    np.testing.assert_array_equal(replayed.futures_account_ledger.dates, expected.futures_account_ledger.dates)
    np.testing.assert_array_equal(replayed.futures_account_ledger.residual_quantities,
                                  expected.futures_account_ledger.residual_quantities)
    np.testing.assert_allclose(replayed.futures_account_ledger.strategy_returns,
                               expected.futures_account_ledger.strategy_returns, atol=1e-6)
    _, second_context = slice_futures_account_ledger(replayed.futures_account_ledger, full_context, 1, 2)
    assert any(second_context.initial_quantities)
    second, _ = trainer._load_backtest_artifact(tmp_path / "fold_02/deployment_test_backtest.npz",
                                                futures_account_context=second_context)
    assert second.futures_account_ledger.account_only_mask.tolist() == [True, False]
    carried_dir = tmp_path / "carried_as_test"
    carried_dir.mkdir()
    (carried_dir / "test_backtest_symbols.json").write_text(json.dumps(list(source.universe)))
    trainer._save_backtest_artifact(carried_dir / "test_backtest.npz", second, panel.dates[2:],
                                    futures_account_context=second_context)
    with pytest.raises(ValueError, match="pinned context differs"):
        trainer._load_fresh_dated_futures_test_artifact(carried_dir, source=source, initial_capital=1000.)
    assert [r.fold_id for r in trainer._stitched_deployment_prefix_results(
        [SimpleNamespace(fold_id=1), SimpleNamespace(fold_id=3)],
        execution_mode=config.trading.execution_mode, require_contiguous_account=True)] == [1]
    # Refresh must load independent test accounts again but plot the continuous
    # deployment on its actual account-event clock, including the holiday.
    captured = {}
    monkeypatch.setattr(trainer, "_write_summary", lambda *args, **kwargs: None)
    for name in ("plot_fold_first_year_returns_log10", "plot_first_year_fold_metric_bars",
                 "plot_first_year_turnover_concentration"):
        monkeypatch.setattr(trainer, name, lambda *args, **kwargs: None)

    def capture(dates, strategy, benchmark, *args, **kwargs):
        if kwargs.get("scope_label") == "Canonical Stitched Walk-Forward Deployment":
            captured["dates"] = dates
            captured["strategy"] = strategy

    monkeypatch.setattr(trainer, "plot_fold_first_year_returns", capture)
    trainer._refresh_walkforward_artifacts(tmp_path, results, panel=panel, config=config)
    np.testing.assert_array_equal(captured["dates"][1], calendar.dates[2:])
    np.testing.assert_allclose(captured["strategy"][1],
                               expected.futures_account_ledger.strategy_returns[1:])
    coverage = json.loads((tmp_path / "walkforward_deployment_coverage.json").read_text())
    assert coverage["report_contract"] == "contiguous_completed_fold_prefix_dated_futures_v1"
    assert coverage["account_only_event_rows"] == 1
    assert coverage["futures_account_source_fingerprint"] == source.fingerprint
    assert coverage["futures_execution_release_fingerprint"] == source.execution_release_fingerprint
    with pytest.raises(ValueError, match="pinned context differs"):
        trainer._load_fresh_dated_futures_test_artifact(
            tmp_path / "fold_02", source=replace(source, execution_release_fingerprint="a" * 64),
            initial_capital=1000.)

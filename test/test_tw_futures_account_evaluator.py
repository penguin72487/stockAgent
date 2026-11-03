from dataclasses import replace
from types import SimpleNamespace
import json

import numpy as np
import pytest
import torch

from stockagent.backtest import report
from stockagent.backtest.tw_futures_account_calendar import run_futures_account_calendar_torch
from stockagent.config import TradingConfig, TrainingConfig, WalkForwardConfig
from stockagent.data import tw_futures_margin as margin
from stockagent.data.tw_futures_account_calendar import build_futures_account_calendar
from stockagent.training.futures_account_bridge import PreparedFuturesAccountSource
from stockagent.training.futures_account_artifact import (
    FuturesAccountArtifactContext, bind_futures_account_result_context,
    continue_futures_account_context, slice_futures_account_ledger,
)
from stockagent.training.trainer import (
    TimingBreakdown, _ExecutionRuntime, _load_backtest_artifact, _prefix_backtest_result,
    _materialize_dated_futures_fold_outputs,
    _run_eval_backtest_from_weight_buffers, _save_backtest_artifact,
    _save_best_val_backtest_snapshot, _save_deployment_test_artifacts,
    _save_fold_output_artifacts, _slice_backtest_rows, FoldResult,
)
from test_tw_futures_margin import tape


class _Split:
    def __init__(self, source, indices=(0, 1)):
        self._valid_indices_cpu = torch.tensor(indices, dtype=torch.int64)
        self.sample_mask = None
        self.session_advance_mask = None
        self.execution_mode = "tw_stock_context_futures_portfolio"
        self.futures_account_source_fingerprint = source.fingerprint
        self.futures_execution_release_fingerprint = source.execution_release_fingerprint
        if source.decision_execution is not None:
            self.overnight_log_returns = source.decision_execution
            self._futures_execution_tensor_version = self.overnight_log_returns._version

    def __len__(self):
        return len(self._valid_indices_cpu)


def _case(calendar=None, *, prepared_execution=False):
    calendar = calendar or build_futures_account_calendar(
        ["2016-02-03", "2016-02-15"],
        ["2016-02-03", "2016-02-04", "2016-02-15"],
        [False, True, False],
    )
    decisions, events = tape(rows=2, slots=1936), tape(rows=1, slots=1936)
    events[..., 1] = events[..., 8] = 0
    events[0, 0, 2] = events[0, 0, margin.CASH_SETTLEMENT] = 1
    events[0, 0, margin.TERMINAL_MARK] = 1100
    events[0, 0, 5], events[0, 0, 7] = 2, 3
    decisions[1, 0, 4] = 1300
    daily = SimpleNamespace(futures_account_calendar=calendar, integer_execution=decisions.numpy(),
                            account_only_integer_execution=events.numpy(),
                            symbols=tuple(f"slot{i:04}" for i in range(1936)))
    if prepared_execution:
        daily.margin_contract_version = 1
        daily.must_liquidate_mask = decisions[..., 2].numpy().astype(bool)
    source = PreparedFuturesAccountSource.from_daily(
        daily, calendar.decision_dates, prepare_execution=prepared_execution,
    )
    weights = torch.zeros(2, 1936)
    weights[:, 0] = torch.tensor([-.5, .5])
    return source, weights, decisions


def _evaluate(source, weights, decisions, *, chunk_rows, history, reset_at_rows=None, device="cpu",
              benchmark_returns=None, indices=(0, 1), initial_context=None, release=None):
    runtime = _ExecutionRuntime(
        mode="tw_stock_context_futures_portfolio", buy_fee_rates=None, sell_fee_rates=None,
        lot_sizes=None, settlement_lag_sessions=0, futures_initial_capital=1000.,
        futures_account_source=source, futures_execution_release_fingerprint=(
            release or source.execution_release_fingerprint or "2" * 64),
    )
    zeros = torch.zeros(len(indices), 1)
    yes = torch.ones(len(indices), 1, dtype=torch.bool)
    no = torch.zeros(len(indices), 1, dtype=torch.bool)
    benchmark = torch.zeros(len(indices)) if benchmark_returns is None else torch.as_tensor(benchmark_returns)
    return _run_eval_backtest_from_weight_buffers(
        weights.to(device), zeros, yes, yes, yes, yes, no, no, benchmark,
        device=torch.device(device), non_blocking=False, long_only=False,
        buy_fee_rate=0., sell_fee_rate=0., max_turnover_ratio=0., gross_leverage=1.,
        min_trade_weight=0., backtest_chunk_rows=chunk_rows,
        portfolio_activation="pre_normalized", compute_metrics_summary=True,
        return_weights_history=history, profile_timing=False, progress_label=None,
        timing=TimingBreakdown(), reset_at_rows=reset_at_rows,
        overnight_log_returns_all=decisions, execution_runtime=runtime, physical_split=_Split(source, indices),
        initial_futures_account_context=initial_context,
    )


@pytest.mark.parametrize("history", [False, True])
def test_evaluator_keeps_event_dates_and_account_only_drawdown(monkeypatch, history):
    monkeypatch.setenv("STOCKAGENT_EVAL_BACKTEST_COMPILE", "0")
    source, weights, decisions = _case()
    chunked, metrics = _evaluate(source, weights, decisions, chunk_rows=1, history=history)
    whole, whole_metrics = _evaluate(source, weights, decisions, chunk_rows=2, history=history)
    reference = run_futures_account_calendar_torch(
        source.calendar, weights, decisions, source.events, initial_capital=1000.,
        return_weights_history=history,
    )
    chunk_ledger = chunked.futures_account_calendar_result
    assert chunk_ledger.calendar.dates.tolist() == source.calendar.dates.tolist()
    torch.testing.assert_close(chunk_ledger.account_ledger.strategy_returns,
                               reference.account_ledger.strategy_returns)
    torch.testing.assert_close(chunked.strategy_returns, reference.decision_log_returns)
    torch.testing.assert_close(chunked.strategy_returns, whole.strategy_returns)
    assert metrics == pytest.approx(whole_metrics)
    assert metrics["max_drawdown"] == pytest.approx(-.525, abs=1e-6)
    assert metrics["max_drawdown"] < min(0., float(torch.expm1(chunked.strategy_returns.cumsum(0)).min()))
    with pytest.raises(ValueError, match="dated account-ledger artifact writer"):
        chunked.to_numpy()


def test_evaluator_rejects_implicit_account_resets():
    source, weights, decisions = _case()
    with pytest.raises(ValueError, match="no account reset"):
        _evaluate(source, weights, decisions, chunk_rows=1, history=True, reset_at_rows=[1])


def test_evaluator_consumes_prepared_release_not_an_unbound_buffer(monkeypatch):
    monkeypatch.setenv("STOCKAGENT_EVAL_BACKTEST_COMPILE", "0")
    source, weights, decisions = _case(prepared_execution=True)
    canonical, _ = _evaluate(source, weights, decisions, chunk_rows=1, history=True)
    altered = decisions.clone()
    altered[0, 0, 3] *= 10
    tampered, _ = _evaluate(source, weights, altered, chunk_rows=1, history=True)
    torch.testing.assert_close(tampered.strategy_returns, canonical.strategy_returns)
    torch.testing.assert_close(tampered.final_weights, canonical.final_weights)
    assert (tampered.futures_account_calendar_result.execution_release_fingerprint
            == source.execution_release_fingerprint)
    reference = run_futures_account_calendar_torch(
        source.calendar, weights, source.decision_execution, source.events,
        initial_capital=1000., return_weights_history=True,
    )
    torch.testing.assert_close(tampered.strategy_returns, reference.decision_log_returns)
    with pytest.raises(ValueError, match="runtime and prepared decision releases differ"):
        _evaluate(source, weights, decisions, chunk_rows=1, history=True, release="3" * 64)


def test_evaluator_continues_actual_account_across_dated_boundary(monkeypatch):
    monkeypatch.setenv("STOCKAGENT_EVAL_BACKTEST_COMPILE", "0")
    source, weights, decisions = _case()
    whole, _ = _evaluate(source, weights, decisions, chunk_rows=1, history=True)
    first, _ = _evaluate(source, weights[:1], decisions[:1], chunk_rows=1,
                         history=True, indices=(0,))
    first_calendar = source.prepare_batch(_Split(source, (0,)), 0, 1).calendar
    first_context = FuturesAccountArtifactContext(
        universe=source.universe, event_source_fingerprint=source.fingerprint,
        execution_release_fingerprint="2" * 64,
        expected_calendar_fingerprint=first_calendar.fingerprint,
        initial_capital=1000., initial_equity_scale=1., initial_alive=True,
        initial_quantities=(0,) * 1936,
    )
    ledger = first.to_numpy(futures_account_context=first_context).futures_account_ledger
    incoming = continue_futures_account_context(
        ledger, first_context, source, _Split(source, (1,)), execution_release_fingerprint="2" * 64)
    assert any(incoming.initial_quantities)
    second, _ = _evaluate(source, weights[1:], decisions[1:], chunk_rows=1,
                          history=True, indices=(1,), initial_context=incoming)
    assert second.futures_account_calendar_result.calendar.account_only_mask.tolist() == [True, False]
    torch.testing.assert_close(torch.cat([first.strategy_returns, second.strategy_returns]), whole.strategy_returns)
    torch.testing.assert_close(
        torch.cat([first.futures_account_calendar_result.account_ledger.strategy_returns,
                   second.futures_account_calendar_result.account_ledger.strategy_returns]),
        whole.futures_account_calendar_result.account_ledger.strategy_returns)
    torch.testing.assert_close(second.final_weights, whole.final_weights)
    torch.testing.assert_close(second.final_equity_scale, whole.final_equity_scale)
    with pytest.raises(ValueError, match="source, universe or execution release differs"):
        continue_futures_account_context(
            ledger, first_context, source, _Split(source, (1,)), execution_release_fingerprint="3" * 64)
    with pytest.raises(ValueError, match="immediately next"):
        continue_futures_account_context(
            ledger, first_context, source, _Split(source, (0,)), execution_release_fingerprint="2" * 64)
    with pytest.raises(ValueError, match="bound source or execution release"):
        _evaluate(source, weights[1:], decisions[1:], chunk_rows=1, history=True, indices=(1,),
                  initial_context=replace(incoming, execution_release_fingerprint="3" * 64))


def test_result_context_binding_requires_actual_fresh_or_verified_continuing_state(monkeypatch):
    monkeypatch.setenv("STOCKAGENT_EVAL_BACKTEST_COMPILE", "0")
    source, weights, decisions = _case()
    first, _ = _evaluate(source, weights[:1], decisions[:1], chunk_rows=1,
                         history=True, indices=(0,))
    first_context = bind_futures_account_result_context(
        first.futures_account_calendar_result, source, _Split(source, (0,)),
        execution_release_fingerprint="2" * 64, initial_capital=1000.,
    )
    first_numpy = first.to_numpy(futures_account_context=first_context)
    incoming = continue_futures_account_context(
        first_numpy.futures_account_ledger, first_context, source, _Split(source, (1,)),
        execution_release_fingerprint="2" * 64,
    )
    assert any(incoming.initial_quantities)
    second, _ = _evaluate(source, weights[1:], decisions[1:], chunk_rows=1,
                          history=True, indices=(1,), initial_context=incoming)
    with pytest.raises(ValueError, match="actual incoming state"):
        bind_futures_account_result_context(
            second.futures_account_calendar_result, source, _Split(source, (1,)),
            execution_release_fingerprint="2" * 64, initial_capital=1000.,
        )
    bound = bind_futures_account_result_context(
        second.futures_account_calendar_result, source, _Split(source, (1,)),
        execution_release_fingerprint="2" * 64, initial_capital=1000., incoming_context=incoming,
    )
    assert bound == incoming
    assert second.to_numpy(futures_account_context=bound).futures_account_ledger.account_only_mask.tolist() == [True, False]
    with pytest.raises(ValueError, match="calendar differs"):
        bind_futures_account_result_context(
            second.futures_account_calendar_result, source, _Split(source, (0,)),
            execution_release_fingerprint="2" * 64, initial_capital=1000., incoming_context=incoming,
        )
    with pytest.raises(ValueError, match="source or execution release"):
        bind_futures_account_result_context(
            second.futures_account_calendar_result, source, _Split(source, (1,)),
            execution_release_fingerprint="3" * 64, initial_capital=1000., incoming_context=incoming,
        )
    changed_events = source.events.clone()
    changed_events[0, 0, margin.TERMINAL_MARK] += 1
    changed_daily = SimpleNamespace(
        futures_account_calendar=source.calendar, integer_execution=decisions.numpy(),
        account_only_integer_execution=changed_events.numpy(), symbols=source.universe,
    )
    other_source = PreparedFuturesAccountSource.from_daily(changed_daily, source.calendar.decision_dates)
    with pytest.raises(ValueError, match="provenance differs"):
        bind_futures_account_result_context(
            second.futures_account_calendar_result, other_source, _Split(other_source, (1,)),
            execution_release_fingerprint="2" * 64, initial_capital=1000.,
        )
    with pytest.raises(ValueError, match="provenance differs"):
        second.to_numpy(futures_account_context=replace(incoming, event_source_fingerprint=other_source.fingerprint))
    padded = _Split(source, (1, 1))
    padded.sample_mask = torch.tensor([True, False])
    with pytest.raises(ValueError, match="padded decision rows"):
        bind_futures_account_result_context(
            second.futures_account_calendar_result, source, padded,
            execution_release_fingerprint="2" * 64, initial_capital=1000., incoming_context=incoming,
        )


@pytest.mark.skipif(not torch.cuda.is_available(), reason="CUDA required for GPU parity")
def test_evaluator_gpu_matches_cpu_with_dated_settlement(monkeypatch):
    monkeypatch.setenv("STOCKAGENT_EVAL_BACKTEST_COMPILE", "0")
    source, weights, decisions = _case()
    cpu, cpu_metrics = _evaluate(source, weights, decisions, chunk_rows=1, history=True)
    gpu, gpu_metrics = _evaluate(source, weights, decisions, chunk_rows=1, history=True, device="cuda")
    torch.testing.assert_close(gpu.strategy_returns.cpu(), cpu.strategy_returns)
    gpu_ledger = gpu.futures_account_calendar_result.account_ledger
    cpu_ledger = cpu.futures_account_calendar_result.account_ledger
    for field in ("strategy_returns", "equity_scale_history", "margin_audit_history",
                  "contract_quantities_history", "residual_contract_quantities_history"):
        torch.testing.assert_close(getattr(gpu_ledger, field).cpu(), getattr(cpu_ledger, field))
    assert gpu_metrics == pytest.approx(cpu_metrics, rel=2e-5, abs=2e-6)


def test_dated_evaluator_archive_roundtrip_requires_exact_source(monkeypatch, tmp_path):
    monkeypatch.setenv("STOCKAGENT_EVAL_BACKTEST_COMPILE", "0")
    source, weights, decisions = _case()
    tensor, _ = _evaluate(source, weights, decisions, chunk_rows=1, history=True)
    context = FuturesAccountArtifactContext(
        universe=tuple(f"slot{i:04}" for i in range(1936)),
        event_source_fingerprint=source.fingerprint,
        execution_release_fingerprint="2" * 64,
        expected_calendar_fingerprint=source.calendar.fingerprint,
        initial_capital=1000., initial_equity_scale=1., initial_alive=True,
        initial_quantities=(0,) * 1936,
    )
    result = tensor.to_numpy(futures_account_context=context)
    path = tmp_path / "dated.npz"
    with pytest.raises(ValueError, match="full ledger and pinned context"):
        _save_backtest_artifact(path, result, source.calendar.decision_dates)
    _save_backtest_artifact(path, result, source.calendar.decision_dates,
                            futures_account_context=context)
    loaded, dates = _load_backtest_artifact(path, futures_account_context=context)
    np.testing.assert_array_equal(dates, source.calendar.decision_dates)
    np.testing.assert_array_equal(loaded.futures_account_ledger.dates, source.calendar.dates)
    np.testing.assert_array_equal(loaded.futures_account_ledger.strategy_returns,
                                  result.futures_account_ledger.strategy_returns)
    with pytest.raises(ValueError, match="pinned source context"):
        _load_backtest_artifact(path)
    with pytest.raises(ValueError, match="context|source|fingerprint"):
        _load_backtest_artifact(path, futures_account_context=replace(
            context, expected_calendar_fingerprint="3" * 64))
    with pytest.raises(ValueError, match="source-pinned account-calendar slice"):
        _prefix_backtest_result(loaded, 1)
    with pytest.raises(ValueError, match="source-pinned account-calendar slice"):
        _slice_backtest_rows(loaded, 0, 1)
    prefix = _prefix_backtest_result(loaded, 1, futures_account_context=context)
    suffix = _slice_backtest_rows(loaded, 1, 2, futures_account_context=context)
    _, prefix_context = slice_futures_account_ledger(loaded.futures_account_ledger, context, 0, 1)
    _, suffix_context = slice_futures_account_ledger(loaded.futures_account_ledger, context, 1, 2)
    np.testing.assert_array_equal(prefix.futures_account_ledger.dates, source.calendar.dates[:1])
    np.testing.assert_array_equal(suffix.futures_account_ledger.dates, source.calendar.dates[1:])
    assert suffix_context.initial_equity_scale == float(prefix.final_equity_scale)
    np.testing.assert_array_equal(suffix_context.initial_quantities, prefix.final_weights)
    assert any(suffix_context.initial_quantities)
    for name, segment, segment_context in (("prefix", prefix, prefix_context),
                                            ("suffix", suffix, suffix_context)):
        segment_path = tmp_path / f"{name}.npz"
        _save_backtest_artifact(segment_path, segment, segment.futures_account_ledger.decision_dates,
                                futures_account_context=segment_context)
        restored, _ = _load_backtest_artifact(segment_path, futures_account_context=segment_context)
        np.testing.assert_array_equal(restored.futures_account_ledger.dates,
                                      segment.futures_account_ledger.dates)
    with np.load(path, allow_pickle=False) as archive:
        payload = {key: archive[key].copy() for key in archive.files}
    assert int(payload["artifact_schema_version"]) == 9
    payload["artifact_schema_version"] = np.asarray(7)
    broken = tmp_path / "downgraded.npz"
    np.savez(broken, **payload)
    with pytest.raises(ValueError, match="legacy archive schema"):
        _load_backtest_artifact(broken)


def test_best_validation_and_deployment_keep_the_pinned_event_day(monkeypatch, tmp_path):
    monkeypatch.setenv("STOCKAGENT_EVAL_BACKTEST_COMPILE", "0")
    source, weights, decisions = _case()
    tensor, _ = _evaluate(source, weights, decisions, chunk_rows=1, history=True)
    context = FuturesAccountArtifactContext(
        universe=tuple(f"slot{i:04}" for i in range(1936)),
        event_source_fingerprint=source.fingerprint,
        execution_release_fingerprint="2" * 64,
        expected_calendar_fingerprint=source.calendar.fingerprint,
        initial_capital=1000., initial_equity_scale=1., initial_alive=True,
        initial_quantities=(0,) * 1936,
    )
    fold = SimpleNamespace(fold_id=14, train_years=[2014], val_years=[2016], test_years=[2017])
    snapshot = dict(fold_dir=tmp_path, fold=fold, epoch=4, val_loss=-.1,
                    val_backtest=tensor, row_start=0, row_end=2,
                    dates=source.calendar.decision_dates, objective="log_utility")
    with pytest.raises(ValueError, match="complete account ledger"):
        _save_best_val_backtest_snapshot(**snapshot)
    assert not (tmp_path / "best_val_backtest.npz").exists()
    with pytest.raises(ValueError, match="fingerprint|calendar"):
        _save_best_val_backtest_snapshot(
            **snapshot, futures_account_context=replace(context, expected_calendar_fingerprint="3" * 64))
    _save_best_val_backtest_snapshot(**snapshot, futures_account_context=context)
    restored, _ = _load_backtest_artifact(tmp_path / "best_val_backtest.npz", futures_account_context=context)
    np.testing.assert_array_equal(restored.futures_account_ledger.dates, source.calendar.dates)
    metadata = json.loads((tmp_path / "best_val_snapshot.json").read_text())
    assert metadata["metrics"]["max_drawdown"] == pytest.approx(-.525, abs=1e-6)

    # A validation slice beginning after a market decision must retain the
    # settlement event immediately before its first decision and carry the
    # actual incoming contracts/equity, not restart with a flat account.
    result = tensor.to_numpy(futures_account_context=context)
    _, suffix_context = slice_futures_account_ledger(result.futures_account_ledger, context, 1, 2)
    suffix_dir = tmp_path / "suffix"
    suffix_dir.mkdir()
    _save_best_val_backtest_snapshot(
        **{**snapshot, "fold_dir": suffix_dir, "row_start": 1,
           "dates": source.calendar.decision_dates[1:]}, futures_account_context=context)
    suffix, _ = _load_backtest_artifact(suffix_dir / "best_val_backtest.npz",
                                        futures_account_context=suffix_context)
    np.testing.assert_array_equal(suffix.futures_account_ledger.dates, source.calendar.dates[1:])
    assert any(suffix_context.initial_quantities)
    with pytest.raises(ValueError, match="full ledger and pinned source context"):
        _save_deployment_test_artifacts(suffix_dir, suffix, source.calendar.decision_dates[1:],
                                        symbols=context.universe)
    _save_deployment_test_artifacts(suffix_dir, suffix, source.calendar.decision_dates[1:],
                                    symbols=context.universe, futures_account_context=suffix_context)
    deployment, _ = _load_backtest_artifact(suffix_dir / "deployment_test_backtest.npz",
                                            futures_account_context=suffix_context)
    np.testing.assert_array_equal(deployment.futures_account_ledger.dates, source.calendar.dates[1:])

    # Metric-only evaluation intentionally avoids the large per-slot history;
    # it must fail closed if asked to produce a replayable account snapshot.
    metrics_only, _ = _evaluate(source, weights, decisions, chunk_rows=1, history=False)
    incomplete_dir = tmp_path / "incomplete"
    incomplete_dir.mkdir()
    with pytest.raises(ValueError, match="recorded residual quantities"):
        _save_best_val_backtest_snapshot(
            **{**snapshot, "fold_dir": incomplete_dir, "val_backtest": metrics_only},
            futures_account_context=context)
    assert not (incomplete_dir / "best_val_backtest.npz").exists()


def test_fold_output_requires_the_exact_dated_deployment_prefix(monkeypatch, tmp_path):
    monkeypatch.setenv("STOCKAGENT_EVAL_BACKTEST_COMPILE", "0")
    source, weights, decisions = _case()
    tensor, _ = _evaluate(source, weights, decisions, chunk_rows=1, history=True)
    runtime = _ExecutionRuntime(
        mode="tw_stock_context_futures_portfolio", buy_fee_rates=None, sell_fee_rates=None,
        lot_sizes=None, settlement_lag_sessions=0, futures_initial_capital=1000.,
        futures_account_source=source, futures_execution_release_fingerprint="2" * 64,
    )
    split = _Split(source)
    result, prefix, context, prefix_context = _materialize_dated_futures_fold_outputs(
        tensor, split, 1, runtime=runtime,
    )
    with pytest.raises(ValueError, match="nonempty owned decision prefix"):
        _materialize_dated_futures_fold_outputs(tensor, split, 0, runtime=runtime)
    with pytest.raises(ValueError, match="calendar differs"):
        _materialize_dated_futures_fold_outputs(tensor, _Split(source, (0,)), 1, runtime=runtime)
    fold_result = FoldResult(
        fold_id=14, train_years=[2014], val_years=[2015], test_years=[2016],
        best_val_loss=-.1, val_ic={}, val_metrics={}, test_ic={}, test_metrics={},
    )
    config = SimpleNamespace(
        trading=TradingConfig(
            frequency="daily", buy_fee_rate=0., sell_fee_rate=0., long_only=False,
            execution_mode="tw_stock_context_futures_portfolio",
            tw_futures_portfolio_capital_basis="initial_margin"),
        training=TrainingConfig(non_blocking_transfer=False, save_daily_weights_table=False,
                                save_integer_share_daily_weights_table=False,
                                save_integer_share_holdings_table=False),
        walk_forward=WalkForwardConfig(), data=SimpleNamespace(benchmark_name="cash"),
    )
    kwargs = dict(fold_dir=tmp_path, fold_result=fold_result, model=None,
                  test_backtest=result, test_dates=source.calendar.decision_dates,
                  symbols=list(context.universe), config=config, deployment_backtest=prefix,
                  deployment_dates=source.calendar.decision_dates[:1], print_report=False,
                  write_plots=False, write_model_checkpoint=False)
    with pytest.raises(ValueError, match="full test ledger"):
        _save_fold_output_artifacts(**kwargs)
    with pytest.raises(ValueError, match="source-pinned test prefix"):
        _save_fold_output_artifacts(**kwargs, futures_account_context=context,
                                    deployment_futures_account_context=replace(
                                        prefix_context, execution_release_fingerprint="3" * 64))
    packet = np.zeros((2, 1936, 2), dtype=np.float32)
    mismatched_packet = packet[:1].copy()
    mismatched_packet[0, 0, 0] = 1
    with pytest.raises(ValueError, match="deployment policy differs"):
        _save_fold_output_artifacts(
            **{**kwargs, "test_backtest": replace(result, futures_account_policy_packet=packet),
               "deployment_backtest": replace(prefix, futures_account_policy_packet=mismatched_packet)},
            futures_account_context=context, deployment_futures_account_context=prefix_context)
    _save_fold_output_artifacts(**kwargs, futures_account_context=context,
                                deployment_futures_account_context=prefix_context)
    full, _ = _load_backtest_artifact(tmp_path / "test_backtest.npz", futures_account_context=context)
    owned, _ = _load_backtest_artifact(tmp_path / "deployment_test_backtest.npz",
                                       futures_account_context=prefix_context)
    np.testing.assert_array_equal(full.futures_account_ledger.dates, source.calendar.dates)
    np.testing.assert_array_equal(owned.futures_account_ledger.dates, source.calendar.dates[:1])
    assert fold_result.test_metrics["max_drawdown"] == pytest.approx(-.525, abs=1e-6)
    contract = json.loads((tmp_path / "mode_artifact_contract.json").read_text())
    assert contract["mode_details"]["futures_account_source_fingerprint"] == source.fingerprint


def test_report_uses_account_dates_for_risk_and_equity_curve(monkeypatch, tmp_path):
    monkeypatch.setenv("STOCKAGENT_EVAL_BACKTEST_COMPILE", "0")
    source, weights, decisions = _case()
    tensor, online_metrics = _evaluate(
        source, weights, decisions, chunk_rows=1, history=True,
        benchmark_returns=torch.log(torch.tensor([1.1, 1.2])),
    )
    context = FuturesAccountArtifactContext(
        universe=tuple(f"slot{i:04}" for i in range(1936)),
        event_source_fingerprint=source.fingerprint,
        execution_release_fingerprint="2" * 64,
        expected_calendar_fingerprint=source.calendar.fingerprint,
        initial_capital=1000., initial_equity_scale=1., initial_alive=True,
        initial_quantities=(0,) * 1936,
    )
    result = tensor.to_numpy(futures_account_context=context)
    dates = source.calendar.decision_dates
    reported = report.compute_metrics(result)
    assert reported == pytest.approx(online_metrics, rel=2e-5, abs=2e-6)
    assert reported["max_drawdown"] == pytest.approx(-.525, abs=1e-6)
    assert report.compute_metrics_by_year(result, dates)[2016]["max_drawdown"] == pytest.approx(-.525, abs=1e-6)
    assert "-52.50%" in report.generate_annual_report(result, dates)
    lines = []
    from matplotlib.axes import Axes
    original = Axes.plot

    def capture(ax, *args, **kwargs):
        if len(args) >= 2 and kwargs.get("label") in {"Strategy", "Benchmark"}:
            lines.append((np.asarray(args[0]).copy(), np.asarray(args[1]).copy(), kwargs["label"]))
        return original(ax, *args, **kwargs)

    monkeypatch.setattr(Axes, "plot", capture)
    report.plot_equity_curve(result, dates, tmp_path / "equity.png")
    assert (tmp_path / "equity.png").read_bytes().startswith(b"\x89PNG\r\n\x1a\n")
    strategy = next(line for line in lines if line[2] == "Strategy")
    np.testing.assert_array_equal(strategy[0], source.calendar.dates)
    np.testing.assert_allclose(strategy[1], [1., .475, 1.075], rtol=1e-5)
    benchmark = next(line for line in lines if line[2] == "Benchmark")
    np.testing.assert_array_equal(benchmark[0], source.calendar.dates)
    np.testing.assert_allclose(benchmark[1], [1.1, 1.1, 1.32], rtol=1e-6)
    with pytest.raises(ValueError, match="decision dates disagree"):
        report.compute_metrics_by_year(result, dates[:1])


def test_annual_report_attributes_account_event_to_its_actual_year(monkeypatch):
    monkeypatch.setenv("STOCKAGENT_EVAL_BACKTEST_COMPILE", "0")
    calendar = build_futures_account_calendar(
        ["2016-12-29", "2017-01-03"],
        ["2016-12-29", "2016-12-30", "2017-01-03"],
        [False, True, False],
    )
    source, weights, decisions = _case(calendar)
    tensor, _ = _evaluate(
        source, weights, decisions, chunk_rows=1, history=True,
        benchmark_returns=torch.log(torch.tensor([1.1, 1.2])),
    )
    context = FuturesAccountArtifactContext(
        universe=tuple(f"slot{i:04}" for i in range(1936)),
        event_source_fingerprint=source.fingerprint,
        execution_release_fingerprint="2" * 64,
        expected_calendar_fingerprint=calendar.fingerprint,
        initial_capital=1000., initial_equity_scale=1., initial_alive=True,
        initial_quantities=(0,) * 1936,
    )
    result = tensor.to_numpy(futures_account_context=context)
    annual = report.compute_metrics_by_year(result, calendar.decision_dates)
    assert annual[2016]["cumulative_return"] == pytest.approx(-.525, abs=1e-6)
    assert annual[2017]["cumulative_return"] == pytest.approx(1.075 / .475 - 1., rel=1e-5)
    assert annual[2016]["cumulative_benchmark"] == pytest.approx(.1, rel=1e-6)
    assert annual[2017]["cumulative_benchmark"] == pytest.approx(.2, rel=1e-6)


def test_tx_one_contract_report_keeps_account_day_marks_and_funding_out_of_return(monkeypatch, tmp_path):
    from matplotlib.axes import Axes
    from downloader.artifact_io import atomic_write_json
    from stockagent.data.tw_futures_benchmark import (
        TxOneContractReplayContext, tx_one_contract_replay_payload,
        tx_one_contract_replay_from_payload, tx_one_contract_replay_input_fingerprint,
        tx_one_contract_continuous_report_segment,
    )

    monkeypatch.setenv("STOCKAGENT_EVAL_BACKTEST_COMPILE", "0")
    calendar = build_futures_account_calendar(
        ["2016-12-29", "2017-01-03"],
        ["2016-12-29", "2016-12-30", "2017-01-03"],
        [False, True, False],
    )
    source, weights, decisions = _case(calendar)
    tensor, _ = _evaluate(source, weights, decisions, chunk_rows=1, history=True,
                          benchmark_returns=torch.log(torch.tensor([1.1, 1.2])))
    context = FuturesAccountArtifactContext(
        universe=source.universe, event_source_fingerprint=source.fingerprint,
        execution_release_fingerprint="2" * 64, expected_calendar_fingerprint=calendar.fingerprint,
        initial_capital=1000., initial_equity_scale=1., initial_alive=True,
        initial_quantities=(0,) * len(source.universe),
    )
    result = tensor.to_numpy(futures_account_context=context)
    events = [
        dict(time="2016-12-29T09:00:00+08:00", event="open", contract="201701", price=100,
             price_evidence="synthetic", margin_twd=100, margin_evidence="synthetic", cost_twd=0),
        dict(time="2016-12-29T13:45:00+08:00", event="mark", contract="201701", price=105,
             price_evidence="synthetic", margin_twd=100, margin_evidence="synthetic", cost_twd=0),
        dict(time="2016-12-30T13:45:00+08:00", event="mark", contract="201701", price=102,
             price_evidence="synthetic", margin_twd=150, margin_evidence="synthetic", cost_twd=0),
        dict(time="2017-01-03T13:45:00+08:00", event="mark", contract="201701", price=102,
             price_evidence="synthetic", margin_twd=150, margin_evidence="synthetic", cost_twd=0),
    ]
    replay_context = TxOneContractReplayContext("a" * 64, "b" * 64, calendar.fingerprint,
        tx_one_contract_replay_input_fingerprint(events, initial_capital_twd=100,
            multiplier_twd_per_point=1))
    payload = tx_one_contract_replay_payload(
        events, calendar=calendar, context=replay_context, initial_capital_twd=100,
        multiplier_twd_per_point=1,
    )
    replay_path = tmp_path / "tx_one_contract_benchmark.json"
    atomic_write_json(replay_path, payload)
    replay = tx_one_contract_replay_from_payload(
        json.loads(replay_path.read_text()), context=replay_context,
    )
    assert replay["external_deposit_twd"].tolist() == [0, 48, 0]
    assert replay["account_scope"] == "continuous_from_first_supplied_event"
    metrics = report.compute_metrics(result, futures_benchmark_replay=replay)
    assert metrics["cumulative_benchmark"] == pytest.approx(.02)
    assert report.compute_metrics(result)["cumulative_benchmark"] == pytest.approx(.32, rel=1e-6)
    annual = report.compute_metrics_by_year(result, calendar.decision_dates, futures_benchmark_replay=replay)
    assert annual[2016]["cumulative_benchmark"] == pytest.approx(.02)
    assert annual[2017]["cumulative_benchmark"] == pytest.approx(0.)
    segment, segment_context = _slice_backtest_rows(
        result, 1, 2, futures_account_context=context, return_futures_context=True,
    )
    replay_segment = tx_one_contract_continuous_report_segment(
        payload, context=replay_context, start=1, stop=2,
    )
    assert segment_context.expected_calendar_fingerprint == replay_segment["account_calendar_fingerprint"]
    segment_metrics = report.compute_metrics(segment, futures_benchmark_replay=replay_segment)
    assert segment_metrics["cumulative_benchmark"] == pytest.approx(102 / 105 - 1)
    assert replay_segment["external_deposit_twd"].tolist() == [48, 0]
    text_report = report.generate_annual_report(result, calendar.decision_dates,
                                                futures_benchmark_replay=replay)
    assert "2.00%" in text_report
    assert report.TX_ONE_CONTRACT_BENCHMARK_LABEL in text_report

    lines = []
    original = Axes.plot
    def capture(ax, *args, **kwargs):
        if len(args) >= 2 and kwargs.get("label") == report.TX_ONE_CONTRACT_BENCHMARK_LABEL:
            lines.append((np.asarray(args[0]).copy(), np.asarray(args[1]).copy()))
        return original(ax, *args, **kwargs)
    monkeypatch.setattr(Axes, "plot", capture)
    report.plot_equity_curve(result, calendar.decision_dates, tmp_path / "equity.png",
                             benchmark_label="legacy 1x gross",
                             futures_benchmark_replay=replay)
    report.plot_equity_curve_log(result, calendar.decision_dates, tmp_path / "equity-log.png",
                                 benchmark_label="legacy 1x gross",
                                 futures_benchmark_replay=replay)
    report.plot_configured_leverage_equity_curve(
        result, calendar.decision_dates, tmp_path / "leverage.png",
        benchmark_label="legacy 1x gross", futures_benchmark_replay=replay,
    )
    assert len(lines) == 3
    for x, y in lines:
        np.testing.assert_array_equal(x, calendar.dates)
        np.testing.assert_allclose(y, [1.05, 1.02, 1.02])

    for changed in (
        {"account_calendar_fingerprint": "0" * 64},
        {"decision_dates": calendar.decision_dates[:1]},
        {"market_session_mask": np.ones(3, dtype=bool)},
        {"benchmark_log_returns": np.array([0., np.nan, 0.])},
        {"decision_log_returns": np.zeros(2)},
    ):
        with pytest.raises(ValueError, match="dated TX benchmark replay"):
            report.compute_metrics(result, futures_benchmark_replay={**replay, **changed})

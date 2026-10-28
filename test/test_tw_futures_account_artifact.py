from dataclasses import replace

import numpy as np
import pytest
import torch

from stockagent.backtest.tw_futures_account_calendar import run_futures_account_calendar_torch
from stockagent.data import tw_futures_margin as margin
from stockagent.training.futures_account_artifact import (
    FuturesAccountArtifactContext, futures_account_artifact_from_payload,
    futures_account_artifact_payload,
)
from test_tw_futures_account_calendar import calendar
from test_tw_futures_margin import tape


def _case(terminal_mark=1100):
    decisions, events = tape(rows=2), tape(rows=1)
    events[..., 1] = 0
    events[..., 8] = 0
    events[..., 2] = 1
    events[..., margin.CASH_SETTLEMENT] = 1
    events[..., margin.TERMINAL_MARK] = terminal_mark
    events[..., 5] = 2
    events[..., 7] = 3
    result = run_futures_account_calendar_torch(
        calendar(), torch.tensor([[.5], [0.]]), decisions, events, initial_capital=1000.,
    )
    context = FuturesAccountArtifactContext(
        universe=("TAIFEX_SLOT_0001",), event_source_fingerprint="1" * 64,
        execution_release_fingerprint="2" * 64, expected_calendar_fingerprint=calendar().fingerprint,
        initial_capital=1000.,
        initial_equity_scale=1., initial_alive=True, initial_quantities=(0,),
    )
    return result, context


def test_dated_archive_roundtrip_keeps_real_cash_settlement(tmp_path):
    result, context = _case()
    payload = futures_account_artifact_payload(result, context)
    path = tmp_path / "backtest.npz"
    np.savez(path, **payload)
    with np.load(path, allow_pickle=False) as saved:
        ledger = futures_account_artifact_from_payload(saved, context)
    assert ledger.dates.astype(str).tolist() == ["2016-02-03", "2016-02-04", "2016-02-15"]
    assert ledger.decision_dates.astype(str).tolist() == ["2016-02-03", "2016-02-15"]
    assert ledger.account_only_mask.tolist() == [False, True, False]
    assert ledger.opening_quantities[:, 0].tolist() == [5, 5, 0]
    assert ledger.residual_quantities[:, 0].tolist() == [5, 0, 0]
    assert ledger.margin_audit[1, 2] == 1475
    assert ledger.strategy_returns[1] > 0
    assert ledger.decision_returns[1] == ledger.strategy_returns[1] + ledger.strategy_returns[2]
    assert ledger.final_quantities.tolist() == [0]


def test_dated_archive_records_event_day_bankruptcy_and_absorbing_state():
    result, context = _case(terminal_mark=1)
    ledger = futures_account_artifact_from_payload(futures_account_artifact_payload(result, context), context)
    assert ledger.default.tolist() == [False, True, False]
    assert ledger.default_reason.tolist() == [0, 3, 0]
    assert ledger.margin_audit[1, 2] < 0
    assert ledger.equity_scale.tolist() == [1, 0, 0]
    assert not ledger.final_alive
    assert ledger.strategy_returns[2] == 0


def test_dated_archive_rejects_wrong_source_missing_fields_and_moved_event():
    result, context = _case()
    payload = futures_account_artifact_payload(result, context)
    with pytest.raises(ValueError, match="pinned context differs"):
        futures_account_artifact_from_payload(payload, replace(context, event_source_fingerprint="3" * 64))
    missing = dict(payload)
    del missing["futures_account_ledger_residual_quantities"]
    with pytest.raises(ValueError, match="fields differ"):
        futures_account_artifact_from_payload(missing, context)
    moved = dict(payload)
    moved["futures_account_ledger_dates"] = moved["futures_account_ledger_dates"].copy()
    moved["futures_account_ledger_dates"][1] = np.datetime64("2016-02-05")
    with pytest.raises(ValueError, match="fingerprint changed"):
        futures_account_artifact_from_payload(moved, context)
    wrong_returns = dict(payload)
    wrong_returns["futures_account_ledger_decision_returns"] = np.zeros(2, dtype=np.float32)
    with pytest.raises(ValueError, match="omit or move"):
        futures_account_artifact_from_payload(wrong_returns, context)


def test_dated_archive_refuses_unrecorded_history_or_false_incoming_state():
    result, context = _case()
    with pytest.raises(ValueError, match="actual incoming state"):
        futures_account_artifact_payload(result, replace(context, initial_quantities=(1,)))
    with pytest.raises(ValueError, match="actual incoming state"):
        futures_account_artifact_payload(result, replace(context, initial_capital=2000.))
    result.account_ledger.margin_audit_history = None
    with pytest.raises(ValueError, match="recorded margin audit"):
        futures_account_artifact_payload(result, context)


@pytest.mark.skipif(not torch.cuda.is_available(), reason="CUDA is unavailable")
def test_dated_account_cuda_matches_cpu_ledger_and_action_gradient():
    _, context = _case()
    decisions, events = tape(rows=2), tape(rows=1)
    events[..., 1] = events[..., 8] = 0
    events[..., 2] = events[..., margin.CASH_SETTLEMENT] = 1
    events[..., margin.TERMINAL_MARK] = 1100
    events[..., 5] = 2
    events[..., 7] = 3
    def run(device):
        actions = torch.tensor([[.5], [0.]], device=device, requires_grad=True)
        result = run_futures_account_calendar_torch(
            calendar(), actions, decisions.to(device), events.to(device), initial_capital=1000.,
        )
        ledger = futures_account_artifact_from_payload(
            futures_account_artifact_payload(result, context), context,
        )
        result.decision_log_returns.sum().backward()
        return ledger, actions.grad.detach().cpu()
    cpu, cpu_gradient = run("cpu")
    cuda, cuda_gradient = run("cuda")
    for name in ("strategy_returns", "decision_returns", "turnovers", "opening_quantities",
                 "residual_quantities", "equity_scale", "default", "default_reason", "margin_audit"):
        np.testing.assert_allclose(getattr(cuda, name), getattr(cpu, name), rtol=2e-5, atol=2e-6)
    torch.testing.assert_close(cuda_gradient, cpu_gradient, rtol=2e-5, atol=2e-6)

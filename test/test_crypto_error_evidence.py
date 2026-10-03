"""Failure-only crypto evidence must preserve coordinates and data semantics."""
from __future__ import annotations

import json
import os
import pickle
from types import SimpleNamespace

import numpy as np
import pytest
import torch

from stockagent.backtest.crypto_perpetual import CryptoPerpetualDataError, run_crypto_perpetual_torch
from stockagent.training import trainer


def _runtime():
    return trainer._ExecutionRuntime(
        mode="crypto_perpetual", buy_fee_rates=None, sell_fee_rates=None,
        lot_sizes=None, settlement_lag_sessions=0,
        diagnostic_panel_dates=np.arange("2026-09-01", "2026-09-11", dtype="datetime64[D]"),
        diagnostic_panel_symbols=["AAAUSDT", "BBBUSDT", "CCCUSDT", "DDDUSDT"],
    )


@pytest.mark.parametrize("keep_history", [False, True])
@pytest.mark.parametrize("price_known,effective_known", [(False, True), (True, False), (False, False)])
def test_ledger_reports_first_invalid_cell_without_inventing_funding_availability(
    keep_history, price_known, effective_known,
):
    target = torch.zeros((5, 2))
    target[:, 1] = 0.5
    price, effective = torch.zeros_like(target), torch.zeros_like(target)
    price[2, 1] = 0.0 if price_known else float("nan")
    effective[2, 1] = 0.0 if effective_known else float("nan")
    yes = torch.ones_like(target, dtype=torch.bool)
    eligible, sell, exit_request = yes.clone(), yes.clone(), ~yes
    eligible[2, 1], sell[2, 1], exit_request[2, 1] = False, False, True
    with pytest.raises(CryptoPerpetualDataError) as caught:
        run_crypto_perpetual_torch(
            target, effective, price, eligible, yes, sell, yes, exit_request,
            buy_fee_rate=0.0, sell_fee_rate=0.0, long_only=False, maximum_gross=1.0,
            return_weights_history=keep_history,
        )
    evidence = caught.value.evidence
    assert (evidence["row"], evidence["symbol_index"]) == (2, 1)
    assert evidence["price_valuation_available"] is price_known
    assert evidence["funding_adjusted_valuation_available"] is effective_known
    assert "raw funding availability is not passed" in evidence["valuation_availability_note"]
    assert evidence["force_exit"] is True and evidence["tradable"] is False
    assert evidence["can_sell_source"] is False
    assert evidence["can_sell_effective"] is False
    assert evidence["can_buy_effective"] is True
    assert evidence["remaining_executed_weight"] == (0.5 if keep_history else None)
    assert evidence["volume_capacity_weight_reference"] is None
    assert evidence["volume_capacity_weight_live"] is None
    assert evidence["volume_capacity_unlimited"] is True
    assert evidence["incoming_equity_scale"] == 1.0
    assert json.loads(json.dumps(evidence, allow_nan=False)) == evidence
    restored = pickle.loads(pickle.dumps(caught.value))
    assert restored.evidence == evidence
    assert str(restored) == str(caught.value)


@pytest.mark.parametrize("initial_scale", [None, 2.0, [2.0]])
def test_missing_mark_reports_capacity_in_carried_nav_units(initial_scale):
    target = torch.zeros((1, 1))
    yes = torch.ones_like(target, dtype=torch.bool)
    with pytest.raises(CryptoPerpetualDataError) as caught:
        run_crypto_perpetual_torch(
            target, torch.full_like(target, float("nan")), torch.zeros_like(target),
            yes, yes, ~yes, yes, yes, buy_fee_rate=0.0, sell_fee_rate=0.0,
            long_only=False, maximum_gross=1.0, initial_weights=torch.tensor([0.5]),
            initial_equity_scale=None if initial_scale is None else torch.tensor(initial_scale),
            volume_limit_weights=torch.full_like(target, 0.1),
        )
    evidence = caught.value.evidence
    expected_scale = 1.0 if initial_scale is None else float(torch.tensor(initial_scale).reshape(()))
    assert evidence["incoming_equity_scale"] == expected_scale
    assert evidence["volume_capacity_weight_reference"] == pytest.approx(0.1)
    assert evidence["volume_capacity_weight_live"] == pytest.approx(0.1 / expected_scale)
    assert evidence["volume_capacity_unlimited"] is False
    assert evidence["remaining_executed_weight"] == 0.5
    json.dumps(evidence, allow_nan=False)


@pytest.mark.skipif(
    os.environ.get("STOCKAGENT_TEST_CRYPTO_CUDA") != "1" or not torch.cuda.is_available(),
    reason="explicit bounded CUDA ledger check only",
)
def test_cuda_failure_accepts_cpu_one_element_initial_equity_scale(monkeypatch):
    monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "0")
    target = torch.zeros((1, 1), device="cuda")
    yes = torch.ones_like(target, dtype=torch.bool)
    with pytest.raises(CryptoPerpetualDataError) as caught:
        run_crypto_perpetual_torch(
            target, torch.full_like(target, float("nan")), torch.zeros_like(target),
            yes, yes, ~yes, yes, yes, buy_fee_rate=0.0, sell_fee_rate=0.0,
            long_only=False, maximum_gross=1.0,
            initial_weights=torch.tensor([0.5], device="cuda"),
            initial_equity_scale=torch.tensor([2.0], device="cpu"),
            volume_limit_weights=torch.full_like(target, 0.1),
        )
    assert caught.value.evidence["incoming_equity_scale"] == 2.0
    assert caught.value.evidence["volume_capacity_weight_live"] == pytest.approx(0.05)
    json.dumps(caught.value.evidence, allow_nan=False)


def test_train_failure_maps_compacted_symbol_and_panel_date_before_clearing_gradients():
    split = SimpleNamespace(
        execution_mode="crypto_perpetual", overnight_log_returns=None,
        _valid_indices_cpu=torch.tensor([2, 4, 6, 8]), symbol_indices=torch.tensor([3, 1]),
    )
    parameter = torch.nn.Parameter(torch.tensor(1.0))
    parameter.grad = torch.tensor(7.0)
    optimizer = torch.optim.AdamW([parameter])
    with pytest.raises(CryptoPerpetualDataError) as caught:
        with trainer._carry_loss_data_guard(
            split, optimizer, torch.device("cpu"), 1, execution_runtime=_runtime(),
        ):
            raise CryptoPerpetualDataError("missing mark", evidence={"row": 1, "symbol_index": 1})
    assert caught.value.evidence == {
        "row": 1, "symbol_index": 1, "scope": "train", "chunk_start": 1,
        "split_row": 2, "panel_row": 6, "date": "2026-09-07",
        "global_symbol_index": 1, "symbol": "BBBUSDT",
    }
    assert parameter.grad is None and not optimizer.state


def test_evaluation_failure_maps_actual_chunk_to_global_date_and_symbol():
    target = torch.full((3, 2), 0.25)
    returns, price = torch.zeros_like(target), torch.zeros_like(target)
    returns[2, 1] = float("nan")
    yes = torch.ones_like(target, dtype=torch.bool)
    with pytest.raises(CryptoPerpetualDataError) as caught:
        trainer._run_eval_backtest_from_weight_buffers(
            target, returns, yes, yes, yes, yes, ~yes, ~yes, torch.zeros(3),
            device=torch.device("cpu"), non_blocking=False, long_only=False,
            buy_fee_rate=0.0, sell_fee_rate=0.0, max_turnover_ratio=0.0,
            gross_leverage=1.0, min_trade_weight=0.0, backtest_chunk_rows=2,
            compute_metrics_summary=False, return_weights_history=True,
            profile_timing=False, progress_label="validation fold 5",
            timing=trainer.TimingBreakdown(), reset_at_rows=None,
            portfolio_activation="pre_normalized", overnight_log_returns_all=price,
            execution_runtime=_runtime(), panel_row_indices=torch.tensor([2, 4, 8]),
            symbol_indices=torch.tensor([3, 1]),
        )
    evidence = caught.value.evidence
    assert evidence["row"] == 0 and evidence["split_row"] == 2
    assert evidence["panel_row"] == 8 and evidence["date"] == "2026-09-09"
    assert evidence["symbol_index"] == 1 and evidence["symbol"] == "BBBUSDT"
    assert evidence["chunk_start"] == 2 and evidence["chunk_end"] == 3
    assert evidence["scope"] == "evaluation"
    assert evidence["progress_label"] == "validation fold 5"
    assert evidence["price_valuation_available"] is True
    assert evidence["funding_adjusted_valuation_available"] is False


def test_valid_train_guard_never_resolves_diagnostic_identities(monkeypatch):
    def unexpected(*args, **kwargs):
        raise AssertionError("normal path attempted diagnostic mapping")
    monkeypatch.setattr(trainer, "_enrich_crypto_data_error", unexpected)
    parameter = torch.nn.Parameter(torch.tensor(1.0))
    optimizer = torch.optim.AdamW([parameter])
    split = SimpleNamespace(execution_mode="crypto_perpetual", overnight_log_returns=None)
    with trainer._carry_loss_data_guard(split, optimizer, torch.device("cpu"), 0):
        pass

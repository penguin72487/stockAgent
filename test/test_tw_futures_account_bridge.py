from types import SimpleNamespace

import numpy as np
import pytest
import torch

from stockagent.backtest.tw_futures_account_calendar import run_futures_account_calendar_torch
from stockagent.data import tw_futures_margin as margin
from stockagent.training.futures_account_bridge import (
    PreparedFuturesAccountSource, bind_futures_account_loss, bind_padded_futures_account_loss,
    futures_account_execution_fingerprint,
)
from test_tw_futures_account_calendar import calendar
from test_tw_futures_margin import tape


def _source():
    dates = calendar()
    market = tape(rows=2).numpy()
    event = tape(rows=1).numpy()
    event[..., 1] = 0
    event[..., 8] = 0
    event[..., 2] = 1
    event[..., margin.CASH_SETTLEMENT] = 1
    event[..., margin.TERMINAL_MARK] = 1100
    event[..., 5] = 2
    event[..., 7] = 3
    daily = SimpleNamespace(futures_account_calendar=dates, integer_execution=market,
                            account_only_integer_execution=event, symbols=("slot0",))
    return daily, PreparedFuturesAccountSource.from_daily(daily, dates.decision_dates)


def _split(*rows):
    return SimpleNamespace(_valid_indices_cpu=torch.tensor(rows, dtype=torch.long))


def test_prepared_source_copies_and_checks_dated_identity():
    daily, source = _source()
    source.verify_identity()
    daily.account_only_integer_execution[0, 0, margin.TERMINAL_MARK] = 12
    assert source.events[0, 0, margin.TERMINAL_MARK] == 1100
    source.verify_identity()
    source.events[0, 0, margin.TERMINAL_MARK] = 13
    with pytest.raises(ValueError, match="identity changed"):
        source.verify_identity()
    with pytest.raises(ValueError, match="panel dates"):
        PreparedFuturesAccountSource.from_daily(daily, ["2016-02-03", "2016-02-16"])
    daily.account_only_integer_execution[0, 0, 8] = 1
    with pytest.raises(ValueError, match="executable capacity"):
        PreparedFuturesAccountSource.from_daily(daily, daily.futures_account_calendar.decision_dates)
    daily.account_only_integer_execution[0, 0, 8] = np.nan
    with pytest.raises(ValueError, match="executable capacity"):
        PreparedFuturesAccountSource.from_daily(daily, daily.futures_account_calendar.decision_dates)
    daily.account_only_integer_execution[0, 0, 8] = 0
    daily.account_only_integer_execution[0, 0, 2] = np.nan
    with pytest.raises(ValueError, match="terminal events"):
        PreparedFuturesAccountSource.from_daily(daily, daily.futures_account_calendar.decision_dates)


def test_prepared_source_pins_ordered_slot_names():
    daily, source = _source()
    daily.symbols = ("another-slot",)
    changed = PreparedFuturesAccountSource.from_daily(daily, daily.futures_account_calendar.decision_dates)
    assert changed.fingerprint != source.fingerprint
    daily.symbols = ()
    with pytest.raises(ValueError, match="ordered slot universe"):
        PreparedFuturesAccountSource.from_daily(daily, daily.futures_account_calendar.decision_dates)


def test_execution_identity_pins_decision_tape_mask_and_clock():
    daily, source = _source()
    daily.must_liquidate_mask = np.zeros(daily.integer_execution.shape[:2], dtype=bool)
    daily.decision_clock_contract = {"decision_time": "08:45", "source_path": "/old/location"}
    first = futures_account_execution_fingerprint(daily, source)
    daily.decision_clock_contract["source_path"] = "/new/location"
    assert futures_account_execution_fingerprint(daily, source) == first
    daily.integer_execution[0, 0, 3] += 1
    assert futures_account_execution_fingerprint(daily, source) != first
    daily.integer_execution[0, 0, 3] -= 1
    daily.must_liquidate_mask[0, 0] = True
    assert futures_account_execution_fingerprint(daily, source) != first
    daily.must_liquidate_mask[0, 0] = False
    daily.decision_clock_contract["decision_time"] = "09:00"
    assert futures_account_execution_fingerprint(daily, source) != first


def test_prepared_decision_execution_is_a_shared_owned_snapshot():
    daily, _ = _source()
    daily.must_liquidate_mask = np.zeros(daily.integer_execution.shape[:2], dtype=bool)
    daily.must_liquidate_mask[0, 0] = True
    daily.margin_contract_version = 1
    source = PreparedFuturesAccountSource.from_daily(
        daily, daily.futures_account_calendar.decision_dates, prepare_execution=True,
    )
    assert source.decision_execution[0, 0, 2] == 1
    assert source.execution_release_fingerprint == futures_account_execution_fingerprint(daily, source)
    before = source.decision_execution[0, 0, 3].item()
    daily.integer_execution[0, 0, 3] += 1
    assert source.decision_execution[0, 0, 3].item() == before
    source.verify_identity()
    source.decision_execution[0, 0, 3] += 1
    with pytest.raises(ValueError, match="decision execution was modified"):
        source.verify_identity()


def test_prepared_training_loss_consumes_its_source_release_after_padding():
    daily, _ = _source()
    daily.must_liquidate_mask = np.zeros(daily.integer_execution.shape[:2], dtype=bool)
    daily.margin_contract_version = 1
    source = PreparedFuturesAccountSource.from_daily(
        daily, daily.futures_account_calendar.decision_dates, prepare_execution=True,
    )
    split = SimpleNamespace(_valid_indices_cpu=torch.tensor([0, 1, 1]),
                            sample_mask=torch.tensor([True, True, False]),
                            execution_mode="tw_stock_context_futures_portfolio")
    seen = []

    def exact_loss(actions, _returns, _mask, **kwargs):
        seen.append(kwargs["overnight_log_returns"].clone())
        result = run_futures_account_calendar_torch(
            kwargs["futures_account_calendar"], actions, kwargs["overnight_log_returns"],
            kwargs["account_only_execution"], initial_capital=1000.,
        )
        return -result.decision_log_returns.sum()

    bound = bind_padded_futures_account_loss(
        exact_loss, source=source, split=split, start=0, stop=3, device=torch.device("cpu"),
    )
    actions = torch.tensor([[.5], [0.], [.8]], requires_grad=True)
    supplied = torch.from_numpy(daily.integer_execution[[0, 1, 1]]).clone()
    supplied[0, 0, 3] *= 10
    loss = bound(actions, torch.zeros(3, 1), torch.ones(3, 1, dtype=torch.bool),
                 objective="log_utility", sample_mask=split.sample_mask,
                 state_advance_mask=split.sample_mask, overnight_log_returns=supplied)
    torch.testing.assert_close(seen[0], source.decision_execution)
    reference_actions = actions[:2].detach().clone().requires_grad_()
    reference = run_futures_account_calendar_torch(
        source.calendar, reference_actions, source.decision_execution, source.events,
        initial_capital=1000.,
    )
    torch.testing.assert_close(loss, -reference.decision_log_returns.sum())
    loss.backward()
    (-reference.decision_log_returns.sum()).backward()
    torch.testing.assert_close(actions.grad[:2], reference_actions.grad)
    assert not actions.grad[2].any()


def test_prepared_decision_execution_requires_margin_account():
    daily, _ = _source()
    daily.must_liquidate_mask = np.zeros(daily.integer_execution.shape[:2], dtype=bool)
    with pytest.raises(ValueError, match="cash-denominated margin account"):
        PreparedFuturesAccountSource.from_daily(
            daily, daily.futures_account_calendar.decision_dates, prepare_execution=True,
        )


def test_prepared_decision_execution_tracks_versions_inside_inference_mode():
    daily, _ = _source()
    daily.must_liquidate_mask = np.zeros(daily.integer_execution.shape[:2], dtype=bool)
    daily.margin_contract_version = 1
    with torch.inference_mode():
        source = PreparedFuturesAccountSource.from_daily(
            daily, daily.futures_account_calendar.decision_dates, prepare_execution=True,
        )
    assert not torch.is_inference(source.decision_execution)
    source.verify_identity()


def test_sparse_batches_carry_real_settlement_and_refuse_skipped_rows():
    daily, source = _source()
    split = _split(0, 1)
    first = source.prepare_batch(split, 0, 1)
    second = source.prepare_batch(split, 1, 2)
    assert len(first.calendar.account_only_indices) == 0
    assert second.calendar.account_only_mask.tolist() == [True, False]
    assert second.account_execution.data_ptr() == source.events.data_ptr()
    with pytest.raises(ValueError, match="contiguous"):
        source.prepare_batch(_split(0, 2), 0, 2)

    def exact_loss(actions, returns, mask, **kwargs):
        aux = kwargs["aux_outputs"]
        result = run_futures_account_calendar_torch(
            kwargs["futures_account_calendar"], actions, kwargs["overnight_log_returns"],
            kwargs["account_only_execution"], initial_capital=1000.,
            initial_quantities=aux.get("initial_weights"),
            initial_equity_scale=aux.get("initial_equity_scale"),
            initial_alive=aux.get("initial_alive"),
        )
        ledger = result.account_ledger
        aux.update(_final_weights=ledger.final_weights, _final_equity_scale=ledger.final_equity_scale,
                   _final_alive=ledger.final_alive)
        return -result.decision_log_returns.sum()

    first_loss = bind_futures_account_loss(exact_loss, source=source, split=split,
                                           start=0, stop=1, device=torch.device("cpu"))
    second_loss = bind_futures_account_loss(exact_loss, source=source, split=split,
                                            start=1, stop=2, device=torch.device("cpu"))
    actions = torch.tensor([[.5], [0.]], requires_grad=True)
    returns = torch.zeros(1, 1)
    mask = torch.ones(1, 1, dtype=torch.bool)
    aux = {}
    common = dict(execution_mode="tw_stock_context_futures_portfolio", objective="log_utility",
                  state_advance_mask=torch.ones(1, dtype=torch.bool))
    loss_a = first_loss(actions[:1], returns, mask, aux_outputs=aux,
                        overnight_log_returns=torch.from_numpy(daily.integer_execution[:1]), **common)
    incoming = {"initial_weights": aux["_final_weights"],
                "initial_equity_scale": aux["_final_equity_scale"], "initial_alive": aux["_final_alive"]}
    with pytest.raises(ValueError, match="explicit incoming account state"):
        second_loss(actions[1:], returns, mask, aux_outputs={}, **common)
    loss_b = second_loss(actions[1:], returns, mask, aux_outputs=incoming,
                         overnight_log_returns=torch.from_numpy(daily.integer_execution[1:]), **common)
    reference = run_futures_account_calendar_torch(
        source.calendar, actions, torch.from_numpy(daily.integer_execution), source.events,
        initial_capital=1000.,
    )
    torch.testing.assert_close(loss_a + loss_b, -reference.decision_log_returns.sum())
    torch.testing.assert_close(incoming["_final_equity_scale"], reference.account_ledger.final_equity_scale)
    (loss_a + loss_b).backward()
    assert torch.isfinite(actions.grad).all()
    with pytest.raises(ValueError, match="exact unpadded"):
        first_loss(actions, returns, mask, aux_outputs={}, **common)
    with pytest.raises(ValueError, match="cannot skip"):
        first_loss(actions[:1], returns, mask, aux_outputs={}, **{**common,
                   "state_advance_mask": torch.zeros(1, dtype=torch.bool)})


def test_training_boundary_event_is_consumed_once_with_additive_credit():
    daily, source = _source()
    split = _split(0, 1)
    first = source.prepare_training_batch(split, 0, 1)
    second = source.prepare_training_batch(split, 1, 2)
    assert first.calendar.account_only_mask.tolist() == [False, True]
    assert first.calendar.training_trailing_event_credit
    assert second.calendar.account_only_mask.tolist() == [False]
    assert not second.calendar.training_trailing_event_credit
    assert first.account_event_start == 0 and first.account_event_stop == 1
    assert second.account_event_start == 1 and second.account_event_stop == 1
    assert first.account_execution.data_ptr() == source.events.data_ptr()
    assert first.calendar.fingerprint != source.calendar.fingerprint

    actions = torch.tensor([[.5], [0.]], requires_grad=True)
    first_result = run_futures_account_calendar_torch(
        first.calendar, actions[:1], torch.from_numpy(daily.integer_execution[:1]),
        first.account_execution, initial_capital=1000.,
    )
    second_result = run_futures_account_calendar_torch(
        second.calendar, actions[1:], torch.from_numpy(daily.integer_execution[1:]),
        second.account_execution, initial_capital=1000.,
        initial_quantities=first_result.account_ledger.final_weights.detach(),
        initial_equity_scale=first_result.account_ledger.final_equity_scale.detach(),
        initial_alive=first_result.account_ledger.final_alive.detach(),
    )
    full = run_futures_account_calendar_torch(
        source.calendar, actions, torch.from_numpy(daily.integer_execution),
        source.events, initial_capital=1000.,
    )
    torch.testing.assert_close(first_result.decision_log_returns.sum()
                               + second_result.decision_log_returns.sum(),
                               full.decision_log_returns.sum())
    torch.testing.assert_close(second_result.account_ledger.final_equity_scale,
                               full.account_ledger.final_equity_scale)
    chunk_grad, = torch.autograd.grad(first_result.decision_log_returns.sum()
                                     + second_result.decision_log_returns.sum(), actions)
    full_grad, = torch.autograd.grad(full.decision_log_returns.sum(), actions)
    torch.testing.assert_close(chunk_grad, full_grad)
    assert chunk_grad[0].abs().item() > 0

    from stockagent.training.loss import risk_aware_loss
    with pytest.raises(ValueError, match="unweighted additive log utility"):
        risk_aware_loss(actions[:1], torch.zeros(1, 1), torch.ones(1, 1, dtype=torch.bool),
                        objective="log_utility", execution_mode="tw_stock_context_futures_portfolio",
                        log_utility_risk_aversion=1., futures_account_calendar=first.calendar)


def test_evaluation_chunk_merge_retains_original_dated_account_and_state():
    from dataclasses import replace
    from stockagent.backtest.tw_futures_account_calendar import merge_futures_account_calendar_results

    daily, source = _source()
    split = _split(0, 1)
    actions = torch.tensor([[.5], [0.]])
    first, second = source.prepare_batch(split, 0, 1), source.prepare_batch(split, 1, 2)
    a = run_futures_account_calendar_torch(
        first.calendar, actions[:1], torch.from_numpy(daily.integer_execution[:1]),
        first.account_execution, initial_capital=1000.,
    )
    b = run_futures_account_calendar_torch(
        second.calendar, actions[1:], torch.from_numpy(daily.integer_execution[1:]),
        second.account_execution, initial_capital=1000.,
        initial_quantities=a.account_ledger.final_weights,
        initial_equity_scale=a.account_ledger.final_equity_scale,
        initial_alive=a.account_ledger.final_alive,
    )
    merged = merge_futures_account_calendar_results([a, b], source.calendar)
    whole = run_futures_account_calendar_torch(
        source.calendar, actions, torch.from_numpy(daily.integer_execution), source.events,
        initial_capital=1000.,
    )
    assert merged.calendar.dates.tolist() == source.calendar.dates.tolist()
    for field in ("strategy_returns", "margin_audit_history", "contract_quantities_history",
                  "residual_contract_quantities_history", "equity_scale_history"):
        torch.testing.assert_close(getattr(merged.account_ledger, field),
                                   getattr(whole.account_ledger, field))
    torch.testing.assert_close(merged.decision_log_returns, whole.decision_log_returns)
    with pytest.raises(ValueError, match="incoming state"):
        merge_futures_account_calendar_results(
            [a, replace(b, initial_quantities=b.initial_quantities + 1)], source.calendar,
        )
    with pytest.raises(ValueError, match="source dates"):
        merge_futures_account_calendar_results([a], source.calendar)
    training = source.prepare_training_batch(split, 0, 1)
    training_result = run_futures_account_calendar_torch(
        training.calendar, actions[:1], torch.from_numpy(daily.integer_execution[:1]),
        training.account_execution, initial_capital=1000.,
    )
    with pytest.raises(ValueError, match="training event attribution"):
        merge_futures_account_calendar_results([training_result, b], source.calendar)


def test_padded_gather_trims_only_duplicate_suffix_and_keeps_exact_gradient():
    daily, source = _source()
    split = SimpleNamespace(_valid_indices_cpu=torch.tensor([0, 1, 1, 1]),
                            sample_mask=torch.tensor([True, True, False, False]),
                            execution_mode="tw_stock_context_futures_portfolio")
    def exact_loss(actions, returns, mask, **kwargs):
        assert kwargs["execution_mode"] == split.execution_mode
        assert kwargs["benchmark_returns"].shape == (2,)
        assert kwargs["symbol_indices"].tolist() == [0, 1, 2, 3]
        result = run_futures_account_calendar_torch(
            kwargs["futures_account_calendar"], actions, kwargs["overnight_log_returns"],
            kwargs["account_only_execution"], initial_capital=1000.,
        )
        aux = kwargs["aux_outputs"]
        aux["_daily_log_returns"] = result.decision_log_returns.detach().clone()
        aux["_final_equity_scale"] = result.account_ledger.final_equity_scale
        return -result.decision_log_returns.sum()
    loss = bind_padded_futures_account_loss(exact_loss, source=source, split=split,
                                            start=0, stop=4, device=torch.device("cpu"))
    # Concatenation represents the ordered outputs after the rank-wise DDP
    # gather. Padded model rows must get exactly zero action gradients.
    rank0 = torch.tensor([[.5], [0.]], requires_grad=True)
    rank1 = torch.tensor([[.8], [-.8]], requires_grad=True)
    actions = torch.cat((rank0, rank1), dim=0)
    aux = {"_capture_daily_log_returns": True}
    result = loss(actions, torch.zeros(4, 1), torch.ones(4, 1, dtype=torch.bool),
                  objective="log_utility", sample_mask=split.sample_mask,
                  state_advance_mask=split.sample_mask.clone(), benchmark_returns=torch.zeros(4),
                  symbol_indices=torch.arange(4), aux_outputs=aux,
                  overnight_log_returns=torch.from_numpy(daily.integer_execution[[0, 1, 1, 1]]))
    reference_actions = rank0.detach().clone().requires_grad_()
    reference = run_futures_account_calendar_torch(
        source.calendar, reference_actions, torch.from_numpy(daily.integer_execution),
        source.events, initial_capital=1000.,
    )
    torch.testing.assert_close(result, -reference.decision_log_returns.sum())
    torch.testing.assert_close(aux["_final_equity_scale"], reference.account_ledger.final_equity_scale)
    assert aux["_daily_log_returns"].shape == (4,)
    assert not aux["_daily_log_returns"][2:].any()
    result.backward()
    (-reference.decision_log_returns.sum()).backward()
    torch.testing.assert_close(rank0.grad, reference_actions.grad)
    assert not rank1.grad.any()
    with pytest.raises(ValueError, match="differs from the verified"):
        loss(actions, torch.zeros(4, 1), torch.ones(4, 1, dtype=torch.bool),
             objective="log_utility", sample_mask=torch.tensor([True, False, True, False]))


def test_padded_gather_rejects_interior_holes_and_nonduplicate_dates():
    _, source = _source()
    split = SimpleNamespace(_valid_indices_cpu=torch.tensor([0, 1, 1, 1]),
                            sample_mask=torch.tensor([True, False, True, False]))
    with pytest.raises(ValueError, match="nonempty real prefix"):
        bind_padded_futures_account_loss(lambda *_a, **_k: None, source=source, split=split,
                                          start=0, stop=4, device=torch.device("cpu"))
    split.sample_mask = torch.tensor([True, True, False, False])
    split._valid_indices_cpu[-1] = 2
    with pytest.raises(ValueError, match="duplicate the last real decision"):
        bind_padded_futures_account_loss(lambda *_a, **_k: None, source=source, split=split,
                                          start=0, stop=4, device=torch.device("cpu"))


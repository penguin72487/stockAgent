"""Canonical train/eval batching over real physical kernels, synthetic sources."""
from dataclasses import fields, replace
from functools import partial
import os
import time
import weakref
from types import SimpleNamespace

import numpy as np
import pytest
import torch
from torch import nn
from torch.amp import GradScaler

import stockagent.training.trainer as trainer
from stockagent.training.day_trade_carry_bridge import PreparedDayTradeCarrySource
from stockagent.training.loss import risk_aware_loss
from stockagent.training.windowed import WindowedSplitTensors
from stockagent.backtest.tw_day_trade_carry import _compact_detached_carry_state
from test_crypto_trajectory_optimizer import _LOSS_OPTIONS
from test_tw_day_trade_carry import DAY, session, v


class Policy(nn.Module):
    def __init__(self):
        super().__init__()
        self.action = nn.Parameter(torch.tensor([.21, -.21]))

    def forward(self, x, mask, **kwargs):
        return self.action * x[:, -1, :, 0]


def fixture(rows=5, symbols=2, device='cpu', price=1000., price_step=5.):
    total = rows + 1  # prior feature context owns no trade in this split
    features = torch.ones((total, symbols, 1))
    features[1::2] = -1
    mask = torch.ones((total, symbols), dtype=torch.bool)
    sessions = []
    for index in range(total):
        one = session(DAY + index, price=price + index * price_step, exits=index == total - 1)
        sessions.append(replace(one, **{f.name: getattr(one, f.name).expand(
            symbols, *getattr(one, f.name).shape[1:]).contiguous()
            for f in fields(one) if isinstance(getattr(one, f.name), torch.Tensor)}))
    source = PreparedDayTradeCarrySource(tuple(sessions), tuple(str(2300 + s) for s in range(symbols)),
                                        'synthetic-carry-training-fixture-v1')
    split = WindowedSplitTensors(features=features, valid_indices=torch.arange(1, total),
        future_log_returns=torch.zeros((total, symbols)), tradable_mask=mask,
        can_buy_mask=mask.clone(), can_sell_mask=mask.clone(), can_short_open_mask=mask.clone(),
        benchmark=torch.zeros(total), lookback=1, execution_mode='tw_day_trade',
        day_trade_eligible_mask=mask.clone(), day_trade_can_buy_open_mask=mask.clone(),
        day_trade_can_sell_open_mask=mask.clone())
    rates = lambda n: torch.full((symbols,), n, dtype=torch.float64, device=device)
    runtime = trainer._ExecutionRuntime(mode='tw_day_trade', buy_fee_rates=rates(.001425),
        sell_fee_rates=rates(.002925), lot_sizes=None, settlement_lag_sessions=2,
        normal_sell_fee_rates=rates(.004425), commission_rebate_rates=rates(.00114),
        day_trade_unlimited_margin_conversion=True, short_capacity_limit_enabled=False,
        day_trade_carry_source=source)
    loss_fn = partial(risk_aware_loss, execution_mode='tw_day_trade',
        day_trade_unlimited_margin_conversion=True, normal_sell_fee_rates=runtime.normal_sell_fee_rates,
        buy_fee_rates=runtime.buy_fee_rates, sell_fee_rates=runtime.sell_fee_rates,
        commission_rebate_rates=runtime.commission_rebate_rates,
        day_trade_execution_initial_capital=10_000_000., portfolio_activation='pre_normalized')
    return split, runtime, loss_fn


def train_epoch(split, runtime, loss_fn, model, *, device='cpu', ddp=False, lr=0., batch_size=2,
                amp_dtype=None, optimizer=None, use_panel_slab=False,
                optimizer_step_per_trajectory=False):
    if optimizer is None:
        optimizer = torch.optim.SGD(model.parameters(), lr=lr)
    options = dict(_LOSS_OPTIONS, batch_size=batch_size, device=torch.device(device),
        amp_dtype=amp_dtype, non_blocking=False, grad_clip_norm=10., execution_runtime=runtime,
        max_volume_participation=.5, volume_participation_equity=10_000_000.,
        optimizer_step_per_trajectory=optimizer_step_per_trajectory)
    if ddp:
        return trainer._train_epoch_windowed_tensor_ddp(model, loss_fn, split, optimizer,
            GradScaler(device, enabled=False), replicated_ledger_local_metadata=True,
            use_panel_slab=use_panel_slab, **options)
    return trainer._train_epoch_windowed_tensor(model, None, loss_fn, split, optimizer,
        GradScaler(device, enabled=False), **options)


def reference(split, runtime, loss_fn, model):
    batch = split.batch_by_rows(0, len(split), torch.device('cpu'), False)
    aux = {}
    value = loss_fn(model(batch['x'], batch['tradable_mask']), batch['future_log_returns'],
        batch['tradable_mask'], benchmark_returns=batch['benchmark'],
        day_trade_carry_sessions=runtime.day_trade_carry_source.sessions[1:],
        day_trade_eligible_mask=batch['day_trade_eligible_mask'],
        day_trade_can_buy_open_mask=batch['day_trade_can_buy_open_mask'],
        day_trade_can_sell_open_mask=batch['day_trade_can_sell_open_mask'],
        can_short_open_mask=batch['can_short_open_mask'], aux_outputs=aux, **_LOSS_OPTIONS)
    return value, aux['_final_day_trade_carry_state']


@pytest.mark.parametrize('padding', [False, True])
def test_train_carries_fifo_between_batches_and_padding_does_not_trade(padding):
    split, runtime, loss_fn = fixture()
    model = Policy()
    _, expected = reference(split, runtime, loss_fn, model)
    observed = []
    def record(*args, **kwargs):
        value = loss_fn(*args, **kwargs)
        observed.append(kwargs['aux_outputs']['_final_day_trade_carry_state'])
        return value
    if padding:
        split = trainer._pad_windowed_training_split(split, 2)
    loss, timing = train_epoch(split, runtime, record, model)
    assert torch.isfinite(loss)
    assert timing.optimizer_steps == timing.batches == 3
    assert [state.last_session_day for state in observed] == [DAY + 2, DAY + 4, DAY + 5]
    torch.testing.assert_close(observed[-1].last_nav, expected.last_nav, rtol=0, atol=1e-8)
    observed_compact = _compact_detached_carry_state(observed[-1].detached())
    expected_compact = _compact_detached_carry_state(expected.detached())
    for field in fields(expected_compact.inventory):
        torch.testing.assert_close(
            getattr(observed_compact.inventory, field.name),
            getattr(expected_compact.inventory, field.name),
            rtol=0,
            atol=1e-8,
        )
    assert torch.isfinite(model.action.grad).all()
    assert model.action.grad.abs().sum() > 0


def test_train_restarts_account_at_epoch_boundary_but_updates_model():
    split, runtime, loss_fn = fixture()
    model = Policy()
    before = model.action.detach().clone()
    for _ in range(2):
        loss, timing = train_epoch(split, runtime, loss_fn, model, lr=1e-4)
        assert torch.isfinite(loss) and timing.optimizer_steps == 3
    assert not torch.equal(before, model.action.detach())


class _CountingSGD(torch.optim.SGD):
    def __init__(self, params):
        super().__init__(params, lr=1.0e-4)
        self.step_calls = 0

    def step(self, closure=None):
        self.step_calls += 1
        return super().step(closure)


def test_physical_day_trade_trajectory_cadence_matches_one_fixed_policy_loss():
    split, runtime, loss_fn = fixture()
    model = Policy()
    expected_loss, _ = reference(split, runtime, loss_fn, model)
    optimizer = _CountingSGD(model.parameters())

    actual_loss, timing = train_epoch(
        split,
        runtime,
        loss_fn,
        model,
        batch_size=2,
        optimizer=optimizer,
        optimizer_step_per_trajectory=True,
    )

    torch.testing.assert_close(
        actual_loss.double(), expected_loss.double(), rtol=0, atol=1.0e-6
    )
    assert optimizer.step_calls == 1
    assert timing.optimizer_steps == 1
    assert timing.batches == 3
    assert timing.gradient_norm_observations == 1


def test_physical_training_prefetches_each_chronological_batch(monkeypatch):
    split, runtime, loss_fn = fixture()
    calls = []
    original = trainer.prefetch_physical_carry_batches

    def observed_prefetch(source, current_split, batch_order, batch_size):
        calls.append((list(batch_order), int(batch_size)))
        yield from original(source, current_split, batch_order, batch_size)

    monkeypatch.setattr(
        trainer, "prefetch_physical_carry_batches", observed_prefetch
    )
    loss, timing = train_epoch(
        split,
        runtime,
        loss_fn,
        Policy(),
        batch_size=2,
        optimizer_step_per_trajectory=True,
    )

    assert torch.isfinite(loss)
    assert timing.batches == 3
    assert calls == [([0, 1, 2], 2)]


def test_nonfinite_trajectory_gradient_fails_before_only_optimizer_step():
    split, runtime, loss_fn = fixture()
    model = Policy()
    model.action.register_hook(
        lambda grad: torch.full_like(grad, float("nan"))
    )
    optimizer = _CountingSGD(model.parameters())

    with pytest.raises(
        FloatingPointError,
        match="non-finite gradient after full-trajectory accumulation",
    ):
        train_epoch(
            split,
            runtime,
            loss_fn,
            model,
            batch_size=2,
            optimizer=optimizer,
            optimizer_step_per_trajectory=True,
        )

    assert optimizer.step_calls == 0


def test_exact_whole_share_flat_policy_is_a_zero_gradient_dead_zone():
    split, runtime, loss_fn = fixture()
    model = Policy()
    with torch.no_grad():
        model.action.zero_()

    loss, _ = reference(split, runtime, loss_fn, model)
    loss.backward()

    torch.testing.assert_close(loss, torch.zeros_like(loss), rtol=0, atol=0)
    assert model.action.grad is not None
    assert torch.count_nonzero(model.action.grad).item() == 0


def test_loss_preflight_uses_the_same_physical_fifo_adapter_as_epoch_one():
    split, runtime, loss_fn = fixture()
    split = trainer._pad_windowed_training_split(split, 2)

    ok, error = trainer._probe_compiled_loss_forward_backward(
        loss_fn,
        split,
        batch_size=2,
        device=torch.device("cpu"),
        amp_dtype=None,
        non_blocking=False,
        loss_kwargs=_LOSS_OPTIONS,
        max_volume_participation=0.5,
        volume_participation_equity=10_000_000.0,
        execution_runtime=runtime,
    )

    assert ok, error


def test_train_routes_avoidance_mask_to_a_physical_zero_target():
    split, runtime, loss_fn = fixture()
    split.unresolved_corporate_action_mask = torch.zeros_like(split.tradable_mask)
    split.unresolved_corporate_action_mask[1, 0] = True
    observed = []
    def record(weights, *args, **kwargs):
        observed.append(weights.detach().clone())
        return loss_fn(weights, *args, **kwargs)
    loss, _ = train_epoch(split, runtime, record, Policy())
    assert torch.isfinite(loss)
    assert observed[0][0, 0] == 0
    assert observed[0][0, 1] != 0


def test_train_does_not_skip_invalid_physical_batches():
    split, runtime, loss_fn = fixture()
    source = runtime.day_trade_carry_source
    # First session carries. Missing all second-session valuation is an error.
    broken = replace(source.sessions[2], marks=torch.full((2, 270), float('nan'), dtype=torch.float64))
    runtime = replace(runtime, day_trade_carry_source=replace(source,
        sessions=source.sessions[:2] + (broken,) + source.sessions[3:]))
    with pytest.raises((RuntimeError, FloatingPointError), match='valuation|trajectory failed'):
        train_epoch(split, runtime, loss_fn, Policy())


def test_canonical_eval_retains_float64_minute_curves_and_fifo_endpoint():
    split, runtime, loss_fn = fixture()
    model = Policy()
    _, expected = reference(split, runtime, loss_fn, model)
    actual, metrics, _ = trainer._evaluate_windowed_tensor_batch_decoupled(model, None, split,
        device=torch.device('cpu'), amp_dtype=None, non_blocking=False,
        long_only=False, buy_fee_rate=.001425, sell_fee_rate=.002925,
        max_turnover_ratio=0., gross_leverage=1., min_trade_weight=0.,
        model_chunk_rows=2, backtest_chunk_rows=2, portfolio_activation='pre_normalized',
        max_volume_participation=.5, volume_participation_equity=10_000_000., execution_runtime=runtime)
    assert actual.minute_nav.shape == (5, 270)
    assert actual.minute_nav.dtype == actual.shares_history.dtype == torch.float64
    assert actual.settlement_ledger_unit == 'currency'
    torch.testing.assert_close(actual.day_trade_carry_state.last_nav, expected.last_nav, rtol=0, atol=1e-8)
    assert actual.day_trade_carry_state.last_session_day == DAY + 5
    assert metrics


def test_physical_eval_releases_prior_dense_chunk_before_staging_next(monkeypatch):
    split, runtime, _ = fixture(rows=5)
    original_bind = trainer.bind_physical_carry_backtest
    bound_refs = []

    def tracked_bind(*args, **kwargs):
        # The bound closure owns the staged dense physical sessions.  A live
        # previous closure here would double the peak CUDA tape allocation.
        assert not bound_refs or bound_refs[-1]() is None
        bound = original_bind(*args, **kwargs)
        bound_refs.append(weakref.ref(bound))
        return bound

    monkeypatch.setattr(trainer, "bind_physical_carry_backtest", tracked_bind)
    result, _, _ = trainer._evaluate_windowed_tensor_batch_decoupled(
        Policy(), None, split,
        device=torch.device("cpu"), amp_dtype=None, non_blocking=False,
        long_only=False, buy_fee_rate=.001425, sell_fee_rate=.002925,
        max_turnover_ratio=0., gross_leverage=1., min_trade_weight=0.,
        model_chunk_rows=2, backtest_chunk_rows=2,
        portfolio_activation="pre_normalized",
        max_volume_participation=.5, volume_participation_equity=10_000_000.,
        execution_runtime=runtime,
    )
    assert len(bound_refs) == 3
    assert all(ref() is None for ref in bound_refs)
    assert result.day_trade_carry_state.last_session_day == DAY + 5


def test_physical_formal_replay_caps_dense_chunk_size(monkeypatch):
    split, runtime, _ = fixture(rows=5)
    original_ranges = trainer._eval_ranges_by_reset
    observed_chunk_rows = []

    def tracked_ranges(total_rows, chunk_rows, reset_at_rows):
        observed_chunk_rows.append(chunk_rows)
        return original_ranges(total_rows, chunk_rows, reset_at_rows)

    monkeypatch.setattr(trainer, "_eval_ranges_by_reset", tracked_ranges)
    result, _, _ = trainer._evaluate_windowed_tensor_batch_decoupled(
        Policy(), None, split,
        device=torch.device("cpu"), amp_dtype=None, non_blocking=False,
        long_only=False, buy_fee_rate=.001425, sell_fee_rate=.002925,
        max_turnover_ratio=0., gross_leverage=1., min_trade_weight=0.,
        model_chunk_rows=2, backtest_chunk_rows=512,
        return_weights_history=True,
        portfolio_activation="pre_normalized",
        max_volume_participation=.5, volume_participation_equity=10_000_000.,
        execution_runtime=runtime,
    )
    assert observed_chunk_rows == [128]
    assert result.minute_nav.shape == (5, 270)


def test_repeated_epoch_eval_compacts_only_minute_audit_not_account_math(
    monkeypatch,
):
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_EVENT_COMPRESSION", "1")
    split, runtime, _ = fixture()
    model = Policy()
    options = dict(
        device=torch.device("cpu"),
        amp_dtype=None,
        non_blocking=False,
        long_only=False,
        buy_fee_rate=0.001425,
        sell_fee_rate=0.002925,
        max_turnover_ratio=0.0,
        gross_leverage=1.0,
        min_trade_weight=0.0,
        model_chunk_rows=2,
        backtest_chunk_rows=2,
        portfolio_activation="pre_normalized",
        max_volume_participation=0.5,
        volume_participation_equity=10_000_000.0,
        execution_runtime=runtime,
    )
    formal, formal_metrics, _ = (
        trainer._evaluate_windowed_tensor_batch_decoupled(
            model, None, split, return_weights_history=True, **options
        )
    )
    repeated, repeated_metrics, _ = (
        trainer._evaluate_windowed_tensor_batch_decoupled(
            model, None, split, return_weights_history=False, **options
        )
    )

    assert formal.minute_nav.shape == (len(split), 270)
    assert repeated.minute_nav.shape == (len(split), 2)
    torch.testing.assert_close(
        repeated.minute_nav[:, 0], formal.minute_nav.amin(dim=-1), rtol=0, atol=1e-8
    )
    torch.testing.assert_close(
        repeated.minute_nav[:, 1], formal.minute_nav[:, -1], rtol=0, atol=1e-8
    )
    torch.testing.assert_close(
        repeated.strategy_returns, formal.strategy_returns, rtol=0, atol=1e-12
    )
    torch.testing.assert_close(
        repeated.turnovers, formal.turnovers, rtol=0, atol=1e-10
    )
    torch.testing.assert_close(
        repeated.shares_history, formal.shares_history, rtol=0, atol=1e-8
    )
    torch.testing.assert_close(
        repeated.day_trade_carry_state.last_nav,
        formal.day_trade_carry_state.last_nav,
        rtol=0,
        atol=1e-8,
    )
    assert repeated_metrics == formal_metrics


def test_physical_eval_accepts_boundary_markers_and_rejects_internal_account_reset():
    split, runtime, _ = fixture()
    model = Policy()
    options = dict(
        device=torch.device("cpu"),
        amp_dtype=None,
        non_blocking=False,
        long_only=False,
        buy_fee_rate=0.001425,
        sell_fee_rate=0.002925,
        max_turnover_ratio=0.0,
        gross_leverage=1.0,
        min_trade_weight=0.0,
        model_chunk_rows=2,
        backtest_chunk_rows=2,
        portfolio_activation="pre_normalized",
        max_volume_participation=0.5,
        volume_participation_equity=10_000_000.0,
        execution_runtime=runtime,
    )
    baseline, _, _ = trainer._evaluate_windowed_tensor_batch_decoupled(
        model, None, split, reset_at_rows=None, **options
    )
    bounded, _, _ = trainer._evaluate_windowed_tensor_batch_decoupled(
        model, None, split, reset_at_rows=[0, len(split)], **options
    )
    torch.testing.assert_close(
        bounded.minute_nav, baseline.minute_nav, rtol=0, atol=1e-8
    )
    torch.testing.assert_close(
        bounded.day_trade_carry_state.last_nav,
        baseline.day_trade_carry_state.last_nav,
        rtol=0,
        atol=1e-8,
    )
    with pytest.raises(ValueError, match="without implicit account resets"):
        trainer._evaluate_windowed_tensor_batch_decoupled(
            model, None, split, reset_at_rows=[0, 2, len(split)], **options
        )


def test_identity_batch_reuses_immutable_source_tensors_without_copy():
    split, runtime, _ = fixture()
    source = runtime.day_trade_carry_source
    sessions, count = source.batch(split, 0, 2, torch.device('cpu'))
    assert count == 2
    for actual, original in zip(sessions, source.sessions[1:3]):
        for field in fields(actual):
            tensor = getattr(actual, field.name)
            if isinstance(tensor, torch.Tensor):
                assert tensor.data_ptr() == getattr(original, field.name).data_ptr()


def test_training_does_not_mutate_shared_prepared_source():
    split, runtime, loss_fn = fixture()
    source = runtime.day_trade_carry_source
    before = [{field.name: getattr(session, field.name).clone() for field in fields(session)
               if isinstance(getattr(session, field.name), torch.Tensor)} for session in source.sessions]
    train_epoch(split, runtime, loss_fn, Policy(), lr=1e-4)
    for session, saved in zip(source.sessions, before):
        for key, value in saved.items():
            torch.testing.assert_close(getattr(session, key), value, rtol=0, atol=0, equal_nan=True)


def test_compacted_batch_preserves_original_symbol_mapping():
    split, runtime, _ = fixture(symbols=3)
    source = runtime.day_trade_carry_source
    source.sessions[1].official_open.copy_(v(1000, 1010, 1020))
    subset = split.subset_symbols(torch.tensor([2, 0]))
    sessions, count = source.batch(subset, 0, 2, torch.device('cpu'))
    assert count == 2
    torch.testing.assert_close(sessions[0].official_open, v(1020, 1000))
    assert source.sessions[1].official_open.tolist() == [1000, 1010, 1020]


@pytest.mark.parametrize('rows', [[1, 3, 4, 5], [1, 2, 4, 5]])
def test_missing_session_inside_or_between_batches_fails_closed(rows):
    split, runtime, loss_fn = fixture()
    split.valid_indices = torch.tensor(rows)
    split._valid_indices_cpu = split.valid_indices
    with pytest.raises(ValueError, match='skips|skipped'):
        train_epoch(split, runtime, loss_fn, Policy())


def test_nonfinite_action_never_reaches_generic_skip_batch_path():
    split, runtime, loss_fn = fixture()
    model = Policy()
    with torch.no_grad():
        model.action[0] = float('nan')
    with pytest.raises(FloatingPointError, match='model actions are nonfinite'):
        train_epoch(split, runtime, loss_fn, model)


def test_nonfinite_parameter_gradient_never_skips_owned_days():
    split, runtime, loss_fn = fixture()
    model = Policy()
    model.action.register_hook(lambda grad: torch.full_like(grad, float('nan')))
    with pytest.raises(FloatingPointError, match='non-finite physical training gradient'):
        train_epoch(split, runtime, loss_fn, model)


def test_eval_segment_handoff_preserves_account_not_rebased_capital():
    split, runtime, loss_fn = fixture()
    model = Policy()
    _, expected = reference(split, runtime, loss_fn, model)
    options = dict(device=torch.device('cpu'), amp_dtype=None, non_blocking=False,
        long_only=False, buy_fee_rate=.001425, sell_fee_rate=.002925,
        max_turnover_ratio=0., gross_leverage=1., min_trade_weight=0.,
        model_chunk_rows=2, backtest_chunk_rows=2, portfolio_activation='pre_normalized',
        max_volume_participation=.5, volume_participation_equity=10_000_000., execution_runtime=runtime)
    first = replace(split, valid_indices=split.valid_indices[:2])
    tail = replace(split, valid_indices=split.valid_indices[2:])
    first_result, _, _ = trainer._evaluate_windowed_tensor_batch_decoupled(model, None, first, **options)
    tail_result, _, _ = trainer._evaluate_windowed_tensor_batch_decoupled(model, None, tail,
        initial_day_trade_carry_state=first_result.day_trade_carry_state.detached(), **options)
    torch.testing.assert_close(tail_result.day_trade_carry_state.last_nav, expected.last_nav, rtol=0, atol=1e-8)
    assert tail_result.day_trade_carry_state.initial_capital == 10_000_000.
    assert first_result.day_trade_carry_state.inventory.shares.abs().sum() > 0
    # Reusing the same account with the same dates must fail, not trade twice.
    with pytest.raises(ValueError, match='skipped or repeated'):
        trainer._evaluate_windowed_tensor_batch_decoupled(model, None, first,
            initial_day_trade_carry_state=first_result.day_trade_carry_state.detached(), **options)


def test_artifact_prefix_replay_and_segment_concatenation_match_full_fifo_account():
    split, runtime, _ = fixture()
    model = Policy()
    options = dict(device=torch.device('cpu'), amp_dtype=None, non_blocking=False,
        long_only=False, buy_fee_rate=.001425, sell_fee_rate=.002925,
        max_turnover_ratio=0., gross_leverage=1., min_trade_weight=0.,
        model_chunk_rows=2, backtest_chunk_rows=2, portfolio_activation='pre_normalized',
        max_volume_participation=.5, volume_participation_equity=10_000_000.,
        execution_runtime=runtime)
    full_tensor, _, _ = trainer._evaluate_windowed_tensor_batch_decoupled(
        model, None, split, **options
    )
    full = full_tensor.to_numpy()
    config = SimpleNamespace(trading=SimpleNamespace(
        buy_fee_rate=.001425, sell_fee_rate=.002925, long_only=False,
        max_turnover_ratio=0., min_trade_weight=0.,
        volume_participation_equity=10_000_000., max_volume_participation=.5,
    ))
    first = trainer._replay_physical_carry_split_prefix(
        full, split, 2, runtime=runtime, config=config
    )
    tail_split = replace(split, valid_indices=split.valid_indices[2:])
    tail_split._valid_indices_cpu = tail_split.valid_indices
    tail_requests = trainer._slice_backtest_rows(full, 2, len(split))
    tail = trainer._replay_physical_carry_split_prefix(
        tail_requests, tail_split, len(split) - 2, runtime=runtime,
        config=config, initial_state=first.day_trade_carry_state,
    )
    stitched = trainer._concatenate_physical_carry_segments([first, tail])
    for name in ('strategy_returns', 'turnovers', 'weights_history',
                 'requested_weights_history', 'shares_history', 'minute_nav'):
        np.testing.assert_allclose(getattr(stitched, name), getattr(full, name),
                                   rtol=0, atol=1e-8)
    torch.testing.assert_close(
        stitched.day_trade_carry_state.last_nav,
        full.day_trade_carry_state.last_nav,
        rtol=0, atol=1e-8,
    )


@pytest.mark.skipif(int(os.environ.get('WORLD_SIZE', '1')) != 2, reason='requires real two-rank torchrun')
@pytest.mark.parametrize('optimizer_step_per_trajectory', [False, True])
def test_real_ddp_physical_training_matches_single_device(
    optimizer_step_per_trajectory,
):
    """Real NCCL/autograd/optimizer, not synthetic collective monkeypatches."""
    import torch.distributed as dist
    from torch.nn.parallel import DistributedDataParallel
    rank = int(os.environ['LOCAL_RANK'])
    torch.cuda.set_device(rank)
    dist.init_process_group('nccl')
    device = f'cuda:{rank}'
    try:
        split, runtime, loss_fn = fixture(device=device)
        split = trainer._pad_windowed_training_split(split, 2)
        model = Policy().to(device)
        ddp = DistributedDataParallel(model, device_ids=[rank])
        timings = []
        for _ in range(2):
            torch.cuda.synchronize()
            begin = time.perf_counter()
            loss, timing = train_epoch(
                split, runtime, loss_fn, ddp, device=device, ddp=True, lr=1e-4,
                optimizer_step_per_trajectory=optimizer_step_per_trajectory,
            )
            torch.cuda.synchronize()
            timings.append(time.perf_counter() - begin)
            assert torch.isfinite(loss) and timing.batches == 3
            assert timing.optimizer_steps == (
                1 if optimizer_step_per_trajectory else 3
            )
        # Same optimizer cadence and detached physical state on an independent
        # CPU reference, including the one-real-row padded final global batch.
        reference_split, reference_runtime, reference_fn = fixture()
        reference_model = Policy()
        for _ in range(2):
            train_epoch(
                reference_split, reference_runtime, reference_fn,
                reference_model, lr=1e-4,
                optimizer_step_per_trajectory=optimizer_step_per_trajectory,
            )
        torch.testing.assert_close(model.action.detach().cpu(), reference_model.action.detach(),
                                   rtol=1e-5, atol=1e-7)
        gathered = [torch.empty_like(model.action) for _ in range(2)]
        dist.all_gather(gathered, model.action.detach())
        torch.testing.assert_close(gathered[0], gathered[1], rtol=0, atol=0)
        print(f'physical_ddp rank={rank} trajectory={optimizer_step_per_trajectory} epochs_seconds={timings} '
              f'parameters={model.action.detach().cpu().tolist()} synthetic_source=true', flush=True)
    finally:
        dist.destroy_process_group()


@pytest.mark.skipif(os.environ.get('STOCKAGENT_TEST_ATTENTION_CARRY') != '1',
                    reason='opt-in full architecture GPU integration, synthetic prices/features')
def test_named_attention_bf16_physical_training():
    """Production architecture/epoch loop; NOT a real-data/full-year benchmark.

    No pretrained transfer or production completion marker is written. Synthetic
    RMS/features/prices here are integration fixtures, never accepted sources.
    """
    import json
    import torch.distributed as dist
    from stockagent.config import load_config
    from stockagent.models.factory import build_model
    from stockagent.models.temporal_basis_fit import fit_training_only_pca_klt
    from downloader.artifact_io import atomic_write_json
    from pathlib import Path

    assert torch.cuda.is_available()
    rank = int(os.environ.get('LOCAL_RANK', '0'))
    world = int(os.environ.get('WORLD_SIZE', '1'))
    assert world in (1, 2)
    torch.cuda.set_device(rank)
    device = f'cuda:{rank}'
    if world > 1:
        dist.init_process_group('nccl')
    try:
        torch.manual_seed(417)
        config = load_config('configs/markets/tw_day_trade_1m_hybrid_v12_attention_full_then_last_layernorm.yaml')
        symbols = int(os.environ.get('STOCKAGENT_TEST_CARRY_SYMBOLS', '2330'))
        split, runtime, loss_fn = fixture(rows=config.training.lookback + 4, symbols=symbols,
                                         device=device, price=5., price_step=.01)
        split.lookback = config.training.lookback
        split.features = torch.randn((len(split.features), symbols, len(config.data.feature_include)))
        split.valid_indices = torch.arange(split.lookback, len(split.features))
        split._valid_indices_cpu = split.valid_indices
        basis_config = trainer._temporal_basis_runtime_config(config)
        pca = fit_training_only_pca_klt(split.features, split.valid_indices,
            lookback=split.lookback, feature_lag=1,
            components=basis_config.temporal_basis_components_by_family.get(
                'pca_klt', basis_config.temporal_basis_components))
        split = trainer._pad_windowed_training_split(split, 2)
        model = build_model(config=config, lookback=split.lookback,
                            num_features=len(config.data.feature_include), num_symbols=symbols,
                            feature_names=config.data.feature_include,
                            temporal_basis_overrides={'pca_klt': pca.basis}).to(device)
        model.set_causal_feature_rms_normalizer(torch.ones(len(config.data.feature_include), device=device),
                                                torch.ones(len(config.data.feature_include), device=device, dtype=torch.bool))
        optimizer = trainer._create_adamw_optimizer(model, config, torch.device(device), label='integration-only')
        initial = [p.detach().clone() for p in model.parameters()]
        compile_model = os.environ.get('STOCKAGENT_TEST_CARRY_COMPILE') == '1'
        use_panel_slab = world > 1
        forward_base = trainer._PanelSlabForwardWrapper(model) if use_panel_slab else model
        if compile_model:
            forward_base = torch.compile(forward_base, fullgraph=True, dynamic=False,
                options=trainer._torch_compile_options(config.training.torch_compile_mode, cudagraphs=False))
        forward = (trainer._wrap_distributed_data_parallel_model(
            forward_base, config=config, device=torch.device(device)) if world > 1 else forward_base)
        timings = []
        gradient_diagnostics = []
        torch.cuda.reset_peak_memory_stats(rank)
        for _ in range(2):
            torch.cuda.synchronize(rank)
            begin = time.perf_counter()
            loss, timing = train_epoch(split, runtime, loss_fn, forward, device=device,
                ddp=world > 1, amp_dtype=torch.bfloat16, optimizer=optimizer, use_panel_slab=use_panel_slab)
            torch.cuda.synchronize(rank)
            timings.append(time.perf_counter() - begin)
            assert torch.isfinite(loss) and timing.optimizer_steps == 3
            diagnostic = {'norm_sum': timing.gradient_norm_before_clip_sum,
                          'zero_batches': timing.gradient_norm_zero_batches,
                          'observations': timing.gradient_norm_observations}
            gradient_diagnostics.append(diagnostic)
            print(f'attention_gradients rank={rank} epoch={len(timings)} {diagnostic}', flush=True)
            # A legitimate empty final target may have zero gradient. Verify
            # the entire epoch instead of relying on only its last padded step.
            assert diagnostic['norm_sum'] > 0 and diagnostic['zero_batches'] < diagnostic['observations']
        assert any(not torch.equal(before, after) for before, after in zip(initial, model.parameters()))
        for parameter in model.parameters():
            assert torch.isfinite(parameter).all()
        if world > 1:
            flat = torch.cat([p.detach().reshape(-1) for p in model.parameters()])
            received = [torch.empty_like(flat) for _ in range(world)]
            dist.all_gather(received, flat)
            torch.testing.assert_close(received[0], received[1], rtol=0, atol=0)
        result = {'kind': 'named_attention_physical_integration_synthetic_source', 'status': 'passed',
                  'rank': rank, 'world_size': world, 'symbols': symbols,
                  'features': len(config.data.feature_include), 'lookback': split.lookback,
                  'actual_days': 5, 'global_batch': 2, 'epochs_seconds': timings,
                  'gradient_diagnostics': gradient_diagnostics,
                  'amp_dtype': 'bf16', 'model_compile': compile_model, 'optimizer': 'canonical_adamw',
                  'panel_slab': use_panel_slab,
                  'peak_allocated_bytes': torch.cuda.max_memory_allocated(rank),
                  'full_epoch_workflow_measured': False, 'formal_training_ready': False,
                  'pretrained_transfer_validated': False, 'source_receipts_validated': False}
        variant = 'compiled' if compile_model else 'eager'
        atomic_write_json(Path(f'artifacts/operations/daytrade_training_20260910/named_model_{variant}_rank{rank}.json'), result)
        print(json.dumps(result), flush=True)
    finally:
        if world > 1:
            dist.destroy_process_group()

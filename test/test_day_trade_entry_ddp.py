"""Run explicitly with torchrun --nproc_per_node=2 -m pytest on vastai1T.

This is a two-GPU autograd/collective test, NOT a full-fold readiness claim.
"""
import os
from dataclasses import fields, replace
from datetime import timedelta

import pytest
import torch
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel

from stockagent.backtest.tw_day_trade_carry import run_day_trade_carry_sessions
from test_day_trade_entry_continuation import sweep_session


@pytest.fixture(scope='module')
def ddp_device():
    rank = int(os.environ["LOCAL_RANK"])
    torch.cuda.set_device(rank)
    dist.init_process_group("nccl", timeout=timedelta(seconds=120))
    try:
        yield rank, torch.device("cuda", rank)
    finally:
        dist.destroy_process_group()


@pytest.mark.skipif(int(os.environ.get("WORLD_SIZE", "1")) != 2,
                    reason="requires an explicit two-GPU torchrun")
def test_captured_real_fifo_session_repeated_two_gpu_parity(monkeypatch, ddp_device):
    """Opt-in replay of an immutable failure capsule, without source downloads."""
    from pathlib import Path
    import stockagent.backtest.tw_day_trade_carry as carry
    from stockagent.backtest.tw_day_trade_inventory import DayTradeInventoryState
    path = os.environ.get('STOCKAGENT_FIFO_FAILURE_CAPSULE')
    if not path:
        pytest.skip('requires an explicitly selected real failure capsule')
    _, device = ddp_device
    payload = torch.load(Path(path), weights_only=True, map_location='cpu')
    move = lambda value: value.to(device) if isinstance(value, torch.Tensor) else value
    state = carry.DayTradeCarryState(
        inventory=DayTradeInventoryState(**{k: move(v) for k, v in payload['inventory'].items()}),
        **{k: move(v) for k, v in payload['state'].items()})
    session = carry.DayTradeCarrySession(**{k: move(v) for k, v in payload['session'].items()})
    args = {k: move(v) for k, v in payload['arguments'].items()}
    with monkeypatch.context() as eager, torch.no_grad():
        eager.setenv('STOCKAGENT_BACKTEST_COMPILE', '0')
        reference, nav, notional = carry.execute_carry_session(state, session, event_compression=False, **args)
    for _ in range(3):
        with torch.no_grad():
            actual, actual_nav, actual_notional = carry.execute_carry_session(
                state, session, event_compression=False, **args)
        assert actual.inventory.failed.item() == 0
        torch.testing.assert_close(actual_nav, nav, rtol=1e-12, atol=1e-7)
        torch.testing.assert_close(actual_notional, notional, rtol=1e-12, atol=1e-7)
        for field in fields(reference.inventory):
            torch.testing.assert_close(getattr(actual.inventory, field.name),
                                       getattr(reference.inventory, field.name), rtol=1e-12, atol=1e-7)


@pytest.mark.skipif(int(os.environ.get("WORLD_SIZE", "1")) != 2,
                    reason="requires an explicit two-GPU torchrun")
def test_compiled_fifo_search_tail_repeatability_and_ddp_gradient(ddp_device):
    """Non-aligned 2754 x 270 tail, held last symbols and 64 FIFO cohorts."""
    import stockagent.backtest.tw_day_trade_carry as carry
    from stockagent.backtest.tw_day_trade_inventory import (
        DayTradeInventoryState, append_inventory_fill,
        reduce_inventory_fifo_path, inventory_path_nav,
    )
    from test_day_trade_entry_continuation import DAY
    rank, device = ddp_device
    policy = torch.nn.Linear(1, 1, bias=False, dtype=torch.float64, device=device)
    with torch.no_grad():
        policy.weight.fill_(.05)
    ddp = DistributedDataParallel(policy, device_ids=[rank])
    count = 2754
    vector = lambda value: torch.full((count,), value, dtype=torch.float64, device=device)
    profile = vector(0.)
    profile[17], profile[-7], profile[-1] = 1., -1., 2.
    x = torch.tensor([[1. + rank * .1]], dtype=torch.float64, device=device)
    prices = vector(103.)[:, None].expand(-1, 270).contiguous()
    capacity = torch.zeros_like(prices)
    capacity[:, [7, 129, 269]] = 1000.
    marks = prices + torch.arange(270, dtype=torch.float64, device=device)[None, :] * .001

    def path(weight, compiled):
        initial = DayTradeInventoryState.empty(count, device=device)
        initial = replace(initial, cohorts=vector(0.).new_zeros((63, count, 12)))
        inventory = append_inventory_fill(initial, signed_shares=weight.reshape(()) * profile * 100_000,
            price=vector(100.), buy_fee_rate=vector(.001425), day_sell_fee_rate=vector(.002925),
            normal_sell_fee_rate=vector(.004425), rebate_rate=vector(.00114), day=DAY)
        if compiled:
            return carry._run_inventory_path(inventory, prices=prices, capacity_shares=capacity,
                                              marks=marks, initial_capital=10_000_000.)
        reduced = reduce_inventory_fifo_path(inventory, prices=prices, capacity_shares=capacity)
        nav = inventory_path_nav(inventory, prices=prices,
            minute_filled_shares=reduced.minute_filled_shares, marks=marks, initial_capital=10_000_000.)
        return reduced, nav

    weight = ddp(x)
    actual, actual_nav = path(weight, True)
    (-actual_nav.mean()).backward()
    actual_gradient = policy.weight.grad.detach().clone()
    reference_weight = policy.weight.detach().clone().requires_grad_()
    expected, expected_nav = path(x @ reference_weight.T, False)
    (-expected_nav.mean()).backward()
    expected_gradient = reference_weight.grad.clone()
    dist.all_reduce(expected_gradient)
    expected_gradient /= dist.get_world_size()
    failure = None
    try:
        torch.testing.assert_close(actual_nav, expected_nav, rtol=1e-12, atol=1e-7)
        torch.testing.assert_close(actual.reduction.state.shares, expected.reduction.state.shares, rtol=0, atol=1e-8)
        torch.testing.assert_close(actual_gradient, expected_gradient, rtol=1e-10, atol=1e-8)
        for _ in range(3):
            with torch.no_grad():
                repeated, nav = path(weight.detach(), True)
            torch.testing.assert_close(nav, expected_nav, rtol=1e-12, atol=1e-7)
            torch.testing.assert_close(repeated.reduction.state.shares, expected.reduction.state.shares, rtol=0, atol=1e-8)
    except AssertionError as exc:
        failure = f"rank={rank}: {exc}"
    failures = [None] * dist.get_world_size()
    dist.all_gather_object(failures, failure)
    assert not any(failures), str(failures)


@pytest.mark.skipif(int(os.environ.get("WORLD_SIZE", "1")) != 2,
                    reason="requires an explicit two-GPU torchrun")
@pytest.mark.parametrize('volume', [2000., 20000.])
def test_frozen_target_fifo_two_gpu_ddp_gradients(monkeypatch, volume, ddp_device):
    rank, device = ddp_device
    with torch.cuda.device(device):
        print(f"rank={rank} phase=initialized", flush=True)
        policy = torch.nn.Linear(1, 1, bias=False, dtype=torch.float64, device=device)
        with torch.no_grad():
            policy.weight.fill_(.051)
        ddp = DistributedDataParallel(policy, device_ids=[rank])
        print(f"rank={rank} phase=ddp_ready", flush=True)
        session = sweep_session(volume=volume, exit_volume=0.)
        session = replace(session, **{f.name: getattr(session, f.name).to(device)
                                      for f in fields(session)
                                      if isinstance(getattr(session, f.name), torch.Tensor)})
        x = torch.tensor([[1. + rank * .1]], dtype=torch.float64, device=device)
        kwargs = dict(can_enter=torch.ones_like(x), buy_fee_rate=x.new_tensor([.001425]),
                      day_sell_fee_rate=x.new_tensor([.002925]),
                      normal_sell_fee_rate=x.new_tensor([.004425]), rebate_rate=x.new_tensor([.00114]),
                      initial_capital=10_000_000., event_compression=True)
        weights = ddp(x)
        result = run_day_trade_carry_sessions(weights, (session,), **kwargs)
        print(f"rank={rank} phase=forward_done", flush=True)
        loss = -result.strategy_returns.mean()
        loss.backward()
        print(f"rank={rank} phase=backward_done", flush=True)
        assert torch.isfinite(loss) and torch.isfinite(policy.weight.grad).all()
        actual = policy.weight.grad.detach().clone()
        reference_weight = policy.weight.detach().clone().requires_grad_()
        with monkeypatch.context() as eager:
            eager.setenv("STOCKAGENT_DAY_TRADE_SWEEP_COMPILE", "0")
            eager.setenv("STOCKAGENT_DAY_TRADE_SWEEP_CHECKPOINT", "0")
            eager.setenv("STOCKAGENT_DAY_TRADE_SWEEP_SUFFIX_FASTPATH", "0")
            reference = run_day_trade_carry_sessions(x @ reference_weight.T, (session,), **kwargs)
            (-reference.strategy_returns.mean()).backward()
        expected = reference_weight.grad.clone()
        dist.all_reduce(expected)
        expected /= dist.get_world_size()
        print(f"rank={rank} actual={actual.item():.17g} expected={expected.item():.17g} "
              f"cohorts={result.final_state.inventory.cohorts.shape[0]}", flush=True)
        # Every rank must reach the same collective even if one comparison
        # fails, otherwise the diagnostic hides its assertion behind NCCL's
        # shutdown timeout on the peer.
        failure = None
        try:
            torch.testing.assert_close(actual, expected, rtol=1e-10, atol=1e-12)
            assert result.final_state.inventory.cohorts.shape[0] == 1
        except AssertionError as exc:
            failure = f"rank={rank}: {exc}"
        failures = [None] * dist.get_world_size()
        dist.all_gather_object(failures, failure)
        assert not any(failures), str(failures)


@pytest.mark.skipif(int(os.environ.get("WORLD_SIZE", "1")) != 2,
                    reason="requires an explicit two-GPU torchrun")
@pytest.mark.parametrize('nested_checkpoint', [False, True])
def test_frozen_target_full_universe_step_ddp_gradients(ddp_device, nested_checkpoint):
    """Full [32,2754,12] FIFO ABI, not a one-symbol compile readiness proxy."""
    import time
    import stockagent.backtest.tw_day_trade_carry as carry
    from stockagent.backtest.tw_day_trade_inventory import (
        DayTradeInventoryState, rebalance_inventory_at_open,
    )
    from test_day_trade_entry_continuation import DAY

    rank, device = ddp_device
    symbols = 2754
    policy = torch.nn.Linear(1, 1, bias=False, dtype=torch.float64, device=device)
    with torch.no_grad():
        policy.weight.fill_(.051)
    ddp = DistributedDataParallel(policy, device_ids=[rank])
    x = torch.tensor([[1. + rank * .1]], dtype=torch.float64, device=device)
    profile = torch.zeros(symbols, dtype=torch.float64, device=device)
    profile[17] = 1.
    vector = lambda value: torch.full((symbols,), value, dtype=torch.float64, device=device)
    state = DayTradeInventoryState.empty(symbols, device=device)
    claims = vector(0.).new_zeros((31, symbols, 3))
    claims[0, 17] = claims.new_tensor([102.5, DAY + 5., 0.])
    claims[1, 17] = claims.new_tensor([-27.5, DAY + 5., 0.])
    state = replace(state, cohorts=vector(0.).new_zeros((31, symbols, 12)),
                    claims=claims.requires_grad_())
    common = dict(official_open=vector(100.), opening_marks=vector(100.),
        lower_limit=vector(90.), upper_limit=vector(110.), can_enter=vector(1.),
        buy_fee_rate=vector(.001425), day_sell_fee_rate=vector(.002925),
        normal_sell_fee_rate=vector(.004425), rebate_rate=vector(.00114),
        initial_capital=10_000_000., day=DAY, halted=vector(0.), daily_proxy_mask=vector(0.))
    # Use the same strided minute views as a dense source, not a contiguous
    # hand-picked tiny slice. Only one minute is differentiated in this test.
    base = vector(0.).new_zeros((symbols, 270, 2))
    base[..., 0] = 101.
    base[..., 1] = 20000.
    fields = (torch.full_like(base, float('nan'))[:, 1],
              torch.zeros_like(base)[:, 1], base[:, 1],
              torch.zeros_like(base)[:, 1], vector(101.))
    def forward(weights, compiled):
        opening = rebalance_inventory_at_open(state, weights=weights,
            entry_price=vector(100.), entry_volume_shares=vector(0.), **common)
        inventory = opening.state
        fn = carry._sweep_step_function(weights, inventory,
            initial_capital=10_000_000., entry_phase=True) if compiled else (
                lambda a, b, c, d: carry._flat_sweep_step(a, b, c, d,
                    initial_capital=10_000_000., entry_phase=True))
        args = (tuple(getattr(inventory, name) for name in carry._INVENTORY_FIELDS),
                (weights, opening.target_shares, opening.sizing_nav,
                 vector(0.).bool(), inventory.shares == opening.target_shares,
                 weights.new_tensor(True, dtype=torch.bool)), fields,
                common | dict(day=inventory.observed_day, state_already_advanced=True))
        return (carry.checkpoint(fn, *args, use_reentrant=False, preserve_rng_state=False)
                if compiled and nested_checkpoint else fn(*args))
    started = time.perf_counter()
    weights = ddp(x).reshape(()) * profile
    actual = (carry.checkpoint(forward, weights, True, use_reentrant=False,
                               preserve_rng_state=False)
              if nested_checkpoint else forward(weights, True))
    (-actual[-2] / 10_000_000.).backward()
    grad = policy.weight.grad.detach().clone()
    reference_weight = policy.weight.detach().clone().requires_grad_()
    expected = forward((x @ reference_weight.T).reshape(()) * profile, False)
    (-expected[-2] / 10_000_000.).backward()
    expected_grad = reference_weight.grad.clone()
    dist.all_reduce(expected_grad)
    expected_grad /= dist.get_world_size()
    failures = [None] * dist.get_world_size()
    failure = None
    try:
        for left, right in zip(actual, expected, strict=True):
            torch.testing.assert_close(left, right, rtol=1e-10, atol=1e-8)
        torch.testing.assert_close(grad, expected_grad, rtol=1e-10, atol=1e-12)
    except AssertionError as exc:
        failure = f'rank={rank}: {exc}'
    print(f'rank={rank} full_universe_step_s={time.perf_counter()-started:.3f}', flush=True)
    dist.all_gather_object(failures, failure)
    assert not any(failures), str(failures)

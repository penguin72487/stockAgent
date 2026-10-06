"""Exact block rematerialization: no new detached financial boundary."""
from dataclasses import fields, replace
from datetime import timedelta
import itertools
import os

import pytest
import torch

from stockagent.backtest import tw_day_trade_carry as carry
from test_tw_day_trade_carry import DAY, run, session, v


FLAG = "STOCKAGENT_DAY_TRADE_CARRY_CHECKPOINT_BLOCK_ROWS"
FLAT_FLAG = "STOCKAGENT_DAY_TRADE_CARRY_FLAT_TERMINAL"


def _assert_financially_close(actual, expected):
    """User-approved summation tolerance, but no relaxed physical/calendar ABI."""
    for name in carry._CARRY_RESULT_FIELDS:
        discrete = name in {"shares_history", "settlement_default"}
        torch.testing.assert_close(getattr(actual, name), getattr(expected, name),
            rtol=0 if discrete else 1e-12, atol=0 if discrete else 1e-8,
            equal_nan=True)
    for f in fields(expected.final_state.inventory):
        # Cohorts retain all prices, ownership and calendar metadata; a compact
        # financial sum does not authorize changing any of those fields.
        discrete = f.name in {"cohorts", "claims", "failed", "observed_day"}
        torch.testing.assert_close(getattr(actual.final_state.inventory, f.name),
            getattr(expected.final_state.inventory, f.name),
            rtol=0 if discrete else 1e-12, atol=0 if discrete else 1e-8)
    torch.testing.assert_close(actual.final_state.last_nav, expected.final_state.last_nav,
                               rtol=1e-12, atol=1e-8)
    assert torch.equal(actual.final_state.alive, expected.final_state.alive)
    assert actual.final_state.last_session_day == expected.final_state.last_session_day


@pytest.fixture(autouse=True)
def bounded_cpu_threads():
    previous = torch.get_num_threads()
    torch.set_num_threads(1)
    try:
        yield
    finally:
        torch.set_num_threads(previous)


def _cases(kind):
    sessions = [session(DAY+i, 1000+10*i, exits=i % 3 == 2) for i in range(7)]
    if kind == "claim":
        sessions[2] = replace(sessions[2], action_mask=v(1), share_ratio=v(.5),
            cash_per_old_share=v(1), payment_day=v(DAY+5))
    elif kind == "locked":
        sessions[2] = replace(sessions[2], action_mask=v(1), share_ratio=v(1.1),
            cash_per_old_share=v(0), payment_day=v(0), stock_delivery_day=v(DAY+5))
    elif kind == "terminal":
        sessions = [replace(s, terminal_liquidation_price=v(1000+10*i))
                    for i,s in enumerate(sessions)]
    elif kind == "default":
        sessions[0] = session(DAY, volume=20_000)
        sessions[1] = session(DAY+1, 3000, volume=0)
    return sessions


def _evaluate(monkeypatch, rows, kind, compact):
    monkeypatch.setenv(FLAG, str(rows))
    weights = v(-.9,.2,-.1,.3,0,-.2,.1).reshape(-1,1).requires_grad_()
    saved_bytes = 0
    def pack(value):
        nonlocal saved_bytes
        saved_bytes += value.numel() * value.element_size()
        return value
    with torch.autograd.graph.saved_tensors_hooks(pack, lambda value: value):
        result = run(weights, _cases(kind), event_compression=compact)
        objective = (result.strategy_returns.sum() + result.minute_nav.sum()*1e-11
            + result.final_state.last_nav*1e-7)
        grad, = torch.autograd.grad(objective, weights)
    return result, grad, saved_bytes


@pytest.mark.parametrize("kind", ["ordinary", "claim", "locked", "terminal", "default"])
@pytest.mark.parametrize("compact", [False, True])
def test_checkpoint_blocks_preserve_all_outputs_state_and_cross_day_gradient(monkeypatch, kind, compact):
    expected, expected_grad, original_saved = _evaluate(monkeypatch, 0, kind, compact)
    actual, grad, checkpoint_saved = _evaluate(monkeypatch, 2, kind, compact)
    for name in carry._CARRY_RESULT_FIELDS:
        torch.testing.assert_close(getattr(actual,name), getattr(expected,name),
            rtol=0, atol=0, equal_nan=True)
    assert actual.final_state.last_session_day == expected.final_state.last_session_day
    torch.testing.assert_close(actual.final_state.last_nav, expected.final_state.last_nav, rtol=0, atol=0)
    assert torch.equal(actual.final_state.alive, expected.final_state.alive)
    for field in fields(actual.final_state.inventory):
        torch.testing.assert_close(getattr(actual.final_state.inventory,field.name),
            getattr(expected.final_state.inventory,field.name), rtol=0, atol=0)
    torch.testing.assert_close(grad, expected_grad, rtol=1e-12, atol=1e-10)
    assert torch.isfinite(grad).all()
    assert checkpoint_saved < original_saved


@pytest.mark.parametrize("raw", ["-2", "3", "invalid"])
def test_checkpoint_block_size_fails_closed(monkeypatch, raw):
    monkeypatch.setenv(FLAG, raw)
    with pytest.raises(ValueError, match="zero or a power of two"):
        run(v(.2).reshape(1,1).requires_grad_(), [session()])


def test_checkpoint_never_changes_no_grad_artifact_path(monkeypatch):
    monkeypatch.setenv(FLAG, "2")
    carry.reset_day_trade_carry_compile_stats()
    with torch.no_grad():
        run(v(.2,.2,.2).reshape(-1,1), [session(DAY+i) for i in range(3)])
    assert carry.get_day_trade_carry_compile_stats()["checkpointed_trajectory_blocks"] == 0


def _evaluate_loss_outputs(monkeypatch, rows, history_grad, kind, compact):
    monkeypatch.setenv(FLAG, str(rows))
    weights = v(-.9, .2, -.1, .3, 0, -.2, .1).reshape(-1, 1).requires_grad_()
    saved_bytes = 0

    def pack(value):
        nonlocal saved_bytes
        saved_bytes += value.numel() * value.element_size()
        return value

    with torch.autograd.graph.saved_tensors_hooks(pack, lambda value: value):
        result = carry.run_day_trade_carry_sessions(
            weights, tuple(_cases(kind)), can_enter=torch.ones_like(weights),
            buy_fee_rate=v(.001425), day_sell_fee_rate=v(.002925),
            normal_sell_fee_rate=v(.004425), rebate_rate=v(.00114),
            initial_capital=10_000_000., event_compression=compact,
            require_minute_nav_grad=history_grad,
        )
        # All the physical outputs the canonical objectives can consume.
        # Intraday diagnostics are not used by risk_aware_loss.
        objective = (result.strategy_returns.sum() + result.turnovers.sum()*1e-3
                     + result.weights_history.square().sum()*1e-4
                     + result.final_state.last_nav*1e-7)
        grad, = torch.autograd.grad(objective, weights)
    return result, grad, saved_bytes


@pytest.mark.parametrize("kind", ["ordinary", "claim", "locked", "terminal", "default"])
@pytest.mark.parametrize("compact", [False, True])
def test_unused_intraday_adjoint_preserves_values_and_loss_gradient(monkeypatch, kind, compact):
    expected, expected_grad, saved = _evaluate_loss_outputs(monkeypatch, 0, True, kind, compact)
    lean, lean_grad, lean_saved = _evaluate_loss_outputs(monkeypatch, 0, False, kind, compact)
    actual, grad, checkpoint_saved = _evaluate_loss_outputs(monkeypatch, 2, False, kind, compact)
    for result in (lean, actual):
        for name in carry._CARRY_RESULT_FIELDS:
            torch.testing.assert_close(getattr(result, name), getattr(expected, name),
                                       rtol=0, atol=0, equal_nan=True)
        for f in fields(result.final_state.inventory):
            torch.testing.assert_close(getattr(result.final_state.inventory, f.name),
                                       getattr(expected.final_state.inventory, f.name),
                                       rtol=0, atol=0)
        torch.testing.assert_close(result.final_state.last_nav, expected.final_state.last_nav,
                                   rtol=0, atol=0)
        assert torch.equal(result.final_state.alive, expected.final_state.alive)
        assert result.final_state.last_session_day == expected.final_state.last_session_day
    for observed in (lean_grad, grad):
        torch.testing.assert_close(observed, expected_grad, rtol=1e-12, atol=1e-10)
        assert torch.isfinite(observed).all()
    assert lean_saved < saved
    assert checkpoint_saved < saved


def _legacy_commit(candidate, working, original, *, trade_alive, alive, valid):
    committed = carry._choose_inventory(trade_alive, candidate, working)
    committed = carry._choose_inventory(alive, committed, original)
    return carry._choose_inventory(valid, committed, original)


def _selection_states(device="cpu", *, symbols=5, rows=3):
    # Unique values per column expose misplaced dtype/layout or calendar data.
    # These are selection-kernel inputs, not a fake market-data source.
    states = []
    inputs = []
    for variant in range(3):
        state = carry.DayTradeInventoryState.empty(symbols, device=device)
        values = {}
        for i, f in enumerate(fields(state)):
            value = getattr(state, f.name)
            shape = ((rows if variant == 0 else rows-1), symbols, len(carry.CohortField)) if f.name == "cohorts" else (
                (2-variant, symbols, 3) if f.name == "claims" else value.shape)
            count = 1
            for dimension in shape:
                count *= dimension
            value = (torch.arange(count, device=device, dtype=torch.float64)
                     .reshape(shape) / 128 + 100*variant + i).requires_grad_()
            values[f.name] = value
            inputs.append(value)
        states.append(replace(state, **values))
    return states, inputs


def _selection_objective(state):
    return sum((i+1)*getattr(state, f.name).sum() for i, f in enumerate(fields(state)))


@pytest.mark.parametrize("trading,alive,valid", list(itertools.product((False, True), repeat=3)))
def test_combined_commit_preserves_padding_all_fields_and_gradient(trading, alive, valid):
    states, inputs = _selection_states()
    predicates = [torch.tensor(value) for value in (trading, alive, valid)]
    expected = _legacy_commit(*states, trade_alive=predicates[0],
                              alive=predicates[1], valid=predicates[2])
    actual = carry._commit_carry_inventory(*states, trade_alive=predicates[0],
                                          accept=predicates[1] & predicates[2])
    for f in fields(expected):
        torch.testing.assert_close(getattr(actual, f.name), getattr(expected, f.name),
                                   rtol=0, atol=0)
    reference = torch.autograd.grad(_selection_objective(expected), inputs, retain_graph=True)
    gradients = torch.autograd.grad(_selection_objective(actual), inputs)
    for grad, expected_grad in zip(gradients, reference):
        torch.testing.assert_close(grad, expected_grad, rtol=0, atol=0)


def _check_large_compiled_commit(monkeypatch, device):
    states, inputs = _selection_states(device, symbols=2757, rows=128)
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_CARRY_COMMIT_COMPILE", "1")
    constructors = None
    for trading, alive, valid in itertools.product((False, True), repeat=3):
        predicates = [torch.tensor(value, device=device) for value in (trading, alive, valid)]
        expected = _legacy_commit(*states, trade_alive=predicates[0],
                                  alive=predicates[1], valid=predicates[2])
        actual = carry._commit_carry_inventory(*states, trade_alive=predicates[0],
                                              accept=predicates[1] & predicates[2])
        for f in fields(expected):
            torch.testing.assert_close(getattr(actual, f.name), getattr(expected, f.name),
                                       rtol=0, atol=0)
        reference = torch.autograd.grad(_selection_objective(expected), inputs, retain_graph=True)
        gradients = torch.autograd.grad(_selection_objective(actual), inputs)
        for grad, expected_grad in zip(gradients, reference):
            torch.testing.assert_close(grad, expected_grad, rtol=0, atol=0)
        count = carry.get_day_trade_carry_compile_stats()["commit_compile_constructors"]
        if constructors is None:
            constructors = count
        assert count == constructors, "Do not specialize per trading/default flag"


def _flat_case(monkeypatch, flat, compact, symbols, kind, device="cpu"):
    monkeypatch.setenv(FLAT_FLAG, str(int(flat)))
    monkeypatch.setenv(FLAG, "0")
    source = _cases("terminal")
    if kind == "unowned_action":
        source[2] = replace(source[2], action_mask=v(1), share_ratio=v(1.1),
            cash_per_old_share=v(1), payment_day=v(DAY+5), stock_delivery_day=v(DAY+5))
    elif kind == "default":
        # An intraday adverse mark, not a fictitious overnight held position.
        source[0] = replace(source[0], marks=torch.full_like(source[0].marks, 5000))
    source = tuple(replace(s, **{
        f.name: value.repeat(symbols, *(1 for _ in range(value.ndim-1))).to(device)
        for f in fields(s) if isinstance(value := getattr(s, f.name), torch.Tensor)
    }) for s in source)
    weights = (v(-.9, .2, -.1, .3, 0, -.2, .1).reshape(-1, 1)
               * torch.linspace(.5, 1.5, symbols, dtype=torch.float64) / symbols).to(device).requires_grad_()
    # Synthetic capital scaling keeps a multi-symbol oracle's lot fills real,
    # rather than reporting parity because every target rounded to zero.
    capital = 10_000_000. * max(1, symbols / 7)
    result = carry.run_day_trade_carry_sessions(weights, source,
        can_enter=torch.ones_like(weights), buy_fee_rate=v(.001425).expand(symbols).to(device),
        day_sell_fee_rate=v(.002925).expand(symbols).to(device),
        normal_sell_fee_rate=v(.004425).expand(symbols).to(device), rebate_rate=v(.00114).expand(symbols).to(device),
        initial_capital=capital, event_compression=compact, require_minute_nav_grad=False)
    objective = (result.strategy_returns.sum() + result.turnovers.sum()*1e-3
        + result.weights_history.square().sum()*1e-4 + result.final_state.last_nav*1e-7)
    # The analytic certificate is about the whole dormant physical state, not
    # only one summation that could hide cancelling tangent contributions.
    if kind != "default":
        coefficients = torch.arange(result.final_state.inventory.cohorts.numel(),
            dtype=torch.float64, device=device).reshape_as(result.final_state.inventory.cohorts) / 128 + 1
        dormant_grad, = torch.autograd.grad((result.final_state.inventory.cohorts*coefficients).sum(),
                                          weights, retain_graph=True)
        # Exact arithmetic Jacobian is zero. Preserve the existing FP64
        # gradient acceptance tolerance for cancellation roundoff, rather than
        # treating an observed ~1e-12 adjoint as a physical open position.
        torch.testing.assert_close(dormant_grad, torch.zeros_like(dormant_grad), rtol=0,
                                   atol=1e-8 if symbols > 7 else 2e-12)
    grad, = torch.autograd.grad(objective, weights)
    return result, grad


@pytest.mark.parametrize("symbols", [1, 7])
@pytest.mark.parametrize("compact", [False, True])
@pytest.mark.parametrize("kind", ["ordinary", "unowned_action", "default"])
def test_flat_terminal_keeps_financial_values_physical_state_and_gradient(monkeypatch, symbols, compact, kind):
    expected, reference = _flat_case(monkeypatch, False, compact, symbols, kind)
    carry.reset_day_trade_carry_compile_stats()
    actual, grad = _flat_case(monkeypatch, True, compact, symbols, kind)
    _assert_financially_close(actual, expected)
    torch.testing.assert_close(grad, reference, rtol=1e-12, atol=1e-10)
    stats = carry.get_day_trade_carry_compile_stats()
    assert stats["flat_terminal_cleared_sessions"] > 0
    assert stats["flat_terminal_batches"] == int(kind != "default")
    assert stats["flat_terminal_fallback_batches"] == int(kind == "default")


def test_flat_terminal_does_not_touch_minute_diagnostic_gradient_or_carry(monkeypatch):
    monkeypatch.setenv(FLAT_FLAG, "1")
    carry.reset_day_trade_carry_compile_stats()
    _evaluate_loss_outputs(monkeypatch, 0, True, "terminal", True)
    _evaluate_loss_outputs(monkeypatch, 0, False, "locked", True)
    assert carry.get_day_trade_carry_compile_stats()["flat_terminal_batches"] == 0


def test_flat_terminal_never_reintroduces_quadratic_zero_padding(monkeypatch):
    """Each proved-empty incoming ledger is O(S), not padded back to O(B*S)."""
    calls = []
    original = carry.execute_carry_session

    def observe(state, *args, **kwargs):
        calls.append(state.inventory.cohorts.shape[0])
        return original(state, *args, **kwargs)

    monkeypatch.setattr(carry, "execute_carry_session", observe)
    result, gradient = _flat_case(monkeypatch, True, True, 7, "ordinary")
    assert calls == [0] * 7
    assert result.turnovers.abs().sum() > 0
    assert torch.isfinite(gradient).all() and gradient.abs().sum() > 0


@pytest.mark.skipif(
    os.environ.get("STOCKAGENT_TEST_CARRY_CHECKPOINT_CUDA") != "1",
    reason="explicit managed two-GPU CUDA acceptance only",
)
def test_checkpoint_cuda_collective_preserves_compiled_outputs_and_gradients(monkeypatch):
    """Match training's differentiable all-gather before the replicated ledger.

    Invoke only with two torchrun ranks under the canonical GPU lease. This is
    an accounting/gradient oracle, not a throughput benchmark or a third runner.
    """
    import torch.distributed as dist
    from torch.distributed.nn.functional import all_gather

    assert torch.cuda.is_available()
    assert int(os.environ.get("WORLD_SIZE", "0")) == 2
    rank = int(os.environ["LOCAL_RANK"])
    torch.cuda.set_device(rank)
    device = torch.device("cuda", rank)
    dist.init_process_group("nccl", device_id=device, timeout=timedelta(seconds=120))
    finished = dist.new_group(backend="gloo", timeout=timedelta(seconds=120))
    try:
        for kind in ("ordinary", "claim", "locked", "terminal", "default"):
            source = _cases(kind) + [session(DAY+i, 1000+10*i, exits=i % 3 == 2)
                                     for i in range(7, 14)]
            if kind == "terminal":
                source = [replace(s, terminal_liquidation_price=v(1000+10*i))
                          for i, s in enumerate(source)]
            source = tuple(replace(s, **{
                f.name: getattr(s, f.name).to(device)
                for f in fields(s) if isinstance(getattr(s, f.name), torch.Tensor)
            }) for s in source)
            outputs = []
            gradients = []
            for block_rows, history_grad, compile_commit, flat in (
                    (0, True, False, False), (2, True, True, False),
                    (0, False, True, False), (2, False, True, False),
                    (0, False, True, True), (2, False, True, True)):
                monkeypatch.setenv(FLAG, str(block_rows))
                monkeypatch.setenv(FLAT_FLAG, str(int(flat)))
                monkeypatch.setenv("STOCKAGENT_DAY_TRADE_CARRY_COMMIT_COMPILE", str(int(compile_commit)))
                local = (v(-.9, .2, -.1, .3, 0, -.2, .1).reshape(-1, 1)
                         + rank*1e-5).to(device).requires_grad_()
                actions = torch.cat(all_gather(local), dim=0)
                result = carry.run_day_trade_carry_sessions(
                    actions, source, can_enter=torch.ones_like(actions),
                    buy_fee_rate=v(.001425).to(device),
                    day_sell_fee_rate=v(.002925).to(device),
                    normal_sell_fee_rate=v(.004425).to(device),
                    rebate_rate=v(.00114).to(device), initial_capital=10_000_000.,
                    event_compression=True,
                    require_minute_nav_grad=history_grad,
                )
                objective = (result.strategy_returns.sum() + result.turnovers.sum()*1e-3
                             + result.weights_history.square().sum()*1e-4
                             + result.final_state.last_nav*1e-7)
                grad, = torch.autograd.grad(objective, local)
                assert torch.isfinite(grad).all()
                outputs.append(result)
                gradients.append(grad)
            expected = outputs[0]
            for actual, grad in zip(outputs[1:], gradients[1:]):
                for name in carry._CARRY_RESULT_FIELDS:
                    torch.testing.assert_close(getattr(actual, name), getattr(expected, name),
                                               rtol=0, atol=0, equal_nan=True)
                for f in fields(actual.final_state.inventory):
                    torch.testing.assert_close(getattr(actual.final_state.inventory, f.name),
                                               getattr(expected.final_state.inventory, f.name),
                                               rtol=0, atol=0)
                torch.testing.assert_close(actual.final_state.last_nav,
                                           expected.final_state.last_nav, rtol=0, atol=0)
                assert torch.equal(actual.final_state.alive, expected.final_state.alive)
                assert actual.final_state.last_session_day == expected.final_state.last_session_day
                torch.testing.assert_close(grad, gradients[0], rtol=1e-12, atol=1e-10)
            print(f"[rank{rank}] checkpoint / lean-adjoint CUDA oracle {kind}: exact forward / gradient passed", flush=True)
        assert carry.get_day_trade_carry_compile_stats()["checkpointed_trajectory_blocks"] > 0
        assert carry.get_day_trade_carry_compile_stats()["compiled_session_calls"] > 0
        assert carry.get_day_trade_carry_compile_stats()["compile_failures"] == 0
        for symbols in (7, 127):
            expected, reference = _flat_case(monkeypatch, False, True, symbols, "ordinary", device)
            actual, grad = _flat_case(monkeypatch, True, True, symbols, "ordinary", device)
            _assert_financially_close(actual, expected)
            assert actual.turnovers.abs().sum() > 0, "Do not accept an empty-action oracle"
            torch.testing.assert_close(grad, reference, rtol=1e-10, atol=1e-8)
            nav_error = (actual.final_state.last_nav - expected.final_state.last_nav).abs().item()
            grad_error = (grad - reference).abs().max().item()
            print(f"[rank{rank}] {symbols}-symbol flat oracle: physical state exact / "
                  f"financial tolerance passed; NAV_abs_error={nav_error:.12g} "
                  f"gradient_max_abs_error={grad_error:.12g}", flush=True)
        _check_large_compiled_commit(monkeypatch, device)
        assert carry.get_day_trade_carry_compile_stats()["compiled_commit_calls"] > 0
        assert carry.get_day_trade_carry_compile_stats()["flat_terminal_batches"] > 0
        print(f"[rank{rank}] [128,2757,12] compiled commit: all 8 predicates / exact state and gradients", flush=True)
        print(f"[rank{rank}] all financial invariants passed; CPU completion barrier", flush=True)
        # No GPU work remains. Match the maintained artifact-completion contract
        # instead of introducing an implicit-device NCCL scalar barrier.
        dist.monitored_barrier(group=finished, timeout=timedelta(seconds=30), wait_all_ranks=True)
        print(f"[rank{rank}] completion barrier passed; destroying groups", flush=True)
    finally:
        dist.destroy_process_group()
        print(f"[rank{rank}] groups destroyed", flush=True)

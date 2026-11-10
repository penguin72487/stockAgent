"""Hold actions fixed when validating transport, independently of SGD noise."""
from dataclasses import fields, replace
from types import SimpleNamespace

import pytest
import torch

from stockagent.backtest.tw_day_trade_carry import run_day_trade_carry_sessions
from stockagent.training.day_trade_carry_bridge import (
    PackedDayTradeCarrySession,
    PreparedDayTradeCarrySource,
    prefetch_physical_carry_batches,
)
from test_tw_day_trade_graph import _inputs, _packed_source_fixture


@pytest.mark.parametrize("row", [0, 2])
def test_uncompressed_packed_single_row_preserves_original_source_bits(monkeypatch, row):
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_PREPARED_CACHE_BYTES", str(1 << 20))
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_PREPARED_CACHE_PINNED", "0")
    source, split, _ = _packed_source_fixture()
    prepared = source.prepare_batch(split, row, row + 1, event_compression=False)
    actual, = prepared.sessions(torch.device("cpu"))
    expected = source.session_at(row)
    for f in fields(expected):
        a, b = getattr(actual, f.name), getattr(expected, f.name)
        if isinstance(a, torch.Tensor):
            assert torch.equal(a.contiguous().view(torch.int64), b.contiguous().view(torch.int64))
        else:
            assert a == b
    assert source.prepare_batch(split, row, row + 1, event_compression=False) is prepared


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires CUDA transport")
@pytest.mark.parametrize("device_index", [0, 1])
@pytest.mark.parametrize("compiled", ["0", "1"])
@pytest.mark.parametrize("full_minute_nav", [False, True])
def test_fixed_actions_source_cache_prefetch_financial_forward_and_vjp(
    monkeypatch, device_index, compiled, full_minute_nav
):
    if device_index >= torch.cuda.device_count():
        pytest.skip("second device unavailable")
    monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", compiled)
    monkeypatch.setenv("STOCKAGENT_STRICT_NO_FALLBACK", "0")
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_EXPLICIT_GRAPH", "1")
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_CUDAGRAPHS", "0")
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_GRAPH_BLOCK_ROWS", "4")
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_SPARSE_EVENTS", "1")
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_SPARSE_EVENT_SLOTS", "16")
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_SPARSE_COMPACT_TRANSPORT", "1")
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_DEVICE_PREFETCH_ROWS", "1")
    device = torch.device("cuda", device_index)
    cpu_weights, original, _, _, common = _inputs()
    sessions = [replace(s, short_limit_up_penalty_per_share=torch.zeros_like(s.official_open))
                for s in original]
    # Include a residual short, a close at its limit, and the 7% source charge.
    s = sessions[1]
    marks, capacity = s.marks.clone(), s.exit_capacity.clone()
    marks[1] = s.upper_limit[1]
    capacity[1] = 0
    terminal = s.terminal_liquidation_price.clone()
    terminal[1] = s.upper_limit[1]
    penalty = torch.zeros_like(terminal)
    penalty[1] = terminal[1] * .07
    sessions[1] = replace(s, marks=marks, exit_capacity=capacity,
        terminal_liquidation_price=terminal, short_limit_up_penalty_per_share=penalty)
    sessions = tuple(sessions)

    def move(s):
        return replace(s, **{f.name: getattr(s, f.name).to(device) for f in fields(s)
                             if isinstance(getattr(s, f.name), torch.Tensor)})

    options = {k: v.to(device) if isinstance(v, torch.Tensor) else v
               for k, v in common.items()}
    options["event_compression"] = not full_minute_nav
    options["require_minute_nav_grad"] = full_minute_nav
    weights = cpu_weights.detach().to(device).requires_grad_()
    expected = run_day_trade_carry_sessions(weights, tuple(map(move, sessions)),
        **options, _allow_flat_terminal=False)
    expected_gradient, = torch.autograd.grad(expected.strategy_returns.sum(), weights)
    assert torch.isfinite(expected.strategy_returns).all()
    assert torch.isfinite(expected_gradient).all() and expected_gradient.abs().sum() > 0

    class Split(SimpleNamespace):
        def __len__(self):
            return len(self._valid_indices_cpu)

    split = Split(features=torch.zeros((3, 2, 1)), _valid_indices_cpu=torch.arange(3),
                  sample_mask=None, symbol_indices=None, execution_mode="tw_day_trade")
    packed = []
    for s in sessions:
        values = {f.name: getattr(s, f.name) for f in fields(PackedDayTradeCarrySession)
                  if hasattr(s, f.name)}
        flat = torch.nonzero(torch.isfinite(s.exit_prices.flatten()), as_tuple=False).flatten()
        values.update(exit_flat=flat, exit_price=s.exit_prices.flatten()[flat],
                      exit_capacity=s.exit_capacity.flatten()[flat])
        packed.append(PackedDayTradeCarrySession(**values))
    for variant in ("uncached", "prepared", "prefetch"):
        monkeypatch.setenv("STOCKAGENT_DAY_TRADE_PREPARED_CACHE_BYTES",
                           "0" if variant == "uncached" else str(1 << 20))
        monkeypatch.setenv("STOCKAGENT_DAY_TRADE_PREPARED_CACHE_PINNED", "1")
        monkeypatch.setenv("STOCKAGENT_DAY_TRADE_DEVICE_PREFETCH",
                           "1" if variant == "prefetch" else "0")
        source = PreparedDayTradeCarrySource((), ("2300", "2301"), "fixed-actions-v1",
            session_days=tuple(s.day for s in sessions),
            session_loader=lambda row: sessions[row], packed_session_loader=lambda row: packed[row])
        staged = tuple(s for _, batch in prefetch_physical_carry_batches(
            source, split, [0, 1], 2, batch_spans=((0, 2), (2, 3)), device=device,
            event_compression=not full_minute_nav) for s in batch.sessions(device))
        weights = cpu_weights.detach().to(device).requires_grad_()
        actual = run_day_trade_carry_sessions(weights, staged, **options)
        for name in ("strategy_returns", "turnovers", "weights_history", "shares_history",
                     "settlement_default"):
            torch.testing.assert_close(getattr(actual, name), getattr(expected, name),
                                       rtol=1e-12, atol=1e-7)
        # The loss-only compressed ABI exposes terminal NAV, not a full minute
        # timeline. The report ABI explicitly requests and checks all 270 marks.
        torch.testing.assert_close(actual.minute_nav if full_minute_nav else actual.minute_nav[:, -1],
            expected.minute_nav if full_minute_nav else expected.minute_nav[:, -1],
            rtol=1e-12, atol=1e-7)
        for f in fields(actual.final_state.inventory):
            torch.testing.assert_close(getattr(actual.final_state.inventory, f.name),
                getattr(expected.final_state.inventory, f.name), rtol=1e-12, atol=1e-7)
        gradient, = torch.autograd.grad(actual.strategy_returns.sum(), weights)
        torch.testing.assert_close(gradient, expected_gradient, rtol=1e-5, atol=1e-8)
        assert torch.isfinite(gradient).all() and gradient.abs().sum() > 0

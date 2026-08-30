from __future__ import annotations

import torch

import stockagent.backtest.distributed_reduction as reduction


def test_scalar_collective_packing_switch_preserves_values(monkeypatch) -> None:
    monkeypatch.setattr(reduction, "symbol_sharded_world_size", lambda enabled: 2)
    calls: list[torch.Tensor] = []

    def fake_sum(value: torch.Tensor) -> torch.Tensor:
        calls.append(value.clone())
        return value + 10.0

    monkeypatch.setattr(reduction, "_functional_sum", fake_sum)
    values = tuple(torch.tensor(float(index)) for index in range(3))

    monkeypatch.setenv("STOCKAGENT_SYMBOL_SHARDED_PACK_SCALARS", "1")
    packed = reduction.global_symbol_scalar_pack(values, symbol_sharded=True)
    assert len(calls) == 1
    assert tuple(calls[0].shape) == (3,)

    calls.clear()
    monkeypatch.setenv("STOCKAGENT_SYMBOL_SHARDED_PACK_SCALARS", "0")
    unpacked = reduction.global_symbol_scalar_pack(values, symbol_sharded=True)
    assert len(calls) == 3
    assert all(call.ndim == 0 for call in calls)
    torch.testing.assert_close(unpacked, packed)


def test_noop_collective_specialization_switch(monkeypatch) -> None:
    monkeypatch.delenv(
        "STOCKAGENT_SYMBOL_SHARDED_SKIP_NOOP_COLLECTIVES",
        raising=False,
    )
    assert reduction.symbol_sharded_skip_noop_collectives_enabled() is True
    monkeypatch.setenv("STOCKAGENT_SYMBOL_SHARDED_SKIP_NOOP_COLLECTIVES", "0")
    assert reduction.symbol_sharded_skip_noop_collectives_enabled() is False

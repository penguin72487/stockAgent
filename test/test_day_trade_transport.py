"""Lossless CPU transport must reconstruct the exact dense source tape."""
from dataclasses import fields, replace

import pytest
import torch

from stockagent.training.day_trade_carry_bridge import (
    PackedDayTradeCarrySession,
    PreparedDayTradeCarryBatch,
    _stack_transport_tensors,
)


def _assert_same_bits(actual, expected):
    torch.testing.assert_close(
        actual.view(torch.int64), expected.view(torch.int64), rtol=0, atol=0,
    )


def _packed(day, events, *, terminal):
    symbols = 3
    vector = torch.tensor([100., -0., float("nan")], dtype=torch.float64)
    values = {
        field.name: vector.clone()
        for field in fields(PackedDayTradeCarrySession)
        if field.name not in {
            "day", "marks", "exit_flat", "exit_price", "exit_capacity",
            "terminal_liquidation_price",
        }
    }
    # Noncontiguous views, empty days and unequal event counts are legitimate
    # source representations. Transport may change their layout only.
    prices = torch.arange(events * 2, dtype=torch.float64)[::2]
    if events:
        prices[0] = -0.
    return PackedDayTradeCarrySession(
        day=day, **values,
        marks=torch.arange(symbols * 540, dtype=torch.float64).reshape(symbols, 540)[:, ::2],
        exit_flat=torch.arange(events * 2, dtype=torch.int64)[::2],
        exit_price=prices,
        exit_capacity=torch.arange(events * 2, dtype=torch.float64)[::2],
        terminal_liquidation_price=vector.clone() if terminal else None,
    )


@pytest.mark.parametrize("event_compression", [False, True])
@pytest.mark.parametrize("terminal", [False, True])
def test_packed_transport_preserves_every_source_byte_and_dense_cell(
    event_compression, terminal,
):
    sources = tuple(
        _packed(730000 + i, n, terminal=terminal)
        for i, n in enumerate([0, 3, 7, 4])
    )
    before = [
        {f.name: getattr(s, f.name).clone() for f in fields(s)
         if isinstance(getattr(s, f.name), torch.Tensor)}
        for s in sources
    ]
    batch = PreparedDayTradeCarryBatch.from_packed_sessions(
        sources, len(sources), event_compression=event_compression,
    )
    assert batch.count == 4 and batch.event_compression == event_compression
    assert batch.tensors["_transport_exit_flat"].shape == (4, 8)
    rebuilt_sessions = batch.sessions(torch.device("cpu"))
    for index, (source, rebuilt) in enumerate(zip(sources, rebuilt_sessions, strict=True)):
        for name, original in before[index].items():
            _assert_same_bits(getattr(source, name), original)
            if not name.startswith("exit_"):
                _assert_same_bits(getattr(rebuilt, name), original.contiguous())
        expected_prices = torch.full((3 * 270 * 2,), float("nan"), dtype=torch.float64)
        expected_capacity = torch.zeros_like(expected_prices)
        expected_prices[source.exit_flat] = source.exit_price
        expected_capacity[source.exit_flat] = source.exit_capacity
        _assert_same_bits(rebuilt.exit_prices.reshape(-1), expected_prices)
        _assert_same_bits(rebuilt.exit_capacity.reshape(-1), expected_capacity)
    # Batch storage is independently owned: consumers cannot mutate the cache.
    batch.tensors["official_open"][0, 0] = 999.
    assert sources[0].official_open[0] == 100.


def test_transport_padding_keeps_autograd_for_non_source_tensors():
    first = torch.tensor([2., 3.], dtype=torch.float64, requires_grad=True)
    second = torch.tensor([4.], dtype=torch.float64, requires_grad=True)
    result = _stack_transport_tensors((first, second), padded_columns=4)
    result.sum().backward()
    torch.testing.assert_close(first.grad, torch.ones_like(first))
    torch.testing.assert_close(second.grad, torch.ones_like(second))


def test_transport_rejects_invalid_event_indices():
    source = _packed(730000, 3, terminal=False)
    bad = replace(source, exit_flat=torch.tensor([-1, 0, 3]))
    with pytest.raises(ValueError, match="invalid lossless"):
        PreparedDayTradeCarryBatch.from_packed_sessions((bad,), 1, event_compression=True)

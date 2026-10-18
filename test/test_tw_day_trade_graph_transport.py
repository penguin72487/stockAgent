"""Source-bit, lifetime and financial-VJP checks for grouped graph transport."""
from collections import OrderedDict
import gc
from pathlib import Path
import runpy
import weakref

import pytest
import torch

from stockagent.backtest.tw_day_trade_graph_transport import (
    copy_packed_session_fields_, packed_session_copy_contract)


def test_packed_copy_is_opt_in_and_versions_complete_checkpoint_contracts(monkeypatch):
    from stockagent.config import load_config
    from stockagent.training.checkpoint_contract import _configuration_fingerprint_snapshot, _training_checkpoint_contract
    cfg = load_config('configs/deployments/tw_day_trade_factorized_values_20261007_scale_separated_cash_annual_v1.yaml')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_GRAPH_PACKED_COPY', 'native')
    assert packed_session_copy_contract() is None
    configuration, training = _configuration_fingerprint_snapshot(cfg), _training_checkpoint_contract(cfg)
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_GRAPH_PACKED_COPY', 'triton')
    selected, contract = _configuration_fingerprint_snapshot(cfg), _training_checkpoint_contract(cfg)
    assert selected != configuration and contract != training
    assert selected['training']['physical_fifo_packed_copy_contract'] == contract['physical_fifo_packed_copy_contract']
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_GRAPH_PACKED_COPY', 'unknown')
    with pytest.raises(ValueError, match='copy must be'):
        packed_session_copy_contract()


def test_unsupported_cpu_or_overlapping_transport_preserves_fallback_inputs():
    source = torch.arange(21).reshape(3, 7)
    target = torch.full_like(source, -1)
    assert not copy_packed_session_fields_((target,), (source,), source_row=0, rows=3, cache=OrderedDict())
    assert (target == -1).all()
    assert not copy_packed_session_fields_((source,), (source,), source_row=0, rows=3, cache=OrderedDict())


@pytest.mark.skipif(not torch.cuda.is_available(), reason='requires CUDA pointer transport')
@pytest.mark.parametrize('layout', ['contiguous', 'strided', 'broadcast'])
def test_grouped_copy_preserves_all_bits_strides_tail_and_mutated_values(layout):
    types = [torch.float64, torch.float32, torch.float16, torch.int64,
             torch.int32, torch.int16, torch.int8, torch.bool]
    sources, destinations = [], []
    for index, dtype in enumerate(types):
        shape = (9, *(range(4, 4 + index % 4)))
        raw = (torch.randint(0, 2, shape, device='cuda', dtype=dtype) if dtype == torch.bool else
               torch.randint(-100, 100, shape, device='cuda').to(dtype))
        if raw.is_floating_point():
            raw.reshape(-1)[:3] = torch.tensor([float('nan'), -0., 1e-18], device='cuda', dtype=dtype)
        value = raw[:, ::2] if layout == 'strided' and raw.ndim > 1 else raw
        if layout == 'broadcast':
            value = value[:1].expand(9, *value.shape[1:])
        sources.append(value)
        destinations.append(torch.empty((6, *value.shape[1:]), device='cuda', dtype=dtype))
    cache = OrderedDict()
    for rows, source_row in [(6, 0), (4, 2), (1, 7)]:
        assert copy_packed_session_fields_(destinations, sources,
            source_row=source_row, rows=rows, cache=cache)
        for source, actual in zip(sources, destinations, strict=True):
            expected = torch.cat((source[source_row:source_row + rows],
                source[source_row + rows - 1:source_row + rows].expand(6 - rows, *source.shape[1:])))
            assert torch.equal(actual.view(torch.uint8), expected.contiguous().view(torch.uint8))
    assert len(cache) == 1
    metadata = next(iter(cache.values()))[0]
    for source in sources:
        source.zero_()
    assert copy_packed_session_fields_(destinations, sources, source_row=1, rows=3, cache=cache)
    assert next(iter(cache.values()))[0] is metadata
    assert all(not actual.any() for actual in destinations)


@pytest.mark.skipif(not torch.cuda.is_available(), reason='requires CUDA source lifetime checks')
def test_pointer_cache_does_not_retain_source_or_inference_only_metadata():
    cache = OrderedDict()
    destination = torch.empty((4, 3), device='cuda')
    with torch.inference_mode():
        source = torch.randn((5, 3), device='cuda')
        reference = weakref.ref(source)
        assert copy_packed_session_fields_((destination,), (source,), source_row=1, rows=4, cache=cache)
    assert not torch.is_inference(next(iter(cache.values()))[0])
    del source
    gc.collect()
    assert reference() is None
    # Geometry changes are metadata misses; cached values are never reused.
    for i in range(40):
        source = torch.ones((5, 3 + i), device='cuda')
        destination = torch.empty((4, 3 + i), device='cuda')
        assert copy_packed_session_fields_((destination,), (source,), source_row=1, rows=4, cache=cache)
        assert (destination == 1).all()
    assert len(cache) <= 32
    assert sum(t.numel() * t.element_size() for t, _ in cache.values()) <= 256 * 1024


@pytest.mark.skipif(not torch.cuda.is_available(), reason='requires financial CUDA Graph replay')
@pytest.mark.parametrize('adjoint', ['recompute', 'saved'])
@pytest.mark.parametrize('compressed', [False, True])
def test_grouped_copy_preserves_pending_financial_graph_adjoints(monkeypatch, adjoint, compressed):
    from stockagent.backtest.tw_day_trade_graph import get_explicit_carry_graph_stats
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_GRAPH_PACKED_COPY', 'triton')
    before = get_explicit_carry_graph_stats()['packed_field_copy_replays']
    oracle = runpy.run_path(str(Path(__file__).with_name('test_tw_day_trade_graph.py')))
    oracle['test_graph_reuse_different_calendar_tail_and_pending_backward'](
        monkeypatch, adjoint, compressed, True)
    assert get_explicit_carry_graph_stats()['packed_field_copy_replays'] > before

"""Immutable observations can persist on device; learned values never can."""
from types import SimpleNamespace

import pytest
import torch

from stockagent.training.windowed import ImmutableWindowedBatchCache


def fixture():
    source = SimpleNamespace(metadata=torch.arange(8, dtype=torch.float32), lookback=32)
    return source, {'sample': source.metadata.clone()}


def test_reuse_is_fold_owned_and_dictionary_edits_do_not_mutate_the_entry():
    source, batch = fixture()
    cache = ImmutableWindowedBatchCache(128)
    assert cache.get(source, (0, 4)) is None
    cache.put(source, (0, 4), batch)
    actual = cache.get(source, (0, 4))
    assert actual['sample'] is batch['sample']
    actual['sample'] = torch.zeros(8)
    assert torch.equal(cache.get(source, (0, 4))['sample'], source.metadata)
    other, _ = fixture()
    assert cache.get(other, (0, 4)) is None and cache.bytes == 0


@pytest.mark.parametrize('mutation', ['inplace', 'replacement', 'lookback'])
def test_source_changes_invalidate_the_device_copy(mutation):
    source, batch = fixture()
    cache = ImmutableWindowedBatchCache(128)
    cache.put(source, (0, 4), batch)
    if mutation == 'inplace':
        source.metadata.add_(1)
    elif mutation == 'replacement':
        source.metadata = source.metadata.clone()
    else:
        source.lookback = 16
    assert cache.get(source, (0, 4)) is None and cache.bytes == 0


def test_device_input_mutation_is_rejected_instead_of_reused():
    source, batch = fixture()
    cache = ImmutableWindowedBatchCache(128)
    cache.put(source, 0, batch)
    batch['sample'].add_(1)
    with pytest.raises(RuntimeError, match='was mutated'):
        cache.get(source, 0)


def test_bytes_are_bounded_and_recent_entries_survive_eviction():
    source, batch = fixture()
    cache = ImmutableWindowedBatchCache(64)
    for key in range(2):
        cache.put(source, key, {'sample': batch['sample'].clone()})
    assert cache.get(source, 0) is not None
    cache.put(source, 2, {'sample': batch['sample'].clone()})
    assert cache.get(source, 1) is None
    assert cache.get(source, 0) is not None and cache.get(source, 2) is not None
    assert cache.bytes == 64
    cache.put(source, 3, {'large': torch.zeros(128)})
    assert cache.bytes == 64 and cache.get(source, 3) is None


def test_learned_values_and_disabled_budget_cannot_be_cached():
    source, batch = fixture()
    cache = ImmutableWindowedBatchCache(128)
    with pytest.raises(ValueError, match='learned activations'):
        cache.put(source, 0, {'sample': batch['sample'].requires_grad_()})
    source.metadata.requires_grad_()
    with pytest.raises(ValueError, match='learned windowed data'):
        cache.get(source, 0)
    source, batch = fixture()
    disabled = ImmutableWindowedBatchCache(0)
    disabled.put(source, 0, batch)
    assert disabled.get(source, 0) is None and disabled.bytes == 0

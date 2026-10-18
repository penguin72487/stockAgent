"""Verified source bytes survive device eviction without freezing learned weights."""
from copy import deepcopy
from types import SimpleNamespace

import numpy as np
import pytest
import torch

from stockagent.data.factorized_panel import FactorizedPanelSlab
from stockagent.models.financial_transformer import CandleEncoder
from stockagent.models.factorized_sparse_projection import project_factorized_sparse_rows
from test_factorized_sparse_projection import source_fixture

pytestmark = pytest.mark.filterwarnings(
    'ignore:Sparse CSR tensor support is in beta state:UserWarning')


@pytest.mark.parametrize('device', ['cpu', 'cuda'])
@pytest.mark.parametrize('norm', ['rmsnorm', 'layernorm'])
@pytest.mark.parametrize('policy', ['lru', 'lfu'])
def test_verified_host_reuse_after_device_eviction_has_live_full_adjoint(tmp_path, monkeypatch, device, norm, policy):
    if device == 'cuda' and not torch.cuda.is_available():
        pytest.skip('requires CUDA')
    monkeypatch.setenv('STOCKAGENT_SPARSE_HOST_CACHE_BYTES', str(1024**2))
    monkeypatch.setenv('STOCKAGENT_SPARSE_DEVICE_CACHE_POLICY', policy)
    monkeypatch.setenv('STOCKAGENT_SPARSE_ROW_DISK_CACHE_DIR', str(tmp_path/'csr-cache'))
    source = source_fixture(tmp_path)
    torch.manual_seed(607)
    candle = CandleEncoder(num_features=28, d_model=8, dropout=0., norm_type=norm,
        ffn_type='swiglu', sanitize_inputs=True, projection_fp32=True,
        causal_feature_rms_normalization=True).to(device)
    candle.set_causal_feature_rms_normalizer(torch.rand(28, device=device)+.5,
                                           torch.arange(28, device=device) != 13)
    model = SimpleNamespace(candle_encoder=candle, factorized_sparse_cache_bytes=1024**2)
    rows = np.array([0, 3, 4, 8, 3, 3])
    slab = FactorizedPanelSlab(source, rows, device=device)
    raw = torch.from_numpy(source[rows]).to(device)
    original = project_factorized_sparse_rows(model, slab).detach()
    store = model._factorized_sparse_row_store
    assert len(store.host_entries) == 2 and store.host_bytes <= store.host_budget_bytes
    if device == 'cuda':
        assert all(t.is_pinned() for values in store.host_entries.values() for t in values.values())
    # A real smaller device working set, while both immutable host blocks fit.
    store.budget_bytes = max(entry.nbytes for entry in store.entries.values())
    store.entries.clear()
    store.bytes = 0
    def reject_reconstruction(*args, **kwargs):
        raise AssertionError('verified host reuse must not reread/decompress source')
    monkeypatch.setattr(source._cache, 'get', reject_reconstruction)
    monkeypatch.setattr(store.disk, 'load', reject_reconstruction)
    reused = project_factorized_sparse_rows(model, slab).detach()
    torch.testing.assert_close(reused, original, rtol=0, atol=0)
    oracle = deepcopy(candle)
    with torch.no_grad():
        candle.joint_projection.proj.weight.add_(.02)
        oracle.joint_projection.proj.weight.add_(.02)
    actual = project_factorized_sparse_rows(model, slab)
    expected = oracle(raw)[0]
    torch.testing.assert_close(actual, expected, rtol=3e-5, atol=3e-5)
    assert not torch.equal(actual, original)
    coefficients = torch.randn_like(actual)
    (actual*coefficients).sum().backward()
    (expected*coefficients).sum().backward()
    for (name, p), (_, q) in zip(candle.named_parameters(), oracle.named_parameters(), strict=True):
        assert (p.grad is None) == (q.grad is None), name
        if p.grad is not None:
            torch.testing.assert_close(p.grad, q.grad, rtol=7e-5, atol=7e-5)
    assert store.host_hits >= 2 and store.bytes <= store.budget_bytes
    assert store.host_bytes <= store.host_budget_bytes
    candle.causal_feature_rms_scale.add_(.1)
    with pytest.raises(RuntimeError, match='normalizer changed'):
        project_factorized_sparse_rows(model, slab)


def test_host_budget_is_bounded_and_disabled_default_preserves_existing_path(tmp_path, monkeypatch):
    from stockagent.data.factorized_sparse_rows import FactorizedSparseRowStore
    source = source_fixture(tmp_path)
    candle = CandleEncoder(num_features=28, d_model=8, dropout=0., norm_type='rmsnorm',
        ffn_type='swiglu', sanitize_inputs=True, projection_fp32=True)
    monkeypatch.delenv('STOCKAGENT_SPARSE_HOST_CACHE_BYTES', raising=False)
    disabled = FactorizedSparseRowStore(source, candle, 'cpu', budget_bytes=1024**2)
    list(disabled.slices(np.arange(9)))
    assert disabled.host_bytes == 0 and not disabled.host_entries
    monkeypatch.setenv('STOCKAGENT_SPARSE_HOST_CACHE_BYTES', '1')
    bounded = FactorizedSparseRowStore(source, candle, 'cpu', budget_bytes=1024**2)
    list(bounded.slices(np.arange(9)))
    assert bounded.host_bytes == 0 and bounded.host_capacity_skips == 2
    monkeypatch.setenv('STOCKAGENT_SPARSE_HOST_CACHE_BYTES', '1300')
    lru = FactorizedSparseRowStore(source, candle, 'cpu', budget_bytes=1300)
    list(lru.slices(np.arange(9)))
    assert list(lru.host_entries) == [1]
    assert lru.host_bytes == 1292
    list(lru.slices(np.array([0])))
    assert list(lru.host_entries) == [0]
    assert lru.host_bytes == 1036 and lru.host_misses == 3
    assert lru.bytes <= lru.budget_bytes
    monkeypatch.setenv('STOCKAGENT_SPARSE_HOST_CACHE_BYTES', '-1')
    with pytest.raises(ValueError, match='nonnegative'):
        FactorizedSparseRowStore(source, candle, 'cpu', budget_bytes=1024**2)


def test_frequency_cache_retains_hot_rows_across_a_cold_scan(tmp_path, monkeypatch):
    from stockagent.data.factorized_panel import FactorizedPanelFeatures, write_array_block
    from stockagent.data.factorized_sparse_rows import FactorizedSparseRowStore
    rng = np.random.default_rng(311)
    raw = rng.normal(size=(9, 4, 5)).astype(np.float32)
    np.save(tmp_path/'common.npy', np.ones((9, 1), np.float32))
    blocks = []
    for start in [0, 3, 6]:
        proof = write_array_block(tmp_path/f'{start}.zst', raw[start:start+3, :, 2:])
        blocks.append({'start': start, 'shape': [3, 4, 3],
                       'symbol_blocks': [{**proof, 'symbol_start': 0}]})
    source = FactorizedPanelFeatures(tmp_path, dict(common={'path': 'common.npy'},
        blocks=blocks, individual_channels=['a', 'b', 'c'], common_channels=['d']), raw[:, :, :2])
    candle = CandleEncoder(num_features=6, d_model=8, dropout=0., norm_type='rmsnorm',
        ffn_type='swiglu', sanitize_inputs=True, projection_fp32=True)
    monkeypatch.setenv('STOCKAGENT_SPARSE_HOST_CACHE_BYTES', '1048576')
    retained = {}
    for policy in ['lru', 'lfu']:
        monkeypatch.setenv('STOCKAGENT_SPARSE_DEVICE_CACHE_POLICY', policy)
        store = FactorizedSparseRowStore(source, candle, 'cpu', budget_bytes=1048576)
        one = store._build(0).nbytes
        store.bytes = 0  # _build has no cache entry owner until slices admits it.
        store.budget_bytes = one*2
        for row in [0, 0, 0, 3, 6]:
            [(values, _)] = list(store.slices(np.array([row])))
            assert values.shape == (4, 6)
            assert store.bytes <= store.budget_bytes
        retained[policy] = set(store.entries)
        assert store.device_evictions == 1
    assert retained == {'lru': {1, 2}, 'lfu': {0, 2}}
    monkeypatch.setenv('STOCKAGENT_SPARSE_DEVICE_CACHE_POLICY', 'unsupported')
    with pytest.raises(ValueError, match='cache policy'):
        FactorizedSparseRowStore(source, candle, 'cpu', budget_bytes=1048576)

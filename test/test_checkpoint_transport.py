from collections import OrderedDict
import io

import pytest
import torch

from stockagent.training.checkpoint_transport import (
    cpu_checkpoint_snapshot, encode_checkpoint_manifest, decode_checkpoint_manifest,
)


def test_compact_metadata_canonical_roundtrip_and_corruption_rejection(tmp_path, monkeypatch):
    from stockagent.training.runtime import load_checkpoint
    from stockagent.training.trainer import _atomic_torch_save
    manifest = {'schema_version': 4, 'fingerprint': 'declared',
                'data': {'features': ['cash', '價量'], 'values': [1, 0.1, None, False]}}
    payload = {'experiment_manifest': manifest, 'model_state_dict': {'w': torch.ones(3)}}
    monkeypatch.setenv('STOCKAGENT_CHECKPOINT_MANIFEST_JSON', '1')
    monkeypatch.setenv('STOCKAGENT_CHECKPOINT_CPU_PACK', '1')
    path = tmp_path/'packed.pt'
    _atomic_torch_save(payload, path)
    raw = torch.load(path, map_location='cpu', weights_only=True)
    assert raw['experiment_manifest']['encoding'] == 'stockagent_checkpoint_manifest_json_v1'
    assert load_checkpoint(path)['experiment_manifest'] == manifest
    assert payload['experiment_manifest'] is manifest
    raw['experiment_manifest']['payload'] += b' '
    torch.save(raw, path)
    with pytest.raises(ValueError, match='SHA256'):
        load_checkpoint(path)


def test_non_json_metadata_keeps_its_native_contract():
    for manifest in ({'tuple': (1, 2)}, {3: 'integer key'}, {'tensor': torch.ones(1)}):
        payload = {'experiment_manifest': manifest}
        encoded = encode_checkpoint_manifest(payload)
        assert encoded['experiment_manifest'] is manifest
        assert decode_checkpoint_manifest(encoded)['experiment_manifest'] is manifest


def test_cpu_snapshot_preserves_state_metadata_and_requires_positive_budget():
    state = OrderedDict(weight=torch.arange(8).reshape(2, 4))
    state._metadata = {'': {'version': 7}}
    payload = {'model': state, 'nested': [None, (1, b'rng')]}
    assert cpu_checkpoint_snapshot(payload) is payload
    assert payload['model']._metadata == {'': {'version': 7}}
    with pytest.raises(ValueError, match='positive'):
        cpu_checkpoint_snapshot(payload, max_pack_bytes=0)


@pytest.mark.skipif(not torch.cuda.is_available(), reason='requires CUDA storage transport')
@pytest.mark.parametrize('budget', [32, 64 * 1024**2])
def test_cuda_snapshot_roundtrip_preserves_dtype_strides_aliases_and_rng(budget):
    base = torch.arange(48, device='cuda', dtype=torch.float64).reshape(6, 8)
    view = base[1:5:2, 2:7:2]
    state = OrderedDict(weight=view, alias=view, other=base.t(),
                        integer=torch.arange(17, device='cuda'),
                        bf16=torch.arange(13, device='cuda', dtype=torch.bfloat16),
                        empty=torch.empty(0, device='cuda'))
    state._metadata = {'': {'version': 3}}
    rng = torch.cuda.get_rng_state().clone()
    snap = cpu_checkpoint_snapshot({'state': state, 'rng': bytes(rng.tolist())}, max_pack_bytes=budget)
    torch.testing.assert_close(torch.cuda.get_rng_state(), rng, rtol=0, atol=0)
    assert snap['state']._metadata == state._metadata
    assert snap['state']['weight'] is snap['state']['alias']
    for name, value in state.items():
        actual = snap['state'][name]
        assert actual.device.type == 'cpu' and actual.dtype == value.dtype
        assert actual.stride() == value.stride()
        torch.testing.assert_close(actual, value.cpu(), rtol=0, atol=0)
    snap['state']['other'][2, 1] = -17
    assert snap['state']['weight'][0, 0] == -17  # Source view alias survives.
    assert base[1, 2] == 10  # Snapshot never mutates the training owner.
    stream = io.BytesIO()
    torch.save(snap, stream)
    stream.seek(0)
    restored = torch.load(stream, map_location='cpu', weights_only=True)
    assert restored['state']._metadata == state._metadata
    torch.testing.assert_close(restored['state']['weight'], snap['state']['weight'], rtol=0, atol=0)


@pytest.mark.skipif(not torch.cuda.is_available(), reason='requires CUDA optimizer checkpoint')
def test_cuda_optimizer_resume_matches_unpacked_checkpoint_next_update():
    torch.manual_seed(7)
    model = torch.nn.Sequential(torch.nn.Linear(5, 7), torch.nn.Linear(7, 2)).cuda()
    optimizer = torch.optim.AdamW(model.parameters(), lr=.001)
    inputs = torch.randn(11, 5, device='cuda')
    model(inputs).square().mean().backward()
    optimizer.step()
    payload = {'model': model.state_dict(), 'optimizer': optimizer.state_dict()}
    reference = io.BytesIO()
    torch.save(payload, reference)
    reference.seek(0)
    unpacked = torch.load(reference, map_location='cpu', weights_only=True)
    packed = cpu_checkpoint_snapshot(payload)
    clones = []
    for state in (unpacked, packed):
        clone = torch.nn.Sequential(torch.nn.Linear(5, 7), torch.nn.Linear(7, 2)).cuda()
        clone.load_state_dict(state['model'])
        opt = torch.optim.AdamW(clone.parameters(), lr=.001)
        opt.load_state_dict(state['optimizer'])
        opt.zero_grad(set_to_none=True)
        clone(inputs).square().mean().backward()
        opt.step()
        clones.append((clone, opt))
    for a, b in zip(clones[0][0].parameters(), clones[1][0].parameters(), strict=True):
        torch.testing.assert_close(a, b, rtol=0, atol=0)

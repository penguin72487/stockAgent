from __future__ import annotations

import json
from pathlib import Path

import pytest

from stockagent.data_sync import packed_snapshots as p, remote_return_repack as r, stream_repack as s
from stockagent.data_sync.artifact_maintenance import automatic_dataset_name
from stockagent.data_sync.desync_snapshots import SnapshotError


def fixture(tmp_path, monkeypatch):
    relative = 'markets/suite/complete/training'
    dataset = automatic_dataset_name(relative)
    source, cold, workspace = tmp_path/'source'/'training', tmp_path/'cold', tmp_path/'workspace'
    source.mkdir(parents=True); workspace.mkdir()
    (source/'results.bin').write_bytes(b'completed run content')
    monkeypatch.setattr(p, '_notify_packed_publication', lambda *_a, **_kw:None)
    old = p.publish_packed_snapshot(cold, dataset, source, node_id='test',
                                    metadata={'artifact_relative_root':relative})
    stage = workspace/'migration/stream-source-repack-v1'/dataset/old.manifest_sha256
    result = s.repack(cold, stage, dataset, old.manifest_sha256, node_id='test')
    new = p.resolve_latest_packed(cold, dataset)
    receipt = {'contract':'stockagent.source_local_exact_repack.v1',
               'state':'new_D_head_committed_from_exact_E_source', 'dataset':dataset,
               'old_manifest_sha256':old.manifest_sha256, 'new_manifest_sha256':new.manifest_sha256,
               'new_snapshot_id':new.manifest['snapshot_id'], 'files':1,
               'logical_bytes':len(b'completed run content'), 'old_release_removed':True,
               'source_mode':'D_streamed_to_E_dataset_pack',
               'retirement':{'state':'completed','old_source_member_hashes_checked_while_packing':True}}
    receipt_dir = workspace/'migration/source-local-exact-repack'/dataset
    receipt_dir.mkdir(parents=True)
    receipt_path = receipt_dir/(new.manifest_sha256+'.json')
    receipt_path.write_text(json.dumps(receipt))
    prepared = json.loads((stage/'prepared.json').read_text())
    (stage/'retirement.json').write_text(json.dumps({'contract':'stockagent.source_local_repack_retirement.v1',
        'state':'completed','dataset':dataset,'old_manifest_sha256':old.manifest_sha256,
        'new_manifest_sha256':new.manifest_sha256,'prepared_sha256':prepared['identity_sha256']}))
    wave = {'relative_root':relative, 'dataset':dataset, 'snapshot_id':old.manifest['snapshot_id'],
            'manifest_sha256':old.manifest_sha256}
    old.manifest_path.unlink()
    return cold, workspace, wave, new, stage, receipt_path


def test_exact_retired_wave_can_use_fixed_direct_repack_without_payload_readback(tmp_path, monkeypatch):
    cold, workspace, wave, new, _stage, _receipt = fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(p, 'fetch_packed_snapshot', lambda *_a, **_k:pytest.fail('unexpected restore'))
    resolved = r.resolve(cold, workspace, wave)
    assert resolved.manifest_sha256 == new.manifest_sha256


@pytest.mark.parametrize('change', ['incomplete_retirement','wrong_old_snapshot','missing_object','changed_install'])
def test_exact_repack_rejects_missing_or_conflicting_evidence(tmp_path, monkeypatch, change):
    cold, workspace, wave, new, stage, receipt_path = fixture(tmp_path, monkeypatch)
    if change == 'incomplete_retirement':
        receipt = json.loads(receipt_path.read_text()); receipt['retirement']['state'] = 'pending'
        receipt_path.write_text(json.dumps(receipt))
    elif change == 'wrong_old_snapshot':
        wave['snapshot_id'] = 'some-other-snapshot'
    elif change == 'missing_object':
        (cold/new.manifest['archive']['objects'][0]['relpath']).unlink()
    else:
        install = json.loads((stage/'install.json').read_text()); install['prepared_sha256'] = '0'*64
        (stage/'install.json').write_text(json.dumps(install))
    with pytest.raises((SnapshotError, FileNotFoundError)):
        r.resolve(cold, workspace, wave)


def test_existing_original_is_not_silently_redirected(tmp_path, monkeypatch):
    cold, workspace, wave, _new, _stage, _receipt = fixture(tmp_path, monkeypatch)
    original = cold/'manifests'/wave['dataset']/(wave['snapshot_id']+'.json')
    original.write_text('{}')
    with pytest.raises(SnapshotError, match='not eligible'):
        r.resolve(cold, workspace, wave)

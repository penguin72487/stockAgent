import json

import pytest

from stockagent.data_sync import packed_snapshots as packed, repack_generation_evidence as evidence
from stockagent.data_sync.desync_snapshots import SnapshotError
from scripts import run_source_local_repack_queue as queue
from test_dataset_migration_freshness import _source_hashed_repack


def _source(tmp_path, monkeypatch):
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    source, cold, workspace = tmp_path/'source', tmp_path/'cold', tmp_path/'E'
    source.mkdir()
    (source/'old.txt').write_text('original immutable observation')
    (source/'new.txt').write_text('existing observation')
    first = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test')
    (source/'old.txt').unlink()
    (source/'new.txt').write_text('new observed value')
    fixed = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test',
        require_previous_files_present=True, merge_previous_missing_files=True)
    return source, cold, workspace, first, fixed


def test_completed_physical_repack_is_not_rescheduled_due_to_virtual_source_provenance(tmp_path, monkeypatch):
    _source_path, cold, workspace, first, original = _source(tmp_path, monkeypatch)
    _source_hashed_repack(workspace, cold, 'fixture', original, monkeypatch, private=False)
    current = packed.resolve_latest_packed(cold, 'fixture')
    assert current.manifest['source']['selection']['incremental_base']
    first.manifest_path.unlink()
    original.manifest_path.unlink()
    assert queue.source_layout_confirmed(cold, 'fixture', current.manifest, workspace=workspace)
    stage = workspace/'migration/stream-source-repack-v1/fixture'/original.manifest_sha256
    journal = json.loads((stage/'install.json').read_text())
    journal['prepared_sha256'] = '0'*64
    (stage/'install.json').write_text(json.dumps(journal))
    assert not queue.source_layout_confirmed(cold, 'fixture', current.manifest, workspace=workspace)


def test_exact_chain_survives_retired_intermediate_manifests_and_rejects_broken_edges(tmp_path, monkeypatch):
    _source_path, cold, workspace, _first, original = _source(tmp_path, monkeypatch)
    generations = [original]
    receipts = []
    for _ in range(3):
        _, receipt = _source_hashed_repack(workspace, cold, 'fixture', generations[-1], monkeypatch, private=False)
        receipts.append(receipt)
        generations.append(packed.resolve_latest_packed(cold, 'fixture'))
    for prior in generations[:-1]:
        prior.manifest_path.unlink()
    binding = {'manifest_sha256': original.manifest_sha256, 'snapshot_id': original.manifest['snapshot_id']}
    proof = evidence.chain(cold, workspace, 'fixture', binding, private=False)
    assert proof['contract'] == 'stockagent.refresh_exact_repack_chain_binding.v1'
    assert len(proof['transitions']) == 3
    assert proof['manifest_sha256'] == generations[-1].manifest_sha256
    assert proof['historical_recovery_claimed'] is False
    assert proof['independent_D_readback'] is False
    with pytest.raises(SnapshotError, match='exceeds its bound'):
        evidence.chain(cold, workspace, 'fixture', binding, private=False, limit=2)
    middle = json.loads(receipts[1].read_text())
    middle['private'] = True
    receipts[1].write_text(json.dumps(middle))
    with pytest.raises(SnapshotError, match='publication receipt differs'):
        evidence.chain(cold, workspace, 'fixture', binding, private=False)

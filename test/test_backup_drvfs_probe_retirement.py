"""Only fixed benchmark duplicates may use the probe owner's cleanup."""
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import benchmark_backup_drvfs as probe
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.offhost_backup import export_incremental_delivery
from stockagent.data_sync.packed_backup import signature
from stockagent.data_sync.packed_snapshots import initialize_packed_layout


@pytest.fixture
def scope(tmp_path):
    cold, transport, state = (tmp_path / name for name in ('cold', 'transport', 'state'))
    initialize_packed_layout(cold, node_id='test')
    state.mkdir()
    raw = b'original observation, never a disposable source'
    digest = hashlib.sha256(raw).hexdigest()
    original = cold / 'objects/blobs' / digest[:2] / (digest + '.blob')
    original.parent.mkdir(parents=True, exist_ok=True)
    original.write_bytes(raw)
    row = {'relative': original.relative_to(cold).as_posix(), 'bytes': original.stat().st_size,
           'sha256': hashlib.sha256(original.read_bytes()).hexdigest(),
           'signature': list(signature(original)), 'role': 'packed_object'}
    name = 'drvfs-benchmark-' + 'a' * 32
    stage = transport / '.staging' / name
    closed = export_incremental_delivery(cold, [row], stage, catalog_identity='a' * 64)
    checked = probe.verify(stage, closed['envelope_identity_sha256'])
    receipt = tmp_path / 'trial.json'
    receipt.write_text(json.dumps({'contract': 'source_complete_closed_delivery_drvfs_comparison_v1',
        'probe_files_retained_in_ignored_staging': True, 'trials': [{
            'staging_path': '/srv/stockagent-backup-ingress-lab203/.staging/' + name,
            'envelope_identity_sha256': closed['envelope_identity_sha256'],
            'bytes': checked['bytes_verified'], 'files': checked['files_verified'],
            'closed_envelope_and_READY_verified': True, 'every_file_sha256_verified': True,
            'complete_file_set_verified': True}]}))
    ledger = {'deliveries': {}}
    queue = SimpleNamespace(cold=cold, transport=transport, state=state,
                            storage_guard=lambda: None, load_ledger=lambda: ledger)
    return queue, original, stage, receipt, tmp_path / 'retirement.json', ledger


def test_fresh_dry_run_then_apply_removes_only_exact_probe_copies(scope):
    queue, source, stage, receipt, output, _ = scope
    raw = source.read_bytes()
    dry = probe.retire_probe_copies(queue, [receipt], output)
    assert dry['items'][0]['state'] == 'verified_dry_run' and stage.is_dir()
    done = probe.retire_probe_copies(queue, [receipt], output, apply=True)
    assert done['items'][0]['state'] == 'retired' and not stage.exists()
    assert source.read_bytes() == raw and done['source_files_deleted'] == 0
    assert done['NAS_snapshots_deleted'] == 0


@pytest.mark.parametrize('change', ['source', 'probe', 'unknown', 'shared', 'registered', 'process'])
def test_changed_unique_or_in_use_names_survive(scope, monkeypatch, change):
    queue, source, stage, receipt, output, ledger = scope
    if change == 'source':
        source.write_bytes(b'changed original: keep the old probe')
    elif change == 'probe':
        (stage / 'cold' / source.relative_to(queue.cold)).write_bytes(b'changed probe')
    elif change == 'unknown':
        (stage / 'unknown.txt').write_text('must survive')
    elif change == 'shared':
        import os
        os.link(stage / 'cold' / source.relative_to(queue.cold), stage.parent / 'other-link')
    elif change == 'registered':
        ledger['deliveries']['real'] = {'staging_relative': '.staging/' + stage.name}
    else:
        from stockagent.data_sync import materialized_cache
        monkeypatch.setattr(materialized_cache, 'process_references_many', lambda *a, **kw: ['live FD'])
    with pytest.raises((SnapshotError, ValueError)):
        probe.retire_probe_copies(queue, [receipt], output, apply=True)
    assert stage.is_dir() and source.is_file()


def test_arbitrary_staging_names_cannot_borrow_a_trial_receipt(scope):
    queue, source, stage, receipt, output, _ = scope
    document = json.loads(receipt.read_bytes())
    document['trials'][0]['staging_path'] = '/arbitrary/' + stage.name
    receipt.write_text(json.dumps(document))
    with pytest.raises(SnapshotError, match='frozen benchmark scope'):
        probe.retire_probe_copies(queue, [receipt], output, apply=True)
    assert stage.is_dir() and source.is_file()

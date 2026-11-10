import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from stockagent.data_sync import packed_retention, packed_snapshots, rescue_source_references as refs
from stockagent.data_sync.desync_snapshots import SnapshotError


def reference(dataset='sample-dataset', snapshot='sample-snapshot', sha='a'*64):
    return {'dataset': dataset, 'snapshot_id': snapshot, 'manifest_sha256': sha}


def roots(tmp_path):
    cold, state, materialized = tmp_path/'cold', tmp_path/'state', tmp_path/'materialized'
    for path in (state, materialized, cold/'.local-state/locks'):
        path.mkdir(parents=True)
    (cold/'.local-state/locks/publish-retention-global.lock').touch()
    return cold, state, materialized


def test_missing_rescue_source_is_protected_when_it_reappears_without_blocking_others(tmp_path):
    cold, state, materialized = roots(tmp_path)
    assert refs.protect(cold, state, [reference()], guard=lambda _: None) == [reference()]
    assert (state/refs.REGISTRY_NAME).stat().st_mode & 0o777 == 0o600
    assert (state/refs.MARKER_NAME).stat().st_mode & 0o777 == 0o600
    pins = state/'research-pins'
    protected, _, errors = packed_retention._local_protected_snapshot_ids(
        materialized, {'unrelated'}, research_pin_root=pins)
    assert protected == set() and not errors
    protected, evidence, errors = packed_retention._local_protected_snapshot_ids(
        materialized, {'sample-snapshot'}, research_pin_root=pins)
    assert protected == {'sample-snapshot'} and not errors
    assert evidence == ['sample-snapshot:rescue-source-reference']
    assert refs.protect(cold, state, [reference()], guard=lambda _: None) == [reference()]


def test_registry_is_monotonic_and_rejects_conflicting_snapshot_identity(tmp_path):
    cold, state, _ = roots(tmp_path)
    rows = [reference('beta', 'snapshot-b', 'b'*64), reference('alpha', 'snapshot-a')]
    assert refs.protect(cold, state, rows, guard=lambda _: None) == list(reversed(rows))
    assert refs.protect(cold, state, [rows[0]], guard=lambda _: None) == list(reversed(rows))
    with pytest.raises(SnapshotError, match='conflicting'):
        refs.protect(cold, state, [reference('other', 'snapshot-a', 'c'*64)], guard=lambda _: None)
    assert refs.load(state) == list(reversed(rows))


@pytest.mark.parametrize('damage', ['missing_registry', 'bad_identity', 'bad_marker', 'symlink'])
def test_enabled_registry_damage_never_looks_like_no_pins(tmp_path, damage):
    cold, state, materialized = roots(tmp_path)
    refs.protect(cold, state, [reference()], guard=lambda _: None)
    path, marker = state/refs.REGISTRY_NAME, state/refs.MARKER_NAME
    if damage == 'missing_registry':
        path.unlink()
    elif damage == 'bad_identity':
        value = json.loads(path.read_text()); value['bindings'][0]['manifest_sha256'] = 'b'*64
        path.write_text(json.dumps(value))
    elif damage == 'bad_marker':
        marker.write_text('{}')
    else:
        path.unlink(); path.symlink_to(tmp_path/'elsewhere')
    protected, _, errors = packed_retention._local_protected_snapshot_ids(
        materialized, {'unrelated'}, research_pin_root=state/'research-pins')
    assert protected == set() and errors


def test_reference_commit_yields_to_existing_publication_owner(tmp_path):
    cold, state, _ = roots(tmp_path)
    lock = cold/'.local-state/locks/publish-retention-global.lock'
    with packed_snapshots._try_publication_lock(lock):
        with pytest.raises(packed_snapshots.PackedPublicationBusy):
            refs.protect(cold, state, [reference()], guard=lambda _: None)
    assert not (state/refs.REGISTRY_NAME).exists()


def test_reference_reconciliation_failure_does_not_block_checkpoint_owner(tmp_path):
    from scripts.training_rescue import reconcile_references_without_blocking_capture
    def busy():
        raise BlockingIOError('publication owner busy')
    owner = SimpleNamespace(state=tmp_path, reconcile_source_references=busy)
    reconcile_references_without_blocking_capture(owner)
    status = json.loads((tmp_path/'rescue-source-reference-status.json').read_text())
    assert status['state'] == 'waiting' and status['error_type'] == 'BlockingIOError'
    assert status['source_objects_checked'] is False and status['recovery_or_nas_acceptance_claimed'] is False


def test_capacity_observation_records_original_reserve_without_acknowledging_checkpoint(tmp_path, monkeypatch):
    from scripts.training_rescue import record_cycle_without_blocking_capture
    from scripts import training_rescue as cli
    monkeypatch.setattr(cli.shutil, 'disk_usage', lambda _: SimpleNamespace(free=500))
    owner = SimpleNamespace(state=tmp_path, cold=tmp_path/'cold', policy={'reserve_bytes': 640})
    record_cycle_without_blocking_capture(owner, 'waiting_d_capacity')
    status = json.loads((tmp_path/'rescue-cycle-status.json').read_text())
    assert status['state'] == 'waiting_d_capacity'
    assert (status['D_available_bytes'], status['D_reserve_bytes']) == (500, 640)
    assert status['source_retirement'] is False and status['recovery_or_nas_acceptance_claimed'] is False

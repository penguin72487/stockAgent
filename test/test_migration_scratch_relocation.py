import hashlib
import json
from pathlib import Path

import pytest

from stockagent.data_sync import dataset_migration as migration
from stockagent.data_sync import materialized_cache, workspace_volume
from stockagent.data_sync import migration_scratch_relocation as relocation
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.fixture
def frozen_preparation(tmp_path, monkeypatch):
    operations = tmp_path / 'operations'
    operation = operations / ('a' * 16 + '-' + 'b' * 32)
    stage = operation / 'sqlite-snapshots' / ('sqlite-' + 'c' * 32)
    stage.mkdir(parents=True)
    original = b'closed SQLite source observation, preserved byte for byte'
    digest = hashlib.sha256(original).hexdigest()
    (stage / 'snapshot.sqlite3').write_bytes(original)
    proof = {'snapshot_sha256': digest, 'snapshot_bytes': len(original),
             'source_relative': 'state/example.sqlite3'}
    (stage / 'snapshot.json').write_text(json.dumps(proof))
    (stage / 'backup-intent.json').write_text(json.dumps({
        'contract': 'owned_sqlite_backup_intent_v1',
        'source_relative': proof['source_relative'], 'target': 'snapshot.sqlite3'}))
    preparation = operation / 'preparations' / 'private.json'
    preparation.parent.mkdir()
    preparation.write_text(json.dumps({'contract': migration.CONTRACT, 'dataset': 'private',
        'sqlite_states': [{'source_relative': proof['source_relative'],
            'snapshot_id': 'fixed-original-release', 'consistent_state': proof,
            'retained_snapshot_staging': str(stage)}]}))
    (operation / 'progress.json').write_text(json.dumps({'datasets': {'private': {'state': 'waiting_retry'}}}))
    enrollment = tmp_path / 'e-enrollment.json'
    enrollment.touch()
    workspace = tmp_path / 'e-migration'
    workspace.mkdir()
    monkeypatch.setattr(migration, 'MIGRATION_OPERATIONS', operations)
    monkeypatch.setattr(workspace_volume, 'ENROLLMENT', enrollment)
    monkeypatch.setattr(workspace_volume, 'admit', lambda *_: {})
    monkeypatch.setattr(workspace_volume, 'path', lambda *_a, **_k: workspace)
    monkeypatch.setattr(materialized_cache, 'process_references_many', lambda *_a, **_k: [])
    return operation, stage, preparation, proof, original


def run(case):
    operation, _, _, proof, _ = case
    return relocation.relocate(operation, 'private', proof['source_relative'], proof['snapshot_sha256'])


def test_old_preparation_is_rebound_only_after_exact_e_readback(frozen_preparation):
    operation, stage, preparation, proof, original = frozen_preparation
    result = run(frozen_preparation)
    new_stage = Path(json.loads(preparation.read_bytes())['sqlite_states'][0]['retained_snapshot_staging'])
    assert result['state'] == 'completed'
    assert result['owned_scratch_files_deleted'] == 1 and result['source_files_deleted'] == 0
    assert result['cold_recovery_acceptance'] is result['nas_acceptance'] is False
    assert (new_stage / 'snapshot.sqlite3').read_bytes() == original
    assert not (stage / 'snapshot.sqlite3').exists()
    assert (stage / 'snapshot.json').exists()
    assert run(frozen_preparation) == result


def test_open_reference_waits_without_rebinding_or_deleting(frozen_preparation, monkeypatch):
    _, stage, preparation, _, _ = frozen_preparation
    monkeypatch.setattr(materialized_cache, 'process_references_many', lambda *_: [{'pid': 7}])
    assert run(frozen_preparation)['state'] == 'waiting_process_references'
    assert Path(json.loads(preparation.read_bytes())['sqlite_states'][0]['retained_snapshot_staging']) == stage
    assert (stage / 'snapshot.sqlite3').exists()
    monkeypatch.setattr(materialized_cache, 'process_references_many', lambda *_: [])
    assert run(frozen_preparation)['state'] == 'completed'


def test_corrupt_e_copy_never_rebinds_or_deletes(frozen_preparation, monkeypatch):
    _, stage, preparation, _, _ = frozen_preparation
    real = relocation.copy_owner.copy
    def corrupt(source, destination, *args, **kwargs):
        result = real(source, destination, *args, **kwargs)
        (destination / 'snapshot.sqlite3').write_bytes(b'bad')
        return result
    monkeypatch.setattr(relocation.copy_owner, 'copy', corrupt)
    with pytest.raises(SnapshotError, match='does not match'):
        run(frozen_preparation)
    assert (stage / 'snapshot.sqlite3').exists()
    assert Path(json.loads(preparation.read_bytes())['sqlite_states'][0]['retained_snapshot_staging']) == stage


def test_interrupted_after_binding_resumes_without_losing_the_original(frozen_preparation, monkeypatch):
    _, stage, preparation, _, _ = frozen_preparation
    unlink = Path.unlink
    def interrupt(path, *args, **kwargs):
        if path == stage / 'snapshot.sqlite3':
            raise OSError('interrupted unlink')
        return unlink(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'unlink', interrupt)
    with pytest.raises(OSError, match='interrupted unlink'):
        run(frozen_preparation)
    assert (stage / 'snapshot.sqlite3').exists()
    assert Path(json.loads(preparation.read_bytes())['sqlite_states'][0]['retained_snapshot_staging']) != stage
    monkeypatch.setattr(Path, 'unlink', unlink)
    assert run(frozen_preparation)['state'] == 'completed'


def test_other_fixed_receipt_prevents_rebinding(frozen_preparation):
    operation, stage, _, _, _ = frozen_preparation
    receipt = operation / 'publication' / 'another.json'
    receipt.parent.mkdir()
    receipt.write_text(json.dumps({'retained_snapshot_staging': str(stage)}))
    with pytest.raises(SnapshotError, match='another fixed migration receipt'):
        run(frozen_preparation)
    assert (stage / 'snapshot.sqlite3').exists()


@pytest.fixture
def frozen_controls(frozen_preparation):
    operation, _, preparation, _, _ = frozen_preparation
    stage = operation / 'controls' / ('controls-' + 'd' * 32)
    (stage / 'nested').mkdir(parents=True)
    payloads = {'a.json': b'first fixed control', 'nested/b.bin': b'second fixed control'}
    rows = {}
    for name, payload in payloads.items():
        path = stage / name
        path.write_bytes(payload)
        rows[name] = {'path': str(path), 'bytes': len(payload), 'sha256': hashlib.sha256(payload).hexdigest()}
    stage.with_suffix('.intent.json').write_text(json.dumps({'contract': 'fixed-intent'}))
    document = json.loads(preparation.read_bytes())
    document.update(state='prepared', controls_staging=str(stage), controls=rows)
    preparation.write_text(json.dumps(document))
    return operation, stage, preparation, payloads, relocation.controls_identity(rows)


def run_controls(case):
    operation, _, _, _, identity = case
    return relocation.relocate_controls(operation, 'private', identity)


def test_controls_move_only_owned_exact_copy_and_remain_resumable(frozen_controls):
    _, stage, preparation, payloads, _ = frozen_controls
    result = run_controls(frozen_controls)
    updated = json.loads(preparation.read_bytes())
    target = Path(updated['controls_staging'])
    assert result['state'] == 'completed'
    assert result['files'] == 2 and result['source_files_deleted'] == 0
    assert result['cold_recovery_acceptance'] is result['nas_acceptance'] is False
    for name, payload in payloads.items():
        assert (target / name).read_bytes() == payload
        assert updated['controls'][name]['path'] == str(target / name)
        assert not (stage / name).exists()
    assert target.with_suffix('.intent.json').read_bytes() == stage.with_suffix('.intent.json').read_bytes()
    assert run_controls(frozen_controls) == result


def test_controls_wait_for_open_reference(frozen_controls, monkeypatch):
    _, stage, preparation, _, _ = frozen_controls
    monkeypatch.setattr(materialized_cache, 'process_references_many', lambda *_a, **_k: ['in use'])
    assert run_controls(frozen_controls)['state'] == 'waiting_process_references'
    assert Path(json.loads(preparation.read_bytes())['controls_staging']) == stage
    assert (stage / 'a.json').exists()
    monkeypatch.setattr(materialized_cache, 'process_references_many', lambda *_a, **_k: [])
    assert run_controls(frozen_controls)['state'] == 'completed'


def test_controls_reject_corrupt_e_readback(frozen_controls, monkeypatch):
    _, stage, preparation, _, _ = frozen_controls
    real = relocation.copy_owner.copy
    def corrupt(source, destination, *args, **kwargs):
        result = real(source, destination, *args, **kwargs)
        (destination / 'a.json').write_bytes(b'bad')
        return result
    monkeypatch.setattr(relocation.copy_owner, 'copy', corrupt)
    with pytest.raises(SnapshotError, match='readback or generation'):
        run_controls(frozen_controls)
    assert (stage / 'a.json').exists()
    assert Path(json.loads(preparation.read_bytes())['controls_staging']) == stage


def test_controls_interrupted_unlink_resumes_same_identity(frozen_controls, monkeypatch):
    _, stage, preparation, _, _ = frozen_controls
    unlink = Path.unlink
    def interrupt(path, *args, **kwargs):
        if path == stage / 'nested/b.bin':
            raise OSError('interrupted control cleanup')
        return unlink(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'unlink', interrupt)
    with pytest.raises(OSError, match='interrupted control cleanup'):
        run_controls(frozen_controls)
    assert not (stage / 'a.json').exists()
    assert (stage / 'nested/b.bin').exists()
    assert Path(json.loads(preparation.read_bytes())['controls_staging']) != stage
    monkeypatch.setattr(Path, 'unlink', unlink)
    result = run_controls(frozen_controls)
    assert result['state'] == 'completed'
    assert result['reclaimed_bytes_state'] == 'partial_after_interruption'


def test_controls_reject_unknown_members(frozen_controls):
    _, stage, preparation, _, _ = frozen_controls
    (stage / 'unexpected').write_bytes(b'unknown')
    with pytest.raises(SnapshotError, match='namespace'):
        run_controls(frozen_controls)
    assert (stage / 'a.json').exists()
    assert Path(json.loads(preparation.read_bytes())['controls_staging']) == stage


def test_controls_reject_hardlinked_original(frozen_controls):
    _, stage, preparation, _, _ = frozen_controls
    import os
    os.link(stage / 'a.json', stage.parent / 'other-hardlink')
    with pytest.raises(SnapshotError, match='link changed'):
        run_controls(frozen_controls)
    assert (stage / 'a.json').exists()
    assert Path(json.loads(preparation.read_bytes())['controls_staging']) == stage


def test_controls_reject_changed_fixed_identity(frozen_controls):
    operation, stage, preparation, _, identity = frozen_controls
    doc = json.loads(preparation.read_bytes())
    doc['controls']['a.json']['sha256'] = 'f' * 64
    preparation.write_text(json.dumps(doc))
    with pytest.raises(SnapshotError, match='identity changed'):
        relocation.relocate_controls(operation, 'private', identity)
    assert (stage / 'a.json').exists()


def test_controls_reject_symlink_replacement_on_interrupted_retirement(frozen_controls, monkeypatch):
    _, stage, _, _, _ = frozen_controls
    unlink = Path.unlink
    def interrupt(path, *args, **kwargs):
        if path == stage / 'nested/b.bin':
            raise OSError('interrupted control cleanup')
        return unlink(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'unlink', interrupt)
    with pytest.raises(OSError, match='interrupted control cleanup'):
        run_controls(frozen_controls)
    monkeypatch.setattr(Path, 'unlink', unlink)
    (stage / 'a.json').symlink_to('nonexistent')
    with pytest.raises(SnapshotError, match='redirected'):
        run_controls(frozen_controls)
    assert (stage / 'nested/b.bin').exists()

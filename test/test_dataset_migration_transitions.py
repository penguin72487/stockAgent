"""Per-source cutovers retain exact accepted work and failure evidence."""
import hashlib
import json
from pathlib import Path

import pytest

from scripts import publish_data_releases as publisher
from stockagent.data_sync import dataset_migration as migration
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packed_snapshots import publish_packed_snapshot
from test_dataset_migration import setup, _futures_interfaces


@pytest.fixture
def transition(setup, monkeypatch):
    repo, cold, entries, args, policy = setup
    monkeypatch.setattr(publisher, '_load_catalog', lambda *_: entries)
    configuration = {'cold_root': str(cold), 'state_root': str(args.migration_state.parent / 'control')}
    root = Path(configuration['state_root']) / 'dataset-versions'
    return repo, cold, entries, policy, configuration, root


def state(root):
    current = json.loads((root / 'current.json').read_bytes())
    operation = Path(current['operation_directory'])
    return current, operation, json.loads((operation / 'progress.json').read_bytes())


def test_pending_cutover_preserves_full_accepted_proof_and_never_follows_a_new_head(transition, monkeypatch):
    repo, cold, entries, policy, configuration, root = transition
    migration.advance_catalog_versions(repo, configuration, now=100)
    current, operation, before = state(root)
    row = before['datasets']['public']
    original = {name: (operation / name).read_bytes() for name in ('plan.json', 'progress.json', 'recovery/public.json')}
    # A newer observation/head must not be backdated into the old acceptance.
    source = Path(entries[0]['source'])
    (source / 'raw.txt').write_bytes(b'newer source observation')
    newer = publish_packed_snapshot(cold, 'public', source, node_id='test',
        metadata={'observation': 'newer outside this fixed cycle'},
        file_chunk_bytes=8 * 1024**2)
    assert newer.manifest['snapshot_id'] != row['snapshot_id']
    entries[1] = {**entries[1], 'role': 'new-pending-contract'}
    native = publisher._publish_entry
    calls = []
    def publish(entry, *args, **kwargs):
        calls.append(entry['dataset'])
        assert entry['dataset'] != 'public', 'accepted source must not be republished'
        return native(entry, *args, **kwargs)
    monkeypatch.setattr(publisher, '_publish_entry', publish)
    result = migration.advance_catalog_versions(repo, configuration, now=200)
    revised, destination, after = state(root)
    accepted = after['datasets']['public']
    assert result['state'] == 'verified' and result['verified_datasets'] == 2
    assert calls == []  # the other source is private, through its original private owner
    assert revised['plan_sha256'] != current['plan_sha256'] and destination != operation
    assert revised['adopted_verified_sources'] == ['public']
    for key in ('snapshot_id', 'manifest_sha256', 'verified_at_utc', 'attempts', 'logical_bytes', 'source_files'):
        assert accepted[key] == row[key]
    assert Path(accepted['recovery_receipt']).read_bytes() == original['recovery/public.json']
    assert accepted['adopted_verification']['receipt_sha256'] == hashlib.sha256(original['recovery/public.json']).hexdigest()
    assert {name: (operation / name).read_bytes() for name in original} == original
    assert (destination / 'previous-progress.json').read_bytes() == original['progress.json']
    receipt = json.loads((destination / 'transition.json').read_bytes())
    assert receipt['changed_sources'] == ['private']
    assert not receipt['NAS_acceptance'] and not receipt['new_source_payload_verification']


def test_adding_source_retains_fixed_accepted_proof_and_existing_pending_work(transition):
    repo, _, entries, _, configuration, root = transition
    migration.advance_catalog_versions(repo, configuration, now=100)
    current, operation, before = state(root)
    accepted = before['datasets']['public']
    original_proof = (operation / 'recovery/public.json').read_bytes()
    extra = repo / 'extra'
    extra.mkdir()
    (extra / 'raw.txt').write_text('new private source')
    entries.append({**entries[1], 'dataset': 'extra', 'source': str(extra)})
    migration.advance_catalog_versions(repo, configuration, now=200)
    revised, destination, after = state(root)
    assert destination != operation and revised['plan_sha256'] != current['plan_sha256']
    assert revised['adopted_verified_sources'] == ['public']
    assert after['datasets']['public']['snapshot_id'] == accepted['snapshot_id']
    assert Path(after['datasets']['public']['recovery_receipt']).read_bytes() == original_proof
    assert after['datasets']['private']['state'] == 'verified'
    assert after['datasets']['extra']['state'] == 'pending'
    receipt = json.loads((destination / 'transition.json').read_bytes())
    assert receipt['added_sources'] == ['extra'] and receipt['changed_sources'] == []
    result = migration.advance_catalog_versions(repo, configuration, now=300)
    assert result['state'] == 'verified' and result['source_count'] == 3


def test_added_source_waits_for_unfinished_original_fixed_recovery(transition, monkeypatch):
    repo, _, entries, _, configuration, root = transition
    native = migration._recover
    monkeypatch.setattr(migration, '_recover', lambda *_a, **_k:
        (_ for _ in ()).throw(SnapshotError('interrupted original recovery')))
    migration.advance_catalog_versions(repo, configuration, now=100)
    original, operation, before = state(root)
    before['datasets']['public']['retry_at_unix'] = 0
    (operation / 'progress.json').write_text(json.dumps(before))
    extra = repo / 'extra'
    extra.mkdir()
    (extra / 'raw.txt').write_text('new private source')
    entries.append({**entries[1], 'dataset': 'extra', 'source': str(extra)})
    monkeypatch.setattr(migration, '_recover', native)
    migration.advance_catalog_versions(repo, configuration, now=200)
    resumed, same, after = state(root)
    assert same == operation and resumed['plan_sha256'] == original['plan_sha256']
    assert resumed['deferred_catalog_plan_sha256'] != original['plan_sha256']
    assert after['datasets']['public']['state'] == 'verified'
    assert after['datasets']['private']['attempts'] == 0 and 'extra' not in after['datasets']
    migration.advance_catalog_versions(repo, configuration, now=300)
    revised, destination, after = state(root)
    assert destination != operation and revised['adopted_verified_sources'] == ['public']
    assert after['datasets']['public']['snapshot_id'] == before['datasets']['public']['snapshot_id']
    assert after['datasets']['extra']['state'] == 'pending'


def test_a_committed_unfinished_release_finishes_before_the_pending_cutover(transition, monkeypatch):
    repo, _, entries, _, configuration, root = transition
    native = migration._recover
    monkeypatch.setattr(migration, '_recover', lambda *_a, **_k: (_ for _ in ()).throw(SnapshotError('interrupted original recovery')))
    migration.advance_catalog_versions(repo, configuration, now=100)
    original, operation, before = state(root)
    assert before['datasets']['public']['snapshot_id']
    before['datasets']['public']['retry_at_unix'] = 0
    (operation / 'progress.json').write_text(json.dumps(before))
    entries[1] = {**entries[1], 'role': 'new-pending-contract'}
    monkeypatch.setattr(migration, '_recover', native)
    result = migration.advance_catalog_versions(repo, configuration, now=200)
    resumed, same, during = state(root)
    assert same == operation and resumed['plan_sha256'] == original['plan_sha256']
    assert during['datasets']['public']['state'] == 'verified'
    assert during['datasets']['private']['attempts'] == 0
    assert result['verified_datasets'] == 1
    last = migration.advance_catalog_versions(repo, configuration, now=300)
    revised, destination, after = state(root)
    assert last['state'] == 'verified' and destination != operation
    assert after['datasets']['public']['snapshot_id'] == before['datasets']['public']['snapshot_id']
    assert revised['adopted_verified_sources'] == ['public']


def test_original_fixed_recovery_cooldown_never_starts_a_superseded_pending_contract(transition, monkeypatch):
    repo, _, entries, _, configuration, root = transition
    monkeypatch.setattr(migration, '_recover', lambda *_a, **_k: (_ for _ in ()).throw(SnapshotError('original recovery retry')))
    migration.advance_catalog_versions(repo, configuration, now=100)
    _, operation, progress = state(root)
    progress['datasets']['public']['retry_at_unix'] = migration.time.time() + 600
    (operation / 'progress.json').write_text(json.dumps(progress))
    entries[1] = {**entries[1], 'role': 'new-pending-contract'}
    result = migration.advance_catalog_versions(repo, configuration, now=200)
    _, same, after = state(root)
    assert same == operation and after['datasets']['private']['attempts'] == 0
    assert result['next_poll_seconds'] == json.loads((repo / 'configs/columnar_storage.json').read_text())['dataset_version_lifecycle']['busy_retry_seconds']


@pytest.mark.parametrize('damage', ['incomplete', 'count', 'bytes', 'manifest', 'snapshot', 'redirect', 'receipt-path'])
def test_bad_previous_acceptance_never_replaces_the_current_pointer_or_republishes(transition, monkeypatch, damage):
    repo, _, entries, _, configuration, root = transition
    migration.advance_catalog_versions(repo, configuration, now=100)
    current, operation, progress = state(root)
    pointer = (root / 'current.json').read_bytes()
    path = operation / 'recovery/public.json'
    proof = json.loads(path.read_bytes())
    if damage == 'receipt-path':
        progress['datasets']['public']['recovery_receipt'] = str(operation / 'unexpected.json')
        (operation / 'progress.json').write_text(json.dumps(progress))
    elif damage == 'redirect':
        saved = path.with_suffix('.original')
        path.rename(saved)
        path.symlink_to(saved)
    else:
        field, value = {'incomplete': ('full_original_file_reconstruction_verified', False),
            'count': ('independently_reconstructed_files', 0),
            'bytes': ('independently_reconstructed_bytes', 0),
            'manifest': ('manifest_sha256', 'f' * 64),
            'snapshot': ('snapshot_id', 'unrelated') }[damage]
        proof[field] = value
        path.write_text(json.dumps(proof))
    entries[1] = {**entries[1], 'role': 'new-pending-contract'}
    monkeypatch.setattr(migration, 'migrate_catalog', lambda *_a, **_k: pytest.fail('bad evidence must stop before source work'))
    with pytest.raises(SnapshotError):
        migration.advance_catalog_versions(repo, configuration, now=200)
    assert (root / 'current.json').read_bytes() == pointer
    assert (Path(entries[0]['source']) / 'raw.txt').read_text() == 'public'


def test_interrupted_metadata_copy_preserves_original_plan_and_resumes_without_source_replay(transition, monkeypatch):
    repo, _, entries, _, configuration, root = transition
    migration.advance_catalog_versions(repo, configuration, now=100)
    _, operation, _ = state(root)
    pointer = (root / 'current.json').read_bytes()
    original = {name: (operation / name).read_bytes() for name in ('plan.json', 'progress.json', 'recovery/public.json')}
    entries[1] = {**entries[1], 'role': 'new-pending-contract'}
    native = migration.atomic_write_bytes
    def failed(path, raw, **kwargs):
        if path.name == 'public.json' and path.parent.parent != operation:
            raise OSError('interruption before replacement current pointer')
        return native(path, raw, **kwargs)
    monkeypatch.setattr(migration, 'atomic_write_bytes', failed)
    with pytest.raises(OSError, match='interruption'):
        migration.advance_catalog_versions(repo, configuration, now=200)
    assert (root / 'current.json').read_bytes() == pointer
    assert {name: (operation / name).read_bytes() for name in original} == original
    monkeypatch.setattr(migration, 'atomic_write_bytes', native)
    assert migration.advance_catalog_versions(repo, configuration, now=300)['state'] == 'verified'
    _, _, progress = state(root)
    assert progress['datasets']['public']['attempts'] == 1


def test_busy_original_operation_never_adopts_or_replaces_its_current_pointer(transition):
    repo, _, entries, _, configuration, root = transition
    migration.advance_catalog_versions(repo, configuration, now=100)
    _, operation, _ = state(root)
    pointer = (root / 'current.json').read_bytes()
    entries[1] = {**entries[1], 'role': 'new-pending-contract'}
    with migration._operation_owner(operation):
        with pytest.raises(migration.PackedPublicationBusy):
            migration.advance_catalog_versions(repo, configuration, now=200)
    assert (root / 'current.json').read_bytes() == pointer
    assert migration.advance_catalog_versions(repo, configuration, now=300)['state'] == 'verified'


def test_unchanged_retry_budget_deadline_and_failure_survive_another_pending_cutover(transition, monkeypatch):
    repo, _, entries, _, configuration, root = transition
    third = repo / 'third'
    third.mkdir()
    (third / 'raw.txt').write_text('third')
    entries.append({**entries[1], 'dataset': 'third', 'source': str(third)})
    migration.advance_catalog_versions(repo, configuration, now=100)
    native = migration._private_publish
    monkeypatch.setattr(migration, '_private_publish', lambda *_a, **_k:
                        (_ for _ in ()).throw(SnapshotError('recorded retry failure')))
    migration.advance_catalog_versions(repo, configuration, now=200)
    _, _, before = state(root)
    retry = before['datasets']['private']
    assert retry['attempts'] == 1 and retry['state'] == 'waiting_retry'
    entries[2] = {**entries[2], 'role': 'new-pending-contract'}
    monkeypatch.setattr(migration, '_private_publish', native)
    migration.advance_catalog_versions(repo, configuration, now=300)
    _, _, after = state(root)
    assert after['datasets']['private'] == retry
    assert after['datasets']['third']['state'] == 'verified'


def test_private_sqlite_side_release_proofs_are_preserved_byte_for_byte(transition):
    import sqlite3
    repo, _, entries, _, configuration, root = transition
    third = repo / 'third'
    third.mkdir()
    (third / 'raw.txt').write_text('third')
    entries.append({**entries[1], 'dataset': 'third', 'source': str(third)})
    with sqlite3.connect(Path(entries[1]['source']) / 'source.sqlite3') as connection:
        connection.execute('CREATE TABLE original (value INTEGER)')
        connection.execute('INSERT INTO original VALUES (-7)')
    migration.advance_catalog_versions(repo, configuration, now=100)
    migration.advance_catalog_versions(repo, configuration, now=200)
    _, operation, before = state(root)
    row = before['datasets']['private']
    assert row['state'] == 'verified' and row['publication']['sqlite_states']
    side = {database['dataset'] + '.json':
        (operation / 'recovery' / (database['dataset'] + '.json')).read_bytes()
        for database in row['publication']['sqlite_states']}
    assert side
    entries[2] = {**entries[2], 'role': 'new-pending-contract'}
    result = migration.advance_catalog_versions(repo, configuration, now=300)
    _, destination, after = state(root)
    assert result['state'] == 'verified'
    directory = Path(after['datasets']['private']['recovery_receipt']).parent
    assert {name: (directory / name).read_bytes() for name in side} == side
    assert after['datasets']['private']['snapshot_id'] == row['snapshot_id']


def test_changed_accepted_source_is_reverified_without_extending_the_original_daily_deadline(transition):
    repo, _, entries, _, configuration, root = transition
    migration.advance_catalog_versions(repo, configuration, now=100)
    migration.advance_catalog_versions(repo, configuration, now=200)
    current, operation, before = state(root)
    original = (operation / 'progress.json').read_bytes()
    (Path(entries[1]['source']) / 'raw.txt').write_text('fresh changed source observation')
    entries[1] = {**entries[1], 'role': 'revised-accepted-contract'}
    result = migration.advance_catalog_versions(repo, configuration, now=300)
    revised, _, after = state(root)
    assert result['state'] == 'verified'
    assert revised['adopted_verified_sources'] == ['public']
    assert revised['next_cycle_unix'] == current['next_cycle_unix']
    assert after['datasets']['public']['snapshot_id'] == before['datasets']['public']['snapshot_id']
    assert after['datasets']['private']['snapshot_id'] != before['datasets']['private']['snapshot_id']
    assert after['datasets']['private']['attempts'] == 1
    assert (operation / 'progress.json').read_bytes() == original
    # Another same-day revision preserves the first day's deadline and can
    # re-adopt the prior exact receipt from its bounded dependency namespace.
    entries[1] = {**entries[1], 'role': 'second-revised-accepted-contract'}
    migration.advance_catalog_versions(repo, configuration, now=400)
    again, _, final = state(root)
    assert again['next_cycle_unix'] == current['next_cycle_unix']
    assert final['datasets']['public']['verified_at_utc'] == before['datasets']['public']['verified_at_utc']
    migration.advance_catalog_versions(repo, configuration, now=current['next_cycle_unix'])
    following, _, _ = state(root)
    assert 'adopted_verified_sources' not in following


def test_an_attempted_changed_contract_preserves_the_original_failure_journal(transition, monkeypatch):
    repo, _, entries, _, configuration, root = transition
    native = publisher._publish_entry
    monkeypatch.setattr(publisher, '_publish_entry', lambda *_a, **_k:
                        (_ for _ in ()).throw(SnapshotError('old contract failure')))
    migration.advance_catalog_versions(repo, configuration, now=100)
    _, operation, before = state(root)
    assert before['datasets']['public']['attempts'] == 1
    original = (operation / 'progress.json').read_bytes()
    entries[0] = {**entries[0], 'role': 'corrected-source-contract'}
    monkeypatch.setattr(publisher, '_publish_entry', native)
    migration.advance_catalog_versions(repo, configuration, now=200)
    _, destination, after = state(root)
    assert after['datasets']['public']['state'] == 'verified'
    assert (operation / 'progress.json').read_bytes() == original
    assert (destination / 'previous-progress.json').read_bytes() == original


@pytest.fixture
def linked_transition(transition, setup):
    repo, cold, entries, policy, configuration, root = transition
    _, _, linked, _, _ = _futures_interfaces(setup)
    third = repo / 'third'
    third.mkdir()
    (third / 'raw.txt').write_text('third')
    entries[:] = [*linked, {**entries[1], 'dataset': 'third', 'source': str(third)}]
    migration.advance_catalog_versions(repo, configuration, now=100)
    migration.advance_catalog_versions(repo, configuration, now=200)
    return repo, cold, entries, policy, configuration, root


def test_fixed_source_link_closure_survives_a_changed_dependency_and_multiple_cutovers(linked_transition):
    repo, _, entries, _, configuration, root = linked_transition
    _, operation, before = state(root)
    parent = before['datasets']['local-tw-futures']
    assert parent['state'] == 'verified'
    proof_names = ('local-tw-futures.json', 'local-tw-futures.source-links.json',
                   'local-tw-index-futures.json')
    original = {name: (operation / 'recovery' / name).read_bytes() for name in proof_names}
    (repo / 'data_tw_index_futures/preparation_sources/margin_sources/new.csv').write_text('new later observation')
    entries[0] = {**entries[0], 'role': 'changed-dependency-contract'}
    migration.advance_catalog_versions(repo, configuration, now=300)
    _, destination, after = state(root)
    accepted = after['datasets']['local-tw-futures']
    directory = Path(accepted['recovery_receipt']).parent
    assert {name: (directory / name).read_bytes() for name in proof_names} == original
    assert Path(accepted['source_links_recovery_receipt']).parent == directory
    current_dependency = after['datasets']['local-tw-index-futures']
    old_dependency = json.loads(original['local-tw-index-futures.json'])
    assert current_dependency['snapshot_id'] != old_dependency['snapshot_id']
    assert Path(current_dependency['recovery_receipt']).parent != directory
    entries[2] = {**entries[2], 'role': 'third-source-contract'}
    migration.advance_catalog_versions(repo, configuration, now=400)
    _, _, again = state(root)
    directory = Path(again['datasets']['local-tw-futures']['recovery_receipt']).parent
    assert {name: (directory / name).read_bytes() for name in proof_names} == original


@pytest.mark.parametrize('damage', ['dependency-count', 'dependency-manifest', 'closure', 'closure-path'])
def test_incomplete_dependency_closure_never_adopts_the_previous_acceptance(linked_transition, damage):
    repo, _, entries, _, configuration, root = linked_transition
    _, operation, before = state(root)
    pointer = (root / 'current.json').read_bytes()
    if damage == 'closure-path':
        before['datasets']['local-tw-futures']['source_links_recovery_receipt'] = str(operation / 'outside.json')
        (operation / 'progress.json').write_text(json.dumps(before))
    else:
        name = 'local-tw-futures.source-links.json' if damage == 'closure' else 'local-tw-index-futures.json'
        path = operation / 'recovery' / name
        document = json.loads(path.read_bytes())
        field, value = {'dependency-count': ('independently_reconstructed_files', 0),
                        'dependency-manifest': ('manifest_sha256', 'f' * 64),
                        'closure': ('all_fixed_dependencies_recovered', False)}[damage]
        document[field] = value
        path.write_text(json.dumps(document))
    entries[2] = {**entries[2], 'role': 'new-third-contract'}
    with pytest.raises(SnapshotError):
        migration.advance_catalog_versions(repo, configuration, now=300)
    assert (root / 'current.json').read_bytes() == pointer

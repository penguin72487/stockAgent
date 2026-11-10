"""The canonical owner can refresh each whole data root while recovery is deferred."""
import json
from pathlib import Path

import pytest

from scripts import publish_data_releases as publisher
from stockagent.data_sync import dataset_migration as migration
from stockagent.data_sync.dataset_version_layout import version_policy
from stockagent.data_sync.packed_snapshots import fetch_packed_snapshot, publish_packed_snapshot, resolve_latest_packed
from test_dataset_migration import setup


@pytest.fixture
def refresh_setup(setup, monkeypatch):
    repo, cold, entries, args, _ = setup
    document_path = repo / 'configs/columnar_storage.json'
    document = json.loads(document_path.read_text())
    document['dataset_version_lifecycle']['whole_root_refresh_interval_seconds'] = 3600
    document['dataset_version_lifecycle']['pending_source_priority'] = []
    document['source_versioning']['roots'] = {}
    for entry in entries:
        old = Path(entry['source'])
        new = repo / ('data_' + entry['dataset'])
        old.rename(new)
        entry['source'] = str(new)
        document['source_versioning']['roots'][new.name] = {'native_layout': 'raw', 'target_grain': 'day'}
    document_path.write_text(json.dumps(document))
    monkeypatch.setattr(publisher, '_load_catalog', lambda *_: entries)
    clock = {'epoch': 100}
    monkeypatch.setattr(migration, '_now', lambda: migration.datetime.fromtimestamp(
        clock['epoch'], migration.UTC).isoformat())
    config = {'cold_root': str(cold), 'state_root': str(args.migration_state.parent/'control')}
    state = Path(config['state_root'])
    state.mkdir(parents=True)
    pause = state/'dataset-recovery-migration-pause.json'
    pause.write_text(json.dumps({'contract': migration.RECOVERY_PAUSE_CONTRACT, 'paused': True}))
    pause.chmod(0o600)
    return repo, cold, entries, config, state, clock


def _run(fixture, clock):
    repo, _, _, config, _, epoch = fixture
    epoch['epoch'] = clock
    return migration.advance_catalog_versions(repo, config, now=clock)


def test_calendar_merge_holds_evening_delta_then_merges_once_per_day(refresh_setup, monkeypatch):
    from datetime import datetime
    f = refresh_setup
    path=f[0]/'configs/columnar_storage.json'
    settings=json.loads(path.read_text())
    settings['dataset_version_lifecycle']['daily_merge_window']={
        'timezone':'Asia/Taipei','start_hour':3,'end_hour':7}
    path.write_text(json.dumps(settings))
    stamp=lambda value: datetime.fromisoformat(value+'+08:00').timestamp()
    evening=stamp('2026-11-10T20:00:00')
    _run(f,evening); _run(f,evening+1)  # initial baselines can migrate at any hour
    original=resolve_latest_packed(f[1],'public')
    (f[0]/'data_public/raw.txt').write_text('evening incremental observation')
    _run(f,evening+3600)
    assert resolve_latest_packed(f[1],'public').manifest_sha256==original.manifest_sha256
    morning=stamp('2026-11-11T03:00:00')
    _run(f,morning); _run(f,morning+1)
    current=resolve_latest_packed(f[1],'public')
    assert current.manifest_sha256!=original.manifest_sha256
    monkeypatch.setattr(publisher,'_publish_entry',lambda *_a,**_k:pytest.fail('same-day merge repeated'))
    _run(f,morning+60)


def test_fresh_source_is_published_without_waiting_for_old_recovery(refresh_setup, tmp_path):
    f = refresh_setup
    _run(f, 100)
    _run(f, 200)
    original = resolve_latest_packed(f[1], 'public')
    (f[0]/'data_public'/'raw.txt').write_text('new raw source')
    result = _run(f, 3700)
    updated = resolve_latest_packed(f[1], 'public')
    assert updated.manifest['snapshot_id'] != original.manifest['snapshot_id']
    output = tmp_path/'out'
    restored = fetch_packed_snapshot(f[1], output, updated)
    assert (restored/'raw.txt').read_text() == 'new raw source'
    current = json.loads((f[4]/'dataset-versions/current.json').read_text())
    op = Path(current['operation_directory'])
    row = json.loads((op/'progress.json').read_text())['datasets']['public']
    assert row['state'] == 'recovery_deferred_user_migration'
    assert row['freshness_refresh']['source_freshness_checked'] is True
    prior = json.loads(Path(row['freshness_refresh']['previous_receipt']).read_text())
    assert prior['previous']['snapshot_id'] == original.manifest['snapshot_id']
    assert result['state'] == 'processing'  # no independent recovery was claimed


def test_busy_root_does_not_block_another_due_root(refresh_setup, monkeypatch):
    f = refresh_setup
    _run(f, 100)
    _run(f, 200)
    original = publisher._publish_entry
    def selective(entry, *args):
        if entry['dataset'] == 'public':
            raise OSError('temporary source owner problem')
        return original(entry, *args)
    monkeypatch.setattr(publisher, '_publish_entry', selective)
    _run(f, 3800)
    before = resolve_latest_packed(migration.private_history_root(f[1], version_policy(f[0])), 'private')
    (f[0]/'data_private'/'raw.txt').write_text('new private source')
    _run(f, 3801)
    after = resolve_latest_packed(migration.private_history_root(f[1], version_policy(f[0])), 'private')
    assert after.manifest['snapshot_id'] != before.manifest['snapshot_id']
    current = json.loads((f[4]/'dataset-versions/current.json').read_text())
    rows = json.loads((Path(current['operation_directory'])/'progress.json').read_text())['datasets']
    assert rows['public']['state'] == 'waiting_retry'
    assert rows['private']['state'] == 'recovery_deferred_user_migration'


def test_ambiguous_root_is_not_guessed(refresh_setup):
    repo, _, entries, _, state, _ = refresh_setup
    policy = version_policy(repo)
    duplicate = {**entries[0], 'dataset': 'second-owner'}
    rows = {'public': {'state': 'verified', 'snapshot_id': 'one', 'manifest_sha256': 'a'*64},
            'second-owner': {'state': 'verified', 'snapshot_id': 'two', 'manifest_sha256': 'b'*64},
            'private': {'state': 'verified', 'snapshot_id': 'three', 'manifest_sha256': 'c'*64}}
    choices, ambiguous, closed = migration._whole_root_refresh_schedule(
        repo, [*entries, duplicate], policy, rows, observed=10000, interval=3600)
    assert ambiguous == 1
    assert closed == 0
    assert [choice[1] for choice in choices] == ['private']


def test_explicitly_closed_historical_root_is_counted_without_daily_rescans(refresh_setup):
    repo, _, entries, _, _, _ = refresh_setup
    policy = version_policy(repo)
    catalog = [{**entries[0], 'closed_source_contract': 'fixture_closed_history'}, entries[1]]
    rows = {'public': {'state': 'verified', 'snapshot_id': 'one', 'manifest_sha256': 'a'*64},
            'private': {'state': 'verified', 'snapshot_id': 'two', 'manifest_sha256': 'b'*64}}
    choices, ambiguous, closed = migration._whole_root_refresh_schedule(
        repo, catalog, policy, rows, observed=10000, interval=3600)
    assert ambiguous == 0 and closed == 1
    assert [choice[1] for choice in choices] == ['private']


def test_no_source_read_before_its_daily_deadline(refresh_setup, monkeypatch):
    f = refresh_setup
    _run(f, 100)
    _run(f, 200)
    monkeypatch.setattr(publisher, '_publish_entry', lambda *args: pytest.fail('source reread too soon'))
    result = _run(f, 3699)
    assert result['state'] == 'processing'
    current = json.loads((f[4]/'dataset-versions/current.json').read_text())
    rows = json.loads((Path(current['operation_directory'])/'progress.json').read_text())['datasets']
    assert all(row['state'] == 'recovery_deferred_user_migration' for row in rows.values())


def test_trading_window_defers_a_due_and_interrupted_refresh(refresh_setup):
    f = refresh_setup
    _run(f, 100)
    _run(f, 200)
    current = json.loads((f[4]/'dataset-versions/current.json').read_text())
    original = Path(current['operation_directory'])
    args = migration.SimpleNamespace(sync_root=f[1], migration_state=original, node_id=None,
        retry_seconds=60, maximum_passes=1, watch=False, maximum_completed_sources=1,
        maximum_attempted_sources=1, retire_staging=True, recovery_pause_path=f[4]/
        'dataset-recovery-migration-pause.json', pending_source_priority=[], source_lock_fd=None,
        whole_root_refresh_interval_seconds=3600, whole_root_refresh_allowed=False,
        whole_root_refresh_observed_epoch=3700)
    result = migration.migrate_catalog(f[0], args, f[2])
    assert result['whole_root_refresh']['due_roots'] == 1
    assert result['whole_root_refresh']['window_allowed'] is False
    assert all(row['state'] == 'recovery_deferred_user_migration' for row in result['datasets'].values())


def test_refresh_yields_when_trading_priority_window_begins(refresh_setup):
    f = refresh_setup
    _run(f, 100)
    _run(f, 200)
    current = json.loads((f[4]/'dataset-versions/current.json').read_text())
    operation = Path(current['operation_directory'])
    original = resolve_latest_packed(f[1], 'public')
    (f[0]/'data_public'/'raw.txt').write_text('new raw source')
    args = migration.SimpleNamespace(sync_root=f[1], migration_state=operation, node_id=None,
        retry_seconds=60, maximum_passes=1, watch=False, maximum_completed_sources=1,
        maximum_attempted_sources=1, retire_staging=True, recovery_pause_path=f[4]/
        'dataset-recovery-migration-pause.json', pending_source_priority=[], source_lock_fd=None,
        whole_root_refresh_interval_seconds=3600, whole_root_refresh_allowed=True,
        whole_root_refresh_stop_requested=lambda: True, whole_root_refresh_observed_epoch=3700)
    result = migration.migrate_catalog(f[0], args, f[2])
    row = result['datasets']['public']
    assert row['state'] == 'waiting_retry'
    assert 'yielded to the TW trading priority window' in row['error']
    assert resolve_latest_packed(f[1], 'public').manifest_sha256 == original.manifest_sha256


def test_partial_workspace_never_replaces_complete_cold_head(refresh_setup, monkeypatch):
    f = refresh_setup
    extra = f[0]/'data_public'/'old-part.txt'
    extra.write_text('older original')
    _run(f, 100)
    _run(f, 200)
    original = resolve_latest_packed(f[1], 'public')
    extra.unlink()  # A previous archive may be the only remaining copy.
    (f[0]/'data_public'/'raw.txt').write_text('new data')
    _run(f, 3700)
    assert resolve_latest_packed(f[1], 'public').manifest_sha256 == original.manifest_sha256
    current = json.loads((f[4]/'dataset-versions/current.json').read_text())
    row = json.loads((Path(current['operation_directory'])/'progress.json').read_text())['datasets']['public']
    assert row['state'] == 'waiting_retry'
    assert 'incremental workspace omits previously published files' in row['error']
    assert row['freshness_refresh']['source_freshness_checked'] is False
    assert row['blocked_incremental_paths'] == [{'path': 'old-part.txt', 'kind': 'file'}]
    # A missing item is checked cheaply, not by continuously rebuilding the
    # entire old source inventory, and other sources may still make progress.
    calls = []
    original_publish = publisher._publish_entry
    def observe(entry, *args):
        calls.append(entry['dataset'])
        return original_publish(entry, *args)
    monkeypatch.setattr(publisher, '_publish_entry', observe)
    progress_path = Path(current['operation_directory'])/'progress.json'
    state = json.loads(progress_path.read_text())
    state['datasets']['public']['retry_at_unix'] = 0
    progress_path.write_text(json.dumps(state))
    _run(f, 3701)
    assert 'public' not in calls
    assert resolve_latest_packed(f[1], 'public').manifest_sha256 == original.manifest_sha256

    # The hint is only a wakeup: the publisher still checks the entire source.
    extra.write_text('older original')
    state = json.loads(progress_path.read_text())
    state['datasets']['public']['retry_at_unix'] = 0
    progress_path.write_text(json.dumps(state))
    _run(f, 3702)
    assert 'public' in calls
    assert resolve_latest_packed(f[1], 'public').manifest_sha256 != original.manifest_sha256


def test_audited_append_only_root_can_publish_its_partial_workspace(refresh_setup, tmp_path):
    f = refresh_setup
    config_path = f[0]/'configs/columnar_storage.json'
    document = json.loads(config_path.read_text())
    document['source_versioning']['roots']['data_public']['incremental_base_union'] = 'immutable_path_union_v1'
    config_path.write_text(json.dumps(document))
    (f[0]/'data_public'/'old-part.txt').write_text('immutable earlier part')
    _run(f, 100)
    _run(f, 200)
    original = resolve_latest_packed(f[1], 'public')
    (f[0]/'data_public'/'old-part.txt').unlink()
    (f[0]/'data_public'/'raw.txt').write_text('new current part')
    _run(f, 3700)
    merged = resolve_latest_packed(f[1], 'public')
    assert merged.manifest_sha256 != original.manifest_sha256
    assert merged.manifest['source']['selection']['incremental_base']['manifest_sha256'] == original.manifest_sha256
    recovered = fetch_packed_snapshot(f[1], tmp_path/'audited-restore', merged)
    assert (recovered/'old-part.txt').read_text() == 'immutable earlier part'
    assert (recovered/'raw.txt').read_text() == 'new current part'


def test_missing_original_head_cannot_turn_a_delta_into_a_new_baseline(refresh_setup):
    f = refresh_setup
    _run(f, 100)
    _run(f, 200)
    for head in (f[1]/'heads'/'public').glob('*.json'):
        head.unlink()
    (f[0]/'data_public'/'raw.txt').write_text('partial delta')
    _run(f, 3700)
    current = json.loads((f[4]/'dataset-versions/current.json').read_text())
    row = json.loads((Path(current['operation_directory'])/'progress.json').read_text())['datasets']['public']
    assert row['state'] == 'waiting_retry'
    assert 'incremental base head is missing' in row['error']
    assert not list((f[1]/'heads'/'public').glob('*.json'))


def test_qualified_successor_advances_base_without_claiming_old_history(refresh_setup, monkeypatch):
    f = refresh_setup
    _run(f, 100)
    _run(f, 200)
    original = resolve_latest_packed(f[1], 'public')
    (f[0]/'data_public'/'raw.txt').write_text('intermediate current head')
    successor = publish_packed_snapshot(f[1], 'public', f[0]/'data_public')
    proof = {'contract': 'stockagent.refresh_latest_successor_binding.v1',
        'snapshot_id': successor.manifest['snapshot_id'], 'manifest_sha256': successor.manifest_sha256,
        'previous_snapshot_id': original.manifest['snapshot_id'], 'previous_manifest_sha256': original.manifest_sha256,
        'changed_member_values_compared': False, 'old_historical_value_coverage': 'not_compared',
        'uncompared_changed_members': 1, 'historical_recovery_claimed': False}
    monkeypatch.setattr(migration, '_retired_refresh_successor',
                        lambda cold, entry, prior: proof if entry['dataset'] == 'public' else None)
    (f[0]/'data_public'/'raw.txt').write_text('later physical source')
    _run(f, 3700)
    current = json.loads((f[4]/'dataset-versions/current.json').read_text())
    row = json.loads((Path(current['operation_directory'])/'progress.json').read_text())['datasets']['public']
    assert row['freshness_refresh']['base_rebinding'] == proof
    assert row['freshness_refresh']['source_freshness_checked'] is True
    assert row['freshness_refresh']['previous_manifest_sha256'] == successor.manifest_sha256
    assert resolve_latest_packed(f[1], 'public').manifest_sha256 != successor.manifest_sha256


def _source_hashed_repack(workspace, cold, dataset, original, monkeypatch, *, private):
    from stockagent.data_sync import stream_repack
    stage = workspace/'migration/stream-source-repack-v1'/dataset/original.manifest_sha256
    monkeypatch.setattr(stream_repack, '_notify_committed_repack', lambda *_args, **_kwargs: None)
    result = stream_repack.repack(cold, stage, dataset, original.manifest_sha256,
                                  node_id=original.manifest['publisher']['node_id'])
    receipt_dir = workspace/'migration/source-local-exact-repack'/dataset
    receipt_dir.mkdir(parents=True, exist_ok=True)
    receipt = {'contract': 'stockagent.source_local_exact_repack.v1',
        'state': 'new_D_head_committed_from_exact_E_source', 'dataset': dataset,
        'old_manifest_sha256': original.manifest_sha256,
        'new_snapshot_id': result['snapshot_id'], 'new_manifest_sha256': result['new_manifest_sha256'],
        'files': result['files'], 'logical_bytes': result['logical_bytes'],
        'source_mode': 'D_streamed_to_E_dataset_pack', 'private': private}
    receipt_path = receipt_dir/(result['new_manifest_sha256']+'.json')
    receipt_path.write_text(json.dumps(receipt))
    return result, receipt_path


@pytest.mark.parametrize('private', [False, True])
def test_refresh_exact_repack_requires_matching_journal_and_receipt(refresh_setup, tmp_path, monkeypatch, private):
    f = refresh_setup
    _run(f, 100)
    _run(f, 200)
    dataset = 'private' if private else 'public'
    cold = migration.private_history_root(f[1], version_policy(f[0])) if private else f[1]
    original = resolve_latest_packed(cold, dataset)
    workspace = tmp_path/'workspace'
    result, receipt_path = _source_hashed_repack(workspace, cold, dataset, original, monkeypatch, private=private)
    entry = {'dataset': dataset, 'publish': not private}
    previous = {'domain': 'host_private' if private else 'shared',
                'snapshot_id': original.manifest['snapshot_id'], 'manifest_sha256': original.manifest_sha256}
    proof = migration._exact_repack_refresh_successor(cold, workspace, entry, previous)
    assert proof['manifest_sha256'] == result['new_manifest_sha256']
    assert proof['source_member_sha_streamed'] is True and proof['independent_D_readback'] is False
    receipt = json.loads(receipt_path.read_text())
    receipt['old_manifest_sha256'] = 'f'*64
    receipt_path.write_text(json.dumps(receipt))
    with pytest.raises(migration.SnapshotError, match='publication receipt differs'):
        migration._exact_repack_refresh_successor(cold, workspace, entry, previous)


def test_daily_refresh_follows_exact_physical_repack(refresh_setup, tmp_path, monkeypatch):
    from stockagent.data_sync import workspace_volume
    f = refresh_setup
    _run(f, 100)
    _run(f, 200)
    original = resolve_latest_packed(f[1], 'public')
    workspace = tmp_path/'workspace'
    result, _ = _source_hashed_repack(workspace, f[1], 'public', original, monkeypatch, private=False)
    monkeypatch.setattr(workspace_volume, 'configuration', lambda: {'mount_point': str(workspace)})
    monkeypatch.setattr(workspace_volume, 'check', lambda **_kwargs: None)
    (f[0]/'data_public'/'raw.txt').write_text('fresh data after repack')
    _run(f, 3700)
    current = json.loads((f[4]/'dataset-versions/current.json').read_text())
    row = json.loads((Path(current['operation_directory'])/'progress.json').read_text())['datasets']['public']
    proof = row['freshness_refresh']['base_rebinding']
    assert proof['contract'] == 'stockagent.refresh_exact_repack_binding.v1'
    assert proof['manifest_sha256'] == result['new_manifest_sha256']
    assert row['freshness_refresh']['source_freshness_checked'] is True
    assert resolve_latest_packed(f[1], 'public').manifest_sha256 != result['new_manifest_sha256']


def test_waiting_refresh_rebinds_when_repack_receipt_arrives_later(refresh_setup, tmp_path, monkeypatch):
    from stockagent.data_sync import workspace_volume
    f = refresh_setup
    _run(f, 100)
    _run(f, 200)
    original = resolve_latest_packed(f[1], 'public')
    workspace = tmp_path/'workspace'
    result, receipt_path = _source_hashed_repack(workspace, f[1], 'public', original, monkeypatch, private=False)
    sealed_receipt = receipt_path.read_bytes()
    receipt_path.unlink()
    monkeypatch.setattr(workspace_volume, 'configuration', lambda: {'mount_point': str(workspace)})
    monkeypatch.setattr(workspace_volume, 'check', lambda **_kwargs: None)
    (f[0]/'data_public'/'raw.txt').write_text('fresh data after delayed receipt')
    _run(f, 3700)
    current = json.loads((f[4]/'dataset-versions/current.json').read_text())
    progress = Path(current['operation_directory'])/'progress.json'
    state = json.loads(progress.read_text())
    assert state['datasets']['public']['state'] == 'waiting_retry'
    assert resolve_latest_packed(f[1], 'public').manifest_sha256 == result['new_manifest_sha256']
    receipt_path.write_bytes(sealed_receipt)
    state['datasets']['public']['retry_at_unix'] = 0
    progress.write_text(json.dumps(state))
    _run(f, 3701)
    row = json.loads(progress.read_text())['datasets']['public']
    assert row['freshness_refresh']['base_rebinding']['manifest_sha256'] == result['new_manifest_sha256']
    assert row['freshness_refresh']['source_freshness_checked'] is True

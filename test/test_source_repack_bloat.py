import json
from contextlib import contextmanager
import sys
from pathlib import Path

import pytest

from stockagent.data_sync import packed_snapshots as p, source_repack_bloat as bloat, stream_repack
from stockagent.data_sync.desync_snapshots import SnapshotError


def make_source(tmp_path, monkeypatch, *, duplicate=True):
    source, cold, workspace = tmp_path/'source', tmp_path/'cold', tmp_path/'workspace'
    source.mkdir()
    workspace.mkdir()
    payload = b'abc123'*1024
    (source/'a.bin').write_bytes(payload)
    (source/'b.bin').write_bytes(payload if duplicate else b'xyz987'*1024)
    monkeypatch.setattr(p, '_notify_packed_publication', lambda *_args, **_kwargs: None)
    snapshot = p.publish_packed_snapshot(cold, 'fixture', source, node_id='penguin')
    monkeypatch.setattr(bloat, 'MIN_DUPLICATE_BYTES', 1)
    return cold, workspace, snapshot


def test_duplicate_census_enrolls_exact_stage_without_reading_payload(tmp_path, monkeypatch):
    cold, workspace, old = make_source(tmp_path, monkeypatch)
    def no_payload(*_args, **_kwargs):
        pytest.fail('auto dedup planning must not read source payloads')
    monkeypatch.setattr(stream_repack, '_build', no_payload)
    record = bloat.preflight(workspace, cold, 'fixture', old.manifest_sha256, old.manifest)
    assert record['duplicate_path_count'] == 1
    assert record['source_payload_reread'] is False and record['D_head_changed'] is False
    assert record['retirement_authorized'] is False
    assert bloat.auto_holds(workspace) == {('fixture', old.manifest_sha256)}
    stage = workspace/'migration/stream-source-repack-v1/fixture'/old.manifest_sha256
    assert json.loads((stage/'intent.json').read_text()) == {
        'contract': stream_repack.CONTRACT+'.intent', 'dataset': 'fixture',
        'old_manifest_sha256': old.manifest_sha256}
    assert p.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256


def test_unique_content_does_not_block_normal_packing(tmp_path, monkeypatch):
    cold, workspace, old = make_source(tmp_path, monkeypatch, duplicate=False)
    assert bloat.preflight(workspace, cold, 'fixture', old.manifest_sha256, old.manifest) is None
    assert bloat.auto_holds(workspace) == set()
    assert not (workspace/'migration/stream-source-repack-v1/fixture'/old.manifest_sha256).exists()


def test_interrupted_hold_resumes_intent_and_bad_record_holds_only_its_source(tmp_path, monkeypatch):
    cold, workspace, old = make_source(tmp_path, monkeypatch)
    record = bloat.preflight(workspace, cold, 'fixture', old.manifest_sha256, old.manifest)
    stage = workspace/'migration/stream-source-repack-v1/fixture'/old.manifest_sha256
    (stage/'intent.json').unlink()
    stage.rmdir()
    assert bloat.ensure_stream_intent(workspace, 'fixture', old.manifest_sha256) == stage
    hold = workspace/'migration/source-local-exact-repack/auto-dedup-holds-v1'/('fixture-'+old.manifest_sha256+'.json')
    record['identity_sha256'] = '0'*64
    hold.write_text(json.dumps(record))
    assert bloat.auto_holds(workspace) == {('fixture', old.manifest_sha256)}
    (stage/'intent.json').write_text('{}')
    with pytest.raises(SnapshotError, match='intent differs'):
        bloat.ensure_stream_intent(workspace, 'fixture', old.manifest_sha256)


def test_preflight_leaves_existing_transaction_unchanged(tmp_path, monkeypatch):
    cold, workspace, old = make_source(tmp_path, monkeypatch)
    stage = workspace/'migration/stream-source-repack-v1/fixture'/old.manifest_sha256
    stage.mkdir(parents=True)
    (stage/'intent.json').write_text('existing')
    assert bloat.preflight(workspace, cold, 'fixture', old.manifest_sha256, old.manifest) is None
    assert (stage/'intent.json').read_text() == 'existing'


def test_already_deduplicated_alias_format_never_gets_a_new_hold(tmp_path):
    workspace, cold = tmp_path/'workspace', tmp_path/'cold'
    workspace.mkdir()
    cold.mkdir()
    manifest = {'dataset': 'fixture', 'source': {}, 'archive': {'format': p.ALIAS_ARCHIVE_FORMAT}}
    assert bloat.preflight(workspace, cold, 'fixture', 'a'*64, manifest) is None
    assert bloat.auto_holds(workspace) == set()


def test_original_stream_cli_routes_a_new_duplicate_source_before_D_write(tmp_path, monkeypatch):
    from scripts import run_stream_packed_repack as cli
    cold, workspace, old = make_source(tmp_path, monkeypatch)
    monkeypatch.setattr(cli, 'COLD', cold)
    monkeypatch.setattr(cli, '_check_d_primary_mount', lambda *_args: None)
    monkeypatch.setattr(cli.workspace_volume, 'configuration', lambda: {'mount_point': str(workspace)})
    monkeypatch.setattr(cli.workspace_volume, 'check', lambda **_kwargs: None)
    monkeypatch.setattr(cli.workspace_volume, 'admit', lambda *_args, **_kwargs: None)
    monkeypatch.setattr(cli.workspace_volume, 'path', lambda *_args, **_kwargs: workspace/'migration')
    monkeypatch.setattr(sys, 'argv', ['run_stream_packed_repack.py', '--dataset', 'fixture',
                                      '--expected-manifest-sha256', old.manifest_sha256])
    for _ in range(2):
        with pytest.raises(SnapshotError, match='queued for E dedup candidate'):
            cli.main()
    assert bloat.auto_holds(workspace) == {('fixture', old.manifest_sha256)}
    assert p.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256
    assert old.manifest_path.is_file()


def test_auto_hold_remains_in_original_queue_after_discovery_rotation(tmp_path, monkeypatch):
    from scripts import run_source_local_repack_queue as queue
    cold, workspace, old = make_source(tmp_path, monkeypatch)
    bloat.preflight(workspace, cold, 'fixture', old.manifest_sha256, old.manifest)
    root = workspace/'migration/source-local-exact-repack'
    monkeypatch.setattr(queue.workspace_volume, 'configuration', lambda: {'mount_point': str(workspace)})
    monkeypatch.setattr(queue.workspace_volume, 'check', lambda **_kwargs: None)
    monkeypatch.setattr(queue, 'load_queue', lambda *_args: [])
    monkeypatch.setattr(queue, 'load_generation_holds', lambda: set())
    monkeypatch.setattr(queue, 'testing_disposition', lambda: None)
    monkeypatch.setattr(queue, 'private_source_enrollment_sha', lambda: 'f'*64)
    monkeypatch.setattr(queue.staged_head_capacity_relief, 'E_only_pending_generations', lambda *_args: set())
    monkeypatch.setattr(queue, 'prepared_installer_busy', lambda *_args: False)
    (root/'discovered-sources.json').write_text(json.dumps({
        'contract': queue.DISCOVERY_CONTRACT, 'next_scan_epoch': queue.time.time()+600,
        'private_source_enrollment_sha256': 'f'*64, 'testing_disposition_identity_sha256': None,
        'extra_has_more': False, 'entries': []}))
    monkeypatch.setattr(queue, 'run_observed', lambda *_args, **_kwargs: pytest.fail('a held source cannot write D'))
    assert queue.main() == 0
    status = json.loads((root/'queue-status.json').read_text())
    row, = status['entries']
    assert row['dataset'] == 'fixture' and row['reason'] == 'source_duplicate_bloat'
    assert row.get('E_prepared') is not True and row['state'] == 'retrying'


def test_monitor_sees_fixed_dedup_candidate_before_queue_refresh(tmp_path, monkeypatch):
    from stockagent.data_sync import storage_progress as monitor
    from scripts import run_source_local_repack_queue as queue
    cold, workspace, old = make_source(tmp_path, monkeypatch)
    bloat.preflight(workspace, cold, 'fixture', old.manifest_sha256, old.manifest)
    root = workspace/'migration/source-local-exact-repack'
    (root/'queue-status.json').write_text(json.dumps({
        'contract': queue.CONTRACT+'.status', 'entries': [], 'observed_at_epoch': monitor.time.time()}))
    status = workspace/'migration/source-local-dedup-candidate-v1/fixture.status.json'
    status.parent.mkdir(parents=True)
    status.write_text(json.dumps({'contract': 'stockagent.dedup_candidate_queue.v1',
        'dataset': 'fixture', 'expected_manifest_sha256': old.manifest_sha256,
        'state': 'running', 'owner_identity': {}, 'observed_at_epoch': monitor.time.time(),
        'publishable': False, 'retirement_authorized': False,
        'unique_bytes': 100, 'unique_contents': 10,
        'progress': {'stage': 'streaming_source_into_E_packs',
                     'logical_bytes_processed': 40, 'files_completed': 4}}))
    monkeypatch.setattr(monitor, '_process_identity_matches', lambda *_args: True)
    row, = monitor.source_local_repack_queue_tasks(workspace, workspace_checked=True)
    assert (row['state'], row['phase'], row['completed_bytes'], row['total_bytes']) == (
        'running', 'source_local_dedup_packing', 40, 100)
    assert row['complete_evidence'] is False
    value = json.loads(status.read_text())
    value['retirement_authorized'] = True
    status.write_text(json.dumps(value))
    assert monitor.source_local_repack_queue_tasks(workspace, workspace_checked=True) == []


def test_stale_discovery_does_not_supersede_a_newer_auto_hold(tmp_path, monkeypatch):
    from scripts import run_source_local_repack_queue as queue
    cold, workspace, old = make_source(tmp_path, monkeypatch)
    bloat.preflight(workspace, cold, 'fixture', old.manifest_sha256, old.manifest)
    root = workspace/'migration/source-local-exact-repack'
    stale = 'a'*64 if old.manifest_sha256 != 'a'*64 else 'b'*64
    row = {'dataset': 'fixture', 'expected_manifest_sha256': stale, 'origin': 'cold',
           'source': str(workspace/'migration/stream-source-repack-v1/fixture'/stale),
           'files': 2, 'logical_bytes': 12288}
    monkeypatch.setattr(queue.workspace_volume, 'configuration', lambda: {'mount_point': str(workspace)})
    monkeypatch.setattr(queue.workspace_volume, 'check', lambda **_kwargs: None)
    monkeypatch.setattr(queue, 'load_queue', lambda *_args: [])
    monkeypatch.setattr(queue, 'load_generation_holds', lambda: set())
    monkeypatch.setattr(queue, 'testing_disposition', lambda: None)
    monkeypatch.setattr(queue, 'private_source_enrollment_sha', lambda: 'f'*64)
    monkeypatch.setattr(queue.staged_head_capacity_relief, 'E_only_pending_generations', lambda *_args: set())
    monkeypatch.setattr(queue, 'prepared_installer_busy', lambda *_args: False)
    monkeypatch.setattr(queue, 'prior_published', lambda _root, item: item['expected_manifest_sha256'] == stale)
    (root/'discovered-sources.json').write_text(json.dumps({
        'contract': queue.DISCOVERY_CONTRACT, 'next_scan_epoch': queue.time.time()+600,
        'private_source_enrollment_sha256': 'f'*64, 'testing_disposition_identity_sha256': None,
        'extra_has_more': False, 'entries': [row]}))
    monkeypatch.setattr(queue, 'run_observed', lambda *_args, **_kwargs: pytest.fail('a held source cannot write D'))
    assert queue.main() == 0
    entries = json.loads((root/'queue-status.json').read_text())['entries']
    held = next(entry for entry in entries if entry['expected_manifest_sha256'] == old.manifest_sha256)
    assert held['state'] == 'retrying' and held['reason'] == 'source_duplicate_bloat'


def test_adoption_reuses_fixed_metadata_validation_after_owner_contention(tmp_path, monkeypatch):
    from stockagent.data_sync import dedup_stream_candidate as candidate, packed_dedup_plan as plan
    cold, workspace, old = make_source(tmp_path, monkeypatch)
    stage = workspace/'candidate/fixture'/old.manifest_sha256
    stage.parent.mkdir(parents=True)
    report = workspace/'report'
    report.mkdir()
    index = plan.build_plan(cold, 'fixture', old.manifest_sha256, report)['index_path']
    candidate.prepare(cold, old, stage, index)
    original_lock = p._try_publication_lock
    @contextmanager
    def busy(*_args, **_kwargs):
        raise p.PackedPublicationBusy('another owner')
        yield
    monkeypatch.setattr(p, '_try_publication_lock', busy)
    with pytest.raises(p.PackedPublicationBusy):
        candidate.prepare_stream_adoption(cold, stage, 'fixture', old.manifest_sha256)
    assert (stage/'adoption-validation.json').is_file() and not (stage/'prepared.json').exists()
    monkeypatch.setattr(p, '_try_publication_lock', original_lock)
    monkeypatch.setattr(p, '_validate_inventory', lambda *_args, **_kwargs: pytest.fail('unexpected repeat validation'))
    prepared = candidate.prepare_stream_adoption(cold, stage, 'fixture', old.manifest_sha256)
    assert prepared['candidate_inventory_metadata_validated'] is True
    assert p.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256


def test_cached_adoption_never_trusts_a_changed_sealed_file(tmp_path, monkeypatch):
    from stockagent.data_sync import dedup_stream_candidate as candidate, packed_dedup_plan as plan
    cold, workspace, old = make_source(tmp_path, monkeypatch)
    stage = workspace/'candidate/fixture'/old.manifest_sha256
    stage.parent.mkdir(parents=True)
    report = workspace/'report'
    report.mkdir()
    index = plan.build_plan(cold, 'fixture', old.manifest_sha256, report)['index_path']
    sealed = candidate.prepare(cold, old, stage, index)
    @contextmanager
    def busy(*_args, **_kwargs):
        raise p.PackedPublicationBusy('another owner')
        yield
    original_lock = p._try_publication_lock
    monkeypatch.setattr(p, '_try_publication_lock', busy)
    with pytest.raises(p.PackedPublicationBusy):
        candidate.prepare_stream_adoption(cold, stage, 'fixture', old.manifest_sha256)
    monkeypatch.setattr(p, '_try_publication_lock', original_lock)
    (stage/sealed['packages'][0]['name']).write_bytes(b'changed')
    with pytest.raises(SnapshotError, match='generation changed'):
        candidate.prepare_stream_adoption(cold, stage, 'fixture', old.manifest_sha256)
    assert not (stage/'prepared.json').exists()


def test_automatic_hold_runs_existing_dedup_queue_until_reader_activation(tmp_path, monkeypatch):
    from scripts import run_dedup_candidate_queue as queue
    cold, workspace, old = make_source(tmp_path, monkeypatch)
    record = bloat.preflight(workspace, cold, 'fixture', old.manifest_sha256, old.manifest)
    assert record is not None
    monkeypatch.setattr(queue, 'COLD', cold)
    monkeypatch.setattr(queue, '_check_d_primary_mount', lambda *_args: None)
    monkeypatch.setattr(queue, 'load_generation_holds', lambda: set())
    monkeypatch.setattr(queue.workspace_volume, 'configuration',
                        lambda: {'mount_point': str(workspace), 'minimum_free_bytes': 0})
    monkeypatch.setattr(queue.workspace_volume, 'check', lambda **_kwargs: None)
    monkeypatch.setattr(queue.workspace_volume, 'admit', lambda *_args, **_kwargs: None)
    monkeypatch.setattr(queue.workspace_volume, 'path', lambda *_args, **_kwargs: workspace/'migration')
    monkeypatch.setattr(queue.s, '_repack_pin', lambda *_args: 'test-pin')
    monkeypatch.setattr(queue.s, '_check_repack_pin', lambda _pin: None)
    monkeypatch.setattr(queue.dedup_stage_handoff, 'process_references_many', lambda *_args, **_kwargs: [])
    @contextmanager
    def capacity(*_args, **_kwargs):
        yield {'other_reserved_growth_bytes': 0}
    monkeypatch.setattr(queue, 'reserve_capacity', capacity)
    assert queue.main() == 0
    root = workspace/'migration/source-local-dedup-candidate-v1'
    assert json.loads((root/'fixture.status.json').read_text())['state'] == 'sealed_waiting_reader'
    assert queue.main() == 0
    status = json.loads((root/'fixture.status.json').read_text())
    assert status['state'] == 'v3_handed_off_waiting_activation'
    stage = workspace/'migration/stream-source-repack-v1/fixture'/old.manifest_sha256
    assert stream_repack._read_prepared(stage, 'fixture', old.manifest_sha256)['packed_format'] == 'content-alias-v3'
    assert bloat.auto_holds(workspace) == {('fixture', old.manifest_sha256)}
    assert p.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256
    stream_repack.activate_content_alias_install(stage, 'fixture', old.manifest_sha256,
                                                 reader_contract='packed-v3-content-alias')
    assert bloat.auto_holds(workspace) == set()


def test_many_logical_paths_do_not_share_the_unique_content_memory_limit(tmp_path, monkeypatch):
    cold, workspace, old = make_source(tmp_path, monkeypatch)
    # There are two logical paths but only one distinct content. The unique
    # bound remains enforced by the census, independently of the path count.
    monkeypatch.setattr(bloat.packed_dedup_plan, 'MAX_UNIQUE_CONTENTS', 1)
    record = bloat.preflight(workspace, cold, 'fixture', old.manifest_sha256, old.manifest)
    assert record is not None and record['duplicate_path_count'] == 1
    assert bloat.MAX_FILES > bloat.packed_dedup_plan.MAX_UNIQUE_CONTENTS


def test_completed_private_encoding_is_compacted_installed_and_old_E_payload_reclaimed(tmp_path, monkeypatch):
    import random
    from scripts import run_prepared_stream_install_queue as install_queue, run_dedup_candidate_queue as dedup_queue
    from stockagent.data_sync import dedup_stage_handoff, dedup_stream_candidate as candidate

    source, public, workspace, materialized = (tmp_path/name for name in ('source', 'cold', 'workspace', 'materialized'))
    source.mkdir(); workspace.mkdir(); materialized.mkdir()
    payload = random.Random(94).randbytes(64*1024)
    for number in range(3):
        (source/f'copy-{number}.parquet').write_bytes(payload)
    private = public/'.local-state/private-history'
    monkeypatch.setattr(p, '_notify_packed_publication', lambda *_a, **_kw: None)
    old = p.publish_packed_snapshot(private, 'fixture', source, node_id='penguin',
                                    loose_file_threshold_bytes=1024, file_chunk_bytes=16*1024)
    stage = workspace/'migration/stream-source-repack-v1/fixture'/old.manifest_sha256
    with pytest.raises(SnapshotError, match='no D capacity'):
        stream_repack.repack(private, stage, 'fixture', old.manifest_sha256,
            materialized_pin_root=materialized,
            admit_install=lambda *_a: (_ for _ in ()).throw(SnapshotError('no D capacity')))
    normal = stream_repack._read_prepared(stage, 'fixture', old.manifest_sha256)
    original_bytes = sum(item['bytes'] for item in normal['packages'])
    original_prepared = (stage/'prepared.json').read_bytes()
    assert original_bytes > sum(item['bytes'] for item in old.manifest['archive']['objects'])
    root = workspace/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    (root/'queue-status.json').write_text(json.dumps({'contract': install_queue.q.CONTRACT+'.status',
        'entries': [{'dataset': 'fixture', 'expected_manifest_sha256': old.manifest_sha256,
            'origin': 'private_cold', 'state': 'retrying', 'E_prepared': True}]}))
    monkeypatch.setattr(bloat, 'MIN_DUPLICATE_BYTES', 1)
    rows = install_queue.run_once(workspace, public, free_bytes=0, enable_relief=True,
        runner=lambda *_a, **_kw: pytest.fail('an inflated build must not be installed or relieved'))
    assert rows[0]['state'] == 'waiting_dedup'
    assert bloat.auto_hold_rows(workspace)[0]['origin'] == 'private_cold'
    assert not (stage/'temporary-authority.json').exists()
    assert not (stage/'install.json').exists() and (stage/'prepared.json').read_bytes() == original_prepared

    index = root/'dedup-plans'/('fixture-'+old.manifest_sha256+'.sqlite')
    alternate = workspace/'migration/source-local-dedup-candidate-v1/fixture'/old.manifest_sha256
    monkeypatch.setattr(dedup_queue, 'COLD', public)
    monkeypatch.setattr(dedup_queue, '_check_d_primary_mount', lambda *_a: None)
    monkeypatch.setattr(dedup_queue, 'load_generation_holds', lambda: set())
    monkeypatch.setattr(dedup_queue.workspace_volume, 'configuration',
                        lambda: {'mount_point': str(workspace), 'minimum_free_bytes': 0})
    monkeypatch.setattr(dedup_queue.workspace_volume, 'check', lambda **_kw: None)
    monkeypatch.setattr(dedup_queue.workspace_volume, 'admit', lambda *_a, **_kw: None)
    monkeypatch.setattr(dedup_queue.workspace_volume, 'path', lambda *_a, **_kw: workspace/'migration')
    pin = stream_repack._repack_pin
    monkeypatch.setattr(stream_repack, '_repack_pin',
        lambda _root, *args, **kwargs: pin(materialized, *args, **kwargs))
    @contextmanager
    def capacity(*_args, **_kwargs):
        yield {'other_reserved_growth_bytes': 0}
    monkeypatch.setattr(dedup_queue, 'reserve_capacity', capacity)
    assert dedup_queue.main() == 0
    candidate_status = json.loads((alternate.parent.parent/'fixture.status.json').read_text())
    assert candidate_status['origin'] == 'private_cold'
    assert candidate_status['state'] == 'sealed_waiting_local_adoption'
    sealed = candidate.read_sealed(alternate, 'fixture', old.manifest_sha256)
    assert sealed['files'] == 3 and sealed['unique_contents'] == 1
    assert sum(item['bytes'] for item in sealed['packages']) < original_bytes
    candidate.prepare_stream_adoption(private, alternate, 'fixture', old.manifest_sha256)
    monkeypatch.setattr(dedup_stage_handoff, 'process_references_many', lambda *_a, **_kw: [])
    exchanged = dedup_stage_handoff.handoff(private, alternate, stage, 'fixture', old.manifest_sha256,
                                          materialized=materialized)
    assert exchanged['legacy_prepared_sha256'] == normal['identity_sha256']
    reclaimed = dedup_stage_handoff.prune_legacy(private, alternate, stage, 'fixture', old.manifest_sha256,
                                               materialized=materialized)
    assert reclaimed['state'] == 'completed' and reclaimed['reclaimed_allocated_bytes'] > 0
    assert (alternate/'prepared.json').read_bytes() == original_prepared
    assert not list(alternate.glob('pack-*.zip'))
    assert dedup_stage_handoff.prune_legacy(private, alternate, stage, 'fixture', old.manifest_sha256,
                                          materialized=materialized) == reclaimed
    activation = dedup_queue.activate_private_candidate(private, stage, 'fixture', old.manifest_sha256)
    assert activation['remote_reader_accepted'] is False
    assert bloat.auto_holds(workspace) == set()
    published = stream_repack.repack(private, stage, 'fixture', old.manifest_sha256,
                                     materialized_pin_root=materialized)
    current = p.resolve_latest_packed(private, 'fixture')
    assert current.manifest_sha256 == published['new_manifest_sha256']
    assert current.manifest['schema_version'] == 3 and not (public/'heads').exists()
    result = p.read_packed_files(private, current, names=[f'copy-{n}.parquet' for n in range(3)],
                                 maximum_bytes=4*len(payload), maximum_object_bytes=1024**2)
    assert result == {f'copy-{n}.parquet': payload for n in range(3)}


def test_a_fixed_unique_limit_is_observed_once_until_its_planner_or_bound_changes(tmp_path, monkeypatch):
    cold, workspace, old = make_source(tmp_path, monkeypatch, duplicate=False)
    monkeypatch.setattr(bloat.packed_dedup_plan, 'MAX_UNIQUE_CONTENTS', 1)
    assert bloat.preflight(workspace, cold, 'fixture', old.manifest_sha256, old.manifest) is None
    status = workspace/'migration/source-local-exact-repack/dedup-plans'/('fixture-'+old.manifest_sha256+'.status.json')
    value = json.loads(status.read_text())
    assert value['state'] == 'unique_limit' and value['source_payload_reread'] is False
    assert value['retirement_authorized'] is False
    planner = bloat.packed_dedup_plan.build_plan
    monkeypatch.setattr(bloat.packed_dedup_plan, 'build_plan', lambda *_a, **_kw: pytest.fail('repeated futile census'))
    assert bloat.preflight(workspace, cold, 'fixture', old.manifest_sha256, old.manifest) is None
    monkeypatch.setattr(bloat.packed_dedup_plan, 'MAX_UNIQUE_CONTENTS', 2)
    monkeypatch.setattr(bloat.packed_dedup_plan, 'build_plan', planner)
    assert bloat.preflight(workspace, cold, 'fixture', old.manifest_sha256, old.manifest) is None
    assert json.loads(status.read_text())['state'] == 'ready'


def test_monitor_uses_the_live_census_owner_without_claiming_candidate_completion(tmp_path, monkeypatch):
    from stockagent.data_sync import storage_progress as monitor
    from scripts import run_source_local_repack_queue as queue
    cold, workspace, old = make_source(tmp_path, monkeypatch)
    root = workspace/'migration/source-local-exact-repack'
    plans = root/'dedup-plans'
    plans.mkdir(parents=True)
    (root/'queue-status.json').write_text(json.dumps({'contract': queue.CONTRACT+'.status',
        'observed_at_epoch': monitor.time.time(), 'entries': [{
            'dataset': 'fixture', 'expected_manifest_sha256': old.manifest_sha256,
            'state': 'retrying', 'origin': 'private_cold', 'reason': 'repack_D_capacity'}]}))
    status = plans/('fixture-'+old.manifest_sha256+'.status.json')
    value = {'contract': bloat.CONTRACT+'.census-v1', 'dataset': 'fixture',
        'old_manifest_sha256': old.manifest_sha256, 'state': 'running',
        'source_payload_reread': False, 'D_head_changed': False, 'retirement_authorized': False,
        'owner_identity': {}, 'observed_at_epoch': monitor.time.time(),
        'progress': {'files_completed': 1, 'files_total': 2}}
    status.write_text(json.dumps(value))
    monkeypatch.setattr(monitor, '_process_identity_matches', lambda *_a: True)
    row, = monitor.source_local_repack_queue_tasks(workspace, workspace_checked=True)
    assert (row['state'], row['phase'], row['completed_items'], row['total_items']) == (
        'running', 'source_local_dedup_indexing', 1, 2)
    assert row['_priority_install_owner'] and not row.get('_dedup_candidate_owner')
    assert not row['complete_evidence']
    value['old_manifest_sha256'] = 'f'*64
    status.write_text(json.dumps(value))
    row, = monitor.source_local_repack_queue_tasks(workspace, workspace_checked=True)
    assert row['state'] != 'running'


def test_completed_installer_admission_matches_its_0800_bounded_deadline():
    from datetime import datetime, time
    from scripts.check_outside_tw_opening_resource_window import TAIPEI, evaluate
    from scripts.run_bounded_storage_maintenance import allowed_seconds
    from stockagent.data_sync import storage_progress as monitor
    at = datetime(2026, 11, 10, 7, 30, tzinfo=TAIPEI)
    assert evaluate(at, minimum_runway_minutes=20, protected_until=time(13, 30))['allowed']
    assert allowed_seconds(at, session_on=lambda _day: True) == 30*60
    assert not evaluate(at.replace(hour=8, minute=0), minimum_runway_minutes=20,
                         protected_until=time(13, 30))['allowed']
    condition = ('{ argv[]=/bin/bash -c source scripts/runtime_env.sh; run_fintech_python '
        'scripts/check_outside_tw_opening_resource_window.py --minimum-runway-minutes 20 '
        '--protected-until 13:30 --official-calendar-root data_tw_public ; code=exited ; status=1 }')
    assert monitor._storage_priority_condition_runway({'Result': 'exec-condition', 'ExecCondition': condition}) == 20
    assert monitor._storage_priority_condition_runway({'Result': 'exec-condition',
        'ExecCondition': condition.replace('status=1', 'status=2')}) is None


@pytest.mark.parametrize('intent', ['install.json', 'temporary-authority.json', 'current-head-capacity-relief.json'])
def test_sealed_compaction_never_redirects_an_existing_installation(tmp_path, monkeypatch, intent):
    cold, workspace, old = make_source(tmp_path, monkeypatch)
    stage = workspace/'migration/stream-source-repack-v1/fixture'/old.manifest_sha256
    with pytest.raises(SnapshotError, match='capacity'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256,
            admit_install=lambda *_a: (_ for _ in ()).throw(SnapshotError('capacity')))
    (stage/intent).write_text('{}')
    before = (stage/'prepared.json').read_bytes()
    assert bloat.preflight(workspace, cold, 'fixture', old.manifest_sha256,
                            old.manifest, allow_sealed=True) is None
    assert (stage/'prepared.json').read_bytes() == before
    assert bloat.auto_holds(workspace) == set()

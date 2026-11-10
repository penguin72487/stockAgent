import hashlib
import io
import json
from contextlib import contextmanager

import ijson
import pytest

from scripts import stage_d_cold_relocation as staging
from scripts import retire_staged_cold_objects as retirement
from scripts.verify_backup_delivery import verify
from stockagent.data_sync.backup_stream import _private_catalog_generation
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packed_backup import signature
from stockagent.data_sync.packed_snapshots import initialize_packed_layout, publish_packed_snapshot
from stockagent.data_sync import storage_progress


@pytest.fixture
def fixed_catalog(tmp_path):
    cold = tmp_path/'cold'
    initialize_packed_layout(cold, node_id='fixture')
    data = b'original bytes from a fixed cold object'
    sha = hashlib.sha256(data).hexdigest()
    relative = f'objects/blobs/{sha[:2]}/{sha}.blob'
    member = cold/relative
    member.parent.mkdir(parents=True)
    member.write_bytes(data)
    row = {'relative': relative, 'sha256': sha, 'bytes': len(data),
           'signature': list(signature(member)), 'role': 'unreferenced_cold_object'}
    catalog = tmp_path/'catalog.json'
    catalog.write_text(json.dumps({'files': [row], 'metadata_errors': []}))
    catalog.chmod(0o600)
    observation = tmp_path/'observation.json'
    observation.write_text(json.dumps({'contract': 'stockagent.backup_catalog_observation.v1',
        'catalog_identity_sha256': 'a'*64, 'catalog_generation': _private_catalog_generation(catalog)}))
    ledger = tmp_path/'ledger.json'
    ledger.write_text(json.dumps({'deliveries': {'accepted-delivery': {
        'nas_acceptance': {'state': 'nas_archive_file_recovery_verified'},
        'file_keys': [relative+'@'+sha]}}}))
    return cold, member, catalog, observation, ledger


def test_acknowledged_object_stages_with_native_source_portable_E_verify(fixed_catalog, tmp_path):
    cold, member, catalog, observation, ledger = fixed_catalog
    plan = staging.select(catalog, observation, ledger, 1024)
    assert plan['files'] == 1 and not plan['deletion_authorized']
    destination = tmp_path/'E-wave'
    result = staging.stage(plan, cold=cold, destination=destination, native_source_read=True)
    proof = verify(destination, result['delivery_identity_sha256'])
    assert proof['files_verified'] == 1 and proof['bytes_verified'] == member.stat().st_size
    assert result['D_names_deleted'] == 0 and member.exists()
    assert (destination/'cold'/member.relative_to(cold)).read_bytes() == member.read_bytes()


def test_staging_does_not_wait_for_nas_but_requires_fixed_catalog_generation(fixed_catalog):
    _cold, _member, catalog, observation, ledger = fixed_catalog
    ledger.write_text(json.dumps({'deliveries': {}}))
    assert staging.select(catalog, observation, ledger, 1024)['files'] == 1
    catalog.write_text('{}')
    with pytest.raises(SnapshotError, match='catalog identity'):
        staging.select(catalog, observation, ledger, 1024)


def test_staging_catalog_allows_only_trusted_device_renumber(fixed_catalog, tmp_path, monkeypatch):
    _cold, _member, catalog, observation, ledger = fixed_catalog
    marker = json.loads(observation.read_text())
    marker['catalog_generation'][0] += 1
    observation.write_text(json.dumps(marker))
    with pytest.raises(SnapshotError, match='catalog identity'):
        staging.select(catalog, observation, ledger, 1024)
    monkeypatch.setattr(retirement, '_canonical_catalog_on_system_volume', lambda *_a: True)
    plan = staging.select(catalog, observation, ledger, 1024)
    (tmp_path/(plan['identity_sha256']+'.plan.json')).write_text(json.dumps(plan))
    assert staging.caught_up_unchanged({'plan_identity_sha256': plan['identity_sha256']},
                                       tmp_path, catalog, observation)
    marker['catalog_generation'][1] += 1
    observation.write_text(json.dumps(marker))
    with pytest.raises(SnapshotError, match='catalog identity'):
        staging.select(catalog, observation, ledger, 1024)
    assert not staging.caught_up_unchanged({'plan_identity_sha256': plan['identity_sha256']},
                                           tmp_path, catalog, observation)


def test_fast_wave_and_retirement_resume_trusted_device_renumber(fixed_catalog, tmp_path, monkeypatch):
    cold, member, catalog, observation, _ledger = fixed_catalog
    original = member.read_bytes()
    plan = staging.select(catalog, observation, None, 1024)
    plan['objects'][0]['signature'][0] += 1
    plan['identity_sha256'] = retirement._identity(plan)
    with pytest.raises(SnapshotError, match='changed before E staging'):
        staging.stage_fast(plan, cold=cold, destination=tmp_path/'untrusted')
    monkeypatch.setattr(staging.stream_repack, '_trusted_device_renumbering', lambda *_a: True)
    destination = tmp_path/'trusted'
    result = staging.stage_fast(plan, cold=cold, destination=destination)
    done = retirement.retire(plan, result, cold=cold, destination=destination,
        journal=tmp_path/'trusted-handoff.json', catalog=catalog, observation=observation,
        extra_locks=(), authority_guard=lambda: None, apply=True)
    assert done['state'] == 'D_names_removed_E_copy_retained' and not member.exists()
    assert (destination/'cold'/member.relative_to(cold)).read_bytes() == original


def test_changed_original_is_not_accepted_or_deleted(fixed_catalog, tmp_path):
    cold, member, catalog, observation, ledger = fixed_catalog
    plan = staging.select(catalog, observation, ledger, 1024)
    member.write_bytes(b'changed')
    with pytest.raises(SnapshotError):
        staging.stage(plan, cold=cold, destination=tmp_path/'failed-wave', native_source_read=True)
    assert member.read_bytes() == b'changed'


def test_handoff_removes_only_unreferenced_D_name_after_E_is_closed(fixed_catalog, tmp_path):
    cold, member, catalog, observation, ledger = fixed_catalog
    original = member.read_bytes()
    plan = staging.select(catalog, observation, ledger, 1024)
    destination = tmp_path/'E-wave'
    result = staging.stage(plan, cold=cold, destination=destination, native_source_read=True)
    journal = tmp_path/'handoff.json'
    done = retirement.retire(plan, result, cold=cold, destination=destination, journal=journal,
        catalog=catalog, observation=observation, extra_locks=(), authority_guard=lambda: None, apply=True)
    assert done['state'] == 'D_names_removed_E_copy_retained' and not member.exists()
    assert (destination/'cold'/member.relative_to(cold)).read_bytes() == original
    assert json.loads(journal.read_text())['removed'][0]['bytes'] == len(original)


def test_new_manifest_reference_prevents_handoff(fixed_catalog, tmp_path):
    cold, member, catalog, observation, ledger = fixed_catalog
    plan = staging.select(catalog, observation, ledger, 1024)
    destination = tmp_path/'E-wave'
    result = staging.stage(plan, cold=cold, destination=destination, native_source_read=True)
    source = tmp_path/'new-source'
    source.mkdir()
    (source/'data').write_bytes(member.read_bytes())
    publish_packed_snapshot(cold, 'new-data', source, loose_file_threshold_bytes=1)
    with pytest.raises(SnapshotError, match='new or changed manifest uses'):
        retirement.retire(plan, result, cold=cold, destination=destination, journal=tmp_path/'handoff.json',
            catalog=catalog, observation=observation, extra_locks=(), authority_guard=lambda: None, apply=True)
    assert member.exists()


def test_changed_E_copy_prevents_handoff(fixed_catalog, tmp_path):
    cold, member, catalog, observation, ledger = fixed_catalog
    plan = staging.select(catalog, observation, ledger, 1024)
    destination = tmp_path/'E-wave'
    result = staging.stage(plan, cold=cold, destination=destination, native_source_read=True)
    (destination/'cold'/member.relative_to(cold)).write_bytes(b'bad')
    with pytest.raises(SnapshotError, match='payload changed'):
        retirement.retire(plan, result, cold=cold, destination=destination, journal=tmp_path/'handoff.json',
            catalog=catalog, observation=observation, extra_locks=(), authority_guard=lambda: None, apply=True)
    assert member.exists()


def test_fast_handoff_does_not_read_back_or_decode_E_payload(fixed_catalog, tmp_path, monkeypatch):
    cold, member, catalog, observation, _ledger = fixed_catalog
    plan = staging.select(catalog, observation, None, 1024)
    destination = tmp_path/'E-fast'
    result = staging.stage_fast(plan, cold=cold, destination=destination)
    assert result['destination_independent_readback'] is False
    from scripts import verify_backup_delivery
    monkeypatch.setattr(verify_backup_delivery, 'verify', lambda *_a, **_k: pytest.fail('no reread'))
    done = retirement.retire(plan, result, cold=cold, destination=destination, journal=tmp_path/'handoff.json',
        catalog=catalog, observation=observation, extra_locks=(), authority_guard=lambda: None, apply=True)
    assert done['state'] == 'D_names_removed_E_copy_retained' and not member.exists()


def test_fast_handoff_detects_E_file_changed_after_source_hashed_write(fixed_catalog, tmp_path):
    cold, member, catalog, observation, _ledger = fixed_catalog
    plan = staging.select(catalog, observation, None, 1024)
    destination = tmp_path/'E-fast'
    result = staging.stage_fast(plan, cold=cold, destination=destination)
    target = destination/'cold'/member.relative_to(cold)
    target.write_bytes(b'x' * target.stat().st_size)
    with pytest.raises(SnapshotError, match='payload changed'):
        retirement.retire(plan, result, cold=cold, destination=destination, journal=tmp_path/'handoff.json',
            catalog=catalog, observation=observation, extra_locks=(), authority_guard=lambda: None, apply=True)
    assert member.exists()


def test_interrupted_fast_copy_resumes_journalled_members_without_reread(tmp_path, monkeypatch):
    cold = tmp_path/'cold'
    cold.mkdir()
    rows = []
    for data in (b'first-contents', b'second-contents'):
        sha = hashlib.sha256(data).hexdigest()
        relative = f'objects/blobs/{sha[:2]}/{sha}.blob'
        source = cold/relative
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_bytes(data)
        rows.append({'relative': relative, 'sha256': sha, 'bytes': len(data), 'signature': list(signature(source))})
    body = {'contract': staging.CONTRACT, 'catalog_identity_sha256': 'b'*64,
            'objects': rows, 'files': len(rows), 'bytes': sum(r['bytes'] for r in rows)}
    plan = {**body, 'identity_sha256': retirement._identity(body)}
    destination = tmp_path/'E-wave'
    original = staging.copy_verified_bytes
    calls = []
    def interrupted(source, target, **kwargs):
        calls.append(source)
        if len(calls) == 2:
            target.write_bytes(b'incomplete')
            raise OSError('simulated interruption')
        return original(source, target, **kwargs)
    monkeypatch.setattr(staging, 'copy_verified_bytes', interrupted)
    with pytest.raises(OSError, match='simulated interruption'):
        staging.stage_fast(plan, cold=cold, destination=destination)
    assert len(json.loads((destination/'write-progress.json').read_text())['files']) == 1
    calls.clear()
    def resumed(source, target, **kwargs):
        calls.append(source)
        return original(source, target, **kwargs)
    monkeypatch.setattr(staging, 'copy_verified_bytes', resumed)
    result = staging.stage_fast(plan, cold=cold, destination=destination, resume=True)
    assert calls == [cold/rows[1]['relative']]
    assert len(retirement.closed_E_handoff(plan, result, destination)) == 2


def test_interrupted_fast_copy_rejects_changed_journalled_E_member(fixed_catalog, tmp_path):
    cold, member, catalog, observation, _ledger = fixed_catalog
    plan = staging.select(catalog, observation, None, 1024)
    destination = tmp_path/'E-wave'
    staging.stage_fast(plan, cold=cold, destination=destination)
    (destination/'write-complete.json').unlink()
    target = destination/'cold'/member.relative_to(cold)
    target.write_bytes(b'x' * target.stat().st_size)
    with pytest.raises(SnapshotError, match='E member changed'):
        staging.stage_fast(plan, cold=cold, destination=destination, resume=True)
    assert member.exists()


def test_fast_copy_recovers_completed_record_without_reread(fixed_catalog, tmp_path, monkeypatch):
    cold, _member, catalog, observation, _ledger = fixed_catalog
    plan = staging.select(catalog, observation, None, 1024)
    destination = tmp_path/'E-wave'
    staging.stage_fast(plan, cold=cold, destination=destination)
    monkeypatch.setattr(staging, 'copy_verified_bytes', lambda *_a, **_k: pytest.fail('no second copy'))
    result = staging.stage_fast(plan, cold=cold, destination=destination, resume=True)
    assert result['destination_independent_readback'] is False
    assert len(retirement.closed_E_handoff(plan, result, destination)) == 1


def test_fast_handoff_accepts_only_verified_workspace_device_renumber(fixed_catalog, tmp_path, monkeypatch):
    cold, _member, catalog, observation, _ledger = fixed_catalog
    plan = staging.select(catalog, observation, None, 1024)
    destination = tmp_path/'E-wave'
    result = staging.stage_fast(plan, cold=cold, destination=destination)
    original = retirement.signature
    def renumbered(path):
        value = original(path)
        return (value[0]+1, *value[1:]) if path.is_relative_to(destination) else value
    monkeypatch.setattr(retirement, 'signature', renumbered)
    with pytest.raises(SnapshotError, match='payload changed'):
        retirement.closed_E_handoff(plan, result, destination)
    monkeypatch.setattr(retirement.stream_repack, '_trusted_device_renumbering', lambda _path, _dev: True)
    assert len(retirement.closed_E_handoff(plan, result, destination)) == 1
    copied = destination/'cold'/plan['objects'][0]['relative']
    copied.write_bytes(copied.read_bytes()+b'changed')
    with pytest.raises(SnapshotError, match='payload changed'):
        retirement.closed_E_handoff(plan, result, destination)


def test_interrupted_handoff_reconciles_exact_missing_next_name(fixed_catalog, tmp_path):
    cold, member, catalog, observation, _ledger = fixed_catalog
    plan = staging.select(catalog, observation, None, 1024)
    destination = tmp_path/'E-fast'
    result = staging.stage_fast(plan, cold=cold, destination=destination)
    journal = tmp_path/'handoff.json'
    journal.write_text(json.dumps({'contract': retirement.CONTRACT, 'state': 'handoff_started',
        'plan_identity_sha256': plan['identity_sha256'], 'destination': str(destination),
        'files': 1, 'bytes': plan['bytes'], 'removed': [],
        'next_relative': plan['objects'][0]['relative']}))
    member.unlink()
    done = retirement.retire(plan, result, cold=cold, destination=destination, journal=journal,
        catalog=catalog, observation=observation, extra_locks=(), authority_guard=lambda: None, apply=True)
    assert done['state'] == 'D_names_removed_E_copy_retained'
    assert done['removed'][0]['reconciled_after_interruption'] is True
    assert done['removed'][0]['last_link_allocated_bytes'] is None


def test_monitor_samples_partial_write_without_reading_payload(fixed_catalog, tmp_path):
    _cold, _member, catalog, observation, _ledger = fixed_catalog
    plan = staging.select(catalog, observation, None, 1024)
    workspace = tmp_path/'workspace'
    root = workspace/'migration/d-cold-object-staging-v1'
    root.mkdir(parents=True)
    key = plan['identity_sha256']
    (root/(key+'.plan.json')).write_text(json.dumps(plan))
    path = root/(key+'.payload')/'cold'/plan['objects'][0]['relative']
    path.parent.mkdir(parents=True)
    path.with_name(path.name+'.partial').write_bytes(b'abc')
    (root/'scheduler-status.json').write_text(json.dumps({
        'contract': staging.CONTRACT+'.scheduler', 'state': 'copying',
        'plan_identity_sha256': key, 'files_total': 1, 'bytes_total': plan['bytes'],
        'files_completed': 0, 'bytes_written': 0, 'observed_at_epoch': 10}))
    row, total = storage_progress.d_cold_e_handoff_tasks(workspace, workspace_checked=True, now=123)
    assert row['state'] == 'running' and row['completed_bytes'] == 3 and row['total_bytes'] == plan['bytes']
    assert row['observed_at_utc'] == storage_progress.iso(123)
    assert total['kind'] == 'd_cold_e_total' and total['completed_bytes'] == 0
    assert total['total_bytes'] == plan['bytes']
    blocked, = storage_progress.d_cold_e_handoff_tasks(workspace, workspace_checked=False, now=123)
    assert blocked['state'] == 'blocked' and blocked['completed_bytes'] is None


def test_heterogeneous_handoff_estimates_bytes_not_file_count():
    row = storage_progress.task('d_cold_e_total', 'fixture', state='running', phase='d_e_object_handoff',
        observed=1000, generation='same', done_bytes=10, total_bytes=100,
        done_items=1, total_items=100)
    _, history = storage_progress.estimate(row, {}, now=1000)
    row.update(completed_bytes=50, completed_items=2, observed_at_utc=storage_progress.iso(1060))
    eta, _ = storage_progress.estimate(row, history, now=1060)
    assert eta['unit'] == 'bytes' and eta['state'] == 'estimated'


def test_small_objects_share_one_source_stream_and_keep_individual_identities(tmp_path):
    cold = tmp_path/'cold'
    cold.mkdir()
    rows = []
    for raw in (b'first', b'second', b''):
        sha = hashlib.sha256(raw).hexdigest()
        relative = f'objects/blobs/{sha[:2]}/{sha}.blob'
        path = cold/relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        rows.append({'relative': relative, 'sha256': sha, 'bytes': len(raw), 'signature': list(signature(path))})
    calls = []
    @contextmanager
    def grouped(members):
        calls.append(len(members))
        yield io.BytesIO(b''.join(path.read_bytes() for path, _ in members))
    destination = tmp_path/'E'
    destination.mkdir()
    copied = staging._copy_small_native_group(cold, destination, rows, grouped)
    assert calls == [3] and [row['sha256'] for row in copied] == [row['sha256'] for row in rows]
    assert all((destination/'cold'/row['relative']).stat().st_size == row['bytes'] for row in rows)


def test_fast_copy_batches_medium_objects_without_a_native_process_per_file(tmp_path,monkeypatch):
    from stockagent.data_sync import windows_cold_io
    cold=tmp_path/'cold';cold.mkdir()
    monkeypatch.setattr(staging,'COLD',cold)
    rows=[]
    for value in (b'a',b'b'):
        raw=value*(9*1024**2)
        digest=hashlib.sha256(raw).hexdigest()
        relative=f'objects/blobs/{digest[:2]}/{digest}.blob'
        path=cold/relative;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw)
        rows.append({'relative':relative,'sha256':digest,'bytes':len(raw),'signature':list(signature(path))})
    calls=[]
    @contextmanager
    def grouped(members):
        calls.append(len(members))
        yield io.BytesIO(b''.join(path.read_bytes() for path,_ in members))
    monkeypatch.setattr(windows_cold_io,'binary_reader_many',grouped)
    monkeypatch.setattr(staging,'copy_verified_bytes',lambda *_a,**_k:pytest.fail('native process per medium file'))
    plan={'objects':rows,'bytes':sum(row['bytes'] for row in rows),'files':2,'identity_sha256':'a'*64}
    result=staging.stage_fast(plan,cold=cold,destination=tmp_path/'E-medium')
    assert calls==[2] and result['files']==2


def test_small_group_source_corruption_never_removes_original(tmp_path):
    cold = tmp_path/'cold'
    cold.mkdir()
    raw = b'original'
    sha = hashlib.sha256(raw).hexdigest()
    relative = f'objects/blobs/{sha[:2]}/{sha}.blob'
    source = cold/relative
    source.parent.mkdir(parents=True)
    source.write_bytes(raw)
    row = {'relative': relative, 'sha256': sha, 'bytes': len(raw), 'signature': list(signature(source))}
    @contextmanager
    def corrupted(_members):
        yield io.BytesIO(b'changed!')
    destination = tmp_path/'E'
    destination.mkdir()
    with pytest.raises(SnapshotError, match='checksum differs'):
        staging._copy_small_native_group(cold, destination, [row], corrupted)
    assert source.read_bytes() == raw


def test_catalog_index_cache_is_reused_only_for_the_same_catalog_generation(fixed_catalog, tmp_path, monkeypatch):
    _cold, member, catalog, observation, _ledger = fixed_catalog
    relative = member.relative_to(_cold).as_posix()
    cache = tmp_path/'index.json'
    _metadata, rows, _identity = retirement.catalog_index(catalog, observation, {relative}, cache_path=cache)
    assert relative in rows and cache.is_file()
    original = ijson.items
    monkeypatch.setattr(ijson, 'items', lambda *_a, **_k: pytest.fail('unchanged index should be reused'))
    assert relative in retirement.catalog_index(catalog, observation, {relative}, cache_path=cache)[1]
    monkeypatch.setattr(ijson, 'items', original)
    value = json.loads(catalog.read_text())
    value['files'][0]['role'] = 'packed_object'
    catalog.write_text(json.dumps(value))
    marker = json.loads(observation.read_text())
    marker['catalog_generation'] = _private_catalog_generation(catalog)
    observation.write_text(json.dumps(marker))
    with pytest.raises(SnapshotError, match='no longer proves'):
        retirement.catalog_index(catalog, observation, {relative}, cache_path=cache)


def test_catalog_index_recovers_device_renumber_only_for_trusted_service_root(fixed_catalog, tmp_path, monkeypatch):
    cold, member, catalog, observation, _ledger = fixed_catalog
    relative = member.relative_to(cold).as_posix()
    cache = tmp_path/'index.json'
    retirement.catalog_index(catalog, observation, {relative}, cache_path=cache)
    marker = json.loads(observation.read_text())
    marker['catalog_generation'][0] += 1
    observation.write_text(json.dumps(marker))
    value = json.loads(cache.read_text())
    value['catalog_generation'][0] += 1
    value['identity_sha256'] = retirement._identity(value)
    cache.write_text(json.dumps(value))
    with pytest.raises(SnapshotError, match='fixed observation differ'):
        retirement.catalog_index(catalog, observation, {relative}, cache_path=cache)
    monkeypatch.setattr(retirement, '_canonical_catalog_on_system_volume', lambda *_a: True)
    monkeypatch.setattr(ijson, 'items', lambda *_a, **_k: pytest.fail('cache must be reused'))
    assert relative in retirement.catalog_index(catalog, observation, {relative}, cache_path=cache)[1]
    marker['catalog_generation'][1] += 1
    observation.write_text(json.dumps(marker))
    with pytest.raises(SnapshotError, match='fixed observation differ'):
        retirement.catalog_index(catalog, observation, {relative}, cache_path=cache)


def test_caught_up_skips_only_an_unchanged_catalog(fixed_catalog, tmp_path):
    _cold, _member, catalog, observation, _ledger = fixed_catalog
    key = 'a'*64
    identity = json.loads(observation.read_text())['catalog_identity_sha256']
    (tmp_path/(key+'.plan.json')).write_text(json.dumps({'catalog_identity_sha256': identity}))
    status = {'plan_identity_sha256': key}
    assert staging.caught_up_unchanged(status, tmp_path, catalog, observation)
    catalog.write_text(catalog.read_text() + ' ')
    assert not staging.caught_up_unchanged(status, tmp_path, catalog, observation)


def test_caught_up_recheck_refreshes_liveness_without_backdating_scan():
    previous = {'state': 'caught_up', 'observed_at_epoch': 100,
                'plan_identity_sha256': 'a'*64, 'files_completed': 5}
    refreshed = staging.refresh_caught_up(previous, now=160)
    assert previous.get('last_checked_at_epoch') is None
    assert refreshed['observed_at_epoch'] == 100
    assert refreshed['last_checked_at_epoch'] == 160 and refreshed['next_scan_epoch'] == 220
    assert refreshed['plan_identity_sha256'] == 'a'*64 and refreshed['files_completed'] == 5


def test_total_eta_uses_completed_wave_intervals_not_last_deletion_burst(tmp_path):
    workspace = tmp_path/'workspace'
    root = workspace/'migration/d-cold-object-staging-v1'
    root.mkdir(parents=True)
    def wave(digit, size, completed=None):
        sha = digit*64
        relative = f'objects/blobs/{sha[:2]}/{sha}.blob'
        body = {'contract': staging.CONTRACT, 'catalog_identity_sha256': 'a'*64,
                'objects': [{'relative': relative, 'sha256': sha, 'bytes': size}],
                'files': 1, 'bytes': size, 'remaining_files_before_wave': 1,
                'remaining_bytes_before_wave': size}
        key = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':'),
                                        ensure_ascii=False).encode()).hexdigest()
        (root/(key+'.plan.json')).write_text(json.dumps({**body, 'identity_sha256': key}))
        (root/(key+'.payload')).mkdir()
        if completed is not None:
            (root/(key+'.handoff.json')).write_text(json.dumps({
                'contract': retirement.CONTRACT, 'plan_identity_sha256': key,
                'state': 'D_names_removed_E_copy_retained', 'completed_at_epoch': completed,
                'removed': [{'relative': relative, 'bytes': size}]}))
        return key
    wave('1', 20, 900)
    wave('2', 30, 930)
    wave('3', 40, 960)
    current = wave('4', 100)
    (root/'scheduler-status.json').write_text(json.dumps({
        'contract': staging.CONTRACT+'.scheduler', 'state': 'copying',
        'plan_identity_sha256': current, 'files_total': 1, 'bytes_total': 100,
        'files_completed': 0, 'bytes_written': 0, 'observed_at_epoch': 999}))
    _wave, overall = storage_progress.d_cold_e_handoff_tasks(workspace, workspace_checked=True, now=1000)
    eta, _ = storage_progress.estimate(overall, {}, now=1000)
    assert overall['completed_bytes'] == 90 and overall['total_bytes'] == 190
    assert eta['state'] == 'estimated' and eta['unit'] == 'bytes' and eta['remaining_seconds'] == 46

    # Idle state must keep the completed cohort, including records written by
    # an older scheduler that did not preserve the last plan identity.
    (root/(current+'.handoff.json')).write_text(json.dumps({
        'contract': retirement.CONTRACT, 'plan_identity_sha256': current,
        'state': 'D_names_removed_E_copy_retained', 'completed_at_epoch': 1030,
        'removed': [{'relative': f'objects/blobs/{"4"*2}/{"4"*64}.blob', 'bytes': 100}]}))
    (root/'scheduler-status.json').write_text(json.dumps({
        'contract': staging.CONTRACT+'.scheduler', 'state': 'caught_up', 'observed_at_epoch': 1031,
        'last_checked_at_epoch': 1060}))
    _wave, overall = storage_progress.d_cold_e_handoff_tasks(workspace, workspace_checked=True, now=1061)
    assert _wave['observed_at_utc'] == storage_progress.iso(1060)
    assert overall['state'] == 'completed' and overall['complete_evidence'] is True
    assert overall['completed_bytes'] == overall['total_bytes'] == 190


def test_promotion_receipt_only_claims_published_dataset(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack/cftc-legacy-pre2000'
    root.mkdir(parents=True)
    digest = 'e'*64
    (root/(digest+'.json')).write_text(json.dumps({
        'contract': 'stockagent.source_local_exact_repack.v1',
        'state': 'new_D_head_committed_from_exact_E_source',
        'dataset': 'cftc-legacy-pre2000', 'new_manifest_sha256': digest,
        'pack_layout': 'source-dataset-v1', 'files': 10, 'logical_bytes': 100,
        'old_release_removed': False, 'independent_D_readback': False,
        'NAS_accepted': False, 'observed_at_epoch': 1000}))
    row, = storage_progress.source_local_promotion_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'completed' and row['completed_items'] == 10
    assert row['subject'] == 'cftc-legacy-pre2000'
    assert '未獨立讀回 D 或 NAS' in storage_progress.SCOPES['source_local_promotion']
    assert storage_progress.source_local_promotion_tasks(tmp_path, workspace_checked=False) == []

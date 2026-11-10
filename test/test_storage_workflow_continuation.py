"""Fixed pending delivery survives a history-bounded NAS outage continuation."""
import asyncio
from contextlib import nullcontext
from datetime import timedelta
import fcntl
from types import SimpleNamespace
from unittest.mock import patch

import pytest

pytest.importorskip('temporalio', reason='run with the locked lakehouse control role')
from stockagent.control import storage_workflow as sw


class Continued(BaseException):
    def __init__(self, request):
        self.request = request


def continue_request(request):
    raise Continued(request)


def test_catalog_ack_is_checkpointed_without_waiting_for_transport_gc(tmp_path):
    proof = {'state':'nas_archive_file_recovery_verified','delivery_identity_sha256':'a'*64}
    with patch.object(sw,'configuration',lambda:{'state_root':str(tmp_path)}), \
         patch.object(sw,'activity_lease',lambda:nullcontext()), \
         patch.object(sw,'acceptance',lambda *args:dict(proof)), \
         patch.object(sw,'retire_accepted_transport',side_effect=AssertionError('inline GC blocks publication')):
        result=sw.check_nas_acceptance('a'*64)
    assert result['transport_retirement']['state']=='delegated_to_transport_gc'
    assert (tmp_path/('catalog-acceptance-'+'a'*64+'.json')).is_file()


def test_catalog_activity_admits_the_exact_enrolled_export_and_keeps_gc_separate(tmp_path):
    proof = {'state': 'nas_archive_file_recovery_verified', 'delivery_identity_sha256': 'a' * 64,
             'catalog_semantic_restore_verified': True}
    enrollment = {'state': 'published', 'snapshot_id': 7, 'delivery_identity_sha256': 'a' * 64}
    seen = []
    def admit(c, identity, **options):
        seen.append((identity, options))
        return dict(proof)
    with patch.object(sw, 'configuration', lambda: {'state_root': str(tmp_path)}), \
         patch.object(sw, 'activity_lease', lambda: nullcontext()), \
         patch.object(sw, 'catalog_enrollment', lambda *args: enrollment), \
         patch.object(sw, 'acceptance', admit), \
         patch.object(sw, 'retire_accepted_transport', side_effect=AssertionError('inline GC is forbidden')):
        result = sw.check_nas_acceptance('a' * 64)
    assert seen == [('a' * 64, {'enrolled_catalog': enrollment})]
    assert result['transport_retirement']['state'] == 'delegated_to_transport_gc'
    assert (tmp_path / ('catalog-acceptance-' + 'a' * 64 + '.json')).is_file()


def test_nas_outage_continuation_keeps_exact_unaccepted_delivery():
    fixed = {'snapshot_id': 10, 'delivery_identity_sha256': 'a' * 64, 'state': 'published'}
    async def execute(handler, *args, **options):
        if handler == sw.register_versions: return {'snapshot_id': 10}
        if handler == sw.export_version: return fixed
        assert handler == sw.check_nas_acceptance and args == ('a' * 64,)
        return {'state': 'waiting_nas_archive_acceptance'}
    info = SimpleNamespace(is_continue_as_new_suggested=lambda: False, get_current_history_length=lambda: 5000)
    with patch.object(sw.workflow, 'execute_activity', execute), patch.object(sw.workflow, 'info', lambda: info), \
         patch.object(sw.workflow, 'patched', lambda _name: True), \
         patch.object(sw.workflow, 'continue_as_new', side_effect=continue_request):
        with pytest.raises(Continued) as continued:
            asyncio.run(sw.StorageLifecycle().run({'last_snapshot': 9}))
    assert continued.value.request == {'last_snapshot': 9, 'pending_export': fixed}


def test_resumed_outage_checks_fixed_delivery_before_registering_new_latest():
    fixed = {'snapshot_id': 10, 'delivery_identity_sha256': 'a' * 64, 'state': 'published'}
    calls = []
    async def execute(handler, *args, **options):
        calls.append((handler, args))
        assert handler == sw.check_nas_acceptance and args == ('a' * 64,)
        return {'state': 'waiting_nas_archive_acceptance'}
    info = SimpleNamespace(is_continue_as_new_suggested=lambda: True, get_current_history_length=lambda: 2)
    with patch.object(sw.workflow, 'execute_activity', execute), patch.object(sw.workflow, 'info', lambda: info), \
         patch.object(sw.workflow, 'continue_as_new', side_effect=continue_request):
        with pytest.raises(Continued):
            asyncio.run(sw.StorageLifecycle().run({'last_snapshot': 9, 'pending_export': fixed}))
    assert len(calls) == 1


def test_only_catalog_lock_contention_is_a_normal_wait():
    with patch.object(sw.activity, 'heartbeat'), patch.object(sw, 'activity_lease', lambda **_kw: nullcontext()), \
         patch.object(sw, 'configuration', lambda: {}), \
         patch.object(sw, 'register', side_effect=sw.CatalogOwnerBusy('owned')):
        assert sw.register_versions() == {'state': 'waiting_catalog_owner', 'retry_after_seconds': 15}
    with patch.object(sw.activity, 'heartbeat'), patch.object(sw, 'activity_lease', lambda **_kw: nullcontext()), \
         patch.object(sw, 'configuration', lambda: {}), \
         patch.object(sw, 'register', side_effect=BlockingIOError('unrelated I/O')):
        with pytest.raises(BlockingIOError, match='unrelated I/O'):
            sw.register_versions()


def test_activity_lease_covers_entire_activity_and_releases(tmp_path):
    lock = tmp_path / 'activity-lease.lock'
    with patch.object(sw.activity, 'heartbeat'), patch.object(sw, 'configuration', lambda: {'state_root': str(tmp_path)}):
        with sw.activity_lease():
            with lock.open('r+b') as other, pytest.raises(BlockingIOError):
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
    with lock.open('r+b') as other:
        fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_activity_lease_exits_when_worker_stops_during_handoff(tmp_path):
    lock = tmp_path / 'activity-lease.lock'
    with lock.open('w+b') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with patch.object(sw.activity, 'heartbeat'), patch.object(sw, 'configuration', lambda: {'state_root': str(tmp_path)}), \
             patch.object(sw.activity, 'is_cancelled', lambda: False), \
             patch.object(sw.activity, 'is_worker_shutdown', lambda: True):
            with pytest.raises(InterruptedError):
                with sw.activity_lease():
                    pytest.fail('must not start an activity under a handoff owner')


def test_full_cold_volume_waits_without_publishing_a_snapshot():
    with patch.object(sw.activity, 'heartbeat'), patch.object(sw, 'activity_lease', lambda **_kw: nullcontext()), \
         patch.object(sw, 'configuration', lambda: {}), \
         patch.object(sw, 'register', side_effect=sw.CatalogColdCapacity('full')):
        assert sw.register_versions() == {'state': 'waiting_cold_physical_capacity', 'retry_after_seconds': 60}


@pytest.mark.parametrize('cancelled,shutdown', [(False, False), (True, False), (False, True)])
def test_version_activity_forwards_actual_temporal_stop_state(cancelled, shutdown):
    def advance(_repo, configuration, *, stop_requested):
        assert configuration == {'authority': 'original'}
        assert stop_requested() is (cancelled or shutdown)
        return {'state': 'processing'}
    with patch.object(sw.activity, 'heartbeat'), patch.object(sw, 'activity_lease', lambda: nullcontext()), \
         patch.object(sw, 'configuration', lambda: {'authority': 'original'}), \
         patch.object(sw.activity, 'is_cancelled', lambda: cancelled), \
         patch.object(sw.activity, 'is_worker_shutdown', lambda: shutdown), \
         patch.object(sw, 'advance_catalog_versions', advance):
        assert sw.advance_dataset_versions() == {'state': 'processing'}


@pytest.mark.parametrize('wait_state,delay', [('waiting_catalog_owner', 15),
                                               ('waiting_cold_physical_capacity', 60)])
def test_catalog_busy_wait_retries_without_exporting_unaccepted_data(wait_state, delay):
    calls, sleeps = [], []
    async def execute(handler, *args, **options):
        calls.append((handler, options))
        if len(calls) == 1:
            return {'state': wait_state, 'retry_after_seconds': delay}
        raise Continued({'next_activity': handler.__name__})
    async def sleep(duration):
        sleeps.append(duration)
    with patch.object(sw.workflow, 'execute_activity', execute), \
         patch.object(sw.workflow, 'patched', lambda _name: True), \
         patch.object(sw.workflow, 'sleep', sleep):
        with pytest.raises(Continued) as continued:
            asyncio.run(sw.StorageLifecycle().run({'last_snapshot': 9}))
    assert continued.value.request == {'next_activity': 'register_versions'}
    assert [handler for handler, _ in calls] == [sw.register_versions, sw.register_versions]
    assert sleeps == [timedelta(seconds=delay)]
    assert calls[0][1]['start_to_close_timeout'] == timedelta(hours=24)


@pytest.mark.parametrize('patched,expected', [(False, timedelta(minutes=20)), (True, timedelta(hours=24))])
def test_catalog_timeout_retains_history_compatibility(patched, expected):
    calls = []
    async def execute(handler, *args, **options):
        calls.append((handler, options))
        raise Continued({})
    with patch.object(sw.workflow, 'execute_activity', execute), \
         patch.object(sw.workflow, 'patched', lambda _name: patched):
        with pytest.raises(Continued):
            asyncio.run(sw.StorageLifecycle().run({}))
    assert calls[0][0] == sw.register_versions
    assert calls[0][1]['start_to_close_timeout'] == expected


@pytest.mark.parametrize('patched,expected', [(False, timedelta(minutes=20)), (True, timedelta(hours=24))])
def test_export_timeout_retains_history_and_the_fixed_snapshot(patched, expected):
    calls = []
    async def execute(handler, *args, **options):
        calls.append((handler, args, options))
        if handler == sw.register_versions:
            return {'snapshot_id': 1060}
        raise Continued({})
    def patch_state(name):
        return patched if name == 'storage-long-catalog-export-v1' else True
    with patch.object(sw.workflow, 'execute_activity', execute), \
         patch.object(sw.workflow, 'patched', patch_state):
        with pytest.raises(Continued):
            asyncio.run(sw.StorageLifecycle().run({'last_snapshot': 1059}))
    assert calls[1][0] == sw.export_version
    assert calls[1][1] == (() if patched else (1060,))
    assert calls[1][2].get('args') == ([1060, True] if patched else None)
    assert calls[1][2]['start_to_close_timeout'] == expected
    assert calls[1][2]['heartbeat_timeout'] == timedelta(seconds=60)


def test_legacy_export_activity_requests_reschedule_without_reading_or_writing():
    with patch.object(sw, 'export', side_effect=AssertionError('short activity must not start a copy')):
        assert sw.export_version(1060) == {'snapshot_id': 1060, 'delivery_identity_sha256': None,
                                           'state': 'reschedule_with_long_timeout'}
    with pytest.raises(ValueError, match='boolean'):
        sw.export_version(1060, 1)


@pytest.mark.parametrize('write_failure', [False, True])
def test_export_observation_is_attempt_bound_and_cannot_break_work(tmp_path, write_failure):
    import json
    from stockagent.data_sync.work_progress import report_work
    info = SimpleNamespace(workflow_run_id='run-1', activity_id='30', attempt=3)
    original = sw.atomic_write_json
    def write(*args, **kwargs):
        if write_failure:
            raise OSError('monitor disk unavailable')
        return original(*args, **kwargs)
    with patch.object(sw.activity, 'in_activity', return_value=True), \
         patch.object(sw.activity, 'info', return_value=info), patch.object(sw, 'atomic_write_json', write):
        with sw.observe_catalog_export({'state_root': str(tmp_path)}, 1061):
            report_work('catalog_copy', done=0, total=2)
            report_work('catalog_copy', done=2, total=2)
    path = tmp_path / 'catalog-export-progress.json'
    if write_failure:
        assert not path.exists()
    else:
        value = json.loads(path.read_bytes())
        assert (value['workflow_run_id'], value['activity_id'], value['attempt'], value['snapshot_id']) == ('run-1', '30', 3, 1061)
        assert (value['phase'], value['completed_items'], value['total_items']) == ('catalog_copy', 2, 2)


@pytest.mark.parametrize('cancelled,shutdown,expected', [(False, False, True), (True, False, False), (False, True, False)])
def test_fixed_export_wait_is_heartbeat_owned_and_honors_worker_stop(tmp_path, cancelled, shutdown, expected):
    observed = []
    def export(c, snapshot, *, on_owner_busy):
        observed.append((c, snapshot, on_owner_busy()))
        return {'snapshot_id': snapshot, 'delivery_identity_sha256': None, 'state': 'catalog_advanced'}
    with patch.object(sw.activity, 'heartbeat'), patch.object(sw, 'activity_lease', lambda: nullcontext()), \
         patch.object(sw, 'configuration', lambda: {'authority': 'fixed', 'state_root': str(tmp_path)}), patch.object(sw, 'export', export), \
         patch.object(sw.activity, 'is_cancelled', lambda: cancelled), \
         patch.object(sw.activity, 'is_worker_shutdown', lambda: shutdown), patch.object(sw.time, 'sleep'):
        assert sw.export_version(1060, True)['snapshot_id'] == 1060
    assert observed == [({'authority': 'fixed', 'state_root': str(tmp_path)}, 1060, expected)]


def test_legacy_scheduled_export_requeues_the_identical_snapshot_with_long_timeout():
    calls = []
    async def execute(handler, *args, **options):
        calls.append((handler, args, options))
        if handler == sw.register_versions:
            return {'snapshot_id': 1060}
        if len(calls) == 2:
            assert handler == sw.export_version and args == (1060,)
            return {'snapshot_id': 1060, 'delivery_identity_sha256': None,
                    'state': 'reschedule_with_long_timeout'}
        raise Continued({})
    def patch_state(name):
        return name != 'storage-long-catalog-export-v1'
    with patch.object(sw.workflow, 'execute_activity', execute), \
         patch.object(sw.workflow, 'patched', patch_state):
        with pytest.raises(Continued):
            asyncio.run(sw.StorageLifecycle().run({'last_snapshot': 1059}))
    assert calls[2][0] == sw.export_version and calls[2][1] == ()
    assert calls[2][2]['args'] == [1060, True]
    assert calls[2][2]['start_to_close_timeout'] == timedelta(hours=24)
    assert calls[2][2]['heartbeat_timeout'] == timedelta(seconds=60)


def test_legacy_reschedule_cannot_replace_the_fixed_snapshot():
    calls = []
    async def execute(handler, *args, **options):
        calls.append(handler)
        if handler == sw.register_versions:
            return {'snapshot_id': 1060}
        return {'snapshot_id': 1061, 'delivery_identity_sha256': None,
                'state': 'reschedule_with_long_timeout'}
    with patch.object(sw.workflow, 'execute_activity', execute), \
         patch.object(sw.workflow, 'patched', lambda name: name != 'storage-long-catalog-export-v1'):
        with pytest.raises(ValueError, match='changed the fixed snapshot'):
            asyncio.run(sw.StorageLifecycle().run({'last_snapshot': 1059}))
    assert calls == [sw.register_versions, sw.export_version]


@pytest.mark.parametrize('patched,expected', [(False, 2), (True, 24)])
def test_source_replication_timeout_retains_history_compatibility(patched, expected):
    calls = []
    async def execute(handler, *args, **options):
        calls.append((handler, options))
        raise Continued({})
    with patch.object(sw.workflow, 'execute_activity', execute), \
         patch.object(sw.workflow, 'patched', lambda _name: patched):
        with pytest.raises(Continued):
            asyncio.run(sw.SourceReplicationLifecycle().run({}))
    assert calls[0][0] == sw.stage_source_replication
    assert calls[0][1]['start_to_close_timeout'] == timedelta(hours=expected)

"""Fixed pending delivery survives a history-bounded NAS outage continuation."""
import asyncio
from contextlib import nullcontext
from datetime import timedelta
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
    with patch.object(sw.activity, 'heartbeat'), patch.object(sw, 'activity_lease', lambda: nullcontext()), \
         patch.object(sw, 'configuration', lambda: {}), \
         patch.object(sw, 'register', side_effect=sw.CatalogOwnerBusy('owned')):
        assert sw.register_versions() == {'state': 'waiting_catalog_owner', 'retry_after_seconds': 15}
    with patch.object(sw.activity, 'heartbeat'), patch.object(sw, 'activity_lease', lambda: nullcontext()), \
         patch.object(sw, 'configuration', lambda: {}), \
         patch.object(sw, 'register', side_effect=BlockingIOError('unrelated I/O')):
        with pytest.raises(BlockingIOError, match='unrelated I/O'):
            sw.register_versions()


def test_catalog_busy_wait_retries_without_exporting_unaccepted_data():
    calls, sleeps = [], []
    async def execute(handler, *args, **options):
        calls.append((handler, options))
        if len(calls) == 1:
            return {'state': 'waiting_catalog_owner', 'retry_after_seconds': 15}
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
    assert sleeps == [timedelta(seconds=15)]
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

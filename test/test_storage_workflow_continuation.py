"""Fixed pending delivery survives a history-bounded NAS outage continuation."""
import asyncio
from contextlib import nullcontext
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


def test_nas_outage_continuation_keeps_exact_unaccepted_delivery():
    fixed = {'snapshot_id': 10, 'delivery_identity_sha256': 'a' * 64, 'state': 'published'}
    async def execute(handler, *args, **options):
        if handler == sw.register_versions: return {'snapshot_id': 10}
        if handler == sw.export_version: return fixed
        assert handler == sw.check_nas_acceptance and args == ('a' * 64,)
        return {'state': 'waiting_nas_archive_acceptance'}
    info = SimpleNamespace(is_continue_as_new_suggested=lambda: False, get_current_history_length=lambda: 5000)
    with patch.object(sw.workflow, 'execute_activity', execute), patch.object(sw.workflow, 'info', lambda: info), \
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

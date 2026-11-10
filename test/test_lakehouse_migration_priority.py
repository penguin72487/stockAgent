"""A migration catalog yields to trading without weakening its owner gates."""
from contextlib import nullcontext
from datetime import datetime
import json
from zoneinfo import ZoneInfo

import pytest

from stockagent.control import lakehouse


@pytest.fixture
def migration(tmp_path, monkeypatch):
    state = tmp_path / "state"
    state.mkdir()
    pause = state / "source-replication-migration-pause.json"
    pause.write_text(json.dumps({"contract": "stockagent.source_replication_migration_pause.v1", "paused": True}))
    pause.chmod(0o600)
    from scripts import storage_priority_cache
    from stockagent.live import market_status
    monkeypatch.setattr(storage_priority_cache, "evaluate_cached", lambda now, root, compute: compute())
    monkeypatch.setattr(market_status, "verified_tw_stock_session_day",
                        lambda day, **kwargs: (day.weekday() < 5, "official TWSE schedule as-of test"))
    return {"state_root": str(state), "cold_root": "/srv/stockagent-packed"}, pause


@pytest.mark.parametrize("hour,minute,protected", [(6, 19, False), (6, 20, True),
                                                    (9, 0, True), (13, 29, True),
                                                    (13, 30, False)])
def test_migration_catalog_respects_trading_window(migration, hour, minute, protected):
    config, _ = migration
    now = datetime(2026, 11, 9, hour, minute, tzinfo=ZoneInfo("Asia/Taipei"))
    assert lakehouse._migration_catalog_trading_deferred(config, now=now) is protected


def test_migration_catalog_runs_on_verified_closed_day(migration):
    config, _ = migration
    now = datetime(2026, 11, 8, 9, 0, tzinfo=ZoneInfo("Asia/Taipei"))
    assert lakehouse._migration_catalog_trading_deferred(config, now=now) is False


def test_unpaused_and_other_authorities_are_not_deferred(migration):
    config, pause = migration
    now = datetime(2026, 11, 9, 9, 0, tzinfo=ZoneInfo("Asia/Taipei"))
    assert not lakehouse._migration_catalog_trading_deferred({**config, "cold_root": "/tmp/other"}, now=now)
    pause.unlink()
    assert not lakehouse._migration_catalog_trading_deferred(config, now=now)


def test_invalid_pause_does_not_become_permission_to_scan(migration):
    config, pause = migration
    pause.write_text('{"paused": true}')
    with pytest.raises(ValueError, match="pause identity"):
        lakehouse._migration_catalog_trading_deferred(config)


def test_activity_uses_existing_replay_safe_wait_with_explicit_reason(monkeypatch, tmp_path):
    pytest.importorskip("temporalio")
    from stockagent.control import storage_workflow
    monkeypatch.setattr(storage_workflow.activity, "heartbeat", lambda *args: None)
    monkeypatch.setattr(storage_workflow, "activity_lease", lambda **kwargs: nullcontext())
    monkeypatch.setattr(storage_workflow, "configuration", lambda: {"state_root": str(tmp_path)})
    def deferred(_config):
        raise lakehouse.CatalogTradingPriority("trading")
    monkeypatch.setattr(storage_workflow, "register", deferred)
    assert storage_workflow.register_versions() == {
        "state": "waiting_catalog_owner", "reason": "tw_day_trade_priority_window", "retry_after_seconds": 60}
    status = json.loads((tmp_path / "catalog-activity-observation.json").read_text())
    assert status['state'] == 'waiting' and status['reason'] == 'tw_day_trade_priority_window'
    assert status['catalog_acceptance_claimed'] is False


def test_failed_catalog_is_reported_without_hiding_original_error(monkeypatch, tmp_path):
    pytest.importorskip("temporalio")
    from stockagent.control import storage_workflow
    monkeypatch.setattr(storage_workflow.activity, "heartbeat", lambda *args: None)
    monkeypatch.setattr(storage_workflow, "activity_lease", lambda **kwargs: nullcontext())
    monkeypatch.setattr(storage_workflow, "configuration", lambda: {"state_root": str(tmp_path)})
    def failed(_config):
        raise ValueError('private error is not public')
    monkeypatch.setattr(storage_workflow, "register", failed)
    with pytest.raises(ValueError, match='private error'):
        storage_workflow.register_versions()
    status = json.loads((tmp_path / "catalog-activity-observation.json").read_text())
    assert status['state'] == 'blocked' and status['reason'] == 'catalog_refresh_failed'
    assert 'private error' not in json.dumps(status)


def test_new_catalog_backup_waits_while_user_pauses_migration(monkeypatch, tmp_path):
    pytest.importorskip('temporalio')
    from stockagent.control import storage_workflow
    monkeypatch.setattr(storage_workflow, 'configuration', lambda: {'state_root': str(tmp_path)})
    monkeypatch.setattr(storage_workflow, '_migration_pause_requested', lambda root: True)
    assert storage_workflow.export_version(1099, True) == {
        'snapshot_id': 1099, 'delivery_identity_sha256': None, 'state': 'waiting_user_migration'}
    assert storage_workflow.export_version(1099, False)['state'] == 'reschedule_with_long_timeout'


def test_workflow_sandbox_imports_while_previous_worker_module_is_loaded(monkeypatch):
    """Temporal can reload workflow source before the running owner hands off."""
    pytest.importorskip('temporalio')
    import runpy
    monkeypatch.delattr(lakehouse, 'CatalogTradingPriority')
    monkeypatch.delattr(lakehouse, '_migration_pause_requested')
    loaded = runpy.run_path('stockagent/control/storage_workflow.py')
    assert issubclass(loaded['CatalogTradingPriority'], Exception)
    assert not issubclass(loaded['CatalogTradingPriority'], lakehouse.CatalogOwnerBusy)
    assert loaded['_migration_pause_requested'](None) is False


def test_paused_export_retries_fixed_snapshot_without_rescanning_catalog(monkeypatch):
    pytest.importorskip('temporalio')
    import asyncio
    from stockagent.control import storage_workflow as storage
    calls, delays = [], []
    class Finished(Exception):
        pass
    async def execute(fn, *args, **kwargs):
        calls.append((fn.__name__, args, kwargs.get('args')))
        if fn is storage.register_versions:
            return {'snapshot_id': 1099}
        if fn is storage.export_version:
            return {'state': 'waiting_user_migration', 'snapshot_id': 1099,
                    'delivery_identity_sha256': None}
        pytest.fail('unexpected activity')
    async def sleep(delay):
        delays.append(delay)
        if len(delays) == 3:
            raise Finished()
    monkeypatch.setattr(storage.workflow, 'execute_activity', execute)
    monkeypatch.setattr(storage.workflow, 'sleep', sleep)
    monkeypatch.setattr(storage.workflow, 'patched', lambda _name: True)
    with pytest.raises(Finished):
        asyncio.run(storage.StorageLifecycle().run({}))
    assert [call[0] for call in calls] == [
        'register_versions', 'export_version', 'export_version', 'export_version']
    assert all(call[2] == [1099, True] for call in calls[1:])


def test_paused_snapshot_is_preserved_on_workflow_continuation(monkeypatch):
    pytest.importorskip('temporalio')
    import asyncio
    from stockagent.control import storage_workflow as storage
    class Continued(Exception):
        pass
    async def execute(fn, *args, **kwargs):
        if fn is storage.register_versions:
            return {'snapshot_id': 1099}
        return {'state': 'waiting_user_migration', 'snapshot_id': 1099,
                'delivery_identity_sha256': None}
    async def sleep(_delay):
        return None
    def continue_as_new(request):
        raise Continued(request)
    monkeypatch.setattr(storage.workflow, 'execute_activity', execute)
    monkeypatch.setattr(storage.workflow, 'sleep', sleep)
    monkeypatch.setattr(storage.workflow, 'patched', lambda _name: True)
    monkeypatch.setattr(storage.workflow, 'continue_as_new', continue_as_new)
    with pytest.raises(Continued) as caught:
        asyncio.run(storage.StorageLifecycle().run({}))
    assert caught.value.args[0] == {'last_snapshot': -1, 'pending_snapshot': 1099}

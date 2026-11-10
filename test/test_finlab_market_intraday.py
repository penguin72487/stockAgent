from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path
import sqlite3
import fcntl
from unittest.mock import patch

import pandas as pd
import polars as pl
import pytest
from finlab.exceptions import DataError

from scripts.download_finlab_market_intraday import (
    _account_sync_lock, _eligible, _open_index, _record, _summarize, _target_count, _repair_derived_errors,
    annotate_daily_close_coverage, load_universe, session_days, sync_market,
)
from scripts.download_finlab_history import safe_stem
from scripts.download_finlab_intraday import _partition_paths
from stockagent.data.finlab_acquisition_contract import intraday_progress


@pytest.mark.parametrize('general_allowed,sdk_supported', [(False, True), (True, True), (False, False)])
def test_futures_priority_runs_under_existing_minute_owner_gates(tmp_path, monkeypatch, general_allowed, sdk_supported):
    from downloader.artifact_io import atomic_write_json
    from downloader.provider_priority import CONTRACT, register
    from scripts import download_finlab_market_intraday as module

    public = tmp_path / 'public'
    public.mkdir()
    _fixture(public)
    root = tmp_path / 'finlab'
    root.mkdir()
    db = root / 'intraday/market_index.sqlite3'
    db.parent.mkdir()
    module._open_index(db).close()
    now = datetime.now(UTC)
    key, day = 'tw_futures_minute:I5F_201611', date(2016, 11, 8)
    request = {'contract': CONTRACT, 'provider': 'finlab', 'reason': 'source verification',
        'created_at_utc': (now-timedelta(seconds=1)).isoformat(),
        'expires_at_utc': (now+timedelta(hours=1)).isoformat(),
        'items': [{'dataset': key, 'partition': day.isoformat()}]}
    register(root, request, now=now)
    calls = []

    def fetch(output, dataset, partition, *, now, force_download=False):
        calls.append((dataset, partition, force_download))
        receipt = {'dataset': dataset, 'trade_date': partition.isoformat(),
                   'status': 'verified_no_trade', 'rows': 0}
        atomic_write_json(_partition_paths(output, dataset, partition)[0], receipt)
        return receipt

    monkeypatch.setattr(module, 'credential_available', lambda: True)
    monkeypatch.setattr(module, '_general_work_admission', lambda *a, **kw:
        {'minute_residual_allowed': general_allowed, 'supplemental_allowed': general_allowed})
    monkeypatch.setattr(module, 'quota_room_mb', lambda: (1000, 5000))
    monkeypatch.setattr(module, 'opening_admission', lambda *a, **kw: {'allowed': True})
    monkeypatch.setattr(module, '_supports_futures_partition', lambda key: sdk_supported)
    result = module.sync_market(root, public, start=date(2026, 8, 19), end=date(2026, 9, 24),
        limit=1, reserve_mb=50, minimum_free_gb=0, now=now, fetch=fetch, kind='tw_minute')
    if general_allowed:
        assert calls == [(key, day, False)]
        assert result['futures_priority_attempts'] == result['futures_priority_successes'] == 1
        with sqlite3.connect(db) as con:
            assert con.execute("SELECT status FROM partitions WHERE kind='tw_futures_minute'").fetchone()[0] == 'verified_no_trade'
    else:
        assert calls == []
        assert result['state'] == 'waiting_required_general_data'
        assert result['futures_priority_attempts'] == 0
        assert result['futures_priority_sdk_unsupported'] is (not sdk_supported)


@pytest.mark.parametrize('registered', [False, True])
def test_finite_priority_pass_never_scans_unregistered_history(tmp_path, monkeypatch, registered):
    from downloader.artifact_io import atomic_write_json
    from downloader.provider_priority import CONTRACT, register
    from scripts import download_finlab_market_intraday as module

    public = tmp_path / 'public'
    public.mkdir()
    _fixture(public)
    root = tmp_path / 'finlab'
    db = root / 'intraday/market_index.sqlite3'
    db.parent.mkdir(parents=True)
    module._open_index(db).close()
    cursor = root / 'intraday/minute_cursor.json'
    market_status = root / 'intraday/market_status.json'
    history_metrics = root / 'intraday/batch_metrics.json'
    atomic_write_json(cursor, {'old_frontier': 'preserve'})
    atomic_write_json(market_status, {'scope': 'full_market', 'state': 'paused'})
    atomic_write_json(history_metrics, {'contract_version': 1, 'batches': [{'scope': 'full_market'}]})
    before_cursor, before_status = cursor.read_bytes(), market_status.read_bytes()
    before_metrics = history_metrics.read_bytes()
    now = datetime.now(UTC)
    key, day = 'tw_futures_minute:I5F_201611', date(2016, 11, 8)
    if registered:
        register(root, {'contract': CONTRACT, 'provider': 'finlab', 'reason': 'verify exact futures partition',
            'created_at_utc': (now-timedelta(seconds=1)).isoformat(),
            'expires_at_utc': (now+timedelta(hours=1)).isoformat(),
            'items': [{'dataset': key, 'partition': day.isoformat()}]}, now=now)
    calls = []

    def fetch(output, dataset, partition, *, now, force_download=False):
        calls.append((dataset, partition))
        receipt = {'dataset': dataset, 'trade_date': partition.isoformat(),
                   'status': 'verified_no_trade', 'rows': 0}
        atomic_write_json(_partition_paths(output, dataset, partition)[0], receipt)
        return receipt

    monkeypatch.setattr(module, 'credential_available', lambda: True)
    monkeypatch.setattr(module, '_general_work_admission', lambda *a, **kw:
        {'minute_residual_allowed': True, 'supplemental_allowed': True})
    monkeypatch.setattr(module, 'quota_room_mb', lambda: (1000, 5000))
    monkeypatch.setattr(module, 'opening_admission', lambda *a, **kw: {'allowed': True})
    monkeypatch.setattr(module, '_supports_futures_partition', lambda key: True)
    result = module.sync_market(root, public, start=date(2026, 8, 19), end=date(2026, 9, 24),
        limit=64, reserve_mb=50, minimum_free_gb=0, now=now, fetch=fetch, kind='tw_minute',
        finite_priority_only=True)
    assert calls == ([(key, day)] if registered else [])
    assert result['state'] == 'finite_priority_pass_complete'
    assert result['selection_scope'] == 'finite_registered_priority'
    assert result['futures_priority_attempts'] == int(registered)
    assert cursor.read_bytes() == before_cursor
    assert market_status.read_bytes() == before_status
    assert history_metrics.read_bytes() == before_metrics
    assert json.loads((root / 'intraday/priority_status_tw_minute.json').read_text()) == result


@pytest.mark.parametrize('boundary', ['daytrade', 'expired', 'physical_capacity'])
def test_finite_priority_stops_at_handoff_expiry_and_backing_reserve(tmp_path, monkeypatch, boundary):
    from types import SimpleNamespace
    from downloader.artifact_io import atomic_write_json
    from downloader import provider_priority
    from downloader.provider_priority import CONTRACT, register
    from scripts import download_finlab_market_intraday as module
    from stockagent.data import finlab_gap_priority

    public = tmp_path / 'public'
    public.mkdir()
    _fixture(public)
    root = tmp_path / 'finlab'
    db = root / 'intraday/market_index.sqlite3'
    db.parent.mkdir(parents=True)
    module._open_index(db).close()
    now = datetime.now(UTC)
    key = 'tw_futures_minute:I5F_201611'
    dates = [date(2016, 11, 8), date(2016, 11, 9)]
    register(root, {'contract': CONTRACT, 'provider': 'finlab', 'reason': 'verify exact futures partitions',
        'created_at_utc': (now-timedelta(seconds=1)).isoformat(),
        'expires_at_utc': (now+timedelta(hours=1)).isoformat(),
        'items': [{'dataset': key, 'partition': day.isoformat()} for day in dates]}, now=now)
    calls = []
    clock = [100.0]
    real_time = module.time
    monkeypatch.setattr(module, 'time', SimpleNamespace(monotonic=lambda: clock[0], time=real_time.time))

    def fetch(output, dataset, partition, *, now, force_download=False):
        calls.append((dataset, partition))
        receipt = {'dataset': dataset, 'trade_date': partition.isoformat(),
                   'status': 'verified_no_trade', 'rows': 0}
        atomic_write_json(_partition_paths(output, dataset, partition)[0], receipt)
        clock[0] += 61
        return receipt

    monkeypatch.setattr(module, 'credential_available', lambda: True)
    monkeypatch.setattr(module, '_general_work_admission', lambda *a, **kw:
        {'minute_residual_allowed': True, 'supplemental_allowed': True})
    monkeypatch.setattr(module, 'quota_room_mb', lambda: (1000, 5000))
    monkeypatch.setattr(module, 'opening_admission', lambda *a, **kw: {'allowed': True})
    monkeypatch.setattr(module, '_supports_futures_partition', lambda key: True)
    if boundary == 'daytrade':
        monkeypatch.setattr(finlab_gap_priority, 'pending_daytrade_priority', lambda *a, **kw: True)
    elif boundary == 'expired':
        candidates = iter([[(key, dates[0])], []])
        monkeypatch.setattr(provider_priority, 'finlab_futures_candidates', lambda *a, **kw: next(candidates))
    else:
        monkeypatch.setattr(module, 'check_dispatch', lambda *a, **kw: 75)
    result = module.sync_market(root, public, start=date(2026, 8, 19), end=date(2026, 9, 24),
        limit=64, reserve_mb=50, minimum_free_gb=0, now=now, fetch=fetch, kind='tw_minute',
        finite_priority_only=True)
    if boundary == 'physical_capacity':
        assert calls == [(key, dates[0])]
        assert result['state'] == 'capacity_deferred'
    else:
        assert not calls
        assert result['state'] == ('priority_handoff' if boundary == 'daytrade' else 'finite_priority_pass_complete')


@pytest.mark.parametrize('catalog_outcome', ['fresh', 'failure', 'handoff'])
def test_finite_priority_refreshes_stale_catalog_only_with_account_work_due(
        tmp_path, monkeypatch, capsys, catalog_outcome):
    from downloader.provider_priority import CONTRACT, register
    from scripts import download_finlab_market_intraday as module
    from stockagent.data import finlab_gap_priority

    public = tmp_path / 'public'
    public.mkdir()
    _fixture(public)
    root = tmp_path / 'finlab'
    db = root / 'intraday/market_index.sqlite3'
    db.parent.mkdir(parents=True)
    module._open_index(db).close()
    discovery = root / 'catalog/discovery.json'
    discovery.parent.mkdir()
    discovery.write_text('{"observed_at_utc":"old-source-evidence"}')
    now = datetime.now(UTC)
    key, day = 'tw_futures_minute:I5F_201611', date(2016, 11, 8)
    register(root, {'contract': CONTRACT, 'provider': 'finlab', 'reason': 'verify exact futures partition',
        'created_at_utc': (now-timedelta(seconds=1)).isoformat(),
        'expires_at_utc': (now+timedelta(hours=1)).isoformat(),
        'items': [{'dataset': key, 'partition': day.isoformat()}]}, now=now)
    calls = []

    def catalog():
        calls.append('discovery')
        if catalog_outcome == 'failure':
            raise RuntimeError('private account secret')
        return ['price:close']

    def admission(*args, **kwargs):
        if json.loads(discovery.read_text()).get('observed_at_utc') == 'old-source-evidence':
            return {'state': 'discovery_unverified'}
        return {'state': 'general_work_pending', 'minute_residual_allowed': False,
                'supplemental_allowed': False, 'actionable_pending': 1}

    monkeypatch.setattr(module, 'credential_available', lambda: True)
    monkeypatch.setattr(module, '_general_work_admission', admission)
    monkeypatch.setattr(module, 'provider_catalog', catalog)
    monkeypatch.setattr(module, 'load_catalog', lambda: {'datasets': [{'key': 'price:close'}]})
    monkeypatch.setattr(module, 'opening_admission', lambda *a, **kw: {'allowed': True})
    monkeypatch.setattr(module, '_supports_futures_partition', lambda key: True)
    monkeypatch.setattr(finlab_gap_priority, 'pending_daytrade_priority',
                        lambda *a, **kw: catalog_outcome == 'handoff')
    result = module.sync_market(root, public, start=date(2026, 8, 19), end=date(2026, 9, 24),
        limit=64, reserve_mb=50, minimum_free_gb=0, now=now, kind='tw_minute',
        fetch=lambda *a, **kw: pytest.fail('formal due work must retain priority'), finite_priority_only=True)
    if catalog_outcome == 'fresh':
        assert calls == ['discovery']
        assert json.loads(discovery.read_text())['keys'] == ['price:close']
        assert result['state'] == 'waiting_required_general_data'
        assert result['catalog_refresh'] == 'observed'
    else:
        assert json.loads(discovery.read_text())['observed_at_utc'] == 'old-source-evidence'
        assert calls == ([] if catalog_outcome == 'handoff' else ['discovery'])
        assert result['state'] == ('priority_handoff' if catalog_outcome == 'handoff' else 'priority_catalog_unavailable')
        assert 'private account secret' not in capsys.readouterr().out
    assert result['futures_priority_attempts'] == 0


@pytest.mark.parametrize('ordinary,quota,priority_guard,expected,state', [
    ({'state': 'deferred', 'reason': 'physical_backing_reserve',
      'physical_free_bytes': 40, 'required_bytes_including_reserve': 64}, 2500, 0, 2, 'finite_priority_pass_complete'),
    ({'state': 'deferred', 'reason': 'filesystem_reserve',
      'available_bytes': 40, 'required_bytes': 64}, 2500, 0, 2, 'finite_priority_pass_complete'),
    ({'state': 'admitted'}, 2500, 0, 0, 'waiting_required_general_data'),
    ({'state': 'deferred', 'reason': 'physical_backing_headroom_unconfirmed'}, 2500, 0, 0, 'waiting_required_general_data'),
    ({'state': 'deferred', 'reason': 'physical_backing_reserve',
      'physical_free_bytes': 65, 'required_bytes_including_reserve': 64}, 2500, 0, 0, 'waiting_required_general_data'),
    ({'state': 'deferred', 'reason': 'physical_backing_reserve',
      'physical_free_bytes': 40, 'required_bytes_including_reserve': 64}, 1999, 0, 0, 'quota_margin_reached'),
    ({'state': 'deferred', 'reason': 'physical_backing_reserve',
      'physical_free_bytes': 40, 'required_bytes_including_reserve': 64}, 2500, 75, 0, 'capacity_deferred'),
])
def test_explicit_capacity_deferred_minute_is_bounded_and_retains_other_guards(
        tmp_path, monkeypatch, ordinary, quota, priority_guard, expected, state):
    from downloader.artifact_io import atomic_write_json
    from downloader.provider_priority import CONTRACT, register
    from scripts import download_finlab_market_intraday as module

    public = tmp_path / 'public'
    public.mkdir()
    _fixture(public)
    root = tmp_path / 'finlab'
    db = root / 'intraday/market_index.sqlite3'
    db.parent.mkdir(parents=True)
    module._open_index(db).close()
    now = datetime.now(UTC)
    key = 'tw_futures_minute:I5F_201611'
    days = [date(2016, 11, 8), date(2016, 11, 9)]
    register(root, {'contract': CONTRACT, 'provider': 'finlab', 'reason': 'test exact source checks',
        'created_at_utc': (now-timedelta(seconds=1)).isoformat(),
        'expires_at_utc': (now+timedelta(hours=1)).isoformat(),
        'items': [{'dataset': key, 'partition': day.isoformat()} for day in days]}, now=now)
    calls = []
    quota_checks = []

    def fetch(output, dataset, partition, *, now, force_download=False):
        calls.append(partition)
        receipt = {'dataset': dataset, 'trade_date': partition.isoformat(),
                   'status': 'verified_no_trade', 'rows': 0}
        atomic_write_json(_partition_paths(output, dataset, partition)[0], receipt)
        return receipt

    monkeypatch.setattr(module, 'credential_available', lambda: True)
    monkeypatch.setattr(module, '_general_work_admission', lambda *a, **kw:
        {'state': 'general_work_pending', 'minute_residual_allowed': False,
         'supplemental_allowed': False, 'actionable_pending': 9, 'scheduled_reserve_mb': 500})
    monkeypatch.setattr(module, 'acquisition_dispatch_status', lambda _: ordinary)
    capacity_guards = []
    monkeypatch.setattr(module, 'check_dispatch', lambda *a, **kw: capacity_guards.append(kw) or priority_guard)
    monkeypatch.setattr(module, 'quota_room_mb', lambda: quota_checks.append(True) or (quota, 5000))
    monkeypatch.setattr(module, 'opening_admission', lambda *a, **kw: {'allowed': True})
    monkeypatch.setattr(module, '_supports_futures_partition', lambda key: True)
    result = module.sync_market(root, public, start=date(2026, 8, 19), end=date(2026, 9, 24),
        limit=4, reserve_mb=0, minimum_free_gb=0, now=now, fetch=fetch, kind='tw_minute',
        finite_priority_only=True, capacity_deferred_priority=True)
    assert len(calls) == expected
    assert result['state'] == state
    assert result['general_acquisition_admission']['actionable_pending'] == 9
    assert result['general_acquisition_admission']['minute_residual_allowed'] is False
    if expected or state == 'quota_margin_reached':
        assert result['quota_policy']['effective_reserve_mb'] >= 2000
    if expected:
        assert len(quota_checks) >= expected
        assert sum(g.get('additional_priority_headroom_bytes') == 24 * 1024**3
                   for g in capacity_guards) >= expected


@pytest.mark.parametrize('guard_result', [0, 75, 'before_source'])
def test_workspace_priority_selects_futures_and_rechecks_both_volumes(tmp_path, monkeypatch, guard_result):
    from downloader.artifact_io import atomic_write_json
    from downloader.provider_priority import CONTRACT, register
    from scripts import download_finlab_market_intraday as module
    from stockagent.data import finlab_futures_sdk as adapter

    public = tmp_path / 'public'
    public.mkdir()
    _fixture(public)
    root = tmp_path / 'finlab'
    db = root / 'intraday/market_index.sqlite3'
    db.parent.mkdir(parents=True)
    module._open_index(db).close()
    now = datetime.now(UTC)
    key, day = 'tw_futures_minute:I5F_201611', date(2016, 11, 8)
    register(root, {'contract': CONTRACT, 'provider': 'finlab', 'reason': 'workspace fixture',
        'created_at_utc': (now - timedelta(seconds=1)).isoformat(),
        'expires_at_utc': (now + timedelta(hours=1)).isoformat(),
        'items': [{'dataset': key, 'partition': day.isoformat()}]}, now=now)
    calls, guards = [], []
    def fetch(output, dataset, partition, *, overlay, futures_workspace):
        assert futures_workspace is True and dataset == key
        if guard_result == 'before_source':
            raise adapter.PartitionFailure('workspace_unavailable_before_source')
        calls.append(partition)
        receipt = {'dataset': dataset, 'trade_date': partition.isoformat(),
                   'status': 'verified_no_trade', 'rows': 0}
        atomic_write_json(_partition_paths(output, dataset, partition)[0], receipt)
        return receipt
    monkeypatch.setattr(module, 'credential_available', lambda: True)
    monkeypatch.setattr(module, '_general_work_admission', lambda *a, **kw:
        {'state': 'general_work_pending', 'minute_residual_allowed': False,
         'supplemental_allowed': False, 'actionable_pending': 9, 'scheduled_reserve_mb': 500})
    monkeypatch.setattr(module, 'acquisition_dispatch_status', lambda _:
        {'state': 'deferred', 'reason': 'physical_backing_reserve',
         'physical_free_bytes': 40, 'required_bytes_including_reserve': 64})
    monkeypatch.setattr(module, 'check_futures_workspace_dispatch',
                        lambda *a, **kw: guards.append(True) or (75 if guard_result == 75 else 0))
    monkeypatch.setattr(module, 'quota_room_mb', lambda: (2500, 5000))
    monkeypatch.setattr(module, 'opening_admission', lambda *a, **kw: {'allowed': True})
    monkeypatch.setattr(module, '_supports_futures_partition',
                        lambda *a: pytest.fail('workspace must use pinned bridge'))
    monkeypatch.setattr(adapter, 'validated_overlay', lambda _: tmp_path / 'overlay')
    monkeypatch.setattr(adapter, 'fetch_with_owner_session', fetch)
    result = module.sync_market(root, public, start=date(2026, 8, 19), end=date(2026, 9, 24),
        limit=4, reserve_mb=0, minimum_free_gb=0, now=now, kind='tw_minute',
        finite_priority_only=True, capacity_deferred_priority=True, futures_workspace_only=True)
    assert result['futures_workspace_only'] is True and result['stock_priority_attempts'] == 0
    assert result['futures_priority_attempts'] == (1 if guard_result == 0 else 0)
    assert calls == ([day] if guard_result == 0 else [])
    assert guards and result['state'] == ('finite_priority_pass_complete' if guard_result == 0 else 'capacity_deferred')


@pytest.mark.parametrize('pause_results,quota,expected,state', [
    ([True, True], 2500, 2, 'finite_priority_pass_complete'),
    ([False], 2500, 0, 'waiting_required_general_data'),
    ([True, False], 2500, 1, 'waiting_required_general_data'),
    ([True], 1999, 0, 'quota_margin_reached'),
])
def test_registered_workspace_futures_yield_only_verified_paused_general_owner(
        tmp_path, monkeypatch, pause_results, quota, expected, state):
    from downloader.artifact_io import atomic_write_json
    from downloader.provider_priority import CONTRACT, register
    from downloader import paused_provider_owners
    from scripts import download_finlab_market_intraday as module
    from stockagent.data import finlab_futures_sdk as adapter

    public = tmp_path / 'public'
    public.mkdir()
    _fixture(public)
    root = tmp_path / 'finlab'
    db = root / 'intraday/market_index.sqlite3'
    db.parent.mkdir(parents=True)
    module._open_index(db).close()
    now = datetime.now(UTC)
    key = 'tw_futures_minute:I5F_201611'
    days = [date(2016, 11, 8), date(2016, 11, 9)]
    register(root, {'contract': CONTRACT, 'provider': 'finlab', 'reason': 'paused owner fixture',
        'created_at_utc': (now-timedelta(seconds=1)).isoformat(),
        'expires_at_utc': (now+timedelta(hours=1)).isoformat(),
        'items': [{'dataset': key, 'partition': day.isoformat()} for day in days]}, now=now)
    calls, checks = [], []
    observations = iter(pause_results)

    def fetch(output, dataset, partition, *, overlay, futures_workspace):
        assert futures_workspace and dataset == key
        calls.append(partition)
        receipt = {'dataset': dataset, 'trade_date': partition.isoformat(),
                   'status': 'verified_no_trade', 'rows': 0}
        atomic_write_json(_partition_paths(output, dataset, partition)[0], receipt)
        return receipt

    monkeypatch.setattr(module, 'credential_available', lambda: True)
    monkeypatch.setattr(module, '_general_work_admission', lambda *a, **kw:
        {'state': 'general_work_pending', 'minute_residual_allowed': False,
         'supplemental_allowed': False, 'actionable_pending': 9, 'scheduled_reserve_mb': 500})
    monkeypatch.setattr(module, 'acquisition_dispatch_status', lambda _: {'state': 'admitted'})
    monkeypatch.setattr(paused_provider_owners, 'explicitly_paused',
                        lambda _: frozenset({'finlab'}) if next(observations) else frozenset())
    monkeypatch.setattr(module, 'check_futures_workspace_dispatch', lambda *a, **kw: checks.append(True) or 0)
    monkeypatch.setattr(module, 'quota_room_mb', lambda: (quota, 5000))
    monkeypatch.setattr(module, 'opening_admission', lambda *a, **kw: {'allowed': True})
    monkeypatch.setattr(adapter, 'validated_overlay', lambda _: tmp_path / 'overlay')
    monkeypatch.setattr(adapter, 'fetch_with_owner_session', fetch)
    result = module.sync_market(root, public, start=date(2026, 8, 19), end=date(2026, 9, 24),
        limit=4, reserve_mb=0, minimum_free_gb=0, now=now, kind='tw_minute',
        finite_priority_only=True, capacity_deferred_priority=True, futures_workspace_only=True)
    assert len(calls) == expected and result['state'] == state
    assert result['general_acquisition_admission']['actionable_pending'] == 9
    assert checks
    if expected or state == 'quota_margin_reached':
        assert result['quota_policy']['effective_reserve_mb'] >= 2000


def test_workspace_owner_executes_the_preferred_registered_request_first(tmp_path, monkeypatch):
    from downloader.artifact_io import atomic_write_json
    from downloader.provider_priority import CONTRACT, register
    from scripts import download_finlab_market_intraday as module
    from stockagent.data import finlab_futures_sdk as adapter
    public = tmp_path / 'public'
    public.mkdir()
    _fixture(public)
    root = tmp_path / 'finlab'
    db = root / 'intraday/market_index.sqlite3'
    db.parent.mkdir(parents=True)
    module._open_index(db).close()
    now = datetime.now(UTC)
    items = [{'dataset':'tw_futures_minute:AAA_201611', 'partition':'2016-11-08'},
             {'dataset':'tw_futures_minute:ZZZ_201611', 'partition':'2016-11-09'}]
    requests = [register(root, {'contract':CONTRACT, 'provider':'finlab', 'reason':'order fixture',
        'created_at_utc':(now-timedelta(seconds=2-index)).isoformat(),
        'expires_at_utc':(now+timedelta(hours=1)).isoformat(), 'items':[item]}, now=now)
        for index, item in enumerate(items)]
    calls = []
    def fetch(output, dataset, partition, *, overlay, futures_workspace):
        calls.append(dataset)
        receipt = {'dataset':dataset, 'trade_date':partition.isoformat(), 'status':'verified_no_trade', 'rows':0}
        atomic_write_json(_partition_paths(output, dataset, partition)[0], receipt)
        return receipt
    monkeypatch.setattr(module, 'credential_available', lambda: True)
    monkeypatch.setattr(module, '_general_work_admission', lambda *a, **kw:
                        {'state':'ready', 'minute_residual_allowed':True, 'supplemental_allowed':True})
    monkeypatch.setattr(module, 'acquisition_dispatch_status', lambda _: {'state':'admitted'})
    monkeypatch.setattr(module, 'check_futures_workspace_dispatch', lambda *a, **kw: 0)
    monkeypatch.setattr(module, 'quota_room_mb', lambda: (2500, 5000))
    monkeypatch.setattr(module, 'opening_admission', lambda *a, **kw: {'allowed':True})
    monkeypatch.setattr(adapter, 'validated_overlay', lambda _: tmp_path/'overlay')
    monkeypatch.setattr(adapter, 'fetch_with_owner_session', fetch)
    result = module.sync_market(root, public, start=date(2026, 8, 19), end=date(2026, 9, 24),
        limit=1, reserve_mb=0, minimum_free_gb=0, now=now, kind='tw_minute',
        finite_priority_only=True, capacity_deferred_priority=True, futures_workspace_only=True,
        preferred_futures_request_ids=(requests[1]['request_id'],))
    assert result['futures_priority_attempts'] == 1 and calls == [items[1]['dataset']]


def test_capacity_deferred_exception_rechecks_due_dispatch_before_each_partition(tmp_path, monkeypatch):
    from downloader.artifact_io import atomic_write_json
    from downloader.provider_priority import CONTRACT, register
    from scripts import download_finlab_market_intraday as module

    public = tmp_path / 'public'
    public.mkdir()
    _fixture(public)
    root = tmp_path / 'finlab'
    db = root / 'intraday/market_index.sqlite3'
    db.parent.mkdir(parents=True)
    module._open_index(db).close()
    now = datetime.now(UTC)
    key = 'tw_futures_minute:I5F_201611'
    days = [date(2016, 11, 8), date(2016, 11, 9)]
    register(root, {'contract': CONTRACT, 'provider': 'finlab', 'reason': 'test exact source checks',
        'created_at_utc': (now-timedelta(seconds=1)).isoformat(),
        'expires_at_utc': (now+timedelta(hours=1)).isoformat(),
        'items': [{'dataset': key, 'partition': day.isoformat()} for day in days]}, now=now)
    capacity = iter([{'state': 'deferred', 'reason': 'physical_backing_reserve',
                      'physical_free_bytes': 40, 'required_bytes_including_reserve': 64},
                     {'state': 'admitted'}])
    calls = []

    def fetch(output, dataset, partition, *, now, force_download=False):
        calls.append(partition)
        receipt = {'dataset': dataset, 'trade_date': partition.isoformat(),
                   'status': 'verified_no_trade', 'rows': 0}
        atomic_write_json(_partition_paths(output, dataset, partition)[0], receipt)
        return receipt

    monkeypatch.setattr(module, 'credential_available', lambda: True)
    monkeypatch.setattr(module, '_general_work_admission', lambda *a, **kw:
        {'state': 'general_work_pending', 'minute_residual_allowed': False,
         'supplemental_allowed': False, 'actionable_pending': 9})
    monkeypatch.setattr(module, 'acquisition_dispatch_status', lambda _: next(capacity))
    monkeypatch.setattr(module, 'check_dispatch', lambda *a, **kw: 0)
    monkeypatch.setattr(module, 'quota_room_mb', lambda: (2500, 5000))
    monkeypatch.setattr(module, 'opening_admission', lambda *a, **kw: {'allowed': True})
    monkeypatch.setattr(module, '_supports_futures_partition', lambda key: True)
    result = module.sync_market(root, public, start=date(2026, 8, 19), end=date(2026, 9, 24),
        limit=4, reserve_mb=0, minimum_free_gb=0, now=now, fetch=fetch, kind='tw_minute',
        finite_priority_only=True, capacity_deferred_priority=True)
    assert len(calls) == 1
    assert result['state'] == 'waiting_required_general_data'


@pytest.mark.parametrize('kind,finite,limit', [('tw_tick', True, 4), ('tw_tick', True, 2),
                                               ('tw_tick', False, 1), ('tw_minute', False, 4),
                                               ('tw_minute', True, 5)])
def test_capacity_deferred_exception_rejects_unbounded_scope(kind, finite, limit):
    from scripts import download_finlab_market_intraday as module

    with pytest.raises(ValueError, match='at most four serial'):
        module.sync_market(Path('unused'), Path('unused'), start=date(2026, 1, 1), end=date(2026, 1, 2),
            limit=limit, reserve_mb=0, minimum_free_gb=0, now=datetime.now(UTC), kind=kind,
            finite_priority_only=finite, capacity_deferred_priority=True)


@pytest.mark.parametrize('ordinary,quota,priority_guard,handoff,expected,state', [
    ({'state': 'deferred', 'reason': 'physical_backing_reserve',
      'physical_free_bytes': 40, 'required_bytes_including_reserve': 64}, 2500, 0, False, 1, 'finite_priority_pass_complete'),
    ({'state': 'deferred', 'reason': 'filesystem_reserve',
      'available_bytes': 40, 'required_bytes': 64}, 2500, 0, False, 1, 'finite_priority_pass_complete'),
    ({'state': 'admitted'}, 2500, 0, False, 0, 'waiting_required_minutes'),
    ({'state': 'deferred', 'reason': 'physical_backing_headroom_unconfirmed'}, 2500, 0, False, 0, 'waiting_required_minutes'),
    ({'state': 'deferred', 'reason': 'physical_backing_reserve',
      'physical_free_bytes': 40, 'required_bytes_including_reserve': 64}, 1999, 0, False, 0, 'quota_margin_reached'),
    ({'state': 'deferred', 'reason': 'physical_backing_reserve',
      'physical_free_bytes': 40, 'required_bytes_including_reserve': 64}, 2500, 75, False, 0, 'capacity_deferred'),
    ({'state': 'deferred', 'reason': 'physical_backing_reserve',
      'physical_free_bytes': 40, 'required_bytes_including_reserve': 64}, 2500, 0, True, 0, 'priority_handoff'),
])
def test_explicit_capacity_deferred_single_tick_preserves_other_guards(
        tmp_path, monkeypatch, ordinary, quota, priority_guard, handoff, expected, state):
    from downloader.artifact_io import atomic_write_json
    from downloader.provider_priority import CONTRACT, register
    from scripts import download_finlab_market_intraday as module
    from stockagent.data import finlab_gap_priority

    public = tmp_path / 'public'
    public.mkdir()
    _fixture(public)
    root = tmp_path / 'finlab'
    db = root / 'intraday/market_index.sqlite3'
    db.parent.mkdir(parents=True)
    module._open_index(db).close()
    now = datetime.now(UTC)
    key, day = 'tw_tick:2330', date(2026, 8, 19)
    register(root, {'contract': CONTRACT, 'provider': 'finlab', 'reason': 'verify one stock fixing',
        'created_at_utc': (now-timedelta(seconds=1)).isoformat(),
        'expires_at_utc': (now+timedelta(hours=1)).isoformat(),
        'items': [{'dataset': key, 'partition': day.isoformat()}]}, now=now)
    calls, capacity_guards = [], []

    def fetch(output, dataset, partition, *, now):
        calls.append((dataset, partition))
        receipt = {'dataset': dataset, 'trade_date': partition.isoformat(),
                   'status': 'verified_no_trade', 'rows': 0}
        atomic_write_json(_partition_paths(output, dataset, partition)[0], receipt)
        return receipt

    monkeypatch.setattr(module, 'credential_available', lambda: True)
    monkeypatch.setattr(module, '_general_work_admission', lambda *a, **kw:
        {'state': 'general_work_pending', 'minute_residual_allowed': False,
         'supplemental_allowed': False, 'actionable_pending': 9, 'scheduled_reserve_mb': 500})
    monkeypatch.setattr(module, '_minute_admission', lambda *a, **kw: {'tick_residual_allowed': False})
    monkeypatch.setattr(module, 'acquisition_dispatch_status', lambda _: ordinary)
    monkeypatch.setattr(module, 'check_dispatch', lambda *a, **kw: capacity_guards.append(kw) or priority_guard)
    monkeypatch.setattr(module, 'quota_room_mb', lambda: (quota, 5000))
    monkeypatch.setattr(module, 'opening_admission', lambda *a, **kw: {'allowed': True})
    monkeypatch.setattr(finlab_gap_priority, 'pending_daytrade_priority', lambda *a, **kw: handoff)
    result = module.sync_market(root, public, start=date(2026, 8, 19), end=date(2026, 9, 24),
        limit=1, reserve_mb=0, minimum_free_gb=0, now=now, fetch=fetch, kind='tw_tick',
        enforce_minute_priority=True, finite_priority_only=True, capacity_deferred_priority=True)
    assert calls == ([(key, day)] if expected else [])
    assert result['state'] == state
    assert result['stock_priority_attempts'] == result['stock_priority_successes'] == expected
    if expected:
        assert result['quota_policy']['effective_reserve_mb'] >= 2000
        assert any(g.get('additional_priority_headroom_bytes') == 24 * 1024**3 for g in capacity_guards)


@pytest.mark.parametrize('admission,minutes_clear,quota,expected', [
    ({'supplemental_allowed': True, 'minute_residual_allowed': True, 'actionable_pending': 0}, True, 1000, 1),
    ({'supplemental_allowed': False, 'minute_residual_allowed': True, 'actionable_pending': 0}, True, 1000, 1),
    ({'supplemental_allowed': False, 'minute_residual_allowed': False, 'actionable_pending': 1}, True, 1000, 0),
    ({'supplemental_allowed': False, 'minute_residual_allowed': True, 'actionable_pending': 0}, False, 1000, 0),
    ({'supplemental_allowed': False, 'minute_residual_allowed': True, 'actionable_pending': 0}, True, 10, 0),
])
def test_stock_tick_priority_uses_existing_owner_and_admission(tmp_path, monkeypatch, admission, minutes_clear, quota, expected):
    from downloader.artifact_io import atomic_write_json
    from downloader.provider_priority import CONTRACT, register
    from scripts import download_finlab_market_intraday as module

    public = tmp_path / 'public'
    public.mkdir()
    _fixture(public)
    root = tmp_path / 'finlab'
    db = root / 'intraday/market_index.sqlite3'
    db.parent.mkdir(parents=True)
    module._open_index(db).close()
    now = datetime.now(UTC)
    key, day = 'tw_tick:2330', date(2026, 8, 19)
    register(root, {'contract': CONTRACT, 'provider': 'finlab', 'reason': 'verify exact stock price path',
        'created_at_utc': (now-timedelta(seconds=1)).isoformat(),
        'expires_at_utc': (now+timedelta(hours=1)).isoformat(),
        'items': [{'dataset': key, 'partition': day.isoformat()}]}, now=now)
    calls = []

    def fetch(output, dataset, partition, *, now):
        calls.append((dataset, partition))
        receipt = {'dataset': dataset, 'trade_date': partition.isoformat(),
                   'status': 'verified_no_trade', 'rows': 0}
        atomic_write_json(_partition_paths(output, dataset, partition)[0], receipt)
        return receipt

    monkeypatch.setattr(module, 'credential_available', lambda: True)
    monkeypatch.setattr(module, '_general_work_admission', lambda *a, **kw: admission)
    monkeypatch.setattr(module, '_minute_admission', lambda *a, **kw: {'tick_residual_allowed': minutes_clear})
    monkeypatch.setattr(module, 'quota_room_mb', lambda: (quota, 5000))
    monkeypatch.setattr(module, 'opening_admission', lambda *a, **kw: {'allowed': True})
    result = module.sync_market(root, public, start=date(2026, 8, 19), end=date(2026, 9, 24),
        limit=1, reserve_mb=50, minimum_free_gb=0, now=now, fetch=fetch, kind='tw_tick',
        enforce_minute_priority=True)
    assert calls == ([(key, day)] if expected else [])
    assert result['stock_priority_attempts'] == result['stock_priority_successes'] == expected
    if expected:
        with sqlite3.connect(db) as con:
            assert con.execute("SELECT status FROM partitions WHERE kind='tw_tick' AND symbol='2330'").fetchone()[0] == 'verified_no_trade'


@pytest.mark.parametrize('day,scope_written', [(date(2026, 8, 19), True), (date(2026, 8, 18), False)])
def test_stock_priority_scope_does_not_create_provider_evidence_or_suppress_outside_range(
        tmp_path, monkeypatch, day, scope_written):
    from downloader.provider_priority import CONTRACT, inspect, register
    from scripts import check_finlab_refresh_due as dispatch
    from scripts import download_finlab_market_intraday as module
    from scripts.download_finlab_intraday import _priority_scope_path

    public = tmp_path / 'public'
    public.mkdir()
    _fixture(public)
    root = tmp_path / 'finlab'
    db = root / 'intraday/market_index.sqlite3'
    db.parent.mkdir(parents=True)
    module._open_index(db).close()
    now = datetime.now(UTC)
    key = 'tw_tick:NOTEXIST'
    receipt = register(root, {'contract': CONTRACT, 'provider': 'finlab', 'reason': 'test local eligibility',
        'created_at_utc': (now-timedelta(seconds=1)).isoformat(),
        'expires_at_utc': (now+timedelta(hours=1)).isoformat(),
        'items': [{'dataset': key, 'partition': day.isoformat()}]}, now=now)
    monkeypatch.setattr(module, 'credential_available', lambda: True)
    monkeypatch.setattr(module, 'intraday_source_identity', lambda *a, **kw: 'known-source')
    monkeypatch.setattr(dispatch, 'intraday_source_identity', lambda *a, **kw: 'known-source')
    monkeypatch.setattr(module, '_general_work_admission', lambda *a, **kw:
        {'supplemental_allowed': False, 'minute_residual_allowed': False, 'actionable_pending': 1})
    monkeypatch.setattr(module, 'opening_admission', lambda *a, **kw: {'allowed': True})
    monkeypatch.setattr(module, 'quota_room_mb', lambda: pytest.fail('scope must not query provider quota'))
    module.sync_market(root, public, start=date(2026, 8, 19), end=date(2026, 9, 24),
        limit=1, reserve_mb=50, minimum_free_gb=0, now=now, kind='tw_tick')
    scope_path = _priority_scope_path(root, key, day)
    provider_receipt, provider_attempt = _partition_paths(root, key, day)
    assert scope_path.exists() is scope_written
    assert not provider_receipt.exists() and not provider_attempt.exists()
    status = inspect(root, receipt['request_id'], now=datetime.now(UTC))['items'][0]
    if scope_written:
        assert status['state'] == 'waiting_local_scope'
        assert status['local_scope_reason'] == 'symbol_not_in_universe'
        scope = json.loads(scope_path.read_text())
        assert scope['provider_calls'] == 0 and scope['source_no_trade_claim'] is False
    else:
        assert status['state'] == 'pending_existing_owner'


def test_cached_and_ineligible_candidates_never_call_account_quota(tmp_path):
    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    output = tmp_path / "finlab"
    output.mkdir()
    for symbol, day in (("2330", date(2026, 8, 19)), ("2330", date(2026, 9, 24)),
                        ("2867", date(2026, 8, 19))):
        receipt_path, _ = _partition_paths(output, f"tw_tick:{symbol}", day)
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
        receipt_path.write_text(json.dumps({"dataset": f"tw_tick:{symbol}", "trade_date": day.isoformat(),
                                            "source_checked_at_utc": datetime.now(UTC).isoformat(),
                                            "status": "verified_no_trade", "rows": 0}))
    with patch("scripts.download_finlab_market_intraday.credential_available", return_value=True), patch(
        "scripts.download_finlab_market_intraday.quota_room_mb",
        side_effect=AssertionError("local replay/absent lifecycle must not query account"),
    ) as quota:
        summary = sync_market(output, public, start=date(2026, 8, 19), end=date(2026, 9, 24),
                              limit=3, reserve_mb=50, minimum_free_gb=0, now=datetime.now(UTC))
    assert summary["attempted_this_run"] == 0
    assert summary["by_kind"]["tw_tick"]["receipted_partitions"] == 3
    assert quota.call_count == 0


def test_batch_eta_includes_quota_before_acquire_not_just_source_fetch(tmp_path, monkeypatch):
    from scripts import download_finlab_market_intraday as module

    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    output = tmp_path / "finlab"
    output.mkdir()
    clock = [0.0]

    def quota():
        clock[0] += 9  # shared-account quota round trip is outside fetch()
        return (1000, 5000)

    def fetch(*args, **kwargs):
        clock[0] += 1
        return {"status": "verified_no_trade", "rows": 0, "parquet_size_bytes": 0}

    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(module, "credential_available", lambda: True)
    monkeypatch.setattr(module, "_general_work_admission", lambda *a, **kw: {"supplemental_allowed": True})
    monkeypatch.setattr(module, "quota_room_mb", quota)
    monkeypatch.setattr(module, "load_policy", lambda: module.QuotaPolicy(quota_sample_interval_seconds=0))
    monkeypatch.setattr(module, "derive_partition", lambda *a, **kw: {
        "status": "derived_no_regular_trades", "rows": 0, "parquet_size_bytes": 0})
    module.sync_market(output, public, start=date(2026, 8, 19), end=date(2026, 9, 24),
                       limit=3, reserve_mb=50, minimum_free_gb=0, now=datetime.now(UTC), fetch=fetch)
    progress = json.loads((output / "intraday/active_run.json").read_text())
    assert progress["completed_attempts"] == 3
    assert progress["eta_measurement_contract_version"] == 2
    assert progress["seconds_per_attempt_estimate"] == 10  # not fetch's one second
    assert progress["remaining_batch_seconds_estimate"] == 0
    assert progress["estimated_batch_finish_at_utc"] is None
    batches = json.loads((output / "intraday/batch_metrics.json").read_text())["batches"]
    assert len(batches) == 1
    assert batches[0]["completed_attempts"] == 3
    assert batches[0]["new_receipted_partitions"] == 3
    assert batches[0]["elapsed_seconds"] == 30
    assert batches[0]["quota_sample_successes"] == 2  # final request has no later quota sample
    assert sum(item["cycle_seconds"] for item in batches[0]["phases"].values()) == 30


def test_derived_error_repair_is_bounded_local_work_not_an_api_retry(tmp_path, monkeypatch):
    now = datetime.now(UTC)
    db = _open_index(tmp_path / "index.sqlite3")
    for i in range(10):
        _record(db, "tw_minute_derived", str(2300+i), date(2026, 10, 2), "derivation_error")
    db.execute("UPDATE partitions SET checked_at_utc=?", ((now-timedelta(hours=2)).isoformat(),))
    db.commit()
    monkeypatch.setattr("scripts.download_finlab_market_intraday._stored_receipt", lambda path, root, key, day:
        {"status": "downloaded_unverified_for_pit", "rows": 2} if key.startswith("tw_tick:") else None)
    monkeypatch.setattr("scripts.download_finlab_market_intraday.derive_partition", lambda *args: {
        "status": "derived_unverified_for_pit", "rows": 1})
    monkeypatch.setattr("scripts.download_finlab_market_intraday.fetch_partition",
                        lambda *args, **kwargs: pytest.fail("must not refetch stored ticks"))
    assert _repair_derived_errors(db, tmp_path, now=now) == 8
    assert db.execute("SELECT count(*) FROM partitions WHERE status='derivation_error'").fetchone()[0] == 2
    db.close()


def test_inflight_progress_and_interruption_preserve_uncommitted_frontier(tmp_path):
    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    output = tmp_path / "finlab"
    output.mkdir()
    calls = []

    def interrupted(root, key, day, *, now):
        calls.append((key, day))
        live = intraday_progress(root, now=datetime.now(UTC))
        assert live["state"] == "running" and live["owner_alive"]
        assert live["active_key"] == key
        assert json.loads((root / "intraday/market_cursor.json").read_text())["recent_queue"][0]["next"] == 0
        raise SystemExit(143)

    with patch("scripts.download_finlab_market_intraday.credential_available", return_value=True), patch(
        "scripts.download_finlab_market_intraday.quota_room_mb", return_value=(1000, 5000)), patch(
        "scripts.download_finlab_market_intraday.derive_partition", return_value={
            "status": "derived_unverified_for_pit", "rows": 1, "parquet_size_bytes": 10}):
        with pytest.raises(SystemExit):
            sync_market(output, public, start=date(2026, 9, 24), end=date(2026, 9, 24),
                        limit=1, reserve_mb=50, minimum_free_gb=0, now=datetime.now(UTC), fetch=interrupted)
        assert json.loads((output / "intraday/market_status.json").read_text())["state"] == "interrupted"
        assert json.loads((output / "intraday/active_run.json").read_text())["state"] == "interrupted"

        def resumed(root, key, day, *, now):
            calls.append((key, day))
            return {"status": "downloaded_unverified_for_pit", "rows": 2, "parquet_size_bytes": 100}

        summary = sync_market(output, public, start=date(2026, 9, 24), end=date(2026, 9, 24),
                              limit=1, reserve_mb=50, minimum_free_gb=0, now=datetime.now(UTC), fetch=resumed)
    assert calls[0] == calls[1]
    assert summary["successes_this_run"] == 1
    assert json.loads((output / "intraday/active_run.json").read_text())["state"] == "batch_limit"


def test_wall_clock_budget_exits_before_request_and_keeps_candidate(tmp_path):
    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    output = tmp_path / "finlab"
    output.mkdir()
    with patch("scripts.download_finlab_market_intraday.credential_available", return_value=True), patch(
        "scripts.download_finlab_market_intraday.quota_room_mb",
        side_effect=AssertionError("no API after batch wall-clock budget")):
        summary = sync_market(output, public, start=date(2026, 9, 24), end=date(2026, 9, 24),
                              limit=3, reserve_mb=50, minimum_free_gb=0, now=datetime.now(UTC),
                              max_run_seconds=180)
    assert summary["state"] == "run_time_budget_reached" and summary["attempted_this_run"] == 0
    assert json.loads((output / "intraday/market_cursor.json").read_text())["recent_queue"][0]["next"] == 0


@pytest.mark.parametrize('already_replayed', [False, True])
def test_universe_frontier_reset_retains_completed_tick_replay_but_runs_unfinished_bootstrap(
        tmp_path, monkeypatch, already_replayed):
    from scripts import download_finlab_market_intraday as module
    public, output = tmp_path / 'public', tmp_path / 'finlab'
    public.mkdir()
    _fixture(public)
    (output / 'intraday').mkdir(parents=True)
    cursor = output / 'intraday/market_cursor.json'
    old_base = ['2003-08-01', 2462, 'previous-universe', 'tick_only_v1']
    cursor.write_text(json.dumps({'base': old_base, 'bootstrapped': True,
                                 'derived_bootstrapped': already_replayed}))
    # Original history/index facts survive a changed universe and an empty
    # bounded dispatch. No account quota, source fetch or database reset.
    database = output / 'intraday/market_index.sqlite3'
    with _open_index(database) as index:
        _record(index, 'tw_tick', '2330', date(2026, 8, 19), 'verified_no_trade', 0, 0)
        before = index.execute('SELECT * FROM partitions ORDER BY kind,symbol,trade_date').fetchall()
    bootstraps = []
    monkeypatch.setattr(module, 'credential_available', lambda: True)
    monkeypatch.setattr(module, '_bootstrap_derived', lambda *_a: bootstraps.append(True))
    monkeypatch.setattr(module, 'quota_room_mb', lambda: pytest.fail('budget guard must precede API'))
    summary = module.sync_market(output, public, start=date(2026, 9, 24), end=date(2026, 9, 24),
        limit=3, reserve_mb=50, minimum_free_gb=0, now=datetime.now(UTC), max_run_seconds=180,
        fetch=lambda *_a, **_k: pytest.fail('no source fetch after budget guard'))
    after_cursor = json.loads(cursor.read_bytes())
    assert after_cursor['base'] != old_base
    assert after_cursor['bootstrapped'] is True and after_cursor['derived_bootstrapped'] is True
    assert len(bootstraps) == (0 if already_replayed else 1)
    assert summary['state'] == 'run_time_budget_reached' and summary['attempted_this_run'] == 0
    with _open_index(database) as index:
        assert index.execute('SELECT * FROM partitions ORDER BY kind,symbol,trade_date').fetchall() == before


@pytest.fixture(autouse=True)
def required_work_completed_for_partition_tests():
    # Partition/cursor tests isolate their subject from the account-wide gate.
    with patch("scripts.download_finlab_market_intraday._general_work_admission",
               return_value={"supplemental_allowed": True, "required_pending": 0}):
        yield


def test_standalone_market_lock_respects_regular_downloader_and_inherited_lock(tmp_path):
    with (tmp_path / ".sync.lock").open("a") as parent:
        fcntl.flock(parent.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            with _account_sync_lock(tmp_path):
                pytest.fail("must not acquire a competing lock")
        with _account_sync_lock(tmp_path, parent.fileno()):
            pass
        with pytest.raises(BlockingIOError):
            with _account_sync_lock(tmp_path):
                pytest.fail("inherited context must retain its parent's lock")
    with _account_sync_lock(tmp_path):
        pass


def test_tick_batch_refuses_quota_when_required_general_work_is_blocked(tmp_path):
    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    output = tmp_path / "finlab"
    output.mkdir()
    with patch("scripts.download_finlab_market_intraday.credential_available", return_value=True), patch(
        "scripts.download_finlab_market_intraday._general_work_admission",
        return_value={"supplemental_allowed": False, "required_blocked": 1},
    ), patch("scripts.download_finlab_market_intraday.quota_room_mb",
             side_effect=AssertionError("no quota API before general gate")), patch(
        "scripts.download_finlab_market_intraday.fetch_partition",
        side_effect=AssertionError("no tick fetch before general gate"),
    ):
        summary = sync_market(output, public, start=date(2026, 9, 24),
                              end=date(2026, 9, 24), limit=5, reserve_mb=500,
                              minimum_free_gb=0, now=datetime(2026, 9, 25, tzinfo=UTC))
    assert summary["state"] == "waiting_required_general_data"
    assert summary["attempted_this_run"] == 0


@pytest.mark.parametrize("message,state", [("quota exceeded", "quota_exhausted"),
                                          ("session expired", "authentication_failed")])
def test_tick_batch_stops_account_wide_errors_after_one_request(tmp_path, message, state):
    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    output = tmp_path / "finlab"
    output.mkdir()
    with patch("scripts.download_finlab_market_intraday.credential_available", return_value=True), patch(
        "scripts.download_finlab_market_intraday.quota_room_mb", return_value=(1000.0, 5000.0),
    ), patch("scripts.download_finlab_market_intraday.fetch_partition",
             side_effect=RuntimeError(message)) as fetch:
        summary = sync_market(output, public, start=date(2026, 8, 19),
                              end=date(2026, 9, 24), limit=5, reserve_mb=500,
                              minimum_free_gb=0, now=datetime(2026, 9, 25, tzinfo=UTC),
                              fetch=fetch)
    assert summary["state"] == state
    assert summary["attempted_this_run"] == 1
    assert fetch.call_count == 1


def test_tick_batch_yields_when_general_admission_changes_mid_batch(tmp_path):
    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    output = tmp_path / "finlab"
    output.mkdir()

    class MovingClock(datetime):
        calls = 0

        @classmethod
        def now(cls, tz=None):
            cls.calls += 1
            return datetime(2026, 9, 25, 3, tzinfo=UTC) + timedelta(minutes=cls.calls)

    with patch("scripts.download_finlab_market_intraday.credential_available", return_value=True), patch(
        "scripts.download_finlab_market_intraday.datetime", MovingClock,
    ), patch("scripts.download_finlab_market_intraday._general_work_admission", side_effect=[
        {"supplemental_allowed": True}, {"supplemental_allowed": False},
    ]) as gate, patch("scripts.download_finlab_market_intraday.quota_room_mb",
                     return_value=(1000.0, 5000.0)), patch(
        "scripts.download_finlab_market_intraday.derive_partition", return_value={
            "status": "derived_unverified_for_pit", "rows": 1, "parquet_size_bytes": 10,
        },
    ), patch("scripts.download_finlab_market_intraday.fetch_partition", return_value={
        "status": "downloaded_unverified_for_pit", "rows": 1, "parquet_size_bytes": 10,
    }) as fetch:
        summary = sync_market(output, public, start=date(2026, 8, 19),
                              end=date(2026, 9, 24), limit=5, reserve_mb=500,
                              minimum_free_gb=0, now=datetime(2026, 9, 25, tzinfo=UTC),
                              fetch=fetch)
    assert summary["state"] == "waiting_required_general_data"
    assert gate.call_count == 2
    assert fetch.call_count == 1


def _fixture(root: Path) -> None:
    stocks = root / "stocks"
    stocks.mkdir(parents=True)
    pl.DataFrame({
        "code": ["2330", "2867"], "name": ["TSMC", "Former"],
        "market": ["twse", "twse"], "security_type": ["stock", "stock"],
        "source": ["official", "official"],
    }).write_csv(stocks / "symbols.csv")
    for symbol in ("2330", "2867"):
        pl.DataFrame({"date": [date(2026, 8, 19), date(2026, 9, 24)]
                      if symbol == "2330" else [date(2026, 8, 19)]}).write_parquet(
            stocks / f"{symbol}_features.parquet"
        )
    pl.DataFrame({"公司代號": ["2330"], "date": ["2026-09-24"]}).write_parquet(
        root / "twse_listed_company_basic.parquet")
    pl.DataFrame({"SecuritiesCompanyCode": ["9999"], "date": ["2026-09-24"]}).write_parquet(
        root / "tpex_basic_company.parquet")
    pl.DataFrame({"symbol": ["2867"], "date": ["2026-09-01"]}).write_parquet(
        root / "twse_delisted_company.parquet")
    pl.DataFrame({"symbol": ["9998"], "date": ["2020-01-01"]}).write_parquet(
        root / "tpex_delisted_company.parquet")
    pl.DataFrame({"date": ["2026-08-19", "2026-09-24"]}).write_parquet(
        root / "twse_market_index.parquet")


def test_direct_minutes_have_separate_cursor_and_never_request_ticks(tmp_path, monkeypatch):
    from scripts import download_finlab_market_intraday as module
    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    output = tmp_path / "finlab"
    calls = []
    monkeypatch.setattr(module, "credential_available", lambda: True)
    monkeypatch.setattr(module, "_general_work_admission", lambda *a, **kw: {"supplemental_allowed": True})
    monkeypatch.setattr(module, "quota_room_mb", lambda: (1000, 5000))
    monkeypatch.setattr(module, "derive_partition", lambda *a: pytest.fail("direct minutes do not download or derive Tick"))
    def fetch(root, key, day, *, now):
        calls.append((key, day))
        return {"status": "downloaded_unverified_for_pit", "rows": 100, "parquet_size_bytes": 1024}
    summary = sync_market(output, public, start=date(2026, 8, 19), end=date(2026, 9, 24),
        limit=2, reserve_mb=50, minimum_free_gb=0, now=datetime.now(UTC), fetch=fetch, kind="tw_minute")
    assert calls[0] == ("tw_minute:2330", date(2026, 9, 24))
    assert len(calls) == 2 and all(key.startswith("tw_minute:") for key, _ in calls)
    assert (output / "intraday/minute_cursor.json").is_file()
    assert not (output / "intraday/market_cursor.json").exists()
    assert summary["active_kind"] == "tw_minute"
    assert summary["by_kind"]["tw_minute"]["direct_api_partitions"] == 2
    assert summary["by_kind"]["tw_tick"]["receipted_partitions"] == 0
    batches = json.loads((output / "intraday/batch_metrics.json").read_text())["batches"]
    assert batches[-1]["kind"] == "tw_minute"


def test_minute_direct_and_derived_are_deduplicated_and_tick_failure_is_independent(tmp_path):
    _fixture(tmp_path)
    db = _open_index(tmp_path / "index.sqlite3")
    day = date(2026, 9, 24)
    _record(db, "tw_minute", "2330", day, "downloaded_unverified_for_pit", 100, 1000)
    _record(db, "tw_minute_derived", "2330", day, "derived_unverified_for_pit", 90, 900)
    _record(db, "tw_tick", "2330", day, "partition_not_ready")
    summary = _summarize(db, load_universe(tmp_path), [day], started=datetime.now(UTC),
                         attempts=0, successes=0, state="inventory_only", active_kind="tw_minute")
    minute = summary["by_kind"]["tw_minute"]
    assert (minute["receipted_partitions"], minute["rows"], minute["parquet_bytes"]) == (1, 100, 1000)
    assert minute["provider_not_ready_partitions"] == 0
    assert summary["by_kind"]["tw_tick"]["provider_not_ready_partitions"] == 1
    assert summary["minute_priority"]["tick_residual_allowed"]
    _record(db, "tw_minute", "2330", day, "partition_not_ready")
    summary = _summarize(db, load_universe(tmp_path), [day], started=datetime.now(UTC),
                         attempts=0, successes=0, state="inventory_only")
    assert summary["by_kind"]["tw_minute"]["rows"] == 90
    assert summary["by_kind"]["tw_minute"]["derived_partitions"] == 1
    db.close()


def test_minute_reuses_stored_ticks_without_quota_or_source_calls(tmp_path, monkeypatch):
    from scripts import download_finlab_market_intraday as module
    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    output = tmp_path / "finlab"
    for symbol, day in (("2330", date(2026, 8, 19)), ("2330", date(2026, 9, 24)), ("2867", date(2026, 8, 19))):
        path, _ = _partition_paths(output, f"tw_tick:{symbol}", day)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"dataset": f"tw_tick:{symbol}", "trade_date": day.isoformat(),
                                   "status": "verified_no_trade", "rows": 0,
                                   "source_checked_at_utc": datetime.now(UTC).isoformat()}))
    monkeypatch.setattr(module, "credential_available", lambda: True)
    monkeypatch.setattr(module, "quota_room_mb", lambda: pytest.fail("no quota request for local reuse"))
    summary = sync_market(output, public, start=date(2026, 8, 19), end=date(2026, 9, 24),
        limit=3, reserve_mb=50, minimum_free_gb=0, now=datetime.now(UTC), kind="tw_minute",
        fetch=lambda *a, **kw: pytest.fail("no duplicate download"))
    assert summary["attempted_this_run"] == 0
    assert summary["by_kind"]["tw_minute"]["derived_partitions"] == 3


def test_tick_does_not_rebuild_minutes_already_received_directly(tmp_path, monkeypatch):
    from scripts import download_finlab_market_intraday as module
    day = date(2026, 9, 24)
    path, _ = _partition_paths(tmp_path, "tw_minute:2330", day)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"dataset": "tw_minute:2330", "trade_date": day.isoformat(),
        "status": "verified_no_trade", "rows": 0}))
    db = _open_index(tmp_path / "index.sqlite3")
    monkeypatch.setattr(module, "derive_partition", lambda *a: pytest.fail("do not recompute existing minutes"))
    module._record_tick_and_minute(db, tmp_path, "2330", day, {"status": "verified_no_trade"})
    assert db.execute("SELECT COUNT(*) FROM effective_partitions WHERE kind='tw_minute'").fetchone()[0] == 1
    db.close()


def test_inventory_only_cannot_race_an_active_account_writer(tmp_path, monkeypatch, capsys):
    from scripts import download_finlab_market_intraday as module
    monkeypatch.setattr(module.sys, "argv", ["finlab-market", "--status-only", "--output-root", str(tmp_path)])
    monkeypatch.setattr(module, "load_universe", lambda *a, **kw: pytest.fail("must not inspect the writer index"))
    with _account_sync_lock(tmp_path):
        assert module.main() == 0
    assert json.loads(capsys.readouterr().out)["state"] == "account_sync_active"


def test_tick_admission_cannot_bypass_pending_minutes(tmp_path, monkeypatch):
    from scripts import download_finlab_market_intraday as module
    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    monkeypatch.setattr(module, "credential_available", lambda: True)
    monkeypatch.setattr(module, "quota_room_mb", lambda: pytest.fail("priority gate precedes quota call"))
    summary = sync_market(tmp_path / "finlab", public, start=date(2026, 8, 19), end=date(2026, 9, 24),
        limit=3, reserve_mb=50, minimum_free_gb=0, now=datetime.now(UTC), kind="tw_tick",
        enforce_minute_priority=True, fetch=lambda *a, **kw: pytest.fail("minutes must come first"))
    assert summary["state"] == "waiting_required_minutes"
    assert summary["attempted_this_run"] == 0


def test_blocked_general_table_and_tick_reserve_cannot_starve_minutes_above_50mb(tmp_path, monkeypatch):
    from scripts import download_finlab_market_intraday as module
    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    monkeypatch.setattr(module, "credential_available", lambda: True)
    monkeypatch.setattr(module, "_general_work_admission", lambda *a, **kw: {
        "supplemental_allowed": False, "minute_residual_allowed": True, "scheduled_reserve_mb": 800})
    monkeypatch.setattr(module, "quota_room_mb", lambda: (700, 5000))
    kwargs = dict(start=date(2026, 8, 19), end=date(2026, 9, 24), limit=1, reserve_mb=50,
                  minimum_free_gb=0, now=datetime.now(UTC), kind="tw_minute",
                  fetch=lambda *a, **kw: {"status": "verified_no_trade"})
    assert sync_market(tmp_path / "finlab", public, **kwargs)["successes_this_run"] == 1
    monkeypatch.setattr(module, "quota_room_mb", lambda: (50, 5000))
    result = sync_market(tmp_path / "below", public, **kwargs)
    assert result["state"] == "quota_margin_reached" and result["attempted_this_run"] == 0
    assert result["quota_policy"]["effective_reserve_mb"] == 50


def test_minute_quota_sampling_is_bounded_and_metrics_exclude_unsampled_tail(tmp_path, monkeypatch):
    from scripts import download_finlab_market_intraday as module
    from stockagent.data.finlab_quota_policy import QuotaPolicy
    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    output = tmp_path / "finlab"
    monkeypatch.setattr(module, "credential_available", lambda: True)
    monkeypatch.setattr(module, "quota_room_mb", lambda: (700, 5000))
    result = sync_market(output, public, start=date(2026, 8, 19), end=date(2026, 9, 24),
        limit=3, reserve_mb=50, minimum_free_gb=0, now=datetime.now(UTC), kind="tw_minute",
        quota_policy=QuotaPolicy(quota_sample_interval_seconds=60),
        fetch=lambda *a, **kw: {"status": "verified_no_trade"})
    assert result["successes_this_run"] == 3
    progress = json.loads((output / "intraday/active_run.json").read_text())
    assert progress["quota_checks"] == 1 and progress["quota_cache_hits"] == 2
    batch = json.loads((output / "intraday/batch_metrics.json").read_text())["batches"][-1]
    assert batch["quota_sample_successes"] == 0  # No later authoritative quota observation.
    assert batch["account_delta_bytes"] == 0


def test_minute_drain_under_50mb_still_stops_at_official_quota_error(tmp_path, monkeypatch):
    from scripts import download_finlab_market_intraday as module
    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    class BeforeReset(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 10, 9, 23, 15, tzinfo=UTC)
    monkeypatch.setattr(module, "datetime", BeforeReset)
    monkeypatch.setattr(module, "credential_available", lambda: True)
    monkeypatch.setattr(module, "quota_room_mb", lambda: (30, 5000))
    with patch("scripts.download_finlab_market_intraday.fetch_partition", side_effect=RuntimeError("quota exceeded")) as fetch:
        result = sync_market(tmp_path / "finlab", public, start=date(2026, 8, 19), end=date(2026, 9, 24),
            limit=3, reserve_mb=50, minimum_free_gb=0, now=BeforeReset.now(), kind="tw_minute", fetch=fetch)
    assert result["quota_policy"]["effective_reserve_mb"] == 0
    assert result["state"] == "quota_exhausted" and fetch.call_count == 1


def test_long_lived_drain_owner_yields_before_opening_without_api_calls(tmp_path, monkeypatch):
    from scripts import download_finlab_market_intraday as module
    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    class OpeningRunway(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 10, 12, 0, 11, tzinfo=UTC)  # Monday 08:11.
    monkeypatch.setattr(module, "datetime", OpeningRunway)
    monkeypatch.setattr(module, "credential_available", lambda: True)
    monkeypatch.setattr(module, "opening_admission", lambda *a, **kw: {"allowed": False})
    monkeypatch.setattr(module, "quota_room_mb", lambda: pytest.fail("no network within opening runway"))
    result = sync_market(tmp_path / "finlab", public, start=date(2026, 8, 19), end=date(2026, 9, 24),
        limit=3, reserve_mb=50, minimum_free_gb=0, now=OpeningRunway.now(), kind="tw_minute",
        fetch=lambda *a, **kw: pytest.fail("no source request within opening runway"))
    assert result["state"] == "opening_window_yield" and result["attempted_this_run"] == 0


def test_minute_interruption_preserves_its_own_inflight_cursor(tmp_path, monkeypatch):
    from scripts import download_finlab_market_intraday as module
    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    output = tmp_path / "finlab"
    monkeypatch.setattr(module, "credential_available", lambda: True)
    monkeypatch.setattr(module, "_general_work_admission", lambda *a, **kw: {"supplemental_allowed": True})
    monkeypatch.setattr(module, "quota_room_mb", lambda: (1000, 5000))
    calls = []
    def fetch(root, key, day, *, now):
        calls.append((key, day))
        if len(calls) == 1:
            raise SystemExit(143)
        return {"status": "verified_no_trade"}
    kwargs = dict(start=date(2026, 8, 19), end=date(2026, 9, 24), limit=1,
                  reserve_mb=50, minimum_free_gb=0, now=datetime.now(UTC), kind="tw_minute", fetch=fetch)
    with pytest.raises(SystemExit):
        sync_market(output, public, **kwargs)
    cursor = json.loads((output / "intraday/minute_cursor.json").read_text())
    assert cursor["recent_queue"][0]["next"] == 0
    assert json.loads((output / "intraday/active_run.json").read_text())["state"] == "interrupted"
    assert sync_market(output, public, **kwargs)["successes_this_run"] == 1
    assert calls == [("tw_minute:2330", date(2026, 9, 24))]*2


def test_older_minute_probes_are_bounded_per_cycle_without_retry_starvation(tmp_path, monkeypatch):
    from scripts import download_finlab_market_intraday as module
    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    output = tmp_path / "finlab"
    floor = date(2026, 9, 24)
    observed_days = [date(2026, 8, 19)+timedelta(days=i) for i in range(37)]
    pl.DataFrame({"date": [day.isoformat() for day in observed_days]}).write_parquet(public / "twse_market_index.parquet")
    db = _open_index(output / "intraday/market_index.sqlite3")
    _record(db, "tw_minute", "2330", floor, "downloaded_unverified_for_pit", 1, 1)
    db.close()
    monkeypatch.setattr(module, "credential_available", lambda: True)
    monkeypatch.setattr(module, "_general_work_admission", lambda *a, **kw: {"supplemental_allowed": True})
    monkeypatch.setattr(module, "quota_room_mb", lambda: (1000, 5000))
    monkeypatch.setattr(module, "_available_receipt", lambda root, kind, symbol, day:
                        {"status": "verified_no_trade"} if day >= floor else None)
    calls = []
    def unavailable(root, key, day, *, now):
        calls.append((key, day))
        raise DataError("not_ready: partition unpublished")
    kwargs = dict(start=observed_days[0], end=floor, limit=64, reserve_mb=50,
                  minimum_free_gb=0, kind="tw_minute", fetch=unavailable)
    now = datetime.now(UTC)
    first = sync_market(output, public, now=now, **kwargs)
    assert first["attempted_this_run"] == 16
    assert first["state"] == "waiting_older_publication"
    assert sync_market(output, public, now=now, **kwargs)["attempted_this_run"] == 0
    previous = set(calls)
    calls.clear()
    sync_market(output, public, now=now+timedelta(days=1), **kwargs)
    cursor = json.loads((output / "intraday/minute_cursor.json").read_text())
    assert cursor["older_retry_attempts"] <= 4
    assert len(calls) <= 16
    assert set(calls) - previous  # retries cannot monopolize the discovery budget


@pytest.mark.parametrize("kind", ["tw_minute", "tw_tick"])
def test_parallel_older_retries_share_discovery_budget_before_dispatch(tmp_path, monkeypatch, kind):
    from multiprocessing.pool import ThreadPool
    from scripts import download_finlab_market_intraday as module
    from scripts import download_finlab_intraday as intraday
    from scripts.download_finlab_intraday import record_failed_partition

    public, output = tmp_path / "public", tmp_path / "finlab"
    public.mkdir()
    _fixture(public)
    floor = date(2026, 9, 24)
    days = [date(2026, 8, 19) + timedelta(days=i) for i in range(37)]
    pl.DataFrame({"date": list(map(str, days))}).write_parquet(public / "twse_market_index.parquet")
    db = _open_index(output / "intraday/market_index.sqlite3")
    _record(db, kind, "2330", floor, "downloaded_unverified_for_pit", 1, 1)
    db.close()
    monkeypatch.setattr(module, "credential_available", lambda: True)
    monkeypatch.setattr(module, "_general_work_admission", lambda *a, **kw: {"supplemental_allowed": True})
    monkeypatch.setattr(module, "quota_room_mb", lambda: (1000, 5000))
    monkeypatch.setattr(module, "_minute_pool", lambda workers, **kw: ThreadPool(workers))
    monkeypatch.setattr(module, "_available_receipt", lambda root, kind, symbol, day:
                        {"status": "verified_no_trade"} if day >= floor else None)
    calls = []
    clock = [datetime.now(UTC)]

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0].astimezone(tz) if tz else clock[0].replace(tzinfo=None)

    monkeypatch.setattr(intraday, "datetime", Clock)

    def unavailable(root, key, target_days, *, now):
        assert len(target_days) == 1  # Unknown history must not become a 31-day retry.
        day = target_days[0]
        calls.append((key, day))
        failure = record_failed_partition(root, key, day, DataError("not_ready: partition unpublished"))
        return {"outcomes": [{"trade_date": str(day), "failure": failure}], "sdk_calls": 1,
                "fallback_calls": 0, "source_seconds": 0.0, "persist_seconds": 0.0}

    kwargs = dict(start=days[0], end=floor, limit=64, reserve_mb=50, minimum_free_gb=0,
                  kind=kind, workers=4, queue_depth=4, range_fetch=unavailable)
    now = clock[0]
    assert sync_market(output, public, now=now, **kwargs)["attempted_this_run"] == 16
    previous = set(calls)
    calls.clear()
    clock[0] = now + timedelta(days=1)
    sync_market(output, public, now=clock[0], **kwargs)
    cursor = json.loads((output / f"intraday/{'minute' if kind == 'tw_minute' else 'market'}_cursor.json").read_text())
    assert cursor["older_retry_attempts"] <= 4
    assert cursor["older_probe_attempts"] <= 16 and len(calls) <= 16
    assert len(set(calls) & previous) <= 4
    assert set(calls) - previous


def test_official_current_snapshot_does_not_relabel_former_symbol_as_current(tmp_path):
    _fixture(tmp_path)
    universe = load_universe(tmp_path, start=date(2026, 8, 19))
    by_symbol = {item["symbol"]: item for item in universe}
    days = session_days(tmp_path, start=date(2026, 8, 19), end=date(2026, 9, 24))
    assert by_symbol["2867"]["current"] is False
    assert _eligible(by_symbol["2867"], date(2026, 8, 19))
    assert not _eligible(by_symbol["2867"], date(2026, 9, 24))
    assert _target_count(by_symbol["2867"], days) == 1
    assert by_symbol["9998"]["first_local_daily_date"] is None
    assert _target_count(by_symbol["9998"], days) == 0


def test_pre_2009_sessions_come_from_official_twse_and_tpex_daily_rows(tmp_path):
    _fixture(tmp_path)
    pl.DataFrame({"date": ["2004-02-11"]}).write_parquet(
        tmp_path / "twse_daily_ohlcv.parquet"
    )
    pl.DataFrame({"date": ["2003-08-01"]}).write_parquet(
        tmp_path / "tpex_daily_ohlcv.parquet"
    )
    days = session_days(tmp_path, start=date(2003, 8, 1), end=date(2026, 9, 24))
    assert days == [date(2003, 8, 1), date(2004, 2, 11),
                    date(2026, 8, 19), date(2026, 9, 24)]


def test_market_sync_counts_per_symbol_receipts_without_catalog_example_limit(tmp_path):
    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    output = tmp_path / "finlab"
    output.mkdir()
    calls = []

    def fetch(root, key, day, *, now):
        calls.append((key, day))
        return {"status": "downloaded_unverified_for_pit", "rows": 17,
                "parquet_size_bytes": 1234}

    with patch("scripts.download_finlab_market_intraday.derive_partition", return_value={
        "status": "derived_unverified_for_pit", "rows": 5, "parquet_size_bytes": 500,
    }), patch("scripts.download_finlab_market_intraday.credential_available", return_value=True), patch(
        "scripts.download_finlab_market_intraday.quota_room_mb", return_value=(1000.0, 5000.0)
    ):
        summary = sync_market(output, public, start=date(2026, 8, 19),
                              end=date(2026, 9, 24), limit=2,
                              reserve_mb=50, minimum_free_gb=0,
                              now=datetime(2026, 9, 25, tzinfo=UTC), fetch=fetch)
    assert all(key.startswith("tw_tick:") for key, _ in calls)
    assert calls[0] == ("tw_tick:2330", date(2026, 9, 24))
    assert summary["universe_symbols"] == 3
    assert summary["former_symbols"] == 2
    assert summary["by_kind"]["tw_minute"]["receipted_partitions"] == 2
    assert summary["by_kind"]["tw_tick"]["receipted_partitions"] == 2
    assert summary["by_kind"]["tw_minute"]["source_kind"] == "derived_from_tw_tick"
    assert next(row for row in summary["symbols"] if row["key"] == "tw_minute:2867")["target_partitions"] == 1
    assert summary["completion_eta"] is None


def test_real_market_path_calls_only_tick_api_and_writes_derived_minute(tmp_path):
    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    output = tmp_path / "finlab"
    output.mkdir()
    tick = pd.DataFrame({
        "stock_id": ["2330", "2330"],
        "trade_date": ["2026-09-24", "2026-09-24"],
        "timestamp": pd.to_datetime(["2026-09-24 09:00:01+08:00",
                                     "2026-09-24 09:00:02+08:00"]),
        "sequence": [0, 1], "close": [100.0, 101.0],
        "volume": [1, 2], "session": ["regular", "regular"],
    })
    with patch("scripts.download_finlab_market_intraday.credential_available", return_value=True), patch(
        "scripts.download_finlab_market_intraday.quota_room_mb", return_value=(1000.0, 5000.0)
    ), patch("finlab.data.get", return_value=tick) as get:
        summary = sync_market(output, public, start=date(2026, 9, 24),
                              end=date(2026, 9, 24), limit=1,
                              reserve_mb=50, minimum_free_gb=0,
                              now=datetime(2026, 9, 25, tzinfo=UTC))
    assert get.call_count == 1
    assert get.call_args.args[0] == "tw_tick:2330"
    assert summary["by_kind"]["tw_tick"]["receipted_partitions"] == 1
    assert summary["by_kind"]["tw_minute"]["receipted_partitions"] == 1
    assert summary["by_kind"]["tw_minute"]["rows"] == 1
    assert list((output / "intraday/derived_minute/receipts").glob("*/*.json"))


def test_new_exchange_session_keeps_older_backfill_cursor(tmp_path):
    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    output = tmp_path / "finlab"
    output.mkdir()
    with patch("scripts.download_finlab_market_intraday.credential_available", return_value=True), patch(
        "scripts.download_finlab_market_intraday.quota_room_mb", return_value=(0.0, 5000.0)
    ):
        sync_market(output, public, start=date(2026, 8, 19),
                    end=date(2026, 8, 19), limit=1, reserve_mb=50,
                    minimum_free_gb=0, now=datetime(2026, 9, 25, tzinfo=UTC))
        first = json.loads((output / "intraday/market_cursor.json").read_text())
        sync_market(output, public, start=date(2026, 8, 19),
                    end=date(2026, 9, 24), limit=1, reserve_mb=50,
                    minimum_free_gb=0, now=datetime(2026, 9, 25, tzinfo=UTC))
    second = json.loads((output / "intraday/market_cursor.json").read_text())
    assert first["oldest"] == second["oldest"]
    assert second["newest"] == first["newest"] + 3
    assert [row["day"] for row in second["recent_queue"]] == ["2026-09-24", "2026-08-19"]


def test_not_ready_partition_retries_after_next_quota_cycle(tmp_path):
    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    output = tmp_path / "finlab"
    output.mkdir()
    calls = []

    def fetch(root, key, day, *, now):
        calls.append((key, day))
        if len(calls) == 1:
            raise DataError("not_ready: partition not published")
        return {"status": "downloaded_unverified_for_pit", "rows": 2,
                "parquet_size_bytes": 100}

    with patch("scripts.download_finlab_market_intraday.credential_available", return_value=True), patch(
        "scripts.download_finlab_market_intraday.quota_room_mb", return_value=(1000.0, 5000.0)
    ):
        sync_market(output, public, start=date(2026, 8, 19),
                    end=date(2026, 9, 24), limit=1, reserve_mb=50,
                    minimum_free_gb=0, now=datetime(2026, 9, 25, tzinfo=UTC), fetch=fetch)
        # The injected logical date does not freeze the production wall clock
        # used by _record(). Align only this test's attempt receipt with the
        # simulated first quota cycle before advancing to the next one.
        index = sqlite3.connect(output / "intraday/market_index.sqlite3")
        try:
            index.execute(
                "UPDATE partitions SET checked_at_utc=? "
                "WHERE kind='tw_tick' AND symbol='2330' AND trade_date='2026-09-24'",
                ("2026-09-25T00:00:00+00:00",),
            )
            index.commit()
        finally:
            index.close()
        sync_market(output, public, start=date(2026, 8, 19),
                    end=date(2026, 9, 24), limit=1, reserve_mb=50,
                    minimum_free_gb=0, now=datetime(2026, 9, 26, tzinfo=UTC), fetch=fetch)
    assert calls == [("tw_tick:2330", date(2026, 9, 24))] * 2


def test_former_stock_gets_early_probe_before_full_history_frontier(tmp_path):
    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    output = tmp_path / "finlab"
    output.mkdir()
    calls = []

    def fetch(root, key, day, *, now):
        calls.append((key, day))
        return {"status": "downloaded_unverified_for_pit", "rows": 1,
                "parquet_size_bytes": 100}

    with patch("scripts.download_finlab_market_intraday.credential_available", return_value=True), patch(
        "scripts.download_finlab_market_intraday.quota_room_mb", return_value=(1000.0, 5000.0)
    ):
        sync_market(output, public, start=date(2026, 8, 19),
                    end=date(2026, 9, 24), limit=3, reserve_mb=50,
                    minimum_free_gb=0, now=datetime(2026, 9, 25, tzinfo=UTC), fetch=fetch)
    assert calls[2] == ("tw_tick:2867", date(2026, 8, 19))


def test_daily_close_counts_nonnull_symbols_and_local_fallback_separately(tmp_path):
    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    universe = load_universe(public, start=date(2026, 8, 19))
    output = tmp_path / "finlab"
    (output / "receipts").mkdir(parents=True)
    (output / "datasets").mkdir()
    pl.DataFrame({"source_index": ["2026-08-19", "2026-09-24"],
                  "2330": [1.0, 2.0], "2867": [5.0, None]}).write_parquet(
        output / "datasets/close.parquet"
    )
    (output / "receipts" / f"{safe_stem('price:收盤價')}.json").write_text(json.dumps({
        "dataset": "price:收盤價", "status": "downloaded_unverified_for_pit",
        "parquet_path": "datasets/close.parquet",
    }))
    coverage = annotate_daily_close_coverage(output, universe)
    assert coverage["symbols_with_values"] == 2
    assert coverage["former_with_values"] == 1
    assert coverage["missing_both_sources"] == 1
    assert {item["symbol"]: item["finlab_daily_close_rows"] for item in universe} == {
        "2330": 2, "2867": 1, "9998": 0,
    }

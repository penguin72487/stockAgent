from types import SimpleNamespace

import pytest

from scripts import run_finmind_scoped_due_wait as waiter


def test_bounded_settlement_cycle_uses_existing_owner_and_capacity(tmp_path, monkeypatch):
    root = tmp_path / 'complement'
    root.mkdir()
    (root / 'queue.sqlite3').touch()
    calls = []
    sampler = SimpleNamespace(root=root, initial_sample=lambda **kw: {'state': 'admitted'})
    def cycle(actual_root, **kwargs):
        calls.append((actual_root, kwargs))
        return {'state': 'batch_complete'}
    monkeypatch.setattr(waiter.owner, 'run_once', cycle)
    result = waiter.run(root, max_requests=4, max_wait_seconds=10, sampler=sampler)
    assert result['state'] == 'batch_complete'
    assert calls == [(root, {'max_requests': 4, 'datasets': waiter.DATASETS, 'capacity_sampler': sampler})]
    assert waiter.DATASETS == ('TaiwanFuturesFinalSettlementPrice', 'TaiwanOptionFinalSettlementPrice')


@pytest.mark.parametrize('requests,wait', [(0, 1), (65, 1), (1, 0), (1, 3601)])
def test_invalid_scope_limits_fail_before_owner_call(tmp_path, requests, wait):
    with pytest.raises(ValueError, match='bounded'):
        waiter.run(tmp_path, max_requests=requests, max_wait_seconds=wait)


def test_bounded_continuation_reuses_sampler_and_stops_on_owner_gate(tmp_path, monkeypatch):
    calls = []
    sampler = object()
    def cycle(root, **kwargs):
        calls.append(kwargs)
        return {'state': 'batch_complete' if len(calls) == 1 else 'incremental_reserve'}
    monkeypatch.setattr(waiter, 'run', cycle)
    result = waiter.run_batches(tmp_path, max_requests=64, max_batches=6,
                                max_wait_seconds=60, sampler=sampler)
    assert result['state'] == 'incremental_reserve'
    assert result['completed_batches'] == 1
    assert len(calls) == 2
    assert all(call['sampler'] is sampler and call['max_requests'] == 64 for call in calls)


def test_instrument_master_scope_is_exact_and_rejects_other_datasets(tmp_path, monkeypatch):
    root = tmp_path / 'complement'
    root.mkdir()
    (root / 'queue.sqlite3').touch()
    sampler = SimpleNamespace(root=root, initial_sample=lambda **kw: {'state': 'admitted'})
    calls = []
    monkeypatch.setattr(waiter.owner, 'run_once', lambda actual_root, **kwargs: calls.append(kwargs) or {'state': 'batch_complete'})
    waiter.run(root, max_requests=2, max_wait_seconds=10, sampler=sampler, datasets=waiter.MASTER_DATASETS)
    assert calls[0]['datasets'] == ('TaiwanFutOptDailyInfo', 'TaiwanFutOptTickInfo')
    with pytest.raises(ValueError, match='exact derivative'):
        waiter.run(root, max_requests=2, max_wait_seconds=10, sampler=sampler, datasets=('Unrelated',))


def test_derivative_snapshot_scope_uses_the_existing_owner(tmp_path, monkeypatch):
    root = tmp_path / 'complement'
    root.mkdir()
    (root / 'queue.sqlite3').touch()
    sampler = SimpleNamespace(root=root, initial_sample=lambda **kw: {'state': 'admitted'})
    calls = []
    monkeypatch.setattr(waiter.owner, 'run_once', lambda actual_root, **kwargs: calls.append(kwargs) or {'state': 'batch_complete'})
    waiter.run(root, max_requests=64, max_wait_seconds=10, sampler=sampler, datasets=waiter.SNAPSHOT_DATASETS)
    assert calls[0]['datasets'] == ('taiwan_futures_snapshot', 'taiwan_options_snapshot')
    assert waiter.SCOPES['derivative_snapshot'] == calls[0]['datasets']


def test_futures_spread_scope_uses_only_the_existing_spread_owner(tmp_path, monkeypatch):
    root = tmp_path / 'complement'
    root.mkdir()
    (root / 'queue.sqlite3').touch()
    sampler = SimpleNamespace(root=root, initial_sample=lambda **kw: {'state': 'admitted'})
    calls = []
    monkeypatch.setattr(waiter.owner, 'run_once', lambda actual_root, **kwargs: calls.append(kwargs) or {'state': 'batch_complete'})
    waiter.run(root, max_requests=4, max_wait_seconds=10, sampler=sampler, datasets=waiter.SPREAD_DATASETS)
    assert calls[0]['datasets'] == ('TaiwanFuturesSpreadTrading',)
    assert waiter.SCOPES['futures_spread_market'] == calls[0]['datasets']


@pytest.mark.parametrize('batches,wait', [(0, 10), (17, 10), (1, 0), (1, 3601)])
def test_continuation_rejects_unbounded_work(tmp_path, batches, wait):
    with pytest.raises(ValueError, match='bounded'):
        waiter.run_batches(tmp_path, max_requests=4, max_batches=batches, max_wait_seconds=wait)

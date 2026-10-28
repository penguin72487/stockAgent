from types import SimpleNamespace

import pytest

from scripts import run_finmind_sponsor_derivative_due_wait as waiter


def test_scoped_derivative_refresh_uses_canonical_owner(tmp_path, monkeypatch):
    root = tmp_path / 'sponsor'
    root.mkdir()
    (root / 'queue.sqlite3').touch()
    sampler = SimpleNamespace(root=root, initial_sample=lambda **_kw: {'state': 'admitted'})
    calls = []
    monkeypatch.setattr(waiter.owner, 'run_once', lambda actual_root, **kwargs:
                        calls.append((actual_root, kwargs)) or {'state': 'batch_complete'})
    result = waiter.run(root, max_requests=12, max_wait_seconds=10, sampler=sampler)
    assert result['state'] == 'batch_complete'
    assert calls == [(root, {'max_requests': 12, 'workers': 1, 'datasets': waiter.DATASETS,
                             'capacity_sampler': sampler})]
    assert set(waiter.DATASETS) <= {spec.dataset for spec in waiter.owner.SOURCES}
    assert all(name.startswith(('TaiwanFutures', 'TaiwanOption')) for name in waiter.DATASETS)


@pytest.mark.parametrize('requests,wait', [(0, 1), (17, 1), (1, 0), (1, 1801)])
def test_scope_rejects_unbounded_work_before_owner_call(tmp_path, requests, wait):
    with pytest.raises(ValueError, match='bounded'):
        waiter.run(tmp_path, max_requests=requests, max_wait_seconds=wait)

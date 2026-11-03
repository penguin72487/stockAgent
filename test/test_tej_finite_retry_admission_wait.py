"""A continuation cannot convert an unavailable source into market data."""
from contextlib import closing
import json

import pytest

from downloader.tej_history import connect
from scripts import run_tej_finite_retry_admission_wait as waiter
from test_tej_finite_retry import finite_retry
from test_tej_planning import registry


def stop_wait(_):
    raise StopIteration


def test_verified_pending_source_enrolls_without_provider_query(finite_retry, tmp_path, monkeypatch):
    root, plan, receipt, _, _ = finite_retry
    monkeypatch.setattr(waiter, 'inspect', lambda *_: {'provider': 'tej', 'priority_active': True})
    output = tmp_path/'enrolled.json'
    assert waiter.wait(root, plan, receipt, tmp_path/'upstream.json', 'priority', output, 1, interval=1) == 0
    result = json.loads(output.read_text())
    assert result['state'] == 'registered' and result['tasks'] == 2 and result['provider_queries'] == 0


def test_without_metadata_retry_the_waiter_never_mutates_tasks(finite_retry, tmp_path, monkeypatch):
    root, plan, receipt, _, _ = finite_retry
    with closing(connect(root)) as con, con:
        con.execute('DELETE FROM desktop_retry_windows')
    monkeypatch.setattr(waiter, 'inspect', lambda *_: {'provider': 'tej', 'priority_active': True})
    monkeypatch.setattr(waiter.time, 'sleep', stop_wait)
    with pytest.raises(StopIteration):
        waiter.wait(root, plan, receipt, tmp_path/'upstream.json', 'priority', tmp_path/'out.json', 1, interval=1)
    with closing(connect(root)) as con:
        assert con.execute("SELECT count(*) FROM meta WHERE key LIKE 'verified_finite_metadata_retry_v1:%'").fetchone()[0] == 0


def test_upstream_failure_and_expiry_do_not_create_receipts(finite_retry, tmp_path, monkeypatch):
    root, _, _, _, _ = finite_retry
    upstream = tmp_path/'failed-upstream.json'
    upstream.write_text(json.dumps({'state': 'failed'}))
    output = tmp_path/'out.json'
    monkeypatch.setattr(waiter, 'inspect', lambda *_: {'provider': 'tej', 'priority_active': True})
    assert waiter.wait(root, tmp_path/'absent-plan', tmp_path/'absent-receipt', upstream,
                       'priority', output, 1, interval=1) == 75
    monkeypatch.setattr(waiter, 'inspect', lambda *_: {'provider': 'tej', 'priority_active': False})
    assert waiter.wait(root, tmp_path/'absent-plan', tmp_path/'absent-receipt', upstream,
                       'priority', output, 1, interval=1) == 75
    assert not output.exists()

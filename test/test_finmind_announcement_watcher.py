from datetime import UTC, datetime, timedelta
import json

from scripts import watch_finmind_announcements as watcher


NOW = datetime(2026, 9, 27, 3, tzinfo=UTC)


def test_daily_check_includes_holidays():
    # Sunday 11:00 -> Monday 06:10, without a trading-session filter.
    assert watcher.next_check(NOW) == '2026-09-28T06:10:00+08:00'
    assert watcher.next_check(NOW - timedelta(days=1)) == '2026-09-27T06:10:00+08:00'


def test_fetch_failure_preserves_plan_and_records_degraded(tmp_path, monkeypatch):
    old = {'plan_id': 'old', 'state': {'entries': {'old-entry': {}}}}
    path = tmp_path / 'repair_plan.json'
    path.write_text(json.dumps(old))
    previous_bytes = path.read_bytes()
    monkeypatch.setattr(watcher, 'fetch_announcements', lambda *_: {
        'state': 'degraded', 'fetch_status': 'failed', 'entries': [{'stale': True}],
    })
    monkeypatch.setattr(watcher, 'build_repair_plan', lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError('must not plan stale cache')))
    status = watcher.run_once(tmp_path, now=NOW)
    assert status['state'] == 'degraded'
    assert status['previous_plan_preserved']
    assert path.read_bytes() == previous_bytes
    assert json.loads((tmp_path / 'monitor_status.json').read_bytes())['state'] == 'degraded'


def test_watermarks_advance_atomically_with_plan(tmp_path, monkeypatch):
    monkeypatch.setattr(watcher, 'fetch_announcements', lambda *_: {
        'state': 'ok', 'fetch_status': 'not_modified', 'entries': [], 'snapshot_sha256': 'a' * 64,
    })
    seen = []
    def build(entries, previous, *, now):
        seen.append(previous)
        return {'schema_version': 1, 'plan_id': 'same', 'requests': [], 'notices': [], 'state': {'entries': {'e': {'first_seen': now.isoformat()}}}}
    monkeypatch.setattr(watcher, 'build_repair_plan', build)
    first = watcher.run_once(tmp_path, now=NOW)
    second = watcher.run_once(tmp_path, now=NOW + timedelta(days=1))
    assert seen[0] is None
    assert seen[1]['entries']['e']['first_seen'] == NOW.isoformat()
    assert first['plan_changed'] is True and second['plan_changed'] is False
    assert first['data_api_requests'] == 0


def test_dry_run_does_not_publish_watermarks(tmp_path, monkeypatch):
    monkeypatch.setattr(watcher, 'fetch_announcements', lambda *_: {
        'state': 'ok', 'entries': [], 'snapshot_sha256': 'b' * 64,
    })
    monkeypatch.setattr(watcher, 'build_repair_plan', lambda *_args, **_kwargs: {
        'plan_id': 'dry', 'requests': [], 'notices': [], 'state': {},
    })
    assert watcher.run_once(tmp_path, now=NOW, dry_run=True)['dry_run']
    assert not (tmp_path / 'repair_plan.json').exists()
    assert not (tmp_path / 'monitor_status.json').exists()

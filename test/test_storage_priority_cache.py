from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import fcntl
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from scripts import storage_priority_cache as cache


@pytest.fixture
def context(tmp_path):
    root = tmp_path/'data_tw_public'
    root.mkdir()
    (root/cache.HOLIDAY).write_bytes(b'official schedule generation one')
    local = datetime(2026, 11, 9, 10, tzinfo=ZoneInfo('Asia/Taipei'))
    return root, tmp_path/'cache', local


def decision(local, *, session=True, verified=True):
    return {'allowed': not session, 'protected': session, 'observed_at_taipei': local.isoformat(timespec='seconds'),
        'window': '08:20:00..13:30:00 Asia/Taipei', 'minimum_runway_minutes': 120,
        'protected_start': '06:20:00', 'stock_session': session,
        'calendar_evidence': 'official TWSE schedule as-of 2026-09-16: verified' if verified else 'calendar unavailable',
        'policy': 'best_effort_maintenance_must_not_start_during_tw_session'}


@pytest.mark.parametrize('session', [True, False])
def test_verified_session_and_verified_holiday_share_only_same_generation(context, session):
    root, target, local = context
    calls = []
    def compute():
        calls.append(1)
        return decision(local, session=session)
    first = cache.evaluate_cached(local, root, compute, cache_root=target)
    later = cache.evaluate_cached(local+timedelta(seconds=3), root, compute, cache_root=target)
    assert len(calls) == 1 and first['stock_session'] is session and later['stock_session'] is session
    assert later['observed_at_taipei'] == (local+timedelta(seconds=3)).isoformat(timespec='seconds')


@pytest.mark.parametrize('name', [cache.HOLIDAY, cache.ACTUAL, 'twse_taiex_ohlc.summary.json'])
def test_calendar_generation_change_forces_canonical_decision(context, name):
    root, target, local = context
    calls = []
    def compute():
        calls.append(1)
        return decision(local)
    cache.evaluate_cached(local, root, compute, cache_root=target)
    (root/name).write_bytes(b'new generation')
    cache.evaluate_cached(local+timedelta(seconds=1), root, compute, cache_root=target)
    assert len(calls) == 2


def test_unknown_calendar_is_never_cached(context):
    root, target, local = context
    calls = []
    def compute():
        calls.append(1)
        return decision(local, verified=False)
    cache.evaluate_cached(local, root, compute, cache_root=target)
    cache.evaluate_cached(local+timedelta(seconds=1), root, compute, cache_root=target)
    assert len(calls) == 2 and not (target/'decision.json').exists()


@pytest.mark.parametrize(('session', 'expected_calls'), [(True, 1), (False, 2)])
def test_actual_session_receipt_can_only_cache_a_confirmed_open(context, session, expected_calls):
    root, target, local = context
    calls = []
    def compute():
        calls.append(1)
        return {**decision(local, session=session), 'calendar_evidence': 'receipt-verified TWSE TAIEX actual session'}
    cache.evaluate_cached(local, root, compute, cache_root=target)
    cache.evaluate_cached(local+timedelta(seconds=1), root, compute, cache_root=target)
    assert len(calls) == expected_calls


def test_expired_or_backwards_clock_never_reuses_decision(context):
    root, target, local = context
    calls = []
    def compute():
        calls.append(1)
        return decision(local)
    cache.evaluate_cached(local, root, compute, cache_root=target)
    cache.evaluate_cached(local-timedelta(seconds=1), root, compute, cache_root=target)
    assert len(calls) == 2
    cache.evaluate_cached(local+timedelta(seconds=cache.TTL_SECONDS+2), root, compute, cache_root=target)
    assert len(calls) == 3


def test_unrelated_time_and_explicit_day_do_not_use_a_prior_cache(context):
    root, target, local = context
    calls = []
    def compute():
        calls.append(1)
        return decision(local)
    cache.evaluate_cached(local, root, compute, cache_root=target)
    cache.evaluate_cached(local.replace(hour=13, minute=30), root, compute, cache_root=target)
    cache.evaluate_cached(local+timedelta(days=1), root, compute, cache_root=target)
    assert len(calls) == 3


def test_mutation_during_canonical_decision_is_not_cached(context):
    root, target, local = context
    def compute():
        (root/cache.HOLIDAY).write_bytes(b'changed while reading')
        return decision(local)
    assert cache.evaluate_cached(local, root, compute, cache_root=target)['protected']
    assert not (target/'decision.json').exists()


def test_busy_owner_never_blocks_other_guard(context, monkeypatch):
    root, target, local = context
    monkeypatch.setattr(cache, 'LOCK_WAIT_SECONDS', 0)
    target.mkdir(mode=0o700)
    with (target/'owner.lock').open('a+b') as owner:
        os.chmod(target/'owner.lock', 0o600)
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        calls = []
        assert cache.evaluate_cached(local, root, lambda: calls.append(1) or decision(local), cache_root=target)['protected']
    assert calls == [1] and not (target/'decision.json').exists()


def test_concurrent_storage_guards_share_one_canonical_calendar_read(context):
    root, target, local = context
    calls, mutex = [], threading.Lock()
    def compute():
        with mutex:
            calls.append(1)
        time.sleep(0.1)
        return decision(local)
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: cache.evaluate_cached(local, root, compute, cache_root=target), range(8)))
    assert calls == [1] and all(result['protected'] is True for result in results)


def test_separate_processes_share_one_verified_session_decision(context):
    root, target, local = context
    calls = root.parent/'canonical-calls.txt'
    program = '''
from datetime import datetime
from zoneinfo import ZoneInfo
import json,sys,time
from pathlib import Path
from scripts.storage_priority_cache import evaluate_cached
root,target,calls,stamp,raw = sys.argv[1:]
def compute():
    with open(calls, 'a') as stream:
        stream.write('x')
    time.sleep(0.15)
    return json.loads(raw)
local = datetime.fromisoformat(stamp).replace(tzinfo=ZoneInfo('Asia/Taipei'))
value = evaluate_cached(local, Path(root), compute, cache_root=Path(target))
print(json.dumps(value['protected']))
'''
    args = [sys.executable, '-c', program, str(root), str(target), str(calls), local.isoformat(),
            json.dumps(decision(local))]
    processes = [subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for _ in range(4)]
    for process in processes:
        stdout, stderr = process.communicate(timeout=15)
        assert process.returncode == 0, stderr
        assert json.loads(stdout) is True
    assert calls.read_text() == 'x'


def test_corrupt_or_redirected_cache_falls_back_to_canonical(context, tmp_path):
    root, target, local = context
    target.mkdir(mode=0o700)
    (target/'decision.json').write_text('{bad json')
    os.chmod(target/'decision.json', 0o600)
    assert cache.evaluate_cached(local, root, lambda: decision(local), cache_root=target)['protected']
    redirected = tmp_path/'redirected'
    redirected.symlink_to(target, target_is_directory=True)
    calls = []
    cache.evaluate_cached(local, root, lambda: calls.append(1) or decision(local), cache_root=redirected)
    assert calls == [1]


def test_cache_tampering_cannot_override_verified_session(context):
    root, target, local = context
    cache.evaluate_cached(local, root, lambda: decision(local), cache_root=target)
    path = target/'decision.json'
    data = json.loads(path.read_text())
    data['decision'].update(allowed=True, protected=False, stock_session=False)
    path.write_text(json.dumps(data))
    calls = []
    result = cache.evaluate_cached(local, root, lambda: calls.append(1) or decision(local), cache_root=target)
    assert calls == [1] and result['allowed'] is False


@pytest.mark.parametrize(('change', 'cached'), [
    ({}, True),
    ({'observed_at': '2026-11-09T10:00:00+08:00'}, False),
    ({'minimum_runway_minutes': 0}, False),
    ({'minimum_runway_minutes': 20}, True),
    ({'protected_until': '13:30:00'}, False),
    ({'calendar_refresh': True}, False),
    ({'session_status': True}, False),
])
def test_cli_cache_is_confined_to_exact_storage_guard(monkeypatch, capsys, change, cached):
    from scripts import check_outside_tw_opening_resource_window as guard
    root = Path(guard.__file__).resolve().parents[1]/'data_tw_public'
    values = {'observed_at': None, 'minimum_runway_minutes': 120, 'protected_until': '13:30',
              'official_calendar_root': root, 'calendar_refresh': False, 'session_status': False}
    monkeypatch.setattr(guard, 'parse_args', lambda: SimpleNamespace(**{**values, **change}))
    local = datetime(2026, 11, 9, 10, tzinfo=ZoneInfo('Asia/Taipei'))
    monkeypatch.setattr(guard, 'evaluate', lambda *_a, **_kw: decision(local))
    calls = []
    def wrapped(_local, _root, compute):
        calls.append(1)
        return compute()
    monkeypatch.setattr(cache, 'evaluate_cached', wrapped)
    assert guard.main() == (0 if change.get('session_status') else 1)
    assert bool(calls) is cached
    assert json.loads(capsys.readouterr().out)['protected'] is True


@pytest.mark.parametrize(('hour', 'session', 'installer_allowed'), [(7, True, True), (8, True, False), (8, False, True)])
def test_installer_reuses_the_session_cache_without_inheriting_an_early_start_block(
        tmp_path, monkeypatch, capsys, hour, session, installer_allowed):
    from scripts import check_outside_tw_opening_resource_window as guard
    local = datetime(2026, 11, 10, hour, 30, tzinfo=ZoneInfo('Asia/Taipei'))
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return local
    monkeypatch.setattr(guard, 'datetime', Clock)
    root = Path(guard.__file__).resolve().parents[1]/'data_tw_public'
    values = {'observed_at': None, 'minimum_runway_minutes': 120, 'protected_until': '13:30',
              'official_calendar_root': root, 'calendar_refresh': False, 'session_status': False}
    monkeypatch.setattr(guard, 'parse_args', lambda: SimpleNamespace(**values))
    calls = []
    def canonical(observed, **options):
        calls.append(options['minimum_runway_minutes'])
        return decision(observed, session=session)
    monkeypatch.setattr(guard, 'evaluate', canonical)
    original = cache.evaluate_cached
    monkeypatch.setattr(cache, 'evaluate_cached', lambda at, directory, compute:
        original(at, directory, compute, cache_root=tmp_path/'cache'))
    assert guard.main() == (1 if session else 0)
    capsys.readouterr()
    values['minimum_runway_minutes'] = 20
    assert guard.main() == (0 if installer_allowed else 1)
    value = json.loads(capsys.readouterr().out)
    assert value['allowed'] is installer_allowed and value['protected_start'] == '08:00:00'
    assert value['minimum_runway_minutes'] == 20 and value['stock_session'] is session
    assert calls == [120]

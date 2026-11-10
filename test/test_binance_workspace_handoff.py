from __future__ import annotations

from contextlib import ExitStack
from datetime import datetime, time
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from downloader import download_binance_perp_15m as binance
from downloader import materialize_ohlcv_daily as daily
from scripts import run_workspace_source_handoff as runner
from scripts.check_outside_tw_opening_resource_window import evaluate
from stockagent.data_sync import workspace_source_handoff as handoff
from stockagent.data_sync.desync_snapshots import SnapshotError


def test_large_scan_waits_for_tw_close_with_eight_hour_runway():
    zone = ZoneInfo('Asia/Taipei')
    def allowed(stamp):
        return evaluate(datetime.fromisoformat(stamp).replace(tzinfo=zone),
            minimum_runway_minutes=500, protected_until=time(13, 30))['allowed']
    assert not allowed('2026-11-06T08:49:00')
    assert not allowed('2026-11-06T13:29:59')
    assert allowed('2026-11-06T13:30:00')


@pytest.mark.parametrize('source_name', ['BINANCE_NAME', 'BINANCE_ARCHIVE_NAME'])
def test_timer_defers_new_scan_but_boot_keeps_recovery_available(tmp_path, monkeypatch, capsys, source_name):
    monkeypatch.setattr(handoff, 'CONTROL', tmp_path / 'isolated-control')
    selected = getattr(handoff, source_name)
    if source_name == 'BINANCE_ARCHIVE_NAME':
        monkeypatch.setitem(handoff.EXTRA_SOURCES, selected, ('data_binance_archive', 'binance-archive'))
    monkeypatch.setattr(handoff, 'registered_names', lambda: (selected,))
    monkeypatch.setattr(handoff, 'read_journal', lambda: None)
    monkeypatch.setattr(runner, 'large_binance_handoff_allowed', lambda: False)
    calls = []
    monkeypatch.setattr(handoff, 'handoff', lambda *, boot: calls.append(boot) or {'state': 'not_enrolled'})
    monkeypatch.setattr('sys.argv', ['handoff'])
    assert runner.main() == 75 and calls == []
    assert 'tw_day_trade_priority_window' in capsys.readouterr().out
    monkeypatch.setattr('sys.argv', ['handoff', '--boot'])
    assert runner.main() == 0 and calls == [True]


@pytest.mark.parametrize('source_name', ['BINANCE_NAME', 'BINANCE_ARCHIVE_NAME'])
def test_monitor_reports_only_the_earliest_eligible_time(tmp_path, monkeypatch, source_name):
    from stockagent.data_sync import storage_progress
    from scripts import run_workspace_source_staging as staging
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 11, 6, 8, 49, tzinfo=tz)
    monkeypatch.setattr(storage_progress, 'datetime', Clock)
    monkeypatch.setattr(handoff, 'ROOT', tmp_path)
    monkeypatch.setattr(handoff, 'CONTROL', tmp_path/'control')
    selected = getattr(handoff, source_name)
    if source_name == 'BINANCE_ARCHIVE_NAME':
        monkeypatch.setitem(handoff.EXTRA_SOURCES, selected, ('data_binance_archive', 'binance-archive'))
    monkeypatch.setattr(handoff, 'registered_names', lambda: (selected,))
    monkeypatch.setattr(handoff, 'read_journal', lambda: None)
    monkeypatch.setattr(handoff, 'binance_handoff_window_allowed', lambda **kw: False)
    monkeypatch.setattr(handoff, 'enrolled', lambda: ({'name': selected}, tmp_path, None, None, None))
    monkeypatch.setattr(staging, 'verified_report', lambda *_: {'files': 117416, 'logical_bytes': 987654321})
    row = storage_progress.workspace_source_handoff_tasks(tmp_path)[0]
    assert row['state'] == 'waiting' and not row['complete_evidence']
    assert row['total_items'] == 117416 and row['total_bytes'] == 987654321
    assert row['completed_items'] is None and row['completed_bytes'] is None
    assert row['reason'] == 'tw_day_trade_priority_window'
    assert row['next_attempt_at_utc'] == '2026-11-06T05:30:00+00:00'


@pytest.mark.parametrize('side,grain', [(side, grain) for side in ('source', 'destination')
                                       for grain in ('1m', 'daily')])
def test_handoff_fences_the_original_and_e_grain_writer_locks(tmp_path, monkeypatch, side, grain):
    source, destination = tmp_path/'source', tmp_path/'destination'
    for root in (source, destination):
        root.mkdir()
        (root/'.download.lock').write_bytes(b'')
        for sub in ('1m', 'daily'):
            (root/sub).mkdir()
            (root/sub/'.download.lock').write_bytes(b'')
    monkeypatch.setattr(handoff, 'ROOT', tmp_path)
    busy = {'source': source, 'destination': destination}[side]/grain/'.download.lock'
    with handoff.selected_source(handoff.BINANCE_NAME), handoff.lock_file(busy):
        with pytest.raises(BlockingIOError), ExitStack() as stack:
            handoff._lock_writer_roots(stack, source, destination)
    with handoff.selected_source(handoff.BINANCE_NAME), ExitStack() as stack:
        handoff._lock_writer_roots(stack, source, destination)


def test_handoff_rejects_grain_namespace_mismatch(tmp_path, monkeypatch):
    source, destination = tmp_path/'source', tmp_path/'destination'
    for root in (source, destination):
        root.mkdir()
        (root/'.download.lock').write_bytes(b'')
    (source/'1m').mkdir()
    monkeypatch.setattr(handoff, 'ROOT', tmp_path)
    with handoff.selected_source(handoff.BINANCE_NAME), pytest.raises(SnapshotError, match='grain namespace'), ExitStack() as stack:
        handoff._lock_writer_roots(stack, source, destination)


def test_binance_collector_stops_before_provider_work_if_workspace_is_missing(tmp_path, monkeypatch):
    args = SimpleNamespace(tail_only=False, refresh=False, mode='incremental',
                           output_dir=str(tmp_path/'data_binance'/'1m'), lock_timeout_seconds=0)
    monkeypatch.setattr(binance, 'parse_args', lambda: args)
    monkeypatch.setattr(handoff, 'guard_path', lambda *_: (_ for _ in ()).throw(SnapshotError('missing E')))
    monkeypatch.setattr(binance, '_run_locked_download', lambda *a, **kw: pytest.fail('provider work started'))
    with pytest.raises(SnapshotError, match='missing E'):
        binance.main()


def test_daily_materializer_stops_before_output_if_workspace_is_missing(tmp_path, monkeypatch):
    args = SimpleNamespace(input_dir=str(tmp_path/'data_binance'/'1m'),
                           output_dir=str(tmp_path/'data_binance'/'daily'),
                           provider='Binance', lock_timeout_seconds=0)
    monkeypatch.setattr(daily, 'parse_args', lambda: args)
    monkeypatch.setattr(handoff, 'guard_path', lambda *_: (_ for _ in ()).throw(SnapshotError('missing E')))
    monkeypatch.setattr(daily, '_run_locked_materialization', lambda *a, **kw: pytest.fail('output work started'))
    with pytest.raises(SnapshotError, match='missing E'):
        daily.main()


def test_standalone_materializer_imports_repo_guard_without_pythonpath(tmp_path):
    env = dict(os.environ)
    env.pop('PYTHONPATH', None)
    repo = Path(__file__).resolve().parents[1]
    result = subprocess.run([sys.executable, 'downloader/materialize_ohlcv_daily.py',
        '--input-dir', str(tmp_path/'empty-input'), '--output-dir', str(tmp_path/'output'),
        '--provider', 'Binance', '--lock-timeout-seconds', '0'],
        cwd=repo, env=env, text=True, capture_output=True, timeout=30)
    assert result.returncode == 0, result.stderr


def test_standalone_binance_imports_repo_guard_without_provider_call():
    env = dict(os.environ)
    env.pop('PYTHONPATH', None)
    repo = Path(__file__).resolve().parents[1]
    result = subprocess.run([sys.executable, 'downloader/download_binance_perp_1m.py',
        '--output-dir', '/proc/stockagent-binance-import-check/1m', '--lock-timeout-seconds', '0'],
        cwd=repo, env=env, text=True, capture_output=True, timeout=30)
    assert result.returncode != 0 and 'FileNotFoundError' in result.stderr
    assert 'ModuleNotFoundError' not in result.stderr

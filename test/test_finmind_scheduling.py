from datetime import UTC, datetime, timedelta
import json
import sqlite3
from types import SimpleNamespace

from downloader.finmind_account import backfill_budget
from downloader.finmind_scheduling import calendar_next_check, incremental_reservation, Source

NOW = datetime(2026, 9, 30, 9, 10, tzinfo=UTC)  # 17:10 Taipei


def free_current(root, now=NOW):
    (root / 'calendar.json').write_text(json.dumps({'observed_at_utc': now.isoformat(), 'dates': []}))
    folder = root / 'receipts' / 'TaiwanStockInfoWithWarrant'
    folder.mkdir(parents=True, exist_ok=True)
    (folder / '2026-09-30.json').write_text(json.dumps({'status': 'complete'}))


def queue(root, rows):
    folder = root / 'complement'
    folder.mkdir(exist_ok=True)
    with sqlite3.connect(folder / 'queue.sqlite3') as conn:
        conn.execute('CREATE TABLE tasks(dataset TEXT,priority INT,kind TEXT,state TEXT,next_attempt_at_utc TEXT)')
        conn.executemany('INSERT INTO tasks VALUES (?,?,?,?,?)', rows)


def test_only_current_hour_not_previous_or_next_hour(tmp_path):
    free_current(tmp_path)
    queue(tmp_path, [('USStockPrice', 0, 'id_history', 'complete', (NOW + timedelta(hours=2)).isoformat())])
    plan = incremental_reservation(tmp_path, NOW, sources=())
    assert plan['reserve_requests'] == plan['ready_requests'] == 0
    assert plan['window_end_at_utc'] == '2026-09-30T10:00:00+00:00'


def test_ready_preempts_other_worker_but_future_does_not(tmp_path):
    free_current(tmp_path)
    queue(tmp_path, [('TaiwanStockNews', 0, 'day', 'complete', (NOW + timedelta(minutes=20)).isoformat())])
    account = {'observed_at_utc': NOW.isoformat(), 'official_requests_per_hour': 6000, 'provider_used_in_hour': 5}
    plan = incremental_reservation(tmp_path, NOW, sources=())
    assert (plan['ready_requests'], plan['upcoming_requests']) == (0, 1)
    assert backfill_budget(account, tmp_path, fixed_incremental_requests=1, now=NOW, prioritize_due=True)['allowed']
    later = NOW + timedelta(minutes=21)
    account['observed_at_utc'] = later.isoformat()
    budget = backfill_budget(account, tmp_path, fixed_incremental_requests=1, now=later, prioritize_due=True)
    assert not budget['allowed'] and budget['priority_wait'] and budget['remaining'] == 5995


def test_retry_is_not_ready_and_completed_without_retry_costs_nothing(tmp_path):
    free_current(tmp_path)
    queue(tmp_path, [
        ('TaiwanStockNews', 0, 'day', 'failed', (NOW + timedelta(minutes=40)).isoformat()),
        ('done', 0, 'day', 'complete', None), ('derived', 0, 'derived', 'pending', None),
        ('blocked', 0, 'day', 'blocked', None), ('old', 2, 'day', 'pending', None),
    ])
    plan = incremental_reservation(tmp_path, NOW, sources=())
    assert (plan['ready_requests'], plan['upcoming_requests']) == (0, 1)


def test_calendar_hourly_and_publication_boundary(tmp_path):
    free_current(tmp_path, NOW + timedelta(minutes=40))  # 17:50
    assert calendar_next_check(tmp_path, NOW + timedelta(minutes=41)) == datetime(2026, 9, 30, 10, tzinfo=UTC)
    assert calendar_next_check(tmp_path, NOW) == NOW  # Future stamp is not fresh.


def test_calendar_failure_retries_without_refreshing_observation(tmp_path):
    free_current(tmp_path, NOW - timedelta(hours=2))
    retry = NOW + timedelta(minutes=5)
    (tmp_path / 'calendar_refresh.json').write_text(json.dumps({
        'observed_at_utc': NOW.isoformat(), 'retry_at_utc': retry.isoformat(), 'state': 'http_502'}))
    assert calendar_next_check(tmp_path, NOW) == retry
    assert json.loads((tmp_path / 'calendar.json').read_text())['observed_at_utc'] != NOW.isoformat()


def test_bad_calendar_metadata_cannot_crash_other_workers_budget_reads(tmp_path):
    free_current(tmp_path)
    for dates in (None, ['1900-not-a-date']):
        (tmp_path / 'calendar.json').write_text(json.dumps({'observed_at_utc': NOW.isoformat(), 'dates': dates}))
        assert incremental_reservation(tmp_path, NOW, sources=())['queue_errors'] == []


def test_release_uses_verified_stock_calendar_not_for_news(tmp_path):
    free_current(tmp_path)
    sources = (Source('TaiwanStockPrice', None, 'day', release_hour=17, release_minute=30),
               Source('TaiwanStockDividend', None, 'day', release_hour=17, release_minute=30))
    closed = lambda *_a, **_k: SimpleNamespace(status='closed')
    plan = incremental_reservation(tmp_path, NOW, sources=sources, day_decision=closed)
    assert [row['dataset'] for row in plan['events']] == ['TaiwanStockDividend']


def test_future_local_request_does_not_deplete_current_quota(tmp_path):
    free_current(tmp_path)
    with sqlite3.connect(tmp_path / 'request_traffic.sqlite3') as conn:
        conn.execute('CREATE TABLE requests(started_at_utc TEXT)')
        conn.execute('INSERT INTO requests VALUES (?)', ((NOW + timedelta(seconds=1)).isoformat(),))
    budget = backfill_budget({'observed_at_utc': NOW.isoformat(), 'official_requests_per_hour': 6000,
                             'provider_used_in_hour': 8}, tmp_path, fixed_incremental_requests=0, now=NOW)
    assert budget['used_estimate'] == 8


def test_completed_sponsor_owner_does_not_leave_a_phantom_complement_reserve(tmp_path):
    free_current(tmp_path)
    sponsor = tmp_path / 'sponsor'
    sponsor.mkdir()
    (sponsor / 'status.json').write_text(json.dumps({
        'observed_at_utc': NOW.isoformat(), 'state': 'running', 'tier': 'Sponsor',
        'series': {'TaiwanStockPrice': {'target': 100, 'blocked': 0}},
    }))
    queue(tmp_path, [('TaiwanStockPrice', 0, 'id_history', 'pending', None)])
    assert incremental_reservation(tmp_path, NOW, sources=())['ready_requests'] == 0


def test_long_batch_account_probe_failure_is_not_repeated_per_data_call(tmp_path, monkeypatch):
    from downloader import finmind_account as account
    calls = []
    def unavailable(*_a):
        calls.append(True)
        raise RuntimeError('provider unavailable')
    monkeypatch.setattr(account, 'verified_account', unavailable)
    previous = {'observed_at_utc': (NOW - timedelta(minutes=2)).isoformat()}
    assert account.refresh_dispatch_account(previous, 'fixture', tmp_path, NOW) == previous
    assert account.refresh_dispatch_account(previous, 'fixture', tmp_path, NOW + timedelta(seconds=10)) == previous
    assert len(calls) == 1
    account.refresh_dispatch_account(previous, 'fixture', tmp_path, NOW + timedelta(seconds=60))
    assert len(calls) == 2


def test_large_due_universe_does_not_starve_other_incremental_datasets(tmp_path):
    from downloader import download_finmind_complement as worker
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        worker._add_tasks(conn, [('EuropeStockPrice', str(i), 'history', 'id_history', 0) for i in range(50)])
        worker._add_tasks(conn, [('TaiwanStockNews', '', '2026-09-30', 'day', 0),
                                 ('USStockPrice', 'A', 'history', 'id_history', 0),
                                 ('TaiwanFuturesKBar', 'TX', '2020-01-02', 'id_day', 1)])
        picked = [worker._next_task(conn, NOW, advance_cursor=True).dataset for _ in range(3)]
        assert picked == ['EuropeStockPrice', 'TaiwanStockNews', 'USStockPrice']
        cursor = conn.execute('SELECT * FROM dispatch_cursor').fetchall()
        assert worker._next_task(conn, NOW).dataset == 'EuropeStockPrice'
        assert conn.execute('SELECT * FROM dispatch_cursor').fetchall() == cursor

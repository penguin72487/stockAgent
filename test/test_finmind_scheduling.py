from datetime import UTC, datetime, timedelta
import json
import sqlite3
from types import SimpleNamespace

import pytest

from downloader.finmind_account import backfill_budget
from downloader.finmind_scheduling import calendar_next_check, incremental_reservation, Source
from downloader.finmind_scheduling import ReservationReads

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


def test_reused_reservations_see_commits_and_clock_boundaries_without_reopening(tmp_path):
    free_current(tmp_path)
    due = NOW + timedelta(minutes=1)
    queue(tmp_path, [('new-release', 0, 'day', 'pending', due.isoformat())])
    path = tmp_path / 'complement/queue.sqlite3'
    with ReservationReads(tmp_path) as scope:
        before = incremental_reservation(tmp_path, NOW, sources=())
        connection = scope.connections[path.resolve()][1]
        assert (before['ready_requests'], before['upcoming_requests']) == (0, 1)
        at_due = incremental_reservation(tmp_path, due, sources=())
        assert (at_due['ready_requests'], at_due['upcoming_requests']) == (1, 0)
        with sqlite3.connect(path) as writer:
            writer.execute("UPDATE tasks SET state='complete',next_attempt_at_utc=NULL")
        assert incremental_reservation(tmp_path, due, sources=())['reserve_requests'] == 0
        assert scope.connections[path.resolve()][1] is connection
        assert not connection.in_transaction
    assert not scope.connections and not scope.metadata
    with pytest.raises(sqlite3.ProgrammingError):
        connection.execute('SELECT 1')


def test_reused_reservations_observe_metadata_replacement_corruption_and_new_queue(tmp_path):
    free_current(tmp_path)
    path = tmp_path / 'calendar.json'
    with ReservationReads(tmp_path) as scope:
        assert incremental_reservation(tmp_path, NOW, sources=())['ready_requests'] == 0
        replacement = tmp_path / 'calendar-next.json'
        replacement.write_text(json.dumps({'observed_at_utc': (NOW-timedelta(hours=2)).isoformat()}))
        replacement.replace(path)
        assert incremental_reservation(tmp_path, NOW, sources=())['ready_requests'] == 1
        path.write_text('{broken')
        assert incremental_reservation(tmp_path, NOW, sources=())['ready_requests'] == 1
        assert path not in scope.metadata  # Never reuse the old successful metadata.
        free_current(tmp_path)
        queue(tmp_path, [('created-after-scope', 0, 'day', 'pending', None)])
        assert incremental_reservation(tmp_path, NOW, sources=())['ready_requests'] == 1


def test_reused_reservations_reopen_replaced_queue_and_reset_scope_after_exception(tmp_path):
    from downloader.finmind_scheduling import _RESERVATION_READS
    free_current(tmp_path)
    queue(tmp_path, [('original', 0, 'day', 'pending', None)])
    path = tmp_path / 'complement/queue.sqlite3'
    with pytest.raises(RuntimeError, match='fixture interruption'):
        with ReservationReads(tmp_path) as scope:
            assert incremental_reservation(tmp_path, NOW, sources=())['ready_requests'] == 1
            previous = scope.connections[path.resolve()][1]
            replacement = path.with_name('replacement.sqlite3')
            with sqlite3.connect(replacement) as writer:
                writer.execute('CREATE TABLE tasks(dataset TEXT,priority INT,kind TEXT,state TEXT,next_attempt_at_utc TEXT)')
            replacement.replace(path)
            assert incremental_reservation(tmp_path, NOW, sources=())['ready_requests'] == 0
            assert scope.connections[path.resolve()][1] is not previous
            path.write_bytes(b'corrupt synthetic fixture')
            assert incremental_reservation(tmp_path, NOW, sources=())['queue_errors'] == ['complement']
            raise RuntimeError('fixture interruption')
    assert _RESERVATION_READS.get() is None


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


def _release_queue(indexed):
    from downloader.finmind_queue_indexes import ensure_release_reserve_index
    conn = sqlite3.connect(':memory:')
    conn.execute('CREATE TABLE tasks(dataset TEXT,data_id TEXT,partition TEXT,priority INT,kind TEXT,'
                 'state TEXT,next_attempt_at_utc TEXT,error_code TEXT,rows INT)')
    if indexed:
        ensure_release_reserve_index(conn)
    return conn


@pytest.mark.parametrize('indexed', [False, True])
def test_release_ranges_preserve_null_times_states_priorities_and_exact_horizon(indexed):
    from collections import Counter
    from itertools import product
    from downloader.finmind_scheduling import _reservation_rows
    horizon = NOW.replace(minute=0) + timedelta(hours=1)
    cutoff = horizon.isoformat()
    expected = Counter()
    with _release_queue(indexed) as conn:
        for number, (state, due, priority, kind) in enumerate(product(
                ('pending', 'failed', 'complete', 'observed_empty', 'blocked', 'retry_exhausted', None),
                (None, NOW.isoformat(), cutoff, (horizon + timedelta(seconds=1)).isoformat(), 'invalid'),
                (0, 1, None), ('day', 'derived', None))):
            dataset = 'market' + str(number % 2)
            conn.execute('INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?)',
                         (dataset, str(number), 'fixture', priority, kind, state, due, None, 0))
            if priority == 0 and kind == 'day' and (
                    (state == 'pending' and due is None)
                    or (state in {'pending', 'failed', 'complete', 'observed_empty'}
                        and due is not None and due <= cutoff)):
                expected[(dataset, due)] += 1
        actual = _reservation_rows(conn, horizon)
        assert actual == [(dataset, due, count) for (dataset, due), count in sorted(
            expected.items(), key=lambda item: (item[0][0], item[0][1] is not None, item[0][1] or ''))]


@pytest.mark.parametrize('probe_due', [False, True])
def test_release_ranges_keep_cohort_canary_and_unrelated_errors(probe_due):
    from downloader import finmind_retry_cohorts as retry
    from downloader.finmind_scheduling import _reservation_rows
    horizon = NOW.replace(minute=0) + timedelta(hours=1)
    dataset = sorted(retry.DAILY_EQUITY)[0]
    with _release_queue(True) as conn:
        retry.ensure_schema(conn)
        conn.execute('INSERT INTO finmind_retry_cohorts(dataset,active,canary_id,next_probe_at_utc) '
                     'VALUES (?,1,?,?)', (dataset, 'canary',
                     (horizon + timedelta(seconds=-1 if probe_due else 1)).isoformat()))
        for identifier, state, error, count in [('canary', 'failed', retry.ERROR, 5),
                ('peer', 'failed', retry.ERROR, 5), ('empty', 'failed', retry.ERROR, 0),
                ('other-error', 'failed', 'network_error', 5), ('pending', 'pending', retry.ERROR, 5)]:
            conn.execute('INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?)',
                         (dataset, identifier, 'history', 0, 'id_history', state, NOW.isoformat(), error, count))
        assert _reservation_rows(conn, horizon) == [(dataset, NOW.isoformat(), 3 + int(probe_due))]


def test_release_ranges_do_not_scan_future_priority_zero_keys():
    from downloader.finmind_scheduling import _reservation_rows
    horizon = NOW.replace(minute=0) + timedelta(hours=1)
    with _release_queue(True) as conn:
        conn.executemany('INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?)', [
            ('market', 'ready', 'fixture', 0, 'day', 'pending', None, None, 0),
            ('market', 'retry', 'fixture', 0, 'day', 'failed', NOW.isoformat(), None, 0)])
        def steps():
            count = 0
            def progress():
                nonlocal count
                count += 1
                return 0
            conn.set_progress_handler(progress, 100)
            try:
                assert sum(row[2] for row in _reservation_rows(conn, horizon)) == 2
            finally:
                conn.set_progress_handler(None, 0)
            return count
        initial = steps()
        conn.executemany('INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?)',
                         [('future', str(i), 'fixture', 0, 'day', 'pending',
                           (horizon + timedelta(days=10)).isoformat(), None, 0) for i in range(5000)])
        assert steps() <= initial + 2

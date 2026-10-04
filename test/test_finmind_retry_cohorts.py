"""Source failures consume quota, not proof that missing work disappeared."""
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import json
import sqlite3

import pytest

from downloader import download_finmind_complement as worker
from downloader import download_finmind_sponsor as sponsor
from downloader import finmind_retry_cohorts as cohorts
from downloader import finmind_runtime as runtime
from downloader import finmind_scheduling as scheduling

NOW = datetime(2026, 10, 3, 16, tzinfo=UTC)


def seed(conn, count=131):
    worker._add_tasks(conn, [('UKStockPrice', f'{index:04}.L', 'history', 'id_history', 0)
                             for index in range(count)])
    conn.execute("UPDATE tasks SET state='failed',rows=20,receipt_path='original_receipt',"
                 "last_attempt_at_utc=?,next_attempt_at_utc=?,error_code=? WHERE dataset='UKStockPrice'",
                 ((NOW - timedelta(minutes=20)).isoformat(), NOW.isoformat(), cohorts.ERROR))
    conn.commit()


def fail(conn, index, now=NOW, code=cohorts.ERROR, retry=900):
    task = worker.Task('UKStockPrice', f'{index:04}.L', 'history', 'id_history', 0, 'failed')
    worker._save_failure(conn, task, worker.SourceError(code, retry_after=retry), now)
    return task


def activate(conn):
    for index in range(3):
        fail(conn, index)


def test_distinct_failure_cohort_retains_every_key_and_rotates_minute_probes(tmp_path):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        seed(conn)
        worker._add_tasks(conn, [('TaiwanStockKBar', '2330', '2026-09-30', 'id_day', 8)])
        activate(conn)
        assert worker._next_task(conn, NOW).dataset == 'TaiwanStockKBar'
        probes = []
        for minute in range(1, 61):
            now = NOW + timedelta(minutes=minute)
            task = worker._next_task(conn, now)
            assert task.dataset == 'UKStockPrice'
            probes.append(task.data_id)
            worker._save_failure(conn, task, worker.SourceError(cohorts.ERROR, retry_after=900), now)
            assert worker._next_task(conn, now).dataset == 'TaiwanStockKBar'
        assert len(set(probes)) == 60
        assert conn.execute("SELECT count(*),sum(rows) FROM tasks WHERE dataset='UKStockPrice' AND state='failed'").fetchone() == (131, 2620)
        assert conn.execute("SELECT count(*) FROM tasks WHERE dataset='UKStockPrice' AND receipt_path='original_receipt'").fetchone()[0] == 131
        untouched = conn.execute("SELECT count(*) FROM tasks WHERE dataset='UKStockPrice' AND last_attempt_at_utc=?",
                                 ((NOW - timedelta(minutes=20)).isoformat(),)).fetchone()[0]
        assert untouched == 68  # Only 3 real failures + 60 real probes update attempts.
        assert cohorts.summary(conn)[0]['source_unavailable_proven'] is False


def test_one_symbol_old_evidence_and_normal_empties_never_open_cohort(tmp_path):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        seed(conn, 3)
        for _ in range(4):
            fail(conn, 0)
        assert cohorts.summary(conn) == []
        fail(conn, 1, NOW + timedelta(seconds=901))
        fail(conn, 2, NOW + timedelta(seconds=902))
        assert cohorts.summary(conn) == []
        conn.execute("UPDATE tasks SET rows=0 WHERE data_id='0000.L'")
        fail(conn, 0, NOW + timedelta(seconds=902))
        assert cohorts.summary(conn) == []


def test_valid_probe_reopens_all_repairs_immediately_and_unrelated_success_does_not(tmp_path):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        seed(conn, 8)
        activate(conn)
        task = worker._next_task(conn, NOW + timedelta(minutes=1))
        assert cohorts.full_probe_required(conn, task)
        assert not cohorts.record_recovery(conn, replace(task, data_id='0007.L'), 20, NOW)
        assert not cohorts.record_recovery(conn, task, 0, NOW)
        assert cohorts.record_recovery(conn, task, 20, NOW + timedelta(minutes=1))
        assert not cohorts.summary(conn)[0]['active']
        assert conn.execute("SELECT count(*) FROM tasks WHERE dataset='UKStockPrice' AND state='failed' AND next_attempt_at_utc<=?",
                            ((NOW + timedelta(minutes=1)).isoformat(),)).fetchone()[0] == 8


def test_restart_keeps_gate_and_never_drops_null_error_or_unrelated_failures(tmp_path):
    path = tmp_path / 'queue.sqlite3'
    with worker._db(path) as conn:
        seed(conn, 5)
        activate(conn)
        conn.execute("UPDATE tasks SET error_code=NULL,next_attempt_at_utc=? WHERE data_id='0004.L'", (NOW.isoformat(),))
        conn.commit()
    with worker._db(path) as conn:
        task = worker._next_task(conn, NOW)
        assert task.data_id == '0004.L'
        assert cohorts.summary(conn)[0]['active']
        assert worker._next_task(conn, NOW, required_keys=(('UKStockPrice', '0001.L', 'history'),)) is None


@pytest.mark.parametrize('code', ['ReadTimeout', 'provider_bad_request', 'not_entitled'])
def test_probe_other_error_preserves_timeout_or_rotates_terminal_without_stranding_peers(tmp_path, code):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        seed(conn, 5)
        activate(conn)
        now = NOW + timedelta(minutes=1)
        task = worker._next_task(conn, now)
        worker._save_failure(conn, task, worker.SourceError(code, retry_after=300), now)
        if code == 'not_entitled':
            assert not cohorts.summary(conn)[0]['active']
            assert worker._next_task(conn, now + timedelta(hours=1)) is None
            assert conn.execute('SELECT count(*) FROM finmind_entitlement_holds').fetchone()[0] == 5
        elif code == 'provider_bad_request':
            following = worker._next_task(conn, now + timedelta(minutes=1))
            assert following and following.data_id != task.data_id
        else:
            assert worker._next_task(conn, now + timedelta(seconds=299)) is None
            following = worker._next_task(conn, now + timedelta(seconds=300))
            assert following.data_id == task.data_id
            assert cohorts.full_probe_required(conn, following)


def test_shared_quota_reserves_one_probe_not_all_failed_copies(tmp_path, monkeypatch):
    monkeypatch.setattr(scheduling, '_free_refresh_events', lambda *_a: [])
    monkeypatch.setattr(scheduling, 'active_sponsor_aliases', lambda *_a: frozenset())
    with worker._db(tmp_path / 'complement' / 'queue.sqlite3') as conn:
        seed(conn)
        activate(conn)
    before = scheduling.incremental_reservation(tmp_path, NOW)
    ready = scheduling.incremental_reservation(tmp_path, NOW + timedelta(minutes=1))
    assert before['reserve_requests'] == before['upcoming_requests'] == 1
    assert before['ready_requests'] == 0
    assert ready['ready_requests'] == ready['reserve_requests'] == 1


def test_old_readonly_queue_adds_no_schema(tmp_path):
    path = tmp_path / 'old.sqlite3'
    with sqlite3.connect(path) as conn:
        conn.execute('CREATE TABLE tasks(dataset TEXT)')
    with sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True) as conn:
        assert cohorts.admission_clause(conn, NOW) == ('', ())
        assert cohorts.summary(conn) == []


@pytest.mark.parametrize('code,expected', [(sqlite3.SQLITE_BUSY, True), (sqlite3.SQLITE_LOCKED, True),
                                        (sqlite3.SQLITE_BUSY | 0x100, True), (sqlite3.SQLITE_ERROR, False),
                                        (sqlite3.SQLITE_IOERR, False), (sqlite3.SQLITE_CORRUPT, False)])
def test_retryable_sqlite_errors_use_codes_not_strings(code, expected):
    error = sqlite3.OperationalError('no persisted sensitive exception text')
    error.sqlite_errorcode = code
    assert runtime.retryable_queue_error(error) is expected


@pytest.mark.parametrize('owner', [worker, sponsor])
def test_loop_recovers_busy_under_same_lock_and_nonbusy_propagates(tmp_path, monkeypatch, owner):
    error = sqlite3.OperationalError('sensitive text never printed')
    error.sqlite_errorcode = sqlite3.SQLITE_BUSY
    calls = []
    def cycle(*_a, **kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            raise error
        raise KeyboardInterrupt
    waits = []
    monkeypatch.setattr(owner, 'run_once', cycle)
    monkeypatch.setattr(owner, 'wait_for_next_cycle', lambda root, result, delay, **kwargs: waits.append((result, delay)))
    assert owner.main(['--root', str(tmp_path), '--loop', '--max-requests', '120']) == 130
    assert len(calls) == 2 and calls[0] == calls[1]
    assert waits == [({'state': 'queue_busy', 'error_code': 'sqlite_busy'}, 5)]
    error.sqlite_errorcode = sqlite3.SQLITE_ERROR
    calls.clear()
    with pytest.raises(sqlite3.OperationalError):
        owner.main(['--root', str(tmp_path), '--loop'])


def test_batch_ready_continues_without_artificial_sleep_and_keeps_limiter_owner(tmp_path, monkeypatch):
    results = iter([{'state': 'batch_complete', 'next_task': {'dataset': 'TaiwanStockKBar'}},
                    {'state': 'batch_complete', 'next_task': None}])
    def cycle(*_a, **_k):
        try:
            return next(results)
        except StopIteration:
            raise KeyboardInterrupt
    delays = []
    monkeypatch.setattr(worker, 'run_once', cycle)
    monkeypatch.setattr(worker, 'wait_for_next_cycle', lambda _root, _result, delay, **_k: delays.append(delay))
    assert worker.main(['--root', str(tmp_path), '--loop']) == 130
    assert delays == [0, 5]


@pytest.mark.parametrize('owner', [worker, sponsor])
def test_early_cycle_return_closes_queue_without_garbage_collection(tmp_path, monkeypatch, owner):
    opened = []
    original = worker._db
    def db(path):
        conn = original(path)
        opened.append(conn)  # Keep strong refs: GC cannot hide a leaked handle.
        return conn
    monkeypatch.setattr(owner, '_db', db)
    monkeypatch.setattr(owner, 'load_env_file', lambda *_a, **_k: None)
    monkeypatch.setenv('FINMIND_TOKEN', 'unit-test-placeholder')
    if owner is sponsor:
        def unavailable(*_a, **_k):
            raise ValueError('unit test unverified account')
        monkeypatch.setattr(owner, 'verified_account', unavailable)
    else:
        account = {'tier': 'Sponsor', 'official_requests_per_hour': 6000,
                   'observed_at_utc': NOW.isoformat(), 'provider_used_in_hour': 0}
        monkeypatch.setattr(owner, 'verified_account', lambda *_a, **_k: account)
        monkeypatch.setattr(owner, 'refresh_dispatch_account', lambda *_a, **_k: account)
        monkeypatch.setattr(owner, '_now', lambda: NOW)
        monkeypatch.setattr(owner, '_populate', lambda *_a, **_k: None)
        monkeypatch.setattr(owner, 'incremental_reservation', lambda *_a, **_k: {
            'reserve_requests': 0, 'ready_requests': 0, 'queue_errors': [], 'observed_at_utc': NOW.isoformat()})
    monkeypatch.setattr(owner.requests.Session, 'get', lambda *_a, **_k: pytest.fail('unexpected network call'))
    for _ in range(3):
        result = owner.run_once(tmp_path, max_requests=1)
        assert result['state'] in {'account_unverified', 'current_queue'}
    assert len(opened) == 3
    for conn in opened:
        with pytest.raises(sqlite3.ProgrammingError, match='closed'):
            conn.execute('SELECT 1')


def test_zero_delay_does_not_rescan_queue_or_sleep(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime, 'next_cycle_delay', lambda *_a, **_k: pytest.fail('idle queue scan on ready batch'))
    runtime.wait_for_next_cycle(tmp_path, {'state': 'batch_complete'}, 0,
                                sleep=lambda *_a: pytest.fail('artificial sleep'))
    assert json.loads((tmp_path / 'worker_status.json').read_bytes())['phase'] == 'running'


@pytest.mark.parametrize('corrupt_baseline', [False, True])
def test_worker_empty_retry_never_publishes_old_history_as_recovered_or_zero(tmp_path, monkeypatch, corrupt_baseline):
    from downloader import finmind_integrity as integrity
    root = tmp_path / 'complement'
    with worker._db(root / 'queue.sqlite3') as conn:
        seed(conn, 1)
    monkeypatch.setenv('FINMIND_TOKEN', 'unit-test-placeholder')
    monkeypatch.setattr(worker, 'load_env_file', lambda *_a, **_k: None)
    monkeypatch.setattr(worker, '_now', lambda: NOW)
    monkeypatch.setattr(worker, '_populate', lambda *_a, **_k: None)
    monkeypatch.setattr(worker, 'MIN_FREE_BYTES', 0)
    monkeypatch.setattr(integrity, 'audit_completed_batch', lambda *_a, **_k: {})
    account = {'tier': 'Sponsor', 'official_requests_per_hour': 6000,
               'observed_at_utc': NOW.isoformat(), 'provider_used_in_hour': 0}
    monkeypatch.setattr(worker, 'verified_account', lambda *_a, **_k: account)
    monkeypatch.setattr(worker, 'refresh_dispatch_account', lambda *_a, **_k: account)
    monkeypatch.setattr(worker, 'incremental_reservation', lambda *_a, **_k: {
        'reserve_requests': 0, 'ready_requests': 0, 'queue_errors': [], 'observed_at_utc': NOW.isoformat()})
    def baseline(*_a):
        if corrupt_baseline:
            raise ValueError('invalid local baseline')
        return {'source_last_date': '2026-10-01', 'fetched_at_utc': (NOW - timedelta(hours=1)).isoformat()}, [
            {'date': '2026-10-01', 'stock_id': '0000.L', 'Close': 100}]
    monkeypatch.setattr(worker, 'read_baseline', baseline)
    requested = []
    def fetch(_session, _limiter, _root, _dataset, _token, params, **_kwargs):
        requested.append(params)
        return []
    monkeypatch.setattr(worker, '_fetch_rows', fetch)
    monkeypatch.setattr(worker, '_store', lambda *_a, **_k: pytest.fail('published fake recovery / zero replacement'))
    worker.run_once(root, max_requests=1)
    assert requested[0]['start_date'] == '1900-01-01'
    with worker._db(root / 'queue.sqlite3') as conn:
        expected = 'empty_response_without_valid_baseline' if corrupt_baseline else cohorts.ERROR
        assert conn.execute("SELECT state,error_code,rows,receipt_path FROM tasks WHERE dataset='UKStockPrice'").fetchone() == (
            'failed', expected, 20, 'original_receipt')

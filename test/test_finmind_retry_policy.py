"""Exhaustion is a recorded gap, not accepted source data or infinite retry."""
from datetime import UTC, datetime, timedelta
import json

import pytest

from downloader import download_finmind_complement as worker
from downloader import download_finmind_sponsor as sponsor
from downloader import finmind_retry_policy as policy
from downloader import finmind_retry_cohorts as cohorts
from downloader import finmind_scheduling as scheduling
from downloader import finmind_eta_work as work
from downloader import finmind_updates as updates

NOW = datetime(2026, 10, 4, 1, tzinfo=UTC)


def seed(conn, dataset='UKStockPrice', identifier='BAD.L', kind='id_history', priority=0):
    task = worker.Task(dataset, identifier, 'history' if kind == 'id_history' else '2026-10-02', kind, priority, 'pending')
    worker._add_tasks(conn, [(task.dataset, task.data_id, task.partition, kind, priority)])
    conn.execute('UPDATE tasks SET rows=20,bytes=123,first_data_date=?,last_data_date=?,receipt_path=?',
                 ('2020-01-01', '2026-10-02', 'original_receipt'))
    conn.commit()
    return task


@pytest.mark.parametrize('owner', [worker, sponsor])
def test_five_real_failures_persist_exhaustion_and_keep_last_good_data(tmp_path, owner):
    path = tmp_path / 'queue.sqlite3'
    with worker._db(path) as conn:
        task = seed(conn) if owner is worker else seed(conn, 'TaiwanStockPrice', '', 'day', 2)
        save = worker._save_failure if owner is worker else sponsor._fail
        for index in range(5):
            stamp = NOW + timedelta(seconds=index)
            save(conn, task, worker.SourceError(cohorts.ERROR), stamp)
            assert conn.execute('SELECT state FROM tasks').fetchone()[0] == ('failed' if index < 4 else policy.EXHAUSTED)
        before = conn.execute('SELECT rows,bytes,first_data_date,last_data_date,receipt_path,last_attempt_at_utc,error_code FROM tasks').fetchone()
        assert before == (20, 123, '2020-01-01', '2026-10-02', 'original_receipt', stamp.isoformat(), cohorts.ERROR)
        assert conn.execute('SELECT next_attempt_at_utc FROM tasks').fetchone()[0] is None
        event = json.loads(conn.execute('SELECT metadata_json FROM finmind_retry_events').fetchone()[0])
        assert event['failures'] == 5 and event['source_unavailable_proven'] is False
    with worker._db(path) as conn:
        assert conn.execute('SELECT state FROM tasks').fetchone()[0] == policy.EXHAUSTED
        assert conn.execute('SELECT rows,bytes,first_data_date,last_data_date,receipt_path,last_attempt_at_utc,error_code FROM tasks').fetchone() == before
        assert worker._next_task(conn, NOW + timedelta(days=1)) is None


def test_legacy_per_key_observations_migrate_once_without_new_api_calls(tmp_path):
    path = tmp_path / 'queue.sqlite3'
    with worker._db(path) as conn:
        task = seed(conn)
        # Emulate pre-policy observations without using the new failure saver.
        for index in range(8):
            updates.record_failure(tmp_path, task, cohorts.ERROR, NOW + timedelta(seconds=index))
        conn.execute("UPDATE tasks SET state='failed',error_code=?,last_attempt_at_utc=?",
                     (cohorts.ERROR, (NOW + timedelta(seconds=7)).isoformat()))
        conn.execute('DELETE FROM finmind_retry_policy_migrations')
        conn.commit()
    with worker._db(path) as conn:
        assert conn.execute('SELECT failures FROM finmind_task_retries').fetchone()[0] == 8
        assert conn.execute('SELECT state FROM tasks').fetchone()[0] == policy.EXHAUSTED
        assert conn.execute("SELECT count(*) FROM finmind_retry_events WHERE event='retry_exhausted'").fetchone()[0] == 1
    with worker._db(path) as conn:
        assert conn.execute('SELECT failures FROM finmind_task_retries').fetchone()[0] == 8
        assert conn.execute("SELECT count(*) FROM finmind_retry_events WHERE event='retry_exhausted'").fetchone()[0] == 1


def test_repeated_save_of_same_attempt_is_idempotent_and_verified_success_resets(tmp_path):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        task = seed(conn)
        for _ in range(10):
            worker._save_failure(conn, task, worker.SourceError('ReadTimeout'), NOW)
        assert conn.execute('SELECT failures FROM finmind_task_retries').fetchone()[0] == 1
        policy.record_success(conn, task, NOW + timedelta(seconds=1))
        assert conn.execute('SELECT failures,cycle FROM finmind_task_retries').fetchone() == (0, 2)
        worker._save_failure(conn, task, worker.SourceError('ReadTimeout'), NOW + timedelta(seconds=2))
        assert conn.execute('SELECT failures FROM finmind_task_retries').fetchone()[0] == 1


@pytest.mark.parametrize('owner', [worker, sponsor])
@pytest.mark.parametrize('code', ['rate_limited', 'ip_banned', 'local_traffic_busy', 'local_traffic_unavailable'])
def test_account_or_local_accounting_holds_never_retire_real_source_work(tmp_path, owner, code):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        task = seed(conn) if owner is worker else seed(conn, 'TaiwanStockPrice', '', 'day', 2)
        save = worker._save_failure if owner is worker else sponsor._fail
        for index in range(10):
            save(conn, task, worker.SourceError(code), NOW + timedelta(seconds=index))
        assert conn.execute('SELECT state FROM tasks').fetchone()[0] == 'failed'
        assert conn.execute('SELECT count(*) FROM finmind_task_retries').fetchone()[0] == 0


def test_exhausted_canary_cannot_strand_other_symbols_or_spend_reserve(tmp_path, monkeypatch):
    monkeypatch.setattr(scheduling, '_free_refresh_events', lambda *_a: [])
    monkeypatch.setattr(scheduling, 'active_sponsor_aliases', lambda *_a: frozenset())
    with worker._db(tmp_path / 'complement/queue.sqlite3') as conn:
        worker._add_tasks(conn, [('UKStockPrice', f'{n}.L', 'history', 'id_history', 0) for n in range(3)])
        conn.execute('UPDATE tasks SET rows=20')
        for n in range(3):
            task = worker.Task('UKStockPrice', f'{n}.L', 'history', 'id_history', 0, 'failed')
            for index in range(5):
                worker._save_failure(conn, task, worker.SourceError(cohorts.ERROR), NOW + timedelta(seconds=index + n * 10))
        assert not any(row['active'] for row in cohorts.summary(conn))
        worker._add_tasks(conn, [('TaiwanStockKBar', '2330', '2026-10-02', 'id_day', 8)])
        assert worker._next_task(conn, NOW + timedelta(hours=1)).dataset == 'TaiwanStockKBar'
    reservation = scheduling.incremental_reservation(tmp_path, NOW + timedelta(hours=1))
    assert reservation['reserve_requests'] == reservation['ready_requests'] == 0


def test_exhausted_gap_is_separate_from_eta_requests_and_done_counts(tmp_path):
    path = tmp_path / 'complement/queue.sqlite3'
    with worker._db(path) as conn:
        task = seed(conn)
        for index in range(5):
            worker._save_failure(conn, task, worker.SourceError(cohorts.ERROR), NOW + timedelta(seconds=index))
    rows, evidence = work._queue(path, 'complement', NOW + timedelta(hours=1))
    assert evidence['state'] == 'observed'
    row = work._observed_row('UKStockPrice', 'complement', {'query_shape': 'per_id_full_history'}, rows['UKStockPrice'], NOW)
    assert row['retry_exhausted_tasks'] == 1
    assert row['current_plan_requests'] == row['retry_tasks'] == row['completed_tasks'] == row['blocked_tasks'] == 0
    assert row['state_counts'][policy.EXHAUSTED] == 1


def test_only_explicit_scope_can_reopen_without_losing_exhaustion_audit(tmp_path):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        task = seed(conn)
        for index in range(5):
            worker._save_failure(conn, task, worker.SourceError(cohorts.ERROR), NOW + timedelta(seconds=index))
        with pytest.raises(ValueError, match='requires_dataset'):
            policy.reopen(conn, (), NOW)
        assert policy.reopen(conn, ('USStockPrice',), NOW) == 0
        assert policy.reopen(conn, ('UKStockPrice',), NOW + timedelta(minutes=1)) == 1
        conn.commit()
        reopened = worker._next_task(conn, NOW + timedelta(minutes=1))
        assert cohorts.full_probe_required(conn, reopened)
        assert conn.execute('SELECT failures,cycle FROM finmind_task_retries').fetchone() == (0, 2)
        assert conn.execute('SELECT count(*) FROM finmind_retry_events').fetchone()[0] == 2
        assert conn.execute('SELECT rows,receipt_path FROM tasks').fetchone() == (20, 'original_receipt')


def test_unreadable_old_observations_use_only_proven_queue_failure_and_do_not_stop_owner(tmp_path, caplog):
    path = tmp_path / 'queue.sqlite3'
    with worker._db(path) as conn:
        seed(conn)
        conn.execute("UPDATE tasks SET state='failed',error_code=?,last_attempt_at_utc=?,next_attempt_at_utc=?",
                     (cohorts.ERROR, NOW.isoformat(), NOW.isoformat()))
        conn.execute('DELETE FROM finmind_retry_policy_migrations')
        conn.commit()
    (tmp_path / 'update_observations.sqlite3').write_bytes(b'broken old observation sidecar')
    with worker._db(path) as conn:
        assert conn.execute('SELECT failures FROM finmind_task_retries').fetchone()[0] == 1
        assert conn.execute('SELECT state FROM tasks').fetchone()[0] == 'failed'
        assert worker._next_task(conn, NOW).data_id == 'BAD.L'
        evidence = json.loads(conn.execute("SELECT metadata_json FROM finmind_retry_events WHERE event='legacy_import'").fetchone()[0])
        assert evidence['basis'] == 'queue_proven_last_failure_only'
    assert 'finmind_retry_legacy_observation_unavailable' in caplog.text


@pytest.mark.parametrize('owner,dataset,identifier,kind', [
    (worker, 'UKStockPrice', 'BAD.L', 'id_history'),
    (sponsor, 'TaiwanStockPrice', '', 'day')])
def test_explicit_reopen_cli_requires_scope_and_never_calls_provider(tmp_path, monkeypatch, owner, dataset, identifier, kind):
    monkeypatch.setattr(owner, 'run_once', lambda *_a, **_k: pytest.fail('reopen must not start provider calls'))
    with pytest.raises(SystemExit):
        owner.main(['--root', str(tmp_path), '--reopen-exhausted'])
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        task = seed(conn, dataset, identifier, kind)
        save = worker._save_failure if owner is worker else sponsor._fail
        for index in range(5):
            save(conn, task, worker.SourceError(cohorts.ERROR), NOW + timedelta(seconds=index))
    assert owner.main(['--root', str(tmp_path), '--reopen-exhausted', '--dataset', dataset]) == 0
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        assert conn.execute('SELECT state FROM tasks').fetchone()[0] == 'failed'
        assert conn.execute('SELECT failures,cycle FROM finmind_task_retries').fetchone() == (0, 2)

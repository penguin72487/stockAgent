from datetime import UTC, date, datetime
import fcntl
import json

import pytest

from downloader import download_finmind_complement as worker
from downloader import finmind_storage_objects as objects
from downloader import finmind_supplemental as supplemental
from downloader.finmind_history_calendar import load_closures


DATASET = 'TaiwanOptionTick'
SOURCE = supplemental.Source('market', date(2026, 9, 1), 'day', 10, 6,
                             endpoint='storage_objects', identity_field='option_id')


def setup_frontier(conn, *, existing_forward=False):
    worker._add_tasks(conn, [(DATASET, '', f'2026-09-{day:02}', 'id_day', 10)
                             for day in range(1, 6)])
    if existing_forward:
        worker._add_tasks(conn, [(DATASET, '', '2026-10-04', 'id_day', 10)])
    conn.execute('CREATE TABLE finmind_source_frontiers ('
                 'dataset TEXT,data_id TEXT,older_than TEXT,newer_than TEXT,PRIMARY KEY(dataset,data_id))')
    conn.execute('INSERT INTO finmind_source_frontiers VALUES (?,?,NULL,?)',
                 (DATASET, '', '2026-10-03'))
    conn.commit()


def refill(conn, anchor=date(2026, 10, 6), source=SOURCE):
    return supplemental._refill_source(conn, DATASET, source, anchor, load_closures(conn))


def test_forward_workset_is_bounded_independently_of_historic_and_targeted_tasks(tmp_path, monkeypatch):
    monkeypatch.setattr(supplemental, 'WORKING_SET', 2)
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        setup_frontier(conn)
        worker._add_tasks(conn, [(DATASET, 'TXO', '2026-09-10', 'id_day', 10)])
        before = conn.execute('SELECT * FROM tasks ORDER BY data_id,partition').fetchall()
        assert refill(conn) == 2
        assert conn.execute('SELECT newer_than FROM finmind_source_frontiers').fetchone() == ('2026-10-05',)
        assert conn.execute('SELECT partition FROM finmind_object_forward_workset ORDER BY partition').fetchall() == [
            ('2026-10-04',), ('2026-10-05',)]
        assert refill(conn) == 0
        assert conn.execute('SELECT newer_than FROM finmind_source_frontiers').fetchone() == ('2026-10-05',)
        assert conn.execute("SELECT * FROM tasks WHERE partition<'2026-10-01' ORDER BY data_id,partition").fetchall() == before

        conn.execute("UPDATE tasks SET state='complete',last_attempt_at_utc='2026-10-07T00:00:00+00:00' "
                     "WHERE dataset=? AND data_id='' AND partition='2026-10-04'", (DATASET,))
        assert refill(conn) == 1
        assert conn.execute('SELECT partition FROM finmind_object_forward_workset ORDER BY partition').fetchall() == [
            ('2026-10-05',), ('2026-10-06',)]


def test_existing_forward_task_counts_once_and_failed_task_keeps_retry_state(tmp_path, monkeypatch):
    monkeypatch.setattr(supplemental, 'WORKING_SET', 2)
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        setup_frontier(conn, existing_forward=True)
        assert refill(conn) == 1
        assert refill(conn) == 0
        assert conn.execute('SELECT partition FROM finmind_object_forward_workset ORDER BY partition').fetchall() == [
            ('2026-10-04',), ('2026-10-05',)]
        conn.execute("UPDATE tasks SET state='failed',last_attempt_at_utc='2026-10-07T00:00:00+00:00',"
                     "next_attempt_at_utc='2026-10-08T00:00:00+00:00',error_code='source_error' "
                     "WHERE dataset=? AND data_id='' AND partition='2026-10-04'", (DATASET,))
        assert refill(conn) == 1
        assert conn.execute("SELECT state,next_attempt_at_utc,error_code FROM tasks WHERE dataset=? AND "
                            "data_id='' AND partition='2026-10-04'", (DATASET,)).fetchone() == (
            'failed', '2026-10-08T00:00:00+00:00', 'source_error')


def test_workset_and_cursor_roll_back_with_canonical_transaction(tmp_path, monkeypatch):
    monkeypatch.setattr(supplemental, 'WORKING_SET', 2)
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        setup_frontier(conn)
        conn.execute('BEGIN')
        assert refill(conn) == 2
        conn.rollback()
        assert conn.execute('SELECT newer_than FROM finmind_source_frontiers').fetchone() == ('2026-10-03',)
        assert conn.execute("SELECT count(*) FROM tasks WHERE partition>='2026-10-01'").fetchone() == (0,)
        assert conn.execute("SELECT count(*) FROM sqlite_master WHERE name='finmind_object_forward_workset'").fetchone() == (0,)


def test_non_object_source_keeps_existing_workset_policy(tmp_path, monkeypatch):
    monkeypatch.setattr(supplemental, 'WORKING_SET', 2)
    source = supplemental.Source('options', date(2026, 9, 1), 'day', 10, 6, identity_field='option_id')
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        setup_frontier(conn)
        assert refill(conn, source=source) == 0
        assert conn.execute('SELECT newer_than FROM finmind_source_frontiers').fetchone() == ('2026-10-03',)
        assert conn.execute("SELECT count(*) FROM sqlite_master WHERE name='finmind_object_forward_workset'").fetchone() == (0,)


def test_owner_frontier_only_uses_existing_queue_and_shared_lock_without_source_calls(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(supplemental, 'SOURCES', {DATASET: SOURCE})
    monkeypatch.setattr(supplemental, 'WORKING_SET', 2)
    now = datetime(2026, 10, 6, 8, tzinfo=UTC)
    monkeypatch.setattr(worker, '_now', lambda: now)
    monkeypatch.setattr(worker.object_pipeline, 'initialize_cpu', lambda *_: None)
    monkeypatch.setattr(worker, 'load_env_file', lambda *_a, **_k: pytest.fail('credential access'))
    monkeypatch.setattr(worker, 'verified_account', lambda *_a, **_k: pytest.fail('provider access'))
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        setup_frontier(conn)
        objects.configure(conn, {'tier': 'SponsorPro'}, now)
    args = ['--root', str(tmp_path), '--refill-frontier-only', DATASET]
    with (tmp_path / 'worker.lock').open('a+') as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert worker.main(args) == 2
        fcntl.flock(held, fcntl.LOCK_UN)
    assert worker.main(args) == 0
    result = json.loads(capsys.readouterr().out.strip())
    assert result == {'state': 'frontier_refilled', 'dataset': DATASET, 'inserted_tasks': 2,
                      'provider_calls': 0}
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        assert conn.execute('SELECT newer_than FROM finmind_source_frontiers').fetchone() == ('2026-10-05',)
        assert conn.execute("SELECT count(*) FROM tasks WHERE partition<'2026-10-01'").fetchone() == (5,)


def test_owner_frontier_only_rejects_missing_frontier_and_mixed_dispatch(tmp_path, monkeypatch):
    monkeypatch.setattr(worker.object_pipeline, 'initialize_cpu', lambda *_: None)
    with worker._db(tmp_path / 'queue.sqlite3'):
        pass
    args = ['--root', str(tmp_path), '--refill-frontier-only', DATASET]
    with pytest.raises(ValueError, match='existing canonical source frontier'):
        worker.main(args)
    with pytest.raises(SystemExit):
        worker.main(args + ['--dataset', DATASET])

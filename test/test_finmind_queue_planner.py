"""Planner maintenance preserves task evidence and initialization recovery."""
from contextlib import closing
import sqlite3

import pytest

from downloader import download_finmind_complement as worker
from downloader import download_finmind_sponsor as sponsor
from downloader import finmind_runtime as runtime


def test_shared_initializer_collects_statistics_without_changing_tasks(tmp_path):
    path = tmp_path / 'queue.sqlite3'
    assert sponsor._db is worker._db
    with closing(worker._db(path)) as conn:
        worker._add_tasks(conn, [('TaiwanStockKBar', str(i), '2026-10-02', 'id_day', 8)
                                 for i in range(1200)])
        conn.execute("UPDATE tasks SET state='complete',rows=42,bytes=512,"
                     "last_attempt_at_utc='2026-10-03T17:00:00+00:00',"
                     "next_attempt_at_utc='2026-11-03T17:00:00+00:00',receipt_path='unchanged.json'")
        conn.commit()
        before = conn.execute('SELECT * FROM tasks ORDER BY data_id').fetchall()
    with closing(worker._db(path)) as conn:
        assert conn.execute('SELECT * FROM tasks ORDER BY data_id').fetchall() == before
        assert conn.execute("SELECT count(*) FROM sqlite_stat1 WHERE tbl='tasks'").fetchone()[0] > 0
        assert conn.execute('PRAGMA journal_mode').fetchone()[0] == 'wal'
        assert conn.execute('PRAGMA synchronous').fetchone()[0] == 2  # FULL, not a throughput shortcut.


@pytest.mark.parametrize('previous', [0, 100, 5000])
def test_older_sqlite_analysis_is_bounded_and_restores_caller_setting(monkeypatch, previous):
    monkeypatch.setattr(runtime.sqlite3, 'sqlite_version_info', (3, 45, 0))
    with closing(sqlite3.connect(':memory:')) as conn:
        conn.execute(f'PRAGMA analysis_limit={previous}')
        queries = []
        conn.set_trace_callback(queries.append)
        runtime.optimize_queue(conn)
        assert f'PRAGMA analysis_limit={min(previous, 1000) if previous else 1000}' in queries
        assert 'PRAGMA optimize=0x10002' in queries
        assert conn.execute('PRAGMA analysis_limit').fetchone()[0] == previous
        assert not any(query.startswith('ANALYZE') or query.startswith('VACUUM') for query in queries)


def test_modern_sqlite_keeps_existing_analysis_setting(monkeypatch):
    monkeypatch.setattr(runtime.sqlite3, 'sqlite_version_info', (3, 46, 0))
    with closing(sqlite3.connect(':memory:')) as conn:
        conn.execute('PRAGMA analysis_limit=200')
        runtime.optimize_queue(conn)
        assert conn.execute('PRAGMA analysis_limit').fetchone()[0] == 200


@pytest.mark.parametrize('busy', [True, False])
def test_initializer_failure_closes_connection_and_preserves_error(tmp_path, monkeypatch, busy):
    conn = sqlite3.connect(tmp_path / 'queue.sqlite3')
    error = sqlite3.OperationalError('bounded planner failure')
    error.sqlite_errorcode = sqlite3.SQLITE_BUSY if busy else sqlite3.SQLITE_ERROR
    monkeypatch.setattr(worker.sqlite3, 'connect', lambda *a, **k: conn)
    def fail(connection):
        assert connection is conn
        raise error
    monkeypatch.setattr(worker, 'optimize_queue', fail)
    with pytest.raises(sqlite3.OperationalError) as found:
        worker._db(tmp_path / 'queue.sqlite3')
    assert found.value is error
    assert runtime.retryable_queue_error(found.value) is busy
    with pytest.raises(sqlite3.ProgrammingError, match='closed'):
        conn.execute('SELECT 1')

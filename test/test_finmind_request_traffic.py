"""One durable ledger shared by all FinMind lanes, before any outbound call."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import sqlite3
from unittest.mock import Mock

import pytest

from downloader import download_finmind_free as free
from downloader import download_finmind_complement as complement


def test_wal_reader_does_not_block_three_writers_and_preserves_old_rows(tmp_path):
    path = tmp_path / 'request_traffic.sqlite3'
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute('CREATE TABLE requests(started_at_utc TEXT NOT NULL,dataset TEXT NOT NULL)')
        connection.execute("INSERT INTO requests VALUES ('2026-09-30T00:00:00+00:00','old')")
    free._record_request_start(tmp_path, 'initial')
    with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)) as reader:
        assert reader.execute('PRAGMA journal_mode').fetchone()[0] == 'wal'
        reader.execute('BEGIN')
        assert reader.execute('SELECT count(*) FROM requests').fetchone()[0] == 2

        def write(lane):
            for _ in range(25):
                free._record_request_start(tmp_path, lane)

        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = [pool.submit(write, lane) for lane in ('free', 'sponsor', 'complement')]
            for future in futures:
                future.result(timeout=10)
        # Snapshot consistency and writer progress must both hold.
        assert reader.execute('SELECT count(*) FROM requests').fetchone()[0] == 2
    with closing(sqlite3.connect(path)) as connection:
        assert dict(connection.execute('SELECT dataset,count(*) FROM requests GROUP BY dataset')) == {
            'old': 1, 'initial': 1, 'free': 25, 'sponsor': 25, 'complement': 25}


@pytest.mark.parametrize('lane', ['free', 'complement'])
def test_exclusive_writer_contention_defers_without_untracked_api(tmp_path, monkeypatch, lane):
    free._record_request_start(tmp_path, 'initial')
    monkeypatch.setattr(free, 'TRAFFIC_BUSY_TIMEOUT_SECONDS', 0.02)
    session, limiter = Mock(), Mock()
    path = tmp_path / 'request_traffic.sqlite3'
    with closing(sqlite3.connect(path)) as writer:
        writer.execute('BEGIN IMMEDIATE')
        expected = free.ProviderError if lane == 'free' else complement.SourceError
        with pytest.raises(expected) as failure:
            if lane == 'free':
                free._request(session, limiter, 'test', start_date=None,
                              token='private-test-token', traffic_root=tmp_path)
            else:
                complement._fetch_rows(session, limiter, tmp_path, 'test', 'private-test-token', {})
        assert failure.value.code == 'local_traffic_busy'
        assert failure.value.retry_after == 15
        session.get.assert_not_called()
        limiter.wait.assert_called_once()
        assert writer.execute('SELECT count(*) FROM requests').fetchone()[0] == 1


def test_ledger_failure_uses_existing_queue_retry_not_permanent_source_failure(tmp_path, monkeypatch):
    free._record_request_start(tmp_path, 'initial')
    monkeypatch.setattr(free, 'TRAFFIC_BUSY_TIMEOUT_SECONDS', 0.02)
    task = complement.Task('TaiwanFuturesKBar', 'TX', '2026-09-30', 'id_day', 8, 'pending')
    with closing(complement._db(tmp_path / 'complement' / 'queue.sqlite3')) as queue:
        queue.execute('INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) VALUES (?,?,?,?,?,?)',
                      (task.dataset, task.data_id, task.partition, task.kind, task.priority, task.state))
        queue.commit()
        with closing(sqlite3.connect(tmp_path / 'request_traffic.sqlite3')) as writer:
            writer.execute('BEGIN IMMEDIATE')
            with pytest.raises(complement.SourceError) as failure:
                complement._fetch_rows(Mock(), Mock(), tmp_path, task.dataset, 'private', {})
            now = complement._now()
            complement._save_failure(queue, task, failure.value, now)
        row = queue.execute('SELECT state,error_code,next_attempt_at_utc FROM tasks').fetchone()
        assert row == ('failed', 'local_traffic_busy', (now + complement.timedelta(seconds=15)).isoformat())


def test_write_connection_is_closed_not_only_committed(tmp_path, monkeypatch):
    closed = []
    original = sqlite3.connect

    class Connection(sqlite3.Connection):
        def close(self):
            closed.append(True)
            super().close()

    monkeypatch.setattr(free.sqlite3, 'connect', lambda *a, **kw: original(*a, **kw, factory=Connection))
    free._record_request_start(tmp_path, 'test')
    assert closed == [True]

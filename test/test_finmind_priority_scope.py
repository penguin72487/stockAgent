"""A finite override's dataset seek must not change queue selection semantics."""
from contextlib import closing
from datetime import UTC, datetime, timedelta

import pytest

from downloader import download_finmind_complement as worker
from downloader.finmind_queue_indexes import STATUS_INDEX


NOW = datetime(2026, 10, 3, 17, tzinfo=UTC)


def _override(conn, keys):
    conn.execute('CREATE TABLE finmind_priority_tasks(dataset TEXT,data_id TEXT,partition TEXT,'
                 'PRIMARY KEY(dataset,data_id,partition))')
    conn.executemany('INSERT INTO finmind_priority_tasks VALUES (?,?,?)', keys)


@pytest.mark.parametrize("status_index", (False, True))
def test_finished_overrides_seek_only_their_datasets_then_use_normal_order(tmp_path, status_index):
    with closing(worker._db(tmp_path / 'queue.sqlite3')) as conn, conn:
        if not status_index:
            conn.execute(f"DROP INDEX {STATUS_INDEX}")
        worker._add_tasks(conn, [('TaiwanFuturesKBar', 'TX', '2026-10-02', 'id_day', 8),
                                ('TaiwanExchangeRate', 'USD', 'history', 'id_history', 1)])
        _override(conn, [('TaiwanFuturesKBar', 'TX', '2026-10-02')])
        conn.execute("UPDATE tasks SET state='complete' WHERE dataset='TaiwanFuturesKBar'")
        queries = []
        conn.set_trace_callback(queries.append)
        assert worker._next_task(conn, NOW).dataset == 'TaiwanExchangeRate'
        conn.set_trace_callback(None)
        scoped = next(q for q in queries if 'WITH RECURSIVE override_datasets' in q)
        plan = conn.execute('EXPLAIN QUERY PLAN ' + scoped).fetchall()
        # The prior accepted covering status projection can also seek the exact
        # dataset + outstanding-state prefixes. Require that bounded plan, not
        # a specific index name that fails on the unchanged predecessor too.
        assert any(any(index in row[-1] for index in ('idx_finmind_outstanding_dispatch', STATUS_INDEX))
                   and 'dataset=?' in row[-1] and 'state=?' in row[-1] for row in plan)


def test_multiple_override_datasets_keep_partition_and_identifier_order(tmp_path):
    with closing(worker._db(tmp_path / 'queue.sqlite3')) as conn, conn:
        keys = [('TaiwanExchangeRate', 'USD', '2024'), ('TaiwanBusinessIndicator', '', '2025')]
        worker._add_tasks(conn, [(d, i, p, 'year', 3) for d, i, p in keys])
        _override(conn, keys)
        assert worker._next_task(conn, NOW).dataset == 'TaiwanBusinessIndicator'
        assert worker._next_task(conn, NOW, delegated=frozenset({'TaiwanBusinessIndicator'})).dataset == 'TaiwanExchangeRate'


def test_priority_zero_and_cooling_failures_still_govern_override_admission(tmp_path):
    with closing(worker._db(tmp_path / 'queue.sqlite3')) as conn, conn:
        worker._add_tasks(conn, [('TaiwanStockInfo', '', 'latest', 'snapshot', 0),
                                ('TaiwanExchangeRate', 'USD', 'history', 'id_history', 3)])
        _override(conn, [('TaiwanExchangeRate', 'USD', 'history')])
        assert worker._next_task(conn, NOW).dataset == 'TaiwanStockInfo'
        conn.execute("UPDATE tasks SET state='failed',next_attempt_at_utc=? WHERE dataset='TaiwanExchangeRate'",
                     ((NOW + timedelta(minutes=5)).isoformat(),))
        assert worker._next_task(conn, NOW, background_only=True) is None


def test_empty_override_table_does_not_suppress_ordinary_pending_work(tmp_path):
    with closing(worker._db(tmp_path / 'queue.sqlite3')) as conn, conn:
        worker._add_tasks(conn, [('TaiwanExchangeRate', 'USD', 'history', 'id_history', 1)])
        _override(conn, [])
        assert worker._next_task(conn, NOW).data_id == 'USD'

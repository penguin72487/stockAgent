from datetime import UTC, datetime, timedelta
import json
import sqlite3

import pytest

from downloader import download_finmind_complement as worker
from downloader.finmind_queue_indexes import STATUS_COLUMNS, STATUS_INDEX, dispatch_indexes, ensure_status_index
from downloader.finmind_queue_indexes import (
    RELEASE_RESERVE_INDEX, RELEASE_RESERVE_COLUMNS, ensure_release_reserve_index, release_reserve_index,
    PERIODIC_INDEX, PERIODIC_COLUMNS, ensure_periodic_index, periodic_index,
    ETA_WORK_INDEX, ETA_RECENT_INDEX, ETA_WORK_COLUMNS, ETA_RECENT_COLUMNS,
    ensure_eta_read_indexes, eta_work_index, eta_recent_index,
)
from scripts.benchmark_finmind_status import AGGREGATE_SQL, claim_release, main
from downloader.finmind_queue_indexes import (
    MONITOR_COUNTS_INDEX, ensure_monitor_counts_index, monitor_counts_index, monitor_counts_sql,
)


_BAR_COUNTS_SQL = """SELECT dataset,SUM(rows),COUNT(DISTINCT first_data_date),
    MIN(first_data_date),MAX(last_data_date),COUNT(*),
    SUM(CASE WHEN first_data_date IS NULL OR last_data_date IS NULL
        OR first_data_date!=last_data_date THEN 1 ELSE 0 END)
    FROM tasks WHERE dataset IN (?,?) AND state='complete' AND rows>0 GROUP BY dataset"""


def _counts_db(path=':memory:'):
    connection = sqlite3.connect(path)
    # No affinity deliberately retains legacy real/text/blob/null values.
    connection.execute('CREATE TABLE tasks(dataset TEXT,state TEXT,rows,bytes,first_data_date,'
                       'last_data_date,last_attempt_at_utc)')
    ensure_status_index(connection)
    connection.execute('DROP INDEX ' + MONITOR_COUNTS_INDEX)
    return connection


@pytest.mark.parametrize('legacy', [None, 1.25, '3x', b'4', float('inf')])
def test_monitor_projection_preserves_types_null_dates_and_legacy_sum_order(legacy):
    with _counts_db() as connection:
        values = [('A', 'complete', 2**51, 0, '2026-01-02', '2026-01-03', None),
                  ('A', 'complete', 3, 0, '2026-01-01', '2026-01-01', None),
                  ('A', 'complete', 5, 0, None, None, None),
                  ('B', 'complete', 7, 0, '2026-01-01', None, None),
                  ('B', 'complete', 0, 0, 'wrong', 'wrong', None),
                  ('B', 'failed', 99, 0, 'wrong', 'wrong', None)]
        if legacy is not None:
            values.append(('A', 'complete', legacy, 0, '2026-01-04', '2026-01-04', None))
        connection.executemany('INSERT INTO tasks VALUES (?,?,?,?,?,?,?)', values)
        expected = connection.execute(_BAR_COUNTS_SQL, ('A', 'B')).fetchall()
        before = connection.total_changes
        ensure_monitor_counts_index(connection)
        ensure_monitor_counts_index(connection)
        selected = monitor_counts_sql(connection, _BAR_COUNTS_SQL, ('A', 'B'))
        assert connection.execute(selected, ('A', 'B')).fetchall() == expected
        assert connection.total_changes == before
        assert not connection.execute("SELECT name FROM sqlite_master WHERE type='trigger'").fetchall()
        assert ('INDEXED BY ' + (STATUS_INDEX if legacy is not None else MONITOR_COUNTS_INDEX)) in selected
        if legacy is None:
            plan = connection.execute('EXPLAIN QUERY PLAN ' + selected, ('A', 'B')).fetchall()
            assert any('COVERING INDEX ' + MONITOR_COUNTS_INDEX in row[3] for row in plan)
        assert monitor_counts_sql(connection, 'SELECT * FROM tasks', ()) == 'SELECT * FROM tasks'
        connection.execute('DROP INDEX ' + MONITOR_COUNTS_INDEX)
        assert monitor_counts_sql(connection, _BAR_COUNTS_SQL, ('A', 'B')) == _BAR_COUNTS_SQL
        connection.execute(f'CREATE INDEX {MONITOR_COUNTS_INDEX} ON tasks(dataset)')
        assert monitor_counts_index(connection) == ''
        assert monitor_counts_sql(connection, _BAR_COUNTS_SQL, ('A', 'B')) == _BAR_COUNTS_SQL


def test_monitor_projection_retains_integer_overflow_failure():
    with _counts_db() as connection:
        connection.executemany('INSERT INTO tasks(dataset,state,rows) VALUES (?,?,?)',
                               [('A', 'complete', 2**62)] * 2)
        with pytest.raises(sqlite3.OperationalError, match='integer overflow'):
            connection.execute(_BAR_COUNTS_SQL, ('A', 'B')).fetchall()
        ensure_monitor_counts_index(connection)
        query = monitor_counts_sql(connection, _BAR_COUNTS_SQL, ('A', 'B'))
        with pytest.raises(sqlite3.OperationalError, match='integer overflow'):
            connection.execute(query, ('A', 'B')).fetchall()


def test_monitor_type_guard_and_aggregate_share_wal_snapshot(tmp_path):
    path = tmp_path / 'queue.sqlite3'
    with _counts_db(path) as connection:
        connection.execute('PRAGMA journal_mode=WAL')
        connection.execute("INSERT INTO tasks(dataset,state,rows) VALUES ('A','complete',2)")
        ensure_monitor_counts_index(connection)
        connection.commit()
        connection.execute('BEGIN')
        query = monitor_counts_sql(connection, _BAR_COUNTS_SQL, ('A', 'B'))
        assert connection.in_transaction
        with sqlite3.connect(path) as writer:
            writer.execute("INSERT INTO tasks(dataset,state,rows) VALUES ('A','complete',1.5)")
        assert connection.execute(query, ('A', 'B')).fetchone()[1] == 2
        connection.rollback()
        query = monitor_counts_sql(connection, _BAR_COUNTS_SQL, ('A', 'B'))
        assert 'INDEXED BY ' + STATUS_INDEX in query
        assert connection.execute(query, ('A', 'B')).fetchone()[1] == 3.5


def test_index_is_idempotent_and_changes_no_rows_or_triggers(tmp_path):
    with worker._db(tmp_path / "queue.sqlite3") as connection:
        connection.execute("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state,rows,bytes) "
                           "VALUES ('GoldPrice','','2026','year',1,'complete',?,?)", (2**52, 2**55))
        connection.commit()
        before = connection.execute("SELECT * FROM tasks").fetchall()
        changes = connection.total_changes
        ensure_status_index(connection)
        ensure_status_index(connection)
        assert connection.total_changes == changes
        assert connection.execute("SELECT * FROM tasks").fetchall() == before
        assert not connection.execute("SELECT name FROM sqlite_master WHERE type='trigger'").fetchall()
        columns = connection.execute(f"PRAGMA index_info({STATUS_INDEX})").fetchall()
        assert tuple(row[2] for row in columns) == STATUS_COLUMNS
        assert any("COVERING INDEX " + STATUS_INDEX in row[3]
                   for row in connection.execute("EXPLAIN QUERY PLAN " + AGGREGATE_SQL))


def test_eta_covering_reads_preserve_due_classes_nulls_and_recent_time_bounds(tmp_path):
    from downloader import finmind_eta_work as work, finmind_eta_telemetry as telemetry

    now = datetime(2026, 10, 24, tzinfo=UTC)
    root = tmp_path / "complement"
    root.mkdir()
    with worker._db(root / "queue.sqlite3") as connection:
        states = ("pending", "complete", "observed_empty", "failed", "inflight", "retry_exhausted",
                  "disabled", "non_session", "not_entitled")
        dates = (None, now - timedelta(days=8), now - timedelta(minutes=1), now, now + timedelta(seconds=1))
        for i, state in enumerate(states):
            for j, stamp in enumerate(dates):
                connection.execute("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state,"
                                   "last_attempt_at_utc,next_attempt_at_utc,rows,bytes) VALUES (?,?,?,?,?,?,?,?,?,?)",
                                   ("USStockPriceMinute", str(i), str(j), "id_day" if j % 2 else "derived",
                                    0 if i % 2 else 12, state, stamp.isoformat() if stamp else None,
                                    stamp.isoformat() if stamp else None, 2**40 if j % 2 else 0, 2**42))
        connection.commit()
        for index in (ETA_WORK_INDEX, ETA_RECENT_INDEX):
            connection.execute(f"DROP INDEX {index}")
        before_work = work._queue(root / "queue.sqlite3", "complement", now)
        before_recent = telemetry._queue_worker(root, "complement", now)
        changes = connection.total_changes
        ensure_eta_read_indexes(connection)
        ensure_eta_read_indexes(connection)
        connection.commit()
        assert connection.total_changes == changes
        assert work._queue(root / "queue.sqlite3", "complement", now) == before_work
        assert telemetry._queue_worker(root, "complement", now) == before_recent
        for index, columns in ((ETA_WORK_INDEX, ETA_WORK_COLUMNS), (ETA_RECENT_INDEX, ETA_RECENT_COLUMNS)):
            assert tuple(row[2] for row in connection.execute(f"PRAGMA index_info({index})")) == columns
        plan = list(connection.execute("EXPLAIN QUERY PLAN SELECT state,kind,sum(rows),sum(bytes) "
                                       f"FROM tasks{eta_recent_index(connection)} "
                                       "WHERE last_attempt_at_utc>? AND last_attempt_at_utc<=? GROUP BY state,kind",
                                       (dates[1].isoformat(), now.isoformat())))
        assert any(f"COVERING INDEX {ETA_RECENT_INDEX}" in row[3]
                   and "last_attempt_at_utc>? AND last_attempt_at_utc<?" in row[3] for row in plan)
        inventory = list(connection.execute("EXPLAIN QUERY PLAN SELECT dataset,kind,priority,state,"
                                           "count(*),min(next_attempt_at_utc),max(next_attempt_at_utc) "
                                           f"FROM tasks{eta_work_index(connection)} GROUP BY 1,2,3,4"))
        assert any(f"COVERING INDEX {ETA_WORK_INDEX}" in row[3] for row in inventory)
        from downloader.finmind_runtime import optimize_queue
        from scripts.reload_finmind_at_idle import eta_projection_readback
        optimize_queue(connection)
        assert eta_projection_readback(connection, now)
        # An exact-schema hint cannot trust only a matching index name.
        for index, reader in ((ETA_WORK_INDEX, eta_work_index), (ETA_RECENT_INDEX, eta_recent_index)):
            connection.execute(f"DROP INDEX {index}")
            assert reader(connection) == ""
            connection.execute(f"CREATE INDEX {index} ON tasks(dataset)")
            assert reader(connection) == ""
        assert eta_projection_readback(connection, now) is None
        assert work._queue(root / "queue.sqlite3", "complement", now) == before_work
        assert telemetry._queue_worker(root, "complement", now) == before_recent


def test_eta_cohort_range_counts_match_all_null_keys_and_avoid_per_row_grouping():
    from downloader.finmind_eta_work import _indexed_work_rows
    from itertools import product

    now = datetime(2026, 10, 24, tzinfo=UTC)
    with sqlite3.connect(':memory:') as connection:
        connection.execute('CREATE TABLE tasks(dataset TEXT,kind TEXT,priority INTEGER,state TEXT,'
                           'next_attempt_at_utc TEXT,last_attempt_at_utc TEXT,rows INTEGER,bytes INTEGER)')
        for dataset, kind, priority, state, stamp in product(
                (None, 'D'), (None, 'id_day'), (None, 0, 12), (None, 'pending', 'complete'),
                (None, (now-timedelta(seconds=1)).isoformat(), now.isoformat(),
                 (now+timedelta(seconds=1)).isoformat(), 'invalid')):
            connection.execute('INSERT INTO tasks(dataset,kind,priority,state,next_attempt_at_utc) VALUES (?,?,?,?,?)',
                               (dataset, kind, priority, state, stamp))
        ensure_eta_read_indexes(connection)
        hint = eta_work_index(connection)
        query = ('SELECT dataset,kind,priority,state,CASE WHEN (state=\'pending\' AND next_attempt_at_utc IS NULL) '
                 'OR next_attempt_at_utc<=? THEN 1 ELSE 0 END,count(*),min(next_attempt_at_utc),'
                 f'max(next_attempt_at_utc),count(next_attempt_at_utc) FROM tasks{hint} '
                 'GROUP BY dataset,kind,priority,state,5')
        expected = list(connection.execute(query, (now.isoformat(),)))
        changes = connection.total_changes
        assert list(_indexed_work_rows(connection, now, hint)) == expected
        assert connection.total_changes == changes
        connection.execute('DELETE FROM tasks')
        connection.executemany('INSERT INTO tasks(dataset,kind,priority,state,next_attempt_at_utc) VALUES (?,?,?,?,?)',
                               [('D', 'id_day', 12, 'complete', now.isoformat())] * 10000)
        connection.execute('ANALYZE')
        counts, results = [], []
        for optimized in (False, True):
            steps = []
            connection.set_progress_handler(lambda: steps.append(1) or 0, 100)
            results.append(list(_indexed_work_rows(connection, now, hint)) if optimized else
                           list(connection.execute(query, (now.isoformat(),))))
            connection.set_progress_handler(None, 0)
            counts.append(len(steps))
        assert results[0] == results[1]
        assert counts[0] > 3 * counts[1]


def test_release_projection_is_exact_read_only_hint_and_legacy_safe(tmp_path):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        before = conn.total_changes
        ensure_release_reserve_index(conn)
        assert release_reserve_index(conn) == f' INDEXED BY {RELEASE_RESERVE_INDEX}'
        assert tuple(row[2] for row in conn.execute(f'PRAGMA index_info({RELEASE_RESERVE_INDEX})')) == RELEASE_RESERVE_COLUMNS
        assert conn.total_changes == before
        conn.execute(f'DROP INDEX {RELEASE_RESERVE_INDEX}')
        assert release_reserve_index(conn) == ''
        conn.execute(f'CREATE INDEX {RELEASE_RESERVE_INDEX} ON tasks(priority)')
        assert release_reserve_index(conn) == ''  # Same name is not the contract.


def test_periodic_cohort_preserves_exact_selection_and_legacy_fallback(tmp_path):
    with worker._db(tmp_path / 'queue.sqlite3') as conn:
        for kind in ('year', 'snapshot', 'id_day', 'derived'):
            for state in ('complete', 'observed_empty', 'failed', 'pending', 'retry_exhausted'):
                for year in ('2013', '2014', '2026', '2027', 'latest'):
                    conn.execute('INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) VALUES (?,?,?,?,?,?)',
                                 (kind, state, year, kind, 3, state))
        original = ('SELECT dataset,data_id,partition,kind,state,last_attempt_at_utc,next_attempt_at_utc '
                    "FROM tasks WHERE state IN ('complete','observed_empty') "
                    "AND (kind='snapshot' OR (kind='year' AND partition=?))")
        expected = sorted(conn.execute(original, ('2026',)).fetchall())
        changes = conn.total_changes
        ensure_periodic_index(conn)
        assert periodic_index(conn) == f' INDEXED BY {PERIODIC_INDEX}'
        assert tuple(row[2] for row in conn.execute(f'PRAGMA index_info({PERIODIC_INDEX})')) == PERIODIC_COLUMNS
        statements = []
        conn.set_trace_callback(statements.append)
        assert sorted(worker._periodic_refresh_tasks(conn, 2026)) == expected
        conn.set_trace_callback(None)
        query = next(sql for sql in statements if sql.startswith('SELECT dataset,data_id,partition'))
        assert any(PERIODIC_INDEX in row[3] for row in conn.execute('EXPLAIN QUERY PLAN ' + query))
        assert conn.total_changes == changes
        conn.execute(f'DROP INDEX {PERIODIC_INDEX}')
        assert periodic_index(conn) == ''
        assert sorted(worker._periodic_refresh_tasks(conn, 2026)) == expected
        conn.execute(f'CREATE INDEX {PERIODIC_INDEX} ON tasks(dataset)')
        assert periodic_index(conn) == ''
        assert sorted(worker._periodic_refresh_tasks(conn, 2026)) == expected


def test_full_status_preserves_states_retained_values_extrema_and_nulls(tmp_path, monkeypatch):
    now = datetime(2026, 10, 14, tzinfo=UTC)
    monkeypatch.setattr(worker, "_now", lambda: now)
    monkeypatch.setattr(worker, "_snapshot_ids", lambda *args: ["2330"])
    monkeypatch.setattr(worker, "_official_delisted_ids", lambda *args: {"1234"})
    states = ("complete", "observed_empty", "pending", "failed", "retry_exhausted", "inflight",
              "not_entitled", "invalid_request", "deprecated_query_shape", "outside_documented_range",
              "identifier_alias", "non_session", "not_observation_date", "object_tier_paused")
    with worker._db(tmp_path / "queue.sqlite3") as connection:
        for index, state in enumerate(states):
            connection.execute(
                "INSERT INTO tasks(dataset,data_id,partition,kind,priority,state,rows,bytes,first_data_date,"
                "last_data_date,last_attempt_at_utc) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                ("GoldPrice", str(index), "2020", "year", 1, state, 0 if index % 2 else 2**40,
                 index * 2**38, None if index % 2 else "2000-01-01", "2020-10-14", now.isoformat()))
        connection.commit()
        # Compare against the exact old planner even after the new canonical
        # initializer has been integrated. Only this disposable test DB changes.
        connection.execute(f"DROP INDEX IF EXISTS {STATUS_INDEX}")
        before = worker._status(connection, tmp_path / "old", state="running", last={"rows": 1})
        ensure_status_index(connection)
        changes = connection.total_changes
        after = worker._status(connection, tmp_path / "new", state="running", last={"rows": 1})
        assert after == before
        assert connection.total_changes == changes
        item = after["series"]["GoldPrice"]
        assert item["target"] == 8
        assert item["rows"] == 2**40
        assert item["retained_rows"] == 2 * 2**40
        assert item["retained_bytes"] == (3 + 4 + 8) * 2**38
        assert item["first_data_date"] == "2000-01-01"
        assert after["candidate_universe"]["additional_official_delisted_ids"] == 1


def test_null_aggregate_and_empty_queue_preserve_sql_results(tmp_path):
    with worker._db(tmp_path / "queue.sqlite3") as connection:
        connection.execute(f"DROP INDEX IF EXISTS {STATUS_INDEX}")
        assert list(connection.execute(AGGREGATE_SQL)) == []
        connection.execute("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) "
                           "VALUES ('GoldPrice','','2026','year',1,'pending')")
        before = list(connection.execute(AGGREGATE_SQL))
        ensure_status_index(connection)
        assert list(connection.execute(AGGREGATE_SQL)) == before
        assert before == [("GoldPrice", "pending", 1, 0, 0, None, None, None)]


def test_dispatch_guards_are_read_only_and_keep_legacy_schema_compatible():
    with sqlite3.connect(":memory:") as connection:
        assert dispatch_indexes(connection) == ("", "", "")
        connection.execute("CREATE TABLE tasks(dataset,state,rows,bytes,first_data_date,last_data_date,last_attempt_at_utc)")
        ensure_status_index(connection)
        assert dispatch_indexes(connection) == ("", "", "")
        connection.execute("CREATE INDEX idx_finmind_outstanding_dispatch ON tasks(state)")
        changes = connection.total_changes
        assert dispatch_indexes(connection) == (" INDEXED BY idx_finmind_outstanding_dispatch", "", "")
        assert connection.total_changes == changes


def test_benchmark_real_claim_release_preserves_unrelated_inflight_and_source(tmp_path, monkeypatch):
    root = tmp_path / "data_finmind" / "complement"
    root.mkdir(parents=True)
    path = root / "queue.sqlite3"
    with worker._db(path) as connection:
        connection.execute(f"DROP INDEX IF EXISTS {STATUS_INDEX}")
        connection.execute("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state,rows,bytes) "
                           "VALUES ('USStockPriceMinute','AAPL','2026-01-02','id_day',12,'complete',390,12345)")
        connection.execute("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) "
                           "VALUES ('USStockPriceMinute','MSFT','2026-01-02','id_day',12,'inflight')")
        worker.object_pipeline.recover_claims(connection)
        connection.execute("INSERT INTO finmind_object_prefetch_claims VALUES "
                           "('USStockPriceMinute','MSFT','2026-01-02','pending')")
        connection.commit()
        before = connection.execute("SELECT * FROM tasks ORDER BY data_id").fetchall()
        task = worker.Task("USStockPriceMinute", "AAPL", "2026-01-02", "id_day", 12, "complete")
        assert claim_release(connection, [task])["exact_task_rows_restored"]
        assert connection.execute("SELECT * FROM tasks ORDER BY data_id").fetchall() == before
        assert connection.execute("SELECT count(*) FROM finmind_object_prefetch_claims").fetchone() == (1,)
    (root / "status.json").write_text(json.dumps({"active_task": {"dataset": "USStockPriceMinute"},
                                                "last_task": {"rows": 390}}))
    monkeypatch.setattr(worker, "_snapshot_ids", lambda *args: [])
    monkeypatch.setattr(worker, "_official_delisted_ids", lambda *args: set())
    output = tmp_path / "accepted.json"
    assert main(["--root", str(root), "--output", str(output), "--repeats", "1", "--write-tasks", "1"]) == 0
    report = json.loads(output.read_bytes())
    assert report["provider_calls"] == report["production_queue_mutations"] == 0
    assert report["code_stable"]
    assert len({trial["whole_status_sha256"] for trial in report["trials"]}) == 1
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT * FROM tasks ORDER BY data_id").fetchall() == before
        assert not connection.execute("SELECT 1 FROM sqlite_master WHERE name=?", (STATUS_INDEX,)).fetchone()
        assert connection.execute("SELECT count(*) FROM finmind_object_prefetch_claims").fetchone() == (1,)
    with pytest.raises(SystemExit):
        main(["--root", str(root), "--output", str(output)])

from datetime import UTC, datetime
import json
import sqlite3

import pytest

from downloader import download_finmind_complement as worker
from downloader.finmind_queue_indexes import STATUS_COLUMNS, STATUS_INDEX, dispatch_indexes, ensure_status_index
from downloader.finmind_queue_indexes import (
    RELEASE_RESERVE_INDEX, RELEASE_RESERVE_COLUMNS, ensure_release_reserve_index, release_reserve_index,
    PERIODIC_INDEX, PERIODIC_COLUMNS, ensure_periodic_index, periodic_index,
)
from scripts.benchmark_finmind_status import AGGREGATE_SQL, claim_release, main


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

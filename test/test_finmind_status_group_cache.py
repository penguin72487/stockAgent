from contextlib import closing
import sqlite3

import pytest

from downloader.finmind_status_aggregates import (
    AGGREGATE_SQL, ROW_FUNCTION, PREFIX, StatusConnection, task_status_aggregates,
)


@pytest.fixture
def queue(tmp_path):
    path = tmp_path / "queue.sqlite3"
    with closing(sqlite3.connect(path, factory=StatusConnection)) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE tasks(dataset TEXT NOT NULL,data_id TEXT NOT NULL,partition TEXT NOT NULL,"
                     "state TEXT NOT NULL,rows INTEGER,bytes INTEGER,first_data_date TEXT,last_data_date TEXT,"
                     "last_attempt_at_utc TEXT,priority INTEGER,next_attempt_at_utc TEXT,"
                     "PRIMARY KEY(dataset,data_id,partition))")
        conn.execute("CREATE INDEX covering ON tasks(dataset,state,rows,bytes,first_data_date,"
                     "last_data_date,last_attempt_at_utc)")
        conn.executemany("INSERT INTO tasks VALUES (?,?,?,'complete',?,?,?,?,'2026-10-15',1,NULL)",
                         [("A", str(i), "history", 2**40 + i, 2**45, "2000-01-01", "2026-10-14")
                          for i in range(8)] + [("B", "x", "history", 0, 0, None, None)])
        conn.commit()
        yield path, conn


def exact(conn):
    assert task_status_aggregates(conn) == conn.execute(AGGREGATE_SQL).fetchall()


def test_unchanged_uses_no_aggregate_scan_and_does_not_write_main(queue):
    _, conn = queue
    before = conn.execute("SELECT * FROM tasks").fetchall()
    changes = conn.total_changes
    exact(conn)
    sql = []
    conn.set_trace_callback(sql.append)
    cached = task_status_aggregates(conn)
    conn.set_trace_callback(None)
    assert cached and not any("SUM(" in q for q in sql)
    assert conn.total_changes == changes
    assert not conn.in_transaction
    assert conn.execute("SELECT * FROM tasks").fetchall() == before
    assert not conn.execute("SELECT name FROM main.sqlite_master WHERE type='trigger'").fetchall()
    assert len(conn.execute("SELECT name FROM temp.sqlite_master WHERE type='trigger'").fetchall()) == 4


@pytest.mark.parametrize("query", [
    "UPDATE tasks SET rows=rows+7,bytes=bytes+3 WHERE dataset='B'",
    "UPDATE tasks SET state='failed',first_data_date=NULL,last_data_date=NULL WHERE dataset='A' AND data_id='0'",
    "UPDATE tasks SET first_data_date='2001-01-01',last_data_date='2025-01-01' WHERE dataset='A'",
    "UPDATE tasks SET last_attempt_at_utc=NULL WHERE dataset='B'",
    "INSERT INTO tasks(dataset,data_id,partition,state) VALUES ('C','new','history','pending')",
    "DELETE FROM tasks WHERE dataset='A'",
    "UPDATE tasks SET dataset='C' WHERE dataset='B'",
    "INSERT OR REPLACE INTO tasks(dataset,data_id,partition,state,rows,bytes) VALUES ('A','0','history','failed',9,8)",
    "UPDATE OR REPLACE tasks SET dataset='A',data_id='0' WHERE dataset='B'",
])
def test_all_mutations_match_exact_sql_including_replace(queue, query):
    _, conn = queue
    exact(conn)
    conn.execute(query)
    exact(conn)  # Uncommitted observations are never cached.
    conn.commit()
    exact(conn)
    exact(conn)


def test_row_deltas_avoid_group_scans_and_rollback_is_safe(queue):
    _, conn = queue
    exact(conn)
    conn.execute("UPDATE tasks SET state='failed' WHERE dataset='B'")
    exact(conn)
    conn.rollback()
    exact(conn)
    conn.execute("UPDATE tasks SET rows=5 WHERE dataset='B'")
    conn.commit()
    sql = []
    conn.set_trace_callback(sql.append)
    task_status_aggregates(conn)
    conn.set_trace_callback(None)
    aggregate = [q for q in sql if "SUM(" in q]
    assert aggregate == []
    exact(conn)


def test_savepoint_rollback_and_failed_statement_do_not_cache_uncommitted_values(queue):
    _, conn = queue
    exact(conn)
    conn.execute("SAVEPOINT original")
    conn.execute("UPDATE tasks SET rows=0 WHERE dataset='A'")
    exact(conn)
    conn.execute("ROLLBACK TO original")
    conn.execute("RELEASE original")
    exact(conn)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("UPDATE OR ABORT tasks SET data_id='0' WHERE dataset='A'")
    exact(conn)
    conn.rollback()
    exact(conn)


def test_external_commit_and_snapshot_are_not_stale(queue):
    path, conn = queue
    exact(conn)
    with closing(sqlite3.connect(path)) as other:
        other.execute("UPDATE tasks SET rows=123,last_data_date='2026-10-15' WHERE dataset='B'")
        other.commit()
        exact(conn)
        conn.execute("BEGIN")
        exact(conn)
        other.execute("DELETE FROM tasks WHERE dataset='B'")
        other.commit()
        exact(conn)  # Match this transaction's snapshot, not the external head.
        conn.rollback()
        exact(conn)


def test_trigger_loss_schema_changes_and_unrelated_writes(queue):
    _, conn = queue
    exact(conn)
    conn.execute(f"DROP TRIGGER temp.{PREFIX}update")
    conn.execute("UPDATE tasks SET rows=999 WHERE dataset='B'")
    conn.commit()
    exact(conn)
    conn.execute("CREATE INDEX test_index ON tasks(data_id)")
    exact(conn)
    conn.execute("UPDATE tasks SET priority=2,next_attempt_at_utc='2030' WHERE dataset='B'")
    conn.commit()
    sql = []
    conn.set_trace_callback(sql.append)
    task_status_aggregates(conn)
    conn.set_trace_callback(None)
    assert not any("SUM(" in q for q in sql)


def test_empty_queue_foreign_read_only_connections_and_bad_value_errors(queue):
    path, conn = queue
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as ro:
        exact(ro)
        assert not ro.execute("SELECT * FROM temp.sqlite_master").fetchall()
    exact(conn)
    conn.execute("DELETE FROM tasks")
    conn.commit()
    assert task_status_aggregates(conn) == []
    conn.execute("INSERT INTO tasks(dataset,data_id,partition,state,rows,bytes) VALUES ('A','0','h','complete',?,1)",
                 (2**63 - 1,))
    conn.execute("INSERT INTO tasks(dataset,data_id,partition,state,rows,bytes) VALUES ('A','1','h','complete',1,1)")
    conn.commit()
    with pytest.raises(sqlite3.OperationalError, match="integer overflow"):
        task_status_aggregates(conn)
    assert not conn.in_transaction
    conn.execute("DELETE FROM tasks WHERE data_id='1'")
    conn.commit()
    exact(conn)


def test_udf_missing_fails_write_instead_of_silently_producing_a_stale_head(queue):
    _, conn = queue
    exact(conn)
    conn.create_function(ROW_FUNCTION, 10, None)
    with pytest.raises(sqlite3.OperationalError):
        conn.execute("UPDATE tasks SET rows=9 WHERE dataset='B'")
    conn.rollback()
    assert conn.execute("SELECT rows FROM tasks WHERE dataset='B'").fetchone() == (0,)


def test_recreated_table_or_disabled_trusted_schema_cannot_reuse_old_groups(queue):
    _, conn = queue
    exact(conn)
    conn.execute("PRAGMA trusted_schema=OFF")
    conn.execute("UPDATE tasks SET rows=111 WHERE dataset='B'")
    conn.commit()
    exact(conn)
    conn.execute("CREATE TABLE replacement AS SELECT * FROM tasks WHERE dataset='B'")
    conn.execute("DROP TABLE tasks")
    conn.execute("ALTER TABLE replacement RENAME TO tasks")
    conn.commit()
    exact(conn)
    conn.execute("UPDATE tasks SET rows=222 WHERE dataset='B'")
    conn.commit()
    exact(conn)


def test_noop_ignored_insert_and_nonaggregate_fields_do_not_rescan(queue):
    _, conn = queue
    exact(conn)
    conn.execute("UPDATE tasks SET rows=rows WHERE dataset='A'")
    conn.execute("INSERT OR IGNORE INTO tasks(dataset,data_id,partition,state) VALUES ('B','x','history','failed')")
    conn.commit()
    sql = []
    conn.set_trace_callback(sql.append)
    task_status_aggregates(conn)
    conn.set_trace_callback(None)
    assert not any("SUM(" in q for q in sql)


def test_seeded_mixed_transactions_always_match_authoritative_sql(queue):
    import random

    _, conn = queue
    rng = random.Random(20261015)
    exact(conn)
    for step in range(160):
        dataset = rng.choice(("A", "B", "C"))
        identifier = str(rng.randrange(8))
        operation = rng.randrange(5)
        if operation == 0:
            conn.execute("INSERT OR REPLACE INTO tasks(dataset,data_id,partition,state,rows,bytes) "
                         "VALUES (?,?,'history',?,?,?)",
                         (dataset, identifier, rng.choice(("complete", "failed", "pending")),
                          rng.randrange(-500, 500), rng.randrange(500)))
        elif operation == 1:
            conn.execute("DELETE FROM tasks WHERE dataset=? AND data_id=?", (dataset, identifier))
        else:
            conn.execute("UPDATE tasks SET state=?,rows=?,first_data_date=?,last_data_date=? "
                         "WHERE dataset=? AND data_id=?",
                         (rng.choice(("complete", "failed", "pending")), rng.randrange(-500, 500),
                          rng.choice((None, "1990-01-01", "2026-10-14")),
                          rng.choice((None, "1990-01-01", "2026-10-14")), dataset, identifier))
        exact(conn)
        if step % 7:
            conn.commit()
        else:
            conn.rollback()
        exact(conn)


def test_frontier_inserts_claims_and_publication_do_not_rescan_history(queue):
    _, conn = queue
    exact(conn)
    conn.executemany("INSERT INTO tasks VALUES ('A',?,'new','pending',0,0,NULL,NULL,NULL,12,NULL)",
                     [(str(i),) for i in range(100)])
    conn.commit()
    for state in ('inflight', 'complete'):
        conn.execute("UPDATE tasks SET state=?,rows=390,bytes=2000,first_data_date='2026-10-16',"
                     "last_data_date='2026-10-16',last_attempt_at_utc='2026-10-17' WHERE partition='new'", (state,))
        conn.commit()
        sql = []
        conn.set_trace_callback(sql.append)
        result = task_status_aggregates(conn)
        conn.set_trace_callback(None)
        assert result == conn.execute(AGGREGATE_SQL).fetchall()
        assert not any('SUM(' in statement for statement in sql)


def test_upsert_ignore_abort_and_repeated_savepoint_rollback_match_sql(queue):
    _, conn = queue
    exact(conn)
    conn.execute("SAVEPOINT first")
    conn.execute("UPDATE tasks SET rows=111 WHERE dataset='B'")
    conn.execute("SAVEPOINT second")
    conn.execute("INSERT OR REPLACE INTO tasks(dataset,data_id,partition,state,rows,bytes) "
                 "VALUES ('B','x','history','pending',99,7)")
    conn.execute("ROLLBACK TO first")
    conn.execute("UPDATE tasks SET bytes=123 WHERE dataset='B'")
    conn.execute("RELEASE first")
    exact(conn)
    conn.execute("INSERT INTO tasks(dataset,data_id,partition,state,rows,bytes) "
                 "VALUES ('B','x','history','failed',5,6) ON CONFLICT(dataset,data_id,partition) "
                 "DO UPDATE SET rows=excluded.rows")
    conn.commit()
    exact(conn)


def test_full_publication_admission_requires_complete_stable_work_and_current_module():
    from scripts.reload_finmind_at_idle import accepted_local_measurement

    proof = {"state": "measurement_complete", "whole_publication_wall_improvement_observed": True,
             "all_task_values_equal": True, "source_files_stable": True, "resource_limits_stable": True,
             "task_count": 100, "rows_published_per_trial": 10,
             "trials": [{"label": label} for label in ("baseline", "candidate", "candidate", "baseline")]}
    assert accepted_local_measurement(proof)
    for key in ("all_task_values_equal", "source_files_stable", "resource_limits_stable",
                "rows_published_per_trial", "whole_publication_wall_improvement_observed"):
        assert not accepted_local_measurement({**proof, key: False})

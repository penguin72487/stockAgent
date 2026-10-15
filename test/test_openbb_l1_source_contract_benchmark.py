from __future__ import annotations

import json
import sqlite3

import pytest

from scripts import benchmark_openbb_l1_source_contract as bench
from scripts.compact_openbb_l1 import _lexical_absolute_path, _stale_source_contract_sql


def database() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.executescript("""
        CREATE TABLE tasks(task_id TEXT PRIMARY KEY, active INTEGER, status TEXT,
                           endpoint TEXT, output_path TEXT, rows INTEGER, updated_at TEXT);
        CREATE TABLE l1_compaction_members(task_id TEXT PRIMARY KEY, endpoint TEXT,
                          segment_id TEXT, source_path TEXT, source_rows INTEGER, task_updated_at TEXT);
        CREATE TABLE l1_compaction_segments(segment_id TEXT PRIMARY KEY, endpoint TEXT,
                                           output_path TEXT, status TEXT);
        INSERT INTO tasks VALUES ('a',1,'success','endpoint','source.parquet',10,'old');
        INSERT INTO l1_compaction_members VALUES ('a','endpoint','segment','/base/source.parquet',10,'old');
        INSERT INTO l1_compaction_segments VALUES ('segment','endpoint','/output.parquet','success');
        """)
    connection.create_function("stockagent_resolve_path", 1, _lexical_absolute_path, deterministic=True)
    bench.create_index(connection)
    return connection


@pytest.mark.parametrize("mutation,reason", [
    (None, None),
    ("DELETE FROM tasks", "source task missing from manifest"),
    ("UPDATE tasks SET active=0", "source task retired from active plan"),
    ("UPDATE tasks SET status='failed'", "source task is no longer successful"),
    ("UPDATE tasks SET endpoint='changed'", "source endpoint changed"),
    ("UPDATE l1_compaction_members SET endpoint='changed'", "source endpoint changed"),
    ("UPDATE l1_compaction_segments SET endpoint='changed'", "source endpoint changed"),
    ("UPDATE tasks SET output_path='changed.parquet'", "source path changed"),
    ("UPDATE tasks SET rows=9", "source row contract changed"),
    ("UPDATE tasks SET updated_at='new'", "source task was refreshed"),
    ("UPDATE tasks SET rows=9, active=0, status='failed'", "source task retired from active plan"),
    ("UPDATE l1_compaction_segments SET status='stale'", None),
    ("UPDATE tasks SET active=NULL", None),
    ("UPDATE tasks SET rows=NULL", None),
    ("UPDATE tasks SET updated_at=NULL", None),
])
def test_index_preserves_every_canonical_stale_predicate(mutation, reason):
    with database() as connection:
        if mutation:
            connection.execute(mutation)
        parameters = ("/base/", "/base/")
        expected = list(connection.execute(_stale_source_contract_sql("", member_first=True), parameters))
        for index in ("sqlite_autoindex_tasks_1", bench.INDEX):
            observed = list(connection.execute(bench.query_for(index), parameters))
            assert observed == expected
            assert [row[3] for row in observed] == ([] if reason is None else [reason])


@pytest.mark.parametrize("spelling", ("/base/source.parquet", "source.parquet", "./source.parquet"))
def test_index_retains_canonical_path_fallback(tmp_path, monkeypatch, spelling):
    monkeypatch.chdir(tmp_path)
    absolute = str(tmp_path / "source.parquet")
    source_path = spelling.replace("/base/source.parquet", absolute)
    with database() as connection:
        connection.execute("UPDATE tasks SET output_path=?", (source_path,))
        connection.execute("UPDATE l1_compaction_members SET source_path=?", (absolute,))
        assert not list(connection.execute(bench.query_for(bench.INDEX), (str(tmp_path) + "/",) * 2))


def test_covering_lookup_is_physical_only_and_plan_covers_all_source_columns():
    with database() as connection:
        plans = [row[3] for row in connection.execute("EXPLAIN QUERY PLAN " + bench.query_for(bench.INDEX),
                                                     ("/base/",) * 2)]
        assert any(f"USING COVERING INDEX {bench.INDEX}" in plan for plan in plans)
        before = list(connection.execute("SELECT * FROM tasks"))
        assert not list(connection.execute(bench.query_for("sqlite_autoindex_tasks_1"), ("/base/",) * 2))
        assert list(connection.execute("SELECT * FROM tasks")) == before


def test_single_segment_retains_multiple_distinct_reasons():
    with database() as connection:
        connection.execute("UPDATE tasks SET rows=9")
        connection.execute("INSERT INTO tasks VALUES ('b',0,'failed','endpoint','b.parquet',2,'old')")
        connection.execute("INSERT INTO l1_compaction_members VALUES ('b','endpoint','segment','/base/b.parquet',2,'old')")
        for index in ("sqlite_autoindex_tasks_1", bench.INDEX):
            actual = list(connection.execute(bench.query_for(index), ("/base/",) * 2))
            assert {row[3] for row in actual} == {"source task retired from active plan", "source row contract changed"}
            assert len(actual) == 2


def test_private_reader_cannot_modify_manifest(tmp_path):
    path = tmp_path / "fixture.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE example(value)")
    with bench.readonly(path) as connection:
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            connection.execute("INSERT INTO example VALUES (1)")


def test_no_foreign_index_or_silent_query_rewrite(monkeypatch):
    with pytest.raises(ValueError, match="measured exact"):
        bench.query_for("another_index")
    monkeypatch.setattr("scripts.compact_openbb_l1._stale_source_contract_sql", lambda *a, **k: "SELECT 1")
    with pytest.raises(RuntimeError, match="canonical query shape"):
        bench.query_for(bench.INDEX)


def test_changed_tracking_contract_rejects_old_index(monkeypatch):
    monkeypatch.setattr("scripts.openbb_l1_source_journal.TRACKED_TASK_COLUMNS", {"task_id"})
    with sqlite3.connect(":memory:") as connection, pytest.raises(RuntimeError, match="columns changed"):
        bench.create_index(connection)


def test_reject_broad_or_existing_scratch(tmp_path):
    with pytest.raises(ValueError, match="private benchmark"):
        bench.fresh(tmp_path / "new")
    with pytest.raises(ValueError, match="preserve earlier"):
        bench.fresh(tmp_path)


@pytest.mark.parametrize("change", ("output", "incomplete", "generation", "cuda"))
def test_aggregate_rejects_mismatched_full_evidence(tmp_path, change):
    fixture = {"manifest_sha256": "fixed", "implementation": {"code": "fixed"}}
    (tmp_path / "fixture.json").write_text(json.dumps(fixture))
    for n, variant in enumerate(("original", "covering", "covering", "original"), 1):
        row = {"contract": bench.CONTRACT, "state": "completed_trial", "variant": variant,
               **fixture, "production_writes": False, "provider_calls": 0,
               "model_runtime_imported": False, "stale_rows": 0, "stale_rows_sha256": "full",
               "wall_seconds": 2, "cpu_seconds": 1, "workflow_wall_seconds": 3}
        if n == 3:
            row[{"output": "stale_rows_sha256", "incomplete": "state", "generation": "manifest_sha256",
                 "cuda": "model_runtime_imported"}[change]] = "different"
        (tmp_path / f"{n:02d}-{variant}.json").write_text(json.dumps(row))
    with pytest.raises(ValueError, match="parity failed|ABBA receipts"):
        bench.aggregate(tmp_path, tmp_path / "aggregate.json")
    assert not (tmp_path / "aggregate.json").exists()

from __future__ import annotations

from pathlib import Path
import sqlite3

import pytest

from scripts.benchmark_openbb_l1_status_counts import prototype_source
from scripts.openbb_l1_status_counts import ENDPOINT_INDEX, POLICY, member_endpoint_counts
from scripts.openbb_l1_status_counts import (
    COUNT_TABLE, install_member_count_proof, baseline_member_count_proof,
)


def _database(*, column="TEXT NOT NULL", index="endpoint", partial=""):
    connection = sqlite3.connect(":memory:")
    connection.execute(f"CREATE TABLE l1_compaction_members (task_id TEXT PRIMARY KEY, endpoint {column})")
    if index:
        connection.execute(f"CREATE INDEX {ENDPOINT_INDEX} ON l1_compaction_members({index}) {partial}")
    return connection


def _reference(connection, clause="", parameters=()):
    return {str(row[0]): int(row[1]) for row in connection.execute(
        f"SELECT m.endpoint,COUNT(*) FROM l1_compaction_members AS m WHERE 1=1{clause} GROUP BY m.endpoint",
        parameters,
    )}


@pytest.mark.parametrize("values", [[], [""], ["a"] * 5,
    ["", "a", "A", "a_", " a", "資料", "資料", "é", "e", "x\x00y", "x'", "x%", "b'e'"],
    [f"endpoint-{i:04}" for i in range(2100)],
    ["a", "b", "c"] * 2000, ["a", b"a", b"\xff", "b'a'"]])
def test_exact_complete_counts(values):
    connection = _database()
    connection.executemany("INSERT INTO l1_compaction_members VALUES (?,?)", enumerate(values))
    before = connection.total_changes
    audit = {}
    assert member_endpoint_counts(connection, "", (), audit=audit) == _reference(connection)
    assert connection.total_changes == before
    assert audit["path"] == "endpoint_index_seeks"
    assert audit["policy"] == POLICY
    assert audit["members"] == sum(_reference(connection).values())


@pytest.mark.parametrize("column,index,partial", [
    ("TEXT NOT NULL", None, ""), ("TEXT", "endpoint", ""),
    ("TEXT COLLATE NOCASE NOT NULL", "endpoint", ""),
    ("TEXT NOT NULL", "endpoint COLLATE NOCASE", ""),
    ("TEXT NOT NULL", "task_id", ""), ("TEXT NOT NULL", "endpoint,task_id", ""),
    ("TEXT NOT NULL", "endpoint", "WHERE endpoint='a'"),
    ("INTEGER NOT NULL", "endpoint", ""),
])
def test_unknown_index_or_schema_uses_exact_original(column, index, partial):
    connection = _database(column=column, index=index, partial=partial)
    values = [None, "a", "A"] if column == "TEXT" else ["a", "A", "a"]
    connection.executemany("INSERT INTO l1_compaction_members VALUES (?,?)", enumerate(values))
    audit = {}
    assert member_endpoint_counts(connection, "", (), audit=audit) == _reference(connection)
    assert audit["path"] == "canonical_group_by"


@pytest.mark.parametrize("clause,parameters", [(" AND m.endpoint=?", ("a",)),
    (" AND m.endpoint LIKE ?", ("a%",)), (" AND m.endpoint IN (?,?)", ("a", "b")),
    (" AND m.endpoint=?", ("absent",)), (" AND m.endpoint=?", ("x' OR 1=1 --",))])
def test_filtered_call_retains_original(clause, parameters):
    connection = _database()
    connection.executemany("INSERT INTO l1_compaction_members VALUES (?,?)", enumerate(["a", "a2", "b", "x' OR 1=1 --"]))
    audit = {}
    assert member_endpoint_counts(connection, clause, parameters, audit=audit) == _reference(connection, clause, parameters)
    assert audit["path"] == "canonical_group_by"


def test_readonly_archive_and_callers_transaction_are_unchanged(tmp_path: Path):
    path = tmp_path / "members.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE l1_compaction_members(task_id TEXT PRIMARY KEY,endpoint TEXT NOT NULL)")
    connection.execute(f"CREATE INDEX {ENDPOINT_INDEX} ON l1_compaction_members(endpoint)")
    connection.executemany("INSERT INTO l1_compaction_members VALUES (?,?)", [("1", "a"), ("2", "b")])
    connection.commit()
    connection.close()
    before = path.read_bytes()
    connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    connection.execute("PRAGMA query_only=ON")
    connection.execute("BEGIN")
    assert member_endpoint_counts(connection, "", ()) == {"a": 1, "b": 1}
    assert connection.in_transaction
    connection.rollback()
    connection.close()
    assert path.read_bytes() == before


def test_missing_table_keeps_the_sql_error():
    with pytest.raises(sqlite3.OperationalError, match="no such table"):
        member_endpoint_counts(sqlite3.connect(":memory:"), "", ())


def test_complete_prototype_changes_only_member_query_and_audit():
    source = '''def original_status(connection, member_filter, member_parameters, timings):
    member_counts = {
        str(row["endpoint"]): int(row["source_files"])
        for row in connection.execute(
            "SELECT m.endpoint,COUNT(*) AS source_files "
            "FROM l1_compaction_members AS m WHERE 1=1 GROUP BY m.endpoint",
            member_parameters,
        )
    }
    timings["status_member_count"] = 0
    return {
        "counts": member_counts,
        "stage_seconds": timings,
    }
'''
    candidate = prototype_source(source)
    prefix = source[:source.index("    member_counts = {")]
    suffix = source[source.index('    timings["status_member_count"]'):]
    assert candidate.startswith(prefix)
    assert candidate.endswith(suffix.replace('        "stage_seconds": timings,\n',
        '        "stage_seconds": timings,\n        "member_count_audit": member_count_audit,\n'))
    assert "member_endpoint_counts(" in candidate


def test_prototype_rejects_a_changed_report_shape():
    with pytest.raises(ValueError):
        prototype_source("def no_member_query():\n    return {}\n")


def _incremental_database():
    connection = _database()
    connection.execute('CREATE TABLE archive_meta(key TEXT PRIMARY KEY,value TEXT)')
    connection.executemany('INSERT INTO l1_compaction_members VALUES(?,?)', [('1','a'),('2','a'),('3','b')])
    connection.commit()
    install_member_count_proof(connection)
    assert baseline_member_count_proof(connection)
    assert not baseline_member_count_proof(connection)
    return connection


def test_member_count_install_is_one_atomic_writer_transaction():
    connection = _database()
    statements = []
    connection.set_trace_callback(statements.append)
    install_member_count_proof(connection)
    assert statements.index("BEGIN IMMEDIATE") < next(i for i, sql in enumerate(statements) if sql.startswith("CREATE"))
    assert statements[-1] == "COMMIT"
    assert sum(query == "COMMIT" for query in statements) == 1
    assert not connection.in_transaction


def test_unsupported_count_schema_preserves_callers_pending_work():
    connection = _database(column='TEXT')
    connection.execute("INSERT INTO l1_compaction_members VALUES('pending','e')")
    with pytest.raises(RuntimeError, match='canonical endpoint schema'):
        install_member_count_proof(connection)
    assert connection.in_transaction
    assert connection.execute('SELECT * FROM l1_compaction_members').fetchall() == [('pending','e')]


def test_late_member_count_hook_install_failure_rolls_back_all_new_objects():
    connection = _database()
    before = connection.execute("SELECT name,sql FROM sqlite_schema ORDER BY name").fetchall()
    def deny_last_hook(action, name, *args):
        return (sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_CREATE_TRIGGER
                and name == "l1_member_count_update_v1" else sqlite3.SQLITE_OK)
    connection.set_authorizer(deny_last_hook)
    with pytest.raises(sqlite3.DatabaseError, match="authorized"):
        install_member_count_proof(connection)
    connection.set_authorizer(None)
    assert not connection.in_transaction
    assert connection.execute("SELECT name,sql FROM sqlite_schema ORDER BY name").fetchall() == before


@pytest.mark.parametrize('mutation', ['insert','delete','move','rename','rollback','replace','update_replace'])
def test_transactional_count_proof_tracks_exact_mutations(mutation):
    connection = _incremental_database()
    if mutation == 'insert':
        connection.execute("INSERT INTO l1_compaction_members VALUES('4','資料')")
    elif mutation == 'delete':
        connection.execute("DELETE FROM l1_compaction_members WHERE endpoint='a'")
    elif mutation == 'move':
        connection.execute("UPDATE l1_compaction_members SET endpoint='資料' WHERE task_id='1'")
    elif mutation == 'rename':
        connection.execute("UPDATE l1_compaction_members SET task_id='renamed' WHERE task_id='1'")
    elif mutation == 'rollback':
        connection.execute("DELETE FROM l1_compaction_members")
        connection.rollback()
    else:
        statement = ("INSERT OR REPLACE INTO l1_compaction_members VALUES('1','b')" if mutation=='replace'
                     else "UPDATE OR REPLACE l1_compaction_members SET task_id='2',endpoint='b' WHERE task_id='1'")
        with pytest.raises(sqlite3.IntegrityError, match='explicit delete'):
            connection.execute(statement)
    audit = {}
    assert member_endpoint_counts(connection,'',(),incremental=True,audit=audit) == _reference(connection)
    assert audit['path'] == 'transactional_counts'


def test_count_hook_gap_falls_back_and_rebaselines():
    connection = _incremental_database()
    connection.execute('DROP TRIGGER l1_member_count_delete_v1')
    connection.execute("DELETE FROM l1_compaction_members WHERE task_id='1'")
    connection.commit()
    audit = {}
    assert member_endpoint_counts(connection,'',(),incremental=True,audit=audit) == _reference(connection)
    assert audit['path'] == 'endpoint_index_seeks'
    install_member_count_proof(connection)
    assert baseline_member_count_proof(connection)
    assert member_endpoint_counts(connection,'',(),incremental=True) == _reference(connection)


def test_count_proof_remains_same_snapshot_and_readonly(tmp_path):
    original = _incremental_database()
    path = tmp_path/'counts.sqlite3'
    with sqlite3.connect(path) as target:
        original.backup(target)
        # SQLite backup can change the destination schema cookie. Re-enroll
        # this exact database before asking its read-only consumer to reuse.
        assert member_endpoint_counts(target,'',(),incremental=True)==_reference(target)
        baseline_member_count_proof(target)
    before = path.read_bytes()
    with sqlite3.connect(path.as_uri()+'?mode=ro',uri=True) as connection:
        connection.execute('PRAGMA query_only=ON')
        connection.execute('BEGIN')
        changes = connection.total_changes
        audit = {}
        assert member_endpoint_counts(connection,'',(),incremental=True,audit=audit) == {'a':2,'b':1}
        assert audit['path']=='transactional_counts'
        assert connection.in_transaction
        assert connection.total_changes==changes
    assert path.read_bytes()==before

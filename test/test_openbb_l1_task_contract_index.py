from __future__ import annotations

import sqlite3

import pytest

from scripts import openbb_l1_task_contract_index as lookup
from scripts.benchmark_openbb_l1_source_contract import query_for, INDEX
from scripts.compact_openbb_l1 import _stale_source_contract_sql
from scripts import compact_openbb_l1 as compactor
from test_openbb_l1_source_contract_benchmark import database
from test_openbb_l1_compaction import _args, _incremental_args, _publish_tasks


def install(connection, expression=None, suffix="", unique=""):
    expression = expression or ",".join(lookup.TASK_CONTRACT_COLUMNS)
    connection.execute(
        f"CREATE {unique} INDEX {lookup.TASK_CONTRACT_INDEX} ON tasks({expression}){suffix}"
    )


def test_missing_index_leaves_exact_original_sql():
    with database() as connection:
        index = lookup.verified_task_contract_index(connection)
        query = _stale_source_contract_sql("", member_first=True)
        assert index is None
        assert lookup.bind_task_contract_index(query, index) is query


@pytest.mark.parametrize(
    "changes",
    [
        {"expression": "task_id,active,status,endpoint,rows,output_path,updated_at"},
        {"expression": "task_id,active,status,endpoint,output_path,rows"},
        {
            "expression": "task_id,active,status,endpoint,output_path,rows,updated_at,task_id"
        },
        {
            "expression": "task_id COLLATE NOCASE,active,status,endpoint,output_path,rows,updated_at"
        },
        {
            "expression": "task_id DESC,active,status,endpoint,output_path,rows,updated_at"
        },
        {
            "expression": "lower(task_id),active,status,endpoint,output_path,rows,updated_at"
        },
        {"suffix": " WHERE active=1"},
        {"unique": "UNIQUE"},
    ],
)
def test_incompatible_index_never_enters_hint(changes):
    with database() as connection:
        install(connection, **changes)
        assert lookup.verified_task_contract_index(connection) is None


@pytest.mark.parametrize(
    "mutation",
    [
        None,
        "DELETE FROM tasks",
        "UPDATE tasks SET active=0",
        "UPDATE tasks SET status='failed'",
        "UPDATE tasks SET rows=9",
        "UPDATE tasks SET updated_at='new'",
        "UPDATE tasks SET endpoint='new'",
        "UPDATE tasks SET output_path='other.parquet'",
        "UPDATE tasks SET rows=NULL",
        "UPDATE tasks SET active=NULL",
        "UPDATE tasks SET updated_at=NULL",
    ],
)
def test_verified_index_changes_only_plan_not_stale_contract(mutation):
    with database() as connection:
        install(connection)
        if mutation:
            connection.execute(mutation)
        query = _stale_source_contract_sql("", member_first=True)
        index = lookup.verified_task_contract_index(connection)
        assert index == lookup.TASK_CONTRACT_INDEX
        bound = lookup.bind_task_contract_index(query, index)
        assert bound.replace(f" INDEXED BY {index}", "") == query
        params = ("/base/",) * 2
        assert list(connection.execute(bound, params)) == list(
            connection.execute(query, params)
        )
        plan = [
            row[3] for row in connection.execute("EXPLAIN QUERY PLAN " + bound, params)
        ]
        assert any(f"USING COVERING INDEX {index}" in row for row in plan)


def test_alias_uses_exact_same_query_as_the_completed_full_size_benchmark():
    with database() as connection:
        index = lookup.verified_task_contract_index(connection, name=INDEX)
        assert lookup.bind_task_contract_index(
            _stale_source_contract_sql("", member_first=True), index
        ) == query_for(INDEX)


@pytest.mark.parametrize(
    "name", ("bad;DROP TABLE tasks", "name with spaces", 'quoted"name', None)
)
def test_unsafe_names_are_not_interpolated(name):
    with sqlite3.connect(":memory:") as connection:
        with pytest.raises(ValueError, match="safe identifier"):
            lookup.verified_task_contract_index(connection, name=name)
        with pytest.raises(ValueError, match="safe identifier"):
            lookup.bind_task_contract_index("SELECT 1", name if name is not None else 1)


@pytest.mark.parametrize(
    "query",
    (
        "SELECT 1",
        "LEFT JOIN tasks AS t ON other=1",
        "LEFT JOIN tasks AS t ON t.task_id=m.task_id LEFT JOIN tasks AS t ON t.task_id=m.task_id",
    ),
)
def test_changed_or_duplicated_lookup_shape_is_not_silently_rewritten(query):
    with pytest.raises(RuntimeError, match="lookup shape changed"):
        lookup.bind_task_contract_index(query, lookup.TASK_CONTRACT_INDEX)


@pytest.mark.parametrize("mode", ("full", "incremental", "incremental_empty"))
@pytest.mark.parametrize("layout", ("absent", "valid", "incompatible"))
def test_native_stale_scan_uses_verified_lookup_and_preserves_footer_audit(
    tmp_path,
    mode,
    layout,
):
    tasks = _publish_tasks(tmp_path, ["aa", "bb"])
    assert compactor.run(_args(tmp_path)) == 0
    with compactor._open_manifest(
        tmp_path / "_state" / "openbb_archive.sqlite3"
    ) as connection:
        fingerprint = compactor._source_audit_fingerprint()
        if layout != "absent":
            install(connection, "task_id,status" if layout == "incompatible" else None)
        assert compactor._source_audit_fingerprint() == fingerprint
        connection.commit()
        queries = []
        connection.set_trace_callback(queries.append)
        changed = (
            None if mode == "full" else (() if mode.endswith("empty") else ("aa", "bb"))
        )
        audit, metadata, timings = {}, {}, {}
        assert compactor._mark_stale_segments(
            connection,
            (),
            changed_task_ids=changed,
            task_lookup_audit=audit,
            metadata_audit=metadata,
            timing=timings,
        ) == (set(), 0)
        assert audit["index"] == (
            lookup.TASK_CONTRACT_INDEX if layout == "valid" else None
        )
        assert audit["index_removed_during_lookup"] is False
        assert any(
            f"INDEXED BY {lookup.TASK_CONTRACT_INDEX}" in query for query in queries
        ) == (layout == "valid" and mode != "incremental_empty")
        assert metadata["footer_open_attempts"] > 0
        assert timings["stale_source_lookup_validation"] >= 0
        assert all(task.output_path for task in tasks)
        if mode == "incremental_empty":
            return
        connection.execute("UPDATE tasks SET rows=rows+1 WHERE task_id='aa'")
        assert compactor._mark_stale_segments(
            connection,
            (),
            changed_task_ids=changed,
            task_lookup_audit=audit,
        ) == ({tasks[0].endpoint}, 1)
        assert (
            connection.execute(
                "SELECT stale_reason FROM l1_compaction_segments WHERE status='stale'",
            ).fetchone()[0]
            == "source row contract changed"
        )


def test_native_lookup_dropped_between_validation_and_read_uses_exact_fallback(
    tmp_path,
    monkeypatch,
):
    _publish_tasks(tmp_path, ["aa", "bb"])
    assert compactor.run(_args(tmp_path)) == 0
    with compactor._open_manifest(
        tmp_path / "_state" / "openbb_archive.sqlite3"
    ) as connection:
        install(connection)
        connection.commit()
        original = compactor.bind_task_contract_index
        calls = []

        def removed_before_prepare(query, index):
            calls.append(index)
            connection.execute(f"DROP INDEX {lookup.TASK_CONTRACT_INDEX}")
            return original(query, index)

        monkeypatch.setattr(
            compactor, "bind_task_contract_index", removed_before_prepare
        )
        audit = {}
        assert compactor._mark_stale_segments(
            connection,
            (),
            task_lookup_audit=audit,
        ) == (set(), 0)
        assert calls == [lookup.TASK_CONTRACT_INDEX]
        assert audit == {
            "policy": lookup.LOOKUP_POLICY,
            "index": None,
            "state": "canonical_fallback",
            "index_removed_during_lookup": True,
        }


def test_native_lookup_does_not_mask_other_sql_errors(tmp_path, monkeypatch):
    _publish_tasks(tmp_path, ["aa"])
    assert compactor.run(_args(tmp_path)) == 0
    with compactor._open_manifest(
        tmp_path / "_state" / "openbb_archive.sqlite3"
    ) as connection:
        install(connection)
        monkeypatch.setattr(
            compactor,
            "bind_task_contract_index",
            lambda *_: "SELECT invalid_column FROM tasks",
        )
        with pytest.raises(sqlite3.OperationalError, match="invalid_column"):
            compactor._mark_stale_segments(connection, ())
        assert (
            connection.execute(
                "SELECT status FROM l1_compaction_segments",
            ).fetchone()[0]
            == "success"
        )


def test_native_attempt_publishes_layout_and_keeps_schema_change_full_audit(tmp_path):
    import json

    _publish_tasks(tmp_path, ["aa", "bb"])
    args = _incremental_args(tmp_path)
    assert compactor.run(args) == 0
    state = tmp_path / "_state"
    with sqlite3.connect(state / "openbb_archive.sqlite3") as connection:
        install(connection)
    assert compactor.run(args) == 0
    attempt = json.loads((state / "l1_compaction_attempt_latest.json").read_text())
    latest = json.loads((state / "l1_compaction_latest.json").read_text())
    assert attempt["source_task_lookup"]["index"] == lookup.TASK_CONTRACT_INDEX
    assert latest["source_audit"]["mode"] == "full"
    assert latest["source_audit"]["reason"] == "schema_changed"
    assert latest["compacted_files"] == 2
    assert compactor.run(args) == 0
    latest = json.loads((state / "l1_compaction_latest.json").read_text())
    assert latest["source_audit"]["mode"] == "incremental"
    assert latest["source_audit"]["changed_task_ids"] == 0

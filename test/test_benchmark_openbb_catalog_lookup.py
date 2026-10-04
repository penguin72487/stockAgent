from __future__ import annotations

from pathlib import Path
import sqlite3

import pytest

from scripts.benchmark_openbb_catalog_lookup import benchmark_membership, run


def _manifest(tmp_path: Path) -> Path:
    root = tmp_path / "source"
    path = root / "_state" / "openbb_archive.sqlite3"
    path.parent.mkdir(parents=True)
    with sqlite3.connect(path) as connection:
        connection.executescript(
            "CREATE TABLE archive_meta(key TEXT PRIMARY KEY,value TEXT);"
            "INSERT INTO archive_meta VALUES ('active_plan_token','archive');"
            "CREATE TABLE tasks(task_id TEXT PRIMARY KEY,active INTEGER,"
            "plan_token TEXT,status TEXT,endpoint TEXT);"
            "CREATE INDEX idx_schedule ON tasks(active,status,plan_token,task_id);"
        )
        connection.executemany(
            "INSERT INTO tasks VALUES (?,1,'archive','success','sec')",
            [(str(number),) for number in range(520)],
        )
        connection.executemany(
            "INSERT INTO tasks VALUES (?,?,?,'success','sec')",
            [("inactive", 0, "archive"), ("foreign", 1, "old")],
        )
    return root


def test_real_read_only_source_snapshot_and_all_abba_samples(tmp_path: Path) -> None:
    root = _manifest(tmp_path)
    path = root / "_state" / "openbb_archive.sqlite3"
    before = path.read_bytes()
    receipt = run(
        [
            "--source-root",
            str(root),
            "--endpoint",
            "sec",
            "--output",
            str(tmp_path / "result.json"),
        ]
    )
    assert receipt["source_query_only"]
    assert receipt["source_snapshot_transaction"]
    assert receipt["source_total_changes"] == 0
    assert receipt["results_identical"]
    assert len(receipt["samples"]) == 4
    assert all(sample["matched_tasks"] == 512 for sample in receipt["samples"])
    assert path.read_bytes() == before


def test_filtering_retains_exact_active_plan_semantics(tmp_path: Path) -> None:
    root = _manifest(tmp_path)
    connection = sqlite3.connect(root / "_state" / "openbb_archive.sqlite3")
    connection.row_factory = sqlite3.Row
    try:
        receipt = benchmark_membership(
            connection,
            plan_token="archive",
            task_ids=["1", "2", "inactive", "foreign", "absent"],
        )
        assert all(row["matched_tasks"] == 2 for row in receipt["samples"])
        with pytest.raises(ValueError):
            benchmark_membership(connection, plan_token="archive", task_ids=[])
    finally:
        connection.close()

from __future__ import annotations

from pathlib import Path
import sqlite3

import pytest

from scripts.benchmark_openbb_l1_covering_index import benchmark


def _sample_manifest(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            CREATE TABLE tasks (
                task_id TEXT PRIMARY KEY,
                endpoint TEXT NOT NULL,
                output_path TEXT NOT NULL,
                status TEXT NOT NULL,
                rows INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                active INTEGER NOT NULL
            );
            CREATE TABLE l1_compaction_segments (
                segment_id TEXT PRIMARY KEY,
                endpoint TEXT NOT NULL,
                output_path TEXT NOT NULL,
                status TEXT NOT NULL
            );
            CREATE TABLE l1_compaction_members (
                task_id TEXT PRIMARY KEY,
                endpoint TEXT NOT NULL,
                segment_id TEXT NOT NULL,
                source_path TEXT NOT NULL,
                source_rows INTEGER NOT NULL,
                task_updated_at TEXT NOT NULL
            );
            """
        )
        connection.execute(
            "INSERT INTO l1_compaction_segments VALUES (?,?,?,?)",
            ("segment-a", "equity.price.historical", "out.parquet", "success"),
        )
        for task_id, active, task_updated_at in (
            ("task-1", 1, "2026-09-25"),
            ("task-2", 1, "2026-09-26"),
            ("task-3", 0, "2026-09-25"),
        ):
            relative = f"data/{task_id}.parquet"
            connection.execute(
                "INSERT INTO tasks VALUES (?,?,?,?,?,?,?)",
                (
                    task_id, "equity.price.historical", relative, "success",
                    10, task_updated_at, active,
                ),
            )
            connection.execute(
                "INSERT INTO l1_compaction_members VALUES (?,?,?,?,?,?)",
                (
                    task_id, "equity.price.historical", "segment-a",
                    str(Path.cwd() / relative), 10, "2026-09-25",
                ),
            )
        connection.commit()
    finally:
        connection.close()


def test_sampled_covering_index_preserves_stale_contract_and_source(tmp_path: Path) -> None:
    manifest = tmp_path / "source.sqlite3"
    _sample_manifest(manifest)
    source_bytes = manifest.read_bytes()

    receipt = benchmark(manifest, sample_members=3, temp_root=tmp_path)

    assert manifest.read_bytes() == source_bytes
    assert receipt["source_open_mode"] == "read_only"
    assert receipt["temporary_sample_root"] == str(tmp_path)
    assert receipt["sampled_members"] == 3
    assert receipt["covering_index_bytes_approx"] > 0
    results = receipt["results"]
    assert [item["index"] for item in results] == [
        "sqlite_autoindex_tasks_1", "idx_l1_tasks_source_contract_cover",
        "idx_l1_tasks_source_contract_cover", "sqlite_autoindex_tasks_1",
    ]
    assert {item["stale_rows"] for item in results} == {2}
    assert len({item["stale_rows_sha256"] for item in results}) == 1
    assert any(
        "COVERING INDEX" in step
        for step in results[1]["query_plan"]
    )
    assert [item["variant"] for item in receipt["write_results"]] == [
        "primary", "covering", "covering", "primary",
    ]
    assert {item["updated_rows"] for item in receipt["write_results"]} == {3}


def test_sample_size_is_bounded(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="sample_members"):
        benchmark(tmp_path / "missing.sqlite3", sample_members=0)

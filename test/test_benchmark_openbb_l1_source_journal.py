from __future__ import annotations

from pathlib import Path
import sqlite3

from scripts.benchmark_openbb_l1_source_journal import benchmark


def test_writer_probe_uses_disposable_copies_and_exact_updates(tmp_path: Path) -> None:
    manifest = tmp_path / "source.sqlite3"
    connection = sqlite3.connect(manifest)
    try:
        connection.executescript(
            """
            CREATE TABLE tasks (
                task_id TEXT PRIMARY KEY, endpoint TEXT NOT NULL,
                output_path TEXT NOT NULL, status TEXT NOT NULL,
                rows INTEGER NOT NULL, updated_at TEXT NOT NULL,
                active INTEGER NOT NULL
            );
            CREATE TABLE l1_compaction_segments (
                segment_id TEXT PRIMARY KEY, endpoint TEXT NOT NULL,
                output_path TEXT NOT NULL, status TEXT NOT NULL
            );
            CREATE TABLE l1_compaction_members (
                task_id TEXT PRIMARY KEY, endpoint TEXT NOT NULL,
                segment_id TEXT NOT NULL, source_path TEXT NOT NULL,
                source_rows INTEGER NOT NULL, task_updated_at TEXT NOT NULL
            );
            CREATE TABLE archive_meta (
                key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL
            );
            """
        )
        connection.execute(
            "INSERT INTO l1_compaction_segments VALUES (?,?,?,?)",
            ("segment", "e", "out.parquet", "success"),
        )
        for index in range(10):
            task_id = f"task-{index}"
            connection.execute(
                "INSERT INTO tasks VALUES (?,?,?,?,?,?,?)",
                (task_id, "e", f"{task_id}.parquet", "success", 1, "t1", 1),
            )
            connection.execute(
                "INSERT INTO l1_compaction_members VALUES (?,?,?,?,?,?)",
                (task_id, "e", "segment", f"{task_id}.parquet", 1, "t1"),
            )
        connection.commit()
    finally:
        connection.close()
    source_bytes = manifest.read_bytes()

    receipt = benchmark(
        manifest, sample_members=10, update_tasks=5, batch_size=2,
        temp_root=tmp_path,
    )

    assert manifest.read_bytes() == source_bytes
    assert receipt["source_open_mode"] == "read_only"
    assert receipt["sampled_members"] == 10
    assert [row["variant"] for row in receipt["results"]] == [
        "baseline", "journal", "journal", "baseline",
    ]
    assert [row["journal_rows_after"] for row in receipt["results"]] == [
        None, 5, 10, None,
    ]
    assert all(row["updated_tasks"] == 5 for row in receipt["results"])
    assert [row["variant"] for row in receipt["insert_results"]] == [
        "baseline", "journal", "journal", "baseline",
    ]
    assert [row["journal_rows_after"] for row in receipt["insert_results"]] == [
        None, 15, 20, None,
    ]

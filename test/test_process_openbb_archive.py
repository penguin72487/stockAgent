from __future__ import annotations

from pathlib import Path
import shutil

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader.download_openbb_archive import DownloadTask, Manifest, TaskResult
from scripts.process_openbb_archive import run


def _task(root: Path, task_id: str, rows: int) -> DownloadTask:
    return DownloadTask(
        task_id=task_id,
        endpoint="equity.price.historical",
        category="equity",
        scope_key=task_id,
        kwargs={"symbol": task_id},
        providers=("yfinance",),
        output_path=str(
            root
            / "data"
            / "equity"
            / "price"
            / "historical"
            / task_id[:2]
            / f"{task_id}.parquet"
        ),
    )


def test_duckdb_compaction_and_polars_pyarrow_validation(tmp_path: Path) -> None:
    state = tmp_path / "_state" / "openbb_archive.sqlite3"
    manifest = Manifest(state)
    tasks = [_task(tmp_path, "aa-task", 2), _task(tmp_path, "bb-task", 1)]
    try:
        manifest.upsert_tasks(tasks)
        for index, task in enumerate(tasks):
            output = Path(task.output_path)
            output.parent.mkdir(parents=True, exist_ok=True)
            records = (
                [
                    {"date": "2000-01-03", "close": 1.0},
                    {"date": "2000-01-04", "close": 2.0},
                ]
                if index == 0
                else [{"date": "2000-01-05", "close": 3.0, "volume": 100}]
            )
            pq.write_table(pa.Table.from_pylist(records), output, compression="zstd")
            manifest.claim([task])
            manifest.complete(
                TaskResult(task, "success", "yfinance", len(records), str(output), 1)
            )
    finally:
        manifest.close()

    assert run(["--output-dir", str(tmp_path), "--threads", "1", "--no-progress"]) == 0
    compact = (
        tmp_path / "compact" / "equity" / "price" / "historical" / "archive.parquet"
    )
    assert pq.ParquetFile(compact).metadata.num_rows == 3
    assert pl.scan_parquet(compact).select(pl.len()).collect().item() == 3
    with duckdb.connect(str(tmp_path / "openbb.duckdb"), read_only=True) as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM openbb_equity_price_historical"
            ).fetchone()[0]
            == 3
        )
    summary = pl.read_parquet(tmp_path / "catalog" / "compaction_summary.parquet")
    assert summary["status"].to_list() == ["success"]
    assert (
        run(
            [
                "--output-dir",
                str(tmp_path),
                "--audit-only",
                "--threads",
                "1",
                "--no-progress",
            ]
        )
        == 0
    )
    audit = pl.read_parquet(tmp_path / "catalog" / "compaction_audit.parquet")
    assert audit["status"].to_list() == ["passed"]
    assert audit["source_rows"].to_list() == [3]
    assert audit["arrow_rows"].to_list() == [3]
    assert audit["polars_rows"].to_list() == [3]
    assert audit["duckdb_rows"].to_list() == [3]

    # A still-readable but truncated derivative must fail the independent
    # audit, then be regenerated instead of accepted as signature-unchanged.
    shutil.copyfile(tasks[0].output_path, compact)
    assert (
        run(
            [
                "--output-dir",
                str(tmp_path),
                "--audit-only",
                "--threads",
                "1",
                "--no-progress",
            ]
        )
        == 2
    )
    failed_audit = pl.read_parquet(
        tmp_path / "catalog" / "compaction_audit.parquet"
    )
    assert failed_audit["status"].to_list() == ["failed"]
    assert "row-count mismatch" in failed_audit["error"][0]

    assert run(["--output-dir", str(tmp_path), "--threads", "1", "--no-progress"]) == 0
    assert pq.ParquetFile(compact).metadata.num_rows == 3
    assert (
        run(
            [
                "--output-dir",
                str(tmp_path),
                "--audit-only",
                "--threads",
                "1",
                "--no-progress",
            ]
        )
        == 0
    )


def test_compaction_does_not_silently_skip_missing_success_shard(
    tmp_path: Path,
) -> None:
    state = tmp_path / "_state" / "openbb_archive.sqlite3"
    manifest = Manifest(state)
    task = _task(tmp_path, "missing-task", 1)
    try:
        manifest.upsert_tasks([task])
        manifest.claim([task])
        manifest.complete(
            TaskResult(task, "success", "yfinance", 1, task.output_path, 1)
        )
    finally:
        manifest.close()

    with pytest.raises(FileNotFoundError):
        run(["--output-dir", str(tmp_path), "--threads", "1", "--no-progress"])

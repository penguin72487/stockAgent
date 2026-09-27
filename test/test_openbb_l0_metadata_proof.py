from __future__ import annotations

from collections import Counter
from dataclasses import replace
import os
from pathlib import Path
import sqlite3

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import compact_openbb_l1 as compactor
from stockagent.data import columnar_lake as lake
from test_openbb_l1_compaction import _args, _publish_tasks


def _write(path: Path, value: int = 1, *, schema_change: bool = False) -> None:
    pq.write_table(pa.table({"other" if schema_change else "value": [value]}), path)


def _compact(paths, target, **kwargs):
    return lake.compact_parquet_files(
        paths, target, threads=1, memory_limit="128MB", **kwargs
    )


def test_same_run_proof_opens_source_once_and_preserves_every_column(tmp_path, monkeypatch):
    paths = [tmp_path / f"source-{index}.parquet" for index in range(3)]
    for index, path in enumerate(paths):
        data = {"id": [index, index], "value": [float(index), None], "name": ["x", "y"]}
        if index == 2:
            data["extra"] = ["last", None]
        pq.write_table(pa.table(data), path)
    original = pq.ParquetFile
    opens = Counter()

    def tracked(path, *args, **kwargs):
        resolved = Path(path).resolve()
        if resolved in paths:
            opens[resolved] += 1
        return original(path, *args, **kwargs)

    monkeypatch.setattr(pq, "ParquetFile", tracked)
    proofs = [lake.observe_source_metadata(path) for path in paths]
    legacy = _compact(paths, tmp_path / "legacy.parquet", expected_rows=6)
    assert set(opens.values()) == {2}
    opens.clear()
    proofs = [lake.observe_source_metadata(path) for path in paths]
    candidate = _compact(
        paths, tmp_path / "candidate.parquet", expected_rows=6, source_metadata_proofs=proofs
    )
    assert set(opens.values()) == {1}
    with duckdb.connect(":memory:") as connection:
        expected = connection.execute(
            "SELECT * FROM read_parquet(?, union_by_name=true) ORDER BY id,name", [[str(path) for path in paths]]
        ).fetchall()
        for output in (legacy.output_path, candidate.output_path):
            assert connection.execute(
                "SELECT * FROM read_parquet(?) ORDER BY id,name", [output]
            ).fetchall() == expected
    assert candidate.source_rows == candidate.pyarrow_rows == candidate.polars_rows == candidate.duckdb_rows == 6
    assert candidate.source_bytes == legacy.source_bytes
    assert candidate.schema_fingerprint == legacy.schema_fingerprint


@pytest.mark.parametrize("change", ["replace", "rewrite_same_mtime", "schema", "delete"])
def test_source_drift_after_observation_cannot_replace_target(tmp_path, change):
    source = tmp_path / "source.parquet"
    target = tmp_path / "target.parquet"
    _write(source)
    _write(target, 99)
    previous = target.read_bytes()
    proof = lake.observe_source_metadata(source)
    before = source.stat()
    if change == "replace":
        replacement = tmp_path / "replacement.parquet"
        replacement.write_bytes(source.read_bytes())
        os.utime(replacement, ns=(before.st_atime_ns, before.st_mtime_ns))
        os.replace(replacement, source)
    elif change == "delete":
        source.unlink()
    else:
        _write(source, 2, schema_change=change == "schema")
        os.utime(source, ns=(before.st_atime_ns, before.st_mtime_ns))
    with pytest.raises((RuntimeError, FileNotFoundError)):
        _compact([source], target, source_metadata_proofs=[proof])
    assert target.read_bytes() == previous


def test_mutation_during_first_footer_read_is_rejected(tmp_path, monkeypatch):
    source = tmp_path / "source.parquet"
    _write(source)
    original = pq.ParquetFile

    def changed(path, *args, **kwargs):
        opened = original(path, *args, **kwargs)
        _write(source, 2)
        return opened

    monkeypatch.setattr(pq, "ParquetFile", changed)
    with pytest.raises(RuntimeError, match="during metadata observation"):
        lake.observe_source_metadata(source)


@pytest.mark.parametrize("phase", ["copy", "summary", "fsync", "callback", "callback_same_mtime"])
def test_source_drift_before_atomic_replace_is_rejected(tmp_path, monkeypatch, phase):
    source = tmp_path / "source.parquet"
    target = tmp_path / "target.parquet"
    _write(source)
    _write(target, 99)
    previous = target.read_bytes()
    proof = lake.observe_source_metadata(source)
    callback = None
    if phase == "copy":
        original_connect = duckdb.connect

        class CopyMutation:
            def __init__(self, connection):
                self.connection = connection

            def __getattr__(self, name):
                return getattr(self.connection, name)

            def execute(self, query, *args, **kwargs):
                result = self.connection.execute(query, *args, **kwargs)
                if query.startswith("COPY"):
                    _write(source, 2)
                return result

        monkeypatch.setattr(duckdb, "connect", lambda *a, **k: CopyMutation(original_connect(*a, **k)))
    elif phase.startswith("callback"):
        def callback():
            before = source.stat()
            _write(source, 2)
            if phase == "callback_same_mtime":
                os.utime(source, ns=(before.st_atime_ns, before.st_mtime_ns))
                assert source.stat().st_size == before.st_size
    else:
        name = "_polars_summary" if phase == "summary" else "_fsync_path"
        original = getattr(lake, name)

        def changed(*args, **kwargs):
            result = original(*args, **kwargs)
            _write(source, 2)
            return result

        monkeypatch.setattr(lake, name, changed)
    with pytest.raises(RuntimeError, match="source changed"):
        _compact([source], target, source_metadata_proofs=[proof], before_publish=callback)
    assert target.read_bytes() == previous
    assert not list(tmp_path.glob(".*.tmp"))


@pytest.mark.parametrize("change", ["count", "path", "bytes", "rows", "schema"])
def test_invalid_proof_cannot_publish(tmp_path, change):
    source = tmp_path / "source.parquet"
    _write(source)
    proof = lake.observe_source_metadata(source)
    proofs = [proof]
    if change == "count":
        proofs = []
    else:
        values = {"path": "/unrelated.parquet", "bytes": -1, "rows": True, "schema_fingerprint": "bad"}
        field = "schema_fingerprint" if change == "schema" else change
        proofs = [replace(proof, **{field: values[field]})]
    target = tmp_path / "target.parquet"
    with pytest.raises(RuntimeError, match="proof"):
        _compact([source], target, source_metadata_proofs=proofs)
    assert not target.exists()


def test_l1_normal_build_reads_each_source_footer_once(tmp_path, monkeypatch):
    tasks = _publish_tasks(tmp_path, ["aa", "bb", "cc"])
    sources = {Path(task.output_path).resolve() for task in tasks}
    original = pq.ParquetFile
    opens = Counter()

    def tracked(path, *args, **kwargs):
        if Path(path).resolve() in sources:
            opens[Path(path).resolve()] += 1
        return original(path, *args, **kwargs)

    monkeypatch.setattr(pq, "ParquetFile", tracked)
    assert compactor.run(_args(tmp_path)) == 0
    assert set(opens) == sources
    assert set(opens.values()) == {1}


@pytest.mark.parametrize("change", ["task", "source", "schema_proof", "after_replace", "symlink"])
def test_l1_changed_contract_never_registers_segment_or_members(tmp_path, monkeypatch, change):
    tasks = _publish_tasks(tmp_path, ["aa", "bb"])
    source = Path(tasks[0].output_path)
    if change == "symlink":
        actual_source = source.with_name("original.parquet")
        source.rename(actual_source)
        source.symlink_to(actual_source)
        replacement = source.with_name("replacement.parquet")
        replacement.write_bytes(actual_source.read_bytes())
    if change == "schema_proof":
        original_load = compactor._load_unassigned_shards

        def changed_load(*args, **kwargs):
            shards = original_load(*args, **kwargs)
            shards[0] = replace(shards[0], metadata_proof=replace(shards[0].metadata_proof, schema_fingerprint="0" * 64))
            return shards

        monkeypatch.setattr(compactor, "_load_unassigned_shards", changed_load)
    elif change == "after_replace":
        original_compact = compactor.compact_parquet_files

        def changed_after(*args, **kwargs):
            receipt = original_compact(*args, **kwargs)
            _write(source, 2)
            return receipt

        monkeypatch.setattr(compactor, "compact_parquet_files", changed_after)
    else:
        original_summary = lake._polars_summary

        def changed_summary(path):
            result = original_summary(path)
            if change == "task":
                with sqlite3.connect(tmp_path / "_state/openbb_archive.sqlite3") as connection:
                    connection.execute("UPDATE tasks SET updated_at='changed' WHERE task_id='aa'")
            elif change == "symlink":
                source.unlink()
                source.symlink_to(replacement)
            else:
                _write(source, 2)
            return result

        monkeypatch.setattr(lake, "_polars_summary", changed_summary)
    compactor.run(_args(tmp_path))
    with sqlite3.connect(tmp_path / "_state/openbb_archive.sqlite3") as connection:
        assert connection.execute("SELECT COUNT(*) FROM l1_compaction_members").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM l1_compaction_segments WHERE status='success'").fetchone()[0] == 0
    assert not list((tmp_path / "compact_l1/equity/price/historical/segments").glob("*.parquet"))


def test_failed_atomic_replace_preserves_original_and_removes_temporary(tmp_path, monkeypatch):
    source = tmp_path / "source.parquet"
    target = tmp_path / "target.parquet"
    _write(source)
    _write(target, 99)
    original = target.read_bytes()

    def failed(*args):
        raise OSError("injected replace failure")

    monkeypatch.setattr(lake.os, "replace", failed)
    with pytest.raises(OSError, match="injected replace"):
        _compact([source], target, source_metadata_proofs=[lake.observe_source_metadata(source)])
    assert target.read_bytes() == original
    assert pl.read_parquet(source)["value"].to_list() == [1]
    assert not list(tmp_path.glob(".*.tmp"))

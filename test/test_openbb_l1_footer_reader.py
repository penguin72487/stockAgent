from __future__ import annotations

from pathlib import Path
import hashlib
import json
import sqlite3
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

from scripts import benchmark_openbb_l1_footer_reader as benchmark
from scripts import compact_openbb_l1 as compactor


@pytest.fixture
def fixture_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    monkeypatch.setattr(benchmark, "ROOT", tmp_path)
    monkeypatch.setattr(benchmark, "implementation_identity", lambda: {"test_generation": 1})
    monkeypatch.setattr(benchmark.shutil, "disk_usage", lambda _path: SimpleNamespace(free=40 * 1024**3))
    # Worker guards are process-scoped in production; preserve the test runner's
    # request method if a test calls that worker in process.
    monkeypatch.setattr(requests.Session, "request", requests.Session.request)
    monkeypatch.delitem(__import__("sys").modules, "torch", raising=False)
    root = tmp_path / "artifacts/benchmarks"
    root.mkdir(parents=True)
    source = tmp_path / "manifest.sqlite3"
    with sqlite3.connect(source) as connection:
        connection.execute("""CREATE TABLE l1_compaction_segments (
            segment_id TEXT PRIMARY KEY, endpoint TEXT, output_path TEXT,
            output_rows INTEGER, output_bytes INTEGER, schema_fingerprint TEXT,
            status TEXT, stale_reason TEXT, updated_at TEXT)""")
        for index in range(3):
            path = tmp_path / f"source-{index}.parquet"
            schema = pa.schema([pa.field("value", pa.int64(), metadata={b"field": b"original"})])
            pq.write_table(pa.Table.from_pylist([{"value": 1}, {"value": None}], schema=schema), path)
            fingerprint = hashlib.sha256(pq.ParquetFile(path).schema_arrow.remove_metadata().serialize()).hexdigest()
            connection.execute(
                "INSERT INTO l1_compaction_segments VALUES (?,?,?,?,?,?,?,NULL,NULL)",
                (str(index), compactor.SCHEMA_GROUPED_ENDPOINT if index < 2 else "equity.price.historical",
                 str(path), 2, path.stat().st_size, fingerprint, "success"),
            )
    return root / "captured", source


@pytest.mark.parametrize("candidate", ["mmap", "schema_cache"])
def test_capture_and_full_canonical_abba_preserve_source_and_proofs(fixture_root, candidate) -> None:
    root, source = fixture_root
    source_before = benchmark.digest(source)
    benchmark.capture(root, source, 0)
    fixture = json.loads((root / "fixture.json").read_bytes())
    assert fixture["selected_segments"] == fixture["available_success_segments"] == 3
    assert fixture["all_success_segments"] is True
    for index, variant in enumerate(("original", candidate, candidate, "original"), 1):
        benchmark.worker(root, root / f"{index:02d}-{variant}.json", variant)
    benchmark.aggregate(root, root / "accepted.json", candidate)
    accepted = json.loads((root / "accepted.json").read_bytes())
    assert accepted["exact_canonical_complete_output_parity"] is True
    assert accepted["all_success_segments"] is True
    assert {row["view_proofs"] for row in accepted["trials"]} == {2}
    assert accepted["production_writes"] is False
    assert benchmark.digest(source) == source_before
    if candidate == "schema_cache":
        assert accepted["trials"][1]["physical_cache"]["hits"] == 1


def test_capture_labels_sample_and_rejects_overwrite(fixture_root) -> None:
    root, source = fixture_root
    benchmark.capture(root, source, 1)
    fixture = json.loads((root / "fixture.json").read_bytes())
    assert fixture["all_success_segments"] is False
    assert fixture["selected_segments"] == 1
    with pytest.raises(ValueError, match="preserve previous"):
        benchmark.capture(root, source, 1)


def test_worker_rejects_source_mutation_before_footer_reads(fixture_root, monkeypatch) -> None:
    root, source = fixture_root
    benchmark.capture(root, source, 0)
    pq.write_table(pa.table({"changed": [1, 2]}), source.parent / "source-1.parquet")
    monkeypatch.setattr(compactor, "_mark_stale_segments", lambda *_args, **_kwargs: pytest.fail("audit started"))
    with pytest.raises(RuntimeError, match="signature changed before"):
        benchmark.worker(root, root / "refused.json", "mmap")
    assert not (root / "refused.json").exists()


def test_worker_rejects_source_mutation_after_complete_canonical_audit(fixture_root, monkeypatch) -> None:
    root, source = fixture_root
    benchmark.capture(root, source, 0)
    original = compactor._mark_stale_segments

    def changed(*args, **kwargs):
        result = original(*args, **kwargs)
        pq.write_table(pa.table({"changed": [1, 2]}), source.parent / "source-1.parquet")
        return result

    monkeypatch.setattr(compactor, "_mark_stale_segments", changed)
    with pytest.raises(RuntimeError, match="signature changed during"):
        benchmark.worker(root, root / "refused.json", "original")


def test_worker_rejects_code_generation_change(fixture_root, monkeypatch) -> None:
    root, source = fixture_root
    benchmark.capture(root, source, 0)
    monkeypatch.setattr(benchmark, "implementation_identity", lambda: {"test_generation": 2})
    with pytest.raises(RuntimeError, match="code/runtime generation changed"):
        benchmark.worker(root, root / "refused.json", "original")


def test_reader_variant_restores_reader_on_memory_error(monkeypatch) -> None:
    observed = []

    def reader(*args, **kwargs):
        observed.append(kwargs)
        raise MemoryError("no memory")

    monkeypatch.setattr(compactor.pq, "ParquetFile", reader)
    with pytest.raises(MemoryError), benchmark.reader_variant(compactor, "mmap"):
        compactor.pq.ParquetFile("path")
    assert observed == [{"memory_map": True}]
    assert compactor.pq.ParquetFile is reader


def test_cache_variant_restores_identity_on_memory_error(monkeypatch) -> None:
    def injected(*_args, **_kwargs):
        raise MemoryError("required failure")

    monkeypatch.setattr(compactor, "schema_identities", injected)
    with pytest.raises(MemoryError), benchmark.reader_variant(compactor, "schema_cache"):
        compactor.schema_identities(None)
    assert compactor.schema_identities is injected


def test_aggregate_rejects_claimed_subset_as_full(fixture_root) -> None:
    root, source = fixture_root
    benchmark.capture(root, source, 0)
    for index, variant in enumerate(("original", "schema_cache", "schema_cache", "original"), 1):
        benchmark.worker(root, root / f"{index:02d}-{variant}.json", variant)
    # Simulate receipts consistently lying about the number of audited files.
    for path in root.glob("0*.json"):
        row = json.loads(path.read_bytes())
        row["selected_segments"] -= 1
        from downloader.artifact_io import atomic_write_json
        atomic_write_json(path, row)
    with pytest.raises(ValueError, match="evidence does not match"):
        benchmark.aggregate(root, root / "refused.json", "schema_cache")

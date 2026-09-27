from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.benchmark_openbb_l1_footer_proof import ENDPOINT, LEGACY_SEQUENCE, SINGLE_PASS_SEQUENCE, benchmark


def _manifest(root: Path) -> Path:
    manifest = root / "manifest.sqlite3"
    with sqlite3.connect(manifest) as connection:
        connection.execute("CREATE TABLE l1_compaction_segments (segment_id TEXT, output_path TEXT, output_rows INTEGER, output_bytes INTEGER, schema_fingerprint TEXT, endpoint TEXT, status TEXT, created_at TEXT)")
        for index, table in enumerate((
            pa.table({"v": [1], "s": ["one"]}),
            pa.table({"v": [2], "s": ["two"]}),
            pa.table({"t": pa.array([1], type=pa.timestamp("ns"))}),
            pa.table({"t": pa.array([1], type=pa.timestamp("ns"))}),
        )):
            path = root / f"{index}.parquet"
            pq.write_table(table, path, use_deprecated_int96_timestamps=index == 2)
            fingerprint = hashlib.sha256(pq.ParquetFile(path).schema_arrow.remove_metadata().serialize().to_pybytes()).hexdigest()
            connection.execute("INSERT INTO l1_compaction_segments VALUES (?,?,?,?,?,?,?,?)", (
                str(index), str(path), 1, path.stat().st_size, fingerprint, ENDPOINT, "success", "2026-09-26",
            ))
    return manifest


def test_real_footer_abba_and_cached_variants_keep_exact_identity(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path)
    before = manifest.read_bytes()
    receipt = benchmark(manifest, count=4, sequence=LEGACY_SEQUENCE)
    assert manifest.read_bytes() == before
    assert receipt["state"] == "verified"
    assert [trial["variant"] for trial in receipt["trials"]] == list(LEGACY_SEQUENCE)
    assert receipt["physical_identity_byte_equal"] is True
    for trial in receipt["trials"]:
        assert trial["timings"]["footer_open"]["calls"] == 4
        assert trial["timings"]["ordinary_arrow_integrity"]["calls"] == 4
        assert trial["wall_seconds"] > 0 and trial["cpu_seconds"] > 0
    profile = receipt["trials"][-1]
    assert profile["timings"]["serialize"]["calls"] == 12
    assert profile["timings"]["logical_json_parse"]["calls"] == 6
    assert profile["physical_groups"] == 3


def test_single_pass_matches_pinned_legacy_identity_and_serializes_once(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path)
    legacy = benchmark(manifest, count=4, sequence=("ordinary", "safe"))
    baseline = tmp_path / "baseline.json"
    baseline.write_text(json.dumps(legacy))
    pin = hashlib.sha256(baseline.read_bytes()).hexdigest()
    receipt = benchmark(manifest, count=4, baseline_receipt=baseline, baseline_receipt_sha256=pin)
    assert receipt["state"] == "verified"
    assert [trial["variant"] for trial in receipt["trials"]] == list(SINGLE_PASS_SEQUENCE)
    assert receipt["historical_physical_identity_equal"] is True
    assert receipt["historical_proof"]["paired_timing_comparison"] is False
    assert receipt["trials"][-2]["timings"]["serialize"]["calls"] == 12
    assert receipt["trials"][-1]["timings"]["serialize"]["calls"] == 4
    assert "ordinary_arrow_integrity" not in receipt["trials"][1]["timings"]
    # Even if current old/new implementations agree, a changed identity cannot
    # be reported verified against the hash-pinned historical proof vector.
    legacy["trials"][1]["physical_identity_sha256"] = "0" * 64
    baseline.write_text(json.dumps(legacy))
    changed_pin = hashlib.sha256(baseline.read_bytes()).hexdigest()
    mismatch = benchmark(manifest, count=4, sequence=("safe", "single_pass_proof"),
                         baseline_receipt=baseline, baseline_receipt_sha256=changed_pin)
    assert mismatch["state"] == "mismatch"
    assert mismatch["historical_physical_identity_equal"] is False


def test_invalid_historical_receipt_pin_fails_before_source_reads(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.json"
    baseline.write_text("{}")
    with pytest.raises(RuntimeError, match="SHA256"):
        benchmark(tmp_path / "missing", baseline_receipt=baseline, baseline_receipt_sha256="0" * 64)


@pytest.mark.parametrize("count", (0, 4097))
def test_probe_is_bounded_before_reading_manifest(tmp_path: Path, count: int) -> None:
    with pytest.raises(ValueError, match="4096"):
        benchmark(tmp_path / "missing", count=count)


@pytest.mark.parametrize("column,value", (("output_rows", 2), ("output_bytes", 1), ("schema_fingerprint", "0" * 64)))
def test_probe_cannot_skip_original_integrity_checks(tmp_path: Path, column: str, value) -> None:
    manifest = _manifest(tmp_path)
    with sqlite3.connect(manifest) as connection:
        connection.execute(f"UPDATE l1_compaction_segments SET {column}=? WHERE segment_id='1'", (value,))
    with pytest.raises((RuntimeError, subprocess.CalledProcessError)):
        benchmark(manifest, count=4, sequence=("ordinary", "safe"))

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.benchmark_openbb_l1_view_binding import benchmark


def _manifest(
    tmp_path: Path, tables: list[pa.Table], *, int96_first: bool = False,
    canonical_sec: bool = False,
) -> Path:
    endpoint = "regulators.sec.filing_headers" if canonical_sec else "test.endpoint"
    manifest = tmp_path / "_state/openbb_archive.sqlite3" if canonical_sec else tmp_path / "source.sqlite3"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(manifest) as connection:
        connection.execute(
            "CREATE TABLE l1_compaction_segments (segment_id TEXT, endpoint TEXT, "
            "output_path TEXT, output_rows INTEGER, output_bytes INTEGER, "
            "schema_fingerprint TEXT, status TEXT, created_at TEXT)"
        )
        for index, table in enumerate(tables):
            segment_id = f"{index:024x}" if canonical_sec else str(index)
            path = (
                tmp_path / "compact_l1/regulators/sec/filing_headers/segments" / f"segment-{segment_id}.parquet"
                if canonical_sec else tmp_path / f"{index}.parquet"
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, path, use_deprecated_int96_timestamps=int96_first and index == 0)
            parquet = pq.ParquetFile(path)
            fingerprint = hashlib.sha256(
                parquet.schema_arrow.remove_metadata().serialize().to_pybytes()
            ).hexdigest()
            connection.execute(
                "INSERT INTO l1_compaction_segments VALUES (?,?,?,?,?,?,?,?)",
                (segment_id, endpoint, str(path), table.num_rows,
                 path.stat().st_size, fingerprint, "success", "2026-09-26T00:00:00+00:00"),
            )
    return manifest


def test_view_binding_probe_preserves_scalar_rows_and_source_manifest(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, [
        pa.table({"x": [1], "name": ["a"]}),
        pa.table({"name": ["b"], "x": ["2"], "missing_before": [True]}),
        pa.table({"x": [3], "name": ["c"]}),
        pa.table({"name": [None], "x": [None], "missing_before": [None]}, schema=pa.schema([
            ("name", pa.string()), ("x", pa.string()), ("missing_before", pa.bool_()),
        ])),
    ])
    original = manifest.read_bytes()
    receipt = benchmark(
        manifest, endpoint="test.endpoint", max_segments=4,
        sequence=("baseline", "schema_groups"),
    )
    assert manifest.read_bytes() == original
    assert receipt["state"] == "verified"
    assert receipt["row_multisets_equal"] is True
    assert receipt["schemas_equal"] is True
    assert receipt["expected_rows"] == 4
    assert receipt["schema_groups"] == 2
    assert receipt["source_open_mode"] == "read_only"
    assert receipt["production_writes"] is False
    assert all(row["resources"]["after_bind"]["peak_rss_bytes"] > 0 for row in receipt["trials"])


def test_arrow_fingerprint_alone_cannot_prove_timestamp_encoding_equivalence(tmp_path: Path) -> None:
    table = pa.table({"t": pa.array([1], type=pa.timestamp("ns"))})
    manifest = _manifest(tmp_path, [table, table], int96_first=True)
    receipt = benchmark(
        manifest, endpoint="test.endpoint", max_segments=2,
        sequence=("baseline", "schema_groups"),
    )
    assert receipt["schema_groups"] == 1
    assert receipt["state"] == "not_fully_verified"
    assert receipt["schemas_equal"] is False
    assert receipt["row_multisets_equal"] is False


def test_probe_rejects_receipt_mismatch_before_binding(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, [pa.table({"x": [1]})])
    with sqlite3.connect(manifest) as connection:
        connection.execute("UPDATE l1_compaction_segments SET output_rows=2")
    with pytest.raises(RuntimeError, match="segment contract mismatch"):
        benchmark(manifest, endpoint="test.endpoint", max_segments=1)


def test_snapshot_cutoff_and_hash_gate_reject_changed_selection(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, [pa.table({"x": [1]}), pa.table({"x": [2]})])
    with sqlite3.connect(manifest) as connection:
        connection.execute(
            "UPDATE l1_compaction_segments SET created_at='2026-09-27T00:00:00+00:00' "
            "WHERE segment_id='1'"
        )
    receipt = benchmark(
        manifest, endpoint="test.endpoint", max_segments=2,
        created_at_before="2026-09-26T23:00:00+00:00", sequence=("baseline",),
    )
    assert receipt["segments"] == 1
    with pytest.raises(RuntimeError, match="frozen snapshot"):
        benchmark(
            manifest, endpoint="test.endpoint", max_segments=2,
            expected_contract_sha256=receipt["ordered_source_contract_sha256"],
        )


def _save_reference(tmp_path: Path, receipt: dict[str, object]) -> tuple[Path, str]:
    path = tmp_path / "prior-baseline.json"
    content = json.dumps(receipt, indent=2).encode()
    path.write_bytes(content)
    return path, hashlib.sha256(content).hexdigest()


def test_production_policy_uses_actual_physical_proofs_and_preserves_int96_rows(tmp_path: Path) -> None:
    table = pa.table({"t": pa.array([1], type=pa.timestamp("ns"))})
    manifest = _manifest(tmp_path, [table, table], int96_first=True, canonical_sec=True)
    original = manifest.read_bytes()
    receipt = benchmark(
        manifest, endpoint="regulators.sec.filing_headers", max_segments=2,
        sequence=("baseline", "production_schema_groups"),
    )
    assert manifest.read_bytes() == original
    assert receipt["state"] == "verified"
    assert receipt["schema_groups"] == 1
    assert receipt["physical_schema_groups"] == 2
    assert receipt["candidate_policy"] == "production_schema_groups"
    candidate = receipt["trials"][1]
    assert candidate["physical_schema_groups"] == 2
    assert candidate["production_reader_policy"]["physical_identity_revision"] == 1
    assert len(candidate["production_view_input_signature"]) == 64
    assert candidate["production_group_plan_seconds"] <= candidate["bind_seconds"]
    assert candidate["resources"]["after_production_group_plan"]["peak_rss_bytes"] > 0
    assert set(candidate["implementation_sha256"]) == {
        "scripts/benchmark_openbb_l1_view_binding.py", "scripts/compact_openbb_l1.py",
        "scripts/openbb_l1_view_schema.py",
    }


def test_candidate_only_compares_pinned_original_baseline_not_itself(tmp_path: Path) -> None:
    table = pa.table({"t": pa.array([1], type=pa.timestamp("ns"))})
    manifest = _manifest(tmp_path, [table, table], int96_first=True, canonical_sec=True)
    baseline = benchmark(
        manifest, endpoint="regulators.sec.filing_headers", max_segments=2,
        sequence=("baseline",),
    )
    path, digest = _save_reference(tmp_path, baseline)
    candidate = benchmark(
        manifest, endpoint="regulators.sec.filing_headers", max_segments=2,
        sequence=("production_schema_groups",), baseline_receipt=path,
        baseline_receipt_sha256=digest,
    )
    assert candidate["state"] == "verified"
    assert candidate["comparison_mode"] == "hash_pinned_historical_baseline"
    assert candidate["historical_baseline"]["receipt_sha256"] == digest
    assert candidate["historical_baseline"]["paired_timing_comparison"] is False
    # Arrow-only grouping silently drops nanoseconds here: it must not compare
    # against itself and claim the single candidate trial is verified.
    unsafe = benchmark(
        manifest, endpoint="regulators.sec.filing_headers", max_segments=2,
        sequence=("schema_groups",), baseline_receipt=path, baseline_receipt_sha256=digest,
    )
    assert unsafe["state"] == "not_fully_verified"
    assert unsafe["row_multisets_equal"] is False


@pytest.mark.parametrize("sequence", (("schema_groups",), ("production_schema_groups",)))
def test_candidate_only_requires_external_reference_before_source_reads(tmp_path: Path, sequence) -> None:
    with pytest.raises(ValueError, match="hash-pinned"):
        benchmark(tmp_path / "does-not-exist", endpoint="regulators.sec.filing_headers", max_segments=2, sequence=sequence)


@pytest.mark.parametrize("fault", ("hash", "unverified", "no_baseline", "disagree", "contract"))
def test_historical_reference_fails_closed_before_workers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str,
) -> None:
    manifest = _manifest(tmp_path, [pa.table({"x": [1]})], canonical_sec=True)
    baseline = benchmark(
        manifest, endpoint="regulators.sec.filing_headers", max_segments=1,
        sequence=("baseline",),
    )
    if fault == "unverified":
        baseline["row_verification_performed"] = False
    elif fault == "no_baseline":
        baseline["trials"][0]["variant"] = "schema_groups"
    elif fault == "disagree":
        baseline["trials"].append({**baseline["trials"][0], "row_multiset_sha256": "0" * 64})
    elif fault == "contract":
        baseline["ordered_source_contract_sha256"] = "0" * 64
    path, digest = _save_reference(tmp_path, baseline)
    if fault == "hash":
        digest = "0" * 64
    monkeypatch.setattr(subprocess, "run", lambda *_a, **_k: pytest.fail("invalid reference cannot start a worker"))
    with pytest.raises(RuntimeError, match="historical"):
        benchmark(
            manifest, endpoint="regulators.sec.filing_headers", max_segments=1,
            sequence=("production_schema_groups",), baseline_receipt=path,
            baseline_receipt_sha256=digest,
        )


def test_production_candidate_rejects_noncanonical_fallback(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, [pa.table({"x": [1]})], canonical_sec=True)
    with sqlite3.connect(manifest) as connection:
        source = Path(connection.execute("SELECT output_path FROM l1_compaction_segments").fetchone()[0])
        replacement = tmp_path / "external.parquet"
        source.rename(replacement)
        connection.execute("UPDATE l1_compaction_segments SET output_path=?", (str(replacement),))
    with pytest.raises(subprocess.CalledProcessError) as failure:
        benchmark(
            manifest, endpoint="regulators.sec.filing_headers", max_segments=1,
            sequence=("baseline", "production_schema_groups"),
        )
    assert "production schema-group policy rejected" in failure.value.stderr


def test_historical_comparison_cannot_skip_full_rows(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="complete row"):
        benchmark(
            tmp_path / "missing", endpoint="regulators.sec.filing_headers", max_segments=1,
            sequence=("production_schema_groups",), verify_rows=False,
            baseline_receipt=tmp_path / "missing.json", baseline_receipt_sha256="0" * 64,
        )

from datetime import date
import hashlib
import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import verify_tw_public_feature_trials as verifier


def _table():
    bits = np.asarray([0x8000000000000000, 0x7FF8000000000001, 0, 0x3FF0000000000000], dtype=np.uint64)
    return pa.table({"date": pa.array([date(2024, 1, 2)] * 4),
                     "symbol": ["2330", "台積電", None, "0050"],
                     "value": pa.array(bits.view(np.float64), mask=np.asarray([False, False, True, False]))})


def _fixture(root: Path):
    source = root / "source"
    source.mkdir(parents=True)
    pq.write_table(_table(), source / "observed.parquet")
    (root / "original_features.py").write_text("# frozen control\n")
    source_proof = {"name": "observed.parquet", "size": (source / "observed.parquet").stat().st_size,
                    "sha256": verifier.file_hash(source / "observed.parquet")}
    fixture = {"contract": "tw_public_private_full_builder_fixture_v1", "source_files": [source_proof],
               "source_bytes": source_proof["size"],
               "original_code_sha256": verifier.file_hash(root / "original_features.py"),
               "symbol_universe": {"file_count": 2}}
    (root / "fixture.json").write_text(json.dumps(fixture))
    for name, variant, group in (("a", "original", 1), ("b", "streaming", 3)):
        trial = root / name
        trial.mkdir()
        pq.write_table(_table(), trial / "features.parquet", row_group_size=group)
        receipt = {"state": "completed_trial", "variant": variant, "build_mode": "full",
                   "original_code_sha256": fixture["original_code_sha256"],
                   "symbol_universe_receipt": fixture["symbol_universe"], "source_receipts": [source_proof],
                   "rows": 4, "stock_rows": 4, "market_rows": 0, "features": 1,
                   "harness_code_sha256": hashlib.sha256(b"same harness").hexdigest(),
                   "pool_profile": {"effective_threads": 2}, "polars_threads": 2, "arrow_threads": 2,
                   "provider_calls": 0, "model_runtime_imported": False, "production_writes": False,
                   "wall_seconds": 12.0, "cpu_seconds": 20.0, "rss_peak_bytes": 100,
                   "output_receipt": {"name": "features.parquet", "size": (trial / "features.parquet").stat().st_size,
                                      "sha256": verifier.file_hash(trial / "features.parquet")}}
        (trial / "receipt.json").write_text(json.dumps(receipt))
    return root


def test_all_column_bit_proof_ignores_only_encoding_and_batch_boundaries(tmp_path):
    root = _fixture(tmp_path / "fixture")
    assert verifier.ordered_column_bits(root / "a/features.parquet", batch_rows=1) == verifier.ordered_column_bits(
        root / "b/features.parquet", batch_rows=3)
    result = verifier.verify_trials(root, ["a", "b"])
    assert result["state"] == "accepted_exact_output" and result["rows"] == 4
    assert result["proof_method"] == "all_ordered_column_bits_and_validity"
    assert result["repeated_per_variant"] is False


@pytest.mark.parametrize("change", ["signed_zero", "nan_bits", "null", "row_order"])
def test_complete_oracle_rejects_changed_observations(tmp_path, change):
    root = _fixture(tmp_path / "fixture")
    table = _table()
    if change == "row_order":
        table = table.take([3, 2, 1, 0])
    elif change == "null":
        table = table.set_column(2, "value", pa.array([None, float("nan"), None, 1.0]))
    else:
        bits = table["value"].combine_chunks().to_numpy(zero_copy_only=False).view(np.uint64).copy()
        bits[0 if change == "signed_zero" else 1] ^= np.uint64(0x8000000000000000 if change == "signed_zero" else 1)
        table = table.set_column(2, "value", pa.array(bits.view(np.float64), mask=np.asarray([False, False, True, False])))
    path = root / "b/features.parquet"
    pq.write_table(table, path)
    receipt_path = root / "b/receipt.json"
    receipt = json.loads(receipt_path.read_text())
    receipt["output_receipt"].update(size=path.stat().st_size, sha256=verifier.file_hash(path))
    receipt_path.write_text(json.dumps(receipt))
    with pytest.raises(ValueError, match="bits differ"):
        verifier.verify_trials(root, ["a", "b"])


@pytest.mark.parametrize("damage", ["scope", "payload", "member", "partial", "redirect", "harness"])
def test_trial_admission_rejects_incomplete_changed_or_unsafe_evidence(tmp_path, damage):
    root = _fixture(tmp_path / "fixture")
    receipt_path = root / "b/receipt.json"
    receipt = json.loads(receipt_path.read_text())
    if damage == "scope":
        receipt["source_receipts"] = [{"name": "different"}]
    elif damage == "harness":
        receipt["harness_code_sha256"] = "different"
    elif damage == "payload":
        (root / "b/features.parquet").write_bytes(b"corrupt")
    elif damage == "member":
        pq.write_table(_table(), root / "source/unlisted.parquet")
    elif damage == "partial":
        receipt["state"] = "started"
    else:
        receipt_path.rename(root / "original-receipt.json")
        receipt_path.symlink_to(root / "original-receipt.json")
    if damage != "redirect":
        receipt_path.write_text(json.dumps(receipt))
    with pytest.raises(ValueError):
        verifier.verify_trials(root, ["a", "b"])


def test_no_candidate_and_duplicate_trial_cannot_claim_acceptance(tmp_path):
    root = _fixture(tmp_path / "fixture")
    with pytest.raises(ValueError, match="distinct"):
        verifier.verify_trials(root, ["a", "a"])
    receipt_path = root / "b/receipt.json"
    receipt = json.loads(receipt_path.read_text())
    receipt["variant"] = "original"
    receipt_path.write_text(json.dumps(receipt))
    with pytest.raises(ValueError, match="candidate"):
        verifier.verify_trials(root, ["a", "b"])


def test_unsupported_nested_values_are_not_a_weakened_semantic_oracle(tmp_path):
    path = tmp_path / "nested.parquet"
    pq.write_table(pa.table({"nested": [[1], None]}), path)
    with pytest.raises(ValueError, match="unsupported exact-bit"):
        verifier.ordered_column_bits(path)


def test_registered_supplemental_source_is_included_in_exact_inventory(tmp_path):
    root = _fixture(tmp_path / "fixture")
    path = root / "source/supplemental/official.parquet"
    path.parent.mkdir()
    pq.write_table(_table(), path)
    fixture_path = root / "fixture.json"
    fixture = json.loads(fixture_path.read_text())
    fixture["source_files"].append({"name": "supplemental/official.parquet", "size": path.stat().st_size,
                                    "sha256": verifier.file_hash(path)})
    fixture["source_bytes"] += path.stat().st_size
    fixture_path.write_text(json.dumps(fixture))
    assert verifier.verify_trials(root, ["a", "b"])["source_files"] == 2
    pq.write_table(_table(), root / "source/supplemental/unregistered.parquet")
    with pytest.raises(ValueError, match="membership"):
        verifier.verify_trials(root, ["a", "b"])


def test_physical_equality_reads_full_committed_payloads_not_samples(tmp_path, monkeypatch):
    root = _fixture(tmp_path / "fixture")
    (root / "b/features.parquet").write_bytes((root / "a/features.parquet").read_bytes())
    receipt_path = root / "b/receipt.json"
    receipt = json.loads(receipt_path.read_text())
    receipt["output_receipt"] = json.loads((root / "a/receipt.json").read_text())["output_receipt"]
    receipt_path.write_text(json.dumps(receipt))
    monkeypatch.setattr(verifier, "ordered_column_bits", lambda *_args, **_kwargs: pytest.fail("identical full bytes"))
    result = verifier.verify_trials(root, ["a", "b"])
    assert result["proof_method"] == "complete_parquet_bytes"
    path = root / "b/features.parquet"
    damaged = bytearray(path.read_bytes())
    damaged[len(damaged) // 2] ^= 1
    path.write_bytes(damaged)
    with pytest.raises(ValueError, match="differs from its committed receipt"):
        verifier.verify_trials(root, ["a", "b"])


def test_source_revision_during_oracle_rejects_without_modifying_receipts(tmp_path, monkeypatch):
    root = _fixture(tmp_path / "fixture")
    before = (root / "a/receipt.json").read_bytes()
    original = verifier.ordered_column_bits
    def changed(*args, **kwargs):
        result = original(*args, **kwargs)
        (root / "source/observed.parquet").write_bytes(b"revised")
        return result
    monkeypatch.setattr(verifier, "ordered_column_bits", changed)
    with pytest.raises(RuntimeError, match="source generation changed"):
        verifier.verify_trials(root, ["a", "b"])
    assert (root / "a/receipt.json").read_bytes() == before


@pytest.mark.parametrize("field,value", [("wall_seconds", float("nan")), ("rows", True),
                                         ("provider_calls", False), ("rss_peak_bytes", -1)])
def test_nonfinite_or_boolean_measurements_cannot_bless_a_trial(tmp_path, field, value):
    root = _fixture(tmp_path / "fixture")
    receipt_path = root / "b/receipt.json"
    receipt = json.loads(receipt_path.read_text())
    receipt[field] = value
    receipt_path.write_text(json.dumps(receipt))
    with pytest.raises(ValueError):
        verifier.verify_trials(root, ["a", "b"])

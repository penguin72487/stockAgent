"""Exact reconstruction of witnessed financial control encoding differences."""
import json
from pathlib import Path
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from stockagent.training_rescue_worker import (
    PREPARED_REPRESENTATION_FILE_SET, atomic_json, file_hash,
    prepared_file_set, reconcile_prepared_files, preparation_references,
    _ordered_prepared_parquet_identity,
)
from test_training_rescue_preparation import setup_spec, controller_fixture, source_binding


def make_financial(tmp_path):
    spec, root, builder = setup_spec(tmp_path)
    old = tmp_path / "original"
    for base in (old, root):
        (base / "release/daily").mkdir(parents=True)
        (base / "release/rules/sources").mkdir(parents=True)
        (base / "materialization").mkdir()
    tables = {"lifetime_scope.parquet": pa.table({"contract": ["TX1", "TX2"], "value": [-0.0, 2.5]}),
              "release/rules/sources/context_only_suppressed_carries.parquet":
                  pa.table({"date": ["2020-01-02", "2020-01-03"], "quantity": [0, -1]})}
    for name, table in tables.items():
        pq.write_table(table, old / name, compression="NONE")
        pq.write_table(table, root / name, compression="zstd")
        assert file_hash(old / name) != file_hash(root / name)
    for base, products in ((old, ["TX", "MTX"]), (root, ["MTX", "TX"])):
        (base / "release/daily/continuous_daily.parquet").write_bytes(b"exact financial observations")
        (base / "release/rules/rules.parquet").write_bytes(b"exact financial rules")
        (base / "product_scope.csv").write_bytes(b"product\nTX\nMTX\n")
        atomic_json(base / "accounting_checkpoint.json", {"source": "fixed", "cash": -12.5})
        atomic_json(base / "materialization/manifest.json", {"zero_print_products": products,
                                                               "rows": 2, "financial_ready": False})
        material_sha = file_hash(base / "materialization/manifest.json")
        atomic_json(base / "release/rules/sources/admission_review.json",
                    {"lifetime_scope_sha256": file_hash(base / "lifetime_scope.parquet"), "PIT": False})
        names = ["context_only_suppressed_carries.parquet", "admission_review.json"]
        source_rows = [{"path": "sources/" + name, "sha256": file_hash(base / "release/rules/sources" / name)}
                       for name in names]
        atomic_json(base / "release/rules/manifest.json", {"sources": source_rows,
            "source_materialization_sha256": material_sha,
            "context_only_prefix": {"suppressed_carries": source_rows[0]}, "financial_values": -12.5})
        atomic_json(base / "release/daily/manifest.json", {"source_materialization_sha256": material_sha,
            "zero_print_products": products, "rows": 2, "live_eligible": False})
        summary = {"daily": str(base / "release/daily/continuous_daily.parquet"),
                   "rules": str(base / "release/rules/rules.parquet"), "rows": 2}
        atomic_json(base / "release/build_summary.json", summary)
        atomic_json(base / "build_acceptance.json", {**summary, "created_at_utc": "original" if base == old else "rebuilt",
            "elapsed_s": 12 if base == old else 5.0, "lifetime_scope_sha256": file_hash(base / "lifetime_scope.parquet")})
    bundle = tmp_path / "controller/original-representations.zip"
    oracle = prepared_file_set(old, artifact_kind="tw_futures_financial_v1", source_root=Path(spec["source_root"]),
                               representation_bundle=bundle)
    assert oracle["contract"] == PREPARED_REPRESENTATION_FILE_SET
    original_reference = tmp_path / "controller/oracle.json"
    atomic_json(original_reference, oracle)
    reference = {"kind": "file_set_v2", "step_index": 0, "rebuilt_root": str(root),
        "reference_path": str(original_reference), "reference_sha256": file_hash(original_reference),
        "representation_bundle": {"path": str(bundle), "sha256": file_hash(bundle), "bytes": bundle.stat().st_size}}
    spec["prepared_reconciliation"] = [reference]
    preparation_references(spec, require_complete=True)
    return spec, root, old, reference, oracle


def unchanged_bytes(root):
    return {p.relative_to(root).as_posix(): p.read_bytes() for p in root.rglob("*") if p.is_file()}


def test_financial_encoding_and_dependency_graph_restore_complete_original_bytes(tmp_path):
    spec, root, old, reference, oracle = make_financial(tmp_path)
    result = reconcile_prepared_files(reference, spec, 0)
    assert result["representation_reconciliation"]["changed_financial_values"] is False
    assert len(result["representation_reconciliation"]["ordered_arrow_bit_identities"]) == 2
    assert unchanged_bytes(root) == unchanged_bytes(old)
    # A fresh complete reconciliation rechecks all hashes and dependency edges.
    second = reconcile_prepared_files(reference, spec, 0)
    assert second["representation_reconciliation"]["restored_originals"] == []


@pytest.mark.parametrize("change", ["numeric", "row_order", "dtype", "negative_zero", "null"])
def test_ordered_parquet_identity_preserves_values_order_types_and_bits(change):
    original = pa.table({"value": pa.array([-0.0, 2.5], type=pa.float64())})
    candidates = {
        "numeric": pa.table({"value": [-0.0, 2.6]}),
        "row_order": pa.table({"value": [2.5, -0.0]}),
        "dtype": pa.table({"value": pa.array([-0.0, 2.5], type=pa.float32())}),
        "negative_zero": pa.table({"value": [0.0, 2.5]}),
        "null": pa.table({"value": pa.array([None, 2.5], type=pa.float64())}),
    }
    def encoded(table):
        sink = pa.BufferOutputStream(); pq.write_table(table, sink)
        return sink.getvalue().to_pybytes()
    assert _ordered_prepared_parquet_identity(encoded(original)) != _ordered_prepared_parquet_identity(encoded(candidates[change]))


@pytest.mark.parametrize("change", ["value", "row_order", "rule", "link", "product", "duplicate", "bundle", "scope"])
def test_financial_reconciliation_rejects_semantic_changes_before_any_restore(tmp_path, change):
    spec, root, old, reference, oracle = make_financial(tmp_path)
    if change in {"value", "row_order"}:
        table = pa.table({"contract": ["TX1", "TX2"], "value": [-0.0, 9.5]})
        if change == "row_order": table = table.take(pa.array([1, 0]))
        pq.write_table(table, root / "lifetime_scope.parquet")
    elif change == "rule":
        (root / "release/rules/rules.parquet").write_bytes(b"different financial value")
    elif change == "link":
        path = root / "release/rules/sources/admission_review.json"
        body = json.loads(path.read_text()); body["lifetime_scope_sha256"] = "f" * 64
        atomic_json(path, body)
    elif change in {"product", "duplicate"}:
        path = root / "materialization/manifest.json"; body = json.loads(path.read_text())
        body["zero_print_products"] = ["TX", "FAKE"] if change == "product" else ["TX", "TX"]
        atomic_json(path, body)
    elif change == "bundle":
        Path(reference["representation_bundle"]["path"]).write_bytes(b"broken originals")
    else:
        (root / "release/new-unclaimed.parquet").write_bytes(b"unknown extra observation")
    before = unchanged_bytes(root)
    with pytest.raises((ValueError, KeyError)):
        reconcile_prepared_files(reference, spec, 0)
    assert unchanged_bytes(root) == before


def test_representation_resource_cannot_overlap_rebuilt_output(tmp_path):
    spec, root, old, reference, oracle = make_financial(tmp_path)
    reference["representation_bundle"]["path"] = str(root / "hidden.zip")
    with pytest.raises(ValueError, match="outside rebuilt"):
        preparation_references(spec)


def test_v1_oracle_cannot_enable_representation_reconciliation(tmp_path):
    spec, root, old, reference, oracle = make_financial(tmp_path)
    reference["kind"] = "file_set_v1"
    with pytest.raises(ValueError, match="v1 original-file"):
        preparation_references(spec)


@pytest.mark.parametrize("missing_bundle", [False, True])
def test_original_representation_resources_preflight_and_real_delivery(tmp_path, monkeypatch, missing_bundle):
    import stockagent.data_sync.packed_snapshots as packed
    from stockagent.training_rescue import RescueError
    spec, root, old, reference, oracle = make_financial(tmp_path)
    source_binding(spec, tmp_path)
    owner = controller_fixture(tmp_path, monkeypatch)
    resources = [(reference["reference_path"], reference["reference_sha256"]),
                 (reference["representation_bundle"]["path"], reference["representation_bundle"]["sha256"])]
    for destination, sha in resources:
        if missing_bundle and destination == reference["representation_bundle"]["path"]:
            continue
        obj = owner.object_path(sha)
        obj.parent.mkdir(parents=True, exist_ok=True)
        obj.write_bytes(Path(destination).read_bytes())
    # A new destination starts empty. The receiver must supply both resources.
    reference["reference_path"] = str(tmp_path / "fresh-worker/oracle.json")
    reference["representation_bundle"]["path"] = str(tmp_path / "fresh-worker/originals.zip")
    calls = []
    class ByteSSH:
        def command(self, argv):
            calls.append(argv)
            worker = Path(__file__).resolve().parents[1] / "stockagent/training_rescue_worker.py"
            return [sys.executable, str(worker), *argv[2:]]
    class ReachedRaw(Exception):
        pass
    def raw_started(*_):
        for row in owner.prepared_reference_objects(spec):
            assert file_hash(Path(row["reference_path"])) == row["sha256"]
        raise ReachedRaw
    monkeypatch.setattr(packed, "resolve_packed_snapshot_id", raw_started)
    if missing_bundle:
        with pytest.raises(FileNotFoundError):
            owner.restore_inputs(ByteSSH(), spec, tmp_path / "incoming")
        assert calls == [] and not (tmp_path / "fresh-worker").exists()
    else:
        for _ in range(2):
            with pytest.raises(ReachedRaw):
                owner.restore_inputs(ByteSSH(), spec, tmp_path / "incoming")
        assert len(calls) == 4  # Idempotent receiver verifies existing originals.
        Path(reference["representation_bundle"]["path"]).write_bytes(b"conflicting target")
        with pytest.raises(RescueError, match="target rejected"):
            owner.restore_inputs(ByteSSH(), spec, tmp_path / "incoming")
        assert Path(reference["representation_bundle"]["path"]).read_bytes() == b"conflicting target"

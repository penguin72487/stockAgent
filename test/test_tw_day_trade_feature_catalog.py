from pathlib import Path
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.prepare_tw_day_trade_feature_catalog import (
    aggregate_fields, contained, economic_records, field_role, finlab_rows,
    inspect_task, monitor_rows,
)
from scripts.audit_tw_public_data_layer import audit_public_feature_table


def parquet(path, **columns):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(columns), path)


@pytest.mark.parametrize("path", ["../escape.parquet", "/etc/passwd", ""])
def test_receipts_cannot_escape(path, tmp_path):
    with pytest.raises(ValueError):
        contained(tmp_path, path)


def test_numeric_identifier_is_not_a_signal():
    assert field_role("stock_id", ["int64"]) == "key_or_provenance"
    assert field_role("volume", ["int64"]) == "numeric_candidate"
    assert field_role("adjustment_reference_price", ["double"]) == "key_or_provenance"
    assert field_role("transaction_date_issue", ["string"]) == "key_or_provenance"


def test_finlab_wide_tickers_are_one_measure_not_independent_features(tmp_path):
    parquet(tmp_path / "datasets/p.parquet", source_index=["2026-09-24"], **{"2330": [10.], "0050": [20.]})
    receipt = tmp_path / "receipts/p.json"
    receipt.parent.mkdir()
    receipt.write_text(json.dumps({"dataset": "price:close", "rows": 1,
                                  "parquet_path": "datasets/p.parquet", "publication_time_status": "not_verified"}))
    rows, errors = finlab_rows(tmp_path)
    assert not errors and len(rows) == 1
    assert rows[0]["source_value_columns"] == 2
    assert rows[0]["non_null_count"] == 2
    assert rows[0]["admission"] == "research_only_needs_adapter"
    assert not rows[0]["sha256_verified"]


def test_partial_finmind_dataset_not_silently_complete(tmp_path):
    parquet(tmp_path / "p.parquet", price=[1., None])
    (tmp_path / "r.json").write_text(json.dumps({"parquet_path": "p.parquet", "rows": 2}))
    good = inspect_task((tmp_path, {"dataset": "prices", "rows": 2, "receipt_path": "r.json"}))
    bad = {"provider": "FinMind", "lane": tmp_path.name, "dataset": "prices", "error": "missing"}
    rows = aggregate_fields([good, bad])
    assert rows[0]["non_null_count"] == 1
    assert rows[0]["admission"] == "incomplete_source"
    assert not rows[0]["schema_verified"]


def test_receipt_row_mismatch_blocks_file(tmp_path):
    parquet(tmp_path / "p.parquet", price=[1.])
    (tmp_path / "r.json").write_text(json.dumps({"parquet_path": "p.parquet"}))
    result = inspect_task((tmp_path, {"dataset": "prices", "rows": 2, "receipt_path": "r.json"}))
    assert result["error"] == "missing_schema_or_receipt_row_mismatch"


def test_economic_only_current_receipt_files_and_no_pit_inference(tmp_path):
    parquet(tmp_path / "selected.parquet", DataValue=[1.])
    parquet(tmp_path / "old.parquet", DataValue=[9.])
    receipt = tmp_path / "receipts/bea/one.json"
    receipt.parent.mkdir(parents=True)
    receipt.write_text(json.dumps({"provider": "bea", "dataset": "one", "first_observation": "2000Q1",
                                  "files": [{"kind": "parquet", "path": "selected.parquet", "rows": 1}]}))
    records = economic_records(tmp_path)
    assert len(records) == 1
    row = aggregate_fields(records)[0]
    assert row["rows"] == 1 and row["admission"] == "research_only_needs_adapter"
    assert row["publication_clock"] == "unverified"


def test_monitor_excludes_finlab_security_axis_and_null_is_not_zero():
    item = {"dataset_id": "d", "provider": "p", "field": "v", "types": ["double"],
            "non_null_count": 0, "schema_state": "verified"}
    rows = monitor_rows({"rows": [item, {**item, "dataset_id": "physical:finlab:downloaded-datasets"}]})
    assert len(rows) == 1 and rows[0]["admission"] == "missing_values"


def test_legacy_openbb_nulls_are_not_provider_wide_missing_data():
    item = {"dataset_id": "physical:openbb:archives", "provider": "OpenBB", "field": "v",
            "types": ["double"], "non_null_count": 0, "schema_state": "verified"}
    row = monitor_rows({"rows": [item]})[0]
    assert row["admission"] == "needs_current_l1_check"
    assert row["reason"] == "legacy_compact_does_not_cover_newer_l1"


def test_null_diagnostic_field_is_not_missing_training_data(tmp_path):
    parquet(tmp_path / "p.parquet", value=[1.], transaction_date_issue=pa.array([None], type=pa.string()))
    (tmp_path / "r.json").write_text(json.dumps({"parquet_path": "p.parquet", "rows": 1}))
    record = inspect_task((tmp_path, {"dataset": "moi", "rows": 1, "receipt_path": "r.json"}))
    row = next(row for row in aggregate_fields([record]) if row["field"] == "transaction_date_issue")
    assert row["admission"] == "not_model_input"


def test_current_l1_recheck_resolves_old_null_and_keeps_absent_value_unknown(tmp_path):
    import sqlite3
    from scripts.recheck_training_source_nulls import recheck_openbb
    path = tmp_path / "compact_l1/cftc/cot/one.parquet"
    parquet(path, value=[0., None], absent=pa.array([None, None], type=pa.float64()))
    state = tmp_path / "_state/openbb_archive.sqlite3"
    state.parent.mkdir()
    with sqlite3.connect(state) as conn:
        conn.execute("CREATE TABLE l1_compaction_segments(endpoint,output_path,output_rows,output_bytes,status)")
        conn.execute("INSERT INTO l1_compaction_segments VALUES (?,?,?,?,?)",
                     ("cftc.cot", str(path), 2, path.stat().st_size, "success"))
    rows, evidence = recheck_openbb(tmp_path, {"value", "absent"})
    assert {row["field"]: row["non_null_count"] for row in rows} == {"value": 1, "absent": 0}
    assert next(row for row in rows if row["field"] == "value")["decision"] == "observed_in_newer_l1"
    assert len(evidence) == 1 and len(evidence[0]["observed_sha256"]) == 64


def test_research_profile_includes_finlab_and_checks_duplicates(tmp_path):
    from datetime import date
    parquet(tmp_path / "research.parquet", date=[date(2026, 9, 24)] * 2, symbol=["2330"] * 2,
            twfl_example_raw=[1., None])
    stats, annual, findings = audit_public_feature_table(tmp_path / "research.parquet", ["twfl_example_raw"], "__MARKET__")
    assert stats["twfl_example_raw"]["count"] == 1
    assert annual[0]["raw_non_null"] == 1
    assert any(f.code == "duplicate_feature_keys" for f in findings)


def test_core_export_keeps_unknown_distinct_from_observed_zero(tmp_path):
    from types import SimpleNamespace
    import numpy as np
    from scripts.build_tw_feature_admission_bundle import export_masked_features
    panel = SimpleNamespace(
        feature_names=["value", "value__available"], symbols=["2330", "0050"],
        dates=np.array(["2026-09-24"], dtype="datetime64[D]"),
        features=np.array([[[0., 1.], [0., 0.]]], dtype=np.float32),
        alive_mask=np.array([[True, True]]),
    )
    result = export_masked_features(panel, tmp_path / "core.parquet", ["value"])
    table = pq.read_table(tmp_path / "core.parquet")
    assert table["value"].to_pylist() == [0., None]
    assert table["value__available"].to_pylist() == [True, False]
    assert result["missing_cells"] == {"value": 1}
    assert result["observed_zero_cells"] == {"value": 1}
    assert "returns_1d" not in table.column_names


def test_core_export_rejects_invalid_indicator(tmp_path):
    from types import SimpleNamespace
    import numpy as np
    from scripts.build_tw_feature_admission_bundle import export_masked_features
    panel = SimpleNamespace(
        feature_names=["value", "value__available"], symbols=["2330"],
        dates=np.array(["2026-09-24"], dtype="datetime64[D]"),
        features=np.array([[[1., .5]]], dtype=np.float32), alive_mask=np.array([[True]]),
    )
    with pytest.raises(ValueError, match="invalid masked"):
        export_masked_features(panel, tmp_path / "core.parquet", ["value"])
    assert not (tmp_path / "core.parquet").exists()

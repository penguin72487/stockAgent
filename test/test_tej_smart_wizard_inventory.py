import csv
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.build_tej_smart_wizard_inventory import (
    cadence, catalog, category, dump_csv, field_role, local_cadence,
    local_features, main, safe_date, unit, worklist,
)


def binding(fields=None, type_name="LISTED & DELISTED", table="TSE/OTC Unadjusted_Price(Daily)"):
    fields = fields if fields is not None else ["Close(NTD)"]
    return {"record_kind": "binding", "type": type_name, "smart_id": "TEJEquity",
            "table": table, "fields": fields, "field_count": len(fields)}


def local(field="收盤價", count=100, first="2007-01-01", family="stock_daily", grain="daily"):
    return {"local_id": "FinLab|price:" + field, "provider": "FinLab", "dataset": "price:" + field,
            "field": field, "category": family, "cadence": grain, "non_null_count": count,
            "first": first, "last": "2026-09-30"}


def capture(path, records, types=None, complete=False, selected=None):
    types = types or ["LISTED & DELISTED", "Public Corp."]
    header = {"record_kind": "header", "provider": "tej_smart_wizard", "contract_version": 1,
              "types": types, "selected_types": selected or types}
    finish = {"record_kind": "completion", "complete": complete}
    path.write_text("\n".join(json.dumps(r) for r in [header, *records, finish]), encoding="utf-8")
    return path


def test_partial_catalog_is_not_full_coverage(tmp_path):
    bs, scan = catalog([capture(tmp_path / "partial.jsonl", [binding()])])
    assert len(bs) == 1
    assert not scan["catalog_scan_complete"]
    assert len(scan["types_remaining"]) == 2


def test_subsets_can_jointly_prove_catalog_not_historical_entitlement(tmp_path):
    a = capture(tmp_path / "a.jsonl", [binding()], selected=["LISTED & DELISTED"], complete=True)
    b = capture(tmp_path / "b.jsonl", [], selected=["Public Corp."], complete=True)
    bs, scan = catalog([a, b])
    assert scan["catalog_scan_complete"]
    assert len(bs) == 1
    assert scan["inputs"][0]["sha256"]


def test_explicit_type_completion_survives_later_failure(tmp_path):
    p = capture(tmp_path / "p.jsonl", [binding(), {"record_kind": "type_completion", "type": "LISTED & DELISTED", "complete": True}])
    assert catalog([p])[1]["types_verified_complete"] == ["LISTED & DELISTED"]


@pytest.mark.parametrize("change", [
    {"field_count": 9}, {"fields": [None], "field_count": 1}, {"fields": "Close(NTD)"},
    {"type": "Not a source type"}, {"smart_id": ""}, {"table": ""},
])
def test_bad_binding_is_rejected(tmp_path, change):
    rec = {**binding(), **change}
    with pytest.raises(ValueError, match="binding"):
        catalog([capture(tmp_path / "bad.jsonl", [rec])])


def test_schema_drift_not_counted_as_another_new_feature(tmp_path):
    with pytest.raises(ValueError, match="schema changed"):
        catalog([capture(tmp_path / "bad.jsonl", [binding(), binding(["Close(NTD)", "Open(NTD)"])])])


def test_conflicting_type_scope_rejected(tmp_path):
    a = capture(tmp_path / "a.jsonl", [binding()])
    b = capture(tmp_path / "b.jsonl", [], types=["Different"])
    with pytest.raises(ValueError, match="type list"):
        catalog([a, b])


def test_no_catalog_is_not_complete():
    with pytest.raises(ValueError):
        catalog([])


def test_type_aliases_kept_but_identical_schema_not_double_counted():
    rows = worklist([binding(), binding(type_name="Public Corp.")], [local()], start="2014-01-01")
    assert len(rows) == 1
    assert rows[0]["available_types"] == ["LISTED & DELISTED", "Public Corp."]
    assert rows[0]["phase"] == "P3"
    assert rows[0]["tej_history_count"] is None
    assert not rows[0]["download_started"]
    assert rows[0]["match_basis"] == "concept_hint_not_proven_equivalence"


def test_unknown_count_is_not_zero_or_proof_of_history():
    r = worklist([binding()], [local(count=None)], start="2014-01-01")[0]
    assert r["phase"] == "P2" and r["local_non_null_count"] is None
    assert r["local_count_basis"] == "unknown_not_zero"


def test_late_start_is_repair_candidate():
    r = worklist([binding()], [local(first="2018-01-01")], start="2014-01-01")[0]
    assert r["phase"] == "P2"


def test_minute_or_unknown_grid_is_not_daily_completeness():
    for grain in ("minute", "tick", "unknown_or_event"):
        r = worklist([binding()], [local(grain=grain)], start="2014-01-01")[0]
        assert r["phase"] == "P2"
        assert r["local_non_null_count"] is None
        assert not r["same_family_cadence_match_ids"]


def test_adjustment_and_time_grain_not_collapsed():
    adjusted = binding(table="TSE/OTC Adjusted_Price(Daily)-ExR+D")
    monthly = binding(table="TSE/OTC Unadjusted_Price(Monthly)")
    rows = worklist([binding(), adjusted, monthly], [local()], start="2014-01-01")
    assert len(rows) == 3
    assert {r["table"]: r["phase"] for r in rows} == {
        binding()["table"]: "P3", adjusted["table"]: "P2", monthly["table"]: "P2"}


def test_phase_order_then_comparable_low_count_order():
    rows = worklist([binding(["Close(NTD)", "High(NTD)", "New Proprietary Signal"])] ,
                    [local(count=20, first="2019-01-01"), local("最高價", 10, "2018-01-01")], start="2014-01-01")
    assert [(r["phase"], r["field"]) for r in rows] == [
        ("P1", "New Proprietary Signal"), ("P2", "High(NTD)"), ("P2", "Close(NTD)")]


def test_preserve_unit_and_ratio_but_do_not_use_log_as_raw():
    assert unit("Volume(1000S)") == ("thousand_shares", "shares_after_multiply_1000")
    assert unit("Amount(NTD1000)") == ("thousand_TWD", "TWD_after_multiply_1000")
    assert unit("Turn Over%") == ("percentage_points", "retain_original_ratio_scale")
    rows = worklist([binding(["ROI%-Ln", "ROI%", "Announcement Date - SALE"])], [], start="2014-01-01")
    by_name = {r["field"]: r for r in rows}
    assert "do_not_use_as_raw_input" in by_name["ROI%-Ln"]["original_format"]
    assert "retained" in by_name["ROI%"]["original_format"]
    assert by_name["Announcement Date - SALE"]["field_role"] == "date_or_availability_metadata"


def test_aliases_are_case_spacing_insensitive():
    row = worklist([binding(["Close (NTD)"])], [local()], start="2014-01-01")[0]
    assert row["phase"] == "P3"


def test_cadence_and_special_family():
    assert category("Abstract Auditor Report", smart="TEJ IFRS") == "audit_report"
    assert category("Subsidiary Monthly Sales") == "segments_subsidiaries"
    assert category("Financial Holding Groups") == "governance_investments"
    assert cadence("TSE/OTC Unadjusted_Price(Monthly)", "stock_daily")[0] == "monthly"
    assert local_cadence("physical:tw-futures:minute", "futures") == "minute"
    assert local_cadence("price:收盤價", "stock_daily") == "daily"
    assert local_cadence("fundamental_features:每股盈餘", "financial") == "unknown_or_event"


@pytest.mark.parametrize("value", [None, "1911-00-07", "2999-01-01", "not-a-date"])
def test_invalid_future_unknown_bounds_are_unknown(value):
    assert safe_date(value) is None


def test_csv_includes_late_keys_and_bom(tmp_path):
    target = tmp_path / "wide.csv"
    dump_csv(target, [{"a": "原始欄位"}, {"a": "b", "c": 123}])
    assert target.read_bytes().startswith(b"\xef\xbb\xbf")
    with target.open(encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[1]["c"] == "123"


def test_local_receipts_do_not_turn_instrument_columns_into_features(tmp_path):
    base = tmp_path / "data_finlab"
    (base / "receipts").mkdir(parents=True)
    (base / "datasets").mkdir()
    pq.write_table(pa.table({"date": ["2020-01-01"], "2330": [1.0], "2317": [2.0]}), base / "datasets/p.parquet")
    (base / "receipts/p.json").write_text(json.dumps({"dataset": "price:收盤價", "parquet_path": "datasets/p.parquet",
                                                     "non_null_values": 2, "first_non_null_source_index": "2020-01-01"}))
    monitor = tmp_path / "artifacts/live/data_monitor"
    monitor.mkdir(parents=True)
    (monitor / "feature_inventory.json").write_text(json.dumps({"rows": [{"provider": "FinLab", "dataset_id": "x", "field": "2330"}]}))
    rows, evidence = local_features(tmp_path)
    assert len(rows) == 1 and rows[0]["field"] == "收盤價"
    assert rows[0]["non_null_count"] == 2
    assert evidence[0]["excluded_finlab_instrument_axis_rows"] == 1


def test_receipt_missing_source_is_not_obtained(tmp_path):
    base = tmp_path / "data_finlab/receipts"
    base.mkdir(parents=True)
    (base / "p.json").write_text(json.dumps({"dataset": "price:收盤價", "parquet_path": "datasets/missing.parquet", "non_null_values": 99}))
    assert local_features(tmp_path)[0] == []


def test_build_artifact_phases_reconcile_and_never_overwrite(tmp_path):
    source = capture(tmp_path / "catalog.jsonl", [binding(["Close(NTD)", "ROI%-Ln"])])
    output = tmp_path / "new"
    args = ["--root", str(tmp_path), "--catalog", str(source), "--output-dir", str(output)]
    assert main(args) == 0
    summary = json.loads((output / "summary.json").read_text())
    assert summary["unique_table_schema_fields"] == sum(summary["phase_counts"].values()) == 2
    assert not summary["catalog"]["catalog_scan_complete"]
    assert not any(summary["limits"].values())
    before = (output / "summary.json").read_bytes()
    with pytest.raises(FileExistsError):
        main(args)
    assert (output / "summary.json").read_bytes() == before


def test_invalid_catalog_creates_no_artifact(tmp_path):
    source = capture(tmp_path / "catalog.jsonl", [{**binding(), "field_count": 99}])
    output = tmp_path / "new"
    with pytest.raises(ValueError):
        main(["--root", str(tmp_path), "--catalog", str(source), "--output-dir", str(output)])
    assert not output.exists()


def test_empty_field_table_is_still_in_complete_table_catalog(tmp_path):
    source = capture(tmp_path / "catalog.jsonl", [binding([])], complete=True)
    output = tmp_path / "result"
    main(["--root", str(tmp_path), "--catalog", str(source), "--output-dir", str(output)])
    with (output / "tables.csv").open(encoding="utf-8-sig") as stream:
        tables = list(csv.DictReader(stream))
    summary = json.loads((output / "summary.json").read_text())
    assert len(tables) == summary["tables"] == 1
    assert summary["unique_table_schema_fields"] == 0
    assert summary["catalog"]["catalog_scan_complete"]

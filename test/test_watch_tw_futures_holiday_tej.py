import json

import polars as pl
import pytest

from downloader.artifact_io import sha256_file
from scripts.watch_tw_futures_holiday_tej import TABLE, compare_observation, read_completed, validate_scope


ENTRY = {"task_id": "task", "symbol": "LO2201209", "query_date": "2012-08-03"}
TERM = {"effective_date": "2012-08-02", "contract_multiplier": 2100,
        "has_equity_credit_fields": True, "equity_cash_credit_twd": 1000}
HEADERS = ["Future Code", "Adjusted Date", "Shares per Contract", "Cash Dividends per Unit"]


def source(adjusted="2012/08/02", shares="2100", cash="1000"):
    values = ["LO2201209 contract", adjusted, shares, cash]
    row = dict(zip(HEADERS, values, strict=True))
    row.update(_query_symbol="LO2201209", _query_period="2012-08-03")
    return [row], {"cells": [HEADERS, values]}


def task():
    return {"task_id": "task", "table_id": TABLE, "kind": "download",
            "scope_contract": "editable_source_scope_v1", "request_json": json.dumps({
                "action": "download", "company_labels": ["LO2201209==>contract"],
                "date_labels": ["20120803"], "start": "2012-08-03", "end": "2012-08-03"})}


def test_control_preserves_effective_date_and_compares_original_cells():
    rows, raw = source()
    result = compare_observation(rows, raw, ENTRY, TERM)
    assert result["control_only"] is True
    assert result["query_date"] == "2012-08-03"
    assert result["official_effective_date"] == "2012-08-02"
    assert result["vendor_adjusted_date"] == "2012-08-02"
    assert result["same_event_date"] is True
    assert result["shares_per_contract"]["status"] == "equal"
    assert result["cash_credit_per_contract_twd"]["status"] == "equal"
    assert result["source_units_and_account_transition_verified"] is False


def test_source_date_difference_and_missing_observation_are_not_rewritten():
    rows, raw = source(adjusted="2012/08/03")
    result = compare_observation(rows, raw, ENTRY, TERM)
    assert result["same_event_date"] is False
    assert result["official_effective_date"] == "2012-08-02"
    assert result["vendor_adjusted_date"] == "2012-08-03"
    empty = compare_observation([], {}, ENTRY, TERM)
    assert empty["state"] == "no_matching_observation"
    assert empty["absence_establishes_no_event"] is False


def test_raw_conflict_and_wrong_source_cell_fail_closed():
    rows, raw = source()
    raw["cells"][1][2] = "2099"
    with pytest.raises(ValueError, match="raw preview and parquet"):
        compare_observation(rows, raw, ENTRY, TERM)
    rows, raw = source()
    rows[0]["_query_symbol"] = "WRONG"
    with pytest.raises(ValueError, match="nonempty native source"):
        compare_observation(rows, raw, ENTRY, TERM)


def test_only_exact_native_query_is_admitted():
    native = task()
    validate_scope(native, ENTRY)
    request = json.loads(native["request_json"])
    request["date_labels"].append("20120802")
    native["request_json"] = json.dumps(request)
    with pytest.raises(ValueError, match="exact contract and date"):
        validate_scope(native, ENTRY)


def test_missing_official_cash_remains_unknown():
    rows, raw = source()
    result = compare_observation(rows, raw, ENTRY, {**TERM, "has_equity_credit_fields": False})
    assert result["cash_credit_per_contract_twd"]["status"] == "unknown"


def test_completed_receipt_readback_rejects_changed_native_payload(tmp_path):
    rows, raw = source()
    for name in ("receipts", "raw", "datasets"):
        (tmp_path / name).mkdir()
    raw_path = tmp_path / "raw/source.json"
    raw_path.write_text(json.dumps(raw))
    parquet = tmp_path / "datasets/source.parquet"
    pl.DataFrame(rows).write_parquet(parquet)
    receipt = {"task_id": "task", "table_id": TABLE, "provider": "tej_smart_wizard",
               "exported_rows": 1, "source_outcome": "exported_query_grid",
               "raw_path": "raw/source.json", "raw_sha256": sha256_file(raw_path),
               "parquet_path": "datasets/source.parquet", "parquet_sha256": sha256_file(parquet)}
    (tmp_path / "receipts/task.json").write_text(json.dumps(receipt))
    native = {**task(), "state": "complete"}
    entry = {**ENTRY, "request_id": "request"}
    result = read_completed(tmp_path, entry, TERM, native, {"rows": 1})
    assert result["raw_cells_match_parquet"] is True
    assert result["financial_or_training_adopted"] is False
    raw_path.write_text(json.dumps({"cells": []}))
    with pytest.raises(ValueError, match="raw source digest"):
        read_completed(tmp_path, entry, TERM, native, {"rows": 1})


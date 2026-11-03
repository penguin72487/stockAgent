from datetime import date
import hashlib

import polars as pl
from polars.testing import assert_frame_equal
import pytest

import stockagent.data.tw_public_incremental as planner
import stockagent.data.tw_public_features as features

build = features.build_tw_public_training_features


def _daily(root, changed=False):
    days = ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08", "2024-01-09"]
    closes = ["100", "101", "102", "103", "104", "105"]
    if changed:
        closes[2] = "112"
    pl.DataFrame({"date": days, "證券代號": ["2330"] * len(days),
                  "收盤價": closes, "最高價": closes, "最低價": closes,
                  "成交股數": ["1000"] * len(days), "成交金額": ["100000"] * len(days),
                  "成交筆數": ["10"] * len(days)}).write_parquet(root / "twse_daily_ohlcv.parquet")


@pytest.mark.parametrize("change", [
    "future", "cosmetic", "empty_symbol", "revise_old", "delete_old", "duplicate_order",
    "clock_shift", "mixed_stock", "extend_end", "invalid_output", "source_added",
    "weekend_collision",
])
def test_transfer_semantics_against_full_builder(tmp_path, change):
    root = tmp_path / "source"
    root.mkdir()
    _daily(root)
    path = root / "tpex_api_mopsfin_t187ap12_o.parquet"
    rows = [
        {"Date": "1130102", "SecuritiesCompanyCode": "2330", "預定轉讓方式及股數-轉讓股數": "100",
         "_downloaded_at_utc": "2024-01-02T00:30:00Z", "CompanyName": "old"},
        {"Date": "1130102", "SecuritiesCompanyCode": "2330", "預定轉讓方式及股數-轉讓股數": "200",
         "_downloaded_at_utc": "2024-01-02T00:30:00Z", "CompanyName": "old"},
        {"Date": "1130106", "SecuritiesCompanyCode": "2330", "預定轉讓方式及股數-轉讓股數": "30",
         "_downloaded_at_utc": "2024-01-06T00:30:00Z", "CompanyName": "old"},
        {"Date": "1130107", "SecuritiesCompanyCode": "2330", "預定轉讓方式及股數-轉讓股數": "40",
         "_downloaded_at_utc": "2024-01-07T00:30:00Z", "CompanyName": "old"},
    ]
    pl.DataFrame(rows).write_parquet(path)
    target = tmp_path / "result.parquet"
    common = dict(input_dir=root, output_path=target, symbols_root=None,
                  allow_daily_publication_lag=True, end_date=date(2024, 1, 8))
    first = build(**common, incremental_auto=True)
    assert first.input_partitions["sources"][path.name]["session_clock"]["column"] == "canonical_tpex_transfer_availability"
    prior_bytes = target.read_bytes()
    rows = [dict(row) for row in rows]
    if change == "future":
        rows.append({**rows[0], "Date": "1130109", "_downloaded_at_utc": "2024-01-09T01:30:00Z"})
    elif change == "cosmetic":
        rows[0]["CompanyName"] = "revised name"
    elif change == "empty_symbol":
        rows.append({**rows[0], "SecuritiesCompanyCode": ""})
    elif change == "revise_old":
        rows[0]["預定轉讓方式及股數-轉讓股數"] = "800"
    elif change == "delete_old":
        rows.pop(0)
    elif change == "duplicate_order":
        rows[0], rows[1] = rows[1], rows[0]
    elif change == "clock_shift":
        rows[0]["_downloaded_at_utc"] = "2024-01-02T01:30:00Z"
    elif change == "weekend_collision":
        rows[2]["預定轉讓方式及股數-轉讓股數"] = "90"
    else:
        rows.append({**rows[0], "Date": "1130104", "_downloaded_at_utc": "2024-01-04T00:30:00Z"})
    pl.DataFrame(rows).write_parquet(path)
    if change == "mixed_stock":
        _daily(root, changed=True)
    elif change == "extend_end":
        common["end_date"] = date(2024, 1, 9)
    elif change == "invalid_output":
        pl.read_parquet(target).with_columns(pl.lit(1.0).alias("twpub_insider_transfer_shares_log")).write_parquet(target)
    elif change == "source_added":
        pl.DataFrame({"x": [1]}).write_parquet(root / "unknown_dependency.parquet")
    actual = build(**common, incremental_auto=True)
    result = pl.read_parquet(target)
    if change in {"future", "cosmetic", "empty_symbol"}:
        assert actual.build_mode == "unchanged_verified"
        assert target.read_bytes() == prior_bytes
    elif change in {"invalid_output", "source_added"}:
        assert actual.build_mode == "full"
    else:
        assert actual.build_mode == "incremental_tail"
        assert actual.incremental_start_date == ("2024-01-02" if change in {
            "revise_old", "delete_old", "duplicate_order", "clock_shift"
        } else ("2024-01-06" if change == "weekend_collision" else "2024-01-04"))
    reference = tmp_path / "reference.parquet"
    build(**{**common, "output_path": reference})
    assert_frame_equal(result, pl.read_parquet(reference), check_exact=True,
                       check_row_order=True, check_column_order=True)


def test_transfer_proof_fails_closed_on_receipt_schema_and_concurrent_mutation(tmp_path, monkeypatch):
    path = tmp_path / "tpex_api_mopsfin_t187ap12_o.parquet"
    pl.DataFrame({"Date": ["1130102"]}).write_parquet(path)
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(ValueError, match="captured receipt"):
        planner._tpex_transfer_feature_session_fingerprints(path, source_sha256="0" * 64)
    with pytest.raises(ValueError, match="feature column"):
        planner._tpex_transfer_feature_session_fingerprints(path, source_sha256=sha)
    pl.DataFrame({"Date": ["1130102"], "SecuritiesCompanyCode": ["2330"],
                  "預定轉讓方式及股數-轉讓股數": ["1"]}).write_parquet(path)
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    generation = planner._generation
    calls = []
    def unstable(path):
        result = generation(path)
        calls.append(result)
        return result if len(calls) == 1 else (*result[:-1], result[-1] + 1)
    monkeypatch.setattr(planner, "_generation", unstable)
    with pytest.raises(OSError, match="changed during"):
        planner._tpex_transfer_feature_session_fingerprints(path, source_sha256=sha)


def test_transfer_proof_reuses_existing_audited_exclusions():
    assert planner.proven_non_feature_sources() == planner.NON_FEATURE_SOURCES


@pytest.mark.parametrize("values", [["0.5"], ["NaN"], ["inf"], [str(2**52)], [str(2**52 - 1), "1"]])
def test_transfer_proof_rejects_inexact_or_unbounded_sums(tmp_path, values):
    path = tmp_path / "tpex_api_mopsfin_t187ap12_o.parquet"
    pl.DataFrame({"Date": ["1130102"] * len(values), "SecuritiesCompanyCode": ["2330"] * len(values),
                  "預定轉讓方式及股數-轉讓股數": values}).write_parquet(path)
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(ValueError, match="bounded exact integers"):
        planner._tpex_transfer_feature_session_fingerprints(path, source_sha256=sha)
    receipts = features._source_content_receipts(tmp_path)
    assert planner.capture_partitions(tmp_path, receipts, None)["sources"][path.name]["unsupported"] == "ValueError"


def test_fractional_transfer_change_falls_back_to_full_builder(tmp_path):
    root = tmp_path / "source"
    root.mkdir()
    _daily(root)
    path = root / "tpex_api_mopsfin_t187ap12_o.parquet"
    row = {"Date": "1130102", "SecuritiesCompanyCode": "2330", "預定轉讓方式及股數-轉讓股數": "100"}
    pl.DataFrame([row]).write_parquet(path)
    target = tmp_path / "result.parquet"
    common = dict(input_dir=root, output_path=target, symbols_root=None,
                  allow_daily_publication_lag=True, end_date=date(2024, 1, 8))
    build(**common, incremental_auto=True)
    pl.DataFrame([{**row, "預定轉讓方式及股數-轉讓股數": "100.5"}]).write_parquet(path)
    actual = build(**common, incremental_auto=True)
    assert actual.build_mode == "full"
    assert actual.incremental_plan["reason"] == "source_partition_proof_missing_or_schema_changed"
    reference = tmp_path / "reference.parquet"
    build(**{**common, "output_path": reference})
    assert_frame_equal(pl.read_parquet(target), pl.read_parquet(reference), check_exact=True,
                       check_row_order=True, check_column_order=True)


@pytest.mark.parametrize("change", ["ap13", "ap31", "both", "mixed_future"])
def test_audited_unused_sources_preserve_output_and_refresh_receipts(tmp_path, change):
    root = tmp_path / "source"
    root.mkdir()
    _daily(root)
    names = ("tpex_api_mopsfin_t187ap13_o.parquet", "tpex_api_mopsfin_t187ap31_o.parquet")
    for name in names:
        pl.DataFrame({"raw": ["old"]}).write_parquet(root / name)
    path = root / "tpex_api_mopsfin_t187ap12_o.parquet"
    rows = [{"Date": "1130102", "SecuritiesCompanyCode": "2330",
             "預定轉讓方式及股數-轉讓股數": "100", "_downloaded_at_utc": "2024-01-02T00:30:00Z"}]
    pl.DataFrame(rows).write_parquet(path)
    target = tmp_path / "result.parquet"
    common = dict(input_dir=root, output_path=target, symbols_root=None,
                  allow_daily_publication_lag=True, end_date=date(2024, 1, 8))
    first = build(**common, incremental_auto=True)
    prior_bytes = target.read_bytes()
    selected = names if change in {"both", "mixed_future"} else (names[0 if change == "ap13" else 1],)
    for name in selected:
        pl.DataFrame({"raw": ["new"]}).write_parquet(root / name)
    if change == "mixed_future":
        rows.append({**rows[0], "Date": "1130109", "_downloaded_at_utc": "2024-01-09T01:30:00Z"})
        pl.DataFrame(rows).write_parquet(path)
    actual = build(**common, incremental_auto=True)
    assert actual.build_mode == "unchanged_verified"
    assert target.read_bytes() == prior_bytes
    assert actual.source_receipts != first.source_receipts
    assert set(actual.incremental_plan["non_feature_sources"]) == set(selected)
    reference = tmp_path / "reference.parquet"
    build(**{**common, "output_path": reference})
    assert_frame_equal(pl.read_parquet(target), pl.read_parquet(reference), check_exact=True,
                       check_row_order=True, check_column_order=True)

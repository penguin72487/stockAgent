"""The diagnostic reports later gates without relaxing the real attachment."""
from datetime import date
import hashlib
import json

import polars as pl
import pytest

from scripts.audit_tw_futures_holiday_gate_coverage import audit, _prior_market_settlement_comparison


def daily_fixture(tmp_path):
    path = tmp_path / "continuous_daily.parquet"
    pl.DataFrame({
        "date": [date(2011, 1, 3), date(2013, 2, 7), date(2013, 2, 7), date(2026, 9, 24)],
        "product": ["TX", "TX", "MTX", "TX"],
        "accounting_only_date": [False, True, True, False],
        "cash_settlement": [False, False, True, False],
        "executable": [True, False, False, True],
    }).write_parquet(path)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    path.with_name("manifest.json").write_text(json.dumps({
        "status": "complete", "outputs": {"continuous_daily": {"sha256": digest}},
    }))
    return path


def test_diagnostic_collects_each_gate_and_distinguishes_candidate_rows(tmp_path):
    result = audit(daily_fixture(tmp_path), "2011-01-03", "2026-09-24")
    assert [gate["state"] for gate in result["canonical_gates"]["early_holidays"]] == ["rejected", "rejected"]
    assert result["canonical_gates"]["known_account_dates"]["state"] == "rejected"
    assert result["canonical_gates"]["nonterminal_valuations"]["state"] == "rejected"
    assert "2013-02-08" in result["known_account_dates_missing"]
    assert "2013-02-07" not in result["known_account_dates_missing"]
    assert result["account_only_dates"] == [{"date": "2013-02-07", "candidate_contract_rows": 2,
        "cash_settlement_rows": 1, "nonterminal_rows": 1, "unknown_settlement_rows": 0,
        "executable_rows": 0, "unknown_execution_rows": 0}]
    assert not result["training_admitted"]


def test_diagnostic_rejects_unpinned_source_and_non_iso_dates(tmp_path):
    path = daily_fixture(tmp_path)
    with pytest.raises(ValueError, match="audit needs"):
        audit(path, "2011-1-3", "2026-09-24")
    path.with_name("manifest.json").write_text('{"status":"complete","outputs":{}}')
    with pytest.raises(ValueError, match="source manifest"):
        audit(path, "2011-01-03", "2026-09-24")


def test_research_dates_are_counted_without_promoting_the_checkpoint(tmp_path):
    daily = daily_fixture(tmp_path)
    root = tmp_path / "research"
    root.mkdir()
    frame = root / "frame.parquet"
    pl.DataFrame({"date": [date(2013, 2, 7), date(2013, 2, 8)],
                  "accounting_only_date": [True, True], "product": ["TX", "TX"],
                  "physical_instance": ["TX:201303#generation=0", "TX:201303#generation=0"],
                  "asset_class": ["index_future", "index_future"],
                  "cash_settlement": [False, False], "valuation_price": [50.0, None],
                  "previous_settlement": [50.0, 50.0], "executable": [False, False],
                  "valuation_research_applied": [False, True]}).write_parquet(frame)
    checkpoint = root / "accounting_checkpoint.json"
    checkpoint.write_text(json.dumps({"outputs": {"frame.parquet": hashlib.sha256(frame.read_bytes()).hexdigest()}}))
    stage = root / "accounting_stage.json"
    stage.write_text(json.dumps({"status": "holiday_account_dates_research_checkpoint_not_published",
        "financial_release_accepted": False, "holiday_valuation_complete": False,
        "training_admitted": False, "checkpoint_sha256": hashlib.sha256(checkpoint.read_bytes()).hexdigest()}))
    night = root / "night.json"
    night.write_text(json.dumps({
        "contract": "holiday_night_session_corrected_readback_v3",
        "remote_financial_parent_sha256": hashlib.sha256(daily.read_bytes()).hexdigest(),
        "physical_parent_count": 1,
        "physical_rows": [{
            "source_status": "official_and_unique_finmind_match",
            "parent": {"product": "TX", "contract": "201303", "physical_instance": "TX:201303#generation=0"},
            "observation": {"product": "TX", "contract": "201303", "exact_close_match": True,
                            "official": {"close": 54.0},
                            "finmind_last_tick": {"unique_price_at_last_timestamp": True, "prices": [54.0]}},
            "account_dates": ["2013-02-07", "2013-02-08"],
        }],
    }))
    result = audit(daily, "2011-01-03", "2026-09-24", research_dir=root, night_readback=night)
    candidate = result["existing_research_checkpoint"]
    assert candidate["v22_missing_dates_present_in_research_frame"] == ["2013-02-08"]
    assert candidate["candidate_valuation_scope_by_asset_and_settlement"] == [{
        "asset_class": "index_future", "cash_settlement": False, "candidate_rows": 2,
        "candidate_product_codes": 1, "null_valuation_rows": 1,
        "nonfinite_nonnull_valuation_rows": 0, "executable_rows": 0,
        "unknown_execution_rows": 0, "research_applied_rows": 1,
        "unknown_research_applied_rows": 0,
    }]
    assert not candidate["training_admitted"]
    assert not result["training_admitted"]
    assert candidate["candidate_nonterminal_previous_settlement_comparison_by_year"] == [{
        "year": 2013, "candidate_nonterminal_rows": 2, "equal_previous_settlement_rows": 1,
        "different_previous_settlement_rows": 0, "unknown_comparison_rows": 1,
    }]
    assert [row["comparison"] for row in result["night_readback_comparison"]["rows"]] == [
        "different", "unknown_research_valuation"]
    assert not result["night_readback_comparison"]["historical_broker_mark_verified"]
    invalid = json.loads(night.read_text())
    invalid["physical_rows"][0]["observation"]["finmind_last_tick"]["prices"] = [53.0, 54.0]
    night.write_text(json.dumps(invalid))
    with pytest.raises(ValueError, match="does not support"):
        audit(daily, "2011-01-03", "2026-09-24", research_dir=root, night_readback=night)
    frame.write_bytes(frame.read_bytes() + b"changed")
    with pytest.raises(ValueError, match="research checkpoint"):
        audit(daily, "2011-01-03", "2026-09-24", research_dir=root)


def test_prior_settlement_comparison_keeps_contract_identity_and_future_data_out():
    # Contract B has only a later market price; C's same-month identifier is a
    # different generation. Neither may be silently used for the holiday mark.
    frame = pl.DataFrame({
        "date": [date(2018, 2, 12)] * 2 + [date(2018, 2, 13)] * 3 + [date(2018, 2, 21)],
        "product": ["TGF"] * 6,
        "contract": ["201802", "201806", "201802", "201804", "201806", "201804"],
        "physical_instance": ["A#0", "C#0", "A#0", "B#0", "C#1", "B#0"],
        "asset_class": ["commodity_future"] * 6,
        "accounting_only_date": [False, False, True, True, True, False],
        "cash_settlement": [False] * 6,
        "valuation_price": [50.0, 60.0, 50.0, 70.0, 60.0, 70.0],
        "official_settlement": ["50", "60", None, None, None, "70"],
        "official_source_sha256": ["a" * 64, "a" * 64, None, None, None, "b" * 64],
    }).lazy()
    result = _prior_market_settlement_comparison(frame, frame.filter(pl.col("accounting_only_date")))
    assert [(row["contract"], row["comparison"]) for row in result["tgf_2018_detail"]] == [
        ("201802", "equal"), ("201804", "same_physical_prior_row_missing"),
        ("201806", "same_physical_prior_row_missing")]
    assert all(row["prior_market_date"] == "2018-02-12" for row in result["tgf_2018_detail"])
    assert [row["contract"] for row in result["non_equal_details"]] == ["201804", "201806"]
    assert not result["historical_broker_mark_verified"]


def test_prior_settlement_reports_roundoff_separately_from_economic_difference():
    frame = pl.DataFrame({
        "date": [date(2020, 1, 20)] * 2 + [date(2020, 1, 21)] * 2,
        "product": ["EY1"] * 4, "contract": ["A", "B", "A", "B"],
        "physical_instance": ["A#0", "B#0", "A#0", "B#0"],
        "asset_class": ["stock_future"] * 4, "accounting_only_date": [False, False, True, True],
        "cash_settlement": [False] * 4, "valuation_price": [7.93, 7.93, 7.929999999999999, 7.92],
        "official_settlement": ["7.93", "7.93", None, None],
        "official_source_sha256": ["a" * 64, "a" * 64, None, None],
    }).lazy()
    result = _prior_market_settlement_comparison(frame, frame.filter(pl.col("accounting_only_date")))
    assert [r["comparison"] for r in result["non_equal_details"]] == [
        "equal_within_fp64_roundoff", "different"]

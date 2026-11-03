from datetime import date
import json
from pathlib import Path

import polars as pl
import pytest

from downloader.artifact_io import sha256_file
from scripts import audit_tw_futures_monthly_listing_candidates as audit
from scripts.audit_tw_futures_monthly_listing_candidates import classify, manual_five_months


def test_manual_five_months_avoids_duplicate_quarter_and_rolls_year():
    assert manual_five_months(2011, 1) == ("201101", "201102", "201103", "201106", "201109")
    assert manual_five_months(2011, 2) == ("201102", "201103", "201106", "201109", "201112")
    assert manual_five_months(2011, 3) == ("201103", "201104", "201106", "201109", "201112")
    assert manual_five_months(2011, 4) == ("201104", "201105", "201106", "201109", "201112")
    assert manual_five_months(2011, 12) == ("201112", "201201", "201203", "201206", "201209")


def test_only_new_month_after_observed_third_wednesday_is_candidate():
    days = {date(2011, 1, 19), date(2011, 1, 20)}
    match = classify("201112", date(2011, 1, 20), days)
    assert match["state"] == "matches_2009_manual_schedule_candidate"
    assert match["manual_new_month"] == "201112"
    assert classify("201102", date(2011, 1, 20), days)["state"] == "different_from_manual_new_month"


def test_quarter_transition_introduces_following_second_month():
    days = {date(2011, 3, 16), date(2011, 3, 17)}
    assert classify("201105", date(2011, 3, 17), days)["state"] == "matches_2009_manual_schedule_candidate"
    assert classify("201112", date(2011, 3, 17), days)["state"] == "different_from_manual_new_month"


def test_holiday_or_absent_source_is_not_an_ordinary_candidate():
    assert classify("201112", date(2011, 1, 20), {date(2011, 1, 20)})["state"] == "nominal_session_source_not_observed"
    assert classify("201112", date(2011, 1, 21), {date(2011, 1, 21)})["state"] == "not_next_day_after_nominal_third_wednesday"


@pytest.mark.parametrize("contract", ["201113", "000001", "201111W4", "", None])
def test_malformed_or_weekly_label_is_not_assumed_monthly(contract):
    assert classify(contract, date(2011, 1, 20), set())["state"] == "unsupported_contract_label"


def test_real_parquet_shape_preserves_input_provenance_and_candidate_status(tmp_path, monkeypatch):
    data = tmp_path / "data_fixture"
    data.mkdir()
    report = tmp_path / "artifacts/markets/monthly/report.json"
    report.parent.mkdir(parents=True)
    manual = data / "manual.pdf"
    manual.write_bytes(b"%PDF-synthetic")
    monkeypatch.setattr(audit, "MANUAL_SHA256", sha256_file(manual))
    monkeypatch.setattr(audit, "capacity_check", lambda _: {"state": "test"})
    receipt = data / "manual-receipt.json"
    receipt.write_text(json.dumps({"sha256": sha256_file(manual), "url": audit.MANUAL_URL,
                                   "historical_published_at": None}))
    warm, daily, obs, checkpoint = (data / name for name in ("warm.parquet", "daily.parquet", "obs.parquet", "checkpoint.json"))
    common = {"asset_class": "stock_future", "series_type": "monthly", "product": "ABC"}
    pl.DataFrame([{**common, "date": date(2011, 1, 20), "first_observed_date": date(2011, 1, 20),
                   "physical_contract": "ABC:201112", "contract": "201112", "outright_volume": 5,
                   "official_expiry": date(2011, 12, 21), "corporate_boundary": None,
                   "corporate_identity_candidate_only": True}]).write_parquet(warm)
    pl.DataFrame([{**common, "date": date(2011, 1, 19), "contract": "201101",
                   "official_expiry": date(2011, 1, 19), "official_row_observed": True}]).write_parquet(daily)
    pl.DataFrame([{**common, "date": date(2011, 1, 20), "physical_contract": "ABC:201112",
                   "source_status": "raw_receipt_matched", "0845_raw_print_rows": 1,
                   "0845_invalid_raw_rows": 0, "0900_raw_print_rows": 0,
                   "0900_invalid_raw_rows": 0}]).write_parquet(obs)
    checkpoint.write_text(json.dumps({"identity": {"objects": [
        {"date": "2011-01-19", "rows": 1}, {"date": "2011-01-20", "rows": 1}]}}))
    daily_meta, obs_meta = data / "daily.json", data / "obs.json"
    daily_meta.write_text(json.dumps({"outputs": {"continuous_daily": {"sha256": sha256_file(daily)}}}))
    obs_meta.write_text(json.dumps({"contract": "tw_futures_omitted_first_day_windows_v1", "training_ready": False,
        "data_path": str(obs), "data_sha256": sha256_file(obs),
        "input_sha256": {str(warm): sha256_file(warm), str(checkpoint): sha256_file(checkpoint)}}))
    output = data / "output"
    result = audit.build(tmp_path, warm, daily, daily_meta, obs_meta, checkpoint, manual, receipt, output, report)
    assert result["training_ready"] is False
    assert result["manual_historical_published_at"] is None
    assert result["candidate_rows"] == result["manual_schedule_matches"]["matching_contract_days"] == 1
    assert result["manual_schedule_matches"]["prior_month_recorded_expiry_matches"] == 1
    assert result["manual_schedule_matches"]["0845_positive_raw_window"] == 1
    assert result["manual_schedule_matches"]["0900_positive_raw_window"] == 0
    assert sha256_file(Path(result["data_path"])) == result["data_sha256"]
    assert json.loads(report.read_text())["inputs_sha256"][str(obs)] == sha256_file(obs)

    daily.write_bytes(b"changed")
    with pytest.raises(ValueError, match="daily source differs"):
        audit.build(tmp_path, warm, daily, daily_meta, obs_meta, checkpoint, manual, receipt,
                    data / "new-output", report.parent / "new-report.json")

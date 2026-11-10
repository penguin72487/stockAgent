from datetime import datetime, timedelta
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from stockagent.live.taifex_settlement_reference import SettlementReference
import stockagent.live.taifex_settlement_reference as settlement_reference
from stockagent.live.public_dashboards import sanitize_taifex_status


def test_observed_samples_are_deduplicated_and_never_claim_complete(tmp_path: Path) -> None:
    reference = SettlementReference(tmp_path / "missing.parquet")
    now = datetime(2026, 11, 4, 13, 1, tzinfo=ZoneInfo("Asia/Taipei"))
    def observation(clock: str, value: str) -> dict:
        return {"msgArray": [{"ch": "t00.tw", "d": "20261104", "t": clock, "z": value}]}

    reference.observe(observation("13:00:00", "100"), now)
    reference.observe(observation("13:00:05", "100"), now)
    reference.observe(observation("13:00:05", "100"), now)
    reference.observe(observation("13:00:10", "101"), now)
    snapshot = reference.snapshot(now)
    assert snapshot["sample_count"] == 2
    assert snapshot["partial_average_price"] == 100.5
    assert snapshot["partial_estimate_price"] == 101
    assert snapshot["complete_tape_verified"] is False
    assert snapshot["index_state"] == "stale_or_unavailable"


def test_public_gateway_keeps_settlement_prices_private_without_grant() -> None:
    payload = {"simulation_only": True, "production_order_possible": False,
               "settlement_reference": {"index_price": 42000.0,
                                        "partial_estimate_price": 42001}}
    public = sanitize_taifex_status(payload)
    private = sanitize_taifex_status(payload, private_prices=True)
    assert public["settlement_reference"]["index_price"] is None
    assert public["settlement_reference"]["partial_estimate_price"] is None
    assert private["settlement_reference"]["index_price"] == 42000.0


def test_close_is_added_once_and_only_after_complete_official_report(tmp_path: Path) -> None:
    reference = SettlementReference(tmp_path / "missing.parquet")
    tz = ZoneInfo("Asia/Taipei")
    start = datetime(2026, 11, 4, 13, 0, tzinfo=tz)
    rows = [[(start + timedelta(seconds=5 * i)).strftime("%H:%M:%S"), "100"]
            for i in range(1, 301)]
    rows.append(["13:33:00", "401"])
    payload = {"stat": "OK", "date": "20261104", "fields": ["時間", "發行量加權股價指數"], "data": rows}
    now = datetime(2026, 11, 4, 13, 36, tzinfo=tz)
    reference.observe_close_report(payload, now)
    reference.observe_close_report(payload, now)
    snapshot = reference.snapshot(now)
    assert snapshot["sample_count"] == 300
    assert snapshot["average_sample_count"] == 301
    assert snapshot["closing_index_price"] == 401
    assert snapshot["closing_index_at"].startswith("2026-11-04T13:33:00")
    assert snapshot["partial_average_price"] == 101
    assert snapshot["partial_estimate_price"] == 101
    assert snapshot["closing_index_included"] is True
    assert snapshot["complete_tape_verified"] is False


def test_close_report_rejects_missing_grid_and_future_or_stale_data(tmp_path: Path) -> None:
    reference = SettlementReference(tmp_path / "missing.parquet")
    now = datetime(2026, 11, 4, 13, 36, tzinfo=ZoneInfo("Asia/Taipei"))
    payload = {"stat": "OK", "date": "20261104", "fields": ["時間", "發行量加權股價指數"],
               "data": [["13:00:05", "100"], ["13:30:00", "101"]]}
    with pytest.raises(ValueError, match="complete regular"):
        reference.observe_close_report(payload, now)
    payload["date"] = "20261103"
    with pytest.raises(ValueError, match="date differs"):
        reference.observe_close_report(payload, now)
    assert reference.snapshot(now)["closing_index_included"] is False


def test_restart_preserves_only_same_day_partial_summary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tz = ZoneInfo("Asia/Taipei")
    now = datetime(2026, 11, 4, 13, 40, tzinfo=tz)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.astimezone(tz) if tz else now.replace(tzinfo=None)

    monkeypatch.setattr(settlement_reference, "datetime", FixedDateTime)
    path = tmp_path / "saved.json"
    path.write_text(json.dumps({"sample_first_at": "2026-11-04T13:00:05+08:00",
                                "sample_last_at": "2026-11-04T13:24:00+08:00",
                                "sample_count": 24, "partial_average_price": 42967.0775}))
    reference = SettlementReference(tmp_path / "missing.parquet", path)
    snapshot = reference.snapshot(now)
    assert snapshot["sample_count"] == 24
    assert snapshot["partial_average_price"] == 42967.0775
    assert snapshot["observation_source"] == "retained_partial_summary"
    assert snapshot["closing_index_included"] is False
    assert reference.snapshot(now + timedelta(days=1))["sample_count"] == 0

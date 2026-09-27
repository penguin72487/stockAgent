from datetime import UTC, datetime, timedelta
import json

from stockagent.live import data_monitor_dashboard as monitor


NOW = datetime(2026, 9, 27, 11, tzinfo=UTC)


def project(tmp_path, item=None):
    registry = tmp_path / "configs/free_public_data_sources.json"
    registry.parent.mkdir(parents=True, exist_ok=True)
    registry.write_text(json.dumps({"sources": [{"id": "keyed_public_catalogs"}]}))
    if item is not None:
        path = tmp_path / "data_keyed_public_catalogs/download_summary.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"providers": [item]}))
    return {row["id"].split(":")[1]: row for row in monitor._keyed_public_catalog_sources(tmp_path, now=NOW)}


def test_every_catalog_stays_visible_before_first_request(tmp_path):
    rows = project(tmp_path)
    assert len(rows) == 9
    assert rows["noaa_cdo"]["status"] == "waiting"
    assert rows["airnow"]["status"] == "deferred"
    assert rows["api_data_gov"]["automation_eligible"] is False
    assert monitor._profile_for_row(rows["airnow"])["service_keys"] == ()
    assert monitor._profile_for_row(rows["api_data_gov"])["mode"] == "not_configured"


def test_advertised_catalog_history_is_never_local_observation_history(tmp_path):
    row = project(tmp_path, {"provider": "noaa_cdo", "status": "acquired", "rows": 11,
        "observed_at_utc": NOW.isoformat(), "catalog_complete": True, "advertised_history_start": "1750-01-01",
        "advertised_history_end": "2026-09-26", "unexpected_secret": "SECRET"})["noaa_cdo"]
    assert row["status"] == "current"
    assert row["data_through"] is None
    assert row["record_stats"]["first"] is None
    assert row["record_stats"]["count"] == 11
    assert row["coverage"]["unit"] == "本輪目錄／快照請求"
    assert row["public_catalog"]["history_complete"] is False
    assert row["eta"]["remaining_seconds"] is None
    assert "SECRET" not in json.dumps(row)
    assert monitor._record_stats_for_row(row, {})["count"] == 11


def test_truncated_catalog_never_claims_discovery_complete(tmp_path):
    row = project(tmp_path, {"provider": "noaa_cdo", "status": "acquired", "rows": 1000,
        "observed_at_utc": NOW.isoformat(), "catalog_complete": False})["noaa_cdo"]
    assert row["status"] == "degraded"
    assert row["public_catalog"]["bounded_capture_complete"] is False
    assert row["coverage"] is None


def test_snapshot_dates_and_refresh_schedule_are_separate(tmp_path):
    row = project(tmp_path, {"provider": "cwa", "status": "acquired", "rows": 500,
        "observed_at_utc": NOW.isoformat(), "source_event_start": "2026-09-27T18:00:00+08:00",
        "source_event_end": "2026-09-27T18:10:00+08:00"})["cwa"]
    assert row["record_stats"]["last"] == "2026-09-27T18:10:00+08:00"
    assert row["public_catalog"]["history_complete"] is False
    assert monitor._profile_for_row(row)["service_keys"] == ("keyed_public_catalogs",)


def test_error_and_future_clock_never_go_green(tmp_path):
    assert project(tmp_path, {"provider": "bea", "status": "http_403"})["bea"]["status"] == "degraded"
    row = project(tmp_path, {"provider": "noaa_cdo", "status": "acquired",
        "observed_at_utc": (NOW + timedelta(days=1)).isoformat()})["noaa_cdo"]
    assert row["status"] == "degraded"


def test_unknown_credential_not_mislabelled_missing(tmp_path):
    path = tmp_path / "artifacts/data_credentials/status.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"providers": [{"id": "finlab", "state": "unknown"}]}))
    row = monitor._credential_registry_sources(tmp_path, now=NOW)[0]
    assert row["status"] == "degraded"
    assert "無法驗證" in row["status_label"]
    assert row["parent_id"] == "group:finlab-research"


def test_partial_run_and_failed_refresh_preserve_previous_inventory(tmp_path):
    path = tmp_path / "data_keyed_public_catalogs/receipts/cwa.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"provider": "cwa", "dataset": "O-A0001-001", "status": "acquired",
        "rows": 500, "observed_at_utc": (NOW - timedelta(minutes=5)).isoformat(),
        "source_event_start": "2026-09-27T18:00:00+08:00", "source_event_end": "2026-09-27T18:00:00+08:00"}))
    row = project(tmp_path, {"provider": "bea", "status": "http_403"})["cwa"]
    assert row["status"] == "current"
    assert row["record_stats"]["count"] == 500
    row = project(tmp_path, {"provider": "cwa", "status": "http_429", "checked_at_utc": NOW.isoformat()})["cwa"]
    assert row["status"] == "degraded"
    assert row["record_stats"]["count"] == 500
    assert row["record_stats"]["first"] == "2026-09-27T18:00:00+08:00"


def test_malformed_json_structure_does_not_crash_whole_monitor(tmp_path):
    project(tmp_path)
    path = tmp_path / "data_keyed_public_catalogs/download_summary.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    for payload in ([], {"providers": 1}, {"providers": None}):
        path.write_text(json.dumps(payload))
        rows = monitor._keyed_public_catalog_sources(tmp_path, now=NOW)
        assert len(rows) == 9
        assert next(row for row in rows if row["provider"] == "cwa")["status"] == "degraded"

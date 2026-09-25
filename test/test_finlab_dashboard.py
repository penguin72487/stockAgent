from __future__ import annotations

from datetime import UTC, datetime, timedelta
import hashlib
import json
import sqlite3

import pytest

from scripts.snapshot_finlab_quota import daily_reset_evidence, load_quota_history, persist_observation, quota_observation
from scripts.download_finlab_history import fetch_one, safe_stem
from scripts.finlab_release_gate import catalog_readiness
from stockagent.live.finlab_dashboard import _public_quota_series, _volume_estimate, build_finlab_public_status


def _write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def test_quota_observation_uses_account_data_limit_and_official_reset_rule():
    at = datetime.fromisoformat("2026-09-23T23:59:00+00:00")
    row = quota_observation({"quota": 4720.002, "limit_size": 5000}, now=at)
    assert row["used_mb"] == 4720.002
    assert row["remaining_mb"] == 279.998
    assert row["reset_at_utc"] == "2026-09-24T00:00:00+00:00"
    assert row["used_ratio"] == pytest.approx(4720.002 / 5000)
    with pytest.raises(ValueError):
        quota_observation({"quota": -1, "limit_size": 5000}, now=at)


def test_quota_history_keeps_missing_intervals_missing(tmp_path):
    at = datetime.fromisoformat("2026-09-23T12:00:00+00:00")
    row = quota_observation({"quota": 100, "limit_size": 5000}, now=at)
    assert persist_observation(tmp_path, row) == 1
    later = quota_observation({"quota": 200, "limit_size": 5000}, now=datetime.fromisoformat("2026-09-23T13:00:00+00:00"))
    assert persist_observation(tmp_path, later) == 2
    assert len(load_quota_history(tmp_path, now=datetime.fromisoformat("2026-09-23T13:00:00+00:00"))) == 2
    assert (tmp_path / "quota_history.sqlite3").is_file()


def test_minute_quota_history_migrates_legacy_without_rewriting_it(tmp_path):
    previous = quota_observation(
        {"quota": 200, "limit_size": 5000},
        now=datetime.fromisoformat("2026-09-23T12:00:00+00:00"),
    )
    _write(tmp_path / "quota_history.json", [previous])
    original = (tmp_path / "quota_history.json").read_bytes()
    current = quota_observation(
        {"quota": 201, "limit_size": 5000},
        now=datetime.fromisoformat("2026-09-23T12:01:00+00:00"),
    )
    assert persist_observation(tmp_path, current) == 2
    assert persist_observation(tmp_path, current) == 2
    assert (tmp_path / "quota_history.json").read_bytes() == original
    assert [row["used_mb"] for row in load_quota_history(tmp_path, now=datetime.fromisoformat("2026-09-23T12:01:00+00:00"))] == [200, 201]
    with sqlite3.connect(f"{(tmp_path / 'quota_history.sqlite3').as_uri()}?mode=ro", uri=True) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "delete"


def test_public_quota_history_keeps_full_month_and_all_recent_minutes():
    now = datetime.fromisoformat("2026-09-24T03:00:00+00:00")
    history = [
        {"observed_at_utc": (now - timedelta(days=30) + timedelta(minutes=index)).isoformat(),
         "used_mb": index % 5000, "limit_mb": 5000}
        for index in range(30 * 24 * 60 + 1)
    ]
    public = _public_quota_series(history, now)
    assert len(public) <= 4000
    assert datetime.fromisoformat(public[0]["observed_at_utc"]) <= now - timedelta(days=30) + timedelta(minutes=20)
    assert public[-1]["observed_at_utc"] == now.isoformat()
    assert sum(datetime.fromisoformat(row["observed_at_utc"]) >= now - timedelta(days=1) for row in public) == 1441


def test_daily_quota_reset_requires_post_0800_account_evidence():
    before = quota_observation(
        {"quota": 4720, "limit_size": 5000},
        now=datetime.fromisoformat("2026-09-23T23:55:00+00:00"),
    )
    early = quota_observation(
        {"quota": 0, "limit_size": 5000},
        now=datetime.fromisoformat("2026-09-23T23:59:59+00:00"),
    )
    stale = quota_observation(
        {"quota": 4720, "limit_size": 5000},
        now=datetime.fromisoformat("2026-09-24T00:00:05+00:00"),
    )
    reset = quota_observation(
        {"quota": 12, "limit_size": 5000},
        now=datetime.fromisoformat("2026-09-24T00:00:15+00:00"),
    )
    assert daily_reset_evidence(early, [before]) is None
    assert daily_reset_evidence(stale, [before]) is None
    assert daily_reset_evidence(reset, [before]) == "observed_quota_drop_across_08"
    assert daily_reset_evidence(reset, []) is None
    assert daily_reset_evidence({**reset, "used_mb": 0, "remaining_mb": 5000}, []) == "full_quota_observed_after_08"


def test_public_projection_is_read_only_sanitized_and_does_not_infer_quota(tmp_path):
    now = datetime.fromisoformat("2026-09-23T12:00:00+00:00")
    _write(tmp_path / "artifacts/live/data_monitor/public_status.json", {
        "read_only": True, "production_control_possible": False,
        "generated_at_utc": now.isoformat(),
        "finlab_acquisition": {"downloaded": 1, "catalog_total": 2, "state": "running", "api_key": "SECRET"},
        "sources": [
            {"id": "finlab:price:收盤價", "category": "price", "finlab_acquisition_state": "downloaded",
             "record_stats": {"count": 100, "first": "2007-01-01", "last": "2026-09-23"}, "api_key": "SECRET"},
            {"id": "finlab:monthly_revenue:當月營收", "finlab_acquisition_state": "pending"},
        ],
    })
    _write(tmp_path / "artifacts/live/finlab/quota_latest.json", {
        "schema_version": 1, "observed_at_utc": now.isoformat(), "used_mb": 1000,
        "limit_mb": 5000, "remaining_mb": 4000, "used_ratio": .2,
        "reset_at_utc": "2026-09-24T00:00:00+00:00", "token": "SECRET",
    })
    payload = build_finlab_public_status(tmp_path, now=now, staged_root=tmp_path / "stage")
    assert payload["read_only"] is True
    assert payload["quota"]["used_mb"] == 1000
    assert payload["quota_definition"]["request_per_second_limit"] is None
    assert len(payload["datasets"]) == 2
    assert payload["volume_estimate"]["estimated_total_bytes"] is None
    assert "SECRET" not in json.dumps(payload)
    assert payload["training"]["state"] == "not_built"


def test_public_page_lists_unacquired_first_with_bounded_reason(tmp_path):
    now = datetime.fromisoformat("2026-09-24T15:00:00+00:00")
    _write(tmp_path / "artifacts/live/data_monitor/public_status.json", {
        "read_only": True, "production_control_possible": False,
        "generated_at_utc": now.isoformat(),
        "finlab_acquisition": {
            "catalog_total": 3, "downloaded": 1, "not_downloaded": 2,
            "not_downloaded_by_reason": {"provider_empty": 1, "deferred_windowed": 1},
            "api_key": "SECRET",
        },
        "sources": [
            {"id": "finlab:price:收盤價", "category": "price", "finlab_acquisition_state": "downloaded"},
            {"id": "finlab:dividend_otc:權息", "category": "dividend_otc", "finlab_acquisition_state": "provider_empty",
             "finlab_attempt_status": "provider_empty", "finlab_provider_rows": 2395,
             "finlab_provider_fields": 1153, "finlab_last_attempt_at_utc": now.isoformat(),
             "finlab_next_retry_at_utc": (now + timedelta(days=1)).isoformat(), "exception_message": "SECRET"},
            {"id": "finlab:tw_minute:2330", "category": "tw_minute", "finlab_acquisition_state": "deferred_windowed",
             "finlab_deferred_reason": "requires_date_window"},
        ],
    })
    payload = build_finlab_public_status(tmp_path, now=now, staged_root=tmp_path / "stage")
    assert [row["key"] for row in payload["datasets"]] == [
        "dividend_otc:權息", "tw_minute:2330", "price:收盤價",
    ]
    assert payload["datasets"][0]["next_retry_at_utc"] == (now + timedelta(days=1)).isoformat()
    assert payload["datasets"][0]["provider_rows"] == 2395
    assert payload["acquisition"]["not_downloaded_by_reason"]["deferred_windowed"] == 1
    assert "SECRET" not in json.dumps(payload)


def test_volume_forecast_keeps_measured_bytes_separate_from_estimates():
    datasets = [
        {"key": f"price:{index}", "category": "price", "state": "downloaded", "local_bytes": size}
        for index, size in enumerate((10, 20, 30))
    ]
    datasets.extend([
        {"key": "price:pending", "category": "price", "state": "pending", "local_bytes": None},
        {"key": "other:pending", "category": "other", "state": "pending", "local_bytes": None},
        {"key": "broken:receipt", "category": "other", "state": "downloaded", "local_bytes": None},
    ])
    estimate = _volume_estimate(datasets, len(datasets))
    assert estimate["catalog_verified"] is True
    assert estimate["measured_bytes"] == 60
    assert estimate["measured_files"] == 3
    assert estimate["unmeasured_keys"] == 3
    assert estimate["same_category_estimated_keys"] == 1
    assert estimate["global_estimated_keys"] == 2
    assert estimate["estimated_remaining_bytes"] == 60
    assert estimate["estimated_total_bytes"] == 120
    assert estimate["high_scenario_total_bytes"] == 150
    assert estimate["estimated_coverage_ratio"] == .5
    assert _volume_estimate([{"key": "unknown", "state": "pending"}], 1)["estimated_total_bytes"] is None
    inconsistent = _volume_estimate(datasets, len(datasets) + 1)
    assert inconsistent["catalog_verified"] is False
    assert inconsistent["measured_bytes"] == 60
    assert inconsistent["estimated_total_bytes"] is None


def test_public_projection_rejects_untrusted_monitor(tmp_path):
    _write(tmp_path / "artifacts/live/data_monitor/public_status.json", {
        "read_only": False, "production_control_possible": True,
    })
    with pytest.raises(ValueError, match="untrusted"):
        build_finlab_public_status(tmp_path, now=datetime.now(UTC))


def test_training_cold_badge_requires_exact_current_staged_receipt(tmp_path):
    at = datetime.fromisoformat("2026-09-23T12:00:00+00:00")
    stage = tmp_path / "stage"
    staged = {
        "status": "complete", "research_only": True, "feature_rows": 10,
        "model_channels": 636, "base_feature_sha256": "base-a", "end_date": "2026-09-23",
    }
    _write(stage / "release_receipt.json", staged)
    digest = hashlib.sha256((stage / "release_receipt.json").read_bytes()).hexdigest()
    _write(tmp_path / "artifacts/live/finlab/training_delivery.json", {
        "dataset": "tw-public-research-finlab-2014-v4", "cold_objects_verified": True,
        "feature_rows": 10, "model_channels": 636, "base_feature_sha256": "base-a",
        "staged_receipt_sha256": digest, "snapshot_id": "release-1",
    })
    current = build_finlab_public_status(tmp_path, now=at, staged_root=stage)["training"]
    assert current["cold_publication"] == "verified_exact_release"
    staged["end_date"] = "2026-09-24"
    _write(stage / "release_receipt.json", staged)
    changed = build_finlab_public_status(tmp_path, now=at, staged_root=stage)["training"]
    assert changed["cold_publication"] == "not_verified"


def test_finlab_packaging_requires_every_recent_key_and_verified_bytes(tmp_path, monkeypatch):
    import pandas as pd
    at = datetime.now(UTC)
    root = tmp_path / "data_finlab"
    _write(root / "catalog/discovery.json", {"keys": ["price:a", "price:b"], "observed_at_utc": at.isoformat()})
    monkeypatch.setattr("finlab.data.get", lambda *_args, **_kwargs: pd.DataFrame({"2330": [1.]}, index=[at.date()]))
    fetch_one("price:a", root, refresh=True)
    partial = catalog_readiness(root, now=at)
    assert partial["missing"] == 1 and not partial["ready"]
    fetch_one("price:b", root, refresh=True)
    complete = catalog_readiness(root, now=datetime.now(UTC), verify_hashes=True)
    assert complete["ready"] and complete["hashes_verified"]
    fetch_one("price:a", root, refresh=True)
    rechecked = catalog_readiness(root, now=datetime.now(UTC))
    assert rechecked["source_receipts_sha256"] == complete["source_receipts_sha256"]
    _write(root / "catalog/discovery.json", {
        "keys": ["price:a", "price:b"], "observed_at_utc": "2026-01-01T00:00:00+00:00",
    })
    assert not catalog_readiness(root, now=datetime.now(UTC))["catalog_fresh"]
    _write(root / "catalog/discovery.json", {
        "keys": ["price:a", "price:b"], "observed_at_utc": datetime.now(UTC).isoformat(),
    })
    receipt_path = root / "receipts" / f"{safe_stem('price:a')}.json"
    receipt = json.loads(receipt_path.read_text())
    receipt["source_checked_at_utc"] = "2026-01-01T00:00:00+00:00"
    _write(receipt_path, receipt)
    assert catalog_readiness(root, now=datetime.now(UTC))["stale"] == 1
    receipt["source_checked_at_utc"] = datetime.now(UTC).isoformat()
    _write(receipt_path, receipt)
    with (root / receipt["parquet_path"]).open("ab") as handle:
        handle.write(b"corrupt")
    invalid = catalog_readiness(root, now=datetime.now(UTC), verify_hashes=True)
    assert not invalid["ready"] and invalid["invalid"] == 1


def test_per_key_eta_uses_measured_fetches_and_never_invents_queue_finish(tmp_path):
    at = datetime.now(UTC)
    keys = ["price:a", "price:b", "price:c", "price:d"]
    rows = []
    for key in keys:
        rows.append({"id": f"finlab:{key}", "category": "price",
                     "finlab_acquisition_state": "pending", "record_stats": {}})
    _write(tmp_path / "artifacts/live/data_monitor/public_status.json", {
        "read_only": True, "production_control_possible": False,
        "generated_at_utc": at.isoformat(), "sources": rows,
    })
    for row in rows[:3]:
        row["finlab_acquisition_state"] = "downloaded"
    _write(tmp_path / "artifacts/live/data_monitor/public_status.json", {
        "read_only": True, "production_control_possible": False,
        "generated_at_utc": at.isoformat(), "sources": rows,
    })
    for key, seconds in zip(keys[:3], [10, 20, 50]):
        _write(tmp_path / "data_finlab/receipts" / f"{safe_stem(key)}.json", {
            "dataset": key, "status": "downloaded_unverified_for_pit",
            "last_fetch_elapsed_seconds": seconds,
            "source_checked_at_utc": at.isoformat(),
        })
    payload = build_finlab_public_status(tmp_path, now=at, staged_root=tmp_path / "stage")
    by_key = {row["key"]: row for row in payload["datasets"]}
    assert by_key["price:d"]["estimated_fetch_seconds_low"] == 20
    assert by_key["price:d"]["estimated_fetch_seconds_high"] == 50
    assert by_key["price:d"]["estimate_basis"] == "same_category_measured"
    assert by_key["price:d"]["estimated_finish_at_utc"] is None

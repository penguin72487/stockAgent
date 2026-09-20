from __future__ import annotations

from datetime import UTC, date, datetime
import json
import os
from pathlib import Path

import pytest
from scripts import snapshot_data_refresh_services as snapshot_service

from stockagent.live.data_monitor_dashboard import (
    _apply_verified_storage_freshness,
    _market_category,
    _record_stats_for_row,
    build_data_monitor_feature_inventory,
)
from stockagent.live.data_monitor_inventory import build_feature_inventory, build_record_inventory, parquet_footer_stats


def _write_parquet(path: Path, dates: list[date]) -> None:
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table({"date": pa.array(dates, type=pa.date32()), "value": list(range(len(dates)))}),
        path,
        row_group_size=2,
    )


def test_parquet_footer_proves_actual_bounds_not_requested_start(tmp_path: Path) -> None:
    path = tmp_path / "test.parquet"
    _write_parquet(path, [date(2020, 5, 3), date(2020, 5, 1), date(2020, 5, 4)])
    stats = parquet_footer_stats(path)
    assert stats is not None
    assert stats["count"] == 3
    assert stats["first"] == "2020-05-01"
    assert stats["last"] == "2020-05-04"
    assert stats["time_column"] == "date"


def test_microstructure_unix_nanoseconds_are_reported_in_utc(tmp_path: Path) -> None:
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    path = tmp_path / "microstructure.parquet"
    pq.write_table(pa.table({"snapshot_ts_ns": [1_785_200_400_000_000_000]}), path)
    stats = parquet_footer_stats(path)
    assert stats is not None
    assert stats["count"] == 1
    assert stats["first"] == "2026-07-28T01:00:00+00:00"
    assert stats["time_column"] == "snapshot_ts_ns"


def test_inventory_is_bounded_and_never_reports_partial_sum_as_total(tmp_path: Path) -> None:
    base = tmp_path / "data_tw_public"
    base.mkdir()
    (base / "dataset_manifest.json").write_text(
        json.dumps([{"name": "twse_one", "start_date": "1900-01-01"}, {"name": "twse_two"}]),
        encoding="utf-8",
    )
    _write_parquet(base / "twse_one.parquet", [date(2023, 1, 2), date(2023, 1, 3)])
    _write_parquet(base / "twse_two.parquet", [date(2024, 2, 1)])
    first = build_record_inventory(tmp_path, refresh=True, max_refresh_files=1)
    assert first["datasets"]["tw-public:twse_one"]["count"] == 2
    assert first["datasets"]["tw-public:twse_one"]["first"] == "2023-01-02"
    assert first["datasets"]["tw-public:twse_two"]["count"] is None
    assert first["datasets"]["group:tw-public"]["count"] is None
    second = build_record_inventory(tmp_path, refresh=True, max_refresh_files=1)
    assert second["datasets"]["group:tw-public"]["count"] is None  # absent archive files
    assert second["datasets"]["tw-public:twse_two"]["count"] == 1
    cached = build_record_inventory(tmp_path)
    assert cached["refreshed_files"] == 0
    assert cached["datasets"]["tw-public:twse_two"]["last"] == "2024-02-01"
    _write_parquet(base / "twse_one.parquet", [date(2025, 1, 1)])
    stale = build_record_inventory(tmp_path)
    assert stale["datasets"]["tw-public:twse_one"]["count"] == 2  # snapshot is read-only
    fresh = build_record_inventory(tmp_path, refresh=True, max_refresh_files=1)
    assert fresh["datasets"]["tw-public:twse_one"]["count"] == 1


def test_inventory_groups_crypto_and_yahoo_by_market(tmp_path: Path) -> None:
    _write_parquet(tmp_path / "data_okx/1m/BTC_features.parquet", [date(2020, 1, 1)])
    _write_parquet(tmp_path / "data_yahoo/tw_stocks/2330_features.parquet", [date(2021, 1, 1)])
    _write_parquet(tmp_path / "data_yahoo/crypto/BTC_features.parquet", [date(2022, 1, 1)])
    _write_parquet(tmp_path / "data_forex_frankfurter/USDTWD_features.parquet", [date(2023, 1, 1)])
    _write_parquet(tmp_path / "data_coinmetrics_community/assets/btc_features.parquet", [date(2024, 1, 1)])
    _write_parquet(tmp_path / "data_tw_microstructure/hft_dataset/trade_date=2025-01-01/data.parquet", [date(2025, 1, 1)])
    inventory = build_record_inventory(tmp_path, refresh=True, max_refresh_files=10)["datasets"]
    assert inventory["group:okx"]["count"] == 1
    assert inventory["yahoo:tw_stocks"]["first"] == "2021-01-01"
    assert inventory["yahoo:crypto"]["last"] == "2022-01-01"
    assert inventory["group:yahoo-market"]["count"] is None  # absent other partitions
    assert inventory["group:forex-frankfurter"]["count"] == 1
    assert inventory["group:coinmetrics-community"]["count"] == 1
    assert inventory["group:tw-microstructure-train"]["count"] == 1
    assert _market_category({"id": "tw-public:twse_daily_ohlcv"}) == "taiwan_equity"
    assert _market_category({"id": "tw-public:dgbas_cpi_basic"}) == "macro"
    assert _market_category({"id": "inventory:yahoo:crypto"}) == "crypto"
    assert _market_category({"id": "group:okx"}) == "crypto"
    assert _record_stats_for_row({"id": "product:okx_perpetual_swaps:1m", "granularity": "1m"}, inventory)["count"] == 1
    assert _record_stats_for_row({"id": "unknown"}, inventory)["count"] is None
    assert _record_stats_for_row({"id": "credential:test", "scope": "credential_gate"}, inventory)["state"] == "not_applicable"
    assert "重疊" in _record_stats_for_row({"id": "group:tw-minute-source-cold"}, inventory)["basis"]


def test_crypto_hot_tail_is_listed_separately_without_double_counting(tmp_path: Path) -> None:
    _write_parquet(tmp_path / "data_okx/1m/BTC_features.parquet", [date(2026, 9, 1)])
    _write_parquet(
        tmp_path / "data_okx/1m/_hot_tail/BTC_features.parquet",
        [date(2026, 9, 1), date(2026, 9, 2)],
    )
    inventory = build_record_inventory(tmp_path, refresh=True, max_refresh_files=10)["datasets"]
    assert inventory["group:okx"]["count"] == 1
    assert inventory["physical:okx:hot-tail"]["count"] == 2
    assert inventory["physical:okx:hot-tail"]["last"] == "2026-09-02"


def test_physical_inventory_keeps_derived_views_out_of_group_total(tmp_path: Path) -> None:
    _write_parquet(
        tmp_path / "data_tw_index_futures/all_futures_daily_sessions.parquet",
        [date(2020, 1, 1), date(2020, 1, 2)],
    )
    _write_parquet(
        tmp_path / "data_tw_index_futures/day_session_front_month.parquet",
        [date(2020, 1, 2)],
    )
    _write_parquet(
        tmp_path / "data_free_public/observations.parquet",
        [date(2021, 1, 1)],
    )
    inventory = build_record_inventory(tmp_path, refresh=True, max_refresh_files=8)["datasets"]
    assert inventory["physical:tw-futures:all-contracts"]["count"] == 2
    assert inventory["physical:tw-futures:front-month"]["count"] == 1
    assert inventory["group:tw-index-futures"]["count"] == 2
    assert inventory["group:free-public-context"]["count"] == 1
    assert _record_stats_for_row({"id": "product:tw_index_futures:daily"}, inventory)["count"] == 2
    assert _record_stats_for_row(
        {"id": "inventory:tw-futures:front-month", "record_inventory_key": "physical:tw-futures:front-month"},
        inventory,
    )["count"] == 1


def test_supported_event_date_alias_proves_bounds(tmp_path: Path) -> None:
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    path = tmp_path / "observations.parquet"
    pq.write_table(pa.table({"event_ts_utc": ["2024-01-03T00:00:00Z", "2024-01-01T00:00:00Z"]}), path)
    stats = parquet_footer_stats(path)
    assert stats is not None
    assert stats["first"] == "2024-01-01T00:00:00+00:00"
    assert stats["last"] == "2024-01-03T00:00:00+00:00"


def test_macro_subject_and_publication_dates_have_footer_bounds(tmp_path: Path) -> None:
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    daily = tmp_path / "daily.parquet"
    pq.write_table(pa.table({"subject_date": ["2002-05-03", "2002-05-02"]}), daily)
    assert parquet_footer_stats(daily)["first"] == "2002-05-02"
    provisional = tmp_path / "provisional.parquet"
    pq.write_table(pa.table({"published_at_taipei": [
        "2024-05-10T16:00:00", "2024-05-09T16:00:00"
    ]}), provisional)
    assert parquet_footer_stats(provisional)["last"] == "2024-05-10T16:00:00"


def test_invalid_parquet_is_visible_and_does_not_trigger_endless_retries(tmp_path: Path) -> None:
    directory = tmp_path / "data_yahoo/crypto"
    directory.mkdir(parents=True)
    (directory / "bad_features.parquet").write_bytes(b"not a parquet footer")
    first = build_record_inventory(tmp_path, refresh=True, max_refresh_files=10)
    stats = first["datasets"]["yahoo:crypto"]
    assert stats["state"] == "invalid"
    assert stats["count"] is None
    assert stats["verified_partial_count"] is None
    assert stats["invalid_files"] == 1
    second = build_record_inventory(tmp_path, refresh=True, max_refresh_files=10)
    assert second["refreshed_files"] == 0
    _write_parquet(directory / "bad_features.parquet", [date(2025, 1, 1)])
    repaired = build_record_inventory(tmp_path, refresh=True, max_refresh_files=10)
    assert repaired["datasets"]["yahoo:crypto"]["count"] == 1
    assert repaired["datasets"]["yahoo:crypto"]["invalid_files"] == 0


def test_feature_inventory_lists_every_field_by_source_without_doubling_rollups(tmp_path: Path) -> None:
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    tw = tmp_path / "data_tw_public/features/tw_public_stock_daily.parquet"
    tw.parent.mkdir(parents=True)
    pq.write_table(pa.table({
        "date": pa.array([date(2024, 1, 2), date(2024, 1, 3)], type=pa.date32()),
        "twpub_value": pa.array([1.0, None]),
    }), tw)
    crypto = tmp_path / "data_okx/1m/BTC_features.parquet"
    crypto.parent.mkdir(parents=True)
    pq.write_table(pa.table({
        "date": pa.array([date(2024, 1, 4)], type=pa.date32()),
        "close": [42.0],
    }), crypto)
    stored = build_record_inventory(tmp_path, refresh=True, max_refresh_files=10)["datasets"]
    assert stored["physical:tw-public:training-features"]["count"] == 2
    assert stored["group:tw-public"]["count"] is None  # Derived feature table is not a raw-group total.
    raw = build_feature_inventory(tmp_path)
    assert raw["files_with_schema"] == raw["files_total"] == 2
    field = next(row for row in raw["rows"] if row["field"] == "twpub_value")
    assert field["non_null_count"] == 1
    assert field["rows_with_field"] == 2
    assert field["dataset_first"] == "2024-01-02"  # Dataset bound, not field bound.
    status = {"generated_at_utc": "2026-09-17T00:00:00+00:00", "sources": []}
    projected = build_data_monitor_feature_inventory(tmp_path, monitor_status=status)
    assert projected["read_only"] is True
    assert projected["production_control_possible"] is False
    assert projected["rows"][0]["market_category"] == "taiwan_equity"
    assert projected["rows"][-1]["market_category"] == "crypto"
    assert {row["field"] for row in projected["rows"]} == {"date", "twpub_value", "close"}


def test_feature_non_null_count_fails_closed_without_footer_null_statistics(tmp_path: Path) -> None:
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    first = tmp_path / "data_yahoo/tw_stocks/1111_features.parquet"
    second = tmp_path / "data_yahoo/tw_stocks/2222_features.parquet"
    first.parent.mkdir(parents=True)
    pq.write_table(pa.table({"date": [date(2024, 1, 2)], "feature": [1.0]}), first)
    pq.write_table(pa.table({"date": [date(2024, 1, 3)], "feature": [None]}), second,
                   write_statistics=False)
    build_record_inventory(tmp_path, refresh=True, max_refresh_files=10)
    field = next(row for row in build_feature_inventory(tmp_path)["rows"] if row["field"] == "feature")
    assert field["rows_with_field"] == 2
    assert field["non_null_count"] is None
    assert field["verified_partial_non_null"] == 1


def test_unchanged_feature_snapshot_is_reused_until_footer_cache_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(snapshot_service, "REPO_ROOT", tmp_path)
    cache = tmp_path / "artifacts/live/data_monitor/record_inventory_cache.json"
    feature = tmp_path / "artifacts/live/data_monitor/feature_inventory.json"
    cache.parent.mkdir(parents=True)
    cache.write_text("{}", encoding="utf-8")
    feature.write_text(json.dumps({
        "schema_version": 1, "read_only": True,
        "production_control_possible": False,
        "rows": [{"field": "close"}],
    }), encoding="utf-8")
    assert snapshot_service._current_feature_snapshot(feature) is not None
    newer = feature.stat().st_mtime_ns + 1_000_000_000
    os.utime(cache, ns=(newer, newer))
    assert snapshot_service._current_feature_snapshot(feature) is None


def test_crypto_completion_cannot_use_requested_end_date_as_stored_tail() -> None:
    now = datetime(2026, 9, 17, 2, 0, tzinfo=UTC)
    rows = [
        {"id": "group:bybit", "status": "current", "data_through": "2026-09-17", "warnings": [], "eta": {}},
        {"id": "product:bybit_perpetuals:1m", "status": "current", "data_through": "2026-09-17", "warnings": [], "eta": {}},
        {"id": "group:okx", "status": "blocked", "data_through": "2026-09-17", "warnings": [], "eta": {}},
    ]
    inventory = {
        "group:bybit": {"state": "verified", "last": "2026-09-14T09:07:00"},
        "group:okx": {"state": "verified", "last": "2026-09-12T21:54:00"},
    }
    _apply_verified_storage_freshness(rows, inventory, now=now)
    assert rows[0]["status"] == "stale"
    assert rows[0]["data_through"] == "2026-09-14T09:07:00"
    assert rows[0]["requested_data_through"] == "2026-09-17"
    assert rows[1]["status"] == "stale"
    assert rows[2]["status"] == "blocked"  # stronger failure state is preserved

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from pathlib import Path

import pytest

from stockagent.live import data_monitor_dashboard as dashboard
from stockagent.live.data_monitor_dashboard import (
    build_data_monitor_public_status,
    build_tw_public_monitor_status,
)


def test_date_only_coverage_is_measured_through_end_of_day() -> None:
    parsed = dashboard._parse_time("2026-08-17")

    assert parsed == datetime(2026, 8, 17, 23, 59, 59, tzinfo=UTC)


def test_tw_public_monitor_builds_only_requested_official_source_scope(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        dashboard,
        "_tw_public_sources",
        lambda _root, *, now: [
            {
                "id": "tw-public:fixture",
                "parent_id": "group:tw-public",
                "scope": "logical_source",
                "title": "fixture",
                "provider": "TWSE",
                "category": "daily",
                "status": "current",
                "status_label": "完整",
                "cadence": "每日",
                "latest_at_utc": now.isoformat(),
                "data_through": "2026-09-02",
                "freshness": {"state": "current", "age_seconds": 0.0},
                "coverage": {
                    "current": 1,
                    "total": 1,
                    "ratio": 1.0,
                    "unit": "資料集",
                },
                "eta": {"state": "complete", "remaining_seconds": 0},
                "rows": 1,
                "publishable": True,
                "automation_eligible": True,
                "detail": "fixture",
                "warnings": [],
            }
        ],
    )
    monkeypatch.setattr(
        dashboard,
        "_shioaji_sources",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("unrelated Shioaji rows must not be built")
        ),
    )

    payload = build_tw_public_monitor_status(
        tmp_path,
        now=datetime(2026, 9, 2, tzinfo=UTC),
        refresh_services={},
    )

    assert payload["scope"] == "tw_public_official_sources"
    assert payload["read_only"] is True
    assert payload["production_control_possible"] is False
    assert payload["summary"]["registered_items"] == 1
    assert payload["sources"][0]["id"] == "tw-public:fixture"
    json.dumps(payload, allow_nan=False)


def test_data_monitor_registers_catalog_and_marks_stale_receipt(tmp_path: Path) -> None:
    registry = tmp_path / "configs/data_sync"
    registry.mkdir(parents=True)
    (registry / "packed_datasets.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "datasets": [
                    {
                        "dataset": "okx",
                        "source": "data_okx",
                        "role": "training",
                        "publish": True,
                        "note": "fixture",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    data = tmp_path / "data_okx"
    data.mkdir()
    (data / "download_summary.json").write_text(
        json.dumps(
            {
                "end_date": "2026-07-01",
                "symbol_count": 2,
                "row_count": 100,
                "status_counts": {"updated": 2},
            }
        ),
        encoding="utf-8",
    )

    payload = build_data_monitor_public_status(
        tmp_path,
        now=datetime(2026, 8, 16, tzinfo=UTC),
        refresh_services={},
    )

    assert payload["read_only"] is True
    assert payload["production_control_possible"] is False
    assert "integrity_checks" in payload
    assert payload["summary"]["storage_groups"] == 1
    assert payload["groups"][0]["id"] == "group:okx"
    assert payload["groups"][0]["status"] == "stale"
    assert payload["groups"][0]["operation_state"] == "catching_up"
    assert payload["groups"][0]["coverage"]["ratio"] == 1.0
    assert payload["groups"][0]["eta"]["remaining_seconds"] is None
    json.dumps(payload, allow_nan=False)


def test_data_monitor_uses_fresh_progress_receipt_for_eta(tmp_path: Path) -> None:
    registry = tmp_path / "configs/data_sync"
    registry.mkdir(parents=True)
    (registry / "packed_datasets.json").write_text(
        json.dumps(
            {
                "datasets": [
                    {
                        "dataset": "coinmetrics-community",
                        "source": "data_coinmetrics_community",
                        "role": "analytics",
                        "publish": False,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    data = tmp_path / "data_coinmetrics_community"
    data.mkdir()
    (data / "progress.json").write_text(
        json.dumps(
            {
                "state": "running",
                "label": "Coin Metrics Community 全量日資料",
                "current": 100,
                "total": 400,
                "unit": "asset",
                "remaining_seconds": 900,
                "estimated_complete_at_utc": "2026-08-16T05:15:00+00:00",
                "updated_at_utc": "2026-08-16T05:00:00+00:00",
                "status_counts": {"updated": 99, "failed": 1},
                "basis": "measured asset throughput",
            }
        ),
        encoding="utf-8",
    )

    payload = build_data_monitor_public_status(
        tmp_path,
        now=datetime(2026, 8, 16, 5, 1, tzinfo=UTC),
        refresh_services={},
    )
    group = payload["groups"][0]

    assert group["status"] == "updating"
    assert group["operation_state"] == "catching_up"
    assert group["coverage"]["ratio"] == 0.25
    assert group["eta"]["remaining_seconds"] == 900
    assert group["eta"]["basis"] == "measured asset throughput"
    assert (
        "目前批次已有 1 個失敗／部分完成項；更新器仍會完成其餘工作並保留錯誤明細。"
        in group["warnings"]
    )


def test_structured_progress_receipt_beats_transient_tqdm_log(
    tmp_path: Path,
) -> None:
    registry = tmp_path / "configs/data_sync"
    registry.mkdir(parents=True)
    (registry / "packed_datasets.json").write_text(
        json.dumps(
            {
                "datasets": [
                    {
                        "dataset": "bybit",
                        "source": "data_bybit",
                        "role": "training",
                        "publish": True,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    data = tmp_path / "data_bybit/1m"
    data.mkdir(parents=True)
    (data / "progress.json").write_text(
        json.dumps(
            {
                "state": "running",
                "phase": "candles",
                "current": 600,
                "total": 815,
                "unit": "symbol",
                "remaining_seconds": 200,
                "updated_at_utc": "2026-08-20T01:00:00Z",
            }
        ),
        encoding="utf-8",
    )
    logs = tmp_path / "artifacts/daily_downloader/registered_intraday"
    logs.mkdir(parents=True)
    (logs / "fixture.log").write_text(
        "download:bybit: 1%|#| 16/815 [00:10<08:00, 1.0it/s]\n",
        encoding="utf-8",
    )

    payload = build_data_monitor_public_status(
        tmp_path,
        now=datetime(2026, 8, 20, 1, 1, tzinfo=UTC),
        refresh_services={"registered_intraday": {"active": True}},
        shioaji_status={"pipelines": []},
        openbb_status={},
    )
    group = payload["groups"][0]

    assert group["coverage"]["current"] == 600
    assert group["coverage"]["total"] == 815
    assert group["eta"]["remaining_seconds"] == 200


def test_running_legacy_page_progress_cannot_claim_false_hundred_percent(
    tmp_path: Path,
) -> None:
    registry = tmp_path / "configs/data_sync"
    registry.mkdir(parents=True)
    (registry / "packed_datasets.json").write_text(
        json.dumps(
            {
                "datasets": [
                    {
                        "dataset": "binance",
                        "source": "data_binance",
                        "role": "training",
                        "publish": True,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    data = tmp_path / "data_binance/1m"
    data.mkdir(parents=True)
    (data / "progress.json").write_text(
        json.dumps(
            {
                "state": "running",
                "phase": "taker_buy_sell_volume",
                "current": 6_270,
                "total": 6_270,
                "unit": "request-page-or-feature-stage",
                "remaining_seconds": 0,
                "updated_at_utc": "2026-08-20T01:00:00+00:00",
                "status_counts": {"page_fetched": 400_000, "ok": 800},
            }
        ),
        encoding="utf-8",
    )

    payload = build_data_monitor_public_status(
        tmp_path,
        now=datetime(2026, 8, 20, 1, 1, tzinfo=UTC),
        refresh_services={},
    )
    group = payload["groups"][0]

    assert group["status"] == "updating"
    assert group["coverage"] is None
    assert group["eta"]["state"] == "warming_up"
    assert group["eta"]["remaining_seconds"] is None
    assert any("假 100%" in warning for warning in group["warnings"])


def test_partial_archive_summary_cannot_be_hidden_by_complete_progress(
    tmp_path: Path,
) -> None:
    registry = tmp_path / "configs/data_sync"
    registry.mkdir(parents=True)
    (registry / "packed_datasets.json").write_text(
        json.dumps(
            {
                "datasets": [
                    {
                        "dataset": "binance-public-archive",
                        "source": "data_binance_archive",
                        "role": "training",
                        "publish": True,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    archive = tmp_path / "data_binance_archive"
    archive.mkdir()
    (archive / "download_summary.json").write_text(
        json.dumps(
            {
                "state": "partial",
                "end_date": "2026-08-19",
                "status_counts": {
                    "complete": 306_378,
                    "quarantined_repair_required": 1_355,
                },
            }
        ),
        encoding="utf-8",
    )
    (archive / "progress.json").write_text(
        json.dumps(
            {
                "state": "complete",
                "current": 307_733,
                "total": 307_733,
                "updated_at_utc": "2026-08-20T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )

    payload = build_data_monitor_public_status(
        tmp_path,
        now=datetime(2026, 8, 20, 1, 0, tzinfo=UTC),
        refresh_services={},
    )
    group = payload["groups"][0]

    assert group["status"] == "degraded"
    assert group["coverage"]["ratio"] < 1.0
    assert "失敗" in group["status_label"]


def test_sequential_service_does_not_mark_future_provider_stage_active(
    tmp_path: Path,
) -> None:
    registry = tmp_path / "configs/data_sync"
    registry.mkdir(parents=True)
    (registry / "packed_datasets.json").write_text(
        json.dumps(
            {
                "datasets": [
                    {
                        "dataset": "bybit",
                        "source": "data_bybit",
                        "role": "training",
                        "publish": True,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    payload = build_data_monitor_public_status(
        tmp_path,
        now=datetime(2026, 8, 16, tzinfo=UTC),
        refresh_services={
            "registered_intraday": {"active": True},
            "registered_daily": {"active": False},
        },
        shioaji_status={"pipelines": []},
        openbb_status={},
    )

    assert payload["groups"][0]["status"] == "unavailable"
    assert payload["groups"][0]["operation_state"] == "unable"


def test_refresh_services_use_fresh_sanitized_snapshot(
    monkeypatch, tmp_path: Path
) -> None:
    snapshot = tmp_path / "refresh_services.json"
    snapshot.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at_utc": "2026-08-18T02:00:00+00:00",
                "services": {
                    "registered_daily": {
                        "active": False,
                        "state": "failed",
                        "result": "exit-code",
                        "timer_active": True,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(dashboard, "_systemd_property_sets", lambda *_: {})

    states = dashboard._refresh_service_states(
        snapshot_path=snapshot,
        now=datetime(2026, 8, 18, 2, 1, tzinfo=UTC),
        prefer_snapshot=True,
    )

    assert states["registered_daily"]["state"] == "failed"
    assert states["registered_daily"]["timer_active"] is True
    assert states["registered_daily"]["evidence_source"] == "systemd_snapshot"


def test_data_monitor_page_is_local_read_only_and_exposes_progress() -> None:
    root = Path(__file__).resolve().parents[1] / "services/data_monitor_dashboard"
    html = (root / "index.html").read_text(encoding="utf-8")
    javascript = (root / "app.js").read_text(encoding="utf-8")
    assert "dashboard-core.css?v=6" in html
    assert 'src="../dashboard-core.js?v=1"' in html
    assert 'role="status" aria-live="polite"' in html
    assert 'class="table-scroll" tabindex="0" role="region"' in html
    assert "DETAIL_LINKS.has" in javascript
    assert 'id="overall-progress"' in html
    assert 'id="source-rows"' in html
    assert "http://" not in html and "https://" not in html
    assert 'details ? "api/status" : "api/summary"' in javascript
    assert "FULL_REFRESH_TICKS = 6" in javascript
    assert "SOURCE_PAGE_SIZE = 100" in javascript
    assert 'id="load-more"' in html
    assert "textContent" in javascript
    assert "remaining_seconds" in javascript
    assert "OPERATION_ORDER" in javascript
    assert "automation.next_run_at_utc" in javascript
    assert "publicationLines" in javascript
    assert "row.acquisition_progress" in javascript
    assert "發布／偵測／下次取得" in html
    assert "首筆到達就推進下一資料日" in html
    assert "min-width:1440px" not in (root / "styles.css").read_text(encoding="utf-8")
    assert "正在抓／還沒到最新" in html
    assert "正在串流" in html
    assert "已完成／已到最新" in html
    assert "無法完成" in html
    assert "已延後／未啟用" in html
    assert "設定／憑證閘門" in html
    assert "清冊參照／不重複計算" in html
    assert "styles.css?v=9" in html
    assert "app.js?v=13" in html
    assert 'id="overall-denominator"' in html
    assert 'id="deferred-items"' in html
    assert 'id="control-items"' in html
    assert 'stateName === "complete"' in javascript
    assert "completionDate.getTime() > Date.now()" in javascript
    assert 'value == null || value === ""' in javascript


def test_operation_sort_and_endpoint_timing_reconcile(tmp_path: Path) -> None:
    registry = tmp_path / "configs/data_sync"
    registry.mkdir(parents=True)
    (registry / "packed_datasets.json").write_text(
        json.dumps(
            {
                "datasets": [
                    {
                        "dataset": "legacy-parquet",
                        "source": "data_parquet",
                        "role": "legacy",
                    },
                    {
                        "dataset": "forex-pepperstone",
                        "source": "missing",
                        "role": "training",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "data_parquet").mkdir()

    payload = build_data_monitor_public_status(
        tmp_path,
        now=datetime(2026, 8, 16, tzinfo=UTC),
        refresh_services={},
        shioaji_status={"pipelines": []},
        openbb_status={},
    )

    rows = payload["sources"]
    ranks = [row["operation_rank"] for row in rows]
    assert ranks == sorted(ranks)
    assert [row["sort_index"] for row in rows] == list(range(1, len(rows) + 1))
    assert sum(payload["endpoint_inventory"]["state_counts"].values()) == len(rows)
    assert payload["endpoint_inventory"]["timing_defined"] == len(rows)
    assert all(row["endpoint_id"] for row in rows)
    assert all(row["automation"]["schedule_label"] for row in rows)
    assert all(row["publication"]["schedule_label"] for row in rows)
    assert all("acquisition_progress" in row for row in rows)


def test_first_data_advances_next_date_without_claiming_full_completion() -> None:
    row = {
        "id": "fixture:daily",
        "parent_id": "group:yahoo-market",
        "scope": "logical_source",
        "title": "fixture daily",
        "provider": "fixture",
        "status": "updating",
        "status_label": "running",
        "cadence": "每日",
        "latest_at_utc": "2026-08-19T06:01:00Z",
        "data_through": "2026-08-19",
        "coverage": {
            "current": 1,
            "total": 10,
            "ratio": 0.1,
            "unit": "檔",
            "label": "目前批次",
        },
        "freshness": {"state": "current", "age_seconds": 60},
        "eta": {"state": "estimating", "remaining_seconds": 90},
        "automation_eligible": True,
    }

    enriched = dashboard._enrich_and_sort_rows(
        [row],
        now=datetime(2026, 8, 19, 6, 2, tzinfo=UTC),
        refresh_services={"registered_daily": {"active": True}},
    )[0]
    progress = enriched["acquisition_progress"]

    assert progress["first_data_observed"] is True
    assert progress["preparing_for_date"] == "2026-08-20"
    assert progress["ratio"] == 0.1
    assert progress["batch_complete"] is False
    assert progress["up_to_date"] is False
    assert progress["state"] == "acquiring"


def test_unknown_denominator_stays_unknown_until_completed_receipt() -> None:
    publication = {"detected_at_utc": None, "applied_at_utc": None}
    partial = dashboard._acquisition_progress(
        {"data_through": "2026-08-19", "rows": 1, "coverage": None},
        operation="catching_up",
        execution="running",
        publication=publication,
    )
    complete = dashboard._acquisition_progress(
        {
            "data_through": "2026-08-19",
            "rows": 1,
            "coverage": None,
            "latest_at_utc": "2026-08-19T06:00:00Z",
            "eta": {"state": "complete"},
        },
        operation="complete",
        execution="idle_current",
        publication=publication,
    )

    assert partial["ratio"] is None
    assert partial["up_to_date"] is False
    assert complete["ratio"] == 1.0
    assert complete["unit"] == "完成收據"
    assert complete["up_to_date"] is True


def test_tw_publication_receipt_keeps_probe_boundary_and_detection_separate(
    tmp_path: Path,
) -> None:
    receipt = tmp_path / "artifacts/data_refresh/tw_public/publications/close"
    receipt.mkdir(parents=True)
    (receipt / "latest.json").write_text(
        json.dumps(
            {
                "phase": "close",
                "scheduled_boundary": "14:00:00",
                "official_basis": "fixture product boundary",
                "started_at_taipei": "2026-08-19T14:00:01+08:00",
                "completed_at_taipei": "2026-08-19T14:00:03+08:00",
                "status": "ok",
                "selected_datasets": ["fixture"],
                "changed_datasets": [{"dataset": "fixture"}],
            }
        ),
        encoding="utf-8",
    )

    index = dashboard._tw_public_publication_index(tmp_path)

    assert index["fixture"][0]["scheduled_boundary"] == "14:00:00"
    assert index["fixture"][0]["content_change_observed"] is True
    assert index["fixture"][0]["last_completed_at_utc"] == "2026-08-19T06:00:03Z"


def test_tw_public_source_rollup_accepts_only_verified_official_fallback(
    tmp_path: Path,
) -> None:
    data = tmp_path / "data_tw_public"
    events = tmp_path / "artifacts/data_refresh/tw_public/events"
    data.mkdir(parents=True)
    events.mkdir(parents=True)
    (data / "dataset_manifest.json").write_text(
        json.dumps(
            [
                {
                    "name": "tdcc_shareholding_distribution",
                    "source": "TDCC OpenAPI",
                    "tags": ["tdcc", "ownership"],
                },
                {
                    "name": "data_gov_tdcc_shareholding_distribution",
                    "source": "data.gov.tw",
                    "tags": ["tdcc", "ownership"],
                },
            ]
        ),
        encoding="utf-8",
    )
    (data / "download_summary.json").write_text(
        json.dumps({"end_date": "2026-08-24", "coverage_complete": True}),
        encoding="utf-8",
    )
    (data / "download_report.csv").write_text(
        "dataset,status,rows,failed_dates,missing_dates_after,coverage_complete\n"
        "tdcc_shareholding_distribution,up_to_date,136629,0,0,\n"
        "data_gov_tdcc_shareholding_distribution,ok,68578,0,0,\n",
        encoding="utf-8",
    )
    (events / "latest.json").write_text(
        json.dumps(
            {
                "status": "ok",
                "accepted_source_fallbacks": {
                    "tdcc_shareholding_distribution": (
                        "data_gov_tdcc_shareholding_distribution"
                    )
                },
                "datasets": {
                    "tdcc_shareholding_distribution": {
                        "last_probe_status": "failed",
                        "last_checked_at_taipei": "2026-08-25T09:00:00+08:00",
                    },
                    "data_gov_tdcc_shareholding_distribution": {
                        "last_probe_status": "ok",
                        "observed_version": "v2",
                        "applied_version": "v2",
                        "last_download_status": "ok",
                        "last_checked_at_taipei": "2026-08-25T09:00:00+08:00",
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    rows = dashboard._tw_public_sources(
        tmp_path,
        now=datetime(2026, 8, 25, 1, 5, tzinfo=UTC),
    )
    enriched = dashboard._enrich_and_sort_rows(
        rows,
        now=datetime(2026, 8, 25, 1, 5, tzinfo=UTC),
        refresh_services={},
    )
    direct = next(
        row
        for row in enriched
        if row["title"] == "tdcc_shareholding_distribution"
    )

    assert direct["operation_state"] == "complete"
    assert direct["is_latest"] is True
    assert direct["source_fallback"]["accepted"] is True
    assert direct["source_fallback"]["replacement_dataset"] == (
        "data_gov_tdcc_shareholding_distribution"
    )
    assert any("原端點探測失敗" in warning for warning in direct["warnings"])


def test_streaming_requires_open_window_and_recent_endpoint_heartbeat() -> None:
    base = {
        "id": "shioaji:fop_stream",
        "parent_id": "group:tw-microstructure-captures-cold",
        "scope": "logical_source",
        "title": "fixture stream",
        "provider": "Shioaji",
        "category": "realtime",
        "status": "updating",
        "status_label": "active",
        "latest_at_utc": "2026-08-17T01:04:30Z",
        "freshness": {"state": "continuous", "age_seconds": 30},
        "eta": {"state": "continuous", "remaining_seconds": None},
        "automation_eligible": True,
    }
    service = {"shioaji_fop_stream": {"active": True}}
    open_rows = dashboard._enrich_and_sort_rows(
        [base],
        now=datetime(2026, 8, 17, 1, 5, tzinfo=UTC),
        refresh_services=service,
    )
    closed_rows = dashboard._enrich_and_sort_rows(
        [base],
        now=datetime(2026, 8, 16, 1, 5, tzinfo=UTC),
        refresh_services=service,
    )

    assert open_rows[0]["operation_state"] == "streaming"
    assert open_rows[0]["execution_state"] == "streaming"
    assert closed_rows[0]["operation_state"] == "catching_up"
    assert closed_rows[0]["execution_state"] == "waiting_stream_window"


def test_shared_active_service_does_not_mark_completed_endpoint_running() -> None:
    row = {
        "id": "free-source:bybit_public_derivatives",
        "parent_id": "group:bybit",
        "scope": "source_registry",
        "title": "Bybit",
        "provider": "Bybit",
        "status": "current",
        "status_label": "current",
        "freshness": {"state": "current", "age_seconds": 60},
        "eta": {"state": "complete", "remaining_seconds": 0},
        "automation_eligible": True,
    }
    rows = dashboard._enrich_and_sort_rows(
        [row],
        now=datetime(2026, 8, 16, tzinfo=UTC),
        refresh_services={"registered_intraday": {"active": True}},
    )

    assert rows[0]["automation"]["service_active"] is True
    assert rows[0]["automation"]["job_running"] is False
    assert rows[0]["operation_state"] == "complete"


def test_data_monitor_reuses_prebuilt_dependency_snapshots(
    tmp_path: Path, monkeypatch
) -> None:
    registry = tmp_path / "configs/data_sync/packed_datasets.json"
    registry.parent.mkdir(parents=True)
    registry.write_text(json.dumps({"datasets": []}), encoding="utf-8")
    monkeypatch.setattr(
        dashboard,
        "build_shioaji_public_status",
        lambda _root: (_ for _ in ()).throw(AssertionError("duplicate Shioaji read")),
    )
    monkeypatch.setattr(
        dashboard,
        "build_openbb_public_status",
        lambda _root: (_ for _ in ()).throw(AssertionError("duplicate OpenBB read")),
    )
    payload = dashboard.build_data_monitor_public_status(
        tmp_path,
        now=datetime(2026, 8, 16, tzinfo=UTC),
        refresh_services={},
        shioaji_status={"pipelines": []},
        openbb_status={},
    )
    assert payload["read_only"] is True


def test_data_monitor_preserves_shioaji_partial_freshness_evidence(
    tmp_path: Path,
) -> None:
    registry = tmp_path / "configs/data_sync/packed_datasets.json"
    registry.parent.mkdir(parents=True)
    registry.write_text(json.dumps({"datasets": []}), encoding="utf-8")
    payload = dashboard.build_data_monitor_public_status(
        tmp_path,
        now=datetime(2026, 8, 22, 20, 0, tzinfo=UTC),
        refresh_services={},
        shioaji_status={
            "pipelines": [
                {
                    "id": "minute_research",
                    "title": "股票分鐘因果研究資料",
                    "status": "partial",
                    "status_label": "既有研究資料可用；等待最新來源",
                    "data_through": "2026-08-20",
                    "coverage": {"current": 2621, "total": 2747},
                    "eta": {"state": "waiting_upstream"},
                    "warnings": ["目標為 2026-08-21。"],
                }
            ]
        },
        openbb_status={},
    )
    row = next(
        item for item in payload["sources"] if item["id"] == "shioaji:minute_research"
    )
    assert row["status"] == "degraded"
    assert row["data_through"] == "2026-08-20"
    assert row["eta"]["state"] == "waiting_upstream"


def test_data_monitor_preserves_completed_shioaji_history_receipt(
    tmp_path: Path,
) -> None:
    registry = tmp_path / "configs/data_sync/packed_datasets.json"
    registry.parent.mkdir(parents=True)
    registry.write_text(
        json.dumps(
            {
                "datasets": [
                    {
                        "dataset": "tw-shioaji-history",
                        "source": "data_tw_shioaji_history",
                        "role": "training",
                        "publish": True,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    payload = dashboard.build_data_monitor_public_status(
        tmp_path,
        now=datetime(2026, 9, 1, 14, 18, tzinfo=UTC),
        refresh_services={
            "shioaji_historical_market_data": {"active": False, "state": "dead"}
        },
        shioaji_status={
            "pipelines": [
                {
                    "id": "historical_market_data",
                    "title": "週選／月選／實際月份期貨／指數歷史",
                    "category": "historical",
                    "status": "complete",
                    "status_label": "Receipt 全部完成",
                    "latest_at_utc": "2026-09-01T14:17:23Z",
                    "coverage": {"current": 85284, "total": 85284, "ratio": 1.0},
                    "eta": {"state": "complete", "remaining_seconds": 0},
                }
            ]
        },
        openbb_status={},
    )

    source = next(
        row
        for row in payload["sources"]
        if row["id"] == "shioaji:historical_market_data"
    )
    group = next(
        row for row in payload["groups"] if row["id"] == "group:tw-shioaji-history"
    )
    assert source["parent_id"] == "group:tw-shioaji-history"
    assert source["status"] == "complete"
    assert source["operation_state"] == "complete"
    assert group["status"] == "complete"
    assert group["operation_state"] == "complete"


def test_data_monitor_registers_openbb_l1_compaction_progress(tmp_path: Path) -> None:
    now = datetime(2026, 8, 18, 2, 0, tzinfo=UTC)
    registry = tmp_path / "configs/data_sync/packed_datasets.json"
    registry.parent.mkdir(parents=True)
    registry.write_text(json.dumps({"datasets": []}), encoding="utf-8")
    payload = dashboard.build_data_monitor_public_status(
        tmp_path,
        now=now,
        refresh_services={},
        shioaji_status={"pipelines": []},
        openbb_status={
            "providers": [],
            "l1_compaction": {
                "generated_at_utc": (now - timedelta(minutes=5)).isoformat(),
                "source_age_seconds": 300,
                "success_files": 50_000,
                "compacted_files": 10_000,
                "compacted_rows": 123_456,
                "pending_files": 40_000,
                "active_segments": 5,
                "source_bytes": 1_000_000,
                "output_bytes": 250_000,
                "l0_deleted": False,
            },
        },
    )
    row = next(
        item for item in payload["sources"] if item["id"] == "openbb:l1-compaction"
    )
    assert row["status"] == "waiting"
    assert row["coverage"]["current"] == 10_000
    assert row["coverage"]["total"] == 50_000
    assert row["eta"]["remaining_seconds"] == 24_000
    assert "2,048 shard" in row["eta"]["basis"]
    assert "小於 32 檔" in row["eta"]["basis"]
    assert "75.00%" in row["detail"]

    service = (
        Path(__file__).resolve().parents[1]
        / "deploy/systemd/stockagent-openbb-l1-compaction.service.in"
    ).read_text(encoding="utf-8")
    assert (
        f"--max-source-files {dashboard.OPENBB_L1_MAX_SOURCE_FILES_PER_RUN}" in service
    )
    assert (
        f"--min-files-per-segment {dashboard.OPENBB_L1_MIN_FILES_PER_SEGMENT}"
        in service
    )


def test_data_monitor_does_not_report_complete_when_query_view_is_deferred(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 18, 2, 0, tzinfo=UTC)
    registry = tmp_path / "configs/data_sync/packed_datasets.json"
    registry.parent.mkdir(parents=True)
    registry.write_text(json.dumps({"datasets": []}), encoding="utf-8")
    payload = dashboard.build_data_monitor_public_status(
        tmp_path,
        now=now,
        refresh_services={},
        shioaji_status={"pipelines": []},
        openbb_status={
            "providers": [],
            "l1_compaction": {
                "generated_at_utc": now.isoformat(),
                "source_age_seconds": 0,
                "success_files": 10,
                "compacted_files": 10,
                "pending_files": 0,
                "source_bytes": 100,
                "output_bytes": 50,
                "deferred_query_views": {
                    "economy.fred_series": "long_form_normalization_required"
                },
            },
        },
    )
    row = next(
        item for item in payload["sources"] if item["id"] == "openbb:l1-compaction"
    )
    assert row["status"] == "partial"
    assert row["eta"]["state"] == "query_normalization_required"
    assert "economy.fred_series" in " ".join(row["warnings"])


def test_free_public_registry_maps_one_source_to_multiple_datasets(
    tmp_path: Path,
) -> None:
    config = tmp_path / "configs"
    config.mkdir()
    (config / "free_public_data_sources.json").write_text(
        json.dumps(
            {
                "sources": [
                    {
                        "id": "bitcoin_network",
                        "provider": "fixture",
                        "implementation_status": "implemented",
                        "dataset_ids": ["fees", "hashrate"],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    data = tmp_path / "data_free_public"
    data.mkdir()
    (data / "download_manifest.json").write_text(
        json.dumps(
            {
                "results": [
                    {
                        "dataset": "fees",
                        "status": "updated",
                        "observations_added": 5,
                        "observed_at_utc": "2026-08-16T05:00:00+00:00",
                    },
                    {
                        "dataset": "hashrate",
                        "status": "updated",
                        "observations_added": 10,
                        "observed_at_utc": "2026-08-16T05:00:00+00:00",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    rows = dashboard._free_public_registry_sources(
        tmp_path, now=datetime(2026, 8, 16, 5, 1, tzinfo=UTC)
    )

    assert rows[0]["status"] == "current"
    assert rows[0]["coverage"]["current"] == 2
    assert rows[0]["coverage"]["total"] == 2
    assert rows[0]["rows"] == 15


def test_free_public_registry_can_use_specialized_summary(tmp_path: Path) -> None:
    config = tmp_path / "configs"
    config.mkdir()
    (config / "free_public_data_sources.json").write_text(
        json.dumps(
            {
                "sources": [
                    {
                        "id": "history",
                        "provider": "fixture",
                        "implementation_status": "implemented",
                        "summary_path": "data_history/download_summary.json",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    data = tmp_path / "data_history"
    data.mkdir()
    (data / "download_summary.json").write_text(
        json.dumps(
            {
                "row_count": 123,
                "status_counts": {"updated": 5},
                "ended_at_utc": "2026-08-16T05:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )

    rows = dashboard._free_public_registry_sources(
        tmp_path, now=datetime(2026, 8, 16, 5, 1, tzinfo=UTC)
    )

    assert rows[0]["status"] == "current"
    assert rows[0]["rows"] == 123


def test_product_granularity_and_credential_contracts_are_public_but_secret_free(
    tmp_path: Path,
) -> None:
    config = tmp_path / "configs"
    (config / "data_sync").mkdir(parents=True)
    (config / "data_sync/packed_datasets.json").write_text(
        json.dumps({"datasets": []}), encoding="utf-8"
    )
    (config / "data_product_granularities.json").write_text(
        json.dumps(
            {
                "products": [
                    {
                        "id": "fixture_product",
                        "title": "Fixture",
                        "provider": "Fixture Provider",
                        "granularities": [
                            {
                                "granularity": grain,
                                "implementation": (
                                    "deferred_by_user_1m_only"
                                    if grain == "tick"
                                    else "registered_capacity_gate"
                                ),
                                "availability": "fixture",
                                "acquisition_enabled": grain != "tick",
                            }
                            for grain in ("daily", "1m", "tick")
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    receipt = tmp_path / "artifacts/data_credentials"
    receipt.mkdir(parents=True)
    (receipt / "status.json").write_text(
        json.dumps(
            {
                "generated_at_utc": "2026-08-16T05:00:00+00:00",
                "secret_values_included": False,
                "providers": [
                    {
                        "id": "fixture",
                        "provider": "Fixture Provider",
                        "state": "configured",
                        "configured_count": 1,
                        "required_count": 1,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    payload = build_data_monitor_public_status(
        tmp_path,
        now=datetime(2026, 8, 16, 5, 1, tzinfo=UTC),
        refresh_services={},
        shioaji_status={"pipelines": []},
        openbb_status={},
    )

    assert payload["summary"]["product_granularities"] == 3
    assert payload["summary"]["credential_gates"] == 1
    product_rows = [
        row for row in payload["sources"] if row["scope"] == "product_granularity"
    ]
    assert {row["granularity"] for row in product_rows} == {"daily", "1m", "tick"}
    deferred_tick = next(row for row in product_rows if row["granularity"] == "tick")
    assert deferred_tick["status"] == "deferred"
    assert deferred_tick["operation_state"] == "deferred"
    assert deferred_tick["execution_state"] == "deferred"
    assert deferred_tick["in_active_scope"] is False
    assert deferred_tick["is_latest"] is False
    assert deferred_tick["coverage"] is None
    assert deferred_tick["acquisition_progress"]["ratio"] is None
    assert deferred_tick["eta"]["state"] == "deferred"
    assert payload["summary"]["deferred"] == 1
    assert payload["endpoint_inventory"]["active_scope_total"] == sum(
        row["scope"] != "storage_group"
        and row["operation_state"] not in {"deferred", "control", "reference"}
        for row in payload["sources"]
    )
    credential = next(
        row for row in payload["sources"] if row["scope"] == "credential_gate"
    )
    assert credential["operation_state"] == "control"
    assert credential["in_active_scope"] is False
    assert credential["acquisition_progress"]["ratio"] is None
    assert payload["summary"]["control_items"] == 1
    assert payload["summary"]["credential_ready"] == 1
    serialized = json.dumps(payload, ensure_ascii=False)
    assert "API key、secret、token 值永不進入公開 payload" in serialized


def test_running_saturated_progress_and_expired_eta_are_not_published() -> None:
    row = {
        "id": "fixture:phase-transition",
        "parent_id": "group:yahoo-market",
        "scope": "logical_source",
        "title": "phase transition",
        "provider": "fixture",
        "status": "updating",
        "status_label": "still running",
        "latest_at_utc": "2026-08-20T01:00:00Z",
        "coverage": {
            "current": 10,
            "total": 10,
            "ratio": 1.0,
            "unit": "階段",
            "label": "目前階段",
        },
        "freshness": {"state": "current", "age_seconds": 60},
        "eta": {
            "state": "estimating",
            "remaining_seconds": 90,
            "estimated_complete_at_utc": "2026-08-20T01:00:30Z",
        },
        "automation_eligible": True,
    }

    enriched = dashboard._enrich_and_sort_rows(
        [row],
        now=datetime(2026, 8, 20, 1, 2, tzinfo=UTC),
        refresh_services={"registered_daily": {"active": True}},
    )[0]

    assert enriched["operation_state"] == "catching_up"
    assert enriched["execution_state"] == "running"
    assert enriched["eta"]["state"] == "warming_up"
    assert enriched["eta"]["remaining_seconds"] is None
    assert enriched["eta"]["estimated_complete_at_utc"] is None
    assert enriched["acquisition_progress"]["ratio"] is None
    assert any("ETA 已過期" in warning for warning in enriched["warnings"])


def test_stale_complete_receipt_cannot_claim_current_completion() -> None:
    row = {
        "id": "fixture:stale-complete",
        "parent_id": "group:fixture",
        "scope": "logical_source",
        "title": "stale complete",
        "provider": "fixture",
        "status": "complete",
        "status_label": "old batch complete",
        "latest_at_utc": "2026-08-01T00:00:00Z",
        "coverage": dashboard._coverage(10, 10, unit="資料集", label="舊收據"),
        "freshness": {"state": "stale", "age_seconds": 1_000_000},
        "eta": dashboard._complete_eta(),
        "automation_eligible": True,
    }

    enriched = dashboard._enrich_and_sort_rows(
        [row],
        now=datetime(2026, 8, 20, tzinfo=UTC),
        refresh_services={},
    )[0]

    assert enriched["operation_state"] == "catching_up"
    assert enriched["is_latest"] is False
    assert enriched["eta"]["state"] == "waiting_schedule"
    assert enriched["acquisition_progress"]["ratio"] is None
    assert enriched["acquisition_progress"]["state"] == "stale_complete_receipt"
    assert enriched["acquisition_progress"]["evidence_coverage"]["ratio"] == 1.0


def test_incomplete_coverage_cannot_be_overridden_by_current_status() -> None:
    row = {
        "id": "fixture:incomplete-current",
        "parent_id": "group:fixture",
        "scope": "logical_source",
        "title": "incomplete current",
        "provider": "fixture",
        "status": "current",
        "status_label": "reported current",
        "coverage": dashboard._coverage(9, 10, unit="任務", label="完整稽核"),
        "freshness": {"state": "current", "age_seconds": 60},
        "eta": dashboard._complete_eta(),
        "automation_eligible": True,
    }

    enriched = dashboard._enrich_and_sort_rows(
        [row],
        now=datetime(2026, 8, 20, tzinfo=UTC),
        refresh_services={},
    )[0]

    assert enriched["operation_state"] == "catching_up"
    assert enriched["acquisition_progress"]["ratio"] == 0.9
    assert enriched["eta"]["state"] == "waiting_schedule"


def test_blocked_endpoint_keeps_full_receipt_as_evidence_not_progress() -> None:
    row = {
        "id": "fixture:blocked-full-receipt",
        "parent_id": "group:fixture",
        "scope": "logical_source",
        "title": "blocked full receipt",
        "provider": "fixture",
        "status": "blocked",
        "status_label": "upstream unavailable",
        "coverage": dashboard._coverage(4, 4, unit="端點", label="舊批次"),
        "freshness": {"state": "stale", "age_seconds": 1_000_000},
        "eta": dashboard._complete_eta(),
        "automation_eligible": True,
    }

    enriched = dashboard._enrich_and_sort_rows(
        [row],
        now=datetime(2026, 8, 20, tzinfo=UTC),
        refresh_services={},
    )[0]

    assert enriched["operation_state"] == "unable"
    assert enriched["eta"]["state"] == "blocked"
    assert enriched["acquisition_progress"]["ratio"] is None
    assert enriched["acquisition_progress"]["state"] == "blocked"
    assert enriched["acquisition_progress"]["evidence_coverage"]["ratio"] == 1.0


def test_registry_alias_is_reference_and_excluded_from_denominator() -> None:
    row = {
        "id": "fixture:registry-alias",
        "parent_id": "group:fixture",
        "scope": "source_registry",
        "title": "registry alias",
        "provider": "fixture",
        "status": "current",
        "status_label": "owned elsewhere",
        "freshness": {"state": "unknown", "age_seconds": None},
        "coverage": None,
        "eta": dashboard._not_applicable_eta("reference", "owned elsewhere"),
        "registry_alias": True,
        "automation_eligible": False,
    }

    enriched = dashboard._enrich_and_sort_rows(
        [row],
        now=datetime(2026, 8, 20, tzinfo=UTC),
        refresh_services={},
    )[0]

    assert enriched["operation_state"] == "reference"
    assert enriched["execution_state"] == "registry_alias"
    assert enriched["in_active_scope"] is False
    assert enriched["eta"]["state"] == "reference"
    assert enriched["acquisition_progress"]["state"] == "reference"
    assert enriched["acquisition_progress"]["ratio"] is None


def test_monitor_integrity_checks_recompute_final_public_dto_contracts() -> None:
    group = {
        "id": "group:fixture",
        "scope": "storage_group",
        "operation_state": "complete",
        "in_active_scope": True,
        "freshness": {"state": "current"},
        "eta": {"state": "complete"},
        "coverage": {"ratio": 1.0},
        "acquisition_progress": {"ratio": 1.0},
    }
    endpoint = {
        "id": "fixture:endpoint",
        "parent_id": "group:fixture",
        "scope": "logical_source",
        "operation_state": "complete",
        "in_active_scope": True,
        "freshness": {"state": "current"},
        "eta": {"state": "complete"},
        "coverage": {"ratio": 1.0},
        "acquisition_progress": {"ratio": 1.0},
    }

    passed = dashboard._monitor_integrity_checks(
        [group, endpoint], active_data_endpoints=1
    )
    failed = dashboard._monitor_integrity_checks(
        [group, {**endpoint, "freshness": {"state": "stale"}}],
        active_data_endpoints=1,
    )

    assert passed["state"] == "pass"
    assert passed["violations"] == 0
    assert failed["state"] == "fail"
    assert failed["checks"]["complete_with_stale_freshness"] == 1


def test_crypto_feature_catalog_separates_reference_from_deferred_scope(
    tmp_path: Path,
) -> None:
    catalog_dir = tmp_path / "data_okx/1m"
    catalog_dir.mkdir(parents=True)
    (catalog_dir / "okx_historical_feature_catalog.json").write_text(
        json.dumps(
            {
                "catalog": [
                    {
                        "id": "one_minute_candle_archive",
                        "download_status": "excluded_duplicate",
                    },
                    {
                        "id": "orderbook_400_5000_archive",
                        "download_status": "separate_extreme_archive",
                    },
                    {
                        "id": "current_open_interest",
                        "download_status": "excluded_snapshot",
                    },
                    {
                        "id": "liquidation_orders",
                        "download_status": "excluded_unreconstructable",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    raw_rows = dashboard._crypto_feature_sources(tmp_path, now=datetime.now(UTC))
    rows = dashboard._enrich_and_sort_rows(
        raw_rows,
        now=datetime.now(UTC),
        refresh_services={},
    )
    by_id = {row["id"]: row for row in rows}

    assert by_id["okx-feature:one_minute_candle_archive"]["operation_state"] == (
        "reference"
    )
    assert (
        by_id["okx-feature:orderbook_400_5000_archive"]["operation_state"] == "deferred"
    )
    assert by_id["okx-feature:current_open_interest"]["operation_state"] == ("deferred")
    assert by_id["okx-feature:liquidation_orders"]["operation_state"] == "deferred"


def test_product_storage_presence_is_not_a_completion_denominator(
    tmp_path: Path,
) -> None:
    config = tmp_path / "configs"
    config.mkdir()
    storage = tmp_path / "data_fixture"
    storage.mkdir()
    (config / "data_product_granularities.json").write_text(
        json.dumps(
            {
                "products": [
                    {
                        "id": "fixture",
                        "title": "Fixture",
                        "provider": "fixture",
                        "granularities": [
                            {
                                "granularity": "daily",
                                "implementation": "implemented",
                                "storage_path": "data_fixture",
                            }
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    row = dashboard._product_granularity_sources(
        tmp_path, now=datetime(2026, 8, 20, tzinfo=UTC)
    )[0]

    assert row["storage_present"] is True
    assert row["completion_receipt_present"] is False
    assert row["coverage"] is None
    assert row["status"] == "waiting"


def test_storage_group_rollup_cannot_hide_unable_child() -> None:
    group = {
        "id": "group:fixture",
        "status": "current",
        "status_label": "current",
        "eta": dashboard._complete_eta(),
        "warnings": [],
    }
    children = [
        {"parent_id": "group:fixture", "operation_state": "complete"},
        {"parent_id": "group:fixture", "operation_state": "unable"},
        {"parent_id": "group:fixture", "operation_state": "deferred"},
    ]

    dashboard._rollup_storage_groups([group], children)

    assert group["status"] == "blocked"
    assert group["child_operation_counts"]["deferred"] == 1
    assert group["active_child_endpoint_count"] == 2
    assert group["eta"]["state"] == "blocked"


def test_cex_group_uses_same_canonical_1m_progress_as_product_row() -> None:
    group = {
        "id": "group:bybit",
        "status": "updating",
        "status_label": "running",
        "coverage": dashboard._coverage(16, 815, unit="項", label="tqdm"),
        "eta": {"state": "estimating", "remaining_seconds": 200},
        "warnings": [],
    }
    children = [
        {
            "id": "product:bybit_perpetuals:1m",
            "parent_id": "group:bybit",
            "scope": "product_granularity",
            "granularity": "1m",
            "operation_state": "catching_up",
            "execution_state": "running",
            "coverage": dashboard._coverage(600, 815, unit="symbol", label="目前批次"),
            "eta": {"state": "estimating", "remaining_seconds": 200},
        }
    ]

    dashboard._rollup_storage_groups([group], children)

    assert group["coverage"]["current"] == 600
    assert group["coverage_source_endpoint_id"] == children[0]["id"]


def test_fred_crypto_macro_is_registered_and_daily_scheduled() -> None:
    root = Path(__file__).resolve().parents[1]
    packed = json.loads(
        (root / "configs/data_sync/packed_datasets.json").read_text(encoding="utf-8")
    )
    free_sources = json.loads(
        (root / "configs/free_public_data_sources.json").read_text(encoding="utf-8")
    )

    assert any(item["dataset"] == "fred-crypto-macro" for item in packed["datasets"])
    assert any(
        item["id"] == "fred_crypto_macro_initial_releases"
        and item["dataset_group"] == "fred-crypto-macro"
        for item in free_sources["sources"]
    )
    assert dashboard._AUTOMATION_PROFILES["group:fred-crypto-macro"][
        "service_keys"
    ] == ("registered_daily",)


def test_real_product_registry_has_exact_three_granularities_per_product() -> None:
    root = Path(__file__).resolve().parents[1]
    registry = json.loads(
        (root / "configs/data_product_granularities.json").read_text(encoding="utf-8")
    )

    assert registry["contract"]["canonical_granularities"] == ["daily", "1m", "tick"]
    assert registry["contract"]["crypto_active_intraday_granularity"] == "1m"
    assert len(registry["products"]) >= 16
    for product in registry["products"]:
        assert [row["granularity"] for row in product["granularities"]] == [
            "daily",
            "1m",
            "tick",
        ]
    active_crypto_products = {
        "okx_perpetual_swaps",
        "bybit_perpetuals",
        "binance_usdm_perpetuals",
    }
    inactive_crypto_products = {
        "coinbase_spot",
        "kraken_spot",
        "bitfinex_spot_derivatives",
        "hyperliquid_perpetuals",
        "deribit_derivatives",
    }
    by_product = {item["id"]: item for item in registry["products"]}
    for product_id in active_crypto_products:
        tick = by_product[product_id]["granularities"][2]
        assert tick["implementation"] == "deferred_by_user_1m_only"
        assert tick["acquisition_enabled"] is False
        assert tick["stream"] is False
    for product_id in inactive_crypto_products:
        for granularity in by_product[product_id]["granularities"]:
            assert granularity["implementation"] == "deferred_by_user_exchange_scope"
            assert granularity["acquisition_enabled"] is False
            assert granularity.get("stream", False) is False


def test_registered_refresh_reuses_downloaders_and_preserves_tw_snapshot_owner() -> (
    None
):
    root = Path(__file__).resolve().parents[1]
    runner = (root / "scripts/run_registered_data_refresh.sh").read_text(
        encoding="utf-8"
    )
    taifex = (root / "scripts/run_taifex_auxiliary_daily.sh").read_text(
        encoding="utf-8"
    )
    assert "RUN_TW_PUBLIC_DATA=0" in runner
    assert 'YAHOO_ASSETS="us_stocks forex"' in runner
    assert 'YAHOO_ASSETS=""' in runner
    assert "RUN_YAHOO=0" in runner
    assert "RUN_CEX_PERP=1" in runner
    assert "CRYPTO_TAIL_ONLY=1" in runner
    daily_scope = runner.split("  daily)", 1)[1].split("    ;;", 1)[0]
    intraday_scope = runner.split("  intraday)", 1)[1].split("    ;;", 1)[0]
    backfill_scope = runner.split("  backfill)", 1)[1].split("    ;;", 1)[0]
    assert "CRYPTO_HISTORICAL_FEATURES=0" in daily_scope
    assert "CRYPTO_HISTORICAL_FEATURES=0" in intraday_scope
    assert "CRYPTO_HISTORICAL_FEATURES=1" in backfill_scope
    assert "registered-backfill" in runner
    assert "CRYPTO_TAIL_ONLY=0" in runner
    assert "registered_backfill" in dashboard._REFRESH_UNITS
    assert (
        "registered_backfill"
        in dashboard._AUTOMATION_PROFILES["group:okx"]["service_keys"]
    )
    assert "RUN_CRYPTO_REFERENCE=1" in runner
    assert "RUN_FREE_PUBLIC_CONTEXT=0" in runner
    assert "RUN_COINMETRICS_COMMUNITY=0" in runner
    assert "RUN_CRYPTO_DAILY_MATERIALIZE=0" in runner
    assert "RUN_FRED_CRYPTO_MACRO=1" in runner
    assert "CRYPTO_ACTIVE_INTRADAY_GRAIN=1m" in runner
    assert "RUN_CRYPTO_TRADE_TICKS=0" in runner
    assert "RUN_CRYPTO_ORDER_BOOK=0" in runner
    assert "RUN_CRYPTO_LIQUIDATIONS=0" in runner
    assert "run_daily_all_markets.sh" in runner
    daily_runner = (root / "downloader/run_daily_all_markets.sh").read_text(
        encoding="utf-8"
    )
    assert "download_free_public_context.py" in daily_runner
    assert "download_coinmetrics_community.py" in daily_runner
    assert "download_crypto_keyed_context.py" in daily_runner
    assert "run_free_public_context_incremental" in daily_runner
    assert "run_coinmetrics_community_incremental" in daily_runner
    assert "run_crypto_reference_incremental" in daily_runner
    assert "run_fred_crypto_macro_daily" in daily_runner
    assert 'CRYPTO_COLUMNAR_THREADS="${CRYPTO_COLUMNAR_THREADS:-2}"' in daily_runner
    assert "download_fred_crypto_macro_vintages.py" in daily_runner
    assert (
        'CRYPTO_ACTIVE_INTRADAY_GRAIN="${CRYPTO_ACTIVE_INTRADAY_GRAIN:-1m}"'
        in daily_runner
    )
    assert "crypto event acquisition is deferred" in daily_runner
    assert "okx_perpetuals run_okx_perp_incremental" in daily_runner
    assert "bybit_perpetuals run_bybit_perp_incremental" in daily_runner
    assert "binance_perpetuals run_binance_perp_incremental" in daily_runner
    assert "cex run_cex_incremental" not in daily_runner
    assert "refresh_tw_public_live_snapshot.py" not in runner
    assert "download_taifex_option_daily_history.py" in taifex
    assert "download_taifex_recent_index_derivatives_ticks.py" in taifex
    assert "download_taifex_final_settlement_history.py" in taifex

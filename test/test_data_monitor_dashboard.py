from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path

from stockagent.live import data_monitor_dashboard as dashboard
from stockagent.live.data_monitor_dashboard import build_data_monitor_public_status


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


def test_data_monitor_page_is_local_read_only_and_exposes_progress() -> None:
    root = Path(__file__).resolve().parents[1] / "services/data_monitor_dashboard"
    html = (root / "index.html").read_text(encoding="utf-8")
    javascript = (root / "app.js").read_text(encoding="utf-8")
    assert "dashboard-core.css?v=5" in html
    assert 'role="status" aria-live="polite"' in html
    assert 'class="table-scroll" tabindex="0" role="region"' in html
    assert "DETAIL_LINKS.has" in javascript
    assert 'id="overall-progress"' in html
    assert 'id="source-rows"' in html
    assert "http://" not in html and "https://" not in html
    assert 'fetchJson("api/status")' in javascript
    assert "textContent" in javascript
    assert "remaining_seconds" in javascript
    assert "OPERATION_ORDER" in javascript
    assert "automation.next_run_at_utc" in javascript
    assert "正在抓／還沒到最新" in html
    assert "正在串流" in html
    assert "已完成／已到最新" in html
    assert "無法完成" in html
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
    assert deferred_tick["operation_state"] == "complete"
    assert deferred_tick["execution_state"] == "deferred"
    assert deferred_tick["in_active_scope"] is False
    assert deferred_tick["is_latest"] is False
    assert payload["summary"]["deferred"] == 1
    assert payload["endpoint_inventory"]["active_scope_total"] == (
        payload["endpoint_inventory"]["total"] - 1
    )
    credential = next(
        row for row in payload["sources"] if row["scope"] == "credential_gate"
    )
    assert credential["operation_state"] == "complete"
    serialized = json.dumps(payload, ensure_ascii=False)
    assert "API key、secret、token 值永不進入公開 payload" in serialized


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
    assert "RUN_CRYPTO_REFERENCE=1" in runner
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
    assert 'CRYPTO_ACTIVE_INTRADAY_GRAIN="${CRYPTO_ACTIVE_INTRADAY_GRAIN:-1m}"' in daily_runner
    assert "crypto event acquisition is deferred" in daily_runner
    assert "okx_perpetuals run_okx_perp_incremental" in daily_runner
    assert "bybit_perpetuals run_bybit_perp_incremental" in daily_runner
    assert "binance_perpetuals run_binance_perp_incremental" in daily_runner
    assert "cex run_cex_incremental" not in daily_runner
    assert "refresh_tw_public_live_snapshot.py" not in runner
    assert "download_taifex_option_daily_history.py" in taifex
    assert "download_taifex_recent_index_derivatives_ticks.py" in taifex
    assert "download_taifex_final_settlement_history.py" in taifex

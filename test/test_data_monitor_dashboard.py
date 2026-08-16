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
    assert payload["groups"][0]["coverage"]["ratio"] == 1.0
    assert payload["groups"][0]["eta"]["remaining_seconds"] is None
    json.dumps(payload, allow_nan=False)


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
    assert 'value == null || value === ""' in javascript


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


def test_registered_refresh_reuses_downloaders_and_preserves_tw_snapshot_owner() -> None:
    root = Path(__file__).resolve().parents[1]
    runner = (root / "scripts/run_registered_data_refresh.sh").read_text(
        encoding="utf-8"
    )
    taifex = (root / "scripts/run_taifex_auxiliary_daily.sh").read_text(
        encoding="utf-8"
    )
    assert "RUN_TW_PUBLIC_DATA=0" in runner
    assert 'YAHOO_ASSETS="us_stocks forex"' in runner
    assert 'YAHOO_ASSETS="crypto"' in runner
    assert "RUN_CEX_PERP=1" in runner
    assert "run_daily_all_markets.sh" in runner
    assert "refresh_tw_public_live_snapshot.py" not in runner
    assert "download_taifex_option_daily_history.py" in taifex
    assert "download_taifex_recent_index_derivatives_ticks.py" in taifex
    assert "download_taifex_final_settlement_history.py" in taifex

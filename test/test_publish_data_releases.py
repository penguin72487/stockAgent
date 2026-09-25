from __future__ import annotations

import importlib.util
import json
import shlex
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType

import pytest

from stockagent.data_sync.desync_snapshots import SnapshotError

from stockagent.data_sync.packed_snapshots import (
    fetch_packed_snapshot,
    initialize_packed_layout,
    publish_packed_snapshot,
    verify_packed_snapshot,
)


def test_writer_gate_uses_script_argv_not_waiting_shell_text():
    module = _module()
    entry = {"active_process_substrings": ["download_tw_corporate_action_entitlements.py"]}
    program = "python downloader/download_tw_corporate_action_entitlements.py --mode repair"
    commands = [(1, shlex.join(["bash", "-c", program])),
                (2, shlex.join(["python", "downloader/download_tw_corporate_action_entitlements.py", "--mode", "repair"]))]
    assert [r["pid"] for r in module._blockers(entry, commands)] == [2]


def test_bybit_release_keeps_data_but_excludes_rebuildable_panel_cache(
    tmp_path: Path,
) -> None:
    module = _module()
    catalog = Path(__file__).resolve().parents[1] / "configs/data_sync/packed_datasets.json"
    entry = next(
        item for item in module._load_catalog(catalog) if item["dataset"] == "bybit"
    )
    assert entry["excluded_subtrees"] == ["perpetual_daily/panel_cache_v2"]

    source = tmp_path / "data_bybit"
    daily = source / "perpetual_daily"
    cache = daily / "panel_cache_v2"
    raw = source / "1m"
    cache.mkdir(parents=True)
    raw.mkdir()
    (raw / "BTCUSDT_features.parquet").write_bytes(b"raw source")
    (daily / "BTCUSDT_features.parquet").write_bytes(b"derived daily")
    (cache / "features.npy").write_bytes(b"rebuildable cache")
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="node-a")

    release = publish_packed_snapshot(
        cold,
        "bybit",
        source,
        excluded_subtrees=entry["excluded_subtrees"],
    )
    target = fetch_packed_snapshot(cold, tmp_path / "materialized", release)

    assert release.manifest["source"]["excluded_subtrees"] == entry["excluded_subtrees"]
    assert (target / "1m/BTCUSDT_features.parquet").read_bytes() == b"raw source"
    assert (target / "perpetual_daily/BTCUSDT_features.parquet").read_bytes() == b"derived daily"
    assert not (target / "perpetual_daily/panel_cache_v2").exists()
    assert verify_packed_snapshot(cold, release, materialized_path=target)[
        "materialized_verified"
    ]


def test_missing_cold_bytes_do_not_erase_freshness_non_regression(tmp_path):
    module = _module()
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="node-a")
    published = publish_packed_snapshot(cold, "prices", _source(tmp_path, "2026-08-18"),
        metadata={"freshness_value": "2026-08-18", "freshness_field": "end_date", "freshness_format": "iso-date"})
    for obj in published.manifest["archive"]["objects"]:
        (cold / obj["relpath"]).unlink()
    status = module._status(_entry(_source(tmp_path, "2026-08-17")), [], sync_root=cold)
    assert not status["freshness_non_regression"] and not status["publish_ready"]


def _module() -> ModuleType:
    path = Path(__file__).resolve().parents[1] / "scripts/publish_data_releases.py"
    spec = importlib.util.spec_from_file_location("publish_data_releases", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _entry(source: Path) -> dict[str, object]:
    return {
        "dataset": "prices",
        "source": str(source),
        "role": "training",
        "publish": True,
        "active_process_substrings": [],
        "note": "fixture",
        "freshness": {
            "receipt": "download_summary.json",
            "field": "end_date",
            "format": "iso-date",
            "required_values": {
                "coverage_complete": True,
                "failed_dates": 0,
            },
        },
    }


def _source(tmp_path: Path, end_date: str) -> Path:
    source = tmp_path / f"source-{end_date}"
    source.mkdir()
    (source / "prices.csv").write_text("price\n1\n", encoding="utf-8")
    (source / "download_summary.json").write_text(
        json.dumps(
            {
                "coverage_complete": True,
                "end_date": end_date,
                "failed_dates": 0,
            }
        ),
        encoding="utf-8",
    )
    return source


def test_publish_status_blocks_freshness_regression(tmp_path: Path) -> None:
    module = _module()
    cold_root = tmp_path / "cold"
    initialize_packed_layout(cold_root, node_id="node-a")
    newer = _source(tmp_path, "2026-08-18")
    publish_packed_snapshot(
        cold_root,
        "prices",
        newer,
        metadata={
            "freshness_field": "end_date",
            "freshness_format": "iso-date",
            "freshness_value": "2026-08-18",
        },
    )
    older = _source(tmp_path, "2026-08-17")

    status = module._status(_entry(older), [], sync_root=cold_root)

    assert status["source_freshness"]["value"] == "2026-08-17"
    assert status["latest_cold_freshness"]["value"] == "2026-08-18"
    assert status["freshness_non_regression"] is False
    assert status["publish_ready"] is False


def test_publish_status_requires_completion_receipt(tmp_path: Path) -> None:
    module = _module()
    source = _source(tmp_path, "2026-08-18")
    receipt = json.loads((source / "download_summary.json").read_text())
    receipt["failed_dates"] = 1
    (source / "download_summary.json").write_text(
        json.dumps(receipt), encoding="utf-8"
    )

    status = module._status(_entry(source), [], sync_root=tmp_path / "cold")

    assert status["publish_ready"] is False
    assert "completion gate failed" in status["freshness_error"]


def test_publish_freshness_can_require_recent_catalog_check(tmp_path: Path) -> None:
    module = _module()
    source = _source(tmp_path, "2026-08-18")
    entry = _entry(source)
    entry["freshness"]["timestamp_field"] = "catalog_checked_at_utc"
    entry["freshness"]["max_receipt_age_hours"] = 24
    path = source / "download_summary.json"
    receipt = json.loads(path.read_text())
    receipt["catalog_checked_at_utc"] = (datetime.now(UTC) - timedelta(hours=25)).isoformat()
    path.write_text(json.dumps(receipt))
    with pytest.raises(SnapshotError, match="outside its configured age window"):
        module._source_freshness(entry)
    receipt["catalog_checked_at_utc"] = datetime.now(UTC).isoformat()
    path.write_text(json.dumps(receipt))
    assert module._source_freshness(entry)["value"] == "2026-08-18"

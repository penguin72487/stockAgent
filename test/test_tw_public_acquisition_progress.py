from __future__ import annotations

from datetime import UTC, datetime
import csv
import gzip
import hashlib
import json
from pathlib import Path
import pytest

from stockagent.live.tw_public_acquisition_progress import (
    ADDED_DATASETS,
    _cached_inventory_membership,
    _verified_inventory_membership,
    build_tw_public_acquisition_progress,
)
from stockagent.live.data_monitor_dashboard import _tw_public_sources


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def test_cold_inventory_disk_cache_reuses_only_the_same_verified_object(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "inventory.jsonl.gz"
    compressed = gzip.compress(b'{"kind":"file","path":"raw/gcis_open_data_catalog/a.csv"}\n')
    path.write_bytes(compressed)
    cache_path = tmp_path / "status/cache.json"
    monkeypatch.setenv("STOCKAGENT_COLD_INVENTORY_CACHE_PATH", str(cache_path))

    def identity() -> tuple[int, int, int, int, int]:
        stat = path.stat()
        return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns

    expected_sha = hashlib.sha256(compressed).hexdigest()
    _verified_inventory_membership.cache_clear()
    first = _cached_inventory_membership(path, expected_sha, identity(), 123)
    assert dict(first[1])["gcis_open_data_catalog"] == 1
    assert cache_path.is_file()
    assert _verified_inventory_membership.cache_info().misses == 1
    _verified_inventory_membership.cache_clear()
    assert _cached_inventory_membership(path, expected_sha, identity(), 123) == first
    assert _verified_inventory_membership.cache_info().misses == 0
    assert _cached_inventory_membership(path, expected_sha, identity(), 124) == first
    assert _verified_inventory_membership.cache_info().misses == 1
    path.write_bytes(compressed + b"corrupt")
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        _cached_inventory_membership(path, expected_sha, identity(), 124)


def test_acquisition_progress_keeps_four_evidence_gates_separate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    live = tmp_path / "live"
    cold = tmp_path / "cold"
    _write_json(
        tmp_path / "configs/data_sync/packed_datasets.json",
        {"datasets": [{"dataset": "tw-public", "source": str(live)}]},
    )
    _write_json(
        tmp_path / "configs/data_sync/packed_backup.json", {"source": str(cold)}
    )
    event_rows = {}
    for name, _ in ADDED_DATASETS:
        event_rows[name] = {
            "last_probe_status": "ok", "observed_version": "v1",
            "applied_version": "v1", "last_download_status": "ok",
            "last_applied_at_taipei": "2026-09-16T23:44:20+08:00",
        }
        (live / "raw" / name).mkdir(parents=True)
        (live / "raw" / name / "source.csv").write_text("x\n", encoding="utf-8")
        (live / f"{name}.parquet").write_bytes(b"parquet fixture")
        _write_json(live / "metadata" / f"{name}.json", {"license": "open"})
    _write_json(
        tmp_path / "artifacts/data_refresh/tw_public/events/latest.json",
        {"registered_dataset_count": 159, "observed_dataset_count": 159,
         "failed_probe_count": 0, "blocking_unapplied_event_count": 0,
         "updated_at_taipei": "2026-09-17T00:08:00+08:00",
         "datasets": event_rows},
    )
    _write_json(
        live / "download_summary.json",
        {"dataset_count": 156, "coverage_complete": True,
         "blocking_failed_count": 0, "incomplete_count": 0,
         "missing_dates_after": 0, "end_date": "2026-09-15",
         "generated_at_utc": datetime.now(UTC).isoformat()},
    )
    _write_json(
        tmp_path / "artifacts/data_refresh/tw_public/catalogs/latest/download_summary.json",
        {"dataset_count": 2, "generated_at_utc": "2026-09-16T08:00:00+00:00"},
    )
    _write_json(live / "dataset_manifest.json", [
        {"name": "current_only", "kind": "snapshot_url", "tags": ["snapshot"]},
        {"name": "dated", "kind": "historical_json_table", "tags": ["historical"]},
    ])
    _write_json(live / "state/dgbas_release_vintages.json", {
        "complete": False, "saved_releases": 267, "registered_releases": 650,
    })
    _write_json(live / "state/cbc_fx_reserve_release_vintages.json", {
        "complete": True,
    })
    _write_json(live / "state/gcis_open_data_catalog.json", {
        "catalog_entry_count": 3,
        "datasets": {
            "ok": {"name": "已存", "complete": True, "resources": {"csv": {}}},
            "missing": {"name": "待補資料", "complete": False, "resources": {}},
            "api": {"name": "僅 API", "complete": True, "resources": {}},
        },
    })
    _write_json(live / "state/dated.json", {
        "coverage_complete": True, "missing_dates_after": 0,
        "coverage_end": "2026-09-16",
    })
    _write_json(live / "state/mof_original_release_archive.json", {
        "status": "complete", "archived_releases": 9,
        "indexed_releases": 9, "index_period_gaps": {"tax": ["2018-04", "2018-05"], "trade": []},
        "generated_at_utc": "2026-09-16T10:00:00Z",
    })
    _write_json(live / "state/mof_macro_release_dates.json", {
        "distinct_release_periods": 10,
        "tax_pdf_fallback": {"not_found": ["2018-05"], "failures": []},
    })
    raw_mof = live / "raw/mof_original_release_archive/tax/2020-01/original-abc.pdf"
    raw_mof.parent.mkdir(parents=True)
    raw_mof.write_bytes(b"%PDF")
    _write_json(tmp_path / "artifacts/data_quality/tw_public_run/summary.json", {
        "model_safe": False,
        "finding_counts": {"critical": 2, "high": 2},
        "config": "configs/markets/tw_public.yaml",
    })
    with (live / "download_report.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["dataset", "status", "coverage_complete"])
        writer.writeheader()
        for name, _ in ADDED_DATASETS:
            writer.writerow({"dataset": name, "status": "ok", "coverage_complete": ""})

    inventory_paths = [
        path for name, _ in ADDED_DATASETS
        for path in (f"{name}.parquet", f"metadata/{name}.json")
    ]
    inventory_paths.append("raw/gcis_open_data_catalog/ok/csv.csv")
    inventory_bytes = gzip.compress(
        "".join(json.dumps({"kind": "file", "path": path}) + "\n" for path in inventory_paths).encode()
    )
    inventory_sha = hashlib.sha256(inventory_bytes).hexdigest()
    inventory_path = cold / "objects/inventories/data.jsonl.gz"
    inventory_path.parent.mkdir(parents=True)
    inventory_path.write_bytes(inventory_bytes)
    manifest = {
        "dataset": "tw-public", "snapshot_id": "fixture-release",
        "published_at": "2026-09-16T15:51:46Z",
        "archive": {"inventory": {
            "relpath": "objects/inventories/data.jsonl.gz", "sha256": inventory_sha,
        }},
    }
    manifest_path = cold / "manifests/tw-public/fixture-release.json"
    _write_json(manifest_path, manifest)
    _write_json(cold / "heads/tw-public/penguin.json", {
        "snapshot_id": "fixture-release",
        "manifest_relpath": "manifests/tw-public/fixture-release.json",
        "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
    })
    materialized_root = tmp_path / "materialized"
    monkeypatch.setenv("STOCKAGENT_MATERIALIZED_ROOT", str(materialized_root))

    _verified_inventory_membership.cache_clear()
    progress = build_tw_public_acquisition_progress(
        tmp_path, now=datetime(2026, 9, 16, 16, 8, 30, tzinfo=UTC)
    )
    assert _verified_inventory_membership.cache_info().misses == 1
    assert build_tw_public_acquisition_progress(tmp_path)["cold_release"]["state"] == "inventory_verified"
    assert _verified_inventory_membership.cache_info().hits == 1
    assert progress["observation"]["observed"] == 159
    assert progress["observation"]["fresh"] is True
    assert progress["full_batch"]["coverage_complete"] is False
    assert progress["cold_release"]["state"] == "inventory_verified"
    assert progress["cold_release"]["matches_full_batch"] is False
    assert progress["training_readiness"]["current_snapshot_sources"] == 1
    assert progress["training_readiness"]["dgbas_saved_releases"] == 267
    assert progress["training_readiness"]["all_source_history_ready"] is False
    assert progress["background_catalogs"]["gcis"]["failed_names"] == ["待補資料"]
    assert progress["background_catalogs"]["gcis"]["cold_inventory_raw_files"] == 1
    assert progress["background_catalogs"]["gcis"]["no_csv_entries"] == 1
    assert progress["background_catalogs"]["gcis"]["last_poll_at_utc"] == "2026-09-16T08:00:00+00:00"
    assert progress["history_backfill"]["official_daily_complete"] == 1
    assert progress["history_backfill"]["official_daily_common_end"] == "2026-09-16"
    assert progress["history_backfill"]["mof_archived_releases"] == 9
    assert progress["history_backfill"]["mof_indexed_releases"] == 10
    assert progress["history_backfill"]["mof_raw_periods"] == 1
    assert progress["history_backfill"]["mof_status"] == "backfilling"
    assert progress["history_backfill"]["mof_index_period_gaps"] == 1
    assert progress["model_audit"]["model_safe"] is False
    assert progress["model_audit"]["finding_counts"]["critical"] == 2
    assert progress["model_audit"]["matches_current_release"] is False
    assert progress["materialization"]["state"] == "not_started"

    (materialized_root / "tw-public/fixture-release").mkdir(parents=True)
    _write_json(materialized_root / "tw-public/.fixture-release.READY.json", {
        "snapshot_id": "fixture-release",
        "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        "verified_at": "2026-09-16T16:00:00Z",
    })
    _write_json(materialized_root / "tw-public-training-20260917.pin.json", {
        "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        "manifest": {"snapshot_id": "fixture-release"},
    })
    progress = build_tw_public_acquisition_progress(tmp_path)
    assert progress["materialization"]["state"] == "ready_receipt"
    _write_json(tmp_path / "artifacts/data_quality/tw_public_run/summary.json", {
        "model_safe": True,
        "finding_counts": {"critical": 0, "high": 0},
        "feature_count": 33,
        "feature_availability": {"allow_same_close_feature_approximation": True},
        "config": "configs/markets/tw_public_pinned.yaml",
        "panel": {
            "parquet_root": str(materialized_root / "tw-public/fixture-release/stocks"),
            "public_feature_path": str(materialized_root / "tw-public/fixture-release/features/tw_public_stock_daily.parquet"),
        },
    })
    progress = build_tw_public_acquisition_progress(tmp_path)
    assert progress["model_audit"]["matches_current_release"] is True
    assert progress["model_audit"]["audited_snapshot_id"] == "fixture-release"
    assert progress["model_audit"]["pinned_release_ready"] is True
    assert progress["model_audit"]["research_only_same_close"] is True
    assert progress["training_readiness"]["research_subset_ready"] is True
    assert progress["training_readiness"]["all_source_history_ready"] is False
    _write_json(cold / "heads/tw-public/penguin.json", {
        "snapshot_id": "new-release",
        "manifest_relpath": "manifests/tw-public/new-release.json",
        "manifest_sha256": "0" * 64,
    })
    progress = build_tw_public_acquisition_progress(tmp_path)
    assert progress["model_audit"]["matches_current_release"] is False
    assert progress["model_audit"]["pinned_release_ready"] is True
    assert progress["training_readiness"]["research_subset_ready"] is True
    _write_json(cold / "heads/tw-public/penguin.json", {
        "snapshot_id": "fixture-release",
        "manifest_relpath": "manifests/tw-public/fixture-release.json",
        "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
    })
    progress = build_tw_public_acquisition_progress(tmp_path)
    assert all(row["live_downloaded"] and row["in_cold_inventory"] for row in progress["datasets"])
    assert all(not row["in_full_batch"] for row in progress["datasets"])

    _write_json(live / "download_summary.json", {
        "dataset_count": 159, "coverage_complete": True,
        "blocking_failed_count": 0, "incomplete_count": 0,
        "missing_dates_after": 0, "end_date": "2026-09-16",
    })
    progress = build_tw_public_acquisition_progress(
        tmp_path, now=datetime(2026, 9, 16, 16, 8, 30, tzinfo=UTC)
    )
    assert all(row["in_full_batch"] for row in progress["datasets"])

    inventory_path.write_bytes(inventory_bytes + b"corrupt")
    progress = build_tw_public_acquisition_progress(
        tmp_path, now=datetime(2026, 9, 16, 16, 30, tzinfo=UTC)
    )
    assert progress["observation"]["fresh"] is False
    assert progress["cold_release"]["state"] == "unverified"
    assert _verified_inventory_membership.cache_info().misses >= 2
    assert all(not row["in_cold_inventory"] for row in progress["datasets"])


def test_new_source_row_reports_pending_full_audit_without_old_batch_date(
    tmp_path: Path,
) -> None:
    name = ADDED_DATASETS[0][0]
    live = tmp_path / "data_tw_public"
    live.mkdir()
    (live / f"{name}.parquet").write_bytes(b"fixture")
    _write_json(live / "metadata" / f"{name}.json", {})
    (live / "raw" / name).mkdir(parents=True)
    (live / "raw" / name / "source.csv").write_text("fixture\n", encoding="utf-8")
    _write_json(live / "dataset_manifest.json", [{"name": name, "source": "data.gov.tw"}])
    _write_json(live / "download_summary.json", {
        "dataset_count": 156, "end_date": "2026-09-15",
        "generated_at_utc": "2026-09-16T01:05:30+00:00",
    })
    _write_json(
        tmp_path / "artifacts/data_refresh/tw_public/events/latest.json",
        {"datasets": {name: {
            "last_probe_status": "ok", "observed_version": "v1",
            "applied_version": "v1", "last_download_status": "ok",
        }}},
    )
    rows = _tw_public_sources(tmp_path, now=datetime(2026, 9, 17, tzinfo=UTC))
    row = next(item for item in rows if item["id"] == f"tw-public:{name}")
    assert row["status"] == "waiting"
    assert row["status_label"] == "live 已下載，待完整批次稽核"
    assert row["data_through"] is None


def test_full_scan_does_not_cover_later_source_version(tmp_path: Path) -> None:
    live = tmp_path / "live"
    _write_json(tmp_path / "configs/data_sync/packed_datasets.json", {
        "datasets": [{"dataset": "tw-public", "source": str(live)}],
    })
    _write_json(live / "download_summary.json", {
        "dataset_count": 1, "coverage_complete": True,
        "blocking_failed_count": 0, "incomplete_count": 0,
        "missing_dates_after": 0, "end_date": "2026-09-16",
        "generated_at_utc": "2026-09-16T16:35:00+00:00",
    })
    _write_json(tmp_path / "artifacts/data_refresh/tw_public/events/latest.json", {
        "registered_dataset_count": 1,
        "datasets": {"changed": {"last_applied_at_taipei": "2026-09-17T00:38:00+08:00"}},
    })
    progress = build_tw_public_acquisition_progress(tmp_path)
    assert progress["full_batch"]["coverage_complete"] is True
    assert progress["full_batch"]["current_versions_complete"] is False
    assert progress["full_batch"]["changed_after_batch"] == 1

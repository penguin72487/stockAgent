"""Small, public-safe evidence chain for the TW public acquisition plan."""

from __future__ import annotations

from datetime import UTC, datetime
import gzip
import hashlib
import io
import json
import csv
import os
import time
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping
from uuid import uuid4


ADDED_DATASETS = (
    ("sitca_domestic_fund_monthly_report_index", "境內基金月報索引"),
    ("sitca_domestic_fund_basic", "境內基金基本資料"),
    ("twse_etf_fund_basic", "ETF 基本資料"),
)


def _object(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _utc(value: object) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC).isoformat() if parsed.tzinfo else None


def _background_catalog_progress(
    live: Path, name: str, cold_raw_files: int = 0,
    last_poll_at_utc: str | None = None,
) -> dict[str, Any]:
    state = _object(live / "state" / f"{name}.json")
    datasets = state.get("datasets")
    datasets = datasets if isinstance(datasets, dict) else {}
    completed = sum(isinstance(item, dict) and item.get("complete") is True
                    for item in datasets.values())
    resources = sum(len(item.get("resources", {})) for item in datasets.values()
                    if isinstance(item, dict) and isinstance(item.get("resources"), dict))
    no_csv = sum(item.get("complete") is True and not item.get("resources")
                 for item in datasets.values() if isinstance(item, dict))
    failed_names = [str(item.get("name") or dataset_id)
                    for dataset_id, item in datasets.items()
                    if isinstance(item, dict) and item.get("complete") is False][:3]
    return {
        "catalog_entries": state.get("catalog_entry_count"),
        "checked_entries": len(datasets),
        "complete_entries": completed,
        "saved_resources": resources,
        "no_csv_entries": no_csv,
        "cold_inventory_raw_files": cold_raw_files,
        "failed_entries": len(datasets) - completed,
        "failed_names": failed_names,
        "last_catalog_check_utc": _utc(state.get("last_catalog_check_utc")),
        "last_poll_at_utc": last_poll_at_utc,
        "inventory_present": (live / f"{name}.parquet").is_file(),
    }


@lru_cache(maxsize=8)
def _verified_inventory_membership(
    inventory_path: str,
    expected_sha256: str,
    file_identity: tuple[int, int, int, int, int],
    verification_hour: int,
) -> tuple[frozenset[str], tuple[tuple[str, int], ...]]:
    """Hash and parse an immutable inventory once per identity and hour.

    The caller checks the current head, manifest, path and file identity on
    every snapshot.  Hourly rehash also bounds undetected in-place corruption
    whose metadata was somehow preserved.
    """

    del file_identity, verification_hour
    compressed = Path(inventory_path).read_bytes()
    if hashlib.sha256(compressed).hexdigest() != expected_sha256:
        raise ValueError("cold inventory SHA-256 mismatch")
    wanted = {
        path
        for name, _ in ADDED_DATASETS
        for path in (f"{name}.parquet", f"metadata/{name}.json")
    }
    found: set[str] = set()
    catalog_raw_files = {"gcis_open_data_catalog": 0, "fsc_open_data_catalog": 0}
    with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as rows:
        for row in rows:
            item = json.loads(row)
            path = item.get("path")
            if item.get("kind") != "file" or not isinstance(path, str):
                continue
            if path in wanted:
                found.add(path)
            for name in catalog_raw_files:
                if path.startswith(f"raw/{name}/"):
                    catalog_raw_files[name] += 1
    return frozenset(found), tuple(sorted(catalog_raw_files.items()))


def _cached_inventory_membership(
    inventory_path: Path,
    expected_sha256: str,
    file_identity: tuple[int, int, int, int, int],
    verification_hour: int,
) -> tuple[frozenset[str], tuple[tuple[str, int], ...]]:
    """Persist the small proof summary for the short-lived snapshot process.

    Only the dedicated snapshot service opts in to disk caching.  A changed
    head hash, file identity, or hour misses immediately and rehashes the
    immutable inventory before another verified status can be reported.
    """

    cache_name = os.environ.get("STOCKAGENT_COLD_INVENTORY_CACHE_PATH")
    cache_path = Path(cache_name) if cache_name else None
    key = {
        "schema_version": 1,
        "inventory_path": str(inventory_path),
        "sha256": expected_sha256,
        "file_identity": list(file_identity),
        "verification_hour": verification_hour,
    }
    if cache_path is not None:
        cached = _object(cache_path)
        if all(cached.get(field) == value for field, value in key.items()):
            found = cached.get("found")
            counts = cached.get("raw_file_counts")
            allowed = {
                path
                for name, _ in ADDED_DATASETS
                for path in (f"{name}.parquet", f"metadata/{name}.json")
            }
            expected_names = {"gcis_open_data_catalog", "fsc_open_data_catalog"}
            if (
                isinstance(found, list)
                and all(isinstance(path, str) and path in allowed for path in found)
                and isinstance(counts, dict)
                and set(counts) == expected_names
                and all(type(value) is int and value >= 0 for value in counts.values())
            ):
                return frozenset(found), tuple(sorted(counts.items()))
    result = _verified_inventory_membership(
        str(inventory_path), expected_sha256, file_identity, verification_hour
    )
    if cache_path is not None:
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            temporary = cache_path.with_name(f".{cache_path.name}.{uuid4().hex}.tmp")
            try:
                temporary.write_text(
                    json.dumps({
                        **key,
                        "found": sorted(result[0]),
                        "raw_file_counts": dict(result[1]),
                    }, separators=(",", ":")),
                    encoding="utf-8",
                )
                os.replace(temporary, cache_path)
            finally:
                temporary.unlink(missing_ok=True)
        except OSError:
            # A cache write failure does not weaken inventory verification.
            pass
    return result


def _latest_cold_inventory(cold_root: Path) -> dict[str, Any]:
    """Verify the head, manifest and inventory before claiming file membership.

    This proves that paths were listed in the immutable release inventory. It
    intentionally does not claim that every packed object or peer is verified.
    """

    head = _object(cold_root / "heads/tw-public/penguin.json")
    manifest_rel = head.get("manifest_relpath")
    expected_manifest_sha = head.get("manifest_sha256")
    if not isinstance(manifest_rel, str) or not isinstance(expected_manifest_sha, str):
        return {"state": "unverified"}
    try:
        manifest_path = (cold_root / manifest_rel).resolve(strict=True)
        manifest_path.relative_to(cold_root.resolve(strict=True))
        manifest_bytes = manifest_path.read_bytes()
        if hashlib.sha256(manifest_bytes).hexdigest() != expected_manifest_sha:
            return {"state": "unverified"}
        manifest = json.loads(manifest_bytes)
        if manifest.get("dataset") != "tw-public" or manifest.get("snapshot_id") != head.get("snapshot_id"):
            return {"state": "unverified"}
        inventory = manifest["archive"]["inventory"]
        inventory_rel = inventory["relpath"]
        expected_inventory_sha = inventory["sha256"]
        inventory_path = (cold_root / inventory_rel).resolve(strict=True)
        inventory_path.relative_to(cold_root.resolve(strict=True))
        inventory_stat = inventory_path.stat()
        if inventory_stat.st_size > 64 * 1024 * 1024:
            return {"state": "unverified"}
        found, raw_file_counts = _cached_inventory_membership(
            inventory_path,
            expected_inventory_sha,
            (
                inventory_stat.st_dev,
                inventory_stat.st_ino,
                inventory_stat.st_size,
                inventory_stat.st_mtime_ns,
                inventory_stat.st_ctime_ns,
            ),
            int(time.time() // 3600),
        )
        metadata = manifest.get("metadata")
        metadata = metadata if isinstance(metadata, dict) else {}
        return {
            "state": "inventory_verified",
            "snapshot_id": manifest.get("snapshot_id"),
            "manifest_sha256": expected_manifest_sha,
            "published_at_utc": _utc(manifest.get("published_at")),
            "data_through": metadata.get("freshness_value"),
            "freshness_receipt_sha256": metadata.get("freshness_receipt_sha256"),
            "included_datasets": [
                name for name, _ in ADDED_DATASETS
                if f"{name}.parquet" in found and f"metadata/{name}.json" in found
            ],
            "background_catalog_raw_files": dict(raw_file_counts),
        }
    except (OSError, ValueError, TypeError, KeyError, gzip.BadGzipFile, EOFError):
        return {"state": "unverified"}


def _latest_model_audit(
    root: Path, cold: Mapping[str, Any], materialization: Mapping[str, Any],
) -> dict[str, Any]:
    candidates = [
        *root.glob("artifacts/data_quality/tw_public*/summary.json"),
        *root.glob("artifacts/data_refresh/tw_public/0830/audit/*/summary.json"),
    ]
    try:
        path = max((item for item in candidates if item.is_file()), key=lambda item: item.stat().st_mtime_ns)
        modified = datetime.fromtimestamp(path.stat().st_mtime, UTC).isoformat()
    except (OSError, ValueError):
        return {"state": "not_run"}
    payload = _object(path)
    counts = payload.get("finding_counts")
    if not isinstance(payload.get("model_safe"), bool) or not isinstance(counts, dict):
        return {"state": "invalid"}
    availability = payload.get("feature_availability")
    availability = availability if isinstance(availability, dict) else {}
    snapshot = cold.get("snapshot_id")
    panel = payload.get("panel")
    panel = panel if isinstance(panel, dict) else {}
    materialized_root = Path(os.environ.get(
        "STOCKAGENT_MATERIALIZED_ROOT", "/srv/stockagent-packed-materialized"
    ))
    audited_snapshot_id: str | None = None
    pinned_release_ready = False
    parquet_path = panel.get("parquet_root")
    feature_path = panel.get("public_feature_path")
    if isinstance(parquet_path, str) and isinstance(feature_path, str):
        stocks = Path(parquet_path).resolve()
        release_root = stocks.parent
        if (
            stocks.name == "stocks"
            and release_root.parent == (materialized_root / "tw-public").resolve()
            and Path(feature_path).resolve()
            == (release_root / "features/tw_public_stock_daily.parquet").resolve()
            and Path(release_root.name).name == release_root.name
        ):
            audited_snapshot_id = release_root.name
            ready = _object(release_root.parent / f".{audited_snapshot_id}.READY.json")
            pin = _object(materialized_root / "tw-public-training-20260917.pin.json")
            pin_manifest = pin.get("manifest")
            pin_manifest = pin_manifest if isinstance(pin_manifest, dict) else {}
            pinned_release_ready = bool(
                release_root.is_dir()
                and ready.get("snapshot_id") == audited_snapshot_id
                and isinstance(ready.get("manifest_sha256"), str)
                and pin.get("manifest_sha256") == ready.get("manifest_sha256")
                and pin_manifest.get("snapshot_id") == audited_snapshot_id
            )
    release_root = materialized_root / "tw-public" / str(snapshot or "")
    matches_current_release = bool(
        isinstance(snapshot, str)
        and materialization.get("state") == "ready_receipt"
        and panel.get("parquet_root")
        and panel.get("public_feature_path")
        and Path(str(panel["parquet_root"])).resolve() == (release_root / "stocks").resolve()
        and Path(str(panel["public_feature_path"])).resolve()
        == (release_root / "features/tw_public_stock_daily.parquet").resolve()
    )
    return {
        "state": "reported",
        "model_safe": payload["model_safe"],
        "finding_counts": {name: counts.get(name) for name in ("critical", "high")},
        "observed_at_utc": modified,
        "config": Path(str(payload.get("config") or "")).name,
        "matches_current_release": matches_current_release,
        "audited_snapshot_id": audited_snapshot_id,
        "pinned_release_ready": pinned_release_ready,
        "feature_count": payload.get("feature_count") if isinstance(payload.get("feature_count"), int) else None,
        "research_only_same_close": availability.get("allow_same_close_feature_approximation") is True,
    }


def _materialization_status(cold: Mapping[str, Any]) -> dict[str, Any]:
    snapshot = cold.get("snapshot_id")
    if not isinstance(snapshot, str) or not snapshot or Path(snapshot).name != snapshot:
        return {"state": "not_started"}
    materialized_root = Path(os.environ.get(
        "STOCKAGENT_MATERIALIZED_ROOT", "/srv/stockagent-packed-materialized"
    )) / "tw-public"
    ready = _object(materialized_root / f".{snapshot}.READY.json")
    if (
        ready.get("snapshot_id") == snapshot
        and ready.get("manifest_sha256") == cold.get("manifest_sha256")
        and (materialized_root / snapshot).is_dir()
    ):
        return {"state": "ready_receipt", "verified_at_utc": _utc(ready.get("verified_at"))}
    if any(materialized_root.glob(f".{snapshot}.partial.*")):
        return {"state": "materializing"}
    return {"state": "not_started"}


def build_tw_public_acquisition_progress(
    repo_root: Path, *, now: datetime | None = None,
    next_full_scan_at_utc: str | None = None,
) -> dict[str, Any]:
    """Report independent observation, local, batch and cold-release gates."""

    root = Path(repo_root)
    catalog = _object(root / "configs/data_sync/packed_datasets.json")
    config = next(
        (item for item in catalog.get("datasets", [])
         if isinstance(item, dict) and item.get("dataset") == "tw-public"),
        {},
    )
    source = config.get("source")
    live = Path(source) if isinstance(source, str) and source else root / "data_tw_public"
    if not live.is_absolute():
        live = root / live
    event = _object(root / "artifacts/data_refresh/tw_public/events/latest.json")
    observed_now = (now or datetime.now(UTC)).astimezone(UTC)
    event_updated = _utc(event.get("updated_at_taipei"))
    event_age = (
        (observed_now - datetime.fromisoformat(event_updated)).total_seconds()
        if event_updated else None
    )
    event_rows = event.get("datasets") if isinstance(event.get("datasets"), dict) else {}
    summary = _object(live / "download_summary.json")
    catalog_run = _object(
        root / "artifacts/data_refresh/tw_public/catalogs/latest/download_summary.json"
    )
    catalog_poll = (
        _utc(catalog_run.get("generated_at_utc"))
        if catalog_run.get("dataset_count") == 2 else None
    )
    report_rows: dict[str, Mapping[str, str]] = {}
    try:
        with (live / "download_report.csv").open(encoding="utf-8", newline="") as handle:
            report_rows = {
                str(row.get("dataset") or ""): row for row in csv.DictReader(handle)
            }
    except (OSError, UnicodeError, csv.Error):
        pass
    backup = _object(root / "configs/data_sync/packed_backup.json")
    cold_path = backup.get("source")
    cold = _latest_cold_inventory(Path(cold_path)) if isinstance(cold_path, str) and cold_path else {"state": "unverified"}
    cold_names = set(cold.get("included_datasets", []))
    registered = event.get("registered_dataset_count")
    observed_count = event.get("observed_dataset_count")
    formal_count = summary.get("dataset_count")
    formal_ok = (
        isinstance(registered, int) and isinstance(formal_count, int)
        and formal_count >= registered and summary.get("coverage_complete") is True
        and summary.get("blocking_failed_count") == 0
        and summary.get("incomplete_count") == 0
        and summary.get("missing_dates_after") == 0
    )
    batch_generated = _utc(summary.get("generated_at_utc"))
    changed_after_batch = sum(
        isinstance(row, dict)
        and (_utc(row.get("last_applied_at_taipei")) or "") > (batch_generated or "")
        for row in event_rows.values()
    ) if batch_generated else 0
    current_versions_complete = bool(
        formal_ok and batch_generated and len(event_rows) >= registered
        and changed_after_batch == 0
    )
    try:
        parsed_manifest = json.loads((live / "dataset_manifest.json").read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError):
        parsed_manifest = []
    source_rows = parsed_manifest if isinstance(parsed_manifest, list) else []
    snapshot_count = sum(
        isinstance(item, dict) and (
            item.get("kind") == "snapshot_url"
            or (item.get("kind") == "data_gov" and "snapshot" in (item.get("tags") or []))
        )
        for item in source_rows
    )
    dgbas = _object(live / "state/dgbas_release_vintages.json")
    cbc = _object(live / "state/cbc_fx_reserve_release_vintages.json")
    mof_original = _object(live / "state/mof_original_release_archive.json")
    mof_index = _object(live / "state/mof_macro_release_dates.json")
    mof_raw_root = live / "raw/mof_original_release_archive"
    mof_raw_periods = len({path.parent for path in mof_raw_root.glob("*/*/original-*.pdf")})
    daily_names = [str(item.get("name")) for item in source_rows
                   if isinstance(item, dict) and item.get("kind") == "historical_json_table"
                   and isinstance(item.get("name"), str)]
    daily_states = [_object(live / "state" / f"{name}.json") for name in daily_names]
    daily_complete = sum(
        state.get("coverage_complete") is True and state.get("missing_dates_after") == 0
        for state in daily_states
    )
    common_end = min((str(state.get("coverage_end")) for state in daily_states
                      if state.get("coverage_end")), default=None)
    mof_gaps = mof_original.get("index_period_gaps") or {}
    mof_gap_count = sum(len(value) for value in mof_gaps.values() if isinstance(value, list))
    mof_fallback = mof_index.get("tax_pdf_fallback")
    if isinstance(mof_fallback, dict) and (
        mof_original.get("indexed_releases") != mof_index.get("distinct_release_periods")
    ):
        mof_gap_count = (len(mof_fallback.get("not_found") or [])
                         + len(mof_fallback.get("failures") or []))
    try:
        batch_receipt_sha256 = hashlib.sha256((live / "download_summary.json").read_bytes()).hexdigest()
    except OSError:
        batch_receipt_sha256 = None
    cold["matches_full_batch"] = bool(
        current_versions_complete and cold.get("state") == "inventory_verified"
        and cold.get("data_through") == summary.get("end_date")
        and cold.get("freshness_receipt_sha256") == batch_receipt_sha256
    )
    datasets = []
    for name, title in ADDED_DATASETS:
        row = event_rows.get(name, {})
        row = row if isinstance(row, dict) else {}
        observed = row.get("last_probe_status") == "ok" and bool(row.get("observed_version"))
        applied = (
            observed
            and row.get("observed_version") == row.get("applied_version")
            and row.get("last_download_status") == "ok"
        )
        raw_dir = live / "raw" / name
        raw_present = raw_dir.is_dir() and any(item.is_file() for item in raw_dir.iterdir())
        local = bool(applied and (live / f"{name}.parquet").is_file()
                     and (live / "metadata" / f"{name}.json").is_file() and raw_present)
        audit = report_rows.get(name, {})
        audited = (
            formal_ok and str(audit.get("status") or "").lower()
            in {"ok", "up_to_date", "complete"}
            and str(audit.get("failed_dates") or "0") == "0"
            and str(audit.get("missing_dates_after") or "0") == "0"
            and str(audit.get("coverage_complete") or "").lower() != "false"
        )
        datasets.append({
            "id": name,
            "title": title,
            "observed": bool(observed),
            "live_downloaded": local,
            "in_full_batch": bool(audited),
            "in_cold_inventory": name in cold_names,
            "last_applied_at_utc": _utc(row.get("last_applied_at_taipei")) if local else None,
        })
    materialization = _materialization_status(cold)
    model_audit = _latest_model_audit(root, cold, materialization)
    return {
        "scope": "tw_public_official_acquisition",
        "observation": {
            "registered": registered if isinstance(registered, int) else None,
            "observed": observed_count if isinstance(observed_count, int) else None,
            "failed_probes": event.get("failed_probe_count"),
            "unapplied_events": event.get("blocking_unapplied_event_count"),
            "cycle_completed_at": event.get("cycle_completed_at_taipei"),
            "updated_at_utc": event_updated,
            "fresh": event_age is not None and -60 <= event_age <= 15 * 60,
            "refresh_in_progress": event.get("refresh_in_progress") is True,
        },
        "full_batch": {
            "dataset_count": formal_count if isinstance(formal_count, int) else None,
            "coverage_complete": formal_ok,
            "current_versions_complete": current_versions_complete,
            "changed_after_batch": changed_after_batch,
            "generated_at_utc": batch_generated,
            "data_through": summary.get("end_date"),
            "next_scheduled_at_utc": _utc(next_full_scan_at_utc),
        },
        "cold_release": cold,
        "materialization": materialization,
        "model_audit": model_audit,
        "training_readiness": {
            "all_source_history_ready": False,
            "research_subset_ready": bool(
                model_audit.get("model_safe")
                and (model_audit.get("matches_current_release") or model_audit.get("pinned_release_ready"))
            ),
            "reason": "all_source_historical_vintages_incomplete",
            "current_snapshot_sources": snapshot_count,
            "dgbas_saved_releases": dgbas.get("saved_releases"),
            "dgbas_registered_releases": dgbas.get("registered_releases"),
            "dgbas_complete": dgbas.get("complete") is True,
            "cbc_complete": cbc.get("complete") is True,
        },
        "history_backfill": {
            "official_daily_complete": daily_complete,
            "official_daily_total": len(daily_names),
            "official_daily_common_end": common_end if daily_complete == len(daily_names)
                                         and daily_names else None,
            "mof_archived_releases": mof_original.get("archived_releases"),
            "mof_indexed_releases": mof_index.get("distinct_release_periods")
                                    or mof_original.get("indexed_releases"),
            "mof_raw_periods": mof_raw_periods,
            "mof_index_period_gaps": mof_gap_count,
            "mof_status": (
                mof_original.get("status")
                if mof_original.get("indexed_releases") == mof_index.get("distinct_release_periods")
                else "backfilling" if mof_raw_periods else "not_started"
            ) or "not_started",
            "mof_updated_at_utc": _utc(mof_original.get("generated_at_utc")),
        },
        "background_catalogs": {
            "gcis": _background_catalog_progress(
                live, "gcis_open_data_catalog",
                int(cold.get("background_catalog_raw_files", {}).get("gcis_open_data_catalog", 0)),
                catalog_poll,
            ),
            "fsc": _background_catalog_progress(
                live, "fsc_open_data_catalog",
                int(cold.get("background_catalog_raw_files", {}).get("fsc_open_data_catalog", 0)),
                catalog_poll,
            ),
        },
        "datasets": datasets,
        "next_work": [
            {"id": "mops_history", "title": "MOPS 逐期歷史批次與更正版本", "state": "source_pilot_pending"},
            {"id": "fund_holdings", "title": "基金前十大、主動式 ETF 實際持股與 PCF", "state": "source_pilot_pending"},
            {"id": "toalpha_unique", "title": "ToAlpha 單次研究查詢", "state": "on_demand_only"},
        ],
        "definitions": {
            "observed": "來源探測成功，不代表完整下載。",
            "live_downloaded": "來源版本已套用且原始檔、metadata、Parquet 均在 live 工作目錄。",
            "full_batch": "完整批次需涵蓋目前已註冊來源且缺口稽核通過；舊批次不代表新來源已驗收。",
            "cold_release": "只驗證本機最新冷庫 head、manifest 與 inventory 及列名；不代表物件逐一重建、對端同步或熱資料可用。",
            "materialization": "READY 收據由固定 release 的完整物件及還原檔案驗證後產生；頁面不會逐次重讀所有檔案。",
            "training": "全來源歷史訓練需逐期原始版本、公告時間和模型稽核；當期快照不可倒填歷史。",
            "model_audit": "稽核輸入須與 READY release 相符；若最新冷庫版更新，受 pin 保護的舊版研究稽核仍獨立有效。",
            "history": "三份新增檔案是首次觀測後的當期快照，不能倒填首次觀測前的歷史版本。",
        },
    }

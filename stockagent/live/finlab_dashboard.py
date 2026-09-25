"""Credential-free projection for the public FinLab data and quota page."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import statistics
from typing import Any, Mapping

from scripts.finlab_release_gate import catalog_readiness
from scripts.download_finlab_history import safe_stem
from scripts.snapshot_finlab_quota import load_quota_history


STAGE_DATASET = "tw-public-research-finlab-2014-v4"


def _public_quota_series(history: list[dict], now: datetime) -> list[dict]:
    """Keep every recent sample, and bound older chart points by time buckets."""

    cutoff = now - timedelta(days=30)
    recent_cutoff = now - timedelta(days=1)
    older: dict[int, dict] = {}
    recent: list[dict] = []
    for row in history:
        timestamp = _utc(row.get("observed_at_utc"))
        used = row.get("used_mb")
        limit = row.get("limit_mb")
        if (timestamp is None or not cutoff <= timestamp <= now
                or not isinstance(used, (int, float))
                or not isinstance(limit, (int, float)) or limit <= 0):
            continue
        point = {"observed_at_utc": timestamp.isoformat(), "used_mb": used, "limit_mb": limit}
        if timestamp >= recent_cutoff:
            recent.append(point)
        else:
            older[int(timestamp.timestamp() // (20 * 60))] = point
    return sorted([*older.values(), *recent], key=lambda row: row["observed_at_utc"])


def _read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, UnicodeError):
        return default


def _utc(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(UTC) if parsed.tzinfo is not None else None
    except ValueError:
        return None


def _age_seconds(value: object, now: datetime) -> int | None:
    observed = _utc(value)
    return max(0, int((now - observed).total_seconds())) if observed else None


def _positive_seconds(value: object) -> float | None:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    return float(value) if 0 < value < 86_400 else None


def _per_key_estimates(root: Path, datasets: list[dict[str, Any]],
                       now: datetime) -> None:
    """Use measured full SDK fetches, not count/time extrapolation or quota MB."""
    samples: dict[str, list[float]] = {}
    receipts: dict[str, Mapping[str, Any]] = {}
    for row in datasets:
        key = row["key"]
        receipt = _read_json(root / "data_finlab/receipts" / f"{safe_stem(key)}.json", {})
        if (not isinstance(receipt, Mapping) or receipt.get("dataset") != key
                or receipt.get("status") != "downloaded_unverified_for_pit"
                or row.get("state") != "downloaded"):
            continue
        receipts[key] = receipt
        seconds = _positive_seconds(receipt.get("last_fetch_elapsed_seconds"))
        if seconds is not None:
            samples.setdefault(str(row.get("category") or ""), []).append(seconds)
    active = _read_json(root / "data_finlab/runs/latest.json", {})
    active = active if isinstance(active, Mapping) and active.get("state") == "running" else {}
    for row in datasets:
        key = row["key"]
        receipt = receipts.get(key, {})
        own = _positive_seconds(receipt.get("last_fetch_elapsed_seconds"))
        group = samples.get(str(row.get("category") or ""), [])
        # Three independent completed keys prevent a single unusually small
        # table from defining a family-wide forecast.
        if own is not None:
            low = high = own
            basis, sample_count = "same_key_previous_fetch", 1
        elif len(group) >= 3:
            ordered = sorted(group)
            low = statistics.median(ordered)
            high = ordered[(9 * len(ordered) - 1) // 10]
            basis, sample_count = "same_category_measured", len(group)
        else:
            low = high = None
            basis, sample_count = "insufficient_measured_samples", len(group)
        checked = _utc(receipt.get("source_checked_at_utc"))
        row["source_checked_at_utc"] = checked.isoformat() if checked else None
        relative = Path(str(receipt.get("parquet_path") or ""))
        data_path = (root / "data_finlab" / relative).resolve()
        row["local_bytes"] = (
            data_path.stat().st_size if row.get("state") == "downloaded"
            and not relative.is_absolute() and relative.parts[:1] == ("datasets",)
            and data_path.is_relative_to((root / "data_finlab").resolve())
            and data_path.is_file() else None
        )
        if row.get("state") == "partial_windowed":
            row["local_bytes"] = row.get("partition_bytes")
        row["latest_check_within_24h"] = bool(
            receipt.get("source_check_mode") == "upstream_forced"
            and checked and timedelta(0) <= now - checked <= timedelta(hours=24)
        )
        row["observed_last_fetch_seconds"] = round(own, 1) if own is not None else None
        row["estimated_fetch_seconds_low"] = round(low, 1) if low is not None else None
        row["estimated_fetch_seconds_high"] = round(high, 1) if high is not None else None
        row["estimate_basis"] = basis
        row["estimate_samples"] = sample_count
        row["estimated_finish_at_utc"] = None
        row["estimated_remaining_seconds"] = None
        row["active"] = active.get("active_key") == key
        row["running_elapsed_seconds"] = None
        if row["active"]:
            started = _utc(active.get("active_started_at_utc"))
            if started and timedelta(0) <= now - started:
                row["running_elapsed_seconds"] = round((now - started).total_seconds())
                if high is not None and now - started < timedelta(seconds=high):
                    row["estimated_remaining_seconds"] = round(high - (now - started).total_seconds())
                    row["estimated_finish_at_utc"] = (started + timedelta(seconds=high)).isoformat()


def _volume_estimate(datasets: list[dict[str, Any]],
                     expected_catalog_keys: int | None) -> dict[str, Any]:
    """Estimate current Parquet bytes, separately from SDK traffic and completion.

    The catalogue does not publish per-key sizes.  Missing keys use the median
    of measured files in their category, then the global median if that category
    has no sample.  The p90 scenario is illustrative, never an upper bound.
    """
    groups: dict[str, list[int]] = {}
    measured = 0
    measured_files = 0
    for row in datasets:
        size = row.get("local_bytes")
        if row.get("state") == "downloaded" and isinstance(size, int) and size > 0:
            groups.setdefault(str(row.get("category") or ""), []).append(size)
            measured += size
            measured_files += 1
    samples = sorted(size for sizes in groups.values() for size in sizes)
    result: dict[str, Any] = {
        "unit": "local_parquet_bytes",
        "measured_bytes": measured,
        "measured_files": measured_files,
        "catalog_keys": len(datasets),
        "expected_catalog_keys": expected_catalog_keys,
        "catalog_verified": (
            type(expected_catalog_keys) is int
            and expected_catalog_keys > 0
            and expected_catalog_keys == len(datasets)
            and len({row["key"] for row in datasets}) == len(datasets)
        ),
        "unmeasured_keys": len(datasets) - measured_files,
        "same_category_estimated_keys": 0,
        "small_category_estimated_keys": 0,
        "global_estimated_keys": 0,
        "estimated_total_bytes": None,
        "estimated_remaining_bytes": None,
        "high_scenario_total_bytes": None,
        "estimated_coverage_ratio": None,
        "basis": "現有收據指向的本機 Parquet 檔案大小；未下載鍵依同類或全域已下載鍵大小中位數外推。",
    }
    if not samples or result["catalog_verified"] is False:
        return result
    ordered_groups = {category: sorted(sizes) for category, sizes in groups.items()}
    forecast = 0
    high_forecast = 0
    for row in datasets:
        size = row.get("local_bytes")
        if row.get("state") == "downloaded" and isinstance(size, int) and size > 0:
            continue
        category_samples = ordered_groups.get(str(row.get("category") or ""), [])
        if len(category_samples) >= 3:
            basis = "same_category_estimated_keys"
            selected = category_samples
        elif category_samples:
            basis = "small_category_estimated_keys"
            selected = category_samples
        else:
            basis = "global_estimated_keys"
            selected = samples
        result[basis] += 1
        forecast += round(statistics.median(selected))
        high_forecast += selected[(9 * len(selected) - 1) // 10]
    result["estimated_remaining_bytes"] = forecast
    result["estimated_total_bytes"] = measured + forecast
    result["high_scenario_total_bytes"] = measured + high_forecast
    result["estimated_coverage_ratio"] = (
        measured / (measured + forecast) if measured + forecast else None
    )
    return result


def _training_status(root: Path, staged_root: Path) -> dict[str, Any]:
    local_receipt = _read_json(
        root / "artifacts/research_features/tw_public_research_finlab_2014_v4.finlab_research.json",
        {},
    )
    staged_receipt = _read_json(
        staged_root / "release_receipt.json",
        {},
    )
    delivery = _read_json(root / "artifacts/live/finlab/training_delivery.json", {})
    if not isinstance(local_receipt, Mapping):
        local_receipt = {}
    if not isinstance(staged_receipt, Mapping):
        staged_receipt = {}
    if not isinstance(delivery, Mapping):
        delivery = {}
    staged = staged_receipt.get("status") == "complete" and staged_receipt.get("research_only") is True
    local_ready = local_receipt.get("research_only") is True and bool(local_receipt.get("output_sha256"))
    try:
        staged_receipt_hash = hashlib.sha256(
            (staged_root / "release_receipt.json").read_bytes()
        ).hexdigest()
    except OSError:
        staged_receipt_hash = None
    cold_verified = (
        staged
        and delivery.get("cold_objects_verified") is True
        and delivery.get("dataset") == STAGE_DATASET
        and delivery.get("feature_rows") == staged_receipt.get("feature_rows")
        and delivery.get("model_channels") == staged_receipt.get("model_channels")
        and delivery.get("base_feature_sha256") == staged_receipt.get("base_feature_sha256")
        and delivery.get("staged_receipt_sha256") == staged_receipt_hash
        and isinstance(delivery.get("snapshot_id"), str)
    )
    return {
        "state": "private_cold_verified" if cold_verified else "staged_for_private_cold" if staged else "local_research_table" if local_ready else "not_built",
        "research_only": True,
        "historical_point_in_time": False,
        "local_rows": local_receipt.get("rows") if local_ready else None,
        "local_finlab_channels": len(local_receipt.get("feature_columns") or ()) if local_ready else None,
        "local_source_keys": len(local_receipt.get("inputs", {}).get("sources") or ()) if local_ready else None,
        "staged_rows": staged_receipt.get("feature_rows") if staged else None,
        "staged_model_channels": staged_receipt.get("model_channels") if staged else None,
        "staged_end_date": staged_receipt.get("end_date") if staged else None,
        "cold_publication": "verified_exact_release" if cold_verified else "not_verified",
        "cold_snapshot_id": delivery.get("snapshot_id") if cold_verified else None,
        "cold_verified_at_utc": delivery.get("observed_at_utc") if cold_verified else None,
        "base_cold_feature_match": delivery.get("base_cold_feature_match") is True if cold_verified else False,
        "base_cold_snapshot_id": delivery.get("base_snapshot_id") if cold_verified else None,
        "remote_materialization": "not_verified",
        "basis": "本機研究表／分發區收據；遠端 READY 與實際訓練需另行核驗。",
    }


def build_finlab_public_status(
    repo_root: Path, *, now: datetime | None = None, staged_root: Path | None = None
) -> dict[str, Any]:
    """Read only sanitized receipts; never consult SDK or expose source frames."""

    root = Path(repo_root)
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    monitor = _read_json(root / "artifacts/live/data_monitor/public_status.json", {})
    if not isinstance(monitor, Mapping):
        monitor = {}
    if monitor and (
        monitor.get("read_only") is not True
        or monitor.get("production_control_possible") is not False
    ):
        raise ValueError("untrusted data-monitor snapshot")
    acquisition = monitor.get("finlab_acquisition", {})
    acquisition = acquisition if isinstance(acquisition, Mapping) else {}
    monitor_age = _age_seconds(monitor.get("generated_at_utc"), observed)
    all_rows = monitor.get("sources") if isinstance(monitor.get("sources"), list) else []
    datasets: list[dict[str, Any]] = []
    public_states = {
        "downloaded", "pending", "deferred_resource", "deferred_windowed", "partial_windowed",
        "provider_error", "provider_empty", "resource_timeout", "vip_only",
        "authentication_failed", "quota_wait",
    }
    public_attempts = {
        "provider_error", "provider_empty", "timed_out", "vip_only",
        "authentication_failed", "quota_exhausted",
    }
    public_deferred_reasons = {
        "oversized_metadata", "oversized_table", "oversized_wide_refresh", "requires_date_window",
    }
    for item in all_rows:
        if not isinstance(item, Mapping):
            continue
        row_id = str(item.get("id") or "")
        if not row_id.startswith("finlab:") or row_id == "finlab:catalog-acquisition":
            continue
        stats = item.get("record_stats") if isinstance(item.get("record_stats"), Mapping) else {}
        source_state = item.get("finlab_acquisition_state")
        deferred_reason = item.get("finlab_deferred_reason")
        attempt_status = item.get("finlab_attempt_status")
        attempted = _utc(item.get("finlab_last_attempt_at_utc"))
        retry = _utc(item.get("finlab_next_retry_at_utc"))
        provider_rows = item.get("finlab_provider_rows")
        provider_fields = item.get("finlab_provider_fields")
        datasets.append({
            "key": row_id.removeprefix("finlab:"),
            "category": item.get("category"),
            "state": source_state if isinstance(source_state, str) and source_state in public_states else "pending",
            "deferred_reason": (
                deferred_reason if isinstance(deferred_reason, str)
                and deferred_reason in public_deferred_reasons else None
            ),
            "attempt_status": (
                attempt_status if isinstance(attempt_status, str)
                and attempt_status in public_attempts else None
            ),
            "provider_rows": provider_rows if type(provider_rows) is int and 0 <= provider_rows <= 10**12 else None,
            "provider_fields": provider_fields if type(provider_fields) is int and 0 <= provider_fields <= 10**12 else None,
            "partition_receipts": (item.get("finlab_intraday_partitions") or {}).get("receipted_partitions"),
            "partition_total": (item.get("finlab_intraday_partitions") or {}).get("requested_weekday_partitions"),
            "partition_bytes": (item.get("finlab_intraday_partitions") or {}).get("parquet_bytes"),
            "partition_not_ready": (item.get("finlab_intraday_partitions") or {}).get("provider_not_ready_partitions"),
            "partition_other_failed": (item.get("finlab_intraday_partitions") or {}).get("other_failed_partitions"),
            "last_attempt_at_utc": attempted.isoformat() if attempted else None,
            "next_retry_at_utc": retry.isoformat() if retry else None,
            "rows": stats.get("count"),
            "first": stats.get("first"),
            "last": stats.get("last"),
            "fetched_at_utc": item.get("latest_at_utc"),
        })
    _per_key_estimates(root, datasets, observed)
    order = {
        "resource_timeout": 1, "provider_error": 1, "provider_empty": 1,
        "authentication_failed": 1,
        "vip_only": 1, "quota_wait": 1, "deferred_windowed": 2,
        "partial_windowed": 2,
        "deferred_resource": 2, "pending": 2, "downloaded": 4,
    }
    datasets.sort(key=lambda row: (
        0 if row["active"] else 3 if row["state"] == "downloaded" and not row["latest_check_within_24h"]
        else order.get(row["state"], 5), str(row.get("category") or ""), row["key"],
    ))
    readiness = catalog_readiness(root / "data_finlab", now=observed)
    raw_bytes = sum(row["local_bytes"] or 0 for row in datasets)
    raw_files = sum(row["local_bytes"] is not None for row in datasets)
    volume_estimate = _volume_estimate(datasets, acquisition.get("catalog_total"))
    overlay_path = root / "artifacts/research_features/tw_public_research_finlab_2014_v4.parquet"
    try:
        overlay_bytes = overlay_path.stat().st_size
    except OSError:
        overlay_bytes = None
    try:
        disk_free = shutil.disk_usage(root / "data_finlab").free
    except OSError:
        disk_free = None

    quota_root = root / "artifacts/live/finlab"
    quota = _read_json(quota_root / "quota_latest.json", {})
    if not isinstance(quota, Mapping) or quota.get("schema_version") != 1:
        quota = {}
    quota_age = _age_seconds(quota.get("observed_at_utc"), observed)
    # Raw minute observations remain local. The public 30-day chart keeps
    # every recent minute and one last observation per older 20-minute bucket.
    try:
        series = _public_quota_series(load_quota_history(quota_root, now=observed), observed)
        history_state = "available"
    except (OSError, sqlite3.DatabaseError):
        series = []
        history_state = "unavailable"

    last_success_age = _age_seconds(acquisition.get("last_receipt_at_utc"), observed)
    if monitor_age is None or monitor_age > 180:
        health = "stale"
    elif quota_age is None or quota_age > 180 or history_state == "unavailable":
        health = "degraded"
    elif acquisition.get("service_active") and last_success_age is not None and last_success_age > 900:
        health = "waiting"
    elif acquisition.get("state") in {"service_failed", "catalog_unknown"}:
        health = "degraded"
    else:
        health = "active"
    safe_quota = {key: quota[key] for key in (
        "observed_at_utc", "used_mb", "limit_mb", "remaining_mb", "used_ratio", "reset_at_utc",
    ) if key in quota}
    safe_acquisition = {key: acquisition[key] for key in (
        "state", "catalog_total", "downloaded", "not_downloaded", "deferred_resource",
        "not_downloaded_by_reason", "failed_or_entitlement", "recent_downloaded_15m", "last_receipt_at_utc",
        "next_run_at_utc", "ratio", "service_active", "timer_active", "quota_reserve_mb",
    ) if key in acquisition}
    running_receipt = _read_json(root / "data_finlab/runs/latest.json", {})
    market_raw = _read_json(root / "data_finlab/intraday/market_status.json", {})
    market_status = {}
    if isinstance(market_raw, Mapping) and market_raw.get("schema_version") in (1, 2):
        market_status = {key: market_raw.get(key) for key in (
            "observed_at_utc", "target_start_date", "target_end_date",
            "session_basis", "universe_basis", "universe_symbols",
            "ordinary_stock_symbols", "tdr_symbols",
            "current_symbols", "former_symbols", "missing_daily_bounds",
            "pre_horizon_former_symbols",
            "attempted_this_run", "successes_this_run", "state",
            "completion_eta", "completion_eta_reason", "acquisition_mode",
            "legacy_direct_minute_partitions",
        )}
        market_status["by_kind"] = market_raw.get("by_kind", {})
        market_status["daily_price_coverage"] = market_raw.get("daily_price_coverage", {})
        market_status["symbols"] = [
            {key: row.get(key) for key in (
                "key", "market", "security_type", "current", "delisting_date",
                "first_local_daily_date", "last_local_daily_date",
                "target_partitions", "receipted_partitions",
                "provider_not_ready_partitions", "other_failed_partitions",
                "rows", "parquet_bytes", "first_data_date", "last_data_date",
                "finlab_daily_close_rows", "source_kind",
            )}
            for row in market_raw.get("symbols", [])
            if isinstance(row, Mapping) and isinstance(row.get("key"), str)
            and row["key"].startswith(("tw_minute:", "tw_tick:"))
        ]
    current_fetch = {}
    if isinstance(running_receipt, Mapping) and running_receipt.get("state") == "running":
        active_key = running_receipt.get("active_key")
        if isinstance(active_key, str) and any(row["key"] == active_key for row in datasets):
            current_fetch = {
                "key": active_key,
                "started_at_utc": running_receipt.get("active_started_at_utc"),
            }
    return {
        "schema_version": 1,
        "generated_at_utc": observed.isoformat(),
        "read_only": True,
        "production_control_possible": False,
        "health": health,
        "monitor_age_seconds": monitor_age,
        "quota_age_seconds": quota_age,
        "quota": safe_quota,
        "quota_history": series,
        "quota_history_state": history_state,
        "quota_definition": {
            "unit": "MB as reported by FinLab SDK",
            "daily_limit_source": "account_get_data_status",
            "request_per_second_limit": None,
            "request_limit_basis": "FinLab public docs do not establish an account requests-per-second ceiling; do not infer 10 req/s.",
            "reset_basis": "Official CLI docs state 08:00 Asia/Taipei; reset timestamp is a schedule inference, not a provider response.",
        },
        "acquisition": safe_acquisition,
        "volume_estimate": volume_estimate,
        "current_fetch": current_fetch,
        "release_gate": readiness,
        "storage": {
            "raw_current_bytes": raw_bytes,
            "raw_current_files": raw_files,
            "research_table_bytes": overlay_bytes,
            "raw_filesystem_free_bytes": disk_free,
            "growth_bytes_per_day": None,
            "basis": "僅目前下載收據指向的本機 Parquet；不含舊版、SDK 快取或冷庫物件。日成長尚無容量快照樣本。",
        },
        "datasets": datasets,
        "intraday_market": market_status,
        "training": _training_status(
            root,
            staged_root or Path("/srv/stockagent-live/data_tw_public_research_finlab_2014_v4"),
        ),
    }

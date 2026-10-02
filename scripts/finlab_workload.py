"""Local-only, byte/record weighted FinLab workload snapshot.

This is monitoring, not another collector. Receipts own data/freshness and
shared policy owns ordering. Full raw sizes weight source-check progress;
measured SDK payloads predict transfer, with a conservative full-fetch fallback.
Neither is cumulative account billing. Footers are signature/time-budget cached.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
import fcntl
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.download_finlab_history import (  # noqa: E402
    NON_NUMERIC_DEFERRED_KEYS,
    _atomic_json, _sync_work_plan, has_local_download, load_catalog,
    persist_local_integrity_cache, quota_cycle_start, local_plan_reads, read_key_state,
)
from scripts.check_outside_tw_opening_resource_window import evaluate  # noqa: E402
from scripts.finlab_arrow_history import STREAMING_KEYS, whole_table_reserve_bytes  # noqa: E402
from scripts.finlab_stage_schedule import forecast, stage_summary  # noqa: E402
from stockagent.data.finlab_acquisition_contract import (  # noqa: E402
    QUOTA_POLICY_VERSION, UPSTREAM_CHECK_MODES, attempt_retry_at, incremental_quota_exempt,
    next_source_check, queue_stage, source_check_due,
)

CONTRACT = 3
MEASURE_CONTRACT = 2
MIB = 1024**2
GRAINS = {
    "wide_values": "寬表非空資料格（來源列 × 欄位）",
    "event_rows": "事件／明細列",
    "metadata_values": "來源標籤非空資料格",
}


def read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def timestamp(value) -> datetime | None:
    try:
        value = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return value.astimezone(UTC) if value.tzinfo else None
    except (TypeError, ValueError):
        return None


def number(value, *, positive=False):
    if (isinstance(value, bool) or not isinstance(value, (float, int))
            or not math.isfinite(value) or value < 0 or (positive and value == 0)):
        return None
    return value


def signature(path: Path) -> list[int] | None:
    try:
        s = path.stat()
        return [s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns]
    except OSError:
        return None


def local_object(root: Path, relative, prefix: str, *, resolved_root: Path | None = None) -> Path | None:
    p = Path(str(relative or ""))
    resolved = (root / p).resolve()
    if (p.is_absolute() or p.parts[:1] != (prefix,) or ".." in p.parts
            or not resolved.is_relative_to(resolved_root if resolved_root is not None else root.resolve())
            or not resolved.is_file()):
        return None
    return resolved


def record_measure(path: Path, receipt: dict) -> dict:
    """Read metadata only. Missing null statistics mean unknown, never zero."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    footer = pq.read_metadata(path)
    layout = receipt.get("storage_layout")
    if layout in {"provider_long_arrow", "sparse_long_raw_columns"}:
        return {"unit": "event_rows" if layout == "provider_long_arrow" else "metadata_values",
                "count": footer.num_rows, "stored_rows": footer.num_rows,
                "basis": "parquet_footer_rows"}
    excluded = set(receipt.get("source_index_columns") or ["source_index"])
    schema = footer.schema.to_arrow_schema()
    if len(schema) != footer.num_columns:
        return {"unit": "wide_values", "count": None, "stored_rows": footer.num_rows,
                "basis": "nested_field_grain_unverified"}
    columns = [i for i, name in enumerate(footer.schema.names) if name not in excluded]
    values = 0
    for group in range(footer.num_row_groups):
        rg = footer.row_group(group)
        for column in columns:
            if pa.types.is_null(schema.field(column).type):
                continue  # Arrow null type proves no values without statistics.
            stats = rg.column(column).statistics
            if stats is None or not stats.has_null_count:
                return {"unit": "wide_values", "count": None, "stored_rows": footer.num_rows,
                        "basis": "null_statistics_unavailable"}
            values += rg.num_rows - stats.null_count
    return {"unit": "wide_values", "count": values, "stored_rows": footer.num_rows,
            "basis": "parquet_footer_non_null_cells"}


def schedule_jobs(jobs: list[dict], *, now: datetime, quota: dict, reserve_mb: float,
                  next_run: datetime | None, duration_factor: float = 1,
                  quota_share: float = 1, opening_policy=None) -> dict:
    """Legacy fixed-queue reference; live snapshots use stage_schedule.forecast.

    This freezes today's queue. New arrivals, failures and other consumers are
    not silently counted as zero: callers label these as conditional scenarios.
    """
    limit = number(quota.get("limit_mb"), positive=True)
    remaining = number(quota.get("remaining_mb"))
    reset = timestamp(quota.get("reset_at_utc"))
    observed = timestamp(quota.get("observed_at_utc"))
    if (limit is None or remaining is None or remaining > limit or reset is None or reset <= now
            or observed is None or not timedelta(0) <= now - observed <= timedelta(minutes=3)):
        return {"state": "quota_unverified", "finish_at_utc": None}
    budget = (limit - reserve_mb) * MIB * quota_share
    room = max(0, remaining - reserve_mb) * MIB * quota_share
    if budget <= 0:
        return {"state": "no_daily_budget", "finish_at_utc": None}
    cursor = max(now, next_run or now) if jobs else now
    waits = work = 0.0
    queue_waits = (cursor - now).total_seconds()
    completions = {}
    resets = 0
    for job in jobs:
        cost = number(job.get("transfer_bytes"), positive=True)
        seconds = number(job.get("fetch_seconds"), positive=True)
        if cost is None or seconds is None:
            return {"state": "insufficient_samples", "finish_at_utc": None}
        admission = max(cost, number(job.get("admission_bytes")) or 0)
        if admission > budget:
            return {"state": "object_exceeds_daily_budget", "finish_at_utc": None}
        retry = timestamp(job.get("retry_at_utc"))
        if retry and retry > cursor:
            queue_waits += (retry - cursor).total_seconds()
            cursor = retry
        while cursor >= reset:
            room = budget
            reset += timedelta(days=1)
            resets += 1
        if admission > room:
            waits += max(0, (reset - cursor).total_seconds()) + 5
            cursor = reset + timedelta(seconds=5)
            room = budget
            reset += timedelta(days=1)
            resets += 1
        # Same entry/runway contract as run_finlab_refresh.sh. An unknown
        # weekday calendar stays protected, exactly as the maintenance gate.
        if opening_policy:
            shifted = opening_policy(cursor)
            waits += (shifted - cursor).total_seconds()
            cursor = shifted
        duration = seconds * duration_factor
        work += duration
        cursor += timedelta(seconds=duration)
        room -= cost
        completions[job["key"]] = cursor.isoformat()
    return {"state": "conditional" if jobs else "complete", "finish_at_utc": cursor.isoformat(),
            "remaining_seconds": round((cursor - now).total_seconds(), 1),
            "processing_seconds": round(work, 1), "quota_opening_wait_seconds": round(waits, 1),
            "queue_wait_seconds": round(queue_waits, 1),
            "total_wait_seconds": round(waits + queue_waits, 1),
            "quota_resets": resets, "quota_share": quota_share,
            "duration_factor": duration_factor, "key_finish_at_utc": completions}


def _opening_policy(root: Path):
    sessions = {}

    def shift(instant):
        from zoneinfo import ZoneInfo
        local = instant.astimezone(ZoneInfo("Asia/Taipei"))
        if local.date() not in sessions:
            result = evaluate(instant, official_calendar_root=root / "data_tw_public")
            sessions[local.date()] = result["stock_session"]
        if sessions[local.date()]:
            # Default key timeout is 600 s plus 30 s termination grace.
            start = local.replace(hour=8, minute=7, second=30, microsecond=0)
            end = local.replace(hour=9, minute=10, second=0, microsecond=0)
            if start <= local < end:
                return end.astimezone(UTC)
        return instant
    return shift


def build_workload(root: Path, *, sdk_cache_root: Path, now: datetime,
                   quota: dict, cache: dict | None = None,
                   footer_budget_seconds: float = 3.0) -> tuple[dict, dict]:
    with local_plan_reads():
        return _build_workload(root, sdk_cache_root=sdk_cache_root, now=now,
                               quota=quota, cache=cache, footer_budget_seconds=footer_budget_seconds)


def _build_workload(root: Path, *, sdk_cache_root: Path, now: datetime,
                    quota: dict, cache: dict | None, footer_budget_seconds: float) -> tuple[dict, dict]:
    """No SDK/API access; source-work weights and expected payload stay distinct."""
    started = time.monotonic()
    source = root / "data_finlab"
    source_bound = source.resolve()
    sdk_bound = sdk_cache_root.resolve()
    discovery = read_json(source / "catalog/discovery.json")
    keys = discovery.get("keys")
    if (not isinstance(keys, list) or not keys or any(not isinstance(k, str) for k in keys)
            or len(keys) != len(set(keys))):
        return {"contract_version": CONTRACT, "state": "catalog_unverified",
                "generated_at_utc": now.isoformat()}, {}
    catalog_at = timestamp(discovery.get("observed_at_utc"))
    catalog_fresh = bool(catalog_at and timedelta(0) <= now - catalog_at <= timedelta(hours=4))
    cache = cache if isinstance(cache, dict) and cache.get("contract_version") in {1, 2, CONTRACT} else {}
    cached = cache.get("files", {})
    next_cache = {}
    core = read_json(source / "core_acquisition_status.json")
    refresh_days = core.get("refresh_days", 1)
    refresh_days = refresh_days if type(refresh_days) is int and 1 <= refresh_days <= 90 else 1
    curated = {item["key"]: item for item in load_catalog(root / "configs/finlab_history_candidates.json")["datasets"]}
    plan = _sync_work_plan(keys, curated, source, now=now, refresh_days=refresh_days,
                           retry_unavailable=False)
    # Label tables are optional for the core gate, not unscheduled: the
    # collector re-admits their observed per-key backoff like every other key.
    order = list(dict.fromkeys([*plan["primary"], *plan["validation"], *plan["required_blocked"],
                               *(key for key in keys if key in NON_NUMERIC_DEFERRED_KEYS)]))
    outstanding = set(order)
    queue_order = {key: i for i, key in enumerate(order)}
    rows = []
    groups = {}
    completion_samples = []
    footer_reads = 0
    for key in keys:
        if key.split(":", 1)[0] in {"tw_tick", "tw_minute"}:
            continue  # example keys are not the full-market denominator
        receipt = read_key_state(key, source)
        path = local_object(source, receipt.get("parquet_path"), "datasets", resolved_root=source_bound)
        valid = bool(path and has_local_download(key, source))
        attempt = read_key_state(key, source, "attempts")
        raw = local_object(source, receipt.get("raw_path"), "raw", resolved_root=source_bound) if valid else None
        # This name is SDK-owned. Refuse path separators and do not expose paths.
        if raw is None and "/" not in key and "\\" not in key and key not in {".", ".."}:
            candidate = sdk_cache_root / (key.replace(":", "#") + ".feather")
            raw = candidate if candidate.is_file() and candidate.resolve().is_relative_to(sdk_bound) else None
        raw_stat = signature(raw) if raw else None
        size = raw_stat[2] if raw_stat and raw_stat[2] > 0 else None
        stat = signature(path) if valid else None
        binding = [MEASURE_CONTRACT, receipt.get("sha256"), stat, receipt.get("storage_layout"), receipt.get("source_index_columns")]
        entry = cached.get(key, {})
        measure = None
        if valid and entry.get("binding") == binding:
            measure = entry.get("measure")
        elif valid and time.monotonic() - started < footer_budget_seconds:
            try:
                measure = record_measure(path, receipt)
                footer_reads += 1
                if signature(path) != stat:
                    measure = None
            except (OSError, ValueError):
                measure = None
        if measure is not None:
            next_cache[key] = {"binding": binding, "measure": measure}
        checked = timestamp(receipt.get("source_checked_at_utc"))
        current = bool(valid and checked and checked <= now and receipt.get("source_check_mode") in UPSTREAM_CHECK_MODES
                       and not source_check_due(receipt, now=now, days=refresh_days))
        # Never mistake a scheduler exclusion or cooldown for a fresh receipt.
        pending = not current
        attempted = timestamp(attempt.get("attempted_at_utc"))
        failed_after_check = bool(pending and attempt.get("dataset") == key and attempted
                                  and attempted <= now and (checked is None or attempted > checked))
        # Old valid data does not prove the latest source refresh was allowed.
        # A later successful source check supersedes any earlier failure.
        blocked = attempt.get("status") if not valid or failed_after_check else None
        if blocked not in {"provider_empty", "vip_only", "authentication_failed"}:
            blocked = None
        retry = attempt_retry_at(attempt, downloaded=valid) if failed_after_check else None
        if attempted and attempted < quota_cycle_start(now):
            retry = None  # collector re-admits failures after the account reset
        duration = number(receipt.get("last_fetch_elapsed_seconds"), positive=True)
        if receipt.get("source_check_mode") not in UPSTREAM_CHECK_MODES or (duration and duration >= 86400):
            duration = None  # cached conversion time is not a network-fetch sample
        category = key.split(":", 1)[0]
        if duration and duration < 86400 and size:
            groups.setdefault(category, []).append((duration, size))
            if checked and quota_cycle_start(now) <= checked <= now:
                completion_samples.append((checked, duration))
        stage = queue_stage(key, downloaded=valid, metadata=key in NON_NUMERIC_DEFERRED_KEYS)
        if blocked:
            stage = "source_issues"
        payload_sample = number(receipt.get("last_payload_bytes"), positive=True)
        payload_measured = (receipt.get("source_check_mode") == "upstream_incremental"
                            and receipt.get("payload_measurement_basis") == "sdk_progress_payload_estimate_not_billing"
                            and payload_sample is not None)
        estimated_payload = payload_sample if payload_measured else size
        next_check = next_source_check(receipt, days=refresh_days) if valid else None
        rows.append({"key": key, "category": category, "downloaded": valid,
                     "incremental_quota_exempt": incremental_quota_exempt(key, downloaded=valid),
                     "needs_refresh": pending, "blocked_reason": blocked,
                     "queue_role": stage,
                     "queue_order": queue_order.get(key, len(order) + len(rows)),
                     "scheduled": key in outstanding,
                     "transfer_bytes": size, "transfer_basis": "previous_raw_object_bytes" if size else "unknown",
                     "expected_payload_bytes": estimated_payload,
                     "payload_basis": "same_key_sdk_payload_sample" if payload_measured else "conservative_full_object_proxy",
                     "admission_bytes": whole_table_reserve_bytes(key, size or 0) if key in STREAMING_KEYS else estimated_payload,
                     "local_parquet_bytes": stat[2] if stat else None,
                     "fetch_seconds": duration, "time_basis": "same_key_previous_source_check" if duration else "unknown",
                     "record_count": measure.get("count") if measure else None,
                     "record_unit": measure.get("unit") if measure else None,
                     "stored_rows": measure.get("stored_rows") if measure else None,
                     "retry_at_utc": retry.isoformat() if retry else None,
                     "source_checked_at_utc": checked.isoformat() if checked else None,
                     "next_source_check_at_utc": next_check.isoformat() if next_check else None})
    for row in rows:
        samples = groups.get(row["category"], [])
        if row["fetch_seconds"] is None and len(samples) >= 3:
            row["fetch_seconds"] = statistics.median(s for s, _ in samples)
            row["time_basis"] = "same_category_median"
        elif row["fetch_seconds"] is None and row["transfer_bytes"] and samples:
            row["fetch_seconds"] = row["transfer_bytes"] * statistics.median(s / b for s, b in samples)
            row["time_basis"] = "same_category_size_scaled_small_sample"
    # Fresh SDK processes, local selection and account checks occur between
    # fetches. Ignoring this cost seriously understates a hundreds-key queue.
    # This is inferred cadence, not an attributed API/CPU timing measurement.
    completion_samples.sort()
    overhead_samples = [gap for (left, _), (right, duration) in zip(completion_samples, completion_samples[1:])
                        if 0 <= (gap := (right - left).total_seconds() - duration) <= 60]
    overhead = statistics.median(overhead_samples) if len(overhead_samples) >= 3 else None
    by_key = {row["key"]: row for row in rows}
    eligible = [r for r in rows if not r["blocked_reason"]]
    pending = [r for r in eligible if r["needs_refresh"]]
    unknown_transfer = sum(r["transfer_bytes"] is None for r in eligible)
    done_bytes = sum(r["transfer_bytes"] or 0 for r in eligible if not r["needs_refresh"])
    left_bytes = sum(r["transfer_bytes"] or 0 for r in pending)
    total_bytes = done_bytes + left_bytes
    unknown_time = sum(r["fetch_seconds"] is None for r in eligible)
    done_seconds = sum((r["fetch_seconds"] or 0) + (overhead or 0) for r in eligible
                       if not r["needs_refresh"] and r["fetch_seconds"] is not None)
    remaining_seconds = sum((r["fetch_seconds"] or 0) + (overhead or 0) for r in pending
                            if r["fetch_seconds"] is not None)
    measured_storage_bytes = sum(r["local_parquet_bytes"] or 0 for r in rows)
    measures = []
    for unit, label in GRAINS.items():
        selected = [r for r in eligible if r["record_unit"] == unit and r["record_count"] is not None]
        done = sum(r["record_count"] for r in selected if not r["needs_refresh"])
        left = sum(r["record_count"] for r in selected if r["needs_refresh"])
        measures.append({"unit": unit, "label": label, "local_count": done + left if selected else None,
                         "completed_count": done, "remaining_estimate": left,
                         "total_estimate": done + left,
                         "ratio": done / (done + left) if done + left else None})
    monitor = read_json(root / "artifacts/live/data_monitor/public_status.json")
    acq = monitor.get("finlab_acquisition", {})
    monitor_at = timestamp(monitor.get("generated_at_utc"))
    monitor_fresh = bool(monitor_at and timedelta(0) <= now - monitor_at <= timedelta(minutes=3)
                         and monitor.get("read_only") is True and monitor.get("production_control_possible") is False)
    run = read_json(source / "runs/latest.json")
    reserve = number(run.get("quota_reserve_mb"))
    reserve = 50 if reserve is None else reserve
    validation_waiting = []
    jobs = [dict(by_key[k]) for k in order if k in by_key
            and by_key[k]["needs_refresh"] and not by_key[k]["blocked_reason"]
            and k not in validation_waiting]
    simulation_rows = [dict(r, source_weight_bytes=r["transfer_bytes"],
                            transfer_bytes=r["expected_payload_bytes"]) for r in rows]
    for job in simulation_rows:
        if job["fetch_seconds"] is not None and overhead is not None:
            job["fetch_seconds"] += overhead
    unscheduled = [r["key"] for r in pending if r["key"] not in order]
    scenarios = {}
    for name, factor, share in [("fast", .75, 1.0), ("reference", 1.0, 1.0), ("slow", 1.5, .5)]:
        if not catalog_fresh:
            scenario = {"state": "catalog_unverified", "finish_at_utc": None}
        elif not monitor_fresh or (acq.get("timer_active") is not True and acq.get("service_active") is not True):
            scenario = {"state": "scheduler_unverified", "finish_at_utc": None}
        elif unscheduled:
            scenario = {"state": "unscheduled_work", "finish_at_utc": None}
        elif not jobs and any(r["needs_refresh"] for r in rows):
            scenario = {"state": "blocked", "finish_at_utc": None}
        else:
            scenario = forecast(simulation_rows, now=now, quota=quota, reserve_mb=reserve,
                                next_run=now if acq.get("service_active") else timestamp(acq.get("next_run_at_utc")),
                                duration_factor=factor, quota_share=share,
                                opening_policy=_opening_policy(root), refresh_days=refresh_days)
        completions = scenario.pop("key_finish_at_utc", {})
        if name == "reference":
            for row in rows:
                row["estimated_finish_at_utc"] = completions.get(row["key"])
        scenarios[name] = scenario
    blocked_keys = [r["key"] for r in rows if r["blocked_reason"]]
    market = read_json(source / "intraday/market_status.json")
    tick = market.get("by_kind", {}).get("tw_tick", {})
    payload = {
        "contract_version": CONTRACT, "state": "available" if catalog_fresh else "catalog_unverified", "generated_at_utc": now.isoformat(),
        "catalog_sha256": hashlib.sha256(json.dumps(sorted(keys), ensure_ascii=False).encode()).hexdigest(),
        "catalog_observed_at_utc": discovery.get("observed_at_utc"), "catalog_keys": len(keys),
        "scope": "general_catalog_refresh_excludes_windowed_examples",
        "cycle_started_at_utc": quota_cycle_start(now).isoformat(), "refresh_days": refresh_days,
        "quota_policy_version": QUOTA_POLICY_VERSION,
        "incremental_quota_policy": "provider_enforced_no_local_reserve",
        "general_keys": len(rows), "refresh_pending": len(pending), "blocked_keys": blocked_keys,
        "validation_waiting_keys": validation_waiting,
        "unknown_transfer_keys": unknown_transfer,
        "unknown_record_keys": sum(r["record_count"] is None for r in eligible),
        "transfer": {"completed_bytes": done_bytes, "remaining_bytes_estimate": left_bytes,
                     "total_bytes_estimate": total_bytes, "local_source_bytes": sum(r["transfer_bytes"] or 0 for r in rows if r["downloaded"]),
                     "ratio": done_bytes / total_bytes if total_bytes and not unknown_transfer else None,
                     "basis": "以本機完整原始檔大小加權計算來源查核工作量，不是實際下載位元組或帳號計費。分階段傳輸預估另採增量樣本；缺樣本則估整表。來源全空／權限與 Tick 不在可估分母內。"},
        "records": measures, "stored_parquet_bytes": measured_storage_bytes,
        "work_time": {"completed_seconds": round(done_seconds, 1),
                      "remaining_seconds_estimate": round(remaining_seconds, 1),
                      "total_seconds_estimate": round(done_seconds + remaining_seconds, 1),
                      "unknown_keys": unknown_time,
                      "ratio": done_seconds / (done_seconds + remaining_seconds)
                      if done_seconds + remaining_seconds and not unknown_time else None},
        "scenarios": scenarios, "all_data_finish_at_utc": None,
        "eta_basis": "依相同優先序串行模擬：到期市場行情→歷史缺口→其他追新→標籤→來源重試→Tick；已驗證 SDK 增量追新不扣本機回補保留量，也不被配額待排的歷史前綴擋住；來源實際容量仍計入情境。跨完整配額週期仍未查核的標籤提升至最舊特徵修補，避免飢餓。跨日加入新到期工作，不凍結今天佇列；首次未來檢查用已綁定 SDK 到期時間，之後未知新到期時間時保守採日配額週期。增量使用同鍵正值 SDK 傳輸樣本，未量測／只有未變更樣本則估整表，不視為零成本。快速耗時×0.75；參考×1；慢速×1.5且只分得50%配額。不是信賴區間、發布證明或排程承諾；14日／2萬次模擬無法完成則顯示容量受限。",
        "expected_pending_payload_bytes": sum(r["expected_payload_bytes"] for r in pending)
            if all(r["expected_payload_bytes"] is not None for r in pending) else None,
        "daily_budget_bytes": max(0, (number(quota.get("limit_mb")) or 0) - reserve) * MIB,
        "incremental_daily_capacity_bytes": (number(quota.get("limit_mb")) or 0) * MIB,
        "overhead_seconds_per_key_estimate": overhead, "overhead_samples": len(overhead_samples),
        "all_data_finish_reason": "來源全空／未量測範圍、持續增量及 Tick 可得歷史未驗證，不能宣稱全資料完成日。",
        "tick": {"observed_at_utc": market.get("observed_at_utc"),
                 "local_rows": tick.get("rows"), "local_parquet_bytes": tick.get("parquet_bytes"),
                 "candidate_partitions": tick.get("target_partitions"),
                 "receipted_partitions": tick.get("receipted_partitions"),
                 "remaining_transfer_bytes": None, "finish_at_utc": None,
                 "reason": "最低優先級；候選交易日不等於供應商可得歷史。缺逐分區傳輸量與代表性樣本，不能由2330或已收小樣本外推全市場。分鐘本機合成不重算API流量。"},
        "datasets": rows, "measurement": {"footer_reads": footer_reads,
                                         "elapsed_seconds": round(time.monotonic() - started, 3)},
    }
    payload["stages"] = stage_summary(simulation_rows, scenarios, now=now, tick=payload["tick"])
    for scenario in scenarios.values():
        scenario.pop("stage_finish_at_utc", None)
        scenario.pop("stage_start_at_utc", None)
    return payload, {"contract_version": CONTRACT, "files": next_cache}


def snapshot_workload(root: Path, output_root: Path, *, sdk_cache_root: Path,
                      quota: dict, now: datetime | None = None, footer_budget_seconds=3.0) -> dict:
    output_root.mkdir(parents=True, exist_ok=True)
    with (output_root / ".workload.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {"state": "another_snapshot_active"}
        payload, cache = build_workload(root, sdk_cache_root=sdk_cache_root,
                                        now=now or datetime.now(UTC), quota=quota,
                                        cache=read_json(output_root / "workload_file_cache.json"),
                                        footer_budget_seconds=footer_budget_seconds)
        _atomic_json(output_root / "workload_file_cache.json", cache)
        _atomic_json(output_root / "workload_latest.json", payload)
        persist_local_integrity_cache(root / 'data_finlab')
        return payload


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--footer-budget-seconds", type=float, default=3)
    args = parser.parse_args()
    from finlab import utils
    output = args.root / "artifacts/live/finlab"
    value = snapshot_workload(args.root, output, sdk_cache_root=Path(utils.get_tmp_dir()),
                              quota=read_json(output / "quota_latest.json"),
                              footer_budget_seconds=args.footer_budget_seconds)
    print(json.dumps({k: v for k, v in value.items() if k != "datasets"}, ensure_ascii=False))


if __name__ == "__main__":
    main()

"""Admission of *additional API validation*, using the existing source registry.

This is a read-only veto, not a downloader, quota allocator or completeness
certificate. Required downloads and local comparisons never wait on this gate.
Each caller must also enforce its own queue, publication clock and shared quota.
"""
from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from functools import lru_cache
import json
from pathlib import Path
import sqlite3
import time
from typing import Any, Mapping

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SNAPSHOT = ROOT / "artifacts/live/data_monitor/public_status.json"
POLICY_VERSION = 1
MAX_SNAPSHOT_AGE_SECONDS = 900
FACT_IDENTITY = "instrument_episode|venue|event_time|grain|field|adjustment|currency|unit|revision"
SECONDARY_ENDPOINTS = {"finmind:sponsor:TaiwanStockPrice", "finlab:price:收盤價"}
# These are projections of existing canonical workers, not substitute datasets.
# A missing owner still vetoes; no projection is allowed to certify its owner.
ACQUISITION_OWNERS = {
    "binance-feature:trade_candles_1m": ("product:binance_usdm_perpetuals:1m",),
    "okx-feature:trade_candles_1m": ("product:okx_perpetual_swaps:1m",),
    "product:tw_listed_stocks:daily": ("tw-public:twse_daily_ohlcv", "tw-public:tpex_daily_ohlcv"),
    "product:tw_index_futures:tick": ("shioaji:futures_history",),
}
FINMIND_SNAPSHOTS = {"finmind:TaiwanStockTradingDate", "finmind:TaiwanStockInfoWithWarrant"}


def _utc(value: Any) -> datetime | None:
    try:
        stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return stamp.astimezone(UTC) if stamp.tzinfo is not None else None
    except (TypeError, ValueError, OverflowError):
        return None


@lru_cache(maxsize=8)
def _json_cached(path: str, mtime: int, size: int, epoch: int) -> dict:
    del mtime, size, epoch
    with Path(path).open(encoding="utf-8") as source:
        value = json.load(source)
    if not isinstance(value, dict):
        raise ValueError("invalid_object")
    return value


def _read(path: Path) -> dict:
    stat = path.stat()
    if stat.st_size > 32 * 1024 * 1024:
        raise ValueError("oversized_policy_input")
    return _json_cached(str(path.resolve()), stat.st_mtime_ns, stat.st_size,
                        int(time.monotonic() // 30))


def source_admission_class(source: Mapping[str, Any]) -> str:
    """Classify work, never infer equality from matching display names."""
    name = str(source.get("id") or "")
    if source.get("registry_alias") or source.get("scope") in {
        "storage_group", "physical_inventory", "inventory_partition", "credential_gate",
        "crypto_fact_family",
    } or source.get("in_active_scope") is not True:
        return "reference_not_work"
    if (name.startswith("tw-public:provisional-feature:") or
            name == "tw-public:provisional_macro_feature_events"):
        return "quality_only_values_already_observed"
    if source.get("acquisition_enabled") is False:
        return "disabled_not_acquired"
    if name in ACQUISITION_OWNERS:
        return "delegated_projection"
    if name in FINMIND_SNAPSHOTS:
        return "current_snapshot_local_proof_required"
    if source.get("category") == "storage-compaction":
        return "local_storage_maintenance_not_API_acquisition"
    if name in SECONDARY_ENDPOINTS:
        return "mixed_primary_and_validation_local_queue_required"
    if name == "finlab:catalog-acquisition":
        return "finlab_core_receipt_required"
    automation = source.get("automation") or {}
    if isinstance(automation, Mapping) and automation.get("mode") == "stream":
        return "continuous_capture_separate_live_priority"
    if source.get("automation_eligible") is False:
        return "not_automatable_not_acquired"
    return "required_acquisition"


def source_blocker(source: Mapping[str, Any]) -> str | None:
    if source_admission_class(source) != "required_acquisition":
        return None
    progress = source.get("acquisition_progress")
    if not isinstance(progress, Mapping):
        return "acquisition_proof_missing"
    # Receiving a batch, being active, or having a 100% UI ratio alone is not
    # enough. Publication waiting is idle only after declared coverage is met.
    if progress.get("coverage_complete") is True:
        if progress.get("up_to_date") is True and progress.get("state") == "complete":
            return None
        if progress.get("state") == "waiting_publication":
            return None
    state = str(progress.get("state") or "acquisition_incomplete")
    return "coverage_unproven_despite_completed_batch" if state == "complete" else state


@lru_cache(maxsize=8)
def _finmind_required(path: str, epoch: int) -> int:
    del epoch
    # Read WAL normally (not immutable=1), and do not create a missing queue.
    with sqlite3.connect(f"{Path(path).resolve().as_uri()}?mode=ro", uri=True,
                         timeout=2) as connection:
        count = connection.execute(
            "SELECT COUNT(*) FROM tasks WHERE priority < 8 "
            "AND state NOT IN ('complete','observed_empty','non_session',"
            "'not_observation_date','deprecated_query_shape')"
        ).fetchone()[0]
        total = connection.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
    if not total:
        raise ValueError("empty_finmind_queue")
    return int(count)


def evaluate_snapshot(snapshot: Mapping[str, Any], *, now: datetime,
                      finlab_core_complete: bool | None,
                      finmind_required: int | None,
                      snapshot_proofs: Mapping[str, bool | None] | None = None) -> dict[str, Any]:
    """Pure evaluator for tests and the inventory export; no provider requests."""
    result: dict[str, Any] = {
        "policy_version": POLICY_VERSION, "allowed": False,
        "reason": "acquisition_proof_unavailable", "blocking_counts": {},
        "blocked_endpoints": [], "classification_counts": {},
        "basis": "registered acquisition veto only; caller queue and shared quota also required",
    }
    stamp = _utc(snapshot.get("generated_at_utc"))
    if now.tzinfo is None or stamp is None:
        return result
    age = (now.astimezone(UTC) - stamp).total_seconds()
    result["snapshot_at_utc"] = stamp.isoformat()
    if not -60 <= age <= MAX_SNAPSHOT_AGE_SECONDS:
        result["reason"] = "acquisition_snapshot_stale"
        return result
    sources = snapshot.get("sources")
    if not isinstance(sources, list) or not sources:
        return result
    summary = snapshot.get("summary") or {}
    integrity = snapshot.get("integrity_checks") or {}
    if (snapshot.get("schema_version") != 8 or snapshot.get("read_only") is not True or
            not isinstance(summary, Mapping) or
            summary.get("registered_items") != len(sources) or
            not isinstance(integrity, Mapping) or integrity.get("state") != "pass" or
            integrity.get("violations") != 0):
        result["reason"] = "acquisition_registry_incomplete_or_invalid"
        return result
    blockers: list[dict[str, str]] = []
    classifications: Counter[str] = Counter()
    seen: set[str] = set()
    active_names = {row.get("id") for row in sources if isinstance(row, Mapping) and isinstance(row.get("id"), str)
                    and row.get("in_active_scope") is True and not row.get("registry_alias")}
    for source in sources:
        if not isinstance(source, Mapping) or not isinstance(source.get("id"), str):
            return result
        name = source["id"]
        if name in seen:
            result["reason"] = "duplicate_registry_id"
            return result
        seen.add(name)
        kind = source_admission_class(source)
        classifications[kind] += 1
        reason = source_blocker(source)
        if kind == "delegated_projection" and not set(ACQUISITION_OWNERS[name]) <= active_names:
            reason = "canonical_acquisition_owner_missing"
        if kind == "current_snapshot_local_proof_required" and (snapshot_proofs or {}).get(name) is not True:
            reason = "current_snapshot_acquisition_unproven"
        if kind == "finlab_core_receipt_required" and finlab_core_complete is not True:
            reason = "finlab_core_pending_or_unproven"
        if kind == "mixed_primary_and_validation_local_queue_required" and name.startswith("finmind:"):
            if finmind_required is None or finmind_required > 0:
                reason = "finmind_primary_pending_or_unproven"
        if reason:
            blockers.append({"id": name, "reason": reason})
    result.update({
        "classification_counts": dict(classifications),
        "blocking_counts": dict(Counter(row["reason"] for row in blockers)),
        "blocked_endpoints": blockers[:100], "blocked_endpoint_count": len(blockers),
        "finmind_required_tasks": finmind_required,
    })
    if not classifications["required_acquisition"]:
        return result
    result["allowed"] = not blockers
    result["reason"] = "no_known_required_acquisition_debt" if not blockers else "required_acquisition_pending"
    return result


def evaluate_secondary_admission(*, snapshot_path: Path = DEFAULT_SNAPSHOT,
                                 now: datetime | None = None,
                                 caller_provider: str | None = None,
                                 local_core_complete: bool | None = None) -> dict[str, Any]:
    checked = now or datetime.now(UTC)
    try:
        snapshot = _read(Path(snapshot_path))
    except (OSError, ValueError):
        snapshot = {}
    core: bool | None = None
    if caller_provider == "finlab":
        core = local_core_complete
    else:
        try:
            receipt = _read(ROOT / "data_finlab/core_acquisition_status.json")
            observed = _utc(receipt.get("checked_at_utc"))
            if observed is not None and checked.tzinfo is not None and \
                    -60 <= (checked - observed).total_seconds() <= 3600:
                core = (receipt.get("required_complete") is True and
                        receipt.get("status") == "ready" and
                        receipt.get("required_pending") == 0 and
                        receipt.get("required_blocked") == 0 and
                        bool(receipt.get("catalog_sha256")))
        except (OSError, ValueError):
            pass
    try:
        remaining = _finmind_required(str(ROOT / "data_finmind/sponsor/queue.sqlite3"),
                                      int(time.monotonic() // 30))
    except (sqlite3.Error, OSError, ValueError):
        remaining = None
    from downloader.acquisition_snapshot_proofs import finmind_snapshot_ready

    proofs = {name: finmind_snapshot_ready(ROOT / "data_finmind", name, checked)
              for name in FINMIND_SNAPSHOTS}
    return evaluate_snapshot(snapshot, now=checked, finlab_core_complete=core,
                             finmind_required=remaining, snapshot_proofs=proofs)

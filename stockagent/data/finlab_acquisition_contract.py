"""Dependency-free FinLab receipt naming and per-key retry policy.

Collectors and public readers use the same rules. Importing this contract
must not import a collector, data-frame library, SDK or credential loader.
There is no provider access or persistent freshness cache here.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import re
import time
from zoneinfo import ZoneInfo


UPSTREAM_CHECK_MODES = frozenset({"upstream_forced", "upstream_incremental"})
QUOTA_POLICY_VERSION = 1
WORKLOAD_CONTRACT_VERSION = 4
INTRADAY_PROGRESS_VERSION = 1
DISPATCH_INTERVAL_SECONDS = 60
# These adapters currently sign and stream whole objects, not SDK deltas.
WHOLE_TABLE_KEYS = frozenset({
    "broker_transactions", "after_market_fixed_price:市場別", "after_market_fixed_price:資料來源",
})
TAIPEI = ZoneInfo("Asia/Taipei")


def read_receipt_bound_source(root: Path, key: str, receipt_path: Path) -> tuple[Path, dict]:
    """Capture one current receipt and its immutable object, not a stale catalog path.

    A later atomic head update does not invalidate the captured old object;
    callers retain this receipt body and rehash that same object after reading.
    This avoids locking out the normal acquisition owner during publication.
    """
    root = root.resolve()
    if not receipt_path.resolve().is_relative_to(root / 'receipts'):
        raise ValueError('FinLab receipt escapes canonical receipt root')
    receipt = json.loads(receipt_path.read_text())
    if not isinstance(receipt, dict) or receipt.get('dataset') != key:
        raise ValueError('FinLab receipt identity mismatch')
    relative = Path(str(receipt.get('parquet_path') or ''))
    if relative.is_absolute() or '..' in relative.parts or relative.parts[:1] != ('datasets',):
        raise ValueError('FinLab source escapes immutable dataset root')
    source = root / relative
    if not source.resolve().is_relative_to(root / 'datasets'):
        raise ValueError('FinLab immutable source escapes dataset root')
    with source.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    if digest != receipt.get('sha256'):
        raise ValueError('FinLab captured receipt/source hash mismatch')
    return source, receipt


def process_owner(pid: int | None = None) -> dict | None:
    """Local Linux identity prevents a killed/reused PID remaining 'running'."""
    pid = os.getpid() if pid is None else pid
    if type(pid) is not int or pid <= 0:
        return None
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        if fields[0] in {"Z", "X"}:
            return None
        # ProcSubset=pid deliberately hides kernel sysctls in the public
        # gateway. A readable PID generation is not a dead process merely
        # because the boot UUID is unavailable in that mount namespace.
        try:
            boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        except OSError:
            boot_id = None
        return {"pid": pid, "start_ticks": fields[19], "boot_id": boot_id}
    except (OSError, IndexError):
        return None


def process_owner_alive(owner: object, *, started_at_utc=None, now: datetime) -> bool:
    """Verify the PID generation without weakening the gateway's proc policy.

    Prefer the kernel boot UUID. If a sandbox hides it, require the exact PID
    start ticks AND a recorded run start after the current boot. CLOCK_BOOTTIME
    is available without exposing kernel sysctls or host credentials; old
    receipts from a previous boot cannot establish current liveness.
    """
    if not isinstance(owner, dict):
        return False
    current = process_owner(owner.get("pid"))
    if (not current or current["pid"] != owner.get("pid")
            or current["start_ticks"] != owner.get("start_ticks")):
        return False
    if current["boot_id"] is not None and owner.get("boot_id") is not None:
        return current["boot_id"] == owner["boot_id"]
    started = utc_time(started_at_utc)
    if started is None or started > now:
        return False
    try:
        since_boot = time.clock_gettime(time.CLOCK_BOOTTIME)
    except (AttributeError, OSError):
        return False  # Unverifiable is never promoted to running.
    return started >= now - timedelta(seconds=since_boot)


def intraday_progress(source_root: Path, *, now: datetime) -> dict:
    """Read bounded local worker evidence; never infer liveness from an old JSON."""
    try:
        raw = json.loads((source_root / "intraday/active_run.json").read_text())
    except (OSError, ValueError):
        return {}
    if not isinstance(raw, dict) or raw.get("contract_version") != INTRADAY_PROGRESS_VERSION:
        return {}
    observed = utc_time(raw.get("observed_at_utc"))
    # The public DTO captures its clock before reading multiple local files;
    # a worker may atomically checkpoint during that short projection. Allow
    # that bounded read race, not genuinely future-dated evidence.
    if not observed or observed > now + timedelta(seconds=5):
        return {}
    owner = raw.get("owner")
    live = process_owner_alive(owner, started_at_utc=raw.get("run_started_at_utc"), now=max(now, observed))
    result = {**raw, "owner_alive": live, "age_seconds": round(max(0, (now - observed).total_seconds()), 1)}
    if raw.get("state") == "running" and not live:
        result.update(state="interrupted", active_key=None, active_trade_date=None,
                      estimated_batch_finish_at_utc=None)
    estimated = utc_time(result.get("estimated_batch_finish_at_utc"))
    deadline = utc_time(result.get("stop_by_at_utc"))
    result["batch_eta_state"] = "conditional" if estimated else "insufficient_samples"
    if estimated and deadline and estimated > deadline:
        result.update(batch_eta_state="time_budget_limited", estimated_batch_finish_at_utc=None)
    elif result["state"] == "running" and estimated and estimated <= now:
        result.update(batch_eta_state="sample_overrun", estimated_batch_finish_at_utc=None)
    return result


def incremental_quota_exempt(key: str, *, downloaded: bool) -> bool:
    """Verified SDK refreshes ignore our backfill reserve, never provider limits.

    A missing/corrupt baseline or a whole-object streaming adapter cannot be
    called a small incremental check merely because its new rows are few.
    The caller supplies independently verified local-download evidence.
    """
    return downloaded and key not in WHOLE_TABLE_KEYS


def utc_time(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.astimezone(UTC) if parsed.tzinfo else None
    except (ValueError, TypeError):
        return None


def quota_cycle_start(now: datetime) -> datetime:
    """Account reset clock, not a market session or a publication claim."""
    local = now.astimezone(TAIPEI)
    reset = local.replace(hour=8, minute=0, second=0, microsecond=0)
    if local < reset:
        reset -= timedelta(days=1)
    return reset.astimezone(UTC)


def next_source_check(receipt: dict, *, days: int = 1) -> datetime | None:
    """Use the observed SDK expiry; legacy receipts retain the daily policy.

    An expiry is only a recommended check time, never proof of publication.
    Accept it only when bound to an upstream check and within a bounded range.
    """
    checked = utc_time(receipt.get("source_checked_at_utc") or receipt.get("fetched_at_utc"))
    if checked is None or receipt.get("source_check_mode") == "sdk_cache_allowed":
        return None
    planned = utc_time(receipt.get("next_source_check_at_utc"))
    if (receipt.get("source_check_mode") in UPSTREAM_CHECK_MODES and planned
            and checked < planned <= checked + timedelta(days=90)):
        return planned
    return (quota_cycle_start(checked) + timedelta(days=1) if days == 1
            else checked + timedelta(days=days))


def source_check_due(receipt: dict, *, now: datetime, days: int = 1) -> bool:
    checked = utc_time(receipt.get("source_checked_at_utc") or receipt.get("fetched_at_utc"))
    due = next_source_check(receipt, days=days)
    return checked is None or checked > now or due is None or now >= due


def proven_source_empty(key: str, attempt: dict) -> bool:
    """Recognize an all-null claim; the collector verifies its backing raw proof."""
    return (attempt.get("dataset") == key and attempt.get("status") == "provider_empty"
            and type(attempt.get("raw_non_null_values")) is int
            and attempt["raw_non_null_values"] == 0
            and bool(attempt.get("empty_evidence_path")))


def queue_stage(key: str, *, downloaded: bool, metadata: bool = False,
                source_empty: bool = False) -> str:
    if source_empty:
        return "source_issues"
    if metadata:
        return "metadata"
    if not downloaded:
        return "history"
    if key.split(":", 1)[0] == "price":
        return "priority_updates"
    return "updates"


STAGE_PRIORITY = {"priority_updates": 0, "history": 1, "updates": 2,
                  "metadata": 3, "source_issues": 4, "tick": 5}


def queue_priority(stage: str, *, checked: datetime | None, now: datetime) -> int:
    """Age labels that missed a quota cycle into oldest-first feature repairs.

    Missing numeric history still goes first. This is priority promotion, not
    a mandatory retry delay, and does not change the displayed source stage.
    """
    if stage == "metadata" and (checked is None or checked < quota_cycle_start(now) - timedelta(days=1)):
        return STAGE_PRIORITY["updates"]
    return STAGE_PRIORITY[stage]


# These are dated partitions, not whole-table matrix keys. Calling data.get
# without both dates fails independently of account entitlement.
AUTOMATICALLY_DEFERRED_REASONS = {
    "tw_minute:2330": "requires_date_window",
    "tw_tick:2330": "requires_date_window",
}
# Short bounded backoff avoids both a blind spot and a deterministic tight
# failure loop. The canonical timer supplies the actual retry clock.
ATTEMPT_RETRY_SECONDS = {
    "vip_only": (30 * 60, 4 * 60 * 60),
    "provider_error": (5 * 60, 60 * 60),
    "provider_empty": (30 * 60, 2 * 60 * 60),
    "normalization_error": (5 * 60, 60 * 60),
    "timed_out": (30 * 60, 2 * 60 * 60),
    "resource_deferred": (60 * 60, 2 * 60 * 60),
}


def safe_stem(key: str) -> str:
    """Stable filenames, without allowing provider names to become paths."""
    readable = re.sub(r"[^A-Za-z0-9_-]+", "_", key.split(":", 1)[0])[:56]
    return f"{readable}-{hashlib.sha256(key.encode()).hexdigest()[:12]}"


def attempt_retry_at(attempt: dict, *, downloaded: bool = False) -> datetime | None:
    """Only per-key failures get a cooldown; account-wide failures do not."""
    policy = ATTEMPT_RETRY_SECONDS.get(attempt.get("status"))
    if policy is None:
        return None
    base, maximum = policy
    if downloaded and attempt.get("status") == "timed_out":
        base, maximum = 15 * 60, 60 * 60
    streak = attempt.get("failure_streak", 1)
    if not isinstance(streak, int) or isinstance(streak, bool) or streak < 1:
        streak = 1
    delay = timedelta(seconds=min(maximum, base * (2 ** min(streak - 1, 10))))
    try:
        attempted = datetime.fromisoformat(attempt["attempted_at_utc"])
        return attempted.astimezone(UTC) + delay if attempted.tzinfo else None
    except (ValueError, KeyError, TypeError):
        return None

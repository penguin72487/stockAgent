"""Dependency-free FinLab receipt naming and per-key retry policy.

Collectors and public readers use the same rules. Importing this contract
must not import a collector, data-frame library, SDK or credential loader.
There is no provider access or persistent freshness cache here.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import hashlib
import re
from zoneinfo import ZoneInfo


UPSTREAM_CHECK_MODES = frozenset({"upstream_forced", "upstream_incremental"})
QUOTA_POLICY_VERSION = 1
# These adapters currently sign and stream whole objects, not SDK deltas.
WHOLE_TABLE_KEYS = frozenset({
    "broker_transactions", "after_market_fixed_price:市場別", "after_market_fixed_price:資料來源",
})
TAIPEI = ZoneInfo("Asia/Taipei")


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

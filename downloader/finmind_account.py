"""Verified FinMind entitlement and shared request pacing.

The subscription level and hourly quota come from FinMind, not from a plan
name hard-coded in a worker.  This file never persists credentials or PII.
"""

from __future__ import annotations

from contextlib import closing
from datetime import UTC, datetime, timedelta
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3

import requests

from downloader.artifact_io import atomic_write_json
from downloader.common import SharedRateLimiter


USER_INFO_URL = "https://api.web.finmindtrade.com/v2/user_info"
_CACHE: tuple[str, datetime, dict[str, object]] | None = None
ACCOUNT_CACHE_SECONDS = 60
_DISPATCH_REFRESH_RETRY: dict[str, datetime] = {}


def _fresh_observation(stamp: datetime, now: datetime) -> bool:
    # A wall-clock minute bucket lets the existing once-per-minute timer make
    # its scheduled sample even if an idle worker sampled late last minute.
    return (stamp.tzinfo is not None and 0 <= (now - stamp).total_seconds() < ACCOUNT_CACHE_SECONDS
            and int(stamp.timestamp()) // 60 == int(now.timestamp()) // 60)


def _cached_account(path: Path, fingerprint: str, now: datetime) -> dict[str, object] | None:
    try:
        with path.open('rb') as stream:
            raw = stream.read(8193)
        if len(raw) > 8192:
            return None
        envelope = json.loads(raw)
        value = envelope['account']
        stamp = datetime.fromisoformat(value['observed_at_utc'])
        if (envelope.get('schema_version') != 1 or envelope.get('credential_fingerprint') != fingerprint
                or value.get('source') != USER_INFO_URL
                or value.get('tier') not in {'Free', 'Backer', 'Sponsor', 'SponsorPro'}
                or type(value.get('official_requests_per_hour')) is not int
                or not 1 <= value['official_requests_per_hour'] <= 100_000
                or not _fresh_observation(stamp, now)):
            return None
        # Never return arbitrary keys from a cached envelope to public callers.
        return {key: value.get(key) for key in ('observed_at_utc', 'tier', 'official_requests_per_hour',
                                                'provider_used_in_hour', 'source')}
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        return None


def verified_account(session: requests.Session, token: str, root: Path) -> dict[str, object]:
    """Fail closed when a token's actual tier or quota cannot be verified."""
    global _CACHE
    if not token:
        raise RuntimeError("FINMIND_TOKEN is missing")
    now = datetime.now(UTC)
    root = root.resolve()
    fingerprint = hashlib.sha256(('finmind-account-cache-v1:' + token).encode()).hexdigest()
    cache_key = str(root) + ':' + fingerprint
    if _CACHE and _CACHE[0] == cache_key and _fresh_observation(_CACHE[1], now):
        return _CACHE[2]
    root.mkdir(parents=True, exist_ok=True)
    # The quota timer and every downloader share one authenticated observation
    # per minute. Cross-process locking avoids a simultaneous cache-miss burst.
    # A token rotation cannot inherit another token's tier or remaining quota.
    with os.fdopen(os.open(root / 'account_status.lock', os.O_CREAT | os.O_RDWR, 0o600), 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        now = datetime.now(UTC)
        result = _cached_account(root / 'account_probe_cache.json', fingerprint, now)
        if result is None:
            result = _probe_account(session, token)
            atomic_write_json(root / 'account_probe_cache.json', {
                'schema_version': 1, 'credential_fingerprint': fingerprint, 'account': result,
            })
            atomic_write_json(root / 'account_status.json', result)
        # Cache age is anchored to the actual provider observation, not reuse.
        _CACHE = (cache_key, datetime.fromisoformat(str(result['observed_at_utc'])), result)
        return result


def _probe_account(session: requests.Session, token: str) -> dict[str, object]:
    response = session.get(USER_INFO_URL, headers={"Authorization": f"Bearer {token}"},
                           timeout=(10, 20))
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict) or payload.get("status") != 200:
        raise RuntimeError("FinMind user_info rejected this token")
    tier = payload.get("level_title")
    limit = payload.get("api_request_limit_hour")
    if tier not in {"Free", "Backer", "Sponsor", "SponsorPro"} or \
            type(limit) is not int or not 1 <= limit <= 100_000:
        raise RuntimeError("FinMind user_info returned an unrecognized tier or hourly quota")
    result: dict[str, object] = {
        "observed_at_utc": datetime.now(UTC).isoformat(),
        "tier": tier,
        "official_requests_per_hour": limit,
        "provider_used_in_hour": payload.get("user_count") if type(payload.get("user_count")) is int else None,
        "source": USER_INFO_URL,
    }
    return result


def rate_limiter(account: dict[str, object]) -> SharedRateLimiter:
    """One process-shared pacing key across Free and Sponsor workers.

    Slight spacing above the nominal cadence protects against sliding-window
    rounding.  402/429 still triggers the existing shared provider cooldown.
    """
    limit = account["official_requests_per_hour"]
    assert type(limit) is int
    return SharedRateLimiter(3600.0 / limit + 0.01, name="finmind-v4-data")


def refresh_dispatch_account(account: dict, token: str, root: Path, now: datetime) -> dict:
    """Long-running batches reuse the same minute probe as the quota timer.

    A failed probe retains its original timestamp; backfill_budget will stop
    dispatch once evidence is stale, rather than fabricating a fresh quota.
    """
    try:
        stamp = datetime.fromisoformat(str(account.get('observed_at_utc')))
        due = stamp.tzinfo is None or (now - stamp).total_seconds() >= 60
    except (ValueError, TypeError):
        due = False  # The next ordinary cycle retries unverified credentials.
    retry_key = str(root.resolve())
    retry = _DISPATCH_REFRESH_RETRY.get(retry_key)
    if token and due and (retry is None or now >= retry):
        try:
            with requests.Session() as session:
                refreshed = verified_account(session, token, root)
                _DISPATCH_REFRESH_RETRY.pop(retry_key, None)
                return refreshed
        except (requests.RequestException, RuntimeError, ValueError):
            _DISPATCH_REFRESH_RETRY[retry_key] = now + timedelta(seconds=60)
    return account


def backfill_budget(
    account: dict[str, object], traffic_root: Path, *,
    fixed_incremental_requests: int, in_flight: int = 0,
    now: datetime | None = None, prioritize_due: bool = False,
    reservation_plan: dict | None = None,
) -> dict[str, int | bool | str]:
    """Admit historical work only after protecting the fixed incremental lane.

    The account observation is authoritative at its timestamp. Local request
    starts after it are added once. Earlier local calls are already included in
    the provider sample; a rolling local-hour count would incorrectly carry
    old-hour traffic across the provider's observed reset.
    No request here is sent to FinMind.
    """

    limit = account.get("official_requests_per_hour")
    if type(limit) is not int or limit <= 0:
        raise ValueError("verified hourly FinMind quota required")
    current = (now or datetime.now(UTC)).astimezone(UTC)
    observed_raw = account.get("observed_at_utc")
    try:
        observed = datetime.fromisoformat(str(observed_raw).replace("Z", "+00:00")).astimezone(UTC)
    except (ValueError, TypeError):
        observed = None
    provider_used = account.get("provider_used_in_hour")
    if type(provider_used) is not int or provider_used < 0 or observed is None:
        return {"allowed": False, "remaining": 0, "reserve": limit,
                "used_estimate": limit, "basis": "provider_usage_unverified"}
    if observed > current + timedelta(minutes=1) or current - observed > timedelta(minutes=5):
        return {"allowed": False, "remaining": 0, "reserve": limit,
                "used_estimate": limit, "basis": "provider_usage_stale"}
    since_observation = 0
    ledger = traffic_root / "request_traffic.sqlite3"
    if ledger.is_file():
        try:
            with closing(sqlite3.connect(ledger.resolve().as_uri() + "?mode=ro", uri=True, timeout=2.0)) as connection:
                since_observation = int(connection.execute(
                    "SELECT count(*) FROM requests WHERE started_at_utc>? AND started_at_utc<=?",
                    (observed.isoformat(), current.isoformat()),
                ).fetchone()[0])
        except sqlite3.Error:
            return {"allowed": False, "remaining": 0, "reserve": limit,
                    "used_estimate": limit, "basis": "local_traffic_unverified"}
    used = provider_used + since_observation
    if fixed_incremental_requests < 0 or in_flight < 0:
        raise ValueError("incremental demand and in-flight requests must be nonnegative")
    # Reserve the actual named demand plus the two other serial FinMind worker
    # lanes, not an arbitrary percentage that strands a large paid entitlement.
    reserve = min(limit, fixed_incremental_requests + 2)
    remaining = max(0, limit - used)
    priority_wait = False
    ready = 0
    schedule_verified = True
    if prioritize_due:
        from downloader.finmind_scheduling import incremental_reservation
        plan = reservation_plan if reservation_plan is not None else incremental_reservation(traffic_root, current)
        ready = plan['ready_requests']
        schedule_verified = not plan['queue_errors']
        if reservation_plan is not None:
            schedule_verified = bool(schedule_verified and plan.get('observed_at_utc') == current.isoformat()
                                     and plan.get('reserve_requests') == fixed_incremental_requests)
        priority_wait = ready > 0 or not schedule_verified
    return {"allowed": remaining > reserve + in_flight and not priority_wait,
            "remaining": remaining, "reserve": reserve,
            "ready_incremental_requests": ready, "priority_wait": priority_wait,
            "schedule_verified": schedule_verified,
            "used_estimate": used, "basis": "provider_observation_plus_local_starts"}

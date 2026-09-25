"""Verified FinMind entitlement and shared request pacing.

The subscription level and hourly quota come from FinMind, not from a plan
name hard-coded in a worker.  This file never persists credentials or PII.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import requests

from downloader.artifact_io import atomic_write_json
from downloader.common import SharedRateLimiter


USER_INFO_URL = "https://api.web.finmindtrade.com/v2/user_info"
_CACHE: tuple[str, datetime, dict[str, object]] | None = None


def verified_account(session: requests.Session, token: str, root: Path) -> dict[str, object]:
    """Fail closed when a token's actual tier or quota cannot be verified."""
    global _CACHE
    if not token:
        raise RuntimeError("FINMIND_TOKEN is missing")
    now = datetime.now(UTC)
    if _CACHE and _CACHE[0] == token and now - _CACHE[1] < timedelta(minutes=15):
        return _CACHE[2]
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
    root.mkdir(parents=True, exist_ok=True)
    atomic_write_json(root / "account_status.json", result)
    _CACHE = (token, now, result)
    return result


def rate_limiter(account: dict[str, object]) -> SharedRateLimiter:
    """One process-shared pacing key across Free and Sponsor workers.

    Slight spacing above the nominal cadence protects against sliding-window
    rounding.  402/429 still triggers the existing shared provider cooldown.
    """
    limit = account["official_requests_per_hour"]
    assert type(limit) is int
    return SharedRateLimiter(3600.0 / limit + 0.01, name="finmind-v4-data")

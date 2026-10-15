"""Shared, credential-free FinLab byte-budget and dispatch policy.

An account's actual quota is authoritative. These are local admission floors,
not guaranteed exact balances: the next partition's billed size is unknown.
Only useful, eligible work may consume a balance before its 08:00 Taipei reset.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
import hashlib
import json
import math
from pathlib import Path

from stockagent.data.finlab_acquisition_contract import quota_cycle_start, utc_time


CONTRACT_VERSION = 1
ACQUISITION_POLICY_VERSION = 8
DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "configs/finlab_quota_policy.json"


@dataclass(frozen=True)
class QuotaPolicy:
    minute_reserve_mb: float = 50.0
    tick_reserve_mb: float = 500.0
    drain_before_reset_minutes: int = 60
    quota_sample_interval_seconds: int = 15
    quota_sample_max_requests: int = 5
    quota_sample_near_floor_mb: float = 10.0
    minute_partitions_per_pass: int = 512
    maximum_intraday_passes: int = 4
    catalog_refresh_minutes: int = 60
    minute_workers: int = 4
    minute_range_calendar_days: int = 31
    minute_http_keepalive: bool = True
    minute_download_threads: int = 1
    minute_queue_depth: int = 1
    minute_authorization_slots: int = 8
    general_batch_size: int = 1
    pre_reset_publication_recheck_minutes: int = 10
    processing_threads: int = 1

    @property
    def fingerprint(self) -> str:
        payload = json.dumps({"acquisition_policy_version": ACQUISITION_POLICY_VERSION,
                              "processing_engine": "polars_arrow_v1", **asdict(self)},
                             sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode()).hexdigest()


def load_policy(path: Path = DEFAULT_CONFIG) -> QuotaPolicy:
    try:
        raw = json.loads(path.read_text())
    except FileNotFoundError:
        return QuotaPolicy()  # Legacy checkouts/test roots retain explicit defaults.
    if not isinstance(raw, dict) or type(raw.get("schema_version")) is not int or raw["schema_version"] != 1:
        raise ValueError("invalid FinLab quota policy schema")
    fields = asdict(QuotaPolicy())
    if set(raw) - {"schema_version", *fields}:
        raise ValueError("unknown FinLab quota policy field")
    for key, default in fields.items():
        value = raw.get(key, default)
        if isinstance(default, bool):
            if type(value) is not bool:
                raise ValueError(f"invalid FinLab quota policy: {key}")
            fields[key] = value
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError(f"invalid FinLab quota policy: {key}")
        bounds = {
            "minute_reserve_mb": (0, 5000), "tick_reserve_mb": (0, 5000),
            "drain_before_reset_minutes": (0, 180), "quota_sample_interval_seconds": (0, 60),
            "quota_sample_max_requests": (1, 32), "quota_sample_near_floor_mb": (0, 100),
            "minute_partitions_per_pass": (1, 2048), "maximum_intraday_passes": (1, 8),
            "catalog_refresh_minutes": (1, 180),
            "minute_workers": (1, 32), "minute_range_calendar_days": (1, 31),
            "minute_download_threads": (1, 5),
            "minute_queue_depth": (1, 4),
            "minute_authorization_slots": (1, 32),
            "general_batch_size": (1, 16),
            "pre_reset_publication_recheck_minutes": (1, 120),
            "processing_threads": (1, 8),
        }
        low, high = bounds[key]
        if not low <= value <= high or isinstance(default, int) and type(value) is not int:
            raise ValueError(f"out-of-range FinLab quota policy: {key}")
        fields[key] = value
    return QuotaPolicy(**fields)


def quota_admission(*, kind: str, now: datetime, reserve_mb: float | None = None,
                    scheduled_reserve_mb: float = 0, policy: QuotaPolicy | None = None) -> dict:
    """Minutes have their own floor; Tick alone reserves future general work."""
    policy = policy or QuotaPolicy()
    if kind not in {"tw_minute", "tw_tick"} or now.tzinfo is None:
        raise ValueError("invalid FinLab quota admission context")
    configured = (policy.minute_reserve_mb if kind == "tw_minute" else policy.tick_reserve_mb) if reserve_mb is None else reserve_mb
    if isinstance(configured, bool) or not isinstance(configured, (int, float)) or not math.isfinite(configured) or configured < 0:
        raise ValueError("invalid FinLab local quota floor")
    if (isinstance(scheduled_reserve_mb, bool) or not isinstance(scheduled_reserve_mb, (int, float))
            or not math.isfinite(scheduled_reserve_mb) or scheduled_reserve_mb < 0):
        scheduled_reserve_mb = 500.0  # Unverified future cost, relevant only to Tick.
    reset = quota_cycle_start(now) + timedelta(days=1)
    draining = (policy.drain_before_reset_minutes > 0
                and reset - timedelta(minutes=policy.drain_before_reset_minutes) <= now < reset)
    normal = configured if kind == "tw_minute" else max(configured, scheduled_reserve_mb)
    return {"contract_version": CONTRACT_VERSION, "configuration_fingerprint": policy.fingerprint,
            "kind": kind, "mode": "pre_reset_drain" if draining else "normal",
            "normal_reserve_mb": float(normal), "effective_reserve_mb": 0.0 if draining else float(normal),
            "reset_at_utc": reset.isoformat(),
            "drain_starts_at_utc": (reset - timedelta(minutes=policy.drain_before_reset_minutes)).isoformat(),
            "drain_before_reset_minutes": policy.drain_before_reset_minutes,
            "basis": "local_floor_only; provider_quota_and_due_general_work_remain_authoritative"}


def catalog_current(payload: dict, *, now: datetime, policy: QuotaPolicy) -> bool:
    observed = utc_time(payload.get("observed_at_utc")) if isinstance(payload, dict) else None
    keys = payload.get("keys") if isinstance(payload, dict) else None
    return bool(observed and timedelta(0) <= now - observed < timedelta(minutes=policy.catalog_refresh_minutes)
                and isinstance(keys, list) and keys and all(isinstance(key, str) and key for key in keys))


def publication_recheck_delay(*, now: datetime, policy: QuotaPolicy) -> timedelta:
    """Recheck known pending supply more promptly in the finite reset tail.

    Unknown older history retains its separate daily probe budget. This is a
    local cadence, not a claimed provider publication time or an HTTP quota.
    """
    state = quota_admission(kind="tw_minute", now=now, policy=policy)
    minutes = policy.pre_reset_publication_recheck_minutes if state["mode"] == "pre_reset_drain" else 120
    return timedelta(minutes=minutes)


class QuotaSampleWindow:
    """Short-lived quota evidence; never reuse it across the reset or floor.

    The SDK still checks official quota on every source request. Near the local
    floor we sample before every request. A failed refresh is never stale-valid.
    """
    def __init__(self, policy: QuotaPolicy):
        self.policy = policy
        self.sample: tuple[float, float] | None = None
        self.at_clock: float | None = None
        self.cycle: datetime | None = None
        self.reserve_mb: float | None = None
        self.requests = 0

    def needs_refresh(self, *, now: datetime, clock: float, reserve_mb: float) -> bool:
        return (self.sample is None or self.at_clock is None or self.cycle != quota_cycle_start(now)
                or self.reserve_mb != reserve_mb or clock < self.at_clock
                or clock - self.at_clock >= self.policy.quota_sample_interval_seconds
                or self.requests >= self.policy.quota_sample_max_requests
                or self.sample[0] <= reserve_mb + self.policy.quota_sample_near_floor_mb)

    def observe(self, room: tuple[float, float] | None, *, now: datetime, clock: float,
                reserve_mb: float) -> tuple[float, float] | None:
        if room is not None:
            remaining, limit = room
            if (any(isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) for v in room)
                    or not 0 <= remaining <= limit or limit <= 0):
                room = None
        self.sample, self.at_clock = room, clock
        self.cycle, self.reserve_mb = quota_cycle_start(now), reserve_mb
        self.requests = 0
        return room

    def requested(self) -> None:
        self.requests += 1

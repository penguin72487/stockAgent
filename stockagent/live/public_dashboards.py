"""Public-safe projections for the two read-only live dashboards.

The private dashboards intentionally retain operational identifiers and local
artifact paths for audit work.  Public viewers do not need those fields.  This
module keeps the public boundary explicit and fails closed if a supposedly
read-only payload could represent production order capability.
"""

from __future__ import annotations

from copy import deepcopy
import threading
import time
from typing import Any, Final, Mapping


PUBLIC_MAX_EVENT_ROWS: Final[int] = 250


class UnsafePublicDashboardPayload(ValueError):
    """Raised when an upstream payload violates the public read-only contract."""


def _require_simulation_only(payload: Mapping[str, Any]) -> None:
    if payload.get("simulation_only") is not True:
        raise UnsafePublicDashboardPayload("simulation_only must be true")
    if payload.get("production_order_possible") is not False:
        raise UnsafePublicDashboardPayload("production_order_possible must be false")


def _scrub_tw_value(value: Any) -> Any:
    """Remove local paths and opaque execution identifiers recursively."""

    dropped_keys = {
        "checkpoint_fingerprint",
        "checkpoint_path",
        "config_fingerprint",
        "config_path",
        "live_output_dir",
        "order_id",
        "path",
        "position_id",
        "previous_signal_id",
        "signal_id",
        "source_path",
    }
    if isinstance(value, Mapping):
        output: dict[str, Any] = {}
        for raw_key, item in value.items():
            key = str(raw_key)
            if key in dropped_keys or key.endswith("_path") or key.endswith("_dir"):
                continue
            if key in {"error", "readiness_error"}:
                output[key] = "unavailable" if item else None
                continue
            output[key] = _scrub_tw_value(item)
        return output
    if isinstance(value, list):
        return [_scrub_tw_value(item) for item in value]
    return value


def sanitize_taifex_status(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return the TAIFEX status payload without local receipt/file identities."""

    _require_simulation_only(payload)
    output = deepcopy(dict(payload))
    for source in output.get("sources") or []:
        if isinstance(source, dict):
            source.pop("path", None)
    api_round_trip = output.get("api_round_trip")
    if isinstance(api_round_trip, dict):
        api_round_trip.pop("source_file", None)
    active_cycle = output.get("active_cycle")
    if isinstance(active_cycle, dict):
        active_cycle.pop("cycle_id", None)
    return output


def sanitize_taifex_history(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return bounded display history; it contains no order or account fields."""

    allowed = {
        "dashboard_schema_version",
        "generated_at_utc",
        "history",
        "record_counts",
        "source_updated_at_utc",
    }
    return {key: deepcopy(value) for key, value in payload.items() if key in allowed}


def sanitize_tw_status(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return the stock dashboard status with operational identities removed."""

    _require_simulation_only(payload)
    output = _scrub_tw_value(deepcopy(dict(payload)))
    if not isinstance(output, dict):  # pragma: no cover - defensive typing guard
        raise TypeError("sanitized status is not an object")

    for key in ("orders", "fills", "events"):
        rows = output.get(key)
        if isinstance(rows, list):
            output[key] = rows[-PUBLIC_MAX_EVENT_ROWS:]

    payload_window = output.get("payload_window")
    if isinstance(payload_window, dict):
        for key in ("orders", "fills", "events"):
            rows = output.get(key)
            if isinstance(rows, list):
                payload_window[key] = len(rows)

    source_contract = output.get("source_contract")
    if isinstance(source_contract, dict):
        source_contract["preopen"] = (
            "same-day recorded pre-open stages; missing intermediate states are "
            "not estimated"
        )
        source_contract["signal"] = (
            "recorded live target weights after the observed opening quote"
        )
    return output


def sanitize_tw_signals(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return one public signal page without the internal signal identifiers."""

    output = _scrub_tw_value(deepcopy(dict(payload)))
    if not isinstance(output, dict):  # pragma: no cover - defensive typing guard
        raise TypeError("sanitized signal page is not an object")
    return output


class TokenBucketRateLimiter:
    """Small in-memory per-client limiter for the localhost public gateway."""

    def __init__(
        self,
        *,
        capacity: float = 30.0,
        refill_per_second: float = 1.0,
        maximum_clients: int = 4_096,
    ) -> None:
        if capacity <= 0.0 or refill_per_second <= 0.0 or maximum_clients <= 0:
            raise ValueError("rate limiter settings must be positive")
        self.capacity = float(capacity)
        self.refill_per_second = float(refill_per_second)
        self.maximum_clients = int(maximum_clients)
        self._buckets: dict[str, tuple[float, float, float]] = {}
        self._lock = threading.Lock()

    def allow(
        self,
        client: str,
        *,
        cost: float = 1.0,
        now: float | None = None,
    ) -> bool:
        if cost <= 0.0:
            raise ValueError("rate-limit cost must be positive")
        observed = time.monotonic() if now is None else float(now)
        key = str(client)
        with self._lock:
            previous = self._buckets.get(key)
            if previous is None:
                tokens = self.capacity
            else:
                previous_tokens, previous_at, _last_seen = previous
                elapsed = max(0.0, observed - previous_at)
                tokens = min(
                    self.capacity,
                    previous_tokens + elapsed * self.refill_per_second,
                )
            allowed = tokens >= cost
            if allowed:
                tokens -= cost
            self._buckets[key] = (tokens, observed, observed)
            if len(self._buckets) > self.maximum_clients:
                oldest = sorted(self._buckets.items(), key=lambda item: item[1][2])[
                    : len(self._buckets) - self.maximum_clients
                ]
                for stale_key, _bucket in oldest:
                    self._buckets.pop(stale_key, None)
            return allowed


__all__ = [
    "PUBLIC_MAX_EVENT_ROWS",
    "TokenBucketRateLimiter",
    "UnsafePublicDashboardPayload",
    "sanitize_taifex_history",
    "sanitize_taifex_status",
    "sanitize_tw_signals",
    "sanitize_tw_status",
]

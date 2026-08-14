"""Public-safe projections for the read-only live dashboards.

The private dashboards intentionally retain operational identifiers and local
artifact paths for audit work.  Public viewers do not need those fields.  This
module keeps the public boundary explicit and fails closed if a supposedly
read-only payload could represent production order capability.
"""

from __future__ import annotations

from copy import deepcopy
import math
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


def _scrub_public_value(value: Any) -> Any:
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
        "source_file",
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
            output[key] = _scrub_public_value(item)
        return output
    if isinstance(value, list):
        return [_scrub_public_value(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _project_rows(
    payload: dict[str, Any],
    key: str,
    *,
    allowed_fields: set[str],
) -> None:
    """Keep only fields consumed by the public UI for a repeated row set."""

    rows = payload.get(key)
    if not isinstance(rows, list):
        return
    payload[key] = [
        {
            str(field): value
            for field, value in row.items()
            if str(field) in allowed_fields
        }
        for row in rows
        if isinstance(row, Mapping)
    ]


def sanitize_taifex_status(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return the TAIFEX status payload without local receipt/file identities."""

    _require_simulation_only(payload)
    output = _scrub_public_value(deepcopy(dict(payload)))
    if not isinstance(output, dict):  # pragma: no cover - defensive typing guard
        raise TypeError("sanitized status is not an object")
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
        "range",
        "range_seconds",
        "anchor_at_utc",
        "coverage_start_utc",
        "coverage_end_utc",
        "downsampled",
    }
    output = {
        key: _scrub_public_value(deepcopy(value))
        for key, value in payload.items()
        if key in allowed
    }
    _project_rows(
        output,
        "history",
        allowed_fields={
            "cumulative_pnl_twd",
            "decision_ts_ns",
            "fixed_capital_return",
            "initial_capital_twd",
            "strategy_id",
            "total_equity_twd",
            "valuation_carried_forward",
        },
    )
    history = output.get("history")
    if isinstance(history, list):
        # Public charts render TWD to at most two decimals.  Keep eight decimal
        # places for the fractional return (sub-basis-point precision after
        # conversion to percent) while the private ledger remains untouched.
        for row in history:
            if not isinstance(row, dict):
                continue
            for field in (
                "cumulative_pnl_twd",
                "initial_capital_twd",
                "total_equity_twd",
            ):
                if isinstance(row.get(field), float):
                    row[field] = round(row[field], 2)
            if isinstance(row.get("fixed_capital_return"), float):
                row["fixed_capital_return"] = round(row["fixed_capital_return"], 8)
    return output


def sanitize_tw_status(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return the stock dashboard status with operational identities removed."""

    _require_simulation_only(payload)
    output = _scrub_public_value(deepcopy(dict(payload)))
    if not isinstance(output, dict):  # pragma: no cover - defensive typing guard
        raise TypeError("sanitized status is not an object")

    # Detailed ledgers have their own bounded, server-filtered endpoints.  Do
    # not retransmit hundreds of duplicate rows with every status refresh.
    for key in ("orders", "fills", "events"):
        if isinstance(output.get(key), list):
            output[key] = []

    # These arrays dominate the status payload.  Their complete private rows
    # repeat accounting provenance that is not rendered by the public page.
    # Projecting at the trust boundary reduces JSON parsing and DOM refresh
    # latency without weakening the private audit ledger.
    _project_rows(
        output,
        "positions",
        allowed_fields={
            "counterfactual_open_replay",
            "closing_auction_limit_price",
            "closing_auction_order_status",
            "entry_at",
            "entry_fee_twd",
            "entry_price",
            "eod_limit_order_status",
            "eod_limit_price",
            "eod_limit_submitted_at",
            "exit_at",
            "exit_price",
            "exit_reason",
            "filled_shares",
            "last_complete_net_pnl_twd",
            "last_exit_at",
            "last_exit_price",
            "last_mark_price",
            "last_quote_at",
            "market",
            "name",
            "net_pnl_twd",
            "realized_net_pnl_twd",
            "reconciled_total_net_pnl_twd",
            "requested_shares",
            "side",
            "signed_shares",
            "simulation_replay",
            "source_signal_at",
            "status",
            "stop_order_status",
            "stop_trigger_price",
            "symbol",
            "take_profit_price",
            "target_weight",
            "total_net_pnl_twd",
            "unrealized_net_pnl_twd",
            "valuation_stale",
        },
    )
    _project_rows(
        output,
        "marks",
        allowed_fields={"market", "minute", "return_pct", "valuation_stale"},
    )
    _project_rows(
        output,
        "benchmark_marks",
        allowed_fields={
            "benchmark_id",
            "minute",
            "return_pct",
            "valuation_stale",
        },
    )

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
        source_contract["events"] = (
            "complete selected-day order and fill ledgers are available through "
            "the bounded read-only event pages"
        )
    return output


def sanitize_tw_history(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Project the bounded cross-session curve endpoint for public use."""

    _require_simulation_only(payload)
    allowed = {
        "schema_version",
        "simulation_only",
        "production_order_possible",
        "range",
        "range_seconds",
        "anchor_at_utc",
        "coverage_start_utc",
        "coverage_end_utc",
        "raw_points_in_range",
        "returned_points",
        "downsampled",
        "history",
    }
    output = {
        key: _scrub_public_value(deepcopy(value))
        for key, value in payload.items()
        if key in allowed
    }
    _project_rows(
        output,
        "history",
        allowed_fields={
            "series_id",
            "series_type",
            "market",
            "benchmark_id",
            "minute",
            "return_fraction",
            "return_pct",
            "valuation_stale",
        },
    )
    return output


def sanitize_tw_signals(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return one public signal page without the internal signal identifiers."""

    output = _scrub_public_value(deepcopy(dict(payload)))
    if not isinstance(output, dict):  # pragma: no cover - defensive typing guard
        raise TypeError("sanitized signal page is not an object")
    _project_rows(
        output,
        "rows",
        allowed_fields={
            "action",
            "ask",
            "bid",
            "counterfactual_open_replay",
            "day_trade_eligible",
            "execution_price",
            "filled_shares",
            "market",
            "name",
            "quote_at",
            "raw_score",
            "reason",
            "requested_shares",
            "score",
            "sell_first_allowed",
            "side",
            "signal_at",
            "simulation_replay",
            "source_signal_at",
            "status",
            "symbol",
            "target_weight",
            "top_book_capacity_shares",
        },
    )
    return output


def sanitize_tw_events(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return one public event page after enforcing the simulation boundary."""

    _require_simulation_only(payload)
    output = _scrub_public_value(deepcopy(dict(payload)))
    if not isinstance(output, dict):  # pragma: no cover - defensive typing guard
        raise TypeError("sanitized event page is not an object")
    _project_rows(
        output,
        "rows",
        allowed_fields={
            "event_kind",
            "fill_at",
            "market",
            "order_type",
            "price",
            "pricing_rule",
            "purpose",
            "quantity",
            "recorded_at",
            "side",
            "simulation_only",
            "status",
            "symbol",
        },
    )
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
    "sanitize_tw_events",
    "sanitize_tw_history",
    "sanitize_tw_signals",
    "sanitize_tw_status",
]

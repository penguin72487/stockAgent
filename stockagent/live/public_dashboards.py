"""Public-safe projections for the read-only live dashboards.

The private dashboards intentionally retain operational identifiers and local
artifact paths for audit work.  Public viewers do not need those fields.  This
module keeps the public boundary explicit and fails closed if a supposedly
read-only payload could represent production order capability.
"""

from __future__ import annotations

from collections.abc import Mapping
import math
import threading
import time
from typing import Any, Final


PUBLIC_MAX_EVENT_ROWS: Final[int] = 250
PUBLIC_INITIAL_POSITION_ROWS: Final[int] = 100
PUBLIC_STATUS_FALLBACK_POINTS_PER_SERIES: Final[int] = 2


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
            if (
                key in dropped_keys
                or key.endswith("_path")
                or key.endswith("_dir")
                or key.endswith("_file")
            ):
                continue
            if key == "error" or key.endswith("_error"):
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


def _retain_latest_series_rows(
    payload: dict[str, Any], key: str, *, series_field: str
) -> None:
    """Keep a tiny chart fallback; full curves use the history endpoint."""

    rows = payload.get(key)
    if not isinstance(rows, list):
        return
    by_series: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        series = str(row.get(series_field) or "")
        if not series:
            continue
        bucket = by_series.setdefault(series, [])
        bucket.append(row)
        if len(bucket) > PUBLIC_STATUS_FALLBACK_POINTS_PER_SERIES:
            del bucket[0]
    payload[key] = [row for bucket in by_series.values() for row in bucket]


def sanitize_taifex_status(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return the TAIFEX status payload without local receipt/file identities."""

    _require_simulation_only(payload)
    output = _scrub_public_value(dict(payload))
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
        "backfills",
    }
    output = {
        key: _scrub_public_value(value)
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
            "cumulative_contributed_capital_twd",
            "strategy_id",
            "total_equity_twd",
            "capital_contribution_count",
            "recapitalization_count",
            "bankruptcy_count",
            "entry_state",
            "alive",
            "valuation_carried_forward",
            "history_source",
            "replay_id",
            "replay_contract_version",
            "history_event",
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
                "cumulative_contributed_capital_twd",
                "total_equity_twd",
            ):
                if isinstance(row.get(field), float):
                    row[field] = round(row[field], 2)
            if isinstance(row.get("fixed_capital_return"), float):
                row["fixed_capital_return"] = round(row["fixed_capital_return"], 8)
            for optional_field in (
                "history_source",
                "replay_id",
                "replay_contract_version",
                "history_event",
            ):
                if row.get(optional_field) is None:
                    row.pop(optional_field, None)
            if row.get("history_source") == "live_forward_ledger":
                row.pop("history_source", None)
    return output


def sanitize_tw_status(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return the stock dashboard status with operational identities removed."""

    _require_simulation_only(payload)
    # These arrays dominate the status payload.  Their complete private rows
    # repeat accounting provenance that is not rendered by the public page.
    # Project before the recursive scrub so discarded fields are never copied
    # or inspected on the request path.
    projected = dict(payload)
    for key in ("signals", "orders", "fills", "events"):
        if isinstance(projected.get(key), list):
            projected[key] = []
    _project_rows(
        projected,
        "positions",
        allowed_fields={
            "counterfactual_open_replay",
            "counterfactual_0901_price_fill",
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
            "open_reconstructed_at",
            "realized_net_pnl_twd",
            "reconciled_total_net_pnl_twd",
            "requested_shares",
            "session_date",
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
    positions = projected.get("positions")
    if isinstance(positions, list):
        positions.sort(
            key=lambda row: (
                str(row.get("session_date") or ""),
                abs(float(row.get("target_weight") or 0.0))
                if isinstance(row.get("target_weight"), (int, float))
                and math.isfinite(float(row.get("target_weight") or 0.0))
                else 0.0,
                str(row.get("market") or ""),
                str(row.get("symbol") or ""),
            ),
            reverse=True,
        )
        projected["positions"] = positions[:PUBLIC_INITIAL_POSITION_ROWS]
    _project_rows(
        projected,
        "marks",
        allowed_fields={"market", "minute", "return_pct", "valuation_stale"},
    )
    _project_rows(
        projected,
        "benchmark_marks",
        allowed_fields={
            "benchmark_id",
            "minute",
            "return_pct",
            "valuation_stale",
        },
    )
    _retain_latest_series_rows(projected, "marks", series_field="market")
    _retain_latest_series_rows(
        projected,
        "benchmark_marks",
        series_field="benchmark_id",
    )
    output = _scrub_public_value(projected)
    if not isinstance(output, dict):  # pragma: no cover - defensive typing guard
        raise TypeError("sanitized status is not an object")

    payload_window = output.get("payload_window")
    if isinstance(payload_window, dict):
        for key in ("signals", "orders", "fills", "events"):
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
            "complete selected-range order and fill ledgers are available through "
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
        "start_date",
        "end_date",
        "available_start_date",
        "available_end_date",
        "anchor_at_utc",
        "coverage_start_utc",
        "coverage_end_utc",
        "raw_points_in_range",
        "returned_points",
        "downsampled",
        "curve_granularity",
        "expected_right_labelled_session_minute_points",
        "expected_strategy_session_points_from_09_01",
        "expected_stock_benchmark_session_points_including_09_00",
        "expected_tx_day_session_points",
        "return_basis",
        "range_summary",
        "historical_minute_replay_points",
        "historical_minute_carried_price_points",
        "historical_minute_missing_price_points",
        "historical_minute_min_fresh_trade_notional_coverage_ratio",
        "historical_minute_mean_fresh_trade_notional_coverage_ratio",
        "history",
    }
    output = {
        key: _scrub_public_value(value)
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
            "historical_minute_replay",
            "minute_valuation_contract",
            "valuation_source",
            "valuation_executable",
            "fresh_trade_position_count",
            "last_trade_carried_position_count",
            "missing_price_position_count",
            "fresh_trade_notional_coverage_ratio",
        },
    )
    _project_rows(
        output,
        "range_summary",
        allowed_fields={
            "series_id",
            "series_type",
            "baseline_kind",
            "baseline_at_utc",
            "baseline_equity_twd",
            "start_at_utc",
            "end_at_utc",
            "start_equity_twd",
            "end_equity_twd",
            "range_net_pnl_twd",
            "return_fraction",
            "return_pct",
            "point_count",
            "session_point_counts",
            "expected_minute_points",
            "expected_points_per_session",
            "minute_coverage_ratio",
        },
    )
    return output


def sanitize_tw_signals(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return one public signal page without the internal signal identifiers."""

    _require_simulation_only(payload)
    allowed = {
        "schema_version",
        "simulation_only",
        "production_order_possible",
        "session_date",
        "start_date",
        "end_date",
        "session_dates",
        "available_session_dates",
        "offset",
        "limit",
        "returned",
        "total",
        "has_more",
        "source_rows_scanned",
        "record_count",
        "direction_summary_scope",
        "direction_summary",
        "opening_execution_audit_scope",
        "opening_execution_audit",
        "feature_drivers_scope",
        "feature_drivers_by_signal",
        "rows",
    }
    output = _scrub_public_value(
        {key: value for key, value in payload.items() if key in allowed}
    )
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
            "counterfactual_0901_price_fill",
            "day_trade_eligible",
            "execution_price",
            "filled_shares",
            "market",
            "name",
            "open_reconstructed_at",
            "quote_at",
            "raw_score",
            "reason",
            "requested_shares",
            "score",
            "sell_first_allowed",
            "session_date",
            "side",
            "signal_at",
            "simulation_replay",
            "sizing_open_price",
            "source_signal_at",
            "status",
            "symbol",
            "target_weight",
            "top_book_capacity_shares",
        },
    )
    return output


def sanitize_tw_positions(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return a bounded public position page without internal ledger IDs."""

    _require_simulation_only(payload)
    allowed = {
        "schema_version",
        "simulation_only",
        "production_order_possible",
        "session_date",
        "start_date",
        "end_date",
        "session_dates",
        "available_session_dates",
        "offset",
        "limit",
        "returned",
        "total",
        "has_more",
        "record_count",
        "rows",
    }
    output = _scrub_public_value(
        {key: value for key, value in payload.items() if key in allowed}
    )
    if not isinstance(output, dict):  # pragma: no cover - defensive typing guard
        raise TypeError("sanitized position page is not an object")
    _project_rows(
        output,
        "rows",
        allowed_fields={
            "closing_auction_limit_price",
            "closing_auction_order_status",
            "counterfactual_open_replay",
            "counterfactual_0901_price_fill",
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
            "open_reconstructed_at",
            "realized_net_pnl_twd",
            "reconciled_total_net_pnl_twd",
            "requested_shares",
            "session_date",
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
    return output


def sanitize_tw_events(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return one public event page after enforcing the simulation boundary."""

    _require_simulation_only(payload)
    allowed = {
        "schema_version",
        "simulation_only",
        "production_order_possible",
        "session_date",
        "start_date",
        "end_date",
        "session_dates",
        "available_session_dates",
        "offset",
        "limit",
        "returned",
        "total",
        "order_total",
        "fill_total",
        "has_more",
        "record_counts",
        "rows",
    }
    output = _scrub_public_value(
        {key: value for key, value in payload.items() if key in allowed}
    )
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
            "session_date",
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
    "sanitize_tw_positions",
    "sanitize_tw_signals",
    "sanitize_tw_status",
]

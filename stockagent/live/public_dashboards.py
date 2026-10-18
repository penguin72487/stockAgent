"""Public-safe projections for the read-only live dashboards.

The private dashboards intentionally retain operational identifiers and local
artifact paths for audit work.  Public viewers do not need those fields.  This
module keeps the public boundary explicit and fails closed if a supposedly
read-only payload could represent production order capability.
"""

from __future__ import annotations

from collections.abc import Mapping
from functools import lru_cache
import math
import re
import threading
import time
from typing import Any, Final

from stockagent.live.public_quote_display import (
    CONTRACT as PUBLIC_QUOTE_DISPLAY_CONTRACT,
    PROTECTED_OBSERVATIONS_KEY,
    is_quote_price_field,
    public_quote_display_notice,
    public_quote_display_policy,
)
from stockagent.live.public_daily_quotes import DAILY_QUOTE_FIELD, OfficialDailyQuoteSource
from stockagent.live.public_price_metrics import public_entry_distances


PUBLIC_MAX_EVENT_ROWS: Final[int] = 250
PUBLIC_INITIAL_POSITION_ROWS: Final[int] = 100
PUBLIC_STATUS_FALLBACK_POINTS_PER_SERIES: Final[int] = 2
_TW_PUBLIC_POSITION_FIELDS: Final[set[str]] = {
    "closing_auction_limit_price", "closing_auction_order_status", "closing_auction_order_submitted_at",
    "counterfactual_open_replay", "counterfactual_0901_price_fill",
    "entry_at", "entry_exchange_at", "entry_fee_twd", "entry_price",
    "entry_price_source", "entry_completion_broker_fill",
    "entry_completion_contract", "entry_completion_recorded_at",
    "inventory_basis_price", "odd_lot_execution_policy",
    "share_replacement_contract", "share_replacement_halted_until",
    "eod_limit_order_status", "eod_limit_price", "eod_limit_submitted_at",
    "exit_at", "exit_exchange_at", "exit_price", "exit_reason",
    "exit_session_date", "filled_shares", "last_complete_net_pnl_twd",
    "last_exit_at", "last_exit_price", "last_mark_price",
    "last_mark_quote_source", "last_quote_at", "market",
    "mandatory_exit_phase", "name", "net_pnl_twd", "open_reconstructed_at",
    "opening_exit_limit_price", "opening_exit_order_session_date",
    "opening_exit_order_status", "opening_exit_submitted_at",
    "realized_net_pnl_twd", "reconciled_total_net_pnl_twd", "requested_shares",
    "session_date", "security_type", "side", "signed_shares", "simulation_replay",
    "source_signal_at", "status", "stop_order_status", "stop_trigger_price",
    "symbol", "take_profit_price", "target_weight", "target_unsubmitted_shares",
    "temporary_day_trade_model_adapter", "total_net_pnl_twd",
    "unrealized_net_pnl_twd", "valuation_stale", "public_price_display",
    PROTECTED_OBSERVATIONS_KEY,
}
_TAIFEX_HISTORY_ROW_FIELDS: Final[tuple[str, ...]] = (
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
)
_TAIFEX_TWD_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "cumulative_pnl_twd",
        "initial_capital_twd",
        "cumulative_contributed_capital_twd",
        "total_equity_twd",
    }
)


class UnsafePublicDashboardPayload(ValueError):
    """Raised when an upstream payload violates the public read-only contract."""


_PUBLIC_DROPPED_KEYS: Final[frozenset[str]] = frozenset({
    "checkpoint_fingerprint", "checkpoint_path", "config_fingerprint",
    "config_path", "daily_return_previous_close_source", "live_output_dir",
    "order_id", "path", "position_id", "previous_signal_id", "signal_id",
    "source_file", "source_path", PROTECTED_OBSERVATIONS_KEY,
})
_PUBLIC_CREDENTIAL_KEYS: Final[frozenset[str]] = frozenset({
    "apikey", "apisecret", "secret", "secretkey", "password", "passwd",
    "authorization", "cookie", "setcookie", "token", "accesstoken",
    "refreshtoken", "privatekey", "accountid", "accountnumber", "brokerid",
})
_PUBLIC_KEY_NORMALIZER: Final[re.Pattern[str]] = re.compile(r"[^a-z0-9]")


def _public_key_disposition_uncached(key: str) -> str:
    normalized = _PUBLIC_KEY_NORMALIZER.sub("", key.lower())
    if (
        key in _PUBLIC_DROPPED_KEYS
        or normalized in _PUBLIC_CREDENTIAL_KEYS
        or key.endswith(("_path", "_dir", "_file"))
    ):
        return "drop"
    if key == "error" or key.endswith("_error"):
        return "redact_error"
    return "keep"


@lru_cache(maxsize=512)
def _public_key_disposition_cached(key: str) -> str:
    return _public_key_disposition_uncached(key)


def _public_key_disposition(key: str) -> str:
    # Do not retain an unbounded source-controlled key in the process cache.
    if len(key) > 128:
        return _public_key_disposition_uncached(key)
    return _public_key_disposition_cached(key)


def _require_simulation_only(payload: Mapping[str, Any]) -> None:
    if payload.get("simulation_only") is not True:
        raise UnsafePublicDashboardPayload("simulation_only must be true")
    if payload.get("production_order_possible") is not False:
        raise UnsafePublicDashboardPayload("production_order_possible must be false")


def _public_replay_signal_origin(row: Mapping[str, Any]) -> str | None:
    """Disclose retrospective generation without publishing private signal IDs."""
    if row.get("counterfactual_open_replay") is not True:
        return None
    match = re.fullmatch(r".+-counterfactual-official-open-(\d{4}-\d{2}-\d{2})",
                         str(row.get("signal_id") or ""))
    if match and match.group(1) == str(row.get("session_date") or "")[:10]:
        return "current_model_counterfactual"
    return None


def _public_projected_row(row: Mapping[str, Any], allowed_fields: set[str]) -> dict[str, Any]:
    output = {str(field): value for field, value in row.items() if str(field) in allowed_fields}
    origin = _public_replay_signal_origin(row)
    if origin is not None:
        output["replay_signal_origin"] = origin
    return output


def _scrub_public_value(
    value: Any, *, quote_source: OfficialDailyQuoteSource | None = None,
    session_date: str | None = None,
    record_kind: str | None = None,
    record_collections: Mapping[str, str] | None = None,
    private_prices: bool = False,
) -> Any:
    """Strip private identifiers and unlicensed prices without mutating inputs."""

    if isinstance(value, Mapping):
        session_date = value.get("session_date") or session_date
        if quote_source is not None:
            value = quote_source.enrich(value, session_date=session_date)
        output: dict[str, Any] = {}
        origin = _public_replay_signal_origin(value)
        if origin is not None:
            output["replay_signal_origin"] = origin
        withheld: list[str] = []
        withheld_reasons: dict[str, str] = {}
        visible: dict[str, Any] = {}
        quote_policy = public_quote_display_policy()
        for raw_key, item in value.items():
            key = str(raw_key)
            disposition = _public_key_disposition(key)
            if disposition == "drop":
                continue
            if disposition == "redact_error":
                output[key] = "unavailable" if item else None
                continue
            # Publication is independent of the collector/model/ledger. A
            # provider's API access or an upstream boolean is not a grant.
            if is_quote_price_field(key):
                if private_prices:
                    output[key] = _scrub_public_value(item, private_prices=True)
                    if output[key] is not None:
                        visible[key] = {"source": "本人私人檢視", "price_role": key,
                                        "publication_basis": "owner_private_view"}
                    continue
                permitted = quote_policy.observation(value, key, item)
                if permitted is None and record_kind is not None:
                    permitted = quote_policy.strategy_report_price(
                        value, key, item, record_kind=record_kind,
                    )
                if permitted is None and record_kind is not None:
                    permitted = quote_policy.transaction_report_price(
                        value, key, item, record_kind=record_kind,
                    )
                if permitted is not None:
                    output[key], visible[key] = permitted
                else:
                    output[key] = None
                    if item is not None and not (
                        isinstance(item, float) and not math.isfinite(item)
                    ):
                        withheld.append(key)
                        withheld_reasons[key] = quote_policy.withheld_reason(value, key)
                continue
            output[key] = _scrub_public_value(
                item, quote_source=None if key == DAILY_QUOTE_FIELD else quote_source,
                session_date=session_date,
                record_kind=((record_collections or {}).get(key)
                             or ("paper_position" if record_kind == "paper_signal" and key == "position" else None)),
                private_prices=private_prices,
            )
        if withheld or visible:
            output["public_price_display"] = {
                "contract": "owner_private_quote_display_v1" if private_prices else PUBLIC_QUOTE_DISPLAY_CONTRACT,
                "withheld_fields": withheld,
                "withheld_reasons": withheld_reasons,
                "visible_fields": visible,
            }
        if record_kind == "paper_position":
            # Derived movements must never expose a withheld valuation. The
            # canonical dated grid is consulted only on the already-public DTO.
            distances = public_entry_distances(output)
            if distances:
                output["entry_price_distances"] = distances
        return output
    if isinstance(value, list):
        return [_scrub_public_value(item, quote_source=quote_source, session_date=session_date,
                                   record_kind=record_kind, private_prices=private_prices) for item in value]
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
        _public_projected_row(row, allowed_fields)
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


def _price_display_notice(*, private_prices: bool, include_paper_reports: bool = False) -> dict[str, Any]:
    if private_prices:
        return {"contract": "owner_private_quote_display_v1", "private_view": True,
                "message": "本人私人檢視：顯示既有來源觀測與帳本原始價格；缺值仍為未知，模擬成交仍非券商成交。"}
    return public_quote_display_notice(include_paper_reports=include_paper_reports)


def sanitize_taifex_status(payload: Mapping[str, Any], *, private_prices: bool = False) -> dict[str, Any]:
    """Return the TAIFEX status payload without local receipt/file identities."""

    _require_simulation_only(payload)
    output = _scrub_public_value(dict(payload), private_prices=private_prices)
    if not isinstance(output, dict):  # pragma: no cover - defensive typing guard
        raise TypeError("sanitized status is not an object")
    active_cycle = output.get("active_cycle")
    if isinstance(active_cycle, dict):
        active_cycle.pop("cycle_id", None)
    output["price_display_policy"] = _price_display_notice(private_prices=private_prices)
    return output


def sanitize_taifex_history(payload: Mapping[str, Any], *, private_prices: bool = False) -> dict[str, Any]:
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
    # History is the only large field. Project its fixed public contract before
    # recursive credential scrubbing: scanning every already-allowlisted key
    # with a regular expression made cold requests CPU-bound. Unexpected nested
    # values still take the conservative recursive scrub path.
    output = {
        key: _scrub_public_value(value, private_prices=private_prices)
        for key, value in payload.items()
        if key in allowed and key != "history"
    }
    rows = payload.get("history")
    if isinstance(rows, list):
        history: list[dict[str, Any]] = []
        for source_row in rows:
            if not isinstance(source_row, Mapping):
                continue
            row: dict[str, Any] = {}
            for field in _TAIFEX_HISTORY_ROW_FIELDS:
                if field not in source_row:
                    continue
                value = source_row[field]
                if isinstance(value, float):
                    value = None if not math.isfinite(value) else value
                    if value is not None:
                        if field in _TAIFEX_TWD_FIELDS:
                            value = round(value, 2)
                        elif field == "fixed_capital_return":
                            value = round(value, 8)
                elif isinstance(value, (Mapping, list)):
                    value = _scrub_public_value(value, private_prices=private_prices)
                row[field] = value
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
            history.append(row)
        output["history"] = history
    return output


def sanitize_tw_status(
    payload: Mapping[str, Any], *, quote_source: OfficialDailyQuoteSource | None = None,
    private_prices: bool = False,
) -> dict[str, Any]:
    """Return the stock dashboard status with operational identities removed."""

    _require_simulation_only(payload)
    # These arrays dominate the status payload.  Their complete private rows
    # repeat accounting provenance that is not rendered by the public page.
    # Project before the recursive scrub so discarded fields are never copied
    # or inspected on the request path.
    projected = dict(payload)
    if quote_source is not None:
        # Independent public reference stays useful even when a strategy has
        # no signals. It is not a position, fill, valuation or readiness proof.
        selected_session = str(payload.get("session_date") or "")
        reference_session = selected_session
        if not quote_source.session(selected_session):
            reference_session = quote_source.prior_completed_session(selected_session) or selected_session
        projected["official_price_reference"] = {
            "session_date": reference_session, "requested_session_date": selected_session,
            "role": "reference_only",
            "quotes": [{"symbol": symbol} for symbol in ("0050", "2330")],
        }
    for key in ("signals", "orders", "fills", "events"):
        if isinstance(projected.get(key), list):
            projected[key] = []
    opening_latency = projected.get("opening_signal_latency")
    if isinstance(opening_latency, Mapping):
        # The page renders historical aggregate timings, while today's modes
        # remain in the top-level summary. Repeating every mode and failure
        # receipt in each trend row adds >100 KiB to every status refresh.
        latency_summary = dict(opening_latency)
        trend = latency_summary.get("trend")
        if isinstance(trend, list):
            latency_summary["trend"] = [
                {key: value for key, value in row.items()
                 if key not in ("modes", "failures")}
                if isinstance(row, Mapping) else row
                for row in trend
            ]
        projected["opening_signal_latency"] = latency_summary
    _project_rows(
        projected,
        "positions",
        allowed_fields={
            "counterfactual_open_replay",
            "counterfactual_0901_price_fill",
            "counterfactual_overnight_replay",
            "closing_auction_limit_price",
            "closing_auction_order_status",
            "closing_auction_order_submitted_at",
            "entry_at",
            "entry_exchange_at",
            "entry_fee_twd",
            "entry_price",
            "entry_price_source",
            "inventory_basis_price",
            "odd_lot_execution_policy",
            "share_replacement_contract",
            "share_replacement_halted_until",
            "eod_limit_order_status",
            "eod_limit_price",
            "eod_limit_submitted_at",
            "exit_at",
            "exit_exchange_at",
            "exit_price",
            "exit_reason",
            "exit_session_date",
            "filled_shares",
            "last_complete_net_pnl_twd",
            "last_exit_at",
            "last_exit_price",
            "last_mark_price",
            "last_quote_at",
            "market",
            "mandatory_exit_phase",
            "name",
            "net_pnl_twd",
            "open_reconstructed_at",
            "opening_exit_limit_price",
            "opening_exit_order_session_date",
            "opening_exit_order_status",
            "opening_exit_submitted_at",
            "realized_net_pnl_twd",
            "reconciled_total_net_pnl_twd",
            "requested_shares",
            "session_date",
            "security_type",
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
            "temporary_day_trade_model_adapter",
            "total_net_pnl_twd",
            "unrealized_net_pnl_twd",
            "valuation_stale",
            "public_price_display",
            PROTECTED_OBSERVATIONS_KEY,
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
    output = _scrub_public_value(projected, quote_source=quote_source,
                                 record_collections={"positions": "paper_position"}, private_prices=private_prices)
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
    output["price_display_policy"] = _price_display_notice(private_prices=private_prices, include_paper_reports=True)
    return output


def sanitize_tw_history(payload: Mapping[str, Any], *, private_prices: bool = False) -> dict[str, Any]:
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
        "history_contract",
        "expected_right_labelled_session_minute_points",
        "expected_strategy_session_points_from_09_01",
        "expected_stock_benchmark_session_points_including_09_00",
        "expected_tx_day_session_points",
        "expected_overnight_auction_event_points",
        "return_basis",
        "cumulative_return_basis",
        "period_return_basis",
        "range_summary",
        "historical_minute_replay_points",
        "historical_minute_carried_price_points",
        "historical_minute_missing_price_points",
        "historical_minute_min_fresh_trade_notional_coverage_ratio",
        "historical_minute_mean_fresh_trade_notional_coverage_ratio",
        "historical_minute_valuation_contracts",
        "historical_minute_valuation_sources",
        "history",
    }
    output = {
        key: _scrub_public_value(value, private_prices=private_prices)
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
            "missing_price_position_count",
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
            "initial_capital_twd",
            "start_at_utc",
            "end_at_utc",
            "start_equity_twd",
            "end_equity_twd",
            "range_net_pnl_twd",
            "return_fraction",
            "return_pct",
            "cumulative_net_pnl_twd",
            "cumulative_return_fraction",
            "cumulative_return_pct",
            "period_return_fraction",
            "period_return_pct",
            "point_count",
            "session_point_counts",
            "expected_minute_points",
            "expected_points_per_session",
            "minute_coverage_ratio",
        },
    )
    if payload.get("history_encoding") == "minute_columns_v1":
        series = []
        for raw in payload.get("minute_series") or ():
            if not isinstance(raw, Mapping):
                raise UnsafePublicDashboardPayload("invalid minute series")
            points = []
            for point in raw.get("points") or ():
                if (
                    not isinstance(point, (list, tuple))
                    or len(point) != 4
                    or any(type(value) not in (int, float) or not math.isfinite(value)
                           for value in point)
                    or int(point[0]) != point[0]
                    or int(point[3]) != point[3]
                    or not 0 <= point[3] <= 7
                ):
                    raise UnsafePublicDashboardPayload("invalid minute point")
                points.append(list(point))
            series.append({
                "series_id": _scrub_public_value(str(raw.get("series_id") or "")),
                "series_type": "benchmark" if raw.get("series_type") == "benchmark" else "strategy",
                "points": points,
            })
        output["history_encoding"] = "minute_columns_v1"
        output["minute_series"] = series
    elif payload.get("history_encoding") == "minute_columns_v2":
        raw_axis = payload.get("minute_axis")
        if not isinstance(raw_axis, (list, tuple)):
            raise UnsafePublicDashboardPayload("invalid minute axis")
        minute_axis: list[int] = []
        previous_minute: int | None = None
        for raw_minute in raw_axis:
            if (
                type(raw_minute) not in (int, float)
                or not math.isfinite(raw_minute)
                or int(raw_minute) != raw_minute
            ):
                raise UnsafePublicDashboardPayload("invalid minute axis value")
            minute = int(raw_minute)
            if previous_minute is not None and minute <= previous_minute:
                raise UnsafePublicDashboardPayload("minute axis must increase")
            minute_axis.append(minute)
            previous_minute = minute
        series = []
        observed_points = 0
        observed_ids: set[str] = set()
        for raw in payload.get("minute_series") or ():
            if not isinstance(raw, Mapping):
                raise UnsafePublicDashboardPayload("invalid minute series")
            series_id = str(raw.get("series_id") or "")
            if not series_id or series_id in observed_ids:
                raise UnsafePublicDashboardPayload("invalid minute series id")
            observed_ids.add(series_id)
            raw_indexes = raw.get("minute_indexes")
            raw_returns = raw.get("return_pct")
            raw_cumulative = raw.get("cumulative_return_pct")
            raw_quality = raw.get("quality_flags")
            columns = (raw_indexes, raw_returns, raw_cumulative, raw_quality)
            if any(not isinstance(column, (list, tuple)) for column in columns):
                raise UnsafePublicDashboardPayload("invalid minute columns")
            column_length = len(raw_indexes)
            if any(len(column) != column_length for column in columns[1:]):
                raise UnsafePublicDashboardPayload("unequal minute columns")
            indexes: list[int] = []
            previous_index: int | None = None
            for raw_index in raw_indexes:
                if (
                    type(raw_index) not in (int, float)
                    or not math.isfinite(raw_index)
                    or int(raw_index) != raw_index
                ):
                    raise UnsafePublicDashboardPayload("invalid minute index")
                index = int(raw_index)
                if (
                    index < 0
                    or index >= len(minute_axis)
                    or (previous_index is not None and index <= previous_index)
                ):
                    raise UnsafePublicDashboardPayload("minute index out of order")
                indexes.append(index)
                previous_index = index

            def finite_numeric_column(values: list[Any] | tuple[Any, ...]) -> list[Any]:
                if any(
                    type(value) not in (int, float) or not math.isfinite(value)
                    for value in values
                ):
                    raise UnsafePublicDashboardPayload("invalid numeric minute column")
                return list(values)

            quality: list[int] = []
            for raw_flag in raw_quality:
                if (
                    type(raw_flag) not in (int, float)
                    or not math.isfinite(raw_flag)
                    or int(raw_flag) != raw_flag
                    or not 0 <= raw_flag <= 7
                ):
                    raise UnsafePublicDashboardPayload("invalid minute quality flag")
                quality.append(int(raw_flag))
            observed_points += column_length
            series.append(
                {
                    "series_id": _scrub_public_value(series_id),
                    "series_type": (
                        "benchmark"
                        if raw.get("series_type") == "benchmark"
                        else "strategy"
                    ),
                    "minute_indexes": indexes,
                    "return_pct": finite_numeric_column(raw_returns),
                    "cumulative_return_pct": finite_numeric_column(raw_cumulative),
                    "quality_flags": quality,
                }
            )
        if observed_points != int(payload.get("returned_points") or 0):
            raise UnsafePublicDashboardPayload("minute point count mismatch")
        output["history_encoding"] = "minute_columns_v2"
        output["minute_axis"] = minute_axis
        output["minute_series"] = series
    return output


def sanitize_tw_signals(
    payload: Mapping[str, Any], *, quote_source: OfficialDailyQuoteSource | None = None,
    private_prices: bool = False,
) -> dict[str, Any]:
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
        "scan_limit",
        "scan_limit_reached",
        "record_count",
        "direction_summary_scope",
        "direction_summary",
        "opening_execution_audit_scope",
        "opening_execution_audit",
        "feature_drivers_scope",
        "feature_drivers_by_signal",
        "rows",
    }
    # Project first and scrub exactly once after nested lifecycle projection:
    # stripping the authorized observation before that projection would lose
    # its proof, and re-scrubbing a permitted public quote would hide it again.
    output = {key: value for key, value in payload.items() if key in allowed}
    _project_rows(
        output,
        "rows",
        allowed_fields={
            "action",
            "ask",
            "bid",
            "counterfactual_open_replay",
            "counterfactual_0901_price_fill",
            "counterfactual_overnight_replay",
            "day_trade_eligible",
            "execution_price",
            "entry_completion_broker_fill",
            "entry_completion_contract",
            "entry_completion_recorded_at",
            "exchange_quote_at",
            "filled_shares",
            "initial_filled_shares",
            "lower_limit",
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
            "simtrade",
            "simulation_replay",
            "sizing_open_price",
            "sizing_capital_twd",
            "sizing_price_at_decision",
            "sizing_price_at_13_25",
            "source_signal_at",
            "status",
            "symbol",
            "target_weight",
            "target_unsubmitted_shares",
            "temporary_day_trade_model_adapter",
            "top_book_capacity_shares",
            "order_limit_price",
            "position",
            "upper_limit",
            "model_trained_for_overnight",
            "stock_futures",
            "public_price_display",
            PROTECTED_OBSERVATIONS_KEY,
        },
    )
    # Keep the enrichment DTO bounded just like the surrounding signal row.
    for row in output.get("rows", []):
        position = row.get("position")
        if isinstance(position, dict):
            # Reuse the lifecycle endpoint's public projection. Embedding a
            # valuation must not expose internal ledger IDs or private fields.
            projected = _public_projected_row(position, _TW_PUBLIC_POSITION_FIELDS)
            if position.get("contract") == "signal_ledger_valuation_v1":
                projected["contract"] = "signal_ledger_valuation_v1"
            row["position"] = projected
        else:
            row.pop("position", None)
        membership = row.get("stock_futures")
        if isinstance(membership, dict):
            row["stock_futures"] = {key: membership[key] for key in (
                "status", "products", "as_of", "scope", "reason", "source_url"
            ) if key in membership}
    return _scrub_public_value(output, quote_source=quote_source,
                               record_collections={"rows": "paper_signal"}, private_prices=private_prices)


def sanitize_tw_positions(
    payload: Mapping[str, Any], *, quote_source: OfficialDailyQuoteSource | None = None,
    private_prices: bool = False,
) -> dict[str, Any]:
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
    output = {key: value for key, value in payload.items() if key in allowed}
    _project_rows(
        output,
        "rows",
        allowed_fields=_TW_PUBLIC_POSITION_FIELDS,
    )
    return _scrub_public_value(output, quote_source=quote_source,
                               record_collections={"rows": "paper_position"}, private_prices=private_prices)


def sanitize_tw_events(
    payload: Mapping[str, Any], *, quote_source: OfficialDailyQuoteSource | None = None,
    private_prices: bool = False,
) -> dict[str, Any]:
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
        "source_rows_scanned",
        "scan_limit",
        "scan_limit_reached",
        "record_counts",
        "rows",
    }
    output = _scrub_public_value(
        {key: value for key, value in payload.items() if key in allowed},
        quote_source=quote_source,
        record_collections={"rows": "paper_event"},
        private_prices=private_prices,
    )
    if not isinstance(output, dict):  # pragma: no cover - defensive typing guard
        raise TypeError("sanitized event page is not an object")
    _project_rows(
        output,
        "rows",
        allowed_fields={
            "event_kind",
            "fill_at",
            "exchange_match_at",
            "fill_contract",
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
            "depth_assumption",
            "odd_lot_execution_policy",
            "public_price_display",
            DAILY_QUOTE_FIELD,
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

"""Read-only dashboard snapshot for the live TAIFEX simulation engine.

The strategy process owns all decisions, broker callbacks, and durable ledgers.
This module only reads already-committed artifacts, derives bounded display
metrics, and deliberately omits account identifiers and broker order IDs.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
from statistics import median
import threading
from typing import Any, Final, Mapping

from stockagent.data.taifex_sessions import (
    TAIPEI,
    taifex_market_phase,
    taifex_trading_date,
)
from stockagent.research.taifex_volatility_metadata import (
    EXPOSURE_RATIO_BASIS,
    EXPOSURE_TAXONOMY,
    STRATEGY_CATALOG,
    STRATEGY_SPEC_BY_ID,
)


DASHBOARD_SCHEMA_VERSION: Final[int] = 6
DEFAULT_MAX_SOURCE_AGE_SECONDS: Final[float] = 15.0
DEFAULT_MARK_LIMIT_PER_STRATEGY: Final[int] = 360

_LINE_COUNT_LOCK = threading.Lock()
_LINE_COUNT_CACHE: dict[Path, tuple[int, int, int, int, int]] = {}


@dataclass
class _DailyEndpointCache:
    device: int
    inode: int
    offset: int = 0
    file_size: int = 0
    mtime_ns: int = 0
    endpoints: dict[str, dict[str, tuple[int, float]]] = field(default_factory=dict)


_PERFORMANCE_LOCK = threading.Lock()
_PERFORMANCE_CACHE: dict[Path, _DailyEndpointCache] = {}


def _load_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON root is not an object: {path}")
    return payload


def _parse_utc(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp has no timezone: {value!r}")
    return parsed.astimezone(timezone.utc)


def _optional_float(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and abs(number) != float("inf") else None


def _line_count(path: Path) -> int:
    """Count append-only rows while only reading bytes added since last refresh."""

    if not path.is_file():
        return 0
    cache_key = path.resolve()
    with path.open("rb") as handle:
        stat = os.fstat(handle.fileno())
        with _LINE_COUNT_LOCK:
            cached = _LINE_COUNT_CACHE.get(cache_key)
            if cached and cached[:4] == (
                stat.st_dev,
                stat.st_ino,
                stat.st_size,
                stat.st_mtime_ns,
            ):
                return cached[4]
            append_offset = (
                cached[2]
                if cached
                and cached[:2] == (stat.st_dev, stat.st_ino)
                and stat.st_size >= cached[2]
                else 0
            )
            previous_count = cached[4] if append_offset else 0
        handle.seek(append_offset)
        appended = handle.read(stat.st_size - append_offset)
        count = previous_count + appended.count(b"\n")
    with _LINE_COUNT_LOCK:
        _LINE_COUNT_CACHE[cache_key] = (
            stat.st_dev,
            stat.st_ino,
            stat.st_size,
            stat.st_mtime_ns,
            count,
        )
    return count


def _tail_json_objects(path: Path, *, maximum_rows: int) -> list[dict[str, Any]]:
    if maximum_rows <= 0 or not path.is_file():
        return []
    chunk_size = 64 * 1024
    with path.open("rb") as handle:
        position = handle.seek(0, 2)
        buffer = b""
        while position > 0 and buffer.count(b"\n") <= maximum_rows:
            read_size = min(chunk_size, position)
            position -= read_size
            handle.seek(position)
            buffer = handle.read(read_size) + buffer
    raw_rows = deque(
        (line for line in buffer.splitlines() if line.strip()),
        maxlen=maximum_rows,
    )
    output: list[dict[str, Any]] = []
    for line in raw_rows:
        payload = json.loads(line)
        if isinstance(payload, dict):
            output.append(payload)
    return output


def _trading_date_from_ns(decision_ts_ns: int) -> str | None:
    if decision_ts_ns <= 0:
        return None
    observed_at = datetime.fromtimestamp(decision_ts_ns / 1e9, tz=TAIPEI)
    return taifex_trading_date(observed_at).isoformat()


def _daily_pnl_endpoints(
    path: Path,
    *,
    strategy_ids: tuple[str, ...],
) -> dict[str, dict[str, tuple[int, float]]]:
    """Incrementally retain the last valid P&L mark per TAIFEX trading date."""

    output = {strategy_id: {} for strategy_id in strategy_ids}
    if not path.is_file():
        return output
    cache_key = path.resolve()
    with _PERFORMANCE_LOCK, path.open("rb") as handle:
        stat = os.fstat(handle.fileno())
        cached = _PERFORMANCE_CACHE.get(cache_key)
        if (
            cached is None
            or (cached.device, cached.inode) != (stat.st_dev, stat.st_ino)
            or stat.st_size < cached.offset
        ):
            cached = _DailyEndpointCache(
                device=stat.st_dev,
                inode=stat.st_ino,
            )
            _PERFORMANCE_CACHE[cache_key] = cached
        if cached.file_size != stat.st_size or cached.mtime_ns != stat.st_mtime_ns:
            handle.seek(cached.offset)
            appended = handle.read(stat.st_size - cached.offset)
            complete_size = appended.rfind(b"\n") + 1
            for line in appended[:complete_size].splitlines():
                if not line.strip():
                    continue
                try:
                    mark = json.loads(line)
                except (UnicodeDecodeError, json.JSONDecodeError):
                    continue
                if not isinstance(mark, Mapping):
                    continue
                strategy_id = str(mark.get("strategy_id") or "")
                if strategy_id not in output:
                    continue
                if mark.get("valuation_available") is False:
                    continue
                has_validity_flags = (
                    "option_books_valid" in mark or "future_book_valid" in mark
                )
                if (
                    "valuation_available" not in mark
                    and has_validity_flags
                    and not (
                        bool(mark.get("option_books_valid"))
                        and bool(mark.get("future_book_valid"))
                    )
                ):
                    continue
                pnl = _optional_float(
                    mark.get("cumulative_pnl_twd", mark.get("net_equity_twd"))
                )
                decision_ts_ns = int(mark.get("decision_ts_ns") or 0)
                trading_date = _trading_date_from_ns(decision_ts_ns)
                if pnl is None or trading_date is None:
                    continue
                strategy_endpoints = cached.endpoints.setdefault(strategy_id, {})
                previous = strategy_endpoints.get(trading_date)
                if previous is None or decision_ts_ns >= previous[0]:
                    strategy_endpoints[trading_date] = (decision_ts_ns, pnl)
            cached.offset += complete_size
            cached.file_size = stat.st_size
            cached.mtime_ns = stat.st_mtime_ns
        for strategy_id in strategy_ids:
            output[strategy_id] = dict(cached.endpoints.get(strategy_id, {}))
    return output


def _performance_metrics(
    *,
    daily_endpoints: Mapping[str, tuple[int, float]],
    current_trading_date: str | None,
    current_ts_ns: int,
    current_pnl_twd: float | None,
    reserved_capital_twd: float,
    explicit_cost_twd: float,
    margin_required_twd: float | None,
) -> dict[str, Any]:
    endpoints = dict(daily_endpoints)
    if current_trading_date and current_pnl_twd is not None:
        previous = endpoints.get(current_trading_date)
        if previous is None or current_ts_ns >= previous[0]:
            endpoints[current_trading_date] = (current_ts_ns, current_pnl_twd)

    valid_capital = math.isfinite(reserved_capital_twd) and reserved_capital_twd > 0.0
    fixed_return = (
        current_pnl_twd / reserved_capital_twd
        if valid_capital and current_pnl_twd is not None
        else None
    )
    compound_factor = 1.0
    previous_pnl = 0.0
    observed_days = 0
    ruined = False
    for trading_date in sorted(endpoints):
        _timestamp, end_pnl = endpoints[trading_date]
        if not valid_capital or not math.isfinite(end_pnl):
            continue
        daily_return = (end_pnl - previous_pnl) / reserved_capital_twd
        observed_days += 1
        previous_pnl = end_pnl
        if ruined or daily_return <= -1.0:
            compound_factor = 0.0
            ruined = True
        else:
            compound_factor *= 1.0 + daily_return
    compounded_return = compound_factor - 1.0 if observed_days else None
    trading_dates = sorted(endpoints)
    return {
        "reserved_capital_twd": reserved_capital_twd,
        "one_unit_net_pnl_twd": current_pnl_twd,
        "one_unit_net_pnl_abs_twd": (
            abs(current_pnl_twd) if current_pnl_twd is not None else None
        ),
        "fixed_capital_return": fixed_return,
        "compounded_return_to_live_mark": compounded_return,
        "explicit_cost_twd": explicit_cost_twd,
        "net_pnl_to_explicit_cost_ratio": (
            current_pnl_twd / explicit_cost_twd
            if current_pnl_twd is not None and explicit_cost_twd > 0.0
            else None
        ),
        "margin_utilization": (
            margin_required_twd / reserved_capital_twd
            if valid_capital and margin_required_twd is not None
            else None
        ),
        "observed_trading_day_count": observed_days,
        "first_observed_trading_date": trading_dates[0] if trading_dates else None,
        "last_observed_trading_date": trading_dates[-1] if trading_dates else None,
        "compound_includes_partial_trading_day": bool(
            trading_dates
            and current_trading_date
            and trading_dates[-1] == current_trading_date
        ),
    }


def _portfolio_summary(strategy_rows: list[dict[str, Any]]) -> dict[str, Any]:
    valid = [
        row for row in strategy_rows if row.get("fixed_capital_return") is not None
    ]
    ordered = sorted(valid, key=lambda row: row["fixed_capital_return"])
    best = ordered[-1] if ordered else None
    worst = ordered[0] if ordered else None
    return {
        "strategy_count": len(strategy_rows),
        "valid_return_count": len(valid),
        "independent_strategy_reserved_capital_twd": sum(
            float(row.get("reserved_capital_twd") or 0.0) for row in strategy_rows
        ),
        "independent_strategy_explicit_cost_twd": sum(
            float(row.get("explicit_cost_twd") or 0.0) for row in strategy_rows
        ),
        "median_fixed_capital_return": (
            median(float(row["fixed_capital_return"]) for row in valid)
            if valid
            else None
        ),
        "best_strategy": (
            {
                "strategy_id": best["strategy_id"],
                "label": best["label"],
                "fixed_capital_return": best["fixed_capital_return"],
            }
            if best
            else None
        ),
        "worst_strategy": (
            {
                "strategy_id": worst["strategy_id"],
                "label": worst["label"],
                "fixed_capital_return": worst["fixed_capital_return"],
            }
            if worst
            else None
        ),
    }


def _live_position_exposure(
    option_positions: object,
    futures_position: object,
) -> dict[str, Any]:
    positions = option_positions if isinstance(option_positions, Mapping) else {}
    long_contracts = 0
    short_contracts = 0
    for raw_quantity in positions.values():
        try:
            quantity = int(raw_quantity)
        except (TypeError, ValueError):
            continue
        if quantity > 0:
            long_contracts += quantity
        elif quantity < 0:
            short_contracts += -quantity
    gross_contracts = long_contracts + short_contracts
    if gross_contracts > 0:
        long_ratio = long_contracts / gross_contracts
        short_ratio = short_contracts / gross_contracts
        net_ratio = (long_contracts - short_contracts) / gross_contracts
        option_ratio_label = (
            f"多 {long_ratio:.0%} / 空 {short_ratio:.0%} "
            f"({long_contracts}:{short_contracts} 口)"
        )
    else:
        long_ratio = short_ratio = net_ratio = None
        option_ratio_label = "目前無選擇權部位"
    try:
        future_quantity = int(futures_position)
    except (TypeError, ValueError):
        future_quantity = 0
    if future_quantity > 0:
        future_direction = "long"
        future_label = f"期貨多 {future_quantity} 口"
    elif future_quantity < 0:
        future_direction = "short"
        future_label = f"期貨空 {abs(future_quantity)} 口"
    else:
        future_direction = "flat"
        future_label = "期貨空手"
    return {
        "live_option_long_contracts": long_contracts,
        "live_option_short_contracts": short_contracts,
        "live_option_gross_contracts": gross_contracts,
        "live_option_long_ratio": long_ratio,
        "live_option_short_ratio": short_ratio,
        "live_option_net_ratio": net_ratio,
        "live_option_ratio_label": option_ratio_label,
        "live_futures_direction": future_direction,
        "live_futures_direction_label": future_label,
        "live_exposure_label": f"{option_ratio_label} · {future_label}",
    }


def _exposure_counts(
    rows: list[dict[str, Any]],
    dimension: str,
) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for row in rows:
        code = str(row.get(dimension) or "")
        if code:
            counts[code] = counts.get(code, 0) + 1
    return [
        {
            "code": code,
            "label": metadata["label"],
            "definition": metadata["definition"],
            "count": counts[code],
        }
        for code, metadata in EXPOSURE_TAXONOMY[dimension].items()
        if counts.get(code, 0) > 0
    ]


def _exposure_summary(
    live_rows: list[dict[str, Any]],
    catalog_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "ratio_basis": EXPOSURE_RATIO_BASIS,
        "live": {
            dimension: _exposure_counts(live_rows, dimension)
            for dimension in EXPOSURE_TAXONOMY
        },
        "catalog": {
            dimension: _exposure_counts(catalog_rows, dimension)
            for dimension in EXPOSURE_TAXONOMY
        },
    }


def _strategy_label(strategy_id: str) -> tuple[str, str]:
    spec = STRATEGY_SPEC_BY_ID.get(strategy_id)
    if spec is None:
        return strategy_id, "unknown"
    return spec.label, spec.implementation_level


def _safe_cycle(value: object) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    allowed = (
        "cycle_id",
        "entry_date",
        "expiry_date",
        "series",
        "strike",
        "call_code",
        "put_code",
        "call_entry_ask",
        "put_entry_ask",
        "entry_timing",
        "strategy_mode",
        "status",
    )
    return {key: value.get(key) for key in allowed if key in value}


def _latest_api_receipt(root: Path) -> dict[str, Any] | None:
    paths = sorted(root.glob("*-futures-simulation-lifecycle.json"))
    if not paths:
        return None
    payload = _load_object(paths[-1])
    return {
        "source_file": paths[-1].name,
        "result": payload.get("result"),
        "simulation": payload.get("simulation"),
        "production_order_possible": payload.get("production_order_possible"),
        "logical_contract": payload.get("logical_contract"),
        "resolved_contract": payload.get("resolved_contract"),
        "baseline_position": payload.get("baseline_position"),
        "final_position": payload.get("final_position"),
        "finished_at_utc": payload.get("finished_at_utc"),
    }


def build_dashboard_snapshot(
    *,
    state_dir: Path,
    api_receipt_dir: Path,
    now: datetime | None = None,
    mark_limit_per_strategy: int = DEFAULT_MARK_LIMIT_PER_STRATEGY,
    max_source_age_seconds: float = DEFAULT_MAX_SOURCE_AGE_SECONDS,
    include_history: bool = True,
) -> dict[str, Any]:
    """Build one bounded, account-safe snapshot from committed live files."""

    state_dir = Path(state_dir)
    status = _load_object(state_dir / "status.json")
    state = _load_object(state_dir / "state.json")
    observed_now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    current_market_phase = taifex_market_phase(observed_now.astimezone(TAIPEI))
    source_fresh_expected = current_market_phase in {
        "day_preopen",
        "day_continuous",
        "night_preopen",
        "night_continuous",
    }
    source_updated = _parse_utc(status.get("updated_at_utc"))
    source_age = max(0.0, (observed_now - source_updated).total_seconds())
    blocked_reason = status.get("blocked_reason")
    engine_status = str(status.get("engine_status") or "unknown")
    if blocked_reason or engine_status == "blocked":
        health = "blocked"
    elif source_age > float(max_source_age_seconds) and source_fresh_expected:
        health = "stale"
    elif source_age > float(max_source_age_seconds):
        health = "waiting"
    elif engine_status in {"cycle_open", "intraday_cycle_open", "intraday_active"}:
        health = "active"
    elif engine_status.startswith(("waiting_", "calibrated_", "flat_")):
        health = "waiting"
    else:
        health = "ready"

    strategy_ids = tuple(
        str(strategy_id)
        for strategy_id in (state.get("strategy_ids") or status.get("strategies") or ())
    )
    strategy_rows: list[dict[str, Any]] = []
    for strategy_id in strategy_ids:
        strategy_key = str(strategy_id)
        mark = status.get("strategies", {}).get(strategy_key, {})
        ledger = state.get("strategies", {}).get(strategy_key, {})
        label, implementation = _strategy_label(strategy_key)
        spec = STRATEGY_SPEC_BY_ID.get(strategy_key)
        option_valid = bool(mark.get("option_books_valid"))
        future_valid = bool(mark.get("future_book_valid"))
        cumulative_pnl = _optional_float(
            mark.get("cumulative_pnl_twd", mark.get("net_equity_twd"))
        )
        initial_capital = _optional_float(
            mark.get("initial_capital_twd", ledger.get("initial_capital_twd"))
        )
        if initial_capital is None:
            initial_capital = 0.0
        total_equity = _optional_float(mark.get("total_equity_twd"))
        if total_equity is None and cumulative_pnl is not None:
            total_equity = initial_capital + cumulative_pnl
        valuation_available = bool(
            mark.get("valuation_available", total_equity is not None)
        )
        valuation_stale = bool(
            mark.get("valuation_stale") or mark.get("valuation_carried_forward")
        )
        strategy_rows.append(
            {
                "strategy_id": strategy_key,
                "label": label,
                "implementation_level": implementation,
                "family": spec.family if spec else "unknown",
                "category": spec.category if spec else "unknown",
                "summary": spec.summary if spec else "",
                "entry_rule": spec.entry_rule if spec else "",
                "exit_rule": spec.exit_rule if spec else "",
                "risk_note": spec.risk_note if spec else "",
                "broker_monitoring": (spec.broker_monitoring if spec else "unknown"),
                **(spec.exposure_payload() if spec else {}),
                "entry_state": mark.get("entry_state", ledger.get("entry_state")),
                "option_position_count": len(mark.get("option_positions") or {}),
                "net_equity_twd": cumulative_pnl,
                "cumulative_pnl_twd": cumulative_pnl,
                "initial_capital_twd": initial_capital,
                "total_equity_twd": total_equity,
                "margin_required_twd": _optional_float(mark.get("margin_required_twd")),
                "margin_excess_twd": _optional_float(mark.get("margin_excess_twd")),
                "alive": bool(mark.get("alive", ledger.get("alive", True))),
                "margin_call_count": int(
                    mark.get("margin_call_count", ledger.get("margin_call_count", 0))
                    or 0
                ),
                "forced_liquidation_pending": bool(
                    mark.get(
                        "forced_liquidation_pending",
                        ledger.get("forced_liquidation_pending", False),
                    )
                ),
                "gross_cash_twd": float(mark.get("gross_cash_twd", 0.0)),
                "open_liquidation_value_twd": _optional_float(
                    mark.get("open_liquidation_value_twd")
                ),
                "fixed_fees_twd": float(mark.get("fixed_fees_twd", 0.0)),
                "transaction_tax_twd": float(mark.get("transaction_tax_twd", 0.0)),
                "futures_position": int(mark.get("futures_position", 0)),
                "option_books_valid": option_valid,
                "future_book_valid": future_valid,
                "books_valid": option_valid and future_valid,
                "valuation_available": valuation_available,
                "valuation_stale": valuation_stale,
                "valuation_carried_forward": bool(
                    mark.get("valuation_carried_forward")
                ),
                "valuation_age_seconds": _optional_float(
                    mark.get("valuation_age_seconds")
                ),
                "valuation_source": mark.get("valuation_source"),
                **_live_position_exposure(
                    mark.get("option_positions", ledger.get("option_positions")),
                    mark.get("futures_position", ledger.get("futures_position", 0)),
                ),
            }
        )

    strategy_count = len(strategy_rows)
    valuation_available_count = sum(
        bool(row["valuation_available"]) for row in strategy_rows
    )
    fresh_valuation_count = sum(
        bool(row["valuation_available"]) and not bool(row["valuation_stale"])
        for row in strategy_rows
    )
    carried_valuation_count = sum(
        bool(row["valuation_carried_forward"]) for row in strategy_rows
    )
    timely_valuation_count = sum(
        bool(row["valuation_available"])
        and (
            not bool(row["valuation_stale"])
            or (
                row["valuation_age_seconds"] is not None
                and float(row["valuation_age_seconds"])
                <= float(max_source_age_seconds)
            )
        )
        for row in strategy_rows
    )
    recent_carried_valuation_count = max(
        0, timely_valuation_count - fresh_valuation_count
    )
    if (
        health in {"active", "ready"}
        and source_fresh_expected
        and strategy_count > 0
        and timely_valuation_count < strategy_count
    ):
        health = "degraded"

    current_trading_date = (
        str(status.get("current_trading_date"))
        if status.get("current_trading_date")
        else None
    )
    current_ts_ns = int(source_updated.timestamp() * 1e9)
    daily_endpoints = _daily_pnl_endpoints(
        state_dir / "marks.jsonl",
        strategy_ids=strategy_ids,
    )
    for row in strategy_rows:
        explicit_cost = float(row["fixed_fees_twd"]) + float(row["transaction_tax_twd"])
        row.update(
            _performance_metrics(
                daily_endpoints=daily_endpoints.get(row["strategy_id"], {}),
                current_trading_date=current_trading_date,
                current_ts_ns=current_ts_ns,
                current_pnl_twd=row["cumulative_pnl_twd"],
                reserved_capital_twd=float(row["initial_capital_twd"]),
                explicit_cost_twd=explicit_cost,
                margin_required_twd=row["margin_required_twd"],
            )
        )

    mark_limit = max(1, min(int(mark_limit_per_strategy), 1_440))
    raw_marks = (
        _tail_json_objects(
            state_dir / "marks.jsonl",
            maximum_rows=max(1, mark_limit * max(1, len(strategy_ids)) * 2),
        )
        if include_history
        else []
    )
    grouped_marks: dict[str, deque[dict[str, Any]]] = {
        str(strategy_id): deque(maxlen=mark_limit) for strategy_id in strategy_ids
    }
    last_complete_open_value: dict[str, tuple[object, int, float, int]] = {}
    capital_by_strategy = {
        row["strategy_id"]: float(row["initial_capital_twd"]) for row in strategy_rows
    }
    for mark in raw_marks:
        strategy_id = str(mark.get("strategy_id") or "")
        if strategy_id not in grouped_marks:
            continue
        decision_ts_ns = int(mark.get("decision_ts_ns") or 0)
        cycle_id = mark.get("active_cycle_id")
        futures_position = int(mark.get("futures_position") or 0)
        gross_cash = float(mark.get("gross_cash_twd") or 0.0)
        fixed_fees = float(mark.get("fixed_fees_twd") or 0.0)
        transaction_tax = float(mark.get("transaction_tax_twd") or 0.0)
        has_validity_flags = "option_books_valid" in mark or "future_book_valid" in mark
        fresh_complete = (
            bool(mark.get("option_books_valid")) and bool(mark.get("future_book_valid"))
            if has_validity_flags
            else True
        )
        open_value = _optional_float(mark.get("open_liquidation_value_twd"))
        carried = bool(mark.get("valuation_carried_forward"))
        valuation_available = bool(
            mark.get("valuation_available", open_value is not None)
        )
        if fresh_complete and open_value is not None:
            last_complete_open_value[strategy_id] = (
                cycle_id,
                futures_position,
                open_value,
                decision_ts_ns,
            )
        elif "valuation_available" not in mark:
            cached = last_complete_open_value.get(strategy_id)
            if cached is not None and cached[:2] == (cycle_id, futures_position):
                open_value = cached[2]
                carried = True
                valuation_available = True
            else:
                open_value = None
                valuation_available = False
        cumulative_pnl = _optional_float(
            mark.get("cumulative_pnl_twd", mark.get("net_equity_twd"))
        )
        if carried and open_value is not None:
            cumulative_pnl = gross_cash + open_value - fixed_fees - transaction_tax
        total_equity = _optional_float(mark.get("total_equity_twd"))
        if cumulative_pnl is not None and (total_equity is None or carried):
            total_equity = capital_by_strategy[strategy_id] + cumulative_pnl
        if not valuation_available:
            cumulative_pnl = None
            total_equity = None
        cached_ts = (
            last_complete_open_value.get(strategy_id, (None, 0, 0.0, 0))[3]
            if carried
            else decision_ts_ns
        )
        grouped_marks[strategy_id].append(
            {
                "recorded_at_utc": mark.get("recorded_at_utc"),
                "decision_ts_ns": decision_ts_ns,
                "strategy_id": strategy_id,
                "net_equity_twd": cumulative_pnl,
                "cumulative_pnl_twd": cumulative_pnl,
                "initial_capital_twd": capital_by_strategy[strategy_id],
                "total_equity_twd": total_equity,
                "gross_cash_twd": gross_cash,
                "open_liquidation_value_twd": open_value,
                "fixed_fees_twd": fixed_fees,
                "transaction_tax_twd": transaction_tax,
                "explicit_cost_twd": fixed_fees + transaction_tax,
                "futures_position": futures_position,
                "fixed_capital_return": (
                    cumulative_pnl / capital_by_strategy[strategy_id]
                    if cumulative_pnl is not None
                    and capital_by_strategy[strategy_id] > 0.0
                    else None
                ),
                "valuation_available": valuation_available,
                "valuation_stale": carried,
                "valuation_carried_forward": carried,
                "valuation_age_seconds": (
                    max(0.0, (decision_ts_ns - cached_ts) / 1e9)
                    if carried and cached_ts
                    else 0.0
                    if valuation_available
                    else None
                ),
            }
        )
    history = [
        row for strategy_id in strategy_ids for row in grouped_marks[str(strategy_id)]
    ]
    history.sort(key=lambda row: (row["decision_ts_ns"], row["strategy_id"]))

    option_contract_count = int(status.get("option_contract_count") or 0)
    latest_book_count = int(status.get("latest_book_count") or 0)
    expected_book_count = option_contract_count + 2
    held_option_contract_count = int(
        status.get("held_option_contract_count") or 0
    )
    held_option_subscribed_count = int(
        status.get("held_option_subscribed_count") or 0
    )
    held_option_book_count = int(status.get("held_option_book_count") or 0)
    pending_targets = status.get("pending_targets") or {}
    pending_rows = []
    if isinstance(pending_targets, Mapping):
        for strategy_id, target in pending_targets.items():
            if not isinstance(target, Mapping):
                continue
            pending_rows.append(
                {
                    "strategy_id": str(strategy_id),
                    "target_contracts": int(target.get("target_contracts") or 0),
                    "decision_date": target.get("decision_date"),
                    "decision_ts_ns": int(target.get("decision_ts_ns") or 0),
                }
            )

    catalog_rows = [spec.dashboard_payload() for spec in STRATEGY_CATALOG]

    return {
        "dashboard_schema_version": DASHBOARD_SCHEMA_VERSION,
        "generated_at_utc": observed_now.isoformat(),
        "source_updated_at_utc": source_updated.isoformat(),
        "source_age_seconds": round(source_age, 3),
        "max_source_age_seconds": float(max_source_age_seconds),
        "refresh_interval_seconds": 5,
        "health": health,
        "engine_status": engine_status,
        "blocked_reason": blocked_reason,
        "simulation_only": status.get("simulation_only") is True,
        "production_order_possible": status.get("production_order_possible") is True,
        "strategy_mode": status.get("strategy_mode"),
        "history_included": include_history,
        "runner_mode": "always_on_scheduled_capture",
        "current_market_phase": current_market_phase,
        "source_fresh_expected": source_fresh_expected,
        "trading_schedule": [
            {
                "phase": "day_preopen",
                "time": "08:30-08:45",
                "market_data": "auction_simtrade_five_levels",
                "ideal_fill_allowed": False,
            },
            {
                "phase": "day_continuous",
                "time": "08:45-13:45",
                "market_data": "live_tick_and_five_levels",
                "ideal_fill_allowed": True,
            },
            {
                "phase": "day_close_to_night_preopen",
                "time": "13:45-14:50",
                "market_data": "closed_monitoring",
                "ideal_fill_allowed": False,
            },
            {
                "phase": "night_preopen",
                "time": "14:50-15:00",
                "market_data": "auction_simtrade_five_levels",
                "ideal_fill_allowed": False,
            },
            {
                "phase": "night_continuous",
                "time": "15:00-05:00",
                "market_data": "live_tick_and_five_levels",
                "ideal_fill_allowed": True,
            },
            {
                "phase": "closed_monitoring",
                "time": "05:00-08:30",
                "market_data": "closed_monitoring",
                "ideal_fill_allowed": False,
            },
        ],
        "catalog_expansion_entry_policy": status.get("catalog_expansion_entry_policy"),
        "current_session": status.get("current_session"),
        "current_trading_date": current_trading_date,
        "intraday_decision_interval_seconds": status.get(
            "intraday_decision_interval_seconds"
        ),
        "intraday_entry_cutoff": status.get("intraday_entry_cutoff"),
        "intraday_flatten_time": status.get("intraday_flatten_time"),
        "night_entry_cutoff": status.get("night_entry_cutoff"),
        "night_flatten_time": status.get("night_flatten_time"),
        "last_intraday_decision_bucket": status.get("last_intraday_decision_bucket"),
        "last_intraday_flatten_date": status.get("last_intraday_flatten_date"),
        "last_intraday_decision_session": status.get("last_intraday_decision_session"),
        "last_intraday_flatten_session": status.get("last_intraday_flatten_session"),
        "bootstrap_after_date": status.get("bootstrap_after_date"),
        "broker": {
            "orders_enabled": bool(status.get("broker_orders_enabled")),
            "order_failures": int(status.get("broker_order_failures") or 0),
            "inflight_order_count": int(status.get("inflight_order_count") or 0),
        },
        "market": {
            "underlying_contract": status.get("underlying_contract"),
            "hedge_contract": status.get("hedge_contract"),
            "hedge_product": status.get("hedge_product"),
            "hedge_multiplier_twd_per_point": status.get(
                "hedge_multiplier_twd_per_point"
            ),
            "hedge_fee_per_side_twd": status.get("hedge_fee_per_side_twd"),
            "option_risk_margin_a_twd": status.get("option_risk_margin_a_twd"),
            "option_risk_margin_b_twd": status.get("option_risk_margin_b_twd"),
            "option_risk_margin_c_twd": status.get("option_risk_margin_c_twd"),
            "option_risk_margin_effective_trading_date": status.get(
                "option_risk_margin_effective_trading_date"
            ),
            "option_risk_margin_source_url": status.get(
                "option_risk_margin_source_url"
            ),
            "option_margin_policy": status.get("option_margin_policy"),
            "margin_requirement_basis": status.get("margin_requirement_basis"),
            "official_margin_schedule": status.get("official_margin_schedule"),
            "futures_initial_margin_per_contract_twd": status.get(
                "futures_initial_margin_per_contract_twd"
            ),
            "strategy_capital_buffer_multiple": status.get(
                "strategy_capital_buffer_multiple"
            ),
            "option_contract_count": option_contract_count,
            "latest_book_count": latest_book_count,
            "expected_book_count": expected_book_count,
            "book_coverage_ratio": (
                latest_book_count / expected_book_count
                if expected_book_count > 0
                else 0.0
            ),
            "held_option_contract_count": held_option_contract_count,
            "held_option_subscribed_count": held_option_subscribed_count,
            "held_option_book_count": held_option_book_count,
            "held_option_subscription_coverage_ratio": (
                held_option_subscribed_count / held_option_contract_count
                if held_option_contract_count > 0
                else 1.0
            ),
            "missing_held_option_subscription_codes": list(
                status.get("missing_held_option_subscription_codes") or ()
            ),
            "strategy_count": strategy_count,
            "strategy_valuation_available_count": valuation_available_count,
            "strategy_fresh_valuation_count": fresh_valuation_count,
            "strategy_carried_valuation_count": carried_valuation_count,
            "strategy_timely_valuation_count": timely_valuation_count,
            "strategy_recent_carried_valuation_count": (
                recent_carried_valuation_count
            ),
            "strategy_valuation_coverage_ratio": (
                valuation_available_count / strategy_count
                if strategy_count > 0
                else 0.0
            ),
            "strategy_fresh_valuation_coverage_ratio": (
                fresh_valuation_count / strategy_count
                if strategy_count > 0
                else 0.0
            ),
            "strategy_timely_valuation_coverage_ratio": (
                timely_valuation_count / strategy_count
                if strategy_count > 0
                else 0.0
            ),
        },
        "active_cycle": _safe_cycle(status.get("active_cycle")),
        "pending_targets": pending_rows,
        "strategies": strategy_rows,
        "portfolio_summary": _portfolio_summary(strategy_rows),
        "metric_definitions": {
            "strategy_unit": (
                "One minimum executable strategy recipe; it can contain multiple "
                "option and futures legs."
            ),
            "reserved_capital_twd": (
                "Fixed initial_capital_twd assigned to one independent strategy "
                "ledger; it is the return denominator."
            ),
            "one_unit_net_pnl_twd": (
                "Latest signed cumulative P&L after executable Bid/Ask liquidation, "
                "fixed fees, transaction tax, and hedge P&L."
            ),
            "fixed_capital_return": ("one_unit_net_pnl_twd / reserved_capital_twd"),
            "compounded_return_to_live_mark": (
                "Product over TAIFEX trading dates of "
                "(1 + daily P&L change / fixed reserved capital) minus 1; "
                "the current incomplete trading date is included."
            ),
            "explicit_cost_twd": "fixed_fees_twd + transaction_tax_twd",
            "capital_aggregation": (
                "Sum of independent one-unit ledger reserves; not a claim that "
                "all strategies can share collateral or margin offsets."
            ),
            "exposure_classification": (
                "Direction, volatility, and hedge labels describe the strategy "
                "payoff or target. They are not a live Greek measurement."
            ),
            "option_long_short_ratio": EXPOSURE_RATIO_BASIS,
            "strategy_fresh_valuation_coverage_ratio": (
                "Strategies with a complete current executable Bid/Ask liquidation "
                "mark divided by all live ideal strategy ledgers. CARRIED marks do "
                "not count as fresh."
            ),
            "strategy_timely_valuation_coverage_ratio": (
                "Strategies with either a fresh executable mark or an explicitly "
                "labelled CARRIED last-complete mark no older than the dashboard "
                "source-age threshold, divided by all live ideal ledgers."
            ),
            "book_coverage_ratio": (
                "Contracts with any latest callback divided by the worker-0 "
                "subscription set; this is a generic feed metric, not proof that "
                "every held leg can be valued."
            ),
        },
        "exposure_taxonomy": EXPOSURE_TAXONOMY,
        "exposure_summary": _exposure_summary(strategy_rows, catalog_rows),
        "strategy_catalog": catalog_rows,
        "strategy_counts": {
            "live_ideal": sum(
                spec.availability == "live_ideal" for spec in STRATEGY_CATALOG
            ),
            "blocked_contract": sum(
                spec.availability == "blocked_contract" for spec in STRATEGY_CATALOG
            ),
            "catalog_total": len(STRATEGY_CATALOG),
        },
        "history": history,
        "record_counts": {
            "ideal_trades": _line_count(state_dir / "ideal_ledger.jsonl"),
            "marks": _line_count(state_dir / "marks.jsonl"),
            "calibrations": _line_count(state_dir / "calibrations.jsonl"),
            "events": _line_count(state_dir / "events.jsonl"),
            "history_rows_returned": len(history),
        },
        "api_round_trip": _latest_api_receipt(Path(api_receipt_dir)),
        "sources": [
            {"name": "即時策略快照", "path": "status.json", "grain": "5 seconds"},
            {"name": "策略總權益序列", "path": "marks.jsonl", "grain": "1 minute"},
            {"name": "理想成交帳", "path": "ideal_ledger.jsonl", "grain": "trade leg"},
            {
                "name": "模型校準",
                "path": "calibrations.jsonl",
                "grain": "model decision",
            },
            {"name": "引擎事件", "path": "events.jsonl", "grain": "event"},
        ],
    }


__all__ = [
    "DASHBOARD_SCHEMA_VERSION",
    "DEFAULT_MARK_LIMIT_PER_STRATEGY",
    "DEFAULT_MAX_SOURCE_AGE_SECONDS",
    "build_dashboard_snapshot",
]

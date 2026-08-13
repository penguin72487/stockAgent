"""Read-only, source-backed dashboard snapshot for stock day-trade simulation."""

from __future__ import annotations

from collections import deque
from datetime import datetime, time as datetime_time, timezone
import json
import math
from pathlib import Path
import threading
from typing import Any, Final, Mapping
from zoneinfo import ZoneInfo


DASHBOARD_SCHEMA_VERSION: Final[int] = 3
DEFAULT_MAX_SOURCE_AGE_SECONDS: Final[float] = 30.0
TAIPEI: Final[ZoneInfo] = ZoneInfo("Asia/Taipei")
_LINE_COUNT_CACHE: dict[Path, tuple[int, int, int, int]] = {}
_LINE_COUNT_LOCK = threading.Lock()
_TAIL_CACHE: dict[
    tuple[Path, int], tuple[int, int, int, int, list[dict[str, Any]]]
] = {}
_TAIL_CACHE_LOCK = threading.Lock()
_TAIL_CACHE_MAX_ENTRIES: Final[int] = 16


def _object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON root is not an object: {path}")
    return payload


def _tail(path: Path, maximum_rows: int) -> list[dict[str, Any]]:
    if maximum_rows <= 0 or not path.is_file():
        return []
    stat = path.stat()
    cache_key = (path.resolve(), int(maximum_rows))
    with _TAIL_CACHE_LOCK:
        cached = _TAIL_CACHE.get(cache_key)
        if cached and cached[:4] == (
            stat.st_dev,
            stat.st_ino,
            stat.st_size,
            stat.st_mtime_ns,
        ):
            return list(cached[4])
    rows: deque[dict[str, Any]] = deque(maxlen=maximum_rows)
    with path.open("rb") as handle:
        size = handle.seek(0, 2)
        cursor = size
        encoded = b""
        while cursor > 0 and encoded.count(b"\n") <= maximum_rows:
            chunk_size = min(1 << 20, cursor)
            cursor -= chunk_size
            handle.seek(cursor)
            encoded = handle.read(chunk_size) + encoded
    for line in encoded.splitlines()[-maximum_rows:]:
        if not line.strip():
            continue
        payload = json.loads(line.decode("utf-8"))
        if isinstance(payload, dict):
            rows.append(payload)
    result = list(rows)
    final_stat = path.stat()
    if (
        final_stat.st_dev,
        final_stat.st_ino,
        final_stat.st_size,
        final_stat.st_mtime_ns,
    ) == (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns):
        with _TAIL_CACHE_LOCK:
            if len(_TAIL_CACHE) >= _TAIL_CACHE_MAX_ENTRIES:
                _TAIL_CACHE.pop(next(iter(_TAIL_CACHE)))
            _TAIL_CACHE[cache_key] = (
                stat.st_dev,
                stat.st_ino,
                stat.st_size,
                stat.st_mtime_ns,
                result,
            )
    return list(result)


def _line_count(path: Path) -> int:
    if not path.is_file():
        return 0
    stat = path.stat()
    key = path.resolve()
    with _LINE_COUNT_LOCK:
        cached = _LINE_COUNT_CACHE.get(key)
        same_append_only_file = bool(
            cached
            and cached[0] == stat.st_dev
            and cached[1] == stat.st_ino
            and stat.st_size >= cached[2]
        )
        start = cached[2] if same_append_only_file and cached else 0
        count = cached[3] if same_append_only_file and cached else 0
        with path.open("rb") as handle:
            handle.seek(start)
            remaining = stat.st_size - start
            while remaining > 0:
                chunk = handle.read(min(1 << 20, remaining))
                if not chunk:
                    break
                count += chunk.count(b"\n")
                remaining -= len(chunk)
        _LINE_COUNT_CACHE[key] = (stat.st_dev, stat.st_ino, stat.st_size, count)
        return count


def _timestamp(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp has no timezone: {value!r}")
    return parsed.astimezone(timezone.utc)


def _finite_float(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _capital_return(
    initial_capital: object, total_equity: object
) -> tuple[float | None, float | None]:
    initial = _finite_float(initial_capital)
    equity = _finite_float(total_equity)
    if initial is None or initial <= 0.0 or equity is None:
        return None, None
    return equity / initial - 1.0, (equity / initial - 1.0) * 100.0


def _ratio(numerator: object, denominator: object) -> float:
    top = _finite_float(numerator)
    bottom = _finite_float(denominator)
    if top is None or bottom is None or bottom <= 0.0:
        return 0.0
    return min(max(top / bottom, 0.0), 1.0)


def _seconds_between(start: object, end: object) -> float | None:
    if not start or not end:
        return None
    try:
        return max(0.0, (_timestamp(end) - _timestamp(start)).total_seconds())
    except (TypeError, ValueError):
        return None


def _preopen_progress(
    *,
    path: Path | None,
    modes: list[dict[str, Any]],
    observed: datetime,
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if path is not None and Path(path).is_file():
        try:
            payload = _object(Path(path))
        except (OSError, ValueError, json.JSONDecodeError):
            payload = {}
    updated_at = payload.get("updated_at")
    source_age_seconds: float | None = None
    same_trading_date = False
    if updated_at:
        try:
            updated = _timestamp(updated_at)
            source_age_seconds = max(0.0, (observed - updated).total_seconds())
            same_trading_date = (
                updated.astimezone(TAIPEI).date() == observed.astimezone(TAIPEI).date()
            )
        except (TypeError, ValueError):
            pass

    raw_markets = payload.get("markets") if same_trading_date else {}
    if not isinstance(raw_markets, Mapping):
        raw_markets = {}
    rows: list[dict[str, Any]] = []
    for mode in modes:
        market = str(mode.get("market") or "")
        raw = raw_markets.get(market)
        item = dict(raw) if isinstance(raw, Mapping) else {}
        status = str(item.get("status") or "pending")
        step = max(0, int(item.get("step") or 0))
        total = max(0, int(item.get("total") or 0))
        if status == "ready":
            progress_ratio = 1.0
        elif status == "failed":
            progress_ratio = 1.0
        elif total:
            progress_ratio = _ratio(step, total)
        else:
            progress_ratio = 0.0
        elapsed_seconds = _finite_float(item.get("elapsed_seconds"))
        if elapsed_seconds is None and item.get("started_at"):
            end = (
                item.get("completed_at") if status in {"ready", "failed"} else observed
            )
            elapsed_seconds = _seconds_between(item.get("started_at"), end)
        symbol_count = int(item.get("symbol_count") or 0)
        latency = item.get("live_latency")
        latency = dict(latency) if isinstance(latency, Mapping) else {}
        inference_ms = _finite_float(latency.get("model_inference_ms"))
        price_limits = item.get("preopen_price_limits")
        price_limits = dict(price_limits) if isinstance(price_limits, Mapping) else {}
        same_session = item.get("same_session_eligibility")
        same_session = dict(same_session) if isinstance(same_session, Mapping) else {}
        rule_venues = same_session.get("venues")
        rule_venues = dict(rule_venues) if isinstance(rule_venues, Mapping) else {}
        requested = int(price_limits.get("requested_count") or 0)
        prepared = int(price_limits.get("prepared_count") or 0)
        rate = (
            symbol_count / elapsed_seconds
            if symbol_count and elapsed_seconds and elapsed_seconds > 0.0
            else None
        )
        inference_rate = (
            symbol_count / (inference_ms / 1000.0)
            if symbol_count and inference_ms and inference_ms > 0.0
            else None
        )
        rows.append(
            {
                "market": market,
                "label": mode.get("label") or market,
                "status": status,
                "progress_ratio": round(progress_ratio, 6),
                "step": step or None,
                "total": total or None,
                "message": item.get("message"),
                "started_at": item.get("started_at"),
                "completed_at": item.get("completed_at"),
                "elapsed_seconds": (
                    round(elapsed_seconds, 3) if elapsed_seconds is not None else None
                ),
                "panel_date": item.get("panel_date"),
                "symbol_count": symbol_count or None,
                "symbols_per_second": round(rate, 3) if rate is not None else None,
                "model_inference_ms": inference_ms,
                "model_symbols_per_second": (
                    round(inference_rate, 3) if inference_rate is not None else None
                ),
                "compute_before_publish_ms": _finite_float(
                    latency.get("compute_before_publish_ms")
                ),
                "checkpoint_cache_hit": latency.get("checkpoint_cache_hit"),
                "model_cache_hit": latency.get("model_cache_hit"),
                "price_limit_prepared": prepared,
                "price_limit_requested": requested,
                "price_limit_coverage_ratio": _ratio(prepared, requested),
                "price_limit_missing": int(price_limits.get("missing_count") or 0),
                "eligibility_target_date": same_session.get("target_date"),
                "eligibility_coverage": rule_venues,
                "eligibility_ready": bool(rule_venues)
                and all(
                    bool(dict(value).get("covered"))
                    for value in rule_venues.values()
                    if isinstance(value, Mapping)
                ),
                "error": item.get("error"),
            }
        )

    ready_count = sum(row["status"] == "ready" for row in rows)
    failed_count = sum(row["status"] == "failed" for row in rows)
    terminal_count = ready_count + failed_count
    running_count = sum(row["status"] == "running" for row in rows)
    starts = [row["started_at"] for row in rows if row.get("started_at")]
    ends = [row["completed_at"] for row in rows if row.get("completed_at")]
    wall_elapsed = None
    if starts and ends:
        wall_elapsed = _seconds_between(min(starts), max(ends))
    overall_status = (
        "failed"
        if failed_count
        else "ready"
        if rows and ready_count == len(rows)
        else "running"
        if running_count or (same_trading_date and terminal_count)
        else "pending"
    )
    return {
        "status": overall_status,
        "updated_at": updated_at if same_trading_date else None,
        "source_age_seconds": (
            round(source_age_seconds, 3)
            if same_trading_date and source_age_seconds is not None
            else None
        ),
        "ready_count": ready_count,
        "failed_count": failed_count,
        "running_count": running_count,
        "completed_count": terminal_count,
        "total_count": len(rows),
        "progress_ratio": _ratio(terminal_count, len(rows)),
        "wall_elapsed_seconds": round(wall_elapsed, 3)
        if wall_elapsed is not None
        else None,
        "modes_per_minute": (
            round(terminal_count * 60.0 / wall_elapsed, 3)
            if terminal_count and wall_elapsed and wall_elapsed > 0.0
            else None
        ),
        "markets": rows,
        "source_path": str(path) if path is not None else None,
    }


def _session_progress(
    *,
    observed: datetime,
    mode_count: int,
    modes: list[dict[str, Any]],
    marks: list[dict[str, Any]],
) -> dict[str, Any]:
    local = observed.astimezone(TAIPEI)
    day = local.date()

    def at(hour: int, minute: int) -> datetime:
        return datetime.combine(day, datetime_time(hour, minute), tzinfo=TAIPEI)

    preopen_at = at(8, 30)
    signal_at = at(9, 0)
    exit_limit_at = at(13, 20)
    force_exit_at = at(13, 24)
    session_end_at = at(13, 30)
    if local < preopen_at:
        phase = "waiting_prewarm"
        label = "等待 08:30 預熱"
        phase_start, phase_end = at(0, 0), preopen_at
        next_label, next_at = "開始預熱", preopen_at
    elif local < signal_at:
        phase = "preopen"
        label = "盤前預熱"
        phase_start, phase_end = preopen_at, signal_at
        next_label, next_at = "09:00 訊號閘門", signal_at
    elif local < exit_limit_at:
        phase = "active"
        label = "盤中每分鐘估值"
        phase_start, phase_end = signal_at, exit_limit_at
        next_label, next_at = "13:20 限價退出", exit_limit_at
    elif local < force_exit_at:
        phase = "exit_limit"
        label = "13:20 限價退出"
        phase_start, phase_end = exit_limit_at, force_exit_at
        next_label, next_at = "13:24 市價強平", force_exit_at
    elif local < session_end_at:
        phase = "force_exit"
        label = "13:24 市價強平"
        phase_start, phase_end = force_exit_at, session_end_at
        next_label, next_at = "盤後完成檢查", session_end_at
    else:
        phase = "complete"
        label = "本日流程結束"
        phase_start, phase_end = signal_at, session_end_at
        next_label, next_at = "已完成", session_end_at

    signal_completed = sum(bool(mode.get("signal_at")) for mode in modes)
    entry_completed = sum(bool(mode.get("entry_completed_at")) for mode in modes)
    exit_started = sum(
        bool(mode.get("exit_limit_submitted_at") or mode.get("force_exit_started_at"))
        for mode in modes
    )
    unique_mode_minutes = {
        (str(row.get("market")), str(row.get("minute")))
        for row in marks
        if row.get("market") and row.get("minute")
    }
    elapsed_active_minutes = 0
    if local >= signal_at:
        elapsed_active_minutes = max(
            0,
            min(
                int((min(local, force_exit_at) - signal_at).total_seconds() // 60) + 1,
                int((force_exit_at - signal_at).total_seconds() // 60) + 1,
            ),
        )
    expected_mode_marks = elapsed_active_minutes * max(0, mode_count)
    return {
        "phase": phase,
        "label": label,
        "phase_progress_ratio": _ratio(
            (min(max(local, phase_start), phase_end) - phase_start).total_seconds(),
            (phase_end - phase_start).total_seconds(),
        ),
        "session_progress_ratio": _ratio(
            (min(max(local, signal_at), session_end_at) - signal_at).total_seconds(),
            (session_end_at - signal_at).total_seconds(),
        ),
        "next_milestone_label": next_label,
        "next_milestone_at": next_at.isoformat(timespec="seconds"),
        "seconds_to_next_milestone": max(0.0, (next_at - local).total_seconds()),
        "decision_interval_seconds": 60,
        "signal_completed_modes": signal_completed,
        "entry_completed_modes": entry_completed,
        "exit_started_modes": exit_started,
        "mode_count": mode_count,
        "signal_progress_ratio": _ratio(signal_completed, mode_count),
        "entry_progress_ratio": _ratio(entry_completed, mode_count),
        "exit_progress_ratio": _ratio(exit_started, mode_count),
        "observed_mode_minutes": len(unique_mode_minutes),
        "expected_mode_minutes": expected_mode_marks,
        "mark_progress_ratio": _ratio(len(unique_mode_minutes), expected_mode_marks),
        "mark_rows_per_minute": (
            round(len(unique_mode_minutes) / elapsed_active_minutes, 3)
            if elapsed_active_minutes
            else 0.0
        ),
    }


def _safe_position(position: Mapping[str, Any]) -> dict[str, Any]:
    allowed = (
        "position_id",
        "market",
        "session_date",
        "signal_id",
        "symbol",
        "name",
        "side",
        "target_weight",
        "requested_shares",
        "pre_balance_filled_shares",
        "filled_shares",
        "signed_shares",
        "lot_size",
        "entry_at",
        "entry_quote_at",
        "entry_price",
        "entry_fee_twd",
        "remaining_entry_fee_twd",
        "entry_gross_fee_and_tax_twd",
        "entry_commission_rebate_accrued_twd",
        "upper_limit",
        "lower_limit",
        "take_profit_price",
        "stop_trigger_price",
        "take_profit_order_status",
        "stop_order_status",
        "eod_limit_price",
        "eod_limit_submitted_at",
        "eod_limit_order_status",
        "eod_limit_liquidity_status",
        "status",
        "last_mark_at",
        "last_quote_at",
        "last_mark_price",
        "last_complete_net_pnl_twd",
        "total_net_pnl_twd",
        "realized_net_pnl_twd",
        "valuation_stale",
        "last_exit_at",
        "last_exit_quote_at",
        "last_exit_price",
        "last_exit_quantity",
        "exit_at",
        "exit_quote_at",
        "exit_price",
        "gross_pnl_twd",
        "net_pnl_twd",
        "exit_reason",
    )
    return {key: position.get(key) for key in allowed if key in position}


def build_dashboard_snapshot(
    *,
    state_dir: Path,
    preopen_readiness_path: Path | None = None,
    now: datetime | None = None,
    max_source_age_seconds: float = DEFAULT_MAX_SOURCE_AGE_SECONDS,
    maximum_signal_rows: int = 0,
    maximum_event_rows: int = 500,
    maximum_mark_rows: int = 4_000,
) -> dict[str, Any]:
    root = Path(state_dir)
    state = _object(root / "state.json")
    status = _object(root / "status.json")
    observed = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    source_updated = _timestamp(status.get("updated_at"))
    source_age = max(0.0, (observed - source_updated).total_seconds())
    health = str(status.get("health") or "unknown")
    if source_age > float(max_source_age_seconds):
        health = "stale"

    modes: list[dict[str, Any]] = []
    positions: list[dict[str, Any]] = []
    session_dates: list[str] = []
    for market, raw_mode in (state.get("modes") or {}).items():
        mode = dict(raw_mode) if isinstance(raw_mode, Mapping) else {}
        return_fraction, return_pct = _capital_return(
            mode.get("initial_capital_twd"), mode.get("total_equity_twd")
        )
        session_date = str(mode.get("session_date") or "")
        if session_date:
            session_dates.append(session_date)
        mode_positions = [
            _safe_position(item)
            for item in (mode.get("positions") or {}).values()
            if isinstance(item, Mapping)
        ]
        positions.extend(mode_positions)
        modes.append(
            {
                "market": market,
                "label": mode.get("label"),
                "engine_status": mode.get("engine_status"),
                "checkpoint_ready": mode.get("checkpoint_ready"),
                "readiness_error": mode.get("readiness_error"),
                "checkpoint_path": mode.get("checkpoint_path"),
                "checkpoint_fingerprint": mode.get("checkpoint_fingerprint"),
                "config_path": mode.get("config_path"),
                "config_fingerprint": mode.get("config_fingerprint"),
                "live_output_dir": mode.get("live_output_dir"),
                "session_date": mode.get("session_date"),
                "signal_id": mode.get("signal_id"),
                "signal_at": mode.get("signal_at"),
                "feature_cutoff_date": mode.get("feature_cutoff_date"),
                "signal_counts": mode.get("signal_counts") or {},
                "execution_projection": mode.get("execution_projection") or {},
                "initial_capital_twd": mode.get("initial_capital_twd"),
                "total_equity_twd": mode.get("total_equity_twd"),
                "return_fraction": return_fraction,
                "return_pct": return_pct,
                "cumulative_realized_net_pnl_twd": mode.get(
                    "cumulative_realized_net_pnl_twd"
                ),
                "cumulative_commission_rebate_accrued_twd": mode.get(
                    "cumulative_commission_rebate_accrued_twd"
                ),
                "open_net_liquidation_pnl_twd": mode.get(
                    "open_net_liquidation_pnl_twd"
                ),
                "open_position_count": mode.get("open_position_count", 0),
                "stale_position_count": mode.get("stale_position_count", 0),
                "entry_completed_at": mode.get("entry_completed_at"),
                "exit_limit_submitted_at": mode.get("exit_limit_submitted_at"),
                "force_exit_started_at": mode.get("force_exit_started_at"),
                "force_exit_failures": mode.get("force_exit_failures", 0),
                "eligibility_coverage": mode.get("eligibility_coverage") or {},
                "current_eligibility_coverage": mode.get("current_eligibility_coverage")
                or {},
                "position_count": len(mode_positions),
            }
        )

    benchmarks: list[dict[str, Any]] = []
    for benchmark_id, raw_benchmark in (state.get("benchmarks") or {}).items():
        if not isinstance(raw_benchmark, Mapping):
            continue
        benchmark = dict(raw_benchmark)
        return_fraction, return_pct = _capital_return(
            benchmark.get("initial_capital_twd"), benchmark.get("total_equity_twd")
        )
        benchmark["benchmark_id"] = str(benchmark.get("benchmark_id") or benchmark_id)
        benchmark["return_fraction"] = return_fraction
        benchmark["return_pct"] = return_pct
        benchmarks.append(benchmark)

    session_date = (
        max(session_dates)
        if session_dates
        else observed.astimezone(TAIPEI).date().isoformat()
    )

    def current(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            row
            for row in rows
            if not row.get("session_date")
            or str(row.get("session_date")) == session_date
        ]

    signals = current(_tail(root / "signals.jsonl", maximum_signal_rows))
    orders = current(_tail(root / "orders.jsonl", maximum_event_rows))
    fills = current(_tail(root / "fills.jsonl", maximum_event_rows))
    raw_marks = current(_tail(root / "marks.jsonl", maximum_mark_rows))
    marks_by_mode_minute: dict[tuple[str, str], dict[str, Any]] = {}
    for source_row in raw_marks:
        row = dict(source_row)
        return_fraction, return_pct = _capital_return(
            row.get("initial_capital_twd"), row.get("total_equity_twd")
        )
        row["return_fraction"] = return_fraction
        row["return_pct"] = return_pct
        marks_by_mode_minute[(str(row.get("market")), str(row.get("minute")))] = row
    marks = list(marks_by_mode_minute.values())
    raw_benchmark_marks = current(
        _tail(root / "benchmark_marks.jsonl", maximum_mark_rows)
    )
    benchmark_marks_by_id_minute: dict[tuple[str, str], dict[str, Any]] = {}
    for source_row in raw_benchmark_marks:
        row = dict(source_row)
        return_fraction, return_pct = _capital_return(
            row.get("initial_capital_twd"), row.get("total_equity_twd")
        )
        row["return_fraction"] = return_fraction
        row["return_pct"] = return_pct
        benchmark_marks_by_id_minute[
            (str(row.get("benchmark_id")), str(row.get("minute")))
        ] = row
    benchmark_marks = list(benchmark_marks_by_id_minute.values())
    events = _tail(root / "events.jsonl", min(maximum_event_rows, 2_000))
    modes.sort(key=lambda row: str(row.get("market")))
    benchmarks.sort(key=lambda row: str(row.get("benchmark_id")))
    positions.sort(
        key=lambda row: (
            str(row.get("market")),
            0 if int(row.get("signed_shares") or 0) else 1,
            str(row.get("symbol")),
        )
    )
    preopen = _preopen_progress(
        path=preopen_readiness_path,
        modes=modes,
        observed=observed,
    )
    session_progress = _session_progress(
        observed=observed,
        mode_count=len(modes),
        modes=modes,
        marks=marks,
    )
    record_counts = {
        "signals": _line_count(root / "signals.jsonl"),
        "orders": _line_count(root / "orders.jsonl"),
        "fills": _line_count(root / "fills.jsonl"),
        "marks": _line_count(root / "marks.jsonl"),
        "benchmark_marks": _line_count(root / "benchmark_marks.jsonl"),
        "events": _line_count(root / "events.jsonl"),
    }

    return {
        "schema_version": DASHBOARD_SCHEMA_VERSION,
        "generated_at_utc": observed.isoformat(timespec="seconds"),
        "health": health,
        "source_updated_at": status.get("updated_at"),
        "source_age_seconds": round(source_age, 3),
        "simulation_only": True,
        "production_order_possible": False,
        "session_date": session_date,
        "schedule": status.get("schedule") or {},
        "preopen": preopen,
        "session_progress": session_progress,
        "modes": modes,
        "benchmarks": benchmarks,
        "positions": positions,
        "signals": signals,
        "orders": orders,
        "fills": fills,
        "marks": marks,
        "benchmark_marks": benchmark_marks,
        "events": events,
        "record_counts": record_counts,
        "payload_window": {
            "positions": len(positions),
            "signals": len(signals),
            "orders": len(orders),
            "fills": len(fills),
            "marks": len(marks),
            "benchmark_marks": len(benchmark_marks),
            "events": len(events),
        },
        "source_contract": {
            "preopen": "artifacts/discord_bot/preopen_readiness.json; only same-day recorded stages are shown and missing intermediate states are not estimated",
            "signal": "Discord live target_weights.parquet after observed opening quote",
            "entry_fill": "causally later best ask for buy and best bid for sell",
            "mark": "best bid liquidates long; best ask covers short",
            "missing_mark": "carry only the same open position's last complete liquidation value and flag stale",
            "eligibility": "exact-session TWSE and TPEx official day-trade membership; missing venue/date blocks",
            "fees": "gross commission and sell tax are charged first; earned commission rebate is recorded separately in economic NAV",
            "comparison": "all strategies and benchmarks are compared as cumulative net return divided by their own capital basis; TX uses one-contract official initial margin, while 0050/2330 use one-board-lot entry notional",
            "benchmarks": "0050/2330 price-return ledgers enter at best ask and liquidate at best bid after tw_cash costs; future cash distributions are not credited. TXFR1 holds one TX across sessions, rolls only when old bid and new ask coexist, and includes TWD 60 per side plus statutory futures tax",
            "depth_limit": "each mode is an independent counterfactual; only displayed level-one MIS volume is fillable (lots x 1,000 shares), while queue and deeper book are unknown",
        },
    }


def build_dashboard_signal_page(
    *,
    state_dir: Path,
    mode: str = "",
    symbol: str = "",
    status: str = "all",
    offset: int = 0,
    limit: int = 250,
    maximum_scan_rows: int = 100_000,
) -> dict[str, Any]:
    """Return a bounded, server-filtered page from the append-only signal ledger."""

    if offset < 0:
        raise ValueError("offset must be non-negative")
    if not 1 <= limit <= 1_000:
        raise ValueError("limit must be between 1 and 1000")
    root = Path(state_dir)
    state = _object(root / "state.json")
    session_dates = [
        str(item.get("session_date"))
        for item in (state.get("modes") or {}).values()
        if isinstance(item, Mapping) and item.get("session_date")
    ]
    session_date = (
        max(session_dates) if session_dates else datetime.now(TAIPEI).date().isoformat()
    )
    normalized_mode = str(mode or "").strip()
    normalized_symbol = str(symbol or "").strip().casefold()
    normalized_status = str(status or "all").strip().casefold()
    rows = _tail(root / "signals.jsonl", maximum_scan_rows)
    current_rows = [
        row
        for row in rows
        if not row.get("session_date") or str(row.get("session_date")) == session_date
    ]

    def included(row: Mapping[str, Any]) -> bool:
        if normalized_mode and normalized_mode != "all":
            if str(row.get("market") or "") != normalized_mode:
                return False
        if normalized_symbol:
            haystack = f"{row.get('symbol') or ''} {row.get('name') or ''}".casefold()
            if normalized_symbol not in haystack:
                return False
        if normalized_status == "blocked":
            return str(row.get("status") or "") not in {
                "ready",
                "partial_depth",
                "partial_directional_mix",
                "hold",
            }
        return True

    filtered = [row for row in current_rows if included(row)]

    def sort_key(row: Mapping[str, Any]) -> tuple[float, float, str, str]:
        weight = _finite_float(row.get("target_weight"))
        resolved = weight if weight is not None else 0.0
        return (
            -abs(resolved),
            -resolved,
            str(row.get("market") or ""),
            str(row.get("symbol") or ""),
        )

    filtered.sort(key=sort_key)
    capitals = {
        str(market): _finite_float(raw_mode.get("initial_capital_twd"))
        for market, raw_mode in (state.get("modes") or {}).items()
        if isinstance(raw_mode, Mapping)
    }
    current_signal_ids = {
        str(market): str(raw_mode.get("signal_id") or "")
        for market, raw_mode in (state.get("modes") or {}).items()
        if isinstance(raw_mode, Mapping) and raw_mode.get("signal_id")
    }
    direction_summary: dict[str, dict[str, float | int]] = {
        stage: {
            "long_count": 0,
            "short_count": 0,
            "long_gross": 0.0,
            "short_gross": 0.0,
        }
        for stage in ("target", "pre_balance", "actual")
    }
    summary_rows = [
        row
        for row in filtered
        if not current_signal_ids.get(str(row.get("market") or ""))
        or str(row.get("signal_id") or "")
        == current_signal_ids[str(row.get("market") or "")]
    ]
    for row in summary_rows:
        target = _finite_float(row.get("target_weight")) or 0.0
        capital = capitals.get(str(row.get("market") or ""))
        entry_price = _finite_float(row.get("ask") if target > 0.0 else row.get("bid"))

        def executed_weight(explicit_key: str, shares_key: str) -> float:
            explicit = _finite_float(row.get(explicit_key))
            if explicit is not None:
                return explicit
            shares = _finite_float(row.get(shares_key)) or 0.0
            if not capital or not entry_price or target == 0.0:
                return 0.0
            return math.copysign(shares * entry_price / capital, target)

        values = {
            "target": target,
            "pre_balance": executed_weight(
                "pre_balance_filled_weight",
                "pre_balance_filled_shares"
                if "pre_balance_filled_shares" in row
                else "filled_shares",
            ),
            "actual": executed_weight("filled_weight", "filled_shares"),
        }
        for stage, value in values.items():
            if value > 0.0:
                direction_summary[stage]["long_count"] += 1
                direction_summary[stage]["long_gross"] += value
            elif value < 0.0:
                direction_summary[stage]["short_count"] += 1
                direction_summary[stage]["short_gross"] += -value

    page = filtered[offset : offset + limit]
    return {
        "schema_version": DASHBOARD_SCHEMA_VERSION,
        "session_date": session_date,
        "offset": offset,
        "limit": limit,
        "returned": len(page),
        "total": len(filtered),
        "has_more": offset + len(page) < len(filtered),
        "source_rows_scanned": len(current_rows),
        "record_count": _line_count(root / "signals.jsonl"),
        "direction_summary_scope": "current_signal_id_per_mode",
        "direction_summary": direction_summary,
        "rows": page,
    }


__all__ = ["build_dashboard_signal_page", "build_dashboard_snapshot"]

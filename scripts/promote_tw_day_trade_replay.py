#!/usr/bin/env python3
"""Validate and atomically promote a rebuilt TW day-trade paper ledger."""

from __future__ import annotations

import argparse
import ctypes
from datetime import datetime, timedelta
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import sys
from typing import Any, Mapping
import uuid
from zoneinfo import ZoneInfo

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data.tw_price_rules import move_price_ticks_numpy
from scripts.rebuild_tw_day_trade_minute_curves import (
    historical_minute_mark_has_source,
)


TAIPEI = ZoneInfo("Asia/Taipei")
AT_FDCWD = -100
RENAME_EXCHANGE = 2
HYBRID_ENTRY_POLICY = "causal_best_quote_else_adverse_open_tick"
HYBRID_REPLAY_CONTRACT = (
    "retrospective_historical_best_quote_else_adverse_open_tick_counterfactual"
)
PAPER_MARKET_ENTRY_POLICY = "market_at_best_quote_else_adverse_open_tick"
PAPER_MARKET_REPLAY_CONTRACT = (
    "retrospective_historical_best_quote_market_else_adverse_open_tick_counterfactual"
)
OFFICIAL_OPEN_ENTRY_POLICY = "official_open_at_09_01"
OFFICIAL_OPEN_REPLAY_CONTRACT = (
    "retrospective_official_session_open_at_09_01_counterfactual"
)
MINUTE_VWAP_0901_ENTRY_POLICY = "official_open_signal_0900_execute_0901_vwap"
MINUTE_VWAP_0901_REPLAY_CONTRACT = (
    "retrospective_official_open_signal_at_09_00_observed_09_01_minute_vwap_counterfactual"
)
MINUTE_CURVE_CONTRACT = "right_labelled_historical_last_trade_mark_v1"
MINUTE_CURVE_SESSION_POINTS = 270


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".tmp.{uuid.uuid4().hex}")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _load_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON root is not an object: {path}")
    return payload


def _validate_minute_curve_coverage(
    candidate: Path,
    *,
    completed_session_dates: list[str],
    expected_markets: set[str],
    failures: list[str],
) -> dict[str, Any]:
    """Require every completed replay session to ship a minute-grain curve."""

    if not completed_session_dates:
        return {
            "required": False,
            "completed_session_dates": [],
            "validated_rows": 0,
        }
    receipt_path = candidate / "minute_curve_receipt.json"
    marks_path = candidate / "marks.jsonl"
    benchmark_path = candidate / "benchmark_history.json"
    if not receipt_path.is_file():
        failures.append(
            "minute_curve_receipt.json is required for every completed replay session"
        )
        return {
            "required": True,
            "completed_session_dates": completed_session_dates,
            "validated_rows": 0,
        }
    try:
        receipt = _load_object(receipt_path)
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        failures.append(f"invalid minute_curve_receipt.json: {type(exc).__name__}:{exc}")
        return {
            "required": True,
            "completed_session_dates": completed_session_dates,
            "validated_rows": 0,
        }

    strategy = receipt.get("strategy")
    strategy = strategy if isinstance(strategy, dict) else {}
    coverage = receipt.get("coverage_after_fetch")
    coverage = coverage if isinstance(coverage, dict) else {}
    outputs = receipt.get("outputs")
    outputs = outputs if isinstance(outputs, dict) else {}
    expected_rows = (
        len(completed_session_dates)
        * len(expected_markets)
        * MINUTE_CURVE_SESSION_POINTS
    )
    if receipt.get("simulation_only") is not True:
        failures.append("minute curve receipt is not simulation_only=true")
    if receipt.get("production_order_possible") is not False:
        failures.append("minute curve receipt does not prohibit production orders")
    if str(receipt.get("minute_contract") or "") != MINUTE_CURVE_CONTRACT:
        failures.append("minute curve receipt has the wrong one-minute contract")
    if receipt.get("linear_interpolation_used") is not False:
        failures.append("minute curve receipt permits linear interpolation")
    if receipt.get("accepted_09_01_strategy_and_13_30_endpoints_preserved") is not True:
        failures.append("minute curve receipt did not preserve accepted endpoints")
    if int(coverage.get("missing_pairs") or 0) != 0:
        failures.append("minute curve receipt still has missing symbol-date pairs")
    if sorted(str(value) for value in strategy.get("session_dates") or ()) != completed_session_dates:
        failures.append("minute curve session dates do not match completed replay sessions")
    if {str(value) for value in strategy.get("markets") or ()} != expected_markets:
        failures.append("minute curve markets do not match the promoted mode set")
    if int(strategy.get("generated_rows") or 0) != expected_rows:
        failures.append(
            "minute curve generated row count does not equal "
            f"{MINUTE_CURVE_SESSION_POINTS} points per completed session and mode"
        )
    if str(receipt.get("start_date") or "") != completed_session_dates[0]:
        failures.append("minute curve start date does not match the replay start")
    if str(receipt.get("end_date") or "") != completed_session_dates[-1]:
        failures.append("minute curve end date does not match the latest completed replay")

    for path, key in (
        (marks_path, "marks"),
        (benchmark_path, "benchmark_history"),
    ):
        output = outputs.get(key)
        output = output if isinstance(output, dict) else {}
        if not path.is_file():
            failures.append(f"minute curve output is missing: {path.name}")
        elif str(output.get("sha256") or "") != _sha256(path):
            failures.append(f"minute curve output hash mismatch: {path.name}")

    observed: dict[tuple[str, str], set[str]] = {}
    unverified_interior_rows = 0
    unverified_interior_samples: list[str] = []
    if marks_path.is_file():
        with marks_path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                try:
                    row = json.loads(line)
                except (TypeError, ValueError, json.JSONDecodeError):
                    failures.append(f"marks.jsonl:{line_number}: invalid JSON")
                    continue
                if not isinstance(row, dict):
                    continue
                session_date = str(row.get("session_date") or "")
                market = str(row.get("market") or "")
                if session_date not in completed_session_dates or market not in expected_markets:
                    continue
                minute = str(row.get("minute") or "")
                observed.setdefault((session_date, market), set()).add(minute)
                if (
                    minute[11:16] not in {"09:01", "13:30"}
                    and not historical_minute_mark_has_source(row)
                ):
                    unverified_interior_rows += 1
                    if len(unverified_interior_samples) < 20:
                        unverified_interior_samples.append(
                            f"{session_date}:{market}:{minute}"
                        )
    for session_date in completed_session_dates:
        base = datetime.fromisoformat(f"{session_date}T09:01:00+08:00")
        expected_minutes = {
            (base + timedelta(minutes=offset)).isoformat(timespec="minutes")
            for offset in range(MINUTE_CURVE_SESSION_POINTS)
        }
        for market in expected_markets:
            actual = observed.get((session_date, market), set())
            if actual != expected_minutes:
                failures.append(
                    f"{session_date}/{market}: minute curve does not contain exactly "
                    "09:01 through 13:30"
                )
    if unverified_interior_rows:
        failures.append(
            "minute curve contains interior rows without auditable historical "
            f"price provenance: {unverified_interior_rows}; "
            f"sample={unverified_interior_samples}"
        )
    return {
        "required": True,
        "contract": str(receipt.get("minute_contract") or ""),
        "completed_session_dates": completed_session_dates,
        "points_per_session_mode": MINUTE_CURVE_SESSION_POINTS,
        "validated_rows": sum(len(values) for values in observed.values()),
        "audited_historical_interior_rows": (
            len(completed_session_dates) * len(expected_markets) * 268
            - unverified_interior_rows
        ),
        "unverified_historical_interior_rows": unverified_interior_rows,
        "receipt_sha256": _sha256(receipt_path),
    }


def _validate_hybrid_signal_ledger(
    candidate: Path,
    *,
    expected_best_quote_fills: int,
    expected_synthetic_fallback_fills: int,
    failures: list[str],
) -> dict[str, int]:
    path = candidate / "signals.jsonl"
    if not path.is_file():
        failures.append("hybrid replay has no signals.jsonl audit ledger")
        return {"best_quote_fills": 0, "synthetic_fallback_fills": 0}
    exact_count = 0
    fallback_count = 0
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                failures.append(f"signals.jsonl:{line_number}: invalid JSON")
                continue
            policy = str(row.get("entry_fill_policy") or "")
            if policy not in {HYBRID_ENTRY_POLICY, PAPER_MARKET_ENTRY_POLICY}:
                continue
            filled_shares = int(row.get("filled_shares") or 0)
            if filled_shares <= 0:
                continue
            side = str(row.get("side") or "")
            execution_price = float(row.get("execution_price") or math.nan)
            is_fallback = row.get("synthetic_fallback_fill") is True
            if is_fallback:
                fallback_count += 1
                if (
                    row.get("synthetic_fill") is not True
                    or row.get("status") != "forced_synthetic_fill"
                    or int(row.get("entry_price_offset_ticks") or 0) != 1
                    or row.get("entry_price_source")
                    != "official_daily_session_open:adverse_one_legal_tick_fallback"
                ):
                    failures.append(
                        f"signals.jsonl:{line_number}: malformed adverse-tick fallback"
                    )
                    continue
                if (
                    policy == PAPER_MARKET_ENTRY_POLICY
                    and row.get("paper_market_fill") is not True
                ):
                    failures.append(
                        f"signals.jsonl:{line_number}: paper fallback is not labelled"
                    )
                sizing_price = float(row.get("sizing_open_price") or math.nan)
                session_date = str(row.get("session_date") or "")
                direction = 1 if side == "long" else -1 if side == "short" else 0
                if direction == 0 or not math.isfinite(sizing_price):
                    failures.append(
                        f"signals.jsonl:{line_number}: fallback has invalid side/open"
                    )
                    continue
                expected_price = float(
                    move_price_ticks_numpy(
                        np.asarray([sizing_price], dtype=np.float64),
                        direction,
                        np.asarray([session_date]),
                    )[0]
                )
                lower = float(row.get("lower_limit") or math.nan)
                upper = float(row.get("upper_limit") or math.nan)
                if math.isfinite(lower):
                    expected_price = max(expected_price, lower)
                if math.isfinite(upper):
                    expected_price = min(expected_price, upper)
                if not math.isclose(
                    execution_price, expected_price, rel_tol=0.0, abs_tol=1e-9
                ):
                    failures.append(
                        f"signals.jsonl:{line_number}: fallback price is not adverse one tick"
                    )
            else:
                exact_count += 1
                price_key = "ask" if side == "long" else "bid"
                quote_price = float(row.get(price_key) or math.nan)
                source_quote_at = str(row.get("historical_source_quote_at") or "")
                try:
                    source_time = datetime.fromisoformat(source_quote_at)
                except ValueError:
                    source_time = None
                common_invalid = bool(
                    row.get("synthetic_fill") is not False
                    or row.get("status") not in {"ready", "partial_depth"}
                    or not math.isclose(
                        execution_price, quote_price, rel_tol=0.0, abs_tol=1e-9
                    )
                    or source_time is None
                    or source_time.date().isoformat()
                    != str(row.get("session_date") or "")
                    or source_time.hour != 9
                    or source_time.minute != 0
                )
                depth_invalid = bool(
                    policy == HYBRID_ENTRY_POLICY
                    and int(row.get("top_book_capacity_shares") or 0) < filled_shares
                )
                paper_market_invalid = bool(
                    policy == PAPER_MARKET_ENTRY_POLICY
                    and (
                        row.get("paper_market_fill") is not True
                        or filled_shares != int(row.get("requested_shares") or 0)
                    )
                )
                if common_invalid or depth_invalid or paper_market_invalid:
                    failures.append(
                        f"signals.jsonl:{line_number}: malformed historical best-quote fill"
                    )
    if exact_count != expected_best_quote_fills:
        failures.append(
            f"signals ledger best-quote fills={exact_count} receipt={expected_best_quote_fills}"
        )
    if fallback_count != expected_synthetic_fallback_fills:
        failures.append(
            "signals ledger fallback fills="
            f"{fallback_count} receipt={expected_synthetic_fallback_fills}"
        )
    return {
        "best_quote_fills": exact_count,
        "synthetic_fallback_fills": fallback_count,
    }


def _validate_official_open_signal_ledger(
    candidate: Path,
    *,
    expected_fills: int,
    failures: list[str],
) -> dict[str, int]:
    path = candidate / "signals.jsonl"
    if not path.is_file():
        failures.append("official-open replay has no signals.jsonl audit ledger")
        return {"official_open_fills": 0}
    fill_count = 0
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                failures.append(f"signals.jsonl:{line_number}: invalid JSON")
                continue
            if str(row.get("entry_fill_policy") or "") != OFFICIAL_OPEN_ENTRY_POLICY:
                continue
            filled_shares = int(row.get("filled_shares") or 0)
            if filled_shares <= 0:
                continue
            fill_count += 1
            try:
                execution_price = float(row.get("execution_price"))
                sizing_open = float(row.get("sizing_open_price"))
                recorded_at = datetime.fromisoformat(str(row.get("recorded_at")))
            except (TypeError, ValueError):
                failures.append(
                    f"signals.jsonl:{line_number}: malformed official-open fill"
                )
                continue
            if (
                row.get("counterfactual_open_price_fill") is not True
                or row.get("synthetic_fill") is not False
                or row.get("synthetic_fallback_fill") is not False
                or row.get("paper_market_fill") is not False
                or int(row.get("entry_price_offset_ticks") or 0) != 0
                or not math.isclose(
                    execution_price, sizing_open, rel_tol=0.0, abs_tol=1e-9
                )
                or recorded_at.hour != 9
                or recorded_at.minute != 1
            ):
                failures.append(
                    f"signals.jsonl:{line_number}: official-open/09:01 contract mismatch"
                )
    if fill_count != expected_fills:
        failures.append(
            f"signals ledger official-open fills={fill_count} receipt={expected_fills}"
        )
    return {"official_open_fills": fill_count}


def _validate_official_open_fill_ledger(
    candidate: Path,
    *,
    expected_fills: int,
    failures: list[str],
) -> dict[str, int]:
    path = candidate / "fills.jsonl"
    if not path.is_file():
        failures.append("official-open replay has no fills.jsonl audit ledger")
        return {"fill_ledger_official_open_fills": 0}
    fill_count = 0
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                failures.append(f"fills.jsonl:{line_number}: invalid JSON")
                continue
            if (
                str(row.get("purpose") or "") != "entry"
                or str(row.get("fill_contract") or "")
                != OFFICIAL_OPEN_REPLAY_CONTRACT
            ):
                continue
            fill_count += 1
            try:
                price = float(row.get("price"))
                fill_at = datetime.fromisoformat(str(row.get("fill_at")))
            except (TypeError, ValueError):
                failures.append(
                    f"fills.jsonl:{line_number}: malformed official-open fill"
                )
                continue
            if (
                str(row.get("entry_fill_policy") or "")
                != OFFICIAL_OPEN_ENTRY_POLICY
                or int(row.get("entry_price_offset_ticks") or 0) != 0
                or row.get("counterfactual_open_price_fill") is not True
                or row.get("synthetic_fill") is not False
                or row.get("synthetic_fallback_fill") is not False
                or row.get("paper_market_fill") is not False
                or not math.isfinite(price)
                or price <= 0.0
                or fill_at.hour != 9
                or fill_at.minute != 1
            ):
                failures.append(
                    f"fills.jsonl:{line_number}: official-open/09:01 contract mismatch"
                )
    if fill_count != expected_fills:
        failures.append(
            f"fills ledger official-open fills={fill_count} receipt={expected_fills}"
        )
    return {"fill_ledger_official_open_fills": fill_count}


def _validate_0901_vwap_signal_ledger(
    candidate: Path,
    *,
    expected_fills: int,
    failures: list[str],
) -> dict[str, int]:
    path = candidate / "signals.jsonl"
    if not path.is_file():
        failures.append("09:01 VWAP replay has no signals.jsonl audit ledger")
        return {"minute_vwap_0901_fills": 0}
    fill_count = 0
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                failures.append(f"signals.jsonl:{line_number}: invalid JSON")
                continue
            if str(row.get("entry_fill_policy") or "") != MINUTE_VWAP_0901_ENTRY_POLICY:
                continue
            if int(row.get("filled_shares") or 0) <= 0:
                continue
            fill_count += 1
            try:
                execution_price = float(row.get("execution_price"))
                sizing_open = float(row.get("sizing_open_price"))
                recorded_at = datetime.fromisoformat(str(row.get("recorded_at")))
            except (TypeError, ValueError):
                failures.append(
                    f"signals.jsonl:{line_number}: malformed 09:01 VWAP fill"
                )
                continue
            source = str(row.get("entry_price_source") or "")
            if (
                row.get("counterfactual_0901_price_fill") is not True
                or row.get("counterfactual_open_price_fill") is not False
                or row.get("synthetic_fill") is not False
                or row.get("synthetic_fallback_fill") is not False
                or row.get("paper_market_fill") is not False
                or int(row.get("entry_price_offset_ticks") or 0) != 0
                or not math.isfinite(execution_price)
                or execution_price <= 0.0
                or not math.isfinite(sizing_open)
                or sizing_open <= 0.0
                or "0901" not in source.replace(":", "").replace("_", "")
                or recorded_at.hour != 9
                or recorded_at.minute != 1
            ):
                failures.append(
                    f"signals.jsonl:{line_number}: 09:00-open/09:01-VWAP contract mismatch"
                )
    if fill_count != expected_fills:
        failures.append(
            f"signals ledger 09:01 VWAP fills={fill_count} receipt={expected_fills}"
        )
    return {"minute_vwap_0901_fills": fill_count}


def _validate_0901_vwap_fill_ledger(
    candidate: Path,
    *,
    expected_fills: int,
    failures: list[str],
) -> dict[str, int]:
    path = candidate / "fills.jsonl"
    if not path.is_file():
        failures.append("09:01 VWAP replay has no fills.jsonl audit ledger")
        return {"fill_ledger_minute_vwap_0901_fills": 0}
    fill_count = 0
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                failures.append(f"fills.jsonl:{line_number}: invalid JSON")
                continue
            if (
                str(row.get("purpose") or "") != "entry"
                or str(row.get("fill_contract") or "")
                != MINUTE_VWAP_0901_REPLAY_CONTRACT
            ):
                continue
            fill_count += 1
            try:
                price = float(row.get("price"))
                fill_at = datetime.fromisoformat(str(row.get("fill_at")))
            except (TypeError, ValueError):
                failures.append(
                    f"fills.jsonl:{line_number}: malformed 09:01 VWAP fill"
                )
                continue
            if (
                str(row.get("entry_fill_policy") or "")
                != MINUTE_VWAP_0901_ENTRY_POLICY
                or row.get("counterfactual_0901_price_fill") is not True
                or row.get("counterfactual_open_price_fill") is not False
                or row.get("synthetic_fill") is not False
                or row.get("synthetic_fallback_fill") is not False
                or row.get("paper_market_fill") is not False
                or not math.isfinite(price)
                or price <= 0.0
                or fill_at.hour != 9
                or fill_at.minute != 1
            ):
                failures.append(
                    f"fills.jsonl:{line_number}: 09:01 VWAP contract mismatch"
                )
    if fill_count != expected_fills:
        failures.append(
            f"fills ledger 09:01 VWAP fills={fill_count} receipt={expected_fills}"
        )
    return {"fill_ledger_minute_vwap_0901_fills": fill_count}


def _validate_rebuild(
    candidate: Path,
    *,
    expected_markets: set[str],
    allow_current_open_session: bool = False,
) -> dict[str, Any]:
    receipt_path = candidate / "rebuild_receipt.json"
    state_path = candidate / "state.json"
    receipt = _load_object(receipt_path)
    state = _load_object(state_path)
    failures: list[str] = []
    if receipt.get("simulation_only") is not True:
        failures.append("rebuild receipt is not simulation_only=true")
    if receipt.get("production_order_possible") is not False:
        failures.append("rebuild receipt does not prohibit production orders")
    replay_contract = receipt.get("replay_contract")
    receipt_entry_contract = (
        str(replay_contract.get("entry") or "")
        if isinstance(replay_contract, dict)
        else ""
    )
    modes = state.get("modes")
    if not isinstance(modes, dict):
        failures.append("state.modes is not an object")
        modes = {}
    if set(modes) != expected_markets:
        failures.append(f"mode set={sorted(modes)} expected={sorted(expected_markets)}")

    sessions = receipt.get("sessions")
    if not isinstance(sessions, list) or not sessions:
        failures.append("rebuild has no sessions")
        sessions = []
    session_dates: list[str] = []
    registrations = 0
    best_quote_fills = 0
    synthetic_fallback_fills = 0
    official_open_fills = 0
    minute_vwap_0901_fills = 0
    current_date = datetime.now(TAIPEI).date().isoformat()
    current_open_session: str | None = None
    for session_index, session in enumerate(sessions):
        if not isinstance(session, dict):
            failures.append("malformed session receipt")
            continue
        session_date = str(session.get("session_date") or "")
        session_dates.append(session_date)
        close = session.get("close")
        close_status = close.get("status") if isinstance(close, dict) else None
        is_allowed_current_open = bool(
            allow_current_open_session
            and session_index == len(sessions) - 1
            and session_date == current_date
            and close_status == "current_session_left_open_for_live_service"
        )
        if is_allowed_current_open:
            current_open_session = session_date
        elif close_status != "settled_official_close":
            failures.append(f"{session_date}: session is not settled at official close")
        mode_rows = session.get("modes")
        if not isinstance(mode_rows, list):
            failures.append(f"{session_date}: modes is not a list")
            continue
        observed_markets = {
            str(row.get("market") or "") for row in mode_rows if isinstance(row, dict)
        }
        if observed_markets != expected_markets:
            failures.append(
                f"{session_date}: modes={sorted(observed_markets)} "
                f"expected={sorted(expected_markets)}"
            )
        for row in mode_rows:
            if not isinstance(row, dict):
                continue
            market = str(row.get("market") or "")
            if row.get("register_result") != "registered":
                failures.append(
                    f"{session_date}/{market}: register_result="
                    f"{row.get('register_result')!r}"
                )
            else:
                registrations += 1
            entry = row.get("entry")
            if isinstance(entry, dict):
                policy = str(entry.get("entry_fill_policy") or "")
                fill_count = int(entry.get("entry_fill_count") or 0)
                exact_count = int(entry.get("entry_best_quote_fill_count") or 0)
                fallback_count = int(
                    entry.get("entry_synthetic_fallback_fill_count") or 0
                )
                official_open_count = int(
                    entry.get("entry_official_open_fill_count") or 0
                )
                minute_vwap_0901_count = int(
                    entry.get("entry_0901_vwap_fill_count") or 0
                )
                best_quote_fills += exact_count
                synthetic_fallback_fills += fallback_count
                official_open_fills += official_open_count
                minute_vwap_0901_fills += minute_vwap_0901_count
                if policy == MINUTE_VWAP_0901_ENTRY_POLICY:
                    if receipt_entry_contract != MINUTE_VWAP_0901_REPLAY_CONTRACT:
                        failures.append(
                            f"{session_date}/{market}: 09:01 VWAP replay contract is not explicit"
                        )
                    if minute_vwap_0901_count != fill_count:
                        failures.append(
                            f"{session_date}/{market}: 09:01 VWAP fill counts do not reconcile"
                        )
                    if (
                        entry.get("entry_fill_is_synthetic") is True
                        or fallback_count
                        or official_open_count
                    ):
                        failures.append(
                            f"{session_date}/{market}: 09:01 VWAP fill uses a forbidden fallback"
                        )
                if policy == OFFICIAL_OPEN_ENTRY_POLICY:
                    if receipt_entry_contract != OFFICIAL_OPEN_REPLAY_CONTRACT:
                        failures.append(
                            f"{session_date}/{market}: official-open replay contract is not explicit"
                        )
                    if official_open_count != fill_count:
                        failures.append(
                            f"{session_date}/{market}: official-open fill counts do not reconcile"
                        )
                    if entry.get("entry_fill_is_synthetic") is True or fallback_count:
                        failures.append(
                            f"{session_date}/{market}: official-open fill uses a synthetic tick"
                        )
                if policy in {HYBRID_ENTRY_POLICY, PAPER_MARKET_ENTRY_POLICY}:
                    required_contract = (
                        PAPER_MARKET_REPLAY_CONTRACT
                        if policy == PAPER_MARKET_ENTRY_POLICY
                        else HYBRID_REPLAY_CONTRACT
                    )
                    if receipt_entry_contract != required_contract:
                        failures.append(
                            f"{session_date}/{market}: replay contract is not explicit"
                        )
                    if exact_count + fallback_count != fill_count:
                        failures.append(
                            f"{session_date}/{market}: exact+fallback fill counts do not reconcile"
                        )
                elif fallback_count or entry.get("entry_fill_is_synthetic") is True:
                    failures.append(
                        f"{session_date}/{market}: synthetic fallback is not labelled hybrid"
                    )
            if is_allowed_current_open:
                if (
                    not isinstance(entry, dict)
                    or entry.get("engine_status") != "active"
                ):
                    failures.append(
                        f"{session_date}/{market}: current entry is not active"
                    )
            else:
                after_close = row.get("after_close")
                if (
                    not isinstance(after_close, dict)
                    or int(after_close.get("open_position_rows") or 0) != 0
                ):
                    failures.append(f"{session_date}/{market}: not flat after close")

    if session_dates != sorted(set(session_dates)):
        failures.append("session dates are duplicated or not strictly increasing")

    final_open_positions: dict[str, int] = {}
    ending_equity: dict[str, float] = {}
    for market, mode_value in modes.items():
        mode = mode_value if isinstance(mode_value, dict) else {}
        mode_policy = str(mode.get("entry_fill_policy") or "")
        mode_is_hybrid = mode_policy == HYBRID_ENTRY_POLICY
        mode_is_paper_market = mode_policy == PAPER_MARKET_ENTRY_POLICY
        mode_is_official_open = mode_policy == OFFICIAL_OPEN_ENTRY_POLICY
        mode_is_0901_vwap = mode_policy == MINUTE_VWAP_0901_ENTRY_POLICY
        if mode_policy == "synthetic_open_tick" or (
            mode.get("entry_fill_is_synthetic") is True
            and not (mode_is_hybrid or mode_is_paper_market)
        ):
            failures.append(
                f"{market}: legacy synthetic open-tick replay cannot be promoted"
            )
        if mode_is_hybrid and receipt_entry_contract != HYBRID_REPLAY_CONTRACT:
            failures.append(f"{market}: hybrid final state has no matching receipt")
        if (
            mode_is_paper_market
            and receipt_entry_contract != PAPER_MARKET_REPLAY_CONTRACT
        ):
            failures.append(
                f"{market}: paper-market final state has no matching receipt"
            )
        if (
            mode_is_official_open
            and receipt_entry_contract != OFFICIAL_OPEN_REPLAY_CONTRACT
        ):
            failures.append(
                f"{market}: official-open final state has no matching receipt"
            )
        if (
            mode_is_0901_vwap
            and receipt_entry_contract != MINUTE_VWAP_0901_REPLAY_CONTRACT
        ):
            failures.append(
                f"{market}: 09:01 VWAP final state has no matching receipt"
            )
        positions = mode.get("positions")
        if not isinstance(positions, dict):
            positions = {}
        open_count = sum(
            int(position.get("signed_shares") or 0) != 0
            for position in positions.values()
            if isinstance(position, dict)
        )
        final_open_positions[str(market)] = open_count
        if current_open_session is None and open_count:
            failures.append(f"{market}: final open positions={open_count}")
        if current_open_session is not None:
            if str(mode.get("session_date") or "") != current_open_session:
                failures.append(
                    f"{market}: final session_date is not {current_open_session}"
                )
            if mode.get("engine_status") != "active":
                failures.append(f"{market}: final current-session engine is not active")
            if mode.get("counterfactual_open_replay") is not True:
                failures.append(
                    f"{market}: current session is not counterfactual replay"
                )
            allowed_contracts = {"retrospective_observed_best_quote_counterfactual"}
            if mode_is_hybrid:
                allowed_contracts.add(HYBRID_REPLAY_CONTRACT)
            if mode_is_paper_market:
                allowed_contracts.add(PAPER_MARKET_REPLAY_CONTRACT)
            if mode_is_official_open:
                allowed_contracts.add(OFFICIAL_OPEN_REPLAY_CONTRACT)
            if mode_is_0901_vwap:
                allowed_contracts.add(MINUTE_VWAP_0901_REPLAY_CONTRACT)
            if mode.get("entry_fill_contract") not in allowed_contracts:
                failures.append(f"{market}: current entry fill contract is invalid")
            if mode.get("entry_fill_is_synthetic") is not False and not (
                mode_is_hybrid or mode_is_paper_market
            ):
                failures.append(
                    f"{market}: current entries are not received-book fills"
                )
            for symbol, position_value in positions.items():
                position = position_value if isinstance(position_value, dict) else {}
                if int(position.get("signed_shares") or 0) == 0:
                    continue
                for price_key in ("entry_price", "sizing_open_price"):
                    try:
                        value = float(position.get(price_key))
                    except (TypeError, ValueError):
                        value = float("nan")
                    if not (value > 0.0 and value < float("inf")):
                        failures.append(
                            f"{market}/{symbol}: invalid {price_key}={value!r}"
                        )
                if position.get("counterfactual_open_replay") is not True:
                    failures.append(
                        f"{market}/{symbol}: position is not counterfactual replay"
                    )
                if mode_is_0901_vwap and (
                    position.get("counterfactual_0901_price_fill") is not True
                    or position.get("counterfactual_open_price_fill") is not False
                ):
                    failures.append(
                        f"{market}/{symbol}: position is not an observed 09:01 VWAP fill"
                    )
                if (
                    position.get("entry_fill_is_synthetic") is not False
                    and not (mode_is_hybrid or mode_is_paper_market)
                ):
                    failures.append(
                        f"{market}/{symbol}: position is not a received-book fill"
                    )
        ending_equity[str(market)] = float(mode.get("total_equity_twd") or 0.0)

    signal_ledger_validation = {
        "best_quote_fills": best_quote_fills,
        "synthetic_fallback_fills": synthetic_fallback_fills,
    }
    if receipt_entry_contract in {
        HYBRID_REPLAY_CONTRACT,
        PAPER_MARKET_REPLAY_CONTRACT,
    }:
        signal_ledger_validation = _validate_hybrid_signal_ledger(
            candidate,
            expected_best_quote_fills=best_quote_fills,
            expected_synthetic_fallback_fills=synthetic_fallback_fills,
            failures=failures,
        )
    elif receipt_entry_contract == OFFICIAL_OPEN_REPLAY_CONTRACT:
        signal_ledger_validation = _validate_official_open_signal_ledger(
            candidate,
            expected_fills=official_open_fills,
            failures=failures,
        )
        signal_ledger_validation.update(
            _validate_official_open_fill_ledger(
                candidate,
                expected_fills=official_open_fills,
                failures=failures,
            )
        )
    elif receipt_entry_contract == MINUTE_VWAP_0901_REPLAY_CONTRACT:
        signal_ledger_validation = _validate_0901_vwap_signal_ledger(
            candidate,
            expected_fills=minute_vwap_0901_fills,
            failures=failures,
        )
        signal_ledger_validation.update(
            _validate_0901_vwap_fill_ledger(
                candidate,
                expected_fills=minute_vwap_0901_fills,
                failures=failures,
            )
        )

    completed_session_dates = [
        session_date
        for session_date in session_dates
        if session_date != current_open_session
    ]
    minute_curve_validation = _validate_minute_curve_coverage(
        candidate,
        completed_session_dates=completed_session_dates,
        expected_markets=expected_markets,
        failures=failures,
    )

    if failures:
        raise RuntimeError("replay promotion validation failed: " + "; ".join(failures))
    return {
        "session_dates": session_dates,
        "session_count": len(session_dates),
        "registrations": registrations,
        "best_quote_fills": best_quote_fills,
        "synthetic_fallback_fills": synthetic_fallback_fills,
        "official_open_fills": official_open_fills,
        "minute_vwap_0901_fills": minute_vwap_0901_fills,
        "signal_ledger_validation": signal_ledger_validation,
        "minute_curve_validation": minute_curve_validation,
        "mode_set": sorted(expected_markets),
        "final_open_positions": final_open_positions,
        "ending_equity_twd": ending_equity,
        "allow_current_open_session": bool(allow_current_open_session),
        "current_open_session": current_open_session,
        "rebuild_receipt_sha256": _sha256(receipt_path),
        "state_sha256": _sha256(state_path),
    }


def _acquire_engine_lock(directory: Path):
    path = directory / ".engine.lock"
    path.touch(exist_ok=True)
    handle = path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        raise RuntimeError(
            f"paper ledger still has a live writer: {directory}"
        ) from None
    return handle


def _exchange_directories(left: Path, right: Path) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = libc.renameat2
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    result = renameat2(
        AT_FDCWD,
        os.fsencode(left),
        AT_FDCWD,
        os.fsencode(right),
        RENAME_EXCHANGE,
    )
    if result != 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live-dir", type=Path, required=True)
    parser.add_argument("--candidate-dir", type=Path, required=True)
    parser.add_argument(
        "--expected-market",
        action="append",
        required=True,
        help="Repeat once for every and only intended active paper mode.",
    )
    parser.add_argument(
        "--allow-current-open-session",
        action="store_true",
        help=(
            "Permit only the final Taipei-today session to remain open after "
            "validating its explicitly recorded counterfactual fill contract."
        ),
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate the candidate and print acceptance without exchanging directories.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    live = args.live_dir.resolve(strict=True)
    candidate = args.candidate_dir.resolve(strict=True)
    if live == candidate or live in candidate.parents or candidate in live.parents:
        raise ValueError("live and candidate directories must be separate siblings")
    if live.stat().st_dev != candidate.stat().st_dev:
        raise RuntimeError("atomic directory exchange requires the same filesystem")
    expected_markets = {str(value).strip() for value in args.expected_market}
    if not expected_markets or "" in expected_markets:
        raise ValueError("--expected-market values must be non-empty")
    acceptance = _validate_rebuild(
        candidate,
        expected_markets=expected_markets,
        allow_current_open_session=bool(args.allow_current_open_session),
    )
    if args.validate_only:
        print(
            json.dumps(
                {"status": "validated", "acceptance": acceptance},
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return

    live_lock = _acquire_engine_lock(live)
    candidate_lock = _acquire_engine_lock(candidate)
    exchanged = False
    try:
        _exchange_directories(live, candidate)
        exchanged = True
        promotion_receipt = {
            "schema_version": 3,
            "promoted_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
            "simulation_only": True,
            "production_order_possible": False,
            "promotion_method": "same_filesystem_atomic_directory_exchange",
            "live_directory": str(live),
            "rollback_directory": str(candidate),
            "acceptance": acceptance,
        }
        _atomic_json(live / "promotion_receipt.json", promotion_receipt)
    except Exception:
        if exchanged:
            _exchange_directories(live, candidate)
        raise
    finally:
        candidate_lock.close()
        live_lock.close()

    print(
        json.dumps(
            {
                "status": "promoted",
                "live_directory": str(live),
                "rollback_directory": str(candidate),
                "acceptance": acceptance,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

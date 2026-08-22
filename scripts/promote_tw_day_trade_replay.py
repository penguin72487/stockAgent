#!/usr/bin/env python3
"""Validate and atomically promote a rebuilt TW day-trade paper ledger."""

from __future__ import annotations

import argparse
import ctypes
from datetime import datetime
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


TAIPEI = ZoneInfo("Asia/Taipei")
AT_FDCWD = -100
RENAME_EXCHANGE = 2
HYBRID_ENTRY_POLICY = "causal_best_quote_else_adverse_open_tick"
HYBRID_REPLAY_CONTRACT = (
    "retrospective_historical_best_quote_else_adverse_open_tick_counterfactual"
)


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
            if row.get("entry_fill_policy") != HYBRID_ENTRY_POLICY:
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
                if (
                    row.get("synthetic_fill") is not False
                    or row.get("status") not in {"ready", "partial_depth"}
                    or not math.isclose(
                        execution_price, quote_price, rel_tol=0.0, abs_tol=1e-9
                    )
                    or int(row.get("top_book_capacity_shares") or 0) < filled_shares
                    or source_time is None
                    or source_time.date().isoformat()
                    != str(row.get("session_date") or "")
                    or source_time.hour != 9
                    or source_time.minute != 0
                ):
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
                best_quote_fills += exact_count
                synthetic_fallback_fills += fallback_count
                if policy == HYBRID_ENTRY_POLICY:
                    if receipt_entry_contract != HYBRID_REPLAY_CONTRACT:
                        failures.append(
                            f"{session_date}/{market}: hybrid replay contract is not explicit"
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
        if mode_policy == "synthetic_open_tick" or (
            mode.get("entry_fill_is_synthetic") is True and not mode_is_hybrid
        ):
            failures.append(
                f"{market}: legacy synthetic open-tick replay cannot be promoted"
            )
        if mode_is_hybrid and receipt_entry_contract != HYBRID_REPLAY_CONTRACT:
            failures.append(f"{market}: hybrid final state has no matching receipt")
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
            if mode.get("entry_fill_contract") not in allowed_contracts:
                failures.append(f"{market}: current entry fill contract is invalid")
            if mode.get("entry_fill_is_synthetic") is not False and not mode_is_hybrid:
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
                if (
                    position.get("entry_fill_is_synthetic") is not False
                    and not mode_is_hybrid
                ):
                    failures.append(
                        f"{market}/{symbol}: position is not a received-book fill"
                    )
        ending_equity[str(market)] = float(mode.get("total_equity_twd") or 0.0)

    signal_ledger_validation = {
        "best_quote_fills": best_quote_fills,
        "synthetic_fallback_fills": synthetic_fallback_fills,
    }
    if receipt_entry_contract == HYBRID_REPLAY_CONTRACT:
        signal_ledger_validation = _validate_hybrid_signal_ledger(
            candidate,
            expected_best_quote_fills=best_quote_fills,
            expected_synthetic_fallback_fills=synthetic_fallback_fills,
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
        "signal_ledger_validation": signal_ledger_validation,
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

#!/usr/bin/env python3
"""Validate and atomically promote a rebuilt TW day-trade paper ledger."""

from __future__ import annotations

import argparse
import ctypes
from datetime import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sys
from typing import Any, Mapping
import uuid
from zoneinfo import ZoneInfo


TAIPEI = ZoneInfo("Asia/Taipei")
AT_FDCWD = -100
RENAME_EXCHANGE = 2


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


def _validate_rebuild(
    candidate: Path,
    *,
    expected_markets: set[str],
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
    modes = state.get("modes")
    if not isinstance(modes, dict):
        failures.append("state.modes is not an object")
        modes = {}
    if set(modes) != expected_markets:
        failures.append(
            f"mode set={sorted(modes)} expected={sorted(expected_markets)}"
        )

    sessions = receipt.get("sessions")
    if not isinstance(sessions, list) or not sessions:
        failures.append("rebuild has no sessions")
        sessions = []
    session_dates: list[str] = []
    registrations = 0
    for session in sessions:
        if not isinstance(session, dict):
            failures.append("malformed session receipt")
            continue
        session_date = str(session.get("session_date") or "")
        session_dates.append(session_date)
        close = session.get("close")
        if not isinstance(close, dict) or close.get("status") != "settled_official_close":
            failures.append(f"{session_date}: session is not settled at official close")
        mode_rows = session.get("modes")
        if not isinstance(mode_rows, list):
            failures.append(f"{session_date}: modes is not a list")
            continue
        observed_markets = {
            str(row.get("market") or "")
            for row in mode_rows
            if isinstance(row, dict)
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
            after_close = row.get("after_close")
            if not isinstance(after_close, dict) or int(
                after_close.get("open_position_rows") or 0
            ) != 0:
                failures.append(f"{session_date}/{market}: not flat after close")

    final_open_positions: dict[str, int] = {}
    ending_equity: dict[str, float] = {}
    for market, mode_value in modes.items():
        mode = mode_value if isinstance(mode_value, dict) else {}
        positions = mode.get("positions")
        if not isinstance(positions, dict):
            positions = {}
        open_count = sum(
            int(position.get("signed_shares") or 0) != 0
            for position in positions.values()
            if isinstance(position, dict)
        )
        final_open_positions[str(market)] = open_count
        if open_count:
            failures.append(f"{market}: final open positions={open_count}")
        ending_equity[str(market)] = float(mode.get("total_equity_twd") or 0.0)

    if failures:
        raise RuntimeError("replay promotion validation failed: " + "; ".join(failures))
    return {
        "session_dates": session_dates,
        "session_count": len(session_dates),
        "registrations": registrations,
        "mode_set": sorted(expected_markets),
        "final_open_positions": final_open_positions,
        "ending_equity_twd": ending_equity,
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
        raise RuntimeError(f"paper ledger still has a live writer: {directory}") from None
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
    acceptance = _validate_rebuild(candidate, expected_markets=expected_markets)

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

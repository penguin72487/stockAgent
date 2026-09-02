#!/usr/bin/env python3
"""Keep every completed TW day-trade replay at audited one-minute grain."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Mapping
import uuid

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.promote_tw_day_trade_replay import (  # noqa: E402
    _validate_minute_curve_coverage,
)
from scripts.rebuild_tw_day_trade_minute_curves import (  # noqa: E402
    historical_minute_mark_has_source,
)
from stockagent.live.shioaji_schedule import (  # noqa: E402
    HISTORICAL_MAX_TRAFFIC_FRACTION,
    TAIPEI,
    historical_query_is_protected,
)


def _object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON root must be an object: {path}")
    return payload


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".tmp.{uuid.uuid4().hex}")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def _completed_scope(
    state_dir: Path,
) -> tuple[list[str], set[str]]:
    rebuild = _object(state_dir / "rebuild_receipt.json")
    completed = {
        str(session.get("session_date") or "")
        for session in rebuild.get("sessions") or ()
        if isinstance(session, dict)
        and isinstance(session.get("close"), dict)
        and session["close"].get("status") == "settled_official_close"
    }
    state = _object(state_dir / "state.json")
    modes = state.get("modes")
    if not isinstance(modes, dict) or not modes:
        raise RuntimeError("state has no active day-trade modes")
    markets = {str(market) for market in modes}
    session_dates = {
        str(mode.get("session_date") or "")
        for mode in modes.values()
        if isinstance(mode, dict)
    }
    current_is_terminal = bool(
        len(session_dates) == 1
        and all(
            isinstance(mode, dict)
            and int(mode.get("open_position_count") or 0) == 0
            and not any(
                int(position.get("signed_shares") or 0) != 0
                for position in (mode.get("positions") or {}).values()
                if isinstance(position, dict)
            )
            and (
                bool(mode.get("closing_auction_settled_at"))
                or str(mode.get("engine_status") or "") == "terminal"
            )
            for mode in modes.values()
        )
    )
    if current_is_terminal:
        completed.update(session_dates)
    return sorted(value for value in completed if value), markets


def _validate_current(
    state_dir: Path,
    *,
    completed_session_dates: list[str],
    expected_markets: set[str],
) -> dict[str, Any]:
    failures: list[str] = []
    result = _validate_minute_curve_coverage(
        state_dir,
        completed_session_dates=completed_session_dates,
        expected_markets=expected_markets,
        failures=failures,
    )
    if failures:
        raise RuntimeError("; ".join(failures))
    return result


def _inspect_strategy_price_provenance(
    state_dir: Path,
    *,
    completed_session_dates: list[str],
    expected_markets: set[str],
) -> dict[str, Any]:
    """Inspect every completed-session interior minute, not just timestamps."""

    expected_sessions = set(completed_session_dates)
    expected_keys = {
        (session_date, market, minute)
        for session_date in completed_session_dates
        for market in expected_markets
        for minute in (
            datetime.fromisoformat(f"{session_date}T09:02:00+08:00")
            + timedelta(minutes=index)
            for index in range(268)
        )
    }
    audited: set[tuple[str, str, datetime]] = set()
    unverified: set[tuple[str, str, datetime]] = set()
    marks_path = state_dir / "marks.jsonl"
    with marks_path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            row = json.loads(line)
            if not isinstance(row, dict):
                continue
            session_date = str(row.get("session_date") or "")
            market = str(row.get("market") or "")
            if session_date not in expected_sessions or market not in expected_markets:
                continue
            try:
                minute = datetime.fromisoformat(str(row.get("minute") or ""))
            except ValueError:
                raise RuntimeError(f"marks.jsonl:{line_number}: invalid minute")
            key = (session_date, market, minute)
            if key not in expected_keys:
                continue
            if historical_minute_mark_has_source(row):
                audited.add(key)
            else:
                unverified.add(key)
    unverified.update(expected_keys - audited)
    return {
        "contract": "right_labelled_historical_last_trade_mark_v1",
        "expected_interior_rows": len(expected_keys),
        "audited_interior_rows": len(audited),
        "unverified_interior_rows": len(unverified),
        "unverified_sample": [
            f"{session_date}:{market}:{minute.isoformat(timespec='minutes')}"
            for session_date, market, minute in sorted(unverified)[:20]
        ],
    }


def _validate_benchmarks(
    state_dir: Path,
    *,
    completed_session_dates: list[str],
) -> dict[str, Any]:
    payload = _object(state_dir / "benchmark_history.json")
    marks = payload.get("marks")
    if not isinstance(marks, list):
        raise RuntimeError("benchmark history has no marks")
    expected_sessions = set(completed_session_dates)
    contracts = {
        "benchmark_0050": ("09:00", "13:30", 271),
        "benchmark_2330": ("09:00", "13:30", 271),
        "benchmark_tx_continuous": ("08:45", "13:44", 300),
    }
    counts: dict[str, int] = {}
    for benchmark_id, (first_clock, last_clock, expected_points) in contracts.items():
        rows = [
            row
            for row in marks
            if isinstance(row, dict)
            and row.get("benchmark_id") == benchmark_id
            and str(row.get("session_date") or "") in expected_sessions
        ]
        by_session: dict[str, list[str]] = {}
        for row in rows:
            session_date = str(row.get("session_date") or "")
            by_session.setdefault(session_date, []).append(str(row.get("minute") or ""))
        if set(by_session) != expected_sessions:
            missing = sorted(expected_sessions - set(by_session))
            raise RuntimeError(
                f"{benchmark_id} missing completed sessions: {missing[:20]}"
            )
        for session_date, minutes in by_session.items():
            if len(minutes) != expected_points or len(set(minutes)) != expected_points:
                raise RuntimeError(
                    f"{benchmark_id}:{session_date} minute count "
                    f"{len(minutes)}/{len(set(minutes))} != {expected_points}"
                )
            clocks = sorted(value[11:16] for value in minutes)
            if clocks[0] != first_clock or clocks[-1] != last_clock:
                raise RuntimeError(
                    f"{benchmark_id}:{session_date} minute boundary "
                    f"{clocks[0]}..{clocks[-1]} != {first_clock}..{last_clock}"
                )
        counts[benchmark_id] = len(rows)
    return {
        "completed_session_dates": completed_session_dates,
        "points_per_session": {
            benchmark_id: contract[2]
            for benchmark_id, contract in contracts.items()
        },
        "rows": counts,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=Path("artifacts/live/tw_day_trade_simulation"),
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path(
            "artifacts/data_repair/tw_day_trade_minute_curve/maintenance/current"
        ),
    )
    parser.add_argument(
        "--status-path",
        type=Path,
        default=Path("artifacts/operations/tw_day_trade_minute_curves/latest.json"),
    )
    parser.add_argument("--no-fetch", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    state_dir = args.state_dir.resolve()
    output_root = args.output_root.resolve()
    status_path = args.status_path.resolve()
    status_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = status_path.with_suffix(".lock")
    with lock_path.open("a+", encoding="utf-8") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("[tw-day-trade-minute-curves] another maintenance run is active")
            return

        observed = datetime.now(TAIPEI)
        completed, markets = _completed_scope(state_dir)
        if not completed:
            payload = {
                "schema_version": 1,
                "status": "waiting_completed_session",
                "observed_at": observed.isoformat(timespec="seconds"),
                "simulation_only": True,
                "production_order_possible": False,
            }
            _atomic_json(status_path, payload)
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
            return

        try:
            strategy_validation = _validate_current(
                state_dir,
                completed_session_dates=completed,
                expected_markets=markets,
            )
        except (OSError, RuntimeError, TypeError, ValueError):
            strategy_validation = None
        try:
            price_validation = _inspect_strategy_price_provenance(
                state_dir,
                completed_session_dates=completed,
                expected_markets=markets,
            )
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError):
            price_validation = None
        try:
            benchmark_validation = _validate_benchmarks(
                state_dir,
                completed_session_dates=completed,
            )
        except (OSError, RuntimeError, TypeError, ValueError):
            benchmark_validation = None
        price_provenance_ready = bool(
            price_validation is not None
            and int(price_validation.get("unverified_interior_rows") or 0) == 0
        )
        if (
            strategy_validation is not None
            and benchmark_validation is not None
            and price_provenance_ready
        ):
            payload = {
                "schema_version": 1,
                "status": "ready",
                "action": "no_op_already_complete",
                "observed_at": observed.isoformat(timespec="seconds"),
                "completed_session_dates": completed,
                "validation": {
                    "strategy": strategy_validation,
                    "strategy_price_provenance": price_validation,
                    "benchmarks": benchmark_validation,
                },
                "simulation_only": True,
                "production_order_possible": False,
            }
            _atomic_json(status_path, payload)
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
            return

        if historical_query_is_protected(observed) and not args.no_fetch:
            payload = {
                "schema_version": 1,
                "status": "waiting_live_priority_window",
                "observed_at": observed.isoformat(timespec="seconds"),
                "completed_session_dates": completed,
                "retry_contract": "weekly systemd calendar resumes after 14:31",
                "simulation_only": True,
                "production_order_possible": False,
            }
            _atomic_json(status_path, payload)
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
            return

        output_root.mkdir(parents=True, exist_ok=True)
        benchmark_command = [
            sys.executable,
            str(
                REPO_ROOT
                / "scripts"
                / "rebuild_tw_day_trade_benchmark_history.py"
            ),
            "--state-dir",
            str(state_dir),
            "--start-date",
            completed[0],
            "--end-date",
            completed[-1],
        ]
        benchmark_started = datetime.now(TAIPEI)
        benchmark_process = subprocess.run(
            benchmark_command,
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        if benchmark_process.returncode != 0:
            stderr_tail = benchmark_process.stderr.strip()[-4000:]
            stdout_tail = benchmark_process.stdout.strip()[-4000:]
            payload = {
                "schema_version": 1,
                "status": "failed",
                "failed_stage": "benchmark_history_rebuild",
                "observed_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
                "started_at": benchmark_started.isoformat(timespec="seconds"),
                "completed_session_dates": completed,
                "returncode": benchmark_process.returncode,
                "stderr_tail": stderr_tail,
                "stdout_tail": stdout_tail,
                "simulation_only": True,
                "production_order_possible": False,
            }
            _atomic_json(status_path, payload)
            if stdout_tail:
                print(stdout_tail, file=sys.stdout, flush=True)
            if stderr_tail:
                print(stderr_tail, file=sys.stderr, flush=True)
            raise RuntimeError(
                "benchmark-history rebuild failed with "
                f"{benchmark_process.returncode}"
            )
        if benchmark_process.stdout.strip():
            print(benchmark_process.stdout.strip(), flush=True)
        if benchmark_process.stderr.strip():
            print(benchmark_process.stderr.strip(), file=sys.stderr, flush=True)

        command = [
            sys.executable,
            str(REPO_ROOT / "scripts" / "rebuild_tw_day_trade_minute_curves.py"),
            "--state-dir",
            str(state_dir),
            "--start-date",
            completed[0],
            "--end-date",
            completed[-1],
            "--output-dir",
            str(output_root),
            "--simulation",
            "--max-traffic-fraction",
            str(HISTORICAL_MAX_TRAFFIC_FRACTION),
            "--publish",
        ]
        if not args.no_fetch:
            command.append("--fetch-missing-kbars")
        if strategy_validation is not None and price_provenance_ready:
            command.append("--validate-existing-strategy-marks")
        elif price_validation is not None and not price_provenance_ready:
            command.append("--repair-unverified-strategy-marks")
        started = datetime.now(TAIPEI)
        completed_process = subprocess.run(
            command,
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        if completed_process.returncode != 0:
            stderr_tail = completed_process.stderr.strip()[-4000:]
            stdout_tail = completed_process.stdout.strip()[-4000:]
            payload = {
                "schema_version": 1,
                "status": "failed",
                "observed_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
                "started_at": started.isoformat(timespec="seconds"),
                "completed_session_dates": completed,
                "returncode": completed_process.returncode,
                "stderr_tail": stderr_tail,
                "stdout_tail": stdout_tail,
                "simulation_only": True,
                "production_order_possible": False,
            }
            _atomic_json(status_path, payload)
            if stdout_tail:
                print(stdout_tail, file=sys.stdout, flush=True)
            if stderr_tail:
                print(stderr_tail, file=sys.stderr, flush=True)
            raise RuntimeError(
                f"minute-curve rebuild failed with {completed_process.returncode}"
            )
        if completed_process.stdout.strip():
            print(completed_process.stdout.strip(), flush=True)
        if completed_process.stderr.strip():
            print(completed_process.stderr.strip(), file=sys.stderr, flush=True)
        strategy_validation = _validate_current(
            state_dir,
            completed_session_dates=completed,
            expected_markets=markets,
        )
        price_validation = _inspect_strategy_price_provenance(
            state_dir,
            completed_session_dates=completed,
            expected_markets=markets,
        )
        if int(price_validation.get("unverified_interior_rows") or 0) != 0:
            raise RuntimeError(
                "minute-curve rebuild left unverified interior strategy prices: "
                f"{price_validation['unverified_sample']}"
            )
        benchmark_validation = _validate_benchmarks(
            state_dir,
            completed_session_dates=completed,
        )
        payload = {
            "schema_version": 1,
            "status": "ready",
            "action": "benchmarks_and_minute_curves_rebuilt_and_published",
            "observed_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
            "started_at": started.isoformat(timespec="seconds"),
            "completed_session_dates": completed,
            "validation": {
                "strategy": strategy_validation,
                "strategy_price_provenance": price_validation,
                "benchmarks": benchmark_validation,
            },
            "simulation_only": True,
            "production_order_possible": False,
        }
        _atomic_json(status_path, payload)
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

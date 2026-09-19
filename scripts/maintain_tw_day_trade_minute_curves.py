#!/usr/bin/env python3
"""Keep every completed TW day-trade replay at audited one-minute grain."""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
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
    _validate_benchmarks,
    _validate_minute_curve_coverage,
)
from scripts.rebuild_tw_day_trade_minute_curves import (  # noqa: E402
    _iter_jsonl,
    historical_minute_mark_has_source,
    missing_accepted_endpoints,
)
from stockagent.live.shioaji_schedule import (  # noqa: E402
    HISTORICAL_MAX_TRAFFIC_FRACTION,
    TAIPEI,
    historical_query_is_protected,
)
from stockagent.live.tw_day_trade_simulation import MARGIN_CARRY_CONTRACT
from downloader.download_shioaji_tx_futures_ticks import _valid_receipt  # noqa: E402


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
    # A one-time replay receipt stops at its deployment day; current state
    # retains only the latest session. Include intervening settled sessions
    # from the append-only engine ledger or their gaps disappear from audits.
    events_path = state_dir / "events.jsonl"
    if events_path.is_file():
        for event in _iter_jsonl(events_path):
            if event.get("event") != "closing_auction_settled" or event.get("market") not in markets:
                continue
            settled_at = datetime.fromisoformat(str(event["recorded_at"]))
            if settled_at.tzinfo is None:
                settled_at = settled_at.replace(tzinfo=TAIPEI)
            completed.add(settled_at.astimezone(TAIPEI).date().isoformat())
    session_dates = {
        str(mode.get("session_date") or "")
        for mode in modes.values()
        if isinstance(mode, dict)
    }
    current_is_terminal = bool(
        len(session_dates) == 1
        and all(
            isinstance(mode, dict)
            and not any(
                int(position.get("signed_shares") or 0) != 0 and not (
                    mode.get("margin_carry_contract") == MARGIN_CARRY_CONTRACT
                    and position.get("margin_carry_contract") == MARGIN_CARRY_CONTRACT)
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


def _tx_benchmark_source_state(
    tx_history_root: Path,
    completed_session: str,
) -> dict[str, Any]:
    """Validate the exact TXFR1 session consumed by the benchmark rebuild."""

    trading_date = date.fromisoformat(completed_session)
    receipt_path = (
        tx_history_root
        / "receipts"
        / f"trading_date={completed_session}.json"
    )
    receipt = _valid_receipt(tx_history_root, trading_date)
    ready = bool(receipt is not None and receipt.get("status") == "complete")
    return {
        "ready": ready,
        "trading_date": completed_session,
        "receipt_path": str(receipt_path),
        "status": receipt.get("status") if receipt is not None else "missing_or_invalid",
    }


def _inspect_strategy_price_provenance(
    state_dir: Path,
    *,
    completed_session_dates: list[str],
    expected_markets: set[str],
) -> dict[str, Any]:
    """Inspect completed-session opening and interior prices, not just timestamps."""

    expected_sessions = set(completed_session_dates)
    expected_keys = {
        (session_date, market, minute)
        for session_date in completed_session_dates
        for market in expected_markets
        for minute in (
            datetime.fromisoformat(f"{session_date}T09:01:00+08:00")
            + timedelta(minutes=index)
            for index in range(269)
        )
    }
    # The append-only ledger may contain multiple marks for one minute.  The
    # latest row is what the dashboard projects, so audit that exact row.
    source_by_key: dict[tuple[str, str, datetime], bool] = {}
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
            source_by_key[key] = historical_minute_mark_has_source(row)
    audited = {key for key, has_source in source_by_key.items() if has_source}
    unverified = expected_keys - audited
    opening = {key for key in expected_keys if key[2].hour == 9 and key[2].minute == 1}
    interior = expected_keys - opening
    return {
        "contract": "right_labelled_historical_last_trade_mark_v1",
        "expected_opening_rows": len(opening),
        "audited_opening_rows": len(audited & opening),
        "unverified_opening_rows": len(unverified & opening),
        "expected_interior_rows": len(interior),
        "audited_interior_rows": len(audited & interior),
        "unverified_interior_rows": len(unverified & interior),
        "unverified_sample": [
            f"{session_date}:{market}:{minute.isoformat(timespec='minutes')}"
            for session_date, market, minute in sorted(unverified)[:20]
        ],
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
    parser.add_argument(
        "--tx-history-root",
        type=Path,
        default=Path("data_tw_index_futures/shioaji_history/TXFR1"),
    )
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

        missing_endpoints = missing_accepted_endpoints(
            _iter_jsonl(state_dir / "marks.jsonl"),
            start=date.fromisoformat(completed[0]),
            end=date.fromisoformat(completed[-1]),
            expected_sessions=completed,
            expected_markets=markets,
        )
        if missing_endpoints:
            payload = {
                "schema_version": 1,
                "status": "waiting_accepted_endpoints",
                "observed_at": observed.isoformat(timespec="seconds"),
                "completed_session_dates": completed,
                "missing_endpoint_pairs": len(missing_endpoints),
                "missing_endpoints": missing_endpoints[:50],
                "reason": "Accepted entry/close ledger marks require canonical history repair; minute prices cannot reconstruct missed executions.",
                "simulation_only": True,
                "production_order_possible": False,
            }
            _atomic_json(status_path, payload)
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
            return

        # An attempted session is not a completed session: waiting_source also
        # records completed_session_dates. Always check the exact receipt before
        # starting the expensive benchmark rebuild, even on a retry.
        source_state = _tx_benchmark_source_state(
            getattr(
                args,
                "tx_history_root",
                Path("data_tw_index_futures/shioaji_history/TXFR1"),
            ).resolve(),
            completed[-1],
        )
        if not source_state["ready"]:
            payload = {
                "schema_version": 1,
                "status": "waiting_source",
                "failed_stage": "benchmark_source_preflight",
                "observed_at": observed.isoformat(timespec="seconds"),
                "completed_session_dates": completed,
                "source": source_state,
                "retry_contract": (
                    "A verified TXFR1 receipt change triggers the minute-curve "
                    "path unit; the post-close timer is a fallback. No benchmark "
                    "subprocess was started"
                ),
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
            and int(price_validation.get("unverified_opening_rows") or 0) == 0
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

        # The worker always publishes rebuilt marks.  Skipping API fallback
        # does not make that write safe during the protected live window.
        if historical_query_is_protected(observed):
            payload = {
                "schema_version": 1,
                "status": "waiting_live_priority_window",
                "observed_at": observed.isoformat(timespec="seconds"),
                "completed_session_dates": completed,
                "strategy_price_provenance": price_validation,
                "retry_contract": (
                    "scheduled post-close attempts at 14:35, 15:30 and 18:00; "
                    "do not compete with live quote traffic before 14:31"
                ),
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
        if price_validation is not None and int(price_validation.get("unverified_opening_rows") or 0) > 0:
            command.append("--revalue-opening-marks")
        has_margin_carry = any(m.get("margin_carry_contract") == MARGIN_CARRY_CONTRACT
                              for m in _object(state_dir / "state.json").get("modes", {}).values())
        if price_provenance_ready and (strategy_validation is not None or has_margin_carry):
            command.append("--validate-existing-strategy-marks")
        elif has_margin_carry:
            command.append("--revalue-carried-marks")
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
        if (int(price_validation.get("unverified_opening_rows") or 0) != 0
                or int(price_validation.get("unverified_interior_rows") or 0) != 0):
            raise RuntimeError(
                "minute-curve rebuild left unverified opening/interior strategy prices: "
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

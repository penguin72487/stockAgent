#!/usr/bin/env python3
"""Finish and atomically promote the Multi-Basis 22 day-trade history.

The model may be enabled during an open session, but its historical ledger must
not replace the evolving live ledger.  This reconciler therefore waits until
all paper modes are terminal, rebuilds one closed-session candidate through the
latest day, validates its minute curves, and only then performs the canonical
atomic promotion.
"""

from __future__ import annotations

from datetime import datetime, time
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time as wall_time
from typing import Any, Mapping, Sequence
import urllib.request
import uuid
from zoneinfo import ZoneInfo


REPO_ROOT = Path(__file__).resolve().parents[1]
TAIPEI = ZoneInfo("Asia/Taipei")
MARKETS = (
    "tw_day_trade_100m",
    "tw_day_trade_multi_basis",
    "tw_day_trade_multi_basis_22",
    "tw_day_trade_multi_basis_projection_l1_gelu",
)
START_DATE = "2026-02-25"
DEPLOYMENT_SESSION_DATE = "2026-09-02"
LIVE_DIR = REPO_ROOT / "artifacts/live/tw_day_trade_simulation"
OPERATIONS_ROOT = (
    REPO_ROOT / "artifacts/operations/tw_day_trade_multi_basis_22_deployment"
)
ENTRY_BOOK_ROOT = OPERATIONS_ROOT / "entry_books"
STATUS_PATH = OPERATIONS_ROOT / "status.json"
PRIOR_HISTORY_RECEIPT = OPERATIONS_ROOT / "prior_history_import_receipt.json"
SERVICE = "stockagent-tw-day-trade-simulation.service"
DISCORD_SERVICE = "stockagent-discord-bot.service"
PUBLIC_SERVICE = "stockagent-public-dashboards.service"


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


def _terminal_mode(mode: Mapping[str, Any], *, session_date: str) -> bool:
    if str(mode.get("session_date") or "") != session_date:
        return False
    positions = mode.get("positions") or {}
    if not isinstance(positions, Mapping):
        return False
    if any(
        int(row.get("signed_shares") or 0) != 0
        for row in positions.values()
        if isinstance(row, Mapping)
    ):
        return False
    if int(mode.get("open_position_count") or 0) != 0:
        return False
    return bool(mode.get("closing_auction_settled_at")) or str(
        mode.get("engine_status") or ""
    ) == "terminal"


def _run(stage: str, command: Sequence[str], *, run_dir: Path) -> dict[str, Any]:
    stdout_path = run_dir / f"{stage}.stdout.log"
    stderr_path = run_dir / f"{stage}.stderr.log"
    started = datetime.now(TAIPEI)
    with stdout_path.open("w", encoding="utf-8") as stdout, stderr_path.open(
        "w", encoding="utf-8"
    ) as stderr:
        process = subprocess.run(
            list(command),
            cwd=REPO_ROOT,
            stdout=stdout,
            stderr=stderr,
            text=True,
            check=False,
        )
    result = {
        "stage": stage,
        "command": list(command),
        "started_at": started.isoformat(timespec="seconds"),
        "completed_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
        "returncode": process.returncode,
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
    }
    if process.returncode != 0:
        stderr_tail = stderr_path.read_text(encoding="utf-8", errors="replace")[-4000:]
        raise RuntimeError(f"{stage} failed rc={process.returncode}: {stderr_tail}")
    return result


def _wait_active(service: str, *, timeout_seconds: float = 60.0) -> None:
    deadline = wall_time.monotonic() + timeout_seconds
    while wall_time.monotonic() < deadline:
        result = subprocess.run(
            ["systemctl", "is-active", "--quiet", service], check=False
        )
        if result.returncode == 0:
            return
        wall_time.sleep(1.0)
    raise RuntimeError(f"service did not become active: {service}")


def _wait_engine_sync(*, timeout_seconds: float = 60.0) -> dict[str, Any]:
    deadline = wall_time.monotonic() + timeout_seconds
    while wall_time.monotonic() < deadline:
        try:
            sync = _object(LIVE_DIR / "service_sync.json")
        except (FileNotFoundError, json.JSONDecodeError, ValueError):
            wall_time.sleep(1.0)
            continue
        if (
            set(sync.get("enabled_markets") or ()) == set(MARKETS)
            and int(sync.get("mode_count") or 0) == len(MARKETS)
            and bool(sync.get("ledger_integrity_ready"))
        ):
            return sync
        wall_time.sleep(1.0)
    raise RuntimeError("four-mode engine synchronization did not become ready")


def _json_get(url: str, *, timeout: float = 180.0) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        if int(response.status) != 200:
            raise RuntimeError(f"GET {url} returned HTTP {response.status}")
        payload = json.load(response)
    if not isinstance(payload, dict):
        raise RuntimeError(f"GET {url} did not return an object")
    return payload


def _visible_history_dates(payload: Mapping[str, Any], market: str) -> list[str]:
    dates: set[str] = set()
    for row in payload.get("history") or ():
        if not isinstance(row, Mapping) or str(row.get("series_id") or "") != market:
            continue
        session_date = str(row.get("session_date") or "")[:10]
        if not session_date and row.get("minute"):
            try:
                minute = datetime.fromisoformat(str(row["minute"]))
                if minute.tzinfo is None:
                    minute = minute.replace(tzinfo=TAIPEI)
                session_date = minute.astimezone(TAIPEI).date().isoformat()
            except ValueError:
                session_date = ""
        if session_date:
            dates.add(session_date)
    return sorted(dates)


def _verify_dashboard_history(*, end_date: str) -> dict[str, Any]:
    query = f"range=all&start_date={START_DATE}&end_date={end_date}"
    results: dict[str, Any] = {}
    for name, url in {
        "local": f"http://127.0.0.1:8766/api/history?{query}",
        "public_gateway": f"http://127.0.0.1:8770/tw-day-trade/api/history?{query}",
    }.items():
        payload = _json_get(url)
        dates = _visible_history_dates(payload, "tw_day_trade_multi_basis_22")
        if not dates or dates[0] != START_DATE or dates[-1] != end_date:
            raise RuntimeError(
                f"{name} dashboard history does not expose Multi-Basis 22 "
                f"{START_DATE}..{end_date}: {dates[:1]}..{dates[-1:] if dates else []}"
            )
        results[name] = {
            "http_status": 200,
            "first_session_date": dates[0],
            "last_session_date": dates[-1],
        }
    signals = _json_get(
        "http://127.0.0.1:8770/tw-day-trade/api/signals?"
        f"mode=tw_day_trade_multi_basis_22&date={START_DATE}&offset=0&limit=1"
    )
    if int(signals.get("total") or 0) <= 0:
        raise RuntimeError("public dashboard does not expose Multi-Basis 22 signal detail")
    results["public_signal_detail"] = {
        "http_status": 200,
        "session_date": START_DATE,
        "total": int(signals.get("total") or 0),
    }
    return results


def main() -> None:
    OPERATIONS_ROOT.mkdir(parents=True, exist_ok=True)
    lock_path = OPERATIONS_ROOT / "deploy.lock"
    with lock_path.open("a+", encoding="utf-8") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("[multi-basis-22-deploy] another deployment run is active")
            return

        if STATUS_PATH.is_file():
            existing = _object(STATUS_PATH)
            if existing.get("status") == "complete":
                print(json.dumps(existing, ensure_ascii=False, indent=2, sort_keys=True))
                return

        observed = datetime.now(TAIPEI)
        observed_date = observed.date().isoformat()
        session_date = DEPLOYMENT_SESSION_DATE
        if observed_date > session_date:
            blocked = {
                "schema_version": 1,
                "status": "blocked_target_session_missed",
                "observed_at": observed.isoformat(timespec="seconds"),
                "session_date": session_date,
                "observed_session_date": observed_date,
                "reason": "refusing to replace a newer live session with the fixed deployment target",
            }
            _atomic_json(STATUS_PATH, blocked)
            print(json.dumps(blocked, ensure_ascii=False, indent=2, sort_keys=True))
            return
        if (
            observed_date < session_date
            or observed.timetz().replace(tzinfo=None) < time(13, 40)
        ):
            prior_history = (
                _object(PRIOR_HISTORY_RECEIPT)
                if PRIOR_HISTORY_RECEIPT.is_file()
                else None
            )
            waiting = {
                "schema_version": 1,
                "status": "waiting_market_close",
                "observed_at": observed.isoformat(timespec="seconds"),
                "session_date": session_date,
                "completed_history": prior_history,
                "retry_contract": "Mon..Fri systemd calendar",
            }
            _atomic_json(STATUS_PATH, waiting)
            print(json.dumps(waiting, ensure_ascii=False, indent=2, sort_keys=True))
            return

        state = _object(LIVE_DIR / "state.json")
        modes = state.get("modes") or {}
        if set(modes) != set(MARKETS):
            raise RuntimeError(f"live mode set mismatch: {sorted(modes)}")
        nonterminal = [
            market
            for market in MARKETS
            if not _terminal_mode(modes[market], session_date=session_date)
        ]
        if nonterminal:
            waiting = {
                "schema_version": 1,
                "status": "waiting_terminal_flat",
                "observed_at": observed.isoformat(timespec="seconds"),
                "session_date": session_date,
                "nonterminal_markets": nonterminal,
                "retry_contract": "Mon..Fri systemd calendar",
            }
            _atomic_json(STATUS_PATH, waiting)
            print(json.dumps(waiting, ensure_ascii=False, indent=2, sort_keys=True))
            return
        if len(list(ENTRY_BOOK_ROOT.glob("*.parquet"))) < 130:
            raise RuntimeError("retained entry-book deployment bundle is incomplete")

        run_id = observed.strftime("%Y%m%dT%H%M%S%z")
        run_dir = OPERATIONS_ROOT / "runs" / run_id
        run_dir.mkdir(parents=True, exist_ok=False)
        candidate = Path(
            tempfile.mkdtemp(
                prefix="tw_day_trade_multi_basis_22_postclose.",
                dir=LIVE_DIR.parent,
            )
        )
        stages: list[dict[str, Any]] = []
        running = {
            "schema_version": 1,
            "status": "running",
            "observed_at": observed.isoformat(timespec="seconds"),
            "session_date": session_date,
            "run_dir": str(run_dir),
            "candidate_dir": str(candidate),
            "stages": stages,
        }
        _atomic_json(STATUS_PATH, running)
        python = sys.executable
        try:
            stages.append(
                _run(
                    "replay",
                    [
                        python,
                        str(REPO_ROOT / "scripts/rebuild_tw_day_trade_open_price_replay.py"),
                        "--markets-dir",
                        str(REPO_ROOT / "services/discord_bot/markets"),
                        "--state-dir",
                        str(candidate),
                        "--start-date",
                        START_DATE,
                        "--end-date",
                        session_date,
                        "--benchmark-state-source",
                        str(LIVE_DIR / "state.json"),
                        "--source-ledger-dir",
                        str(LIVE_DIR),
                        "--allow-unpinned-market",
                        "tw_day_trade_multi_basis_22",
                        "--historical-book-root",
                        str(ENTRY_BOOK_ROOT),
                        "--reuse-retained-signal-open",
                        "--replay-intraday-kbars",
                    ],
                    run_dir=run_dir,
                )
            )
            minute_output = run_dir / "minute_curves"
            stages.append(
                _run(
                    "minute_curves",
                    [
                        python,
                        str(REPO_ROOT / "scripts/rebuild_tw_day_trade_minute_curves.py"),
                        "--state-dir",
                        str(candidate),
                        "--start-date",
                        START_DATE,
                        "--end-date",
                        session_date,
                        "--output-dir",
                        str(minute_output),
                        "--simulation",
                        "--fetch-missing-kbars",
                        "--validate-existing-strategy-marks",
                        "--publish",
                    ],
                    run_dir=run_dir,
                )
            )
            promote_base = [
                python,
                str(REPO_ROOT / "scripts/promote_tw_day_trade_replay.py"),
                "--live-dir",
                str(LIVE_DIR),
                "--candidate-dir",
                str(candidate),
            ]
            for market in MARKETS:
                promote_base.extend(("--expected-market", market))
            stages.append(
                _run("validate_promotion", [*promote_base, "--validate-only"], run_dir=run_dir)
            )

            stopped = False
            try:
                subprocess.run(["systemctl", "stop", SERVICE], check=True)
                stopped = True
                stages.append(_run("promote", promote_base, run_dir=run_dir))
            finally:
                if stopped or subprocess.run(
                    ["systemctl", "is-active", "--quiet", SERVICE], check=False
                ).returncode != 0:
                    subprocess.run(["systemctl", "start", SERVICE], check=True)
            _wait_active(SERVICE)
            sync = _wait_engine_sync()
            subprocess.run(["systemctl", "restart", DISCORD_SERVICE], check=True)
            _wait_active(DISCORD_SERVICE)
            subprocess.run(["systemctl", "restart", PUBLIC_SERVICE], check=True)
            _wait_active(PUBLIC_SERVICE)
            dashboard_acceptance = _verify_dashboard_history(end_date=session_date)
            complete = {
                "schema_version": 1,
                "status": "complete",
                "completed_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
                "session_date": session_date,
                "start_date": START_DATE,
                "markets": list(MARKETS),
                "candidate_became_rollback_dir": str(candidate),
                "run_dir": str(run_dir),
                "stages": stages,
                "engine_run_id": sync.get("engine_run_id"),
                "engine_content_revision": sync.get("content_revision"),
                "dashboard_acceptance": dashboard_acceptance,
                "discord_service_active": True,
                "simulation_only": True,
                "production_order_possible": False,
            }
            _atomic_json(STATUS_PATH, complete)
            print(json.dumps(complete, ensure_ascii=False, indent=2, sort_keys=True))
        except Exception as exc:
            failed = {
                **running,
                "status": "failed_retryable",
                "failed_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
                "error_type": type(exc).__name__,
                "error": str(exc),
                "stages": stages,
                "retry_contract": "Mon..Fri systemd calendar",
            }
            _atomic_json(STATUS_PATH, failed)
            raise


if __name__ == "__main__":
    main()

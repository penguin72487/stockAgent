#!/usr/bin/env python3
"""Extend and deploy TW overnight dashboard history through the latest close."""
from __future__ import annotations

import argparse
from datetime import date, datetime
import fcntl
import hashlib
import json
from pathlib import Path
import subprocess
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.artifact_io import atomic_write_json
from scripts.deploy_tw_overnight_history import deploy
from scripts.switch_tw_day_trade_strategy import _latest_completed_session
from stockagent.live.tw_day_trade_simulation import TAIPEI


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _deployment_current(state_dir: Path, end_date: str) -> bool:
    history_path = state_dir / "overnight_history.json"
    receipt_path = state_dir / "overnight_history_deployment.json"
    signal_path = state_dir / "overnight_signal_history.parquet"
    event_path = state_dir / "overnight_event_history.parquet"
    if not all(path.is_file() for path in (history_path, receipt_path, signal_path, event_path)):
        return False
    try:
        history = json.loads(history_path.read_text())
        receipt = json.loads(receipt_path.read_text())
    except (OSError, ValueError, json.JSONDecodeError):
        return False
    return bool(
        history.get("status") in {"ready", "ready_with_stale_unresolved_position"}
        and history.get("end_date") == end_date
        and receipt.get("end_date") == end_date
        and receipt.get("history_sha256") == _sha256(history_path)
        and receipt.get("signal_history_sha256") == _sha256(signal_path)
        and receipt.get("event_history_sha256") == _sha256(event_path)
    )


def maintain(args: argparse.Namespace) -> dict[str, object]:
    state_dir = args.state_dir.resolve()
    work_dir = args.work_dir.resolve()
    state_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)
    lock_path = work_dir / ".maintenance.lock"
    with lock_path.open("a+b") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("overnight history maintenance is already running") from exc

        latest, sessions, calendar_sha256 = _latest_completed_session(
            tw_public_dir=args.public_root,
            start_date=date.fromisoformat(args.start_date),
        )
        latest_text = latest.isoformat()
        if _deployment_current(state_dir, latest_text) and not args.force:
            return {
                "status": "already_current",
                "end_date": latest_text,
                "session_count": len(sessions),
            }

        command = [
            sys.executable,
            str(REPO_ROOT / "scripts/rebuild_tw_overnight_history.py"),
            "--start-date",
            args.start_date,
            "--end-date",
            latest_text,
            "--public-root",
            str(args.public_root),
            "--minute-root",
            str(args.minute_root),
            "--price-limit-dir",
            str(args.price_limit_dir),
            "--output",
            str(work_dir),
            "--stage",
            "all",
        ]
        subprocess.run(command, cwd=REPO_ROOT, check=True)
        receipt = deploy(work_dir, state_dir)
        maintenance = {
            "schema_version": 1,
            "status": "complete",
            "completed_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
            "start_date": args.start_date,
            "end_date": latest_text,
            "session_count": len(sessions),
            "calendar_sha256": calendar_sha256,
            "deployment": receipt,
        }
        output = REPO_ROOT / "artifacts/operations/tw_overnight_history_maintenance/latest.json"
        atomic_write_json(output, maintenance)
        return maintenance


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", default="2026-02-25")
    parser.add_argument(
        "--public-root",
        type=Path,
        default=Path("/srv/stockagent-live/data_tw_public"),
    )
    parser.add_argument(
        "--minute-root", type=Path, default=Path("data_tw_minute/research_dataset")
    )
    parser.add_argument(
        "--price-limit-dir", type=Path, default=Path("artifacts/live/tw_price_limits")
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=Path("artifacts/maintenance/tw_overnight_history"),
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=Path("artifacts/live/tw_overnight_simulation"),
    )
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    print(json.dumps(maintain(args), ensure_ascii=False))


if __name__ == "__main__":
    main()

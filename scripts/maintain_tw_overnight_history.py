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
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.artifact_io import atomic_write_json
from stockagent.live.market_config import load_market_configs


TAIPEI = ZoneInfo("Asia/Taipei")


def configured_history_lineage(markets_dir: Path, start_date: str) -> dict:
    from scripts.rebuild_tw_overnight_history import configured_history_lineage as build

    return build(markets_dir, start_date)


def _source_generation(plan: dict) -> str:
    from scripts.rebuild_tw_overnight_history import _source_generation as generate

    return generate(plan)


def _check_source_revision(plan: dict) -> object:
    from scripts.rebuild_tw_overnight_history import _check_source_revision as check

    return check(plan)


def _latest_completed_session(
    *, tw_public_dir: Path, start_date: date
) -> tuple[date, list[date], str]:
    from scripts.switch_tw_day_trade_strategy import _latest_completed_session as latest

    return latest(tw_public_dir=tw_public_dir, start_date=start_date)


def deploy(source: Path, state_dir: Path) -> dict:
    from scripts.deploy_tw_overnight_history import deploy as publish

    return publish(source, state_dir)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _deployment_current(
    state_dir: Path,
    end_date: str,
    lineage_fingerprint: str,
    price_limit_dir: Path | None = None,
) -> bool:
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
    if not (
        history.get("status") in {"ready", "ready_with_stale_unresolved_position"}
        and history.get("end_date") == end_date
        and receipt.get("end_date") == end_date
        and receipt.get("history_lineage_fingerprint") == lineage_fingerprint
    ):
        return False
    try:
        outputs_current = (
            receipt.get("history_sha256") == _sha256(history_path)
            and receipt.get("signal_history_sha256") == _sha256(signal_path)
            and receipt.get("event_history_sha256") == _sha256(event_path)
        )
    except OSError:
        return False
    if not outputs_current:
        return False
    # A published file can remain byte-identical after the official source is
    # revised in place. The completed date alone is not a freshness receipt.
    try:
        source = Path(receipt["source"])
        plan_path = source / "plan.json"
        if _sha256(plan_path) != receipt["source_plan_sha256"]:
            return False
        plan = json.loads(plan_path.read_text())
        if not plan.get("source_files"):
            return False
        _check_source_revision(plan)
        sessions = plan.get("sessions")
        if price_limit_dir is not None and (
            not isinstance(sessions, list)
            or not sessions
            or str(sessions[-1]) != end_date
        ):
            return False
        if price_limit_dir is not None and plan.get("price_limit_files"):
            if set(plan["price_limit_files"]) != set(sessions):
                return False
        # Legacy published plans predate the per-session price-limit inventory.
        # Verify their actual cached input receipts before allowing a no-op;
        # the next source revision will rebuild into a fully pinned generation.
        if price_limit_dir is not None and not plan.get("price_limit_files"):
            for day in sessions:
                prior = json.loads(
                    (source / "inputs" / str(day) / "source.json").read_text()
                )
                expected = (prior.get("source_hashes") or {}).get("price_limits")
                if not expected or _sha256(price_limit_dir / f"{day}.parquet") != expected:
                    return False
    except (OSError, KeyError, TypeError, ValueError, RuntimeError, json.JSONDecodeError):
        return False
    return True


def _verified_rebuild_output(
    lineage_dir: Path, end_date: str, lineage_fingerprint: str,
) -> Path:
    """Accept only the source-pinned generation selected by this rebuild."""

    resolved_receipt = json.loads((lineage_dir / "resolved_output.json").read_text())
    resolved_dir = Path(str(resolved_receipt["output"])).resolve(strict=True)
    generation_root = (lineage_dir / "source_generations").resolve()
    if resolved_dir.parent != generation_root:
        raise RuntimeError("overnight rebuild resolved outside source generations")
    plan = json.loads((resolved_dir / "plan.json").read_text())
    sessions = plan.get("sessions")
    if (
        not isinstance(sessions, list)
        or not sessions
        or str(sessions[-1]) != end_date
        or set(plan.get("price_limit_files") or {}) != set(sessions)
    ):
        raise RuntimeError("overnight rebuild lacks complete session source receipts")
    generation = _source_generation(plan)
    if not (
        resolved_dir.name == generation
        and plan.get("source_generation_sha256") == generation
        and resolved_receipt.get("source_generation_sha256") == generation
        and plan["history_lineage"]["fingerprint_sha256"] == lineage_fingerprint
        and resolved_receipt.get("history_lineage_fingerprint") == lineage_fingerprint
        and plan.get("end_date") == end_date
        and resolved_receipt.get("end_date") == end_date
    ):
        raise RuntimeError("overnight rebuild generation receipt does not match plan")
    _check_source_revision(plan)
    return resolved_dir


def maintain(args: argparse.Namespace) -> dict[str, object]:
    state_dir = args.state_dir.resolve()
    work_dir = args.work_dir.resolve()
    markets_dir = args.markets_dir.resolve()
    state_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)
    lock_path = work_dir / ".maintenance.lock"
    with lock_path.open("a+b") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("overnight history maintenance is already running") from exc

        enabled = [
            cfg
            for cfg in load_market_configs(markets_dir).values()
            if cfg.enabled and cfg.overnight_simulation_enabled
        ]
        if not enabled:
            idle = {
                "schema_version": 1,
                "status": "idle_no_enabled_modes",
                "observed_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
                "start_date": args.start_date,
                "enabled_market_count": 0,
            }
            atomic_write_json(
                REPO_ROOT
                / "artifacts/operations/tw_overnight_history_maintenance/latest_attempt.json",
                idle,
            )
            return idle
        lineage = configured_history_lineage(markets_dir, args.start_date)
        latest, sessions, calendar_sha256 = _latest_completed_session(
            tw_public_dir=args.public_root,
            start_date=date.fromisoformat(args.start_date),
        )
        latest_text = latest.isoformat()
        lineage_fingerprint = str(lineage["fingerprint_sha256"])
        lineage_dir = work_dir / "lineages" / lineage_fingerprint
        lineage_dir.mkdir(parents=True, exist_ok=True)
        if (
            _deployment_current(
                state_dir, latest_text, lineage_fingerprint, args.price_limit_dir
            )
            and not args.force
        ):
            return {
                "status": "already_current",
                "end_date": latest_text,
                "session_count": len(sessions),
                "history_lineage_fingerprint": lineage_fingerprint,
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
            "--markets-dir",
            str(markets_dir),
            "--output",
            str(lineage_dir),
            "--versioned-output",
            "--resolved-output-receipt",
            str(lineage_dir / "resolved_output.json"),
            "--stage",
            "all",
        ]
        subprocess.run(command, cwd=REPO_ROOT, check=True)
        resolved_dir = _verified_rebuild_output(
            lineage_dir, latest_text, lineage_fingerprint
        )
        receipt = deploy(resolved_dir, state_dir)
        maintenance = {
            "schema_version": 1,
            "status": "complete",
            "completed_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
            "start_date": args.start_date,
            "end_date": latest_text,
            "session_count": len(sessions),
            "calendar_sha256": calendar_sha256,
            "history_lineage_fingerprint": lineage_fingerprint,
            "lineage_work_dir": str(resolved_dir),
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
    parser.add_argument(
        "--markets-dir",
        type=Path,
        default=Path("services/discord_bot/markets"),
    )
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    print(json.dumps(maintain(args), ensure_ascii=False))


if __name__ == "__main__":
    main()

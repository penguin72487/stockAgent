#!/usr/bin/env python3
"""Atomically activate accepted live TW public data for opening workflows.

The local opening path never reads or materializes a packed snapshot.  Packed
publication remains an independent Syncthing cold-backup contract; this command
only selects the already accepted live source tree after rechecking freshness,
source-monitor health, and exact-session day-trade eligibility.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import fcntl
import json
import os
from pathlib import Path
import sys
import uuid
from typing import Any, Mapping
from zoneinfo import ZoneInfo


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.download_tw_public_data import DEFAULT_DATASETS  # noqa: E402
from scripts.run_tw_public_0830_check import (  # noqa: E402
    _event_monitor_errors,
    _publication_errors,
    _same_session_accepted,
)
from scripts.watch_tw_public_publication_group import (  # noqa: E402
    _latest_completed_taiex_session,
    _preopen_acceptance_errors,
)
from stockagent.live.tw_day_trade_simulation import (  # noqa: E402
    require_exact_session_eligibility,
)


TAIPEI = ZoneInfo("Asia/Taipei")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--live-root", type=Path, default=Path("/srv/stockagent-live/data_tw_public")
    )
    parser.add_argument("--link", type=Path, default=Path("data_tw_public"))
    parser.add_argument(
        "--publication-receipt",
        type=Path,
        default=Path(
            "artifacts/data_refresh/tw_public/publications/preopen_all/latest.json"
        ),
    )
    parser.add_argument(
        "--eligibility-receipt",
        type=Path,
        default=Path("artifacts/data_refresh/tw_day_trade_eligibility/latest.json"),
    )
    parser.add_argument(
        "--event-receipt",
        type=Path,
        default=Path("artifacts/data_refresh/tw_public/events/latest.json"),
    )
    parser.add_argument(
        "--receipt",
        type=Path,
        default=Path("artifacts/data_refresh/tw_public/opening_activation/latest.json"),
    )
    return parser.parse_args()


def _repo_path(path: Path) -> Path:
    return path if path.is_absolute() else REPO_ROOT / path


def _json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".tmp.{uuid.uuid4().hex}")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _atomic_symlink(target: Path, link: Path) -> None:
    link.parent.mkdir(parents=True, exist_ok=True)
    if link.exists() and not link.is_symlink():
        raise RuntimeError(f"refusing to replace non-symlink path: {link}")
    temporary = link.with_name(f".{link.name}.tmp.{uuid.uuid4().hex}")
    try:
        os.symlink(str(target), temporary)
        os.replace(temporary, link)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> int:
    args = parse_args()
    observed = datetime.now(TAIPEI)
    session_date = observed.date().isoformat()
    live_root = args.live_root.expanduser().resolve(strict=True)
    link = _repo_path(args.link).absolute()
    receipt_path = _repo_path(args.receipt)
    publication_path = _repo_path(args.publication_receipt)
    eligibility_path = _repo_path(args.eligibility_receipt)
    event_path = _repo_path(args.event_receipt)
    expected_latest = _latest_completed_taiex_session(live_root, observed=observed)

    publication = _json(publication_path)
    failures = [
        f"publication:{item}"
        for item in _publication_errors(
            publication,
            session_date=session_date,
            expected_latest=expected_latest,
        )
    ]
    live_summary = _json(live_root / "download_summary.json")
    failures.extend(
        f"live_summary:{item}"
        for item in _preopen_acceptance_errors(
            live_summary,
            expected_end_date=expected_latest,
            expected_dataset_count=len(DEFAULT_DATASETS),
        )
    )
    event_receipt = _json(event_path)
    failures.extend(
        f"event_monitor:{item}"
        for item in _event_monitor_errors(event_receipt, observed=observed)
    )

    eligibility_receipt = _json(eligibility_path)
    try:
        coverage = require_exact_session_eligibility(
            rule_data_dir=live_root,
            parquet_root=live_root / "stocks",
            trading_date=observed.date(),
        )
    except (OSError, RuntimeError, ValueError) as exc:
        coverage = {}
        failures.append(f"eligibility:{type(exc).__name__}: {exc}")
    eligibility = {
        "trading_date": session_date,
        "venues": coverage,
        "receipt": str(eligibility_path),
        "receipt_status": eligibility_receipt.get("status"),
        "receipt_trading_date": eligibility_receipt.get("trading_date"),
    }
    if not _same_session_accepted(
        {"same_session_eligibility": eligibility}, trading_date=session_date
    ):
        failures.append("eligibility:both exact-session venues are not covered")
    if (
        eligibility_receipt.get("status") != "ok"
        or eligibility_receipt.get("trading_date") != session_date
    ):
        failures.append("eligibility:publication watcher receipt is not current")

    activated = False
    activation_action: str | None = None
    if not failures:
        lock_path = live_root.parent / ".locks" / "tw-public-refresh.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a+", encoding="utf-8") as lock_handle:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_SH)
            # The live root is the mutable runtime boundary.  A verified packed
            # materialization is an immutable delivery/cache proof and must
            # never receive panel-cache or collector writes through this link.
            _atomic_symlink(live_root, link)
            activation_action = "linked_accepted_mutable_live_root"
            activated = link.resolve(strict=True) == live_root
        if not activated:
            failures.append("active_link:link target did not resolve to accepted live root")

    completed = datetime.now(TAIPEI)
    payload = {
        "schema_version": 1,
        "status": "ok" if not failures and activated else "failed",
        "started_at_taipei": observed.isoformat(),
        "completed_at_taipei": completed.isoformat(),
        "elapsed_seconds": (completed - observed).total_seconds(),
        "session_date": session_date,
        "expected_latest_date": expected_latest,
        "live_end_date": live_summary.get("end_date"),
        "live_root": str(live_root),
        "link": str(link),
        "active_target": str(link.resolve(strict=False)),
        "activated": activated,
        "activation_action": activation_action,
        "failures": failures,
        "source_count": len(DEFAULT_DATASETS),
        "event_monitor_updated_at": event_receipt.get("updated_at_taipei"),
        "same_session_eligibility": eligibility,
        "simulation_only": True,
        "production_order_possible": False,
    }
    _atomic_json(receipt_path, payload)
    run_path = receipt_path.parent / "runs" / (
        observed.strftime("%Y%m%dT%H%M%S%f") + ".json"
    )
    _atomic_json(run_path, payload)
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if payload["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())

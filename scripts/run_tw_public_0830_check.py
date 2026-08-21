#!/usr/bin/env python3
"""Accept, publish, materialize, and receipt TW public data by 08:30."""

from __future__ import annotations

import argparse
from datetime import datetime
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid
from typing import Any, Mapping
from zoneinfo import ZoneInfo


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.download_tw_public_data import DEFAULT_DATASETS  # noqa: E402
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
        "--config",
        type=Path,
        default=Path(
            "configs/deployments/"
            "tw_day_trade_multi_basis_projection_l1_gelu_fold11.yaml"
        ),
    )
    parser.add_argument(
        "--live-root", type=Path, default=Path("/srv/stockagent-live/data_tw_public")
    )
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
        default=Path("artifacts/data_refresh/tw_public/0830/latest.json"),
    )
    parser.add_argument(
        "--audit-root",
        type=Path,
        default=Path("artifacts/data_refresh/tw_public/0830/audit"),
    )
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def _repo_path(path: Path) -> Path:
    return path if path.is_absolute() else REPO_ROOT / path


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".tmp.{uuid.uuid4().hex}")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def refresh_command(_config: Path, *, force: bool = False) -> list[str]:
    command = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "watch_tw_public_publication_group.py"),
        "--phase",
        "preopen_all",
    ]
    if force:
        command.extend(["--auto-window-minutes", "1440"])
    return command


def _publication_errors(
    receipt: Mapping[str, Any],
    *,
    session_date: str,
    expected_latest: str,
) -> list[str]:
    failures: list[str] = []
    if receipt.get("status") != "ok":
        failures.append("preopen publication status is not ok")
    if str(receipt.get("phase") or "") != "preopen_all":
        failures.append("preopen publication phase is not preopen_all")
    if str(receipt.get("started_at_taipei") or "")[:10] != session_date:
        failures.append("preopen publication is not from this Taipei session")
    if int(receipt.get("selected_dataset_count") or -1) != len(DEFAULT_DATASETS):
        failures.append("preopen publication does not cover all registered datasets")
    failures.extend(
        _preopen_acceptance_errors(
            receipt.get("download_summary"),
            expected_end_date=expected_latest,
            expected_dataset_count=len(DEFAULT_DATASETS),
        )
    )
    promoted = receipt.get("promoted_live_metadata")
    if not isinstance(promoted, list) or len(promoted) != 3:
        failures.append("accepted preopen metadata was not promoted to the live tree")
    return failures


def _event_monitor_errors(
    receipt: Mapping[str, Any], *, observed: datetime
) -> list[str]:
    failures: list[str] = []
    try:
        updated = datetime.fromisoformat(str(receipt.get("updated_at_taipei")))
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=TAIPEI)
        age = (observed - updated.astimezone(TAIPEI)).total_seconds()
    except (TypeError, ValueError):
        age = float("inf")
    expected = len(DEFAULT_DATASETS)
    checks = {
        "status": receipt.get("status") == "ok",
        "coverage_complete": receipt.get("coverage_complete") is True,
        "registered_dataset_count": int(
            receipt.get("registered_dataset_count") or -1
        )
        == expected,
        "monitored_dataset_count": int(receipt.get("monitored_dataset_count") or -1)
        == expected,
        "observed_dataset_count": int(receipt.get("observed_dataset_count") or -1)
        == expected,
        "failed_probe_count": int(receipt.get("failed_probe_count") or 0) == 0,
        "unapplied_event_count": int(receipt.get("unapplied_event_count") or 0)
        == 0,
        "heartbeat_age_seconds": -60.0 <= age <= 180.0,
    }
    failures.extend(name for name, passed in checks.items() if not passed)
    return failures


def _same_session_accepted(
    snapshot: Mapping[str, Any] | None, *, trading_date: str
) -> bool:
    if not snapshot:
        return False
    same_session = snapshot.get("same_session_eligibility")
    if not isinstance(same_session, Mapping):
        return False
    if str(same_session.get("trading_date") or "") != trading_date:
        return False
    venues = same_session.get("venues")
    if not isinstance(venues, Mapping) or set(venues) != {"twse", "tpex"}:
        return False
    return all(
        isinstance(row, Mapping)
        and row.get("covered") is True
        and str(row.get("target_date") or "") == trading_date
        for row in venues.values()
    )


def _run_json(command: list[str], *, timeout: float | None = None) -> dict[str, Any]:
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if completed.returncode != 0:
        tail = (completed.stderr or completed.stdout).strip()[-2000:]
        raise RuntimeError(
            f"command failed rc={completed.returncode}: {command!r}; tail={tail}"
        )
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"command did not return JSON: {command!r}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"command JSON is not an object: {command!r}")
    return payload


def _audit_command(
    *, config: Path, live_root: Path, output_dir: Path
) -> list[str]:
    return [
        sys.executable,
        str(REPO_ROOT / "scripts" / "audit_tw_public_data_layer.py"),
        "--config",
        str(config),
        "--parquet-root",
        str(live_root / "stocks"),
        "--public-dir",
        str(live_root),
        "--public-feature-path",
        str(live_root / "features" / "tw_public_stock_daily.parquet"),
        "--output-dir",
        str(output_dir),
        "--build-panel",
        "--strict",
        "--require-live-selected-features",
    ]


def main() -> int:
    args = parse_args()
    started = datetime.now(TAIPEI)
    session_date = started.date().isoformat()
    config = _repo_path(args.config).resolve(strict=True)
    live_root = args.live_root.expanduser().resolve(strict=True)
    receipt_path = _repo_path(args.receipt).resolve(strict=False)
    publication_path = _repo_path(args.publication_receipt).resolve(strict=False)
    eligibility_path = _repo_path(args.eligibility_receipt).resolve(strict=False)
    event_path = _repo_path(args.event_receipt).resolve(strict=False)
    audit_dir = _repo_path(args.audit_root) / started.strftime("%Y%m%dT%H%M%S%f")
    expected_latest = _latest_completed_taiex_session(live_root, observed=started)
    steps: list[dict[str, Any]] = []

    publication = _json(publication_path)
    publication_failures = _publication_errors(
        publication,
        session_date=session_date,
        expected_latest=expected_latest,
    )
    if args.force or publication_failures:
        command = refresh_command(config, force=bool(args.force))
        completed = subprocess.run(command, cwd=REPO_ROOT, check=False)
        steps.append({"step": "preopen_full_refresh", "return_code": completed.returncode})
        publication = _json(publication_path)
        publication_failures = _publication_errors(
            publication,
            session_date=session_date,
            expected_latest=expected_latest,
        )

    failures = [f"publication:{item}" for item in publication_failures]
    event_receipt = _json(event_path)
    failures.extend(
        f"event_monitor:{item}"
        for item in _event_monitor_errors(event_receipt, observed=datetime.now(TAIPEI))
    )

    eligibility_receipt = _json(eligibility_path)
    try:
        coverage = require_exact_session_eligibility(
            rule_data_dir=live_root,
            parquet_root=live_root / "stocks",
            trading_date=started.date(),
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

    publish: dict[str, Any] = {}
    materialized: dict[str, Any] = {}
    audit: dict[str, Any] = {}
    lock_path = live_root.parent / ".locks" / "tw-public-refresh.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    if not failures:
        with lock_path.open("a+", encoding="utf-8") as lock_handle:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
            audit_started = time.perf_counter()
            completed = subprocess.run(
                _audit_command(config=config, live_root=live_root, output_dir=audit_dir),
                cwd=REPO_ROOT,
                check=False,
            )
            audit = _json(audit_dir / "summary.json")
            steps.append(
                {
                    "step": "strict_model_safety_audit",
                    "return_code": completed.returncode,
                    "elapsed_seconds": round(time.perf_counter() - audit_started, 3),
                    "summary": str(audit_dir / "summary.json"),
                }
            )
            if completed.returncode != 0 or audit.get("model_safe") is not True:
                failures.append("audit:strict model-safety audit did not pass")
            else:
                publish = _run_json(
                    [
                        str(REPO_ROOT / "scripts" / "run_data_cache.sh"),
                        "publish",
                        "tw-public",
                    ],
                    timeout=7200,
                )
                materialized = _run_json(
                    [
                        str(REPO_ROOT / "scripts" / "run_data_cache.sh"),
                        "use",
                        "tw-public",
                        "--verify",
                    ],
                    timeout=7200,
                )
                temporary_link = REPO_ROOT / (
                    f".data_tw_public.live.{uuid.uuid4().hex}"
                )
                try:
                    temporary_link.symlink_to(live_root)
                    os.replace(temporary_link, REPO_ROOT / "data_tw_public")
                finally:
                    temporary_link.unlink(missing_ok=True)
                steps.extend(
                    [
                        {"step": "packed_publish", "status": "ok"},
                        {"step": "materialize_verify", "status": "ok"},
                        {"step": "activate_mutable_live_root", "status": "ok"},
                    ]
                )

    active_target: str | None = None
    active_summary: dict[str, Any] = {}
    try:
        active_target = str((REPO_ROOT / "data_tw_public").resolve(strict=True))
        active_summary = _json(REPO_ROOT / "data_tw_public" / "download_summary.json")
    except OSError as exc:
        failures.append(f"active_link:{type(exc).__name__}: {exc}")
    active_ready = bool(
        active_summary.get("end_date") == expected_latest
        and active_summary.get("coverage_complete") is True
        and int(active_summary.get("failed_count") or 0) == 0
        and active_target == str(live_root)
        and Path(str(materialized.get("target") or "")).is_dir()
        and materialized.get("verification") == "full"
        and materialized.get("inventory_sha256")
        == (
            (publish.get("published") or [{}])[0].get("inventory_sha256")
            if isinstance(publish.get("published"), list)
            and publish.get("published")
            else None
        )
    )
    if not active_ready:
        failures.append("active_link:materialized dataset is not the accepted release")

    # The strict audit plus packed verification can take minutes.  Reload the
    # heartbeat at the acceptance boundary so the published receipt describes
    # current monitor state instead of the pre-audit snapshot.
    event_receipt = _json(event_path)
    completed_at = datetime.now(TAIPEI)
    failures.extend(
        f"event_monitor_final:{item}"
        for item in _event_monitor_errors(event_receipt, observed=completed_at)
    )
    accepted = not failures
    snapshot = {
        "status": "ok" if accepted else "failed",
        "expected_latest_date": expected_latest,
        "download_end_date": active_summary.get("end_date"),
        "coverage_complete": active_summary.get("coverage_complete"),
        "snapshot_id": materialized.get("snapshot_id"),
        "manifest_sha256": materialized.get("manifest_sha256"),
        "materialized_path": active_target,
        "verified_packed_path": materialized.get("target"),
        "same_session_eligibility": eligibility,
    }
    payload: dict[str, Any] = {
        "schema_version": 2,
        "status": "ok" if accepted else "failed",
        "started_at_taipei": started.isoformat(),
        "completed_at_taipei": completed_at.isoformat(),
        "elapsed_seconds": (completed_at - started).total_seconds(),
        "deadline_taipei": f"{session_date}T08:30:00+08:00",
        "completed_before_deadline": completed_at.hour < 8
        or (completed_at.hour == 8 and completed_at.minute < 30),
        "expected_latest_date": expected_latest,
        "failures": failures,
        "steps": steps,
        "publication": publication,
        "event_monitor": event_receipt,
        "eligibility_receipt": eligibility_receipt,
        "audit": audit,
        "packed_publish": publish,
        "materialized": materialized,
        "snapshot": snapshot,
        "acceptance": {
            "subprocess_ok": accepted,
            "snapshot_receipt_fresh": accepted,
            "snapshot_status_ok": accepted,
            "coverage_complete": active_summary.get("coverage_complete") is True,
            "same_session_eligibility": _same_session_accepted(
                snapshot, trading_date=session_date
            ),
            "all_156_sources": not publication_failures,
            "source_event_monitor": not _event_monitor_errors(
                event_receipt, observed=completed_at
            ),
            "strict_model_safety_audit": audit.get("model_safe") is True,
            "packed_release_verified": active_ready,
        },
    }
    _atomic_json(receipt_path, payload)
    run_path = receipt_path.parent / "runs" / (
        started.strftime("%Y%m%dT%H%M%S%f") + ".json"
    )
    _atomic_json(run_path, payload)
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if accepted else 1


if __name__ == "__main__":
    raise SystemExit(main())

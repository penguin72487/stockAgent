#!/usr/bin/env python3
"""Accept, publish, materialize, and receipt TW public data by 08:30."""

from __future__ import annotations

import argparse
from datetime import datetime
import fcntl
import hashlib
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


def _derived_data_commands(
    *, live_root: Path, expected_latest: str, workers: int = 8
) -> list[list[str]]:
    """Build every dated derived layer consumed by live model inference."""

    stock_root = live_root / "stocks"
    public_feature_path = live_root / "features" / "tw_public_stock_daily.parquet"
    return [
        [
            sys.executable,
            str(
                REPO_ROOT
                / "downloader"
                / "download_tw_corporate_action_reference.py"
            ),
            "--output-dir",
            str(live_root),
            "--mode",
            "daily",
            "--start-year",
            "2000",
            "--end-date",
            expected_latest,
            "--workers",
            str(max(1, int(workers))),
            "--timeout",
            "20",
            "--retries",
            "2",
        ],
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "build_tw_official_symbol_parquets.py"),
            "--input-dir",
            str(live_root),
            "--output-dir",
            str(stock_root),
            "--end-date",
            expected_latest,
            "--workers",
            str(max(1, int(workers))),
            "--allow-daily-publication-lag",
        ],
        [
            sys.executable,
            str(
                REPO_ROOT
                / "downloader"
                / "download_tw_corporate_action_entitlements.py"
            ),
            "--output-dir",
            str(live_root),
            "--reference",
            str(live_root / "tw_corporate_action_reference.parquet"),
            "--universe-report",
            str(stock_root / "official_symbol_build_report.csv"),
            "--start-date",
            "2014-01-01",
            "--end-date",
            expected_latest,
            "--mode",
            "daily",
            "--timeout",
            "20",
            "--retries",
            "2",
            "--workers",
            str(max(1, int(workers))),
        ],
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "build_tw_public_training_features.py"),
            "--input-dir",
            str(live_root),
            "--output-path",
            str(public_feature_path),
            "--symbols-root",
            str(stock_root),
            "--end-date",
            expected_latest,
            "--allow-daily-publication-lag",
        ],
    ]


def _derived_data_dates(live_root: Path) -> dict[str, str | None]:
    """Read effective dates without trusting file mtimes or old receipts."""

    try:
        import polars as pl
    except ImportError:
        return {
            "corporate_action_reference": None,
            "corporate_action_entitlements": None,
            "stock_panel": None,
            "public_features": None,
        }

    reference_summary = _json(live_root / "tw_corporate_action_reference.summary.json")
    entitlement_summary = _json(
        live_root / "tw_corporate_action_entitlements.summary.json"
    )
    dates: dict[str, str | None] = {
        "corporate_action_reference": str(reference_summary.get("end_date") or "")
        or None,
        "corporate_action_entitlements": str(
            entitlement_summary.get("coverage_end") or ""
        )
        or None,
    }
    for label, path in (
        ("stock_panel", live_root / "stocks" / "2330_features.parquet"),
        (
            "public_features",
            live_root / "features" / "tw_public_stock_daily.parquet",
        ),
    ):
        try:
            value = (
                pl.scan_parquet(path)
                .select(pl.col("date").cast(pl.Date, strict=False).max())
                .collect()
                .item()
            )
        except Exception:
            value = None
        dates[label] = value.isoformat() if value is not None else None
    return dates


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _receipt_dependency_errors(
    summary_path: Path,
    *,
    live_root: Path,
    keys: tuple[str, ...],
) -> list[str]:
    """Verify recorded inputs, including same-date upstream revisions."""

    summary = _json(summary_path)
    if not summary:
        return [f"missing summary: {summary_path}"]
    errors: list[str] = []
    checked: set[Path] = set()
    for key in keys:
        payload = summary.get(key)
        if isinstance(payload, Mapping):
            receipts = [payload]
        elif isinstance(payload, list):
            receipts = [row for row in payload if isinstance(row, Mapping)]
        else:
            continue
        for receipt in receipts:
            raw_path = receipt.get("path") or receipt.get("name")
            expected_sha256 = str(receipt.get("sha256") or "")
            expected_size = receipt.get("size")
            if not raw_path or (not expected_sha256 and expected_size is None):
                continue
            path = Path(str(raw_path)).expanduser()
            if not path.is_absolute():
                path = live_root / path
            path = path.resolve(strict=False)
            if path in checked:
                continue
            checked.add(path)
            try:
                actual_size = path.stat().st_size
            except OSError:
                errors.append(f"{key}: missing dependency {path}")
                continue
            if expected_size is not None and actual_size != int(expected_size):
                errors.append(
                    f"{key}: size mismatch {path.name}: "
                    f"receipt={expected_size} current={actual_size}"
                )
                continue
            if expected_sha256:
                try:
                    actual_sha256 = _sha256_file(path)
                except OSError as exc:
                    errors.append(f"{key}: unreadable dependency {path}: {exc}")
                    continue
                if actual_sha256 != expected_sha256:
                    errors.append(f"{key}: sha256 mismatch {path.name}")
    return errors


def _taipei_receipt_date(value: Any) -> str | None:
    try:
        timestamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=TAIPEI)
    return timestamp.astimezone(TAIPEI).date().isoformat()


def _derived_data_status(
    live_root: Path,
    *,
    expected_latest: str,
    session_date: str,
) -> dict[str, Any]:
    """Resolve derived readiness from dates plus immutable input receipts."""

    dates = _derived_data_dates(live_root)
    reference_summary_path = live_root / "tw_corporate_action_reference.summary.json"
    entitlement_summary_path = (
        live_root / "tw_corporate_action_entitlements.summary.json"
    )
    stock_summary_path = live_root / "stocks" / "official_symbol_build_summary.json"
    feature_summary_path = (
        live_root / "features" / "tw_public_stock_daily.summary.json"
    )
    reference_summary = _json(reference_summary_path)
    entitlement_summary = _json(entitlement_summary_path)
    errors: dict[str, list[str]] = {
        "corporate_action_reference": [],
        "stock_panel": [],
        "corporate_action_entitlements": [],
        "public_features": [],
    }
    for label in errors:
        if dates.get(label) != expected_latest:
            errors[label].append(
                f"effective date {dates.get(label)!r} != {expected_latest}"
            )
    if (
        _taipei_receipt_date(reference_summary.get("generated_at_utc"))
        != session_date
    ):
        errors["corporate_action_reference"].append(
            "reference endpoint was not refreshed in this weekly session"
        )
    if reference_summary.get("coverage_complete") is not True or int(
        reference_summary.get("failure_count") or 0
    ):
        errors["corporate_action_reference"].append(
            "reference coverage receipt is incomplete"
        )
    errors["corporate_action_reference"].extend(
        _receipt_dependency_errors(
            reference_summary_path,
            live_root=live_root,
            keys=("source_receipts",),
        )
    )
    errors["stock_panel"].extend(
        _receipt_dependency_errors(
            stock_summary_path,
            live_root=live_root,
            keys=(
                "source_receipts",
                "fallback_source_receipts",
                "legacy_source_receipts",
                "lifecycle_source_receipts",
                "session_calendar_receipt",
                "session_calendar_summary_receipt",
            ),
        )
    )
    if (
        _taipei_receipt_date(entitlement_summary.get("generated_at_utc"))
        != session_date
    ):
        errors["corporate_action_entitlements"].append(
            "entitlement endpoints were not refreshed in this weekly session"
        )
    if entitlement_summary.get("coverage_complete") is not True or int(
        entitlement_summary.get("failure_count") or 0
    ):
        errors["corporate_action_entitlements"].append(
            "entitlement coverage receipt is incomplete"
        )
    errors["corporate_action_entitlements"].extend(
        _receipt_dependency_errors(
            entitlement_summary_path,
            live_root=live_root,
            keys=("reference_receipt", "universe_receipt", "raw_receipt_manifest"),
        )
    )
    errors["public_features"].extend(
        _receipt_dependency_errors(
            feature_summary_path,
            live_root=live_root,
            keys=("source_receipts",),
        )
    )
    return {
        "dates": dates,
        "errors": errors,
        "current": all(not rows for rows in errors.values()),
    }


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
    derived_status_before: dict[str, Any] = {}
    derived_status_after: dict[str, Any] = {}
    lock_path = live_root.parent / ".locks" / "tw-public-refresh.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    if not failures:
        with lock_path.open("a+", encoding="utf-8") as lock_handle:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
            derived_status_before = _derived_data_status(
                live_root,
                expected_latest=expected_latest,
                session_date=session_date,
            )
            step_names = (
                "refresh_corporate_action_reference",
                "build_official_symbol_panel",
                "refresh_corporate_action_entitlements",
                "build_public_feature_panel",
            )
            status_labels = (
                "corporate_action_reference",
                "stock_panel",
                "corporate_action_entitlements",
                "public_features",
            )
            commands = _derived_data_commands(
                live_root=live_root,
                expected_latest=expected_latest,
            )
            for step_name, status_label, command in zip(
                step_names, status_labels, commands, strict=True
            ):
                current_status = _derived_data_status(
                    live_root,
                    expected_latest=expected_latest,
                    session_date=session_date,
                )
                if not current_status["errors"][status_label]:
                    continue
                derived_started = time.perf_counter()
                completed = subprocess.run(command, cwd=REPO_ROOT, check=False)
                steps.append(
                    {
                        "step": step_name,
                        "return_code": completed.returncode,
                        "elapsed_seconds": round(
                            time.perf_counter() - derived_started, 3
                        ),
                        "trigger_errors": current_status["errors"][status_label],
                    }
                )
                if completed.returncode != 0:
                    failures.append(
                        f"derived_data:{step_name} failed rc={completed.returncode}"
                    )
                    break
            derived_status_after = _derived_data_status(
                live_root,
                expected_latest=expected_latest,
                session_date=session_date,
            )
            if not derived_status_after["current"]:
                failures.append(
                    "derived_data:effective dates or source receipts are stale: "
                    f"{derived_status_after['errors']}"
                )

            if not failures:
                audit_started = time.perf_counter()
                completed = subprocess.run(
                    _audit_command(
                        config=config,
                        live_root=live_root,
                        output_dir=audit_dir,
                    ),
                    cwd=REPO_ROOT,
                    check=False,
                )
                audit = _json(audit_dir / "summary.json")
                steps.append(
                    {
                        "step": "strict_model_safety_audit",
                        "return_code": completed.returncode,
                        "elapsed_seconds": round(
                            time.perf_counter() - audit_started, 3
                        ),
                        "summary": str(audit_dir / "summary.json"),
                    }
                )
                if completed.returncode != 0 or audit.get("model_safe") is not True:
                    failures.append("audit:strict model-safety audit did not pass")

            if not failures:
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
        "derived_data_status_before": derived_status_before,
        "derived_data_status_after": derived_status_after,
        "derived_data_dates": _derived_data_dates(live_root),
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

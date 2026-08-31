#!/usr/bin/env python3
"""Accept and receipt the mutable TW public live root by 08:30.

Opening inference reads the catalog-owned live root directly.  Downloaders and
builders atomically replace individual outputs in that tree; this gate must
never publish or materialize a packed release.  Packed publication is an
independent, post-open cold-backup workflow.
"""

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
from stockagent.live.tw_public_opening_revision import (  # noqa: E402
    create_opening_revision_freeze,
    opening_revision_gate_path,
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
    parser.add_argument(
        "--event-settle-seconds",
        type=float,
        default=70.0,
        help="wait for the source-event daemon to apply a just-observed revision",
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
    blocking_unapplied = (
        receipt.get("blocking_unapplied_event_count")
        if "blocking_unapplied_event_count" in receipt
        else receipt.get("unapplied_event_count")
    )
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
        "blocking_unapplied_event_count": int(blocking_unapplied or 0) == 0,
        "heartbeat_age_seconds": -60.0 <= age <= 180.0,
    }
    failures.extend(name for name, passed in checks.items() if not passed)
    return failures


def _wait_for_event_monitor(
    receipt_path: Path,
    *,
    timeout_seconds: float,
) -> tuple[dict[str, Any], list[str], float]:
    """Wait through the event daemon's bounded download-retry interval."""

    started = time.monotonic()
    deadline = started + max(0.0, float(timeout_seconds))
    while True:
        receipt = _json(receipt_path)
        errors = _event_monitor_errors(receipt, observed=datetime.now(TAIPEI))
        if not errors or time.monotonic() >= deadline:
            return receipt, errors, time.monotonic() - started
        time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))


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


def _derived_refresh_blockers(failures: list[str]) -> list[str]:
    """Eligibility gates activation, not completed-session derived warm-up."""

    return [
        failure
        for failure in failures
        if not str(failure).startswith("eligibility:")
    ]


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


def _audit_dependency_state(*, config: Path, live_root: Path) -> dict[str, Any]:
    """Fingerprint small authoritative receipts that define one full audit."""

    paths = (
        config,
        REPO_ROOT / "scripts" / "audit_tw_public_data_layer.py",
        REPO_ROOT / "scripts" / "build_tw_official_symbol_parquets.py",
        REPO_ROOT / "scripts" / "build_tw_public_training_features.py",
        REPO_ROOT / "stockagent" / "data" / "tw_public_features.py",
        live_root / "download_summary.json",
        live_root / "tw_corporate_action_reference.summary.json",
        live_root / "tw_corporate_action_entitlements.summary.json",
        live_root / "stocks" / "official_symbol_build_summary.json",
        live_root / "stocks" / "official_symbol_build_report.csv",
        live_root / "features" / "tw_public_stock_daily.summary.json",
    )
    receipts: list[dict[str, Any]] = []
    for path in paths:
        stat = path.stat()
        receipts.append(
            {
                "path": str(path),
                "size": int(stat.st_size),
                "sha256": _sha256_file(path),
            }
        )
    encoded = json.dumps(
        receipts,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return {
        "sha256": hashlib.sha256(encoded).hexdigest(),
        "receipts": receipts,
    }


def _reusable_audit(
    cache_path: Path,
    *,
    expected_latest: str,
    dependency_sha256: str,
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    receipt = _json(cache_path)
    if (
        receipt.get("status") != "ok"
        or receipt.get("expected_latest_date") != expected_latest
        or receipt.get("dependency_sha256") != dependency_sha256
        or receipt.get("model_safe") is not True
    ):
        return None
    summary_path = Path(str(receipt.get("audit_summary_path") or ""))
    if not summary_path.is_absolute():
        summary_path = REPO_ROOT / summary_path
    try:
        if _sha256_file(summary_path) != receipt.get("audit_summary_sha256"):
            return None
    except OSError:
        return None
    summary = _json(summary_path)
    if summary.get("model_safe") is not True:
        return None
    return summary, receipt


def _write_audit_cache(
    cache_path: Path,
    *,
    expected_latest: str,
    dependency_state: Mapping[str, Any],
    summary_path: Path,
    audit: Mapping[str, Any],
) -> None:
    payload = {
        "schema_version": 1,
        "status": "ok",
        "accepted_at_taipei": datetime.now(TAIPEI).isoformat(),
        "expected_latest_date": expected_latest,
        "dependency_sha256": dependency_state.get("sha256"),
        "dependencies": dependency_state.get("receipts"),
        "audit_summary_path": str(summary_path),
        "audit_summary_sha256": _sha256_file(summary_path),
        "model_safe": audit.get("model_safe") is True,
    }
    _atomic_json(cache_path, payload)


def _audit_revision_errors(
    *,
    derived_status: Mapping[str, Any],
    audited_dependency_state: Mapping[str, Any],
    final_dependency_state: Mapping[str, Any],
) -> list[str]:
    errors: list[str] = []
    if derived_status.get("current") is not True:
        errors.append("derived data changed or became stale after audit")
    audited = str(audited_dependency_state.get("sha256") or "")
    current = str(final_dependency_state.get("sha256") or "")
    if not audited or not current or audited != current:
        errors.append(
            "live-root dependency revision changed after audit; retry convergence"
        )
    return errors


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


def _atomic_symlink(target: Path, link: Path) -> None:
    """Atomically select the accepted mutable root without unpacking data."""

    link.parent.mkdir(parents=True, exist_ok=True)
    if link.exists() and not link.is_symlink():
        raise RuntimeError(f"refusing to replace non-symlink runtime path: {link}")
    temporary = link.with_name(f".{link.name}.tmp.{uuid.uuid4().hex}")
    try:
        temporary.symlink_to(target)
        os.replace(temporary, link)
    finally:
        temporary.unlink(missing_ok=True)


def _live_runtime_errors(
    *,
    live_root: Path,
    active_link: Path,
    active_summary: Mapping[str, Any],
    expected_latest: str,
    derived_status: Mapping[str, Any],
    audit: Mapping[str, Any],
) -> list[str]:
    """Validate the exact mutable source consumed by opening inference."""

    errors = _preopen_acceptance_errors(
        active_summary,
        expected_end_date=expected_latest,
        expected_dataset_count=len(DEFAULT_DATASETS),
    )
    try:
        active_target = active_link.resolve(strict=True)
    except OSError as exc:
        errors.append(f"runtime link is unreadable: {type(exc).__name__}: {exc}")
    else:
        if active_target != live_root:
            errors.append(
                f"runtime link targets {active_target}, expected mutable live root {live_root}"
            )
    if derived_status.get("current") is not True:
        errors.append("derived live-root data is not current")
    if audit.get("model_safe") is not True:
        errors.append("strict model-safety audit is not accepted")
    return errors


def main() -> int:
    args = parse_args()
    if args.event_settle_seconds < 0:
        raise ValueError("--event-settle-seconds must be non-negative")
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
    event_receipt, event_failures, event_wait_seconds = _wait_for_event_monitor(
        event_path,
        timeout_seconds=args.event_settle_seconds,
    )
    if event_wait_seconds >= 0.01:
        steps.append(
            {
                "step": "wait_for_source_event_apply",
                "elapsed_seconds": round(event_wait_seconds, 3),
            }
        )
    failures.extend(f"event_monitor:{item}" for item in event_failures)

    # Establish a race-free handoff from continuous ingestion to one stable
    # opening revision.  The source daemon takes this same gate before applying
    # an observed version.  Once acquired, all versions accepted before this
    # boundary are present; later observations remain visible but queued until
    # the bounded 09:05 lease expires.
    opening_gate_handle = None
    opening_revision_freeze: dict[str, Any] = {}
    if not failures:
        gate_path = opening_revision_gate_path(live_root)
        gate_path.parent.mkdir(parents=True, exist_ok=True)
        gate_started = time.perf_counter()
        opening_gate_handle = gate_path.open("a+", encoding="utf-8")
        fcntl.flock(opening_gate_handle.fileno(), fcntl.LOCK_EX)
        event_receipt = _json(event_path)
        gated_event_failures = _event_monitor_errors(
            event_receipt, observed=datetime.now(TAIPEI)
        )
        if gated_event_failures:
            failures.extend(
                f"event_monitor_handoff:{item}"
                for item in gated_event_failures
            )
            fcntl.flock(opening_gate_handle.fileno(), fcntl.LOCK_UN)
            opening_gate_handle.close()
            opening_gate_handle = None
        else:
            opening_revision_freeze = create_opening_revision_freeze(
                live_root,
                observed=datetime.now(TAIPEI),
                owner={"service": "tw-public-0830-check", "pid": os.getpid()},
            )
            steps.append(
                {
                    "step": "freeze_opening_source_revision",
                    "status": "ok",
                    "lock_wait_seconds": round(
                        time.perf_counter() - gate_started, 3
                    ),
                    "frozen_at_taipei": opening_revision_freeze.get(
                        "frozen_at_taipei"
                    ),
                    "defer_apply_until_taipei": opening_revision_freeze.get(
                        "defer_apply_until_taipei"
                    ),
                }
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

    audit: dict[str, Any] = {}
    audit_dependency_state: dict[str, Any] = {}
    audit_cache_receipt: dict[str, Any] = {}
    final_derived_data_status: dict[str, Any] = {}
    final_audit_dependency_state: dict[str, Any] = {}
    derived_status_before: dict[str, Any] = {}
    derived_status_after: dict[str, Any] = {}
    lock_path = live_root.parent / ".locks" / "tw-public-refresh.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    if not _derived_refresh_blockers(failures):
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

            if not _derived_refresh_blockers(failures):
                audit_started = time.perf_counter()
                audit_dependency_state = _audit_dependency_state(
                    config=config,
                    live_root=live_root,
                )
                audit_cache_path = _repo_path(args.audit_root) / "latest.json"
                reusable = _reusable_audit(
                    audit_cache_path,
                    expected_latest=expected_latest,
                    dependency_sha256=str(audit_dependency_state["sha256"]),
                )
                if reusable is not None:
                    audit, audit_cache_receipt = reusable
                    steps.append(
                        {
                            "step": "strict_model_safety_audit_reused",
                            "status": "ok",
                            "elapsed_seconds": round(
                                time.perf_counter() - audit_started, 3
                            ),
                            "summary": audit_cache_receipt.get(
                                "audit_summary_path"
                            ),
                            "dependency_sha256": audit_dependency_state["sha256"],
                        }
                    )
                else:
                    completed = subprocess.run(
                        _audit_command(
                            config=config,
                            live_root=live_root,
                            output_dir=audit_dir,
                        ),
                        cwd=REPO_ROOT,
                        check=False,
                    )
                    audit_summary_path = audit_dir / "summary.json"
                    audit = _json(audit_summary_path)
                    steps.append(
                        {
                            "step": "strict_model_safety_audit",
                            "return_code": completed.returncode,
                            "elapsed_seconds": round(
                                time.perf_counter() - audit_started, 3
                            ),
                            "summary": str(audit_summary_path),
                            "dependency_sha256": audit_dependency_state["sha256"],
                        }
                    )
                    if completed.returncode == 0 and audit.get("model_safe") is True:
                        _write_audit_cache(
                            audit_cache_path,
                            expected_latest=expected_latest,
                            dependency_state=audit_dependency_state,
                            summary_path=audit_summary_path,
                            audit=audit,
                        )
                        audit_cache_receipt = _json(audit_cache_path)
                    else:
                        failures.append(
                            "audit:strict model-safety audit did not pass"
                        )

            if not failures:
                _atomic_symlink(live_root, REPO_ROOT / "data_tw_public")
                steps.append(
                    {
                        "step": "activate_mutable_live_root",
                        "status": "ok",
                        "target": str(live_root),
                    }
                )

    active_target: str | None = None
    active_summary: dict[str, Any] = {}
    try:
        active_target = str((REPO_ROOT / "data_tw_public").resolve(strict=True))
        active_summary = _json(REPO_ROOT / "data_tw_public" / "download_summary.json")
    except OSError as exc:
        failures.append(f"active_link:{type(exc).__name__}: {exc}")
    runtime_failures = _live_runtime_errors(
        live_root=live_root,
        active_link=REPO_ROOT / "data_tw_public",
        active_summary=active_summary,
        expected_latest=expected_latest,
        derived_status=derived_status_after,
        audit=audit,
    )
    failures.extend(f"live_runtime:{item}" for item in runtime_failures)
    active_ready = not runtime_failures

    # Reload the heartbeat at the acceptance boundary so the receipt describes
    # current monitor state instead of the pre-audit snapshot.
    if opening_gate_handle is not None:
        event_receipt = _json(event_path)
        final_event_failures = _event_monitor_errors(
            event_receipt, observed=datetime.now(TAIPEI)
        )
        final_event_wait_seconds = 0.0
    else:
        event_receipt, final_event_failures, final_event_wait_seconds = (
            _wait_for_event_monitor(
                event_path,
                timeout_seconds=args.event_settle_seconds,
            )
        )
    if final_event_wait_seconds >= 0.01:
        steps.append(
            {
                "step": "wait_for_final_source_event_apply",
                "elapsed_seconds": round(final_event_wait_seconds, 3),
            }
        )
    # Freeze the accepted dependency revision through the final checks and
    # receipt commit.  The source-event daemon uses this same exclusive lock;
    # without this second barrier it can apply a queued event in the gap after
    # the expensive audit and force an avoidable retry storm.
    final_lock_started = time.perf_counter()
    final_lock_handle = lock_path.open("a+", encoding="utf-8")
    fcntl.flock(final_lock_handle.fileno(), fcntl.LOCK_EX)
    steps.append(
        {
            "step": "freeze_final_acceptance_revision",
            "lock_wait_seconds": round(
                time.perf_counter() - final_lock_started, 3
            ),
        }
    )
    event_receipt = _json(event_path)
    final_event_failures = _event_monitor_errors(
        event_receipt, observed=datetime.now(TAIPEI)
    )
    completed_at = datetime.now(TAIPEI)
    failures.extend(
        f"event_monitor_final:{item}" for item in final_event_failures
    )
    # An event can be observed while the exclusive audit lock is held and then
    # be applied immediately after release.  Never publish a stale ready receipt
    # for that race: the bounded service retry rebuilds the new revision.
    final_derived_data_status = _derived_data_status(
        live_root,
        expected_latest=expected_latest,
        session_date=session_date,
    )
    try:
        final_audit_dependency_state = _audit_dependency_state(
            config=config,
            live_root=live_root,
        )
    except OSError as exc:
        failures.append(f"audit_revision_final:{type(exc).__name__}: {exc}")
    else:
        failures.extend(
            f"audit_revision_final:{item}"
            for item in _audit_revision_errors(
                derived_status=final_derived_data_status,
                audited_dependency_state=audit_dependency_state,
                final_dependency_state=final_audit_dependency_state,
            )
        )
    final_runtime_failures = _live_runtime_errors(
        live_root=live_root,
        active_link=REPO_ROOT / "data_tw_public",
        active_summary=active_summary,
        expected_latest=expected_latest,
        derived_status=final_derived_data_status,
        audit=audit,
    )
    failures.extend(
        f"live_runtime_final:{item}" for item in final_runtime_failures
    )
    active_ready = active_ready and not final_runtime_failures and not any(
        item.startswith("audit_revision_final:") for item in failures
    )
    accepted = not failures
    live_runtime = {
        "status": "ok" if accepted else "failed",
        "authority": "catalog_mutable_live_root",
        "expected_latest_date": expected_latest,
        "download_end_date": active_summary.get("end_date"),
        "coverage_complete": active_summary.get("coverage_complete"),
        "live_root": str(live_root),
        "active_link": str(REPO_ROOT / "data_tw_public"),
        "active_target": active_target,
        "atomic_file_replacement": True,
        "packed_release_required_for_opening": False,
        "materialization_required_for_opening": False,
        "same_session_eligibility": eligibility,
    }
    payload: dict[str, Any] = {
        "schema_version": 3,
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
        "opening_revision_freeze": opening_revision_freeze,
        "eligibility_receipt": eligibility_receipt,
        "audit": audit,
        "audit_dependency_state": audit_dependency_state,
        "final_audit_dependency_state": final_audit_dependency_state,
        "audit_cache_receipt": audit_cache_receipt,
        "derived_data_status_before": derived_status_before,
        "derived_data_status_after": derived_status_after,
        "derived_data_status_final": final_derived_data_status,
        "derived_data_dates": _derived_data_dates(live_root),
        "live_runtime": live_runtime,
        "cold_backup": {
            "opening_dependency": False,
            "status": "deferred_to_background_timer",
        },
        "acceptance": {
            "subprocess_ok": accepted,
            "live_root_receipt_fresh": accepted,
            "live_root_status_ok": accepted,
            "live_root_verified": active_ready,
            "coverage_complete": active_summary.get("coverage_complete") is True,
            "same_session_eligibility": _same_session_accepted(
                live_runtime, trading_date=session_date
            ),
            "all_156_sources": not publication_failures,
            "source_event_monitor": not _event_monitor_errors(
                event_receipt, observed=completed_at
            ),
            "strict_model_safety_audit": audit.get("model_safe") is True,
            "runtime_materialization_required": False,
            "runtime_materialized_snapshot": False,
        },
    }
    _atomic_json(receipt_path, payload)
    run_path = receipt_path.parent / "runs" / (
        started.strftime("%Y%m%dT%H%M%S%f") + ".json"
    )
    _atomic_json(run_path, payload)
    fcntl.flock(final_lock_handle.fileno(), fcntl.LOCK_UN)
    final_lock_handle.close()
    if opening_gate_handle is not None:
        fcntl.flock(opening_gate_handle.fileno(), fcntl.LOCK_UN)
        opening_gate_handle.close()
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if accepted else 1


if __name__ == "__main__":
    raise SystemExit(main())

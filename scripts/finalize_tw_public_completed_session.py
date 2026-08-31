#!/usr/bin/env python3
"""Finalize the latest completed TW session without opening-market activation.

This path is intentionally separate from the 08:30/09:00 opening gate.  It
accepts an official TWSE/TPEx close publication, atomically rebuilds the
canonical per-symbol panel and public feature table in the mutable live root,
and receipts the exact completed session.  It never requires next-session
day-trade eligibility, MIS opening quotes, or a packed-data materialization.
"""

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

from scripts.run_tw_public_0830_check import (  # noqa: E402
    _derived_data_commands,
    _derived_data_status,
    _receipt_dependency_errors,
)
from scripts.watch_tw_public_publication_group import (  # noqa: E402
    _latest_completed_taiex_session,
)


TAIPEI = ZoneInfo("Asia/Taipei")
CLOSE_PHASES = ("close_final", "close_revision", "close_initial")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--live-root",
        type=Path,
        default=Path("/srv/stockagent-live/data_tw_public"),
    )
    parser.add_argument(
        "--publication-root",
        type=Path,
        default=Path("artifacts/data_refresh/tw_public/publications"),
    )
    parser.add_argument(
        "--publication-phase",
        choices=CLOSE_PHASES,
        default=None,
        help="Require this exact close phase; otherwise use the newest accepted phase.",
    )
    parser.add_argument(
        "--receipt",
        type=Path,
        default=Path(
            "artifacts/data_refresh/tw_public/completed_session/latest.json"
        ),
    )
    parser.add_argument("--expected-date", default=None)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--force", action="store_true")
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


def _phase_errors(
    receipt: Mapping[str, Any],
    *,
    phase: str,
    expected_date: str,
) -> list[str]:
    errors: list[str] = []
    if receipt.get("status") != "ok":
        errors.append("status is not ok")
    if str(receipt.get("phase") or "") != phase:
        errors.append(f"phase is not {phase}")
    if str(receipt.get("started_at_taipei") or "")[:10] != expected_date:
        errors.append("receipt is not from the completed session date")
    summary = receipt.get("download_summary")
    if not isinstance(summary, Mapping):
        return [*errors, "download_summary is missing"]
    if str(summary.get("end_date") or "")[:10] != expected_date:
        errors.append("download end_date does not match completed session")
    if summary.get("daily_close_ready") is not True:
        errors.append("daily_close_ready is not true")
    if int(summary.get("blocking_failed_count") or 0) != 0:
        errors.append("blocking_failed_count is nonzero")
    if int(summary.get("incomplete_count") or 0) != 0:
        errors.append("incomplete_count is nonzero")
    selected = set(receipt.get("selected_datasets") or ())
    required = {"twse_daily_ohlcv", "tpex_daily_ohlcv"}
    if not required <= selected:
        errors.append("TWSE/TPEx official close datasets are not both selected")
    return errors


def _accepted_close_publication(
    publication_root: Path,
    *,
    expected_date: str,
    required_phase: str | None = None,
) -> tuple[str | None, dict[str, Any], dict[str, list[str]]]:
    phases = (required_phase,) if required_phase else CLOSE_PHASES
    failures: dict[str, list[str]] = {}
    for phase in phases:
        assert phase is not None
        receipt = _json(publication_root / phase / "latest.json")
        errors = _phase_errors(
            receipt,
            phase=phase,
            expected_date=expected_date,
        )
        if not errors:
            return phase, receipt, failures
        failures[phase] = errors
    return None, {}, failures


def _max_date(path: Path) -> str | None:
    try:
        import polars as pl

        value = (
            pl.scan_parquet(path)
            .select(pl.col("date").cast(pl.Date, strict=False).max())
            .collect()
            .item()
        )
    except Exception:
        return None
    return value.isoformat() if value is not None else None


def _derived_state(
    live_root: Path,
    *,
    expected_date: str,
    session_date: str | None = None,
) -> dict[str, Any]:
    close_dates = {
        "twse_close": _max_date(live_root / "twse_daily_ohlcv.parquet"),
        "tpex_close": _max_date(live_root / "tpex_daily_ohlcv.parquet"),
    }
    errors: list[str] = [
        f"{name}: effective date {value!r} != {expected_date}"
        for name, value in close_dates.items()
        if value != expected_date
    ]
    derived = _derived_data_status(
        live_root,
        expected_latest=expected_date,
        session_date=session_date or datetime.now(TAIPEI).date().isoformat(),
    )
    for name, rows in (derived.get("errors") or {}).items():
        errors.extend(f"{name}: {row}" for row in rows)
    dates = {**close_dates, **dict(derived.get("dates") or {})}
    if dates.get("stock_panel") == expected_date:
        errors.extend(
            _receipt_dependency_errors(
                live_root / "stocks" / "official_symbol_build_summary.json",
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
    if dates.get("public_features") == expected_date:
        errors.extend(
            _receipt_dependency_errors(
                live_root
                / "features"
                / "tw_public_stock_daily.summary.json",
                live_root=live_root,
                keys=("source_receipts",),
            )
        )
    return {
        "dates": dates,
        "derived_errors": dict(derived.get("errors") or {}),
        "errors": errors,
        "current": not errors,
    }


def _build_commands(
    *, live_root: Path, expected_date: str, workers: int
) -> list[list[str]]:
    return _derived_data_commands(
        live_root=live_root,
        expected_latest=expected_date,
        workers=workers,
    )


def main() -> int:
    args = parse_args()
    if args.workers <= 0:
        raise ValueError("--workers must be positive")
    started = datetime.now(TAIPEI)
    live_root = args.live_root.expanduser().resolve(strict=True)
    publication_root = _repo_path(args.publication_root).resolve(strict=False)
    receipt_path = _repo_path(args.receipt).resolve(strict=False)
    expected_date = str(
        args.expected_date
        or _latest_completed_taiex_session(live_root, observed=started)
    )
    phase, publication, phase_failures = _accepted_close_publication(
        publication_root,
        expected_date=expected_date,
        required_phase=args.publication_phase,
    )
    steps: list[dict[str, Any]] = []
    if phase is None:
        payload = {
            "schema_version": 1,
            "status": "waiting_source",
            "reason": "publication_pending",
            "expected_date": expected_date,
            "observed_at_taipei": started.isoformat(),
            "publication_failures": phase_failures,
            "live_root": str(live_root),
            "opening_activation_required": False,
            "next_session_eligibility_required": False,
        }
        _atomic_json(receipt_path, payload)
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 75

    lock_path = live_root.parent / ".locks" / "tw-public-refresh.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_started = time.perf_counter()
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        lock_wait_ms = (time.perf_counter() - lock_started) * 1000.0
        before = _derived_state(
            live_root,
            expected_date=expected_date,
            session_date=started.date().isoformat(),
        )
        if args.force or not before["current"]:
            for name, status_label, command in zip(
                (
                    "refresh_corporate_action_reference",
                    "build_official_symbol_panel",
                    "refresh_corporate_action_entitlements",
                    "build_public_feature_panel",
                ),
                (
                    "corporate_action_reference",
                    "stock_panel",
                    "corporate_action_entitlements",
                    "public_features",
                ),
                _build_commands(
                    live_root=live_root,
                    expected_date=expected_date,
                    workers=args.workers,
                ),
                strict=True,
            ):
                current = _derived_state(
                    live_root,
                    expected_date=expected_date,
                    session_date=started.date().isoformat(),
                )
                current_errors = current.get("derived_errors") or {}
                if not args.force and not current_errors.get(status_label):
                    continue
                step_started = time.perf_counter()
                completed = subprocess.run(command, cwd=REPO_ROOT, check=False)
                steps.append(
                    {
                        "step": name,
                        "return_code": int(completed.returncode),
                        "elapsed_seconds": round(
                            time.perf_counter() - step_started, 3
                        ),
                    }
                )
                if completed.returncode != 0:
                    break
        after = _derived_state(
            live_root,
            expected_date=expected_date,
            session_date=started.date().isoformat(),
        )

    status = "ok" if after["current"] and all(
        int(row["return_code"]) == 0 for row in steps
    ) else "failed"
    payload = {
        "schema_version": 1,
        "status": status,
        "expected_date": expected_date,
        "started_at_taipei": started.isoformat(),
        "completed_at_taipei": datetime.now(TAIPEI).isoformat(),
        "source_publication_phase": phase,
        "source_publication_receipt": str(
            publication_root / phase / "latest.json"
        ),
        "source_publication_completed_at_taipei": publication.get(
            "completed_at_taipei"
        ),
        "live_root": str(live_root),
        "lock_wait_ms": round(lock_wait_ms, 3),
        "before": before,
        "after": after,
        "steps": steps,
        "reused": not steps,
        "price_anchor": "official_session_close",
        "opening_activation_required": False,
        "next_session_eligibility_required": False,
        "packed_materialization_required": False,
    }
    _atomic_json(receipt_path, payload)
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if status == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())

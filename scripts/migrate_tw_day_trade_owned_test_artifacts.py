#!/usr/bin/env python3
"""Repair existing split-only TW day-trade owned-test artifacts without inference.

The expanding ``test_backtest.npz`` remains the immutable source/audit ledger.
For each unbiased fold, the owned test is its prefix ending immediately before
the next strictly later fold's first valid test date. A same-year experimental
duplicate owns zero rows and cannot steal the preceding fold's final interval.

Every overwritten deployment artifact/report/plot is copied once to a
``.pre_owned_handoff_v1`` backup beside the source before replacement.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import sys
import time
from pathlib import Path
from typing import Any, Iterable

import numpy as np


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.backtest.report import compute_metrics
from stockagent.training.trainer import (
    _load_backtest_artifact,
    _prefix_backtest_result,
    _save_deployment_test_artifacts,
)


MIGRATION_VERSION = 1
BACKUP_TAG = "pre_owned_handoff_v1"
METRIC_NAMES = (
    "cumulative_return",
    "annualized_return",
    "cagr",
    "sharpe",
    "sortino",
    "max_drawdown",
    "calmar",
    "turnover",
    "daily_hit_rate",
    "excess_return_vs_benchmark",
    "cumulative_benchmark",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _backup_path(path: Path) -> Path:
    return path.with_name(f"{path.stem}.{BACKUP_TAG}{path.suffix}")


def _backup_once(path: Path) -> Path | None:
    if not path.is_file():
        return None
    backup = _backup_path(path)
    if not backup.exists():
        shutil.copy2(path, backup)
    return backup


def _fold_summary(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(payload, list) or not payload:
        return []
    if any(not isinstance(row, dict) or "fold_id" not in row for row in payload):
        return []
    return sorted(payload, key=lambda row: int(row["fold_id"]))


def discover_run_dirs(roots: Iterable[Path]) -> list[Path]:
    discovered: dict[Path, Path] = {}
    for raw_root in roots:
        root = raw_root.expanduser().resolve()
        candidates = [root]
        if root.is_dir():
            candidates.extend(
                child
                for child in root.iterdir()
                if child.is_dir() and child.name != "generated_configs"
            )
        for candidate in candidates:
            if not _fold_summary(candidate / "summary.json"):
                continue
            resolved = candidate.resolve()
            discovered.setdefault(resolved, candidate)
    return sorted(discovered)


def _run_contract(run_dir: Path) -> tuple[str | None, int | None, str | None]:
    manifest_path = run_dir / "run_manifest.json"
    if not manifest_path.is_file():
        return None, None, None
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    config = payload.get("configuration", {})
    return (
        config.get("walk_forward", {}).get("lookback_context"),
        config.get("training", {}).get("lookback"),
        config.get("trading", {}).get("execution_mode"),
    )


def _deployment_symbols(fold_dir: Path) -> list[str] | None:
    path = fold_dir / "deployment_test_symbols.json"
    if not path.is_file():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list) or any(not isinstance(item, str) for item in payload):
        raise ValueError(f"invalid deployment symbol sidecar: {path}")
    return [str(item) for item in payload]


def _arrays_equal(left: np.ndarray, right: np.ndarray) -> bool:
    return left.shape == right.shape and bool(
        np.array_equal(left, right, equal_nan=True)
    )


def _deployment_matches(
    path: Path,
    expected_result: Any,
    expected_dates: np.ndarray,
) -> bool:
    """Check the immutable inputs needed by the stitched account replay.

    A correct deployment artifact is not generally byte-equivalent to a prefix
    of ``test_backtest.npz``.  Its realized returns, cash ledger, and executed
    weights come from one account carried across fold boundaries.  Ownership
    migration must therefore compare only the owned dates and original model
    requests; comparing realized output fields would incorrectly replace every
    valid stitched segment with an independently reset fold backtest.
    """
    if not path.is_file():
        return False
    try:
        current, current_dates = _load_backtest_artifact(path)
    except (OSError, KeyError, ValueError):
        return False
    if not _arrays_equal(
        np.asarray(current_dates, dtype="datetime64[D]"),
        np.asarray(expected_dates, dtype="datetime64[D]"),
    ):
        return False
    current_requested = current.requested_weights_history
    expected_requested = expected_result.requested_weights_history
    if (current_requested is None) != (expected_requested is None):
        return False
    return current_requested is None or _arrays_equal(
        np.asarray(current_requested),
        np.asarray(expected_requested),
    )


def _canonical_and_duplicate_rows(
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    canonical: list[dict[str, Any]] = []
    duplicates: list[dict[str, Any]] = []
    seen_starts: set[int] = set()
    for row in rows:
        years = [int(year) for year in row.get("test_years", [])]
        if not years:
            raise ValueError(f"fold {row.get('fold_id')} has no test years")
        if years[0] in seen_starts:
            duplicates.append(row)
        else:
            seen_starts.add(years[0])
            canonical.append(row)
    return canonical, duplicates


def _full_fold_artifact(run_dir: Path, fold_id: int) -> tuple[Any, np.ndarray]:
    path = run_dir / f"fold_{fold_id:02d}" / "test_backtest.npz"
    result, dates = _load_backtest_artifact(path)
    dates = np.asarray(dates, dtype="datetime64[D]").reshape(-1)
    if dates.size == 0 or np.isnat(dates).any() or np.any(dates[1:] <= dates[:-1]):
        raise ValueError(f"full test dates must be finite and increasing: {path}")
    return result, dates


def _write_owned_summary(
    run_dir: Path,
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in rows:
        fold_id = int(row["fold_id"])
        result, dates = _load_backtest_artifact(
            run_dir / f"fold_{fold_id:02d}" / "deployment_test_backtest.npz"
        )
        dates = np.asarray(dates, dtype="datetime64[D]").reshape(-1)
        metrics = compute_metrics(result)
        output.append(
            {
                "fold_id": fold_id,
                "train_years": [int(year) for year in row.get("train_years", [])],
                "val_years": [int(year) for year in row.get("val_years", [])],
                "test_years": [int(year) for year in row.get("test_years", [])],
                "calculation_rows": int(dates.size),
                "calculation_date_start": str(dates[0]) if dates.size else None,
                "calculation_date_end": str(dates[-1]) if dates.size else None,
                "metrics": metrics,
            }
        )
    payload = {
        "contract": "current_year_post_lookback_to_next_year_pre_lookback",
        "scope_version": MIGRATION_VERSION,
        "source": "existing full test ledger; no model inference",
        "folds": output,
    }
    (run_dir / "owned_test_summary.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    with (run_dir / "owned_test_fold_metrics.csv").open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "fold_id",
                "train_years",
                "val_years",
                "test_years",
                "calculation_rows",
                "calculation_date_start",
                "calculation_date_end",
                *METRIC_NAMES,
            ]
        )
        for row in output:
            writer.writerow(
                [
                    row["fold_id"],
                    "-".join(map(str, row["train_years"])),
                    "-".join(map(str, row["val_years"])),
                    "-".join(map(str, row["test_years"])),
                    row["calculation_rows"],
                    row["calculation_date_start"],
                    row["calculation_date_end"],
                    *(row["metrics"].get(name) for name in METRIC_NAMES),
                ]
            )
    return output


def migrate_run(run_dir: Path, *, write_plots: bool = True) -> dict[str, Any]:
    run_dir = run_dir.resolve()
    receipt_path = run_dir / "owned_test_migration_receipt.json"
    previous_receipt: dict[str, Any] = {}
    if receipt_path.is_file():
        try:
            loaded_receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            if isinstance(loaded_receipt, dict):
                previous_receipt = loaded_receipt
        except (OSError, json.JSONDecodeError):
            previous_receipt = {}
    rows = _fold_summary(run_dir / "summary.json")
    canonical, duplicates = _canonical_and_duplicate_rows(rows)
    context, lookback, execution_mode = _run_contract(run_dir)
    if context not in {None, "split_only"}:
        return {
            "run_dir": str(run_dir),
            "status": "skipped_non_split_only",
            "lookback_context": context,
        }
    if not duplicates:
        return {
            "run_dir": str(run_dir),
            "status": "skipped_incomplete_or_no_duplicate_final_fold",
            "fold_rows": len(rows),
        }

    full_by_fold: dict[int, tuple[Any, np.ndarray]] = {
        int(row["fold_id"]): _full_fold_artifact(run_dir, int(row["fold_id"]))
        for row in canonical
    }
    next_starts: dict[int, np.datetime64 | None] = {}
    for index, row in enumerate(canonical):
        fold_id = int(row["fold_id"])
        if index + 1 < len(canonical):
            next_id = int(canonical[index + 1]["fold_id"])
            next_starts[fold_id] = full_by_fold[next_id][1][0]
        else:
            next_starts[fold_id] = None

    expected: dict[int, tuple[Any, np.ndarray, bool]] = {}
    for row in canonical:
        fold_id = int(row["fold_id"])
        full_result, full_dates = full_by_fold[fold_id]
        cutoff = next_starts[fold_id]
        owned_rows = (
            int(full_dates.size)
            if cutoff is None
            else int(np.searchsorted(full_dates, cutoff, side="left"))
        )
        expected[fold_id] = (
            _prefix_backtest_result(full_result, owned_rows),
            full_dates[:owned_rows],
            True,
        )
    for row in duplicates:
        fold_id = int(row["fold_id"])
        full_result, full_dates = _full_fold_artifact(run_dir, fold_id)
        expected[fold_id] = (
            _prefix_backtest_result(full_result, 0),
            full_dates[:0],
            False,
        )

    changed_folds: list[int] = []
    backups: list[str] = []
    before_after: list[dict[str, Any]] = []
    for fold_id, (desired, desired_dates, owns_test) in sorted(expected.items()):
        fold_dir = run_dir / f"fold_{fold_id:02d}"
        deployment_path = fold_dir / "deployment_test_backtest.npz"
        if _deployment_matches(deployment_path, desired, desired_dates):
            continue
        before_hash = _sha256(deployment_path) if deployment_path.is_file() else None
        for path in (
            deployment_path,
            fold_dir / "deployment_annual_report.txt",
            fold_dir / "deployment_equity_curve.png",
        ):
            backup = _backup_once(path)
            if backup is not None:
                backups.append(str(backup))
        symbols = _deployment_symbols(fold_dir)
        _save_deployment_test_artifacts(
            fold_dir,
            desired,
            desired_dates,
            symbols=symbols,
            backtest_artifact_compression="none",
            write_plots=write_plots,
        )
        changed_folds.append(fold_id)
        before_after.append(
            {
                "fold_id": fold_id,
                "owns_test": owns_test,
                "rows": int(desired_dates.size),
                "date_start": str(desired_dates[0]) if desired_dates.size else None,
                "date_end": str(desired_dates[-1]) if desired_dates.size else None,
                "sha256_before": before_hash,
                "sha256_after": _sha256(deployment_path),
            }
        )

    owned_rows = _write_owned_summary(run_dir, canonical)
    receipt = {
        "migration": "tw_day_trade_owned_test_handoff",
        "migration_version": MIGRATION_VERSION,
        "run_dir": str(run_dir),
        "status": "changed" if changed_folds else "already_correct",
        "lookback_context": context,
        "lookback": lookback,
        "execution_mode": execution_mode,
        "canonical_fold_ids": [int(row["fold_id"]) for row in canonical],
        "excluded_same_year_experimental_fold_ids": [
            int(row["fold_id"]) for row in duplicates
        ],
        "changed_fold_ids": changed_folds,
        "backups": sorted(set(backups)),
        "fold_changes": before_after,
        "owned_test_rows": owned_rows,
        "written_at_unix_seconds": float(time.time()),
    }
    historical_changed = {
        int(fold_id)
        for fold_id in previous_receipt.get("historical_changed_fold_ids", [])
    }
    historical_changed.update(
        int(fold_id) for fold_id in previous_receipt.get("changed_fold_ids", [])
    )
    historical_changed.update(changed_folds)
    receipt["historical_changed_fold_ids"] = sorted(historical_changed)
    receipt["historical_backups"] = sorted(
        {
            *map(str, previous_receipt.get("historical_backups", [])),
            *map(str, previous_receipt.get("backups", [])),
            *map(str, backups),
        }
    )
    if "first_migrated_at_unix_seconds" in previous_receipt:
        receipt["first_migrated_at_unix_seconds"] = previous_receipt[
            "first_migrated_at_unix_seconds"
        ]
    else:
        receipt["first_migrated_at_unix_seconds"] = receipt[
            "written_at_unix_seconds"
        ]
    if "stitched_account_replay" in previous_receipt:
        receipt["stitched_account_replay"] = previous_receipt[
            "stitched_account_replay"
        ]
    receipt_path.write_text(
        json.dumps(receipt, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("roots", nargs="+", type=Path)
    parser.add_argument("--no-plots", action="store_true")
    args = parser.parse_args()

    run_dirs = discover_run_dirs(args.roots)
    if not run_dirs:
        raise SystemExit("no fold-level run directories discovered")
    results = [
        migrate_run(run_dir, write_plots=not args.no_plots)
        for run_dir in run_dirs
    ]
    summary = {
        "runs_discovered": len(run_dirs),
        "runs_changed": sum(row.get("status") == "changed" for row in results),
        "runs_already_correct": sum(
            row.get("status") == "already_correct" for row in results
        ),
        "runs_skipped": sum(str(row.get("status", "")).startswith("skipped") for row in results),
        "results": results,
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

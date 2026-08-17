#!/usr/bin/env python3
"""Replay corrected owned TW day-trade requests through one carried account.

This is the second, stateful stage of the existing-artifact migration.  The
ownership migrator repairs each fold's dates and requested weights without
model inference.  This script then runs those requests chronologically through
the canonical T+2 ledger so cash, claims, equity, and absorbing-default state do
not reset at fold boundaries.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import tempfile
import time
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.config import load_config
from stockagent.data.panel import build_panel
from stockagent.training.trainer import (
    _load_fold_result,
    _replay_taiwan_stitched_deployment,
)
from train import _build_panel_kwargs

from scripts.migrate_tw_day_trade_owned_test_artifacts import (
    _backup_once,
    _canonical_and_duplicate_rows,
    _fold_summary,
    _sha256,
    _write_owned_summary,
    discover_run_dirs,
)


ROOT_REPLAY_ARTIFACTS = (
    "walkforward_deployment_backtest.npz",
    "walkforward_deployment_symbols.json",
    "walkforward_deployment_annual_report.txt",
    "walkforward_equity_curve.png",
    "walkforward_equity_curve_log.png",
    "walkforward_annual_performance.png",
)


def _manifest(run_dir: Path) -> dict[str, Any]:
    path = run_dir / "run_manifest.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not isinstance(
        payload.get("configuration"), dict
    ):
        raise ValueError(f"run manifest has no resolved configuration: {path}")
    return payload


def _load_manifest_config(run_dir: Path):
    payload = _manifest(run_dir)["configuration"]
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        suffix=".yaml",
        prefix="stockagent-owned-replay-",
    ) as handle:
        json.dump(payload, handle, ensure_ascii=False)
        handle.flush()
        return load_config(handle.name)


def _panel_cache_key(run_dir: Path) -> str:
    payload = _manifest(run_dir)
    fingerprint = payload.get("dataset_fingerprint")
    data_config = payload["configuration"].get("data", {})
    encoded = json.dumps(
        {"dataset_fingerprint": fingerprint, "data": data_config},
        sort_keys=True,
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _completed_results(run_dir: Path, rows: list[dict[str, Any]]):
    results = []
    missing = []
    for row in rows:
        fold_id = int(row["fold_id"])
        path = run_dir / f"fold_{fold_id:02d}" / "metrics.json"
        if not path.is_file():
            missing.append(str(path))
            continue
        results.append(_load_fold_result(path))
    if missing:
        raise FileNotFoundError(
            "cannot replay a partial run; missing fold metrics: " + ", ".join(missing)
        )
    return results


def replay_run(
    run_dir: Path,
    *,
    panel_cache: dict[str, Any],
) -> dict[str, Any]:
    run_dir = run_dir.resolve()
    rows = _fold_summary(run_dir / "summary.json")
    canonical, duplicates = _canonical_and_duplicate_rows(rows)
    if not duplicates:
        return {
            "run_dir": str(run_dir),
            "status": "skipped_incomplete_or_no_duplicate_final_fold",
        }

    config = _load_manifest_config(run_dir)
    if str(config.walk_forward.lookback_context) != "split_only":
        return {"run_dir": str(run_dir), "status": "skipped_non_split_only"}
    if str(config.trading.execution_mode) != "tw_day_trade":
        return {"run_dir": str(run_dir), "status": "skipped_non_tw_day_trade"}

    results = _completed_results(run_dir, rows)
    key = _panel_cache_key(run_dir)
    if key not in panel_cache:
        panel_cache[key] = build_panel(
            config.data.parquet_root,
            **_build_panel_kwargs(config),
        )
    panel = panel_cache[key]

    backups: list[str] = []
    for name in ROOT_REPLAY_ARTIFACTS:
        backup = _backup_once(run_dir / name)
        if backup is not None:
            backups.append(str(backup))
    backtest_path = run_dir / "walkforward_deployment_backtest.npz"
    before_hash = _sha256(backtest_path) if backtest_path.is_file() else None
    stitched = _replay_taiwan_stitched_deployment(
        run_dir,
        results,
        panel=panel,
        config=config,
    )
    if stitched is None:
        raise RuntimeError(f"canonical stitched replay returned no result: {run_dir}")

    owned_rows = _write_owned_summary(run_dir, canonical)
    receipt_path = run_dir / "owned_test_migration_receipt.json"
    receipt = (
        json.loads(receipt_path.read_text(encoding="utf-8"))
        if receipt_path.is_file()
        else {}
    )
    replay_details = {
        "status": "complete",
        "rows": int(stitched.strategy_returns.shape[0]),
        "date_start": owned_rows[0]["calculation_date_start"] if owned_rows else None,
        "date_end": owned_rows[-1]["calculation_date_end"] if owned_rows else None,
        "sha256_before": before_hash,
        "sha256_after": _sha256(backtest_path),
        "root_backups": sorted(set(backups)),
        "replayed_at_unix_seconds": float(time.time()),
    }
    receipt["stitched_account_replay"] = replay_details
    receipt_path.write_text(
        json.dumps(receipt, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return {
        "run_dir": str(run_dir),
        **replay_details,
        "canonical_fold_ids": [int(row["fold_id"]) for row in canonical],
        "excluded_same_year_experimental_fold_ids": [
            int(row["fold_id"]) for row in duplicates
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("roots", nargs="+", type=Path)
    args = parser.parse_args()

    run_dirs = discover_run_dirs(args.roots)
    if not run_dirs:
        raise SystemExit("no fold-level run directories discovered")
    panel_cache: dict[str, Any] = {}
    results = [replay_run(run_dir, panel_cache=panel_cache) for run_dir in run_dirs]
    summary = {
        "runs_discovered": len(run_dirs),
        "runs_replayed": sum(row.get("status") == "complete" for row in results),
        "runs_skipped": sum(
            str(row.get("status", "")).startswith("skipped") for row in results
        ),
        "panels_built": len(panel_cache),
        "results": results,
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

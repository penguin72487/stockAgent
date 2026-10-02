#!/usr/bin/env python3
"""Prepare current-session physical-action receipts apart from T-1 features.

Reuse canonical collectors and their source receipts. No model inference, quote
substitution, broker orders, or materialization is performed by this command.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
import fcntl
import json
from pathlib import Path
import subprocess
import sys
import time
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.live.tw_share_replacement import load_share_replacements
from stockagent.data.panel import _CorporateActionReferencePaths, _load_corporate_action_reference


def commands(root: Path, output: Path, start_year: int, day: date) -> list[list[str]]:
    summary = output / "tw_corporate_action_reference.summary.json"
    baseline = json.loads(summary.read_text()) if summary.exists() else {}
    # The canonical reference baseline contract starts in 2000. An isolated
    # current-year download must never be relabelled as a complete baseline.
    reference_start = start_year if baseline.get("baseline_established") else 2000
    return [[sys.executable, str(ROOT / "downloader/download_tw_corporate_action_reference.py"),
             "--output-dir", str(output), "--tpex-daily-path", str(root / "tpex_daily_ohlcv.parquet"),
             "--mode", "repair", "--start-year", str(reference_start), "--end-date", str(day),
             "--workers", "2", "--request-interval", "1.0"],
            [sys.executable, str(ROOT / "downloader/download_tw_corporate_action_entitlements.py"),
             "--output-dir", str(output), "--reference", str(output / "tw_corporate_action_reference.parquet"),
             "--universe-report", str(root / "stocks/official_symbol_build_report.csv"),
             "--retained-source-dir", str(root),
             "--mode", "repair", "--start-date", f"{start_year}-01-01", "--end-date", str(day),
             "--request-interval", "1.0"],
            [sys.executable, str(ROOT / "downloader/download_tw_share_replacement_reference.py"),
             "--output-dir", str(output), "--start-date", f"{start_year}-01-01", "--end-date", str(day),
             "--request-interval", "1.0"]]


def sync_historical_share_catalog(root: Path, output: Path, day: date) -> dict:
    """Reuse an accepted action tail instead of downloading the baseline again.

    Historical catalog readiness is separate from current-session action
    readiness: a missing or rejected baseline must not disable valid opening
    action receipts. The existing merger owns overlap, raw-source, and lock
    acceptance; this orchestrator never extends coverage by editing a date.
    """
    started = time.monotonic()
    baseline_path = root / "tw_share_replacement_reference.summary.json"
    result = {"status": "waiting_baseline"}
    try:
        if not baseline_path.is_file():
            result["reason"] = "accepted_historical_catalog_absent"
            return result
        baseline = json.loads(baseline_path.read_text())
        first = date.fromisoformat(baseline["coverage_start"])
        last = date.fromisoformat(baseline["coverage_end"])
        if last >= day:
            from scripts.merge_tw_share_replacement_reference import _accepted
            _accepted(root, start=first, end=day)
            result.update(status="ready", action="no_op_verified", coverage_end=str(last))
            return result
        tail = json.loads((output / "tw_share_replacement_reference.summary.json").read_text())
        cutover = date.fromisoformat(tail["coverage_start"])
        tail_end = date.fromisoformat(tail["coverage_end"])
        if tail_end < day:
            raise ValueError("accepted action tail does not reach the requested day")
        command = [sys.executable, str(ROOT / "scripts/merge_tw_share_replacement_reference.py"),
                   "--public-root", str(root), "--tail-root", str(output),
                   "--cutover-date", str(cutover), "--required-start-date", str(first),
                   "--required-end-date", str(day), "--apply"]
        process = subprocess.run(command, cwd=ROOT, timeout=300, check=False,
                                 capture_output=True, text=True)
        if process.returncode:
            raise RuntimeError(f"canonical historical catalog merge failed ({process.returncode}): "
                               + process.stderr[-2000:])
        proof = json.loads(process.stdout)
        if proof.get("verified") is not True or proof.get("coverage") != [str(first), str(tail_end)]:
            raise ValueError("canonical historical catalog merge did not accept the requested horizon")
        result.update(status="ready", action="merged_verified_tail", merge=proof)
        return result
    except (OSError, TypeError, ValueError, KeyError, RuntimeError, subprocess.TimeoutExpired) as error:
        result.update(status="blocked", error_type=type(error).__name__, error=str(error)[-2500:])
        return result
    finally:
        result["elapsed_seconds"] = round(time.monotonic() - started, 3)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--public-root", type=Path)
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--session-date", default=datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat())
    args = parser.parse_args()
    day = date.fromisoformat(args.session_date)
    catalog = json.loads((ROOT / "configs/data_sync/packed_datasets.json").read_text())
    source = next(row["source"] for row in catalog["datasets"] if row["dataset"] == "tw-public")
    canonical = (ROOT / source).resolve(strict=True)
    root = args.public_root.resolve(strict=True) if args.public_root else canonical
    if root != canonical:
        raise ValueError("execution-action writer must use the catalog-resolved tw-public producer")
    output = root / "execution_actions"
    output.mkdir(parents=True, exist_ok=True)
    with (output / ".refresh.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        refresh_started = time.monotonic()
        receipt = {"session_date": str(day), "source_start_year": args.start_year,
                   "simulation_only": True, "production_order_possible": False,
                   "feature_panel_end_date_unchanged": True, "steps": [], "status": "running"}
        target = output / "readiness.json"
        atomic_write_json(target, receipt)
        try:
            for command in commands(root, output, args.start_year, day):
                step_started = time.monotonic()
                try:
                    result = subprocess.run(command, cwd=ROOT, timeout=900, check=False)
                except Exception as exc:
                    receipt["steps"].append({
                        "script": Path(command[1]).name,
                        "elapsed_seconds": round(time.monotonic() - step_started, 3),
                        "error_class": type(exc).__name__,
                    })
                    atomic_write_json(target, receipt)
                    raise
                receipt["steps"].append({
                    "script": Path(command[1]).name,
                    "returncode": result.returncode,
                    "elapsed_seconds": round(time.monotonic() - step_started, 3),
                })
                atomic_write_json(target, receipt)
                if result.returncode:
                    raise RuntimeError(f"execution action collector failed: {command[1]}")
            validation_started = time.monotonic()
            ref = output / "tw_corporate_action_reference.parquet"
            ent = output / "tw_corporate_action_entitlements.parquet"
            verified = _load_corporate_action_reference(_CorporateActionReferencePaths(
                ref, ref.with_suffix(".summary.json"), ent, ent.with_suffix(".summary.json")))
            import numpy as np
            if verified.coverage_end < np.datetime64(day) or verified.exact_coverage_end < np.datetime64(day):
                raise RuntimeError("execution-action source horizon does not reach the session")
            events = load_share_replacements(output, required_start=date(args.start_year, 1, 1), required_end=day)
            validation_elapsed = round(time.monotonic() - validation_started, 3)
            historical_sync = sync_historical_share_catalog(root, output, day)
            receipt.update(status="source_ready", share_replacement_events=len(events),
                exact_terms_are_position_gated=True,
                sources={p.name: sha256_file(p) for p in output.glob("*.parquet")},
                validation_elapsed_seconds=validation_elapsed,
                historical_catalog_sync=historical_sync,
                total_elapsed_seconds=round(time.monotonic() - refresh_started, 3),
                completed_at=datetime.now(ZoneInfo("Asia/Taipei")).isoformat())
            atomic_write_json(target, receipt)
        except Exception as exc:
            receipt.update(
                status="blocked", error=f"{type(exc).__name__}: {exc}",
                total_elapsed_seconds=round(time.monotonic() - refresh_started, 3),
            )
            atomic_write_json(target, receipt)
            raise


if __name__ == "__main__":
    main()

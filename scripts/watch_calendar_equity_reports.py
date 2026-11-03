#!/usr/bin/env python3
"""Publish daily-display reports for completed, explicitly scoped ablations.

This observer never starts or changes training. Each publication has its own
source hash and is created alongside the completed training artifacts; an
existing publication is checked, never overwritten.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

import yaml


CONTRACT = "observed_account_session_equity_v2"
LABELS = {
    "tx_front_rolling_1x_gross": "Benchmark (TX front-month rolling buy-and-hold, 1x gross)",
    "flat_cash": "Benchmark (flat cash, nominal TWD)",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for part in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(part)
    return digest.hexdigest()


def _read_json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected a JSON object: {path}")
    return value


def _verify_publication(path: Path, source_hash: str, expected_dates: Path | None = None) -> None:
    receipt = _read_json(path / "account_session_display_receipt.json")
    if receipt.get("report_contract") != CONTRACT or receipt.get("source_sha256") != source_hash:
        raise ValueError("existing display receipt has a different contract or source")
    if expected_dates is not None:
        coverage = receipt.get("session_coverage", {})
        if (coverage.get("status") != "expected_dates_present"
                or coverage.get("source_sha256") != _sha256(expected_dates)):
            raise ValueError("existing display receipt lacks the expected session-source check")
    outputs = receipt.get("outputs_sha256")
    required = {"equity_account_sessions.csv", "walkforward_equity_curve.png", "walkforward_equity_curve_log.png"}
    if not isinstance(outputs, dict) or set(outputs) != required:
        raise ValueError("existing display receipt has incomplete output identities")
    for name in required:
        if _sha256(path / name) != outputs[name]:
            raise ValueError(f"calendar display output changed: {name}")


def refresh_attempt(progress_path: Path, renderer: Path, expected_dates: Path | None = None) -> dict:
    progress = _read_json(progress_path)
    if progress.get("state") != "complete" or progress.get("phase") != "complete":
        return {"state": "waiting_for_training_completion", "training": str(progress_path.parent)}
    training = progress_path.parent
    coverage = _read_json(training / "walkforward_deployment_coverage.json")
    if not coverage.get("stitched_fold_ids") or coverage.get("fabricated_gap_rows") != 0:
        raise ValueError("completed run lacks a validated stitched deployment range")
    config = yaml.safe_load((training.parent / "config.yaml").read_text(encoding="utf-8"))
    mode = config["trading"].get("tw_futures_portfolio_benchmark_mode")
    if mode not in LABELS:
        raise ValueError(f"unsupported benchmark mode in completed run: {mode!r}")
    source = training / "walkforward_deployment_backtest.npz"
    source_hash = _sha256(source)
    output = training / "account_session_display_v2"
    if output.exists():
        _verify_publication(output, source_hash, expected_dates)
        return {"state": "verified_existing", "training": str(training), "source_sha256": source_hash, "output": str(output)}

    # The temporary directory is created and owned by this report operation;
    # the context manager removes only that directory, never a training file.
    with tempfile.TemporaryDirectory(prefix=".calendar-display-", dir=training) as temporary:
        staged = Path(temporary) / "report"
        command = [sys.executable, str(renderer), "--backtest", str(source),
                   "--output-dir", str(staged), "--source-identity", str(source),
                   "--expected-sha256", source_hash, "--benchmark-label", LABELS[mode]]
        if expected_dates is not None:
            command.extend(["--expected-account-dates-json", str(expected_dates)])
        child = subprocess.run(command, text=True, capture_output=True, check=False)
        if child.returncode:
            raise RuntimeError(f"calendar renderer failed ({child.returncode}): {child.stderr[-3000:]}")
        _verify_publication(staged, source_hash, expected_dates)
        if _sha256(source) != source_hash:
            raise ValueError("completed backtest changed before report publication")
        # Refuse all pre-existing destinations, including a racing publication.
        if output.exists():
            raise FileExistsError(f"calendar publication already exists: {output}")
        staged.rename(output)
    _verify_publication(output, source_hash, expected_dates)
    return {"state": "published", "training": str(training), "source_sha256": source_hash, "output": str(output)}


def scan(roots: list[Path], renderer: Path, expected_dates: Path | None = None) -> list[dict]:
    rows = []
    for root in roots:
        for progress in sorted(root.glob("_capacity/*/attempt-*/training/progress.json")):
            try:
                rows.append(refresh_attempt(progress, renderer, expected_dates))
            except Exception as exc:
                rows.append({"state": "error", "training": str(progress.parent), "error": f"{type(exc).__name__}: {exc}"})
    return rows


def _atomic_status(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        temp = Path(handle.name)
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    os.replace(temp, path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, action="append", required=True)
    parser.add_argument("--status", type=Path, required=True)
    parser.add_argument("--expected-account-dates-json", type=Path)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--poll-seconds", type=float, default=60.0)
    args = parser.parse_args()
    if args.poll_seconds <= 0:
        parser.error("--poll-seconds must be positive")
    renderer = Path(__file__).with_name("refresh_equity_calendar_report.py")
    while True:
        rows = scan(args.root, renderer, args.expected_account_dates_json)
        status = {"contract": CONTRACT, "checked_at_unix": time.time(), "roots": [str(x) for x in args.root],
                  "expected_account_dates": str(args.expected_account_dates_json) if args.expected_account_dates_json else None,
                  "cases": rows}
        _atomic_status(args.status, status)
        print(json.dumps(status, ensure_ascii=False), flush=True)
        if args.once:
            if any(row["state"] == "error" for row in rows):
                raise SystemExit(1)
            break
        time.sleep(args.poll_seconds)


if __name__ == "__main__":
    main()

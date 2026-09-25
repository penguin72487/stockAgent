#!/usr/bin/env python3
"""Refresh the existing Bybit 00:05 training chain without a parallel fetch stack.

The raw 1m collectors remain owned by registered-data services. This job only
updates funding, derives daily rows, rebuilds the PIT public table, and audits
the resulting ABI. A running raw writer defers the entire chain.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
DOWNLOADER = ROOT / "downloader"
sys.path.insert(0, str(DOWNLOADER))
from artifact_io import atomic_write_json  # noqa: E402

RAW_WRITERS = {
    "download_bybit_perp_1m.py",
    "download_bybit_perp_daily.py",
    "download_binance_perp_1m.py",
    "download_binance_perp_15m.py",
    "download_okx_perp_1m.py",
    "download_okx_perp_daily.py",
    "okx_historical_features.py",
    "download_bybit_funding_history.py",
    "materialize_bybit_perpetual_daily.py",
    "build_bybit_crypto_public_daily_features.py",
    "build_bybit_venue_daily_features.py",
    "download_free_public_context.py",
    "download_crypto_etf_history.py",
    "download_fred_crypto_macro_vintages.py",
    "download_coinmetrics_community.py",
    "download_crypto_keyed_context.py",
}
SOURCE_SUMMARIES = (
    "data_bybit/1m/download_summary.json",
    "data_binance/1m/download_summary.json",
    "data_okx/1m/download_summary.json",
    "data_okx/download_summary.json",
    "data_free_public/download_summary.json",
    "data_fred_crypto_macro/download_summary.json",
    "data_crypto_etf/download_summary.json",
    "data_coinmetrics_community/download_summary.json",
    "data_crypto_reference/download_summary.json",
)


def source_signatures() -> dict[str, list[int] | None]:
    signatures = {}
    for name in SOURCE_SUMMARIES:
        try:
            stat = (ROOT / name).stat()
            signatures[name] = [stat.st_mtime_ns, stat.st_size]
        except FileNotFoundError:
            signatures[name] = None
    return signatures


def materialization_input_signature(root: Path = ROOT) -> dict[str, object]:
    """Detect normal atomic source replacements across the Bybit build window.

    This is a cheap metadata guard, not the publisher's byte-level proof.
    The canonical release still performs its complete source-hash audit.
    """

    digest = hashlib.sha256()
    paths = [
        *(root / "data_bybit/1m").glob("*_features.parquet"),
        *(root / "data_bybit/1m/_hot_tail").glob("*_features.parquet"),
        *(root / "data_bybit/funding").glob("*_funding.parquet"),
        root / "data_bybit/funding/instruments.csv",
        root / "data_bybit/funding/funding_coverage.csv",
    ]
    for path in sorted(paths):
        if path.is_symlink():
            raise RuntimeError(f"Bybit materialization input is a symlink: {path}")
        try:
            info = path.stat(follow_symlinks=False)
        except FileNotFoundError:
            raise RuntimeError(f"Bybit materialization input disappeared: {path}") from None
        identity = (path.relative_to(root), info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)
        digest.update(repr(identity).encode("utf-8"))
        digest.update(b"\n")
    return {"files": len(paths), "metadata_sha256": digest.hexdigest()}


def expected_complete_date() -> str | None:
    path = ROOT / "data_bybit/1m/download_summary.json"
    if not path.is_file():
        return None
    raw_date = json.loads(path.read_text(encoding="utf-8")).get("end_date")
    if not raw_date:
        return None
    return min(
        date.fromisoformat(str(raw_date)[:10]),
        datetime.now(timezone.utc).date() - timedelta(days=1),
    ).isoformat()


def active_raw_writers() -> list[str]:
    found: set[str] = set()
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit() or int(proc.name) == os.getpid():
            continue
        try:
            args = (proc / "cmdline").read_bytes().split(b"\0")
        except (OSError, PermissionError):
            continue
        for arg in args:
            name = Path(os.fsdecode(arg)).name
            if name in RAW_WRITERS:
                found.add(f"{proc.name}:{name}")
    return sorted(found)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--sync-root", default="/srv/stockagent-packed")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "artifacts/data_quality/crypto_training_latest")
    args = parser.parse_args()
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    lock_path = output / "refresh.lock"
    with lock_path.open("a+b") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("crypto training refresh already running; deferred", flush=True)
            return
        receipt: dict[str, object] = {
            "refresh_contract_version": 3,
            "started_at_utc": datetime.now(timezone.utc).isoformat(),
            "state": "running",
            "steps": [],
            "step_attempts": [],
        }

        def save() -> None:
            atomic_write_json(output / "refresh_receipt.json", receipt)

        def defer_for_raw_writer(writers: list[str]) -> None:
            receipt.update(
                state="deferred_raw_writer",
                active_writers=writers,
                finished_at_utc=datetime.now(timezone.utc).isoformat(),
            )
            save()
            print(json.dumps(receipt, ensure_ascii=False), flush=True)

        def defer_for_source_change(before: dict[str, object], after: dict[str, object]) -> None:
            receipt.update(
                state="deferred_source_changed",
                input_signature_before=before,
                input_signature_after=after,
                finished_at_utc=datetime.now(timezone.utc).isoformat(),
            )
            save()
            print(json.dumps(receipt, ensure_ascii=False), flush=True)

        def run_timed_step(command: list[str]) -> dict[str, object]:
            attempt: dict[str, object] = {
                "command": command,
                "started_at_utc": datetime.now(timezone.utc).isoformat(),
                "status": "running",
            }
            receipt["active_step"] = attempt
            save()
            step_started = time.monotonic()
            try:
                subprocess.run(command, cwd=ROOT, check=True)
            except Exception:
                attempt["status"] = "failed"
                attempt["elapsed_seconds"] = round(time.monotonic() - step_started, 3)
                receipt["step_attempts"].append(attempt)
                receipt.pop("active_step", None)
                save()
                raise
            attempt["status"] = "process_completed"
            attempt["elapsed_seconds"] = round(time.monotonic() - step_started, 3)
            receipt["step_attempts"].append(attempt)
            receipt.pop("active_step", None)
            save()
            return attempt

        busy = active_raw_writers()
        if busy:
            defer_for_raw_writer(busy)
            return
        previous_path = output / "refresh_receipt.json"
        if previous_path.is_file():
            previous = json.loads(previous_path.read_text(encoding="utf-8"))
            prior_inventory = previous.get("inventory_summary") or {}
            if (
                previous.get("state") == "completed"
                and previous.get("refresh_contract_version") == 3
                and prior_inventory.get("materialization_current_to_raw")
                and prior_inventory.get("expected_last_complete_training_date")
                == expected_complete_date()
                and previous.get("source_signatures") == source_signatures()
            ):
                print("crypto training refresh already current for this UTC day", flush=True)
                return
        steps = [
            [sys.executable, "downloader/download_bybit_funding_history.py", "--workers", "8"],
            [sys.executable, "downloader/materialize_bybit_perpetual_daily.py", "--execution-minute-utc", "5", "--workers", "8"],
            [sys.executable, "scripts/build_bybit_venue_daily_features.py"],
            [sys.executable, "scripts/audit_crypto_historical_coverage.py", "--output-dir", str(output)],
            [sys.executable, "scripts/report_crypto_training_features.py", "--output-dir", str(output / "bybit")],
            [sys.executable, "scripts/report_crypto_venue_1m_features.py", "okx", "--output-dir", str(output / "okx")],
            [sys.executable, "scripts/report_crypto_venue_1m_features.py", "binance", "--output-dir", str(output / "binance")],
        ]
        save()
        try:
            materialization_inputs: dict[str, object] | None = None
            for command in steps:
                busy = active_raw_writers()
                if busy:
                    defer_for_raw_writer(busy)
                    return
                if command[1].endswith("materialize_bybit_perpetual_daily.py"):
                    materialization_inputs = materialization_input_signature()
                attempt = run_timed_step(command)
                if materialization_inputs is not None:
                    current_inputs = materialization_input_signature()
                    if current_inputs != materialization_inputs:
                        defer_for_source_change(materialization_inputs, current_inputs)
                        return
                receipt["steps"].append({
                    "command": command,
                    "completed_at_utc": datetime.now(timezone.utc).isoformat(),
                    "elapsed_seconds": attempt["elapsed_seconds"],
                })
                save()
            busy = active_raw_writers()
            if busy:
                defer_for_raw_writer(busy)
                return
            inventory = json.loads((output / "bybit/summary.json").read_text(encoding="utf-8"))
            if not inventory["source_receipt_hashes_match"]:
                raise RuntimeError("public feature receipt hashes do not match")
            if not inventory["materialization_current_to_raw"]:
                raise RuntimeError(
                    "daily training table is behind last completed UTC day: "
                    f"{inventory['daily_last_date']} < "
                    f"{inventory['expected_last_complete_training_date']}"
                )
            if materialization_inputs is not None:
                current_inputs = materialization_input_signature()
                if current_inputs != materialization_inputs:
                    defer_for_source_change(materialization_inputs, current_inputs)
                    return
            if args.publish:
                busy = active_raw_writers()
                if busy:
                    defer_for_raw_writer(busy)
                    return
                command = [str(ROOT / "scripts/run_data_release.sh"), "publish", "bybit", "--sync-root", args.sync_root]
                attempt = run_timed_step(command)
                receipt["steps"].append({
                    "command": command,
                    "completed_at_utc": datetime.now(timezone.utc).isoformat(),
                    "elapsed_seconds": attempt["elapsed_seconds"],
                })
            receipt.update(
                state="completed",
                finished_at_utc=datetime.now(timezone.utc).isoformat(),
                inventory_summary=inventory,
                source_signatures=source_signatures(),
            )
        except Exception as exc:
            receipt.update(state="failed", finished_at_utc=datetime.now(timezone.utc).isoformat(), error=f"{type(exc).__name__}: {exc}")
            save()
            raise
        save()
        print(json.dumps(receipt, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()

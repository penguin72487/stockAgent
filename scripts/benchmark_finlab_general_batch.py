#!/usr/bin/env python3
"""Matched, bounded FinLab general-key benchmark using the canonical collector.

Uses already acquired small financial tables. A lock protects the sole source
writer; the real core status still covers the full catalog, never a toy subset.
This does make native authenticated rechecks and writes real canonical receipts.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import fcntl
import json
from pathlib import Path
import subprocess
import sys
import time
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import download_finlab_history as collector
from scripts.download_finlab_market_intraday import _account_sync_lock


def child(keys: list[str], root: Path) -> int:
    available = collector.sync_provider_catalog(root, now=datetime.now(UTC))
    curated = {row["key"]: row for row in collector.load_catalog()["datasets"]}
    if not keys or any(key not in available or not collector.has_local_download(key, root) for key in keys):
        raise ValueError("benchmark requires already acquired catalog keys")
    # Pin a matched cohort only for this probe. Everything else, including full
    # catalog accounting, native refresh, conversion, hashes and receipts, is
    # the production sync implementation. This is not an all-key sweep proof.
    with patch.object(collector, "sync_selection", return_value=keys):
        result = collector.sync_catalog(available, curated, root, limit=len(keys), refresh_days=1,
            min_quota_remaining_mb=50, retry_unavailable=False, key_timeout_seconds=600)
    print("benchmark_result="+json.dumps({key: result[key] for key in (
        "state", "attempted", "downloaded", "unchanged", "provider_errors", "timed_out", "sdk_frame_releases")}))
    return 0 if result["attempted"] == len(keys) and result["provider_errors"] == 0 else 1


def benchmark(root: Path, output: Path) -> dict:
    with (root / ".sync.lock").open("a") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        keys = []
        for path in sorted((root / "receipts").glob("*.json")):
            row = json.loads(path.read_text())
            key = row.get("dataset", "")
            if (key.startswith("financial_statement:") and 0 < row.get("parquet_size_bytes", 0) <= 4*1024**2
                    and collector.has_local_download(key, root)):
                keys.append(key)
            if len(keys) == 8:
                break
        if len(keys) != 8:
            raise ValueError("eight small acquired financial tables required")
        cases = []
        for index, group_size in enumerate((1, 8, 8, 1)):
            started = time.monotonic()
            records, child_pids = [], []
            for offset in range(0, len(keys), group_size):
                process = subprocess.Popen([sys.executable, __file__, "--child-keys",
                    json.dumps(keys[offset:offset+group_size], ensure_ascii=False), "--source-root", str(root),
                    "--owner-lock-fd", str(owner.fileno())], pass_fds=(owner.fileno(),),
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=ROOT)
                child_pids.append(process.pid)
                try:
                    stdout, _ = process.communicate(timeout=660*group_size)
                except subprocess.TimeoutExpired:
                    process.terminate()
                    try:
                        process.communicate(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.communicate(timeout=5)
                    raise RuntimeError("bounded benchmark child timed out; private SDK output withheld") from None
                rows = [json.loads(line.split("=", 1)[1]) for line in stdout.splitlines()
                        if line.startswith("benchmark_result=")]
                if process.returncode or len(rows) != 1:
                    raise RuntimeError("canonical general benchmark failed; private SDK output withheld")
                records.extend(rows)
            seconds = time.monotonic()-started
            cases.append({"case": index, "group_size": group_size, "owner_seconds": seconds,
                "processes": len(child_pids), "attempted": sum(row["attempted"] for row in records),
                "unchanged": sum(row["unchanged"] for row in records),
                "downloaded": sum(row["downloaded"] for row in records),
                "sdk_frame_releases": sum(row["sdk_frame_releases"] for row in records), "records": records})
            print(json.dumps({"case": index, "group_size": group_size, "owner_seconds": seconds}), flush=True)
            collector._atomic_json(output, {"state": "running", "keys": keys, "cases": cases})
        old = sum(row["owner_seconds"] for row in cases if row["group_size"] == 1)
        new = sum(row["owner_seconds"] for row in cases if row["group_size"] == 8)
        result = {"contract_version": 1, "state": "accepted", "observed_at_utc": datetime.now(UTC).isoformat(),
            "keys": keys, "cases": cases, "single_key_seconds": old, "eight_key_seconds": new,
            "matched_general_owner_speed_ratio": old/new, "order": "1,8,8,1",
            "scope": "matched_eight_small_financial_tables; full_catalog_plans/native_check/conversion/receipt/process_wall",
            "cache_and_order_effects_not_fully_eliminated": True,
            "all_tables_or_intraday_speedup_claim": False, "official_maximum_proven": False}
        collector._atomic_json(output, result)
        return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=collector.DEFAULT_OUTPUT)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--child-keys")
    parser.add_argument("--owner-lock-fd", type=int)
    args = parser.parse_args()
    if args.child_keys:
        if args.owner_lock_fd is None:
            parser.error("child probe requires the parent's exact account lock")
        with _account_sync_lock(args.source_root, args.owner_lock_fd):
            return child(json.loads(args.child_keys), args.source_root)
    if not args.output:
        parser.error("--output is required")
    result = benchmark(args.source_root, args.output)
    print(json.dumps({key: result[key] for key in ("state", "single_key_seconds", "eight_key_seconds", "matched_general_owner_speed_ratio")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

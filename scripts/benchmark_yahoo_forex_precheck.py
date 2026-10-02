"""Bounded read-only comparison of legacy and asset-aware Yahoo FX prechecks.

Only the existing repair-report cohort (at most 64 files/64 MiB) is inspected.
Imports, symbol discovery, provider requests, writes and full-job latency are
outside this measurement. No source, report, receipt or service is promoted.
"""

from __future__ import annotations

import argparse
from collections import Counter
from contextlib import ExitStack
import csv
from datetime import date, datetime, timezone
import json
from pathlib import Path
import re
import statistics
import sys
import time
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader import download_yahoo_ohlcv as yahoo  # noqa: E402
from downloader.artifact_io import atomic_write_json, sha256_file  # noqa: E402

MAX_FILES = 64
MAX_FILE_BYTES = 2 * 1024**2
MAX_TOTAL_BYTES = 64 * 1024**2
MAX_REPORT_BYTES = 256 * 1024


class _QuietProgress:
    def __init__(self, items, **_kwargs):
        self.items = items

    def __iter__(self):
        return iter(self.items)

    def set_postfix_str(self, *_args, **_kwargs):
        pass

    def close(self):
        pass


def run_probe(source_root: Path, *, end_date: str) -> dict:
    started = time.perf_counter()
    date.fromisoformat(end_date)
    source_root = source_root.resolve(strict=True)
    report = source_root / "repair_report.csv"
    if report.stat().st_size > MAX_REPORT_BYTES:
        raise ValueError("repair report exceeds bounded scope")
    report_digest = sha256_file(report)
    with report.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not 1 <= len(rows) <= MAX_FILES:
        raise ValueError("repair-report cohort must contain 1..64 symbols")
    codes = [r.get("code", "") for r in rows]
    if len(set(codes)) != len(codes) or not all(re.fullmatch(r"[A-Z]{6}", c) for c in codes):
        raise ValueError("cohort codes must be unique six-letter FX pairs")
    paths = {code: source_root / f"{code}_features.parquet" for code in codes}
    sizes = {}
    for code, path in paths.items():
        if path.is_symlink() or not path.is_file() or path.resolve().parent != source_root:
            raise ValueError("probe requires regular, direct cohort source files")
        sizes[code] = path.stat().st_size
        if not 0 < sizes[code] <= MAX_FILE_BYTES:
            raise ValueError("source file exceeds bounded scope")
    if sum(sizes.values()) > MAX_TOTAL_BYTES:
        raise ValueError("aggregate source files exceed bounded scope")

    before = {code: sha256_file(path) for code, path in paths.items()}
    args = argparse.Namespace(
        mode="daily-update", start_date="2000-01-01", end_date=end_date,
        repair_overlap_days=7, precheck_file_timeout_seconds=0,
        daily_stale_max_lag_days=14, retry_blacklisted_repair_symbols=False,
    )
    records = [yahoo.SymbolRecord(c, c, "forex", f"{c}=X") for c in codes]
    original_columns = yahoo._repair_required_columns
    result = {
        "schema_version": 1, "state": "not_accepted",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_root": str(source_root), "end_date": end_date,
        "scope": "read-only FX repair-report cohort; canonical planning only",
        "elapsed_seconds_boundary": "probe function entry through final source/report verification; imports excluded",
        "cohort_symbols": len(records), "source_bytes": sum(sizes.values()),
        "acquisition_contract": yahoo.acquisition_contract("forex"),
        "source_files_written": 0, "source_reports_written": 0,
        "network_calls": 0, "promoted": False,
        "imports_and_discovery_measured": False, "full_job_measured": False,
        "process_cold_start_measured": False, "os_cache_controlled": False,
        "precheck_subprocess_timeout_measured": False,
        "price_rows_or_dtypes_changed": False, "trials": [],
        "files": [],
    }
    def forbidden(*_args, **_kwargs):
        raise RuntimeError("read-only precheck attempted source mutation or network work")

    try:
        # Verify identity before considering either schema policy. A different
        # provider, missing source proof or malformed price schema is not fixed
        # by treating FX volume as optional.
        for code, path in paths.items():
            info = yahoo._load_existing_file_info(path)
            if (info.error or info.metadata_error or info.source != "yahoo"
                    or info.asset_class != "forex"
                    or original_columns("forex") - info.columns):
                raise ValueError(f"unverified Yahoo FX source: {code}")
            result["files"].append({
                "code": code, "sha256_before": before[code], "bytes": sizes[code],
                "first_date": info.first_date, "last_date": info.last_date,
                "checked_through_date": info.checked_through_date,
                "requested_start_date": info.requested_start_date,
                "source": info.source, "asset_class": info.asset_class,
                "volume_present": "Trading_Volume" in info.columns,
            })
        with ExitStack() as guards:
            guards.enter_context(patch.object(yahoo, "tqdm", _QuietProgress))
            for name in ("_download_symbol", "_fetch_with_hard_timeout",
                         "_blacklist_record_symbols", "_quarantine_unverified_yahoo_file",
                         "_write_feature_parquet_atomic", "atomic_write_json", "atomic_write_text"):
                guards.enter_context(patch.object(yahoo, name, forbidden))
            for variant in ("legacy_required_volume", "asset_aware", "asset_aware", "legacy_required_volume"):
                policy = original_columns if variant == "asset_aware" else (
                    lambda _asset: yahoo.REPAIR_REQUIRED_COLUMNS.copy()
                )
                with patch.object(yahoo, "_repair_required_columns", policy):
                    begin = time.perf_counter()
                    checks = yahoo._resolve_repair_plan("forex", args, records, source_root)
                    elapsed = time.perf_counter() - begin
                result["trials"].append({
                    "variant": variant, "elapsed_seconds": elapsed,
                    "status_counts": dict(Counter(c.status for c in checks)),
                    "pending_repairs": sum(c.repair_start_date is not None for c in checks),
                    "full_schema_rebuilds": sum(c.status == "schema_mismatch" for c in checks),
                    "incremental_repairs": sum(c.status == "stale" for c in checks),
                })
        result["precheck_medians_seconds"] = {
            v: statistics.median(t["elapsed_seconds"] for t in result["trials"] if t["variant"] == v)
            for v in ("legacy_required_volume", "asset_aware")
        }
        candidate = [t for t in result["trials"] if t["variant"] == "asset_aware"]
        if all(t["full_schema_rebuilds"] == 0 for t in candidate):
            result["state"] = "accepted_readonly_planning_candidate"
    except Exception as exc:
        result["failure"] = {"type": type(exc).__name__, "message": str(exc)}
    finally:
        unchanged = {}
        for code, path in paths.items():
            try:
                unchanged[code] = sha256_file(path) == before[code]
            except OSError:
                unchanged[code] = False
        result["source_hashes_unchanged"] = unchanged
        try:
            result["source_report_hash_unchanged"] = sha256_file(report) == report_digest
        except OSError:
            result["source_report_hash_unchanged"] = False
        if not all(result["source_hashes_unchanged"].values()) or not result["source_report_hash_unchanged"]:
            result["state"] = "not_accepted"
        result["elapsed_seconds"] = time.perf_counter() - started
        result["code_sha256"] = {
            "collector": sha256_file(Path(yahoo.__file__)),
            "runner": sha256_file(ROOT / "downloader/run_daily_all_markets.sh"),
            "benchmark": sha256_file(Path(__file__)),
        }
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=ROOT / "data_yahoo/forex")
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError("benchmark output already exists")
    if args.output.resolve().is_relative_to(args.source_root.resolve()):
        raise ValueError("benchmark output must be outside the source root")
    result = run_probe(args.source_root, end_date=args.end_date)
    atomic_write_json(args.output, result)
    print(json.dumps({k: result[k] for k in ("state", "cohort_symbols", "elapsed_seconds", "precheck_medians_seconds") if k in result}))
    if result["state"] != "accepted_readonly_planning_candidate":
        raise SystemExit(1)


if __name__ == "__main__":
    main()

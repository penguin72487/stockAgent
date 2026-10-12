#!/usr/bin/env python3
"""Offline full-row comparison of bounded inventory conversion and old receipt.

Never calls FinLab. This is normalized-value equivalence, not upstream freshness.
The candidate Parquet is retained with this evidence, not installed as a source.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
from itertools import zip_longest
import json
from pathlib import Path
import resource
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pyarrow.parquet as pq
from scripts.download_finlab_history import _atomic_json, safe_stem
from scripts.finlab_arrow_history import _convert_inventory, _signature


def audit(raw: Path, source_root: Path, output: Path) -> dict:
    receipt = json.loads((source_root / "receipts" / (safe_stem("inventory")+".json")).read_text())
    old = source_root / receipt["parquet_path"]
    if (not old.resolve().is_relative_to(source_root.resolve()) or not old.is_file()
            or old.stat().st_size != receipt["parquet_size_bytes"]):
        raise ValueError("invalid inventory reference object")
    output.parent.mkdir(parents=True, exist_ok=True)
    candidate = output.parent / "inventory_candidate.parquet"
    signature = _signature(raw)
    started = time.monotonic()
    stats = _convert_inventory(raw, candidate)
    converted = time.monotonic()-started
    compared = 0
    equal = True
    reference, result = pq.ParquetFile(old), pq.ParquetFile(candidate)
    for left, right in zip_longest(reference.iter_batches(batch_size=65536), result.iter_batches(batch_size=65536)):
        if left is None or right is None or not left.equals(right, check_metadata=False):
            equal = False
            break
        compared += len(left)
    with old.open("rb") as handle:
        reference_sha = hashlib.file_digest(handle, "sha256").hexdigest()
    with raw.open("rb") as handle:
        raw_sha = hashlib.file_digest(handle, "sha256").hexdigest()
    checks = {"all_values_types_nulls_aliases_row_order_and_indexes_equal": equal,
        "all_reference_rows_compared": compared == reference.metadata.num_rows == stats["rows"],
        "reference_sha256": reference_sha == receipt["sha256"], "raw_source_unchanged": signature == _signature(raw)}
    evidence = {"contract_version": 1, "observed_at_utc": datetime.now(UTC).isoformat(),
        "accepted": all(checks.values()), "checks": checks, "provider_calls": 0,
        "rows_compared": compared, "conversion_seconds": converted,
        "full_conversion_comparison_hash_seconds": time.monotonic()-started,
        "process_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
        "candidate_parquet_bytes": candidate.stat().st_size, "raw_bytes": raw.stat().st_size,
        "raw_sha256": raw_sha, "reference_sha256": reference_sha, "stats": stats,
        "upstream_freshness_proven": False, "scope": "exact_cached_inventory_generation_vs_previous_canonical_object"}
    _atomic_json(output, evidence)
    return evidence


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", type=Path)
    parser.add_argument("--source-root", type=Path, default=ROOT / "data_finlab")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    raw = args.raw
    if raw is None:
        from finlab import utils
        raw = Path(utils.get_tmp_dir()) / "inventory.feather"
    result = audit(raw, args.source_root, args.output)
    print(json.dumps({key: result[key] for key in ("accepted", "rows_compared", "conversion_seconds",
        "full_conversion_comparison_hash_seconds", "process_peak_rss_bytes", "provider_calls")}))
    return 0 if result["accepted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

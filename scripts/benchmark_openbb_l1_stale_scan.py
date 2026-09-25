#!/usr/bin/env python3
"""Read-only A/B benchmark of the complete OpenBB L1 source-contract scan.

Both variants use one SQLite read transaction, the production predicate and
the same row digest. This does not mark segments stale or mutate the archive.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import resource
import sqlite3
import sys
import tempfile
import time


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.compact_openbb_l1 import (  # noqa: E402
    _lexical_absolute_path,
    _stale_source_contract_sql,
)


def _run_variant(
    connection: sqlite3.Connection, *, member_first: bool, prefix: str
) -> dict[str, object]:
    query = _stale_source_contract_sql("", member_first=member_first)
    parameters = (prefix, prefix)
    plan = [
        str(row[3])
        for row in connection.execute("EXPLAIN QUERY PLAN " + query, parameters)
    ]
    row_hashes: list[bytes] = []
    started = time.perf_counter()
    for row in connection.execute(query, parameters):
        row_hashes.append(
            hashlib.sha256(
                json.dumps(
                    tuple(row), ensure_ascii=False, separators=(",", ":")
                ).encode()
            ).digest()
        )
    query_elapsed = time.perf_counter() - started
    digest = hashlib.sha256()
    for row_hash in sorted(row_hashes):
        digest.update(row_hash)
    return {
        "variant": "member_first" if member_first else "segment_first",
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "query_elapsed_seconds": round(query_elapsed, 3),
        "stale_rows": len(row_hashes),
        "stale_rows_sha256": digest.hexdigest(),
        "query_plan": plan,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest", type=Path,
        default=REPO_ROOT / "data_openBB/_state/openbb_archive.sqlite3",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--cache-mib", type=int, default=0)
    parser.add_argument(
        "--variant", choices=("both", "segment_first", "member_first"),
        default="both",
    )
    args = parser.parse_args()
    if args.cache_mib < 0 or args.cache_mib > 2048:
        parser.error("--cache-mib must be 0..2048")
    manifest = args.manifest.resolve(strict=True)
    os.chdir(REPO_ROOT)
    connection = sqlite3.connect(
        f"file:{manifest}?mode=ro", uri=True, timeout=30.0
    )
    try:
        connection.create_function(
            "stockagent_resolve_path", 1, _lexical_absolute_path,
            deterministic=True,
        )
        if args.cache_mib:
            connection.execute(f"PRAGMA cache_size={-args.cache_mib * 1024}")
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        prefix = str(REPO_ROOT) + os.sep
        variants = (
            (True, False) if args.variant == "both"
            else (args.variant == "member_first",)
        )
        results = [
            _run_variant(connection, member_first=member_first, prefix=prefix)
            for member_first in variants
        ]
        connection.rollback()
        if len(results) == 2 and (
            results[0]["stale_rows"] != results[1]["stale_rows"]
            or results[0]["stale_rows_sha256"] != results[1]["stale_rows_sha256"]
        ):
            raise RuntimeError("OpenBB source-contract scan variants disagree")
        payload = {
            "schema_version": 1,
            "observed_at_utc": datetime.now(timezone.utc).isoformat(),
            "manifest": str(manifest),
            "manifest_bytes": manifest.stat().st_size,
            "cache_mib": args.cache_mib,
            "requested_variant": args.variant,
            "claim_boundary": (
                "One read-only SQLite snapshot. When both variants run, "
                "candidate runs first and baseline second. Different cache "
                "warmth and concurrent host load remain possible; this is "
                "not a p95 or write-cost benchmark."
            ),
            "results": results,
            "process_peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        }
        if args.output is not None:
            output = args.output.resolve()
            output.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=output.parent,
                prefix=f".{output.name}.", suffix=".tmp", delete=False,
            ) as handle:
                temporary = Path(handle.name)
                json.dump(payload, handle, ensure_ascii=False, indent=2)
                handle.write("\n")
            os.replace(temporary, output)
        print(json.dumps(payload, ensure_ascii=False))
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

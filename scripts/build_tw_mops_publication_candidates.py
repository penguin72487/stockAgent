#!/usr/bin/env python3
"""Build document-level MOPS release guesses, preserving unknown filing times."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, atomic_write_parquet
from stockagent.data.tw_mops_publication_timing import (
    _source_receipts, build_publication_candidates, source_fingerprint,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("data_tw_public/mops_xbrl"))
    args = parser.parse_args()
    root = args.root.resolve(strict=True)
    frame, summary = build_publication_candidates(root)
    lock_path = root.parent.parent / ".locks" / "tw-public-refresh.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"[mops-timing] documents={frame.height} waiting_for_source_lock={lock_path}",
          flush=True)
    with lock_path.open("a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        if source_fingerprint(_source_receipts(root)) != summary["source_fingerprint_sha256"]:
            raise RuntimeError("MOPS receipts changed while building publication candidates")
        output = root / "publication_candidates.parquet"
        atomic_write_parquet(output, frame)
        with output.open("rb") as stream:
            summary["parquet_sha256"] = hashlib.file_digest(stream, "sha256").hexdigest()
        summary["parquet_path"] = str(output)
        atomic_write_json(root / "publication_candidates_state.json", summary)
    print(f"[mops-timing] documents={frame.height} rows={summary['fact_rows_covered']} "
          f"basis={summary['publication_time_basis_counts']}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

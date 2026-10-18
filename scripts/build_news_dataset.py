"""Materialize existing local news as a verified, versioned Parquet corpus."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from stockagent.data.news_dataset import build_news_dataset, finish_pending_news_dataset, verify_news_dataset


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=Path("data_news_fulltext"))
    parser.add_argument("--output", type=Path, default=Path("data_news"))
    parser.add_argument("--workers", type=int, default=4, help="bounded local file readers, no publisher requests")
    parser.add_argument("--batch-rows", type=int, default=16384)
    parser.add_argument("--refresh", action="store_true", help="commit an incremental copy-on-write version; reuse unchanged parts, never alter source/old generations")
    parser.add_argument("--snapshot-from", type=Path, help="reuse verified immutable snapshot inputs in a fresh generation, not old parts/state")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--verify-only", action="store_true")
    mode.add_argument("--retry-verification", action="store_true", help="verify and publish only an already fully written candidate; never rebuild or retag data")
    parser.add_argument("--verification-memory-mb", type=int, default=2048)
    args = parser.parse_args(argv)
    last = [0.0, None]
    def notify(value):
        now = time.monotonic()
        if now - last[0] >= 15 or value["stage"] != last[1] or value["completed"] == value["total"]:
            print(json.dumps(value, ensure_ascii=False), flush=True)
            last[:] = [now, value["stage"]]
    if args.verify_only:
        result = verify_news_dataset(args.output.resolve(), verification_memory_mb=args.verification_memory_mb)
    elif args.retry_verification:
        result = finish_pending_news_dataset(args.output, verification_memory_mb=args.verification_memory_mb, notify=notify)
    else:
        result = build_news_dataset(args.source_root, args.output, workers=args.workers,
                                    batch_rows=args.batch_rows, refresh=args.refresh,
                                    snapshot_from=args.snapshot_from, notify=notify,
                                    verification_memory_mb=args.verification_memory_mb)
    print(json.dumps(result, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

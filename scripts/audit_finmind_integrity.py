#!/usr/bin/env python3
"""Audit current FinMind queue heads; --repair requeues only proven failures.

Recovery requests are subsequently sent by the canonical worker with its usual
quota, session protection and entitlement checks. No downloads occur here.
"""
from __future__ import annotations

import argparse
import fcntl
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from downloader.finmind_integrity import audit_queue


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--dataset", action="append", default=[])
    parser.add_argument("--repair", action="store_true")
    args = parser.parse_args()
    root = args.root.resolve()
    if not (root / "queue.sqlite3").is_file():
        parser.error("existing owner queue required")
    with (root / "worker.lock").open("a+") as lock:
        if args.repair:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                parser.error("worker is active; stop only this lane before --repair")
        mode = "rw" if args.repair else "ro"
        with sqlite3.connect(f"file:{root / 'queue.sqlite3'}?mode={mode}", uri=True, timeout=10) as conn:
            total = {"checked": 0, "failed": 0, "requeued": 0, "failures": [], "root": str(root)}
            cursor = 0
            while True:
                result = audit_queue(conn, root, after_rowid=cursor, repair=args.repair,
                                     datasets=tuple(args.dataset))
                for key in ("checked", "failed", "requeued"):
                    total[key] += result[key]
                total["failures"].extend(result["failures"])
                total.update(observed_at_utc=result["observed_at_utc"], proof_scope=result["proof_scope"])
                cursor = result["last_rowid"]
                if result["end_of_sweep"]:
                    break
            atomic_write_json(args.output, total)
            print({key: value for key, value in total.items() if key != "failures"}, flush=True)
            return 1 if total["failed"] and not args.repair else 0


if __name__ == "__main__":
    raise SystemExit(main())

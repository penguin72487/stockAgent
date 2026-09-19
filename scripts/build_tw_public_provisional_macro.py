#!/usr/bin/env python3
"""Materialize unverified historical bulk values with labelled release estimates."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.data.tw_public_provisional_macro import write_provisional_macro_events  # noqa: E402


@contextmanager
def _source_lock(root: Path):
    path = root.resolve().parent / ".locks" / "tw-public-refresh.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("canonical Taiwan public source writer is active") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=Path("data_tw_public"))
    parser.add_argument(
        "--output-path", type=Path,
        default=Path("artifacts/data_quality/tw_public_provisional_macro/events.parquet"),
    )
    parser.add_argument("--source-update-lock-held", action="store_true")
    args = parser.parse_args()
    if args.source_update_lock_held:
        summary = write_provisional_macro_events(args.input_dir, args.output_path)
    else:
        with _source_lock(args.input_dir):
            summary = write_provisional_macro_events(args.input_dir, args.output_path)
    print(f"[tw-provisional-macro] rows={summary['total_rows']} "
          f"ambiguous={len(summary['ambiguous_bulk_keys'])} "
          f"strict_pit_eligible=0 output={args.output_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Build a separate wide, estimated-vintage TW research feature table."""

from __future__ import annotations

import argparse
from contextlib import contextmanager, nullcontext
import fcntl
import hashlib
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.data.tw_public_research_features import (  # noqa: E402
    build_tw_public_research_features,
)


@contextmanager
def _source_lock(input_dir: Path):
    path = input_dir.resolve().parent / ".locks" / "tw-public-refresh.lock"
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
        "--base-path", type=Path,
        default=Path("data_tw_public/features/tw_public_stock_daily.parquet"),
    )
    parser.add_argument(
        "--macro-events-path", type=Path,
        default=Path("artifacts/data_quality/tw_public_provisional_macro/events.parquet"),
    )
    parser.add_argument(
        "--output-path", type=Path,
        default=Path("data_tw_public/features/tw_public_research_wide_2014_v1.parquet"),
    )
    parser.add_argument("--minimum-year", type=int, default=2013)
    parser.add_argument("--source-update-lock-held", action="store_true")
    args = parser.parse_args()
    key = hashlib.sha256(str(args.output_path.resolve()).encode()).hexdigest()[:20]
    lock = ROOT / "artifacts/data_locks" / f"tw_public_research_{key}.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    guard = nullcontext() if args.source_update_lock_held else _source_lock(args.input_dir)
    with guard:
        with lock.open("a+") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            summary = build_tw_public_research_features(
                base_path=args.base_path,
                macro_events_path=args.macro_events_path,
                xbrl_root=args.input_dir / "mops_xbrl",
                calendar_path=args.input_dir / "twse_taiex_ohlc.parquet",
                output_path=args.output_path,
                minimum_year=args.minimum_year,
            )
    print(
        "[tw-public-research] "
        f"features={len([name for name in summary['columns'] if name.startswith('twpub_')])} "
        f"xbrl_events={summary['xbrl']['events']} "
        f"macro_events={summary['macro']['events']} "
        f"reused={str(bool(summary.get('reused'))).lower()} "
        f"output={summary['output_path']}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

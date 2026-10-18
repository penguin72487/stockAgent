#!/usr/bin/env python3
"""Build a local-only TW research table with FinLab-observed raw features."""

from __future__ import annotations

import argparse
from contextlib import nullcontext
import fcntl
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.data.finlab_research_overlay import build_finlab_research_overlay  # noqa: E402
from stockagent.live.tw_public_source_guard import live_tw_public_source_read_guard  # noqa: E402

CANONICAL_BASE = ROOT / "artifacts/research_features/tw_public_research_all_2014_v3.parquet"


def refresh_canonical_base(base_path: Path) -> None:
    """Use the existing union builder, never rewrite its lineage receipt.

    The full source/output hashes in that builder make unchanged inputs a no-op.
    This only reconciles its original wide + formal inputs; it cannot change
    PIT eligibility, selected columns, source facts, or the research ABI.
    """
    if base_path.absolute() != CANONICAL_BASE:
        raise ValueError("canonical base refresh cannot overwrite a custom research input")
    subprocess.run([sys.executable, str(ROOT / "scripts/build_tw_public_research_all_features.py"),
                    "--official-path", str(ROOT / "data_tw_public/features/tw_public_stock_daily.parquet"),
                    "--output-path", str(base_path)], cwd=ROOT, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--finlab-root", type=Path, default=ROOT / "data_finlab")
    parser.add_argument(
        "--base-path", type=Path,
        default=CANONICAL_BASE,
    )
    parser.add_argument("--refresh-canonical-base", action="store_true",
                        help="reconcile the original union inputs before the overlay; custom bases are rejected")
    parser.add_argument(
        "--output-path", type=Path,
        default=ROOT / "artifacts/research_features/tw_public_research_finlab_2014_v4.parquet",
    )
    args = parser.parse_args()
    lock = ROOT / "artifacts/data_locks/tw_public_research_finlab_2014_v4.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    guard = (live_tw_public_source_read_guard(ROOT / "data_tw_public/stocks")
             if args.refresh_canonical_base else nullcontext())
    with guard:
        if args.refresh_canonical_base:
            refresh_canonical_base(args.base_path)
        with lock.open("a+") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            summary = build_finlab_research_overlay(
                finlab_root=args.finlab_root, base_path=args.base_path, output_path=args.output_path,
            )
    print(
        f"[finlab-research] rows={summary['rows']} "
        f"features={len(summary['feature_columns'])} "
        f"sources={len(summary['inputs']['sources'])} "
        f"reused={str(summary['reused']).lower()} output={summary['output_path']}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

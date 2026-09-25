#!/usr/bin/env python3
"""Build a local-only TW research table with FinLab-observed raw features."""

from __future__ import annotations

import argparse
import fcntl
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.data.finlab_research_overlay import build_finlab_research_overlay  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--finlab-root", type=Path, default=ROOT / "data_finlab")
    parser.add_argument(
        "--base-path", type=Path,
        default=ROOT / "artifacts/research_features/tw_public_research_all_2014_v3.parquet",
    )
    parser.add_argument(
        "--output-path", type=Path,
        default=ROOT / "artifacts/research_features/tw_public_research_finlab_2014_v4.parquet",
    )
    args = parser.parse_args()
    lock = ROOT / "artifacts/data_locks/tw_public_research_finlab_2014_v4.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
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

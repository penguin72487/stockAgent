#!/usr/bin/env python3
"""Build the complete locally observed TW public research feature union."""

from __future__ import annotations

import argparse
import fcntl
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.data.tw_public_research_all_features import build_all_research_features  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--wide-path", type=Path,
        default=Path("artifacts/research_features/tw_public_research_wide_2014_taifex_v2.parquet"),
    )
    parser.add_argument(
        "--official-path", type=Path,
        default=Path("data_tw_public/features/tw_public_stock_daily.parquet"),
    )
    parser.add_argument(
        "--output-path", type=Path,
        default=Path("artifacts/research_features/tw_public_research_all_2014_v3.parquet"),
    )
    args = parser.parse_args()
    lock = ROOT / "artifacts/data_locks/tw_public_research_all_2014_v3.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    with lock.open("a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        summary = build_all_research_features(
            wide_path=args.wide_path,
            official_path=args.official_path,
            output_path=args.output_path,
        )
    print(
        f"[tw-public-research-all] rows={summary['rows']} "
        f"twpub_columns={len(summary['twpub_columns'])} "
        f"reconstructed={len(summary['reconstructed_columns'])} "
        f"reused={str(summary['reused']).lower()} output={summary['output_path']}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

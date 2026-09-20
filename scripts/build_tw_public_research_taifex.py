#!/usr/bin/env python3
"""Build a separate TAIFEX-enriched TW preopen research feature table."""

from __future__ import annotations

import argparse
import fcntl
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.data.tw_public_research_taifex import build_taifex_research_features  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-path", type=Path, default=Path("data_tw_public/features/tw_public_research_wide_2014_v1.parquet"))
    parser.add_argument("--history-path", type=Path, default=Path("data_taifex_public_history/normalized/put_call_ratio.parquet"))
    parser.add_argument("--futures-path", type=Path, default=Path("data_tw_index_futures/all_futures_daily_sessions.parquet"))
    parser.add_argument("--final-settlement-path", type=Path, default=Path("data_tw_futures/final_settlement_v1/futures_final_settlement_history.parquet"))
    parser.add_argument("--output-path", type=Path, default=Path("artifacts/research_features/tw_public_research_wide_2014_taifex_v2.parquet"))
    args = parser.parse_args()
    lock = ROOT / "artifacts/data_locks/tw_public_research_taifex_v2.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    with lock.open("a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        summary = build_taifex_research_features(
            base_path=args.base_path, history_path=args.history_path,
            futures_path=args.futures_path, final_settlement_path=args.final_settlement_path,
            output_path=args.output_path,
        )
    print(
        f"[tw-public-research-taifex] available_rows={summary['available_rows']} "
        f"source_2014_rows={summary['source_2014_rows']} "
        f"tx_2014_days={summary['tx_source_2014_days']} "
        f"tx_final_2014_days={summary['tx_monthly_final_2014_days']} "
        f"reused={str(summary['reused']).lower()} output={summary['output_path']}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

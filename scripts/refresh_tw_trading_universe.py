#!/usr/bin/env python3
"""Receipt current official trading membership; no inference, ledger or order."""
from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data.tw_current_trading_universe import refresh_current_universe


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-date", default=datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat())
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--parquet-root", default="data_tw_public/stocks")
    args = parser.parse_args()
    result = refresh_current_universe(args.session_date, force=args.force, parquet_root=args.parquet_root)
    print(json.dumps({k: result[k] for k in ("contract", "session_date", "observed_at_taipei", "listed_count", "halted_count", "trading_count", "fingerprint", "elapsed_ms")}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

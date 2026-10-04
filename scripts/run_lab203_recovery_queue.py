#!/usr/bin/env python3
"""Poll data-only NAS recovery requests after the existing lab203 backup owner."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.backup_delivery_receipt import read_json  # noqa: E402
from stockagent.data_sync.offhost_backup import private_file  # noqa: E402
from stockagent.data_sync.recovery_queue import run_cycle  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--configuration", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = run_cycle(read_json(private_file(args.configuration)))
    except BlockingIOError:
        print(json.dumps({"state": "existing_backup_owner_busy"}))
        return 75
    except Exception as error:
        print(json.dumps({"state": "recovery_queue_failed", "error_type": type(error).__name__}))
        return 1
    print(json.dumps({k: result[k] for k in ("contract", "plans", "observed_at_utc", "complete_cycle_seconds")}))
    return 1 if any(p["state"] in {"rejected", "retry_wait"} for p in result["plans"]) else 0


if __name__ == "__main__":
    raise SystemExit(main())

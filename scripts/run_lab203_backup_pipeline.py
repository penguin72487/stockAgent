#!/usr/bin/env python3
"""Run the locally installed concurrent relay under the current service owner."""
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT))
from stockagent.data_sync.backup_relay_pipeline import run_cycle


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--configuration", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = run_cycle(json.loads(args.configuration.read_bytes()))
    except BlockingIOError:
        print(json.dumps({"state": "existing_owner_busy"}))
        raise SystemExit(75) from None
    except Exception as error:
        print(json.dumps({"state": "pipeline_failed", "error_type": type(error).__name__}))
        raise SystemExit(1) from None
    print(json.dumps({k: result[k] for k in ("contract", "observed_at_utc", "complete_cycle_seconds")},ensure_ascii=False))


if __name__ == "__main__":
    main()

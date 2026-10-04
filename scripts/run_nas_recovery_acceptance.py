#!/usr/bin/env python3
"""Run named NAS semantic recovery locally on lab203, or validate its returned receipt."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.backup_delivery_receipt import read_json  # noqa: E402
from stockagent.data_sync.nas_recovery_acceptance import run_acceptance, validate_acceptance  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    modes = parser.add_subparsers(dest="mode", required=True)
    run = modes.add_parser("run")
    for name in ("nas-configuration", "restic", "password-file", "runtime-lock", "owner-lock", "output", "receipt-root"):
        run.add_argument("--" + name, type=Path, required=True)
    run.add_argument("--pg-bin", type=Path)
    run.add_argument("--wait-for-owner", action="store_true")
    review = modes.add_parser("validate")
    review.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    plan = read_json(args.plan)
    if args.mode == "validate":
        result = read_json(args.receipt)
        validate_acceptance(result, plan)
    else:
        result = run_acceptance(plan, nas_configuration=args.nas_configuration, restic=args.restic,
            password_file=args.password_file, runtime_lock=args.runtime_lock, owner_lock=args.owner_lock,
            output=args.output, receipt_root=args.receipt_root, pg_bin=args.pg_bin, wait_for_owner=args.wait_for_owner)
    print(json.dumps({k: result[k] for k in ("contract", "plan_identity_sha256", "identity_sha256", "complete_workflow_seconds")}))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(type(error).__name__, file=sys.stderr)
        raise SystemExit(1)

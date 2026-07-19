#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

import pyarrow.parquet as pq


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect the all-market OpenBB archive completeness contract."
    )
    parser.add_argument("--output-dir", type=Path, default=Path("data_openBB"))
    parser.add_argument("--fail-on-unresolved", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def run(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    summary_path = args.output_dir / "catalog" / "completeness_contract_summary.json"
    audit_path = args.output_dir / "catalog" / "completeness_contract_audit.parquet"
    if not summary_path.is_file() or not audit_path.is_file():
        raise FileNotFoundError(
            "Completeness contract artifacts do not exist; run the archive planner first."
        )
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    table = pq.read_table(audit_path)
    unresolved = [
        row for row in table.to_pylist() if str(row.get("status")) == "unresolved"
    ]
    payload = {**summary, "unresolved_samples": unresolved[:100]}
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(
            "[openbb-contract] "
            f"passed={summary.get('passed')} rows={summary.get('contract_rows', 0):,} "
            f"unresolved={summary.get('unresolved', 0):,} "
            f"by_axis={summary.get('unresolved_by_axis', {})}"
        )
        for row in unresolved[:25]:
            print(
                "[openbb-contract-unresolved] "
                f"{row['endpoint']} provider={row['provider']} "
                f"axis={row['axis']} field={row['field']} evidence={row['evidence']}"
            )
    return 2 if args.fail_on_unresolved and unresolved else 0


if __name__ == "__main__":
    raise SystemExit(run())

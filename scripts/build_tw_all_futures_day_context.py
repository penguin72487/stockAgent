#!/usr/bin/env python3
"""Build causal all-root TAIFEX regular and after-hours context panels."""

from __future__ import annotations

import argparse
from collections import defaultdict
import csv
import hashlib
import json
from pathlib import Path
import sys

import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data.tw_all_futures import build_taifex_all_futures_front_panels


def _write_inventory_and_manifest(
    output: Path,
    *,
    sources: list[Path],
    session: str,
) -> dict[str, object]:
    table = pq.read_table(output, columns=["date", "root"])
    payload = table.to_pydict()
    if table.num_rows == 0:
        raise RuntimeError(f"built {session} TAIFEX context is empty: {output}")
    roots = sorted(set(payload["root"]))
    root_dates: dict[str, list[object]] = defaultdict(list)
    for root, trading_date in zip(payload["root"], payload["date"], strict=True):
        root_dates[str(root)].append(trading_date)
    stem = f"all_products_{'day_session' if session == 'regular' else 'afterhours'}_front"
    inventory_path = output.with_name(f"{stem}_inventory.csv")
    with inventory_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(("root", "first_date", "last_date", "rows"))
        for root in roots:
            dates = root_dates[root]
            writer.writerow((root, min(dates), max(dates), len(dates)))
    manifest_path = output.with_name(f"{stem}_manifest.json")
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "session": session,
                "output": str(output),
                "sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
                "inventory": str(inventory_path),
                "inventory_sha256": hashlib.sha256(
                    inventory_path.read_bytes()
                ).hexdigest(),
                "rows": table.num_rows,
                "roots": len(roots),
                "first_date": str(min(payload["date"])),
                "last_date": str(max(payload["date"])),
                "source_files": [str(path) for path in sources],
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return {
        "session": session,
        "output": str(output),
        "rows": table.num_rows,
        "roots": len(roots),
        "first_date": str(min(payload["date"])),
        "last_date": str(max(payload["date"])),
        "root_first": roots[:10],
        "root_last": roots[-10:],
        "inventory": str(inventory_path),
        "manifest": str(manifest_path),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--raw-root",
        type=Path,
        default=Path("data_tw_index_futures/raw"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data_tw_index_futures/all_products_day_session_front.parquet"),
    )
    parser.add_argument(
        "--afterhours-output",
        type=Path,
        default=Path("data_tw_index_futures/all_products_afterhours_front.parquet"),
    )
    parser.add_argument(
        "--session",
        choices=("both", "regular", "afterhours"),
        default="both",
        help="Build both contexts in one raw-file pass, or only the missing one.",
    )
    args = parser.parse_args()
    sources = sorted((args.raw_root / "annual").glob("*")) + sorted(
        (args.raw_root / "ranges").glob("*")
    )
    requested_sessions = (
        ("regular", "afterhours")
        if args.session == "both"
        else (args.session,)
    )
    output_paths = {
        "regular": args.output,
        "afterhours": args.afterhours_output,
    }
    outputs = build_taifex_all_futures_front_panels(
        sources,
        {session: output_paths[session] for session in requested_sessions},
    )
    summaries = [
        _write_inventory_and_manifest(
            outputs[session],
            sources=sources,
            session=session,
        )
        for session in requested_sessions
    ]
    print(
        json.dumps(
            {"outputs": summaries},
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Compare one regular session's option-family counts in two retained sources.

Counts are source side-rows, not matched trades, fills, liquidity or a full
event-level comparison. This does not produce training observations.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import zipfile

import polars as pl

from downloader.artifact_io import atomic_write_json

WINDOWS = {"0845_0905": ("084500", "090500"), "day_0845_1345": ("084500", "134500")}


def _sha(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(1024 * 1024):
            result.update(chunk)
    return result.hexdigest()


def _official_counts(archive: Path, session: date) -> dict:
    counts = {key: defaultdict(lambda: [0, None]) for key in WINDOWS}
    event_date = session.strftime("%Y%m%d").encode()
    with zipfile.ZipFile(archive) as z:
        members = [x for x in z.infolist() if x.filename.lower().endswith(".csv") and not x.is_dir()]
        if len(members) != 1:
            raise ValueError("expected exactly one official option CSV member")
        with z.open(members[0]) as f:
            if not f.readline() or not f.readline().startswith(b"---"):
                raise ValueError("unexpected official header")
            for line in f:
                cells = [cell.strip() for cell in line.split(b",")]
                if len(cells) < 8 or len(cells[0]) != 8 or not cells[0].isdigit():
                    raise ValueError("invalid official row date or width")
                # TAIFEX's trading-day archive can include the prior calendar
                # date's night session. Only this calendar date's day session
                # is comparable to the corresponding FinMind object.
                if cells[0] != event_date:
                    continue
                product, clock = cells[1].decode("ascii"), cells[5].decode("ascii")
                if not product or len(clock) != 6 or not clock.isdigit():
                    raise ValueError("invalid official product or time")
                for window, (start, end) in WINDOWS.items():
                    if start <= clock < end:
                        item = counts[window][product]
                        item[0] += 1
                        item[1] = clock if item[1] is None else min(item[1], clock)
    return counts


def audit(session: date, data_root: Path, finmind_root: Path) -> dict:
    tag = session.isoformat()
    tick_manifest = data_root / "manifest.json"
    manifest_bytes = tick_manifest.read_bytes()
    manifest = json.loads(manifest_bytes)
    entries = [x for x in manifest["raw_downloads"] if x["kind"] == "options" and x["trading_date"] == tag]
    if len(entries) != 1:
        raise ValueError("official source is absent or ambiguous in tick manifest")
    official_path = Path(entries[0]["path"])
    if _sha(official_path) != entries[0]["sha256"]:
        raise ValueError("official source SHA mismatch")
    receipt_path = finmind_root / "receipts/TaiwanOptionTick/all" / f"{tag}.json"
    receipt_bytes = receipt_path.read_bytes()
    receipt = json.loads(receipt_bytes)
    if receipt.get("status") != "complete" or receipt.get("partition") != tag:
        raise ValueError("FinMind source receipt is not complete for requested day")
    parquet_path = finmind_root / receipt["parquet_path"]
    if _sha(parquet_path) != receipt["sha256"]:
        raise ValueError("FinMind parquet SHA mismatch")

    official = _official_counts(official_path, session)
    source = pl.scan_parquet(parquet_path)
    windows = {}
    for window, (start, end) in WINDOWS.items():
        def iso(clock: str) -> str:
            return f"{tag} {clock[:2]}:{clock[2:4]}:{clock[4:]}"
        grouped = source.filter(
            pl.col("date").is_between(pl.lit(iso(start)), pl.lit(iso(end)), closed="left")
        ).group_by("option_id").agg(
            pl.len().alias("rows"), pl.col("date").min().alias("first")
        ).collect()
        observed = {row["option_id"]: [row["rows"], row["first"][-8:].replace(":", "")] for row in grouped.to_dicts()}
        results = []
        for product in sorted(set(official[window]) | set(observed)):
            left, right = official[window].get(product, [0, None]), observed.get(product, [0, None])
            results.append({"product": product, "official_side_rows": left[0], "finmind_source_rows": right[0], "official_first_hhmmss": left[1], "finmind_first_hhmmss": right[1], "counts_and_first_equal": left == right})
        windows[window] = {"products": len(results), "all_counts_and_first_equal": all(r["counts_and_first_equal"] for r in results), "families": results}
    if _sha(official_path) != entries[0]["sha256"] or _sha(parquet_path) != receipt["sha256"] or tick_manifest.read_bytes() != manifest_bytes or receipt_path.read_bytes() != receipt_bytes:
        raise ValueError("source changed during audit")
    return {
        "contract": "taifex_finmind_option_family_clock_sample_v1",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(), "session_date": tag,
        "official_source": str(official_path.resolve()), "official_source_sha256": entries[0]["sha256"],
        "official_manifest": str(tick_manifest.resolve()), "official_manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "finmind_source": str(parquet_path.resolve()), "finmind_source_sha256": receipt["sha256"],
        "finmind_receipt": str(receipt_path.resolve()), "finmind_receipt_sha256": hashlib.sha256(receipt_bytes).hexdigest(),
        "audit_script_sha256": _sha(Path(__file__)), "windows": windows,
        "limitations": [
            "Group-level row counts and earliest event only; no price, quantity, contract identity or full event parity was tested.",
            "Source rows preserve side records and duplicate events: counts are not independent matched trades.",
            "A print at the decision second is not proof of an executable fill after a decision; no BidAsk is tested.",
            "One regular session does not establish coverage or liquidity for other dates or other physical contracts.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--tick-root", type=Path, default=Path("data_tw_index_derivatives_ticks"))
    parser.add_argument("--finmind-root", type=Path, default=Path("data_finmind/complement"))
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = audit(args.date, args.tick_root, args.finmind_root)
    atomic_write_json(args.output, result)
    print(json.dumps({"output": str(args.output), "windows": {k: {"products": v["products"], "match": v["all_counts_and_first_equal"]} for k, v in result["windows"].items()}}))


if __name__ == "__main__":
    main()

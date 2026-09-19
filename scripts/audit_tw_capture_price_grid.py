"""Audit raw TW cash-market capture prices independently of the mutable universe.

The complete capture audit also verifies the historical universe SHA; older
universe CSVs may no longer be available.  This narrower receipt checks the
capture selected by its immutable worker manifests and each quote grid without
claiming the original universe or source completeness was verified.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from downloader.shioaji_capture_parts import (
    read_capture_manifest_groups,
    select_capture_part_paths,
    shared_capture_id,
)
from scripts.audit_shioaji_microstructure import _price_grid_counts
from stockagent.data.tw_exchange_price_classification import classify_tw_exchange_security
from stockagent.data.tw_price_rules import TW_ORDER_PRICE_CONTRACT_VERSION


CAPTURE_FIELDS = {
    "ticks": (
        "open", "high", "low", "close", "closing_oddlot_close",
        "closing_oddlot_bid_price", "closing_oddlot_ask_price",
    ),
    "book_events": tuple(
        [f"bid_price_{i}" for i in range(1, 6)]
        + [f"ask_price_{i}" for i in range(1, 6)]
    ),
    "book_1s": tuple(
        [f"bid_price_{i}" for i in range(1, 6)]
        + [f"ask_price_{i}" for i in range(1, 6)]
    ),
}


def _one_session(root: Path, date: str, session: str | None, manifests: list[dict]) -> dict:
    if len(manifests) != 2 or any(
        item.get("source") != "shioaji_streaming_v1"
        or item.get("status") != "complete"
        or int(item.get("dropped_events", -1)) != 0
        for item in manifests
    ):
        raise RuntimeError("capture worker manifests are incomplete")
    capture_id = shared_capture_id(manifests)
    result = {
        "date": date,
        "session": session,
        "capture_id": capture_id,
        "manifest_universe_sha256": sorted(set(str(m.get("universe_sha256")) for m in manifests)),
        "historical_universe_sha256_verified": False,
        "groups": {},
    }
    if len(result["manifest_universe_sha256"]) != 1:
        raise RuntimeError("worker universe digests disagree")
    capture_root = root / "data_tw_microstructure/captures"
    for group, fields in CAPTURE_FIELDS.items():
        provenance_error = None
        try:
            paths = select_capture_part_paths(
                capture_root=capture_root,
                kind=group,
                trade_date=date,
                manifests=manifests,
            )
        except RuntimeError as exc:
            if "part count mismatch" not in str(exc):
                raise
            provenance_error = str(exc)
            # Older schema-2 captures cannot distinguish parts from a rerun
            # with overlapping worker time windows. Inspect every selected
            # price, but never certify the manifest provenance.
            paths = select_capture_part_paths(
                capture_root=capture_root,
                kind=group,
                trade_date=date,
                manifests=manifests,
                verify_part_counts=False,
            )
        if not paths:
            raise RuntimeError(f"no selected capture parts for {group}")
        source_stats = [(str(path), path.stat().st_size, path.stat().st_mtime_ns) for path in paths]
        frame = pl.scan_parquet([str(p) for p in paths], missing_columns="raise")
        identities = frame.select("exchange", "code").unique().collect()
        kind_rows = []
        unknown = []
        for exchange, code in identities.iter_rows():
            venue = {"TSE": "twse", "OTC": "tpex"}.get(str(exchange))
            kind = classify_tw_exchange_security(venue or "", code)
            kind_rows.append({"exchange": exchange, "code": code, "_kind": kind})
            if kind is None:
                unknown.append([exchange, code])
        if unknown:
            raise RuntimeError(f"unclassified capture symbols: {unknown[:12]}")
        checked = frame.join(pl.DataFrame(kind_rows).lazy(), on=["exchange", "code"], how="left")
        bad = _price_grid_counts(checked, fields)
        negative_or_infinite = checked.select(
            [(
                ((pl.col(field).cast(pl.Float64) < 0) | pl.col(field).cast(pl.Float64).is_infinite())
                .sum()
            ).alias(field) for field in fields]
        ).collect().row(0, named=True)
        violations = sum(bad.values()) + sum(int(n or 0) for n in negative_or_infinite.values())
        digest = hashlib.sha256()
        for item in sorted(source_stats):
            digest.update(f"{item[0]}\0{item[1]}\0{item[2]}\n".encode())
        result["groups"][group] = {
            "parts": len(paths),
            "rows": int(frame.select(pl.len()).collect().item()),
            "source_set_stat_sha256": digest.hexdigest(),
            "off_grid_by_field": bad,
            "negative_or_infinite_by_field": {
                k: int(v or 0) for k, v in negative_or_infinite.items()
            },
            "violations": violations,
            "manifest_part_count_error": provenance_error,
        }
    result["status"] = (
        "source_value_problem"
        if any(x["violations"] for x in result["groups"].values())
        else "source_provenance_ambiguous"
        if any(x["manifest_part_count_error"] for x in result["groups"].values())
        else "quote_grid_valid_universe_unverified"
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date", help="restrict to one YYYY-MM-DD date")
    parser.add_argument(
        "--output", type=Path,
        default=ROOT / "artifacts/data_quality/tw_price_precision/capture_price_grid.json",
    )
    args = parser.parse_args()
    capture_root = ROOT / "data_tw_microstructure/captures"
    dates = [args.trade_date] if args.trade_date else sorted(
        folder.name.split("=", 1)[1]
        for folder in (capture_root / "manifests").glob("trade_date=*") if folder.is_dir()
    )
    report = {
        "audit_version": 1,
        "cash_price_contract_version": TW_ORDER_PRICE_CONTRACT_VERSION,
        "generated_at_utc": None,
        "scope": "all_selected_capture_prices" if not args.trade_date else "single_capture_date",
        "status": "in_progress",
        "sessions": [],
        "source_semantics": "Only trade/quote prices, not avg_price, settlement, changes or source coverage",
    }
    for date in dates:
        for session, manifests in read_capture_manifest_groups(capture_root, date):
            try:
                result = _one_session(ROOT, date, session, manifests)
            except Exception as exc:
                result = {"date": date, "session": session, "status": "audit_failed", "error": str(exc)}
            report["sessions"].append(result)
            report["generated_at_utc"] = datetime.now(timezone.utc).isoformat()
            atomic_write_json(args.output, report)
            print(json.dumps({"date": date, "session": session, "status": result["status"]}), flush=True)
    states = {x["status"] for x in report["sessions"]}
    if states == {"quote_grid_valid_universe_unverified"}:
        report["status"] = "quote_grid_valid_universe_unverified"
    elif "source_value_problem" in states:
        report["status"] = "source_value_problem"
    elif "audit_failed" in states:
        report["status"] = "audit_failed"
    elif "source_provenance_ambiguous" in states:
        report["status"] = "source_provenance_ambiguous"
    else:
        report["status"] = "no_manifest_sessions"
    atomic_write_json(args.output, report)
    return 0 if report["status"] == "quote_grid_valid_universe_unverified" else 2


if __name__ == "__main__":
    raise SystemExit(main())

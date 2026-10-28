#!/usr/bin/env python3
"""Reconcile a frozen TWD futures catalogue with a later official daily snapshot.

This read-only audit does not add an instrument to a training release. Unknown
codes are reported explicitly; the dated currency review below is evidence for
these specific codes, not a rule that infers currency from the symbol.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data_tw_index_futures/preparation_sources/universe765_repair_20261027_v1"
REPORTS = ROOT / "artifacts/markets/tw_futures_universe765_recovery_20261027_v1"
INVENTORY = REPORTS / "source_inventory.csv"
DAILY = DATA / "all_reported_daily_forward_20261027_v2/all_futures_daily_sessions.parquet"
DAILY_MANIFEST = DAILY.parent / "manifest.json"
CURRENCY_2025 = DATA / "remaining_margin_review_v1/documents/360dc44bfb7e732c6766e8374f2894cbc27468662d176d5a2757b97d460ce726"
CURRENCY_2011 = DATA / "remaining_margin_review_v1/documents/73bb1db1b03ab7daa129d19c9b4ffc8ae7322bf0ddc14667d4d55d2359b7394e"
PK1_REPORT = REPORTS / "pk1_forward_clock_readback_v2.json"

# The source review established these currencies for these individual codes.
# Monetary margin alone is not used as proof of quote currency: see the listed
# exchange contract references as well as the independently pinned notices.
REVIEW = {
    "GDF": ("foreign_currency", "USD", "https://www.taifex.com.tw/cht/2/gDF"),
    "MSF": ("foreign_currency", "USD", "https://www.taifex.com.tw/chinese/9/%E8%82%A1%E5%83%B9%E6%8C%87%E6%95%B8%E9%A1%9E%E5%8F%8A%E8%82%A1%E7%A5%A8%E9%A1%9E%E5%A5%91%E7%B4%84%E6%9C%80%E5%BE%8C%E7%B5%90%E7%AE%97%E5%83%B9%E5%8F%96%E4%BD%8D%E6%96%B9%E5%BC%8F%E8%AA%BF%E6%95%B4.pdf"),
    "RHF": ("foreign_currency", "CNY", "https://www.taifex.com.tw/cht/5/fXFSP"),
    "RTF": ("foreign_currency", "CNY", "https://www.taifex.com.tw/cht/5/fXFSP"),
    "XAF": ("foreign_currency", "USD", "https://www.taifex.com.tw/cht/5/fXFSP"),
    "XBF": ("foreign_currency", "USD", "https://www.taifex.com.tw/cht/5/fXFSP"),
    "XEF": ("foreign_currency", "USD", "https://www.taifex.com.tw/cht/5/fXFSP"),
    "XJF": ("foreign_currency", "JPY", "https://www.taifex.com.tw/cht/5/fXFSP"),
    "PK1": ("new_twd_adjusted_code_candidate", "TWD", "https://www.taifex.com.tw/cht/4/sSFRefferedOpeningPrice"),
}


def sha256(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1 << 20), b""):
            result.update(chunk)
    return result.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cutoff", type=date.fromisoformat, default=date(2026, 9, 24))
    parser.add_argument("--as-of", type=date.fromisoformat, default=date(2026, 10, 27))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.as_of <= args.cutoff:
        parser.error("as-of must follow cutoff")
    if args.cutoff != date(2026, 9, 24):
        parser.error("the selected inventory is frozen at the 2026-09-24 cutoff")
    output = args.output.resolve()
    if not output.is_relative_to(REPORTS.resolve()):
        parser.error("output must be within the formal recovery artifact root")

    inventory = pl.read_csv(INVENTORY, columns=["product"])
    if inventory["product"].null_count() or inventory["product"].n_unique() != len(inventory):
        raise ValueError("catalogue product identities must be unique and present")
    daily_manifest = json.loads(DAILY_MANIFEST.read_text())
    expected_hash = daily_manifest["outputs"][DAILY.name]["sha256"]
    if sha256(DAILY) != expected_hash:
        raise ValueError("official daily snapshot differs from its manifest")
    snapshot_max = pl.scan_parquet(DAILY).select(pl.col("date").max()).collect().item()
    if args.as_of > snapshot_max:
        parser.error(f"as-of exceeds the latest retained official source date {snapshot_max}")
    if sha256(CURRENCY_2025 / "source.pdf") != CURRENCY_2025.name:
        raise ValueError("2025 exchange notice differs from its retained identity")
    if sha256(CURRENCY_2011 / "source.pdf") != CURRENCY_2011.name:
        raise ValueError("2011 exchange notice differs from its retained identity")
    catalogue_observed = pl.scan_parquet(DAILY).filter(
        (pl.col("date") <= args.cutoff) & (pl.col("series_type") == "monthly")
    ).select("product").unique()
    missing_catalogue = (inventory.lazy().join(catalogue_observed, on="product", how="anti")
                         .sort("product").collect()["product"].to_list())
    source = pl.scan_parquet(DAILY).filter(
        (pl.col("date") <= args.as_of) & (pl.col("series_type") == "monthly")
    ).join(inventory.lazy(), on="product", how="anti")
    grouped = source.group_by("product").agg(
        pl.col("date").min().alias("first_date"),
        pl.col("date").max().alias("last_date"),
        pl.len().alias("all_session_contract_rows"),
        ((pl.col("session") == "一般") & (pl.col("volume").fill_null(0) > 0))
        .sum().alias("general_positive_contract_days"),
        ((pl.col("date") > args.cutoff) & (pl.col("session") == "一般"))
        .sum().alias("forward_general_contract_rows"),
        ((pl.col("date") > args.cutoff) & (pl.col("session") == "一般")
         & (pl.col("volume").fill_null(0) > 0)).sum().alias("forward_general_positive_contract_days"),
    ).sort("product").collect()
    candidates = []
    for row in grouped.iter_rows(named=True):
        kind, currency, reference = REVIEW.get(row["product"], ("unreviewed", None, None))
        row.update(classification=kind, currency=currency, exchange_reference=reference)
        candidates.append(row)
    pinned = [Path(__file__).resolve(), INVENTORY, DAILY, DAILY_MANIFEST, PK1_REPORT,
              CURRENCY_2025 / "source.pdf", CURRENCY_2025 / "receipt.json",
              CURRENCY_2025 / "page_003_native.json", CURRENCY_2025 / "page_004_native.json",
              CURRENCY_2025 / "page_006_native.json", CURRENCY_2011 / "source.pdf",
              CURRENCY_2011 / "candidate.txt", CURRENCY_2011 / "receipt.json"]
    payload = {
        "contract": "tw_futures_forward_scope_source_audit_v1",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "catalogue_products": len(inventory),
        "catalogue_cutoff": args.cutoff,
        "catalogue_codes_without_reported_monthly_rows_by_cutoff": missing_catalogue,
        "official_snapshot_as_of": args.as_of,
        "series_scope": "reported monthly contract rows; all sessions",
        "outside_catalogue": candidates,
        "unreviewed_codes": [x["product"] for x in candidates if x["classification"] == "unreviewed"],
        "limitations": [
            "Reported product codes are not proof of complete exchange listing history.",
            "A daily positive print is not a decision-window fill or financial admission.",
            "A later adjusted code requires its own source-backed position transfer.",
            "No data publication or training admission is performed by this audit.",
        ],
        "input_pins": {str(path.relative_to(ROOT)): sha256(path) for path in pinned},
    }
    with output.open("x", encoding="utf-8") as target:
        json.dump(payload, target, ensure_ascii=False, indent=2, default=str)
        target.write("\n")
    print(f"{output}: {len(candidates)} outside catalogue; {len(payload['unreviewed_codes'])} unreviewed")


if __name__ == "__main__":
    main()

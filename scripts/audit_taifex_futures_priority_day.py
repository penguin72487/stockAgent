#!/usr/bin/env python3
"""Compare a receipted official daily futures source with a fixed tick archive.

This measures source-key presence only: it cannot establish execution at a
decision clock, historical listing, contract terms, or account readiness.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import io
import json
from pathlib import Path
import re
import zipfile

from downloader.artifact_io import atomic_write_json, sha256_file


def audit(receipt: Path, tick_manifest: Path, catalog: Path, catalog_sha: str, output: Path,
          *, finmind_root: Path | None = None) -> dict:
    if output.exists() or output.is_symlink():
        raise FileExistsError("preserve existing source audit")
    pins = {str(path): sha256_file(path) for path in (receipt, tick_manifest, catalog)}
    if pins[str(catalog)] != catalog_sha:
        raise ValueError("catalog hash mismatch")
    source = json.loads(receipt.read_text(encoding="utf-8"))
    if source.get("contract") != "taifex_futures_daily_priority_source_v1":
        raise ValueError("unsupported daily receipt")
    day = source["request"]["queryStartDate"].replace("/", "-")
    if source["request"]["queryEndDate"] != source["request"]["queryStartDate"]:
        raise ValueError("daily receipt is not a single day")
    daily = Path(source["path"])
    if daily.is_symlink() or daily.stat().st_size != source["bytes"] or sha256_file(daily) != source["sha256"]:
        raise ValueError("daily source differs from receipt")
    pins[str(daily)] = source["sha256"]
    manifest = json.loads(tick_manifest.read_text(encoding="utf-8"))
    entries = [row for row in manifest["raw_downloads"]
               if row["kind"] == "futures" and row["trading_date"] == day]
    if manifest.get("status") != "complete" or len(entries) != 1:
        raise ValueError("fixed tick manifest does not identify one futures archive")
    tick = Path(entries[0]["path"])
    if tick.is_symlink() or tick.stat().st_size != entries[0]["bytes"] or sha256_file(tick) != entries[0]["sha256"]:
        raise ValueError("tick archive differs from receipt")
    pins[str(tick)] = entries[0]["sha256"]
    with catalog.open(encoding="utf-8", newline="") as stream:
        catalog_rows = list(csv.DictReader(stream))
    categories = {row["product"]: row["asset_class"] for row in catalog_rows}
    if len(categories) != len(catalog_rows):
        raise ValueError("duplicate historical catalog code")
    positive: set[tuple[str, str]] = set()
    products: set[str] = set()
    rows = 0
    unknown_volume = 0
    with daily.open(encoding="cp950", newline="") as stream:
        for row in csv.DictReader(stream):
            rows += 1
            if row["交易日期"].strip().replace("/", "-") != day:
                raise ValueError("daily source contains another date")
            product, contract, session = (row[field].strip() for field in ("契約", "到期月份(週別)", "交易時段"))
            products.add(product)
            volume = row["成交量"].strip().replace(",", "")
            if not volume.isdigit():
                unknown_volume += 1
            elif session == "一般" and re.fullmatch(r"\d{6}", contract) and int(volume) > 0:
                positive.add((product, contract))
    if rows != source["source_rows"] or len(products) != source["source_product_count"]:
        raise ValueError("daily source count differs from receipt")
    observed = set()
    tick_rows = 0
    physical_dates: Counter[str] = Counter()
    with zipfile.ZipFile(tick) as archive:
        members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if len(members) != 1:
            raise ValueError("expected one native tick CSV")
        with archive.open(members[0]) as raw, io.TextIOWrapper(raw, encoding="cp950", newline="") as stream:
            for row in csv.DictReader(stream):
                tick_rows += 1
                physical_dates[row["成交日期"].strip()] += 1
                key = (row["商品代號"].strip(), row["到期月份(週別)"].strip())
                if key not in positive:
                    continue
                try:
                    price = Decimal(row["成交價格"].strip())
                    quantity = Decimal(row["成交數量(B+S)"].strip())
                except InvalidOperation:
                    continue
                if price.is_finite() and quantity.is_finite() and price > 0 and quantity > 0:
                    observed.add(key)
    unmatched = positive - observed
    finmind_comparison = None
    if finmind_root is not None:
        # Reuse the canonical receipt, date and Parquet validation; this is a
        # natural-day presence comparison, not an executable-price check.
        from scripts.audit_taifex_finmind_futures_tick_presence import _load_provider

        frame, info = _load_provider(finmind_root.resolve(), date.fromisoformat(day))
        finmind_comparison = {'contract': 'taifex_finmind_priority_day_presence_v1', **info}
        if frame is not None:
            vendor = {tuple(row) for row in frame.select('product', 'contract').iter_rows()}
            matched = positive & vendor
            union = positive & (observed | vendor)
            finmind_comparison.update({
                'positive_official_keys_found_in_finmind_natural_day': len(matched),
                'positive_official_keys_found_in_either_source': len(union),
                'positive_official_keys_found_only_in_finmind': len(matched - observed),
                'positive_official_keys_missing_from_both_sources': [list(k) for k in sorted(positive - union)],
            })
            pins[info['receipt_path']] = info['receipt_sha256']
            pins[info['payload_path']] = info['payload_sha256']
    if any(sha256_file(Path(path)) != digest for path, digest in pins.items()):
        raise ValueError("input changed during readback")
    result = {
        "contract": "taifex_futures_priority_day_source_presence_v1",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(), "trading_date": day,
        "source_rows": rows, "source_product_count": len(products), "source_product_overlap_with_fixed_catalog": len(products & categories.keys()),
        "source_products_outside_fixed_catalog": sorted(products - categories.keys()),
        "source_products_by_fixed_catalog_asset_class": dict(sorted(Counter(categories.get(p, "outside_fixed_catalog") for p in products).items())),
        "unknown_volume_rows": unknown_volume, "positive_day_session_monthly_contract_keys": len(positive),
        "positive_day_session_monthly_product_count": len({p for p, _ in positive}),
        "positive_keys_found_anywhere_in_trading_date_tick_archive": len(observed),
        "positive_keys_missing_from_trading_date_tick_archive": [list(k) for k in sorted(unmatched)],
        "tick_raw_side_rows": tick_rows, "tick_physical_date_rows": dict(sorted(physical_dates.items())),
        "input_pins": pins, "script_sha256": sha256_file(Path(__file__)),
        "limitations": ["Catalog is a historical TWD-settlement candidate, not an up-to-date quote-currency listing.",
                        "A tick key anywhere in this trading-day archive does not prove a day-session or opening-clock fill.",
                        "Monthly positive-volume keys exclude zero-volume and nonmonthly rows, and do not prove a complete listing.",
                        "This is neither PIT, account, execution nor training admission."],
    }
    if finmind_comparison is not None:
        result['finmind_natural_day_comparison'] = finmind_comparison
        result['limitations'].append(
            'FinMind keys require positive reported volume only; session, positive price and tick completeness are not verified.')
    atomic_write_json(output, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("receipt", "tick-manifest", "catalog", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--catalog-sha", required=True)
    parser.add_argument("--finmind-root", type=Path)
    args = parser.parse_args()
    result = audit(args.receipt.resolve(), args.tick_manifest.resolve(), args.catalog.resolve(), args.catalog_sha,
                   args.output.resolve(), finmind_root=args.finmind_root)
    print(json.dumps({key: result[key] for key in ("trading_date", "source_product_count",
          "source_products_outside_fixed_catalog", "positive_day_session_monthly_contract_keys",
          "positive_keys_found_anywhere_in_trading_date_tick_archive",
          "positive_keys_missing_from_trading_date_tick_archive")}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()

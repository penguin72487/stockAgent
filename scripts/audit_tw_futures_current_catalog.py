"""Compare the freshly observed TAIFEX stock/ETF family list to a pinned list.

The current catalog establishes present membership only; it is never used as a
historical universe or as evidence that a contract had a fill or valid rules.
"""
from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime
import json
from pathlib import Path

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data.tw_stock_futures_catalog import SOURCE_URL, rows_digest


def audit(catalog: Path, previous: Path, previous_receipt: Path, output: Path) -> dict:
    if output.exists():
        raise FileExistsError("preserve the existing current-catalog audit")
    current_sha = sha256_file(catalog)
    doc = json.loads(catalog.read_text())
    if (doc.get("schema_version") != 1 or doc.get("source_url") != SOURCE_URL
            or doc.get("scope") != "latest_catalog_not_signal_date" or doc.get("complete") is not True
            or doc.get("rows_sha256") != rows_digest(doc["rows"])
            or doc.get("row_count") != len(doc["rows"])):
        raise ValueError("latest stock/ETF catalog receipt is invalid")
    raw = catalog.parent / "stock_futures_catalog_sources" / (doc["source_sha256"] + ".html")
    if not raw.is_file() or sha256_file(raw) != doc["source_sha256"]:
        raise ValueError("latest official catalog raw body is missing or changed")
    previous_doc = json.loads(previous_receipt.read_text())
    if (previous_doc.get("contract") != "current_official_family_catalog_membership_readback_v1"
            or previous_doc["output"]["sha256"] != sha256_file(previous)):
        raise ValueError("prior official family listing does not match its receipt")
    with previous.open(newline="", encoding="utf-8") as stream:
        old_rows = [r for r in csv.DictReader(stream)
                    if r["asset_class"] in {"stock_future", "etf_future"}]
    old = {(r["product"], r["underlying_symbol"]) for r in old_rows}
    # The exchange's stockLists identifies a family by its two-character root;
    # the established historical inventory spells standard futures root + F.
    # Pair with the underlying security to catch code reuse or mapping drift.
    current = {(r["product"] + "F", r["symbol"]) for r in doc["rows"] if r["has_futures"]}
    if len(current) != sum(bool(r["has_futures"]) for r in doc["rows"]) or len(old) != len(old_rows):
        raise ValueError("duplicate current or historical stock/ETF family identity")
    pins = {str(path): sha256_file(path) for path in (catalog, raw, previous, previous_receipt)}
    if pins[str(catalog)] != current_sha:
        raise ValueError("catalog changed during readback")
    result = {
        "contract": "current_stock_etf_catalog_comparison_v1",
        "completed_at_utc": datetime.now(UTC).isoformat(),
        "official_catalog_observed_at_utc": doc["retrieved_at_utc"],
        "current_official_stock_etf_family_count": len(current),
        "previous_official_stock_etf_family_count": len(old),
        "same_family_and_underlying_count": len(current & old),
        "current_family_not_in_previous": [dict(product=p, underlying_symbol=s) for p, s in sorted(current - old)],
        "previous_family_not_in_current": [dict(product=p, underlying_symbol=s) for p, s in sorted(old - current)],
        "current_family_listing_is_not_physical_contract_or_fill_evidence": True,
        "current_membership_is_not_historical_eligibility": True,
        "other_twd_product_families_not_reaudited_here": True,
        "financial_or_training_adopted": False, "input_pins": pins,
        "script_sha256": sha256_file(Path(__file__)),
    }
    if any(sha256_file(Path(path)) != digest for path, digest in pins.items()):
        raise ValueError("a source changed during current-catalog readback")
    atomic_write_json(output, result)
    print(json.dumps({k: result[k] for k in ("current_official_stock_etf_family_count",
          "same_family_and_underlying_count", "current_family_not_in_previous", "previous_family_not_in_current")}), flush=True)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("catalog", "previous", "previous-receipt", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    audit(*(getattr(args, name.replace("-", "_")).resolve()
            for name in ("catalog", "previous", "previous-receipt", "output")))


if __name__ == "__main__":
    main()

"""Inventory existing sources and independently verify named open-data grants.

This is an operator audit, NOT a public-request collector or legal opinion.
No credentials, vendor sessions, raw vendor data, ledger writes or API orders.
Fetching an API and holding a license are deliberately separate observations.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys
import time
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from downloader.download_tw_public_data import EXTENDED_DATASETS

# These are documented aliases for the existing daily canonical collectors;
# dataset registration is not a license for every column of MI_INDEX.
GOV_IDS = {"twse_daily_ohlcv": "11549", "tpex_daily_ohlcv": "11370"}
FIELD_NAMES = {
    "twse_daily_ohlcv": {"open": "開盤價", "high": "最高價", "low": "最低價",
                         "close": "收盤價", "volume_shares": "成交股數",
                         "turnover": "成交金額", "trade_count": "成交筆數"},
    "tpex_daily_ohlcv": {"open": "開盤", "high": "最高", "low": "最低", "close": "收盤",
                         "last_bid": "最後買價", "last_ask": "最後賣價", "average_price": "均價",
                         "next_reference_price": "次日參考價", "next_upper_limit": "次日漲停價",
                         "next_lower_limit": "次日跌停價", "volume_shares": "成交股數",
                         "turnover": "成交金額", "trade_count": "成交筆數"},
}


def field_names(value: object) -> list[str]:
    if isinstance(value, list):
        return [item["name"].strip() for item in value
                if isinstance(item, dict) and isinstance(item.get("name"), str) and item["name"].strip()]
    if isinstance(value, str):
        return [field.strip() for field in re.split(r"[,，、;；\n]", value) if field.strip()]
    return []


def fetch_government_record(dataset_id: str) -> dict:
    url = f"https://data.gov.tw/api/v2/rest/dataset/{dataset_id}"
    start = time.perf_counter()
    try:
        with urlopen(Request(url, headers={"User-Agent": "StockAgent-public-source-audit/1"}), timeout=15) as response:
            body = response.read(2 * 1024 * 1024 + 1)
        if len(body) > 2 * 1024 * 1024:
            raise ValueError("oversized_metadata")
        payload = json.loads(body)
        if payload.get("success") is not True or not isinstance(payload.get("result"), dict):
            raise ValueError("invalid_metadata_response")
        record = payload["result"]
        fields = sorted({field for item in record.get("distribution", []) if isinstance(item, dict)
                         for field in field_names(item.get("resourceField"))})
        # Whitelist metadata. Contact details and unrelated text never enter
        # the receipt; do not echo remote exception messages or credentials.
        return {"dataset_id": dataset_id, "metadata_url": url,
                "reference": f"https://data.gov.tw/dataset/{dataset_id}",
                "title": record.get("title"), "license": record.get("license"),
                "fields": fields, "state": "verified_open_v1" if str(record.get("license")) == "1" else "license_needs_review",
                "observed_at_utc": datetime.now(timezone.utc).isoformat(),
                "request_ms": round((time.perf_counter() - start) * 1000, 3)}
    except Exception as error:
        return {"dataset_id": dataset_id, "metadata_url": url, "state": "metadata_unavailable",
                "error_kind": type(error).__name__}


def build_inventory(*, policy: dict, catalog: dict, metadata: dict[str, dict], root: Path = ROOT) -> dict:
    reviews = {row["source_id"]: row for row in policy.get("usage_rules_review", {}).get("sources", [])}
    families = [{"source_id": row["id"], "provider": row["provider"], "access": row["access"],
                 "internal_use": "existing_acquisition_contract_unchanged",
                 "public_operational_status": "per_existing_readonly_gateway_contract_not_a_raw_data_grant",
                 "raw_data_display": "per_dataset_license_review_required",
                 "reference": row.get("documentation")} for row in catalog["sources"]]
    families.extend({"source_id": key, "provider": key, "access": "existing_project_source",
                     "internal_use": "unchanged", "raw_data_display": row["status"],
                     "references": row.get("references", [])} for key, row in reviews.items()
                    if key not in {row["source_id"] for row in families})
    grants = {row["source_id"]: row for row in policy["authorized_sources"] if row.get("public_display")}
    datasets = []
    errors = []
    for name, spec in EXTENDED_DATASETS.items():
        dataset_id = GOV_IDS.get(name) or spec.data_gov_id
        record = metadata.get(dataset_id, {})
        row = {"source_id": name, "provider": spec.source, "kind": spec.kind,
               "local_materialized": (root / "data_tw_public" / f"{name}.parquet").is_file(),
               "data_gov_id": dataset_id, "public_display_basis": record.get("state", "per_dataset_license_review_required"),
               "raw_values_exposed_by_this_change": name in grants}
        if name in grants:
            roles = {field for values in grants[name].get("daily_price_roles", {}).values() for field in values}
            needed = roles | set(FIELD_NAMES.get(name, {}))
            absent = sorted(field for field in needed if FIELD_NAMES.get(name, {}).get(field) not in record.get("fields", []))
            row["published_field_map"] = FIELD_NAMES.get(name, {})
            row["fields_missing_from_open_license"] = absent
            if record.get("state") != "verified_open_v1" or absent:
                errors.append({"source_id": name, "state": record.get("state", "metadata_unavailable"),
                               "unverified_fields": absent})
        datasets.append(row)
    return {"contract": "public_source_display_audit_v1", "observed_at_utc": datetime.now(timezone.utc).isoformat(),
            "scope": "all registered source families and TW canonical collector specs; not all data on the internet",
            "source_families": families, "official_datasets": datasets, "government_metadata": metadata,
            "summary": {"registered_families": len(catalog["sources"]), "reviewed_and_registered_entries": len(families),
                        "official_datasets": len(datasets), "display_basis_counts": dict(Counter(row["public_display_basis"] for row in datasets)),
                        "grant_errors": errors},
            "status": "accepted_current_public_grants" if not errors else "grant_verification_failed",
            "all_source_licenses_verified": False, "vendor_raw_prices_enabled": False,
            "credential_reads": False, "ledger_writes": False, "public_request_provider_calls": False}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    policy = json.loads((ROOT / "configs/public_quote_display.json").read_text())
    catalog = json.loads((ROOT / "configs/free_public_data_sources.json").read_text())
    ids = sorted(set(GOV_IDS.values()) | {spec.data_gov_id for spec in EXTENDED_DATASETS.values() if spec.data_gov_id})
    with ThreadPoolExecutor(max_workers=4) as pool:
        metadata = dict(zip(ids, pool.map(fetch_government_record, ids), strict=True))
    result = build_inventory(policy=policy, catalog=catalog, metadata=metadata)
    atomic_write_json(args.output, result)
    print(json.dumps({"status": result["status"], "summary": result["summary"], "receipt": str(args.output)}, ensure_ascii=False))
    if result["status"] != "accepted_current_public_grants":
        raise SystemExit(1)


if __name__ == "__main__":
    main()

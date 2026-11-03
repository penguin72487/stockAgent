#!/usr/bin/env python3
"""Read back current TAIFEX stock/ETF option families from a pinned catalog.

The family list is not a history, an individual option contract, a quote or a
training admission.  The existing TAIFEX collector owns the source snapshot.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime
import json
from pathlib import Path

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data import tw_stock_futures_catalog as catalog_parser
from stockagent.data.tw_stock_futures_catalog import (
    SOURCE_URL, parse_stock_futures_catalog, parse_stock_option_catalog, rows_digest,
)


def audit(catalog: Path, policy: Path, output: Path) -> dict:
    if output.exists():
        raise FileExistsError("preserve existing catalog audit")
    catalog_sha = sha256_file(catalog)
    doc = json.loads(catalog.read_text(encoding="utf-8"))
    if (doc.get("schema_version") != 1 or doc.get("source_url") != SOURCE_URL
            or doc.get("scope") != "latest_catalog_not_signal_date" or doc.get("complete") is not True
            or doc.get("rows_sha256") != rows_digest(doc["rows"])
            or doc.get("row_count") != len(doc["rows"])):
        raise ValueError("invalid official stock catalog receipt")
    observed = datetime.fromisoformat(doc["retrieved_at_utc"])
    if observed.tzinfo is None:
        raise ValueError("catalog observation must have a timezone")
    raw = catalog.parent / "stock_futures_catalog_sources" / (doc["source_sha256"] + ".html")
    if sha256_file(raw) != doc["source_sha256"]:
        raise ValueError("official stock catalog source digest mismatch")
    body = raw.read_text(encoding="utf-8")
    if parse_stock_futures_catalog(body) != doc["rows"]:
        raise ValueError("official stock catalog parsed receipt mismatch")
    rows = parse_stock_option_catalog(body)
    policy_sha = sha256_file(policy)
    policy_doc = json.loads(policy.read_text(encoding="utf-8"))
    if (policy_doc.get("contract") != "taifex_option_session_policy_candidate_v1"
            or policy_doc.get("research_candidate_only") is not True
            or not isinstance(policy_doc.get("products"), dict)):
        raise ValueError("invalid option comparison candidate policy")
    products = policy_doc["products"]
    for row in rows:
        # Root + O is the existing comparison convention, not evidence of any
        # listed strike, month, weekly series or orderable physical contract.
        row["comparison_product_candidate"] = row["product"] + "O"
        row["candidate_in_policy"] = row["comparison_product_candidate"] in products
    pins = {str(catalog): catalog_sha, str(raw): doc["source_sha256"], str(policy): policy_sha}
    if any(sha256_file(Path(path)) != digest for path, digest in pins.items()):
        raise ValueError("a catalog audit input changed during readback")
    summary = {
        "contract": "taifex_current_stock_option_family_readback_v1",
        "observed_at_utc": datetime.now(UTC).isoformat(),
        "official_catalog_retrieved_at_utc": doc["retrieved_at_utc"],
        "source_url": SOURCE_URL, "source_rows": doc["row_count"],
        "option_family_count": len(rows), "weekly_option_family_count": sum(row["has_weekly"] for row in rows),
        "underlying_kind_counts": dict(sorted(Counter(row["underlying_kind"] for row in rows).items())),
        "candidate_in_policy_count": sum(row["candidate_in_policy"] for row in rows),
        "candidates_absent_from_policy": [row["comparison_product_candidate"] for row in rows if not row["candidate_in_policy"]],
        "rows": rows, "input_pins": pins,
        "parser_sha256": sha256_file(Path(catalog_parser.__file__)),
        "audit_sha256": sha256_file(Path(__file__)),
        "limitations": [
            "This is an observed current family list, not a historical listing calendar or training universe.",
            "Weekly eligibility is not proof of a specific listed expiry or underlying trade.",
            "The derived root+O is a research comparison convention, not a validated physical contract identifier.",
            "Index, commodity and foreign-currency options require separate catalogs and rules.",
        ],
        "financial_or_training_adopted": False,
    }
    atomic_write_json(output, summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("catalog", "policy", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.catalog.resolve(), args.policy.resolve(), args.output.resolve())
    print(json.dumps({key: result[key] for key in (
        "option_family_count", "weekly_option_family_count", "underlying_kind_counts",
        "candidate_in_policy_count", "candidates_absent_from_policy")}, ensure_ascii=False))


if __name__ == "__main__":
    main()

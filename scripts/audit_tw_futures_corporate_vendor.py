#!/usr/bin/env python3
"""Compare source-pinned TEJ adjustment cells with exact TAIFEX terms.

This is a read-only source audit.  It cannot decide inventory transfer,
historical availability, valuation, execution or training admission.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import gzip
import hashlib
import json
from pathlib import Path
import re
import sqlite3

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file


def _inside(root: Path, relative: str) -> Path:
    root = root.resolve()
    path = (root / relative).resolve()
    if not path.is_relative_to(root):
        raise ValueError("source path escapes its owner")
    return path


def _decimal(value: object) -> Decimal | None:
    if value is None or str(value).strip().lower() in {"", "(null)", "null", "none"}:
        return None
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("invalid adjustment amount") from exc
    if not result.is_finite():
        raise ValueError("non-finite adjustment amount")
    return result


def _compare(official: object, vendor: object) -> dict:
    left, right = _decimal(official), _decimal(vendor)
    status = ("unknown" if left is None or right is None else
              "equal" if left == right else "conflict")
    return {"official": None if left is None else str(left),
            "vendor": None if right is None else str(right), "status": status}


def reconcile(candidate: dict, official: list[dict], vendor: list[dict]) -> dict:
    """Match the contract identity before comparing its independently sourced cells."""
    physical = str(candidate["physical_contract"])
    match = re.fullmatch(r"([^:]+):([0-9]{6})@.+", physical)
    successor, month = candidate["successor_product"], candidate["contract_month"]
    if not match or match.group(2) != month:
        raise ValueError("candidate physical contract and month disagree")
    old, day = match.group(1), candidate["adjusted_date"]
    expected_symbol = successor + month
    terms = [x for x in official if x["product"] == successor
             and x["from_product"] == old and x["contract"] == month
             and str(x["effective_date"]) == day]
    cells = [x for x in vendor if x["_query_symbol"] == expected_symbol
             and x["_query_period"] == day
             and str(x["Adjusted Date"]).replace("/", "-") == day
             and str(x["Future Code"]).startswith(expected_symbol + " ")]
    if len(terms) != 1 or len(cells) != 1:
        raise ValueError(f"ambiguous official or vendor source for {physical} at {day}")
    term, cell = terms[0], cells[0]
    if _decimal(cell["Shares per Contract"]) != _decimal(candidate["tej_shares"]):
        raise ValueError("vendor cells differ from the source candidate")
    # A notice which does not contain an equity-credit field is unknown, not
    # zero; the separate subscription-rights leg has different semantics.
    official_cash = term["equity_cash_credit_twd"] if term["has_equity_credit_fields"] else None
    return {
        "physical_contract": physical, "successor_product": successor,
        "contract_month": month, "effective_date": day,
        "shares_per_contract": _compare(term["contract_multiplier"], cell["Shares per Contract"]),
        "cash_credit_per_contract_twd": _compare(official_cash, cell["Cash Dividends per Unit"]),
        "official_source_content_sha256s": term["source_content_sha256s"],
        "official_source_urls": term["source_urls"],
        "vendor_receipt": candidate["receipt"], "vendor_task_id": candidate["task_id"],
        "inventory_transfer_or_valuation_verified": False,
    }


def verify_raw_vendor_cells(candidate: dict, raw: dict, vendor: list[dict]) -> None:
    """Independently compare the retained preview cells to the Parquet row."""
    cells = raw.get("cells")
    if not isinstance(cells, list) or not cells or not isinstance(cells[0], list):
        raise ValueError("TEJ original preview has no cell grid")
    headers = cells[0]
    required = ("Future Code", "Adjusted Date", "Shares per Contract", "Cash Dividends per Unit")
    if len(headers) != len(set(headers)) or any(name not in headers for name in required):
        raise ValueError("TEJ original preview headers are ambiguous")
    symbol = candidate["successor_product"] + candidate["contract_month"]
    day = candidate["adjusted_date"]
    raw_rows = []
    for values in cells[1:]:
        if not isinstance(values, list) or len(values) != len(headers):
            raise ValueError("TEJ original preview row has an invalid width")
        row = dict(zip(headers, values, strict=True))
        if (str(row["Future Code"]).startswith(symbol + " ")
                and str(row["Adjusted Date"]).replace("/", "-") == day):
            raw_rows.append(row)
    parquet_rows = [row for row in vendor if row["_query_symbol"] == symbol
                    and row["_query_period"] == day]
    if (len(raw_rows) != 1 or len(parquet_rows) != 1
            or any(raw_rows[0][name] != parquet_rows[0][name] for name in required)):
        raise ValueError("TEJ original preview differs from its Parquet row")


def audit(*, workspace: Path, candidates: Path, terms: Path, output: Path) -> dict:
    workspace = workspace.resolve()
    if (not candidates.is_relative_to(workspace) or not terms.is_relative_to(workspace)
            or not output.is_relative_to(workspace / "artifacts")):
        raise ValueError("audit inputs and output must use the workspace owners")
    if output.exists():
        raise ValueError("preserve the existing audit; use a new output")
    source = json.loads(candidates.read_text())
    if source.get("contract") != "v13_tej_corporate_arrivals_v1":
        raise ValueError("unsupported candidate source")
    term_pin = source["input_pins"].get(str(terms.relative_to(workspace)))
    if not term_pin or sha256_file(terms) != term_pin:
        raise ValueError("official term source does not match the candidate manifest")
    # Restrict the source before converting to Python; a broad archive should
    # not cost one scan or a full Python materialization per candidate.
    selected = source["candidates"]
    official = (pl.scan_parquet(terms)
                .filter(pl.col("product").is_in(sorted({c["successor_product"] for c in selected})),
                        pl.col("contract").is_in(sorted({c["contract_month"] for c in selected})),
                        pl.col("effective_date").cast(pl.String).is_in(
                            sorted({c["adjusted_date"] for c in selected})))
                .collect().to_dicts())
    receipt_pins = {r["task_id"]: r for r in source["completed_receipts"]}
    tej = workspace / "data_tej"
    rules = workspace / "data_taifex_public_history/rules"
    rows, pinned = [], {}
    with sqlite3.connect(f"file:{rules / 'state/queue.sqlite3'}?mode=ro", uri=True) as db:
        for candidate in source["candidates"]:
            receipt = _inside(workspace, candidate["receipt"])
            pin = receipt_pins[candidate["task_id"]]
            if sha256_file(receipt) != pin["receipt_sha256"]:
                raise ValueError("TEJ receipt changed")
            metadata = json.loads(receipt.read_text())
            if metadata["task_id"] != candidate["task_id"]:
                raise ValueError("TEJ receipt task differs")
            for kind in ("raw", "parquet"):
                path = _inside(tej, metadata[f"{kind}_path"])
                digest = sha256_file(path)
                if digest != metadata[f"{kind}_sha256"] or digest != pin[f"{kind}_sha256"]:
                    raise ValueError(f"TEJ {kind} source changed")
                pinned[str(path.relative_to(workspace))] = digest
            vendor = pl.read_parquet(_inside(tej, metadata["parquet_path"])).to_dicts()
            verify_raw_vendor_cells(candidate, json.loads(
                _inside(tej, metadata["raw_path"]).read_text()), vendor)
            result = reconcile(candidate, official, vendor)
            result["vendor_raw_cells_match_parquet"] = True
            pinned[str(receipt.relative_to(workspace))] = pin["receipt_sha256"]
            for digest in result["official_source_content_sha256s"]:
                found = db.execute("SELECT raw_path,raw_sha256 FROM documents "
                                   "WHERE content_sha256=? AND state=?", (digest, "complete")).fetchall()
                if len(found) != 1:
                    raise ValueError("official original is missing or ambiguous")
                raw_name, raw_hash = found[0]
                raw = _inside(rules, raw_name)
                if (sha256_file(raw) != raw_hash
                        or hashlib.sha256(gzip.decompress(raw.read_bytes())).hexdigest() != digest):
                    raise ValueError("official original changed")
                pinned[str(raw.relative_to(workspace))] = raw_hash
            rows.append(result)
    if len({(r["physical_contract"], r["effective_date"]) for r in rows}) != len(rows):
        raise ValueError("duplicated adjustment candidate")
    result = {
        "contract": "taifex_tej_corporate_adjustment_comparison_v1",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "candidate_count": len(rows),
        "share_comparison_counts": dict(Counter(r["shares_per_contract"]["status"] for r in rows)),
        "cash_comparison_counts": dict(Counter(r["cash_credit_per_contract_twd"]["status"] for r in rows)),
        "candidates": rows,
        "input_pins": {str(candidates.relative_to(workspace)): sha256_file(candidates),
                       str(terms.relative_to(workspace)): sha256_file(terms),
                       str(Path(__file__).resolve().relative_to(workspace)): sha256_file(Path(__file__)),
                       **pinned},
        "scope": "Source-cell comparison only; no independent end-to-end valuation or inventory transfer",
        "historical_point_in_time_verified": False,
        "financial_or_training_auto_adopted": False,
    }
    atomic_write_json(output, result)
    return result


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--candidates", required=True, type=Path)
    p.add_argument("--terms", required=True, type=Path)
    p.add_argument("--output", required=True, type=Path)
    args = p.parse_args()
    root = Path(__file__).resolve().parents[1]
    result = audit(workspace=root, candidates=args.candidates.resolve(),
                   terms=args.terms.resolve(), output=args.output.resolve())
    print(json.dumps({k: result[k] for k in
                      ("candidate_count", "share_comparison_counts", "cash_comparison_counts")}, ensure_ascii=False))


if __name__ == "__main__":
    main()

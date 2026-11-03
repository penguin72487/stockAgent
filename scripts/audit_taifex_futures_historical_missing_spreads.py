#!/usr/bin/env python3
"""Reconcile fixed historical daily-versus-tick gaps against spread legs.

The canonical daily evidence is source-only and does not create executable
prices, a historical listing, or a training release.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timezone
import json
from pathlib import Path
import re

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data.tw_stock_futures_repair import official_day_evidence


def audit(summary: Path, unmatched: Path, manifest: Path, output_dir: Path, output: Path) -> dict:
    if output.exists() or output_dir.exists() or output.is_symlink() or output_dir.is_symlink():
        raise FileExistsError("preserve existing historical spread evidence")
    summary_sha = sha256_file(summary)
    doc = json.loads(summary.read_text())
    if (doc.get("contract") != "taifex_finmind_futures_contract_day_presence_v1"
            or sha256_file(unmatched) != doc["unmatched_sha256"]
            or sha256_file(manifest) != doc["official_manifest_sha256"]
            or sha256_file(Path(doc["official_path"])) != doc["official_sha256"]):
        raise ValueError("historical source-presence inputs differ from their fixed summary")
    records = json.loads(unmatched.read_text())
    if len(records) != doc["unmatched_contract_days_on_verified_natural_dates"]:
        raise ValueError("historical unmatched count changed")
    identities = set()
    for row in records:
        day, product, contract = date.fromisoformat(row["date"]), row["product"], row["contract"]
        if not re.fullmatch(r"[A-Z0-9]+", product) or not re.fullmatch(r"\d{6}(?:W[1-5])?", contract):
            raise ValueError("historical unmatched physical key is unsupported")
        identities.add((day, product, contract))
    if len(identities) != len(records) or not identities:
        raise ValueError("historical unmatched keys must be nonempty and unique")
    years = {day.year for day, _, _ in identities}
    source_manifest = json.loads(manifest.read_text())
    selected = [entry for entry in source_manifest["receipts"]
                if Path(entry["path"]).parent.name == "annual"
                and Path(entry["path"]).name in {f"{year}_fut.zip" for year in years}]
    if {Path(row["path"]).name for row in selected} != {f"{year}_fut.zip" for year in years} or len(selected) != len(years):
        raise ValueError("no unique fixed annual raw source for every unmatched year")
    for row in selected:
        raw = Path(row["path"])
        if raw.is_symlink() or sha256_file(raw) != row["sha256"]:
            raise ValueError("historical raw source differs from its manifest")
    keys = pl.DataFrame({"date": [day for day, _, _ in sorted(identities)],
                         "physical_contract": [f"{p}:{m}" for _, p, m in sorted(identities)]})
    output_dir.mkdir(parents=True)
    adapter = output_dir / "source_manifest.json"
    atomic_write_json(adapter, {"contract": "taifex_historical_fixed_annual_source_adapter_v1",
        "original_manifest": str(manifest), "original_manifest_sha256": doc["official_manifest_sha256"],
        "unmatched_sha256": doc["unmatched_sha256"], "receipts": selected})
    frame = official_day_evidence(adapter, keys, output_dir)
    proof = json.loads((output_dir / "official_evidence_manifest.json").read_text())
    parquet = output_dir / "official_evidence.parquet"
    reread = pl.read_parquet(parquet)
    if (sha256_file(parquet) != proof["sha256"] or not reread.equals(frame)
            or reread.height != len(identities) or sha256_file(summary) != summary_sha
            or sha256_file(unmatched) != doc["unmatched_sha256"]
            or sha256_file(manifest) != doc["official_manifest_sha256"]):
        raise ValueError("historical source or spread evidence changed during readback")
    reasons = Counter(row["official_reason"] for row in reread.to_dicts())
    by_product: dict[str, Counter] = {}
    for row in reread.to_dicts():
        product = row["physical_contract"].split(":", 1)[0]
        by_product.setdefault(product, Counter())[row["official_reason"]] += 1
    result = {"contract": "taifex_historical_futures_missing_spread_readback_v1",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(), "selected_contract_days": len(records),
        "reconciled_reasons": dict(sorted(reasons.items())),
        "reconciled_reasons_by_product": {p: dict(sorted(c.items())) for p, c in sorted(by_product.items())},
        "input_summary": str(summary), "input_summary_sha256": summary_sha,
        "unmatched_sha256": doc["unmatched_sha256"], "original_manifest_sha256": doc["official_manifest_sha256"],
        "official_evidence_dir": str(output_dir), "official_evidence_sha256": proof["sha256"],
        "adapter_manifest_sha256": sha256_file(adapter),
        "canonical_implementation_sha256": sha256_file(Path(official_day_evidence.__code__.co_filename)),
        "script_sha256": sha256_file(Path(__file__)),
        "limitations": ["The result describes fixed unmatched source keys; it does not restore tick observations.",
                        "Spread-leg volume is not an outright-book fill or an executable price.",
                        "It is not a historical listing, PIT, account or training admission."]}
    atomic_write_json(output, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("summary", "unmatched", "manifest", "output-dir", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    result = audit(*(getattr(args, name.replace("-", "_")).resolve()
                     for name in ("summary", "unmatched", "manifest", "output-dir", "output")))
    print(json.dumps({key: result[key] for key in ("selected_contract_days", "reconciled_reasons",
                                                   "reconciled_reasons_by_product")}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()

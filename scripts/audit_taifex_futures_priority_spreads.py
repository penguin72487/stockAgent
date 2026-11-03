#!/usr/bin/env python3
"""Apply the canonical spread-leg source reconciliation to a fixed day audit.

This is source evidence only. A spread-leg price is not an outright fill and
no new data is admitted to a model or account by this script.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data.tw_stock_futures_repair import official_day_evidence


def audit(report: Path, receipt: Path, output_dir: Path, output: Path) -> dict:
    if output.exists() or output_dir.exists() or output.is_symlink() or output_dir.is_symlink():
        raise FileExistsError("preserve existing spread reconciliation evidence")
    report_sha, receipt_sha = sha256_file(report), sha256_file(receipt)
    presence, source = json.loads(report.read_text()), json.loads(receipt.read_text())
    if (presence.get("contract") != "taifex_futures_priority_day_source_presence_v1"
            or source.get("contract") != "taifex_futures_daily_priority_source_v1"
            or presence.get("input_pins", {}).get(str(receipt)) != receipt_sha):
        raise ValueError("daily receipt does not match fixed source-presence audit")
    if any(sha256_file(Path(path)) != digest for path, digest in presence["input_pins"].items()):
        raise ValueError("a source-presence input changed")
    raw = Path(source["path"])
    if sha256_file(raw) != source["sha256"]:
        raise ValueError("daily source does not match receipt")
    day = date.fromisoformat(presence["trading_date"])
    missing = presence["positive_keys_missing_from_trading_date_tick_archive"]
    if (not missing or any(not isinstance(row, list) or len(row) != 2 for row in missing)
            or len({tuple(row) for row in missing}) != len(missing)):
        raise ValueError("expected unique unmatched product and physical-month keys")
    keys = pl.DataFrame({"date": [day] * len(missing),
                         "physical_contract": [f"{product}:{month}" for product, month in missing]})
    output_dir.mkdir(parents=True)
    adapter = output_dir / "source_manifest.json"
    atomic_write_json(adapter, {"contract": "taifex_daily_priority_source_adapter_v1",
        "source_receipt": str(receipt), "source_receipt_sha256": receipt_sha,
        "source_presence_audit": str(report), "source_presence_audit_sha256": report_sha,
        "receipts": [{"path": str(raw), "sha256": source["sha256"]}]})
    frame = official_day_evidence(adapter, keys, output_dir)
    proof = json.loads((output_dir / "official_evidence_manifest.json").read_text())
    parquet = output_dir / "official_evidence.parquet"
    reread = pl.read_parquet(parquet)
    if (proof["sha256"] != sha256_file(parquet) or not reread.equals(frame)
            or reread.height != len(missing) or sha256_file(report) != report_sha
            or sha256_file(receipt) != receipt_sha or sha256_file(raw) != source["sha256"]):
        raise ValueError("spread evidence or original source changed during readback")
    grouped = reread.group_by("official_reason").len().sort("official_reason").to_dicts()
    result = {"contract": "taifex_futures_priority_spread_evidence_readback_v1",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(), "trading_date": day.isoformat(),
        "source_presence_audit": str(report), "source_presence_audit_sha256": report_sha,
        "selected_missing_contract_keys": len(missing), "reconciled_reasons": grouped,
        "official_evidence_dir": str(output_dir), "official_evidence_sha256": proof["sha256"],
        "adapter_manifest_sha256": sha256_file(adapter),
        "canonical_implementation_sha256": sha256_file(Path(official_day_evidence.__code__.co_filename)),
        "script_sha256": sha256_file(Path(__file__)),
        "limitations": ["Spread-leg volume is source evidence, not an outright trade or executable price.",
                        "A daily volume reconciliation does not prove tick completeness, listing or account admission.",
                        "No model or training source is modified by this audit."]}
    atomic_write_json(output, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("report", "receipt", "output-dir", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    result = audit(*(getattr(args, name.replace("-", "_")).resolve()
                     for name in ("report", "receipt", "output-dir", "output")))
    print(json.dumps({key: result[key] for key in ("trading_date", "selected_missing_contract_keys",
                                                   "reconciled_reasons")}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()

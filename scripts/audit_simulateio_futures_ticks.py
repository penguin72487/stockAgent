"""Compare a public continuous futures tick file with a retained physical contract.

This is a diagnostic only: a coincidental offset or multiset match does not
establish the continuous-series adjustment, contract identity, trade type,
historical availability, or permission to use any row for execution.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime, timedelta, timezone
import json
from pathlib import Path
import zipfile

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file


def compare(candidate_path: Path, receipt_path: Path, *, day: str, product: str,
            contract: str, shift: float, candidate_volume_multiplier: int) -> dict:
    if candidate_volume_multiplier < 1:
        raise ValueError("candidate volume diagnostic multiplier must be positive")
    receipt = json.loads(receipt_path.read_text())
    if (receipt.get("dataset") != "TaiwanFuturesTick" or receipt.get("partition") != day
            or receipt.get("status") != "complete"):
        raise ValueError("FinMind receipt does not identify the requested observation")
    # A receipt is under complement/receipts/<dataset>/all/<date>.json.
    provider_root = receipt_path.parents[3]
    parquet_path = provider_root / receipt["parquet_path"]
    if sha256_file(parquet_path) != receipt["sha256"]:
        raise ValueError("FinMind native payload hash differs from receipt")

    with zipfile.ZipFile(candidate_path) as archive:
        members = [entry for entry in archive.infolist() if not entry.is_dir()]
        if len(members) != 1 or not members[0].filename.endswith(".rpt"):
            raise ValueError("expected one original .rpt member")
        native = json.loads(archive.read(members[0]))  # also checks zip CRC
    if not isinstance(native, list) or not native:
        raise ValueError("continuous source contains no list of observations")
    local_tz = timezone(timedelta(hours=8))
    beginning = f"{day} 08:45:00"
    ending = f"{day} 13:44:59"
    candidate = Counter()
    candidate_all_dates = Counter()
    field_names = set()
    prior_ms = None
    regressions = 0
    first_candidate = None
    for row in native:
        if not isinstance(row, dict) or not {"m", "s", "c", "v"} <= row.keys():
            raise ValueError("continuous source row lacks timestamp, sequence, price or volume")
        field_names.update(row)
        ms = int(row["m"])
        if prior_ms is not None and ms < prior_ms:
            regressions += 1
        prior_ms = ms
        stamp = datetime.fromtimestamp(ms / 1000, local_tz)
        candidate_all_dates[stamp.date().isoformat()] += 1
        stamp_text = stamp.strftime("%Y-%m-%d %H:%M:%S")
        if not beginning <= stamp_text <= ending:
            continue
        price, volume = float(row["c"]), int(row["v"])
        if price <= 0 or volume <= 0:
            raise ValueError("nonpositive candidate price or volume")
        candidate[(stamp_text, price, volume * candidate_volume_multiplier)] += 1
        ordered = (ms, int(row["s"]), price, volume)
        if first_candidate is None or ordered < first_candidate:
            first_candidate = ordered
    if not candidate:
        raise ValueError("continuous source has no rows in the requested session")

    frame = (pl.scan_parquet(parquet_path)
             .filter((pl.col("futures_id") == product) & (pl.col("contract_date") == contract)
                     & (pl.col("date") >= beginning) & (pl.col("date") <= ending))
             .select("date", "price", "volume").collect())
    if not frame.height:
        raise ValueError("FinMind has no rows for the physical contract in the session")
    physical = Counter((str(stamp), float(price), int(volume))
                       for stamp, price, volume in frame.iter_rows())
    adjusted = Counter({(stamp, price - shift, volume): count
                        for (stamp, price, volume), count in candidate.items()})
    overlap = adjusted & physical
    candidate_only, physical_only = adjusted - physical, physical - adjusted

    def row_count(values: Counter) -> int:
        return sum(values.values())

    def volume_total(values: Counter) -> int:
        return sum(volume * count for (_, _, volume), count in values.items())

    first_stamp = datetime.fromtimestamp(first_candidate[0] / 1000, local_tz).isoformat()
    return {
        "contract": "simulateio_continuous_physical_tick_diagnostic_v1",
        "completed_at_utc": datetime.now(UTC).isoformat(),
        "day": day, "product": product, "physical_contract_month": contract,
        "session_local": [beginning, ending], "timezone": "Asia/Taipei",
        "candidate": {"path": str(candidate_path), "sha256": sha256_file(candidate_path),
                      "member": members[0].filename, "rows_all": len(native),
                      "rows_by_calendar_date": dict(sorted(candidate_all_dates.items())),
                      "timestamp_regressions_in_native_order": regressions,
                      "session_rows": row_count(candidate),
                      "session_native_volume": volume_total(candidate) / candidate_volume_multiplier,
                      "first_session_record": {"time": first_stamp, "sequence": first_candidate[1],
                                               "price": first_candidate[2], "volume": first_candidate[3]},
                      "observed_field_names": sorted(field_names),
                      "physical_contract_month_independently_identified": False},
        "finmind": {"receipt_path": str(receipt_path), "receipt_sha256": sha256_file(receipt_path),
                    "parquet_path": str(parquet_path), "parquet_sha256": receipt["sha256"],
                    "session_rows": row_count(physical), "session_native_volume": volume_total(physical),
                    "historical_point_in_time_verified": receipt.get("historical_point_in_time") is True},
        "comparison": {"candidate_volume_diagnostic_multiplier": candidate_volume_multiplier,
                       "unadjusted_matched_rows": row_count(candidate & physical),
                       "diagnostic_candidate_price_shift_subtracted": shift,
                       "diagnostic_matched_rows": row_count(overlap),
                       "diagnostic_candidate_only_rows": row_count(candidate_only),
                       "diagnostic_physical_only_rows": row_count(physical_only),
                       "diagnostic_candidate_only_scaled_volume": volume_total(candidate_only),
                       "diagnostic_physical_only_scaled_volume": volume_total(physical_only)},
        "source_transformation_documented": False,
        "trade_types_and_contract_month_independently_verified": False,
        "admitted_to_execution": False,
        "note": "The offset and volume multiplier are diagnostics only; they are not applied to data or proof of fills.",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--finmind-receipt", type=Path, required=True)
    parser.add_argument("--day", required=True)
    parser.add_argument("--product", required=True)
    parser.add_argument("--contract", required=True)
    parser.add_argument("--diagnostic-shift", type=float, required=True)
    parser.add_argument("--candidate-volume-multiplier", type=int, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if args.out.exists():
        raise FileExistsError("retain the previous diagnostic rather than overwriting it")
    result = compare(args.candidate, args.finmind_receipt, day=args.day, product=args.product,
                     contract=args.contract, shift=args.diagnostic_shift,
                     candidate_volume_multiplier=args.candidate_volume_multiplier)
    result["audit_script_sha256"] = sha256_file(Path(__file__))
    atomic_write_json(args.out, result)
    print(json.dumps({"output": str(args.out), "comparison": result["comparison"],
                      "admitted_to_execution": False}, ensure_ascii=False))


if __name__ == "__main__":
    main()

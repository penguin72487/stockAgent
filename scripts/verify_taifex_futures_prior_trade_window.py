#!/usr/bin/env python3
"""Independently check selected pre-session events against exchange CSVs.

This source audit does not admit any price into a training clock or prove that
an observation was historically delivered to a strategy. It intentionally uses
the standard-library CSV reader instead of the source builder's tick parser.
"""
from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
import io
import json
from pathlib import Path
import re
import zipfile

import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file


def scan_archive(path: Path, day: date, keys: set[tuple[str, str]]) -> dict:
    """Return every distinct price at the last relevant raw second per key."""
    cutoff = datetime.combine(day, time(8, 45))
    # The source's denominator selects available_by < midnight. A trade in
    # 23:59 becomes available at 00:00 and is therefore not in that set.
    midnight_trade_cutoff = datetime.combine(day, time()) - timedelta(minutes=1)
    result = {key: {"midnight": None, "decision": None} for key in keys}
    member = f"Daily_{day.strftime('%Y_%m_%d')}.csv"
    with zipfile.ZipFile(path) as archive, archive.open(member) as binary:
        with io.TextIOWrapper(binary, encoding="cp950", newline="") as text:
            for row in csv.reader(text):
                if len(row) < 9:
                    continue
                cells = [value.strip() for value in row]
                key = (cells[1], cells[2])
                if key not in keys or not re.fullmatch(r"\d{8}", cells[0]):
                    continue
                try:
                    stamp = datetime.strptime(cells[0] + cells[3], "%Y%m%d%H%M%S")
                    price, quantity = Decimal(cells[4]), int(cells[5])
                except (InvalidOperation, ValueError) as exc:
                    raise ValueError(f"invalid selected exchange trade {key}") from exc
                if not price.is_finite() or price <= 0 or quantity <= 0 or quantity % 2:
                    raise ValueError(f"invalid selected exchange price or quantity {key}")
                # A completed minute at 08:45 contains trades strictly before
                # 08:45; source ordering inside one second is not known.
                if stamp >= cutoff:
                    continue
                for label, boundary in (("midnight", midnight_trade_cutoff), ("decision", cutoff)):
                    if stamp >= boundary:
                        continue
                    previous = result[key][label]
                    if previous is None or stamp > previous[0]:
                        result[key][label] = (stamp, {price})
                    elif stamp == previous[0]:
                        previous[1].add(price)
    return result


def audit(manifest: Path, output: Path) -> dict:
    manifest, output = manifest.resolve(), output.resolve()
    if output.exists() or output.is_symlink():
        raise ValueError("a new audit output is required")
    manifest_sha = sha256_file(manifest)
    document = json.loads(manifest.read_bytes())
    if document.get("training_ready") is not False or not document.get("contract", "").startswith(
            "taifex_futures_pre_session_reference_events_window_v"):
        raise ValueError("unexpected pre-session source manifest")
    payload = Path(document["data_path"])
    payload_sha = sha256_file(payload)
    if payload_sha != document["data_sha256"]:
        raise ValueError("event payload differs from its manifest")
    rows = pq.read_table(payload).to_pylist()
    if len(rows) != document["rows"]:
        raise ValueError("event row count differs from its manifest")
    dates = [date.fromisoformat(value) for value in document["dates"]]
    if dates != sorted(set(dates)):
        raise ValueError("event dates must be sorted and unique")
    sources = {date.fromisoformat(item["date"]): item
               for item in document["sources"]["official_sources"]}
    if set(sources) != set(dates):
        raise ValueError("official source dates differ from the event window")
    grouped: dict[tuple[date, str], list[dict]] = {}
    for row in rows:
        day, physical = row["source_trading_date"], row["physical_contract"]
        if day not in sources or not re.fullmatch(r"[A-Z0-9]{2,5}:\d{6}(?:W[1-5])?@.+", physical):
            raise ValueError("invalid event date or physical contract")
        if (row["source_sha256"] != sources[day]["sha256"] or row["is_session_open"]
                or row["opening_ambiguous"] or row["trade_date"] != row["trade_ts"].date()
                or row["available_by"] != row["trade_ts"].replace(second=0, microsecond=0) + timedelta(minutes=1)
                or row["available_by"] > datetime.combine(day, time(8, 45))):
            raise ValueError("event source, role, or availability is inconsistent")
        grouped.setdefault((day, physical), []).append(row)

    counts = {"events": len(rows), "contracts": len(grouped), "raw_exact_timestamp_and_price": 0,
              "exact_last_second_price_sets": 0, "latest_before_midnight_matches": 0,
              "latest_before_decision_matches": 0, "later_raw_prints_requiring_identity_review": 0}
    mismatches = []
    for day in dates:
        source = sources[day]
        archive, receipt = Path(source["path"]), Path(source["receipt_path"])
        if sha256_file(archive) != source["sha256"] or sha256_file(receipt) != source["receipt_sha256"]:
            raise ValueError("official archive or receipt SHA mismatch")
        these = {physical: sorted(events, key=lambda row: row["trade_ts"])
                 for (event_day, physical), events in grouped.items() if event_day == day}
        keys = {(physical.split(":", 1)[0], physical.split(":", 1)[1].split("@", 1)[0])
                for physical in these}
        raw = scan_archive(archive, day, keys)
        for physical, events in these.items():
            if len(events) > 2 or len({row["trade_ts"] for row in events}) != len(events):
                raise ValueError("duplicate or unexpected candidate events")
            key = (physical.split(":", 1)[0], physical.split(":", 1)[1].split("@", 1)[0])
            for index, row in enumerate(events):
                label = "midnight" if len(events) == 2 and index == 0 else "decision"
                last = raw[key][label]
                if last is None or row["trade_ts"] != last[0]:
                    counts["later_raw_prints_requiring_identity_review"] += 1
                    mismatches.append({"date": str(day), "physical_contract": physical,
                        "boundary": label, "event_ts": row["trade_ts"].isoformat(),
                        "raw_last_ts": last[0].isoformat() if last else None})
                    continue
                price = Decimal(str(row["price"]))
                if abs(price - min(last[1])) > Decimal("0.000000001"):
                    raise ValueError("event price differs from the exchange's selected last second")
                if row["fallback_ambiguous"] is not (len(last[1]) > 1):
                    raise ValueError("event ambiguity differs from exchange last-second prices")
                counts["raw_exact_timestamp_and_price"] += 1
                counts["exact_last_second_price_sets"] += 1
                counts["latest_before_midnight_matches" if label == "midnight"
                       else "latest_before_decision_matches"] += 1
        if sha256_file(archive) != source["sha256"] or sha256_file(receipt) != source["receipt_sha256"]:
            raise ValueError("official source changed during verification")
        print(json.dumps({"date": str(day), "physical_contracts": len(these)}), flush=True)
    if sha256_file(manifest) != manifest_sha or sha256_file(payload) != payload_sha:
        raise ValueError("manifest or event payload changed during verification")
    result = {"contract": "taifex_prior_trade_window_independent_csv_readback_v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_manifest": str(manifest), "source_manifest_sha256": manifest_sha,
        "event_payload_sha256": payload_sha, "implementation_sha256": sha256_file(Path(__file__)),
        "counts": counts, "later_raw_prints": mismatches,
        "training_ready": False,
        "limitations": ["Uses the same exchange originals, not a second market-data vendor.",
            "A raw later print can be another session or physical generation; it remains withheld for identity review.",
            "A last print alone does not establish historic delivery or source continuity.",
            "This audit does not authorize a fill or alter a training input."]}
    atomic_write_json(output, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = audit(args.manifest, args.output)
    print(json.dumps(result["counts"], ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()

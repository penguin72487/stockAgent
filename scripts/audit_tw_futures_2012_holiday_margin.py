#!/usr/bin/env python3
"""Cross-check the v22 margin schedule against two 2012 TAIFEX notices.

This is a narrow source diagnostic: it does not establish broker holiday
account dates, valuation marks, fills, point-in-time capture, or training
admission. Raw notices and the financial manifest are independently pinned.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime
import gzip
import hashlib
import json
from pathlib import Path
import re
import unicodedata

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file

# Notice order: increased initial, maintenance, clearing, then prior levels.
EXPECTED = {
    "TX": (92000, 71000, 68000, 83000, 64000, 61000),
    "MTX": (23000, 17750, 17000, 20750, 16000, 15250),
    "TE": (75000, 57000, 55000, 68000, 52000, 50000),
    "TF": (68000, 52000, 50000, 61000, 47000, 45000),
    "T5F": (29000, 22000, 21000, 26000, 20000, 19000),
    "GTF": (26000, 20000, 19000, 23000, 18000, 17000),
    "XIF": (64000, 49000, 47000, 57000, 44000, 42000),
    "TGF": (45000, 35000, 33000, 41000, 32000, 30000),
}
NOTICE_HASHES = (
    "9a01ce29a873fe92686d9f43e4a82acdd24c6330473423b0ef85ac5e3fdf94c4",
    "fc224c40d8d4c28070ca4998c2ff9f5c4032d1c48bfbaca30f97d913d14c1827",
)
DATES = ("2012-01-17", "2012-01-18", "2012-01-30", "2012-01-31", "2012-02-01")


def verify_notice(raw: Path, parsed: Path, expected_hash: str, *, rollback: bool) -> None:
    body = gzip.decompress(raw.read_bytes())
    document = json.loads(parsed.read_text())
    if (hashlib.sha256(body).hexdigest() != expected_hash
            or document.get("content_sha256") != expected_hash
            or not document.get("source_url", "").startswith("https://www.taifex.com.tw/")):
        raise ValueError("notice original or parsed identity differs")
    text = " ".join(unicodedata.normalize("NFKC", document.get("text") or "").split())
    for product, values in EXPECTED.items():
        match = re.search(r"(?<![A-Z0-9])" + product + r"\s+原始保證金", text)
        if not match:
            raise ValueError(f"notice table is missing {product}")
        block = text[match.start():match.start() + 250]
        found = re.search(r"保證金\s+((?:[\d,]+\s+){5}[\d,]+)", block)
        actual = tuple(int(value.replace(",", "")) for value in found.group(1).split()) if found else ()
        expected = values[3:] + values[:3] if rollback else values
        if actual != expected:
            raise ValueError(f"notice table values differ for {product}")
    compact = re.sub(r"\s+", "", text)
    if not all(phrase in compact for phrase in (
        ("1月31日交易時段結束後",) if rollback else
        ("1月18日", "收盤後生效", "1月31日"))):
        raise ValueError("notice effective-time wording differs")


def check_rules(rules: pl.DataFrame) -> dict:
    """Check every physical row on five dates without assuming a fill."""
    keys = ["date", "physical_contract"]
    required = {*keys, "product", "contract", "terminal_event", "initial", "maintenance",
                "settlement_initial", "settlement_maintenance"}
    if not required <= set(rules.columns):
        raise ValueError("financial rules lack required identities and margin fields")
    if rules.select(keys).is_duplicated().any():
        raise ValueError("duplicate physical contract and date")
    counts: Counter[str] = Counter()
    groups: dict[str, Counter[str]] = {}
    for row in rules.to_dicts():
        day, product = str(row["date"]), row["product"]
        if day not in DATES or product not in EXPECTED:
            raise ValueError("unexpected product or date in selected scope")
        holiday_initial, holiday_maintenance, _, prior_initial, prior_maintenance, _ = EXPECTED[product]
        prior = (prior_initial, prior_maintenance)
        holiday = (holiday_initial, holiday_maintenance)
        opening = prior if day in (DATES[0], DATES[1], DATES[4]) else holiday
        terminal = prior if (day in (DATES[0], DATES[3], DATES[4])
                             or (day == DATES[1] and row["terminal_event"] == "cash_settlement")) else holiday
        actual = (row["initial"], row["maintenance"], row["settlement_initial"], row["settlement_maintenance"])
        if actual != (*opening, *terminal):
            raise ValueError(f"unexpected margin on {day} {product} {row['contract']}: {actual}")
        counts[f"{day}:{product}"] += 1
        groups.setdefault(day, Counter())[row["terminal_event"]] += 1
    missing = [f"{day}:{product}" for day in DATES for product in EXPECTED if not counts[f"{day}:{product}"]]
    if missing:
        raise ValueError(f"financial rules are missing product dates: {missing}")
    return {"physical_contract_rows_checked": rules.height,
            "product_date_counts": dict(sorted(counts.items())),
            "terminal_events_by_date": {day: dict(sorted(value.items())) for day, value in sorted(groups.items())}}


def audit(rules: Path, up_raw: Path, up_parsed: Path, down_raw: Path, down_parsed: Path) -> dict:
    paths = [p.resolve(strict=True) for p in (rules, rules.with_name("manifest.json"), up_raw, up_parsed,
                                               down_raw, down_parsed, Path(__file__))]
    before = {str(path): sha256_file(path) for path in paths}
    manifest = json.loads(paths[1].read_text())
    if manifest.get("status") != "complete" or manifest.get("outputs", {}).get("rules", {}).get("sha256") != before[str(paths[0])]:
        raise ValueError("financial rules do not match their complete manifest")
    verify_notice(paths[2], paths[3], NOTICE_HASHES[0], rollback=False)
    verify_notice(paths[4], paths[5], NOTICE_HASHES[1], rollback=True)
    frame = (pl.scan_parquet(paths[0]).filter(pl.col("product").is_in(list(EXPECTED))
              & pl.col("date").cast(pl.String).is_in(list(DATES)))
             .select("date", "physical_contract", "product", "contract", "terminal_event", "initial",
                     "maintenance", "settlement_initial", "settlement_maintenance").collect())
    result = check_rules(frame)
    if any(sha256_file(Path(path)) != digest for path, digest in before.items()):
        raise ValueError("an audit input changed during readback")
    return {"contract": "tw_futures_2012_holiday_margin_crosscheck_v1",
            "observed_at_utc": datetime.now(UTC).isoformat(), **result,
            "source_content_sha256": list(NOTICE_HASHES), "inputs_sha256": before,
            "notice_increase_effective": "2012-01-18 after market close",
            "notice_rollback_effective": "2012-01-31 after market close",
            "scope": "eight TWD futures and five selected market dates, opening and settlement margin fields",
            "historical_capture_point_in_time_verified": False,
            "broker_holiday_account_dates_or_valuation_verified": False,
            "execution_or_training_admitted": False}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("rules", "up-raw", "up-parsed", "down-raw", "down-parsed", "output"):
        parser.add_argument("--" + name, required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError("preserve earlier audit; choose a new output")
    result = audit(args.rules, args.up_raw, args.up_parsed, args.down_raw, args.down_parsed)
    atomic_write_json(args.output, result)
    print(json.dumps({"output": str(args.output), "rows": result["physical_contract_rows_checked"]}))


if __name__ == "__main__":
    main()

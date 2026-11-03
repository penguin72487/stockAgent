#!/usr/bin/env python3
"""Reconcile exact finite futures-minute requests with a pinned release.

This is source triage only: a represented row is not a training-admission or
an executable fill. The original provider priority plan is never mutated.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, date, datetime
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import polars as pl
from downloader.artifact_io import atomic_write_json, sha256_file

IDENTITY = ["date", "product", "contract"]
ROW_COLUMNS = ["date", "product", "contract", "physical_contract", "official_volume",
               "outright_volume", "official_reason", "executable", "official_source_sha256"]
WINDOWS = ("0845_1m", "0900_1m", "0845_300m", "0900_300m")


def target_frame(plan: dict) -> pl.DataFrame:
    if plan.get("contract") != "provider_priority_query_v1" or plan.get("provider") != "finlab":
        raise ValueError("expected a finite FinLab priority plan")
    rows = []
    for item in plan.get("items", []):
        match = re.fullmatch(r"tw_futures_minute:([A-Z0-9]{1,6})_(\d{6}(?:W[1-5])?)",
                             item.get("dataset", ""))
        if not match:
            raise ValueError("priority item is not an exact futures product and month")
        day = date.fromisoformat(item["partition"])
        # A syntactically valid YYYYMM is necessary; a provider query alone is
        # still not proof of the expiry or the physical generation.
        datetime.strptime(match[2][:6], "%Y%m")
        rows.append((day, match[1], match[2]))
    frame = pl.DataFrame(rows, schema={"date": pl.Date, "product": pl.String,
                                      "contract": pl.String}, orient="row")
    if frame.is_empty() or frame.select(IDENTITY).is_duplicated().any():
        raise ValueError("priority product/month/day coordinates must be unique and nonempty")
    return frame


def reconcile(targets: pl.DataFrame, daily: pl.DataFrame, warmup: pl.DataFrame,
              clocks: dict[str, pl.DataFrame]) -> list[dict]:
    """Keep missing, ambiguous and warmup identities distinct from represented rows."""
    if set(clocks) != set(WINDOWS):
        raise ValueError("four explicitly labelled clocks are required")
    if targets.select(IDENTITY).is_duplicated().any():
        raise ValueError("duplicate requested coordinate")
    def select_rows(frame: pl.DataFrame, label: str) -> pl.DataFrame:
        return (frame.select(ROW_COLUMNS).join(targets, on=IDENTITY, how="semi")
                .with_columns(pl.lit(label).alias("release_role")))
    found = pl.concat([select_rows(daily, "daily"), select_rows(warmup, "warmup")])
    groups = found.partition_by(IDENTITY, as_dict=True)
    # The clock's key must not collapse multiple physical generations into a
    # single apparent observation, even if their quotes happen to agree.
    clock_maps = {}
    for name, frame in clocks.items():
        values = frame.select("date", "physical_contract", "status", "entry_quote", "entry_volume")
        if values.select("date", "physical_contract").is_duplicated().any():
            raise ValueError(f"ambiguous clock identity: {name}")
        clock_maps[name] = {(row["date"], row["physical_contract"]): row
                            for row in values.iter_rows(named=True)}
    results = []
    for target in targets.sort(IDENTITY).iter_rows(named=True):
        key = tuple(target[c] for c in IDENTITY)
        candidates = groups.get(key)
        count = 0 if candidates is None else candidates.height
        result = {**target, "matching_release_rows": count, "classification": "absent"}
        if count > 1:
            result["classification"] = "ambiguous_physical_generation"
            result["candidate_physical_contracts"] = candidates["physical_contract"].to_list()
        elif count == 1:
            source = candidates.row(0, named=True)
            result.update({c: source[c] for c in ROW_COLUMNS if c not in IDENTITY})
            result["classification"] = source["release_role"]
            result["clocks"] = {}
            for name, lookup in clock_maps.items():
                value = lookup.get((target["date"], source["physical_contract"]))
                result["clocks"][name] = None if value is None else {
                    k: value[k] for k in ("status", "entry_quote", "entry_volume")}
        result["date"] = str(result["date"])
        results.append(result)
    return results


def audit(plan_path: Path, frozen_path: Path, output: Path) -> dict:
    if output.exists() or output.is_symlink():
        raise FileExistsError("preserve prior evidence and select a new output")
    plan = json.loads(plan_path.read_text())
    frozen = json.loads(frozen_path.read_text())
    if frozen.get("contract") != "tw_futures_v18_source_pins_readback_v1":
        raise ValueError("expected the reviewed v18 frozen source readback")
    pins = frozen["source_pins_verified"]
    daily_paths = [Path(p) for p in pins if p.endswith("/continuous_daily.parquet")]
    if len(daily_paths) != 1:
        raise ValueError("one frozen daily source required")
    daily_path = daily_paths[0]
    warmup_path = daily_path.with_name("first_observation_warmup.parquet")
    entries = [Path(p) for p in pins if p.endswith("/entries.parquet")]
    clock_paths = {}
    for path in entries:
        parts = path.parts
        label = path.parent.name + ("_300m" if "entry_window_300m" in parts else "_1m")
        if label not in WINDOWS or label in clock_paths:
            raise ValueError("clock path labels are ambiguous")
        clock_paths[label] = path
    if set(clock_paths) != set(WINDOWS):
        raise ValueError("frozen readback does not identify all four windows")
    source_paths = [plan_path, frozen_path, daily_path, warmup_path, *clock_paths.values(), Path(__file__)]
    hashes = {str(path.resolve()): sha256_file(path) for path in source_paths}
    for path in [daily_path, *clock_paths.values()]:
        if hashes[str(path.resolve())] != pins[str(path)]:
            raise ValueError(f"frozen release source changed: {path}")
    targets = target_frame(plan)
    # Filter by requested product before materializing; never filter solely on
    # contract month or treat two corporate generations as interchangeable.
    products = targets["product"].unique().to_list()
    daily = pl.scan_parquet(daily_path).filter(pl.col("product").is_in(products)).select(ROW_COLUMNS).collect()
    warmup = pl.scan_parquet(warmup_path).filter(pl.col("product").is_in(products)).select(ROW_COLUMNS).collect()
    wanted_keys = (daily.select("date", "physical_contract").join(
        targets, on="date", how="semi").unique())
    clocks = {name: (pl.scan_parquet(path).join(wanted_keys.lazy(),
                        on=["date", "physical_contract"], how="semi").collect())
              for name, path in clock_paths.items()}
    rows = reconcile(targets, daily, warmup, clocks)
    if any(sha256_file(Path(path)) != digest for path, digest in hashes.items()):
        raise ValueError("a source changed during readback")
    classes = Counter(r["classification"] for r in rows)
    status = {name: dict(Counter((r.get("clocks", {}).get(name) or {}).get("status", "no_clock_row")
                                for r in rows)) for name in WINDOWS}
    report = dict(contract="futures_priority_release_reconciliation_v1",
        completed_at_utc=datetime.now(UTC).isoformat(), inputs_sha256=hashes,
        source_version=frozen["contract"], warmup_sha256_is_observed_not_manifest_bound=True,
        target_count=len(rows), classifications=dict(classes), clock_statuses=status, rows=rows,
        interpretation="Daily and window presence are distinct; a represented row is not an order or a fill.",
        source_point_in_time_verified=False, financial_or_training_admitted=False,
        provider_requests=0, priority_plan_mutated=False)
    output.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(output, report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--frozen", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.plan, args.frozen, args.output)
    print(json.dumps({k: result[k] for k in ("target_count", "classifications", "clock_statuses")},
                     ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()

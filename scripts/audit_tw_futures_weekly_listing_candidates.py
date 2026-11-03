#!/usr/bin/env python3
"""Bound ordinary MTX weekly listing schedules to retained exchange sources.

This retrospective audit excludes unusual calendars and is not a listing
admission, a complete holiday calendar, or a trading policy.
"""
from __future__ import annotations

import argparse
import calendar
from datetime import UTC, date, datetime, timedelta
import gzip
import hashlib
import json
from pathlib import Path
import re

import polars as pl

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from scripts.build_taifex_option_finmind_reference_window import capacity_check


REVIEWED = (
    ("431b226150c5d8cf835d15c5bcd1ebab0fa1211c013ac23e65a016a6b3d35d87",
     "739734aa3f9f925a89c64daec8b67b71c4c5074dbf55e7a6852170b103035bb2",
     "2013-07-31", "https://www.taifex.com.tw/cht/11/newsDetail?newsType=1&idx=4830"),
    ("cd3c62c55ba089669c348a048d70f07524940ac6f7018db1410ed66b1e893c73",
     "134a39332c5af3d80c2ba2f27cdd375c4f387db54fd2a6b3e0189cda7799bf1c",
     "2022-11-07", "https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/20221107新聞稿.pdf"),
)
TWO_WEEK_START = date(2022, 11, 9)
FIRST_SCOPE_DAY = date(2013, 8, 1)  # Date-only 2013 notice: no claim about 07-31 at 08:45.


def nominal_expiry(contract: str) -> date | None:
    m = re.fullmatch(r"(\d{4})(\d{2})W([1245])", contract)
    if not m:
        return None
    year, month, week = map(int, m.groups())
    if not 1 <= month <= 12:
        return None
    first = date(year, month, 1)
    day = 1 + (2 - first.weekday()) % 7 + (week - 1) * 7
    return date(year, month, day) if day <= calendar.monthrange(year, month)[1] else None


def classify(contract: str, observed_day: date, official_expiry: date | None, market_days: set[date]) -> dict:
    """Only classify ordinary-session candidates; leave all other cases unknown."""
    expiry = nominal_expiry(contract)
    if expiry is None or observed_day < FIRST_SCOPE_DAY:
        return {"state": "outside_reviewed_scope", "scheduled_listing": None}
    two_week = expiry - timedelta(days=14)
    one_week = expiry - timedelta(days=7)
    listing = two_week if two_week >= TWO_WEEK_START else one_week
    if listing < date(2013, 7, 31):
        return {"state": "outside_reviewed_scope", "scheduled_listing": None}
    if listing not in market_days or expiry not in market_days:
        return {"state": "holiday_or_calendar_unverified", "scheduled_listing": listing}
    if official_expiry != expiry:
        return {"state": "official_expiry_differs_or_missing", "scheduled_listing": listing}
    if not listing <= observed_day <= expiry:
        return {"state": "first_observation_outside_scheduled_lifetime", "scheduled_listing": listing}
    return {"state": "ordinary_schedule_candidate", "scheduled_listing": listing}


def build(workspace: Path, warm_path: Path, observations_manifest: Path, source_dir: Path,
          checkpoint: Path, output: Path, report: Path) -> dict:
    workspace, output, report = workspace.resolve(), output.resolve(), report.resolve()
    if (not output.is_relative_to(workspace) or not output.relative_to(workspace).parts[0].startswith("data_")
            or output.exists() or output.is_symlink() or report.exists() or report.is_symlink()
            or not report.is_relative_to(workspace / "artifacts/markets")):
        raise ValueError("requires new data_* output and artifacts/markets report")
    source_pins = []
    for content_hash, compressed_hash, published, url in REVIEWED:
        path = source_dir / f"{content_hash}.body.gz"
        if (path.is_symlink() or sha256_file(path) != compressed_hash
                or hashlib.sha256(gzip.decompress(path.read_bytes())).hexdigest() != content_hash):
            raise ValueError("reviewed official source changed")
        source_pins.append({"path": str(path.resolve()), "sha256": compressed_hash,
                            "content_sha256": content_hash, "published_date": published, "url": url})
    manifest_sha = sha256_file(observations_manifest)
    manifest = json.loads(observations_manifest.read_text())
    if (manifest.get("contract") != "tw_futures_omitted_first_day_windows_v1"
            or manifest.get("training_ready") is not False):
        raise ValueError("unsupported first-observation research artifact")
    observation_path = Path(manifest["data_path"])
    if sha256_file(observation_path) != manifest["data_sha256"]:
        raise ValueError("first-observation data hash differs")
    warm_sha, checkpoint_sha = sha256_file(warm_path), sha256_file(checkpoint)
    pins = manifest["input_sha256"]
    if pins.get(str(warm_path)) != warm_sha or pins.get(str(checkpoint)) != checkpoint_sha:
        raise ValueError("listing candidates must use the same warmup and checkpoint inputs")
    inventory = json.loads(checkpoint.read_text())["identity"]["objects"]
    days = {date.fromisoformat(obj["date"]) for obj in inventory if obj.get("rows", 0) > 0}
    warm = (pl.scan_parquet(warm_path).filter((pl.col("product") == "MTX")
        & (pl.col("series_type") == "weekly") & (pl.col("date") == pl.col("first_observed_date"))
        & (pl.col("outright_volume") > 0) & (pl.col("date") >= date(2011, 1, 3))
        & (pl.col("date") <= date(2026, 9, 24)))
        .select("date", "physical_contract", "product", "contract", "official_expiry", "outright_volume").collect())
    observations = (pl.scan_parquet(observation_path).filter((pl.col("product") == "MTX")
        & (pl.col("series_type") == "weekly")).select("date", "physical_contract", "source_status",
        "0845_raw_print_rows", "0845_invalid_raw_rows", "0900_raw_print_rows", "0900_invalid_raw_rows").collect())
    if (warm.select("date", "physical_contract").is_duplicated().any()
            or observations.select("date", "physical_contract").is_duplicated().any()):
        raise ValueError("duplicate weekly physical target")
    joined = warm.join(observations, on=["date", "physical_contract"], how="left", validate="1:1")
    if joined.height != observations.height or joined["source_status"].null_count():
        raise ValueError("weekly observations do not match warmup target axis")
    records = []
    for row in joined.iter_rows(named=True):
        state = classify(row["contract"], row["date"], row["official_expiry"], days)
        records.append({**row, **state})
    result = pl.DataFrame(records).sort("date", "physical_contract")
    by_state = result.group_by("state").agg(pl.len().alias("contract_days")).sort("state").to_dicts()
    candidates = result.filter(pl.col("state") == "ordinary_schedule_candidate")
    same_day = candidates.filter(pl.col("date") == pl.col("scheduled_listing"))
    counts = {"candidate_contract_days": candidates.height, "first_observation_on_scheduled_listing": same_day.height,
              "first_observation_after_scheduled_listing": candidates.height - same_day.height}
    for label in ("0845", "0900"):
        condition = ((pl.col("source_status") == "raw_receipt_matched")
                     & (pl.col(f"{label}_raw_print_rows") > 0)
                     & (pl.col(f"{label}_invalid_raw_rows") == 0))
        counts[f"{label}_positive_raw_window"] = candidates.filter(condition).height
    if (sha256_file(observations_manifest) != manifest_sha or sha256_file(observation_path) != manifest["data_sha256"]
            or sha256_file(warm_path) != warm_sha or sha256_file(checkpoint) != checkpoint_sha
            or any(sha256_file(Path(pin["path"])) != pin["sha256"] for pin in source_pins)):
        raise ValueError("a candidate source changed during processing")
    capacity = capacity_check(output.parent)
    output.mkdir(parents=True, exist_ok=False)
    payload = output / "candidates.parquet"
    atomic_write_parquet(payload, result)
    doc = {"contract": "taifex_mtx_ordinary_weekly_listing_candidates_v1", "created_at_utc": datetime.now(UTC).isoformat(),
           "candidate_rows": result.height, "by_state": by_state, "ordinary_candidates": counts,
           "data_path": str(payload), "data_sha256": sha256_file(payload), "sources": source_pins,
           "inputs_sha256": {str(observations_manifest): manifest_sha, str(observation_path): manifest["data_sha256"],
                             str(warm_path): warm_sha, str(checkpoint): checkpoint_sha},
           "implementation_sha256": sha256_file(Path(__file__)), "capacity": capacity, "training_ready": False,
           "limitations": ["Only ordinary nominal Wednesday sessions with matching later official expiry are inspected.",
               "Calendar and expiry checks are retrospective; actual listing and decision-time visibility are not independently certified.",
               "The candidate targets were selected using first historical observation and are not a model eligibility mask.",
               "Observed window prints are not reconciled fills, first-day capacity, or an account tape.",
               "No general holiday, later rule revision, adjusted position, or user training contract is admitted."]}
    atomic_write_json(output / "manifest.json", doc)
    atomic_write_json(report, doc)
    return doc


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("workspace", "warm-path", "observations-manifest", "source-dir", "checkpoint", "output", "report"):
        p.add_argument("--" + name, type=Path, required=True)
    a = p.parse_args()
    result = build(a.workspace, a.warm_path, a.observations_manifest, a.source_dir, a.checkpoint, a.output, a.report)
    print(json.dumps({"candidate_rows": result["candidate_rows"], "by_state": result["by_state"],
                      "ordinary_candidates": result["ordinary_candidates"]}))


if __name__ == "__main__":
    main()

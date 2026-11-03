#!/usr/bin/env python3
"""Read back zero-print futures products without discarding held positions.

Separately inspect native FinMind observations or the pinned futures release.
Neither mode establishes the investor's position transfer or admits training.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.parquet_integrity import parquet_receipt_error


def candidates(universe: dict) -> list[dict]:
    if universe.get("contract") != "tw_futures_universe_release_scope_v1":
        raise ValueError("expected frozen universe readback")
    rows = [item for item in universe["products"] if not item.get("positive_daily_rows")]
    if not rows or len({x["product"] for x in rows}) != len(rows):
        raise ValueError("candidate product identities are empty or duplicated")
    return sorted(rows, key=lambda x: x["product"])


def release_rows(frame: pl.DataFrame, codes: list[str]) -> list[dict]:
    required = {"product", "date", "physical_contract", "official_volume", "outright_volume",
                "executable", "open_interest", "valuation_settlement", "cash_settlement",
                "corporate_identity_candidate_only"}
    if required - set(frame.columns):
        raise ValueError("release lacks required observation and account fields")
    frame = frame.filter(pl.col("product").is_in(codes))
    if set(frame["product"].unique().to_list()) != set(codes):
        raise ValueError("a requested release product has no accounting rows")
    if frame.select("date", "physical_contract").is_duplicated().any():
        raise ValueError("ambiguous release date/physical-contract key")
    rows = frame.group_by("product").agg(
        pl.len().alias("rows"),
        (pl.col("official_volume") > 0).fill_null(False).sum().alias("positive_official_volume_rows"),
        (pl.col("outright_volume") > 0).fill_null(False).sum().alias("positive_outright_rows"),
        pl.col("executable").fill_null(False).sum().alias("executable_rows"),
        (pl.col("open_interest") > 0).fill_null(False).sum().alias("positive_open_interest_rows"),
        (pl.col("valuation_settlement") > 0).fill_null(False).sum().alias("positive_valuation_rows"),
        pl.col("cash_settlement").fill_null(False).sum().alias("cash_settlement_rows"),
        pl.col("corporate_identity_candidate_only").fill_null(False).sum().alias("candidate_identity_rows"),
    ).sort("product").to_dicts()
    return rows


def read_release(universe: dict, release: Path) -> tuple[list[dict], dict]:
    release = release.resolve()
    digest = sha256_file(release)
    if universe.get("input_sha256", {}).get(str(release)) != digest:
        raise ValueError("release does not match frozen universe readback")
    codes = [x["product"] for x in candidates(universe) if x["positive_daily_rows"] == 0]
    frame = pl.scan_parquet(release).filter(pl.col("product").is_in(codes)).collect()
    rows = release_rows(frame, codes)
    if sha256_file(release) != digest:
        raise ValueError("release changed during readback")
    return rows, {str(release): digest}


def read_vendor(universe: dict, root: Path) -> tuple[list[dict], dict]:
    root = root.resolve()
    db = root / "queue.sqlite3"
    if not db.is_file() or db.is_symlink():
        raise ValueError("native provider registry missing or redirected")
    rows, pins = [], {}
    with sqlite3.connect(db.as_uri() + "?mode=ro", uri=True) as con:
        for item in candidates(universe):
            code = item["product"]
            task = con.execute("SELECT state,receipt_path FROM tasks WHERE dataset=? AND data_id=? AND partition=?",
                               ("TaiwanFuturesDaily", code, "history")).fetchone()
            if task is None or task[0] != "complete" or not task[1]:
                raise ValueError(f"native provider history not complete: {code}")
            receipt_path = root / task[1]
            if receipt_path.is_symlink() or not receipt_path.resolve().is_relative_to(root):
                raise ValueError("unsafe receipt path")
            receipt_bytes = receipt_path.read_bytes()
            receipt = json.loads(receipt_bytes)
            if (receipt.get("dataset") != "TaiwanFuturesDaily" or receipt.get("data_id") != code
                    or receipt.get("partition") != "history" or receipt.get("status") != "complete"):
                raise ValueError(f"native receipt identity mismatch: {code}")
            if error := parquet_receipt_error(root, receipt, verify_hash=True):
                raise ValueError(f"native receipt payload error for {code}: {error}")
            data = root / receipt["parquet_path"]
            frame = pl.read_parquet(data)
            if (frame.is_empty() or set(frame["futures_id"].unique().to_list()) != {code}
                    or frame.select("date", "contract_date", "trading_session").is_duplicated().any()):
                raise ValueError(f"native futures observations ambiguous: {code}")
            rows.append({"product": code, "rows": frame.height,
                         "positive_volume_rows": frame.filter(pl.col("volume") > 0).height,
                         "positive_open_interest_rows": frame.filter(pl.col("open_interest") > 0).height,
                         "positive_settlement_rows": frame.filter(pl.col("settlement_price") > 0).height,
                         "first_date": frame["date"].min(), "last_date": frame["date"].max(),
                         "sessions": sorted(frame["trading_session"].unique().to_list()),
                         "receipt_path": str(receipt_path.resolve()),
                         "payload_path": str(data.resolve())})
            pins[str(receipt_path.resolve())] = sha256_file(receipt_path)
            pins[str(data.resolve())] = sha256_file(data)
            if receipt_path.read_bytes() != receipt_bytes:
                raise ValueError(f"native receipt changed during readback: {code}")
    if any(sha256_file(Path(path)) != digest for path, digest in pins.items()):
        raise ValueError("native evidence changed during readback")
    return rows, pins


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--universe", type=Path, required=True)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--release", type=Path)
    source.add_argument("--finmind-root", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    if output.exists() or not output.is_relative_to((ROOT / "artifacts/markets").resolve()):
        parser.error("select a new output inside artifacts/markets")
    universe_path = args.universe.resolve()
    universe_bytes = universe_path.read_bytes()
    universe = json.loads(universe_bytes)
    rows, pins = (read_release(universe, args.release) if args.release
                  else read_vendor(universe, args.finmind_root))
    if universe_path.read_bytes() != universe_bytes:
        raise ValueError("universe readback changed")
    pins[str(universe_path)] = sha256_file(universe_path)
    pins[str(Path(__file__).resolve())] = sha256_file(Path(__file__).resolve())
    report = {"contract": "tw_futures_position_only_source_readback_v1",
              "observed_at_utc": datetime.now(UTC).isoformat(),
              "source_kind": "release" if args.release else "finmind_history",
              "input_sha256": pins, "products": rows, "provider_calls": 0,
              "financial_or_training_admitted": False,
              "limitations": ["Observed open interest is market-wide, not the model account's position.",
                  "Daily prices and volume do not establish a decision-clock execution.",
                  "Corporate transfers, terminal settlement and historical PIT require separate evidence.",
                  "Source coverage and stored release rows are not proof of a complete listing history."]}
    output.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(output, report)
    print(json.dumps({"output": str(output), "source_kind": report["source_kind"],
                      "products": rows}, ensure_ascii=False))


if __name__ == "__main__":
    main()

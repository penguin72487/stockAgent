"""Physical-contract minute facts for one 08:45 daily futures decision.

The dense tape is executor-only. Bars are right labelled: 08:46 covers
[08:45,08:46). A 13:20 limit can use only subsequent bars; replacing it at
13:24 first consumes the 13:25 bar. No stock auction or daily-close fallback.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import polars as pl

from downloader.artifact_io import sha256_file

MINUTE_MODE = "tw_stock_futures_day_trade_0845_minute"
MINUTE_DATASET = "taifex_stock_futures_minute_v1"
MINUTE_CONTRACT_VERSION = 1
EVENT_MINUTES = (526, 800, *range(801, 811))  # 08:46, 13:20..13:30
BAR_FIELDS = ("vwap", "high", "low", "close", "capacity")
TAPE_CHANNELS = (
    "multiplier", "fee_per_side", "tax_rate",
    *(f"{minute // 60:02d}{minute % 60:02d}_{field}"
      for minute in EVENT_MINUTES for field in BAR_FIELDS),
)
TAPE_FIELDS = len(TAPE_CHANNELS)


def build_futures_minute_bars(transactions: pl.DataFrame) -> pl.DataFrame:
    """Aggregate matched outright prints, preserving physical identity."""
    return (
        transactions.filter(
            (pl.col("session") == "day")
            & (pl.col("event_date") == pl.col("trading_date"))
            & ~pl.col("delivery_month_week").str.contains("/", literal=True)
            & pl.col("price").is_finite() & (pl.col("price") > 0)
            & pl.col("matched_quantity").is_finite()
            & (pl.col("matched_quantity") > 0)
        )
        .with_columns(
            pl.col("event_time").cast(pl.Int32).alias("hhmmss"),
            pl.concat_str("product", pl.lit(":"), "delivery_month_week")
            .alias("physical_contract"),
        )
        .with_columns(
            ((pl.col("hhmmss") // 10000) * 60
             + (pl.col("hhmmss") // 100) % 100 + 1).alias("minute")
        )
        .filter(pl.col("minute").is_in(EVENT_MINUTES))
        .sort("event_ts", "source_row_number")
        .group_by("trading_date", "physical_contract", "minute", maintain_order=True)
        .agg(
            ((pl.col("price") * pl.col("matched_quantity")).sum()
             / pl.col("matched_quantity").sum()).alias("vwap"),
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("close"),
            pl.col("matched_quantity").sum().alias("volume"),
            pl.col("source_sha256").first().alias("source_file_sha256"),
        )
        .rename({"trading_date": "date"})
        .sort("date", "physical_contract", "minute")
    )


def load_futures_minute_tape(
    path: str | Path, selected: pl.DataFrame, dates: np.ndarray,
    symbols: tuple[str, ...], *, daily_sha256: str, fee: float,
    participation: float,
) -> tuple[np.ndarray, dict]:
    """Require coverage of every panel date; absent trades have zero capacity."""
    from stockagent.research.taifex_transaction_tax import stock_index_futures_tax_rate

    if not np.isfinite(fee) or fee < 0:
        raise ValueError("fee must be finite and non-negative")
    if not np.isfinite(participation) or not 0 < participation <= 1:
        raise ValueError("participation must be in (0,1]")
    path = Path(path)
    manifest_path = path.parent / "manifest.json"
    if not path.is_file() or not manifest_path.is_file():
        raise FileNotFoundError(f"08:45 futures minute data/receipt missing: {path}")
    receipt = json.loads(manifest_path.read_text())
    if (receipt.get("dataset") != MINUTE_DATASET
            or receipt.get("contract_version") != MINUTE_CONTRACT_VERSION
            or receipt.get("status") != "complete"
            or receipt.get("source_daily_sha256") != daily_sha256
            or receipt.get("outputs", {}).get("minutes", {}).get("sha256") != sha256_file(path)):
        raise ValueError("futures minute identity, completeness or source/output SHA mismatch")
    covered = set(receipt.get("covered_dates", []))
    missing = sorted(set(map(str, dates)) - covered)
    if missing:
        raise ValueError(f"futures minute archive misses {len(missing)} panel dates: {missing[:5]}")
    frame = pl.read_parquet(path)
    required = {"date", "physical_contract", "minute", "vwap", "high", "low",
                "close", "volume", "source_file_sha256"}
    if not required <= set(frame.columns):
        raise ValueError("futures minute schema is incomplete")
    if frame.select(pl.any_horizontal(pl.col(c).is_null() for c in required).any()).item():
        raise ValueError("futures minute facts cannot contain nulls")
    if frame.select("date", "physical_contract", "minute").is_duplicated().any():
        raise ValueError("duplicate physical-contract minute")
    sources = {item["date"]: item["sha256"] for item in receipt.get("sources", [])}
    if any(item.get("day_session_rows", 0) <= 0 or item.get("day_last_time", 0) < 133000
           for item in receipt.get("sources", [])):
        raise ValueError("futures minute source lacks completed day-session evidence")
    if (set(sources) != covered or len(sources) != len(receipt.get("sources", []))
            or len(covered) != len(receipt.get("covered_dates", []))):
        raise ValueError("minute source inventory and covered dates disagree")
    for row in frame.select("date", "source_file_sha256").unique().iter_rows(named=True):
        if sources.get(str(row["date"])) != row["source_file_sha256"]:
            raise ValueError("minute row is not bound to its dated source receipt")
    if frame.filter(
        ~pl.col("minute").is_in(EVENT_MINUTES)
        | ~pl.all_horizontal(pl.col(c).is_finite() & (pl.col(c) > 0)
                             for c in ("vwap", "high", "low", "close", "volume"))
        | (pl.col("high") < pl.col("low"))
        | ~pl.col("close").is_between(pl.col("low"), pl.col("high"))
        | ~pl.col("vwap").is_between(pl.col("low") - 1e-8, pl.col("high") + 1e-8)
    ).height:
        raise ValueError("invalid minute price, interval or capacity")
    tape = np.zeros((len(dates), len(symbols), 2, TAPE_FIELDS), dtype=np.float32)
    di = {str(d): i for i, d in enumerate(dates)}
    si = {s: i for i, s in enumerate(symbols)}
    ei = {m: i for i, m in enumerate(EVENT_MINUTES)}
    keys = selected.select("date", "physical_contract", "underlying_symbol",
                           "candidate_slot", "contract_multiplier")
    for row in keys.iter_rows(named=True):
        d, s = di.get(str(row["date"])), si.get(row["underlying_symbol"])
        if d is not None and s is not None:
            tape[d, s, row["candidate_slot"], :3] = (
                row["contract_multiplier"], fee, stock_index_futures_tax_rate(row["date"])
            )
    aligned = frame.join(keys, on=["date", "physical_contract"], how="inner", validate="m:1")
    for row in aligned.iter_rows(named=True):
        d, s = di.get(str(row["date"])), si.get(row["underlying_symbol"])
        if d is None or s is None:
            continue
        offset = 3 + ei[row["minute"]] * len(BAR_FIELDS)
        tape[d, s, row["candidate_slot"], offset:offset + 5] = (
            row["vwap"], row["high"], row["low"], row["close"],
            np.floor(row["volume"] * participation),
        )
    return tape, receipt

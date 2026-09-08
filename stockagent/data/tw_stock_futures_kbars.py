"""Adapt receipt-backed one-minute futures KBars to the existing daily executor.

No transaction archive is read. Shioaji futures Amount is price * contracts
(not TWD notional); Amount / Volume preserves the executor's minute VWAP.
"""
from __future__ import annotations

from datetime import date, datetime, time
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import re
from zoneinfo import ZoneInfo

import polars as pl

from downloader.artifact_io import sha256_file
from stockagent.data.tw_stock_futures_minute import EVENT_MINUTES

KBAR_SOURCE = "shioaji_exact_futures_kbars_1m"


def contract_sources_digest(contracts: list[dict]) -> str:
    return hashlib.sha256(json.dumps(contracts, sort_keys=True).encode()).hexdigest()


def validate_kbar_completion(item: dict) -> None:
    start, end = date.fromisoformat(item["start"]), date.fromisoformat(item["end"])
    finished = datetime.fromisoformat(item["finished_at_utc"].replace("Z", "+00:00"))
    if start > end or finished.tzinfo is None:
        raise ValueError("invalid KBar query interval or completion timezone")
    if finished.astimezone(ZoneInfo("Asia/Taipei")) < datetime.combine(end, time(13, 45), ZoneInfo("Asia/Taipei")):
        raise ValueError("KBar query finished before the completed day session")


def empty_minute_bars() -> pl.DataFrame:
    return pl.DataFrame(schema={
        "date": pl.Date, "physical_contract": pl.String, "minute": pl.Int32,
        **{field: pl.Float64 for field in ("vwap", "high", "low", "close", "volume")},
        "source_file_sha256": pl.String,
    })


def normalize_futures_kbars(frame: pl.DataFrame, *, code: str,
                           physical_contract: str, source_sha256: str) -> pl.DataFrame:
    required = {"ts", "trading_date", "query_contract", "security_type",
                "Open", "High", "Low", "Close", "Volume", "Amount"}
    if not required <= set(frame.columns):
        raise ValueError(f"incomplete one-minute KBar schema: {code}")
    if frame.select(pl.any_horizontal(pl.col(c).is_null() for c in required).any()).item():
        raise ValueError(f"null one-minute KBar facts: {code}")
    if frame["ts"].is_duplicated().any():
        raise ValueError(f"duplicate KBar timestamp: {code}")
    frame = frame.with_columns(
        pl.col("ts").cast(pl.Datetime("ns")).alias("bar_end"),
        pl.col("trading_date").cast(pl.Date),
        *(pl.col(c).cast(pl.Float64) for c in ("Open", "High", "Low", "Close", "Volume", "Amount")),
    )
    if frame.filter(
        (pl.col("query_contract") != code) | (pl.col("security_type") != "FUT")
        | (pl.col("ts") % 60_000_000_000 != 0)
        | ~pl.all_horizontal(pl.col(c).is_finite() for c in ("Open", "High", "Low", "Close", "Volume", "Amount"))
        | (pl.col("Volume") < 0) | (pl.col("Volume") != pl.col("Volume").floor())
        | (pl.col("Amount") < 0)
        | ((pl.col("Volume") == 0) & (pl.col("Amount") != 0))
    ).height:
        raise ValueError(f"invalid KBar identity, minute timestamp or quantity: {code}")
    frame = frame.filter(pl.col("Volume") > 0).with_columns(
        (pl.col("Amount") / pl.col("Volume")).alias("vwap"),
    )
    if frame.filter(
        (pl.col("Low") <= 0) | (pl.col("High") < pl.col("Low"))
        | ~pl.col("Open").is_between(pl.col("Low"), pl.col("High"))
        | ~pl.col("Close").is_between(pl.col("Low"), pl.col("High"))
        | ~pl.col("vwap").is_between(pl.col("Low") - 1e-8, pl.col("High") + 1e-8)
    ).height:
        raise ValueError(f"KBar OHLC or Amount/Volume price scale mismatch: {code}")
    return (
        frame.with_columns(
            (pl.col("bar_end").dt.hour().cast(pl.Int32) * 60
             + pl.col("bar_end").dt.minute()).alias("minute"),
        )
        .filter((pl.col("bar_end").dt.date() == pl.col("trading_date"))
                & pl.col("minute").is_in(EVENT_MINUTES))
        .select(
            pl.col("trading_date").alias("date"),
            pl.lit(physical_contract).alias("physical_contract"), "minute", "vwap",
            pl.col("High").alias("high"), pl.col("Low").alias("low"),
            pl.col("Close").alias("close"), pl.col("Volume").alias("volume"),
            pl.lit(source_sha256).alias("source_file_sha256"),
        )
    )


def read_futures_kbar_sources(root: Path, selected: pl.DataFrame,
                             expected_dates: list[date]) -> tuple[pl.DataFrame, list[dict], list[dict]]:
    """Use the existing collector's inventory/chunks; missing != zero volume."""
    from downloader.download_shioaji_historical_market_data import load_inventory, _valid_receipt

    needed = {(str(r["date"]), r["physical_contract"]): r for r in selected.iter_rows(named=True)}
    wanted_by_contract = defaultdict(set)
    for d, contract in needed:
        wanted_by_contract[contract].add(d)
    covered: dict[tuple[str, str], dict] = {}
    observed_days: dict[tuple[str, str], pl.DataFrame] = {}
    frames = []
    inventory = root / "inventory" / "contracts.parquet"
    if inventory.is_file():
        digest = sha256_file(inventory)
        manifest = json.loads(inventory.with_name("manifest.json").read_text())
        if manifest.get("source") != "shioaji_contract_v2" or manifest.get("contracts_sha256") != digest:
            raise ValueError("KBar physical-contract inventory SHA mismatch")
        for row in load_inventory(root):
            if row.collection != "exact_futures" or row.security_type != "FUT":
                continue
            # A current R1/R2 target is not a historical physical-contract map.
            if row.code.endswith(("R1", "R2")) or not re.fullmatch(r"\d{6}", row.delivery_month):
                continue
            physical = f"{row.root}:{row.delivery_month}"
            wanted = wanted_by_contract[physical]
            if not wanted:
                continue
            base = root / "contracts" / row.asset_class / row.code / "kbars"
            for receipt_path in sorted(base.glob("*/receipt.json")):
                data_path = receipt_path.with_name("data.parquet")
                receipt = _valid_receipt(receipt_path, data_path, method="kbars", code=row.code)
                if receipt is None:
                    raise ValueError(f"invalid KBar chunk receipt or SHA: {receipt_path}")
                start, end = date.fromisoformat(receipt["start"]), date.fromisoformat(receipt["end"])
                if start > end or start < row.begin_date or end > row.end_date:
                    raise ValueError(f"KBar query interval outside contract inventory: {receipt_path}")
                dates = sorted(d for d in wanted if str(start) <= d <= str(end))
                if not dates:
                    continue
                # A completed API request during the session is still partial history.
                validate_kbar_completion(receipt)
                receipt_sha = sha256_file(receipt_path)
                chunk_sha = receipt.get("sha256") if receipt["status"] == "complete" else receipt_sha
                raw = pl.read_parquet(data_path) if receipt["status"] == "complete" else pl.DataFrame()
                if raw.height != receipt.get("rows"):
                    raise ValueError(f"KBar receipt row count mismatch: {receipt_path}")
                if raw.height:
                    bars = normalize_futures_kbars(raw, code=row.code, physical_contract=physical, source_sha256=chunk_sha)
                    raw_dates = set(map(str, raw["trading_date"].unique()))
                    if raw_dates != set(receipt["observed_trading_dates"]):
                        raise ValueError(f"KBar chunk dates disagree with receipt: {receipt_path}")
                    # A query's last evening can belong to the next trading date.
                    # Only same-date completed day-session bars establish coverage here.
                    bar_end = pl.col("ts").cast(pl.Datetime("ns"))
                    minute = bar_end.dt.hour().cast(pl.Int32) * 60 + bar_end.dt.minute()
                    day_volume = (raw.filter((bar_end.dt.date() == pl.col("trading_date"))
                                            & minute.is_between(526, 825))
                                  .group_by("trading_date").agg(pl.col("Volume").sum()))
                    volumes = {str(d): v for d, v in day_volume.iter_rows()}
                    if sha256_file(data_path) != chunk_sha:
                        raise ValueError(f"KBar source changed during read: {data_path}")
                else:
                    if receipt.get("observed_trading_dates"):
                        raise ValueError(f"empty KBar receipt claims observed trading dates: {receipt_path}")
                    volumes = {}
                if sha256_file(receipt_path) != receipt_sha:
                    raise ValueError(f"KBar receipt changed during read: {receipt_path}")
                for d in dates:
                    if needed[(d, physical)]["volume"] > 0 and volumes.get(d, 0) <= 0:
                        continue  # Missing provider history is not a no-trade day.
                    # Incremental collector queries can overlap when the final
                    # chunk grows. Compare the completed day's actual trades,
                    # not whole-chunk hashes or receipt timestamps. Zero-volume
                    # carried prices are not execution observations.
                    day_observed = (raw.filter(
                        (pl.col("trading_date").cast(pl.String) == d)
                        & (bar_end.dt.date() == pl.col("trading_date"))
                        & minute.is_between(526, 825) & (pl.col("Volume") > 0)
                    ).select("ts", "Open", "High", "Low", "Close", "Volume", "Amount")
                        .sort("ts")) if raw.height else pl.DataFrame()
                    if (d, physical) in covered:
                        previous = observed_days[(d, physical)]
                        if (previous.height != day_observed.height
                                or (previous.height and not previous.equals(day_observed))):
                            raise ValueError(f"conflicting overlapping KBar chunks for {d} {physical}")
                        continue
                    observed_days[(d, physical)] = day_observed
                    if raw.height:
                        frames.append(bars.filter(pl.col("date").cast(pl.String) == d))
                    covered[(d, physical)] = {
                        "physical_contract": physical, "sha256": chunk_sha,
                        "query_contract": row.code, "inventory_sha256": digest,
                        "path": str(data_path), "receipt_path": str(receipt_path),
                        "receipt_sha256": receipt_sha, "status": receipt["status"],
                        "start": str(start), "end": str(end), "finished_at_utc": receipt["finished_at_utc"],
                    }
        if sha256_file(inventory) != digest:
            raise ValueError("KBar contract inventory changed during read")
    missing = [{"date": d, "physical_contract": contract} for d, contract in sorted(needed.keys() - covered.keys())]
    sources = []
    by_date = defaultdict(list)
    for (d, _), value in sorted(covered.items()):
        by_date[d].append(value)
    for d in expected_dates:
        contracts = by_date[str(d)]
        sources.append({"date": str(d), "contracts": contracts, "sha256": contract_sources_digest(contracts)})
    output = pl.concat(frames, how="vertical_relaxed") if frames else empty_minute_bars()
    return output.sort("date", "physical_contract", "minute"), sources, missing

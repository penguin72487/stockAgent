"""Add receipt-backed TAIFEX histories to the local TW research ABI.

The official TXO put/call series combines weekly and monthly expiries.  Its
values differ from the current OpenAPI snapshot aggregation in observed overlap.
TX daily settlements and monthly final settlements are distinct facts.  Keep
distinct names and place post-close observations on their verified session.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import polars as pl


CONTRACT_VERSION = 4
MARKET_SYMBOL = "__MARKET__"
SOURCE_COLUMNS = {
    "put_volume": "twpub_taifex_txo_official_put_volume_raw",
    "call_volume": "twpub_taifex_txo_official_call_volume_raw",
    "put_open_interest": "twpub_taifex_txo_official_put_open_interest_raw",
    "call_open_interest": "twpub_taifex_txo_official_call_open_interest_raw",
    "put_call_volume_ratio_pct": "twpub_taifex_txo_official_put_call_volume_ratio_pct_raw",
    "put_call_open_interest_ratio_pct": "twpub_taifex_txo_official_put_call_open_interest_ratio_pct_raw",
}
TX_COLUMNS = (
    "twpub_taifex_tx_official_volume_raw",
    "twpub_taifex_tx_official_open_interest_raw",
    "twpub_taifex_tx_official_front_settlement_raw",
)
TX_FINAL_COLUMN = "twpub_taifex_tx_official_monthly_final_settlement_raw"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_taifex_research_features(
    *, base_path: Path, history_path: Path, futures_path: Path,
    final_settlement_path: Path, output_path: Path,
) -> dict:
    base_path, history_path, futures_path, final_settlement_path, output_path = map(
        Path, (base_path, history_path, futures_path, final_settlement_path, output_path)
    )
    if output_path.resolve() in {
        base_path.resolve(), history_path.resolve(), futures_path.resolve(), final_settlement_path.resolve()
    }:
        raise ValueError("research output must be separate from every input")
    inputs = {
        "contract_version": CONTRACT_VERSION,
        "base": {"path": str(base_path.resolve()), "sha256": _sha256(base_path)},
        "taifex": {"path": str(history_path.resolve()), "sha256": _sha256(history_path)},
        "futures": {"path": str(futures_path.resolve()), "sha256": _sha256(futures_path)},
        "futures_final": {
            "path": str(final_settlement_path.resolve()),
            "sha256": _sha256(final_settlement_path),
        },
    }
    receipt_path = output_path.with_suffix(".taifex_research.json")
    if output_path.is_file() and receipt_path.is_file():
        try:
            prior = json.loads(receipt_path.read_text())
        except (OSError, ValueError):
            prior = {}
        if prior.get("inputs") == inputs and prior.get("output_sha256") == _sha256(output_path):
            return {**prior, "reused": True}

    history = pl.read_parquet(history_path).select(
        pl.col("date").cast(pl.Date).alias("source_date"),
        pl.col("available_date").cast(pl.Date).alias("date"),
        pl.col("published_after_close"),
        pl.col("availability_rule"),
        *[pl.col(source).cast(pl.Float64).alias(target) for source, target in SOURCE_COLUMNS.items()],
    )
    if history.is_empty():
        raise ValueError("TAIFEX put/call history is empty")
    if history.filter(
        pl.col("source_date").is_null()
        | (pl.col("date").is_not_null() & (pl.col("date") <= pl.col("source_date")))
        | (pl.col("published_after_close") != True).fill_null(True)
        | (pl.col("availability_rule") != "next_receipt_verified_taifex_session").fill_null(True)
    ).height:
        raise ValueError("TAIFEX source availability contract failed")
    unavailable = history.filter(pl.col("date").is_null())
    if unavailable.height > 1 or (
        unavailable.height == 1
        and unavailable.get_column("source_date").item() != history.get_column("source_date").max()
    ):
        raise ValueError("TAIFEX history has an interior missing available date")
    for name in SOURCE_COLUMNS.values():
        if history.filter(pl.col(name).is_null() | (pl.col(name) < 0)).height:
            raise ValueError(f"missing or negative TAIFEX value: {name}")
    for numerator, denominator, ratio in (
        ("twpub_taifex_txo_official_put_volume_raw", "twpub_taifex_txo_official_call_volume_raw", "twpub_taifex_txo_official_put_call_volume_ratio_pct_raw"),
        ("twpub_taifex_txo_official_put_open_interest_raw", "twpub_taifex_txo_official_call_open_interest_raw", "twpub_taifex_txo_official_put_call_open_interest_ratio_pct_raw"),
    ):
        discrepancy = history.filter(
            (pl.col(denominator) > 0)
            & ((pl.col(numerator) / pl.col(denominator) * 100 - pl.col(ratio)).abs() > 0.011)
        )
        if discrepancy.height:
            raise ValueError(f"TAIFEX published ratio disagrees with counts: {ratio}")
    available = history.filter(pl.col("date").is_not_null())
    if available.get_column("date").n_unique() != available.height:
        raise ValueError("more than one TAIFEX source observation maps to one available date")

    tx = (
        pl.scan_parquet(futures_path)
        .filter((pl.col("product") == "TX") & (pl.col("session") == "一般"))
        .select("date", "contract", "volume", "open_interest", "settlement")
        .collect()
    )
    if tx.is_empty() or tx.select(pl.any_horizontal(pl.all().is_null()).any()).item():
        raise ValueError("TX day-session source has missing values or no rows")
    if tx.group_by("date", "contract").len().filter(pl.col("len") > 1).height:
        raise ValueError("duplicate TX physical contract and source session")
    if tx.filter(
        (pl.col("volume") < 0)
        | (pl.col("open_interest") < 0)
        | (pl.col("settlement") < 0)
    ).height:
        raise ValueError("invalid TX volume, open interest, or settlement")
    # An expiring contract may carry an official zero settlement.  Sum its
    # traded volume/OI, but select the nearest contract with a positive price.
    front = tx.filter(pl.col("settlement") > 0).group_by("date").agg(
        pl.col("settlement").sort_by("contract").first().cast(pl.Float64).alias(TX_COLUMNS[2])
    )
    tx_by_day = (
        tx.group_by("date")
        .agg(
            pl.col("volume").sum().cast(pl.Float64).alias(TX_COLUMNS[0]),
            pl.col("open_interest").sum().cast(pl.Float64).alias(TX_COLUMNS[1]),
        )
        .join(front, on="date", how="left")
        .sort("date")
        .with_columns(
            pl.col("date").alias("source_date"),
            pl.col("date").shift(-1).alias("available_date"),
        )
        .drop("date")
        .rename({"available_date": "date"})
    )
    if tx_by_day.get_column(TX_COLUMNS[2]).null_count():
        raise ValueError("TX source has a day with no positive front settlement")
    tx_available = tx_by_day.filter(pl.col("date").is_not_null())
    if tx_available.filter(pl.col("date") <= pl.col("source_date")).height:
        raise ValueError("TX available date is not later than its source session")

    # The official index page shares a TX/MTX/TMF price column.  Weekly
    # contracts in 2014 belong to MTX, so only six-digit monthly delivery
    # identities may become a TX feature.
    final_source = (
        pl.scan_parquet(final_settlement_path)
        .filter(
            (pl.col("product") == "TX")
            & pl.col("contract").str.contains(r"^\d{6}$")
        )
        .select(
            pl.col("settlement_date").alias("source_date"),
            pl.col("contract"),
            pl.col("final_settlement_price").cast(pl.Float64).alias(TX_FINAL_COLUMN),
        )
        .collect()
    )
    if final_source.is_empty() or final_source.filter(
        pl.col("source_date").is_null()
        | pl.col(TX_FINAL_COLUMN).is_null()
        | (pl.col(TX_FINAL_COLUMN) <= 0)
    ).height:
        raise ValueError("TX monthly final-settlement source is empty or invalid")
    if final_source.get_column("source_date").n_unique() != final_source.height:
        raise ValueError("multiple TX monthly final settlements share one source session")
    final_available = final_source.join(
        tx_by_day.select("source_date", "date"), on="source_date", how="left"
    )
    pending_final = final_available.filter(pl.col("date").is_null())
    if pending_final.height > 1 or (
        pending_final.height == 1
        and pending_final.get_column("source_date").item() != tx_by_day.get_column("source_date").max()
    ):
        raise ValueError("TX final settlement lacks an interior next futures session")
    final_available = final_available.filter(pl.col("date").is_not_null())

    base = pl.scan_parquet(base_path)
    base_columns = base.collect_schema().names()
    if any(name in base_columns for name in (*SOURCE_COLUMNS.values(), *TX_COLUMNS, TX_FINAL_COLUMN)):
        raise ValueError("new TAIFEX research columns already exist in base")
    market_dates = base.filter(pl.col("symbol") == MARKET_SYMBOL).select("date").unique()
    bounds = market_dates.select(pl.col("date").min().alias("first"), pl.col("date").max().alias("last")).collect().row(0, named=True)
    available = available.filter(pl.col("date").is_between(bounds["first"], bounds["last"]))
    tx_available = tx_available.filter(pl.col("date").is_between(bounds["first"], bounds["last"]))
    final_available = final_available.filter(pl.col("date").is_between(bounds["first"], bounds["last"]))
    for label, frame in (("put/call", available), ("TX", tx_available), ("TX final", final_available)):
        missing = frame.lazy().select("date").join(market_dates, on="date", how="anti").limit(1).collect()
        if missing.height:
            raise ValueError(f"TAIFEX {label} available date has no market feature row: {missing.item()}")

    sidecar = available.select("date", *SOURCE_COLUMNS.values()).with_columns(
        pl.lit(MARKET_SYMBOL).alias("symbol")
    )
    tx_sidecar = tx_available.select("date", *TX_COLUMNS).with_columns(
        pl.lit(MARKET_SYMBOL).alias("symbol")
    )
    final_sidecar = final_available.select("date", TX_FINAL_COLUMN).with_columns(
        pl.lit(MARKET_SYMBOL).alias("symbol")
    )
    joined = (
        base.join(sidecar.lazy(), on=["date", "symbol"], how="left")
        .join(tx_sidecar.lazy(), on=["date", "symbol"], how="left")
        .join(final_sidecar.lazy(), on=["date", "symbol"], how="left")
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".tmp")
    try:
        joined.sink_parquet(temporary, compression="zstd", statistics=True)
        if (
            _sha256(base_path) != inputs["base"]["sha256"]
            or _sha256(history_path) != inputs["taifex"]["sha256"]
            or _sha256(futures_path) != inputs["futures"]["sha256"]
            or _sha256(final_settlement_path) != inputs["futures_final"]["sha256"]
        ):
            raise RuntimeError("research inputs changed during build")
        os.replace(temporary, output_path)
    finally:
        temporary.unlink(missing_ok=True)
    summary = {
        "contract_version": CONTRACT_VERSION,
        "output_path": str(output_path.resolve()),
        "output_sha256": _sha256(output_path),
        "inputs": inputs,
        "source_first": str(history.get_column("source_date").min()),
        "source_last": str(history.get_column("source_date").max()),
        "source_2014_rows": history.filter(pl.col("source_date").dt.year() == 2014).height,
        "available_rows": available.height,
        "tx_source_first": str(tx_by_day.get_column("source_date").min()),
        "tx_source_last": str(tx_by_day.get_column("source_date").max()),
        "tx_source_2014_days": tx_by_day.filter(pl.col("source_date").dt.year() == 2014).height,
        "tx_available_rows": tx_available.height,
        "tx_monthly_final_2014_days": final_source.filter(pl.col("source_date").dt.year() == 2014).height,
        "feature_columns": [*SOURCE_COLUMNS.values(), *TX_COLUMNS, TX_FINAL_COLUMN],
        "availability_policy": "official next receipt-verified TAIFEX session; preopen usable",
        "population": "TAIFEX official TXO put/call series combining weekly and monthly expiries; distinct from unreconciled OpenAPI snapshot ratios",
        "research_only": True,
        "reused": False,
    }
    receipt_tmp = receipt_path.with_suffix(receipt_path.suffix + ".tmp")
    receipt_tmp.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    os.replace(receipt_tmp, receipt_path)
    return summary

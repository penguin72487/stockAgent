"""Share-valued, receipt-verified view of FinLab after-hours fixed-price lots.

Reads only the requested symbol from three wide provider matrices.  The
published trade amount and price independently prove the lot-to-share factor
on each date; an unproved row has null shares, never an assumed 1,000x value.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from scripts.download_finlab_history import safe_stem


FIELDS = {
    "raw_lots": "after_market_fixed_price:成交張數",
    "price": "after_market_fixed_price:成交價",
    "amount": "after_market_fixed_price:成交金額",
}


def _source_column(root: Path, key: str, symbol: str) -> tuple[pl.DataFrame, str]:
    receipt = json.loads((root / "receipts" / f"{safe_stem(key)}.json").read_text(encoding="utf-8"))
    relative = Path(receipt["parquet_path"])
    if (receipt.get("dataset") != key
            or receipt.get("status") != "downloaded_unverified_for_pit"
            or relative.is_absolute() or ".." in relative.parts
            or relative.parts[:1] != ("datasets",)):
        raise ValueError("untrusted FinLab fixed-price receipt")
    path = root / relative
    if path.stat().st_size != receipt["parquet_size_bytes"]:
        raise ValueError("FinLab fixed-price file size mismatch")
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    if digest.hexdigest() != receipt["sha256"]:
        raise ValueError("FinLab fixed-price file hash mismatch")
    parquet = pq.ParquetFile(path)
    if symbol not in parquet.schema_arrow.names:
        raise ValueError(f"symbol {symbol} absent from {key}")
    if parquet.metadata.num_rows != receipt["rows"]:
        raise ValueError("FinLab fixed-price row count mismatch")
    frame = pl.from_arrow(parquet.read(columns=["source_index", symbol], use_threads=False))
    if frame["source_index"].is_duplicated().any():
        raise ValueError("duplicate FinLab fixed-price date")
    return frame.rename({symbol: next(name for name, value in FIELDS.items() if value == key)}), digest.hexdigest()


def load_fixed_price_shares(root: Path, symbol: str) -> pl.DataFrame:
    """Return all source dates, with nullable `volume_shares` for unsafe rows."""

    if not symbol or any(part in symbol for part in ("/", "\\", "..")):
        raise ValueError("invalid symbol")
    frames = [_source_column(root, key, symbol)[0] for key in FIELDS.values()]
    if any(not frame["source_index"].equals(frames[0]["source_index"]) for frame in frames[1:]):
        raise ValueError("FinLab fixed-price source dates do not align")
    out = frames[0]
    for frame in frames[1:]:
        out = out.hstack(frame.drop("source_index"))  # Alignment already proved above.
    lots, price, amount = [pl.col(name).cast(pl.Float64, strict=False)
                           for name in ("raw_lots", "price", "amount")]
    valid = (pl.all_horizontal(lots.is_finite(), price.is_finite(), amount.is_finite())
             & (lots >= 0) & (price > 0) & (amount >= 0)).fill_null(False)
    factor = amount / (price * lots)
    rounded = factor.round(0)
    share_count = (lots * rounded).round(0)
    proved = (rounded.is_between(1, 100_000)
              & ((factor-rounded).abs() <= pl.max_horizontal(pl.lit(1e-8),
                    pl.max_horizontal(factor.abs(), rounded.abs()) * 1e-8))
              & ((lots * rounded-share_count).abs() <= 1e-6)
              & share_count.is_finite() & (share_count < float(2**63))).fill_null(False)
    proof = (pl.when(~valid).then(pl.lit("missing_or_invalid"))
        .when((lots == 0) & (amount == 0)).then(pl.lit("zero_trade"))
        .when((lots <= 0) | (amount <= 0)).then(pl.lit("amount_lot_disagreement"))
        .when(proved).then(pl.lit("price_amount_exact"))
        .otherwise(pl.lit("unproved_lot_size")))
    out = out.with_columns(pl.lit(symbol).alias("symbol"), proof.alias("unit_proof"))
    return out.with_columns(
        pl.when(pl.col("unit_proof") == "zero_trade").then(pl.lit(0, dtype=pl.Int64))
          .when(pl.col("unit_proof") == "price_amount_exact").then(share_count.cast(pl.Int64, strict=False))
          .otherwise(None).alias("volume_shares"),
        pl.when(pl.col("unit_proof") == "price_amount_exact").then(rounded.cast(pl.Int64, strict=False))
          .otherwise(None).alias("unit_multiplier")).select(
              "source_index", "symbol", "raw_lots", "price", "amount", "volume_shares", "unit_multiplier", "unit_proof")

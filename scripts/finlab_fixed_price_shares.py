"""Share-valued, receipt-verified view of FinLab after-hours fixed-price lots.

Reads only the requested symbol from three wide provider matrices.  The
published trade amount and price independently prove the lot-to-share factor
on each date; an unproved row has null shares, never an assumed 1,000x value.
"""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from scripts.download_finlab_history import safe_stem


FIELDS = {
    "raw_lots": "after_market_fixed_price:成交張數",
    "price": "after_market_fixed_price:成交價",
    "amount": "after_market_fixed_price:成交金額",
}


def _source_column(root: Path, key: str, symbol: str) -> tuple[pd.DataFrame, str]:
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
    frame = parquet.read(columns=["source_index", symbol]).to_pandas()
    if frame["source_index"].duplicated().any():
        raise ValueError("duplicate FinLab fixed-price date")
    return frame.rename(columns={symbol: next(name for name, value in FIELDS.items() if value == key)}), digest.hexdigest()


def load_fixed_price_shares(root: Path, symbol: str) -> pd.DataFrame:
    """Return all source dates, with nullable `volume_shares` for unsafe rows."""

    if not symbol or any(part in symbol for part in ("/", "\\", "..")):
        raise ValueError("invalid symbol")
    frames = [_source_column(root, key, symbol)[0] for key in FIELDS.values()]
    if any(not frame["source_index"].equals(frames[0]["source_index"]) for frame in frames[1:]):
        raise ValueError("FinLab fixed-price source dates do not align")
    out = frames[0]
    for frame in frames[1:]:
        out = out.merge(frame, on="source_index", validate="one_to_one")
    out.insert(1, "symbol", symbol)
    shares: list[int | None] = []
    multipliers: list[int | None] = []
    proofs: list[str] = []
    for lots_raw, price_raw, amount_raw in out[["raw_lots", "price", "amount"]].itertuples(index=False, name=None):
        try:
            lots, price, amount = float(lots_raw), float(price_raw), float(amount_raw)
        except (ValueError, TypeError):
            lots = price = amount = float("nan")
        if not all(map(math.isfinite, (lots, price, amount))) or lots < 0 or price <= 0 or amount < 0:
            shares.append(None); multipliers.append(None); proofs.append("missing_or_invalid")
            continue
        if lots == 0 and amount == 0:
            shares.append(0); multipliers.append(None); proofs.append("zero_trade")
            continue
        if lots <= 0 or amount <= 0:
            shares.append(None); multipliers.append(None); proofs.append("amount_lot_disagreement")
            continue
        factor = amount / (price * lots)
        rounded = round(factor)
        share_count = round(lots * rounded)
        if (not 1 <= rounded <= 100_000
                or not math.isclose(factor, rounded, rel_tol=1e-8, abs_tol=1e-8)
                or not math.isclose(lots * rounded, share_count, rel_tol=0, abs_tol=1e-6)
                or share_count > 2**63 - 1):
            shares.append(None); multipliers.append(None); proofs.append("unproved_lot_size")
            continue
        shares.append(share_count); multipliers.append(rounded); proofs.append("price_amount_exact")
    out["volume_shares"] = pd.Series(shares, dtype="Int64")
    out["unit_multiplier"] = pd.Series(multipliers, dtype="Int64")
    out["unit_proof"] = proofs
    return out

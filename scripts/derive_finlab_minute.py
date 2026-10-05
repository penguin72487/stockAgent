#!/usr/bin/env python3
"""Build left-labelled FinLab-compatible minutes from a verified local tick receipt.

This is a local transformation: it never calls the FinLab API.  A derived
receipt is kept separately from legacy provider-minute receipts.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time
import hashlib
import json
import os
from pathlib import Path
import tempfile

import numpy as np
import pandas as pd

from scripts.download_finlab_history import _atomic_json, safe_stem
from scripts.download_finlab_intraday import (
    _partition_paths, _stored_receipt, _validate_frame, reconcile_stored_tick_unit,
)
from scripts.finlab_volume_units import volume_reconciliation_due

DERIVATION_CONTRACT_VERSION = 2
DERIVED_RECEIPT_SCHEMA_VERSION = 4
DELAYED_CLOSE_RULE_START = date(2012, 2, 20)


def derived_receipt_path(root: Path, symbol: str, day: date) -> Path:
    return (root / "intraday/derived_minute/receipts"
            / safe_stem(f"tw_minute:{symbol}") / f"{day.isoformat()}.json")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stored_derived_receipt(root: Path, symbol: str, day: date,
                           source_sha256: str) -> dict | None:
    path = derived_receipt_path(root, symbol, day)
    try:
        receipt = json.loads(path.read_text(encoding="utf-8"))
        if (receipt.get("dataset") != f"tw_minute:{symbol}"
                or receipt.get("trade_date") != day.isoformat()
                or receipt.get("source_tick_sha256") != source_sha256
                or receipt.get("source_kind") != "derived_from_tw_tick"
                or int(receipt.get("schema_version") or 0) < DERIVED_RECEIPT_SCHEMA_VERSION
                or receipt.get("minute_derivation_contract_version") != DERIVATION_CONTRACT_VERSION):
            return None
        if receipt.get("status") in {"verified_closed_date", "verified_no_trade",
                                     "derived_no_regular_trades"}:
            return receipt
        relative = Path(receipt["parquet_path"])
        if (relative.is_absolute() or ".." in relative.parts
                or relative.parts[:2] != ("intraday", "objects")):
            return None
        target = root / relative
        if (target.is_file() and target.stat().st_size == receipt["parquet_size_bytes"]
                and len(receipt.get("sha256", "")) == 64):
            return receipt
    except (OSError, KeyError, TypeError, ValueError):
        pass
    return None


def ticks_to_minutes(ticks: pd.DataFrame, symbol: str, day: date) -> pd.DataFrame:
    """Regular trades only; actual closing auctions retain their own minute.

    Do not fill zero-trade minutes. ``volume`` remains provider-native;
    ``volume_shares`` is nullable unless the whole-day source unit is proved.
    Identical timestamps retain source sequence.
    The dated TWSE/TPEx delayed-close rule permits an observed 13:33 auction
    from 2012-02-20; it is never moved back to 13:30 or treated as its price.
    """
    _validate_frame(ticks, f"tw_tick:{symbol}", day)
    required = {"close", "volume", "session", "sequence"}
    if not required.issubset(ticks.columns):
        raise ValueError("tick schema lacks price, volume, session or sequence")
    if (ticks["volume"].isna().any() or ticks["session"].isna().any()
            or not np.isfinite(ticks["volume"].to_numpy(dtype=float)).all()
            or (ticks["volume"] < 0).any()):
        raise ValueError("invalid tick volume or session")
    local = ticks["timestamp"].dt.tz_convert("Asia/Taipei")
    if not local.dt.date.eq(day).all():
        raise ValueError("tick timestamp outside declared trade date")
    regular = ticks.loc[ticks["session"].eq("regular") & ticks["volume"].gt(0)].copy()
    if "volume_shares" not in regular:
        regular["volume_shares"] = pd.Series(pd.NA, index=regular.index, dtype="Int64")
    if regular.empty:
        return pd.DataFrame(columns=["stock_id", "trade_date", "timestamp", "session",
                                     "open", "high", "low", "close", "volume",
                                     "volume_shares",
                                     "tick_count", "vwap"])
    if (regular["close"].isna().any()
            or not np.isfinite(regular["close"].to_numpy(dtype=float)).all()
            or (regular["close"] <= 0).any()):
        raise ValueError("invalid regular trade price")
    clock = regular["timestamp"].dt.tz_convert("Asia/Taipei").dt.floor("min").dt.time
    allowed = clock.between(time(9), time(13, 30))
    if day >= DELAYED_CLOSE_RULE_START:
        allowed |= clock.eq(time(13, 33))
    if not allowed.all():
        raise ValueError("regular tick outside 09:00-13:30 or dated 13:33 closing auction")
    regular = regular.sort_values(["timestamp", "sequence"], kind="stable")
    regular["minute"] = regular["timestamp"].dt.floor("min")
    regular["notional"] = regular["close"] * regular["volume"]
    bars = regular.groupby("minute", sort=True).agg(
        open=("close", "first"), high=("close", "max"),
        low=("close", "min"), close=("close", "last"),
        volume=("volume", "sum"), tick_count=("close", "size"),
        volume_shares=("volume_shares", lambda values: values.sum(min_count=len(values))),
        notional=("notional", "sum"),
    ).reset_index().rename(columns={"minute": "timestamp"})
    bars["vwap"] = bars["notional"] / bars["volume"]
    bars = bars.drop(columns="notional")
    bars.insert(0, "stock_id", symbol)
    bars.insert(1, "trade_date", day.isoformat())
    bars.insert(3, "session", "regular")
    _validate_frame(bars, f"tw_minute:{symbol}", day)
    return bars


def derive_partition(root: Path, symbol: str, day: date,
                     *, reference_root: Path | None = None) -> dict:
    key = f"tw_tick:{symbol}"
    source = _stored_receipt(_partition_paths(root, key, day)[0], root, key, day)
    if source is None:
        raise ValueError("verified local tick receipt required for minute derivation")
    if (source.get("status") == "downloaded_unverified_for_pit"
            and volume_reconciliation_due(source, symbol, day, reference_root)):
        source = reconcile_stored_tick_unit(root, key, day, reference_root=reference_root)
    source_sha = source.get("sha256", "")
    existing = stored_derived_receipt(root, symbol, day, source_sha)
    if existing is not None:
        changes = {name: source.get(name) for name in (
            "source_checked_at_utc", "canonical_volume_unit", "canonical_volume_scope",
            "volume_multiplier", "volume_unit_reference", "volume_unit_contract_version",
            "unit_reference_revision", "unit_resolution_error",
        )}
        if any(existing.get(name) != value for name, value in changes.items()):
            existing = {**existing, **changes}
            _atomic_json(derived_receipt_path(root, symbol, day), existing)
        return existing
    status = source["status"]
    bars = None
    if status == "downloaded_unverified_for_pit":
        tick_path = root / source["parquet_path"]
        if _sha256(tick_path) != source_sha:
            raise ValueError("tick object hash differs from source receipt")
        ticks = pd.read_parquet(tick_path)
        volume_evidence = {
            "raw_volume_unit": source.get("raw_volume_unit", "provider_native"),
            "canonical_volume_unit": source.get("canonical_volume_unit", "unresolved"),
            "canonical_volume_scope": source.get("canonical_volume_scope", "regular_session_only"),
            "volume_multiplier": source.get("volume_multiplier"),
            "volume_unit_reference": source.get("volume_unit_reference"),
            "volume_unit_contract_version": source.get("volume_unit_contract_version"),
            "unit_reference_revision": source.get("unit_reference_revision"),
            "unit_resolution_error": source.get("unit_resolution_error"),
        }
        if ("volume_shares" not in ticks
                or volume_evidence["canonical_volume_unit"] != "shares"):
            from scripts.finlab_volume_units import with_verified_volume_shares

            ticks, volume_evidence = with_verified_volume_shares(
                ticks, symbol, day, reference_root=reference_root,
            )
        bars = ticks_to_minutes(ticks, symbol, day)
        status = "derived_unverified_for_pit" if len(bars) else "derived_no_regular_trades"
    else:
        volume_evidence = {"raw_volume_unit": "provider_native",
                           "canonical_volume_unit": "unresolved",
                           "canonical_volume_scope": "regular_session_only"}
    payload = {
        "schema_version": DERIVED_RECEIPT_SCHEMA_VERSION,
        "minute_derivation_contract_version": DERIVATION_CONTRACT_VERSION,
        "dataset": f"tw_minute:{symbol}",
        "trade_date": day.isoformat(), "status": status,
        "source_kind": "derived_from_tw_tick", "source_dataset": key,
        "source_tick_sha256": source_sha,
        "source_tick_receipt": str(_partition_paths(root, key, day)[0].relative_to(root)),
        "source_checked_at_utc": source["source_checked_at_utc"],
        "derived_at_utc": datetime.now(UTC).isoformat(),
        "rows": len(bars) if bars is not None else 0,
        "fields": list(bars.columns) if bars is not None else [],
        "first_timestamp": str(bars["timestamp"].min()) if bars is not None and len(bars) else None,
        "last_timestamp": str(bars["timestamp"].max()) if bars is not None and len(bars) else None,
        "source_grain": "stock_trade_day_left_labelled_minute",
        "volume_unit": "provider_native",
        **volume_evidence,
        "publication_time_status": "not_verified_for_training",
        "delayed_close_observed": bool(bars is not None and len(bars)
            and bars["timestamp"].dt.tz_convert("Asia/Taipei").dt.time.eq(time(13, 33)).any()),
    }
    if bars is not None and len(bars):
        objects = root / "intraday/objects"
        objects.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=objects, prefix=".finlab-derived-",
                                         suffix=".parquet.tmp", delete=False) as handle:
            temporary = Path(handle.name)
        try:
            bars.to_parquet(temporary, index=False, compression="zstd")
            digest = _sha256(temporary)
            destination = objects / f"tw_minute_derived-{safe_stem(symbol)}-{day.isoformat()}-{digest[:24]}.parquet"
            if destination.exists():
                if _sha256(destination) != digest:
                    raise ValueError("derived minute object hash mismatch")
            else:
                os.replace(temporary, destination)
            payload.update({"parquet_path": str(destination.relative_to(root)),
                            "parquet_size_bytes": destination.stat().st_size,
                            "sha256": digest})
        finally:
            temporary.unlink(missing_ok=True)
    receipt_path = derived_receipt_path(root, symbol, day)
    if receipt_path.is_file():
        previous = json.loads(receipt_path.read_text(encoding="utf-8"))
        if (previous.get("source_tick_sha256") != source_sha
                or previous.get("minute_derivation_contract_version") != DERIVATION_CONTRACT_VERSION):
            old_sha = previous.get("sha256") or previous.get("source_tick_sha256")
            if isinstance(old_sha, str) and len(old_sha) == 64:
                archive = (root / "intraday/derived_minute/versions"
                           / safe_stem(f"tw_minute:{symbol}") / day.isoformat()
                           / f"{old_sha}.json")
                if not archive.is_file():
                    _atomic_json(archive, previous)
    _atomic_json(receipt_path, payload)
    return payload

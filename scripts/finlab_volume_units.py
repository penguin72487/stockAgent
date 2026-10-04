"""Receipt-backed FinLab stock-tick volume conversion to shares.

FinLab deliberately calls its intraday ``volume`` provider-native.  Match a
whole regular session to the locally verified Shioaji KBar response, whose
Amount/OHLC proves its stock-volume multiplier, before exposing shares.  An
unmatched day retains raw volume with no share-capacity claim.
"""

from __future__ import annotations

from datetime import date
import hashlib
import json
from pathlib import Path
import re

import pandas as pd
import polars as pl

from downloader.download_shioaji_tw_kbars import normalize_kbars


DEFAULT_REFERENCE_ROOT = Path(__file__).resolve().parents[1] / "data_tw_minute/shioaji_1m"
_CHUNK = re.compile(r"^(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})\.receipt\.json$")
VOLUME_UNIT_CONTRACT_VERSION = 2


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def reference_revision(symbol: str, day: date, root: Path | None = None) -> str:
    """Cheap retry trigger: only small reference receipts, never tick payloads."""

    folder = (root or DEFAULT_REFERENCE_ROOT) / "minute_chunks" / symbol
    digest = hashlib.sha256()
    for path in sorted(folder.glob("*.receipt.json")):
        match = _CHUNK.fullmatch(path.name)
        if match and match[1] <= day.isoformat() <= match[2]:
            digest.update(path.name.encode())
            try:
                digest.update(path.read_bytes())
            except OSError:
                digest.update(b"unreadable")
    return digest.hexdigest()


def volume_reconciliation_due(receipt: dict, symbol: str, day: date,
                              root: Path | None = None) -> bool:
    return (
        receipt.get("volume_unit_contract_version") != VOLUME_UNIT_CONTRACT_VERSION
        or receipt.get("unit_reference_revision") != reference_revision(symbol, day, root)
    )


def _reference(symbol: str, day: date, root: Path) -> dict | None:
    folder = root / "minute_chunks" / symbol
    for receipt_path in sorted(folder.glob("*.receipt.json")):
        match = _CHUNK.fullmatch(receipt_path.name)
        if not match or not (match[1] <= day.isoformat() <= match[2]):
            continue
        try:
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            method = receipt.get("underlying_data_method", "provider_kbars")
            tick_sources = receipt.get("raw_tick_sources") or []
            # A recovered bar's Amount may have been computed from a guessed
            # tick multiplier. It cannot independently prove that multiplier.
            if method == "observed_ticks_aggregated_to_right_labelled_1m":
                continue
            if method not in {"provider_kbars", "mixed_provider_kbars_and_observed_ticks"}:
                continue
            if (not isinstance(tick_sources, list)
                    or any(not isinstance(source, dict) or not source.get("session_date")
                           for source in tick_sources)
                    or (method == "mixed_provider_kbars_and_observed_ticks" and not tick_sources)
                    or any(source["session_date"] == day.isoformat() for source in tick_sources)):
                continue
            output = receipt["output_receipt"]
            parquet = receipt_path.with_suffix("").with_suffix(".parquet")
            if (receipt.get("status") != "ok" or receipt.get("symbol") != symbol
                    or receipt.get("security_type") != "stock"
                    or not parquet.is_file() or parquet.stat().st_size != output["size"]
                    or _sha256(parquet) != output["sha256"]):
                continue
            bars = pl.read_parquet(parquet).filter(pl.col("date") == day)
            if bars.is_empty():
                continue
            normalized = normalize_kbars(
                {name: bars[name].to_list() for name in
                 ("ts", "Open", "High", "Low", "Close", "Volume", "Amount")},
                symbol=symbol, market=receipt["market"],
                contract_unit=float(receipt["contract_unit"]),
            )
            active = normalized.filter(pl.col("Volume") > 0)
            factors = active["source_volume_multiplier"].drop_nulls().unique().to_list()
            if (active.is_empty() or active["source_volume_multiplier"].null_count()
                    or len(factors) != 1 or factors[0] not in (1.0, 10.0, 100.0, 1000.0)):
                continue
            return {
                "multiplier": int(factors[0]),
                "regular_raw_volume": float(active["Volume"].sum()),
                "reference_sha256": output["sha256"],
                "reference_receipt": str(receipt_path.relative_to(root)),
                "basis": "receipt_verified_shioaji_kbars_amount_ohlc_and_exact_regular_volume",
                "reference_kind": "provider_kbars_direct_amount",
            }
        except (OSError, ValueError, KeyError, TypeError, pl.exceptions.PolarsError):
            continue
    return None


def with_verified_volume_shares(
    ticks: pd.DataFrame, symbol: str, day: date,
    *, reference_root: Path | None = None,
) -> tuple[pd.DataFrame, dict]:
    """Keep provider ``volume``; add nullable ``volume_shares`` only if proved."""

    output = ticks.copy()
    output["volume_shares"] = pd.Series(pd.NA, index=output.index, dtype="Int64")
    evidence = {"raw_volume_unit": "provider_native", "canonical_volume_unit": "unresolved",
                "canonical_volume_scope": "regular_session_only",
                "volume_multiplier": None, "volume_unit_reference": None,
                "unit_resolution_error": None,
                "volume_unit_contract_version": VOLUME_UNIT_CONTRACT_VERSION,
                "unit_reference_revision": reference_revision(symbol, day, reference_root)}
    if output.empty or "volume" not in output or "session" not in output:
        return output, evidence
    reference = _reference(symbol, day, reference_root or DEFAULT_REFERENCE_ROOT)
    if reference is None:
        return output, evidence
    regular = output.loc[output["session"].eq("regular"), "volume"]
    if (regular.empty or regular.isna().any()
            or abs(float(regular.sum()) - reference["regular_raw_volume"]) > 1e-6):
        return output, {**evidence, "unit_resolution_error": "regular_volume_mismatch"}
    raw = pd.to_numeric(output["volume"], errors="coerce")
    if raw.isna().any() or (raw < 0).any() or (raw % 1 != 0).any():
        return output, {**evidence, "unit_resolution_error": "invalid_raw_volume"}
    factor = reference["multiplier"]
    if (raw.max() if len(raw) else 0) > (2**63 - 1) // factor:
        return output, {**evidence, "unit_resolution_error": "share_volume_overflow"}
    regular_mask = output["session"].eq("regular")
    output.loc[regular_mask, "volume_shares"] = (
        raw.loc[regular_mask].astype("int64") * factor
    ).astype("Int64")
    return output, {
        **evidence, "canonical_volume_unit": "shares", "volume_multiplier": factor,
        "volume_unit_reference": reference,
    }

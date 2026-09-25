"""Fail-closed source and model-input boundary for single-venue crypto runs."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


VENUES = frozenset({"bybit", "binance", "okx"})
REGISTERED_EXTERNAL_TABLES = {
    "bybit": "data_bybit/public_features/bybit_venue_daily.parquet",
}
PANEL_FEATURES = frozenset({
    "open_logret_1d", "max_logret_1d", "min_logret_1d",
    "close_logret_1d", "trading_volume_logret_1d", "signed_vol",
    "body_ratio", "signed_body_ratio", "delta_body_ratio", "clv",
    "clv_centered", "delta_clv", "upper_shadow", "lower_shadow",
    "shadow_imbalance",
})


def validate_crypto_exchange_scope(
    data: Any, *, repo_root: Path, check_schema: bool = False
) -> None:
    """Reject cross-venue paths or features before a venue-scoped run builds a panel.

    A same-named data directory is not proof of provenance; the producer receipt
    and materialization audit are still necessary. This guard prevents accidental
    cross-venue joins and open-ended feature globs in the canonical trainer.
    """
    get = data.get if isinstance(data, dict) else lambda key: getattr(data, key)
    venue = str(get("crypto_exchange_scope") or "").strip().lower()
    if not venue:
        return  # Historical configs retain their original checkpoint ABI.
    if venue not in VENUES:
        raise ValueError(f"data.crypto_exchange_scope must be one of {sorted(VENUES)}")

    venue_root = (repo_root / f"data_{venue}").resolve()

    def scoped_path(raw: str, label: str) -> Path:
        path = Path(str(raw)).expanduser()
        resolved = (path if path.is_absolute() else repo_root / path).resolve()
        if not resolved.is_relative_to(venue_root):
            raise ValueError(f"{label} must be inside {venue_root}, got {resolved}")
        return resolved

    scoped_path(get("parquet_root"), "data.parquet_root")
    external = bool(get("use_external_features"))
    external_path = None
    if external:
        external_path = scoped_path(get("external_feature_path"), "data.external_feature_path")
        registered = REGISTERED_EXTERNAL_TABLES.get(venue)
        if registered is None or external_path != (repo_root / registered).resolve():
            raise ValueError(f"{venue} has no registered external feature table at {external_path}")
    if get("use_tw_public_features") or get("use_tw_public_rules"):
        raise ValueError("venue-only crypto training cannot join Taiwan public features")
    allowed_prefix = f"crypto_{venue}_"
    for field in ("feature_include", "feature_availability_indicators"):
        patterns = get(field)
        if not patterns and (field == "feature_include" or external):
            raise ValueError(f"data.{field} must be explicit for venue-only training")
        for pattern in patterns:
            if pattern in PANEL_FEATURES:
                continue
            if not (str(pattern).startswith(allowed_prefix) and
                    not any(token in str(pattern) for token in "*?[]")):
                raise ValueError(f"data.{field} exposes out-of-scope feature {pattern!r}")

    if check_schema and external_path is not None:
        import pyarrow.parquet as pq

        columns = pq.read_schema(external_path).names
        foreign = [name for name in columns if name not in {"date", "symbol"}
                   and not name.startswith(allowed_prefix)]
        if foreign:
            raise ValueError(f"external feature table contains out-of-scope columns: {foreign[:8]}")
        missing = [name for name in get("feature_include") if name.startswith(allowed_prefix)
                   and name not in columns]
        if missing:
            raise ValueError(f"external feature table is missing selected columns: {missing}")
        summary_path = external_path.with_name(f"{external_path.stem}_summary.json")
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        if summary.get("exchange_scope") != venue or summary.get("output_columns") != columns:
            raise ValueError("external feature receipt does not match venue or schema")
        digest = hashlib.sha256()
        with external_path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                digest.update(chunk)
        if digest.hexdigest() != summary.get("output_sha256"):
            raise ValueError("external feature bytes do not match venue receipt")

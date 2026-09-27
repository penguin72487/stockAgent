"""Fail-closed execution-venue and public-information boundaries for crypto."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any


VENUES = frozenset({"bybit", "binance", "okx"})
INFORMATION_SCOPES = frozenset({"venue_only", "historical_public_pit"})
REGISTERED_EXTERNAL_TABLES = {
    ("bybit", "venue_only"): "data_bybit/public_features/bybit_venue_daily.parquet",
    ("bybit", "historical_public_pit"): (
        "data_bybit/public_features/bybit_crypto_public_daily.parquet"
    ),
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
    information_scope = str(
        get("crypto_information_scope") or "venue_only"
    ).strip().lower()
    if information_scope not in INFORMATION_SCOPES:
        raise ValueError(
            "data.crypto_information_scope must be one of "
            f"{sorted(INFORMATION_SCOPES)}"
        )

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
        registered = REGISTERED_EXTERNAL_TABLES.get((venue, information_scope))
        if registered is None or external_path != (repo_root / registered).resolve():
            raise ValueError(
                f"{venue}/{information_scope} has no registered external feature "
                f"table at {external_path}"
            )
    if get("use_tw_public_features") or get("use_tw_public_rules"):
        raise ValueError("venue-only crypto training cannot join Taiwan public features")
    allowed_prefixes = (f"crypto_{venue}_",)
    if information_scope == "historical_public_pit":
        if venue != "bybit":
            raise ValueError(
                "historical_public_pit is currently registered only for Bybit execution"
            )
        # These are information sources, never tradable/valuation universes.
        # Snapshot-only web families are deliberately absent because their
        # first locally observed vintages begin in 2026 and cannot be projected
        # backward into the 2020 training period.
        allowed_prefixes = (
            "crypto_bybit_",
            "crypto_binance_",
            "crypto_okx_",
            "crypto_public_macro_",
            "crypto_public_sec_",
        )
    for field in ("feature_include", "feature_availability_indicators"):
        patterns = get(field)
        if not patterns and (field == "feature_include" or external):
            raise ValueError(f"data.{field} must be explicit for venue-only training")
        for pattern in patterns:
            if pattern in PANEL_FEATURES:
                continue
            if not (
                str(pattern).startswith(allowed_prefixes)
                and not any(token in str(pattern) for token in "*?[]")
            ):
                raise ValueError(f"data.{field} exposes out-of-scope feature {pattern!r}")

    if check_schema and external_path is not None:
        import pyarrow.parquet as pq

        columns = pq.read_schema(external_path).names
        if information_scope == "venue_only":
            foreign = [
                name
                for name in columns
                if name not in {"date", "symbol"}
                and not name.startswith(allowed_prefixes)
            ]
            if foreign:
                raise ValueError(
                    f"external feature table contains out-of-scope columns: {foreign[:8]}"
                )
        missing = [
            name
            for name in get("feature_include")
            if name.startswith(allowed_prefixes) and name not in columns
        ]
        if missing:
            raise ValueError(f"external feature table is missing selected columns: {missing}")
        summary_path = external_path.with_name(f"{external_path.stem}_summary.json")
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        if summary.get("output_columns") != columns:
            raise ValueError("external feature receipt does not match the table schema")
        if information_scope == "venue_only":
            if summary.get("exchange_scope") != venue:
                raise ValueError("external feature receipt does not match venue")
        else:
            sources = summary.get("public_web_sources", {})
            if (
                summary.get("historical_event_backprojection") is not False
                or sources.get("fred_macro", {}).get("status")
                != "included_initial_release_vintages"
                or sources.get("sec_etf_filings", {}).get("status")
                != "included_acceptance_time_aligned"
            ):
                raise ValueError(
                    "historical public receipt does not prove PIT-safe FRED/SEC clocks"
                )
            quality_path = external_path.with_name(
                f"{external_path.stem}_quality.csv"
            )
            with quality_path.open(newline="", encoding="utf-8") as handle:
                quality = {
                    str(row["feature"]): row
                    for row in csv.DictReader(handle)
                }
            selected_external = [
                name
                for name in get("feature_include")
                if name.startswith(allowed_prefixes)
            ]
            missing_quality = [
                name for name in selected_external if name not in quality
            ]
            if missing_quality:
                raise ValueError(
                    "historical public quality receipt is missing selected "
                    f"features: {missing_quality[:8]}"
                )
            panel_start = str(get("panel_start_date") or "")
            late_features = [
                name
                for name in selected_external
                if not quality[name].get("first_date")
                or quality[name]["first_date"] > panel_start
            ]
            if late_features:
                raise ValueError(
                    "historical public features must begin no later than the "
                    f"training panel: {late_features[:8]}"
                )
        digest = hashlib.sha256()
        with external_path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                digest.update(chunk)
        if digest.hexdigest() != summary.get("output_sha256"):
            raise ValueError("external feature bytes do not match venue receipt")

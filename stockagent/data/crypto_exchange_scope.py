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
# Explicit node-local research view, not a new producer or publication alias.
# Its preflight proves every unchanged link and changed daily table against
# the receipt written by scripts/prepare_bybit_daily_repairs.py.
BYBIT_REPAIR_VIEW = "artifacts/cache/bybit_perpetual_daily_repaired/perpetual_daily"
BYBIT_MIDNIGHT_VIEW = "artifacts/cache/bybit_perpetual_daily_0000_repaired/perpetual_daily"
BYBIT_MIDNIGHT_SYMBOL_COUNT = 397


def validate_bybit_midnight_view(path: Path, *, venue_root: Path) -> None:
    """Verify all new midnight labels and their full retained-source evidence."""
    receipt = json.loads((path.parent / "midnight_manifest.json").read_text())
    if (receipt.get("schema_version") != 1 or receipt.get("contract_version") != 7
            or receipt.get("decision_cutoff_utc") != "00:00" or receipt.get("execution_boundary_utc") != "00:00"
            or Path(receipt["base_root"]).resolve() != venue_root):
        raise ValueError("Bybit midnight receipt does not pin the active base release/clock")
    originals = sorted((venue_root / "perpetual_daily").glob("*_features.parquet"))
    symbols = [file.stem.removesuffix("_features") for file in originals]
    universe_hash = hashlib.sha256(json.dumps(symbols, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    if (len(symbols) != BYBIT_MIDNIGHT_SYMBOL_COUNT or receipt.get("reference_symbols") != symbols
            or receipt.get("reference_universe_sha256") != universe_hash
            or {file.name for file in path.glob("*_features.parquet")} != {file.name for file in originals}
            or set(receipt.get("symbols", {})) != set(symbols)):
        raise ValueError("Bybit midnight view changed the reference universe")
    if set(receipt.get("announced_symbols", [])) != {"HFTUSDT", "VINEUSDT", "ICXUSDT"}:
        raise ValueError("unexpected midnight announcement scope")
    expected_derived = {f"perpetual_daily/{file.name}" for file in originals}
    if set(receipt["derived_sha256"]) != expected_derived:
        raise ValueError("unexpected midnight derived files")
    expected_sources = {str(file) for file in originals}
    expected_sources.update({str(venue_root / "funding/funding_coverage.csv"), receipt["source_probe"]})
    for symbol in symbols:
        expected_sources.update({str(venue_root / "1m" / f"{symbol}_features.parquet"),
                                 str(venue_root / "funding" / f"{symbol}_funding.parquet")})
        tail = venue_root / "1m/_hot_tail" / f"{symbol}_features.parquet"
        if tail.is_file():
            expected_sources.add(str(tail))
    if set(receipt["sources_sha256"]) != expected_sources:
        raise ValueError("midnight receipt lacks full input evidence")
    def digest(file: Path) -> str:
        with file.open("rb") as handle:
            return hashlib.file_digest(handle, "sha256").hexdigest()
    for name, expected in receipt["derived_sha256"].items():
        target = path.parent / name
        if target.is_symlink() or digest(target) != expected:
            raise ValueError(f"midnight derived file changed: {name}")
    for name, expected in receipt["sources_sha256"].items():
        if digest(Path(name)) != expected:
            raise ValueError(f"midnight input evidence changed: {name}")


def validate_bybit_repair_view(path: Path, *, venue_root: Path) -> None:
    receipt = json.loads((path.parent / "repair_manifest.json").read_text())
    if receipt.get("schema_version") != 1 or Path(receipt["base_root"]).resolve() != venue_root:
        raise ValueError("Bybit repair receipt does not pin the active base release")
    originals = {file.name: file for file in (venue_root / "perpetual_daily").glob("*_features.parquet")}
    if not originals or {file.name for file in path.glob("*_features.parquet")} != originals.keys():
        raise ValueError("Bybit repair view changed the base universe")
    if set(receipt["symbols"]) != {"HFTUSDT", "VINEUSDT", "ICXUSDT"}:
        raise ValueError("unexpected Bybit repair symbol scope")
    derived_expected = {f"perpetual_daily/{symbol}_features.parquet" for symbol in receipt["symbols"]}
    if set(receipt["derived_sha256"]) != derived_expected:
        raise ValueError("unexpected derived Bybit files")
    def digest(file: Path) -> str:
        with file.open("rb") as handle:
            return hashlib.file_digest(handle, "sha256").hexdigest()
    for source, expected in receipt["sources_sha256"].items():
        if digest(Path(source)) != expected:
            raise ValueError(f"Bybit repair input evidence changed: {source}")
    for name, original in originals.items():
        if str(original) not in receipt["sources_sha256"]:
            raise ValueError(f"Bybit base daily source missing from receipt: {name}")
        local = path / name
        key = f"perpetual_daily/{name}"
        if key in derived_expected:
            if local.is_symlink() or digest(local) != receipt["derived_sha256"][key]:
                raise ValueError(f"Bybit derived repair changed: {name}")
        elif not local.is_symlink() or local.resolve() != original:
            raise ValueError(f"Bybit unchanged source link changed: {name}")
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

    raw_panel = Path(str(get("parquet_root"))).expanduser()
    panel_path = (raw_panel if raw_panel.is_absolute() else repo_root / raw_panel).resolve()
    registered_view = (repo_root / BYBIT_REPAIR_VIEW).absolute()
    if venue == "bybit" and panel_path == registered_view:
        if check_schema:
            validate_bybit_repair_view(panel_path, venue_root=venue_root)
    elif venue == "bybit" and panel_path == (repo_root / BYBIT_MIDNIGHT_VIEW).absolute():
        if check_schema:
            validate_bybit_midnight_view(panel_path, venue_root=venue_root)
    else:
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
        if (panel_path == (repo_root / BYBIT_MIDNIGHT_VIEW).absolute()
                and information_scope == "historical_public_pit"
                and summary.get("decision_boundary_utc") != "00:00"):
            raise ValueError("midnight execution requires a midnight-cutoff public feature receipt")
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

"""Causal adapters for non-exchange public crypto context.

Every adapter emits rows at the UTC-midnight decision boundary.  A source row
is usable only when its explicit ``available_at_utc`` is no later than that
boundary.  Latest-view archives without a historical publication clock are
therefore useful only from their first locally observed vintage onward.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
import hashlib
from pathlib import Path
from typing import Callable

import numpy as np
import polars as pl


MARKET_SYMBOL = "__MARKET__"

FRED_MACRO_SPECS: dict[str, tuple[str, Callable[[float], float]]] = {
    "DFF": ("crypto_public_macro_fed_funds_rate", lambda value: value / 100.0),
    "SOFR": ("crypto_public_macro_sofr", lambda value: value / 100.0),
    "DGS2": ("crypto_public_macro_treasury_2y", lambda value: value / 100.0),
    "DGS10": ("crypto_public_macro_treasury_10y", lambda value: value / 100.0),
    "T10Y2Y": (
        "crypto_public_macro_treasury_10y_2y_spread",
        lambda value: value / 100.0,
    ),
    "DTWEXBGS": ("crypto_public_macro_dollar_index_log", lambda value: np.log(value)),
    "VIXCLS": ("crypto_public_macro_vix_log1p", lambda value: np.log1p(value)),
    "BAMLH0A0HYM2": (
        "crypto_public_macro_high_yield_spread",
        lambda value: value / 100.0,
    ),
    "WALCL": (
        "crypto_public_macro_fed_balance_sheet_log1p",
        lambda value: np.log1p(value),
    ),
    "RRPONTSYD": (
        "crypto_public_macro_reverse_repo_log1p",
        lambda value: np.log1p(value),
    ),
    "NFCI": ("crypto_public_macro_financial_conditions", float),
}
FRED_FEATURES = tuple(spec[0] for spec in FRED_MACRO_SPECS.values()) + (
    "crypto_public_macro_available_fraction",
    "crypto_public_macro_max_age_days",
)

SEC_MARKET_FEATURES = (
    "crypto_public_sec_etf_filings_1d_log1p",
    "crypto_public_sec_etf_filings_7d_log1p",
    "crypto_public_sec_etf_filings_30d_log1p",
    "crypto_public_sec_etf_registration_30d_log1p",
    "crypto_public_sec_etf_periodic_30d_log1p",
    "crypto_public_sec_etf_available",
)
SEC_ASSET_FEATURES = (
    "crypto_public_sec_asset_filings_7d_log1p",
    "crypto_public_sec_asset_filings_30d_log1p",
    "crypto_public_sec_asset_registration_30d_log1p",
    "crypto_public_sec_asset_periodic_30d_log1p",
    "crypto_public_sec_asset_available",
)

COINMETRICS_METRICS: dict[str, tuple[str, Callable[[float], float]]] = {
    "AdrActCnt": ("crypto_public_onchain_active_addresses_log1p", np.log1p),
    "AdrBalCnt": ("crypto_public_onchain_balance_addresses_log1p", np.log1p),
    "TxCnt": ("crypto_public_onchain_tx_count_log1p", np.log1p),
    "TxTfrCnt": ("crypto_public_onchain_transfer_count_log1p", np.log1p),
    "FeeTotNtv": ("crypto_public_onchain_fees_native_log1p", np.log1p),
    "IssTotNtv": ("crypto_public_onchain_issuance_native_log1p", np.log1p),
    "SplyCur": ("crypto_public_onchain_supply_log1p", np.log1p),
    "CapMVRVCur": ("crypto_public_onchain_mvrv_log", np.log),
    "HashRate": ("crypto_public_onchain_hashrate_log1p", np.log1p),
}
COINMETRICS_FEATURES = tuple(spec[0] for spec in COINMETRICS_METRICS.values()) + (
    "crypto_public_onchain_exchange_netflow_usd_signed_log1p",
    "crypto_public_onchain_age_days",
    "crypto_public_onchain_available_fraction",
)

COINGECKO_MARKET_FEATURES = (
    "crypto_public_coingecko_total_market_cap_log1p",
    "crypto_public_coingecko_total_volume_log1p",
    "crypto_public_coingecko_btc_dominance",
    "crypto_public_coingecko_eth_dominance",
    "crypto_public_coingecko_market_cap_change_24h",
    "crypto_public_coingecko_active_assets_log1p",
    "crypto_public_coingecko_markets_log1p",
    "crypto_public_coingecko_market_available",
)
COINGECKO_ASSET_FEATURES = (
    "crypto_public_coingecko_market_cap_log1p",
    "crypto_public_coingecko_fdv_to_market_cap_log",
    "crypto_public_coingecko_volume_to_market_cap_log",
    "crypto_public_coingecko_market_cap_rank_log1p",
    "crypto_public_coingecko_circulating_supply_log1p",
    "crypto_public_coingecko_symbol_match_share",
    "crypto_public_coingecko_asset_available",
)

ETF_ISSUER_FEATURES = (
    "crypto_public_etf_holdings_value_usd_log1p",
    "crypto_public_etf_holdings_units_log1p",
    "crypto_public_etf_reserve_coverage_log",
    "crypto_public_etf_issuer_available",
)

# Exact Community asset identifiers are deliberately limited to well-known
# native assets.  Coin Metrics asset ids are not a universal ticker namespace,
# so guessing every Bybit base would create silent cross-asset contamination.
COINMETRICS_SAFE_ASSET_IDS = frozenset(
    {
        "ada",
        "algo",
        "apt",
        "atom",
        "avax",
        "bch",
        "bnb",
        "btc",
        "doge",
        "dot",
        "etc",
        "eth",
        "fil",
        "hbar",
        "icp",
        "link",
        "ltc",
        "near",
        "sol",
        "sui",
        "trx",
        "xlm",
        "xrp",
    }
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_utc(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip().replace("Z", "+00:00")
        if not text:
            return None
        parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _decision_cutoff(value: str) -> datetime:
    return datetime.combine(date.fromisoformat(value), time.min, tzinfo=timezone.utc)


def _base_candidate(base: str) -> str:
    normalized = str(base).strip().upper()
    for multiplier in ("1000000", "10000", "1000"):
        stripped = normalized.removeprefix(multiplier)
        if stripped != normalized:
            return stripped
    return normalized


def _empty_market_rows(
    dates: list[str], features: tuple[str, ...]
) -> list[dict[str, object]]:
    return [
        {"date": value, "symbol": MARKET_SYMBOL, **{name: None for name in features}}
        for value in dates
    ]


def fred_macro_rows(
    path: Path, dates: list[str]
) -> tuple[pl.DataFrame, dict[str, object]]:
    rows = _empty_market_rows(dates, FRED_FEATURES)
    if not path.is_file():
        return pl.from_dicts(rows), {"status": "missing", "path": str(path)}
    raw = pl.read_parquet(path)
    required = {"series_id", "observation_date", "value", "available_at_utc"}
    if missing := required - set(raw.columns):
        raise ValueError(f"FRED macro observations missing columns: {sorted(missing)}")
    records: list[dict[str, object]] = []
    for record in raw.select(required).drop_nulls("value").to_dicts():
        available = _parse_utc(record["available_at_utc"])
        if available is None:
            continue
        records.append(
            {
                **record,
                "available": available,
                "observation": date.fromisoformat(str(record["observation_date"])),
            }
        )
    selected_counts = [0] * len(rows)
    ages_by_row: list[list[int]] = [[] for _ in rows]
    for series_id, (feature, transform) in FRED_MACRO_SPECS.items():
        series_records = sorted(
            (item for item in records if item["series_id"] == series_id),
            key=lambda item: item["available"],
        )
        pointer = 0
        selected: dict[str, object] | None = None
        for index, output in enumerate(rows):
            cutoff = _decision_cutoff(str(output["date"]))
            while (
                pointer < len(series_records)
                and series_records[pointer]["available"] <= cutoff
            ):
                candidate = series_records[pointer]
                if candidate["observation"] < cutoff.date() and (
                    selected is None
                    or (candidate["observation"], candidate["available"])
                    > (selected["observation"], selected["available"])
                ):
                    selected = candidate
                pointer += 1
            if selected is None or selected["observation"] >= cutoff.date():
                continue
            transformed = float(transform(float(selected["value"])))
            if not np.isfinite(transformed):
                continue
            output[feature] = transformed
            selected_counts[index] += 1
            ages_by_row[index].append((cutoff.date() - selected["observation"]).days)
    for index, output in enumerate(rows):
        output["crypto_public_macro_available_fraction"] = selected_counts[index] / len(
            FRED_MACRO_SPECS
        )
        output["crypto_public_macro_max_age_days"] = (
            float(max(ages_by_row[index])) if ages_by_row[index] else None
        )
    earliest = min((item["available"] for item in records), default=None)
    return pl.from_dicts(rows, infer_schema_length=None), {
        "status": "included_initial_release_vintages",
        "path": str(path),
        "sha256": _sha256(path),
        "rows": len(records),
        "series": sorted({str(item["series_id"]) for item in records}),
        "earliest_available_at_utc": earliest.isoformat() if earliest else None,
        "availability_contract": "initial_release_realtime_start_plus_one_utc_day",
        "revision_backprojection": False,
    }


def _is_registration_form(form: str) -> bool:
    value = form.upper()
    return value.startswith(
        ("S-1", "S-3", "N-1A", "N-2", "485", "497", "424", "POS", "FWP")
    )


def _is_periodic_form(form: str) -> bool:
    return form.upper().startswith(("8-K", "10-K", "10-Q"))


def _window_count(
    records: list[dict[str, object]],
    cutoff_date: date,
    days: int,
    field: str | None = None,
) -> int:
    start = cutoff_date - timedelta(days=days - 1)
    return sum(
        start <= item["decision_date"] <= cutoff_date
        and (field is None or bool(item[field]))
        for item in records
    )


def sec_etf_filing_rows(
    sec_root: Path,
    dates: list[str],
    symbol_bases: dict[str, str],
) -> tuple[pl.DataFrame, dict[str, object]]:
    paths = sorted(sec_root.glob("*/filings.parquet"))
    market_rows = _empty_market_rows(dates, SEC_MARKET_FEATURES)
    if not paths:
        return pl.from_dicts(market_rows), {
            "status": "missing",
            "path": str(sec_root),
        }
    raw = pl.concat(
        [
            pl.read_parquet(
                path,
                columns=[
                    "accession_number",
                    "registered_assets",
                    "form",
                    "available_at_utc",
                ],
            )
            for path in paths
        ],
        how="diagonal_relaxed",
    ).unique("accession_number")
    records: list[dict[str, object]] = []
    for item in raw.to_dicts():
        available = _parse_utc(item["available_at_utc"])
        if available is None:
            continue
        first_decision = available.date()
        if available.timetz().replace(tzinfo=None) != time.min:
            first_decision += timedelta(days=1)
        form = str(item["form"] or "")
        records.append(
            {
                "decision_date": first_decision,
                "asset": str(item["registered_assets"] or "").upper(),
                "registration": _is_registration_form(form),
                "periodic": _is_periodic_form(form),
            }
        )
    first_decision = min((item["decision_date"] for item in records), default=None)
    by_asset_symbols: dict[str, list[str]] = {}
    for symbol, raw_base in symbol_bases.items():
        base = _base_candidate(raw_base)
        if any(item["asset"] == base for item in records):
            by_asset_symbols.setdefault(base, []).append(symbol)
    records_by_asset = {
        asset: [item for item in records if item["asset"] == asset]
        for asset in by_asset_symbols
    }
    output_rows: list[dict[str, object]] = []
    for output in market_rows:
        target = date.fromisoformat(str(output["date"]))
        output.update(
            {
                "crypto_public_sec_etf_filings_1d_log1p": float(
                    np.log1p(_window_count(records, target, 1))
                ),
                "crypto_public_sec_etf_filings_7d_log1p": float(
                    np.log1p(_window_count(records, target, 7))
                ),
                "crypto_public_sec_etf_filings_30d_log1p": float(
                    np.log1p(_window_count(records, target, 30))
                ),
                "crypto_public_sec_etf_registration_30d_log1p": float(
                    np.log1p(_window_count(records, target, 30, "registration"))
                ),
                "crypto_public_sec_etf_periodic_30d_log1p": float(
                    np.log1p(_window_count(records, target, 30, "periodic"))
                ),
                "crypto_public_sec_etf_available": float(
                    first_decision is not None and target >= first_decision
                ),
            }
        )
        output_rows.append(output)
        for base, symbols in by_asset_symbols.items():
            matching = records_by_asset[base]
            asset_first = min(item["decision_date"] for item in matching)
            values = {
                "date": output["date"],
                "crypto_public_sec_asset_filings_7d_log1p": float(
                    np.log1p(_window_count(matching, target, 7))
                ),
                "crypto_public_sec_asset_filings_30d_log1p": float(
                    np.log1p(_window_count(matching, target, 30))
                ),
                "crypto_public_sec_asset_registration_30d_log1p": float(
                    np.log1p(_window_count(matching, target, 30, "registration"))
                ),
                "crypto_public_sec_asset_periodic_30d_log1p": float(
                    np.log1p(_window_count(matching, target, 30, "periodic"))
                ),
                "crypto_public_sec_asset_available": float(target >= asset_first),
            }
            output_rows.extend({**values, "symbol": symbol} for symbol in symbols)
    return pl.from_dicts(output_rows, infer_schema_length=None), {
        "status": "included_acceptance_time_aligned",
        "path": str(sec_root),
        "files": len(paths),
        "filings": len(records),
        "first_decision_date": first_decision.isoformat() if first_decision else None,
        "availability_contract": "sec_acceptance_datetime_le_decision_cutoff",
        "entity_universe_risk": "current_crypto_etf_registry_survivorship_selection",
    }


def _safe_transform(
    transform: Callable[[float], float], value: float | None
) -> float | None:
    if value is None or not np.isfinite(value):
        return None
    if transform in {np.log, np.log1p} and value <= (
        0.0 if transform is np.log else -1.0
    ):
        return None
    transformed = float(transform(float(value)))
    return transformed if np.isfinite(transformed) else None


def coinmetrics_vintage_rows(
    vintage_root: Path,
    dates: list[str],
    symbol_bases: dict[str, str],
) -> tuple[pl.DataFrame, dict[str, object]]:
    by_asset: dict[str, list[str]] = {}
    for symbol, raw_base in symbol_bases.items():
        asset = _base_candidate(raw_base).lower()
        if (
            asset in COINMETRICS_SAFE_ASSET_IDS
            and (vintage_root / f"{asset}_vintages.parquet").is_file()
        ):
            by_asset.setdefault(asset, []).append(symbol)
    output: list[dict[str, object]] = []
    input_rows = 0
    first_available: datetime | None = None
    target_metrics = set(COINMETRICS_METRICS) | {"FlowInExUSD", "FlowOutExUSD"}
    max_cutoff = max((_decision_cutoff(value) for value in dates), default=None)
    if max_cutoff is None:
        return pl.DataFrame(), {"status": "empty_dates", "path": str(vintage_root)}
    for asset, symbols in by_asset.items():
        path = vintage_root / f"{asset}_vintages.parquet"
        raw = (
            pl.scan_parquet(path)
            .filter(
                pl.col("metric").is_in(target_metrics) & pl.col("value").is_not_null()
            )
            .select("date", "metric", "value", "available_at_utc")
            .collect()
        )
        records: list[dict[str, object]] = []
        for item in raw.to_dicts():
            available = _parse_utc(item["available_at_utc"])
            if available is None or available > max_cutoff:
                continue
            event_date = date.fromisoformat(str(item["date"]))
            records.append({**item, "available": available, "event_date": event_date})
            first_available = (
                available
                if first_available is None
                else min(first_available, available)
            )
        input_rows += len(records)
        asset_first_available = min(
            (item["available"] for item in records), default=None
        )
        for decision_text in dates:
            cutoff = _decision_cutoff(decision_text)
            if asset_first_available is None or cutoff < asset_first_available:
                continue
            eligible = [
                item
                for item in records
                if item["available"] <= cutoff and item["event_date"] < cutoff.date()
            ]
            if not eligible:
                continue
            latest: dict[str, dict[str, object]] = {}
            for item in eligible:
                metric = str(item["metric"])
                incumbent = latest.get(metric)
                if incumbent is None or (item["event_date"], item["available"]) > (
                    incumbent["event_date"],
                    incumbent["available"],
                ):
                    latest[metric] = item
            feature_values: dict[str, float | None] = {}
            ages: list[int] = []
            observed = 0
            for metric, (feature, transform) in COINMETRICS_METRICS.items():
                item = latest.get(metric)
                transformed = _safe_transform(
                    transform, float(item["value"]) if item is not None else None
                )
                feature_values[feature] = transformed
                if transformed is not None and item is not None:
                    observed += 1
                    ages.append((cutoff.date() - item["event_date"]).days)
            inflow = latest.get("FlowInExUSD")
            outflow = latest.get("FlowOutExUSD")
            netflow: float | None = None
            if inflow is not None and outflow is not None:
                raw_netflow = float(inflow["value"]) - float(outflow["value"])
                netflow = float(np.sign(raw_netflow) * np.log1p(abs(raw_netflow)))
                observed += 1
                ages.extend(
                    [
                        (cutoff.date() - inflow["event_date"]).days,
                        (cutoff.date() - outflow["event_date"]).days,
                    ]
                )
            feature_values[
                "crypto_public_onchain_exchange_netflow_usd_signed_log1p"
            ] = netflow
            feature_values["crypto_public_onchain_age_days"] = (
                float(max(ages)) if ages else None
            )
            feature_values["crypto_public_onchain_available_fraction"] = observed / (
                len(COINMETRICS_METRICS) + 1
            )
            for symbol in symbols:
                output.append(
                    {"date": decision_text, "symbol": symbol, **feature_values}
                )
    status = (
        "included_prospective_vintages"
        if output
        else (
            "unavailable_before_first_vintage" if by_asset else "no_safe_asset_mapping"
        )
    )
    return (
        pl.from_dicts(output, infer_schema_length=None) if output else pl.DataFrame(),
        {
            "status": status,
            "path": str(vintage_root),
            "mapped_assets": sorted(by_asset),
            "mapped_symbols": sum(len(value) for value in by_asset.values()),
            "input_rows_before_cutoff": input_rows,
            "earliest_available_at_utc": first_available.isoformat()
            if first_available
            else None,
            "availability_contract": "retrieval_vintage_available_at_le_decision_cutoff",
            "latest_view_history_backprojected": False,
        },
    )


def _snapshot_files(root: Path, dataset: str) -> list[Path]:
    return sorted((root / "snapshots" / dataset).glob("**/*.parquet"))


def coingecko_snapshot_rows(
    root: Path,
    dates: list[str],
    symbol_bases: dict[str, str],
) -> tuple[pl.DataFrame, dict[str, object]]:
    global_paths = _snapshot_files(root, "coingecko_global_snapshot")
    market_paths = _snapshot_files(root, "coingecko_market_snapshot")
    market_rows = _empty_market_rows(dates, COINGECKO_MARKET_FEATURES)
    if not global_paths and not market_paths:
        return pl.from_dicts(market_rows), {"status": "missing", "path": str(root)}
    globals_: list[dict[str, object]] = []
    for path in global_paths:
        for item in pl.read_parquet(path).to_dicts():
            available = _parse_utc(item.get("available_at_utc"))
            if available is not None:
                globals_.append({**item, "available": available})
    snapshots: list[tuple[datetime, pl.DataFrame]] = []
    for path in market_paths:
        frame = pl.read_parquet(path)
        available = _parse_utc(frame["available_at_utc"][0]) if frame.height else None
        if available is not None:
            snapshots.append((available, frame))
    output: list[dict[str, object]] = []
    match_shares: list[float] = []
    for row in market_rows:
        cutoff = _decision_cutoff(str(row["date"]))
        eligible_global = [item for item in globals_ if item["available"] <= cutoff]
        if eligible_global:
            selected = max(eligible_global, key=lambda item: item["available"])
            row.update(
                {
                    "crypto_public_coingecko_total_market_cap_log1p": _safe_transform(
                        np.log1p, selected.get("total_market_cap_usd")
                    ),
                    "crypto_public_coingecko_total_volume_log1p": _safe_transform(
                        np.log1p, selected.get("total_volume_usd")
                    ),
                    "crypto_public_coingecko_btc_dominance": float(
                        selected["btc_dominance_pct"]
                    )
                    / 100.0,
                    "crypto_public_coingecko_eth_dominance": float(
                        selected["eth_dominance_pct"]
                    )
                    / 100.0,
                    "crypto_public_coingecko_market_cap_change_24h": float(
                        selected["market_cap_change_percentage_24h_usd"]
                    )
                    / 100.0,
                    "crypto_public_coingecko_active_assets_log1p": _safe_transform(
                        np.log1p, selected.get("active_cryptocurrencies")
                    ),
                    "crypto_public_coingecko_markets_log1p": _safe_transform(
                        np.log1p, selected.get("markets")
                    ),
                    "crypto_public_coingecko_market_available": 1.0,
                }
            )
        output.append(row)
        eligible_market = [item for item in snapshots if item[0] <= cutoff]
        if not eligible_market:
            continue
        _, frame = max(eligible_market, key=lambda item: item[0])
        for symbol, raw_base in symbol_bases.items():
            base = _base_candidate(raw_base).lower()
            matches = frame.filter(pl.col("symbol").str.to_lowercase() == base)
            if not matches.height:
                continue
            matches = matches.filter(
                pl.col("market_cap").is_finite() & (pl.col("market_cap") > 0)
            ).sort("market_cap", descending=True)
            if not matches.height:
                continue
            total_cap = float(matches["market_cap"].sum())
            selected = matches.row(0, named=True)
            share = float(selected["market_cap"]) / total_cap if total_cap > 0 else 0.0
            if share < 0.90:
                continue
            cap = float(selected["market_cap"])
            fdv = selected.get("fully_diluted_valuation")
            volume = selected.get("total_volume")
            match_shares.append(share)
            output.append(
                {
                    "date": row["date"],
                    "symbol": symbol,
                    "crypto_public_coingecko_market_cap_log1p": float(np.log1p(cap)),
                    "crypto_public_coingecko_fdv_to_market_cap_log": float(
                        np.log(float(fdv) / cap)
                    )
                    if fdv is not None and float(fdv) > 0
                    else None,
                    "crypto_public_coingecko_volume_to_market_cap_log": float(
                        np.log(float(volume) / cap)
                    )
                    if volume is not None and float(volume) > 0
                    else None,
                    "crypto_public_coingecko_market_cap_rank_log1p": _safe_transform(
                        np.log1p, selected.get("market_cap_rank")
                    ),
                    "crypto_public_coingecko_circulating_supply_log1p": _safe_transform(
                        np.log1p, selected.get("circulating_supply")
                    ),
                    "crypto_public_coingecko_symbol_match_share": share,
                    "crypto_public_coingecko_asset_available": 1.0,
                }
            )
    earliest = min(
        [item["available"] for item in globals_] + [item[0] for item in snapshots],
        default=None,
    )
    return pl.from_dicts(output, infer_schema_length=None), {
        "status": "included_prospective_snapshots",
        "path": str(root),
        "global_snapshots": len(global_paths),
        "market_snapshots": len(market_paths),
        "earliest_available_at_utc": earliest.isoformat() if earliest else None,
        "asset_mapping": "largest_market_cap_symbol_match_only_if_share_ge_0.90",
        "minimum_accepted_symbol_match_share": min(match_shares)
        if match_shares
        else None,
        "historical_snapshot_backprojection": False,
    }


def etf_issuer_snapshot_rows(
    root: Path,
    dates: list[str],
    symbol_bases: dict[str, str],
) -> tuple[pl.DataFrame, dict[str, object]]:
    holdings_path = root / "normalized" / "issuer_holdings_snapshots.parquet"
    reserves_path = root / "normalized" / "issuer_reserve_snapshots.parquet"
    if not holdings_path.is_file() and not reserves_path.is_file():
        return pl.DataFrame(), {"status": "missing", "path": str(root)}
    holdings = (
        pl.read_parquet(holdings_path) if holdings_path.is_file() else pl.DataFrame()
    )
    reserves = (
        pl.read_parquet(reserves_path) if reserves_path.is_file() else pl.DataFrame()
    )
    holding_records: list[dict[str, object]] = []
    for item in holdings.to_dicts() if holdings.height else []:
        available = _parse_utc(item["available_at_utc"])
        if available is not None:
            holding_records.append({**item, "available": available})
    reserve_records: list[dict[str, object]] = []
    for item in reserves.to_dicts() if reserves.height else []:
        available = _parse_utc(item["available_at_utc"])
        if available is not None:
            reserve_records.append({**item, "available": available})
    source_assets = {
        str(item.get("asset") or "").upper()
        for item in [*holding_records, *reserve_records]
    }
    by_asset_symbols: dict[str, list[str]] = {}
    for symbol, raw_base in symbol_bases.items():
        base = _base_candidate(raw_base)
        if base in source_assets:
            by_asset_symbols.setdefault(base, []).append(symbol)
    holdings_by_asset = {
        asset: [
            item
            for item in holding_records
            if str(item.get("asset") or "").upper() == asset
        ]
        for asset in by_asset_symbols
    }
    reserves_by_asset = {
        asset: [
            item
            for item in reserve_records
            if str(item.get("asset") or "").upper() == asset
        ]
        for asset in by_asset_symbols
    }
    global_first_available = min(
        (item["available"] for item in [*holding_records, *reserve_records]),
        default=None,
    )
    output: list[dict[str, object]] = []
    first_available: datetime | None = None
    for decision_text in dates:
        cutoff = _decision_cutoff(decision_text)
        if global_first_available is None or cutoff < global_first_available:
            continue
        for base, symbols in by_asset_symbols.items():
            values: dict[str, object] = {"date": decision_text}
            any_value = False
            eligible_holdings = [
                item for item in holdings_by_asset[base] if item["available"] <= cutoff
            ]
            if eligible_holdings:
                latest_time_by_source: dict[str, datetime] = {}
                for item in eligible_holdings:
                    key = str(item["source_id"])
                    latest_time_by_source[key] = max(
                        item["available"],
                        latest_time_by_source.get(key, item["available"]),
                    )
                asset_rows = [
                    item
                    for item in eligible_holdings
                    if item["available"]
                    == latest_time_by_source[str(item["source_id"])]
                    if str(item.get("holding_ticker") or "").upper() == base
                ]
                market_value = sum(
                    float(item.get("market_value_usd") or 0.0) for item in asset_rows
                )
                units = sum(float(item.get("quantity") or 0.0) for item in asset_rows)
                values["crypto_public_etf_holdings_value_usd_log1p"] = (
                    float(np.log1p(market_value)) if market_value > 0 else None
                )
                values["crypto_public_etf_holdings_units_log1p"] = (
                    float(np.log1p(units)) if units > 0 else None
                )
                any_value = market_value > 0 or units > 0
                for item in eligible_holdings:
                    available = item["available"]
                    first_available = (
                        available
                        if first_available is None
                        else min(first_available, available)
                    )
            eligible_reserves = [
                item for item in reserves_by_asset[base] if item["available"] <= cutoff
            ]
            if eligible_reserves:
                selected = max(eligible_reserves, key=lambda item: item["available"])
                nav = float(selected.get("nav_asset_units") or 0.0)
                reserve = float(selected.get("reserve_asset_units") or 0.0)
                values["crypto_public_etf_reserve_coverage_log"] = (
                    float(np.log(reserve / nav)) if nav > 0 and reserve > 0 else None
                )
                any_value = True
                available = selected["available"]
                first_available = (
                    available
                    if first_available is None
                    else min(first_available, available)
                )
            if any_value:
                values["crypto_public_etf_issuer_available"] = 1.0
                output.extend({**values, "symbol": symbol} for symbol in symbols)
    return (
        pl.from_dicts(output, infer_schema_length=None) if output else pl.DataFrame(),
        {
            "status": "included_prospective_issuer_snapshots"
            if output
            else "unavailable_before_first_snapshot",
            "path": str(root),
            "holdings_rows": holdings.height,
            "reserve_rows": reserves.height,
            "earliest_available_at_utc": first_available.isoformat()
            if first_available
            else None,
            "availability_contract": "issuer_available_at_le_decision_cutoff",
            "issuer_assertion_not_independent_audit": True,
            "historical_snapshot_backprojection": False,
        },
    )

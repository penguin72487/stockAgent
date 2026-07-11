from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import csv
import fnmatch
import hashlib
import json
import math
import sys
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import polars as pl
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.config import ExperimentConfig, load_config
from stockagent.data.panel import build_panel, load_cached_panel
from stockagent.data.walkforward import build_expanding_year_folds
from stockagent.data.tw_public_features import (
    FEATURE_COLUMNS,
    RULE_COLUMNS,
    _date_column_expr,
    _file_content_receipt,
    _source_content_receipts,
    _symbol_universe_receipt,
)
from stockagent.data.tw_security import classify_tw_stock_or_etf, is_tw_stock_or_etf


BASE_FEATURES = {
    "trading_volume_logret_1d",
    "signed_vol",
    "body_ratio",
    "signed_body_ratio",
    "delta_body_ratio",
    "clv",
    "clv_centered",
    "delta_clv",
    "upper_shadow",
    "lower_shadow",
    "shadow_imbalance",
}

MARKET_FEATURE_PREFIXES = (
    "twpub_twse_",
    "twpub_usdtwd_",
    "twpub_cbc_",
    "twpub_dgbas_",
    "twpub_mof_",
    "twpub_taifex_",
)

OFFICIAL_QUOTE_SOURCE = "twse_tpex_official"
MIXED_QUOTE_SOURCE = "twse_tpex_official_with_yahoo_fallback"
YAHOO_FALLBACK_SOURCE = "yahoo_fallback"
APPROVED_FILE_SOURCES = {OFFICIAL_QUOTE_SOURCE, MIXED_QUOTE_SOURCE}
APPROVED_ROW_SOURCES = {
    "twse_official",
    "tpex_official",
    "official_archive",
    YAHOO_FALLBACK_SOURCE,
}


@dataclass(frozen=True)
class HistoricalSourceExpectation:
    name: str
    first_date: str
    feature_patterns: tuple[str, ...]


HISTORICAL_SOURCES = (
    HistoricalSourceExpectation(
        "twse_daily_ohlcv",
        "2004-02-11",
        ("twpub_official_*",),
    ),
    HistoricalSourceExpectation(
        "tpex_daily_ohlcv",
        "2003-08-01",
        ("twpub_official_*",),
    ),
    HistoricalSourceExpectation(
        "twse_daily_valuation",
        "2004-02-11",
        ("twpub_pe_log", "twpub_pb_log", "twpub_dividend_yield"),
    ),
    HistoricalSourceExpectation(
        "tpex_daily_valuation",
        "2007-01-01",
        ("twpub_pe_log", "twpub_pb_log", "twpub_dividend_yield"),
    ),
    HistoricalSourceExpectation(
        "twse_margin_balance",
        "2001-01-01",
        ("twpub_margin_*", "twpub_short_*"),
    ),
    HistoricalSourceExpectation(
        "tpex_margin_balance",
        "2007-01-01",
        ("twpub_margin_*", "twpub_short_*"),
    ),
    HistoricalSourceExpectation(
        "twse_institutional_trades",
        "2012-05-02",
        (
            "twpub_foreign_*",
            "twpub_investment_trust_*",
            "twpub_dealer_*",
            "twpub_institutional_*",
        ),
    ),
    HistoricalSourceExpectation(
        "tpex_institutional_trades",
        "2007-04-20",
        (
            "twpub_foreign_*",
            "twpub_investment_trust_*",
            "twpub_dealer_*",
            "twpub_institutional_*",
        ),
    ),
)


SNAPSHOT_FEATURE_SOURCES: dict[str, tuple[str, ...]] = {
    "twpub_monthly_revenue_*": ("*_api_*t187ap05_[lo].parquet",),
    "twpub_cumulative_revenue_yoy": ("*_api_*t187ap05_[lo].parquet",),
    "twpub_financial_*": (
        "*_api_*t187ap06_*parquet",
        "*_api_*t187ap07_*parquet",
    ),
    "twpub_insider_*": (
        "*_api_*t187ap11_[lo].parquet",
        "*_api_*t187ap12_[lo].parquet",
    ),
    "twpub_borrow_*": (
        "twse_api_sbl_twt96u.parquet",
        "tpex_api_tpex_margin_sbl.parquet",
    ),
    "twpub_sbl_*": ("tpex_api_tpex_margin_sbl.parquet",),
    "twpub_short_sale_available_*": ("tpex_api_tpex_margin_sbl.parquet",),
    "twpub_tdcc_*": (
        "tdcc_shareholding_distribution.parquet",
        "data_gov_tdcc_shareholding_distribution.parquet",
    ),
    "twpub_company_*": (
        "twse_listed_company_basic.parquet",
        "tpex_basic_company.parquet",
    ),
}


NON_VINTAGE_ARCHIVE_SOURCES: dict[str, tuple[str, ...]] = {
    "twpub_dgbas_*": (
        "dgbas_cpi_basic.parquet",
        "dgbas_unemployment_rate.parquet",
        "dgbas_gdp_expenditure_sa.parquet",
    ),
    "twpub_mof_*": (
        "mof_customs_trade.parquet",
        "mof_tax_revenue.parquet",
    ),
}


@dataclass
class Finding:
    severity: str
    code: str
    component: str
    item: str
    evidence: str
    impact: str
    remediation: str


@dataclass
class SourceProfile:
    source: str
    path: str
    status: str
    rows: int
    distinct_dates: int
    first_date: str
    last_date: str
    expected_sessions: int
    covered_sessions: int
    session_coverage: float | None
    selected_features: str
    quarantined: bool


@dataclass
class FeatureProfile:
    feature: str
    scope: str
    configured_zero_fill: bool
    raw_non_null: int
    raw_distinct_symbols: int
    raw_first_date: str
    raw_last_date: str
    raw_min: float | None
    raw_max: float | None
    panel_count: int
    panel_nonzero: int
    panel_nonzero_rate: float | None
    panel_mean: float | None
    panel_std: float | None
    panel_min: float | None
    panel_max: float | None
    panel_nonfinite: int
    first_nonzero_year: int | None
    last_nonzero_year: int | None
    max_annual_nonzero_rate_jump: float | None


def _matches(feature: str, patterns: Iterable[str]) -> bool:
    return any(fnmatch.fnmatchcase(feature, str(pattern)) for pattern in patterns)


def _is_zero_filled(feature: str, config: ExperimentConfig) -> bool:
    return _matches(feature, config.data.feature_zero_fill)


def _selected_features(config: ExperimentConfig) -> list[str]:
    return list(dict.fromkeys(str(name) for name in config.data.feature_include))


def _source_selected_features(
    expectation: HistoricalSourceExpectation,
    selected: list[str],
) -> list[str]:
    return [feature for feature in selected if _matches(feature, expectation.feature_patterns)]


def _benchmark_sessions(
    root: Path,
    benchmark_name: str,
    public_feature_path: Path | None = None,
) -> np.ndarray:
    path = root / f"{benchmark_name}_features.parquet"
    if not path.exists():
        raise FileNotFoundError(f"benchmark parquet is missing: {path}")
    frame = pl.read_parquet(path, columns=["date"]).select(
        _date_column_expr("date").alias("date")
    )
    values = frame.drop_nulls("date").unique("date").sort("date").get_column("date")
    sessions = np.asarray(values.to_numpy(), dtype="datetime64[D]")
    if public_feature_path is not None and public_feature_path.exists():
        schema = set(pq.read_schema(public_feature_path).names)
        if {"date", "symbol", "_twpub_official_traded"} <= schema:
            official = (
                pl.scan_parquet(public_feature_path)
                .filter(pl.col("_twpub_official_traded").cast(pl.Float64, strict=False).fill_null(0.0) > 0.0)
                .select(_date_column_expr("date").alias("date"))
                .drop_nulls("date")
                .unique("date")
                .sort("date")
                .collect()
            )
            if not official.is_empty():
                sessions = np.union1d(
                    sessions,
                    np.asarray(official["date"].to_numpy(), dtype="datetime64[D]"),
                )
    return sessions


def audit_delisted_universe_coverage(
    parquet_root: Path,
    public_dir: Path,
    benchmark_sessions: np.ndarray,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[Finding]]:
    canonical_symbols = {
        path.name.removesuffix("_features.parquet")
        for path in parquet_root.glob("*_features.parquet")
        if not path.name.removesuffix("_features.parquet").endswith(("_TW", "_TWO"))
    }
    synthetic_archives = sorted(
        path.name
        for path in parquet_root.glob("*_features.parquet")
        if path.name.removesuffix("_features.parquet").endswith(("_TW", "_TWO"))
    )
    profiles: list[dict[str, Any]] = []
    missing_rows: list[dict[str, Any]] = []
    findings: list[Finding] = []
    if benchmark_sessions.size == 0:
        raise ValueError("benchmark session calendar is empty")
    panel_start = np.datetime64(benchmark_sessions[0], "D")
    panel_end = np.datetime64(benchmark_sessions[-1], "D")
    history_bounds: dict[str, tuple[np.datetime64 | None, np.datetime64 | None]] = {}

    def symbol_history_bounds(symbol: str) -> tuple[np.datetime64 | None, np.datetime64 | None]:
        cached = history_bounds.get(symbol)
        if cached is not None:
            return cached
        symbol_path = parquet_root / f"{symbol}_features.parquet"
        if not symbol_path.exists() or "date" not in pq.read_schema(symbol_path).names:
            result = (None, None)
        else:
            dates = (
                pl.scan_parquet(symbol_path)
                .select(_date_column_expr("date").alias("date"))
                .drop_nulls("date")
                .select(
                    pl.col("date").min().alias("first_date"),
                    pl.col("date").max().alias("last_date"),
                )
                .collect()
                .row(0, named=True)
            )
            first = dates.get("first_date")
            last = dates.get("last_date")
            result = (
                np.datetime64(first, "D") if first is not None else None,
                np.datetime64(last, "D") if last is not None else None,
            )
        history_bounds[symbol] = result
        return result

    for market in ("twse", "tpex"):
        path = public_dir / f"{market}_delisted_company.parquet"
        if not path.exists():
            findings.append(
                Finding(
                    "high",
                    "missing_delisted_universe_source",
                    "universe",
                    market,
                    f"missing={path}",
                    "Historical constituents that disappeared cannot be measured.",
                    "Download the official delisted-company table before building the universe.",
                )
            )
            continue
        schema = set(pq.read_schema(path).names)
        if not {"symbol", "date"} <= schema:
            findings.append(
                Finding(
                    "high",
                    "invalid_delisted_universe_source",
                    "universe",
                    market,
                    f"schema={sorted(schema)}",
                    "The official delisted-company table has no auditable symbol/date grain.",
                    "Repair the source schema before universe construction.",
                )
            )
            continue
        columns = [name for name in ("symbol", "date", "company_name", "delisting_reason") if name in schema]
        frame = (
            pl.read_parquet(path, columns=columns)
            .with_columns(
                pl.col("symbol").cast(pl.Utf8, strict=False).str.strip_chars(),
                _date_column_expr("date").alias("date"),
            )
            .drop_nulls(["symbol", "date"])
            .filter(pl.col("symbol") != "")
            .filter(
                pl.col("symbol")
                .map_elements(is_tw_stock_or_etf, return_dtype=pl.Boolean)
                .fill_null(False)
            )
            .filter(
                (pl.col("date") >= panel_start.astype(object))
                & (pl.col("date") <= panel_end.astype(object))
            )
            .sort(["symbol", "date"])
            .unique("symbol", keep="last", maintain_order=True)
        )
        rows = frame.to_dicts()
        present: list[dict[str, Any]] = []
        missing: list[dict[str, Any]] = []
        for row in rows:
            symbol = str(row["symbol"])
            event_date = np.datetime64(row["date"], "D")
            first_date, last_date = symbol_history_bounds(symbol)
            if symbol not in canonical_symbols:
                missing.append({**row, "missing_reason": "missing_canonical_file"})
            elif first_date is None or first_date > event_date:
                missing.append(
                    {
                        **row,
                        "missing_reason": "no_history_on_or_before_delisting",
                        "file_first_date": str(first_date) if first_date is not None else "",
                        "file_last_date": str(last_date) if last_date is not None else "",
                    }
                )
            else:
                present.append(row)
        coverage = float(len(present) / len(rows)) if rows else 0.0
        profiles.append(
            {
                "market": market,
                "official_delisted_symbols": len(rows),
                "canonical_symbol_histories": len(present),
                "missing_symbol_histories": len(missing),
                "coverage": coverage,
                "panel_start": str(panel_start),
                "panel_end": str(panel_end),
            }
        )
        missing_rows.extend(
            {
                "market": market,
                "symbol": str(row.get("symbol", "")),
                "delisting_date": str(row.get("date", "")),
                "company_name": str(row.get("company_name", "") or ""),
                "delisting_reason": str(row.get("delisting_reason", "") or ""),
                "missing_reason": str(row.get("missing_reason", "")),
                "file_first_date": str(row.get("file_first_date", "")),
                "file_last_date": str(row.get("file_last_date", "")),
            }
            for row in missing
        )
        if missing:
            findings.append(
                Finding(
                    "high",
                    "delisted_universe_coverage",
                    "universe",
                    market,
                    f"covered={len(present)}/{len(rows)} ({coverage:.2%}), missing={len(missing)}",
                    "Missing failed or disappeared companies creates survivorship bias in every fold.",
                    "Backfill the missing histories from provenance-backed TWSE/TPEx archives, then rebuild the canonical symbol files.",
                )
            )
    if synthetic_archives:
        findings.append(
            Finding(
                "high",
                "synthetic_delisted_symbol_files",
                "universe",
                "*_TW/*_TWO",
                f"count={len(synthetic_archives)}, examples={synthetic_archives[:10]}",
                "Venue-candidate suffixes would duplicate one economic security and break rule joins.",
                "Merge each successful archive candidate into its canonical symbol file before panel construction.",
            )
        )
    return profiles, missing_rows, findings


def _audit_quote_file(path: Path, benchmark_sessions: np.ndarray) -> dict[str, Any]:
    symbol = path.name.removesuffix("_features.parquet")
    security_type = classify_tw_stock_or_etf(symbol)
    required = ("date", "open", "max", "min", "close", "adjclose", "Trading_Volume")
    try:
        metadata = pq.read_metadata(path)
        schema = set(metadata.schema.to_arrow_schema().names)
        raw_source = (metadata.metadata or {}).get(b"stockagent.source", b"")
        source = raw_source.decode("utf-8", errors="replace") if raw_source else ""
        missing = [column for column in required if column not in schema]
        if missing:
            return {
                "symbol": symbol,
                "security_type": security_type or "unsupported",
                "path": str(path),
                "status": "missing_schema",
                "missing_columns": ",".join(missing),
                "source": source,
            }
        has_data_source = "data_source" in schema
        has_adjustment_source = "adjustment_source" in schema
        read_columns = list(required)
        if has_data_source:
            read_columns.append("data_source")
        if has_adjustment_source:
            read_columns.append("adjustment_source")
        select_expressions: list[pl.Expr] = [
            _date_column_expr("date").alias("date"),
            *[
                pl.col(column).cast(pl.Float64, strict=False).fill_nan(None).alias(column)
                for column in required[1:]
            ],
        ]
        if has_data_source:
            select_expressions.append(
                pl.col("data_source")
                .cast(pl.Utf8, strict=False)
                .str.strip_chars()
                .alias("data_source")
            )
        if has_adjustment_source:
            select_expressions.append(
                pl.col("adjustment_source")
                .cast(pl.Utf8, strict=False)
                .str.strip_chars()
                .alias("adjustment_source")
            )
        frame = pl.read_parquet(path, columns=read_columns).select(select_expressions)
    except Exception as exc:
        return {
            "symbol": symbol,
            "security_type": security_type or "unsupported",
            "path": str(path),
            "status": "read_error",
            "error": f"{type(exc).__name__}: {exc}",
        }

    rows = int(frame.height)
    invalid_dates = int(frame["date"].null_count())
    valid_dates = frame.drop_nulls("date")
    in_panel_range = valid_dates.filter(
        (pl.col("date") >= benchmark_sessions[0].astype(object))
        & (pl.col("date") <= benchmark_sessions[-1].astype(object))
    )
    outside_panel_range = int(valid_dates.height - in_panel_range.height)
    duplicate_dates = int(in_panel_range.height - in_panel_range["date"].n_unique())
    off_calendar_rows = int(
        in_panel_range.select(
            (~pl.col("date").is_in(benchmark_sessions.astype(object).tolist())).sum()
        ).item()
        or 0
    )
    numeric = ("open", "max", "min", "close")
    finite_positive = [
        pl.col(column).is_not_null() & pl.col(column).is_finite() & (pl.col(column) > 0)
        for column in numeric
    ]
    complete_ohlc = finite_positive[0]
    for expression in finite_positive[1:]:
        complete_ohlc = complete_ohlc & expression
    invalid_geometry = int(
        in_panel_range.select(
            (
                complete_ohlc
                & (
                    (pl.col("max") < pl.col("min"))
                    | (pl.col("max") < pl.max_horizontal("open", "close"))
                    | (pl.col("min") > pl.min_horizontal("open", "close"))
                )
            ).sum()
        ).item()
        or 0
    )
    negative_volume = int(
        in_panel_range.select((pl.col("Trading_Volume") < 0).fill_null(False).sum()).item() or 0
    )
    close_missing_or_nonpositive = int(
        in_panel_range.select((~finite_positive[3]).fill_null(True).sum()).item() or 0
    )
    valid_adjusted = (
        pl.col("adjclose").is_not_null()
        & pl.col("adjclose").is_finite()
        & (pl.col("adjclose") > 0)
    )
    invalid_adjusted = int(
        in_panel_range.select((~valid_adjusted).fill_null(True).sum()).item() or 0
    )
    first_adjusted = frame.sort("date").select(pl.col("adjclose").drop_nulls().first()).item()
    first_adjusted_not_ten = int(
        first_adjusted is None
        or not math.isfinite(float(first_adjusted))
        or not math.isclose(float(first_adjusted), 10.0, rel_tol=0.0, abs_tol=1e-6)
    )
    fallback_rows = 0
    fallback_adjustment_rows = 0
    unapproved_data_source_rows = 0
    unapproved_adjustment_source_rows = 0
    if has_data_source:
        fallback_rows = int(
            frame.select((pl.col("data_source") == YAHOO_FALLBACK_SOURCE).sum()).item()
            or 0
        )
        unapproved_data_source_rows = int(
            frame.select(
                (~pl.col("data_source").is_in(sorted(APPROVED_ROW_SOURCES)))
                .fill_null(True)
                .sum()
            ).item()
            or 0
        )
    if has_adjustment_source:
        fallback_adjustment_rows = int(
            frame.select(
                (pl.col("adjustment_source") == YAHOO_FALLBACK_SOURCE).sum()
            ).item()
            or 0
        )
        unapproved_adjustment_source_rows = int(
            frame.select(
                (~pl.col("adjustment_source").is_in(sorted(APPROVED_ROW_SOURCES)))
                .fill_null(True)
                .sum()
            ).item()
            or 0
        )
    source_lineage_mismatch = int(
        source in APPROVED_FILE_SOURCES
        and (
            not has_data_source
            or not has_adjustment_source
            or (
                source == OFFICIAL_QUOTE_SOURCE
                and (fallback_rows != 0 or fallback_adjustment_rows != 0)
            )
            or (
                source == MIXED_QUOTE_SOURCE
                and fallback_rows == 0
                and fallback_adjustment_rows == 0
            )
        )
    )

    ordered = (
        in_panel_range.drop_nulls("close")
        .filter(pl.col("close").is_finite() & (pl.col("close") > 0))
        .sort("date")
        .unique("date", keep="last", maintain_order=True)
    )
    raw_close_jumps_gt_2x = 0
    adjusted_index_jumps_gt_3x = 0
    if ordered.height > 1:
        dates = np.asarray(ordered["date"].to_numpy(), dtype="datetime64[D]")
        close = np.asarray(ordered["close"].to_numpy(), dtype=np.float64)
        adjusted = np.asarray(ordered["adjclose"].to_numpy(), dtype=np.float64)
        positions = np.searchsorted(benchmark_sessions, dates)
        in_calendar = (positions < benchmark_sessions.size) & (
            benchmark_sessions[np.minimum(positions, benchmark_sessions.size - 1)] == dates
        )
        consecutive = in_calendar[1:] & in_calendar[:-1] & (positions[1:] == positions[:-1] + 1)
        raw_log_change = np.abs(np.log(close[1:] / close[:-1]))
        raw_close_jumps_gt_2x = int(
            np.count_nonzero(consecutive & (raw_log_change > math.log(2.0) + 1e-9))
        )
        valid_adjusted_pair = (
            np.isfinite(adjusted[1:])
            & np.isfinite(adjusted[:-1])
            & (adjusted[1:] > 0)
            & (adjusted[:-1] > 0)
        )
        adjusted_log_change = np.zeros_like(raw_log_change)
        adjusted_log_change[valid_adjusted_pair] = np.abs(
            np.log(adjusted[1:][valid_adjusted_pair] / adjusted[:-1][valid_adjusted_pair])
        )
        adjusted_index_jumps_gt_3x = int(
            np.count_nonzero(
                consecutive
                & valid_adjusted_pair
                & (adjusted_log_change > math.log(3.0) + 1e-9)
            )
        )
    return {
        "symbol": symbol,
        "security_type": security_type or "unsupported",
        "path": str(path),
        "status": "ok" if rows else "empty",
        "source": source,
        "has_data_source": has_data_source,
        "has_adjustment_source": has_adjustment_source,
        "yahoo_fallback_rows": fallback_rows,
        "yahoo_fallback_adjustment_rows": fallback_adjustment_rows,
        "unapproved_data_source_rows": unapproved_data_source_rows,
        "unapproved_adjustment_source_rows": unapproved_adjustment_source_rows,
        "source_lineage_mismatch": source_lineage_mismatch,
        "rows": rows,
        "audited_rows": int(in_panel_range.height),
        "outside_panel_range_rows": outside_panel_range,
        "first_date": str(valid_dates["date"].min() or "")[:10] if valid_dates.height else "",
        "last_date": str(valid_dates["date"].max() or "")[:10] if valid_dates.height else "",
        "invalid_dates": invalid_dates,
        "duplicate_dates": duplicate_dates,
        "off_calendar_rows": off_calendar_rows,
        "close_missing_or_nonpositive": close_missing_or_nonpositive,
        "invalid_adjclose": invalid_adjusted,
        "first_adjclose_not_ten": first_adjusted_not_ten,
        "invalid_ohlc_geometry": invalid_geometry,
        "negative_volume": negative_volume,
        "raw_close_jumps_gt_2x": raw_close_jumps_gt_2x,
        "adjusted_index_jumps_gt_3x": adjusted_index_jumps_gt_3x,
    }


def audit_quote_source_files(
    parquet_root: Path,
    benchmark_sessions: np.ndarray,
    *,
    workers: int = 16,
    require_official: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any], list[Finding]]:
    paths = sorted(parquet_root.glob("*_features.parquet"))
    with ThreadPoolExecutor(max_workers=max(1, int(workers))) as executor:
        profiles = list(
            executor.map(
                lambda path: _audit_quote_file(path, benchmark_sessions),
                paths,
            )
        )
    totals = {
        "files": len(profiles),
        "read_errors": sum(item.get("status") == "read_error" for item in profiles),
        "schema_errors": sum(item.get("status") == "missing_schema" for item in profiles),
        "empty_files": sum(item.get("status") == "empty" for item in profiles),
        "unsupported_security_files": sum(
            item.get("security_type") not in {"stock", "etf"} for item in profiles
        ),
        "unapproved_source_files": sum(
            str(item.get("source", "")) not in APPROVED_FILE_SOURCES
            for item in profiles
        )
        if require_official
        else 0,
    }
    for field in (
        "rows",
        "audited_rows",
        "outside_panel_range_rows",
        "invalid_dates",
        "duplicate_dates",
        "off_calendar_rows",
        "close_missing_or_nonpositive",
        "invalid_adjclose",
        "first_adjclose_not_ten",
        "invalid_ohlc_geometry",
        "negative_volume",
        "yahoo_fallback_rows",
        "yahoo_fallback_adjustment_rows",
        "unapproved_data_source_rows",
        "unapproved_adjustment_source_rows",
        "source_lineage_mismatch",
        "raw_close_jumps_gt_2x",
        "adjusted_index_jumps_gt_3x",
    ):
        totals[field] = int(sum(int(item.get(field, 0) or 0) for item in profiles))

    findings: list[Finding] = []
    for code, severity, field, impact, remediation in (
        (
            "unsupported_tw_security_type",
            "critical",
            "unsupported_security_files",
            "The model universe contains securities outside ordinary stocks and ETFs.",
            "Rebuild with the canonical TW stock/ETF classifier and remove stale official files.",
        ),
        (
            "unapproved_quote_source",
            "critical",
            "unapproved_source_files",
            "TW training data includes a file outside the approved official-first lineage.",
            "Rebuild with official TWSE/TPEx priority and the audited Yahoo fallback converter.",
        ),
        (
            "unapproved_row_source",
            "critical",
            "unapproved_data_source_rows",
            "Some quote rows have missing or unknown source lineage.",
            "Rebuild canonical symbol files so every row records its approved data_source.",
        ),
        (
            "unapproved_adjustment_source",
            "critical",
            "unapproved_adjustment_source_rows",
            "Some adjusted-return rows have missing or unknown source lineage.",
            "Rebuild canonical symbol files so every row records its approved adjustment_source.",
        ),
        (
            "quote_source_lineage_mismatch",
            "critical",
            "source_lineage_mismatch",
            "File-level source metadata disagrees with row-level fallback usage.",
            "Rebuild the symbol parquet and regenerate its source receipt.",
        ),
        (
            "invalid_adjusted_index",
            "high",
            "invalid_adjclose",
            "Forward returns cannot be reconstructed consistently from the official index.",
            "Repair reference-price coverage and rebuild official symbol parquets.",
        ),
        (
            "adjusted_index_origin",
            "high",
            "first_adjclose_not_ten",
            "The configured normalized adjusted-price origin is inconsistent across symbols.",
            "Normalize each symbol's first adjusted index value to exactly 10.",
        ),
        (
            "quote_file_read_error",
            "critical",
            "read_errors",
            "Some securities cannot enter the panel at all.",
            "Repair or replace every unreadable parquet file.",
        ),
        (
            "quote_schema_error",
            "critical",
            "schema_errors",
            "Required OHLCV inputs or execution liquidity are absent.",
            "Re-download or reconstruct files with the canonical OHLCV schema.",
        ),
        (
            "empty_quote_file",
            "high",
            "empty_files",
            "Empty symbols inflate the declared universe and cannot be audited historically.",
            "Remove empty artifacts or reconstruct their history.",
        ),
        (
            "duplicate_quote_dates",
            "high",
            "duplicate_dates",
            "Duplicate dates make last-row selection dependent on source order.",
            "Canonicalize to one deterministic row per symbol and date.",
        ),
        (
            "invalid_ohlc_geometry",
            "high",
            "invalid_ohlc_geometry",
            "Impossible bars can corrupt K-bar features unless explicitly rejected.",
            "Repair source bars and rebuild the panel cache.",
        ),
        (
            "negative_quote_volume",
            "high",
            "negative_volume",
            "Negative liquidity breaks execution-capacity semantics.",
            "Repair source volume before training.",
        ),
        (
            "off_calendar_quote_rows",
            "medium",
            "off_calendar_rows",
            "Non-canonical dates must never expand the official market calendar.",
            "Verify the events; the panel must continue excluding these rows.",
        ),
        (
            "extreme_adjusted_index_jump",
            "medium",
            "adjusted_index_jumps_gt_3x",
            "An unresolved corporate action or scale error can dominate forward labels.",
            "Verify the official reference price for each event and rebuild the normalized index.",
        ),
    ):
        value = int(totals[field])
        if value:
            findings.append(
                Finding(
                    severity,
                    code,
                    "quote_sources",
                    field,
                    f"count={value}",
                    impact,
                    remediation,
                )
            )
    return profiles, totals, findings


def audit_official_symbol_build(
    parquet_root: Path,
    public_dir: Path,
) -> tuple[dict[str, Any], list[Finding]]:
    path = parquet_root / "official_symbol_build_summary.json"
    summary = _read_json_object(path)
    result: dict[str, Any] = {"path": str(path), "valid": False}
    if summary is None:
        return result, [
            Finding(
                "critical",
                "missing_official_symbol_build_receipt",
                "quote_sources",
                str(path),
                "missing or invalid JSON",
                "Symbol files cannot be proven to originate from TWSE/TPEx.",
                "Run build_tw_official_symbol_parquets.py from complete official sources.",
            )
        ]
    source_paths = [
        public_dir / "twse_daily_ohlcv.parquet",
        public_dir / "tpex_daily_ohlcv.parquet",
        public_dir / "tw_corporate_action_reference.parquet",
    ]

    def resolve_receipt_path(receipt: dict[str, Any]) -> Path | None:
        raw_path = Path(str(receipt.get("path", "")))
        if raw_path.is_file():
            return raw_path
        if not raw_path.name:
            return None
        candidates = [
            candidate
            for candidate in public_dir.rglob(raw_path.name)
            if candidate.is_file()
        ]
        return candidates[0] if len(candidates) == 1 else None

    def receipt_matches(path_to_check: Path, receipt: dict[str, Any]) -> bool:
        try:
            digest = hashlib.sha256()
            with path_to_check.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
            return (
                int(receipt["size"]) == int(path_to_check.stat().st_size)
                and str(receipt["sha256"]) == digest.hexdigest()
            )
        except (KeyError, OSError, TypeError, ValueError):
            return False

    def validate_receipts(receipts: Any) -> bool:
        if not isinstance(receipts, list):
            return False
        for receipt in receipts:
            if not isinstance(receipt, dict):
                return False
            source = resolve_receipt_path(receipt)
            if source is None or not receipt_matches(source, receipt):
                return False
        return True

    legacy_receipts = summary.get("legacy_source_receipts", [])
    legacy_source_name = str(summary.get("legacy_source_name", "")).strip()
    legacy_receipts_valid = validate_receipts(legacy_receipts)
    fallback_receipts = summary.get("fallback_source_receipts", [])
    fallback_source_name = str(summary.get("fallback_source_name", "")).strip()
    fallback_receipts_valid = validate_receipts(fallback_receipts)
    fallback_converter_receipts_valid = fallback_receipts_valid
    if fallback_converter_receipts_valid:
        for receipt in fallback_receipts:
            source = resolve_receipt_path(receipt)
            if source is None:
                fallback_converter_receipts_valid = False
                break
            converter_summary = _read_json_object(source.with_suffix(".summary.json"))
            try:
                output_receipt = converter_summary.get("output_receipt", {})
                converter_start = date.fromisoformat(
                    str(converter_summary.get("start_date"))
                )
                archive_schema = set(pq.read_schema(source).names)
                converter_ok = (
                    converter_summary.get("source") == YAHOO_FALLBACK_SOURCE
                    and int(converter_summary.get("failed_symbol_count", -1)) == 0
                    and converter_start >= date(2000, 1, 1)
                    and receipt_matches(source, output_receipt)
                    and {
                        "date",
                        "symbol",
                        "market",
                        "source_factor",
                        "quote_source",
                    }
                    <= archive_schema
                )
            except (AttributeError, OSError, TypeError, ValueError):
                converter_ok = False
            if not converter_ok:
                fallback_converter_receipts_valid = False
                break
    manifest_path = parquet_root / "symbols.csv"
    manifest_valid = False
    manifest_fallback_symbols = -1
    if manifest_path.exists():
        try:
            manifest = pl.read_csv(manifest_path, infer_schema_length=None)
            required_manifest_columns = {"code", "security_type", "source"}
            manifest_codes = set(
                manifest["code"].cast(pl.Utf8, strict=False).drop_nulls().to_list()
            )
            parquet_codes = {
                item.name.removesuffix("_features.parquet")
                for item in parquet_root.glob("*_features.parquet")
            }
            manifest_sources = set(manifest["source"].drop_nulls().to_list())
            manifest_fallback_symbols = int(
                manifest.select((pl.col("source") == MIXED_QUOTE_SOURCE).sum()).item()
                or 0
            )
            manifest_valid = (
                required_manifest_columns <= set(manifest.columns)
                and manifest_codes == parquet_codes
                and set(manifest["security_type"].drop_nulls().to_list()) <= {"stock", "etf"}
                and all(is_tw_stock_or_etf(code) for code in manifest_codes)
                and bool(manifest_sources)
                and manifest_sources <= APPROVED_FILE_SOURCES
            )
        except Exception:
            manifest_valid = False
    fallback_rows = int(summary.get("fallback_rows", -1))
    fallback_adjustment_rows = int(summary.get("fallback_adjustment_rows", -1))
    fallback_symbols = int(summary.get("fallback_symbols", -1))
    uses_fallback = fallback_rows > 0 or fallback_adjustment_rows > 0
    expected_source = MIXED_QUOTE_SOURCE if uses_fallback else OFFICIAL_QUOTE_SOURCE
    checks: dict[str, bool] = {
        "source": summary.get("source") == expected_source,
        "adjusted_method": str(summary.get("adjusted_price_method", "")).startswith("first=10;"),
        "all_adjustments_resolved": int(summary.get("missing_adjustment_rows", -1)) == 0,
        "source_receipts": all(source.exists() for source in source_paths)
        and summary.get("source_receipts") == [_file_content_receipt(source) for source in source_paths],
        "legacy_source_identity": bool(legacy_source_name) == bool(legacy_receipts),
        "legacy_source_receipts": legacy_receipts_valid,
        "fallback_source_identity": (
            bool(fallback_source_name) == bool(fallback_receipts)
            and (not fallback_receipts or fallback_source_name == YAHOO_FALLBACK_SOURCE)
        ),
        "fallback_source_receipts": fallback_receipts_valid,
        "fallback_converter_receipts": fallback_converter_receipts_valid,
        "fallback_usage_counts": (
            fallback_rows >= 0
            and fallback_adjustment_rows >= 0
            and fallback_symbols >= 0
            and fallback_symbols == manifest_fallback_symbols
            and uses_fallback == (fallback_symbols > 0)
            and (bool(fallback_receipts) or not uses_fallback)
        ),
        "stock_etf_manifest": manifest_valid,
        "symbol_count": int(summary.get("symbols", -1))
        == len(list(parquet_root.glob("*_features.parquet"))),
    }
    result.update({"valid": all(checks.values()), "checks": checks})
    if result["valid"]:
        return result, []
    return result, [
        Finding(
            "critical",
            "stale_official_symbol_build_receipt",
            "quote_sources",
            str(path),
            f"failed_checks={[name for name, passed in checks.items() if not passed]}",
            "The per-symbol prices are stale, incomplete, or outside the approved official-first lineage.",
            "Rebuild symbol parquets after completing official downloads and the audited Yahoo fallback conversion.",
        )
    ]


def audit_return_price_provenance(parquet_root: Path) -> tuple[dict[str, Any], list[Finding]]:
    path = parquet_root / "return_price_provenance.json"
    if not path.exists():
        return {"path": str(path), "tracked_symbols": 0, "raw_close_symbols": 0}, []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        symbols = payload.get("symbols", {})
        if not isinstance(payload, dict) or not isinstance(symbols, dict):
            raise ValueError("manifest must contain an object-valued symbols field")
    except Exception as exc:
        return {"path": str(path), "valid": False}, [
            Finding(
                "critical",
                "invalid_return_price_provenance",
                "quote_sources",
                str(path),
                f"{type(exc).__name__}: {exc}",
                "Return labels cannot be tied to adjusted or raw price semantics.",
                "Rebuild the provenance manifest from source-backed imports.",
            )
        ]
    raw_symbols = sorted(
        symbol
        for symbol, record in symbols.items()
        if isinstance(record, dict)
        and record.get("kind") == "official_raw_close"
        and (parquet_root / f"{symbol}_features.parquet").exists()
    )
    summary = {
        "path": str(path),
        "valid": True,
        "tracked_symbols": len(symbols),
        "raw_close_symbols": len(raw_symbols),
        "raw_close_examples": raw_symbols[:20],
    }
    if not raw_symbols:
        return summary, []
    return summary, [
        Finding(
            "high",
            "unadjusted_delisted_return_history",
            "quote_sources",
            "adjclose",
            f"symbols={len(raw_symbols)}, examples={raw_symbols[:20]}",
            "Raw official close omits distributions and is not a total-return reference index.",
            "Use a provenance-backed TWSE/TPEx archive or the audited 2000+ Yahoo fallback and reconstruct the normalized reference-return index.",
        )
    ]


def audit_walk_forward_availability(
    panel,
    config: ExperimentConfig,
) -> tuple[dict[str, Any], list[Finding]]:
    years = np.asarray(panel.dates, dtype="datetime64[Y]").astype(np.int64) + 1970
    unique_years = sorted(np.unique(years).astype(int).tolist())
    missing_years = (
        sorted(set(range(unique_years[0], unique_years[-1] + 1)) - set(unique_years))
        if unique_years
        else []
    )
    expected_first_year = config.walk_forward.expected_first_year
    findings: list[Finding] = []
    if expected_first_year is not None and (
        not unique_years or unique_years[0] != int(expected_first_year)
    ):
        findings.append(
            Finding(
                "critical",
                "walk_forward_first_year_mismatch",
                "walk_forward",
                "expected_first_year",
                f"expected={int(expected_first_year)}, actual={unique_years[0] if unique_years else None}",
                "Fold IDs no longer refer to the same validation and test years as the original experiment.",
                "Backfill with official data first and the audited 2000+ Yahoo OHLCV fallback where official rows do not exist; do not renumber folds.",
            )
        )
    if config.walk_forward.require_contiguous_years and missing_years:
        findings.append(
            Finding(
                "critical",
                "walk_forward_missing_years",
                "walk_forward",
                "year_coverage",
                f"missing_years={missing_years}",
                "Missing whole years renumber later folds and can bridge unobserved market regimes.",
                "Complete the official archive for every missing year before training.",
            )
        )
    folds = build_expanding_year_folds(
        panel.dates,
        min_train_years=config.walk_forward.min_train_years,
        val_years=config.walk_forward.val_years,
        require_future_test_year=config.walk_forward.require_future_test_year,
    )
    start_fold = config.runner.start_fold
    target_ids = (
        list(range(int(start_fold), int(start_fold) + 3)) if start_fold is not None else []
    )
    by_id = {int(fold.fold_id): fold for fold in folds}
    target_folds = []
    for fold_id in target_ids:
        fold = by_id.get(fold_id)
        target_folds.append(
            {
                "fold_id": fold_id,
                "available": fold is not None,
                "train_years": list(fold.train_years) if fold is not None else [],
                "val_years": list(fold.val_years) if fold is not None else [],
                "test_years": list(fold.test_years) if fold is not None else [],
            }
        )
    summary = {
        "first_year": unique_years[0] if unique_years else None,
        "last_year": unique_years[-1] if unique_years else None,
        "year_count": len(unique_years),
        "missing_years_between_bounds": missing_years,
        "expected_first_year": expected_first_year,
        "require_contiguous_years": bool(config.walk_forward.require_contiguous_years),
        "fold_count": len(folds),
        "first_fold": int(folds[0].fold_id),
        "last_fold": int(folds[-1].fold_id),
        "configured_start_fold": start_fold,
        "target_folds": target_folds,
    }
    if start_fold is not None and int(start_fold) > int(folds[-1].fold_id):
        findings.append(
            Finding(
                "critical",
                "configured_fold_unavailable",
                "walk_forward",
                f"fold{int(start_fold)}",
                (
                    f"panel_years={unique_years[0]}..{unique_years[-1]}, "
                    f"missing_years={missing_years}, available_folds=1..{int(folds[-1].fold_id)}"
                ),
                "The requested training command cannot construct its starting fold from this panel.",
                (
                    "Backfill the missing TWSE/TPEx official years required by the original fold numbering; "
                    "do not silently renumber folds."
                ),
            )
        )
    return summary, findings


def _source_dates(path: Path) -> np.ndarray:
    schema = pq.read_schema(path).names
    if "date" not in schema:
        return np.asarray([], dtype="datetime64[D]")
    frame = (
        pl.scan_parquet(path)
        .select(_date_column_expr("date").alias("date"))
        .drop_nulls("date")
        .unique("date")
        .sort("date")
        .collect()
    )
    if frame.is_empty():
        return np.asarray([], dtype="datetime64[D]")
    return np.asarray(frame.get_column("date").to_numpy(), dtype="datetime64[D]")


def audit_historical_sources(
    public_dir: Path,
    benchmark_sessions: np.ndarray,
    config: ExperimentConfig,
) -> tuple[list[SourceProfile], list[Finding]]:
    selected = _selected_features(config)
    profiles: list[SourceProfile] = []
    findings: list[Finding] = []
    if benchmark_sessions.size == 0:
        raise ValueError("benchmark session calendar is empty")
    end = benchmark_sessions[-1]

    for expectation in HISTORICAL_SOURCES:
        selected_for_source = _source_selected_features(expectation, selected)
        path = public_dir / f"{expectation.name}.parquet"
        quarantined = bool(selected_for_source) and all(
            _is_zero_filled(feature, config) for feature in selected_for_source
        )
        if not path.exists():
            profiles.append(
                SourceProfile(
                    source=expectation.name,
                    path=str(path),
                    status="missing",
                    rows=0,
                    distinct_dates=0,
                    first_date="",
                    last_date="",
                    expected_sessions=0,
                    covered_sessions=0,
                    session_coverage=None,
                    selected_features=",".join(selected_for_source),
                    quarantined=quarantined,
                )
            )
            if selected_for_source:
                findings.append(
                    Finding(
                        severity="medium" if quarantined else "critical",
                        code="missing_historical_source",
                        component="source",
                        item=expectation.name,
                        evidence=f"missing {path}",
                        impact="Selected model features cannot be reconstructed from their official source.",
                        remediation="Run the full historical public-data rebuild before training.",
                    )
                )
            continue

        source_dates = _source_dates(path)
        first = np.datetime64(expectation.first_date, "D")
        expected = benchmark_sessions[(benchmark_sessions >= first) & (benchmark_sessions <= end)]
        covered = np.intersect1d(expected, source_dates, assume_unique=False)
        coverage = float(covered.size / expected.size) if expected.size else None
        rows = int(pq.ParquetFile(path).metadata.num_rows)
        if coverage is None:
            status = "no_expected_sessions"
        elif coverage >= 0.98:
            status = "complete"
        elif coverage >= 0.80:
            status = "degraded"
        else:
            status = "incomplete"
        profiles.append(
            SourceProfile(
                source=expectation.name,
                path=str(path),
                status=status,
                rows=rows,
                distinct_dates=int(source_dates.size),
                first_date=str(source_dates[0]) if source_dates.size else "",
                last_date=str(source_dates[-1]) if source_dates.size else "",
                expected_sessions=int(expected.size),
                covered_sessions=int(covered.size),
                session_coverage=coverage,
                selected_features=",".join(selected_for_source),
                quarantined=quarantined,
            )
        )
        if selected_for_source and (coverage is None or coverage < 0.98):
            severity = "medium" if quarantined else ("high" if coverage and coverage >= 0.80 else "critical")
            findings.append(
                Finding(
                    severity=severity,
                    code="historical_session_coverage",
                    component="source",
                    item=expectation.name,
                    evidence=(
                        f"covered={covered.size}/{expected.size} "
                        f"({0.0 if coverage is None else coverage:.2%}), "
                        f"range={profiles[-1].first_date}..{profiles[-1].last_date}, "
                        f"quarantined={quarantined}"
                    ),
                    impact=(
                        "Coverage onset and market-segment missingness can become a calendar or venue shortcut."
                    ),
                    remediation=(
                        "Backfill every missing official session; keep affected features in feature_zero_fill until coverage passes."
                    ),
                )
            )
    return profiles, findings


def audit_snapshot_contract(
    public_dir: Path,
    config: ExperimentConfig,
) -> list[Finding]:
    selected = _selected_features(config)
    findings: list[Finding] = []
    for feature_pattern, source_globs in SNAPSHOT_FEATURE_SOURCES.items():
        matched = [feature for feature in selected if fnmatch.fnmatchcase(feature, feature_pattern)]
        if not matched:
            continue
        source_paths = [
            path
            for pattern in source_globs
            for path in sorted(public_dir.glob(pattern))
        ]
        all_quarantined = all(_is_zero_filled(feature, config) for feature in matched)
        findings.append(
            Finding(
                severity="low" if all_quarantined else "critical",
                code="snapshot_history_contract",
                component="feature_lineage",
                item=feature_pattern,
                evidence=(
                    f"sources={len(source_paths)}, selected={len(matched)}, "
                    f"all_zero_filled={all_quarantined}"
                ),
                impact=(
                    "A download made today cannot recreate historical point-in-time snapshots. "
                    "Backward filling them would leak future state."
                ),
                remediation=(
                    "Accumulate immutable daily snapshots going forward or obtain a true historical archive; "
                    "until then retain the feature slots but zero-fill them."
                ),
            )
        )
    return findings


def audit_non_vintage_archive_contract(
    public_dir: Path,
    config: ExperimentConfig,
) -> list[Finding]:
    selected = _selected_features(config)
    findings: list[Finding] = []
    for feature_pattern, source_names in NON_VINTAGE_ARCHIVE_SOURCES.items():
        matched = [feature for feature in selected if fnmatch.fnmatchcase(feature, feature_pattern)]
        if not matched:
            continue
        source_paths = [public_dir / name for name in source_names]
        existing = sum(path.exists() for path in source_paths)
        all_quarantined = all(_is_zero_filled(feature, config) for feature in matched)
        findings.append(
            Finding(
                severity="low" if all_quarantined else "critical",
                code="single_vintage_history_contract",
                component="feature_lineage",
                item=feature_pattern,
                evidence=(
                    f"sources={existing}/{len(source_paths)}, selected={len(matched)}, "
                    f"all_zero_filled={all_quarantined}"
                ),
                impact=(
                    "A current historical archive contains the latest revised values, not the vintage "
                    "that was observable at each old release date. A fixed publication lag does not remove revision lookahead."
                ),
                remediation=(
                    "Obtain immutable release vintages and use their actual publication timestamps; "
                    "until then retain the feature slots but zero-fill them."
                ),
            )
        )
    return findings


def audit_feature_lineage_registry(config: ExperimentConfig) -> list[Finding]:
    registered_patterns = [
        *(pattern for source in HISTORICAL_SOURCES for pattern in source.feature_patterns),
        *SNAPSHOT_FEATURE_SOURCES,
        *NON_VINTAGE_ARCHIVE_SOURCES,
    ]
    unregistered = [
        feature
        for feature in _selected_features(config)
        if feature.startswith("twpub_") and not _matches(feature, registered_patterns)
    ]
    if not unregistered:
        return []
    return [
        Finding(
            "critical",
            "unregistered_feature_lineage",
            "feature_lineage",
            "selected_tw_public_features",
            f"unregistered={unregistered}",
            "A model input has no auditable source or point-in-time classification.",
            "Register the source and availability semantics before allowing the feature into the panel.",
        )
    ]


def _read_json_object(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def audit_source_receipts(
    public_dir: Path,
    config: ExperimentConfig,
) -> tuple[dict[str, Any], list[Finding]]:
    findings: list[Finding] = []
    public_path = public_dir / "download_summary.json"
    public_receipt = _read_json_object(public_path)
    receipt_summary: dict[str, Any] = {
        "public_download_summary": str(public_path),
        "public_download_mode": public_receipt.get("mode") if public_receipt else None,
        "public_download_failed_count": public_receipt.get("failed_count") if public_receipt else None,
        "public_download_coverage_complete": (
            public_receipt.get("coverage_complete") if public_receipt else None
        ),
    }
    if public_receipt is None:
        findings.append(
            Finding(
                "high",
                "missing_public_download_receipt",
                "source_receipt",
                str(public_path),
                "missing or invalid JSON",
                "There is no machine-verifiable record that all requested public datasets completed.",
                "Run the strict public-data downloader and retain download_summary.json.",
            )
        )
    else:
        failed_count = int(public_receipt.get("failed_count", -1))
        if failed_count != 0:
            findings.append(
                Finding(
                    "critical",
                    "public_download_failures",
                    "source_receipt",
                    str(public_path),
                    f"failed_count={failed_count}",
                    "At least one requested public source failed or the receipt is malformed.",
                    "Repair every failed dataset before rebuilding features.",
                )
            )
        legacy_full_receipt = (
            "coverage_complete" not in public_receipt
            and str(public_receipt.get("mode", "")) == "full"
        )
        if public_receipt.get("coverage_complete") is not True and not legacy_full_receipt:
            findings.append(
                Finding(
                    "critical",
                    "incomplete_public_download_coverage",
                    "source_receipt",
                    str(public_path),
                    (
                        f"mode={public_receipt.get('mode')!r}, "
                        f"coverage_complete={public_receipt.get('coverage_complete')!r}"
                    ),
                    "The receipt does not prove every requested historical weekday was resolved.",
                    "Run --mode rebuild or --mode repair until coverage_complete is true.",
                )
            )

    if config.data.use_tw_public_rules:
        short_path = public_dir / "tw_short_sale_download_report.json"
        short_receipt = _read_json_object(short_path)
        receipt_summary["short_rule_report"] = str(short_path)
        if short_receipt is None:
            findings.append(
                Finding(
                    "critical",
                    "missing_short_rule_receipt",
                    "source_receipt",
                    str(short_path),
                    "missing or invalid JSON",
                    "Delisting and mandatory-cover masks cannot be certified against the available official archive.",
                    "Run download_tw_short_sale_restrictions.py in strict mode and rebuild the rule parquet.",
                )
            )
        else:
            quality = short_receipt.get("data_quality")
            quality = quality if isinstance(quality, dict) else {}
            checks = {
                "requests_complete": short_receipt.get("requests_complete") is True,
                "failure_count_zero": int(short_receipt.get("failure_count", -1)) == 0,
                "unparseable_zero": int(short_receipt.get("unparseable", -1)) == 0,
                "data_output_written": short_receipt.get("data_output_written") is True,
                "no_duplicate_rows": int(quality.get("composite_key_duplicate_rows", -1)) == 0,
                "all_symbols_parsed": (
                    int(quality.get("rows", 0)) > 0
                    and int(quality.get("symbols_nonempty_rows", -1)) == int(quality.get("rows", 0))
                ),
            }
            receipt_summary["short_rule_checks"] = checks
            failed_checks = [name for name, passed in checks.items() if not passed]
            if failed_checks:
                findings.append(
                    Finding(
                        "critical",
                        "incomplete_short_rule_receipt",
                        "source_receipt",
                        str(short_path),
                        f"failed_checks={failed_checks}",
                        "Available official lifecycle/short-cover archive coverage is incomplete or malformed.",
                        "Rerun the strict archive downloader; do not opt into --allow-partial for training data.",
                    )
                )
            archive_cohort_complete = (
                short_receipt.get("archive_cohort_coverage_complete") is True
                or quality.get("archive_cohort_coverage_complete") is True
            )
            receipt_summary["archive_cohort_coverage_complete"] = archive_cohort_complete
            if not archive_cohort_complete:
                findings.append(
                    Finding(
                        "medium",
                        "incomplete_delisting_announcement_cross_coverage",
                        "source_receipt",
                        str(short_path),
                        "Not every official delisting has a matching relevant announcement in the available archive.",
                        (
                            "Official delisting rows still provide terminal lifecycle dates, but historical mandatory-cover "
                            "timing can be unavailable for securities without an archived applicable notice."
                        ),
                        "Preserve this limitation; never infer a short-cover notice from delisting alone.",
                    )
                )
    return receipt_summary, findings


def audit_feature_build_receipt(
    public_feature_path: Path,
    public_dir: Path,
    parquet_root: Path,
) -> tuple[dict[str, Any], list[Finding]]:
    summary_path = public_feature_path.with_suffix(".summary.json")
    summary = _read_json_object(summary_path)
    receipt_summary: dict[str, Any] = {"path": str(summary_path), "valid": False}
    if summary is None:
        return receipt_summary, [
            Finding(
                "critical",
                "missing_feature_build_receipt",
                "feature_receipt",
                str(summary_path),
                "missing or invalid JSON",
                "The feature table cannot be tied to exact source bytes and symbol membership.",
                "Rebuild the TW public feature table with the current builder.",
            )
        ]

    checks: dict[str, bool] = {}
    checks["feature_schema"] = summary.get("feature_columns") == list(FEATURE_COLUMNS)
    checks["rule_schema"] = summary.get("rule_columns") == list(RULE_COLUMNS)
    checks["source_bytes"] = summary.get("source_receipts") == _source_content_receipts(public_dir)
    checks["symbol_universe"] = summary.get("symbol_universe_receipt") == _symbol_universe_receipt(
        parquet_root
    )
    checks["output_bytes"] = (
        public_feature_path.exists()
        and summary.get("output_receipt") == _file_content_receipt(public_feature_path)
    )
    receipt_summary["checks"] = checks
    receipt_summary["valid"] = all(checks.values())
    failed_checks = [name for name, passed in checks.items() if not passed]
    if not failed_checks:
        return receipt_summary, []
    return receipt_summary, [
        Finding(
            "critical",
            "stale_feature_build_receipt",
            "feature_receipt",
            str(summary_path),
            f"failed_checks={failed_checks}",
            "The feature parquet does not correspond to the current source bytes, schema, or symbol universe.",
            "Rebuild features atomically before constructing or loading a panel cache.",
        )
    ]


def _finite_expr(name: str):
    column = pl.col(name).cast(pl.Float64, strict=False)
    return column.is_not_null() & column.is_finite().fill_null(False)


def audit_public_feature_table(
    path: Path,
    selected_features: list[str],
    market_symbol: str,
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]], list[Finding]]:
    findings: list[Finding] = []
    if not path.exists():
        return {}, [], [
            Finding(
                "critical",
                "missing_public_feature_table",
                "feature_table",
                str(path),
                "file does not exist",
                "TW public features and execution rules cannot be loaded.",
                "Rebuild the public feature parquet before panel construction.",
            )
        ]

    schema = pq.read_schema(path).names
    public_selected = [feature for feature in selected_features if feature.startswith("twpub_")]
    missing_features = [feature for feature in public_selected if feature not in schema]
    missing_rules = [rule for rule in RULE_COLUMNS if rule not in schema]
    if missing_features:
        findings.append(
            Finding(
                "critical",
                "missing_selected_feature_columns",
                "feature_table",
                str(path),
                ",".join(missing_features),
                "The configured model input schema is not present in the built feature table.",
                "Rebuild features with the current code and do not train from this artifact.",
            )
        )
    if missing_rules:
        findings.append(
            Finding(
                "critical",
                "stale_rule_schema",
                "feature_table",
                str(path),
                ",".join(missing_rules),
                "Execution masks omit current mandatory-cover or lifecycle semantics.",
                "Rebuild tw_public_stock_daily.parquet with the current rule schema.",
            )
        )

    available = [feature for feature in public_selected if feature in schema]
    scan = pl.scan_parquet(path)
    duplicate_keys = (
        scan.group_by(["date", "symbol"])
        .len()
        .filter(pl.col("len") > 1)
        .select(pl.len())
        .collect()
        .item()
    )
    if int(duplicate_keys or 0):
        findings.append(
            Finding(
                "critical",
                "duplicate_feature_keys",
                "feature_table",
                str(path),
                f"duplicate_key_groups={int(duplicate_keys)}",
                "The intended date-symbol grain is not unique and joins can be order dependent.",
                "Resolve duplicates at source and rebuild with a deterministic last-observation rule.",
            )
        )

    aggregate_exprs: list[Any] = []
    for feature in available:
        valid = _finite_expr(feature)
        aggregate_exprs.extend(
            [
                valid.sum().alias(f"{feature}__count"),
                pl.col("symbol").filter(valid).n_unique().alias(f"{feature}__symbols"),
                pl.col("date").filter(valid).min().alias(f"{feature}__first"),
                pl.col("date").filter(valid).max().alias(f"{feature}__last"),
                pl.col(feature).cast(pl.Float64, strict=False).filter(valid).min().alias(f"{feature}__min"),
                pl.col(feature).cast(pl.Float64, strict=False).filter(valid).max().alias(f"{feature}__max"),
                (~valid & pl.col(feature).is_not_null()).sum().alias(f"{feature}__nonfinite"),
            ]
        )
    raw_stats: dict[str, dict[str, Any]] = {}
    if aggregate_exprs:
        row = scan.select(aggregate_exprs).collect().row(0, named=True)
        for feature in available:
            raw_stats[feature] = {
                "count": int(row[f"{feature}__count"] or 0),
                "symbols": int(row[f"{feature}__symbols"] or 0),
                "first": str(row[f"{feature}__first"] or "")[:10],
                "last": str(row[f"{feature}__last"] or "")[:10],
                "min": row[f"{feature}__min"],
                "max": row[f"{feature}__max"],
                "nonfinite": int(row[f"{feature}__nonfinite"] or 0),
            }
            if raw_stats[feature]["nonfinite"]:
                findings.append(
                    Finding(
                        "critical",
                        "nonfinite_public_feature",
                        "feature_table",
                        feature,
                        f"nonfinite={raw_stats[feature]['nonfinite']}",
                        "NaN or infinity can destabilize preprocessing and compiled training.",
                        "Fix the source transform and rebuild; do not silently coerce the raw feature table.",
                    )
                )

    annual_rows: list[dict[str, Any]] = []
    if available:
        annual = (
            scan.select(
                pl.col("date").cast(pl.Date, strict=False).dt.year().alias("year"),
                pl.col("symbol"),
                *[pl.col(feature) for feature in available],
            )
            .drop_nulls("year")
            .group_by("year")
            .agg(
                pl.len().alias("rows"),
                (pl.col("symbol") != market_symbol).sum().alias("stock_rows"),
                (pl.col("symbol") == market_symbol).sum().alias("market_rows"),
                *[_finite_expr(feature).sum().alias(f"{feature}__count") for feature in available],
            )
            .sort("year")
            .collect()
        )
        for row in annual.iter_rows(named=True):
            for feature in available:
                annual_rows.append(
                    {
                        "year": int(row["year"]),
                        "feature": feature,
                        "raw_non_null": int(row[f"{feature}__count"] or 0),
                        "stock_rows": int(row["stock_rows"] or 0),
                        "market_rows": int(row["market_rows"] or 0),
                    }
                )
    return raw_stats, annual_rows, findings


def _panel_kwargs(config: ExperimentConfig, public_feature_path: Path) -> dict[str, Any]:
    use_features = bool(config.data.use_tw_public_features)
    use_rules = bool(config.data.use_tw_public_rules)
    use_public = use_features or use_rules
    return {
        "benchmark_name": config.data.benchmark_name,
        "usd_only_trading_pairs": config.data.usd_only_trading_pairs,
        "tradable_mode": config.data.tradable_mode,
        "trading_volume_policy": config.data.trading_volume_policy,
        "security_filter": config.data.security_filter,
        "strict_no_fallback": config.training.strict_no_fallback,
        "panel_backend": config.data.panel_backend,
        "panel_load_workers": config.data.panel_load_workers,
        "external_feature_path": public_feature_path if use_public else None,
        "external_market_symbol": config.data.tw_public_market_symbol,
        "external_include_features": use_features,
        "external_include_rules": use_rules,
        "external_data_required": use_public,
        "feature_include": config.data.feature_include,
        "feature_exclude": config.data.feature_exclude,
        "feature_zero_fill": config.data.feature_zero_fill,
    }


def _load_or_build_panel(
    config: ExperimentConfig,
    parquet_root: Path,
    public_feature_path: Path,
    *,
    build_if_missing: bool,
):
    kwargs = _panel_kwargs(config, public_feature_path)
    panel = load_cached_panel(parquet_root, **kwargs)
    if panel is not None:
        return panel, "cache"
    if not build_if_missing:
        raise RuntimeError(
            "No valid panel cache exists for this exact data/config fingerprint; pass --build-panel."
        )
    return build_panel(parquet_root, **kwargs), "built"


def _empty_panel_accumulator(years: np.ndarray, feature_count: int) -> dict[str, np.ndarray]:
    shape = (years.size, feature_count)
    return {
        "count": np.zeros(shape, dtype=np.int64),
        "nonzero": np.zeros(shape, dtype=np.int64),
        "sum": np.zeros(shape, dtype=np.float64),
        "sum_squares": np.zeros(shape, dtype=np.float64),
        "min": np.full(shape, np.inf, dtype=np.float64),
        "max": np.full(shape, -np.inf, dtype=np.float64),
        "nonfinite": np.zeros(shape, dtype=np.int64),
    }


def _accumulate_panel_features(panel, *, chunk_rows: int = 16):
    panel_years = np.asarray(panel.dates, dtype="datetime64[Y]").astype(np.int64) + 1970
    years = np.unique(panel_years).astype(np.int64)
    year_to_index = {int(year): idx for idx, year in enumerate(years.tolist())}
    feature_count = len(panel.feature_names)
    acc = _empty_panel_accumulator(years, feature_count)
    all_nonfinite = np.zeros(feature_count, dtype=np.int64)

    alive_source = panel.alive_mask
    if alive_source is None:
        alive_source = np.ones(panel.features.shape[:2], dtype=bool)

    for start in range(0, int(panel.num_dates), max(1, int(chunk_rows))):
        end = min(int(panel.num_dates), start + max(1, int(chunk_rows)))
        values = np.asarray(panel.features[start:end], dtype=np.float64)
        all_nonfinite += (~np.isfinite(values)).sum(axis=(0, 1))
        for year in np.unique(panel_years[start:end]):
            local_rows = np.flatnonzero(panel_years[start:end] == year)
            if local_rows.size == 0:
                continue
            active = np.asarray(alive_source[start:end][local_rows], dtype=bool)
            selected = values[local_rows][active]
            if selected.size == 0:
                continue
            finite = np.isfinite(selected)
            safe = np.where(finite, selected, 0.0)
            idx = year_to_index[int(year)]
            acc["count"][idx] += finite.sum(axis=0)
            acc["nonzero"][idx] += (finite & (np.abs(selected) > 1e-12)).sum(axis=0)
            acc["sum"][idx] += safe.sum(axis=0)
            acc["sum_squares"][idx] += np.square(safe).sum(axis=0)
            acc["min"][idx] = np.minimum(
                acc["min"][idx],
                np.min(np.where(finite, selected, np.inf), axis=0),
            )
            acc["max"][idx] = np.maximum(
                acc["max"][idx],
                np.max(np.where(finite, selected, -np.inf), axis=0),
            )
            acc["nonfinite"][idx] += (~finite).sum(axis=0)
    return years, acc, all_nonfinite


def _safe_rate(numerator: int, denominator: int) -> float | None:
    return float(numerator / denominator) if denominator else None


def _feature_scope(feature: str) -> str:
    if feature in BASE_FEATURES:
        return "stock_base"
    if feature.startswith(MARKET_FEATURE_PREFIXES):
        return "market"
    return "stock_public"


def _panel_feature_profiles(
    panel,
    raw_stats: dict[str, dict[str, Any]],
    config: ExperimentConfig,
) -> tuple[list[FeatureProfile], list[dict[str, Any]], list[Finding]]:
    years, acc, all_nonfinite = _accumulate_panel_features(panel)
    profiles: list[FeatureProfile] = []
    annual_rows: list[dict[str, Any]] = []
    findings: list[Finding] = []
    for feature_idx, feature in enumerate(panel.feature_names):
        counts = acc["count"][:, feature_idx]
        nonzero = acc["nonzero"][:, feature_idx]
        total_count = int(counts.sum())
        total_nonzero = int(nonzero.sum())
        total_sum = float(acc["sum"][:, feature_idx].sum())
        total_squares = float(acc["sum_squares"][:, feature_idx].sum())
        mean = total_sum / total_count if total_count else None
        variance = (
            max(0.0, total_squares / total_count - float(mean) ** 2)
            if total_count and mean is not None
            else None
        )
        std = math.sqrt(variance) if variance is not None else None
        yearly_rates = np.divide(
            nonzero,
            counts,
            out=np.zeros_like(nonzero, dtype=np.float64),
            where=counts > 0,
        )
        nonzero_year_indices = np.flatnonzero(nonzero > 0)
        jumps = np.abs(np.diff(yearly_rates)) if yearly_rates.size > 1 else np.asarray([])
        configured_zero = _is_zero_filled(feature, config)
        raw = raw_stats.get(feature, {})
        min_values = acc["min"][:, feature_idx]
        max_values = acc["max"][:, feature_idx]
        finite_min = min_values[np.isfinite(min_values)]
        finite_max = max_values[np.isfinite(max_values)]
        profile = FeatureProfile(
            feature=feature,
            scope=_feature_scope(feature),
            configured_zero_fill=configured_zero,
            raw_non_null=int(raw.get("count", 0)),
            raw_distinct_symbols=int(raw.get("symbols", 0)),
            raw_first_date=str(raw.get("first", "")),
            raw_last_date=str(raw.get("last", "")),
            raw_min=float(raw["min"]) if raw.get("min") is not None else None,
            raw_max=float(raw["max"]) if raw.get("max") is not None else None,
            panel_count=total_count,
            panel_nonzero=total_nonzero,
            panel_nonzero_rate=_safe_rate(total_nonzero, total_count),
            panel_mean=mean,
            panel_std=std,
            panel_min=float(finite_min.min()) if finite_min.size else None,
            panel_max=float(finite_max.max()) if finite_max.size else None,
            panel_nonfinite=int(all_nonfinite[feature_idx]),
            first_nonzero_year=(
                int(years[nonzero_year_indices[0]]) if nonzero_year_indices.size else None
            ),
            last_nonzero_year=(
                int(years[nonzero_year_indices[-1]]) if nonzero_year_indices.size else None
            ),
            max_annual_nonzero_rate_jump=float(jumps.max()) if jumps.size else None,
        )
        profiles.append(profile)
        for year_idx, year in enumerate(years.tolist()):
            annual_rows.append(
                {
                    "year": int(year),
                    "feature": feature,
                    "count": int(counts[year_idx]),
                    "nonzero": int(nonzero[year_idx]),
                    "nonzero_rate": float(yearly_rates[year_idx]),
                    "mean": (
                        float(acc["sum"][year_idx, feature_idx] / counts[year_idx])
                        if counts[year_idx]
                        else None
                    ),
                }
            )

        if profile.panel_nonfinite:
            findings.append(
                Finding(
                    "critical",
                    "nonfinite_panel_feature",
                    "panel",
                    feature,
                    f"nonfinite={profile.panel_nonfinite}",
                    "The model can receive NaN or infinity.",
                    "Fix preprocessing and invalidate the panel cache.",
                )
            )
        if configured_zero and total_nonzero:
            findings.append(
                Finding(
                    "critical",
                    "zero_fill_contract_violation",
                    "panel",
                    feature,
                    f"nonzero={total_nonzero}",
                    "A quarantined feature is still reaching the model.",
                    "Fix feature_zero_fill matching and rebuild the panel cache.",
                )
            )
        if not configured_zero and total_nonzero == 0:
            findings.append(
                Finding(
                    "critical",
                    "unexpected_all_zero_feature",
                    "panel",
                    feature,
                    f"active_count={total_count}",
                    "The configured input slot carries no information and may indicate a broken join.",
                    "Repair the source/join or explicitly quarantine the feature.",
                )
            )
        if (
            not configured_zero
            and profile.max_annual_nonzero_rate_jump is not None
            and profile.max_annual_nonzero_rate_jump > 0.25
        ):
            findings.append(
                Finding(
                    "high",
                    "annual_feature_coverage_jump",
                    "panel",
                    feature,
                    f"max_jump={profile.max_annual_nonzero_rate_jump:.2%}",
                    "The model can identify the calendar/source regime from feature availability.",
                    "Backfill the source or quarantine this feature until coverage is stable.",
                )
            )

    by_name = {profile.feature: profile for profile in profiles}
    bounds = {
        "body_ratio": (0.0, 1.0),
        "signed_body_ratio": (-1.0, 1.0),
        "clv": (0.0, 1.0),
        "clv_centered": (-0.5, 0.5),
        "upper_shadow": (0.0, 1.0),
        "lower_shadow": (0.0, 1.0),
        "shadow_imbalance": (-1.0, 1.0),
    }
    for feature, (lower, upper) in bounds.items():
        profile = by_name.get(feature)
        if profile is None or profile.panel_min is None or profile.panel_max is None:
            continue
        if profile.panel_min < lower - 1e-6 or profile.panel_max > upper + 1e-6:
            findings.append(
                Finding(
                    "critical",
                    "feature_range_violation",
                    "panel",
                    feature,
                    f"range=[{profile.panel_min},{profile.panel_max}] expected=[{lower},{upper}]",
                    "Malformed OHLC geometry reaches the model.",
                    "Repair invalid OHLC rows and rebuild K-bar features.",
                )
            )
    return profiles, annual_rows, findings


def audit_panel_contract(
    panel,
    benchmark_sessions: np.ndarray,
) -> tuple[dict[str, Any], list[Finding]]:
    findings: list[Finding] = []
    returns = np.asarray(panel.returns_1d)
    tradable = np.asarray(panel.tradable_mask, dtype=bool)
    finite = np.isfinite(returns)
    panel_dates = np.asarray(panel.dates, dtype="datetime64[D]")
    benchmark_days = np.asarray(benchmark_sessions, dtype="datetime64[D]")
    extra_panel_dates = np.setdiff1d(panel_dates, benchmark_days)
    missing_panel_dates = np.setdiff1d(benchmark_days, panel_dates)
    panel_date_to_index = {value: idx for idx, value in enumerate(panel_dates.tolist())}
    session_indices = np.asarray(
        [panel_date_to_index[value] for value in benchmark_days.tolist() if value in panel_date_to_index],
        dtype=np.int64,
    )
    destination_violations = 0
    terminal_nonzero_labels = 0
    if session_indices.size > 1:
        current_indices = session_indices[:-1]
        next_indices = session_indices[1:]
        current_returns = returns[current_indices]
        nonzero_labels = np.isfinite(current_returns) & (np.abs(current_returns) > 1e-12)
        next_close = np.asarray(panel.close_prices)[next_indices]
        next_volume = np.asarray(panel.daily_volumes)[next_indices]
        executable_quote = np.isfinite(next_close) & np.isfinite(next_volume) & (next_volume > 0.0)
        destination_violations = int(np.count_nonzero(nonzero_labels & ~executable_quote))
        last_session_returns = returns[session_indices[-1]]
        terminal_nonzero_labels = int(
            np.count_nonzero(np.isfinite(last_session_returns) & (np.abs(last_session_returns) > 1e-12))
        )
    non_session_rows = np.flatnonzero(~np.isin(panel_dates, benchmark_days))
    non_session_nonzero_labels = (
        int(
            np.count_nonzero(
                np.isfinite(returns[non_session_rows])
                & (np.abs(returns[non_session_rows]) > 1e-12)
            )
        )
        if non_session_rows.size
        else 0
    )
    extreme_labels = int(np.count_nonzero(finite & (np.abs(returns) > math.log(3.0))))
    buy_violations = int(np.count_nonzero(np.asarray(panel.can_buy_mask, dtype=bool) & ~tradable))
    sell_violations = int(np.count_nonzero(np.asarray(panel.can_sell_mask, dtype=bool) & ~tradable))
    short_violations = int(
        np.count_nonzero(np.asarray(panel.can_short_open_mask, dtype=bool) & ~tradable)
    )
    summary = {
        "dates": int(panel.num_dates),
        "symbols": int(panel.num_symbols),
        "features": len(panel.feature_names),
        "finite_labels": int(finite.sum()),
        "extra_non_benchmark_dates": int(extra_panel_dates.size),
        "missing_benchmark_dates": int(missing_panel_dates.size),
        "non_session_nonzero_labels": non_session_nonzero_labels,
        "destination_missing_quote_label_violations": destination_violations,
        "terminal_nonzero_labels": terminal_nonzero_labels,
        "abs_log_return_gt_log3": extreme_labels,
        "can_buy_not_tradable": buy_violations,
        "can_sell_not_tradable": sell_violations,
        "can_short_open_not_tradable": short_violations,
        "force_exit_events": int(np.count_nonzero(panel.force_exit_mask)),
        "force_short_cover_events": int(np.count_nonzero(panel.force_short_cover_mask)),
    }
    for code, value in (
        ("extra_non_benchmark_dates", int(extra_panel_dates.size)),
        ("missing_benchmark_dates", int(missing_panel_dates.size)),
        ("non_session_nonzero_label", non_session_nonzero_labels),
        ("destination_missing_quote_label", destination_violations),
        ("terminal_nonzero_label", terminal_nonzero_labels),
        ("can_buy_not_tradable", buy_violations),
        ("can_sell_not_tradable", sell_violations),
        ("can_short_open_not_tradable", short_violations),
    ):
        if value:
            findings.append(
                Finding(
                    "critical",
                    code,
                    "panel_contract",
                    code,
                    f"violations={value}",
                    "Canonical execution or one-session label semantics are inconsistent.",
                    "Fix panel construction and invalidate every affected checkpoint/cache.",
                )
            )
    if extreme_labels:
        findings.append(
            Finding(
                "high",
                "extreme_forward_label",
                "panel_contract",
                "returns_1d",
                f"abs_log_return_gt_log3={extreme_labels}",
                "A small number of corporate-action or vendor discontinuities can dominate portfolio loss and fold selection.",
                "Verify every event against official prices and mask unresolved labels before training.",
            )
        )
    return summary, findings


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        if not fields:
            return
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _fmt(value: Any, *, percent: bool = False) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, float):
        if not math.isfinite(value):
            return ""
        return f"{value:.2%}" if percent else f"{value:.6g}"
    return str(value)


def _write_report(
    path: Path,
    *,
    config_path: Path,
    profiles: list[SourceProfile],
    universe_profiles: list[dict[str, Any]],
    quote_summary: dict[str, Any],
    return_price_summary: dict[str, Any],
    feature_profiles: list[FeatureProfile],
    findings: list[Finding],
    panel_summary: dict[str, Any],
    walk_forward_summary: dict[str, Any],
) -> None:
    severity_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    ordered_findings = sorted(
        findings,
        key=lambda item: (severity_order.get(item.severity, 9), item.code, item.item),
    )
    lines = [
        "# TW Public Data Layer Audit",
        "",
        f"Config: `{config_path}`",
        "",
        "The pass condition is model-safe point-in-time data, not fabricated completeness. "
        "Snapshot-only history must remain quarantined until a real archive exists.",
        "",
        "## Verdict",
        "",
        f"- Critical findings: {sum(item.severity == 'critical' for item in findings)}",
        f"- High findings: {sum(item.severity == 'high' for item in findings)}",
        f"- Medium findings: {sum(item.severity == 'medium' for item in findings)}",
        f"- Low findings: {sum(item.severity == 'low' for item in findings)}",
        f"- Panel shape: {panel_summary.get('dates', 0)} x {panel_summary.get('symbols', 0)} x {panel_summary.get('features', 0)}",
        "",
        "## Findings",
        "",
        "| severity | code | component | item | evidence | remediation |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for item in ordered_findings:
        lines.append(
            f"| {item.severity} | {item.code} | {item.component} | {item.item} | "
            f"{item.evidence.replace('|', '/')} | {item.remediation.replace('|', '/')} |"
        )
    lines.extend(
        [
            "",
            "## Walk Forward",
            "",
            f"Panel years: {walk_forward_summary.get('first_year')}..{walk_forward_summary.get('last_year')}; "
            f"missing years: {walk_forward_summary.get('missing_years_between_bounds', [])}; "
            f"available folds: 1..{walk_forward_summary.get('last_fold')}; "
            f"configured start: {walk_forward_summary.get('configured_start_fold')}.",
            "",
            "| fold | available | train years | validation | test |",
            "| ---: | --- | --- | --- | --- |",
        ]
    )
    for target in walk_forward_summary.get("target_folds", []):
        train_years = target.get("train_years", [])
        train_text = (
            f"{train_years[0]}..{train_years[-1]}" if train_years else ""
        )
        lines.append(
            f"| {target.get('fold_id')} | {target.get('available')} | {train_text} | "
            f"{target.get('val_years', [])} | {target.get('test_years', [])} |"
        )
    lines.extend(
        [
            "",
            "## OHLCV Sources",
            "",
            "| files | rows | Yahoo OHLCV/adjustment rows | unsupported type | unapproved files | lineage errors | read/schema errors | duplicate dates | invalid bars/volume | invalid adjclose | origin != 10 | off-calendar | raw close >2x | adjusted >3x |",
            "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            f"| {quote_summary.get('files', 0)} | {quote_summary.get('rows', 0)} | "
            f"{quote_summary.get('yahoo_fallback_rows', 0)}/{quote_summary.get('yahoo_fallback_adjustment_rows', 0)} | "
            f"{quote_summary.get('unsupported_security_files', 0)} | "
            f"{quote_summary.get('unapproved_source_files', 0)} | "
            f"{quote_summary.get('unapproved_data_source_rows', 0) + quote_summary.get('unapproved_adjustment_source_rows', 0) + quote_summary.get('source_lineage_mismatch', 0)} | "
            f"{quote_summary.get('read_errors', 0) + quote_summary.get('schema_errors', 0)} | "
            f"{quote_summary.get('duplicate_dates', 0)} | "
            f"{quote_summary.get('invalid_ohlc_geometry', 0) + quote_summary.get('negative_volume', 0)} | "
            f"{quote_summary.get('invalid_adjclose', 0)} | "
            f"{quote_summary.get('first_adjclose_not_ten', 0)} | "
            f"{quote_summary.get('off_calendar_rows', 0)} | "
            f"{quote_summary.get('raw_close_jumps_gt_2x', 0)} | "
            f"{quote_summary.get('adjusted_index_jumps_gt_3x', 0)} |",
            "",
            f"Return-price provenance: tracked={return_price_summary.get('tracked_symbols', 0)}, "
            f"raw-close-only={return_price_summary.get('raw_close_symbols', 0)}.",
            "",
            "## Historical Universe",
            "",
            "| market | official delisted | canonical files | missing | coverage |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for item in universe_profiles:
        lines.append(
            f"| {item['market']} | {item['official_delisted_symbols']} | "
            f"{item['canonical_symbol_histories']} | {item['missing_symbol_histories']} | "
            f"{_fmt(item['coverage'], percent=True)} |"
        )
    lines.extend(
        [
            "",
            "## Historical Sources",
            "",
            "| source | status | dates | expected | coverage | range | selected | quarantined |",
            "| --- | --- | ---: | ---: | ---: | --- | --- | --- |",
        ]
    )
    for item in profiles:
        lines.append(
            f"| {item.source} | {item.status} | {item.distinct_dates} | "
            f"{item.expected_sessions} | {_fmt(item.session_coverage, percent=True)} | "
            f"{item.first_date}..{item.last_date} | {item.selected_features} | {item.quarantined} |"
        )
    lines.extend(
        [
            "",
            "## Model Features",
            "",
            "| feature | scope | zero fill | raw count | raw range | panel nonzero | std | first year | max annual jump |",
            "| --- | --- | --- | ---: | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for item in feature_profiles:
        lines.append(
            f"| {item.feature} | {item.scope} | {item.configured_zero_fill} | "
            f"{item.raw_non_null} | {item.raw_first_date}..{item.raw_last_date} | "
            f"{_fmt(item.panel_nonzero_rate, percent=True)} | {_fmt(item.panel_std)} | "
            f"{_fmt(item.first_nonzero_year)} | "
            f"{_fmt(item.max_annual_nonzero_rate_jump, percent=True)} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit source, feature-table, panel, label, and point-in-time contracts for tw_public training."
    )
    parser.add_argument("--config", type=Path, default=Path("configs/markets/tw_public.yaml"))
    parser.add_argument("--parquet-root", type=Path, default=None)
    parser.add_argument("--public-dir", type=Path, default=None)
    parser.add_argument("--public-feature-path", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts/data_quality/tw_public"))
    parser.add_argument("--build-panel", action="store_true")
    parser.add_argument("--strict", action="store_true")
    parser.add_argument(
        "--require-live-selected-features",
        action="store_true",
        help="Fail when any selected public feature is intentionally quarantined or historically incomplete.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    parquet_root = args.parquet_root or Path(config.data.parquet_root)
    public_feature_path = args.public_feature_path or Path(config.data.tw_public_feature_path)
    public_dir = args.public_dir or public_feature_path.parent.parent
    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    sessions = _benchmark_sessions(
        parquet_root,
        config.data.benchmark_name,
        public_feature_path,
    )
    quote_profiles, quote_summary, quote_findings = audit_quote_source_files(
        parquet_root,
        sessions,
        workers=max(1, int(config.data.panel_load_workers)),
        require_official=True,
    )
    official_build_summary, official_build_findings = audit_official_symbol_build(
        parquet_root,
        public_dir,
    )
    return_price_summary, return_price_findings = audit_return_price_provenance(parquet_root)
    universe_profiles, missing_delisted_rows, universe_findings = (
        audit_delisted_universe_coverage(parquet_root, public_dir, sessions)
    )
    receipt_summary, receipt_findings = audit_source_receipts(public_dir, config)
    feature_receipt_summary, feature_receipt_findings = audit_feature_build_receipt(
        public_feature_path,
        public_dir,
        parquet_root,
    )
    source_profiles, source_findings = audit_historical_sources(
        public_dir,
        sessions,
        config,
    )
    snapshot_findings = audit_snapshot_contract(public_dir, config)
    non_vintage_findings = audit_non_vintage_archive_contract(public_dir, config)
    if args.require_live_selected_features:
        for finding in [*source_findings, *snapshot_findings, *non_vintage_findings]:
            if finding.severity in {"low", "medium"}:
                finding.severity = "high"
    findings = [
        *receipt_findings,
        *feature_receipt_findings,
        *quote_findings,
        *official_build_findings,
        *return_price_findings,
        *universe_findings,
        *source_findings,
    ]
    findings.extend(snapshot_findings)
    findings.extend(non_vintage_findings)
    findings.extend(audit_feature_lineage_registry(config))

    selected = _selected_features(config)
    raw_stats, raw_annual_rows, table_findings = audit_public_feature_table(
        public_feature_path,
        selected,
        config.data.tw_public_market_symbol,
    )
    findings.extend(table_findings)

    panel, panel_source = _load_or_build_panel(
        config,
        parquet_root,
        public_feature_path,
        build_if_missing=bool(args.build_panel),
    )
    missing_selected = [feature for feature in selected if feature not in panel.feature_names]
    extra_selected = [feature for feature in panel.feature_names if feature not in selected]
    if missing_selected or extra_selected:
        findings.append(
            Finding(
                "critical",
                "panel_feature_schema_mismatch",
                "panel",
                "feature_names",
                f"missing={missing_selected}, extra={extra_selected}",
                "The panel does not match the exact configured model input order.",
                "Fix feature selection/cache invalidation before training.",
            )
        )
    feature_profiles, panel_annual_rows, panel_findings = _panel_feature_profiles(
        panel,
        raw_stats,
        config,
    )
    findings.extend(panel_findings)
    panel_summary, contract_findings = audit_panel_contract(panel, sessions)
    panel_summary["source"] = panel_source
    panel_summary["parquet_root"] = str(parquet_root)
    panel_summary["public_feature_path"] = str(public_feature_path)
    findings.extend(contract_findings)
    walk_forward_summary, walk_forward_findings = audit_walk_forward_availability(panel, config)
    findings.extend(walk_forward_findings)

    _write_csv(output_dir / "source_profiles.csv", [asdict(item) for item in source_profiles])
    _write_csv(output_dir / "quote_source_profiles.csv", quote_profiles)
    _write_csv(output_dir / "universe_profiles.csv", universe_profiles)
    _write_csv(output_dir / "missing_delisted_symbols.csv", missing_delisted_rows)
    _write_csv(output_dir / "feature_profiles.csv", [asdict(item) for item in feature_profiles])
    _write_csv(output_dir / "raw_annual_feature_profiles.csv", raw_annual_rows)
    _write_csv(output_dir / "panel_annual_feature_profiles.csv", panel_annual_rows)
    _write_csv(output_dir / "findings.csv", [asdict(item) for item in findings])
    summary = {
        "generated_on": date.today().isoformat(),
        "config": str(args.config),
        "panel": panel_summary,
        "walk_forward": walk_forward_summary,
        "source_receipts": receipt_summary,
        "feature_build_receipt": feature_receipt_summary,
        "quote_sources": quote_summary,
        "official_symbol_build": official_build_summary,
        "return_price_provenance": return_price_summary,
        "universe": universe_profiles,
        "finding_counts": {
            severity: sum(item.severity == severity for item in findings)
            for severity in ("critical", "high", "medium", "low")
        },
        "model_safe": not any(item.severity in {"critical", "high"} for item in findings),
        "feature_count": len(feature_profiles),
        "source_count": len(source_profiles),
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    _write_report(
        output_dir / "report.md",
        config_path=args.config,
        profiles=source_profiles,
        universe_profiles=universe_profiles,
        quote_summary=quote_summary,
        return_price_summary=return_price_summary,
        feature_profiles=feature_profiles,
        findings=findings,
        panel_summary=panel_summary,
        walk_forward_summary=walk_forward_summary,
    )
    print(
        f"[tw-public-audit] model_safe={summary['model_safe']} "
        f"findings={summary['finding_counts']} report={output_dir / 'report.md'}"
    )
    if args.strict and not summary["model_safe"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()

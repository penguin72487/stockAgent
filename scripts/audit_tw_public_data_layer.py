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
    POST_CLOSE_CHIP_FEATURE_COLUMNS,
    RULE_COLUMNS,
    TW_PUBLIC_FEATURE_AVAILABILITY_CONTRACT_VERSION,
    _date_column_expr,
    _file_content_receipt,
    _source_content_receipts,
    _symbol_universe_receipt,
)
from stockagent.data.tw_security import classify_tw_stock_or_etf, is_tw_stock_or_etf
from scripts.build_tw_official_symbol_parquets import (
    LIFECYCLE_EVIDENCE_FILENAMES,
    OFFICIAL_SYMBOL_ADJUSTED_PRICE_METHOD,
    OFFICIAL_SYMBOL_BUILD_SCHEMA_VERSION,
    RETURN_QUARANTINE_REASONS,
    YAHOO_FALLBACK_RAW_OHLC_NORMALIZATION,
    _load_verified_taiex_session_calendar,
    _official_symbol_source_paths,
    _validate_yahoo_fallback_archive,
)
from downloader.download_tw_public_data import (
    DEFAULT_DATASETS,
    HistoricalResponseError,
    TAIEX_SESSION_CALENDAR_DATASET,
    TPEX_OFFICIAL_CALENDAR_DATASET,
    _parse_tpex_daily_quotes_html,
    _validated_source_unavailable_receipt_dates,
    _validated_taiex_session_dates,
)


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

# Availability classes are stated relative to a regular-close portfolio
# decision.  The opening price is observable before that decision.  All other
# fields below use final session aggregates and therefore cannot be used to
# establish a position at that same regular close.
PRE_CLOSE_FEATURE_COLUMNS = ("open_logret_1d",)
CLOSE_COMPLETION_FEATURE_COLUMNS = (
    "max_logret_1d",
    "min_logret_1d",
    "close_logret_1d",
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
    "twpub_official_close_logret_1d",
    "twpub_official_trading_volume_log",
    "twpub_official_trading_value_log",
    "twpub_official_trades_log",
    "twpub_official_turnover_ratio",
    "twpub_official_intraday_range",
    "twpub_official_close_to_high",
    "twpub_official_close_to_low",
)

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
        "2005-09-02",
        ("twpub_pe_log", "twpub_pb_log", "twpub_dividend_yield"),
    ),
    HistoricalSourceExpectation(
        "tpex_daily_valuation",
        "2003-08-01",
        ("twpub_pe_log", "twpub_pb_log", "twpub_dividend_yield"),
    ),
    HistoricalSourceExpectation(
        "twse_margin_balance",
        "2001-01-01",
        ("twpub_margin_*", "twpub_short_*"),
    ),
    HistoricalSourceExpectation(
        "tpex_margin_balance",
        "2003-08-01",
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
        "2004-06-01",
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
    observed_sessions: int
    observed_session_coverage: float | None
    source_unavailable_sessions: int
    resolved_sessions: int
    resolved_session_coverage: float | None
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
    included = list(dict.fromkeys(str(name) for name in config.data.feature_include))
    excluded = tuple(str(pattern) for pattern in config.data.feature_exclude)
    return [feature for feature in included if not _matches(feature, excluded)]


def _source_selected_features(
    expectation: HistoricalSourceExpectation,
    selected: list[str],
) -> list[str]:
    return [feature for feature in selected if _matches(feature, expectation.feature_patterns)]


def _benchmark_sessions(
    root: Path,
    benchmark_name: str,
    public_feature_path: Path | None = None,
    public_dir: Path | None = None,
    expected_first_year: int | None = None,
    panel_start_date: str | None = None,
) -> np.ndarray:
    path = root / f"{benchmark_name}_features.parquet"
    if not path.exists():
        raise FileNotFoundError(f"benchmark parquet is missing: {path}")
    if public_dir is None:
        if public_feature_path is None:
            raise ValueError(
                "public_dir or public_feature_path is required for the official session calendar"
            )
        public_dir = public_feature_path.parent.parent
    calendar, _, _ = _load_verified_taiex_session_calendar(public_dir)
    if panel_start_date:
        calendar = calendar.filter(
            pl.col("date") >= date.fromisoformat(str(panel_start_date))
        )
    elif expected_first_year is not None:
        calendar = calendar.filter(
            pl.col("date") >= date(int(expected_first_year), 1, 1)
        )
    else:
        benchmark_first = (
            pl.scan_parquet(path)
            .select(_date_column_expr("date").alias("date"))
            .drop_nulls("date")
            .select(pl.col("date").min())
            .collect()
            .item()
        )
        if benchmark_first is not None:
            calendar = calendar.filter(pl.col("date") >= benchmark_first)
    if calendar.is_empty():
        raise ValueError("receipt-verified TAIEX session calendar is empty")
    return np.asarray(calendar["date"].to_numpy(), dtype="datetime64[D]")


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


def _audit_quote_file(
    path: Path,
    benchmark_sessions: np.ndarray,
    source_sessions: np.ndarray | None = None,
) -> dict[str, Any]:
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
        has_ohlc_normalization = "ohlc_normalization" in schema
        has_raw_ohlc_scale_factor = "raw_ohlc_scale_factor" in schema
        has_raw_ohlc_scale_reference_date = (
            "raw_ohlc_scale_reference_date" in schema
        )
        has_fallback_reason = "fallback_reason" in schema
        has_return_quarantined = "return_quarantined" in schema
        has_return_quarantine_reason = "return_quarantine_reason" in schema
        has_lifecycle_episode_id = "lifecycle_episode_id" in schema
        has_official_listing_evidence = "official_listing_evidence" in schema
        read_columns = list(required)
        if has_data_source:
            read_columns.append("data_source")
        if has_adjustment_source:
            read_columns.append("adjustment_source")
        if has_ohlc_normalization:
            read_columns.append("ohlc_normalization")
        if has_raw_ohlc_scale_factor:
            read_columns.append("raw_ohlc_scale_factor")
        if has_raw_ohlc_scale_reference_date:
            read_columns.append("raw_ohlc_scale_reference_date")
        if has_fallback_reason:
            read_columns.append("fallback_reason")
        if has_return_quarantined:
            read_columns.append("return_quarantined")
        if has_return_quarantine_reason:
            read_columns.append("return_quarantine_reason")
        if has_lifecycle_episode_id:
            read_columns.append("lifecycle_episode_id")
        if has_official_listing_evidence:
            read_columns.append("official_listing_evidence")
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
        if has_ohlc_normalization:
            select_expressions.append(
                pl.col("ohlc_normalization")
                .cast(pl.Utf8, strict=False)
                .str.strip_chars()
                .alias("ohlc_normalization")
            )
        if has_raw_ohlc_scale_factor:
            select_expressions.append(
                pl.col("raw_ohlc_scale_factor")
                .cast(pl.Float64, strict=False)
                .fill_nan(None)
                .alias("raw_ohlc_scale_factor")
            )
        if has_raw_ohlc_scale_reference_date:
            select_expressions.append(
                _date_column_expr("raw_ohlc_scale_reference_date").alias(
                    "raw_ohlc_scale_reference_date"
                )
            )
        if has_fallback_reason:
            select_expressions.append(
                pl.col("fallback_reason")
                .cast(pl.Utf8, strict=False)
                .str.strip_chars()
                .alias("fallback_reason")
            )
        if has_return_quarantined:
            select_expressions.append(
                pl.col("return_quarantined")
                .cast(pl.Boolean, strict=False)
                .fill_null(False)
                .alias("return_quarantined")
            )
        if has_return_quarantine_reason:
            select_expressions.append(
                pl.col("return_quarantine_reason")
                .cast(pl.Utf8, strict=False)
                .str.strip_chars()
                .alias("return_quarantine_reason")
            )
        if has_lifecycle_episode_id:
            select_expressions.append(
                pl.col("lifecycle_episode_id")
                .cast(pl.Int64, strict=False)
                .alias("lifecycle_episode_id")
            )
        if has_official_listing_evidence:
            select_expressions.append(
                pl.col("official_listing_evidence")
                .cast(pl.Utf8, strict=False)
                .str.strip_chars()
                .alias("official_listing_evidence")
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
    calendar_sessions = (
        benchmark_sessions if source_sessions is None else source_sessions
    )
    off_calendar_rows = int(
        valid_dates.select(
            (~pl.col("date").is_in(calendar_sessions.astype(object).tolist())).sum()
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
    unapproved_ohlc_normalization_rows = 0
    invalid_flat_bar_normalization_rows = 0
    fallback_raw_scale_schema_mismatch = int(
        source == MIXED_QUOTE_SOURCE
        and not (
            has_ohlc_normalization
            and has_raw_ohlc_scale_factor
            and has_raw_ohlc_scale_reference_date
        )
    )
    invalid_fallback_raw_scale_rows = 0
    orphan_raw_scale_rows = 0
    audited_data_source = (
        pl.col("data_source")
        if has_data_source
        else pl.lit(None, dtype=pl.String)
    )
    if has_ohlc_normalization:
        normalization = pl.col("ohlc_normalization")
        unapproved_ohlc_normalization_rows = int(
            frame.select(
                (
                    ~normalization.is_null()
                    & ~normalization.is_in(
                        [
                            "official_close_flat_bar",
                            YAHOO_FALLBACK_RAW_OHLC_NORMALIZATION,
                        ]
                    )
                )
                .sum()
            ).item()
            or 0
        )
        invalid_flat_bar_normalization_rows = int(
            frame.select(
                (
                    (normalization == "official_close_flat_bar")
                    & (
                        (pl.col("open") != pl.col("close"))
                        | (pl.col("max") != pl.col("close"))
                        | (pl.col("min") != pl.col("close"))
                        | (audited_data_source == YAHOO_FALLBACK_SOURCE)
                    )
                )
                .fill_null(False)
                .sum()
            ).item()
                or 0
        )
        if has_raw_ohlc_scale_factor and has_raw_ohlc_scale_reference_date:
            fallback_row = audited_data_source == YAHOO_FALLBACK_SOURCE
            scale = pl.col("raw_ohlc_scale_factor")
            scale_reference_date = pl.col("raw_ohlc_scale_reference_date")
            invalid_fallback_raw_scale_rows = int(
                frame.select(
                    (
                        fallback_row
                        & (
                            (normalization != YAHOO_FALLBACK_RAW_OHLC_NORMALIZATION)
                            | scale.is_null()
                            | ~scale.is_finite()
                            | (scale <= 0.0)
                            | scale_reference_date.is_null()
                            | (scale_reference_date > pl.col("date"))
                        )
                    )
                    .fill_null(True)
                    .sum()
                ).item()
                or 0
            )
            orphan_raw_scale_rows = int(
                frame.select(
                    (
                        ~fallback_row
                        & (
                            (normalization == YAHOO_FALLBACK_RAW_OHLC_NORMALIZATION)
                            | scale.is_not_null()
                            | scale_reference_date.is_not_null()
                        )
                    )
                    .fill_null(False)
                    .sum()
                ).item()
                or 0
            )
    unapproved_fallback_reason_rows = 0
    fallback_reason_lineage_mismatch = 0
    fallback_reason_rows = 0
    if has_fallback_reason:
        fallback_reason = pl.col("fallback_reason")
        fallback_reason_rows = int(
            frame.select((fallback_reason == "official_ohlcv_unusable").sum()).item()
            or 0
        )
        unapproved_fallback_reason_rows = int(
            frame.select(
                (
                    ~fallback_reason.is_null()
                    & (fallback_reason != "official_ohlcv_unusable")
                ).sum()
            ).item()
            or 0
        )
        fallback_reason_lineage_mismatch = int(
            frame.select(
                (
                    (fallback_reason == "official_ohlcv_unusable")
                    & (audited_data_source != YAHOO_FALLBACK_SOURCE)
                )
                .fill_null(False)
                .sum()
            ).item()
            or 0
        )

    return_quarantined_rows = 0
    lifecycle_episode_quarantined_rows = 0
    listing_boundary_quarantined_rows = 0
    unverified_extreme_quarantined_rows = 0
    unapproved_return_quarantine_reason_rows = 0
    return_quarantine_lineage_mismatch = 0
    return_quarantine_evidence_mismatch = 0
    validated_quarantine_source_dates: set[date] = set()
    return_quarantine_schema_mismatch = int(
        source in APPROVED_FILE_SOURCES
        and not (has_return_quarantined and has_return_quarantine_reason)
    )
    return_quarantine_evidence_schema_mismatch = int(
        source in APPROVED_FILE_SOURCES
        and not (has_lifecycle_episode_id and has_official_listing_evidence)
    )
    if has_return_quarantined and has_return_quarantine_reason:
        quarantine_reason_present = (
            pl.col("return_quarantine_reason").is_not_null()
            & (pl.col("return_quarantine_reason") != "")
        )
        approved_quarantine_reason = pl.col("return_quarantine_reason").is_in(
            sorted(RETURN_QUARANTINE_REASONS)
        )
        return_quarantined_rows = int(
            frame.select(pl.col("return_quarantined").sum()).item() or 0
        )
        lifecycle_episode_quarantined_rows = int(
            frame.select(
                (
                    pl.col("return_quarantined")
                    & (
                        pl.col("return_quarantine_reason")
                        == "official_lifecycle_episode_boundary"
                    )
                ).sum()
            ).item()
            or 0
        )
        listing_boundary_quarantined_rows = int(
            frame.select(
                (
                    pl.col("return_quarantined")
                    & (
                        pl.col("return_quarantine_reason")
                        == "official_listing_boundary"
                    )
                ).sum()
            ).item()
            or 0
        )
        unverified_extreme_quarantined_rows = int(
            frame.select(
                (
                    pl.col("return_quarantined")
                    & (
                        pl.col("return_quarantine_reason")
                        == "unverified_extreme_adjusted_return"
                    )
                ).sum()
            ).item()
            or 0
        )
        unapproved_return_quarantine_reason_rows = int(
            frame.select(
                (quarantine_reason_present & ~approved_quarantine_reason)
                .fill_null(False)
                .sum()
            ).item()
            or 0
        )

        # Validate both directions of the label-isolation contract.  A
        # whitelist reason is not evidence by itself: it must match the next
        # quote row that the source row's forward return targets.  Conversely,
        # every lifecycle/listing boundary (and every unverified >3x Yahoo
        # transition) must have the exact precedence-selected source reason.
        full_ordered = (
            valid_dates.drop_nulls("close")
            .filter(pl.col("close").is_finite() & (pl.col("close") > 0))
            .sort("date")
            .unique("date", keep="last", maintain_order=True)
        )
        if full_ordered.height:
            quarantine_flags = np.asarray(
                full_ordered["return_quarantined"].to_numpy(), dtype=bool
            )
            quarantine_reasons = full_ordered[
                "return_quarantine_reason"
            ].to_list()
            episode_ids = (
                full_ordered["lifecycle_episode_id"].to_list()
                if has_lifecycle_episode_id
                else [None] * full_ordered.height
            )
            listing_evidence = (
                full_ordered["official_listing_evidence"].to_list()
                if has_official_listing_evidence
                else [None] * full_ordered.height
            )
            data_sources = (
                full_ordered["data_source"].to_list()
                if has_data_source
                else [None] * full_ordered.height
            )
            adjustment_sources = (
                full_ordered["adjustment_source"].to_list()
                if has_adjustment_source
                else [None] * full_ordered.height
            )
            adjusted_values = np.asarray(
                full_ordered["adjclose"].to_numpy(), dtype=np.float64
            )
            ordered_dates = full_ordered["date"].to_list()

            for source_index in range(full_ordered.height):
                actual_reason = (
                    str(quarantine_reasons[source_index] or "").strip()
                    if quarantine_flags[source_index]
                    else ""
                )
                if source_index + 1 >= full_ordered.height:
                    if actual_reason:
                        return_quarantine_evidence_mismatch += 1
                    continue

                destination_index = source_index + 1
                current_episode = episode_ids[source_index]
                next_episode = episode_ids[destination_index]
                episode_boundary = (
                    current_episode is not None
                    and next_episode is not None
                    and current_episode != next_episode
                )
                destination_listing_evidence = str(
                    listing_evidence[destination_index] or ""
                ).strip()
                listing_boundary = bool(destination_listing_evidence)
                current_adjusted = adjusted_values[source_index]
                next_adjusted = adjusted_values[destination_index]
                extreme_transition = bool(
                    math.isfinite(current_adjusted)
                    and math.isfinite(next_adjusted)
                    and current_adjusted > 0.0
                    and next_adjusted > 0.0
                    and abs(math.log(next_adjusted / current_adjusted))
                    > math.log(3.0) + 1e-9
                )
                destination_uses_yahoo = (
                    str(data_sources[destination_index] or "").strip()
                    == YAHOO_FALLBACK_SOURCE
                    or str(adjustment_sources[destination_index] or "").strip()
                    == YAHOO_FALLBACK_SOURCE
                )
                # Non-canonical fixtures without lineage columns may still
                # prove the numerical extreme. Approved official files must
                # carry both lineage columns and cannot use this compatibility
                # allowance.
                unverified_lineage = destination_uses_yahoo or (
                    source not in APPROVED_FILE_SOURCES
                    and not has_data_source
                    and not has_adjustment_source
                )

                expected_reason = ""
                if episode_boundary:
                    expected_reason = "official_lifecycle_episode_boundary"
                elif listing_boundary:
                    expected_reason = "official_listing_boundary"
                elif extreme_transition and unverified_lineage:
                    expected_reason = "unverified_extreme_adjusted_return"

                if actual_reason != expected_reason:
                    if actual_reason or expected_reason:
                        return_quarantine_evidence_mismatch += 1
                    continue
                if actual_reason:
                    validated_quarantine_source_dates.add(
                        ordered_dates[source_index]
                    )
        return_quarantine_lineage_mismatch = int(
            frame.select(
                (
                    (
                        pl.col("return_quarantined")
                        & ~approved_quarantine_reason.fill_null(False)
                    )
                    | (
                        ~pl.col("return_quarantined")
                        & quarantine_reason_present
                    )
                )
                .fill_null(False)
                .sum()
            ).item()
            or 0
        )

    ordered = (
        in_panel_range.drop_nulls("close")
        .filter(pl.col("close").is_finite() & (pl.col("close") > 0))
        .sort("date")
        .unique("date", keep="last", maintain_order=True)
    )
    raw_close_jumps_gt_2x = 0
    raw_close_jumps_touching_yahoo = 0
    adjusted_index_jumps_gt_3x = 0
    quarantined_adjusted_index_jumps_gt_3x = 0
    unresolved_adjusted_index_jumps_gt_3x = 0
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
        if has_data_source:
            ordered_sources = np.asarray(
                ordered["data_source"].to_list(), dtype=object
            )
            touches_yahoo = (
                (ordered_sources[1:] == YAHOO_FALLBACK_SOURCE)
                | (ordered_sources[:-1] == YAHOO_FALLBACK_SOURCE)
            )
            raw_close_jumps_touching_yahoo = int(
                np.count_nonzero(
                    consecutive
                    & (raw_log_change > math.log(2.0) + 1e-9)
                    & touches_yahoo
                )
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
        extreme_adjusted = (
            consecutive
            & valid_adjusted_pair
            & (adjusted_log_change > math.log(3.0) + 1e-9)
        )
        adjusted_index_jumps_gt_3x = int(np.count_nonzero(extreme_adjusted))
        if has_return_quarantined and has_return_quarantine_reason:
            approved_source_quarantine = np.asarray(
                [
                    value in validated_quarantine_source_dates
                    for value in ordered["date"].to_list()
                ],
                dtype=bool,
            )
            quarantined_adjusted_index_jumps_gt_3x = int(
                np.count_nonzero(extreme_adjusted & approved_source_quarantine[:-1])
            )
        unresolved_adjusted_index_jumps_gt_3x = (
            adjusted_index_jumps_gt_3x
            - quarantined_adjusted_index_jumps_gt_3x
        )
    return {
        "symbol": symbol,
        "security_type": security_type or "unsupported",
        "path": str(path),
        "status": "ok" if rows else "empty",
        "source": source,
        "has_data_source": has_data_source,
        "has_adjustment_source": has_adjustment_source,
        "has_ohlc_normalization": has_ohlc_normalization,
        "has_fallback_reason": has_fallback_reason,
        "has_return_quarantined": has_return_quarantined,
        "has_return_quarantine_reason": has_return_quarantine_reason,
        "yahoo_fallback_rows": fallback_rows,
        "yahoo_fallback_adjustment_rows": fallback_adjustment_rows,
        "unapproved_data_source_rows": unapproved_data_source_rows,
        "unapproved_adjustment_source_rows": unapproved_adjustment_source_rows,
        "source_lineage_mismatch": source_lineage_mismatch,
        "fallback_reason_rows": fallback_reason_rows,
        "unapproved_fallback_reason_rows": unapproved_fallback_reason_rows,
        "fallback_reason_lineage_mismatch": fallback_reason_lineage_mismatch,
        "return_quarantined_rows": return_quarantined_rows,
        "lifecycle_episode_quarantined_rows": (
            lifecycle_episode_quarantined_rows
        ),
        "listing_boundary_quarantined_rows": listing_boundary_quarantined_rows,
        "unverified_extreme_quarantined_rows": (
            unverified_extreme_quarantined_rows
        ),
        "unapproved_return_quarantine_reason_rows": (
            unapproved_return_quarantine_reason_rows
        ),
        "return_quarantine_lineage_mismatch": return_quarantine_lineage_mismatch,
        "return_quarantine_evidence_mismatch": return_quarantine_evidence_mismatch,
        "return_quarantine_schema_mismatch": return_quarantine_schema_mismatch,
        "return_quarantine_evidence_schema_mismatch": (
            return_quarantine_evidence_schema_mismatch
        ),
        "unapproved_ohlc_normalization_rows": unapproved_ohlc_normalization_rows,
        "invalid_flat_bar_normalization_rows": invalid_flat_bar_normalization_rows,
        "fallback_raw_scale_schema_mismatch": (
            fallback_raw_scale_schema_mismatch
        ),
        "invalid_fallback_raw_scale_rows": invalid_fallback_raw_scale_rows,
        "orphan_raw_scale_rows": orphan_raw_scale_rows,
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
        "raw_close_jumps_touching_yahoo": raw_close_jumps_touching_yahoo,
        "adjusted_index_jumps_gt_3x": adjusted_index_jumps_gt_3x,
        "quarantined_adjusted_index_jumps_gt_3x": (
            quarantined_adjusted_index_jumps_gt_3x
        ),
        "unresolved_adjusted_index_jumps_gt_3x": (
            unresolved_adjusted_index_jumps_gt_3x
        ),
    }


def audit_quote_source_files(
    parquet_root: Path,
    benchmark_sessions: np.ndarray,
    *,
    workers: int = 16,
    require_official: bool = False,
    source_sessions: np.ndarray | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any], list[Finding]]:
    paths = sorted(parquet_root.glob("*_features.parquet"))
    with ThreadPoolExecutor(max_workers=max(1, int(workers))) as executor:
        profiles = list(
            executor.map(
                lambda path: _audit_quote_file(
                    path,
                    benchmark_sessions,
                    source_sessions,
                ),
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
        "fallback_reason_rows",
        "unapproved_fallback_reason_rows",
        "fallback_reason_lineage_mismatch",
        "return_quarantined_rows",
        "lifecycle_episode_quarantined_rows",
        "listing_boundary_quarantined_rows",
        "unverified_extreme_quarantined_rows",
        "unapproved_return_quarantine_reason_rows",
        "return_quarantine_lineage_mismatch",
        "return_quarantine_evidence_mismatch",
        "return_quarantine_schema_mismatch",
        "return_quarantine_evidence_schema_mismatch",
        "unapproved_ohlc_normalization_rows",
        "invalid_flat_bar_normalization_rows",
        "fallback_raw_scale_schema_mismatch",
        "invalid_fallback_raw_scale_rows",
        "orphan_raw_scale_rows",
        "raw_close_jumps_gt_2x",
        "raw_close_jumps_touching_yahoo",
        "adjusted_index_jumps_gt_3x",
        "quarantined_adjusted_index_jumps_gt_3x",
        "unresolved_adjusted_index_jumps_gt_3x",
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
            "unapproved_fallback_reason",
            "critical",
            "unapproved_fallback_reason_rows",
            "Some Yahoo rows carry an unknown reason for bypassing official data.",
            "Rebuild canonical symbol files with the approved official_ohlcv_unusable reason.",
        ),
        (
            "fallback_reason_lineage_mismatch",
            "critical",
            "fallback_reason_lineage_mismatch",
            "Fallback reasons are attached to rows that do not use Yahoo quote data.",
            "Repair row lineage and rebuild the official-first merge.",
        ),
        (
            "return_quarantine_schema_mismatch",
            "critical",
            "return_quarantine_schema_mismatch",
            "Canonical quote files cannot prove which forward labels were isolated.",
            "Rebuild canonical symbol files with return_quarantined and return_quarantine_reason.",
        ),
        (
            "unapproved_return_quarantine_reason",
            "critical",
            "unapproved_return_quarantine_reason_rows",
            "Unknown quarantine reasons can hide labels outside the approved fail-closed contract.",
            "Use only the canonical listing, lifecycle episode, or unverified-extreme reasons.",
        ),
        (
            "return_quarantine_lineage_mismatch",
            "critical",
            "return_quarantine_lineage_mismatch",
            "The quarantine boolean and its reason disagree.",
            "Rebuild symbol files so a label is quarantined if and only if it has an approved reason.",
        ),
        (
            "return_quarantine_evidence_mismatch",
            "critical",
            "return_quarantine_evidence_mismatch",
            "A forward-label quarantine is missing, spurious, or does not match the destination lifecycle/listing/extreme evidence.",
            "Rebuild canonical symbol files and keep the exact source-row to destination-event quarantine contract.",
        ),
        (
            "return_quarantine_evidence_schema_mismatch",
            "critical",
            "return_quarantine_evidence_schema_mismatch",
            "Canonical quote files lack the episode or listing evidence needed to validate label isolation.",
            "Rebuild canonical symbol files with lifecycle_episode_id and official_listing_evidence.",
        ),
        (
            "unapproved_ohlc_normalization",
            "critical",
            "unapproved_ohlc_normalization_rows",
            "Some official bars use an unknown OHLC normalization.",
            "Retain only the audited official_close_flat_bar normalization.",
        ),
        (
            "invalid_flat_bar_normalization",
            "critical",
            "invalid_flat_bar_normalization_rows",
            "A declared official flat bar does not match its close or uses Yahoo data.",
            "Rebuild the row from the positive official close sentinel and its provenance.",
        ),
        (
            "fallback_raw_scale_schema_mismatch",
            "critical",
            "fallback_raw_scale_schema_mismatch",
            "A mixed-source quote file cannot prove how Yahoo raw OHLC was mapped into the official per-share scale.",
            "Rebuild with schema 7 so every Yahoo fallback row carries a causal scale factor and reference date.",
        ),
        (
            "invalid_fallback_raw_scale",
            "critical",
            "invalid_fallback_raw_scale_rows",
            "A Yahoo fallback quote lacks a positive causal official-overlap scale or points to a future reference date.",
            "Exclude the row until a same-episode official/Yahoo overlap has already established the raw-price scale.",
        ),
        (
            "orphan_fallback_raw_scale",
            "critical",
            "orphan_raw_scale_rows",
            "Yahoo raw-price scale metadata is attached to a non-fallback quote.",
            "Rebuild row-level lineage so scale metadata exists only on Yahoo fallback OHLC rows.",
        ),
        (
            "unresolved_yahoo_raw_price_scale_jump",
            "critical",
            "raw_close_jumps_touching_yahoo",
            "A >2x raw-price transition touching Yahoo fallback can create impossible exact-share cash returns and dominate NAV.",
            "Re-anchor from an already-observed official/Yahoo overlap or exclude the fallback mark; never clip the downstream return.",
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
            "critical",
            "off_calendar_rows",
            "Canonical symbol files contain rows outside the receipt-verified official market calendar.",
            "Remove off-calendar fallback rows and rebuild every dependent feature and panel artifact.",
        ),
        (
            "extreme_adjusted_index_jump",
            "medium",
            "unresolved_adjusted_index_jumps_gt_3x",
            "An unresolved corporate action or scale error can dominate forward labels.",
            "Verify the official reference price or explicitly quarantine the affected forward label.",
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
    quarantined_extremes = int(
        totals["quarantined_adjusted_index_jumps_gt_3x"]
    )
    if quarantined_extremes:
        findings.append(
            Finding(
                "low",
                "quarantined_extreme_adjusted_index_jump",
                "quote_sources",
                "quarantined_adjusted_index_jumps_gt_3x",
                f"count={quarantined_extremes}",
                "The source quote jump remains visible, but its forward label is explicitly isolated.",
                "Retain the provenance and replace the quarantine only if an official reference resolves the transition.",
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
    source_paths = _official_symbol_source_paths(public_dir)
    lifecycle_paths = [
        public_dir / name for name in LIFECYCLE_EVIDENCE_FILENAMES
    ]
    expected_lifecycle_receipts = (
        [_file_content_receipt(source) for source in lifecycle_paths]
        if all(source.is_file() for source in lifecycle_paths)
        else []
    )

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
    fallback_sources: list[Path] = []
    if fallback_converter_receipts_valid:
        for receipt in fallback_receipts:
            source = resolve_receipt_path(receipt)
            if source is None:
                fallback_converter_receipts_valid = False
                break
            fallback_sources.append(source)
            try:
                _validate_yahoo_fallback_archive(source)
                converter_ok = True
            except Exception:
                converter_ok = False
            if not converter_ok:
                fallback_converter_receipts_valid = False
                break
    calendar_valid = False
    calendar_receipt: dict[str, str | int] = {}
    calendar_summary_receipt: dict[str, str | int] = {}
    calendar_rows = -1
    actual_dropped_off_calendar_fallback_rows = -1
    try:
        (
            session_calendar,
            calendar_receipt,
            calendar_summary_receipt,
        ) = _load_verified_taiex_session_calendar(public_dir)
        calendar_rows = int(session_calendar.height)
        actual_dropped_off_calendar_fallback_rows = 0
        for source in fallback_sources:
            actual_dropped_off_calendar_fallback_rows += int(
                pl.scan_parquet(source)
                .select(_date_column_expr("date").alias("date"))
                .join(session_calendar.lazy(), on="date", how="anti")
                .select(pl.len())
                .collect()
                .item()
                or 0
            )
        calendar_valid = True
    except Exception:
        calendar_valid = False
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
    input_fallback_rows = int(summary.get("input_fallback_rows", -1))
    fallback_adjustment_rows = int(summary.get("fallback_adjustment_rows", -1))
    fallback_symbols = int(summary.get("fallback_symbols", -1))
    official_unusable_rows = int(summary.get("official_unusable_ohlcv_rows", -1))
    fallback_replaced_unusable_rows = int(
        summary.get("fallback_replaced_unusable_official_rows", -1)
    )
    unfilled_unusable_rows = int(
        summary.get("unfilled_unusable_official_rows", -1)
    )
    normalized_zero_rows = int(summary.get("normalized_zero_ohlc_rows", -1))
    normalized_fallback_rows = int(
        summary.get("normalized_fallback_ohlc_rows", -1)
    )
    dropped_unanchored_fallback_rows = int(
        summary.get("dropped_unanchored_fallback_rows", -1)
    )
    dropped_discontinuous_fallback_rows = int(
        summary.get("dropped_discontinuous_fallback_rows", -1)
    )
    actual_fallback_reason_rows = 0
    actual_normalized_zero_rows = 0
    actual_normalized_fallback_rows = 0
    actual_return_quarantined_rows = 0
    actual_lifecycle_episode_quarantined_rows = 0
    actual_listing_boundary_quarantined_rows = 0
    actual_unverified_extreme_quarantined_rows = 0
    lineage_columns_valid = True
    for quote_path in parquet_root.glob("*_features.parquet"):
        try:
            quote_schema = set(pq.read_schema(quote_path).names)
            required_lineage_columns = {
                "fallback_reason",
                "ohlc_normalization",
                "raw_ohlc_scale_factor",
                "raw_ohlc_scale_reference_date",
                "return_quarantined",
                "return_quarantine_reason",
            }
            if required_lineage_columns - quote_schema:
                lineage_columns_valid = False
                continue
            lineage = pl.read_parquet(
                quote_path,
                columns=sorted(required_lineage_columns),
            )
            actual_fallback_reason_rows += int(
                lineage.select(
                    (pl.col("fallback_reason") == "official_ohlcv_unusable").sum()
                ).item()
                or 0
            )
            actual_normalized_zero_rows += int(
                lineage.select(
                    (pl.col("ohlc_normalization") == "official_close_flat_bar").sum()
                ).item()
                or 0
            )
            actual_normalized_fallback_rows += int(
                lineage.select(
                    (
                        pl.col("ohlc_normalization")
                        == YAHOO_FALLBACK_RAW_OHLC_NORMALIZATION
                    ).sum()
                ).item()
                or 0
            )
            quarantined = pl.col("return_quarantined").cast(
                pl.Boolean, strict=False
            ).fill_null(False)
            actual_return_quarantined_rows += int(
                lineage.select(quarantined.sum()).item() or 0
            )
            actual_lifecycle_episode_quarantined_rows += int(
                lineage.select(
                    (
                        quarantined
                        & (
                            pl.col("return_quarantine_reason")
                            == "official_lifecycle_episode_boundary"
                        )
                    ).sum()
                ).item()
                or 0
            )
            actual_listing_boundary_quarantined_rows += int(
                lineage.select(
                    (
                        quarantined
                        & (
                            pl.col("return_quarantine_reason")
                            == "official_listing_boundary"
                        )
                    ).sum()
                ).item()
                or 0
            )
            actual_unverified_extreme_quarantined_rows += int(
                lineage.select(
                    (
                        quarantined
                        & (
                            pl.col("return_quarantine_reason")
                            == "unverified_extreme_adjusted_return"
                        )
                    ).sum()
                ).item()
                or 0
            )
        except Exception:
            lineage_columns_valid = False
    uses_fallback = fallback_rows > 0 or fallback_adjustment_rows > 0
    expected_source = MIXED_QUOTE_SOURCE if uses_fallback else OFFICIAL_QUOTE_SOURCE
    checks: dict[str, bool] = {
        "summary_schema": int(summary.get("schema_version", -1))
        == OFFICIAL_SYMBOL_BUILD_SCHEMA_VERSION,
        "source": summary.get("source") == expected_source,
        "adjusted_method": summary.get("adjusted_price_method")
        == OFFICIAL_SYMBOL_ADJUSTED_PRICE_METHOD,
        "all_adjustments_resolved": int(summary.get("missing_adjustment_rows", -1)) == 0,
        "source_receipts": all(source.exists() for source in source_paths)
        and summary.get("source_receipts") == [_file_content_receipt(source) for source in source_paths],
        "lifecycle_source_receipts": summary.get("lifecycle_source_receipts")
        == expected_lifecycle_receipts,
        "session_calendar_receipts": (
            calendar_valid
            and summary.get("session_calendar_receipt") == calendar_receipt
            and summary.get("session_calendar_summary_receipt")
            == calendar_summary_receipt
            and int(summary.get("session_calendar_rows", -1)) == calendar_rows
        ),
        "off_calendar_fallback_reconciliation": (
            calendar_valid
            and actual_dropped_off_calendar_fallback_rows >= 0
            and int(summary.get("dropped_off_calendar_fallback_rows", -1))
            == actual_dropped_off_calendar_fallback_rows
        ),
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
            and input_fallback_rows >= 0
            and fallback_adjustment_rows >= 0
            and fallback_symbols >= 0
            and dropped_unanchored_fallback_rows >= 0
            and dropped_discontinuous_fallback_rows >= 0
            and input_fallback_rows
            == fallback_rows
            + dropped_unanchored_fallback_rows
            + dropped_discontinuous_fallback_rows
            and normalized_fallback_rows == fallback_rows
            and actual_normalized_fallback_rows == fallback_rows
            and fallback_symbols == manifest_fallback_symbols
            and uses_fallback == (fallback_symbols > 0)
            and (bool(fallback_receipts) or not uses_fallback)
        ),
        "unusable_official_reconciliation": (
            official_unusable_rows >= 0
            and fallback_replaced_unusable_rows >= 0
            and unfilled_unusable_rows >= 0
            and official_unusable_rows
            == fallback_replaced_unusable_rows + unfilled_unusable_rows
            and fallback_replaced_unusable_rows <= input_fallback_rows
            and fallback_replaced_unusable_rows
            >= actual_fallback_reason_rows
            and fallback_replaced_unusable_rows - actual_fallback_reason_rows
            <= dropped_unanchored_fallback_rows
            + dropped_discontinuous_fallback_rows
            and normalized_zero_rows == actual_normalized_zero_rows
            and lineage_columns_valid
        ),
        "quarantine_count_reconciliation": (
            lineage_columns_valid
            and actual_return_quarantined_rows
            == actual_lifecycle_episode_quarantined_rows
            + actual_listing_boundary_quarantined_rows
            + actual_unverified_extreme_quarantined_rows
            and int(summary.get("return_quarantined_rows", -1))
            == actual_return_quarantined_rows
            and int(summary.get("lifecycle_episode_quarantined_rows", -1))
            == actual_lifecycle_episode_quarantined_rows
            and int(summary.get("listing_boundary_quarantined_rows", -1))
            == actual_listing_boundary_quarantined_rows
            and int(summary.get("unverified_extreme_quarantined_rows", -1))
            == actual_unverified_extreme_quarantined_rows
        ),
        "stock_etf_manifest": manifest_valid,
        "symbol_count": int(summary.get("symbols", -1))
        == len(list(parquet_root.glob("*_features.parquet"))),
    }
    result.update(
        {
            "valid": all(checks.values()),
            "checks": checks,
            "official_unusable_ohlcv_rows": official_unusable_rows,
            "fallback_replaced_unusable_official_rows": (
                fallback_replaced_unusable_rows
            ),
            "unfilled_unusable_official_rows": unfilled_unusable_rows,
            "return_quarantined_rows": actual_return_quarantined_rows,
            "lifecycle_episode_quarantined_rows": (
                actual_lifecycle_episode_quarantined_rows
            ),
            "listing_boundary_quarantined_rows": (
                actual_listing_boundary_quarantined_rows
            ),
            "unverified_extreme_quarantined_rows": (
                actual_unverified_extreme_quarantined_rows
            ),
            "session_calendar_rows": calendar_rows,
            "dropped_off_calendar_fallback_rows": int(
                summary.get("dropped_off_calendar_fallback_rows", -1)
            ),
        }
    )
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
        split_start_year=config.walk_forward.split_start_year,
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


def _authoritative_source_unavailable_dates(
    public_dir: Path,
    dataset: str,
    expected_sessions: np.ndarray,
    observed_dates: np.ndarray,
) -> np.ndarray:
    """Return provider-receipted no-data sessions, never fabricated rows.

    A historical endpoint can have an immutable archive gap even though the
    exchange was open.  Such dates resolve downloader completeness but must
    remain absent from the source parquet so feature construction continues to
    zero-fill them.  Only a completed strict-calendar state receipt may resolve
    audit coverage.
    """

    if expected_sessions.size == 0:
        return np.asarray([], dtype="datetime64[D]")
    state = _read_json_object(public_dir / "state" / f"{dataset}.json")
    if state is None:
        return np.asarray([], dtype="datetime64[D]")
    calendar_kind = str(state.get("coverage_calendar_kind", "") or "").lower()
    required_checks = (
        state.get("dataset") == dataset,
        state.get("baseline_established") is True,
        state.get("coverage_complete") is True,
        state.get("replacement_promoted") is True,
        not bool(state.get("failed_dates")),
        int(state.get("missing_dates_after", -1)) == 0,
        "official" in calendar_kind and "session" in calendar_kind,
    )
    if not all(required_checks):
        return np.asarray([], dtype="datetime64[D]")

    try:
        coverage_start = np.datetime64(str(state["coverage_start"]), "D")
        coverage_end = np.datetime64(str(state["coverage_end"]), "D")
    except (KeyError, TypeError, ValueError):
        return np.asarray([], dtype="datetime64[D]")
    if coverage_start > expected_sessions[0] or coverage_end < expected_sessions[-1]:
        return np.asarray([], dtype="datetime64[D]")

    if (
        state.get("coverage_calendar_source") != TPEX_OFFICIAL_CALENDAR_DATASET
        or state.get("root_coverage_calendar_source")
        != TAIEX_SESSION_CALENDAR_DATASET
    ):
        return np.asarray([], dtype="datetime64[D]")

    expected_days = {
        date.fromisoformat(str(value))
        for value in expected_sessions.astype("datetime64[D]")
    }
    try:
        taiex_days, taiex_sha256 = _validated_taiex_session_dates(
            public_dir,
            min(expected_days),
            max(expected_days),
        )
    except (OSError, RuntimeError, TypeError, ValueError):
        return np.asarray([], dtype="datetime64[D]")
    if (
        taiex_days != expected_days
        or state.get("root_coverage_calendar_sha256") != taiex_sha256
    ):
        return np.asarray([], dtype="datetime64[D]")

    spec = DEFAULT_DATASETS.get(dataset)
    if spec is None:
        return np.asarray([], dtype="datetime64[D]")
    validated_receipt_days = _validated_source_unavailable_receipt_dates(
        public_dir,
        spec,
        expected_days,
    )

    raw_unavailable = state.get("confirmed_source_unavailable_dates")
    raw_empty = state.get("confirmed_empty_dates")
    accounting = state.get("confirmed_empty_date_accounting")
    if not isinstance(raw_unavailable, list) or not isinstance(raw_empty, list):
        return np.asarray([], dtype="datetime64[D]")
    if not isinstance(accounting, dict):
        return np.asarray([], dtype="datetime64[D]")
    try:
        unavailable_days = {
            date.fromisoformat(str(np.datetime64(str(value), "D")))
            for value in raw_unavailable
        }
        confirmed_empty = {
            date.fromisoformat(str(np.datetime64(str(value), "D")))
            for value in raw_empty
        }
        accounted = int(accounting.get("source_unavailable", -1))
    except (TypeError, ValueError):
        return np.asarray([], dtype="datetime64[D]")
    if accounted != len(unavailable_days):
        return np.asarray([], dtype="datetime64[D]")
    if not unavailable_days.issubset(confirmed_empty):
        return np.asarray([], dtype="datetime64[D]")

    # The state is only accounting metadata.  A date is resolved exclusively
    # when the append-only journal and immutable raw body independently pass
    # the downloader's URL/HTTP/size/hash/explicit-no-data validator.
    unavailable = np.asarray(
        sorted(validated_receipt_days & unavailable_days),
        dtype="datetime64[D]",
    )
    return np.setdiff1d(unavailable, observed_dates, assume_unique=False)


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
                    observed_sessions=0,
                    observed_session_coverage=None,
                    source_unavailable_sessions=0,
                    resolved_sessions=0,
                    resolved_session_coverage=None,
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
        observed = np.intersect1d(expected, source_dates, assume_unique=False)
        observed_coverage = (
            float(observed.size / expected.size) if expected.size else None
        )
        source_unavailable = _authoritative_source_unavailable_dates(
            public_dir,
            expectation.name,
            expected,
            observed,
        )
        resolved = np.union1d(observed, source_unavailable)
        resolved_coverage = (
            float(resolved.size / expected.size) if expected.size else None
        )
        rows = int(pq.ParquetFile(path).metadata.num_rows)
        if resolved_coverage is None:
            status = "no_expected_sessions"
        elif resolved_coverage >= 0.98:
            status = "complete"
        elif resolved_coverage >= 0.80:
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
                # Legacy fields retain observed-row semantics.  New explicit
                # fields separate actual rows from receipt-resolved sessions.
                covered_sessions=int(observed.size),
                session_coverage=observed_coverage,
                observed_sessions=int(observed.size),
                observed_session_coverage=observed_coverage,
                source_unavailable_sessions=int(source_unavailable.size),
                resolved_sessions=int(resolved.size),
                resolved_session_coverage=resolved_coverage,
                selected_features=",".join(selected_for_source),
                quarantined=quarantined,
            )
        )
        if selected_for_source and (
            resolved_coverage is None or resolved_coverage < 0.98
        ):
            severity = (
                "medium"
                if quarantined
                else (
                    "high"
                    if resolved_coverage and resolved_coverage >= 0.80
                    else "critical"
                )
            )
            findings.append(
                Finding(
                    severity=severity,
                    code="historical_session_coverage",
                    component="source",
                    item=expectation.name,
                    evidence=(
                        f"observed={observed.size}/{expected.size} "
                        f"({0.0 if observed_coverage is None else observed_coverage:.2%}), "
                        f"source_unavailable={source_unavailable.size}, "
                        f"resolved={resolved.size}/{expected.size} "
                        f"({0.0 if resolved_coverage is None else resolved_coverage:.2%}), "
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
        if feature.startswith("twpub_")
        and not _is_zero_filled(feature, config)
        and not _matches(feature, registered_patterns)
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


def audit_feature_availability_contract(
    config: ExperimentConfig,
) -> tuple[dict[str, Any], list[Finding]]:
    """Prove every live feature is usable at its configured decision time."""

    selected = _selected_features(config)
    selected_set = set(selected)
    zero_filled = {
        feature for feature in selected if _is_zero_filled(feature, config)
    }
    active = selected_set - zero_filled
    pre_close = active & set(PRE_CLOSE_FEATURE_COLUMNS)
    close_completion = active & set(CLOSE_COMPLETION_FEATURE_COLUMNS)
    post_close = active & set(POST_CLOSE_CHIP_FEATURE_COLUMNS)
    classified = pre_close | close_completion | post_close
    unclassified = active - classified
    overlaps = sorted(
        (pre_close & close_completion)
        | (pre_close & post_close)
        | (close_completion & post_close)
    )
    configured_panel_shift = {
        feature
        for feature in selected
        if _matches(feature, config.data.feature_shift_next_session)
    }
    missing_close_shift = close_completion - configured_panel_shift
    unexpected_panel_shift = configured_panel_shift - close_completion
    execution_mode = str(config.trading.execution_mode).strip().lower()
    allow_same_close_approximation = bool(
        config.data.allow_same_close_feature_approximation
    )

    summary = {
        "execution_mode": execution_mode,
        "decision_time_contract": "regular_close",
        "allow_same_close_feature_approximation": (
            allow_same_close_approximation
        ),
        "selected_features": len(selected),
        "zero_filled_features": sorted(zero_filled),
        "active_features": len(active),
        "pre_close_active_features": sorted(pre_close),
        "close_completion_active_features": sorted(close_completion),
        "post_close_active_features": sorted(post_close),
        "post_close_selected_features": sorted(
            selected_set & set(POST_CLOSE_CHIP_FEATURE_COLUMNS)
        ),
        "configured_panel_shift_features": sorted(configured_panel_shift),
        "unclassified_active_features": sorted(unclassified),
        "classification_overlaps": overlaps,
        "missing_close_completion_shifts": sorted(missing_close_shift),
        "unexpected_panel_shifts": sorted(unexpected_panel_shift),
    }
    findings: list[Finding] = []
    if unclassified:
        findings.append(
            Finding(
                "critical",
                "unclassified_feature_availability",
                "feature_availability",
                "active_model_features",
                f"unclassified={sorted(unclassified)}",
                "A live feature has no machine-checked earliest-use timestamp.",
                "Classify its publication/observation time before training.",
            )
        )
    if overlaps:
        findings.append(
            Finding(
                "critical",
                "overlapping_feature_availability",
                "feature_availability",
                "active_model_features",
                f"overlaps={overlaps}",
                "Conflicting availability classes make the feature lag ambiguous.",
                "Assign exactly one availability class to each active feature.",
            )
        )
    if execution_mode == "naive":
        if missing_close_shift:
            findings.append(
                Finding(
                    "medium" if allow_same_close_approximation else "high",
                    (
                        "same_close_feature_approximation"
                        if allow_same_close_approximation
                        else "same_close_feature_leakage"
                    ),
                    "feature_availability",
                    "regular_close_inputs",
                    f"missing_next_session_shift={sorted(missing_close_shift)}",
                    (
                        "Final session values are deliberately used with the same final close as an approximate execution price; "
                        "the research backtest is not a realizable closing-auction timing simulation."
                        if allow_same_close_approximation
                        else "Final session values would be used to establish a position at that same regular close."
                    ),
                    (
                        "Replace the approximation with an explicit order cutoff and executable price model; "
                        "until then keep the opt-in and caveat in every result."
                        if allow_same_close_approximation
                        else "Add every final-session aggregate to data.feature_shift_next_session and rebuild the panel."
                    ),
                )
            )
        if unexpected_panel_shift:
            findings.append(
                Finding(
                    "high",
                    "unexpected_feature_availability_shift",
                    "feature_availability",
                    "regular_close_inputs",
                    f"unexpected_next_session_shift={sorted(unexpected_panel_shift)}",
                    "A feature is delayed without a registered availability reason.",
                    "Remove the shift or register the feature in the correct availability class.",
                )
            )
    elif configured_panel_shift or post_close or allow_same_close_approximation:
        findings.append(
            Finding(
                "high",
                "availability_shift_execution_conflict",
                "feature_availability",
                "execution_mode",
                (
                    f"execution_mode={execution_mode}, panel_shifted={len(configured_panel_shift)}, "
                    f"source_shifted_post_close={len(post_close)}, "
                    f"same_close_approximation={allow_same_close_approximation}"
                ),
                "Taiwan execution modes already lag the model window by one completed session, so availability-shifted inputs would be delayed twice.",
                "Use the regular-close naive contract or redesign the dataset to consume per-feature availability timestamps exactly once.",
            )
        )
    return summary, findings


def _read_json_object(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _audit_tpex_daily_name_provenance(
    public_dir: Path,
) -> tuple[dict[str, Any], list[Finding]]:
    """Reconcile receipt-level lossy bytes with row-level name provenance."""

    path = public_dir / "tpex_daily_ohlcv.parquet"
    summary: dict[str, Any] = {
        "path": str(path),
        "present": path.is_file(),
    }
    if not path.is_file():
        # Missing selected sources are handled by audit_historical_sources.
        return summary, []

    expected_status = "official_receipt_name_bytes_unrecoverable"
    schema = set(pq.read_schema(path).names)
    if not {"date", "代號", "名稱", "_name_decode_status"} <= schema:
        summary["schema_complete"] = False
        return summary, [
            Finding(
                "critical",
                "tpex_name_provenance_mismatch",
                "source_receipt",
                str(path),
                "missing date, 代號, 名稱, or _name_decode_status column",
                "Lossy official name bytes can enter issuer identity matching without provenance.",
                "Reparse TPEx daily OHLCV with parser contract v12 from verified raw receipts.",
            )
        ]

    status = (
        pl.col("_name_decode_status")
        .cast(pl.String, strict=False)
        .fill_null("")
        .str.strip_chars()
    )
    name = pl.col("名稱").cast(pl.String, strict=False).fill_null("")
    per_date = (
        pl.scan_parquet(path)
        .select(
            pl.col("date").cast(pl.String, strict=False).str.slice(0, 10).alias("date"),
            pl.col("代號")
            .cast(pl.String, strict=False)
            .fill_null("")
            .str.strip_chars()
            .str.to_uppercase()
            .alias("symbol"),
            status.alias("status"),
            name.alias("name"),
        )
        .group_by("date")
        .agg(
            pl.len().alias("rows"),
            (pl.col("status") == expected_status).sum().alias("unrecoverable_rows"),
            (
                (pl.col("status") != "")
                & (pl.col("status") != expected_status)
            )
            .sum()
            .alias("unknown_status_rows"),
            (
                (pl.col("status") == "")
                & pl.col("name").str.contains(r"(?:\ufffd|嚙|ï¿½)")
            )
            .sum()
            .alias("unmarked_rendered_damage_rows"),
        )
        .collect()
    )
    rows_by_date = {
        str(row["date"]): (
            int(row["rows"]),
            int(row["unrecoverable_rows"] or 0),
        )
        for row in per_date.iter_rows(named=True)
    }
    status_dates = {
        value
        for value, (_, marked) in rows_by_date.items()
        if marked > 0
    }
    unknown_status_rows = int(per_date.get_column("unknown_status_rows").sum() or 0)
    unmarked_rendered_damage_rows = int(
        per_date.get_column("unmarked_rendered_damage_rows").sum() or 0
    )
    actual_status_keys = {
        (str(row["date"]), str(row["symbol"]))
        for row in (
            pl.scan_parquet(path)
            .select(
                pl.col("date")
                .cast(pl.String, strict=False)
                .str.slice(0, 10)
                .alias("date"),
                pl.col("代號")
                .cast(pl.String, strict=False)
                .fill_null("")
                .str.strip_chars()
                .str.to_uppercase()
                .alias("symbol"),
                status.alias("status"),
            )
            .filter(pl.col("status") == expected_status)
            .collect()
            .iter_rows(named=True)
        )
    }

    raw_dir = public_dir / "raw" / "tpex_daily_ohlcv"
    # The pre-2007 archive is a CP950 byte stream. Any undecodable byte or
    # embedded UTF-8 replacement marker makes the entire receipt's name column
    # ambiguous, because CP950 can re-pair those bytes into plausible CJK text.
    # The 2007 legacy endpoint instead wraps already-decoded HTML in UTF-8 JSON;
    # its upstream U+FFFD damage is therefore attributable to exact symbol rows.
    lossy_receipt_dates: set[str] = set()
    row_damage_evidence_keys: set[tuple[str, str]] = set()
    unreadable_raw_files: list[str] = []
    for raw_path in sorted(raw_dir.glob("*.html")):
        try:
            receipt_date = date.fromisoformat(raw_path.stem[:10]).isoformat()
            raw = raw_path.read_bytes()
        except (OSError, ValueError):
            unreadable_raw_files.append(str(raw_path))
            continue
        lossy = b"\xef\xbf\xbd" in raw
        if not lossy:
            try:
                raw.decode("cp950")
            except UnicodeDecodeError:
                lossy = True
        if lossy:
            lossy_receipt_dates.add(receipt_date)

    json_damage_markers = (
        b"\xef\xbf\xbd",
        "ï¿½".encode("utf-8"),
        "嚙".encode("utf-8"),
    )
    for raw_path in sorted(raw_dir.glob("*.json")):
        try:
            receipt_date = date.fromisoformat(raw_path.stem[:10]).isoformat()
            raw = raw_path.read_bytes()
        except (OSError, ValueError):
            unreadable_raw_files.append(str(raw_path))
            continue
        lowered_raw = raw.lower()
        if not (
            any(marker in raw for marker in json_damage_markers)
            or b"\\ufffd" in lowered_raw
            or b"\\u5699" in lowered_raw
            or b"\\u00ef\\u00bf\\u00bd" in lowered_raw
        ):
            continue
        try:
            payload = json.loads(raw.decode("utf-8-sig"))
            raw_html = payload.get("html") if isinstance(payload, dict) else None
            if not isinstance(raw_html, str):
                raise ValueError("JSON receipt has no string html field")
            raw_rows = _parse_tpex_daily_quotes_html(
                raw_html,
                date.fromisoformat(receipt_date),
            )
            if raw_rows.is_empty():
                raise ValueError("damaged JSON receipt produced no quote rows")
            damaged_rows = raw_rows.filter(
                pl.col("_name_decode_status") == expected_status
            )
            row_damage_evidence_keys.update(
                (receipt_date, str(symbol).strip().upper())
                for symbol in damaged_rows.get_column("代號").to_list()
            )
        except (
            HistoricalResponseError,
            json.JSONDecodeError,
            UnicodeDecodeError,
            OSError,
            ValueError,
            TypeError,
        ):
            unreadable_raw_files.append(str(raw_path))

    missing_or_partial_status_dates = sorted(
        value
        for value in lossy_receipt_dates
        if value not in rows_by_date
        or rows_by_date[value][1] != rows_by_date[value][0]
    )
    missing_row_damage_status_keys = sorted(
        row_damage_evidence_keys - actual_status_keys
    )
    status_without_raw_evidence_keys = sorted(
        key
        for key in actual_status_keys - row_damage_evidence_keys
        if key[0] not in lossy_receipt_dates
    )
    status_without_raw_evidence_dates = sorted(
        {value[0] for value in status_without_raw_evidence_keys}
    )
    checks = {
        "schema_complete": True,
        "raw_receipts_present": raw_dir.is_dir(),
        "raw_receipts_readable": not unreadable_raw_files,
        "known_status_only": unknown_status_rows == 0,
        "rendered_damage_marked": unmarked_rendered_damage_rows == 0,
        "lossy_receipts_fully_marked": not missing_or_partial_status_dates,
        "raw_row_damage_marked": not missing_row_damage_status_keys,
        "status_has_raw_evidence": not status_without_raw_evidence_keys,
    }
    summary.update(
        {
            "checks": checks,
            "lossy_receipt_dates": len(lossy_receipt_dates),
            "row_damage_evidence_dates": len(
                {value[0] for value in row_damage_evidence_keys}
            ),
            "row_damage_evidence_rows": len(row_damage_evidence_keys),
            "status_dates": len(status_dates),
            "unrecoverable_name_rows": sum(
                marked for _, marked in rows_by_date.values()
            ),
            "unknown_status_rows": unknown_status_rows,
            "unmarked_rendered_damage_rows": unmarked_rendered_damage_rows,
            "missing_or_partial_status_dates": missing_or_partial_status_dates[:20],
            "missing_row_damage_status_keys": [
                {"date": value[0], "symbol": value[1]}
                for value in missing_row_damage_status_keys[:20]
            ],
            "status_without_raw_evidence_dates": status_without_raw_evidence_dates[:20],
            "status_without_raw_evidence_keys": [
                {"date": value[0], "symbol": value[1]}
                for value in status_without_raw_evidence_keys[:20]
            ],
            "unreadable_raw_files": unreadable_raw_files[:20],
        }
    )
    failed_checks = [name for name, passed in checks.items() if not passed]
    if not failed_checks:
        return summary, []
    return summary, [
        Finding(
            "critical",
            "tpex_name_provenance_mismatch",
            "source_receipt",
            str(path),
            (
                f"failed_checks={failed_checks}, "
                f"lossy_receipt_dates={len(lossy_receipt_dates)}, "
                f"status_dates={len(status_dates)}, "
                f"unrecoverable_rows={summary['unrecoverable_name_rows']}"
            ),
            "Lossy official name bytes can enter issuer identity matching or appear falsely recoverable.",
            "Reparse TPEx daily OHLCV with parser contract v12 from receipt-verified raw bytes.",
        )
    ]


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
        "public_download_allowed_failed_count": (
            public_receipt.get("allowed_failed_count") if public_receipt else None
        ),
        "public_download_blocking_failed_count": (
            public_receipt.get("blocking_failed_count") if public_receipt else None
        ),
        "public_download_allowed_failed_datasets": (
            public_receipt.get("allowed_failed_datasets") if public_receipt else None
        ),
        "public_download_configured_allowed_failed_datasets": (
            public_receipt.get("configured_allowed_failed_datasets")
            if public_receipt
            else None
        ),
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
        allowed_failed_count = int(public_receipt.get("allowed_failed_count", -1))
        blocking_failed_count = int(
            public_receipt.get("blocking_failed_count", -1)
        )
        allowed_failed_datasets = public_receipt.get("allowed_failed_datasets")
        configured_allowed_failed_datasets = public_receipt.get(
            "configured_allowed_failed_datasets"
        )
        declared_nonblocking_failures = (
            public_receipt.get("coverage_complete") is True
            and failed_count >= 0
            and failed_count == allowed_failed_count
            and blocking_failed_count == 0
            and isinstance(allowed_failed_datasets, list)
            and isinstance(configured_allowed_failed_datasets, list)
            and len(allowed_failed_datasets) == allowed_failed_count
            and set(map(str, allowed_failed_datasets))
            <= set(map(str, configured_allowed_failed_datasets))
        )
        receipt_summary["public_download_nonblocking_failures_valid"] = (
            declared_nonblocking_failures
        )
        if failed_count != 0 and not declared_nonblocking_failures:
            findings.append(
                Finding(
                    "critical",
                    "public_download_failures",
                    "source_receipt",
                    str(public_path),
                    (
                        f"failed_count={failed_count}, "
                        f"allowed_failed_count={allowed_failed_count}, "
                        f"blocking_failed_count={blocking_failed_count}"
                    ),
                    "At least one requested public source failed outside the declared nonblocking allowlist or the receipt is malformed.",
                    "Repair every blocking/undeclared failed dataset before rebuilding features.",
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

    tpex_name_summary, tpex_name_findings = _audit_tpex_daily_name_provenance(
        public_dir
    )
    receipt_summary["tpex_daily_name_provenance"] = tpex_name_summary
    findings.extend(tpex_name_findings)

    taiex_path = public_dir / "twse_taiex_ohlc.parquet"
    taiex_summary_path = taiex_path.with_suffix(".summary.json")
    taiex_receipt = _read_json_object(taiex_summary_path)
    receipt_summary["taiex_archive_summary"] = str(taiex_summary_path)
    taiex_checks: dict[str, bool] = {
        "summary_present": taiex_receipt is not None,
        "parquet_present": taiex_path.is_file(),
    }
    if taiex_receipt is not None and taiex_path.is_file():
        expected_columns = {
            "date",
            "opening_index",
            "highest_index",
            "lowest_index",
            "closing_index",
            "_dataset",
            "_source",
            "_source_product",
            "_request_month",
            "_downloaded_at_utc",
            "_url",
        }
        try:
            schema = set(pq.read_schema(taiex_path).names)
            stats = (
                pl.scan_parquet(taiex_path)
                .select(
                    [
                        pl.len().alias("rows"),
                        pl.col("date").n_unique().alias("distinct_dates"),
                        pl.col("date").min().alias("first_date"),
                        pl.col("date").max().alias("last_date"),
                        (
                            pl.col("date").is_null()
                            | pl.col("opening_index").is_null()
                            | pl.col("highest_index").is_null()
                            | pl.col("lowest_index").is_null()
                            | pl.col("closing_index").is_null()
                            | ~pl.col("opening_index").is_finite()
                            | ~pl.col("highest_index").is_finite()
                            | ~pl.col("lowest_index").is_finite()
                            | ~pl.col("closing_index").is_finite()
                            | (pl.col("opening_index") <= 0)
                            | (pl.col("highest_index") <= 0)
                            | (pl.col("lowest_index") <= 0)
                            | (pl.col("closing_index") <= 0)
                            | (
                                pl.col("highest_index")
                                < pl.max_horizontal(
                                    "opening_index", "lowest_index", "closing_index"
                                )
                            )
                            | (
                                pl.col("lowest_index")
                                > pl.min_horizontal(
                                    "opening_index", "highest_index", "closing_index"
                                )
                            )
                        )
                        .sum()
                        .alias("invalid_rows"),
                    ]
                )
                .collect()
                .row(0, named=True)
            )
            content_receipt = _file_content_receipt(taiex_path)
            declared_receipt = taiex_receipt.get("output_receipt")
            declared_receipt = (
                declared_receipt if isinstance(declared_receipt, dict) else {}
            )
            public_end = (
                str(public_receipt.get("end_date", ""))[:10]
                if public_receipt is not None
                else ""
            )
            taiex_end = str(taiex_receipt.get("effective_end_date", ""))[:10]
            taiex_checks.update(
                {
                    "identity": (
                        taiex_receipt.get("dataset") == "twse_taiex_ohlc"
                        and taiex_receipt.get("source") == "TWSE"
                    ),
                    "official_start": (
                        taiex_receipt.get("official_start_date") == "1999-01-05"
                        and taiex_receipt.get("effective_start_date") == "1999-01-05"
                        and str(stats["first_date"])[:10] == "1999-01-05"
                    ),
                    "coverage_complete": taiex_receipt.get("coverage_complete") is True,
                    "baseline_established": taiex_receipt.get("baseline_established") is True,
                    "replacement_promoted": taiex_receipt.get("replacement_promoted") is True,
                    "no_failures": (
                        int(taiex_receipt.get("failed_count", -1)) == 0
                        and int(taiex_receipt.get("unresolved_month_count", -1)) == 0
                    ),
                    "date_range_matches_public": (
                        bool(taiex_end) and (not public_end or taiex_end >= public_end)
                    ),
                    "schema": expected_columns <= schema,
                    "rows": (
                        int(stats["rows"]) > 0
                        and int(stats["rows"]) == int(stats["distinct_dates"])
                        and int(stats["rows"])
                        == int(taiex_receipt.get("output_rows", -1))
                    ),
                    "ohlc": int(stats["invalid_rows"] or 0) == 0,
                    "output_receipt": (
                        int(declared_receipt.get("size", -1))
                        == int(content_receipt["size"])
                        and str(declared_receipt.get("sha256", ""))
                        == str(content_receipt["sha256"])
                    ),
                }
            )
            receipt_summary["taiex_archive_first_date"] = str(stats["first_date"])[:10]
            receipt_summary["taiex_archive_last_date"] = str(stats["last_date"])[:10]
            receipt_summary["taiex_archive_rows"] = int(stats["rows"])
        except Exception as exc:
            taiex_checks["readable_and_valid"] = False
            receipt_summary["taiex_archive_error"] = f"{type(exc).__name__}: {exc}"
    receipt_summary["taiex_archive_checks"] = taiex_checks
    failed_taiex_checks = [name for name, passed in taiex_checks.items() if not passed]
    if failed_taiex_checks:
        findings.append(
            Finding(
                "critical",
                "incomplete_taiex_archive_receipt",
                "source_receipt",
                str(taiex_summary_path),
                f"failed_checks={failed_taiex_checks}",
                "The official monthly TAIEX session/OHLC archive is missing, incomplete, or does not match its receipt.",
                "Run download_tw_taiex_ohlc.py in rebuild/repair mode through the audit end date.",
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
            unparseable = int(short_receipt.get("unparseable", -1))
            detail_content_unavailable = int(
                short_receipt.get(
                    "detail_content_unavailable",
                    0 if unparseable == 0 else -1,
                )
            )
            unresolved_unparseable = (
                unparseable - detail_content_unavailable
                if unparseable >= 0
                and 0 <= detail_content_unavailable <= unparseable
                else -1
            )
            checks = {
                "requests_complete": short_receipt.get("requests_complete") is True,
                "failure_count_zero": int(short_receipt.get("failure_count", -1)) == 0,
                # Empty legacy detail shells are explicit provider-source
                # unavailability, not parser ambiguity.  Every other row must
                # still resolve to a symbol or an explicit out-of-universe
                # classification.
                "unparseable_accounted": unresolved_unparseable == 0,
                "data_output_written": short_receipt.get("data_output_written") is True,
                "no_duplicate_rows": int(quality.get("composite_key_duplicate_rows", -1)) == 0,
                "all_symbols_parsed": (
                    int(quality.get("rows", 0)) > 0
                    and int(quality.get("symbols_nonempty_rows", -1)) == int(quality.get("rows", 0))
                ),
            }
            receipt_summary["short_rule_checks"] = checks
            receipt_summary["short_rule_unresolved_unparseable"] = (
                unresolved_unparseable
            )
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
    checks["availability_contract"] = (
        int(summary.get("availability_contract_version", -1))
        == TW_PUBLIC_FEATURE_AVAILABILITY_CONTRACT_VERSION
    )
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
        "feature_shift_next_session": config.data.feature_shift_next_session,
        "panel_start_date": config.data.panel_start_date,
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
    availability_summary: dict[str, Any],
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
            "| files | rows | Yahoo OHLCV/adjustment rows | unsupported type | unapproved files | lineage errors | read/schema errors | duplicate dates | invalid bars/volume | invalid adjclose | origin != 10 | off-calendar | raw close >2x | adjusted >3x total/quarantined/unresolved |",
            "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            f"| {quote_summary.get('files', 0)} | {quote_summary.get('rows', 0)} | "
            f"{quote_summary.get('yahoo_fallback_rows', 0)}/{quote_summary.get('yahoo_fallback_adjustment_rows', 0)} | "
            f"{quote_summary.get('unsupported_security_files', 0)} | "
            f"{quote_summary.get('unapproved_source_files', 0)} | "
            f"{quote_summary.get('unapproved_data_source_rows', 0) + quote_summary.get('unapproved_adjustment_source_rows', 0) + quote_summary.get('source_lineage_mismatch', 0) + quote_summary.get('unapproved_return_quarantine_reason_rows', 0) + quote_summary.get('return_quarantine_lineage_mismatch', 0) + quote_summary.get('return_quarantine_schema_mismatch', 0)} | "
            f"{quote_summary.get('read_errors', 0) + quote_summary.get('schema_errors', 0)} | "
            f"{quote_summary.get('duplicate_dates', 0)} | "
            f"{quote_summary.get('invalid_ohlc_geometry', 0) + quote_summary.get('negative_volume', 0)} | "
            f"{quote_summary.get('invalid_adjclose', 0)} | "
            f"{quote_summary.get('first_adjclose_not_ten', 0)} | "
            f"{quote_summary.get('off_calendar_rows', 0)} | "
            f"{quote_summary.get('raw_close_jumps_gt_2x', 0)} | "
            f"{quote_summary.get('adjusted_index_jumps_gt_3x', 0)}/"
            f"{quote_summary.get('quarantined_adjusted_index_jumps_gt_3x', 0)}/"
            f"{quote_summary.get('unresolved_adjusted_index_jumps_gt_3x', 0)} |",
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
            "| source | status | observed | source unavailable | resolved | expected | observed coverage | resolved coverage | range | selected | quarantined |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |",
        ]
    )
    for item in profiles:
        lines.append(
            f"| {item.source} | {item.status} | {item.observed_sessions} | "
            f"{item.source_unavailable_sessions} | {item.resolved_sessions} | "
            f"{item.expected_sessions} | {_fmt(item.observed_session_coverage, percent=True)} | "
            f"{_fmt(item.resolved_session_coverage, percent=True)} | "
            f"{item.first_date}..{item.last_date} | {item.selected_features} | {item.quarantined} |"
        )
    lines.extend(
        [
            "",
            "## Feature Availability",
            "",
            f"Decision time: {availability_summary.get('decision_time_contract')}; "
            f"execution mode: {availability_summary.get('execution_mode')}; "
            f"same-close approximation: {availability_summary.get('allow_same_close_feature_approximation')}; "
            f"selected/active/zero-filled: {availability_summary.get('selected_features', 0)}/"
            f"{availability_summary.get('active_features', 0)}/"
            f"{len(availability_summary.get('zero_filled_features', []))}.",
            "",
            f"- Pre-close usable: {availability_summary.get('pre_close_active_features', [])}",
            f"- Shifted after close completion: {availability_summary.get('close_completion_active_features', [])}",
            f"- Source-shifted post-close: {availability_summary.get('post_close_active_features', [])}",
            f"- Unclassified active: {availability_summary.get('unclassified_active_features', [])}",
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
        public_dir,
        config.walk_forward.expected_first_year,
        config.data.panel_start_date,
    )
    source_calendar, _, _ = _load_verified_taiex_session_calendar(public_dir)
    source_sessions = np.asarray(
        source_calendar["date"].to_numpy(), dtype="datetime64[D]"
    )
    quote_profiles, quote_summary, quote_findings = audit_quote_source_files(
        parquet_root,
        sessions,
        workers=max(1, int(config.data.panel_load_workers)),
        require_official=True,
        source_sessions=source_sessions,
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
    availability_summary, availability_findings = (
        audit_feature_availability_contract(config)
    )
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
    findings.extend(availability_findings)

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
        "feature_availability": availability_summary,
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
        availability_summary=availability_summary,
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

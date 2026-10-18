"""Rebuild the historical research feature union without changing live tables.

The wide table owns historical macro/XBRL facts and the official table owns
additional columns absent from it.  Both inputs have one row per date/symbol;
the official table must be a key subset of the wide table.  The fifteen legacy
macro transforms are rebuilt from the wide raw observations at their own dates.
"""

from __future__ import annotations

from contextlib import ExitStack
from datetime import date
import hashlib
import json
import os
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from stockagent.data.tw_public_research_taifex import MARKET_SYMBOL


CONTRACT_VERSION = 2
BUILD_STRATEGY = "bounded_annual_range_join_global_market_transforms_v2"
DERIVED_SOURCES = {
    "twpub_dgbas_gdp_log": "twpub_dgbas_gdp_raw",
    "twpub_dgbas_cpi_log": "twpub_dgbas_cpi_raw",
    "twpub_usdtwd_log": "twpub_usdtwd_raw",
    "twpub_usdtwd_logret_1d": "twpub_usdtwd_raw",
    "twpub_cbc_overnight_rate": "twpub_cbc_overnight_pct_raw",
    "twpub_cbc_overnight_rate_chg": "twpub_cbc_overnight_pct_raw",
    "twpub_cbc_m1b_log": "twpub_cbc_m1b_raw",
    "twpub_cbc_m2_log": "twpub_cbc_m2_raw",
    "twpub_mof_business_tax_log": "twpub_mof_business_tax_raw",
    "twpub_mof_export_log": "twpub_mof_export_raw",
    "twpub_mof_futures_tax_log": "twpub_mof_futures_tax_raw",
    "twpub_mof_import_log": "twpub_mof_import_raw",
    "twpub_mof_securities_tax_log": "twpub_mof_securities_tax_raw",
    "twpub_mof_tax_total_log": "twpub_mof_tax_total_raw",
    "twpub_mof_trade_balance_asinh": "twpub_mof_trade_balance_raw",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _positive_log(raw: str) -> pl.Expr:
    x = pl.col(raw).cast(pl.Float64)
    return pl.when(x.is_finite() & (x > 0)).then(x.log()).otherwise(None)


def _positive_log1p(raw: str) -> pl.Expr:
    x = pl.col(raw).cast(pl.Float64)
    return pl.when(x.is_finite() & (x > 0)).then((x + 1).log()).otherwise(None)


def _observed_change(market: pl.DataFrame, raw: str, *, log_ratio: bool) -> pl.DataFrame:
    observations = market.select("date", raw).filter(pl.col(raw).is_finite()).sort("date")
    current = pl.col(raw)
    prior = current.shift(1)
    expression = (
        pl.when((current > 0) & (prior > 0)).then((current / prior).log()).otherwise(None)
        if log_ratio else current - prior
    )
    return observations.with_columns(expression.alias("value")).select("date", "value")


def _derived_market_values(base: pl.LazyFrame) -> pl.DataFrame:
    raw_names = sorted(set(DERIVED_SOURCES.values()))
    market = (
        base.filter(pl.col("symbol") == MARKET_SYMBOL)
        .select("date", *raw_names)
        .sort("date")
        .collect()
    )
    if market.is_empty() or market.get_column("date").n_unique() != market.height:
        raise ValueError("wide market rows are empty or duplicate by date")
    raw = pl.col("twpub_cbc_overnight_pct_raw").cast(pl.Float64)
    # The historical event archive already stores a fraction (0.00386 means
    # 0.386%).  The legacy name ends in pct_raw but dividing by 100 again would
    # make the rate wrong by two orders of magnitude.
    balance = pl.col("twpub_mof_trade_balance_raw").cast(pl.Float64)
    derived = market.select(
        "date",
        _positive_log("twpub_dgbas_gdp_raw").alias("twpub_dgbas_gdp_log"),
        _positive_log("twpub_dgbas_cpi_raw").alias("twpub_dgbas_cpi_log"),
        _positive_log("twpub_usdtwd_raw").alias("twpub_usdtwd_log"),
        pl.when(raw.is_finite()).then(raw).otherwise(None).alias("twpub_cbc_overnight_rate"),
        _positive_log("twpub_cbc_m1b_raw").alias("twpub_cbc_m1b_log"),
        _positive_log("twpub_cbc_m2_raw").alias("twpub_cbc_m2_log"),
        *[
            _positive_log1p(source).alias(target)
            for target, source in DERIVED_SOURCES.items()
            if target.startswith("twpub_mof_") and target.endswith("_log")
        ],
        pl.when(balance.is_finite()).then((balance / 1_000_000).arcsinh()).otherwise(None)
        .alias("twpub_mof_trade_balance_asinh"),
    )
    for raw_name, name, log_ratio in (
        ("twpub_usdtwd_raw", "twpub_usdtwd_logret_1d", True),
        ("twpub_cbc_overnight_pct_raw", "twpub_cbc_overnight_rate_chg", False),
    ):
        change = _observed_change(market, raw_name, log_ratio=log_ratio).rename({"value": name})
        derived = derived.join(change, on="date", how="left")
    return derived.with_columns(pl.lit(MARKET_SYMBOL).alias("symbol"))


def _year_predicate(year: int | None, dtype: pl.DataType) -> pl.Expr:
    if year is None:
        return pl.col("date").is_null()
    if dtype == pl.Date and 1 <= year < 9999:
        # A year() expression reads every row group in Polars 1.38.1. Plain
        # date bounds use Parquet min/max pruning without changing the rows.
        return ((pl.col("date") >= date(year, 1, 1))
                & (pl.col("date") < date(year + 1, 1, 1)))
    # Retain the original calendar-year semantics for timezone-aware datetime
    # or extended-calendar inputs, rather than inventing midnight conversions.
    return pl.col("date").dt.year() == year


def build_all_research_features(*, wide_path: Path, official_path: Path, output_path: Path) -> dict:
    wide_path, official_path, output_path = map(Path, (wide_path, official_path, output_path))
    if output_path.resolve() in {wide_path.resolve(), official_path.resolve()}:
        raise ValueError("research output must be distinct from its inputs")
    inputs = {
        "contract_version": CONTRACT_VERSION,
        "build_strategy": BUILD_STRATEGY,
        "wide": {"path": str(wide_path.resolve()), "sha256": _sha256(wide_path)},
        "official": {"path": str(official_path.resolve()), "sha256": _sha256(official_path)},
    }
    receipt_path = output_path.with_suffix(".all_features.json")
    if output_path.is_file() and receipt_path.is_file():
        try:
            prior = json.loads(receipt_path.read_text())
        except (OSError, ValueError):
            prior = {}
        if prior.get("inputs") == inputs and prior.get("output_sha256") == _sha256(output_path):
            return {**prior, "reused": True}

    wide, official = pl.scan_parquet(wide_path), pl.scan_parquet(official_path)
    wide_schema, official_schema = wide.collect_schema(), official.collect_schema()
    wide_names, official_names = set(wide_schema.names()), set(official_schema.names())
    if not set(DERIVED_SOURCES.values()) <= wide_names:
        raise ValueError("wide research input lacks a required historical raw column")
    if not {"date", "symbol"} <= wide_names & official_names:
        raise ValueError("both source tables require date and symbol")
    official_only = sorted(official_names - wide_names - set(DERIVED_SOURCES))
    overlapping_values = sorted(
        name for name in wide_names & official_names
        if name.startswith("twpub_") and name not in DERIVED_SOURCES
    )
    # These formulas have observed-value lags across New Year. Compute them
    # once on the entire small market time series BEFORE partitioning joins.
    derived = _derived_market_values(wide)
    official_fallbacks = {name: f"_official_fallback__{name}" for name in overlapping_values}
    years = set()
    for source in (wide, official):
        years.update(source.select(pl.col("date").dt.year().unique()).collect()["date"].to_list())
    expected_rows = pq.ParquetFile(wide_path).metadata.num_rows
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".tmp")
    actual_rows = 0
    peak_partition_rows = 0
    output_names = []
    try:
        with ExitStack() as stack:
            writer = None
            for year in sorted(years, key=lambda value: (value is None, value or 0)):
                predicate = _year_predicate(year, wide_schema["date"])
                wide_part = wide.filter(predicate)
                official_part = official.filter(_year_predicate(year, official_schema["date"]))
                wide_keys = wide_part.select("date", "symbol").collect(engine="streaming")
                official_keys = official_part.select("date", "symbol").collect(engine="streaming")
                if wide_keys.n_unique() != wide_keys.height:
                    raise ValueError("wide research table has duplicate date/symbol keys")
                if official_keys.join(wide_keys, on=["date", "symbol"], how="anti").height:
                    raise ValueError("official table has keys absent from the research universe")
                if official_keys.n_unique() != official_keys.height:
                    raise ValueError("official table has duplicate keys")
                joined = (
                    wide_part.drop(*[name for name in DERIVED_SOURCES if name in wide_names])
                    .join(official_part.select(
                        "date", "symbol", *official_only,
                        *[pl.col(name).alias(alias) for name, alias in official_fallbacks.items()],
                    ), on=["date", "symbol"], how="left")
                    .with_columns([
                        pl.coalesce(pl.col(name), pl.col(alias)).alias(name)
                        for name, alias in official_fallbacks.items()
                    ]).drop(*official_fallbacks.values())
                    .join(derived.lazy().filter(predicate), on=["date", "symbol"], how="left")
                ).collect(engine="streaming")
                if joined.height != wide_keys.height:
                    raise ValueError("research join changed a partition row count")
                table = joined.to_arrow()
                if writer is None:
                    output_names = joined.columns
                    writer = stack.enter_context(pq.ParquetWriter(temporary, table.schema, compression="zstd"))
                elif not table.schema.equals(writer.schema):
                    raise ValueError("research union annual schema drift")
                writer.write_table(table, row_group_size=16_384)
                actual_rows += joined.height
                peak_partition_rows = max(peak_partition_rows, joined.height)
                print(f"[tw-public-research-all] year={year} rows={joined.height} total_rows={actual_rows}", flush=True)
                del joined, table, wide_keys, official_keys
        if actual_rows != expected_rows:
            raise ValueError(f"research join changed row count: {expected_rows} -> {actual_rows}")
        if pq.ParquetFile(temporary).metadata.num_rows != actual_rows:
            raise ValueError("persisted research union row count differs")
        if _sha256(wide_path) != inputs["wide"]["sha256"] or _sha256(official_path) != inputs["official"]["sha256"]:
            raise RuntimeError("research inputs changed during build")
        os.replace(temporary, output_path)
    finally:
        temporary.unlink(missing_ok=True)
    summary = {
        "contract_version": CONTRACT_VERSION,
        "inputs": inputs,
        "output_path": str(output_path.resolve()),
        "output_sha256": _sha256(output_path),
        "rows": actual_rows,
        "twpub_columns": sorted(name for name in output_names if name.startswith("twpub_")),
        "build_strategy": BUILD_STRATEGY,
        "peak_join_partition_rows": peak_partition_rows,
        "official_only_columns": official_only,
        "official_null_fallback_columns": overlapping_values,
        "reconstructed_columns": list(DERIVED_SOURCES),
        "research_only": True,
        "availability_policy": "source dates retained; downstream research config applies documented panel shifts and availability flags",
        "reused": False,
    }
    receipt_tmp = receipt_path.with_suffix(receipt_path.suffix + ".tmp")
    receipt_tmp.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    os.replace(receipt_tmp, receipt_path)
    return summary

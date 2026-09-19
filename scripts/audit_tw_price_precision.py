"""Audit source transaction/quote grids without changing stored precision.

Only exchange order/trade prices are checked. Settlement, averages, adjusted
prices, dividends, ratios and model features intentionally remain off-grid.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys

import numpy as np
import polars as pl
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data.tw_price_rules import (
    TW_DERIVATIVE_PRICE_CONTRACT_VERSION,
    TW_ORDER_PRICE_CONTRACT_VERSION,
    price_on_tick_grid_numpy,
    price_on_explicit_tick_grid_numpy,
    taifex_index_future_tick_size_numpy,
    taifex_option_tick_size_numpy,
)
from stockagent.data.tw_exchange_price_classification import (
    VERIFIED_EMERGING_TO_TPEX_LISTINGS,
    classify_tw_broker_security_on_date,
    classify_tw_exchange_security,
)
from stockagent.data.tw_security import TW_ETF_SYMBOL_PATTERN, TW_STOCK_SYMBOL_PATTERN
from stockagent.data.tw_security import classify_tw_stock_or_etf


SOURCES = {
    "twse_daily_ohlcv": {
        "path": "data_tw_public/twse_daily_ohlcv.parquet",
        "venue": "twse", "symbol": "證券代號", "name": "證券名稱", "date": "date",
        "prices": ("開盤價", "最高價", "最低價", "收盤價", "最後揭示買價", "最後揭示賣價"),
    },
    "tpex_daily_ohlcv": {
        "path": "data_tw_public/tpex_daily_ohlcv.parquet",
        "venue": "tpex", "symbol": "代號", "name": "名稱", "date": "date",
        "prices": ("開盤", "最高", "最低", "收盤", "最後買價", "最後賣價"),
    },
    "taifex_stock_futures_daily": {
        "path": "data_tw_futures/taifex_portfolio_daily_v4/continuous_daily.parquet",
        "symbol": "physical_contract", "date": "date",
        "prices": ("open", "high", "low", "close", "last_bid", "last_ask"),
    },
}

# TWSE sixth-position K/M/S/C denotes a foreign-currency ETF counter.  The
# numerical ETF tick is the same, but its unit is the counter's quote currency.
# The suffix alone does not distinguish CNY from USD.
FOREIGN_CURRENCY_ETF_PATTERN = r"^00[5-9][0-9]{2}[KMSC]$"


def _counts(fields: tuple[str, ...]) -> dict[str, dict]:
    return {field: {"observed": 0, "off_grid": 0, "examples": []} for field in fields}


def _record_grid_result(
    counts: dict[str, dict], field: str, raw: np.ndarray, good: np.ndarray,
    *, dates: np.ndarray, symbols: np.ndarray, kinds: np.ndarray,
) -> None:
    values = np.asarray(raw, dtype=np.float64)
    observed = np.isfinite(values) & (values > 0.0)
    bad = np.flatnonzero(
        (observed & ~good) | (np.isfinite(values) & (values < 0.0)) | np.isinf(values)
    )
    bucket = counts[field]
    bucket["observed"] += int(np.count_nonzero(observed))
    bucket["off_grid"] += int(len(bad))
    for i in bad[: max(0, 8 - len(bucket["examples"]))]:
        bucket["examples"].append(
            {
                "date": str(dates[i]),
                "symbol": str(symbols[i]),
                "kind": str(kinds[i]),
                "source_value": str(raw[i]),
            }
        )


def _resolve_manifest_output(root: Path, dataset_root: Path, raw: str) -> Path:
    candidate = Path(raw)
    if candidate.is_absolute():
        return candidate
    repository_relative = root / candidate
    if repository_relative.exists() or (
        candidate.parts and candidate.parts[0].startswith("data_")
    ):
        return repository_relative
    if candidate.parts and candidate.parts[0] == dataset_root.name:
        return root / candidate
    return dataset_root / candidate


def _stat_token(path: Path) -> tuple[int, int]:
    stat = path.stat()
    return stat.st_size, stat.st_mtime_ns


def _stat_set_digest(items: list[tuple[str, int, int]]) -> str:
    digest = hashlib.sha256()
    for path, size, mtime_ns in sorted(items):
        digest.update(f"{path}\0{size}\0{mtime_ns}\n".encode())
    return digest.hexdigest()


def audit_file(
    name: str, root: Path, *, batch_size: int = 100_000,
    path_override: Path | None = None,
) -> dict:
    spec = SOURCES[name]
    path = path_override if path_override is not None else root / spec["path"]
    if not path.is_file():
        return {"dataset": name, "status": "missing", "path": str(path)}
    before = path.stat()
    parquet = pq.ParquetFile(path)
    columns = [spec["symbol"], spec["date"], *spec["prices"]]
    has_name = "name" in spec and spec["name"] in parquet.schema_arrow.names
    if has_name:
        columns.append(spec["name"])
    if name == "taifex_stock_futures_daily":
        columns += ["asset_class", "source_row_observed"]
    if not set(columns) <= set(parquet.schema_arrow.names):
        return {"dataset": name, "status": "schema_unknown", "path": str(path),
                "missing_columns": sorted(set(columns) - set(parquet.schema_arrow.names))}
    counts = {field: {"observed": 0, "off_grid": 0, "examples": []} for field in spec["prices"]}
    rows = eligible_rows = invalid_date_rows = unknown_rows = 0
    currency_classes: Counter[str] = Counter()
    security_types: Counter[str] = Counter()
    invalid_date_examples: list[dict] = []
    unknown_examples: list[dict] = []
    for batch in parquet.iter_batches(columns=columns, batch_size=batch_size):
        frame = pl.from_arrow(batch)
        rows += frame.height
        if name == "taifex_stock_futures_daily":
            frame = frame.filter(
                pl.col("source_row_observed")
                & pl.col("asset_class").is_in(["stock_future", "etf_future"])
            ).with_columns(pl.col("asset_class").alias("_kind"))
        else:
            symbol = pl.col(spec["symbol"]).cast(pl.String)
            if has_name:
                classified = [
                    classify_tw_exchange_security(
                        spec["venue"], symbol_value, security_name
                    )
                    for symbol_value, security_name in frame.select(
                        spec["symbol"], spec["name"]
                    ).iter_rows()
                ]
            else:
                classified = [
                    classify_tw_exchange_security(spec["venue"], symbol_value)
                    for symbol_value in frame[spec["symbol"]].to_list()
                ]
            frame = frame.with_columns(pl.Series("_kind", classified, dtype=pl.String))
            unknown = frame.filter(pl.col("_kind").is_null())
            unknown_rows += unknown.height
            example_columns = [spec["symbol"], spec["date"]]
            if has_name:
                example_columns.insert(1, spec["name"])
            for row in unknown.select(example_columns).head(
                max(0, 8 - len(unknown_examples))
            ).to_dicts():
                unknown_examples.append(row)
            frame = frame.filter(pl.col("_kind").is_not_null()).with_columns(
                pl.when(symbol.str.contains(FOREIGN_CURRENCY_ETF_PATTERN))
                .then(pl.lit("foreign_currency_etf"))
                .otherwise(pl.lit("twd"))
                .alias("_currency_class")
            )
            currency_classes.update(frame["_currency_class"].to_list())
            security_types.update(frame["_kind"].to_list())
        eligible_rows += frame.height
        if frame.is_empty():
            continue
        if name != "taifex_stock_futures_daily":
            frame = frame.with_columns(
                pl.col(spec["date"]).str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                .alias("_trading_date")
            )
        else:
            frame = frame.with_columns(pl.col(spec["date"]).alias("_trading_date"))
        invalid = frame.filter(pl.col("_trading_date").is_null())
        invalid_date_rows += invalid.height
        for row in invalid.select(spec["symbol"], spec["date"]).head(
            max(0, 8 - len(invalid_date_examples))
        ).to_dicts():
            invalid_date_examples.append(row)
        frame = frame.filter(pl.col("_trading_date").is_not_null())
        if frame.is_empty():
            continue
        dates = np.asarray(frame["_trading_date"].to_list(), dtype="datetime64[D]")
        kinds = frame["_kind"].to_numpy()
        symbols = frame[spec["symbol"]].to_list()
        for field in spec["prices"]:
            raw = frame[field]
            if raw.dtype == pl.String:
                values = raw.str.replace_all(",", "").cast(pl.Float64, strict=False).to_numpy()
            else:
                values = raw.cast(pl.Float64, strict=False).to_numpy()
            observed = np.isfinite(values) & (values > 0)
            if not np.any(observed):
                continue
            valid = price_on_tick_grid_numpy(values, dates, security_types=kinds)
            bad = np.flatnonzero(
                (observed & ~valid) | (np.isfinite(values) & (values < 0)) | np.isinf(values)
            )
            bucket = counts[field]
            bucket["observed"] += int(np.count_nonzero(observed))
            bucket["off_grid"] += len(bad)
            for i in bad[: max(0, 8 - len(bucket["examples"]))]:
                bucket["examples"].append({
                    "date": str(dates[i]), "symbol": symbols[i],
                    "kind": str(kinds[i]), "source_value": str(raw[int(i)]),
                })
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        return {"dataset": name, "status": "source_changed", "path": str(path)}
    digest = sha256_file(path)
    final = path.stat()
    if (before.st_size, before.st_mtime_ns) != (final.st_size, final.st_mtime_ns):
        return {"dataset": name, "status": "source_changed", "path": str(path)}
    bad_total = sum(item["off_grid"] for item in counts.values())
    return {
        "dataset": name, "status": "source_value_problem" if bad_total or invalid_date_rows or unknown_rows else "quote_grid_valid",
        "path": str(path), "source_sha256": digest,
        "rows": rows, "eligible_rows": eligible_rows, "off_grid_values": bad_total,
        "security_types": dict(security_types),
        "unknown_security_rows": unknown_rows,
        "unknown_security_examples": unknown_examples,
        "currency_classes": dict(currency_classes) if name != "taifex_stock_futures_daily" else {"twd": eligible_rows},
        "invalid_date_rows": invalid_date_rows, "invalid_date_examples": invalid_date_examples,
        "fields": counts,
        "excluded_semantics": "averages, settlement, adjusted prices, dividends, ratios, features",
    }


def audit_shioaji_daily(root: Path) -> dict:
    """Check broker KBar OHLC only; its Trading_Value/VWAP is not a quote."""
    directory = root / "data_tw_public/shioaji/daily"
    paths = sorted(directory.glob("*.parquet"))
    if not paths:
        return {"dataset": "shioaji_daily", "status": "missing", "path": str(directory)}
    fields = ("open", "max", "min", "close")
    counts = {field: {"observed": 0, "off_grid": 0, "examples": []} for field in fields}
    digest = hashlib.sha256()
    rows = skipped_files = invalid_date_rows = 0
    currency_classes: Counter[str] = Counter()
    for path in paths:
        kind = classify_tw_stock_or_etf(path.stem)
        if kind is None:
            skipped_files += 1
            continue
        before = path.stat()
        frame = pl.read_parquet(path, columns=["date", "symbol", *fields])
        if frame.filter(pl.col("symbol") != path.stem).height:
            raise ValueError(f"Shioaji symbol differs from filename: {path}")
        invalid_date_rows += frame["date"].null_count()
        frame = frame.drop_nulls("date")
        rows += frame.height
        currency_classes[
            "foreign_currency_etf" if re.fullmatch(FOREIGN_CURRENCY_ETF_PATTERN, path.stem)
            else "twd"
        ] += frame.height
        dates = np.asarray(frame["date"].to_list(), dtype="datetime64[D]")
        for field in fields:
            values = frame[field].to_numpy()
            observed = np.isfinite(values) & (values > 0)
            good = price_on_tick_grid_numpy(values, dates, security_types=kind)
            bad = np.flatnonzero((observed & ~good) | (np.isfinite(values) & (values < 0)))
            bucket = counts[field]
            bucket["observed"] += int(np.count_nonzero(observed))
            bucket["off_grid"] += len(bad)
            for i in bad[: max(0, 8 - len(bucket["examples"]))]:
                bucket["examples"].append({"date": str(dates[i]), "symbol": path.stem,
                                           "kind": kind, "source_value": float(values[i])})
        content_sha = sha256_file(path)
        after = path.stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            return {"dataset": "shioaji_daily", "status": "source_changed", "path": str(path)}
        digest.update(path.name.encode())
        digest.update(bytes.fromhex(content_sha))
    bad_total = sum(item["off_grid"] for item in counts.values())
    return {"dataset": "shioaji_daily", "status": "source_value_problem" if bad_total or invalid_date_rows else "quote_grid_valid",
            "path": str(directory), "file_count": len(paths), "skipped_unsupported_files": skipped_files,
            "source_set_sha256": digest.hexdigest(), "rows": rows,
            "currency_classes": dict(currency_classes),
            "invalid_date_rows": invalid_date_rows, "off_grid_values": bad_total,
            "fields": counts,
            "excluded_semantics": "Trading_Value, VWAP, adjusted prices, dividends, ratios, features"}


def audit_index_futures_front_month(root: Path) -> dict:
    """Audit source OHLC for the three index futures used by the trainer."""

    path = root / "data_tw_index_futures/day_session_front_month.parquet"
    name = "taifex_index_futures_front_month"
    if not path.is_file():
        return {"dataset": name, "status": "missing", "path": str(path)}
    fields = ("open", "high", "low", "close")
    counts = _counts(fields)
    before = _stat_token(path)
    rows = 0
    products: Counter[str] = Counter()
    parquet = pq.ParquetFile(path)
    for batch in parquet.iter_batches(columns=["date", "product", *fields], batch_size=200_000):
        frame = pl.from_arrow(batch)
        rows += frame.height
        dates = np.asarray(frame["date"].to_list(), dtype="datetime64[D]")
        product = frame["product"].cast(pl.String).to_numpy()
        products.update(product.tolist())
        for field in fields:
            raw = frame[field].to_numpy()
            tick = taifex_index_future_tick_size_numpy(raw, product_codes=product)
            good = price_on_explicit_tick_grid_numpy(raw, tick)
            _record_grid_result(
                counts, field, raw, good, dates=dates, symbols=product, kinds=product
            )
    if before != _stat_token(path):
        return {"dataset": name, "status": "source_changed", "path": str(path)}
    bad = sum(item["off_grid"] for item in counts.values())
    return {
        "dataset": name,
        "status": "source_value_problem" if bad else "quote_grid_valid",
        "path": str(path),
        "source_sha256": sha256_file(path),
        "rows": rows,
        "products": dict(products),
        "off_grid_values": bad,
        "fields": counts,
        "excluded_semantics": "settlement, log returns, multiplier and derived features",
    }


def audit_txo_daily_chain(root: Path, *, scope: str) -> dict:
    """Audit TXO ordinary-market premiums; strike and settlement are separate values."""

    if scope not in {"monthly", "weekly"}:
        raise ValueError("TXO chain scope must be monthly or weekly")
    name = f"taifex_txo_{scope}_daily_chain"
    path = root / f"data_tw_index_options_daily/{scope}_full_chain.parquet"
    if not path.is_file():
        return {"dataset": name, "status": "missing", "path": str(path)}
    fields = ("open", "close", "last_bid", "last_ask")
    counts = _counts(fields)
    rows = invalid_strike = 0
    before = _stat_token(path)
    parquet = pq.ParquetFile(path)
    columns = ["date", "option_series", "strike", *fields]
    for batch in parquet.iter_batches(columns=columns, batch_size=200_000):
        frame = pl.from_arrow(batch)
        rows += frame.height
        dates = np.asarray(frame["date"].to_list(), dtype="datetime64[D]")
        symbols = frame["option_series"].cast(pl.String).to_numpy()
        strike = frame["strike"].cast(pl.Float64).to_numpy()
        invalid_strike += int(np.count_nonzero(~np.isfinite(strike) | (strike <= 0.0)))
        kinds = np.full(frame.height, "txo", dtype="U3")
        for field in fields:
            raw = frame[field].cast(pl.Float64).to_numpy()
            tick = taifex_option_tick_size_numpy(
                raw, dates, product_families=kinds, trading_method="ordinary"
            )
            good = price_on_explicit_tick_grid_numpy(raw, tick)
            _record_grid_result(
                counts, field, raw, good, dates=dates, symbols=symbols, kinds=kinds
            )
    if before != _stat_token(path):
        return {"dataset": name, "status": "source_changed", "path": str(path)}
    bad = sum(item["off_grid"] for item in counts.values())
    return {
        "dataset": name,
        "status": "source_value_problem" if bad or invalid_strike else "quote_grid_valid",
        "path": str(path),
        "source_sha256": sha256_file(path),
        "rows": rows,
        "off_grid_values": bad,
        "invalid_positive_strikes": invalid_strike,
        "fields": counts,
        "excluded_semantics": "settlement and strike spacing; strike is domain-checked only",
    }


def audit_official_recent_tx_txo(root: Path) -> dict:
    """Audit manifest-selected TAIFEX trade-by-trade TX/TXO partitions."""

    dataset_root = root / "data_tw_index_derivatives_ticks"
    manifest_path = dataset_root / "manifest.json"
    name = "taifex_recent_tx_txo_trade_by_trade"
    if not manifest_path.is_file():
        return {"dataset": name, "status": "missing", "path": str(manifest_path)}
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    counts = _counts(("tx_price", "txo_premium"))
    rows = spread_rows = invalid_strike = 0
    partition_stats: list[tuple[str, int, int]] = []
    source_hash_mismatch: list[str] = []
    for partition in manifest.get("partitions", []):
        path = _resolve_manifest_output(
            root, dataset_root, str(partition.get("output_path", ""))
        )
        if not path.is_file():
            source_hash_mismatch.append(str(path))
            continue
        before = _stat_token(path)
        frame = pl.read_parquet(path)
        rows += frame.height
        kind = str(partition.get("kind", ""))
        dates = np.asarray(frame["trading_date"].to_list(), dtype="datetime64[D]")
        raw = frame["price"].cast(pl.Float64).to_numpy()
        if kind == "futures":
            outright = (
                frame["near_month_price"].is_null()
                & frame["far_month_price"].is_null()
            ).to_numpy()
            spread_rows += int(np.count_nonzero(~outright))
            selected = raw[outright]
            selected_dates = dates[outright]
            products = frame["product"].cast(pl.String).to_numpy()[outright]
            tick = taifex_index_future_tick_size_numpy(
                selected, product_codes=products
            )
            good = price_on_explicit_tick_grid_numpy(selected, tick)
            _record_grid_result(
                counts,
                "tx_price",
                selected,
                good,
                dates=selected_dates,
                symbols=products,
                kinds=products,
            )
        elif kind == "options":
            products = np.full(frame.height, "TXO", dtype="U3")
            families = np.full(frame.height, "txo", dtype="U3")
            tick = taifex_option_tick_size_numpy(
                raw, dates, product_families=families, trading_method="ordinary"
            )
            good = price_on_explicit_tick_grid_numpy(raw, tick)
            _record_grid_result(
                counts,
                "txo_premium",
                raw,
                good,
                dates=dates,
                symbols=products,
                kinds=families,
            )
            strike = frame["strike_price"].cast(pl.Float64).to_numpy()
            invalid_strike += int(
                np.count_nonzero(~np.isfinite(strike) | (strike <= 0.0))
            )
        else:
            source_hash_mismatch.append(f"unknown partition kind: {kind}")
        after = _stat_token(path)
        if before != after:
            source_hash_mismatch.append(f"changed:{path}")
        expected_sha = str(partition.get("output_sha256", ""))
        if expected_sha and sha256_file(path) != expected_sha:
            source_hash_mismatch.append(f"sha256:{path}")
        partition_stats.append((str(path), *after))
    bad = sum(item["off_grid"] for item in counts.values())
    status = (
        "source_value_problem"
        if bad or invalid_strike
        else "source_changed"
        if source_hash_mismatch
        else "quote_grid_valid"
    )
    return {
        "dataset": name,
        "status": status,
        "path": str(dataset_root),
        "manifest_sha256": sha256_file(manifest_path),
        "source_set_stat_sha256": _stat_set_digest(partition_stats),
        "partition_count": len(partition_stats),
        "rows": rows,
        "spread_rows_semantically_excluded": spread_rows,
        "invalid_positive_strikes": invalid_strike,
        "source_problems": source_hash_mismatch[:20],
        "off_grid_values": bad,
        "fields": counts,
        "excluded_semantics": "TX signed calendar spreads and TXO strike spacing",
    }


def _manifest_selected_files(root: Path, dataset_root: Path) -> tuple[Path, list[Path], dict]:
    manifest_path = dataset_root / "manifest.json"
    if not manifest_path.is_file():
        return manifest_path, [], {}
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    paths = [
        _resolve_manifest_output(root, dataset_root, str(item.get("output", "")))
        for item in manifest.get("partitions", [])
        if item.get("status") == "ok"
    ]
    return manifest_path, paths, manifest


def audit_tw_minute_training_prices(root: Path) -> dict:
    """Audit raw Shioaji one-minute OHLC in every manifest-selected partition."""

    dataset_root = root / "data_tw_minute/research_dataset"
    name = "shioaji_tw_minute_training_ohlc"
    manifest_path, paths, manifest = _manifest_selected_files(root, dataset_root)
    if not paths:
        return {"dataset": name, "status": "missing", "path": str(dataset_root)}
    fields = ("Open", "High", "Low", "Close")
    counts = _counts(fields)
    rows = unknown_rows = 0
    unknown_examples: list[dict] = []
    stats: list[tuple[str, int, int]] = []
    changed: list[str] = []
    for path in paths:
        if not path.is_file():
            changed.append(f"missing:{path}")
            continue
        before = _stat_token(path)
        parquet = pq.ParquetFile(path)
        for batch in parquet.iter_batches(
            columns=["date", "symbol", "market", *fields], batch_size=250_000
        ):
            frame = pl.from_arrow(batch)
            rows += frame.height
            symbols = frame["symbol"].cast(pl.String).to_numpy()
            markets = frame["market"].cast(pl.String).to_numpy()
            dates = np.asarray(frame["date"].to_list(), dtype="datetime64[D]")
            unique = set(zip(markets.tolist(), symbols.tolist()))
            mapping = {
                key: classify_tw_exchange_security(key[0], key[1]) for key in unique
            }
            kinds = np.asarray(
                [mapping[(market, symbol)] or "unknown" for market, symbol in zip(markets, symbols)],
                dtype="U16",
            )
            # Broker market=tpex also covers verified emerging listings.
            for code in VERIFIED_EMERGING_TO_TPEX_LISTINGS:
                candidates = np.flatnonzero((markets == "tpex") & (symbols == code))
                for index in candidates:
                    kinds[index] = classify_tw_broker_security_on_date(
                        "tpex", code, dates[index].astype(object)
                    ) or "unknown"
            unknown = kinds == "unknown"
            unknown_rows += int(np.count_nonzero(unknown))
            for i in np.flatnonzero(unknown)[: max(0, 8 - len(unknown_examples))]:
                unknown_examples.append(
                    {"date": str(frame["date"][int(i)]), "symbol": str(symbols[i])}
                )
            supported = ~unknown
            for field in fields:
                raw = frame[field].cast(pl.Float64).to_numpy()[supported]
                good = price_on_tick_grid_numpy(
                    raw, dates[supported], security_types=kinds[supported]
                )
                _record_grid_result(
                    counts,
                    field,
                    raw,
                    good,
                    dates=dates[supported],
                    symbols=symbols[supported],
                    kinds=kinds[supported],
                )
        after = _stat_token(path)
        if before != after:
            changed.append(f"changed:{path}")
        stats.append((str(path), *after))
    bad = sum(item["off_grid"] for item in counts.values())
    status = (
        "source_value_problem"
        if bad or unknown_rows
        else "source_changed"
        if changed
        else "quote_grid_valid"
    )
    return {
        "dataset": name,
        "status": status,
        "path": str(dataset_root),
        "manifest_sha256": sha256_file(manifest_path),
        "manifest_status": manifest.get("status"),
        "source_set_stat_sha256": _stat_set_digest(stats),
        "partition_count": len(stats),
        "rows": rows,
        "unknown_security_rows": unknown_rows,
        "unknown_security_examples": unknown_examples,
        "source_problems": changed[:20],
        "off_grid_values": bad,
        "fields": counts,
        "excluded_semantics": "Amount, previous/next duplicated quotes, VWAP-like ratios, returns and labels",
    }


def audit_tw_hft_training_prices(root: Path) -> dict:
    """Audit executable book/trade prices; derived mid/VWAP/microprice stay off-grid."""

    dataset_root = root / "data_tw_microstructure/hft_dataset"
    name = "shioaji_tw_hft_training_quotes"
    manifest_path, paths, manifest = _manifest_selected_files(root, dataset_root)
    if not paths:
        return {"dataset": name, "status": "missing", "path": str(dataset_root)}
    fields = tuple(
        [f"bid_price_{level}" for level in range(1, 6)]
        + [f"ask_price_{level}" for level in range(1, 6)]
        + ["last_trade_price"]
    )
    counts = _counts(fields)
    rows = unknown_rows = 0
    stats: list[tuple[str, int, int]] = []
    changed: list[str] = []
    for path in paths:
        if not path.is_file():
            changed.append(f"missing:{path}")
            continue
        before = _stat_token(path)
        parquet = pq.ParquetFile(path)
        for batch in parquet.iter_batches(
            columns=["trade_date", "code", "market", *fields], batch_size=250_000
        ):
            frame = pl.from_arrow(batch)
            rows += frame.height
            symbols = frame["code"].cast(pl.String).to_numpy()
            markets = frame["market"].cast(pl.String).to_numpy()
            mapping = {
                key: classify_tw_exchange_security(key[0], key[1])
                for key in set(zip(markets.tolist(), symbols.tolist()))
            }
            kinds = np.asarray(
                [mapping[(market, symbol)] or "unknown" for market, symbol in zip(markets, symbols)]
            )
            supported = kinds != "unknown"
            unknown_rows += int(np.count_nonzero(~supported))
            dates = np.asarray(frame["trade_date"].to_list(), dtype="datetime64[D]")
            for field in fields:
                raw = frame[field].cast(pl.Float64).to_numpy()[supported]
                good = price_on_tick_grid_numpy(
                    raw, dates[supported], security_types=kinds[supported]
                )
                _record_grid_result(
                    counts,
                    field,
                    raw,
                    good,
                    dates=dates[supported],
                    symbols=symbols[supported],
                    kinds=kinds[supported],
                )
        after = _stat_token(path)
        if before != after:
            changed.append(f"changed:{path}")
        stats.append((str(path), *after))
    bad = sum(item["off_grid"] for item in counts.values())
    status = (
        "source_value_problem"
        if bad or unknown_rows
        else "source_changed"
        if changed
        else "quote_grid_valid"
    )
    return {
        "dataset": name,
        "status": status,
        "path": str(dataset_root),
        "manifest_sha256": sha256_file(manifest_path),
        "manifest_status": manifest.get("status"),
        "source_set_stat_sha256": _stat_set_digest(stats),
        "partition_count": len(stats),
        "rows": rows,
        "unknown_security_rows": unknown_rows,
        "source_problems": changed[:20],
        "off_grid_values": bad,
        "fields": counts,
        "excluded_semantics": "mid_price, future_mid_price, microprice, spread and trade_vwap are calculated",
    }


def audit_shioaji_historical_market_prices(root: Path) -> dict:
    """Scan every receipt-backed historical Tick/KBar price in the archive."""

    dataset_root = root / "data_tw_shioaji_history"
    summary_path = dataset_root / "summary.json"
    name = "shioaji_historical_market_prices"
    if not summary_path.is_file():
        return {"dataset": name, "status": "missing", "path": str(dataset_root)}
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    master_path = root / "data_tw_futures/taifex_portfolio_daily_v4/product_master.parquet"
    if not master_path.is_file():
        return {"dataset": name, "status": "missing", "path": str(master_path)}
    master = pl.read_parquet(
        master_path, columns=["shioaji_roots", "official_product", "asset_class"]
    )
    future_roots: dict[str, str] = {}
    for row in master.iter_rows(named=True):
        asset_class = str(row["asset_class"])
        if asset_class not in {"stock_future", "etf_future"}:
            continue
        for raw_root in (row["shioaji_roots"], row["official_product"]):
            normalized_root = str(raw_root or "").strip()
            if normalized_root:
                future_roots[normalized_root] = asset_class
    tick_snapshot_path = (
        root
        / "artifacts/data_quality/tw_price_precision/"
        "shioaji_contract_v2_tick_snapshot.json"
    )
    fixed_future_ticks: dict[str, float] = {}
    if tick_snapshot_path.is_file():
        tick_snapshot = json.loads(tick_snapshot_path.read_text(encoding="utf-8"))
        for row in tick_snapshot.get("rows", []):
            if (
                row.get("security_type") == "FUT"
                and row.get("tick_basis") == "fixed"
                and row.get("tick") is not None
            ):
                fixed_future_ticks[str(row.get("root"))] = float(row["tick"])
    for weekly_root in ("MX1", "MX2", "MX4", "MX5"):
        if "MXF" in fixed_future_ticks:
            fixed_future_ticks[weekly_root] = fixed_future_ticks["MXF"]
    counts = _counts(
        (
            "future_tick_close",
            "future_tick_bid",
            "future_tick_ask",
            "future_kbar_open",
            "future_kbar_high",
            "future_kbar_low",
            "future_kbar_close",
            "fixed_future_tick_close",
            "fixed_future_tick_bid",
            "fixed_future_tick_ask",
            "fixed_future_kbar_open",
            "fixed_future_kbar_high",
            "fixed_future_kbar_low",
            "fixed_future_kbar_close",
            "txo_tick_close",
            "txo_tick_bid",
            "txo_tick_ask",
            "txo_kbar_open",
            "txo_kbar_high",
            "txo_kbar_low",
            "txo_kbar_close",
        )
    )
    paths = sorted((dataset_root / "contracts").glob("*/*/*/*/data.parquet"))
    stats: list[tuple[str, int, int]] = []
    source_problems: list[str] = []
    unknown_future_files = unknown_future_rows = 0
    current_snapshot_only_future_rows = 0
    index_rows = index_observed_values = index_invalid_values = 0
    rows = 0
    files_by_group_method: Counter[str] = Counter()
    rows_by_group_method: Counter[str] = Counter()
    for file_index, path in enumerate(paths, start=1):
        relative = path.relative_to(dataset_root / "contracts")
        group, contract, method = relative.parts[0], relative.parts[1], relative.parts[2]
        key = f"{group}_{method}"
        files_by_group_method[key] += 1
        before = _stat_token(path)
        parquet = pq.ParquetFile(path)
        fields = (
            ("close", "bid_price", "ask_price")
            if method == "ticks"
            else ("Open", "High", "Low", "Close")
        )
        columns = ["trading_date", "query_contract", *fields]
        for batch in parquet.iter_batches(columns=columns, batch_size=250_000):
            frame = pl.from_arrow(batch)
            batch_rows = frame.height
            rows += batch_rows
            rows_by_group_method[key] += batch_rows
            dates = np.asarray(frame["trading_date"].to_list(), dtype="datetime64[D]")
            symbols = frame["query_contract"].cast(pl.String).to_numpy()
            if group == "index":
                index_rows += batch_rows
                for field in fields:
                    values = frame[field].cast(pl.Float64).to_numpy()
                    observed = np.isfinite(values) & (values > 0.0)
                    index_observed_values += int(np.count_nonzero(observed))
                    index_invalid_values += int(np.count_nonzero(
                        (np.isfinite(values) & (values < 0.0)) | np.isinf(values)
                    ))
                continue
            if group == "futures":
                future_root = contract[:-2]
                security_type = future_roots.get(future_root)
                fixed_tick = fixed_future_ticks.get(future_root)
                if security_type is None and fixed_tick is None:
                    unknown_future_rows += batch_rows
                    continue
                if security_type is not None:
                    kinds = np.full(batch_rows, security_type)
                    names = {
                        "close": "future_tick_close",
                        "bid_price": "future_tick_bid",
                        "ask_price": "future_tick_ask",
                        "Open": "future_kbar_open",
                        "High": "future_kbar_high",
                        "Low": "future_kbar_low",
                        "Close": "future_kbar_close",
                    }
                    for field in fields:
                        values = frame[field].cast(pl.Float64).to_numpy()
                        good = price_on_tick_grid_numpy(
                            values, dates, security_types=kinds
                        )
                        _record_grid_result(
                            counts,
                            names[field],
                            values,
                            good,
                            dates=dates,
                            symbols=symbols,
                            kinds=kinds,
                        )
                else:
                    current_snapshot_only_future_rows += batch_rows
                    kinds = np.full(batch_rows, future_root)
                    names = {
                        "close": "fixed_future_tick_close",
                        "bid_price": "fixed_future_tick_bid",
                        "ask_price": "fixed_future_tick_ask",
                        "Open": "fixed_future_kbar_open",
                        "High": "fixed_future_kbar_high",
                        "Low": "fixed_future_kbar_low",
                        "Close": "fixed_future_kbar_close",
                    }
                    for field in fields:
                        values = frame[field].cast(pl.Float64).to_numpy()
                        ticks = np.full(batch_rows, float(fixed_tick))
                        good = price_on_explicit_tick_grid_numpy(values, ticks)
                        _record_grid_result(
                            counts,
                            names[field],
                            values,
                            good,
                            dates=dates,
                            symbols=symbols,
                            kinds=kinds,
                        )
            elif group == "options":
                kinds = np.full(batch_rows, "txo", dtype="U3")
                names = {
                    "close": "txo_tick_close",
                    "bid_price": "txo_tick_bid",
                    "ask_price": "txo_tick_ask",
                    "Open": "txo_kbar_open",
                    "High": "txo_kbar_high",
                    "Low": "txo_kbar_low",
                    "Close": "txo_kbar_close",
                }
                for field in fields:
                    values = frame[field].cast(pl.Float64).to_numpy()
                    tick = taifex_option_tick_size_numpy(
                        values,
                        dates,
                        product_families=kinds,
                        trading_method="ordinary",
                    )
                    good = price_on_explicit_tick_grid_numpy(values, tick)
                    _record_grid_result(
                        counts,
                        names[field],
                        values,
                        good,
                        dates=dates,
                        symbols=symbols,
                        kinds=kinds,
                    )
            else:
                source_problems.append(f"unknown_group:{path}")
        after = _stat_token(path)
        if before != after:
            source_problems.append(f"changed:{path}")
        receipt_path = path.with_name("receipt.json")
        if not receipt_path.is_file():
            source_problems.append(f"missing_receipt:{path}")
        else:
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            expected_sha = str(receipt.get("sha256", ""))
            if expected_sha and sha256_file(path) != expected_sha:
                source_problems.append(f"sha256:{path}")
        stats.append((str(path), *after))
        if (
            group == "futures"
            and contract[:-2] not in future_roots
            and contract[:-2] not in fixed_future_ticks
        ):
            unknown_future_files += 1
    bad = sum(item["off_grid"] for item in counts.values())
    provisional_bad = sum(
        item["off_grid"] for key, item in counts.items() if key.startswith("fixed_future_")
    )
    proven_bad = bad - provisional_bad
    status = (
        "source_value_problem"
        if proven_bad or index_invalid_values
        else "source_changed"
        if source_problems
        else "historical_rule_unverified"
        if unknown_future_rows or current_snapshot_only_future_rows
        else "quote_grid_valid"
    )
    return {
        "dataset": name,
        "status": status,
        "path": str(dataset_root),
        "summary_sha256": sha256_file(summary_path),
        "current_tick_snapshot_sha256": (
            sha256_file(tick_snapshot_path) if tick_snapshot_path.is_file() else None
        ),
        "summary_state": summary.get("state"),
        "summary_coverage_state": summary.get("coverage_state"),
        "source_set_stat_sha256": _stat_set_digest(stats),
        "file_count": len(stats),
        "files_by_group_method": dict(files_by_group_method),
        "rows": rows,
        "rows_by_group_method": dict(rows_by_group_method),
        "unknown_future_files": unknown_future_files,
        "unknown_future_rows": unknown_future_rows,
        "current_snapshot_only_future_rows": current_snapshot_only_future_rows,
        "current_snapshot_only_grid_mismatches": provisional_bad,
        "index_rows_domain_checked": index_rows,
        "index_observed_values": index_observed_values,
        "index_invalid_values": index_invalid_values,
        "source_problems": source_problems[:20],
        "off_grid_values": bad,
        "fields": counts,
        "excluded_semantics": "index levels, Amount, option strikes, settlement and all calculated averages/features; index levels receive domain checks only",
        "coverage_note": "fixed future roots use current provider metadata as a provisional numeric cross-check only, never historical tick proof; quote validity is independent of the archive summary's waiting_source/source_gaps completeness state",
    }


def _aggregate_audit_status(results: list[dict]) -> str:
    statuses = {str(item.get("status")) for item in results}
    if statuses == {"quote_grid_valid"}:
        return "quote_grid_valid"
    for priority in (
        "source_value_problem", "source_changed", "historical_rule_unverified", "missing"
    ):
        if priority in statuses:
            return priority
    return "incomplete_or_unrecognized_status"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts/data_quality/tw_price_precision/audit.json")
    parser.add_argument("--strict", action="store_true", help="fail if an audited quote is off-grid or a source is missing")
    parser.add_argument(
        "--full",
        action="store_true",
        help="also scan the 312M-row minute panel and manifest-selected HFT panel",
    )
    args = parser.parse_args()
    results = [audit_file(name, args.root) for name in SOURCES]
    results.append(audit_shioaji_daily(args.root))
    results.extend(
        [
            audit_index_futures_front_month(args.root),
            audit_txo_daily_chain(args.root, scope="monthly"),
            audit_txo_daily_chain(args.root, scope="weekly"),
            audit_official_recent_tx_txo(args.root),
        ]
    )
    if args.full:
        results.extend(
            [
                audit_tw_minute_training_prices(args.root),
                audit_tw_hft_training_prices(args.root),
                audit_shioaji_historical_market_prices(args.root),
            ]
        )
    receipt = {
        "audit_version": 3,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "cash_price_contract_version": TW_ORDER_PRICE_CONTRACT_VERSION,
        "derivative_price_contract_version": TW_DERIVATIVE_PRICE_CONTRACT_VERSION,
        "scope": "full_manifest_selected_training_prices" if args.full else "core_daily_and_official_recent_ticks",
        "status": _aggregate_audit_status(results),
        "datasets": results,
        "meaning": "The status covers named outright trade/order quote fields in their trading currencies, not every Taiwan numeric field or completeness/PIT. Foreign-currency ETF suffixes identify a foreign counter but not its exact currency.",
    }
    atomic_write_json(args.output, receipt)
    print(json.dumps({"status": receipt["status"], "output": str(args.output),
                      "summary": [{"dataset": x["dataset"], "status": x["status"],
                                   "rows": x.get("rows"), "off_grid_values": x.get("off_grid_values")}
                                  for x in results]}, ensure_ascii=False))
    return 2 if args.strict and receipt["status"] != "quote_grid_valid" else 0


if __name__ == "__main__":
    raise SystemExit(main())

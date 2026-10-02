"""Read-only source/training-input inventory for the penguin source-host policy.

Run with the canonical runtime from /root/stockAgent. No source writes, API
calls, publication, materialization, cache generation, or training occur.
"""

from __future__ import annotations

import argparse
import csv
import dataclasses
import json
import os
from pathlib import Path
import sys
import time
from collections import defaultdict

REPO = Path(__file__).resolve().parents[1]
OUTPUT = REPO / "artifacts/data_quality" / time.strftime("training_inventory_%Y%m%d")
sys.path.insert(0, str(REPO))
os.environ.setdefault("POLARS_MAX_THREADS", "1")

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from stockagent.config import load_config
from stockagent.data_sync.desync_snapshots import atomic_write_json


def dump_csv(path, rows):
    keys = sorted({key for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(
            {
                key: json.dumps(value, ensure_ascii=False)
                if isinstance(value, (list, dict))
                else value
                for key, value in row.items()
            }
            for row in rows
        )


def configs():
    refs, errors = [], []
    for path in sorted((REPO / "configs/markets").glob("*.yaml")):
        try:
            cfg = load_config(path)
            for section in ("data", "trading"):
                for key, value in dataclasses.asdict(getattr(cfg, section)).items():
                    if (
                        isinstance(value, str)
                        and value
                        and ("root" in key or "path" in key)
                        and "cache" not in key
                    ):
                        if value.startswith(("data_", "artifacts/", "/srv/")):
                            target = (
                                Path(value)
                                if Path(value).is_absolute()
                                else REPO / value
                            )
                            refs.append(
                                {
                                    "config": path.name,
                                    "execution_mode": cfg.trading.execution_mode,
                                    "field": f"{section}.{key}",
                                    "declared_path": value,
                                    "resolved_path": str(target.resolve()),
                                    "exists": target.exists(),
                                    "scope": "resolved config including defaults, not an active runtime claim",
                                }
                            )
        except Exception as error:
            errors.append({"config": path.name, "error": str(error)})
    dump_csv(OUTPUT / "config_references.csv", refs)
    atomic_write_json(OUTPUT / "config_errors.json", errors)
    return refs, errors


def timestamp_keys(path):
    column = (
        pq.ParquetFile(path)
        .read(columns=["date"], use_threads=False)
        .column("date")
        .combine_chunks()
    )
    if pa.types.is_string(column.type) or pa.types.is_large_string(column.type):
        # Crypto canonical sources persist ISO timestamps as strings. Parse
        # strictly; never replace an invalid key with a fabricated timestamp.
        series = pl.from_arrow(column)
        sample = series.drop_nulls().head(1)
        fmt = None
        if len(sample):
            value = sample.item()
            if len(value) == 19 and value[10] == " ":
                fmt = "%Y-%m-%d %H:%M:%S"
            elif len(value) == 10:
                fmt = "%Y-%m-%d"
        parsed = series.str.to_datetime(
            format=fmt, time_unit="ns", strict=True, cache=False
        )
        nulls = parsed.null_count()
        values = parsed.drop_nulls().cast(pl.Int64).to_numpy()
        return values, nulls
    if not (pa.types.is_timestamp(column.type) or pa.types.is_date(column.type)):
        raise ValueError(f"date key is not temporal: {path}: {column.type}")
    nulls = column.null_count
    values = (
        column.drop_null()
        .cast(pa.timestamp("ns"))
        .cast(pa.int64())
        .to_numpy(zero_copy_only=False)
    )
    return values, nulls


def footer(path):
    before = path.stat()
    parquet = pq.ParquetFile(path)
    names = parquet.schema.names
    dates = next(
        (
            name
            for name in ("date", "trade_date", "trading_date", "ts", "event_ts")
            if name in names
        ),
        None,
    )
    lows, highs = [], []
    if dates:
        for i in range(parquet.metadata.num_row_groups):
            stats = parquet.metadata.row_group(i).column(names.index(dates)).statistics
            if stats and stats.has_min_max:
                lows.append(str(stats.min))
                highs.append(str(stats.max))
    after = path.stat()
    if signature(before) != signature(after):
        raise RuntimeError("file changed during footer inventory")
    return {
        "path": str(path),
        "device": before.st_dev,
        "inode": before.st_ino,
        "bytes": before.st_size,
        "allocated_bytes": before.st_blocks * 512,
        "mtime_ns": before.st_mtime_ns,
        "rows": parquet.metadata.num_rows,
        "columns": len(names),
        "date_column": dates,
        "first": min(lows) if lows else None,
        "last": max(highs) if highs else None,
    }, names


def signature(info):
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns


def public_information_category(identifier, title="", provider=""):
    """A routing label, not proof that two sources are interchangeable."""
    text = f"{identifier} {title} {provider}".lower()
    # E.g. MOF tax revenue is macro, not an issuer's monthly revenue.
    if identifier.lower().startswith(
        ("mof_", "cbc_", "dgbas_", "supplemental/mof_", "supplemental/cbc_")
    ):
        return "macro_original_releases"
    groups = (
        (
            "corporate_events",
            (
                "dividend",
                "entitlement",
                "corporate_action",
                "replacement",
                "transfer_adjustment",
                "material_info",
                "exright",
                "ex_dividend",
                "capital_reduction",
                "treasury",
                "重大訊息",
                "除權",
                "股利",
                "減資",
            ),
        ),
        (
            "financials_revenue",
            (
                "mops",
                "t187ap",
                "tifrs",
                "financial",
                "revenue",
                "balance_sheet",
                "income_statement",
                "cash_flow",
                "fundamental",
                "財報",
                "財務",
                "營收",
            ),
        ),
        (
            "ownership_institutional_margin",
            (
                "tdcc",
                "shareholding",
                "institutional",
                "3insti",
                "qfii",
                "margin",
                "short_sell",
                "shortsell",
                "sbl",
                "insider",
                "融資",
                "融券",
                "集保",
                "法人",
                "股權",
            ),
        ),
        (
            "macro_original_releases",
            (
                "dgbas",
                "cbc",
                "mof",
                "fred",
                "economic",
                "macro",
                "census",
                "bea",
                "cftc",
                "gdp",
                "cpi",
                "unemployment",
                "nmi",
                "pmi",
                "主計",
                "央行",
                "財政",
                "總經",
            ),
        ),
        (
            "company_rules_catalogs",
            (
                "company",
                "listing",
                "delisted",
                "security",
                "eligibility",
                "disposal",
                "attention",
                "holiday",
                "calendar",
                "constituent",
                "catalog",
                "公司",
                "上市",
                "下市",
                "處置",
                "注意",
            ),
        ),
        (
            "derivatives_funding_oi",
            (
                "funding",
                "open_interest",
                "openinterest",
                "futures_oi",
                "long_short",
                "longshort",
                "position_ratio",
                "basis",
                "premium",
                "期貨",
                "選擇權",
            ),
        ),
        (
            "pricing_and_market_statistics",
            (
                "ohlcv",
                "price",
                "valuation",
                "indices",
                "index",
                "twt",
                "trade",
                "orderbook",
                "order_book",
                "volume",
                "價格",
                "日線",
                "成交",
                "行情",
            ),
        ),
    )
    for category, terms in groups:
        if any(term in text for term in terms):
            return category
    return "other_sources_review_required"


def public_table_footer(path):
    """Record each temporal column separately; date is not publication time."""
    before = path.stat()
    parquet = pq.ParquetFile(path)
    schema = parquet.schema_arrow
    time_bounds = {}
    for field in schema:
        if not (
            pa.types.is_temporal(field.type)
            or any(
                term in field.name.lower()
                for term in ("date", "time", "period", "year", "month")
            )
        ):
            continue
        # Nested physical column indices differ from Arrow field indices.
        if field.name not in parquet.schema.names:
            continue
        column_index = parquet.schema.names.index(field.name)
        lows, highs = [], []
        for group_index in range(parquet.metadata.num_row_groups):
            stats = (
                parquet.metadata.row_group(group_index).column(column_index).statistics
            )
            if stats and stats.has_min_max:
                lows.append(str(stats.min))
                highs.append(str(stats.max))
        time_bounds[field.name] = {
            "type": str(field.type),
            "first": min(lows) if lows else None,
            "last": max(highs) if highs else None,
            "basis": "row-group statistics; not official release or PIT proof",
        }
    after = path.stat()
    if signature(before) != signature(after):
        raise RuntimeError(f"source changed during inventory: {path}")
    return {
        "path": str(path.resolve()),
        "bytes": before.st_size,
        "allocated_bytes": before.st_blocks * 512,
        "device": before.st_dev,
        "inode": before.st_ino,
        "mtime_ns": before.st_mtime_ns,
        "rows": parquet.metadata.num_rows,
        "columns": len(schema),
        "column_names": schema.names,
        "temporal_bounds": time_bounds,
        "unique_rows_verified": False,
        "storage_class": "source_or_source_projection_keep",
        "eviction_authorized_by_inventory": False,
    }


def inventory_public_information(*, output_dir, repo=REPO):
    """Extend the source inventory without APIs, data writes or big-table scans."""
    import subprocess
    from collections import Counter
    from scripts.export_data_acquisition_inventory import rows_from_snapshot

    output_dir.mkdir(parents=True, exist_ok=True)
    started = time.time_ns()
    monitor = json.loads(
        (repo / "artifacts/live/data_monitor/public_status.json").read_text()
    )
    sources = rows_from_snapshot(monitor)
    for row in sources:
        row["information_category"] = public_information_category(
            row["id"], row["title"], row["provider"]
        )
        row["row_counts_additive"] = False
        row["eviction_authorized_by_inventory"] = False
    dump_csv(output_dir / "public_source_registry.csv", sources)
    tables, errors = [], []
    public_root = repo / "data_tw_public"
    paths = sorted(public_root.glob("*.parquet"))
    for subtree in ("supplemental", "mops_xbrl"):
        paths.extend(sorted((public_root / subtree).rglob("*.parquet")))
    seen = set()
    for path in paths:
        try:
            item = public_table_footer(path)
            item["table"] = str(path.relative_to(public_root))
            item["information_category"] = public_information_category(item["table"])
            identity = item["device"], item["inode"]
            item["new_unique_inode_bytes"] = (
                item["allocated_bytes"] if identity not in seen else 0
            )
            seen.add(identity)
            tables.append(item)
        except Exception as exc:
            errors.append({"path": str(path), "error": str(exc)})
    dump_csv(output_dir / "public_tables.csv", tables)
    # Provider roots are deliberately non-overlapping. Include receipts/raw
    # originals; don't conflate these storage bytes with analytical table bytes.
    roots = (
        "data_tw_public/raw",
        "data_tw_public/raw_empty",
        "data_tw_public/raw_failures",
        "data_tw_public/mops_xbrl",
        "data_tw_public/metadata",
        "data_finlab",
        "data_finmind",
        "data_public_economic",
        "data_free_public",
        "data_openBB",
        "data_fred_crypto_macro",
        "data_cftc_legacy",
        "data_coinmetrics_community",
        "data_crypto_historical_public",
        "data_crypto_reference",
    )
    storage = []
    for relative in roots:
        path = repo / relative
        if not path.exists():
            storage.append({"path": relative, "exists": False, "allocated_bytes": None})
            continue
        result = subprocess.run(
            ["du", "-B1", "-s", str(path)], capture_output=True, text=True
        )
        storage.append(
            {
                "path": relative,
                "resolved_path": str(path.resolve()),
                "exists": True,
                "allocated_bytes": int(result.stdout.split()[0])
                if result.returncode == 0
                else None,
                "measurement_error": result.stderr.strip() or None,
                "eviction_authorized_by_inventory": False,
            }
        )
    dump_csv(output_dir / "provider_storage.csv", storage)
    categories = []
    for category in sorted({row["information_category"] for row in sources + tables}):
        selected = [row for row in tables if row["information_category"] == category]
        categories.append(
            {
                "category": category,
                "registry_entries_including_aliases": sum(
                    row["information_category"] == category for row in sources
                ),
                "tw_tables": len(selected),
                "table_bytes": sum(row["bytes"] for row in selected),
                "unique_inode_allocated_bytes": sum(
                    row["new_unique_inode_bytes"] for row in selected
                ),
                "table_footer_rows_not_unique_observations": sum(
                    row["rows"] for row in selected
                ),
                "examples": [row["table"] for row in selected[:8]],
            }
        )
    dump_csv(output_dir / "public_categories.csv", categories)
    summary = {
        "schema_version": 1,
        "started_ns": started,
        "completed_ns": time.time_ns(),
        "source_registry_at_utc": monitor.get("generated_at_utc"),
        "registry_entries": len(sources),
        "registry_by_provider": dict(Counter(str(row["provider"]) for row in sources)),
        "tw_tables": len(tables),
        "categories": categories,
        "provider_storage": storage,
        "errors": errors,
        "all_history_complete_claim": False,
        "training_eligible_samples_claim": False,
        "eviction_authorized_by_inventory": False,
        "limitations": [
            "Category routing is not semantic/content deduplication; aliases and provider facts are not additive.",
            "TW tables use footer rows, not exact unique economic keys; provider record counts retain their original receipt basis.",
            "Historical revisions, original releases, licenses and collection receipts are preserved.",
            "Live sources are not globally frozen; table signatures are checked individually.",
            "Provider storage includes price and non-price data; it is not an additive non-price byte total.",
        ],
    }
    atomic_write_json(output_dir / "public_summary.json", summary)
    print(
        json.dumps(
            {
                "public_summary": str(output_dir / "public_summary.json"),
                "tables": len(tables),
                "registry_entries": len(sources),
                "errors": len(errors),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    return summary


def unique_time_union_count(arrays):
    """Exact union without sorting already-ordered long candle histories.

    Monotonicity is checked, not assumed. A main file plus a small hot tail
    only needs adjacent deduplication and binary searches for the tail keys.
    """
    ordered = [
        np.sort(values) if np.any(values[1:] < values[:-1]) else values
        for values in arrays
        if len(values)
    ]
    if not ordered:
        return 0
    if len(ordered) > 2:
        return len(np.unique(np.concatenate(ordered)))
    base = ordered[0]
    count = len(base) - np.count_nonzero(base[1:] == base[:-1])
    if len(ordered) == 1:
        return int(count)
    tail = ordered[1]
    tail = tail[np.r_[True, tail[1:] != tail[:-1]]]
    positions = np.searchsorted(base, tail)
    inside = positions < len(base)
    overlap = np.count_nonzero(base[positions[inside]] == tail[inside])
    return int(count + len(tail) - overlap)


def storage_class(dataset):
    """Describe ownership, never infer deletion authority from a filename."""
    if dataset == "tw-public-features":
        return "service_projection_keep_until_consumer_migration"
    if dataset in {
        "bybit-1m",
        "tw-stocks-day",
        "us-stocks-day",
        "forex-yahoo",
        "forex-ecb",
        "forex-pepperstone",
    }:
        return "normalized_source_keep"
    if dataset in {
        "binance-1m",
        "okx-1m",
        "tw-futures-daily",
        "tw-options-daily",
        "tw-index-futures-daily",
        "tw-stock-futures-minute",
        "tw-stock-futures-minute-v2",
    }:
        return "source_and_projection_mixed_do_not_delete_whole"
    if dataset == "tw-derivatives-bidask":
        return "configured_projection_availability_unconfirmed"
    if dataset in {
        "tw-minute",
        "tw-minute-v5",
        "tw-hft",
        "bybit-perpetual-daily",
        "bybit-venue-features",
        "tw-derivatives-decisions",
        "tw-preopen-core-54",
        "tw-research-v1",
        "tw-research-v2",
        "tw-research-v3",
        "tw-research-v4",
        "tw-feature-research-20260928",
        "tw-futures-verified-2011",
        "tw-futures-verified-2021",
    }:
        return "remote_rebuild_candidate_not_eviction_proof"
    return "unclassified_keep_until_source_role_proven"


def specs():
    # Explicit analytical scope, not a publication or retirement registry.
    return [
        (
            "tw-stocks-day",
            "台股官方日線",
            "data_tw_public/stocks",
            "*_features.parquet",
            "symbol-date",
            "primary",
        ),
        (
            "tw-public-features",
            "台股正式公開特徵",
            "data_tw_public/features/tw_public_stock_daily.parquet",
            "",
            ["date", "symbol"],
            "feature-sidecar",
        ),
        (
            "tw-minute",
            "台股一分鐘正式成品",
            "data_tw_minute/research_dataset",
            "trade_date=*/data.parquet",
            None,
            "primary",
        ),
        (
            "tw-minute-v5",
            "台股分鐘 developing-v5",
            "data_tw_minute/research_dataset_developing_v5",
            "trade_date=*/data.parquet",
            None,
            "overlapping-version",
        ),
        (
            "tw-hft",
            "台股微結構秒級成品",
            "data_tw_microstructure/hft_dataset",
            "trade_date=*/data.parquet",
            None,
            "separate-grain",
        ),
        (
            "tw-futures-daily",
            "全商品期貨實體契約日線",
            "data_tw_futures/taifex_portfolio_daily_v4/continuous_daily.parquet",
            "",
            ["date", "product", "contract", "tenor_rank", "session"],
            "primary",
        ),
        (
            "tw-index-futures-daily",
            "台指期日盤實體契約",
            "data_tw_index_futures/day_session_contracts.parquet",
            "",
            None,
            "subset-of-futures",
        ),
        (
            "tw-futures-verified-2011",
            "TX/MTX已驗證日線2011版",
            "artifacts/markets/tw_futures_v8_margin_preparation/verified_scope_2011_20260928/daily/continuous_daily.parquet",
            "",
            None,
            "overlapping-version",
        ),
        (
            "tw-futures-verified-2021",
            "TX/MTX已驗證日線2021版",
            "artifacts/markets/tw_futures_v8_margin_preparation/verified_scope_20260927_v3/daily/continuous_daily.parquet",
            "",
            None,
            "overlapping-version",
        ),
        (
            "tw-stock-futures-minute",
            "個股期貨分鐘v1",
            "data_tw_futures/taifex_stock_futures_minute_v1/minutes.parquet",
            "",
            None,
            "separate-grain",
        ),
        (
            "tw-stock-futures-minute-v2",
            "個股期貨歷史分鐘v2",
            "data_tw_futures/taifex_stock_futures_minute_history_v2/all_minutes.parquet",
            "",
            None,
            "overlapping-version-pending-audit",
        ),
        (
            "tw-options-daily",
            "台指選擇權日線",
            "data_tw_index_options_daily",
            "*.parquet",
            None,
            "source-and-derived-tables",
        ),
        (
            "tw-derivatives-decisions",
            "台指期權事後成交研究表",
            "data_tw_index_derivatives_ticks/strategy_dataset",
            "trading_date=*/decisions.parquet",
            None,
            "research-only",
        ),
        (
            "tw-derivatives-bidask",
            "台指期權BidAsk配置成品",
            "data_tw_index_derivatives_ticks/strategy_dataset_bidask",
            "trading_date=*/decisions.parquet",
            None,
            "configured-may-be-missing",
        ),
        (
            "bybit-1m",
            "Bybit永續一分鐘",
            "data_bybit/1m",
            "*_features.parquet",
            "symbol-date-tail",
            "primary",
        ),
        (
            "okx-1m",
            "OKX永續一分鐘",
            "data_okx/1m",
            "*_features.parquet",
            "symbol-date-tail",
            "primary",
        ),
        (
            "binance-1m",
            "Binance USD-M一分鐘",
            "data_binance/1m",
            "*_features.parquet",
            "symbol-date-tail",
            "primary",
        ),
        (
            "bybit-perpetual-daily",
            "Bybit線性USDT日訓練表",
            "data_bybit/perpetual_daily",
            "*_features.parquet",
            "symbol-date",
            "derived-from-bybit-1m",
        ),
        (
            "bybit-venue-features",
            "Bybit單站funding日特徵",
            "data_bybit/public_features/bybit_venue_daily.parquet",
            "",
            ["date", "symbol"],
            "feature-sidecar",
        ),
        (
            "us-stocks-day",
            "Yahoo美股日線",
            "data_yahoo/us_stocks",
            "*_features.parquet",
            "symbol-date",
            "primary",
        ),
        (
            "forex-yahoo",
            "Yahoo外匯日線",
            "data_yahoo/forex",
            "*_features.parquet",
            "symbol-date",
            "primary",
        ),
        (
            "forex-ecb",
            "ECB交叉匯率日線",
            "data_forex_frankfurter",
            "*_features.parquet",
            "symbol-date",
            "alternative-price-source",
        ),
        (
            "forex-pepperstone",
            "Pepperstone歷史日線",
            "data_peperstone",
            "**/*_features.parquet",
            None,
            "mixed-asset-source",
        ),
        (
            "tw-preopen-core-54",
            "台股27值＋27缺值標記核心",
            "artifacts/datasets/tw_day_trade_features_20260928_v1/preopen_core_features.parquet",
            "",
            ["date", "symbol"],
            "derived-feature-projection",
        ),
        (
            "tw-research-v1",
            "台股寬研究特徵v1",
            "data_tw_public/features/tw_public_research_wide_2014_v1.parquet",
            "",
            ["date", "symbol"],
            "research-overlapping-version",
        ),
        (
            "tw-research-v2",
            "台股寬研究＋TAIFEX v2",
            "artifacts/research_features/tw_public_research_wide_2014_taifex_v2.parquet",
            "",
            ["date", "symbol"],
            "research-overlapping-version",
        ),
        (
            "tw-research-v3",
            "台股全已觀測研究v3",
            "artifacts/research_features/tw_public_research_all_2014_v3.parquet",
            "",
            ["date", "symbol"],
            "research-overlapping-version",
        ),
        (
            "tw-research-v4",
            "台股FinLab研究v4",
            "artifacts/research_features/tw_public_research_finlab_2014_v4.parquet",
            "",
            ["date", "symbol"],
            "research-overlapping-version",
        ),
        (
            "tw-feature-research-20260928",
            "台股636通道研究成品",
            "artifacts/datasets/tw_day_trade_features_20260928_v1/research_features.parquet",
            "",
            ["date", "symbol"],
            "research-overlapping-version",
        ),
    ]


def inventory_one(spec, all_files, global_inodes):
    dataset, title, value, pattern, keys, role = spec
    root = REPO / value
    files = (
        sorted(root.glob(pattern))
        if pattern and root.is_dir()
        else ([root] if root.is_file() else [])
    )
    if keys == "symbol-date-tail" and root.is_dir():
        files += sorted((root / "_hot_tail").glob("*_features.parquet"))
    row = {
        "dataset": dataset,
        "title": title,
        "source": str(root.resolve()),
        "role": role,
        "storage_class": storage_class(dataset),
        "eviction_authorized_by_inventory": False,
        "exists": root.exists(),
        "files": len(files),
        "bytes": 0,
        "allocated_bytes": 0,
        "unique_inode_allocated_bytes": 0,
        "new_globally_seen_inode_bytes": 0,
        "footer_rows": 0,
        "unique_key_rows": None,
        "duplicate_key_rows": None,
        "first": None,
        "last": None,
        "errors": [],
        "key_check": "not_scanned",
        "symbols": None,
    }
    metas, local_inodes, buckets = [], set(), defaultdict(list)
    inputs = defaultdict(list)
    for path in files:
        inputs[path.name].append(path)
    # Read footer and keys in the same small per-symbol window, rather than
    # reading all footers before scanning ever-changing hot-tail keys.
    key_count = null_count = 0
    for symbol_index, (symbol, paths) in enumerate(inputs.items(), 1):
        arrays = []
        for path in paths:
            try:
                meta, names = footer(path)
                if isinstance(keys, str):
                    values, nulls = timestamp_keys(path)
                    current = path.stat()
                    if (
                        meta["device"],
                        meta["inode"],
                        meta["bytes"],
                        meta["mtime_ns"],
                    ) != signature(current):
                        raise RuntimeError("timestamp source changed while scanning")
                    arrays.append(values)
                    null_count += nulls
                meta.update(dataset=dataset)
                all_files.append(meta)
                metas.append(meta)
                row["bytes"] += meta["bytes"]
                row["allocated_bytes"] += meta["allocated_bytes"]
                inode = meta["device"], meta["inode"]
                if inode not in local_inodes:
                    row["unique_inode_allocated_bytes"] += meta["allocated_bytes"]
                    local_inodes.add(inode)
                if inode not in global_inodes:
                    row["new_globally_seen_inode_bytes"] += meta["allocated_bytes"]
                    global_inodes.add(inode)
                row["footer_rows"] += meta["rows"]
                buckets[path.name].append((path, signature(path.stat())))
            except Exception as error:
                row["errors"].append({"path": str(path), "error": str(error)})
        if isinstance(keys, str) and arrays:
            key_count += unique_time_union_count(arrays)
        if isinstance(keys, str) and symbol_index % 250 == 0 and len(inputs) < 3000:
            print(
                json.dumps(
                    {
                        "dataset": dataset,
                        "keys_checked_symbols": symbol_index,
                        "total_symbols": len(inputs),
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
    lows = [m["first"] for m in metas if m["first"]]
    highs = [m["last"] for m in metas if m["last"]]
    row.update(first=min(lows) if lows else None, last=max(highs) if highs else None)
    if isinstance(keys, str) and files:
        if not row["errors"]:
            row.update(
                unique_key_rows=key_count,
                duplicate_key_rows=row["footer_rows"] - key_count - null_count,
                key_check="exact filename-symbol x date union, including matching hot tails",
                symbols=len(buckets),
                null_key_rows=null_count,
            )
    elif isinstance(keys, list) and len(files) == 1 and not row["errors"]:
        try:
            path, before = next(iter(buckets.values()))[0]
            valid_key = pl.all_horizontal(pl.col(key).is_not_null() for key in keys)
            actual, nulls = (
                pl.scan_parquet(path)
                .select(
                    pl.struct(keys).filter(valid_key).n_unique().alias("keys"),
                    (~valid_key).sum().alias("nulls"),
                )
                .collect(engine="streaming")
                .row(0)
            )
            if before != signature(path.stat()):
                raise RuntimeError("feature source changed during key scan")
            row.update(
                unique_key_rows=actual,
                null_key_rows=nulls,
                duplicate_key_rows=row["footer_rows"] - actual - nulls,
                key_check="exact non-null " + ",".join(keys),
            )
        except Exception as error:
            row["errors"].append({"key_scan": str(error)})
    row["observed_at_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    atomic_write_json(OUTPUT / f"{dataset}.json", row)
    progress = {
        k: row[k]
        for k in (
            "dataset",
            "files",
            "bytes",
            "footer_rows",
            "unique_key_rows",
            "first",
            "last",
        )
    }
    progress.update(error_count=len(row["errors"]), error_examples=row["errors"][:3])
    print(json.dumps(progress, ensure_ascii=False), flush=True)
    return row


def run(*, selected_datasets=None):
    os.nice(15)
    started = time.time_ns()
    refs, config_errors = configs()
    rows, all_files, seen = [], [], set()
    selected_specs = [
        spec
        for spec in specs()
        if not selected_datasets or spec[0] in selected_datasets
    ]
    for spec in selected_specs:
        rows.append(inventory_one(spec, all_files, seen))
        atomic_write_json(
            OUTPUT / "progress.json",
            {
                "started_ns": started,
                "completed_datasets": len(rows),
                "total_datasets": len(selected_specs),
            },
        )
    dump_csv(OUTPUT / "datasets.csv", rows)
    dump_csv(OUTPUT / "files.csv", all_files)
    # Source registry is supplementary evidence, never an additive ML sample total.
    monitor = json.loads(
        (REPO / "artifacts/live/data_monitor/public_status.json").read_text()
    )
    from scripts.export_data_acquisition_inventory import rows_from_snapshot

    source_rows = rows_from_snapshot(monitor)
    dump_csv(OUTPUT / "source_registry.csv", source_rows)
    summary = {
        "started_ns": started,
        "completed_ns": time.time_ns(),
        "scope": "penguin local logical training inputs; no remote inventory, cold fetch or data deletion",
        "storage_policy": "docs/agents/storage.md#penguin-source-host-and-remote-training-ownership",
        "selected_datasets": [spec[0] for spec in selected_specs],
        "datasets": rows,
        "source_registry_rows": len(source_rows),
        "source_registry_at_utc": monitor.get("generated_at_utc"),
        "resolved_config_references": len(refs),
        "config_errors": config_errors,
        "dedup_definition": "same physical inode counted once; hot-tail keys unioned per symbol; overlapping derived versions never summed as independent training rows",
        "global_unique_allocated_data_bytes": sum(
            row["new_globally_seen_inode_bytes"] for row in rows
        ),
        "cross_dataset_content_hash_dedup_verified": False,
        "training_eligible_samples_claim": False,
        "limitations": [
            "Live sources are not globally frozen; per-file signatures are checked.",
            "Footer rows without unique_key_rows have not undergone a whole-key uniqueness audit.",
            "Different providers, price adjustments, decision clocks and feature ABIs are not assumed interchangeable.",
            "Data-file bytes exclude raw inputs, caches, source receipts and compressed D cold replicas.",
        ],
    }
    atomic_write_json(OUTPUT / "summary.json", summary)
    print(
        json.dumps(
            {key: value for key, value in summary.items() if key != "datasets"},
            ensure_ascii=False,
        ),
        flush=True,
    )
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUT,
        help="Analysis reports only; no source files are modified.",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        choices=[spec[0] for spec in specs()],
        help="Restrict this report to a named dataset; repeat as needed.",
    )
    parser.add_argument(
        "--public-information",
        action="store_true",
        help="Inventory public information, macro originals and provider storage instead of candle/training tables.",
    )
    args = parser.parse_args()
    if args.public_information and args.dataset:
        parser.error("--public-information cannot be combined with --dataset")
    OUTPUT = args.output_dir.resolve()
    OUTPUT.mkdir(parents=True, exist_ok=True)
    if args.public_information:
        os.nice(15)
        inventory_public_information(output_dir=OUTPUT)
    else:
        run(selected_datasets=args.dataset)

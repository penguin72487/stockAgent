"""Bounded, footer-only inventory for the public data monitor.

This is an observation of stored Parquet rows, not an acquisition or
point-in-time availability receipt.  The 30-second status snapshot refreshes
only a bounded number of changed footers; HTTP requests only read the cache.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any, Mapping
from uuid import uuid4


INVENTORY_VERSION = 10
DEFAULT_REFRESH_FILES = 4_096
_DATE_COLUMNS = (
    "snapshot_ts_ns", "event_ts", "event_ts_utc", "ts", "event_date",
    "date", "trade_date", "trading_date", "settlement_date",
    "observation_date", "subject_date", "report_date_utc", "report_date_as_yyyy_mm_dd",
    "filing_date", "filed_date", "report_period_end", "period_end", "as_of_date",
    "published_on", "published_at_taipei", "datetime", "timestamp", "time", "event_time",
    "observed_at_utc", "published_at_utc",
)
_DATE_PREFIX = re.compile(r"^\d{4}-\d{2}-\d{2}(?:[T ])?")
_SAFE_NAME = re.compile(r"^[A-Za-z0-9_\-]+$")
_CRYPTO_EXCHANGES = ("okx", "bybit", "binance")
_YAHOO_CLASSES = ("tw_stocks", "us_stocks", "crypto", "forex")

# Physical families are deliberately explicit.  Raw retries, alternate views,
# caches and SEC filing vintages must not be silently interpreted as unique
# market observations.  Families marked primary alone contribute to a group
# rollup; all families remain separately inspectable in the dashboard.
PHYSICAL_FAMILIES: dict[str, tuple[str, str, str, bool]] = {
    "tw-public:training-features": ("tw-public", "臺灣公開資料逐股訓練特徵", "data_tw_public/features/tw_public_stock_daily.parquet", False),
    "tw-public:stock-features": ("tw-public", "TWSE／TPEx 個股日資料特徵", "data_tw_public/stocks/*_features.parquet", False),
    "tw-public:shioaji-stock-features": ("tw-public", "永豐個股日資料特徵／補充來源", "data_tw_public/shioaji/stocks/*_features.parquet", False),
    "tw-futures:model-features": ("tw-futures", "臺灣期貨組合模型特徵", "data_tw_futures/taifex_portfolio_daily/model_features.parquet", False),
    "tw-futures:symbol-features": ("tw-futures", "臺灣期貨逐合約日資料特徵", "data_tw_futures/taifex_portfolio_daily/symbols/*_features.parquet", False),
    "tw-futures:model-features-v4": ("tw-futures", "臺灣期貨組合模型特徵 v4", "data_tw_futures/taifex_portfolio_daily_v4/model_features.parquet", False),
    "tw-futures:symbol-features-v4": ("tw-futures", "臺灣期貨逐槽位日資料特徵 v4", "data_tw_futures/taifex_portfolio_daily_v4/symbols/*_features.parquet", False),
    "legacy:stock-features": ("legacy-parquet", "舊版台股個股日特徵／可能與現行來源重疊", "data_parquet/*_features.parquet", False),
    "okx:hot-tail": ("okx", "OKX 近期 1 分 K／特徵 hot tail（與基底重疊，不加總）", "data_okx/1m/_hot_tail/*_features.parquet", False),
    "okx:daily": ("okx", "OKX 每日行情衍生表", "data_okx/daily/*_features.parquet", False),
    "okx:legacy-root": ("okx", "OKX 舊版根目錄行情特徵", "data_okx/*_features.parquet", False),
    "bybit:daily": ("bybit", "Bybit 每日行情衍生表", "data_bybit/daily/*_features.parquet", False),
    "bybit:hot-tail": ("bybit", "Bybit 近期 1 分 K hot tail（與基底重疊，不加總）", "data_bybit/1m/_hot_tail/*_features.parquet", False),
    "bybit:perpetual-daily": ("bybit", "Bybit 永續合約每日交易特徵", "data_bybit/perpetual_daily/*_features.parquet", False),
    "bybit:legacy-root": ("bybit", "Bybit 舊版根目錄行情特徵", "data_bybit/*_features.parquet", False),
    "binance:daily": ("binance", "Binance 每日行情衍生表", "data_binance/daily/*_features.parquet", False),
    "binance:hot-tail": ("binance", "Binance 近期 1 分 K／特徵 hot tail（與基底重疊，不加總）", "data_binance/1m/_hot_tail/*_features.parquet", False),
    "binance:legacy-root": ("binance", "Binance 舊版根目錄行情特徵", "data_binance/*_features.parquet", False),
    "openbb:archives": ("openbb-compact", "OpenBB 已驗證端點封存", "data_openBB/compact/**/archive.parquet", True),
    "tw-futures:all-contracts": ("tw-index-futures", "TAIFEX 全期貨日資料", "data_tw_index_futures/all_futures_daily_sessions.parquet", True),
    "tw-futures:day-session": ("tw-index-futures", "TAIFEX 日盤合約視圖", "data_tw_index_futures/day_session_contracts.parquet", False),
    "tw-futures:front-month": ("tw-index-futures", "TAIFEX 近月視圖", "data_tw_index_futures/day_session_front_month.parquet", False),
    "tw-options:monthly-chain": ("tw-index-options-daily", "TXO 月選全鏈", "data_tw_index_options_daily/monthly_full_chain.parquet", True),
    "tw-options:weekly-chain": ("tw-index-options-daily", "TXO 週選全鏈", "data_tw_index_options_daily/weekly_full_chain.parquet", True),
    "tw-options:monthly-atm": ("tw-index-options-daily", "TXO 月選開盤 ATM 衍生表", "data_tw_index_options_daily/monthly_opening_atm_pairs.parquet", False),
    "tw-options:weekly-atm": ("tw-index-options-daily", "TXO 週選開盤 ATM 衍生表", "data_tw_index_options_daily/weekly_nearest_expiry_opening_atm_pairs.parquet", False),
    "tw-options:settlement": ("tw-index-options-daily", "TXO 最後結算價", "data_tw_index_options_daily/txo_final_settlement_history.parquet", False),
    "taifex-public:normalized": ("taifex-public-history", "TAIFEX 籌碼／風險正規化表", "data_taifex_public_history/normalized/*.parquet", True),
    "taifex-ticks:tx": ("tw-index-derivatives-ticks", "台指期逐筆成交", "data_tw_index_derivatives_ticks/tx/trading_date=*/transactions.parquet", True),
    "taifex-ticks:txo": ("tw-index-derivatives-ticks", "台指選逐筆成交", "data_tw_index_derivatives_ticks/txo/trading_date=*/transactions.parquet", True),
    "cftc:legacy": ("cftc-legacy-pre2000", "CFTC 2000 年前 Legacy 合併主表", "data_cftc_legacy/normalized/legacy_pre2000.parquet", True),
    "cftc:futures-only": ("cftc-legacy-pre2000", "CFTC 僅期貨視圖", "data_cftc_legacy/normalized/legacy_futures_only_pre2000.parquet", False),
    "cftc:combined": ("cftc-legacy-pre2000", "CFTC 期貨加選擇權視圖", "data_cftc_legacy/normalized/legacy_futures_and_options_combined_pre2000.parquet", False),
    "free-public:observations": ("free-public-context", "免費公開資料觀測主表", "data_free_public/observations.parquet", True),
    "fred:observations": ("fred-crypto-macro", "FRED 發布版本觀測表", "data_fred_crypto_macro/observations.parquet", True),
    "dune:cex-flows": ("dune-crypto", "Dune 交易所資金流", "data_dune_crypto/normalized/cex_labeled_flows/year=*/*.parquet", True),
    "dune:dex-activity": ("dune-crypto", "Dune DEX 活動", "data_dune_crypto/normalized/dex_asset_activity/year=*/*.parquet", True),
    "dune:stablecoin": ("dune-crypto", "Dune 穩定幣發行／銷毀", "data_dune_crypto/normalized/stablecoin_mint_burn/year=*/*.parquet", True),
    "etf:issuer-metrics": ("crypto-etf-history", "ETF 發行商每日指標", "data_crypto_etf/normalized/issuer_daily_fund_metrics.parquet", True),
    "etf:issuer-holdings": ("crypto-etf-history", "ETF 發行商持股快照", "data_crypto_etf/normalized/issuer_holdings_snapshots.parquet", True),
    "etf:issuer-reserves": ("crypto-etf-history", "ETF 發行商儲備快照", "data_crypto_etf/normalized/issuer_reserve_snapshots.parquet", True),
    "etf:sec-filings": ("crypto-etf-history", "SEC 申報紀錄", "data_crypto_etf/normalized/sec/*/filings.parquet", True),
    "etf:sec-companyfacts": ("crypto-etf-history", "SEC 公司財報事實", "data_crypto_etf/normalized/sec/*/companyfacts.parquet", True),
    "crypto-public:normalized": ("crypto-historical-public", "加密歷史公開資料正規化表", "data_crypto_historical_public/normalized/*.parquet", True),
    "binance-archive:lifecycle": ("binance-public-archive", "Binance 合約生命週期", "data_binance_archive/instrument_lifecycle.parquet", True),
    "pepperstone:forex": ("forex-pepperstone", "Pepperstone 外匯行情", "data_peperstone/fores/*_features.parquet", True),
    "pepperstone:crypto": ("forex-pepperstone", "Pepperstone 加密貨幣行情", "data_peperstone/crypto/*_features.parquet", True),
    "pepperstone:commodities": ("forex-pepperstone", "Pepperstone 大宗商品行情", "data_peperstone/commodites/*_features.parquet", True),
    "pepperstone:other": ("forex-pepperstone", "Pepperstone 24 小時市場行情", "data_peperstone/24hTrading/*_features.parquet", True),
}


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, UnicodeError):
        return None


def _date_text(value: Any, *, column: str) -> str | None:
    if column == "snapshot_ts_ns" and isinstance(value, int):
        try:
            return datetime.fromtimestamp(value / 1_000_000_000, tz=UTC).isoformat()
        except (OverflowError, ValueError, OSError):
            return None
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat() if value.tzinfo else value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    if isinstance(value, str) and _DATE_PREFIX.match(value):
        stamp = value.strip().replace(" ", "T", 1)
        if "T" in stamp and (stamp.endswith("Z") or re.search(r"[+-]\d{2}:\d{2}$", stamp)):
            try:
                return datetime.fromisoformat(stamp.replace("Z", "+00:00")).astimezone(UTC).isoformat()
            except ValueError:
                return None
        return stamp
    return None


def parquet_footer_stats(path: Path) -> dict[str, Any] | None:
    """Return exact stored-row count and date bounds only when footer proves them."""

    try:
        import pyarrow.parquet as pq

        parquet = pq.ParquetFile(path)
        metadata = parquet.metadata
        fields = [(field.name, str(field.type)) for field in parquet.schema_arrow]
        columns = [name for name, _ in fields]
        flat_schema = all(
            metadata.row_group(group_index).num_columns == len(fields)
            for group_index in range(metadata.num_row_groups)
        )
        non_null: list[int | None] = []
        for column_index in range(len(fields)):
            known = flat_schema
            count = 0
            for group_index in range(metadata.num_row_groups) if flat_schema else ():
                group = metadata.row_group(group_index)
                stat = group.column(column_index).statistics
                if stat is None or stat.null_count is None:
                    known = False
                    break
                count += group.num_rows - stat.null_count
            non_null.append(count if known else None)
        schema_id = hashlib.sha256(
            json.dumps(fields, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        date_column = next((name for name in _DATE_COLUMNS if name in columns), None) if flat_schema else None
        if date_column is None:
            return {"count": metadata.num_rows, "first": None, "last": None, "time_column": None,
                    "schema_id": schema_id, "fields": fields, "non_null": non_null}
        column_index = columns.index(date_column)
        lows: list[str] = []
        highs: list[str] = []
        complete_bounds = metadata.num_row_groups > 0
        for index in range(metadata.num_row_groups):
            group = metadata.row_group(index)
            if group.num_rows == 0:
                continue
            stat = group.column(column_index).statistics
            low = _date_text(stat.min, column=date_column) if stat is not None and stat.has_min_max else None
            high = _date_text(stat.max, column=date_column) if stat is not None and stat.has_min_max else None
            if low is None or high is None:
                complete_bounds = False
            else:
                lows.append(low)
                highs.append(high)
        return {
            "count": metadata.num_rows,
            "first": min(lows) if complete_bounds and lows else None,
            "last": max(highs) if complete_bounds and highs else None,
            "time_column": date_column,
            "schema_id": schema_id,
            "fields": fields,
            "non_null": non_null,
        }
    except (OSError, ValueError, RuntimeError, ImportError):
        return None


def _selected_files(root: Path) -> dict[str, list[Path]]:
    selected: dict[str, list[Path]] = {}
    base = root / "data_tw_public"
    manifest = _read_json(base / "dataset_manifest.json")
    names = {
        str(row.get("name"))
        for row in manifest
        if isinstance(row, Mapping) and _SAFE_NAME.fullmatch(str(row.get("name") or ""))
    } if isinstance(manifest, list) else set()
    names.update(("dgbas_release_vintages", "cbc_fx_reserve_release_vintages",
                  "cbc_money_release_vintages", "gcis_open_data_catalog",
                  "fsc_open_data_catalog"))
    for name in sorted(names):
        path = base / f"{name}.parquet"
        selected[f"tw-public:{name}"] = [path] if path.is_file() else []
    overnight_path = base / "supplemental/cbc_overnight_official_pages.parquet"
    selected["tw-public:cbc_overnight_official_pages"] = [overnight_path] if overnight_path.is_file() else []
    annual_path = base / "supplemental/cbc_usdtwd_annual_pages.parquet"
    selected["tw-public:cbc_usdtwd_annual_pages"] = [annual_path] if annual_path.is_file() else []
    mof_dates_path = base / "supplemental/mof_macro_release_dates.parquet"
    selected["tw-public:mof_macro_release_dates"] = [mof_dates_path] if mof_dates_path.is_file() else []
    trade_pdf_path = base / "supplemental/mof_trade_release_values.parquet"
    selected["tw-public:mof_trade_release_values"] = [trade_pdf_path] if trade_pdf_path.is_file() else []
    original_mof_path = base / "supplemental/mof_original_release_archive.parquet"
    selected["tw-public:mof_original_release_archive"] = [original_mof_path] if original_mof_path.is_file() else []
    provisional_path = root / "artifacts/data_quality/tw_public_provisional_macro/events.parquet"
    selected["tw-public:provisional_macro_feature_events"] = [provisional_path] if provisional_path.is_file() else []
    selected["tw-public:mops_xbrl_quarterly"] = sorted(
        base.glob("mops_xbrl/normalized/*/*/*/facts.parquet")
    )
    # One canonical research file per trading date.  Do not sum the raw
    # Shioaji minute_chunks: their overlapping retry windows double-count.
    minute_root = root / "data_tw_minute/research_dataset"
    selected["group:tw-minute-train"] = (
        sorted(minute_root.glob("trade_date=*/data.parquet"))
        if minute_root.is_dir() else []
    )
    microstructure_root = root / "data_tw_microstructure/hft_dataset"
    selected["group:tw-microstructure-train"] = (
        sorted(microstructure_root.glob("trade_date=*/data.parquet"))
        if microstructure_root.is_dir() else []
    )
    for exchange in _CRYPTO_EXCHANGES:
        directory = root / f"data_{exchange}" / "1m"
        selected[f"group:{exchange}"] = sorted(directory.glob("*_features.parquet")) if directory.is_dir() else []
    for asset_class in _YAHOO_CLASSES:
        directory = root / "data_yahoo" / asset_class
        selected[f"yahoo:{asset_class}"] = sorted(directory.glob("*_features.parquet")) if directory.is_dir() else []
    for group, relative in (
        ("forex-frankfurter", "data_forex_frankfurter"),
        ("coinmetrics-community", "data_coinmetrics_community/assets"),
    ):
        directory = root / relative
        selected[f"group:{group}"] = (
            sorted(directory.glob("*_features.parquet"))
            if directory.is_dir() else []
        )
    for family, (_, _, pattern, _) in PHYSICAL_FAMILIES.items():
        selected[f"physical:{family}"] = sorted(
            path for path in root.glob(pattern) if path.is_file()
        )
    return selected


def _aggregate(paths: list[Path], cache: Mapping[str, Any]) -> dict[str, Any]:
    total = len(paths)
    inspected = 0
    invalid = 0
    counts = 0
    first: list[str] = []
    last: list[str] = []
    bounds_complete = True
    time_columns: set[str] = set()
    for path in paths:
        entry = cache.get(str(path))
        if not isinstance(entry, Mapping):
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        if entry.get("size") != stat.st_size or entry.get("mtime_ns") != stat.st_mtime_ns:
            continue
        if entry.get("error") == "invalid_parquet":
            invalid += 1
            continue
        if not isinstance(entry.get("stats"), Mapping):
            continue
        info = entry["stats"]
        if not isinstance(info.get("count"), int):
            continue
        inspected += 1
        counts += info["count"]
        if info.get("first") and info.get("last"):
            first.append(str(info["first"]))
            last.append(str(info["last"]))
            time_columns.add(str(info.get("time_column") or ""))
        elif info["count"]:
            bounds_complete = False
    exact_count = inspected == total and total > 0
    bounds_verified = exact_count and (counts == 0 or bounds_complete and bool(first))
    return {
        "count": counts if exact_count else None,
        "verified_partial_count": counts if inspected else None,
        "first": min(first) if exact_count and bounds_complete and first else None,
        "last": max(last) if exact_count and bounds_complete and last else None,
        "time_column": next(iter(time_columns)) if len(time_columns) == 1 else None,
        "bounds_state": "verified" if bounds_verified else "unavailable" if exact_count else "partial",
        "files_inspected": inspected,
        "files_total": total,
        "invalid_files": invalid,
        "state": "verified" if exact_count else "empty" if total == 0 else "invalid" if invalid else "scanning",
        "basis": "Parquet footer：實際儲存列數與時間欄位 row-group min/max；不是發布或可訓練時間。",
    }


def _combine(parts: list[dict[str, Any]]) -> dict[str, Any]:
    files_total = sum(int(part["files_total"]) for part in parts)
    files_inspected = sum(int(part["files_inspected"]) for part in parts)
    invalid_files = sum(int(part["invalid_files"]) for part in parts)
    exact = bool(parts) and all(part["count"] is not None for part in parts)
    first = [str(part["first"]) for part in parts if part["first"]]
    last = [str(part["last"]) for part in parts if part["last"]]
    columns = {str(part.get("time_column")) for part in parts if part.get("time_column")}
    same_column = len(columns) == 1 and all(part.get("time_column") for part in parts)
    # Different temporal fields or timezone conventions have only a comparable
    # calendar-date envelope.  Their precise instants must not be mixed.
    date_only = not same_column
    return {
        "count": sum(int(part["count"]) for part in parts) if exact else None,
        "verified_partial_count": (
            sum(int(part.get("verified_partial_count") or 0) for part in parts)
            if files_inspected else None
        ),
        "first": min(value[:10] for value in first) if exact and len(first) == len(parts) and date_only else min(first) if exact and len(first) == len(parts) else None,
        "last": max(value[:10] for value in last) if exact and len(last) == len(parts) and date_only else max(last) if exact and len(last) == len(parts) else None,
        "time_column": next(iter(columns)) if same_column else None,
        "bounds_precision": "date" if date_only else "source_column",
        "bounds_state": (
            "verified" if exact and len(first) == len(parts)
            else "unavailable" if exact else "partial"
        ),
        "files_inspected": files_inspected,
        "files_total": files_total,
        "invalid_files": invalid_files,
        "state": "verified" if exact else "invalid" if invalid_files else "scanning" if files_total else "empty",
        "basis": "各子資料集 Parquet footer 合計；不同市場或資料集的列數不代表唯一交易事件。",
    }


def _build_record_inventory_unlocked(
    repo_root: Path, *, refresh: bool = False, max_refresh_files: int = DEFAULT_REFRESH_FILES
) -> dict[str, Any]:
    """Read inventory; optionally refresh at most ``max_refresh_files`` footers."""

    root = Path(repo_root)
    cache_path = root / "artifacts/live/data_monitor/record_inventory_cache.json"
    payload = _read_json(cache_path)
    if not refresh and isinstance(payload, Mapping) and payload.get("version") == INVENTORY_VERSION:
        datasets = payload.get("datasets")
        if isinstance(datasets, Mapping):
            return {
                "datasets": dict(datasets),
                "refreshed_files": 0,
                "cached_files": len(payload.get("files", {})),
            }
    cache: dict[str, Any] = (
        dict(payload.get("files", {}))
        if isinstance(payload, Mapping) and payload.get("version") in {7, INVENTORY_VERSION}
        and isinstance(payload.get("files"), Mapping)
        else {}
    )
    schemas: dict[str, Any] = (
        dict(payload.get("schemas", {}))
        if isinstance(payload, Mapping) and payload.get("version") == INVENTORY_VERSION
        and isinstance(payload.get("schemas"), Mapping) else {}
    )
    selected = _selected_files(root)
    all_paths = {str(path): path for paths in selected.values() for path in paths}
    old_keys = set(cache)
    refreshed = 0
    if refresh:
        for key, path in all_paths.items():
            try:
                stat = path.stat()
            except OSError:
                continue
            entry = cache.get(key)
            if (isinstance(entry, Mapping) and entry.get("size") == stat.st_size
                    and entry.get("mtime_ns") == stat.st_mtime_ns
                    and (isinstance(entry.get("stats"), Mapping) or entry.get("error") == "invalid_parquet")
                    and (entry.get("error") == "invalid_parquet" or (
                        entry["stats"].get("schema_id") in schemas
                        and isinstance(entry["stats"].get("non_null"), list)
                    ))
                    and not (
                        "/data_tw_microstructure/hft_dataset/" in key
                        and isinstance(entry.get("stats"), Mapping)
                        and entry["stats"].get("time_column") != "snapshot_ts_ns"
                    )):
                continue
            if refreshed >= max_refresh_files:
                break
            refreshed += 1
            stats = parquet_footer_stats(path)
            if stats is not None:
                fields = stats.pop("fields")
                schemas[stats["schema_id"]] = fields
                cache[key] = {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns, "stats": stats}
            else:
                cache[key] = {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns, "error": "invalid_parquet"}
        cache = {key: value for key, value in cache.items() if key in all_paths}
        referenced_schemas = {
            value["stats"]["schema_id"] for value in cache.values()
            if isinstance(value, Mapping) and isinstance(value.get("stats"), Mapping)
            and value["stats"].get("schema_id") in schemas
        }
        schemas = {key: value for key, value in schemas.items() if key in referenced_schemas}
        if (
            refreshed == 0
            and set(cache) == old_keys
            and isinstance(payload, Mapping)
            and isinstance(payload.get("datasets"), Mapping)
        ):
            return {
                "datasets": dict(payload["datasets"]),
                "refreshed_files": 0,
                "cached_files": len(cache),
            }
    output = {name: _aggregate(paths, cache) for name, paths in selected.items()}
    tw_parts = [value for key, value in output.items()
                if key.startswith("tw-public:") and key != "tw-public:mops_xbrl_quarterly"]
    output["group:tw-public"] = _combine(tw_parts)
    yahoo_parts = [output[f"yahoo:{asset_class}"] for asset_class in _YAHOO_CLASSES]
    output["group:yahoo-market"] = _combine(yahoo_parts)
    output["yahoo:equities"] = _combine(
        [output["yahoo:tw_stocks"], output["yahoo:us_stocks"]]
    )
    primary_by_group: dict[str, list[dict[str, Any]]] = {}
    for family, (group, _, _, primary) in PHYSICAL_FAMILIES.items():
        if primary:
            primary_by_group.setdefault(group, []).append(output[f"physical:{family}"])
    for group, parts in primary_by_group.items():
        present_parts = [part for part in parts if part["files_total"]]
        missing_families = len(parts) - len(present_parts)
        output[f"group:{group}"] = _combine(present_parts)
        output[f"group:{group}"]["missing_families"] = missing_families
        output[f"group:{group}"]["basis"] = (
            "僅合計清冊明列的主表 Parquet 儲存列；衍生視圖另列，"
            "不重複加總。不同表的列數不是唯一市場事件，亦非 PIT 可用性。"
            + (f" {missing_families} 項預期主表尚無實存檔案。" if missing_families else "")
        )
    if refresh:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = cache_path.with_name(f"{cache_path.name}.tmp.{uuid4().hex}")
        try:
            temporary.write_text(
                json.dumps({"version": INVENTORY_VERSION, "files": cache, "schemas": schemas,
                            "datasets": output}, separators=(",", ":")),
                encoding="utf-8",
            )
            os.replace(temporary, cache_path)
        finally:
            temporary.unlink(missing_ok=True)
    return {"datasets": output, "refreshed_files": refreshed, "cached_files": len(cache)}


def build_record_inventory(
    repo_root: Path, *, refresh: bool = False, max_refresh_files: int = DEFAULT_REFRESH_FILES
) -> dict[str, Any]:
    """Read the inventory, serializing refreshes across snapshot processes."""

    if not refresh:
        return _build_record_inventory_unlocked(repo_root, refresh=False)
    lock_path = Path(repo_root) / "artifacts/live/data_monitor/record_inventory_cache.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            return _build_record_inventory_unlocked(
                repo_root, refresh=True, max_refresh_files=max_refresh_files
            )
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def build_feature_inventory(repo_root: Path) -> dict[str, Any]:
    """List every observed Parquet field once per physical dataset, from cached footers.

    Non-null counts are exact only if every selected file has complete row-group
    null statistics.  Dataset time bounds are *not* field-validity bounds.
    """

    root = Path(repo_root)
    payload = _read_json(root / "artifacts/live/data_monitor/record_inventory_cache.json")
    if not isinstance(payload, Mapping) or payload.get("version") != INVENTORY_VERSION:
        return {"version": 1, "rows": [], "datasets_total": 0,
                "datasets_with_schema": 0, "state": "waiting_inventory"}
    cache = payload.get("files", {})
    schemas = payload.get("schemas", {})
    datasets = payload.get("datasets", {})
    if not isinstance(cache, Mapping) or not isinstance(schemas, Mapping):
        return {"version": 1, "rows": [], "datasets_total": 0,
                "datasets_with_schema": 0, "state": "invalid_inventory"}
    selected = _selected_files(root)
    rows: list[dict[str, Any]] = []
    datasets_with_schema = 0
    files_with_schema = 0
    for dataset, paths in selected.items():
        by_field: dict[str, dict[str, Any]] = {}
        valid_files = 0
        for path in paths:
            entry = cache.get(str(path))
            if not isinstance(entry, Mapping):
                continue
            try:
                stat = path.stat()
            except OSError:
                continue
            if entry.get("size") != stat.st_size or entry.get("mtime_ns") != stat.st_mtime_ns:
                continue
            info = entry.get("stats")
            if not isinstance(info, Mapping):
                continue
            fields = schemas.get(info.get("schema_id"))
            non_null = info.get("non_null")
            if not isinstance(fields, list) or not isinstance(non_null, list) or len(fields) != len(non_null):
                continue
            valid_files += 1
            files_with_schema += 1
            for (name, dtype), value_count in zip(fields, non_null, strict=True):
                field = by_field.setdefault(name, {
                    "types": set(), "files_with_field": 0,
                    "rows_with_field": 0, "non_null_known_files": 0,
                    "verified_non_null": 0,
                })
                field["types"].add(dtype)
                field["files_with_field"] += 1
                field["rows_with_field"] += int(info.get("count") or 0)
                if isinstance(value_count, int):
                    field["non_null_known_files"] += 1
                    field["verified_non_null"] += value_count
        if not by_field:
            continue
        datasets_with_schema += 1
        dataset_info = datasets.get(dataset, {}) if isinstance(datasets, Mapping) else {}
        if not isinstance(dataset_info, Mapping):
            dataset_info = {}
        for name, field in by_field.items():
            complete = valid_files == len(paths) and field["non_null_known_files"] == field["files_with_field"]
            rows.append({
                "dataset_id": dataset,
                "field": name,
                "types": sorted(field["types"]),
                "files_with_field": field["files_with_field"],
                "files_total": len(paths),
                "rows_with_field": field["rows_with_field"] if valid_files == len(paths) else None,
                "verified_partial_rows_with_field": field["rows_with_field"],
                "non_null_count": field["verified_non_null"] if complete else None,
                "verified_partial_non_null": field["verified_non_null"] if field["non_null_known_files"] else None,
                "non_null_known_files": field["non_null_known_files"],
                "schema_state": "verified" if valid_files == len(paths) else "partial",
                "dataset_first": dataset_info.get("first"),
                "dataset_last": dataset_info.get("last"),
                "dataset_bounds_state": dataset_info.get("bounds_state", "unavailable"),
            })
    files_total = sum(len(paths) for paths in selected.values())
    return {
        "version": 1,
        "rows": rows,
        "datasets_total": sum(bool(paths) for paths in selected.values()),
        "datasets_with_schema": datasets_with_schema,
        "files_total": files_total,
        "files_with_schema": files_with_schema,
        "state": "complete" if files_total and files_with_schema == files_total else "empty" if not files_total else "partial",
        "basis": "Parquet schema 與 row-group null_count；不代表歷史完整、發布時間或訓練可用性。",
    }


__all__ = ["PHYSICAL_FAMILIES", "build_feature_inventory", "build_record_inventory", "parquet_footer_stats"]

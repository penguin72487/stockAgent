"""Minute/daily catalog for the existing, read-only acquisition monitor.

Counts retain their source grain. Date unions are evidence, never elapsed spans.
Only the snapshot producer reads local receipts or projects a date column; HTTP
uses ``series_page`` and does no I/O. Cached date bitmaps keep warm work bounded.
"""
from __future__ import annotations

import base64
from contextlib import ExitStack
from collections import Counter
from datetime import UTC, date, datetime
from functools import lru_cache
import hashlib
import json
import os
from pathlib import Path
import time
import zlib

from stockagent.live.data_monitor_inventory import PHYSICAL_FAMILIES, _pattern_sources
from stockagent.live.tick_inventory import (
    HISTORY, KINDS, MARKETS, _Sources, _database, _db_paths, _json, _page_row, _signature,
)

VERSION = 1
GRAINS = {"daily": "日資料", "1m": "一分鐘", "tick": "逐筆／五檔", "native": "其他原生頻率"}
SERIES_KINDS = {"bar": "行情／K 棒", "feature": "特徵／統計", **KINDS}
_EPOCH = date(1800, 1, 1).toordinal()
_DATE_AUDIT_VERSION = 2
_DATE_MEMBER_POLICY = "exact-observation-date-members-v1"
_DATE_CHUNK_POLICY = "exact-date-column-chunks-v1"


def _encode(bits):
    return base64.b64encode(zlib.compress(bits.to_bytes((bits.bit_length() + 7) // 8, "little"))).decode()


def _decode(value):
    return int.from_bytes(zlib.decompress(base64.b64decode(value)), "little")


def _date_bits(values):
    bits = 0
    for value in values:
        day = value.date() if isinstance(value, datetime) else value
        if not isinstance(day, date) or not 1800 <= day.year <= 2200:
            raise ValueError("invalid observation date")
        bits |= 1 << (day.toordinal() - _EPOCH)
    return bits


def _row(key, title, provider, market, grain, *, kind="bar", role="source", **facts):
    return {"id": key, "title": title, "provider": provider, "market_category": market,
            "market_category_label": MARKETS[market], "granularity": grain,
            "granularity_label": GRAINS[grain], "kind": kind, "kind_label": SERIES_KINDS[kind],
            "role": role, "row_unit": "資料列", "source_id": None,
            "count": None, "known_count": None, "days": None, "known_days": None,
            "earliest_date": None, "latest_date": None, "symbols": None,
            "statistics_state": "unavailable", "day_statistics_state": "unavailable",
            "date_basis": "實際非空資料日期；不是日期跨度，也不是發布／可用時間。",
            "basis": "尚無可核對的資料收據；未知不當作零。", **facts}


@lru_cache(maxsize=1)
def _date_member_policy():
    import polars as pl
    import pyarrow as pa
    return f"{_DATE_MEMBER_POLICY}:arrow={pa.__version__}:polars={pl.__version__}"


def _date_member_digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _date_chunk_policy():
    return _DATE_CHUNK_POLICY + ':' + _date_member_policy()


def _date_chunk_key(stream, column, group_rows, schema, file_size):
    """Hash every compressed clock page; unknown/external layouts are misses."""
    if column.file_path or column.path_in_schema != schema[0]:
        return None
    offsets = [column.data_page_offset]
    if column.has_dictionary_page:
        offsets.append(column.dictionary_page_offset)
    if any(type(offset) is not int or offset < 4 for offset in offsets):
        return None
    start, size = min(offsets), column.total_compressed_size
    if type(size) is not int or size <= 0 or start + size > file_size - 8:
        return None
    descriptor = [_date_chunk_policy(), schema, group_rows, column.num_values,
                  column.physical_type, column.compression, column.encodings, size]
    digest = hashlib.sha256(json.dumps(descriptor, separators=(",", ":")).encode())
    stream.seek(start)
    remaining = size
    while remaining:
        body = stream.read(min(remaining, 1024**2))
        if not body:
            raise ValueError("truncated observation date column chunk")
        digest.update(body)
        remaining -= len(body)
    return digest.hexdigest()


def _previous_chunk_dates(value, key, rows):
    try:
        if (type(value) is not dict or set(value) != {"bitmap", "count", "null_dates", "sha256"}
                or type(value["count"]) is not int or value["count"] != rows
                or type(value["null_dates"]) is not int or not 0 <= value["null_dates"] <= rows
                or value["sha256"] != _date_member_digest({"key": key, **{
                    k: v for k, v in value.items() if k != "sha256"}})):
            return None
        bits = _decode(value["bitmap"])
        observed = rows - value["null_dates"]
        if (bits.bit_length() > date(2200, 12, 31).toordinal() - _EPOCH + 1
                or bits.bit_count() > observed or bool(bits) != bool(observed)):
            return None
        return bits, rows, value["null_dates"]
    except (KeyError, TypeError, ValueError, zlib.error):
        return None


def _previous_member_dates(value, *, identity, clock, rows, policy):
    """A cache is an optimization witness; any incomplete proof is a miss."""
    try:
        if (type(value) is not dict or set(value) != {
                "identity", "clock", "policy", "bitmap", "count", "null_dates", "sha256"}
                or value["identity"] != identity or value["clock"] != clock
                or value["policy"] != policy or type(value["count"]) is not int
                or value["count"] != rows or type(value["null_dates"]) is not int
                or not 0 <= value["null_dates"] <= rows
                or value["sha256"] != _date_member_digest({k: v for k, v in value.items() if k != "sha256"})):
            return None
        bits = _decode(value["bitmap"])
        observed = rows - value["null_dates"]
        if (bits.bit_length() > date(2200, 12, 31).toordinal() - _EPOCH + 1
                or bits.bit_count() > observed or bool(bits) != bool(observed)):
            return None
        return bits, rows, value["null_dates"]
    except (KeyError, TypeError, ValueError, zlib.error):
        return None


def _project_member_dates(member, metadata, name, dtype, *, chunks=None, chunk_keys=None):
    import polars as pl
    import pyarrow as pa
    import pyarrow.parquet as pq

    bits = rows = nulls = 0
    with ExitStack() as stack:
        parquet = stack.enter_context(pq.ParquetFile(member, metadata=metadata))
        stream = schema = None
        column_index = next((index for index in range(parquet.metadata.num_columns)
            if parquet.schema.column(index).path == name), None)
        if column_index is None:
            raise ValueError("observation date has no primitive Parquet column")
        for group_index in range(parquet.metadata.num_row_groups):
            group = parquet.metadata.row_group(group_index)
            stats = group.column(column_index).statistics
            # Equal bounds prove one exact date, never a date span.
            if stats is not None and stats.null_count is not None:
                if stats.null_count == group.num_rows:
                    rows += group.num_rows
                    nulls += stats.null_count
                    continue
                if stats.has_min_max:
                    low, high = (date.fromisoformat(str(v)[:10]) for v in (stats.min, stats.max))
                    if low == high:
                        bits |= _date_bits([low])
                        rows += group.num_rows
                        nulls += stats.null_count
                        continue
            key = None
            if chunks is not None:
                if stream is None:
                    stream = stack.enter_context(Path(member).open('rb'))
                    file_size = os.fstat(stream.fileno()).st_size
                    clock = parquet.schema.column(column_index)
                    field_schema = pa.schema([parquet.schema_arrow.field(name)])
                    schema = [name, hashlib.sha256(field_schema.serialize().to_pybytes()).hexdigest(),
                              clock.physical_type, clock.logical_type.to_json(), clock.converted_type,
                              clock.max_definition_level, clock.max_repetition_level]
                key = _date_chunk_key(stream, group.column(column_index), group.num_rows, schema, file_size)
                facts = _previous_chunk_dates(chunks.get(key), key, group.num_rows) if key is not None else None
                if facts is not None:
                    bits |= facts[0]
                    rows += facts[1]
                    nulls += facts[2]
                    chunk_keys.append(key)
                    continue
            group_bits = group_rows = group_nulls = 0
            for batch in parquet.iter_batches(row_groups=[group_index], columns=[name], batch_size=262144, use_threads=False):
                frame = pl.from_arrow(batch)
                column = pl.col(name)
                if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
                    column = column.str.slice(0, 10).unique().str.to_date("%Y-%m-%d", strict=True)
                else:
                    column = column.cast(pl.Date).unique()
                try:
                    values = frame.select(column.alias("day")).get_column("day")
                except pl.exceptions.PolarsError as error:
                    raise ValueError("invalid observation date column") from error
                group_rows += frame.height
                group_nulls += frame.get_column(name).null_count()
                group_bits |= _date_bits(values.drop_nulls().to_list())
            if group_rows != group.num_rows:
                raise ValueError("date projection row group accounting mismatch")
            bits |= group_bits
            rows += group_rows
            nulls += group_nulls
            if key is not None:
                value = {"bitmap": _encode(group_bits), "count": group_rows, "null_dates": group_nulls}
                value["sha256"] = _date_member_digest({"key": key, **value})
                chunks[key] = value
                chunk_keys.append(key)
    return bits, rows, nulls


def _project_dates(path, *, previous=None, chunks=None):
    """Exact clock projection, retaining canonical pointer/member proofs.

    Changed manifests reuse date-column work only for members whose complete
    identity, clock, runtime policy, row count and cached result still match.
    ParquetSource still performs its full source/content/schema verification.
    """
    import pyarrow as pa
    from stockagent.data.partitioned_parquet import ParquetSource

    before = _signature(path)
    bits = rows = nulls = 0
    member_dates = {}
    chunk_keys = []
    with ParquetSource(path) as source:
        members = [[str(member), _signature(member)] for member in source.members if member != path]
        identities = dict(members)
        names = set(source.schema.names)
        name = next((n for n in ("date", "ts", "trade_date", "trading_date", "settlement_date", "source_index")
                     if n in names), None)
        if name is None:
            raise ValueError("no observation date column")
        dtype = source.schema.field(name).type
        if not (pa.types.is_date(dtype) or pa.types.is_timestamp(dtype)
                or pa.types.is_string(dtype) or pa.types.is_large_string(dtype)):
            raise ValueError("observation clock is not an explicit date or timestamp")
        policy = _date_member_policy() if source.manifest is not None else None
        clock = [name, str(dtype)]
        previous_dates = previous.get("member_dates", {}) if isinstance(previous, dict) else {}
        if type(previous_dates) is not dict:
            previous_dates = {}
        for member, metadata in zip(source.members, source.member_footers, strict=True):
            key = str(member)
            saved = previous_dates.get(key)
            facts = (_previous_member_dates(saved, identity=identities[key], clock=clock,
                                           rows=metadata.num_rows, policy=policy)
                     if policy is not None else None)
            if facts is None:
                facts = (_project_member_dates(member, metadata, name, dtype, chunks=chunks, chunk_keys=chunk_keys)
                         if source.manifest is None and chunks is not None
                         else _project_member_dates(member, metadata, name, dtype))
                if policy is not None:
                    saved = {"identity": identities[key], "clock": clock, "policy": policy,
                             "bitmap": _encode(facts[0]), "count": facts[1], "null_dates": facts[2]}
                    saved["sha256"] = _date_member_digest(saved)
            if policy is not None:
                member_dates[key] = saved
            bits |= facts[0]
            rows += facts[1]
            nulls += facts[2]
        if rows != source.rows:
            raise ValueError("date projection row accounting mismatch")
    if _signature(path) != before:
        raise ValueError("source changed during date projection")
    result = {"bitmap": _encode(bits), "count": rows, "null_dates": nulls, "members": members}
    if policy is not None:
        result.update(member_dates=member_dates, member_date_policy=policy)
    if chunk_keys:
        result.update(date_chunk_keys=chunk_keys, date_chunk_policy=_date_chunk_policy())
    return result


class _DateCensus:
    def __init__(self, previous, seconds):
        self.previous = previous.get("dates", {})
        self.current = {}
        self.chunks = dict(previous.get("date_chunks", {})) if type(previous.get("date_chunks", {})) is dict else {}
        self.used_chunks = set()
        self.remaining_seconds = seconds
        self.cold_seconds = 0.
        self.groups = dict(previous.get("date_groups", {}))
        self.generation = previous.get("cycle", 0)
        self.reads = self.pending = self.invalid = self.hits = 0
        # Many independent partitions contain exactly the same date set. The
        # bitmap string determines only its integer; every path/member identity
        # is still checked below. Bound retention to this census, not the daemon.
        repeated = Counter(entry["bitmap"] for entry in self.previous.values()
                           if isinstance(entry, dict) and isinstance(entry.get("bitmap"), str))
        decoded = {}
        fallback = lru_cache(maxsize=256)(_decode)
        def decode_bitmap(value):
            if repeated[value] < 2:
                return fallback(value)
            if value not in decoded:
                decoded[value] = _decode(value)
            return decoded[value]
        self.decode_bitmap = decode_bitmap

    def collect(self, paths):
        bits = known = pending = invalid = nulls = 0
        for path in paths:
            key = str(path)
            signature = _signature(path)
            # A shared physical file can appear in several labelled groups.
            # Recheck its identity below, but reuse this census's completed
            # projection before consulting the previous generation.
            entry = self.current.get(key, self.previous.get(key, {}))
            # ParquetSource owns the initial full proof. Reuse it only while
            # BOTH the pointer and all verified member identities are stable.
            manifest = key.endswith(".manifest.json")
            reusable = signature is not None and entry.get("signature") == signature and (
                not manifest or "error" not in entry and "members" in entry
                and entry.get("member_date_policy", _date_member_policy()) == _date_member_policy()
                and all(_signature(member) == identity for member, identity in entry["members"]))
            if "error" in entry and entry.get("audit_version") != _DATE_AUDIT_VERSION:
                reusable = False
            if "date_chunk_policy" in entry and entry["date_chunk_policy"] != _date_chunk_policy():
                reusable = False
            if reusable:
                self.hits += 1
            elif self.remaining_seconds <= 0:
                pending += 1
                continue
            else:
                self.reads += 1
                started = time.monotonic()
                try:
                    projection = _project_dates(Path(path), previous=entry if manifest else None, chunks=self.chunks)
                    entry = {"signature": signature, "audit_version": _DATE_AUDIT_VERSION, **projection}
                    if signature != _signature(path):
                        raise ValueError("source changed during date census")
                except (OSError, ValueError, RuntimeError, KeyError) as error:
                    entry = {"signature": signature, "audit_version": _DATE_AUDIT_VERSION, "error": type(error).__name__}
                finally:
                    elapsed = time.monotonic() - started
                    self.remaining_seconds -= elapsed
                    self.cold_seconds += elapsed
            self.current[key] = entry
            references = entry.get("date_chunk_keys", [])
            if type(references) is list:
                self.used_chunks.update(key for key in references if type(key) is str)
            if "error" in entry:
                invalid += 1
            else:
                bits |= self.decode_bitmap(entry["bitmap"])
                known += entry["count"]
                nulls += entry["null_dates"]
        self.pending += pending
        self.invalid += invalid
        return {"days": bits.bit_count() if not (pending or invalid or nulls) and paths else None,
                "known_days": bits.bit_count() if paths else None,
                "known_earliest_date": date.fromordinal(_EPOCH + (bits & -bits).bit_length() - 1).isoformat() if bits else None,
                "known_latest_date": date.fromordinal(_EPOCH + bits.bit_length() - 1).isoformat() if bits else None,
                "known_count": known, "date_files_pending": pending,
                "invalid_date_files": invalid, "null_date_rows": nulls,
                "day_statistics_state": "verified" if paths and not (pending or invalid or nulls) else "partial"}


def _physical(root, records, census, *, selected=None, registry=None):
    # These are existing canonical families. Alternate projections remain
    # independently labelled; no global sum is formed from overlapping views.
    specs = [
        ("group:tw-minute-train", "台股一分鐘（永豐已發布研究分區）", "永豐 Shioaji", "taiwan_equity", "1m",
         "data_tw_minute/research_dataset/trade_date=*/data.parquet", "published_view"),
        ("physical:tw-public:stock-features", "上市櫃個股日行情", "TWSE / TPEx / Shioaji", "taiwan_equity", "daily", None, "source"),
        ("physical:tw-public:shioaji-stock-features", "永豐個股日行情補充", "永豐 Shioaji", "taiwan_equity", "daily", None, "overlapping_view"),
        ("physical:tw-public:training-features", "台股每日訓練特徵表", "TWSE / TPEx / Shioaji", "taiwan_equity", "daily", None, "derived"),
        ("physical:tw-futures:all-contracts", "期交所全期貨每日盤別行情", "TAIFEX", "taiwan_derivatives", "daily", None, "source"),
    ]
    for family in ("monthly-chain", "weekly-chain", "monthly-atm", "weekly-atm", "settlement"):
        key = "tw-options:" + family
        specs.append(("physical:" + key, PHYSICAL_FAMILIES[key][1], "TAIFEX", "taiwan_derivatives", "daily", None,
                      "source" if family.endswith("chain") else "derived"))
    for family in ("day-session", "front-month", "model-features", "symbol-features", "model-features-v4", "symbol-features-v4"):
        key = "tw-futures:" + family
        specs.append(("physical:" + key, PHYSICAL_FAMILIES[key][1], "TAIFEX", "taiwan_derivatives", "daily", None, "derived"))
    for venue in ("binance", "okx", "bybit"):
        specs.extend([
            ("group:" + venue, venue.upper() + " 全標的一分鐘", venue.capitalize() if venue != "okx" else "OKX", "crypto", "1m", f"data_{venue}/1m/*_features.parquet", "source"),
            (f"physical:{venue}:daily", venue.upper() + " 每日聚合行情", venue.capitalize() if venue != "okx" else "OKX", "crypto", "daily", None, "derived"),
            (f"physical:{venue}:hot-tail", venue.upper() + " 近期一分鐘補充", venue.capitalize() if venue != "okx" else "OKX", "crypto", "1m", None, "overlapping_view"),
        ])
    for asset, title, market in (("tw_stocks", "台股", "taiwan_equity"), ("us_stocks", "美股", "global_equity"), ("forex", "外匯", "forex")):
        specs.append(("yahoo:" + asset, "Yahoo " + title + "日行情", "Yahoo Finance / yfinance", market, "daily",
                      f"data_yahoo/{asset}/*_features.parquet", "source"))
    specs.append(("group:forex-frankfurter", "Frankfurter 官方匯率日資料", "Frankfurter", "forex", "daily", "data_forex_frankfurter/*_features.parquet", "source"))
    specs.append(("yahoo:crypto", "Yahoo 加密貨幣舊根目錄（頻率未分流）", "Yahoo Finance / yfinance", "crypto", "native", "data_yahoo/crypto/*_features.parquet", "overlapping_view"))
    for grain, directory in (("daily", "daily"), ("1m", "1m")):
        specs.append(("yahoo:crypto:" + grain, "Yahoo 加密貨幣 " + GRAINS[grain], "Yahoo Finance / yfinance", "crypto", grain,
                      f"data_yahoo/crypto/{directory}/*_features.parquet", "source"))
    policies = {}
    covered = {(grain, str(Path(pattern).parent)) for _, _, _, _, grain, pattern, _ in specs if pattern}
    covered.update((grain, str(Path(PHYSICAL_FAMILIES[key.removeprefix("physical:")][2]).parent))
        for key, _, _, _, grain, pattern, _ in specs if pattern is None)
    # The canonical published TW minute view supersedes overlapping raw chunks
    # for count purposes; still retain its broker attribution above.
    covered.add(("1m", "data_tw_minute/shioaji_1m"))
    for product in (registry or {}).get("products", []):
        name = product["id"]
        market = ("taiwan_equity" if name == "tw_listed_stocks" else "taiwan_derivatives" if name.startswith("tw_")
                  else "forex" if "fx" in name or "reference_rates" in name else "global_equity" if "equities" in name else "crypto")
        for grain in product.get("granularities", []):
            frequency = grain.get("granularity")
            if frequency not in {"daily", "1m"}:
                continue
            directory = grain.get("storage_path")
            if (frequency, directory) in covered:
                continue
            key = "product:" + name + ":" + frequency
            pattern = str(Path(directory) / "*_features.parquet") if directory else ""
            specs.append((key, product["title"] + " " + GRAINS[frequency], product["provider"], market, frequency, pattern, "source"))
            policies[key] = grain
    rows = []
    # Previously served/continually rewritten hot tails cannot starve an
    # untouched base dataset. This changes audit order, never source counts.
    specs.sort(key=lambda spec: (
        not census.groups.get(spec[0], {"pending": 1}).get("pending"),
        census.groups.get(spec[0], {}).get("served", -1),
        spec[6] == "overlapping_view"))
    for key, title, provider, market, grain, pattern, role in specs:
        if pattern is None:
            pattern = PHYSICAL_FAMILIES[key.removeprefix("physical:")][2]
        paths = selected[key] if selected is not None and key in selected else _pattern_sources(root, pattern) if pattern else []
        before_reads = census.reads
        dates = census.collect(paths)
        old_group = census.groups.get(key, {})
        census.groups[key] = {"pending": dates["date_files_pending"],
            "served": census.generation if census.reads > before_reads else old_group.get("served", -1)}
        stats = records.get(key, {})
        audited = bool(paths) and not (dates["date_files_pending"] or dates["invalid_date_files"])
        count = dates["known_count"] if audited else stats.get("count")
        state = "parquet_footers" if audited or stats.get("state") == "verified" else "partial" if paths else "not_downloaded"
        policy = policies.get(key, {})
        implementation = policy.get("implementation", "")
        if not paths and implementation:
            state = "not_available" if implementation == "not_available" else "not_enabled" if implementation.startswith(("deferred", "registered")) else "not_downloaded"
        rows.append(_row("bars:" + key, title, provider, market, grain, role=role, source_id=key,
            **{**dates, "known_count": stats.get("verified_partial_count", dates["known_count"])},
            count=count, earliest_date=stats.get("first") or dates["known_earliest_date"],
            latest_date=stats.get("last") or dates["known_latest_date"],
            files=len(paths), statistics_state=state,
            basis="沿用正式 Parquet 清冊的列數與時間界限；相異日期另以日期欄位增量清點。衍生／重疊視圖不加總；起訖不保證中間無缺口。" +
                  (" 產品登錄狀態：" + str(policy.get("cadence") or policy.get("availability") or implementation) if policy else "")))
    return rows


def _finlab(root, sources, census):
    rows = []
    index = root / "data_finlab/intraday/market_index.sqlite3"
    for kind, title, role, status in (
        ("tw_minute", "台股原始一分鐘", "source", "downloaded_unverified_for_pit"),
        ("tw_minute_derived", "台股 Tick 合成一分鐘", "derived", "derived_unverified_for_pit"),
    ):
        def read(kind=kind, status=status):
            result = _database(index, """SELECT COALESCE(SUM(rows),0),COUNT(DISTINCT trade_date),MIN(trade_date),
                MAX(trade_date),COUNT(DISTINCT symbol),COUNT(*) FROM partitions
                WHERE kind=? AND +status=? AND rows>0""", (kind, status))[0]
            return dict(zip(("count", "days", "earliest_date", "latest_date", "symbols", "partitions"), result))
        facts = sources.read("finlab:" + kind, _db_paths(index), read)
        rows.append(_row("finlab:" + kind, title, "FinLab", "taiwan_equity", "1m", role=role,
            statistics_state="receipt_index" if facts is not None else "unavailable",
            day_statistics_state="receipt_index" if facts is not None else "unavailable",
            source_id="finlab-market:" + kind, **(facts or {}),
            basis="逐股日非空成功分區的相異日期；原生與 Tick 衍生分鐘可能重疊，分列且不加總。"))
    # Only the explicit daily price/valuation families are labelled daily.
    # Other native tables remain inspectable without promoting monthly,
    # quarterly, weekly or event observations into daily observations.
    daily = {"price", "price_earning_ratio", "after_market_fixed_price", "after_market_odd_lot_trade",
             "futures_price", "institutional_investors_trading_summary", "margin_transactions",
             "futures_institutional_investors_trading_summary", "futures_institutional_investors_trading_summary_after_hours"}
    receipt_keys = {"dataset", "status", "rows", "rows_with_values", "non_null_values", "parquet_path",
                    "source_index_dtype", "first_non_null_source_index", "last_non_null_source_index",
                    "event_time_column", "first_event_at", "last_event_at"}
    for path in sorted((root / "data_finlab/receipts").glob("*.json")):
        key = "finlab-receipt-v2:" + str(path)
        receipt = sources.read(key, [path], lambda path=path: {k: v for k, v in _json(path).items() if k in receipt_keys})
        if not receipt or not isinstance(receipt.get("dataset"), str):
            continue
        if key in sources.current:
            # Compact older warm entries as well, without re-reading source
            # JSON or retaining unused wide schemas/request metadata forever.
            sources.current[key] = {**sources.current[key], "value": {k: v for k, v in receipt.items() if k in receipt_keys}}
        key = receipt["dataset"]
        prefix, _, field = key.partition(":")
        grain = "daily" if prefix in daily or "daily" in prefix.split("_") or key in {"etl:adj_close", "etl:adj_open", "etl:adj_high", "etl:adj_low"} else "native"
        market = "taiwan_derivatives" if any(token in prefix for token in ("future", "option")) else "taiwan_equity"
        stored = receipt.get("status") == "downloaded_unverified_for_pit"
        relative = receipt.get("parquet_path") or ""
        if not isinstance(relative, str):
            relative = ""
        target = root / "data_finlab" / relative
        safe = bool(relative) and not Path(relative).is_absolute() and ".." not in Path(relative).parts
        dtype = receipt.get("source_index_dtype", "")
        event_dated = bool(receipt.get("event_time_column") and receipt.get("first_event_at"))
        index_dated = not dtype or dtype.startswith("datetime64") or dtype.startswith("date")
        dated = event_dated or index_dated
        dates = census.collect([target]) if stored and safe and dated and receipt.get("rows_with_values") == receipt.get("rows") else {}
        first = receipt.get("first_event_at") if event_dated else receipt.get("first_non_null_source_index") if index_dated else None
        last = receipt.get("last_event_at") if event_dated else receipt.get("last_non_null_source_index") if index_dated else None
        row_unit = "資料列（原始長表）" if event_dated else "日期／期間列（寬表）" if index_dated else "資料列（索引非日期）"
        rows.append(_row("finlab:" + key, key, "FinLab", market, grain, kind="feature",
            role="derived" if prefix == "etl" else "source",
            row_unit=row_unit,
            source_id="finlab:" + key, count=receipt.get("rows") if stored else None,
            non_null_values=receipt.get("non_null_values") if stored else None,
            rows_with_values=receipt.get("rows_with_values") if stored else None,
            earliest_date=first if stored else None, latest_date=last if stored else None,
            date_basis="日期欄位／原生期間；長表採事件日期，可能包含預告事件。不是發布、取得或模型可用時間。",
            **dates, statistics_state="receipt_index" if stored else "not_downloaded",
            basis="FinLab 原表收據；寬表列數不同於逐股 K 棒數，非空資料格另列。其他原生頻率未確認時不標為日資料；source_index 不是發布時間。"))
    return rows


def _finmind(root, sources, *, include_date_receipts=False):
    from downloader.finmind_catalog import ALL_DATASETS, GLOBAL_EQUITY_HISTORY, SNAPSHOTS
    from downloader.finmind_scheduling import SESSION_DAY_DATASETS, SOURCES
    minute = {"TaiwanStockKBar", "TaiwanFuturesKBar", "USStockPriceMinute"}
    daily = (set(SESSION_DAY_DATASETS) | set(GLOBAL_EQUITY_HISTORY) | {
        "TaiwanStockPrice", "TaiwanStockPriceAdj", "TaiwanFuturesDaily", "TaiwanOptionDaily",
        "TaiwanExchangeRate", "TaiwanFuturesInstitutionalInvestors", "TaiwanOptionInstitutionalInvestors",
        "TaiwanFuturesDealerTradingVolumeDaily", "TaiwanOptionDealerTradingVolumeDaily",
        "TaiwanFuturesInstitutionalInvestorsAfterHours", "TaiwanOptionInstitutionalInvestorsAfterHours",
        "TaiwanFuturesOpenInterestLargeTraders", "TaiwanOptionOpenInterestLargeTraders",
        "TaiwanStockConvertibleBondDailyOverview", "TaiwanStockBlockTradingDailyReport"})
    # Polling on a session is not proof of native cadence for these tables.
    excluded = {"TaiwanStockMonthPrice", "TaiwanStockWeekPrice", "TaiwanStock10Year", "TaiwanStockEvery5SecondsIndex",
                "TaiwanStockTradingDailyReport", "TaiwanStockWarrantTradingDailyReport", "TaiwanStockTradingDailyReportSecIdAgg"}
    event_ticks = {"TaiwanStockPriceTick", "TaiwanFuturesTick", "TaiwanOptionTick", "TaiwanFuturesSpreadTick", "TaiwanStockNews"}
    allowed = (set(ALL_DATASETS) | {s.dataset for s in SOURCES}) - set(SNAPSHOTS) - event_ticks - excluded
    price = {"TaiwanStockPrice", "TaiwanStockPriceAdj", "TaiwanFuturesDaily", "TaiwanOptionDaily", *GLOBAL_EQUITY_HISTORY, *minute}
    rows = []
    for owner in ("sponsor", "complement"):
        path = root / f"data_finmind/{owner}/queue.sqlite3"
        def read():
            keys = tuple(sorted(allowed))
            result = _database(path, """SELECT dataset,SUM(rows),COUNT(DISTINCT first_data_date),
                MIN(first_data_date),MAX(last_data_date),COUNT(*),
                SUM(CASE WHEN first_data_date IS NULL OR last_data_date IS NULL
                    OR first_data_date!=last_data_date THEN 1 ELSE 0 END)
                FROM tasks WHERE dataset IN (""" + ",".join("?" for _ in keys) + """ )
                AND state='complete' AND rows>0 GROUP BY dataset""", keys, timeout_seconds=8., monitor_counts=True)
            facts = {r[0]: dict(zip(("count", "distinct_starts", "earliest_date", "latest_date", "partitions", "multi_day_partitions"), r[1:])) for r in result}
            multi = tuple(key for key, info in facts.items() if info["multi_day_partitions"] and key in daily)
            receipts = {}
            if include_date_receipts and multi:
                # Indexed dataset lookup, never walk millions of task files.
                for key, receipt in _database(path, "SELECT dataset,receipt_path FROM tasks WHERE dataset IN ("
                        + ",".join("?" for _ in multi) + ") AND state='complete' AND rows>0", multi, timeout_seconds=8.):
                    receipts.setdefault(key, []).append(receipt)
            return {"facts": facts, "receipts": receipts}
        value = sources.read("finmind-bars-v3:" + owner + str(include_date_receipts), _db_paths(path), read)
        facts = value.get("facts") if value is not None else None
        if facts is None:
            rows.append(_row("finmind:" + owner + ":unavailable", owner + " 來源統計暫不可讀", "FinMind", "taiwan_equity", "native"))
            continue
        for key, info in facts.items():
            market = ("taiwan_derivatives" if any(t in key for t in ("Futures", "Option")) else "global_equity"
                if key in GLOBAL_EQUITY_HISTORY or key == "USStockPriceMinute" else "forex" if key == "TaiwanExchangeRate"
                else "macro" if key in {"InterestRate", "CrudeOilPrices", "GovernmentBondsYield", "GoldPrice", "CnnFearGreedIndex", "TaiwanBusinessIndicator"}
                else "taiwan_equity")
            grain = "1m" if key in minute else "daily" if key in daily else "native"
            complete_dates = info["multi_day_partitions"] == 0
            rows.append(_row("finmind:" + owner + ":" + key, key + "（" + owner + "）", "FinMind", market, grain,
                kind="bar" if key in price else "feature", source_id="finmind:" + key,
                statistics_state="receipt_index", day_statistics_state="receipt_index" if complete_dates else "partial",
                **info, days=info["distinct_starts"] if complete_dates else None,
                known_days=info["distinct_starts"],
                _date_receipts=(owner, value["receipts"][key]) if key in value["receipts"] else None,
                basis="原任務佇列非空成功列數；symbol-day 分區與天數分開。跨多日查詢收據只有起訖時，天數保持待核實，已知天數僅計各分區起日；未重驗所有來源檔 SHA。"))
    return rows


def _finmind_dates(root, rows, sources, census):
    for row in rows:
        evidence = row.pop("_date_receipts", None)
        if evidence is None:
            continue
        owner, receipts = evidence
        base = str(root / "data_finmind" / owner)
        paths, unresolved = [], 0
        for relative in receipts:
            if not isinstance(relative, str):
                unresolved += 1
                continue
            if os.path.isabs(relative) or ".." in relative.split("/"):
                unresolved += 1
                continue
            # normpath retains Path's POSIX normalization without creating
            # multiple Path/parts/hash objects for every warm receipt. The
            # exact same fresh source signatures remain in _Sources.read.
            receipt_path = os.path.normpath(os.path.join(base, relative))
            key = "finmind-date-receipt:" + receipt_path
            if census.remaining_seconds <= 0 and key not in sources.previous:
                unresolved += 1
                continue
            def read(path=receipt_path):
                receipt = _json(Path(path))
                payload = receipt.get("parquet_path")
                if not isinstance(payload, str):
                    raise ValueError("invalid receipt payload path")
                payload_path = Path(payload)
                if payload_path.is_absolute() or ".." in payload_path.parts:
                    raise ValueError("invalid receipt payload path")
                return {"path": payload}
            started = time.monotonic()
            value = sources.read(key, [receipt_path], read)
            if key not in sources.previous:
                elapsed = time.monotonic() - started
                census.remaining_seconds -= elapsed
                census.cold_seconds += elapsed
            if value is None:
                unresolved += 1
            else:
                paths.append(os.path.normpath(os.path.join(base, value["path"])))
        # Match PosixPath's component ordering, including nested directories;
        # raw string sorting could change the cold-audit admission order.
        facts = census.collect(sorted(set(paths), key=lambda path: path.split("/")))
        if unresolved:
            facts.update(days=None, day_statistics_state="partial")
            facts["date_files_pending"] += unresolved
            census.pending += unresolved
        projected_count = facts.pop("known_count")
        if facts["days"] is not None and projected_count != row["count"]:
            facts.update(days=None, day_statistics_state="partial", row_count_mismatch=True)
        facts["known_days"] = max(row.get("known_days") or 0, facts["known_days"] or 0)
        row.update(facts)
        row["basis"] += " 跨日檔案的天數另以原收據選取實存日期欄位核實；分區與起訖不代替實際日期。"


def _history(root, sources):
    path = root / "data_tw_shioaji_history/summary.json"
    value = sources.read("history", [path], lambda: _json(path)) or {}
    rows = []
    for key, (title, market) in HISTORY.items():
        info = value.get("by_collection", {}).get(key, {})
        rows.append(_row("shioaji:kbars:" + key, title + "一分鐘", "永豐 Shioaji", market, "1m",
            count=info.get("kbar_rows"), days=info.get("kbar_data_days"),
            known_days=info.get("kbar_known_data_days"),
            earliest_date=info.get("kbar_first_data_date"), latest_date=info.get("kbar_last_data_date"),
            symbols=info.get("kbar_symbols_with_data"), statistics_state="receipt_index" if info else "unavailable",
            day_statistics_state="receipt_index" if info.get("kbar_data_days") is not None else "unavailable",
            basis="沿用原歷史下載器摘要與有效非空 KBar 收據的交易日聯集；期權夜盤按交易日，指數為報價 K 棒。"))
    return rows


def build_bar_inventory(root, *, record_inventory, inventory_snapshot=None, previous=None, now=None, audit_seconds=2.):
    if not 0 < audit_seconds <= 30:
        raise ValueError("invalid date audit budget")
    previous = previous or {}
    if previous.get("version") != VERSION:
        previous = {}
    sources = _Sources(previous)
    census = _DateCensus(previous, audit_seconds)
    registry_path = root / "configs/data_product_granularities.json"
    registry = sources.read("bar-products", [registry_path], lambda: _json(registry_path)) if registry_path.exists() else {}
    selected = None
    if inventory_snapshot is not None:
        inventory_snapshot.check_root(root)
        selected = inventory_snapshot.selected
    # Receipt indexes first; the budget applies only to optional date-column
    # work, and cannot keep the already known source statistics off the page.
    rows = _finmind(root, sources, include_date_receipts=True) + _history(root, sources)
    cycle = previous.get("cycle", 0)
    if type(cycle) is not int or cycle < 0:
        cycle = 0
    def physical():
        rows.extend(_physical(root, record_inventory.get("datasets", {}), census, selected=selected, registry=registry))
    def finlab():
        rows.extend(_finlab(root, sources, census))
    def finmind():
        _finmind_dates(root, rows, sources, census)
    # Rotate cold-work admission between providers. A large crypto file or a
    # frequently changing hot tail cannot permanently starve FinLab/FinMind.
    stages = (physical, finlab, finmind)
    for index in range(3):
        stages[(cycle + index) % 3]()
    rows.sort(key=lambda r: (list(MARKETS).index(r["market_category"]), r["provider"], r["granularity"], r["id"]))
    inventory = {"version": VERSION, "observed_at_utc": (now or datetime.now(UTC)).isoformat(),
        "groups": rows, "providers": sorted({r["provider"] for r in rows}),
        "collection": {"provider_calls": 0, "date_column_reads": census.reads, "date_cache_hits": census.hits,
            "cold_work_seconds": round(census.cold_seconds, 3),
            "pending_date_files": census.pending, "invalid_date_files": census.invalid,
            "source_errors": sources.errors}}
    inventory["revision"] = hashlib.sha256(json.dumps(rows, ensure_ascii=False, sort_keys=True).encode()).hexdigest()[:24]
    return inventory, {"version": VERSION, "cycle": cycle + 1 if census.pending else cycle, "sources": sources.current,
                       "dates": census.current, "date_groups": census.groups, "errors": sources.failures,
                       "date_chunks": {key: census.chunks[key] for key in sorted(census.used_chunks) if key in census.chunks}}


def series_page(bars, ticks, *, grain="all", provider="all", market="all", kind="all", search="", detail=False, offset=0, limit=30):
    if type(offset) is not int or not 0 <= offset <= 1_000_000 or type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("invalid series page bounds")
    rows = list(bars.get("groups", [])) + [{**r, "granularity": "tick", "granularity_label": GRAINS["tick"]}
        for r in ticks.get("datasets" if detail else "groups", [])]
    needle = search.casefold()
    matches = [r for r in rows if (grain == "all" or r["granularity"] == grain)
        and (provider == "all" or r["provider"] == provider) and (market == "all" or r["market_category"] == market)
        and (kind == "all" or r["kind"] == kind)
        and (not needle or needle in " ".join(str(r.get(k, "")) for k in ("id", "title", "provider")).casefold())]
    matches.sort(key=lambda r: (list(MARKETS).index(r["market_category"]), list(GRAINS).index(r["granularity"]), r["provider"], r["id"]))
    # A changing unrelated provider must not reset this provider's pagination
    # or repaint an unchanged table every 15 seconds. Fence the complete
    # filtered result, including rows beyond the requested page.
    revision = hashlib.sha256(json.dumps(matches, ensure_ascii=False, sort_keys=True).encode()).hexdigest()[:24]
    return {"version": VERSION, "state": "ready" if bars.get("version") == VERSION else "waiting_snapshot",
        "read_only": True, "production_control_possible": False,
        "revision": revision,
        "observed_at_utc": bars.get("observed_at_utc"), "rows": [_page_row(r) for r in matches[offset:offset+limit]],
        "matching_total": len(matches), "offset": offset, "limit": limit, "has_more": offset + limit < len(matches),
        "providers": sorted({r["provider"] for r in rows}), "group_count": len(rows),
        "collection": bars.get("collection", {})}

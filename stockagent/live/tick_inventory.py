"""Receipt-backed tick catalog, built by the existing monitor snapshot owner.

HTTP readers only project this catalog. No provider calls, payload scans,
cross-provider deduplication, or changes to source data happen here.
"""
from __future__ import annotations

from contextlib import closing
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time

VERSION = 1
MARKETS = {"taiwan_equity": "台股／指數", "taiwan_derivatives": "臺灣期貨／選擇權",
           "global_equity": "海外股票", "forex": "外匯", "crypto": "加密貨幣"}
KINDS = {"trade": "逐筆成交", "book": "五檔委買賣事件", "book_snapshot": "每秒五檔快照（非 tick）"}
FINMIND = {"TaiwanStockPriceTick": ("台股全市場逐筆", "taiwan_equity"),
           "TaiwanFuturesTick": ("期貨全市場逐筆", "taiwan_derivatives"),
           "TaiwanOptionTick": ("選擇權全市場逐筆", "taiwan_derivatives"),
           "TaiwanFuturesSpreadTick": ("期貨價差逐筆", "taiwan_derivatives")}
HISTORY = {"exact_futures": ("實際交割期貨歷史", "taiwan_derivatives"),
           "indices": ("指數歷史", "taiwan_equity"),
           "latest_monthly_option": ("月選擇權歷史", "taiwan_derivatives"),
           "latest_weekly_option": ("週選擇權歷史", "taiwan_derivatives")}
_DAY = re.compile(r"\d{4}-\d{2}-\d{2}\Z")


def _integer(value):
    return value if type(value) is int and value >= 0 else None


def _json(path):
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict):
        raise ValueError("metadata must be an object")
    return value


def _signature(path):
    try:
        st = path.stat()
        return [st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns]
    except FileNotFoundError:
        return None


class _Sources:
    def __init__(self, previous, *, audit_seconds=2.):
        self.previous = previous.get("sources", {}) if previous.get("version") == VERSION else {}
        if not isinstance(self.previous, dict):
            self.previous = {}
        self.current = {}
        self.errors = 0
        self.failures = {}
        self.hits = 0
        self.audit_seconds = audit_seconds
        self.audit_deadline = None
        self.audit_pending = 0
        self.footer_reads = 0
        self.footer_pool = None

    def footer_counts(self, paths):
        from stockagent.live.data_monitor_inventory import parquet_footer_count
        # Normal monitoring remains single-process. Extended repair audits use
        # measured spawn isolation: threaded Arrow reads crashed locally, while
        # two processes preserved exact counts and reduced elapsed time.
        if self.audit_seconds <= 2:
            return [parquet_footer_count(path) for path in paths]
        if self.footer_pool is None:
            from concurrent.futures import ProcessPoolExecutor
            import multiprocessing
            self.footer_pool = ProcessPoolExecutor(max_workers=2, mp_context=multiprocessing.get_context("spawn"))
        return list(self.footer_pool.map(parquet_footer_count, paths, chunksize=16))

    def close(self):
        if self.footer_pool is not None:
            self.footer_pool.shutdown(wait=True, cancel_futures=True)

    def read(self, key, paths, loader, *, resume=False):
        paths = sorted(set(paths))
        try:
            before = [[str(p), _signature(p)] for p in paths]
            cached = self.previous.get(key, {})
            same = isinstance(cached, dict) and cached.get("signature") == before and "value" in cached
            incomplete = same and resume and isinstance(cached["value"], dict) and cached["value"].get("pending")
            if same and not incomplete:
                self.hits += 1
                self.current[key] = cached
                return cached["value"]
            value = loader(cached["value"] if same else None) if resume else loader()
            # A read transaction is coherent even during a WAL commit, but its
            # result is reusable only if source identities stayed unchanged.
            if before == [[str(p), _signature(p)] for p in paths]:
                self.current[key] = {"signature": before, "value": value}
            return value
        except (OSError, ValueError, KeyError, TypeError, RuntimeError, sqlite3.Error) as error:
            self.errors += 1
            self.failures[key] = {"type": type(error).__name__, "reason": str(error)[:200]}
            return None


def _database(path, query, parameters=()):
    def read(*, immutable=False):
        deadline = time.monotonic() + 3
        uri = path.resolve().as_uri() + "?mode=ro" + ("&immutable=1" if immutable else "")
        with closing(sqlite3.connect(uri, uri=True, timeout=.2)) as conn:
            conn.execute("PRAGMA query_only=ON")
            conn.set_progress_handler(lambda: int(time.monotonic() > deadline), 10_000)
            conn.execute("BEGIN")
            return conn.execute(query, parameters).fetchall()
    try:
        return read()
    except sqlite3.OperationalError as error:
        if "unable to open database file" not in str(error):
            raise
        # A checkpointed WAL-mode database with no -shm cannot be opened by
        # SQLite inside the snapshot owner's read-only mount. Never ignore an
        # actual WAL/hot journal. A stable single-file image can be read without
        # creating sidecars; concurrent mutation invalidates the whole result.
        members = [path, path.with_name(path.name + "-wal"), path.with_name(path.name + "-journal")]
        before = [_signature(member) for member in members]
        if before[0] is None or any(value is not None for value in before[1:]):
            raise
        result = read(immutable=True)
        if before != [_signature(member) for member in members]:
            raise sqlite3.OperationalError("metadata changed during immutable read")
        return result


def _db_paths(path):
    return [path, path.with_name(path.name + "-wal")]


def _row(key, title, provider, market, *, kind="trade", source_id=None,
         basis="來源尚未建立可核對的資料收據。", **facts):
    value = {"id": key, "title": title, "provider": provider, "market_category": market,
            "market_category_label": MARKETS[market], "kind": kind, "kind_label": KINDS[kind],
            "source_id": source_id, "count": None, "known_count": None, "days": None,
            "first": None, "last": None, "symbols": None, "bytes": None,
            "statistics_state": "unavailable", "date_basis": "實際有資料的日期；不由起訖相減推算",
            "basis": basis, "detail_link": "/data-monitor/providers/" + provider + "/",
            **facts}
    return _public_dates(value)


def _public_dates(row):
    # `last` is a protected quote-price key at the HTTP boundary. Explicit date
    # names preserve metadata without changing any market-price permission.
    return {**row, "earliest_date": row.get("earliest_date", row.get("first")),
            "latest_date": row.get("latest_date", row.get("last"))}


def _sort_key(row):
    return (list(MARKETS).index(row["market_category"]), list(KINDS).index(row["kind"]),
            row["provider"], row["id"])


def _finlab(root, cache):
    path = root / "data_finlab/intraday/market_index.sqlite3"
    def read():
        # The status index also includes every minute partition. Exclude it
        # from planning so SQLite uses the existing (kind,symbol,trade_date)
        # key and only visits Tick rows (measured 0.274 s vs 0.676 s locally).
        row = _database(path, """SELECT COALESCE(SUM(rows),0),COUNT(DISTINCT trade_date),MIN(trade_date),
            MAX(trade_date),COUNT(DISTINCT symbol),COALESCE(SUM(parquet_bytes),0),COUNT(*) FROM partitions
            WHERE kind='tw_tick' AND +status='downloaded_unverified_for_pit' AND rows>0""")[0]
        return dict(zip(("count", "days", "first", "last", "symbols", "bytes", "partitions"), row))
    facts = cache.read("finlab", _db_paths(path), read)
    return _row("finlab:tw_tick", "台股全市場原始 Tick", "FinLab", "taiwan_equity",
                source_id="finlab-market:tw_tick", statistics_state="receipt_index" if facts else "unavailable",
                basis="原下載器逐股日收據索引；只計非空成功分區，非 PIT 或完整歷史證明。",
                **(facts or {}))


def _finmind(root, cache):
    path = root / "data_finmind/complement/queue.sqlite3"
    def read():
        result = _database(path, """SELECT dataset,SUM(rows),COUNT(DISTINCT partition),
            MIN(first_data_date),MAX(last_data_date),SUM(bytes),COUNT(*) FROM tasks
            WHERE dataset IN (?,?,?,?) AND state='complete' AND rows>0 GROUP BY dataset""", tuple(FINMIND))
        return {row[0]: dict(zip(("count", "days", "first", "last", "bytes", "partitions"), row[1:])) for row in result}
    facts = cache.read("finmind", _db_paths(path), read)
    return [_row("finmind:" + key, title, "FinMind", market, source_id="finmind:" + key,
                 statistics_state="receipt_index" if facts is not None else "unavailable",
                 date_basis=("原始檔案事件日期（臺北時間）；期權夜盤不同於交易日" if market == "taiwan_derivatives"
                             else "非空日分區的相異資料日期"),
                 basis="原佇列非空成功分區；不含空回、失敗或舊查詢別名；尚未重驗全部檔案 SHA。",
                 **((facts or {}).get(key, {"count": 0, "days": 0}) if facts is not None else {}))
            for key, (title, market) in FINMIND.items()]


def _official(root, cache):
    from stockagent.live.data_monitor_inventory import parquet_footer_stats
    rows = []
    for product in ("tx", "txo"):
        paths = sorted((root / "data_tw_index_derivatives_ticks" / product).glob("trading_date=*/transactions.parquet"))
        parts, invalid, days = [], 0, set()
        for path in paths:
            def read(path=path):
                stats = parquet_footer_stats(path)
                if stats is None:
                    raise ValueError("unreadable footer")
                return {key: stats.get(key) for key in ("count", "first", "last")}
            stats = cache.read("official:" + str(path), [path], read)
            if stats is None:
                invalid += 1
            else:
                parts.append(stats)
                if stats["count"] > 0:
                    days.add(path.parent.name.removeprefix("trading_date="))
        known = sum(part["count"] for part in parts)
        bounded = [part for part in parts if part["count"] > 0]
        rows.append(_row("taifex:" + product, "台指期 TX" if product == "tx" else "台指選 TXO", "TAIFEX", "taiwan_derivatives",
            source_id="physical:taifex-ticks:" + product, count=None if invalid else known, known_count=known,
            days=None if invalid else len(days), first=min(days) if days else None, last=max(days) if days else None,
            statistics_state="partial" if invalid else "parquet_footers" if paths else "not_downloaded", partitions=len(paths), invalid_files=invalid,
            first_event=min((part["first"] for part in bounded), default=None) if all(part["first"] for part in bounded) else None,
            last_event=max((part["last"] for part in bounded), default=None) if all(part["last"] for part in bounded) else None,
            date_basis="期交所交易日（含前一日夜盤）；保留全部本機分區，非只最近 30 日",
            basis=("Parquet footer 實存列數；TXO 為買賣側明細列，不等於去重成交次數；不同來源不加總。")))
    return rows


def _historical(root, cache):
    path = root / "data_tw_shioaji_history/summary.json"
    summary = cache.read("shioaji-history", [path], lambda: _json(path)) or {}
    rows = []
    for collection, (title, market) in HISTORY.items():
        item = summary.get("by_collection", {}).get(collection, {})
        rows.append(_row("shioaji-history:" + collection, title, "永豐 Shioaji", market,
            count=_integer(item.get("tick_rows")), days=_integer(item.get("tick_data_days")),
            first=item.get("tick_first_data_date"), last=item.get("tick_last_data_date"),
            symbols=_integer(item.get("tick_symbols_with_data")), observed_at_utc=summary.get("written_at_utc"),
            statistics_state="receipt_index" if item else "unavailable",
            date_basis="有非空 Tick 收據的相異交易日；合約日數不當作天數",
            basis="原歷史下載器經收據驗證的逐筆資料；僅附成交時一檔報價，非歷史五檔。"))
    return rows


def _continuous(root, cache):
    directory = root / "data_tw_futures/shioaji_history"
    manifests = sorted(directory.glob("*/manifest.json"))
    def read():
        output, union = [], set()
        for path in manifests:
            data = _json(path)
            if data.get("dataset") not in {"shioaji_continuous_futures_historical_ticks_v1", "shioaji_txfr1_historical_ticks_v1"}:
                continue
            dates = []
            parent = path.parent / "ticks"
            if parent.is_dir():
                with os.scandir(parent) as entries:
                    dates = sorted(entry.name.removeprefix("trading_date=") for entry in entries
                                   if entry.is_dir(follow_symlinks=False) and entry.name.startswith("trading_date=")
                                   and _DAY.fullmatch(entry.name.removeprefix("trading_date=")))
            union.update(dates)
            known_days = _integer(data.get("complete_trading_dates"))
            coherent = known_days == len(dates)
            output.append(_row("shioaji-continuous:" + path.parent.name, path.parent.name + " 連續期貨逐筆", "永豐 Shioaji", "taiwan_derivatives",
                count=_integer(data.get("rows")), days=known_days if coherent else None,
                first=dates[0] if dates else None, last=dates[-1] if dates else None,
                bytes=_integer(data.get("bytes")), observed_at_utc=data.get("checked_at_utc"),
                statistics_state="receipt_index" if coherent else "partial", dataset_scope="contract",
                date_basis="非空資料目錄與原收據的交易日數交叉比對",
                basis="連續 R1/R2 合約收據；可能與實際交割合約及其他來源重疊，未當成獨立成交加總。"))
        return {"datasets": output, "days": len(union)}
    value = cache.read("continuous-history", manifests + [p.parent / "ticks" for p in manifests], read)
    datasets = (value or {}).get("datasets", [])
    summary = _row("shioaji-continuous", "連續期貨 R1／R2 歷史（逐合約可展開）", "永豐 Shioaji", "taiwan_derivatives",
        count=sum(row["count"] for row in datasets) if datasets and all(row["count"] is not None for row in datasets) else None,
        days=value.get("days") if value else None, first=min((r["first"] for r in datasets if r["first"]), default=None),
        last=max((r["last"] for r in datasets if r["last"]), default=None), symbols=len(datasets),
        statistics_state="receipt_index" if value is not None else "unavailable",
        basis="連續合約各儲存視圖的登記列數；R1/R2 與精確合約可能重疊，不代表唯一成交。")
    return summary, datasets


def _captures(root, relative, title, market, cache):
    from stockagent.data.shioaji_capture_parts import read_capture_manifest_groups
    capture_root = root / relative
    records, errors = {}, 0
    for day in sorted((capture_root / "manifests").glob("trade_date=*")):
        trade_date = day.name.removeprefix("trade_date=")
        if not _DAY.fullmatch(trade_date):
            continue
        def read():
            selected = []
            for session, manifests in read_capture_manifest_groups(capture_root, trade_date):
                for value in manifests:
                    selected.append({key: value.get(key) for key in ("capture_id", "worker_index", "tick_rows_written",
                        "book_rows_written", "book_1s_rows_written", "dropped_events", "started_at_utc", "finished_at_utc")})
            return selected
        values = cache.read("capture-v2:" + relative + ":" + trade_date, list(day.glob("**/*.json")), read)
        if values is None:
            errors += 1
            continue
        for item in values:
            identity = (item["capture_id"] or trade_date, item["worker_index"])
            if identity in records:
                # Session mirrors describe the same immutable capture, never
                # another copy of its recorded events.
                previous = records[identity][1]
                if previous != item:
                    errors += 1
                continue
            records[identity] = (trade_date, item)
    output = []
    for kind, column in (("trade", "tick_rows_written"), ("book", "book_rows_written"), ("book_snapshot", "book_1s_rows_written")):
        known = sum(_integer(value.get(column)) or 0 for _, value in records.values())
        invalid = errors + sum(_integer(value.get(column)) is None for _, value in records.values())
        dates = {day for day, value in records.values() if (_integer(value.get(column)) or 0) > 0}
        output.append(_row(relative.split('/')[0] + ":" + kind + (":fop" if "fop" in relative else ""),
            title + " · " + KINDS[kind], "永豐 Shioaji", market, kind=kind,
            count=None if invalid else known, known_count=known, days=None if invalid else len(dates),
            first=min(dates) if dates else None, last=max(dates) if dates else None,
            statistics_state="partial" if invalid else "capture_manifests" if records else "not_downloaded",
            dropped_events=sum(_integer(value.get("dropped_events")) or 0 for _, value in records.values()),
            date_basis="有效 capture 登記的交易日；只計該事件類型已落盤且非空的日期",
            basis="沿用正式 capture 選取規則，排除日／夜盤鏡像；只計 rows_written，不含排隊事件。未附有效清冊的片段及重複成交尚未核實。"))
    return output + _unindexed_capture_parts(capture_root, relative, title, market, records, cache)


def _unindexed_capture_parts(capture_root, relative, title, market, records, cache):
    """Inventory retained pieces outside the current manifest selection.

    File names, capture identity and legacy write windows use the canonical
    selector's grammar. Hourly identities, native streaming enumeration and a
    shared time budget bound cold work. Incomplete hours resume from already
    checked immutable names; a changed directory discards that partial proof.
    """
    from stockagent.data.shioaji_capture_parts import _PART_RE, _legacy_write_window_ns
    owners, legacy = set(), []
    for _, item in records.values():
        worker = item.get("worker_index")
        if item.get("capture_id"):
            owners.add((str(item["capture_id"]), worker))
        else:
            try:
                legacy.append((worker, *_legacy_write_window_ns(item)))
            except (ValueError, TypeError, KeyError, RuntimeError):
                continue
    owner_key = hashlib.sha256(json.dumps([sorted(owners), sorted(legacy)]).encode()).hexdigest()[:20]
    if cache.audit_deadline is None:
        cache.audit_deadline = time.monotonic() + cache.audit_seconds
    output = []
    for kind, folder in (("trade", "ticks"), ("book", "book_events"), ("book_snapshot", "book_1s")):
        parts = []
        for day in sorted((capture_root / folder).glob("trade_date=*")):
            trading_date = day.name.removeprefix("trade_date=")
            if not _DAY.fullmatch(trading_date):
                continue
            with os.scandir(day) as entries:
                hours = sorted(Path(entry.path) for entry in entries
                               if entry.name.startswith("hour=") and entry.is_dir(follow_symlinks=False))
            for directory in [day, *hours]:
                def read(previous, directory=directory):
                    members = dict((previous or {}).get("members", {}))
                    pending = False
                    batch = []
                    def flush():
                        counts = cache.footer_counts([Path(entry.path) for entry in batch])
                        cache.footer_reads += len(batch)
                        members.update((entry.name, count) for entry, count in zip(batch, counts, strict=True))
                        batch.clear()
                    # Do not materialize millions of Path objects in a tuple.
                    with os.scandir(directory) as entries:
                        for entry in entries:
                            if time.monotonic() >= cache.audit_deadline:
                                pending = True
                                break
                            if not entry.name.endswith(".parquet") or not entry.is_file(follow_symlinks=False):
                                continue
                            match = _PART_RE.fullmatch(entry.name)
                            if match:
                                worker = int(match.group("worker"))
                                capture_id = match.group("capture_id")
                                if (capture_id, worker) in owners:
                                    continue
                                stamp = int(match.group("stamp_ns"))
                                if capture_id is None and any(w == worker and lo <= stamp < hi for w, lo, hi in legacy):
                                    continue
                            if entry.name not in members:
                                batch.append(entry)
                                if len(batch) >= (64 if cache.audit_seconds > 2 else 1):
                                    flush()
                    if batch:
                        flush()
                    value = {"count": sum(count for count in members.values() if count is not None),
                             "files": len(members), "invalid": sum(count is None for count in members.values()),
                             "pending": pending}
                    if pending:
                        value["members"] = members
                    return value
                key = f"unindexed-v2:{relative}:{folder}:{trading_date}:{directory.name}:{owner_key}"
                value = cache.read(key, [directory], read, resume=True)
                parts.append((trading_date, value))
        files = sum(value["files"] for _, value in parts if value is not None)
        invalid = sum(value["invalid"] if value is not None else 1 for _, value in parts)
        pending = sum(bool(value and value.get("pending")) for _, value in parts)
        cache.audit_pending += pending
        if not files and not invalid and not pending:
            continue
        dates = {day for day, value in parts if value and value["count"] > 0}
        known = sum(value["count"] for _, value in parts if value)
        output.append(_row("unindexed:" + relative + ":" + kind, title + " · 未配對收據分片 · " + KINDS[kind],
            "永豐 Shioaji", market, kind=kind, count=None if invalid or pending else known, known_count=known,
            days=None if invalid or pending else len(dates), known_days=len(dates),
            first=min(dates) if dates else None, last=max(dates) if dates else None,
            partitions=files, invalid_files=invalid, pending_directories=pending,
            statistics_state="partial" if invalid or pending else "unattributed_footers",
            date_basis="非空實存分片的分區日期；來源交易日尚缺有效收據配對",
            basis=f"已核對 {files:,} 個未配對分片，{invalid:,} 個檔案／目錄無法讀取，尚有 {pending:,} 個目錄待核對。Parquet footer 列數未配對現行 capture ID／worker 或舊寫入時窗；可能是中斷錄製或舊版本，與主清冊分列。"))
    return output


def _registered(root):
    try:
        products = _json(root / "configs/data_product_granularities.json").get("products", [])
    except (OSError, ValueError):
        return []
    output = []
    for product in products:
        for grain in product.get("granularities", []):
            implementation = str(grain.get("implementation", ""))
            if grain.get("granularity") != "tick" or implementation.startswith("reused_existing"):
                continue
            product_id = str(product["id"])
            market = "global_equity" if "equities" in product_id else "forex" if "fx" in product_id else "crypto"
            output.append(_row("registered:" + product_id, str(product["title"]), str(product["provider"]), market,
                source_id="product:" + product_id + ":tick", statistics_state="not_available" if implementation == "not_available" else "not_enabled",
                basis=str(grain.get("cadence") or implementation)))
    return output


def build_tick_inventory(root: Path, *, previous=None, now=None, audit_seconds=2.):
    """Return public observations and a private, identity-bound reusable cache."""
    observed = (now or datetime.now(UTC)).isoformat()
    if not 0 < audit_seconds <= 30:
        raise ValueError("Tick metadata audit budget must be within (0,30] seconds")
    cache = _Sources(previous or {}, audit_seconds=audit_seconds)
    summary, contracts = _continuous(root, cache)
    try:
        common = [_finlab(root, cache), *_finmind(root, cache), *_official(root, cache), *_historical(root, cache),
                  *_captures(root, "data_tw_microstructure/captures", "台股自行錄製", "taiwan_equity", cache),
                  *_captures(root, "data_tw_index_derivatives_ticks/shioaji_fop_captures", "期權自行錄製", "taiwan_derivatives", cache),
                  *_registered(root)]
    finally:
        cache.close()
    groups = sorted(map(_public_dates, common + [summary]), key=_sort_key)
    datasets = list(map(_public_dates, common + contracts))
    payload = {"version": VERSION, "observed_at_utc": observed, "groups": groups, "datasets": datasets,
               "dataset_count": len(datasets), "group_count": len(groups),
               "providers": sorted({row["provider"] for row in groups}),
               "definitions": {"count": "各來源登記列數；跨來源、連續／交割合約及 TXO 側明細不能加總成唯一成交數。",
                               "days": "實際非空日期的相異數量，不是起訖日之差，也不是合約日分區數。",
                               "freshness": "統計更新時間與資料最新日期分開；未知值不當作零。"},
               "collection": {"source_errors": cache.errors, "cache_hits": cache.hits,
                              "provider_calls": 0, "source_payload_scans": 0,
                              "footer_reads": cache.footer_reads, "pending_audit_directories": cache.audit_pending}}
    payload["revision"] = hashlib.sha256(json.dumps(
        [groups, datasets], ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()).hexdigest()[:24]
    return payload, {"version": VERSION, "sources": cache.current, "errors": cache.failures}


def tick_summary(inventory):
    result = {key: value for key, value in inventory.items() if key != "datasets"}
    result["groups"] = [_page_row(row) for row in inventory.get("groups", [])]
    return result


def _page_row(row):
    return {key: value for key, value in _public_dates(row).items()
            if key not in {"first", "last", "public_price_display"}}


def tick_page(inventory, *, search="", provider="all", market="all", kind="all", detail=False, offset=0, limit=30):
    if type(offset) is not int or not 0 <= offset <= 1_000_000 or type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("invalid tick page bounds")
    rows = inventory.get("datasets" if detail else "groups", [])
    needle = search.casefold()
    matches = [row for row in rows if (provider == "all" or row["provider"] == provider)
               and (market == "all" or row["market_category"] == market)
               and (kind == "all" or row["kind"] == kind)
               and (not needle or needle in " ".join(str(row.get(k, "")) for k in ("id", "title", "provider")).casefold())]
    matches.sort(key=_sort_key)
    return {"version": VERSION, "read_only": True, "production_control_possible": False,
            "state": "ready" if inventory.get("version") == VERSION else "waiting_snapshot",
            "revision": inventory.get("revision"),
            "observed_at_utc": inventory.get("observed_at_utc"), "rows": [_page_row(row) for row in matches[offset:offset+limit]],
            "matching_total": len(matches), "offset": offset, "limit": limit,
            "has_more": offset + limit < len(matches), "providers": inventory.get("providers", []),
            "dataset_count": inventory.get("dataset_count"), "group_count": inventory.get("group_count")}

"""Current official regular stock/ETF registry, independent of model history.

Refresh off the opening critical path. Runtime reads a dated durable receipt;
an unavailable/invalid registry is unknown, never an alive-mask fallback.
This is an observation of the current registry, not a historical PIT backfill.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time as daytime
from functools import lru_cache
import hashlib
import io
import json
import os
from pathlib import Path
import re
import threading
import time
from zoneinfo import ZoneInfo

import numpy as np
import requests

from downloader.artifact_io import atomic_write_bytes, atomic_write_json
from downloader.dataset_lock import exclusive_dataset_lock
from stockagent.data.tw_security import classify_tw_stock_or_etf

TAIPEI = ZoneInfo("Asia/Taipei")
# Stable reader envelope: existing execution owners can read a corrected
# source generation without replacing their process or touching pending orders.
# Membership policy has its own version, pinned inside the envelope's hash.
CONTRACT = "tw_official_current_regular_stock_etf_universe_v3"
MEMBERSHIP_CONTRACT = "tw_official_active_stock_etf_membership_v1"
TPEX_ETF_CATALOGS = {
    name: "https://www.tpex.org.tw/www/zh-tw/ETF/list?type=" + query
    for name, query in (("domestic", "domestic"), ("foreign", "foreign"),
        ("bond", "bond&bondType=0"), ("multi", "multi"), ("active", "active"),
        ("leveraged", "leveraged"), ("futures", "futures"))
}
SOURCES = {
    "twse_registry": "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2",
    "tpex_registry": "https://isin.twse.com.tw/isin/C_public.jsp?strMode=4",
    "twse_halts": "https://openapi.twse.com.tw/v1/exchangeReport/TWTAWU",
    "twse_suspensions": "https://www.twse.com.tw/rwd/zh/violation/stop?response=json",
    "tpex_status": "https://www.tpex.org.tw/openapi/v1/tpex_cmode",
    "tpex_halts": "https://www.tpex.org.tw/openapi/v1/tpex_spendi_history",
    "twse_etf_catalog": "https://www.twse.com.tw/rwd/zh/ETF/list?response=json",
    **{"tpex_etf_" + name: url for name, url in TPEX_ETF_CATALOGS.items()},
}
CATALOG_NAMES = ("twse_etf_catalog", *("tpex_etf_" + name for name in TPEX_ETF_CATALOGS))
MEMBERSHIP_SOURCE = "active_product_membership"
MEMBERSHIP_URL = "canonical:tw_current_trading_universe:active_product_membership"
_REFRESH_LOCK = threading.Lock()


class TWTradingUniversePending(RuntimeError):
    code = "tw_official_trading_universe_pending"


def receipt_path(session_date: str) -> Path:
    date.fromisoformat(session_date)
    return Path(os.getenv("STOCKAGENT_TW_TRADING_UNIVERSE_ROOT", "artifacts/live/tw_trading_universe")) / (session_date + ".json")


def _date(value: object) -> date | None:
    text = str(value or "").strip().replace("/", "").replace("-", "")
    if not text:
        return None
    if len(text) == 7 and text.isdigit():
        text = str(int(text[:3]) + 1911) + text[3:]
    if len(text) != 8 or not text.isdigit():
        raise ValueError("invalid official registry/lifecycle date")
    return datetime.strptime(text, "%Y%m%d").date()


def _at(day: object, clock: object) -> datetime | None:
    parsed = _date(day)
    if parsed is None:
        return None
    text = str(clock or "000000").strip().replace(":", "") or "000000"
    return datetime.combine(parsed, datetime.strptime(text, "%H%M%S").time(), TAIPEI)


def parse_registry(content: bytes, market: str, as_of: datetime) -> list[dict]:
    # Reuse the downloader's maintained scope/parser rather than a second
    # regex universe that accidentally admits emerging shares or warrants.
    from downloader.download_yahoo_ohlcv import (
        _HTMLTableRowParser, _parse_tw_exchange_rows, TWSE_CODE_NAME_PATTERN,
    )
    if market not in {"listed", "otc"}:
        raise ValueError("unsupported official registry market")
    html = content.decode("cp950")
    # The official JSP deliberately returns an HTML fragment without closing
    # body/html tags. Require its data table to close, not nonexistent markup.
    if html.lower().rfind("</table>") < html.lower().rfind("<table") or "<table" not in html.lower():
        raise ValueError("official ISIN registry is truncated")
    parser = _HTMLTableRowParser()
    parser.feed(html)
    rows = parser.rows
    if not any("有價證券代號及名稱" in "".join(row) for row in rows):
        raise ValueError("official ISIN registry header missing")
    parsed = _parse_tw_exchange_rows(rows, market)
    if not parsed:
        raise ValueError("official stock/ETF registry is empty")
    fields = {}
    for row in rows:
        match = TWSE_CODE_NAME_PATTERN.match(row[0]) if row else None
        if match and len(row) >= 4:
            code = match.group("code").upper()
            if code in fields and fields[code] != row:
                raise ValueError("conflicting official ISIN symbol rows")
            fields[code] = row
    result = []
    for record in parsed:
        row = fields[record.code]
        listed = _date(row[2])
        if listed is None:
            raise ValueError("official listing date missing")
        if listed > as_of.date():
            continue
        result.append({"symbol": record.code, "name": record.name,
                       "market": "twse" if market == "listed" else "tpex",
                       "security_type": classify_tw_stock_or_etf(record.code),
                       "listing_date": listed.isoformat(), "isin": row[1],
                       "cfi": row[5] if len(row) > 5 else ""})
    if not result:
        raise ValueError("official current-session registry is empty")
    return result


def parse_tpex_etf_catalog(content: bytes, session_date: str) -> set[str]:
    """Validate all rows, including future listings, before taking membership."""
    payload = json.loads(content)
    if not isinstance(payload, dict) or payload.get("date") != session_date.replace("-", ""):
        raise ValueError("TPEx ETF catalogue date mismatch")
    tables = payload.get("tables")
    if not isinstance(tables, list) or len(tables) != 1:
        raise ValueError("TPEx ETF catalogue table missing/ambiguous")
    table = tables[0]
    if not isinstance(table, dict) or table.get("fields") != ["證券代號", "ETF簡稱", "上櫃日期"]:
        raise ValueError("TPEx ETF catalogue schema mismatch")
    rows, count = table.get("data"), table.get("totalCount")
    if (not isinstance(rows, list) or type(count) is not int or count != len(rows)
            or payload.get("stat") != ("ok" if rows else "暫無相關商品")):
        raise ValueError("TPEx ETF catalogue status/count mismatch")
    symbols, seen = set(), set()
    for row in rows:
        if not isinstance(row, list) or len(row) != 3:
            raise ValueError("TPEx ETF catalogue row width mismatch")
        symbol, name, listed = row
        if (not isinstance(symbol, str) or classify_tw_stock_or_etf(symbol) != "etf"
                or not isinstance(name, str) or not name.strip() or symbol in seen):
            raise ValueError("TPEx ETF catalogue invalid/duplicate symbol")
        seen.add(symbol)
        listing_date = _date(listed)
        if listing_date is None:
            raise ValueError("TPEx ETF catalogue listing date missing")
        if listing_date <= date.fromisoformat(session_date):
            symbols.add(symbol)
    return symbols


def parse_twse_etf_catalog(content: bytes, session_date: str) -> set[str]:
    """Include every currency counter; future listings are not active yet.

    This endpoint has no source-date/totalCount fields. Its real acquisition
    clock is mandatory in the receipt; never manufacture a publication date.
    """
    payload = json.loads(content)
    if (not isinstance(payload, dict) or payload.get("stat") != "OK"
            or payload.get("fields") != ["上市日期", "證券代號", "證券簡稱", "發行人", "標的指數"]
            or not isinstance(payload.get("data"), list) or not payload["data"]):
        raise ValueError("TWSE ETF catalogue status/schema missing")
    symbols, seen = set(), set()
    for row in payload["data"]:
        if (not isinstance(row, list) or len(row) != 5
                or not all(isinstance(v, str) for v in row[:4])
                or row[4] is not None and not isinstance(row[4], str)):
            raise ValueError("TWSE ETF catalogue row width/type mismatch")
        dates, codes, names = (re.split(r"<br\s*/?>", value, flags=re.IGNORECASE) for value in row[:3])
        if not len(dates) == len(codes) == len(names):
            raise ValueError("TWSE ETF currency counters are misaligned")
        for listed, code, name in zip(dates, codes, names, strict=True):
            matched_code = re.fullmatch(r"([0-9A-Z]+)(?:\([^()]+\))?", code.strip())
            matched_date = re.fullmatch(r"(\d{4}\.\d{2}\.\d{2})(?:\([^()]+\))?", listed.strip())
            if not matched_code or not matched_date or not name.strip():
                raise ValueError("TWSE ETF catalogue counter/date malformed")
            symbol = matched_code[1]
            if classify_tw_stock_or_etf(symbol) != "etf" or symbol in seen:
                raise ValueError("TWSE ETF catalogue invalid/duplicate symbol")
            seen.add(symbol)
            if _date(matched_date[1].replace(".", "/")) <= date.fromisoformat(session_date):
                symbols.add(symbol)
    return symbols


def _listed_records_identity(records: list[dict]) -> str:
    projection = sorted(({k: v for k, v in row.items() if k != "trading_status"}
                         for row in records), key=lambda row: row["symbol"])
    return hashlib.sha256(json.dumps(projection, sort_keys=True, ensure_ascii=False,
        separators=(",", ":")).encode()).hexdigest()


def reconcile_active_etfs(records: list[dict], catalogs: dict[str, set[str]]) -> tuple[list[dict], dict]:
    """Current product tables govern ETF membership, not quote availability.

    An absent ETF is an explicitly evidenced current-catalog exclusion, not
    an invented permanent-exit date. No historical price/ledger is rewritten.
    Catalogue-only eligible symbols require an updated registry, never a
    silently smaller universe or guessed ISIN/venue metadata.
    """
    if set(catalogs) != set(CATALOG_NAMES):
        raise ValueError("official ETF catalogue coverage is incomplete")
    active = {"twse": catalogs["twse_etf_catalog"],
              "tpex": set().union(*(catalogs["tpex_etf_" + name] for name in TPEX_ETF_CATALOGS))}
    excluded, comparisons = [], {}
    for market, symbols in active.items():
        registry = {r["symbol"] for r in records if r["market"] == market and r["security_type"] == "etf"}
        if registry and not symbols:
            raise ValueError("official ETF catalogue unexpectedly empty: " + market)
        if symbols - registry:
            raise ValueError("official ETF registry missing eligible catalogue symbols: "
                             + market + ":" + ",".join(sorted(symbols - registry)))
        comparisons[market] = {"registry_count": len(registry), "active_catalog_count": len(symbols),
            "excluded_symbols": sorted(registry - symbols), "catalog_only_symbols": []}
    kept = []
    for row in records:
        if row["security_type"] == "etf" and row["symbol"] not in active[row["market"]]:
            excluded.append({**row, "exclusion_reason": "not_in_complete_current_official_etf_catalog"})
        else:
            kept.append(row)
    return kept, {"contract": MEMBERSHIP_CONTRACT, "registry_count": len(records),
                  "listed_count": len(kept), "excluded_count": len(excluded),
                  "listed_records_sha256": _listed_records_identity(kept),
                  "excluded_records": sorted(excluded, key=lambda row: row["symbol"]),
                  "etf_catalog_comparison": comparisons}


def halted_symbols(payloads: dict[str, list], as_of: datetime) -> set[str]:
    halted = set()
    latest_events = {}
    for name, symbol, start, end, start_time, end_time in (
        ("twse_halts", "Code", "TradingHaltDate", "TradingResumptionDate", "TradingHaltTime", "TradingResumptionTime"),
        ("tpex_halts", "SecuritiesCompanyCode", "DateOfSuspendedTrading", "DateOfResumedTrading", "TimeOfSuspendedTrading", "TimeOfResumedTrading"),
    ):
        rows = payloads[name]
        if not isinstance(rows, list):
            raise ValueError("official halt response is not a list")
        events = {}
        for row in rows:
            if not isinstance(row, dict) or not {symbol, start, end}.issubset(row):
                raise ValueError("official halt response schema changed")
            beginning, ending = _at(row[start], row.get(start_time)), _at(row[end], row.get(end_time))
            if beginning and ending and ending < beginning:
                raise ValueError("official halt interval is inverted")
            code = str(row[symbol]).strip()
            if not code:
                raise ValueError("official halt symbol missing")
            # TPEx emits suspend/resume as separate rows, often unsorted. A
            # blank resume in an old suspend row is not a permanent halt.
            for at, state, priority in ((beginning, True, 0), (ending, False, 1)):
                if at is not None and at <= as_of:
                    event = (at, priority, state)
                    if code not in events or event > events[code]:
                        events[code] = event
        for code, event in events.items():
            latest_events[code] = event
            if event[2]:
                halted.add(code)
    statuses = payloads["tpex_status"]
    if not isinstance(statuses, list) or not statuses:
        raise ValueError("official TPEx status table missing")
    for row in statuses:
        if not isinstance(row, dict) or not {"Date", "SecuritiesCompanyCode", "SuspensionOfTrading"}.issubset(row):
            raise ValueError("official TPEx status schema changed")
        effective = _date(row["Date"])
        if effective is None or effective > as_of.date():
            raise ValueError("official TPEx status date missing/future")
        if str(row["SuspensionOfTrading"]).strip() in {"Y", "Ｙ", "是"}:
            code = str(row["SecuritiesCompanyCode"]).strip()
            latest = latest_events.get(code)
            # A dated status table cannot overrule a later explicit resumption.
            if not (latest and not latest[2] and latest[0].date() > effective):
                halted.add(code)
    return halted


def current_twse_suspensions(payload: dict, as_of: datetime) -> set[str]:
    """Current official stop list, distinct from temporary trading-halt history."""
    import re
    if payload.get("stat") != "ok":
        raise ValueError("official TWSE stop table status changed")
    tables = [t for t in payload.get("tables", []) if "證券代號" in (t.get("fields") or [])]
    if len(tables) != 1:
        raise ValueError("official TWSE stop table schema changed")
    table = tables[0]
    title_date = re.search(r"(\d+)年(\d+)月(\d+)日", str(table.get("title")))
    if not title_date:
        raise ValueError("official TWSE stop table publication date missing")
    year, month, day = map(int, title_date.groups())
    published = date(year + 1911 if year < 1911 else year, month, day)
    if published > as_of.date():
        raise ValueError("official TWSE stop table date is future")
    fields = table["fields"]
    if "停止買賣開始日期" not in fields or not isinstance(table.get("data"), list):
        raise ValueError("official TWSE stop table data schema changed")
    halted = set()
    for values in table["data"]:
        row = dict(zip(fields, values, strict=True))
        started = re.fullmatch(r"(\d+)年(\d+)月(\d+)日", str(row["停止買賣開始日期"]))
        if not started:
            raise ValueError("official TWSE stop effective date missing")
        year, month, day = map(int, started.groups())
        if date(year + 1911 if year < 1911 else year, month, day) <= as_of.date():
            halted.add(str(row["證券代號"]).strip())
    return halted


def corporate_suspension_evidence(parquet_root: str | Path, as_of: datetime,
                                 archive_root: Path) -> tuple[set[str], dict]:
    """Use the canonical, receipt-verified official share-replacement collector.

    Financial stops and temporary halts do not cover capital-reduction exchange
    suspensions. Reuse their existing owner; do not add another MOPS crawler.
    """
    import polars as pl
    root = Path(parquet_root).resolve().parent
    summary_raw = (root / "tw_share_replacement_reference.summary.json").read_bytes()
    summary = json.loads(summary_raw)
    observed = datetime.fromisoformat(summary["generated_at_utc"])
    if (not all(summary.get(k) is True for k in ("source_download_complete", "lifecycle_catalog_complete", "complete_all_markets"))
            or _date(summary["coverage_end"]) < as_of.date()
            or _date(summary["announced_resumption_query_end"]) < as_of.date()
            or observed.tzinfo is None or observed > datetime.now(TAIPEI)):
        raise ValueError("official share-replacement lifecycle coverage is not ready")
    evidence = {"share_replacement_summary": summary_raw}
    for name, receipt, filename in (
        ("share_replacement_reference", summary["output_receipt"], "tw_share_replacement_reference.parquet"),
        ("share_replacement_manifest", summary["raw_receipt_manifest"], summary["raw_receipt_manifest"]["relative_path"]),
    ):
        path = (root / filename).resolve()
        if not path.is_relative_to(root):
            raise ValueError("official share-replacement receipt escaped source root")
        raw = path.read_bytes()
        if len(raw) != receipt["size"] or hashlib.sha256(raw).hexdigest() != receipt["sha256"]:
            raise ValueError("official share-replacement receipt fingerprint mismatch")
        evidence[name] = raw
    frame = pl.read_parquet(io.BytesIO(evidence["share_replacement_reference"]))
    if frame.height != summary["rows"] or not {"symbol", "suspension_date", "resume_date", "historical_halt_evidence"}.issubset(frame.columns):
        raise ValueError("official share-replacement lifecycle schema/count mismatch")
    halted = set(frame.filter(
        (pl.col("suspension_date") <= as_of.date()) & (pl.col("resume_date") > as_of.date())
        & pl.col("historical_halt_evidence").eq(True)
    ).get_column("symbol").to_list())
    sources = {}
    for name, raw in evidence.items():
        digest = hashlib.sha256(raw).hexdigest()
        path = archive_root / (name + "." + digest + (".parquet" if name.endswith("reference") else ".json"))
        atomic_write_bytes(path, raw, durable=True)
        sources[name] = {"url": "canonical:tw_share_replacement_reference", "sha256": digest,
                         "bytes": len(raw), "raw_path": str(path.resolve()),
                         "upstream_observed_at_utc": summary["generated_at_utc"]}
    return halted, sources


def _identity(payload: dict) -> str:
    semantic = {k: payload[k] for k in ("contract", "session_date", "records", "sources")}
    return hashlib.sha256(json.dumps(semantic, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()


def _fetch(url: str) -> bytes:
    from downloader.download_tw_short_sale_restrictions import _rate_limited_call
    with _rate_limited_call(requests.get, url, timeout=(3, 15)) as response:
        response.raise_for_status()
        return response.content


def _cached_source(name: str, root: Path, session_date: str, max_age: float) -> tuple[bytes, dict] | None:
    """Keep successful transport facts across a failed aggregate refresh."""
    try:
        state_path = root / "source_state" / session_date / (name + ".json")
        if state_path.exists():
            source = json.loads(state_path.read_text())
            if not isinstance(source, dict):
                return None
        else:
            # Upgrade already archived partial refreshes without losing their
            # real observation clock or redownloading successful large pages.
            previous = json.loads((root / "refresh_status.json").read_text())
            if name not in previous.get("sources", {}):
                previous = json.loads((root / (session_date + ".json")).read_text())
            source = previous["sources"][name].copy()
            source.setdefault("observed_at_taipei", previous["observed_at_taipei"])
        observed = datetime.fromisoformat(source["observed_at_taipei"])
        now = datetime.now(TAIPEI)
        if (source.get("status", "received") != "received" or observed.tzinfo is None
                or observed.astimezone(TAIPEI).date().isoformat() != session_date
                or not 0 <= (now - observed).total_seconds() <= max_age or source["url"] != SOURCES[name]):
            return None
        raw = Path(source["raw_path"]).read_bytes()
        if len(raw) != source["bytes"] or hashlib.sha256(raw).hexdigest() != source["sha256"]:
            return None
        return raw, source
    except (OSError, ValueError, KeyError, TypeError):
        return None


def _publish_refresh_status(root: Path, payload: dict) -> None:
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    atomic_write_json(root / "refresh_attempts" / (digest + ".json"), payload)
    atomic_write_json(root / "refresh_status.json", payload)


def collect_current_sources(names, root: Path, session_date: str, *, fetch=None,
                            force=False, max_age_seconds=900) -> tuple[dict, dict, dict]:
    """One raw-source/cache owner shared by promotion and source-only audits."""
    fetch = fetch or _fetch
    raw, sources, failures = {}, {}, {}

    def receive(name):
        age = 86400 if name.endswith("registry") else max_age_seconds
        cached = None if force else _cached_source(name, root, session_date, age)
        if cached is not None:
            return cached
        content = fetch(SOURCES[name])
        observed = datetime.now(TAIPEI)
        if observed.date().isoformat() != session_date:
            raise ValueError("official source observation crossed the day")
        digest = hashlib.sha256(content).hexdigest()
        path = root / "raw" / session_date / (name + "." + digest + (".html" if name.endswith("registry") else ".json"))
        atomic_write_bytes(path, content, durable=True)
        source = {"url": SOURCES[name], "sha256": digest, "bytes": len(content),
                  "raw_path": str(path.resolve()), "observed_at_taipei": observed.isoformat(), "status": "received"}
        atomic_write_json(root / "source_state" / session_date / (name + ".json"), source)
        return content, source

    with ThreadPoolExecutor(max_workers=min(8, len(names)), thread_name_prefix="tw-official-registry") as pool:
        pending = {pool.submit(receive, name): name for name in names}
        for future in as_completed(pending):
            name = pending[future]
            try:
                raw[name], sources[name] = future.result()
            except Exception as exc:
                failures[name] = type(exc).__name__
                # Keep the accepted generation/raw bytes, but do not let a
                # force-refresh failure silently reuse that older successful
                # transport as the retry of this failed request.
                atomic_write_json(root / "source_state" / session_date / (name + ".json"),
                    {"url": SOURCES[name], "status": "transport_failed",
                     "observed_at_taipei": datetime.now(TAIPEI).isoformat(),
                     "error_type": type(exc).__name__})
    return raw, sources, failures


def refresh_current_universe(session_date: str, *, force: bool = False,
                             max_age_seconds: float | None = None, fetch=None,
                             parquet_root: str | Path | None = None) -> dict:
    """One preopen owner; complete source receipts before atomic promotion."""
    now = datetime.now(TAIPEI)
    if session_date != now.date().isoformat():
        raise ValueError("current official registry cannot be backdated")
    with _REFRESH_LOCK, exclusive_dataset_lock(
        receipt_path(session_date).parent / "state" / "refresh.lock",
        provider="tw_official_trading_universe", timeout_seconds=60,
    ):
        if not force:
            try:
                previous = load_current_universe(session_date)
                age = (datetime.now(TAIPEI) - datetime.fromisoformat(previous["observed_at_taipei"])).total_seconds()
                try:
                    attempt = json.loads((receipt_path(session_date).parent / "refresh_status.json").read_bytes())
                    recovery_due = (isinstance(attempt, dict) and attempt.get("session_date") == session_date
                        and attempt.get("status") in {"waiting_source", "invalid_source"})
                except (OSError, ValueError, TypeError):
                    recovery_due = False
                if not recovery_due and (max_age_seconds is None or age <= max_age_seconds):
                    return previous
            except TWTradingUniversePending:
                pass
        started = time.perf_counter()
        path = receipt_path(session_date)
        raw, sources, failures = collect_current_sources(tuple(SOURCES), path.parent, session_date,
            fetch=fetch, force=force, max_age_seconds=max_age_seconds if max_age_seconds is not None else 900)
        captured = datetime.now(TAIPEI)
        if captured.date().isoformat() != session_date:
            raise ValueError("official registry acquisition crossed the session date")
        # A failed refresh preserves both successful source bytes and the last
        # accepted pointer. Never publish a partial registry as a smaller market.
        if failures:
            _publish_refresh_status(path.parent, {
                "status": "waiting_source", "session_date": session_date,
                "observed_at_taipei": captured.isoformat(), "sources": sources,
                "failures": failures})
            raise TWTradingUniversePending("官方交易名冊來源尚未完整取得；保留既有驗收版本")
        # Preopen: evaluate already announced effective dates at the open.
        # Midday: retain the true current observation, never pretend it was PIT.
        as_of = max(captured, datetime.combine(captured.date(), daytime(9), TAIPEI))
        invalid_sources = set()
        def parse_source(name, parser):
            try:
                return parser(raw[name])
            except (ValueError, KeyError, TypeError):
                invalid_sources.add(name)
                raise
        try:
            records = parse_source("twse_registry", lambda content: parse_registry(content, "listed", as_of)) + parse_source("tpex_registry", lambda content: parse_registry(content, "otc", as_of))
            if len({r["symbol"] for r in records}) != len(records):
                raise ValueError("official venues have conflicting symbol membership")
            catalogs = {name: parse_source(name,
                lambda content, name=name: (parse_twse_etf_catalog if name == "twse_etf_catalog" else parse_tpex_etf_catalog)(content, session_date))
                for name in CATALOG_NAMES}
            try:
                records, membership = reconcile_active_etfs(records, catalogs)
            except ValueError:
                # A newly active product may arrive in the catalogue before
                # ISIN catches up. Refresh those registries on retry instead
                # of waiting out their normal daily cache TTL.
                invalid_sources.update(("twse_registry", "tpex_registry"))
                raise
            lifecycle_names = ("twse_halts", "tpex_status", "tpex_halts")
            lifecycle = {k: parse_source(k, json.loads) for k in lifecycle_names}
            try:
                halted = halted_symbols(lifecycle, as_of)
            except (ValueError, KeyError, TypeError):
                invalid_sources.update(lifecycle_names)
                raise
            halted.update(parse_source("twse_suspensions", lambda content: current_twse_suspensions(json.loads(content), as_of)))
            corporate, corporate_sources = corporate_suspension_evidence(
                parquet_root or os.getenv("STOCKAGENT_TW_REFERENCE_ROOT", "data_tw_public/stocks"),
                as_of, path.parent / "raw" / session_date)
            halted.update(corporate)
            sources.update(corporate_sources)
            membership.update(session_date=session_date, observed_at_taipei=captured.isoformat(),
                evaluated_at_taipei=as_of.isoformat(), historical_pit_claim=False,
                source_sha256={name: sources[name]["sha256"] for name in ("twse_registry", "tpex_registry", *CATALOG_NAMES)})
            policy_raw = json.dumps(membership, ensure_ascii=False, sort_keys=True).encode()
            policy_digest = hashlib.sha256(policy_raw).hexdigest()
            policy_path = path.parent / "raw" / session_date / (MEMBERSHIP_SOURCE + "." + policy_digest + ".json")
            atomic_write_bytes(policy_path, policy_raw, durable=True)
            sources[MEMBERSHIP_SOURCE] = {"url": MEMBERSHIP_URL, "contract": MEMBERSHIP_CONTRACT,
                "sha256": policy_digest, "bytes": len(policy_raw), "raw_path": str(policy_path.resolve()),
                "observed_at_taipei": captured.isoformat()}
        except (OSError, ValueError, KeyError, TypeError) as exc:
            for name in invalid_sources:
                sources[name]["status"] = "invalid_source"
                atomic_write_json(path.parent / "source_state" / session_date / (name + ".json"), sources[name])
            _publish_refresh_status(path.parent, {
                "status": "invalid_source", "session_date": session_date,
                "observed_at_taipei": captured.isoformat(), "sources": sources,
                "error_type": type(exc).__name__, "error_message": str(exc)})
            raise TWTradingUniversePending("官方交易名冊未通過來源驗收；保留既有驗收版本") from exc
        for row in records:
            row["trading_status"] = "halted" if row["symbol"] in halted else "listed_for_regular_trading"
        payload = {"schema_version": 1, "contract": CONTRACT, "session_date": session_date,
                   "membership_contract": MEMBERSHIP_CONTRACT,
                   "registry_count": membership["registry_count"], "excluded_count": membership["excluded_count"],
                   "observed_at_taipei": captured.isoformat(), "evaluated_at_taipei": as_of.isoformat(),
                   "historical_pit_claim": False, "sources": sources,
                   "records": sorted(records, key=lambda r: r["symbol"]),
                   "listed_count": len(records), "halted_count": sum(r["trading_status"] == "halted" for r in records),
                   "trading_count": sum(r["trading_status"] != "halted" for r in records),
                   "elapsed_ms": round((time.perf_counter() - started) * 1000, 3)}
        payload["fingerprint"] = _identity(payload)
        generation = hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        atomic_write_json(path.parent / "generations" / (generation + ".json"), payload)
        atomic_write_json(path, payload)
        _publish_refresh_status(path.parent, {
            "status": "ready", "session_date": session_date,
            "observed_at_taipei": captured.isoformat(), "fingerprint": payload["fingerprint"],
            "listed_count": payload["listed_count"], "trading_count": payload["trading_count"],
            "halted_count": payload["halted_count"], "elapsed_ms": payload["elapsed_ms"],
            "membership_contract": MEMBERSHIP_CONTRACT, "excluded_count": payload["excluded_count"]})
        return payload


@lru_cache(maxsize=4)
def _read_receipt(path: str, mtime_ns: int, size: int) -> dict:
    del mtime_ns, size
    payload = json.loads(Path(path).read_text())
    if (not isinstance(payload, dict) or payload.get("schema_version") != 1 or payload.get("contract") != CONTRACT
            or payload.get("membership_contract") != MEMBERSHIP_CONTRACT
            or payload.get("fingerprint") != _identity(payload)):
        raise ValueError("official trading registry fingerprint mismatch")
    records = payload["records"]
    if not records or len({r["symbol"] for r in records}) != len(records):
        raise ValueError("invalid official trading registry membership")
    if payload["listed_count"] != len(records) or payload["trading_count"] != sum(r["trading_status"] == "listed_for_regular_trading" for r in records):
        raise ValueError("official trading registry count mismatch")
    if (payload["halted_count"] != sum(r["trading_status"] == "halted" for r in records)
            or any(r["market"] not in {"twse", "tpex"} or r["trading_status"] not in {"halted", "listed_for_regular_trading"}
                   or r["security_type"] not in {"stock", "etf"}
                   or classify_tw_stock_or_etf(r["symbol"]) != r["security_type"] for r in records)):
        raise ValueError("official trading registry type/status mismatch")
    raw_sources = {}
    for name in (*SOURCES, "share_replacement_reference", "share_replacement_summary", "share_replacement_manifest", MEMBERSHIP_SOURCE):
        source = payload["sources"][name]
        raw = Path(source["raw_path"]).read_bytes()
        expected_url = MEMBERSHIP_URL if name == MEMBERSHIP_SOURCE else SOURCES.get(name, "canonical:tw_share_replacement_reference")
        if (source["url"] != expected_url or len(raw) != source["bytes"]
                or hashlib.sha256(raw).hexdigest() != source["sha256"]):
            raise ValueError("official trading registry raw evidence mismatch")
        if name in SOURCES or name == MEMBERSHIP_SOURCE:
            observed = datetime.fromisoformat(source["observed_at_taipei"])
            if (observed.tzinfo is None or observed.astimezone(TAIPEI).date().isoformat() != payload["session_date"]
                    or observed > datetime.fromisoformat(payload["observed_at_taipei"])):
                raise ValueError("official membership source clock mismatch")
            raw_sources[name] = raw
    # The policy version is bound by the existing wire fingerprint through its
    # source receipt. Do not change the envelope hash formula: old execution
    # readers must safely consume the same atomically published generation.
    policy = json.loads(raw_sources[MEMBERSHIP_SOURCE])
    if (not isinstance(policy, dict)
            or payload["sources"][MEMBERSHIP_SOURCE].get("contract") != MEMBERSHIP_CONTRACT
            or policy.get("contract") != MEMBERSHIP_CONTRACT
            or policy.get("session_date") != payload["session_date"]
            or any(policy.get(key) != payload.get(key) for key in (
                "observed_at_taipei", "evaluated_at_taipei", "historical_pit_claim"))
            or policy.get("historical_pit_claim") is not False
            or policy.get("source_sha256") != {name: payload["sources"][name]["sha256"]
                for name in ("twse_registry", "tpex_registry", *CATALOG_NAMES)}):
        raise ValueError("official membership policy/source binding mismatch")
    as_of = datetime.fromisoformat(payload["evaluated_at_taipei"])
    if as_of.tzinfo is None or as_of.astimezone(TAIPEI).date().isoformat() != payload["session_date"]:
        raise ValueError("official membership effective clock mismatch")
    # The writer already parsed every source before atomically publishing its
    # immutable policy/projection. Bind that exact projection here while still
    # verifying ALL raw hashes above; do not parse multi-megabyte ISIN HTML or
    # import its downloader in a 09:00 consumer. Independent engineering checks
    # can replay the same canonical reconciliation from the archived sources.
    if (policy.get("listed_records_sha256") != _listed_records_identity(records)
            or any(type(policy.get(key)) is not int or policy[key] != payload[key]
                for key in ("registry_count", "listed_count", "excluded_count"))
            or policy["registry_count"] != policy["listed_count"] + policy["excluded_count"]
            or not isinstance(policy.get("excluded_records"), list)
            or len(policy["excluded_records"]) != policy["excluded_count"]):
        raise ValueError("official membership reconciliation mismatch")
    return payload


def load_current_universe(session_date: str) -> dict:
    path = receipt_path(session_date)
    try:
        info = path.stat()
        payload = _read_receipt(str(path.resolve()), info.st_mtime_ns, info.st_size)
        observed = datetime.fromisoformat(payload["observed_at_taipei"])
        now = datetime.now(TAIPEI)
        if (session_date != now.date().isoformat() or payload["session_date"] != session_date or observed.tzinfo is None
                or observed.astimezone(TAIPEI).date().isoformat() != session_date
                or observed > now):
            raise ValueError("official trading registry session/observation mismatch")
        return payload
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise TWTradingUniversePending("官方當日交易名冊尚未驗收；不使用模型歷史 alive_mask 作為市場分母") from exc


@lru_cache(maxsize=4)
def _official_index(fingerprint: str, session_date: str) -> tuple[frozenset, tuple, dict]:
    payload = load_current_universe(session_date)
    if payload["fingerprint"] != fingerprint:
        raise TWTradingUniversePending("官方名冊版本更新中；重試同一版本的市場與模型交集")
    official = frozenset(r["symbol"] for r in payload["records"] if r["trading_status"] == "listed_for_regular_trading")
    counts = {market: sum(r["market"] == market and r["symbol"] in official for r in payload["records"]) for market in ("twse", "tpex")}
    return official, tuple(sorted(official)), counts


@lru_cache(maxsize=16)
def _model_index(fingerprint: str, session_date: str, symbols: tuple) -> tuple[np.ndarray, tuple, tuple]:
    official, _symbols, _counts = _official_index(fingerprint, session_date)
    model = set(symbols)
    mask = np.asarray([s in official for s in symbols], dtype=bool)
    mask.flags.writeable = False
    return mask, tuple(sorted(official - model)), tuple(sorted(model - official))


def opening_scope(symbols: list[str], session_date: str) -> tuple[np.ndarray, dict]:
    payload = load_current_universe(session_date)
    official, official_symbols, _counts = _official_index(payload["fingerprint"], session_date)
    mask, not_in_model, not_trading = _model_index(payload["fingerprint"], session_date, tuple(symbols))
    # A caller owns its arrays/lists; it cannot mutate a subsequent mode's view.
    mask = mask.copy()
    return mask, {"contract": CONTRACT, "fingerprint": payload["fingerprint"],
                  "membership_contract": MEMBERSHIP_CONTRACT,
                  "official_registry_count": payload["registry_count"], "official_excluded_count": payload["excluded_count"],
                  "session_date": session_date, "observed_at_taipei": payload["observed_at_taipei"],
                  "official_listed_count": payload["listed_count"], "official_trading_count": len(official),
                  "official_halted_count": payload["halted_count"],
                  "model_symbol_count": len(symbols), "model_official_trading_count": int(mask.sum()),
                  "official_not_in_model_symbols": list(not_in_model),
                  "model_not_trading_symbols": list(not_trading),
                  "symbols": list(official_symbols)}


def public_current_universe(session_date: str) -> dict:
    """Current membership proof, not a revision of historical signal evidence."""
    try:
        payload = load_current_universe(session_date)
        _official, _symbols, counts = _official_index(payload["fingerprint"], session_date)
    except TWTradingUniversePending:
        return {"status": "waiting_source", "contract": CONTRACT,
                "session_date": session_date, "historical_pit_claim": False}
    return {"status": "ready", "contract": CONTRACT,
            "membership_contract": MEMBERSHIP_CONTRACT,
            "registry_count": payload["registry_count"], "excluded_count": payload["excluded_count"],
            "session_date": session_date, "fingerprint": payload["fingerprint"],
            "observed_at_taipei": payload["observed_at_taipei"],
            "listed_count": payload["listed_count"], "halted_count": payload["halted_count"],
            "trading_count": payload["trading_count"], "historical_pit_claim": False,
            "scope": "twse_tpex_regular_stock_etf",
            "venue_trading_counts": dict(counts)}


def official_scope_required(parquet_root: str | Path) -> bool:
    """The same registered mutable source boundary as the live read guard.

    Offline/unmanaged engineering fixtures are not a live-market declaration.
    Registered roots must never silently fall back when today's receipt fails.
    """
    root = Path(parquet_root).resolve().parent
    return any((root / name).exists() for name in (
        "twse_daily_ohlcv.parquet", "tw_corporate_action_reference.parquet",
    ))

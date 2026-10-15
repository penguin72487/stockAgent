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
import threading
import time
from zoneinfo import ZoneInfo

import numpy as np
import requests

from downloader.artifact_io import atomic_write_bytes, atomic_write_json
from downloader.dataset_lock import exclusive_dataset_lock
from stockagent.data.tw_security import classify_tw_stock_or_etf

TAIPEI = ZoneInfo("Asia/Taipei")
CONTRACT = "tw_official_current_regular_stock_etf_universe_v3"
SOURCES = {
    "twse_registry": "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2",
    "tpex_registry": "https://isin.twse.com.tw/isin/C_public.jsp?strMode=4",
    "twse_halts": "https://openapi.twse.com.tw/v1/exchangeReport/TWTAWU",
    "twse_suspensions": "https://www.twse.com.tw/rwd/zh/violation/stop?response=json",
    "tpex_status": "https://www.tpex.org.tw/openapi/v1/tpex_cmode",
    "tpex_halts": "https://www.tpex.org.tw/openapi/v1/tpex_spendi_history",
}
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
    response = requests.get(url, timeout=(3, 15))
    response.raise_for_status()
    return response.content


def _cached_source(name: str, root: Path, session_date: str, max_age: float) -> tuple[bytes, dict] | None:
    """Keep successful transport facts across a failed aggregate refresh."""
    try:
        state_path = root / "source_state" / session_date / (name + ".json")
        if state_path.exists():
            source = json.loads(state_path.read_text())
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
                if max_age_seconds is None or age <= max_age_seconds:
                    return previous
            except TWTradingUniversePending:
                pass
        started = time.perf_counter()
        path = receipt_path(session_date)
        fetch = fetch or _fetch
        raw, failures, transport = {}, {}, {}
        with ThreadPoolExecutor(max_workers=len(SOURCES), thread_name_prefix="tw-official-registry") as pool:
            pending = {}
            def receive(url):
                content = fetch(url)
                return content, datetime.now(TAIPEI).isoformat()
            for name, url in SOURCES.items():
                # ISIN membership is daily; halt/status inputs are short-lived.
                # Keep each real source clock rather than refreshing successful
                # multi-MB registry pages whenever one status request fails.
                source_age = 86400 if name.endswith("registry") else (max_age_seconds if max_age_seconds is not None else 900)
                cached = None if force else _cached_source(name, path.parent, session_date, source_age)
                if cached is not None:
                    raw[name], transport[name] = cached
                else:
                    pending[pool.submit(receive, url)] = name
            for future in as_completed(pending):
                name = pending[future]
                try:
                    raw[name], observed_at = future.result()
                    transport[name] = {"observed_at_taipei": observed_at, "status": "received"}
                except Exception as exc:
                    failures[name] = type(exc).__name__
        captured = datetime.now(TAIPEI)
        if captured.date().isoformat() != session_date:
            raise ValueError("official registry acquisition crossed the session date")
        sources = {}
        for name, content in raw.items():
            digest = hashlib.sha256(content).hexdigest()
            raw_path = path.parent / "raw" / session_date / (name + "." + digest + (".html" if name.endswith("registry") else ".json"))
            atomic_write_bytes(raw_path, content, durable=True)
            sources[name] = {"url": SOURCES[name], "sha256": digest,
                             "bytes": len(content), "raw_path": str(raw_path.resolve()),
                             "observed_at_taipei": transport[name]["observed_at_taipei"], "status": "received"}
            atomic_write_json(path.parent / "source_state" / session_date / (name + ".json"), sources[name])
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
            "halted_count": payload["halted_count"], "elapsed_ms": payload["elapsed_ms"]})
        return payload


@lru_cache(maxsize=4)
def _read_receipt(path: str, mtime_ns: int, size: int) -> dict:
    del mtime_ns, size
    payload = json.loads(Path(path).read_text())
    if payload.get("schema_version") != 1 or payload.get("contract") != CONTRACT or payload.get("fingerprint") != _identity(payload):
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
    for name in (*SOURCES, "share_replacement_reference", "share_replacement_summary", "share_replacement_manifest"):
        source = payload["sources"][name]
        raw = Path(source["raw_path"]).read_bytes()
        expected_url = SOURCES.get(name, "canonical:tw_share_replacement_reference")
        if (source["url"] != expected_url or len(raw) != source["bytes"]
                or hashlib.sha256(raw).hexdigest() != source["sha256"]):
            raise ValueError("official trading registry raw evidence mismatch")
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

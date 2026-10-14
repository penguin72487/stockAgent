"""Read-only official daily prices for public views, not execution evidence.

Reuse the canonical TWSE/TPEx materialization. No network, broker session,
ledger write, or invented intraday timestamp is allowed in this projection.
"""
from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping
from datetime import datetime
import hashlib
import math
from pathlib import Path
import threading
from typing import Any
from urllib.parse import urlparse

from stockagent.live.public_quote_display import PROTECTED_OBSERVATIONS_KEY, TAIPEI, _session


DAILY_QUOTE_FIELD = "official_daily_quote"
_SPECS = (
    ("twse_daily_ohlcv", "TWSE", "證券代號", "TWSE", ("twse.com.tw",),
     {"open": "開盤價", "high": "最高價", "low": "最低價", "close": "收盤價"}),
    ("tpex_daily_ohlcv", "TPEX", "代號", "TPEx", ("tpex.org.tw",),
     {"open": "開盤", "high": "最高", "low": "最低", "close": "收盤"}),
)


def _day(value: Any) -> str | None:
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date().isoformat()
    except ValueError:
        return None


def _price(value: Any) -> float | None:
    try:
        result = float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) and result > 0 else None


def _closing_clock(value: Any) -> bool:
    if _session(value) is None:
        return False
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(TAIPEI)
    return parsed.hour == 13 and parsed.minute == 30


class OfficialDailyQuoteSource:
    """Bounded per-session cache; atomic canonical replacement invalidates it.

    Arrow predicates/column projection avoid decoding the entire history. A
    single flight loads a session once, shared by signals, positions and modes.
    """

    def __init__(self, public_root: Path, *, maximum_sessions: int = 64) -> None:
        self.public_root = Path(public_root)
        self.paths = tuple(self.public_root / f"{spec[0]}.parquet" for spec in _SPECS)
        self.maximum_sessions = max(1, maximum_sessions)
        self._lock = threading.RLock()
        self._signature: tuple[Any, ...] = ()
        self._sessions: OrderedDict[str, dict[str, dict[str, Any]]] = OrderedDict()

    def signature(self) -> tuple[Any, ...]:
        values = []
        for dataset, *_ in _SPECS:
            try:
                stat = (self.public_root / f"{dataset}.parquet").stat()
                values.append((stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns))
            except OSError:
                values.append(None)
        return tuple(values)

    def revision(self) -> str:
        return hashlib.sha256(repr(self.signature()).encode()).hexdigest()[:16]

    def _load(self, session: str) -> dict[str, dict[str, Any]]:
        import pyarrow.parquet as pq

        result: dict[str, dict[str, Any]] = {}
        ambiguous: set[str] = set()
        for dataset, venue, symbol_column, source, domains, columns in _SPECS:
            path = self.public_root / f"{dataset}.parquet"
            try:
                rows = pq.read_table(path, filters=[("date", "==", session)], columns=[
                    "date", symbol_column, *columns.values(), "_dataset", "_source",
                    "_downloaded_at_utc", "_url",
                ]).to_pylist()
            except (OSError, ValueError, KeyError):
                # A broken source cannot manufacture prices or stop the other venue.
                continue
            for row in rows:
                symbol = str(row.get(symbol_column) or "").strip()
                if not symbol or row.get("date") != session:
                    continue
                clock = row.get("_downloaded_at_utc")
                host = urlparse(str(row.get("_url") or "")).hostname or ""
                verified = (
                    row.get("_dataset") == dataset and row.get("_source") == source
                    and str(row.get("_url") or "").startswith("https://")
                    and any(host == domain or host.endswith("." + domain) for domain in domains)
                    and _session(clock) is not None
                )
                if not verified:
                    ambiguous.add(symbol)
                    continue
                prices = {field: _price(row.get(column)) for field, column in columns.items()}
                if not any(value is not None for value in prices.values()):
                    continue  # Official no-trade markers are not zero prices.
                item = {"symbol": symbol, "session_date": session, "venue": venue,
                        "dataset_id": dataset, "grain": "daily", "received_at": clock,
                        **prices}
                prior = result.get(symbol)
                if prior and (prior["venue"] != venue or any(prior[k] != item[k] for k in prices)):
                    ambiguous.add(symbol)
                elif prior is None or str(clock) > str(prior["received_at"]):
                    result[symbol] = item
        for symbol in ambiguous:
            result.pop(symbol, None)
        return result

    def session(self, session: str) -> dict[str, dict[str, Any]]:
        if _day(session) is None:
            return {}
        with self._lock:
            signature = self.signature()
            if signature != self._signature:
                self._sessions.clear()
                self._signature = signature
            if session not in self._sessions:
                items = self._load(session)
                # Never cache a mixed generation read across an atomic promotion.
                if self.signature() != signature:
                    return {}
                self._sessions[session] = items
                while len(self._sessions) > self.maximum_sessions:
                    self._sessions.popitem(last=False)
            self._sessions.move_to_end(session)
            return self._sessions[session]

    @staticmethod
    def _observation(fact: Mapping[str, Any], field: str, source_field: str) -> dict[str, Any]:
        return {"source_id": fact["dataset_id"], "grant_id": "government-open-data-v1",
                "dataset_id": fact["dataset_id"], "grain": "daily",
                "source_field": source_field, "price_role": field,
                "symbol": fact["symbol"], "venue": fact["venue"],
                "session_date": fact["session_date"],
                # Acquisition clock is truthful; the daily source has no tick clock.
                "observed_at": fact["received_at"], "value": fact[source_field]}

    def enrich(self, record: Mapping[str, Any], *, session_date: str | None = None) -> Mapping[str, Any]:
        symbol = str(record.get("symbol") or "")
        session = _day(record.get("session_date")) or _day(session_date)
        if not symbol or not session:
            return record
        fact = self.session(session).get(symbol)
        output = dict(record)
        if fact is not None:
            daily = dict(fact)
            daily[PROTECTED_OBSERVATIONS_KEY] = {
                key: self._observation(fact, key, key) for key in ("open", "high", "low", "close")
                if fact[key] is not None
            }
            output[DAILY_QUOTE_FIELD] = daily
        protected = record.get(PROTECTED_OBSERVATIONS_KEY)
        observations = dict(protected) if isinstance(protected, Mapping) else {}

        def verify(field: str, day: str | None, source_field: str) -> None:
            # An older entry/next-session exit has its own market date. Today's
            # report is neither necessary nor sufficient to verify that price.
            role_fact = self.session(day).get(symbol) if day else None
            if (role_fact is not None and role_fact[source_field] is not None
                    and record.get(field) == role_fact[source_field]):
                observations.setdefault(field, self._observation(role_fact, field, source_field))

        # Sizing uses official open, but a 09:01 VWAP fill is a different fact.
        if record.get("sizing_open_price") is not None:
            verify("sizing_open_price", session, "open")
        # Overnight historical replay explicitly names daily auction proxies.
        entry_source = str(record.get("entry_price_source") or "").casefold()
        entry_day = _session(record.get("entry_exchange_at") or record.get("entry_at"))
        if "official_close" in entry_source or entry_source == "actual_close_auction_print":
            if _closing_clock(record.get("entry_exchange_at") or record.get("entry_at")):
                verify("entry_price", entry_day, "close")
        elif "official_open" in entry_source:
            verify("entry_price", entry_day, "open")
        # Stock Buy & Hold origins are explicitly rebased to the previous
        # official close. A generic broker fill or a minute VWAP cannot use
        # this branch, even if its numerical value happens to be equal.
        if (record.get("instrument_type") == "stock_buy_and_hold"
                and record.get("capital_basis") == "one_board_lot_previous_close_notional"
                and record.get("replay_basis") == "cross_session_buy_and_hold_from_previous_close"
                and _closing_clock(record.get("entry_at"))):
            verify("entry_price", _session(record.get("entry_at")), "close")
        # A terminal paper close explicitly settled at the official close can
        # be independently verified. An intraday exit, even at the same number,
        # is not thereby an official closing observation.
        if record.get("exit_reason") in {
            "13_30_unlimited_close_paper_settlement", "13_30_official_close_settlement",
        } and _closing_clock(record.get("exit_exchange_at") or record.get("exit_at")):
            exit_day = _session(record.get("exit_exchange_at") or record.get("exit_at"))
            for field in ("exit_price", "last_exit_price"):
                verify(field, exit_day, "close")
            mark_day = _session(record.get("last_quote_at") or record.get("last_mark_at"))
            if mark_day == exit_day and _closing_clock(record.get("last_quote_at") or record.get("last_mark_at")):
                verify("last_mark_price", mark_day, "close")
        if (record.get("exit_reason") == "next_session_open_auction_fill"
                and record.get("opening_exit_order_status") == "filled"):
            exit_day = _session(record.get("exit_exchange_at") or record.get("exit_at"))
            if exit_day and exit_day == record.get("exit_session_date") and exit_day > session:
                for field in ("exit_price", "last_exit_price"):
                    verify(field, exit_day, "open")
        prior_day = _day(record.get("daily_return_previous_close_date"))
        if prior_day and any(record.get(key) is not None for key in (
            "daily_return_previous_close", "daily_return_reference_price",
        )):
            for field in ("daily_return_previous_close", "daily_return_reference_price"):
                verify(field, prior_day, "close")
        if observations:
            output[PROTECTED_OBSERVATIONS_KEY] = observations
        return output

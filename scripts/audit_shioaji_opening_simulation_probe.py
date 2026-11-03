#!/usr/bin/env python3
"""Read-only comparison of stock simulation deals with opening observations.

This is an offline observer, not an order runner or another market-data
collector. It never creates a journal, logs in, submits orders, or modifies the
paper ledger. Only the canonical simulation journal's StockDeal receipts can
enter the comparison. Source-event wall clocks are not wire or exchange clocks.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

from downloader.artifact_io import atomic_write_json
from stockagent.live.opening_price_journal import observation_values
from stockagent.live.tw_stock_simulation_execution import BROKER_FILL, account_fingerprint
from stockagent.private_json import projected_object_values


TAIPEI = ZoneInfo("Asia/Taipei")
DEFAULT_MARKET = "shioaji_opening_order_probe"


def _at(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp is missing")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("timestamp must include a timezone")
    return result.astimezone(TAIPEI)


def _price(value: object) -> Decimal:
    if isinstance(value, bool):
        raise ValueError("invalid price")
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("invalid price") from exc
    if not result.is_finite() or result <= 0:
        raise ValueError("invalid price")
    return result


def _elapsed_ms(at: datetime, gate: datetime) -> float:
    return round((at - gate).total_seconds() * 1000, 3)


def simulation_deals(path: Path, session: date, market: str) -> dict:
    """Take a consistent read-only SQLite snapshot; never create an empty DB."""
    if not path.is_file():
        return {"state": "journal_missing", "deals": [], "invalid_receipts": 0}
    connection = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        metadata = dict(connection.execute("SELECT key,value FROM meta"))
        if metadata.get("schema") != "1" or not metadata.get("account"):
            raise ValueError("not a canonical stock simulation journal")
        accepted, invalid = [], 0
        for row in connection.execute(
            "SELECT fills.payload AS fill, intents.payload AS intent, intents.tag AS tag "
            "FROM fills JOIN intents ON fills.intent_key=intents.key ORDER BY fills.sequence"
        ):
            try:
                fill, intent = json.loads(row["fill"]), json.loads(row["intent"])
                if intent.get("market") != market or intent.get("session_date") != session.isoformat():
                    continue
                filled = _at(fill["fill_at"])
                signalled = _at(intent["signal_at"])
                if (fill.get("fill_contract") != BROKER_FILL
                        or fill.get("simulation_only") is not True
                        or fill.get("broker_confirmed") is not True
                        or fill.get("intent_key") != intent.get("key")
                        or fill.get("market") != market
                        or fill.get("symbol") != intent.get("symbol")
                        or fill.get("action") != intent.get("action")
                        or not fill.get("trade_id") or not fill.get("exchange_seq")
                        or type(fill.get("shares")) is not int or fill["shares"] <= 0
                        or fill["shares"] % 1000
                        or filled.date() != session or signalled.date() != session
                        or filled < signalled):
                    raise ValueError("simulation receipt identity mismatch")
                _price(fill["price"])
                accepted.append({"receipt": fill, "tag": row["tag"], "received_at": None})
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                invalid += 1

        # A fill's ts is not its callback arrival. Recover arrival only from a
        # matching account/deal event in this same SQLite read transaction.
        pending = {(str(row["receipt"]["trade_id"]), str(row["receipt"]["exchange_seq"])): row
                   for row in accepted}
        for row in connection.execute(
            "SELECT payload,received_at FROM events WHERE state IN ('StockDeal','SDEAL') ORDER BY id"
        ):
            try:
                event = json.loads(row["payload"])
                key = (str(event.get("trade_id") or ""), str(event.get("exchange_seq") or ""))
                match = pending.get(key)
                if match is None:
                    continue
                fill = match["receipt"]
                received = _at(row["received_at"])
                if (account_fingerprint(str(event.get("broker_id") or ""),
                                        str(event.get("account_id") or "")) != metadata["account"]
                        or str(event.get("code") or "") != fill["symbol"]
                        or str(event.get("action") or "") != fill["action"]
                        or (event.get("custom_field") and event["custom_field"] != match["tag"])
                        or str(event.get("order_lot") or "") != "Common"
                        or type(event.get("quantity")) is not int
                        or event["quantity"] * 1000 != fill["shares"]
                        or _price(event.get("price")) != _price(fill["price"])
                        or received.date() != session or received < _at(fill["fill_at"])):
                    continue
                if match["received_at"] is None or received < match["received_at"]:
                    match["received_at"] = received
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                continue
        return {"state": "observed" if accepted else "no_simulation_deals",
                "deals": accepted, "invalid_receipts": invalid}
    finally:
        connection.close()


def source_observations(path: Path, session: date, symbols: set[str]) -> dict:
    """Read one bounded append-only prefix, keeping source timestamps distinct."""
    if not path.is_file():
        return {"state": "source_journal_missing", "rows": {}, "invalid_lines": 0}
    gate = datetime.combine(session, datetime.min.time(), TAIPEI).replace(hour=9)
    before = path.stat()
    digest, consumed, invalid, examined = hashlib.sha256(), 0, 0, 0
    rows: dict[str, dict[str, dict]] = {symbol: {} for symbol in symbols}
    with path.open("rb") as handle:
        while consumed < before.st_size:
            raw = handle.readline(before.st_size - consumed)
            if not raw:
                break
            consumed += len(raw)
            digest.update(raw)
            try:
                if not raw.endswith(b"\n"):
                    raise ValueError("incomplete source journal line")
                header = projected_object_values(raw, ("session_date", "provider", "event", "observed_at_taipei"))
                if header is None:
                    raise ValueError("invalid source event")
                event_date, provider, kind, clock = header
                if event_date != session.isoformat() or kind != "observations_delta" or provider == "merged":
                    continue
                examined += 1
                if not isinstance(provider, str) or not provider:
                    raise ValueError("source provider is missing")
                observed = _at(clock)
                if observed < gate or observed.date() != session:
                    raise ValueError("source event outside session")
                event = json.loads(raw)
                values = list(observation_values(event))
                staged = []
                for symbol, state, source, price, available, market_ms, received_ms, simtrade in values:
                    if symbol not in symbols or state != "valid_open":
                        continue
                    if (available is not True or simtrade in (True, 1)
                            or type(market_ms) is not int or market_ms < int(gate.timestamp() * 1000)
                            or market_ms > int(observed.timestamp() * 1000)):
                        raise ValueError("unverified source opening observation")
                    opening = _price(price)
                    row = {"provider": provider, "source": source, "open_price": str(opening),
                           "journal_event_at": observed.isoformat(),
                           "journal_event_after_open_ms": _elapsed_ms(observed, gate),
                           "market_timestamp_ms": market_ms,
                           "source_received_timestamp_ms": received_ms if type(received_ms) is int else None}
                    staged.append((symbol, row))
                # Reject an invalid bulk event as a whole, not a partial prefix.
                for symbol, row in staged:
                    previous = rows[symbol].get(provider)
                    if previous is None or observed < _at(previous["journal_event_at"]):
                        if previous is not None and previous["open_price"] != row["open_price"]:
                            row["different_later_prices"] = sorted(set(
                                previous.get("different_later_prices", []) + [previous["open_price"]]))
                        rows[symbol][provider] = row
                    elif previous["open_price"] != row["open_price"]:
                        previous["different_later_prices"] = sorted(set(
                            previous.get("different_later_prices", []) + [row["open_price"]]))
            except (KeyError, TypeError, ValueError, json.JSONDecodeError, OverflowError):
                invalid += 1
    after = path.stat()
    stable = (before.st_dev == after.st_dev and before.st_ino == after.st_ino
              and consumed == before.st_size and after.st_size >= before.st_size)
    return {"state": "observed" if stable and not invalid else "degraded",
            "rows": rows, "invalid_lines": invalid,
            "examined_delta_events": examined,
            "prefix": {"bytes": consumed, "sha256": digest.hexdigest(),
                       "same_inode_and_complete_prefix": stable}}


def official_daily_reference(session: date, twse: Path, tpex: Path, symbols: set[str]) -> dict:
    """Optional post-close aggregate comparison, never an opening-time feed.

    Reuse the existing replay reader; hash both source files around its read.
    File hashes make this repeatable, but a caller-supplied path is not itself
    a collector receipt or proof of the source's publication/freshness.
    """
    from scripts import rebuild_tw_day_trade_open_price_replay as replay

    paths = (twse, tpex)
    if not all(path.is_file() for path in paths):
        return {"state": "daily_aggregate_missing", "rows": {}, "files": []}

    def digest(path: Path) -> str:
        value = hashlib.sha256()
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                value.update(block)
        return value.hexdigest()

    before = [digest(path) for path in paths]
    replay._official_aggregate_daily_rows.cache_clear()
    rows = replay._official_aggregate_daily_rows(twse, tpex, session)
    if before != [digest(path) for path in paths]:
        raise RuntimeError("daily aggregate changed during comparison")
    result = {}
    for symbol in symbols:
        row = rows.get(symbol)
        if row is None:
            result[symbol] = {"state": "daily_row_missing"}
            continue
        if replay._official_zero_volume_no_ohlc(row):
            result[symbol] = {"state": "zero_volume_no_daily_trade", "source": row["_official_source"]}
            continue
        try:
            # A positive daily open with unknown/zero daily volume is not
            # sufficient evidence of a first trade.
            volume = _price(row.get("volume_shares"))
            opening = _price(row.get("open"))
        except ValueError:
            result[symbol] = {"state": "daily_trade_or_open_unverified", "source": row["_official_source"]}
            continue
        result[symbol] = {"state": "daily_open_with_positive_volume", "source": row["_official_source"],
                          "open_price": str(opening), "daily_volume_shares": str(volume)}
    return {"state": "loaded_source_file_origin_and_receipts_not_verified", "rows": result,
            "files": [{"path": str(path.resolve()), "sha256": sha} for path, sha in zip(paths, before)]}


def audit(session: date, journal: Path, source_journal: Path, market: str = DEFAULT_MARKET,
          *, daily_paths: tuple[Path, Path] | None = None) -> dict:
    gate = datetime.combine(session, datetime.min.time(), TAIPEI).replace(hour=9)
    simulation = simulation_deals(journal, session, market)
    symbols = {item["receipt"]["symbol"] for item in simulation["deals"]}
    # There is no reason to scan a potentially large live journal without an
    # actual deal. In particular, no rows must remain 'waiting', not 'success'.
    source = (source_observations(source_journal, session, symbols) if symbols
              else {"state": "not_read_without_simulation_deal", "rows": {}, "invalid_lines": 0})
    daily = (official_daily_reference(session, *daily_paths, symbols) if symbols and daily_paths
             else {"state": "not_requested_or_no_simulation_deal", "rows": {}, "files": []})
    comparisons = []
    for item in simulation["deals"]:
        fill, received = item["receipt"], item["received_at"]
        observed = list(source["rows"].get(fill["symbol"], {}).values())
        observed.sort(key=lambda row: (row["journal_event_at"], row["provider"]))
        fill_price = _price(fill["price"])
        source_prices = {_price(price) for row in observed
                         for price in [row["open_price"], *row.get("different_later_prices", [])]}
        source_rows = []
        for row in observed:
            compared = {**row, "same_price_as_simulation_deal": fill_price == _price(row["open_price"]),
                        "simulation_minus_source_price": str(fill_price - _price(row["open_price"]))}
            compared["callback_minus_source_journal_event_ms"] = (
                round((received - _at(row["journal_event_at"])).total_seconds() * 1000, 3)
                if received is not None else None)
            source_rows.append(compared)
        reference = dict(daily["rows"].get(fill["symbol"], {}))
        if reference.get("state") == "daily_open_with_positive_volume":
            reference["same_price_as_simulation_deal"] = fill_price == _price(reference["open_price"])
            reference["simulation_minus_daily_open_price"] = str(fill_price - _price(reference["open_price"]))
        comparisons.append({"receipt_id": fill["receipt_id"], "symbol": fill["symbol"],
            "action": fill["action"], "shares": fill["shares"], "simulation_price": str(fill_price),
            "simulation_fill_at": fill["fill_at"],
            "simulation_fill_after_open_ms": _elapsed_ms(_at(fill["fill_at"]), gate),
            "callback_received_at": received.isoformat() if received is not None else None,
            "callback_received_after_open_ms": _elapsed_ms(received, gate) if received is not None else None,
            "source_price_conflict": len(source_prices) > 1, "sources": source_rows,
            "postclose_daily_reference": reference or None})
    if not comparisons:
        comparison_state = "waiting_simulation_deal"
    elif (simulation["invalid_receipts"] or source["state"] != "observed"
          or any(not row["sources"] or row["callback_received_at"] is None
                 or row["source_price_conflict"] for row in comparisons)):
        comparison_state = "incomplete_or_conflicting_evidence"
    else:
        comparison_state = "observed_comparison_not_exchange_price_verified"
    return {"contract": "shioaji_simulation_vs_opening_observations_v1",
        "session_date": session.isoformat(), "simulation_only": True,
        "orders_submitted": False, "provider_requests": False, "ledger_modified": False,
        "comparison_state": comparison_state,
        "simulation_state": simulation["state"], "simulation_invalid_receipts": simulation["invalid_receipts"],
        "source_state": source["state"], "source_invalid_lines": source["invalid_lines"],
        "source_examined_delta_events": source.get("examined_delta_events", 0),
        "postclose_daily_state": daily["state"], "postclose_daily_files": daily["files"],
        "source_prefix": source.get("prefix"), "simulation_deal_count": len(comparisons),
        "comparisons": comparisons,
        "limitations": ["simulation_deal_is_not_exchange_open_or_real_broker_fill",
            "source_validation_scope_is_targeted_observations_delta_not_all_provider_events",
            "source_journal_event_clock_is_not_wire_or_fsync_completion",
            "wall_clock_differences_are_not_monotonic_or_exchange_latency",
            "matching_a_source_does_not_prove_its_price_is_official_open",
            "daily_aggregate_file_origin_and_publication_receipt_not_verified"]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--journal", type=Path, required=True, help="existing canonical simulation SQLite journal")
    parser.add_argument("--source-journal", type=Path, required=True, help="existing opening acquisition JSONL")
    parser.add_argument("--market", default=DEFAULT_MARKET)
    parser.add_argument("--twse-daily", type=Path, help="optional post-close TWSE aggregate parquet")
    parser.add_argument("--tpex-daily", type=Path, help="optional post-close TPEx aggregate parquet")
    parser.add_argument("--output", type=Path, help="optional separate diagnostic JSON; never the input journal")
    args = parser.parse_args()
    if args.output and args.output.resolve() in {args.journal.resolve(), args.source_journal.resolve()}:
        parser.error("output must not replace an evidence input")
    if bool(args.twse_daily) != bool(args.tpex_daily):
        parser.error("--twse-daily and --tpex-daily must be supplied together")
    daily_paths = (args.twse_daily, args.tpex_daily) if args.twse_daily else None
    if args.output and daily_paths and args.output.resolve() in {path.resolve() for path in daily_paths}:
        parser.error("output must not replace an evidence input")
    report = audit(args.session_date, args.journal, args.source_journal, args.market,
                   daily_paths=daily_paths)
    if args.output:
        atomic_write_json(args.output, report, durable=True, compact=True)
    print(json.dumps(report, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

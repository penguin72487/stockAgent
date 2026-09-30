"""Sponsor per-ID sources, using the existing Complement queue and quota.

Large day/month histories have a compact persisted frontier. Only a bounded
working set is materialized; the frontier is NOT a completed coverage claim.
SponsorPro object storage is deliberately not used with a Sponsor credential.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
import hashlib
import json
import sqlite3
from typing import Any


CATALOG_URL = "https://finmind.github.io/llms-full.txt"
API_BASE = "https://api.finmindtrade.com/api/v4/"
CONTRACT_VERSION = 2
WORKING_SET = 128
MARKET_HISTORY_DATASETS = frozenset({'TaiwanStockConvertibleBondMonthlyAnalysis'})
MARKET_HISTORY_PROOF = 'artifacts/data_quality/finmind_call_efficiency_2026-09-30/range_acceptance.json'


@dataclass(frozen=True)
class Source:
    universe: str
    first: date
    grain: str
    priority: int
    release_hour: int
    endpoint: str = "data"
    identity_field: str = "stock_id"


SOURCES = {
    "TaiwanFuturesSpreadTrading": Source("futures", date(2007, 10, 8), "history", 2, 17, identity_field="futures_id"),
    # Optional data_id filters this table; omitting it returns every bond over
    # the full date range. Verified against the per-ID response and end date.
    "TaiwanStockConvertibleBondMonthlyAnalysis": Source("market", date(2026, 5, 1), "history", 0, 18, identity_field="cb_id"),
    "TaiwanAssetSwapFixedIncomeDaily": Source("bonds", date(2011, 5, 3), "history", 2, 0),
    "TaiwanAssetSwapOptionDaily": Source("bonds", date(2011, 5, 3), "history", 2, 0),
    "TaiwanStockTradingDailyReportSecIdAgg": Source("stocks", date(2021, 6, 30), "derived", 7, 21,
                                                                 "taiwan_stock_trading_daily_report_secid_agg"),
    "TaiwanStockKBar": Source("stocks", date(2019, 1, 1), "day", 8, 16),
    "TaiwanFuturesKBar": Source("futures", date(2011, 1, 3), "day", 8, 17, identity_field="futures_id"),
    "TaiwanStockTradingDailyReport": Source("stocks", date(2021, 6, 30), "day", 8, 21,
                                                      "taiwan_stock_trading_daily_report"),
    # A broker request covers all warrants that day without requiring a current
    # warrant-only universe (which omits expired warrants). Preserve broker grain.
    "TaiwanStockWarrantTradingDailyReport": Source("brokers", date(2023, 6, 21), "day", 8, 22,
                                                             endpoint="taiwan_stock_warrant_trading_daily_report",
                                                             identity_field="securities_trader_id"),
    "USStockPriceMinute": Source("us", date(2021, 4, 28), "day", 8, 8),
    "TaiwanStockPriceTick": Source("stocks", date(2018, 12, 7), "day", 10, 16),
    "TaiwanFuturesTick": Source("futures", date(2011, 1, 3), "day", 10, 17, identity_field="futures_id"),
    "TaiwanOptionTick": Source("options", date(2011, 1, 3), "day", 10, 17, identity_field="option_id"),
    "TaiwanFuturesSpreadTick": Source("futures", date(2026, 4, 27), "day", 10, 17, identity_field="futures_id"),
}


def history_floor(dataset: str, identifier: str) -> date:
    return date(2005, 1, 3) if dataset == 'TaiwanStockKBar' and identifier == 'TAIEX' else SOURCES[dataset].first


def _month_next(day: date) -> date:
    return date(day.year + (day.month == 12), day.month % 12 + 1, 1)


def covered_range(source: Source, partition: str, today: date) -> tuple[date, date]:
    if source.grain == "history":
        return source.first, today
    first = date.fromisoformat(partition)
    return (max(source.first, first), min(today, _month_next(first) - timedelta(days=1))
            if source.grain == "month" else first)


def request_contract(dataset: str, identifier: str, partition: str, today: date) -> tuple[str, dict, dict]:
    source = SOURCES[dataset]
    if source.grain == 'derived':
        raise ValueError('broker_aggregate_is_derived_from_verified_daily_report_not_requested_per_pair')
    start, end = covered_range(source, partition, today)
    if source.universe == 'market' and identifier:
        raise ValueError('market_history_requires_empty_identifier')
    params = ({} if source.universe == 'market' else
              {"securities_trader_id" if source.universe == 'brokers' else "data_id": identifier})
    if source.endpoint == "data":
        params["dataset"] = dataset
    params["date" if source.endpoint in {"taiwan_stock_trading_daily_report", "taiwan_stock_warrant_trading_daily_report"}
           else "start_date"] = start.isoformat()
    if source.grain != "day":
        params["end_date"] = end.isoformat()
    metadata = {
        "supplemental_contract_version": CONTRACT_VERSION, "documentation_url": CATALOG_URL,
        "endpoint": source.endpoint, "query_shape": f"per_{source.universe}_{source.grain}",
        "request_start_date": start.isoformat(), "request_end_date": end.isoformat(),
        "request_end_inclusive": True, "request_count": 1, "data_id": identifier,
        "universe_verified_complete": False, "historical_point_in_time": False,
        "volume_policy": "native_provider_units_preserved_not_assumed_shares",
    }
    if source.universe == 'market':
        metadata.update(query_shape='whole_market_inclusive_date_range',
                        query_contract_evidence=MARKET_HISTORY_PROOF,
                        identifier_filter=None, universe_basis='all_ids_returned_by_provider_not_master_filtered')
    return API_BASE + source.endpoint, params, metadata


def validate_response(dataset: str, identifier: str, partition: str, today: date, rows: list[dict]) -> None:
    source = SOURCES[dataset]
    start, end = covered_range(source, partition, today)
    for row in rows:
        stamp = str(row.get("date", ""))
        try:
            day = date.fromisoformat(stamp[:10])
        except ValueError:
            raise ValueError("supplemental_invalid_date") from None
        if not start <= day <= end:
            raise ValueError("supplemental_response_outside_range")
        actual_id = row.get(source.identity_field)
        if source.universe == 'market':
            if identifier or not isinstance(actual_id, str) or not actual_id.strip():
                raise ValueError('supplemental_missing_market_identity')
        elif str(actual_id or "") != identifier:
            raise ValueError("supplemental_wrong_data_id")


def _migrate_market_history(connection: sqlite3.Connection, now: datetime) -> None:
    """Keep exact old queue evidence; a superseded shape is not downloaded.

    This runs inside the existing worker lock/transaction. A rollback restores
    every old task; no raw receipt/Parquet is overwritten or removed.
    """
    connection.execute('CREATE TABLE IF NOT EXISTS finmind_query_shape_migrations ('
                       'version INTEGER,dataset TEXT,data_id TEXT,partition TEXT,prior_task_json TEXT,'
                       'changed_at_utc TEXT,PRIMARY KEY(version,dataset,data_id,partition))')
    for dataset in MARKET_HISTORY_DATASETS & SOURCES.keys():
        cursor = connection.execute("SELECT * FROM tasks WHERE dataset=? AND data_id!='' "
                                    "AND state!='deprecated_query_shape'", (dataset,))
        columns = [item[0] for item in cursor.description]
        for row in cursor.fetchall():
            prior = dict(zip(columns, row))
            connection.execute('INSERT OR IGNORE INTO finmind_query_shape_migrations VALUES (?,?,?,?,?,?)',
                (CONTRACT_VERSION, dataset, prior['data_id'], prior['partition'],
                 json.dumps(prior, sort_keys=True), now.isoformat()))
        connection.execute("UPDATE tasks SET state='deprecated_query_shape',next_attempt_at_utc=NULL "
                           "WHERE dataset=? AND data_id!='' AND state!='deprecated_query_shape'", (dataset,))


def seed(connection: sqlite3.Connection, universes: dict[str, list[str]], now: datetime,
         *, day_decision=None) -> dict[str, dict]:
    """Bound queue growth; resume even when a process stops after a batch.

    Each identity has an older-history cursor and a newer-data cursor. The
    latter can catch up every intervening day after downtime, not just today.
    Cash-session exclusions are intentionally not guessed from weekdays here;
    historical/overnight instruments need their source-specific calendars.
    """
    from downloader.finmind_scheduling import TAIPEI

    local = now.astimezone(TAIPEI)
    connection.execute("CREATE TABLE IF NOT EXISTS finmind_source_frontiers ("
                       "dataset TEXT,data_id TEXT,older_than TEXT,newer_than TEXT,"
                       "PRIMARY KEY(dataset,data_id))")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_finmind_frontier_older "
                       "ON finmind_source_frontiers(dataset,older_than DESC,data_id)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_finmind_frontier_newer "
                       "ON finmind_source_frontiers(dataset,newer_than,data_id)")
    connection.execute("CREATE TABLE IF NOT EXISTS finmind_frontier_universes "
                       "(dataset TEXT PRIMARY KEY,fingerprint TEXT)")
    _migrate_market_history(connection, now)
    for dataset, source in SOURCES.items():
        if source.grain == 'derived':
            continue
        ids = [''] if source.universe == 'market' else sorted(set(universes.get(source.universe, [])))
        if dataset == 'TaiwanStockKBar' and ids:
            ids = sorted(set(ids) | {'TAIEX'})
        if not ids:
            continue
        eligible = local.date() - timedelta(days=int(local.hour < source.release_hour))
        # Midnight sources describe an update window ending at 24:00.
        if source.release_hour == 0 or source.universe == "us":
            eligible = local.date() - timedelta(days=1)
        anchor = eligible.replace(day=1) if source.grain == "month" else eligible
        floor = source.first.replace(day=1) if source.grain == "month" else source.first
        if anchor < floor:
            continue
        fingerprint = hashlib.sha256(json.dumps(ids).encode()).hexdigest()
        previous = connection.execute("SELECT fingerprint FROM finmind_frontier_universes WHERE dataset=?",
                                      (dataset,)).fetchone()
        if previous != (fingerprint,):
            if source.grain == "history":
                connection.executemany("INSERT OR IGNORE INTO tasks(dataset,data_id,partition,kind,priority,state) "
                                       "VALUES (?,?,'history','id_history',?,'pending')",
                                       [(dataset, identifier, source.priority) for identifier in ids])
            else:
                connection.executemany("INSERT OR IGNORE INTO finmind_source_frontiers VALUES (?,?,?,?)",
                                       [(dataset, identifier, anchor.isoformat(), anchor.isoformat()) for identifier in ids])
            connection.execute("INSERT OR REPLACE INTO finmind_frontier_universes VALUES (?,?)", (dataset, fingerprint))
        if source.grain == "history":
            continue
        active = connection.execute("SELECT COUNT(*) FROM tasks WHERE dataset=? AND state IN ('pending','failed','inflight')",
                                    (dataset,)).fetchone()[0]
        room = max(0, WORKING_SET - active)
        if not room:
            continue
        # New periods take precedence over old periods, but retain the dataset
        # priority so tick never jumps ahead of missing non-tick observations.
        candidates = connection.execute("SELECT data_id,newer_than FROM finmind_source_frontiers "
                                        "WHERE dataset=? AND newer_than<? ORDER BY newer_than,data_id LIMIT ?",
                                        (dataset, anchor.isoformat(), room)).fetchall()
        for identifier, raw in candidates:
            old = date.fromisoformat(raw)
            day = _month_next(old) if source.grain == "month" else old + timedelta(days=1)
            _insert(connection, dataset, identifier, day, source, day_decision=day_decision)
            connection.execute("UPDATE finmind_source_frontiers SET newer_than=? WHERE dataset=? AND data_id=?",
                               (day.isoformat(), dataset, identifier))
        room -= len(candidates)
        if room:
            candidates = connection.execute("SELECT data_id,older_than FROM finmind_source_frontiers "
                                            "WHERE dataset=? AND older_than IS NOT NULL "
                                            "ORDER BY older_than DESC,data_id LIMIT ?", (dataset, room)).fetchall()
            for identifier, raw in candidates:
                day = date.fromisoformat(raw)
                _insert(connection, dataset, identifier, day, source, day_decision=day_decision)
                prior = day - timedelta(days=1)
                if source.grain == "month":
                    prior = prior.replace(day=1)
                floor = source.first.replace(day=1) if source.grain == "month" else history_floor(dataset, identifier)
                connection.execute("UPDATE finmind_source_frontiers SET older_than=? WHERE dataset=? AND data_id=?",
                                   (prior.isoformat() if prior >= floor else None, dataset, identifier))
    connection.commit()
    return frontier_status(connection)


def _insert(connection: sqlite3.Connection, dataset: str, identifier: str, day: date, source: Source,
            *, day_decision=None) -> None:
    state = 'pending'
    if source.grain == 'day' and source.universe in {'stocks', 'brokers'} and day_decision:
        decision = day_decision(day)
        if decision.status == 'closed' and ('receipt-verified' in decision.reason or 'official TWSE' in decision.reason):
            state = 'non_session'
    connection.execute("INSERT OR IGNORE INTO tasks(dataset,data_id,partition,kind,priority,state) "
                       "VALUES (?,?,?,?,?,?)",
                       (dataset, identifier, day.isoformat(), f"id_{source.grain}", source.priority, state))


def frontier_status(connection: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    if not connection.execute("SELECT 1 FROM sqlite_master WHERE name='finmind_source_frontiers'").fetchone():
        return {}
    result = {}
    for dataset, count, older, newest in connection.execute(
        "SELECT dataset,COUNT(*),COUNT(older_than),MIN(newer_than) FROM finmind_source_frontiers GROUP BY dataset"
    ):
        spec = SOURCES.get(dataset)
        if spec is None:
            continue
        if spec.grain == "month":
            expression = "(CAST(substr(older_than,1,4) AS INTEGER)-?)*12+CAST(substr(older_than,6,2) AS INTEGER)-?+1"
            args = (spec.first.year, spec.first.month, dataset)
        else:
            expression = "CAST(julianday(older_than)-julianday(?) AS INTEGER)+1"
            args = (spec.first.isoformat(), dataset)
        remaining = connection.execute(
            f"SELECT COALESCE(SUM({expression}),0) FROM finmind_source_frontiers "
            "WHERE dataset=? AND older_than IS NOT NULL", args,
        ).fetchone()[0]
        if dataset == 'TaiwanStockKBar':
            benchmark = connection.execute("SELECT older_than FROM finmind_source_frontiers WHERE dataset=? AND data_id='TAIEX'",
                                           (dataset,)).fetchone()
            if benchmark and benchmark[0]:
                remaining += (spec.first - history_floor(dataset, 'TAIEX')).days
        result[dataset] = {"known_identifiers": count, "identifiers_with_unseeded_history": older,
                           "unseeded_partition_candidates": remaining,
                           "estimate_basis": "known_ids_times_calendar_periods_before_lifetime_session_filter",
                           "forward_cursor_min": newest, "materialized_working_set_limit": WORKING_SET,
                           "coverage_complete": False, "catalog_contract_version": CONTRACT_VERSION}
    return result


def aggregate_brokers(rows: list[dict]) -> list[dict]:
    """Sum native share quantities and compute price-volume weighted prices.

    FinMind's aggregate endpoint requires BOTH stock and broker IDs (omitted
    in llms-full.txt). The same information is in one stock/day detail response.
    Retain unrounded arithmetic as an explicit local derivation, never relabel
    it as a second provider response. A live 2330/1020 probe verifies the sums
    and the provider's two-decimal displayed average.
    """
    grouped: dict[tuple[str, str, str], dict] = {}
    for row in rows:
        key = tuple(str(row[name]) for name in ('date', 'stock_id', 'securities_trader_id'))
        item = grouped.setdefault(key, {**dict(zip(('date', 'stock_id', 'securities_trader_id'), key)),
                                       'securities_trader': row['securities_trader'],
                                       **{name: Decimal(0) for name in ('buy', 'sell', 'buy_amount', 'sell_amount')}})
        price = Decimal(str(row['price']))
        for side in ('buy', 'sell'):
            quantity = Decimal(str(row[side]))
            if not quantity.is_finite() or quantity < 0 or not price.is_finite() or price < 0:
                raise ValueError('invalid_broker_quantity_or_price')
            item[side] += quantity
            item[side + '_amount'] += quantity * price
    result = []
    for key in sorted(grouped):
        item = grouped[key]
        output = {name: item[name] for name in ('date', 'stock_id', 'securities_trader_id', 'securities_trader')}
        for side in ('buy', 'sell'):
            output[side + '_volume'] = int(item[side]) if item[side] == item[side].to_integral_value() else float(item[side])
            output[side + '_price'] = float(item[side + '_amount'] / item[side]) if item[side] else 0.0
        result.append(output)
    return result

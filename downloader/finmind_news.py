"""Whole-market daily news in the canonical Complement queue.

The API is single-day, not a range endpoint. One no-ID query was verified
against the per-stock result; it also keeps news for delisted/unknown IDs.
1900 is a search floor, NOT a claim that the provider has news since 1900.
No third-party article pages are fetched; source timestamps remain unaltered.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
import json
import sqlite3
from urllib.parse import urlsplit

from downloader.finmind_scheduling import TAIPEI

DATASET = "TaiwanStockNews"
CONTRACT_VERSION = 1
SEARCH_FLOOR = date(1900, 1, 1)
RESEARCH_START = date(2014, 1, 1)
DOCUMENTATION_URL = "https://finmind.github.io/tutor/TaiwanMarket/Others/"
PROBE_EVIDENCE = "artifacts/data_quality/finmind_news_2026-09-29/contract_probe.json"


def seed(connection: sqlite3.Connection, today: date) -> None:
    """Bounded finite calendar inventory; no exchange-day filter for news."""
    connection.execute("CREATE TABLE IF NOT EXISTS finmind_news_plan "
                       "(singleton INTEGER PRIMARY KEY CHECK(singleton=1), through_date TEXT)")
    previous = connection.execute("SELECT through_date FROM finmind_news_plan WHERE singleton=1").fetchone()
    start = date.fromisoformat(previous[0]) + timedelta(days=1) if previous else SEARCH_FLOOR
    def tasks():
        for offset in range(max(0, (today - start).days + 1)):
            day = start + timedelta(days=offset)
            yield DATASET, '', day.isoformat(), 'market_day', 1 if day >= RESEARCH_START else 6, 'pending'
    connection.executemany("INSERT OR IGNORE INTO tasks(dataset,data_id,partition,kind,priority,state) "
                           "VALUES (?,?,?,?,?,?)", tasks())
    connection.execute("INSERT INTO finmind_news_plan VALUES (1,?) ON CONFLICT(singleton) "
                       "DO UPDATE SET through_date=max(through_date,excluded.through_date)", (today.isoformat(),))
    # Hourly rolling overlap for late ingestion, including weekends/holidays.
    recent = (today - timedelta(days=2)).isoformat()
    connection.execute("UPDATE tasks SET priority=1 WHERE dataset=? AND priority=0 AND partition<?", (DATASET, recent))
    connection.execute("UPDATE tasks SET priority=0 WHERE dataset=? AND partition>=? AND partition<=?",
                       (DATASET, recent, today.isoformat()))


def request_contract(partition: str) -> tuple[dict, dict]:
    day = date.fromisoformat(partition)
    return {"dataset": DATASET, "start_date": day.isoformat()}, {
        "news_contract_version": CONTRACT_VERSION, "query_shape": "whole_market_calendar_day",
        "request_start_date": day.isoformat(), "request_end_date": day.isoformat(),
        "request_count": 1, "documentation_url": DOCUMENTATION_URL, "probe_evidence": PROBE_EVIDENCE,
        "search_floor": SEARCH_FLOOR.isoformat(), "provider_earliest_date_verified": False,
        "publication_time_basis": "provider_timestamp_unmodified_timezone_and_precision_unverified",
        "article_body_crawled": False, "historical_point_in_time": False,
    }


def validate_response(partition: str, rows: list[dict]) -> dict:
    expected = date.fromisoformat(partition)
    seen = set()
    duplicates = 0
    for row in rows:
        try:
            stamp = datetime.fromisoformat(str(row['date']))
        except (KeyError, ValueError, TypeError):
            raise ValueError('news_invalid_timestamp') from None
        if stamp.date() != expected:
            raise ValueError('news_response_outside_day')
        if not all(isinstance(row.get(key), str) and row[key].strip() for key in ('stock_id', 'link')):
            raise ValueError('news_missing_identity_or_content')
        if urlsplit(row['link']).scheme not in {'http', 'https'}:
            raise ValueError('news_invalid_link')
        key = json.dumps(row, sort_keys=True, ensure_ascii=False)
        duplicates += int(key in seen)
        seen.add(key)
    # Preserve the response verbatim; an article can legitimately tag many IDs.
    return {'exact_duplicate_rows': duplicates, 'duplicate_policy': 'raw_preserved',
            'grain': 'article_stock_association', 'rows_validated': len(rows),
            'missing_optional_fields': {key: sum(not row.get(key) for row in rows)
                                       for key in ('title', 'source', 'description')}}


def next_refresh(partition: str, now: datetime, *, empty: bool) -> str:
    local = now.astimezone(TAIPEI)
    if date.fromisoformat(partition) >= local.date() - timedelta(days=2):
        return (now + timedelta(hours=1)).isoformat()
    return (now + timedelta(days=90 if empty else 365)).isoformat()

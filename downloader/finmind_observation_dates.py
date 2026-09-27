"""FinMind query dates are observation periods, not publication timestamps.

The official Fundamental examples query quarter ends / month starts for the
whole market. Do not apply these rules to weekly holdings or monthly prices:
their historical dates have exceptions. A nonempty exception disables pruning
for the entire affected series, preserving evidence instead of hiding it.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
import sqlite3
from typing import Any
from zoneinfo import ZoneInfo

TAIPEI = ZoneInfo("Asia/Taipei")
QUARTERLY = frozenset({
    "TaiwanStockFinancialStatements", "TaiwanStockBalanceSheet",
    "TaiwanStockCashFlowsStatement",
})
MONTHLY = frozenset({"TaiwanStockMonthRevenue"})
PERIOD_DATASETS = QUARTERLY | MONTHLY
EXCLUDED_STATE = "not_observation_date"
POLICY_VERSION = 1
SOURCE_URL = "https://finmind.github.io/tutor/TaiwanMarket/Fundamental/"
# The complete local nonempty/empty daily sweep covers 2014 onward. Earlier
# unqueried dates stay necessary work (after period anchors), not presumed empty
# from a modern example or a one-symbol diagnostic.
VERIFIED_PATTERN_START = '2014-01-01'


def is_observation_date(dataset: str, day: date) -> bool:
    if dataset in QUARTERLY:
        return (day.month, day.day) in {(3, 31), (6, 30), (9, 30), (12, 31)}
    return day.day == 1 if dataset in MONTHLY else True


def refresh_interval(dataset: str, day: date, now: datetime) -> timedelta:
    """Polling policy, NOT a legal release deadline or point-in-time proof.

    Revisit recent periods even when their observation date is long past.
    Older observations receive a bounded monthly revision sweep. This allows
    late reports without re-querying every empty calendar day or declaring
    old provider values immutable.
    """
    age = (now.astimezone(TAIPEI).date() - day).days
    active_days = 185 if dataset in QUARTERLY else 62
    return timedelta(hours=4) if age <= active_days else timedelta(days=30)


def next_period_refresh(dataset: str, partition: str, now: datetime) -> datetime:
    return now + refresh_interval(dataset, date.fromisoformat(partition), now)


def reconcile_observation_dates(conn: sqlite3.Connection, now: datetime,
                                datasets: set[str]) -> dict[str, Any]:
    """Reclassify queue only; never erase raw data or prior response receipts."""
    conn.execute(
        "CREATE TABLE IF NOT EXISTS observation_date_audit ("
        "policy_version INTEGER,dataset TEXT,data_id TEXT,partition TEXT,"
        "old_state TEXT,old_next_attempt_at_utc TEXT,receipt_path TEXT,"
        "changed_at_utc TEXT,PRIMARY KEY(policy_version,dataset,data_id,partition))"
    )
    result: dict[str, Any] = {"version": POLICY_VERSION, "source_url": SOURCE_URL,
                              "excluded_is_downloaded": False, "datasets": {}}
    for dataset in sorted(PERIOD_DATASETS & datasets):
        predicate = ("substr(partition,6,5) IN ('03-31','06-30','09-30','12-31')"
                     if dataset in QUARTERLY else "substr(partition,9,2)='01'")
        conflict = conn.execute(
            "SELECT partition FROM tasks WHERE dataset=? AND rows>0 AND "
            f"(NOT ({predicate}) OR first_data_date IS NOT partition OR last_data_date IS NOT partition) LIMIT 1",
            (dataset,),
        ).fetchone()
        if conflict:
            conn.execute(
                "UPDATE tasks SET state=COALESCE((SELECT old_state FROM observation_date_audit a "
                "WHERE a.dataset=tasks.dataset AND a.data_id=tasks.data_id AND a.partition=tasks.partition "
                "AND policy_version=?),'pending'),next_attempt_at_utc=(SELECT old_next_attempt_at_utc "
                "FROM observation_date_audit a WHERE a.dataset=tasks.dataset AND a.data_id=tasks.data_id "
                "AND a.partition=tasks.partition AND policy_version=?) WHERE dataset=? AND state=?",
                (POLICY_VERSION, POLICY_VERSION, dataset, EXCLUDED_STATE),
            )
        else:
            invalid = (f"dataset=? AND NOT ({predicate}) AND rows=0 "
                       "AND state IN ('pending','observed_empty','failed') "
                       f"AND (partition>='{VERIFIED_PATTERN_START}' OR state='observed_empty')")
            conn.execute(
                "INSERT OR IGNORE INTO observation_date_audit "
                "SELECT ?,dataset,data_id,partition,state,next_attempt_at_utc,receipt_path,? "
                f"FROM tasks WHERE {invalid}", (POLICY_VERSION, now.isoformat(), dataset),
            )
            conn.execute(f"UPDATE tasks SET state=?,next_attempt_at_utc=NULL WHERE {invalid}",
                         (EXCLUDED_STATE, dataset))
            conn.execute(
                f"UPDATE tasks SET priority=5 WHERE dataset=? AND NOT ({predicate}) "
                "AND state IN ('pending','failed') AND partition<?",
                (dataset, VERIFIED_PATTERN_START),
            )
        excluded = conn.execute("SELECT count(*) FROM tasks WHERE dataset=? AND state=?",
                                (dataset, EXCLUDED_STATE)).fetchone()[0]
        # At most ~150 quarterly / ~300 monthly anchors, not a daily-table scan
        # into Python. Errors retain their backoff/entitlement state.
        anchors = conn.execute(
            "SELECT partition,last_attempt_at_utc,next_attempt_at_utc,state FROM tasks "
            f"WHERE dataset=? AND ({predicate}) AND state IN ('complete','observed_empty','pending')",
            (dataset,),
        ).fetchall()
        active = 0
        for partition, attempted, next_at, state in anchors:
            interval = refresh_interval(dataset, date.fromisoformat(partition), now)
            # A whole-market fiscal period yields far more than an unverified
            # calendar-day probe, even when it predates the training horizon.
            priority = 0 if interval == timedelta(hours=4) else 1
            active += priority == 0
            # A pending task is already due. Do not invent a prior download.
            if state == 'pending':
                conn.execute("UPDATE tasks SET priority=? WHERE dataset=? AND partition=? AND data_id=''",
                             (priority, dataset, partition))
                continue
            try:
                last = datetime.fromisoformat(attempted)
                if last.tzinfo is None or last > now:
                    raise ValueError("untrusted attempt time")
                due = last + interval
            except (TypeError, ValueError):
                due = now
            # Repair stale NULL (old code stopped after observation date), and
            # recalculate from the last actual request, never from every seed.
            conn.execute("UPDATE tasks SET priority=?,next_attempt_at_utc=? "
                         "WHERE dataset=? AND partition=? AND data_id=''",
                         (priority, due.astimezone(UTC).isoformat(), dataset, partition))
        result["datasets"][dataset] = {
            "state": "nonempty_date_conflict" if conflict else "period_date_contract",
            "conflicting_partition": conflict[0] if conflict else None,
            "excluded_calendar_dates": excluded,
            "unverified_old_dates_remain_required": True,
            "verified_pattern_start": VERIFIED_PATTERN_START,
            "active_release_anchors": active,
            "query_date_grain": "quarter_end" if dataset in QUARTERLY else "month_start",
            "refresh_recent_hours": 4, "revision_sweep_days": 30,
            "query_date_is_publication_time": False,
        }
    return result

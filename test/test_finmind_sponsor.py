from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path
import sqlite3

import pytest

from downloader import download_finmind_sponsor as sponsor
from downloader.download_finmind_complement import SourceError, Task
from downloader.finmind_account import backfill_budget, verified_account


def test_shared_hourly_budget_keeps_incremental_reserve(tmp_path: Path) -> None:
    now = datetime(2026, 9, 26, 4, tzinfo=UTC)
    account = {
        "official_requests_per_hour": 100,
        "provider_used_in_hour": 66,
        "observed_at_utc": (now - timedelta(minutes=1)).isoformat(),
    }
    with sqlite3.connect(tmp_path / "request_traffic.sqlite3") as connection:
        connection.execute("CREATE TABLE requests (started_at_utc TEXT, dataset TEXT)")
        connection.executemany("INSERT INTO requests VALUES (?, 'fixture')", [
            ((now - timedelta(seconds=20)).isoformat(),),
            ((now - timedelta(seconds=10)).isoformat(),),
        ])
    # 66 observed + 2 starts since the observation = 68 used. The named
    # fixed lane needs 25 calls plus two serial worker slots; four in-flight background calls may fit;
    # five may not. Other providers have independent buckets.
    assert backfill_budget(account, tmp_path, fixed_incremental_requests=25,
                           in_flight=4, now=now)["allowed"] is True
    assert backfill_budget(account, tmp_path, fixed_incremental_requests=25,
                           in_flight=5, now=now)["allowed"] is False
    assert backfill_budget({**account, "provider_used_in_hour": None}, tmp_path,
                           fixed_incremental_requests=25, now=now)["allowed"] is False
    reset = backfill_budget(
        {**account, "provider_used_in_hour": 0, "observed_at_utc": now.isoformat()},
        tmp_path, fixed_incremental_requests=25, now=now,
    )
    assert reset["used_estimate"] == 0  # old-hour local calls do not survive an observed reset
    stale = backfill_budget(
        {**account, "observed_at_utc": (now - timedelta(minutes=6)).isoformat()},
        tmp_path, fixed_incremental_requests=25, now=now,
    )
    assert stale["allowed"] is False and stale["basis"] == "provider_usage_stale"


def test_sponsor_reserved_lane_dispatches_only_priority_zero(tmp_path: Path) -> None:
    now = datetime(2026, 9, 26, 4, tzinfo=UTC)
    with sponsor._db(tmp_path / "queue.sqlite3") as connection:
        connection.executemany(
            "INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) "
            "VALUES (?,'','2026-09-25','day',?,'pending')",
            (("Historical", 2), ("Incremental", 0)),
        )
        task = sponsor._next(connection, now, incremental_only=True)
        assert task is not None and task.dataset == "Incremental"
        assert sponsor._next(connection, now, incremental_only=True) is None
        assert sponsor._next(connection, now).dataset == "Historical"


@pytest.mark.parametrize("state", ["pending", "inflight", "failed", "blocked"])
def test_secondary_cannot_jump_sleeping_or_blocked_necessary_work(tmp_path: Path, state: str) -> None:
    now = datetime(2026, 9, 26, 4, tzinfo=UTC)
    with sponsor._db(tmp_path / "queue.sqlite3") as connection:
        connection.execute(
            "INSERT INTO tasks(dataset,data_id,partition,kind,priority,state,next_attempt_at_utc) "
            "VALUES ('Required','','2020-01-01','day',1,?,?)",
            (state, (now + timedelta(hours=1)).isoformat()),
        )
        connection.execute(
            "INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) "
            "VALUES ('Secondary','','2020-01-01','day',8,'pending')"
        )
        assert sponsor._next(connection, now, secondary_admission=lambda: {"allowed": True}) is None
        assert connection.execute("SELECT state FROM tasks WHERE dataset='Secondary'").fetchone()[0] == "pending"


def test_secondary_requires_current_global_admission_and_yields_to_new_work(tmp_path: Path) -> None:
    now = datetime(2026, 9, 26, 4, tzinfo=UTC)
    with sponsor._db(tmp_path / "queue.sqlite3") as connection:
        connection.execute(
            "INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) "
            "VALUES ('Secondary','','2020-01-01','day',8,'pending')"
        )
        assert sponsor._next(connection, now) is None
        assert sponsor._next(connection, now, secondary_admission=lambda: {"allowed": False}) is None
        connection.execute(
            "INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) "
            "VALUES ('Incremental','','2026-09-26','day',0,'pending')"
        )
        assert sponsor._next(connection, now, secondary_admission=lambda: {"allowed": True}).dataset == "Incremental"
        connection.execute("UPDATE tasks SET state='complete' WHERE dataset='Incremental'")
        assert sponsor._next(connection, now, secondary_admission=lambda: {"allowed": True}).dataset == "Secondary"


def test_scheduled_stock_closure_defers_request_without_faking_non_session(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from stockagent.live.market_status import TwStockDayDecision

    monkeypatch.setattr(sponsor, "SOURCES", (
        sponsor._s("TaiwanStockPrice", "2026-09-26", "day", 1, 0),
    ))
    now = datetime(2026, 9, 26, 4, tzinfo=UTC)
    monkeypatch.setattr(
        sponsor, "tw_stock_day_decision",
        lambda *_args, **_kwargs: TwStockDayDecision("closed", "official holiday"),
    )
    with sponsor._db(tmp_path / "queue.sqlite3") as connection:
        policy = sponsor._seed(connection, now)
        state = connection.execute(
            "SELECT state,next_attempt_at_utc FROM tasks WHERE dataset='TaiwanStockPrice'"
        ).fetchone()
        assert policy["today_status"] == "closed"
        assert state[0] == "calendar_wait" and state[1] is not None
        assert policy['calendar_wait'] == 1
        status = sponsor._status(connection, tmp_path, 'current_queue')
        assert status['series']['TaiwanStockPrice']['target'] == 0
        assert status['series']['TaiwanStockPrice']['complete'] == 0
        assert status['acquisition_policy']['required_unfinished'] == 0
        assert sponsor._next(connection, now) is None
        monkeypatch.setattr(
            sponsor, "tw_stock_day_decision",
            lambda *_args, **_kwargs: TwStockDayDecision("scheduled_open", "corrected official schedule"),
        )
        sponsor._seed(connection, now)
        assert sponsor._next(connection, now).dataset == "TaiwanStockPrice"


def test_calendar_hold_does_not_veto_secondary_but_still_requires_global_admission(tmp_path):
    now = datetime(2026, 9, 28, 8, tzinfo=UTC)
    with sponsor._db(tmp_path / 'queue.sqlite3') as conn:
        conn.executemany(
            "INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) VALUES (?,'',?,'day',?,?)",
            [('TaiwanStockMarginPurchaseShortSale', '2026-09-28', 0, 'calendar_wait'),
             ('TaiwanStockPrice', '2010-01-04', 8, 'pending')],
        )
        assert sponsor._next(conn, now, secondary_admission=lambda: {'allowed': False}) is None
        assert sponsor._next(conn, now, secondary_admission=lambda: {'allowed': True}).dataset == 'TaiwanStockPrice'


def test_old_calendar_hold_reopens_when_evidence_is_lost(tmp_path, monkeypatch):
    from stockagent.live.market_status import TwStockDayDecision
    monkeypatch.setattr(sponsor, 'SOURCES', ())
    monkeypatch.setattr(sponsor, 'tw_stock_day_decision',
                        lambda *_a, **_kw: TwStockDayDecision('unknown', 'missing calendar'))
    with sponsor._db(tmp_path / 'queue.sqlite3') as conn:
        conn.execute("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) "
                     "VALUES ('TaiwanStockPrice','','2026-09-01','day',0,'calendar_wait')")
        sponsor._seed(conn, datetime(2026, 9, 28, 8, tzinfo=UTC))
        assert conn.execute('SELECT state FROM tasks').fetchone() == ('pending',)


def test_current_hour_reserve_skips_closed_stock_release_but_not_issuer_event(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from stockagent.live.market_status import TwStockDayDecision

    monkeypatch.setattr(sponsor, "SOURCES", (
        sponsor._s("TaiwanStockPrice", "2026-09-01", "day", 1, 12, 30),
        sponsor._s("TaiwanStockDividend", "2026-09-01", "day", 1, 12, 30),
    ))
    monkeypatch.setattr(
        sponsor, "tw_stock_day_decision",
        lambda *_args, **_kwargs: TwStockDayDecision("closed", "official holiday"),
    )
    # No reservation in the preceding hour. The uninitialized calendar needs
    # one real check, NOT four permanently protected Free calls.
    now = datetime(2026, 9, 26, 3, 30, tzinfo=UTC)
    assert sponsor._fixed_incremental_demand(tmp_path, now) == 1
    # Same release hour: issuer events can publish on a stock holiday.
    assert sponsor._fixed_incremental_demand(tmp_path, now + timedelta(hours=1) - timedelta(minutes=15)) == 2


def test_background_opening_gate_uses_session_evidence_not_just_weekday() -> None:
    from downloader.finmind_scheduling import protected_stock_opening
    from stockagent.live.market_status import TwStockDayDecision

    friday = datetime(2026, 9, 25, 0, 30, tzinfo=UTC)
    saturday = friday + timedelta(days=1)
    closed = lambda *_a, **_kw: TwStockDayDecision("closed", "verified holiday")
    opened = lambda *_a, **_kw: TwStockDayDecision("scheduled_open", "verified special session")
    unknown = lambda *_a, **_kw: TwStockDayDecision("unknown", "missing calendar")
    assert not protected_stock_opening(friday, day_decision=closed)
    assert protected_stock_opening(saturday, day_decision=opened)
    assert protected_stock_opening(friday, day_decision=unknown)
    assert not protected_stock_opening(friday + timedelta(hours=2), day_decision=opened)


def test_sponsor_status_tracks_last_checked_partition_even_when_empty(tmp_path: Path) -> None:
    with sponsor._db(tmp_path / "queue.sqlite3") as connection:
        connection.executemany(
            "INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) "
            "VALUES ('TaiwanStockPrice','',?,'day',1,?)",
            (("2026-09-23", "complete"), ("2026-09-24", "observed_empty")),
        )
        status = sponsor._status(connection, tmp_path, "batch_complete")
    assert status["series"]["TaiwanStockPrice"]["last_checked_partition"] == "2026-09-24"


def test_current_empty_partition_retries_same_day_not_tomorrow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 9, 30, 10, tzinfo=UTC)  # Weekday, after the 17:30 release.
    task = Task("TaiwanStockPrice", "", "2026-09-30", "day", 0, "inflight")
    monkeypatch.setattr(sponsor, "_store", lambda *_args: {
        "status": "observed_empty", "rows": 0, "receipt_path": "receipts/fixture.json",
        "source_first_date": None, "source_last_date": None,
    })
    with sponsor._db(tmp_path / "queue.sqlite3") as connection:
        connection.execute(
            "INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) "
            "VALUES ('TaiwanStockPrice','','2026-09-30','day',0,'inflight')"
        )
        sponsor._finish(connection, tmp_path, task, [], now)
        next_attempt = connection.execute(
            "SELECT next_attempt_at_utc FROM tasks WHERE dataset='TaiwanStockPrice'"
        ).fetchone()[0]
    assert datetime.fromisoformat(next_attempt) - now == timedelta(minutes=15)


def test_sponsor_catalog_is_explicit_about_expensive_unavailable_shapes() -> None:
    assert len(sponsor.SOURCES) >= 50
    assert "TaiwanStockNews" not in sponsor.SPECS
    assert "TaiwanStockPriceTick" not in sponsor.SPECS
    assert "TaiwanStockKBar" not in sponsor.UNSCHEDULED
    assert not sponsor.UNSCHEDULED
    assert sponsor.SPECS["TaiwanStockPrice"].grain == "day"


def test_partition_end_is_exclusive_and_seed_is_idempotent(tmp_path: Path) -> None:
    assert sponsor._end(date(2026, 9, 22), "two_day") == date(2026, 9, 24)
    assert sponsor._end(date(2026, 12, 2), "month") == date(2027, 1, 1)
    with sponsor._db(tmp_path / "queue.sqlite3") as connection:
        now = datetime(2026, 9, 26, 9, tzinfo=UTC)
        sponsor._seed(connection, now)
        count = connection.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
        sponsor._seed(connection, now)
        assert connection.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == count
        assert count > 50_000
        assert connection.execute("SELECT COUNT(*) FROM tasks WHERE dataset='TaiwanStockNews'").fetchone()[0] == 0


def test_verified_sessions_skip_only_session_facts_and_reopen_revised_dates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sponsor, "SOURCES", (
        sponsor._s("TaiwanStockPrice", "2026-09-23", "day", 1),
        sponsor._s("TaiwanStockDividend", "2026-09-23", "day", 1),
    ))
    now = datetime(2026, 9, 28, 9, tzinfo=UTC)
    calendar = sponsor.OfficialSessions(
        date(2026, 9, 23), date(2026, 9, 27),
        frozenset({date(2026, 9, 23), date(2026, 9, 25), date(2026, 9, 26)}),
        "verified-sha",
    )
    with sponsor._db(tmp_path / "queue.sqlite3") as connection:
        first = sponsor._seed(connection, now, official_sessions=calendar)
        assert first["state"] == "receipt_verified"
        assert first["excluded_non_session"] == 2
        assert first["newly_excluded"] == 2
        assert connection.execute(
            "SELECT state FROM tasks WHERE dataset='TaiwanStockPrice' AND partition='2026-09-24'"
        ).fetchone() == ("non_session",)
        assert connection.execute(
            "SELECT state FROM tasks WHERE dataset='TaiwanStockPrice' AND partition='2026-09-26'"
        ).fetchone() == ("pending",)  # Saturday is valid when the official calendar says so.
        assert connection.execute(
            "SELECT state FROM tasks WHERE dataset='TaiwanStockDividend' AND partition='2026-09-24'"
        ).fetchone() == ("pending",)  # Issuer event dates are not exchange sessions.
        status = sponsor._status(connection, tmp_path, "running", session_policy=first)
        assert status["series"]["TaiwanStockPrice"]["target"] == 3
        assert status["series"]["TaiwanStockPrice"]["calendar_wait"] == 1
        assert status["series"]["TaiwanStockPrice"]["non_session"] == 2
        assert sponsor._seed(connection, now, official_sessions=calendar)["newly_excluded"] == 0

        revised = sponsor.OfficialSessions(
            calendar.first, calendar.last, calendar.days | {date(2026, 9, 24)},
            "revised-sha",
        )
        second = sponsor._seed(connection, now, official_sessions=revised)
        assert second["excluded_non_session"] == 1
        assert connection.execute(
            "SELECT state FROM tasks WHERE dataset='TaiwanStockPrice' AND partition='2026-09-24'"
        ).fetchone() == ("pending",)
        assert sponsor._seed(connection, now)["state"] == "calendar_unverified"
        assert connection.execute(
            "SELECT COUNT(*) FROM tasks WHERE state='non_session'"
        ).fetchone()[0] == 0


def test_non_session_conflict_preserves_nonempty_source_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sponsor, "SOURCES", (
        sponsor._s("TaiwanStockPrice", "2026-09-23", "day", 1),
    ))
    now = datetime(2026, 9, 28, 9, tzinfo=UTC)
    calendar = sponsor.OfficialSessions(
        date(2026, 9, 23), date(2026, 9, 27),
        frozenset({date(2026, 9, 23), date(2026, 9, 25)}), "verified-sha",
    )
    with sponsor._db(tmp_path / "queue.sqlite3") as connection:
        sponsor._seed(connection, now)
        connection.execute(
            "UPDATE tasks SET state='complete',rows=3 WHERE dataset='TaiwanStockPrice' "
            "AND partition='2026-09-24'"
        )
        policy = sponsor._seed(connection, now, official_sessions=calendar)
        assert policy["conflicting_datasets"] == ["TaiwanStockPrice"]
        assert policy["excluded_non_session"] == 0
        assert connection.execute(
            "SELECT state,rows FROM tasks WHERE dataset='TaiwanStockPrice' AND partition='2026-09-24'"
        ).fetchone() == ("complete", 3)
        assert connection.execute(
            "SELECT state FROM tasks WHERE dataset='TaiwanStockPrice' AND partition='2026-09-26'"
        ).fetchone() == ("pending",)


def test_official_price_receipts_demote_only_proven_overlap_and_migrate_queue(tmp_path: Path) -> None:
    root = tmp_path / "official"
    (root / "state").mkdir(parents=True)
    catalog = tmp_path / "packed.json"
    catalog.write_text(json.dumps({"datasets": [{"dataset": "tw-public", "source": str(root)}]}))
    for dataset, start in (("twse_daily_ohlcv", "2004-02-11"),
                           ("tpex_daily_ohlcv", "2003-08-01")):
        (root / "state" / f"{dataset}.json").write_text(json.dumps({
            "dataset": dataset, "baseline_established": True,
            "coverage_complete": True, "coverage_calendar_kind": "receipt_verified_official_open_sessions",
            "missing_dates_after": 0, "failed_dates": {},
            "coverage_start": start, "coverage_end": "2026-09-24",
        }))
    coverage = sponsor._official_price_coverage(catalog)
    assert coverage == (date(2004, 2, 11), date(2026, 9, 24))
    now = datetime(2026, 9, 26, 9, tzinfo=UTC)
    with sponsor._db(tmp_path / "queue.sqlite3") as connection:
        sponsor._seed(connection, now)
        old = connection.execute("SELECT partition FROM tasks WHERE dataset='TaiwanStockPrice' "
                                 "AND partition>='2014-01-01' ORDER BY partition LIMIT 1").fetchone()[0]
        connection.execute("UPDATE tasks SET state='complete',priority=1 WHERE dataset='TaiwanStockPrice' "
                           "AND partition=?", (old,))
        sponsor._seed(connection, now, official_price_coverage=coverage)
        assert connection.execute("SELECT priority,state FROM tasks WHERE dataset='TaiwanStockPrice' "
                                  "AND partition=?", (old,)).fetchone() == (8, "complete")
        assert connection.execute("SELECT MAX(priority) FROM tasks WHERE dataset='TaiwanStockPrice' "
                                  "AND partition<'2004-02-11'").fetchone()[0] < 8
        assert connection.execute("SELECT priority FROM tasks WHERE dataset='TaiwanStockPriceAdj' "
                                  "AND partition=?", (old,)).fetchone()[0] != 8
    (root / "state" / "tpex_daily_ohlcv.json").write_text(json.dumps({
        "dataset": "tpex_daily_ohlcv", "coverage_complete": False,
    }))
    assert sponsor._official_price_coverage(catalog) is None


def test_fetch_uses_no_id_half_open_range_and_checks_provider_rows(monkeypatch: pytest.MonkeyPatch,
                                                                     tmp_path: Path) -> None:
    seen = []

    def fake_fetch(_session, _limiter, _root, _dataset, _token, params):
        seen.append(params)
        return [{"date": "2026-09-22", "stock_id": "2330"}]

    monkeypatch.setattr(sponsor, "_fetch_rows", fake_fetch)
    task = Task("TaiwanStockPrice", "", "2026-09-22", "day", 0, "pending")
    rows = sponsor._fetch(task, "secret", object(), tmp_path, date(2026, 9, 26))
    assert len(rows) == 1
    assert seen[0] == {"dataset": "TaiwanStockPrice", "start_date": "2026-09-22"}
    day = Task("TaiwanStockIndustryChainMoneyFlow", "", "2026-09-22", "day", 0, "pending")
    sponsor._fetch(day, "secret", object(), tmp_path, date(2026, 9, 26))
    assert "end_date" not in seen[1]
    for dataset in ("TaiwanStockEvery5SecondsIndex", "TaiwanStockGovernmentBankBuySell",
                    "TaiwanStockBlockTradingDailyReport"):
        sponsor._fetch(Task(dataset, "", "2026-09-22", "day", 0, "pending"),
                       "secret", object(), tmp_path, date(2026, 9, 26))
        assert seen[-1] == {"dataset": dataset, "start_date": "2026-09-22"}
    monkeypatch.setattr(sponsor, "_fetch_rows", lambda *_args, **_kwargs: [{"date": "2026-09-23"}])
    with pytest.raises(SourceError, match="response_outside_partition"):
        sponsor._fetch(task, "secret", object(), tmp_path, date(2026, 9, 26))


def test_sponsor_year_receipt_uses_date_partition_without_free_year_assertion(tmp_path: Path) -> None:
    task = Task("TaiwanBusinessIndicator", "", "2026-01-01", "year", 0, "pending")
    with sponsor._db(tmp_path / "queue.sqlite3") as connection:
        connection.execute(
            "INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) "
            "VALUES (?,?,?,?,?,'inflight')", (task.dataset, "", task.partition, task.kind, 0)
        )
        receipt = sponsor._finish(connection, tmp_path, task, [{"date": "2026-09-01", "leading": 1.0}],
                                  datetime(2026, 9, 26, tzinfo=UTC))
        assert receipt["status"] == "complete"
        assert connection.execute("SELECT rows,state FROM tasks").fetchone() == (1, "complete")


@pytest.mark.parametrize("dataset,kind,start,end", [
    ("TaiwanBusinessIndicator", "year", "2025-01-01", "2025-12-31"),
    ("CnnFearGreedIndex", "year", "2025-01-01", "2025-12-31"),
    ("TaiwanStockInfoWithWarrantSummary", "month", "2026-08-01", "2026-08-31"),
    ("TaiwanOptionVix", "month", "2026-08-01", "2026-08-31"),
])
def test_probed_inclusive_ranges_keep_strict_local_partition(monkeypatch, tmp_path, dataset, kind, start, end):
    seen = []
    monkeypatch.setattr(sponsor, "_fetch_rows", lambda *_a: seen.append(_a[-1]) or [{"date": end}])
    task = Task(dataset, "", start, kind, 1, "pending")
    assert sponsor._fetch(task, "unused", object(), tmp_path, date(2026, 9, 27)) == [{"date": end}]
    assert seen[0]["end_date"] == end
    monkeypatch.setattr(sponsor, "_fetch_rows", lambda *_a: [{"date": str(date.fromisoformat(end) + timedelta(days=1))}])
    with pytest.raises(SourceError, match="response_outside_partition"):
        sponsor._fetch(task, "unused", object(), tmp_path, date(2026, 9, 27))


def test_inclusive_shape_repair_is_narrow_audited_and_one_time(tmp_path: Path) -> None:
    with sponsor._db(tmp_path / "queue.sqlite3") as conn:
        conn.execute("PRAGMA user_version=4")
        for dataset, error in [("TaiwanBusinessIndicator", "response_outside_partition"),
                               ("TaiwanOptionVix", "not_entitled"),
                               ("Unverified", "response_outside_partition")]:
            conn.execute("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state,error_code) "
                         "VALUES (?,'','2025-01-01','year',1,'blocked',?)", (dataset, error))
        sponsor._migrate_query_shape(conn, tmp_path)
        assert conn.execute("SELECT state FROM tasks WHERE dataset='TaiwanBusinessIndicator'").fetchone()[0] == "pending"
        assert conn.execute("SELECT count(*) FROM tasks WHERE state='blocked'").fetchone()[0] == 2
        assert conn.execute("SELECT old_error_code FROM query_shape_repair_audit").fetchone()[0] == "response_outside_partition"
        conn.execute("UPDATE tasks SET state='blocked',error_code='response_outside_partition' WHERE dataset='TaiwanBusinessIndicator'")
        sponsor._migrate_query_shape(conn, tmp_path)
        assert conn.execute("SELECT state FROM tasks WHERE dataset='TaiwanBusinessIndicator'").fetchone()[0] == "blocked"


def test_query_shape_migration_reopens_only_old_400_once(tmp_path: Path) -> None:
    now = datetime(2026, 9, 26, 9, tzinfo=UTC)
    with sponsor._db(tmp_path / "queue.sqlite3") as connection:
        connection.execute(
            "INSERT INTO tasks(dataset,data_id,partition,kind,priority,state,error_code) "
            "VALUES ('TaiwanStockEvery5SecondsIndex','','2025-05-09','day',4,'blocked',"
            "'provider_bad_request')"
        )
        sponsor._seed(connection, now)
        assert connection.execute("SELECT state,error_code FROM tasks WHERE dataset="
                                  "'TaiwanStockEvery5SecondsIndex' AND partition='2025-05-09'").fetchone() == (
                                      "pending", None)
        connection.execute("UPDATE tasks SET state='blocked',error_code='provider_bad_request' "
                           "WHERE dataset='TaiwanStockEvery5SecondsIndex' AND partition='2025-05-09'")
        sponsor._seed(connection, now)
        assert connection.execute("SELECT state FROM tasks WHERE dataset="
                                  "'TaiwanStockEvery5SecondsIndex' AND partition='2025-05-09'").fetchone() == (
                                      "blocked",)


def test_old_month_receipt_is_preserved_but_not_claimed_as_full_month(tmp_path: Path) -> None:
    now = datetime(2026, 9, 26, 9, tzinfo=UTC)
    dataset = "TaiwanStockMonthRevenue"
    with sponsor._db(tmp_path / "queue.sqlite3") as connection:
        connection.execute(
            "INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) "
            "VALUES (?,'','2026-09-01','month',1,'inflight')", (dataset,)
        )
        old_task = Task(dataset, "", "2026-09-01", "month", 1, "inflight")
        sponsor._finish(connection, tmp_path, old_task, [
            {"date": "2026-09-01", "stock_id": "2330", "revenue": 10},
            {"date": "2026-09-02", "stock_id": "2330", "revenue": 11},
        ], now)
        original = (tmp_path / "receipts" / dataset / "all" / "2026-09-01.json").read_bytes()
        sponsor._seed(connection, now, root=tmp_path)
        assert (tmp_path / "legacy_query_shape_receipts" / "receipts" / dataset /
                "all" / "2026-09-01.json").read_bytes() == original
        assert connection.execute("SELECT kind,state FROM tasks WHERE dataset=? AND "
                                  "partition='2026-09-01'", (dataset,)).fetchone() == (
                                      "day", "pending")
        assert connection.execute("SELECT state FROM tasks WHERE dataset=? AND "
                                  "partition='2026-09-02'", (dataset,)).fetchone() == ("pending",)


def test_sponsor_wide_is_derived_only_after_verified_long_receipt(tmp_path: Path,
                                                                  monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 9, 26, 9, tzinfo=UTC)
    long = "TaiwanStockInstitutionalInvestorsBuySell"
    wide = "TaiwanStockInstitutionalInvestorsBuySellWide"
    with sponsor._db(tmp_path / "queue.sqlite3") as connection:
        connection.executemany(
            "INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) "
            "VALUES (?,'','2026-09-01',?,0,'pending')",
            ((long, "month"), (wide, "derived")),
        )
        task = sponsor._next(connection, now)
        assert task is not None and task.dataset == long
        sponsor._finish(connection, tmp_path, task, [
            {"date": "2026-09-01", "stock_id": "2330", "name": "Foreign_Investor",
             "buy": 10, "sell": 3},
            {"date": "2026-09-01", "stock_id": "2330", "name": "Foreign_Investor",
             "buy": 2, "sell": 1},
        ], now)
        derived = sponsor._next(connection, now)
        assert derived is not None and derived.dataset == wide
        monkeypatch.setattr(sponsor, "_fetch_rows", lambda *_args, **_kwargs: pytest.fail("network called"))
        rows = sponsor._fetch(derived, "unused", object(), tmp_path, now.date())
        assert rows[0]["Foreign_Investor_buy"] == 12
        assert rows[0]["Foreign_Investor_sell"] == 4
        receipt = sponsor._finish(connection, tmp_path, derived, rows, now)
        assert receipt["derived_from"] == long
        assert receipt["coverage_claim"] == "derived_from_observed_long_response_not_provider_completeness"


def test_same_priority_datasets_round_robin_without_crossing_priority(tmp_path: Path) -> None:
    now = datetime(2026, 9, 26, 9, tzinfo=UTC)
    with sponsor._db(tmp_path / "queue.sqlite3") as connection:
        connection.executemany(
            "INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) "
            "VALUES (?,'',?,'day',?,'pending')",
            (("Alpha", "2026-09-01", 1), ("Alpha", "2026-09-02", 1),
             ("Beta", "2026-09-01", 1), ("Beta", "2026-09-02", 1),
             ("Lower", "2026-09-01", 2)),
        )
        picked = [sponsor._next(connection, now) for _ in range(5)]
        assert [task.dataset for task in picked if task] == [
            "Alpha", "Beta", "Alpha", "Beta", "Lower",
        ]
        assert sponsor._next(connection, now) is None


def test_account_gate_uses_actual_tier_and_never_serializes_token(tmp_path: Path,
                                                                   monkeypatch: pytest.MonkeyPatch) -> None:
    from downloader import finmind_account
    monkeypatch.setattr(finmind_account, "_CACHE", None)

    class Response:
        def raise_for_status(self):
            pass

        def json(self):
            return {"status": 200, "level_title": "Sponsor", "api_request_limit_hour": 6000,
                    "user_count": 7, "email": "private@example.test"}

    class Session:
        def get(self, _url, *, headers, timeout):
            assert headers["Authorization"] == "Bearer secret"
            return Response()

    account = verified_account(Session(), "secret", tmp_path)
    assert account["official_requests_per_hour"] == 6000
    stored = (tmp_path / "account_status.json").read_text()
    assert "secret" not in stored and "private@example.test" not in stored

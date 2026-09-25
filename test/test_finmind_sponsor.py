from __future__ import annotations

from datetime import UTC, date, datetime
import json
from pathlib import Path

import pytest

from downloader import download_finmind_sponsor as sponsor
from downloader.download_finmind_complement import SourceError, Task
from downloader.finmind_account import verified_account


def test_sponsor_catalog_is_explicit_about_expensive_unavailable_shapes() -> None:
    assert len(sponsor.SOURCES) >= 50
    assert "TaiwanStockNews" not in sponsor.SPECS
    assert "TaiwanStockPriceTick" not in sponsor.SPECS
    assert sponsor.UNSCHEDULED["TaiwanStockKBar"].endswith("sponsorpro_bulk")
    assert sponsor.SPECS["TaiwanStockPrice"].grain == "two_day"


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
    task = Task("TaiwanStockPrice", "", "2026-09-22", "two_day", 0, "pending")
    rows = sponsor._fetch(task, "secret", object(), tmp_path, date(2026, 9, 26))
    assert len(rows) == 1
    assert seen[0] == {"dataset": "TaiwanStockPrice", "start_date": "2026-09-22",
                       "end_date": "2026-09-24"}
    day = Task("TaiwanStockIndustryChainMoneyFlow", "", "2026-09-22", "day", 0, "pending")
    sponsor._fetch(day, "secret", object(), tmp_path, date(2026, 9, 26))
    assert "end_date" not in seen[1]
    for dataset in ("TaiwanStockEvery5SecondsIndex", "TaiwanStockGovernmentBankBuySell",
                    "TaiwanStockBlockTradingDailyReport"):
        sponsor._fetch(Task(dataset, "", "2026-09-22", "day", 0, "pending"),
                       "secret", object(), tmp_path, date(2026, 9, 26))
        assert seen[-1] == {"dataset": dataset, "start_date": "2026-09-22"}
    monkeypatch.setattr(sponsor, "_fetch_rows", lambda *_args, **_kwargs: [{"date": "2026-09-24"}])
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

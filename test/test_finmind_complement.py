from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader import download_finmind_complement as complement


def test_free_catalog_is_exactly_48_additions_without_news_or_paid() -> None:
    assert len(complement.ALL_DATASETS) == 48
    assert len(set(complement.ALL_DATASETS)) == 48
    assert "TaiwanStockNews" not in complement.ALL_DATASETS
    assert "TaiwanStockPriceTick" not in complement.ALL_DATASETS
    assert "TaiwanStockKBar" not in complement.ALL_DATASETS


def test_live_sponsor_delegates_duplicate_finmind_tasks_and_stale_state_falls_back(tmp_path: Path) -> None:
    now = datetime(2026, 9, 26, tzinfo=UTC)
    root = tmp_path / "data_finmind" / "complement"
    sponsor = root.parent / "sponsor"
    sponsor.mkdir(parents=True)
    (sponsor / "status.json").write_text(json.dumps({
        "tier": "Sponsor", "state": "running", "observed_at_utc": now.isoformat(),
        "series": {"TaiwanStockPrice": {"target": 100, "blocked": 0}},
    }))
    with complement._db(root / "queue.sqlite3") as connection:
        complement._add_tasks(connection, [
            ("TaiwanStockPrice", "2330", "history", "id_history", 1),
            ("TaiwanExchangeRate", "USD", "history", "id_history", 2),
        ])
        delegated = complement._sponsor_delegated(root, now)
        assert delegated == frozenset({"TaiwanStockPrice"})
        assert complement._next_task(connection, now, delegated=delegated).dataset == "TaiwanExchangeRate"
        assert not complement._sponsor_delegated(root, now + timedelta(minutes=16))
        assert complement._next_task(connection, now + timedelta(minutes=16)).dataset == "TaiwanStockPrice"


def test_queue_has_all_reference_and_historical_ranges(tmp_path: Path) -> None:
    with complement._db(tmp_path / "queue.sqlite3") as connection:
        complement._populate(connection, tmp_path, today=date(2026, 9, 25))
        count = connection.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
        assert count == (8 + sum(2026 - first + 1 for first in complement.GLOBAL_START_YEAR.values())
                         + sum(len(ids) for ids in complement.FIXED_ID_HISTORY.values())
                         + len(complement.CURRENCIES))
        assert connection.execute("SELECT COUNT(*) FROM tasks WHERE dataset='TaiwanStockNews'").fetchone()[0] == 0
        assert connection.execute("SELECT priority FROM tasks WHERE dataset='GoldPrice' AND partition='1900'").fetchone()[0] == 3
        assert connection.execute("SELECT priority FROM tasks WHERE dataset='GoldPrice' AND partition='2014'").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM tasks WHERE dataset='TaiwanExchangeRate' AND data_id=''").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM tasks WHERE dataset='TaiwanExchangeRate'").fetchone()[0] == 19
        assert complement._next_task(connection, datetime(2026, 9, 25, tzinfo=UTC)) is not None


def test_master_snapshot_expands_free_per_symbol_jobs(tmp_path: Path) -> None:
    snapshot = complement.Task("TaiwanStockInfo", "", "latest", "snapshot", 0, "pending")
    complement._store(tmp_path, snapshot, [
        {"date": "2026-09-25", "stock_id": "2330", "type": "twse"},
        {"date": "2026-09-25", "stock_id": "2317", "type": "twse"},
    ], datetime(2026, 9, 25, tzinfo=UTC))
    with complement._db(tmp_path / "queue.sqlite3") as connection:
        complement._populate(connection, tmp_path, today=date(2026, 9, 25))
        assert connection.execute(
            "SELECT COUNT(*) FROM tasks WHERE dataset='TaiwanStockPrice'"
        ).fetchone()[0] == 2
        assert connection.execute(
            "SELECT COUNT(*) FROM tasks WHERE dataset='TaiwanStockMonthRevenue'"
        ).fetchone()[0] == 2
        assert connection.execute(
            "SELECT COUNT(*) FROM tasks WHERE dataset='TaiwanStockCapitalReductionReferencePrice' "
            "AND kind='id_history'"
        ).fetchone()[0] == 2
        assert connection.execute(
            "SELECT COUNT(*) FROM tasks WHERE dataset=? AND kind='derived'",
            (complement.WIDE_INSTITUTIONAL,),
        ).fetchone()[0] == 2


def test_official_delisted_and_later_listings_expand_existing_jobs(tmp_path: Path) -> None:
    root = tmp_path / "data_finmind" / "complement"
    public = tmp_path / "data_tw_public"
    public.mkdir()
    pq.write_table(pa.Table.from_pylist([{"symbol": "1204"}]), public / "twse_delisted_company.parquet")
    snapshot = complement.Task("TaiwanStockInfo", "", "latest", "snapshot", 0, "pending")
    now = datetime(2026, 9, 25, tzinfo=UTC)
    complement._store(root, snapshot, [{"date": "2026-09-25", "stock_id": "2330"}], now)
    with complement._db(root / "queue.sqlite3") as connection:
        complement._populate(connection, root, today=date(2026, 9, 25))
        assert connection.execute("SELECT COUNT(*) FROM tasks WHERE dataset='TaiwanStockPrice'").fetchone()[0] == 2
        complement._store(root, snapshot, [
            {"date": "2026-09-26", "stock_id": "2330"},
            {"date": "2026-09-26", "stock_id": "2317"},
        ], now)
        complement._populate(connection, root, today=date(2026, 9, 26))
        assert connection.execute("SELECT COUNT(*) FROM tasks WHERE dataset='TaiwanStockPrice'").fetchone()[0] == 3


def test_entitlement_blocks_per_id_dataset_without_retries(tmp_path: Path) -> None:
    now = datetime(2026, 9, 25, tzinfo=UTC)
    with complement._db(tmp_path / "queue.sqlite3") as connection:
        complement._populate(connection, tmp_path, today=date(2026, 9, 25))
        task = complement.Task("TaiwanExchangeRate", "USD", "history", "id_history", 1, "pending")
        complement._save_failure(connection, task, complement.SourceError("not_entitled", retry_after=86400), now)
        assert connection.execute(
            "SELECT COUNT(*) FROM tasks WHERE dataset=? AND state='not_entitled'",
            (task.dataset,),
        ).fetchone()[0] == 19
        due = connection.execute(
            "SELECT COUNT(*) FROM tasks WHERE dataset=? AND next_attempt_at_utc<?",
            (task.dataset, (now + timedelta(days=2)).isoformat()),
        ).fetchone()[0]
        assert due == 0


def test_current_snapshot_refreshes_at_next_taipei_14_and_current_year_in_four_hours() -> None:
    observed = datetime(2026, 9, 25, 15, 0, tzinfo=UTC)
    snapshot = complement.Task("TaiwanStockInfo", "", "latest", "snapshot", 0, "complete")
    current = complement.Task("GoldPrice", "", "2026", "year", 0, "complete")
    assert complement._next_refresh(snapshot, observed, empty=False) == "2026-09-26T06:00:00+00:00"
    assert complement._next_refresh(current, observed, empty=False) == "2026-09-25T19:00:00+00:00"
    margin = complement.Task("TaiwanStockTotalMarginPurchaseShortSale", "", "2026", "year", 0, "complete")
    assert complement._next_refresh(margin, observed, empty=False) == "2026-09-28T13:10:00+00:00"


def test_receipt_keeps_raw_dates_and_rejects_wrong_identity(tmp_path: Path) -> None:
    now = datetime(2026, 9, 25, 8, tzinfo=UTC)
    task = complement.Task("TaiwanStockPrice", "2330", "history", "id_history", 2, "pending")
    receipt = complement._store(tmp_path, task, [
        {"date": "2000-01-03", "stock_id": "2330", "close": 10},
        {"date": "2026-09-24", "stock_id": "2330", "close": 200},
    ], now)
    assert receipt["status"] == "complete"
    assert receipt["rows"] == 2
    assert receipt["source_first_date"] == "2000-01-03"
    assert receipt["source_last_date"] == "2026-09-24"
    assert (tmp_path / receipt["parquet_path"]).is_file()
    assert json.loads((tmp_path / receipt["receipt_path"]).read_text())["historical_point_in_time"] is False
    with pytest.raises(complement.SourceError, match="wrong_data_id"):
        complement._store(tmp_path, task, [{"date": "2020-01-01", "stock_id": "2317"}], now)
    year = complement.Task("GoldPrice", "", "2026", "year", 0, "pending")
    with pytest.raises(complement.SourceError, match="invalid_year_rows"):
        complement._store(tmp_path, year, [{"date": "2025-12-31", "Price": 1}], now)


def test_status_keeps_unknown_universes_unknown(tmp_path: Path) -> None:
    with complement._db(tmp_path / "queue.sqlite3") as connection:
        complement._populate(connection, tmp_path, today=date(2026, 9, 25))
        status = complement._status(connection, tmp_path, state="running")
    assert status["series"]["TaiwanStockPrice"]["target"] == 0
    assert status["series"]["GoldPrice"]["target"] == 127
    assert status["news"] == "disabled_by_user"
    assert "token" not in json.dumps(status)


def test_migrate_denied_marketwide_years_to_free_per_id_without_deleting_receipts(tmp_path: Path) -> None:
    root = tmp_path / "data_finmind" / "complement"
    with complement._db(root / "queue.sqlite3") as connection:
        connection.execute(
            "INSERT INTO tasks(dataset,data_id,partition,kind,priority,state,error_code) "
            "VALUES ('TaiwanExchangeRate','','2026','year',0,'not_entitled','not_entitled')"
        )
        connection.commit()
        complement._populate(connection, root, today=date(2026, 9, 25))
        assert connection.execute(
            "SELECT state FROM tasks WHERE dataset='TaiwanExchangeRate' AND data_id=''"
        ).fetchone()[0] == "deprecated_query_shape"
        assert connection.execute(
            "SELECT COUNT(*) FROM tasks WHERE dataset='TaiwanExchangeRate' AND state='pending'"
        ).fetchone()[0] == len(complement.CURRENCIES)
        status = complement._status(connection, root, state="running")
        assert status["series"]["TaiwanExchangeRate"]["target"] == len(complement.CURRENCIES)
        assert status["series"]["TaiwanExchangeRate"]["not_entitled"] == 0


def test_one_bulk_response_fans_out_and_checks_old_nonempty_year(tmp_path: Path) -> None:
    now = datetime(2002, 9, 25, tzinfo=UTC)
    with complement._db(tmp_path / "queue.sqlite3") as connection:
        complement._populate(connection, tmp_path, today=date(2002, 9, 25))
        task = complement.Task("TaiwanStockDelisting", "", "2002", "year", 0, "pending")
        assert complement._bulk_needed(connection, task, date(2002, 9, 25))
        complement._store_bulk_years(connection, tmp_path, task, [
            {"date": "2001-01-08", "stock_id": "1234"},
            {"date": "2002-09-20", "stock_id": "5678"},
        ], now, date(2002, 9, 25))
        assert connection.execute(
            "SELECT SUM(rows) FROM tasks WHERE dataset='TaiwanStockDelisting'"
        ).fetchone()[0] == 2
        assert not complement._bulk_needed(connection, task, date(2002, 9, 25))
        with pytest.raises(complement.SourceError, match="incomplete_bulk_response"):
            complement._store_bulk_years(connection, tmp_path, task, [
                {"date": "2002-09-20", "stock_id": "5678"},
            ], now, date(2002, 9, 25))


def test_old_pending_year_promotes_current_empty_to_one_bulk_query(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    with complement._db(tmp_path / "queue.sqlite3") as connection:
        complement._populate(connection, tmp_path, today=date(2026, 9, 25))
        connection.execute(
            "UPDATE tasks SET state='observed_empty', last_attempt_at_utc=?, "
            "next_attempt_at_utc=? WHERE dataset='TaiwanStockParValueChange' AND partition='2026'",
            (now.isoformat(), (now + timedelta(days=1)).isoformat()),
        )
        connection.commit()
        complement._populate(connection, tmp_path, today=date(2026, 9, 25))
        task = connection.execute(
            "SELECT dataset,data_id,partition,kind,priority,state FROM tasks "
            "WHERE dataset='TaiwanStockParValueChange' AND partition='2026'"
        ).fetchone()
        current = complement.Task(*task)
        assert current.state == "observed_empty"
        assert complement._bulk_needed(connection, current, date(2026, 9, 25))
        assert connection.execute(
            "SELECT next_attempt_at_utc FROM tasks WHERE dataset=? AND partition=?",
            (current.dataset, current.partition),
        ).fetchone()[0] <= datetime.now(UTC).isoformat()


def test_wide_institutional_is_derived_without_api_call(tmp_path: Path) -> None:
    now = datetime(2026, 9, 25, tzinfo=UTC)
    long = complement.Task(complement.LONG_INSTITUTIONAL, "2330", "history", "id_history", 2, "pending")
    wide = complement.Task(complement.WIDE_INSTITUTIONAL, "2330", "history", "derived", 3, "pending")
    with complement._db(tmp_path / "queue.sqlite3") as connection:
        complement._add_tasks(connection, [(long.dataset, long.data_id, long.partition, long.kind, long.priority),
                                           (wide.dataset, wide.data_id, wide.partition, wide.kind, wide.priority)])
        receipt = complement._store(tmp_path, long, [
            {"date": "2026-09-24", "stock_id": "2330", "name": "Foreign_Investor", "buy": 3, "sell": 1},
            {"date": "2026-09-24", "stock_id": "2330", "name": "Dealer_self", "buy": 2, "sell": 4},
        ], now)
        complement._save_result(connection, long, receipt, now)
        rows = complement._derive_wide(tmp_path, connection, wide)
        assert len(rows) == 1
        assert rows[0]["Foreign_Investor_buy"] == 3
        assert rows[0]["Dealer_self_sell"] == 4
        assert rows[0]["Dealer_buy"] == 0
        derived = complement._store(tmp_path, wide, rows, now)
        assert derived["derived_from"] == complement.LONG_INSTITUTIONAL
        complement._save_result(connection, wide, derived, now)
        assert connection.execute("SELECT state FROM tasks WHERE dataset=?",
                                  (wide.dataset,)).fetchone()[0] == "complete"


def test_request_uses_free_per_id_and_bulk_history_contract(tmp_path: Path) -> None:
    class Limiter:
        def wait(self) -> None:
            pass

        def defer(self, seconds: int) -> None:
            raise AssertionError(f"unexpected defer {seconds}")

    class Response:
        status_code = 200
        headers: dict[str, str] = {}

        def json(self) -> dict:
            return {"status": 200, "msg": "success", "data": []}

    class Session:
        def __init__(self) -> None:
            self.params: list[dict[str, str]] = []

        def get(self, _url: str, *, params: dict[str, str], **_kwargs: object) -> Response:
            self.params.append(params)
            return Response()

    session = Session()
    fx = complement.Task("TaiwanExchangeRate", "USD", "history", "id_history", 1, "pending")
    whole = complement.Task("TaiwanStockDelisting", "", "2026", "year", 0, "pending")
    complement._request(session, Limiter(), tmp_path, fx, "secret", today=date(2026, 9, 25))
    complement._request(session, Limiter(), tmp_path, whole, "secret",
                        today=date(2026, 9, 25), full_history=True)
    assert session.params == [
        {"dataset": "TaiwanExchangeRate", "data_id": "USD", "start_date": "2006-01-01"},
        {"dataset": "TaiwanStockDelisting", "start_date": "2001-01-01"},
    ]


@pytest.mark.parametrize("status,message,expected", [
    (400, "Your level is register. Please update your user level", "not_entitled"),
    (400, "Bad parameters", "provider_bad_request"),
    (403, "ip banned", "ip_banned"),
])
def test_client_errors_are_classified_without_automatic_repeat(
    tmp_path: Path, status: int, message: str, expected: str,
) -> None:
    class Limiter:
        deferred: int | None = None

        def wait(self) -> None:
            pass

        def defer(self, seconds: int) -> None:
            self.deferred = seconds

    class Response:
        status_code = status
        headers: dict[str, str] = {}

        def json(self) -> dict:
            return {"status": status, "msg": message}

    class Session:
        def get(self, *_args: object, **_kwargs: object) -> Response:
            return Response()

    limiter = Limiter()
    task = complement.Task("TaiwanExchangeRate", "USD", "history", "id_history", 1, "pending")
    with pytest.raises(complement.SourceError, match=expected):
        complement._request(Session(), limiter, tmp_path, task, "secret", today=date(2026, 9, 25))
    assert limiter.deferred == (1800 if expected == "ip_banned" else None)

from __future__ import annotations

from datetime import UTC, date, datetime
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

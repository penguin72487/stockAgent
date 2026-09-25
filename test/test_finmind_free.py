from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path

import pytest

from downloader.download_finmind_free import (
    SESSION_DATASETS,
    ProviderError,
    _candidate_days,
    _calendar_dates,
    _receipt_usable,
    _record_session,
    _session_cutoff,
    _validated_session_rows,
)
from downloader import download_finmind_free as finmind
from stockagent.live.data_monitor_dashboard import _finmind_free_sources


def _rows(dataset: str, day: date, *, step: int) -> list[dict[str, object]]:
    result = []
    for seconds in range(9 * 3600, 13 * 3600 + 30 * 60 + 1, step):
        stamp = f"{seconds // 3600:02d}:{seconds // 60 % 60:02d}:{seconds % 60:02d}"
        result.append(
            {"Time": stamp, "date": day.isoformat(), "TotalBuyOrder": 1, "TotalDealVolume": 0}
            if dataset == SESSION_DATASETS[0]
            else {"date": f"{day} {stamp}", "TAIEX": 10000.0}
        )
    return result


@pytest.mark.parametrize("step,grain,expected", [(60, "1m", 271), (5, "5s", 3241)])
@pytest.mark.parametrize("dataset", SESSION_DATASETS)
def test_exact_historical_grains_are_complete(dataset: str, step: int, grain: str, expected: int) -> None:
    day = date(2015, 1, 5)
    rows = _rows(dataset, day, step=step)
    assert _validated_session_rows(dataset, day, rows) == ("complete", grain, expected, 0)


def test_gap_and_duplicate_are_not_complete() -> None:
    day = date(2020, 4, 6)
    rows = _rows(SESSION_DATASETS[0], day, step=5)
    assert _validated_session_rows(SESSION_DATASETS[0], day, rows[:10] + rows[11:])[0] == "partial"
    assert _validated_session_rows(SESSION_DATASETS[0], day, rows[:10] + [rows[9]] + rows[10:])[0] == "partial"


def test_session_cutoff_never_accepts_unfinished_current_day() -> None:
    assert _session_cutoff(datetime(2026, 9, 25, 5, 0, tzinfo=UTC)) == date(2026, 9, 24)
    assert _session_cutoff(datetime(2026, 9, 25, 6, 0, tzinfo=UTC)) == date(2026, 9, 25)


def test_calendar_rejects_broken_rows() -> None:
    with pytest.raises(ProviderError, match="invalid_calendar"):
        _calendar_dates([{"date": "not-a-date"}])


def test_receipt_requires_real_parquet_and_prioritizes_recent(tmp_path: Path) -> None:
    now = datetime(2026, 9, 25, 7, 0, tzinfo=UTC)
    days = [date(2026, 9, 22), date(2026, 9, 23), date(2026, 9, 24)]
    done = _record_session(tmp_path, SESSION_DATASETS[0], days[0], _rows(SESSION_DATASETS[0], days[0], step=60), now=now)
    assert done["status"] == "complete"
    assert _receipt_usable(done, tmp_path, now=now)
    deferred = tmp_path / "receipts" / SESSION_DATASETS[1] / f"{days[0]}.json"
    deferred.parent.mkdir(parents=True)
    deferred.write_text(json.dumps({"status": "provider_empty", "retry_at_utc": (now + timedelta(hours=1)).isoformat()}))
    tasks, counts = _candidate_days(days, tmp_path, now=now)
    assert counts["total"] == 6
    assert counts["complete"] == 1
    assert counts["deferred"] == 1
    assert tasks[0][1] == days[-1]
    (tmp_path / done["parquet_path"]).unlink()
    _, counts = _candidate_days(days, tmp_path, now=now)
    assert counts["complete"] == 0


def test_monitor_keeps_source_series_separate_and_news_disabled(tmp_path: Path) -> None:
    now = datetime(2026, 9, 25, 7, 0, tzinfo=UTC)
    folder = tmp_path / "data_finmind"
    folder.mkdir()
    (folder / "status.json").write_text(json.dumps({
        "state": "backfilling",
        "series": {
            SESSION_DATASETS[0]: {"total": 3, "complete": 2, "rows": 542,
                                  "first_complete_date": "2026-09-22", "last_complete_date": "2026-09-24"},
            SESSION_DATASETS[1]: {"total": 3, "complete": 1, "rows": 271,
                                  "first_complete_date": "2026-09-24", "last_complete_date": "2026-09-24"},
        },
        "news": "disabled_by_user",
    }))
    rows = _finmind_free_sources(tmp_path, now=now, service={"active": True})
    from downloader.download_finmind_sponsor import SOURCES
    assert len(rows) == 52 + len(SOURCES)
    assert {row["id"] for row in rows[:4]} == {
        "finmind:TaiwanStockStatisticsOfOrderBookAndTrade",
        "finmind:TaiwanVariousIndicators5Seconds",
        "finmind:TaiwanStockTradingDate",
        "finmind:TaiwanStockInfoWithWarrant",
    }
    assert rows[0]["record_stats"]["count"] == 542
    assert rows[0]["coverage"]["current"] == 2


@pytest.mark.parametrize("status,message,expected,defer", [
    (402, "quota exceeded", "rate_limited", 3600),
    (403, "ip banned", "ip_banned", 1800),
    (400, "TokenIllegal", "invalid_token", None),
    (404, "bad dataset", "invalid_request", None),
])
def test_base_worker_respects_finmind_client_error_policy(
    status: int, message: str, expected: str, defer: int | None,
) -> None:
    class Limiter:
        deferred: float | None = None

        def wait(self) -> None:
            pass

        def defer(self, seconds: float) -> None:
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
    with pytest.raises(ProviderError, match=expected):
        finmind._request(Session(), limiter, SESSION_DATASETS[0],
                         start_date=date(2026, 9, 25), token="secret")
    assert limiter.deferred == defer

from datetime import datetime
from zoneinfo import ZoneInfo

from stockagent.live.shioaji_schedule import (
    historical_query_is_protected,
    historical_query_pause_seconds,
)
from downloader import download_shioaji_tw_kbars


TAIPEI = ZoneInfo("Asia/Taipei")


def _local(hour: int, minute: int, *, day: int = 17) -> datetime:
    # 2026-08-17 is Monday; 2026-08-16 is Sunday.
    return datetime(2026, 8, day, hour, minute, tzinfo=TAIPEI)


def test_history_queries_stop_before_observed_quota_reset() -> None:
    assert not historical_query_is_protected(_local(7, 44))
    assert historical_query_is_protected(_local(7, 45))
    assert historical_query_is_protected(_local(8, 2))
    assert historical_query_pause_seconds(_local(8, 2)) == 23_340


def test_history_queries_resume_only_after_close_and_weekends_remain_available() -> None:
    assert historical_query_is_protected(_local(14, 30))
    assert not historical_query_is_protected(_local(14, 31))
    assert not historical_query_is_protected(_local(8, 2, day=16))


def test_existing_downloaders_share_the_schedule_guard(monkeypatch) -> None:
    monkeypatch.setattr(
        download_shioaji_tw_kbars,
        "historical_query_is_protected",
        lambda: True,
    )
    assert download_shioaji_tw_kbars._taiwan_market_hours_now()

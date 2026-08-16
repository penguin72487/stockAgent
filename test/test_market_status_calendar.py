from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import polars as pl

from stockagent.live.market_status import (
    expected_latest_data_date,
    market_is_open,
    verified_tw_stock_session_day,
)


def test_tw_expected_date_uses_official_exchange_holiday_schedule(tmp_path) -> None:
    public_root = tmp_path / "data_tw_public"
    stock_root = public_root / "stocks"
    stock_root.mkdir(parents=True)
    pl.DataFrame(
        {
            "Name": [
                "兒童節及民族掃墓節",
                "兒童節及民族掃墓節",
                "農曆春節後開始交易日",
            ],
            "Date": ["1150403", "1150406", "1150223"],
        }
    ).write_parquet(
        public_root / "twse_api_holidayschedule_holidayschedule.parquet"
    )
    cfg = SimpleNamespace(
        timezone="Asia/Taipei",
        data_ready_time="13:30",
        open_time="09:00",
        close_time="13:30",
        holidays=(),
    )
    now = datetime(2026, 4, 6, 14, 0, tzinfo=ZoneInfo("Asia/Taipei"))

    assert expected_latest_data_date(
        cfg,
        market_type="tw",
        now=now,
        parquet_root=stock_root,
    ) == "2026-04-02"
    assert market_is_open(
        cfg,
        market_type="tw",
        now=now.replace(hour=10),
        parquet_root=stock_root,
    ) == (False, "2026-04-06 is not a trading day")


def test_verified_tw_stock_session_fails_closed_and_uses_official_provenance(
    tmp_path,
) -> None:
    public_root = tmp_path / "data_tw_public"
    public_root.mkdir()
    assert verified_tw_stock_session_day(
        datetime(2026, 8, 15).date(), parquet_root=None
    ) == (False, "2026-08-15 is a weekend")
    assert verified_tw_stock_session_day(
        datetime(2026, 8, 17).date(), parquet_root=public_root
    ) == (False, "official TWSE holiday schedule is missing")

    pl.DataFrame(
        {
            "Name": ["中華民國開國紀念日", "國曆新年開始交易日"],
            "Date": ["1150101", "1150102"],
            "_dataset": [
                "twse_api_holidayschedule_holidayschedule",
                "twse_api_holidayschedule_holidayschedule",
            ],
            "_source": ["TWSE OpenAPI", "TWSE OpenAPI"],
            "_as_of_date": ["2026-08-14", "2026-08-14"],
        }
    ).write_parquet(
        public_root / "twse_api_holidayschedule_holidayschedule.parquet"
    )

    open_day, open_reason = verified_tw_stock_session_day(
        datetime(2026, 8, 17).date(), parquet_root=public_root
    )
    holiday, holiday_reason = verified_tw_stock_session_day(
        datetime(2026, 1, 1).date(), parquet_root=public_root
    )
    assert open_day is True
    assert "ordinary weekday session" in open_reason
    assert holiday is False
    assert "中華民國開國紀念日" in holiday_reason

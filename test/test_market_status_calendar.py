from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import polars as pl

from stockagent.live.market_status import (
    expected_latest_data_date,
    market_is_open,
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

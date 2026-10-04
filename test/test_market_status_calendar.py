from __future__ import annotations

import hashlib
import json
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import polars as pl

from stockagent.live.market_status import (
    expected_latest_data_date,
    market_is_open,
    tw_stock_day_decision,
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
            "_dataset": ["twse_api_holidayschedule_holidayschedule"] * 3,
            "_source": ["TWSE OpenAPI"] * 3,
            "_as_of_date": ["2026-04-01"] * 3,
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


def test_calendar_decision_distinguishes_closure_open_and_unknown(tmp_path) -> None:
    root = tmp_path / "public"
    root.mkdir()
    observed = datetime(2026, 9, 26, 8, tzinfo=ZoneInfo("Asia/Taipei"))
    missing = tw_stock_day_decision(
        datetime(2026, 9, 29).date(), parquet_root=root, observed=observed,
    )
    assert missing.status == "unknown"
    pl.DataFrame({
        "Name": ["中秋節", "孔子誕辰紀念日/ 教師節", "國慶日"],
        "Date": ["1150925", "1150928", "1151009"],
        "_dataset": ["twse_api_holidayschedule_holidayschedule"] * 3,
        "_source": ["TWSE OpenAPI"] * 3,
        "_as_of_date": ["2026-09-16"] * 3,
    }).write_parquet(root / "twse_api_holidayschedule_holidayschedule.parquet")
    statuses = {
        day: tw_stock_day_decision(
            datetime(2026, 9, day).date(), parquet_root=root, observed=observed,
        ).status
        for day in (25, 26, 28, 29)
    }
    assert statuses == {25: "closed", 26: "closed", 28: "closed", 29: "scheduled_open"}


def test_latest_official_snapshot_replaces_stale_event(tmp_path) -> None:
    root = tmp_path / "public"
    root.mkdir()
    pl.DataFrame({
        "Name": ["舊錯誤休市", "國曆新年開始交易日"],
        "Date": ["1150929", "1150102"],
        "_dataset": ["twse_api_holidayschedule_holidayschedule"] * 2,
        "_source": ["TWSE OpenAPI"] * 2,
        "_as_of_date": ["2026-09-15", "2026-09-16"],
    }).write_parquet(root / "twse_api_holidayschedule_holidayschedule.parquet")
    decision = tw_stock_day_decision(
        datetime(2026, 9, 29).date(), parquet_root=root,
        observed=datetime(2026, 9, 26, tzinfo=ZoneInfo("Asia/Taipei")),
    )
    assert decision.status == "scheduled_open"


def test_truncated_latest_holiday_snapshot_is_unknown(tmp_path) -> None:
    root = tmp_path / "public"
    root.mkdir()
    pl.DataFrame({
        "Name": ["休市A", "休市B", "休市C", "休市D", "國曆新年開始交易日"],
        "Date": ["1150901", "1150902", "1150903", "1150925", "1150102"],
        "_dataset": ["twse_api_holidayschedule_holidayschedule"] * 5,
        "_source": ["TWSE OpenAPI"] * 5,
        "_as_of_date": ["2026-09-15"] * 4 + ["2026-09-16"],
    }).write_parquet(root / "twse_api_holidayschedule_holidayschedule.parquet")
    decision = tw_stock_day_decision(
        datetime(2026, 9, 25).date(), parquet_root=root,
        observed=datetime(2026, 9, 26, tzinfo=ZoneInfo("Asia/Taipei")),
    )
    assert decision.status == "unknown"


def test_verified_actual_sessions_preserve_saturday_and_prove_historical_closure(tmp_path) -> None:
    root = tmp_path / "public"
    root.mkdir()
    path = root / "twse_taiex_ohlc.parquet"
    pl.DataFrame({
        "date": [datetime(2018, 12, 22).date(), datetime(2018, 12, 24).date()],
        "opening_index": [100.0, 101.0],
        "highest_index": [102.0, 103.0],
        "lowest_index": [99.0, 100.0],
        "closing_index": [101.0, 102.0],
    }).write_parquet(path)
    summary = {
        "dataset": "twse_taiex_ohlc", "source": "TWSE",
        "source_product": "indicesReport/MI_5MINS_HIST",
        "coverage_complete": True, "replacement_promoted": True,
        "failed_count": 0, "unresolved_month_count": 0,
        "effective_start_date": "2018-12-22", "effective_end_date": "2018-12-24",
        "output_rows": 2,
        "output_receipt": {
            "size": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        },
    }
    path.with_suffix(".summary.json").write_text(json.dumps(summary), encoding="utf-8")
    observed = datetime(2018, 12, 25, tzinfo=ZoneInfo("Asia/Taipei"))
    saturday = tw_stock_day_decision(datetime(2018, 12, 22).date(), parquet_root=root, observed=observed)
    sunday = tw_stock_day_decision(datetime(2018, 12, 23).date(), parquet_root=root, observed=observed)
    assert saturday.status == "actual_open"
    assert sunday.status == "closed"
    summary["output_receipt"]["sha256"] = "mismatched"
    path.with_suffix(".summary.json").write_text(json.dumps(summary), encoding="utf-8")
    assert tw_stock_day_decision(
        datetime(2018, 12, 22).date(), parquet_root=root, observed=observed,
    ).status == "unknown"

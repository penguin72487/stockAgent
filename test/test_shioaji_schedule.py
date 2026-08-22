from datetime import date, datetime
from pathlib import Path
import sys
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from downloader import download_shioaji_tx_futures_ticks
from downloader.download_shioaji_tw_kbars import (
    TrafficBudgetReached,
    _check_traffic_budget,
)
from stockagent.live.shioaji_schedule import (
    HISTORICAL_MAX_TRAFFIC_FRACTION,
    historical_query_is_protected,
    historical_query_pause_seconds,
)
from downloader import download_shioaji_tw_kbars, download_shioaji_tw_minute_kbars


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


def test_history_downloaders_default_to_the_ninety_percent_safety_limit(
    monkeypatch,
) -> None:
    assert HISTORICAL_MAX_TRAFFIC_FRACTION == 0.90
    for module in (
        download_shioaji_tw_kbars,
        download_shioaji_tw_minute_kbars,
        download_shioaji_tx_futures_ticks,
    ):
        monkeypatch.setattr(sys, "argv", [module.__name__])
        args = module.parse_args()
        assert args.max_traffic_fraction == 0.90

    limit_bytes = 2 * 1024**3
    futures_ceiling = int(limit_bytes * HISTORICAL_MAX_TRAFFIC_FRACTION)
    assert futures_ceiling == int(limit_bytes * 0.90)


def test_history_budget_has_only_the_ninety_percent_ceiling() -> None:
    class UsageApi:
        used = 89

        def usage(self) -> SimpleNamespace:
            return SimpleNamespace(bytes=self.used, limit_bytes=100)

    api = UsageApi()
    assert _check_traffic_budget(api, max_fraction=0.90) == (89, 100)
    api.used = 90
    with pytest.raises(TrafficBudgetReached, match="ceiling=90"):
        _check_traffic_budget(api, max_fraction=0.90)


def test_service_runners_do_not_override_the_shared_ninety_percent_policy() -> None:
    root = Path(__file__).resolve().parents[1]
    futures_runner = (root / "scripts/run_shioaji_tx_history_backfill.sh").read_text()
    minute_runner = (root / "scripts/run_shioaji_minute_full_backfill.sh").read_text()
    assert "SHIOAJI_FUTURES_HISTORY_MAX_TRAFFIC_FRACTION:-0.90" in futures_runner
    assert "SHIOAJI_MINUTE_MAX_TRAFFIC_FRACTION:-0.90" in minute_runner


def test_completed_futures_contract_does_not_login_again(monkeypatch, tmp_path) -> None:
    trading_date = date(2026, 8, 19)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            download_shioaji_tx_futures_ticks.__name__,
            "--contract",
            "TXFR1",
            "--output-dir",
            str(tmp_path),
            "--start-date",
            trading_date.isoformat(),
            "--end-date",
            trading_date.isoformat(),
        ],
    )
    monkeypatch.setattr(
        download_shioaji_tx_futures_ticks,
        "_calendar",
        lambda *_args: [trading_date],
    )
    monkeypatch.setattr(
        download_shioaji_tx_futures_ticks,
        "_valid_receipt",
        lambda *_args: {"status": "complete"},
    )
    monkeypatch.setattr(
        download_shioaji_tx_futures_ticks,
        "_write_manifest",
        lambda *_args, **_kwargs: {
            "status": "complete",
            "resolved_trading_dates": 1,
            "expected_trading_dates": 1,
            "rows": 1,
            "bytes": 1,
        },
    )
    monkeypatch.setitem(sys.modules, "shioaji", None)

    assert download_shioaji_tx_futures_ticks.main() == 0

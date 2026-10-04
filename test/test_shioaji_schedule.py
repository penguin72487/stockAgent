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
    historical_login_pause_seconds,
    minute_connection_plan,
    minute_pre_night_deadline,
    next_postreset_historical_window,
    reserved_fop_connection_slots,
    historical_query_pause_seconds,
    latest_completed_tw_stock_session,
    previous_tw_stock_session,
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


def test_history_queries_resume_only_after_close_and_weekends_remain_available() -> (
    None
):
    assert historical_query_is_protected(_local(14, 30))
    assert not historical_query_is_protected(_local(14, 31))
    assert not historical_query_is_protected(_local(8, 2, day=16))


def test_history_login_reserves_night_capture_and_uses_one_shared_slot() -> None:
    assert reserved_fop_connection_slots(_local(14, 30)) == 0
    assert reserved_fop_connection_slots(_local(14, 31)) == 3
    assert reserved_fop_connection_slots(_local(4, 59, day=18)) == 3
    assert reserved_fop_connection_slots(_local(5, 1, day=18)) == 0
    assert reserved_fop_connection_slots(_local(5, 1, day=18), active_workers=2) == 2
    assert historical_login_pause_seconds(_local(14, 31)) > 14 * 3600
    assert historical_login_pause_seconds(_local(4, 59, day=18)) > 0
    assert historical_login_pause_seconds(_local(5, 1, day=18)) == 0
    assert historical_login_pause_seconds(_local(5, 1, day=18), fop_workers=3) == 60
    assert historical_login_pause_seconds(_local(5, 1, day=18), fop_workers=2) == 0
    assert historical_login_pause_seconds(_local(5, 1, day=16)) == 0


def test_minute_frontier_uses_only_the_bounded_postclose_window() -> None:
    deadline = datetime(2026, 8, 17, 14, 45, tzinfo=TAIPEI)
    assert minute_pre_night_deadline(_local(14, 30)) is None
    assert minute_pre_night_deadline(_local(14, 31)) == deadline
    assert minute_pre_night_deadline(_local(14, 44)) == deadline
    assert minute_pre_night_deadline(_local(14, 45)) is None
    assert minute_connection_plan(
        _local(14, 31), active_fop_workers=0, active_history_workers=0,
        reserved_stock_quotes=2, configured_workers=4,
    ) == (3, 0, 0, deadline)
    # A still-running historical request keeps its real login slot.
    assert minute_connection_plan(
        _local(14, 31), active_fop_workers=0, active_history_workers=1,
        reserved_stock_quotes=2, configured_workers=4,
    ) == (2, 0, 1, deadline)
    # Before the 14:50 FOP pre-open, all three future slots return to capture.
    assert minute_connection_plan(
        _local(14, 45), active_fop_workers=0, active_history_workers=0,
        reserved_stock_quotes=2, configured_workers=4,
    ) == (0, 3, 0, None)
    assert minute_connection_plan(
        _local(16, 0), active_fop_workers=3, active_history_workers=0,
        reserved_stock_quotes=2, configured_workers=4,
    ) == (0, 3, 0, None)
    assert minute_connection_plan(
        _local(5, 1, day=18), active_fop_workers=0, active_history_workers=0,
        reserved_stock_quotes=2, configured_workers=4,
    ) == (2, 0, 1, None)


def test_traffic_ceiling_waits_for_next_reset_and_historical_window() -> None:
    assert next_postreset_historical_window(_local(6, 0)) == _local(14, 31)
    assert next_postreset_historical_window(_local(22, 0)) == _local(14, 31, day=18)
    assert next_postreset_historical_window(_local(22, 0, day=21)) == _local(14, 31, day=24)


def test_history_runners_share_one_logged_in_batch_lock() -> None:
    root = Path(__file__).resolve().parents[1]
    common = (root / "scripts/shioaji_history_runner_common.sh").read_text()
    for name in ("run_shioaji_historical_market_data.sh", "run_shioaji_tx_history_backfill.sh"):
        runner = (root / "scripts" / name).read_text()
        assert "history_connection_delay" in runner
        assert "flock -n 8" in runner
        assert "flock -u 8" in runner
    assert 'exec 8>"$history_login_lock_root/login.lock"' in common
    general_runner = (root / "scripts/run_shioaji_historical_market_data.sh").read_text()
    assert "history_recent_login_waiter" in general_runner
    assert "yield_to_waiting_tx_history" in general_runner
    assert "history_recent_login_waiter" in common
    assert "incomplete_catalog_sweep" in common
    futures_runner = (root / "scripts/run_shioaji_tx_history_backfill.sh").read_text()
    assert "history_recent_login_waiter" in futures_runner
    assert "yield_to_waiting_exact_history" in futures_runner


def test_previous_tw_stock_session_skips_weekend_targets() -> None:
    observed = datetime(2026, 8, 23, 3, 0, tzinfo=TAIPEI)
    assert previous_tw_stock_session(observed) == date(2026, 8, 21)


def test_latest_completed_session_advances_only_after_close() -> None:
    assert latest_completed_tw_stock_session(_local(14, 30)) == date(2026, 8, 14)
    assert latest_completed_tw_stock_session(_local(14, 31)) == date(2026, 8, 17)


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

    monkeypatch.setattr(sys, "argv", [download_shioaji_tx_futures_ticks.__name__])
    futures_args = download_shioaji_tx_futures_ticks.parse_args()
    assert futures_args.calendar_path == Path(
        "data_tw_index_futures/day_session_contracts.parquet"
    )

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
    assert "rc == 79" in futures_runner
    assert "connection_capacity" in futures_runner
    assert 'inventory_args+=(--refresh-inventory)' in futures_runner
    assert '"${inventory_args[@]}" --refresh-empty' in futures_runner
    assert "next_bounded_batch" in futures_runner
    assert "--contracts-file" in futures_runner
    assert "batch_is_current" not in futures_runner


def test_market_schedule_import_does_not_load_training_config() -> None:
    root = Path(__file__).resolve().parents[1]
    market_status = (root / "stockagent/live/market_status.py").read_text()
    prefix = market_status.split("def data_freshness", maxsplit=1)[0]
    assert "from stockagent.config import load_config" not in prefix


def test_minute_runner_reserves_live_connections_and_does_not_gate_on_futures_calendar() -> None:
    root = Path(__file__).resolve().parents[1]
    minute_runner = (root / "scripts/run_shioaji_minute_full_backfill.sh").read_text()
    assert "SHIOAJI_MINUTE_WORKERS:-4" in minute_runner
    assert "SHIOAJI_MINUTE_WORKERS:-5" not in minute_runner
    assert "minute_connection_plan(" in minute_runner
    assert 'sleep "$market_delay"\n    # The target may advance while sleeping across the close.' in minute_runner
    assert "futures_history=independent_downstream_gate" in minute_runner
    assert "futures_priority_state" not in minute_runner
    assert "--stop-at \"$minute_deadline\"" in minute_runner
    assert "stop_at=$minute_deadline" in minute_runner
    assert "taifex_session_kind(now, include_preopen=True)" in minute_runner
    assert "time(5, 0, 10)" in minute_runner
    assert 'grep -Fq "target=$BACKFILL_END_DATE" "$SOURCE_GAP_RETRY_MARKER"' in minute_runner
    quota_function = minute_runner.split("seconds_until_next_quota_window() {", 1)[1].split("\n}\n", 1)[0]
    assert "next_postreset_historical_window" in quota_function
    assert "time(5, 0, 10)" not in quota_function
    assert "latest_run_stop_reason" in minute_runner
    timer = (root / "deploy/systemd/stockagent-shioaji-minute-backfill.timer.in").read_text()
    assert "14:31:00 Asia/Taipei" in timer


def test_top200_terminal_connection_skip_unblocks_postclose_minute_backfill() -> None:
    root = Path(__file__).resolve().parents[1]
    top200_runner = (root / "scripts/run_shioaji_top200_stream.sh").read_text()
    minute_runner = (root / "scripts/run_shioaji_minute_full_backfill.sh").read_text()

    assert 'write_capture_state "$trade_date" "skipped" "connection_budget"' in top200_runner
    assert 'payload.get("status") == "skipped"' in minute_runner
    assert 'payload.get("reason") == "connection_budget"' in minute_runner
    assert 'print("0 top200_terminal_skip")' in minute_runner
    assert 'print("0 top200_audit_missing")' in minute_runner
    assert top200_runner.index('if (( required_connections > MAX_CONNECTIONS )); then') < top200_runner.index('if ! wait_for_taifex_priority; then')
    assert '"taifex_priority_window_expired"' in top200_runner


def test_taifex_strategy_bootstrap_failure_preserves_data_only_capture() -> None:
    runner = (
        Path(__file__).resolve().parents[1]
        / "scripts/run_shioaji_taifex_bidask_stream.sh"
    ).read_text()
    assert "strategy_bootstrap_ready=false" in runner
    assert 'capture=data_only' in runner
    assert 'if [[ "$strategy_bootstrap_ready" == true && "$worker_index" -eq 0 ]]; then' in runner
    assert 'if [[ "$strategy_bootstrap_ready" == true ]]; then' in runner
    assert 'settlement_bootstrap_failed trade_date=$trade_date retry_seconds=30' not in runner


def test_minute_backfill_service_has_bounded_memory() -> None:
    service = (
        Path(__file__).resolve().parents[1]
        / "deploy/systemd/stockagent-shioaji-minute-backfill.service.in"
    ).read_text(encoding="utf-8")

    assert "MemoryAccounting=true" in service
    assert "MemoryHigh=48G" in service
    assert "MemoryMax=64G" in service
    assert "MemorySwapMax=8G" in service


def test_minute_runner_builds_only_from_the_complete_current_run() -> None:
    root = Path(__file__).resolve().parents[1]
    minute_runner = (root / "scripts/run_shioaji_minute_full_backfill.sh").read_text()
    assert '[[ "$summary_state" == "ready=true "* ]]' in minute_runner
    assert '[[ "$summary_state" == *"collected=true"* ]]' not in minute_runner
    assert "run_reported == run_selected" in minute_runner
    assert 'not bool(run_payload.get("stopped_for_traffic"))' in minute_runner
    assert "run_payload = payload" in minute_runner
    # Partial-run receipts may determine retry timing, never publish readiness.
    assert 'path.name == "download_summary.json"' in minute_runner
    assert 'path.stat().st_mtime_ns >= started_ns' in minute_runner
    assert 'path = Path("data_tw_minute/shioaji_1m/latest_run_summary.json")' in minute_runner
    assert "shioaji_minute_backfill_state reuse" in minute_runner
    assert "run_fintech_python -m scripts.build_shioaji_tw_minute_dataset" in minute_runner
    assert "run_fintech_python scripts/build_shioaji_tw_minute_dataset.py" not in minute_runner
    assert "run_fintech_python -m scripts.audit_shioaji_tw_minute_dataset" in minute_runner


def test_completed_futures_contract_does_not_login_again(monkeypatch, tmp_path) -> None:
    trading_date = date(2026, 8, 19)
    # This test verifies receipt short-circuiting, not the production market-hour
    # guard.  Pin the clock seam so the result cannot change between a Sunday CI
    # run and a weekday 07:45-14:31 run.
    monkeypatch.setattr(
        download_shioaji_tx_futures_ticks,
        "_taiwan_market_hours_now",
        lambda: False,
    )
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


def test_missing_futures_contract_is_a_truthful_terminal_gap(
    monkeypatch, tmp_path
) -> None:
    trading_date = date(2026, 8, 19)
    # Contract-catalog behavior must be deterministic regardless of the wall
    # clock.  The production schedule guard has its own focused assertion.
    monkeypatch.setattr(
        download_shioaji_tx_futures_ticks,
        "_taiwan_market_hours_now",
        lambda: False,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            download_shioaji_tx_futures_ticks.__name__,
            "--contract",
            "KAFR1",
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

    class Contracts:
        @staticmethod
        def get(_code):
            return None

    class Api:
        contracts = Contracts()
        logged_out = False

        def __init__(self, *, simulation):
            assert simulation is False

        def set_event_callback(self, _callback):
            return None

        def login(self, **_kwargs):
            return None

        def logout(self):
            self.logged_out = True

    fake_shioaji = SimpleNamespace(Shioaji=Api)
    monkeypatch.setitem(sys.modules, "shioaji", fake_shioaji)
    monkeypatch.setenv("SHIOAJI_API_KEY", "test-key")
    monkeypatch.setenv("SHIOAJI_SECRET_KEY", "test-secret")

    assert (
        download_shioaji_tx_futures_ticks.main()
        == download_shioaji_tx_futures_ticks.CONTRACT_UNAVAILABLE_EXIT
    )
    manifest = __import__("json").loads(
        (tmp_path / "manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["schema_version"] == 2
    assert manifest["status"] == "contract_unavailable"
    assert manifest["unavailable_reason"] == "shioaji_contract_catalog_missing"
    assert manifest["no_data_fabricated"] is True
    assert manifest["resolved_trading_dates"] == 0
    assert not (tmp_path / "receipts").exists()
    assert not (tmp_path / "ticks").exists()


def test_futures_history_connection_capacity_is_retryable(
    monkeypatch, tmp_path
) -> None:
    trading_date = date(2026, 8, 19)
    monkeypatch.setattr(
        download_shioaji_tx_futures_ticks,
        "_taiwan_market_hours_now",
        lambda: False,
    )
    monkeypatch.setattr(
        download_shioaji_tx_futures_ticks,
        "_calendar",
        lambda *_args: [trading_date],
    )
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

    class CapacityError(Exception):
        code = 451

    class Api:
        def __init__(self, *, simulation):
            assert simulation is False

        def set_event_callback(self, _callback):
            return None

        def login(self, **_kwargs):
            raise CapacityError("Too Many Connections")

    monkeypatch.setitem(sys.modules, "shioaji", SimpleNamespace(Shioaji=Api))
    monkeypatch.setenv("SHIOAJI_API_KEY", "test-key")
    monkeypatch.setenv("SHIOAJI_SECRET_KEY", "test-secret")

    assert (
        download_shioaji_tx_futures_ticks.main()
        == download_shioaji_tx_futures_ticks.CONNECTION_CAPACITY_EXIT
    )


def test_futures_history_market_hour_guard_stops_before_calendar_or_login(
    monkeypatch, tmp_path
) -> None:
    monkeypatch.setattr(
        download_shioaji_tx_futures_ticks,
        "_taiwan_market_hours_now",
        lambda: True,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            download_shioaji_tx_futures_ticks.__name__,
            "--output-dir",
            str(tmp_path),
            "--start-date",
            "2026-08-19",
            "--end-date",
            "2026-08-19",
        ],
    )
    calendar_called = False

    def calendar(*_args):
        nonlocal calendar_called
        calendar_called = True
        raise AssertionError("market-hour guard must run before calendar work")

    monkeypatch.setattr(download_shioaji_tx_futures_ticks, "_calendar", calendar)
    monkeypatch.setitem(sys.modules, "shioaji", None)

    assert download_shioaji_tx_futures_ticks.main() == 76
    assert calendar_called is False

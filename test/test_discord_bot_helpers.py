from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
import inspect
import json
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from stockagent.live.report_formatter import INVESTMENT_WARNING

discord = pytest.importorskip("discord")

from services.discord_bot import bot as discord_bot  # noqa: E402


def test_guide_lists_all_tw_execution_modes() -> None:
    guide = discord_bot._guide_message()

    assert "`tw` 舊版 Naive" in guide
    assert "`tw_cash` 現股/T+2" in guide
    assert "`tw_day_trade_multi_basis` Multi-Basis 現股當沖（初始 1,000 萬）" in guide
    assert "`tw_day_trade_100m` 現股當沖（初始 1 億）" in guide
    assert "`tw_day_trade_multi_basis_22` 多基底22 現股當沖" in guide
    assert "`tw_day_trade_multi_basis_projection_l1_gelu`" in guide


def test_both_enabled_multi_basis_day_trades_are_available_in_market_autocomplete() -> (
    None
):
    choices = asyncio.run(discord_bot.market_autocomplete(None, "multi_basis"))

    values = {choice.value for choice in choices}
    assert "tw_day_trade_multi_basis_22" in values
    assert "tw_day_trade_multi_basis_projection_l1_gelu" in values
    assert "tw_day_trade_multi_basis" in values


def test_all_four_day_trade_modes_share_the_0900_paper_execution_contract() -> None:
    configs = discord_bot._market_configs()
    markets = (
        "tw_day_trade_multi_basis",
        "tw_day_trade_100m",
        "tw_day_trade_multi_basis_22",
        "tw_day_trade_multi_basis_projection_l1_gelu",
    )

    for market in markets:
        config = configs[market]
        assert config.schedule_time == "09:00"
        assert config.day_trade_simulation_enabled is True
        assert config.day_trade_quote_interval_seconds == 60
        assert config.day_trade_simulation_state_dir == (
            "artifacts/live/tw_day_trade_simulation"
        )
        assert config.completed_session_command == (
            "scripts/finalize_tw_public_completed_session.py",
        )
        assert config.completed_session_timeout_seconds == 600


def test_all_four_day_trade_modes_are_in_the_runtime_schedule() -> None:
    scheduled = set(discord_bot._scheduled_markets())

    assert {
        "tw_day_trade_multi_basis",
        "tw_day_trade_100m",
        "tw_day_trade_multi_basis_22",
        "tw_day_trade_multi_basis_projection_l1_gelu",
    }.issubset(scheduled)


def test_day_trade_model_uses_shared_official_opening_snapshot() -> None:
    cfg = discord_bot._market_configs()["tw_day_trade_100m"]

    source = discord_bot._auto_signal_price_source(
        cfg,
        SimpleNamespace(market_open=True),
        "auto",
    )

    assert source == "tw"


def test_day_trade_failure_retry_ignores_slow_batch_retry_setting(monkeypatch) -> None:
    monkeypatch.setenv("STOCKAGENT_SCHEDULED_RETRY_DELAY_SECONDS", "900")
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_RETRY_BASE_SECONDS", "0.25")
    retry_after: dict[str, float] = {}
    failures: dict[str, int] = {}

    first = discord_bot._mark_signal_retry(
        retry_after,
        failures,
        "session:market",
        day_trade=True,
    )
    second = discord_bot._mark_signal_retry(
        retry_after,
        failures,
        "session:market",
        day_trade=True,
    )

    assert first == 0.25
    assert second == 0.5


def test_artifact_backfill_reconciles_completed_external_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = discord_bot._market_configs()["tw_day_trade_multi_basis_projection_l1_gelu"]
    finished: list[tuple[str, str, str]] = []
    synced: list[str] = []
    key = "2026-08-26:tw_day_trade_multi_basis_projection_l1_gelu:artifact_backfill"
    discord_bot.bot._last_artifact_backfill_keys.discard(key)
    monkeypatch.setattr(discord_bot, "_effective_market_config", lambda value: value)
    monkeypatch.setattr(discord_bot, "_ensure_signal_ready", lambda _cfg: object())
    monkeypatch.setattr(discord_bot, "_market_execution_mode", lambda _cfg: "tw_day_trade")
    monkeypatch.setattr(
        discord_bot,
        "_artifact_backfill_is_current",
        lambda _cfg, _status, _mode: True,
    )
    monkeypatch.setattr(
        discord_bot,
        "_sync_latest_live_weights_to_market_artifact",
        lambda _cfg: synced.append(_cfg.market),
    )
    monkeypatch.setattr(
        discord_bot,
        "_finish_artifact_backfill",
        lambda job_key, market, *, status: finished.append(
            (job_key, market, status)
        ),
    )

    assert discord_bot._reconcile_artifact_backfill_if_current(
        cfg,
        key=key,
        market=cfg.market,
    )
    assert synced == [cfg.market]
    assert finished == [(key, cfg.market, "ready")]
    assert key in discord_bot.bot._last_artifact_backfill_keys
    discord_bot.bot._last_artifact_backfill_keys.discard(key)


def test_artifact_backfill_keeps_runtime_scan_off_event_loop() -> None:
    source = inspect.getsource(discord_bot.artifact_backfill.coro)

    assert "await asyncio.to_thread(_artifact_backfill_key, cfg, now)" in source


def test_post_open_catch_up_stops_once_preopen_contract_is_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = discord_bot._market_configs()["tw_day_trade_100m"]
    observed = datetime(2026, 8, 25, 9, 30, tzinfo=ZoneInfo("Asia/Taipei"))

    monkeypatch.setattr(
        discord_bot,
        "_preopen_market_ready_for_session",
        lambda _cfg, _session: True,
    )
    assert discord_bot._scheduled_signal_requires_preopen_catch_up(cfg, observed) is False

    monkeypatch.setattr(
        discord_bot,
        "_preopen_market_ready_for_session",
        lambda _cfg, _session: False,
    )
    assert discord_bot._scheduled_signal_requires_preopen_catch_up(cfg, observed) is True


def test_mis_warm_failure_reuses_only_ready_same_session_receipt(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(
        discord_bot,
        "warm_tw_mis_quote_client",
        lambda **_kwargs: (_ for _ in ()).throw(
            ConnectionError("MIS unavailable")
        ),
    )
    monkeypatch.setattr(
        discord_bot,
        "tw_mis_opening_receipt_status",
        lambda **_kwargs: {
            "ready": True,
            "row_count": 12,
            "path": str(tmp_path / "2026-08-25.json"),
        },
    )

    result = discord_bot._warm_or_reuse_tw_mis_opening_receipt(
        parquet_root=tmp_path,
        session_date="2026-08-25",
    )

    assert result["ready"] is True
    assert result["row_count"] == 12
    assert result["source"] == "twse_tpex:mis"
    assert result["proof"] == "receipt_backed_same_session_opening"


def test_mis_warm_failure_without_receipt_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(
        discord_bot,
        "warm_tw_mis_quote_client",
        lambda **_kwargs: (_ for _ in ()).throw(
            ConnectionError("MIS unavailable")
        ),
    )
    monkeypatch.setattr(
        discord_bot,
        "tw_mis_opening_receipt_status",
        lambda **_kwargs: {"ready": False, "row_count": 0, "path": "missing"},
    )

    with pytest.raises(ConnectionError, match="MIS unavailable"):
        discord_bot._warm_or_reuse_tw_mis_opening_receipt(
            parquet_root=tmp_path,
            session_date="2026-08-25",
        )


def test_post_open_preparation_stops_after_engine_accepts_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = discord_bot._market_configs()["tw_day_trade_100m"]
    observed = datetime(2026, 8, 25, 9, 30, tzinfo=ZoneInfo("Asia/Taipei"))
    monkeypatch.setattr(
        discord_bot,
        "_scheduled_market_session_day",
        lambda _cfg, _now: (True, "calendar session"),
    )
    monkeypatch.setattr(
        discord_bot,
        "_day_trade_schedule_state",
        lambda _cfg, _session: "blocked_open_position",
    )

    assert discord_bot._preopen_prepare_key(cfg, observed) is None


def test_recent_day_trade_artifact_waits_for_engine_instead_of_recomputing(
    monkeypatch,
) -> None:
    cfg = discord_bot._market_configs()["tw_day_trade_100m"]
    observed = datetime.now(ZoneInfo("Asia/Taipei"))
    summary = {
        "generated_at": observed.isoformat(),
        "artifact_published_at": observed.isoformat(),
    }
    monkeypatch.setattr(
        discord_bot,
        "_latest_market_signal",
        lambda _cfg: (SimpleNamespace(), summary),
    )
    monkeypatch.setattr(
        discord_bot,
        "load_service_sync",
        lambda _path: {
            "modes": {
                cfg.market: {
                    "session_date": observed.date().isoformat(),
                    "entry_completed_at": None,
                    "open_position_count": 0,
                }
            }
        },
    )

    assert (
        discord_bot._day_trade_schedule_state(cfg, observed.date().isoformat())
        == "pending_confirmation"
    )
    summary["artifact_published_at"] = (observed - timedelta(hours=3)).isoformat()
    assert (
        discord_bot._day_trade_schedule_state(cfg, observed.date().isoformat())
        == "pending_confirmation"
    )


def test_discord_page_size_and_top_n_floor_to_ten() -> None:
    assert discord_bot._page_size(1) == 10
    assert discord_bot._page_size(5) == 10
    assert discord_bot._page_size(None) == 20
    assert discord_bot._page_size(99) == 40

    assert discord_bot._top_n(1) == 10
    assert discord_bot._top_n(None) == 20


def test_discord_line_pages_can_opt_into_one_row_per_page() -> None:
    rows = [{"symbol": f"S{i:02d}"} for i in range(3)]
    pages = discord_bot._line_pages(
        title="one row",
        rows=rows,
        formatter=lambda row: str(row["symbol"]),
        page_size=1,
        min_page_size=1,
        default_page_size=1,
    )

    assert len(pages) == 3
    assert "`rows 1-1/3`" in pages[0]
    assert "`rows 2-2/3`" in pages[1]
    assert "`rows 3-3/3`" in pages[2]
    assert "S00" in pages[0]
    assert "S01" not in pages[0]


def test_discord_line_pages_use_minimum_ten_rows_and_warning() -> None:
    rows = [{"symbol": f"S{i:02d}"} for i in range(12)]
    pages = discord_bot._line_pages(
        title="test rows",
        rows=rows,
        formatter=lambda row: str(row["symbol"]),
        page_size=5,
    )

    assert len(pages) == 2
    assert "`rows 1-10/12`" in pages[0]
    assert "S09" in pages[0]
    assert "`rows 11-12/12`" in pages[1]
    assert all(INVESTMENT_WARNING in page for page in pages)


def test_discord_empty_trade_page_still_has_warning() -> None:
    pages = discord_bot._line_pages(title="empty", rows=[], formatter=str, page_size=5)

    assert pages == [f"**empty**\n(no rows)\n\n{INVESTMENT_WARNING}"]


def test_user_facing_commands_support_user_install_and_private_contexts() -> None:
    shared_state_commands = {"set_market_enabled", "set_schedule", "set_capital"}

    for command in discord_bot.bot.tree.get_commands():
        payload = command.to_dict(discord_bot.bot.tree)
        if command.name in shared_state_commands:
            assert payload["integration_types"] == [0]
            assert payload["contexts"] == [0]
        else:
            assert payload["integration_types"] == [0, 1]
            assert payload["contexts"] == [0, 1, 2]

    ask_command = discord_bot.bot.tree.get_command("ask")
    assert ask_command is not None
    assert [parameter.name for parameter in ask_command.parameters] == ["question"]


def test_setup_hook_syncs_only_global_commands(monkeypatch: pytest.MonkeyPatch) -> None:
    sync_guilds: list[object | None] = []
    started_loops: list[str] = []
    startup_events: list[str] = []

    async def fake_sync(_tree, *, guild=None):
        startup_events.append("sync")
        sync_guilds.append(guild)
        return []

    def fake_start(loop, *args, **kwargs):
        del args, kwargs
        started_loops.append(loop.coro.__name__)
        startup_events.append(f"start:{loop.coro.__name__}")

    monkeypatch.setattr(discord_bot.app_commands.CommandTree, "sync", fake_sync)
    monkeypatch.setattr(discord_bot.tasks.Loop, "start", fake_start)

    asyncio.run(discord_bot.bot.setup_hook())

    assert sync_guilds == [None]
    assert set(started_loops) == {
        "scheduled_signal",
        "startup_inference_warmup",
        "service_heartbeat",
        "signal_now_job_resumer",
        "preopen_prepare",
        "daily_summary",
        "model_auto_deployment",
    }
    assert "artifact_backfill" not in started_loops
    assert startup_events[:3] == [
        "start:scheduled_signal",
        "start:service_heartbeat",
        "sync",
    ]


def test_postclose_artifact_maintenance_isolated_from_discord_cgroup() -> None:
    service = Path(
        "deploy/systemd/stockagent-discord-artifact-maintenance.service.in"
    ).read_text(encoding="utf-8")
    timer = Path(
        "deploy/systemd/stockagent-discord-artifact-maintenance.timer.in"
    ).read_text(encoding="utf-8")
    installer = Path("scripts/install_discord_bot_service.sh").read_text(
        encoding="utf-8"
    )

    assert "Type=oneshot" in service
    assert "CPUWeight=10" in service
    assert "IOWeight=10" in service
    assert "OOMScoreAdjust=500" in service
    assert "run_discord_artifact_maintenance.sh" in service
    assert "OnCalendar=Mon..Fri" in timer
    assert "RandomizedDelaySec=0" in timer
    assert "stockagent-discord-artifact-maintenance.timer" in installer

    gateway = Path(
        "deploy/systemd/stockagent-discord-bot.service.in"
    ).read_text(encoding="utf-8")
    assert "OOMScoreAdjust=-500" in gateway


def test_opening_watchdog_cannot_undercut_bounded_quote_io(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STOCKAGENT_OPENING_HOT_ATTEMPT_TIMEOUT_SECONDS", "5")
    monkeypatch.setenv("STOCKAGENT_OPENING_COLD_ATTEMPT_TIMEOUT_SECONDS", "5")

    assert discord_bot._opening_attempt_timeout_seconds(hot=True) == 60.0
    assert discord_bot._opening_attempt_timeout_seconds(hot=False) == 180.0

    monkeypatch.setenv("STOCKAGENT_OPENING_HOT_ATTEMPT_TIMEOUT_SECONDS", "90")
    assert discord_bot._opening_attempt_timeout_seconds(hot=True) == 90.0


def test_final_arm_is_process_scoped_and_requires_opening_source_prewarm(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    cfg = discord_bot._market_configs()["tw_day_trade_100m"]
    path = tmp_path / "preopen_readiness.json"
    monkeypatch.setattr(discord_bot, "_preopen_readiness_path", lambda: path)
    payload = {
        "markets": {
            cfg.market: {
                "final_arm": {
                    "status": "ready",
                    "run_id": discord_bot._BOT_RUN_ID,
                    "completed_at": "2026-08-27T08:55:00+08:00",
                    "live_latency": {
                        "panel_cache_hit": True,
                        "checkpoint_cache_hit": True,
                        "model_cache_hit": True,
                    },
                    "opening_source_prewarm": {
                        "ready": True,
                        "run_id": discord_bot._BOT_RUN_ID,
                        "source": "twse_tpex:mis",
                    },
                }
            }
        }
    }
    path.write_text(json.dumps(payload), encoding="utf-8")

    assert discord_bot._preopen_market_final_armed_for_session(
        cfg, "2026-08-27"
    )

    payload["markets"][cfg.market]["final_arm"]["run_id"] = "previous-process"
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert not discord_bot._preopen_market_final_armed_for_session(
        cfg, "2026-08-27"
    )

    payload["markets"][cfg.market]["final_arm"]["run_id"] = (
        discord_bot._BOT_RUN_ID
    )
    payload["markets"][cfg.market]["final_arm"].pop(
        "opening_source_prewarm"
    )
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert not discord_bot._preopen_market_final_armed_for_session(
        cfg, "2026-08-27"
    )


def test_day_trade_opening_quote_symbols_keep_only_alive_rows() -> None:
    assert discord_bot._day_trade_opening_quote_symbols(
        [
            {"symbol": "2330", "alive": True},
            {"symbol": "2317", "alive": False},
            {"symbol": "2330", "alive": True},
        ]
    ) == ["2330"]


def test_day_trade_opening_fallback_prices_align_to_alive_symbols() -> None:
    rows = [
        {"symbol": "2330", "alive": True, "current_price": 100.0},
        {"symbol": "1101", "alive": False, "current_price": 40.0},
        {"symbol": "2454", "alive": True, "current_price": 800.0},
    ]
    symbols = discord_bot._day_trade_opening_quote_symbols(rows)

    fallback = discord_bot._day_trade_opening_fallback_prices(rows, symbols)

    assert symbols == ["2330", "2454"]
    assert fallback.tolist() == [100.0, 800.0]

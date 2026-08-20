from __future__ import annotations

import asyncio

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
    assert "`tw_day_trade_multi_basis_projection_l1_gelu`" in guide


def test_multi_basis_day_trade_is_available_in_market_autocomplete() -> None:
    choices = asyncio.run(discord_bot.market_autocomplete(None, "multi_basis"))

    assert any(choice.value == "tw_day_trade_multi_basis" for choice in choices)


def test_all_three_day_trade_modes_share_the_0900_paper_execution_contract() -> None:
    configs = discord_bot._market_configs()
    markets = (
        "tw_day_trade_multi_basis",
        "tw_day_trade_100m",
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
        "preopen_prepare",
        "daily_summary",
        "artifact_backfill",
        "model_auto_deployment",
    }
    assert startup_events[:2] == ["start:scheduled_signal", "sync"]

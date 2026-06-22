from __future__ import annotations

import pytest

from stockagent.live.report_formatter import INVESTMENT_WARNING

discord = pytest.importorskip("discord")

from services.discord_bot import bot as discord_bot  # noqa: E402


def test_discord_page_size_and_top_n_floor_to_ten() -> None:
    assert discord_bot._page_size(1) == 10
    assert discord_bot._page_size(5) == 10
    assert discord_bot._page_size(None) == 20
    assert discord_bot._page_size(99) == 40

    assert discord_bot._top_n(1) == 10
    assert discord_bot._top_n(None) == 20


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

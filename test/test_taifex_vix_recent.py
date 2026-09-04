from __future__ import annotations

from datetime import date

from scripts.download_taifex_vix_recent import _parse_vix


def test_vix_parser_uses_next_verified_session() -> None:
    content = (
        "交易日期\t時間(時/分/秒/毫秒)\t臺指選擇權波動率指數\t收盤前1分鐘平均指數\r\n"
        "--------\t-------------------\t--------------------\t-------------------\r\n"
        "20260803\t13450000\t\t\t39.46\t\t39.47\r\n"
    ).encode("cp950")
    frame = _parse_vix(content, {date(2026, 8, 3): date(2026, 8, 4)})

    assert frame.loc[0, "taifex_vix"] == 39.46
    assert frame.loc[0, "preclose_1m_average_vix"] == 39.47
    assert frame.loc[0, "available_date"].date() == date(2026, 8, 4)

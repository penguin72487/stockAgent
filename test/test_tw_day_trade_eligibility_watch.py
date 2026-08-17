from __future__ import annotations

from datetime import date, datetime
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from scripts import fetch_tw_day_trade_eligibility_on_publish as watcher


TAIPEI = ZoneInfo("Asia/Taipei")


class _Response:
    def __init__(self, payload: object) -> None:
        self.content = json.dumps(payload, ensure_ascii=False).encode("utf-8")


def test_minimum_rule_date_switches_to_future_at_night() -> None:
    assert watcher._minimum_acceptable_rule_date(
        datetime(2026, 8, 17, 9, 0, tzinfo=TAIPEI)
    ) == date(2026, 8, 17)
    assert watcher._minimum_acceptable_rule_date(
        datetime(2026, 8, 17, 22, 29, 55, tzinfo=TAIPEI)
    ) == date(2026, 8, 18)


def test_twse_probe_uses_official_declared_session_date(monkeypatch) -> None:
    payload = [
        {
            "Date": "1150818",
            "Code": "2330",
            "Name": "台積電",
            "Suspension": "",
        },
        {
            "Date": "1150818",
            "Code": "2317",
            "Name": "鴻海",
            "Suspension": "Y",
        },
    ]
    monkeypatch.setattr(
        watcher,
        "_http_get",
        lambda *_args, **_kwargs: _Response(payload),
    )

    result = watcher._probe_twse(timeout=1)

    assert result["trading_date"] == date(2026, 8, 18)
    assert result["rows"] == 2


def test_download_command_is_exact_date_and_does_not_require_future_calendar(
    tmp_path: Path,
) -> None:
    command = watcher._download_command(
        live_root=tmp_path,
        trading_date=date(2026, 8, 18),
    )

    assert command[command.index("--end-date") + 1] == "2026-08-18"
    assert command[command.index("--same-session-rule-date") + 1] == "2026-08-18"
    assert "--require-taiex-session-calendar" not in command

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


def test_waiting_receipt_is_mutable_liveness_without_fake_run_receipt(
    tmp_path: Path,
) -> None:
    receipt = tmp_path / "latest.json"
    started = datetime(2026, 8, 17, 22, 29, 55, tzinfo=TAIPEI)

    payload = watcher._write_waiting_receipt(
        receipt,
        started=started,
        scheduled_at=datetime(2026, 8, 17, 22, 30, tzinfo=TAIPEI),
        minimum_date=date(2026, 8, 18),
        attempt_count=7,
        poll_interval_seconds=2.0,
        first_twse_observed_at=None,
        first_tpex_observed_at=None,
        both_sources_observed_at=None,
        last_error="PublicationPending: TWSE master is still stale",
        live_root=tmp_path / "live",
    )

    assert payload["status"] == "waiting_source"
    assert payload["attempt_count"] == 7
    assert json.loads(receipt.read_text(encoding="utf-8"))["last_error"].startswith(
        "PublicationPending"
    )
    assert not (tmp_path / "runs").exists()

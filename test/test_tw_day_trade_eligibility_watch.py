from __future__ import annotations

import argparse
from datetime import date, datetime
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from scripts import fetch_tw_day_trade_eligibility_on_publish as watcher


TAIPEI = ZoneInfo("Asia/Taipei")


class _Response:
    def __init__(
        self, payload: object, *, status_code: int = 200,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.content = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.status_code = status_code
        self.headers = headers or {}


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


def test_twse_conditional_poll_reuses_only_validated_body_and_rechecks(
    monkeypatch,
) -> None:
    stale = [{
        "Date": "1150817", "Code": "2330", "Name": "台積電", "Suspension": "",
    }]
    fresh = [{
        "Date": "1150818", "Code": "2330", "Name": "台積電", "Suspension": "",
    }]
    responses = iter((
        _Response(stale, headers={"ETag": 'W/"stale"'}),
        _Response(None, status_code=304),
        _Response(fresh, headers={"ETag": 'W/"fresh"'}),
        _Response(None, status_code=304),
        _Response(fresh, headers={"ETag": 'W/"fresh"'}),
    ))
    request_headers: list[tuple[str | None, str | None]] = []

    def get(*_args, **kwargs):
        request_headers.append((
            kwargs.get("conditional_etag"),
            kwargs.get("conditional_modified_since"),
        ))
        return next(responses)

    monkeypatch.setattr(watcher, "_http_get", get)
    parser = watcher._parse_twse_day_trade_openapi_payload
    validated = []

    def parse(*args):
        validated.append(args[1])
        return parser(*args)

    monkeypatch.setattr(watcher, "_parse_twse_day_trade_openapi_payload", parse)
    cache = watcher._TwseConditionalCache()
    args = {"timeout": 1, "minimum_date": date(2026, 8, 18), "cache": cache}
    old = watcher._probe_twse(**args)
    unchanged_old = watcher._probe_twse(**args)
    new = watcher._probe_twse(**args)
    unchanged_new = watcher._probe_twse(**args)
    cache.last_full_at -= watcher._TWSE_UNCONDITIONAL_RECHECK_SECONDS + 1
    rechecked = watcher._probe_twse(**args)

    assert old["trading_date"] == unchanged_old["trading_date"] == date(2026, 8, 17)
    assert (old["http_status"], unchanged_old["http_status"]) == (200, 304)
    assert new["trading_date"] == unchanged_new["trading_date"] == date(2026, 8, 18)
    assert rechecked["http_status"] == 200
    assert validated == [date(2026, 8, 18), date(2026, 8, 18)]
    assert request_headers == [
        (None, None),
        ('W/"stale"', None),
        ('W/"stale"', None),
        ('W/"fresh"', None),
        (None, None),
    ]


def test_twse_304_without_cached_valid_body_fails_closed(monkeypatch) -> None:
    monkeypatch.setattr(
        watcher, "_http_get", lambda *_args, **_kwargs: _Response(None, status_code=304)
    )
    with pytest.raises(watcher.PublicationPending, match="without a validated"):
        watcher._probe_twse(timeout=1, cache=watcher._TwseConditionalCache())
    with pytest.raises(watcher.PublicationPending, match="without a validated"):
        watcher._probe_twse(
            timeout=1,
            cache=watcher._TwseConditionalCache(
                result={"trading_date": date(2026, 8, 17)},
            ),
        )


def test_watcher_stale_once_receipts_record_transfer_without_claiming_ready(
    tmp_path: Path, monkeypatch,
) -> None:
    live_root = tmp_path / "live"
    live_root.mkdir()
    receipt = tmp_path / "latest.json"
    monkeypatch.setattr(watcher, "parse_args", lambda: argparse.Namespace(
        live_root=live_root,
        receipt=receipt,
        poll_interval_seconds=2.0,
        heartbeat_seconds=30.0,
        max_wait_seconds=0.0,
        request_timeout_seconds=1,
        once=True,
    ))
    monkeypatch.setattr(
        watcher, "_minimum_acceptable_rule_date", lambda _now: date(2026, 8, 18)
    )
    monkeypatch.setattr(watcher, "_probe_twse", lambda **_kwargs: {
        "trading_date": date(2026, 8, 17),
        "http_status": 200,
        "body_bytes": 86473,
    })
    assert watcher.main() == 75
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    assert payload["status"] == "publication_pending_timeout"
    assert payload["twse_transport"] == {
        "full_bodies": 1, "not_modified": 0, "body_bytes": 86473,
    }
    assert len(list((tmp_path / "runs").glob("*.json"))) == 1


def test_download_command_is_exact_date_and_verifies_prior_open_calendar(
    tmp_path: Path,
) -> None:
    command = watcher._download_command(
        live_root=tmp_path,
        trading_date=date(2026, 8, 18),
    )

    assert command[command.index("--end-date") + 1] == "2026-08-18"
    assert command[command.index("--same-session-rule-date") + 1] == "2026-08-18"
    assert "--require-taiex-session-calendar" in command


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
        twse_transport={"full_bodies": 1, "not_modified": 6, "body_bytes": 86473},
    )

    assert payload["status"] == "waiting_source"
    assert payload["attempt_count"] == 7
    assert payload["twse_transport"] == {
        "full_bodies": 1, "not_modified": 6, "body_bytes": 86473,
    }
    assert json.loads(receipt.read_text(encoding="utf-8"))["last_error"].startswith(
        "PublicationPending"
    )
    assert not (tmp_path / "runs").exists()


def test_verified_existing_rules_do_not_wait_for_unrelated_global_writer(
    tmp_path: Path, monkeypatch,
) -> None:
    live_root = tmp_path / "live"
    live_root.mkdir()
    for venue in ("twse", "tpex"):
        (live_root / f"{venue}_day_trade_eligibility.parquet").write_bytes(b"fixture")
    expected = {venue: {"covered": True} for venue in ("twse", "tpex")}
    monkeypatch.setattr(watcher, "_ready_coverage", lambda *_: expected)
    monkeypatch.setattr(
        watcher.fcntl, "flock",
        lambda *_: (_ for _ in ()).throw(AssertionError("no lock for verified reuse")),
    )
    timing_ms: dict[str, float] = {}
    assert watcher._ensure_exact_session_coverage(
        live_root, date(2026, 9, 23), timing_ms=timing_ms
    ) == (
        expected, True,
    )
    assert timing_ms["stable_existing_check"] >= 0
    assert timing_ms["total"] >= timing_ms["stable_existing_check"]
    assert "producer_lock_wait" not in timing_ms


def test_rule_replacement_during_read_cannot_bypass_write_lock(
    tmp_path: Path, monkeypatch,
) -> None:
    live_root = tmp_path / "live"
    live_root.mkdir()
    for venue in ("twse", "tpex"):
        (live_root / f"{venue}_day_trade_eligibility.parquet").write_bytes(b"old")

    def concurrent_change(*_args):
        (live_root / "tpex_day_trade_eligibility.parquet").write_bytes(b"newer")
        return {"twse": {"covered": True}, "tpex": {"covered": True}}

    monkeypatch.setattr(watcher, "_ready_coverage", concurrent_change)
    assert watcher._stable_existing_coverage(live_root, date(2026, 9, 23)) is None


def test_locked_rule_reuse_records_wait_and_check_without_download(
    tmp_path: Path, monkeypatch,
) -> None:
    live_root = tmp_path / "live"
    live_root.mkdir()
    expected = {venue: {"covered": True} for venue in ("twse", "tpex")}
    monkeypatch.setattr(watcher, "_stable_existing_coverage", lambda *_: None)
    monkeypatch.setattr(watcher, "_ready_coverage", lambda *_: expected)
    monkeypatch.setattr(
        watcher.subprocess, "run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no download")),
    )
    timing_ms: dict[str, float] = {}
    assert watcher._ensure_exact_session_coverage(
        live_root, date(2026, 9, 23), timing_ms=timing_ms
    ) == (expected, True)
    assert timing_ms["producer_lock_wait"] >= 0
    assert timing_ms["locked_existing_check"] >= 0
    assert "official_download" not in timing_ms

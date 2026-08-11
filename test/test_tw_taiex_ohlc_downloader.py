from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest
import requests

from downloader import download_tw_taiex_ohlc as taiex
from scripts.rebuild_tw_public_data_layer import (
    _cleanup_published_partials,
    _public_command,
    _run_market_history_stages,
    _taiex_command,
)


class _Response:
    def __init__(
        self,
        payload: object,
        *,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.content = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.status_code = status_code
        self.headers = headers or {"Content-Type": "application/json"}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


def _roc_date(iso_date: str) -> str:
    year, month, day = (int(part) for part in iso_date.split("-"))
    return f"{year - 1911:03d}/{month:02d}/{day:02d}"


def _payload(month: str, rows: list[tuple[str, float, float, float, float]]) -> dict:
    year, month_number = (int(part) for part in month.split("-"))
    return {
        "stat": "OK",
        "title": f"{year - 1911:03d}年{month_number:02d}月 發行量加權股價指數歷史資料",
        "fields": ["日期", "開盤指數", "最高指數", "最低指數", "收盤指數"],
        "data": [
            [
                _roc_date(row_date),
                f"{opening:,.2f}",
                f"{highest:,.2f}",
                f"{lowest:,.2f}",
                f"{closing:,.2f}",
            ]
            for row_date, opening, highest, lowest, closing in rows
        ],
    }


def _args(
    output_dir: Path,
    *,
    mode: str = "rebuild",
    start: str = "1999-01-05",
    end: str = "1999-02-05",
    resume: bool = True,
) -> object:
    argv = [
        "--mode",
        mode,
        "--output-dir",
        str(output_dir),
        "--start-date",
        start,
        "--end-date",
        end,
        "--workers",
        "1",
        "--retries",
        "0",
        "--request-interval",
        "0.1",
        "--no-progress",
    ]
    argv.append("--resume" if resume else "--no-resume")
    return taiex.parse_args(argv)


def test_cli_defaults_to_resume_and_shared_tw_public_rate_policy() -> None:
    args = taiex.parse_args([])

    assert args.resume is True
    assert args.start_date == "earliest"
    assert args.request_interval is None
    assert taiex.resolve_request_interval("tw_public", None) == pytest.approx(0.1)
    assert taiex.parse_args(["--no-resume"]).resume is False


def test_parse_month_payload_normalizes_roc_dates_and_clips_end_date() -> None:
    payload = _payload(
        "1999-01",
        [
            ("1999-01-05", 6_100.0, 6_120.0, 6_090.0, 6_110.0),
            ("1999-01-08", 6_110.0, 6_140.0, 6_100.0, 6_130.0),
        ],
    )

    frame = taiex._parse_month_payload(
        json.dumps(payload).encode(),
        "1999-01",
        range_start=taiex.date(1999, 1, 5),
        range_end=taiex.date(1999, 1, 6),
    )

    assert frame.schema["date"] == pl.Date
    assert frame.height == 1
    assert frame.row(0, named=True) == {
        "date": taiex.date(1999, 1, 5),
        "opening_index": 6100.0,
        "highest_index": 6120.0,
        "lowest_index": 6090.0,
        "closing_index": 6110.0,
    }


@pytest.mark.parametrize(
    "payload,month,error",
    [
        (
            _payload(
                "1999-02",
                [("1999-02-01", 10.0, 11.0, 9.0, 10.5)],
            ),
            "1999-01",
            "month mismatch",
        ),
        (
            {
                "stat": "OK",
                "title": "088年01月 發行量加權股價指數歷史資料",
                "fields": ["日期", "開盤指數", "最高指數", "最低指數", "收盤指數"],
                "data": [],
            },
            "1999-01",
            "no TAIEX rows",
        ),
        (
            _payload(
                "1999-01",
                [("1999-01-05", 10.0, 9.0, 8.0, 10.5)],
            ),
            "1999-01",
            "invalid TAIEX OHLC high",
        ),
    ],
)
def test_month_payload_rejects_wrong_month_empty_and_invalid_ohlc(
    payload: object,
    month: str,
    error: str,
) -> None:
    with pytest.raises(taiex.MonthPayloadError, match=error):
        taiex._parse_month_payload(
            json.dumps(payload).encode(),
            month,
            range_start=taiex.date(1999, 1, 1),
            range_end=taiex.date(1999, 2, 28),
        )


def test_failed_rebuild_persists_partial_and_resume_only_requests_unresolved(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_dir = tmp_path / "public"
    canonical = taiex._attach_provenance(
        pl.DataFrame(
            {
                "date": [taiex.date(1998, 12, 31)],
                "opening_index": [6_000.0],
                "highest_index": [6_010.0],
                "lowest_index": [5_990.0],
                "closing_index": [6_005.0],
            }
        ),
        month="1998-12",
        url="existing",
        fetched_at="existing",
    )
    canonical_path = taiex._canonical_path(output_dir)
    taiex._write_parquet_atomic(canonical_path, canonical)
    original_bytes = canonical_path.read_bytes()

    january = _payload(
        "1999-01",
        [("1999-01-05", 6_100.0, 6_120.0, 6_090.0, 6_110.0)],
    )
    february = _payload(
        "1999-02",
        [("1999-02-01", 6_110.0, 6_130.0, 6_100.0, 6_120.0)],
    )
    first_calls: list[str] = []

    def first_request(url: str, _args: object) -> _Response:
        month = "1999-01" if "date=19990101" in url else "1999-02"
        first_calls.append(month)
        if month == "1999-02":
            raise requests.ConnectionError("temporary failure")
        return _Response(january)

    monkeypatch.setattr(taiex, "_request_once", first_request)
    assert taiex._run(_args(output_dir)) == 1
    assert sorted(first_calls) == ["1999-01", "1999-02"]
    assert canonical_path.read_bytes() == original_bytes

    partial = pl.read_parquet(taiex._partial_path(output_dir))
    assert partial.get_column("date").to_list() == [taiex.date(1999, 1, 5)]
    journal = taiex._journal_path(output_dir).read_text(encoding="utf-8")
    assert '"status":"data"' in journal
    assert '"status":"failed"' in journal
    failed_summary = json.loads(
        taiex._summary_path(output_dir).read_text(encoding="utf-8")
    )
    assert failed_summary["coverage_complete"] is False
    assert failed_summary["replacement_promoted"] is False

    second_calls: list[str] = []

    def second_request(url: str, _args: object) -> _Response:
        month = "1999-01" if "date=19990101" in url else "1999-02"
        second_calls.append(month)
        if month != "1999-02":
            raise AssertionError(f"resolved month was requested again: {month}")
        return _Response(february)

    monkeypatch.setattr(taiex, "_request_once", second_request)
    assert taiex._run(_args(output_dir)) == 0
    assert second_calls == ["1999-02"]
    output = pl.read_parquet(canonical_path).sort("date")
    assert output.get_column("date").to_list() == [
        taiex.date(1999, 1, 5),
        taiex.date(1999, 2, 1),
    ]
    summary = json.loads(taiex._summary_path(output_dir).read_text(encoding="utf-8"))
    assert summary["coverage_complete"] is True
    assert summary["baseline_established"] is True
    assert summary["failed_month_count"] == 0
    assert summary["output_receipt"]["sha256"] == taiex._file_receipt(canonical_path)["sha256"]


def test_no_resume_refetches_every_month(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_dir = tmp_path / "public"
    responses = {
        "1999-01": _payload(
            "1999-01",
            [("1999-01-05", 10.0, 11.0, 9.0, 10.5)],
        ),
        "1999-02": _payload(
            "1999-02",
            [("1999-02-01", 10.5, 11.5, 10.0, 11.0)],
        ),
    }

    def month_from_url(url: str) -> str:
        return "1999-01" if "date=19990101" in url else "1999-02"

    monkeypatch.setattr(
        taiex,
        "_request_once",
        lambda url, _args: _Response(responses[month_from_url(url)]),
    )
    assert taiex._run(_args(output_dir)) == 0

    calls: list[str] = []

    def fresh_request(url: str, _args: object) -> _Response:
        month = month_from_url(url)
        calls.append(month)
        return _Response(responses[month])

    monkeypatch.setattr(taiex, "_request_once", fresh_request)
    assert taiex._run(_args(output_dir, resume=False)) == 0
    assert sorted(calls) == ["1999-01", "1999-02"]
    latest = taiex._load_journal_latest(taiex._journal_path(output_dir))
    assert set(latest) == {"1999-01", "1999-02"}


def test_repair_rehydrates_removed_partial_from_published_canonical_and_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_dir = tmp_path / "public"
    payload = _payload(
        "1999-01",
        [("1999-01-05", 10.0, 11.0, 9.0, 10.5)],
    )
    monkeypatch.setattr(
        taiex,
        "_request_once",
        lambda *_args: _Response(payload),
    )
    assert taiex._run(_args(output_dir, end="1999-01-31")) == 0
    taiex._partial_path(output_dir).unlink()

    monkeypatch.setattr(
        taiex,
        "_request_once",
        lambda *_args: (_ for _ in ()).throw(
            AssertionError("published month with matching journal checksum is resolved")
        ),
    )
    repair_args = _args(
        output_dir,
        mode="repair",
        end="1999-01-31",
    )
    assert taiex._run(repair_args) == 0
    summary = json.loads(taiex._summary_path(output_dir).read_text(encoding="utf-8"))
    assert summary["network_requested_count"] == 0


def test_raw_resume_rejects_stale_wrong_month_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_dir = tmp_path / "public"
    raw_path = taiex._raw_path(output_dir, "1999-01")
    taiex._atomic_write_bytes(
        raw_path,
        json.dumps(
            _payload(
                "2026-07",
                [("2026-07-01", 10.0, 11.0, 9.0, 10.5)],
            )
        ).encode(),
    )
    calls: list[str] = []

    def request(url: str, _args: object) -> _Response:
        calls.append(url)
        return _Response(
            _payload(
                "1999-01",
                [("1999-01-05", 10.0, 11.0, 9.0, 10.5)],
            )
        )

    monkeypatch.setattr(taiex, "_request_once", request)
    args = _args(output_dir, end="1999-01-31")
    assert taiex._run(args) == 0
    assert len(calls) == 1


def test_valid_atomic_raw_receipt_bootstraps_without_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_dir = tmp_path / "public"
    raw_path = taiex._raw_path(output_dir, "1999-01")
    taiex._atomic_write_bytes(
        raw_path,
        json.dumps(
            _payload(
                "1999-01",
                [("1999-01-05", 10.0, 11.0, 9.0, 10.5)],
            )
        ).encode(),
    )
    monkeypatch.setattr(
        taiex,
        "_request_once",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("valid raw month must not be downloaded again")
        ),
    )

    assert taiex._run(_args(output_dir, end="1999-01-31")) == 0
    output = pl.read_parquet(taiex._canonical_path(output_dir))
    assert output.get_column("date").to_list() == [taiex.date(1999, 1, 5)]
    summary = json.loads(taiex._summary_path(output_dir).read_text(encoding="utf-8"))
    assert summary["raw_resumed_month_count"] == 1
    assert summary["network_requested_count"] == 0


def test_security_redirect_defers_global_provider_and_remains_retryable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = iter(
        [
            _Response("FOR SECURITY REASONS", status_code=307),
            _Response(
                _payload(
                    "1999-01",
                    [("1999-01-05", 10.0, 11.0, 9.0, 10.5)],
                )
            ),
        ]
    )
    monkeypatch.setattr(taiex, "_request_once", lambda *_args: next(responses))
    deferred: list[float] = []
    monkeypatch.setattr(
        taiex,
        "_global_rate_limiter",
        lambda: SimpleNamespace(defer=lambda seconds: deferred.append(seconds)),
    )
    monkeypatch.setattr(taiex.time, "sleep", lambda _seconds: None)
    args = _args(tmp_path, end="1999-01-31")
    args.retries = 1

    result = taiex._download_month(
        "1999-01",
        args,
        tmp_path,
        taiex.date(1999, 1, 5),
        taiex.date(1999, 1, 31),
    )

    assert result.error is None
    assert result.response_attempts == 2
    assert deferred == [pytest.approx(taiex.WAF_COOLDOWN_SECONDS)]


def test_daily_refreshes_every_month_intersecting_recent_calendar_overlap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_dir = tmp_path / "public"
    responses = {
        "1999-01": _payload(
            "1999-01",
            [("1999-01-05", 10.0, 11.0, 9.0, 10.5)],
        ),
        "1999-02": _payload(
            "1999-02",
            [("1999-02-01", 10.5, 11.5, 10.0, 11.0)],
        ),
    }

    def month_from_url(url: str) -> str:
        return "1999-01" if "date=19990101" in url else "1999-02"

    monkeypatch.setattr(
        taiex,
        "_request_once",
        lambda url, _args: _Response(responses[month_from_url(url)]),
    )
    assert taiex._run(_args(output_dir)) == 0

    calls: list[str] = []

    def daily_request(url: str, _args: object) -> _Response:
        month = month_from_url(url)
        calls.append(month)
        return _Response(responses[month])

    monkeypatch.setattr(taiex, "_request_once", daily_request)
    daily_args = _args(output_dir, mode="daily")
    daily_args.daily_overlap_days = 7  # 1999-01-30 through 1999-02-05
    assert taiex._run(daily_args) == 0
    assert sorted(calls) == ["1999-01", "1999-02"]


def test_jsonl_loader_tolerates_only_torn_final_record(tmp_path: Path) -> None:
    path = taiex._journal_path(tmp_path)
    result = taiex.MonthResult(
        month="1999-01",
        url="u",
        frame=taiex._attach_provenance(
            pl.DataFrame(
                {
                    "date": [taiex.date(1999, 1, 5)],
                    "opening_index": [10.0],
                    "highest_index": [11.0],
                    "lowest_index": [9.0],
                    "closing_index": [10.5],
                }
            ),
            month="1999-01",
            url="u",
            fetched_at="t",
        ),
    )
    taiex._append_jsonl(path, taiex._event_for_result(result, status="data", source="test"))
    with path.open("ab") as handle:
        handle.write(b'{"torn":')
    assert set(taiex._load_journal_latest(path)) == {"1999-01"}

    path.write_text(
        '{"broken":\n' + json.dumps(taiex._event_for_result(result, status="data", source="test")) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="non-terminal"):
        taiex._load_journal_latest(path)


def test_daily_requires_established_baseline(tmp_path: Path) -> None:
    args = _args(
        tmp_path,
        mode="daily",
        start="1999-01-05",
        end="1999-01-31",
    )
    assert taiex._run(args) == 1
    summary = json.loads(taiex._summary_path(tmp_path).read_text(encoding="utf-8"))
    assert "requires an established rebuild/repair" in summary["fatal_error"]


def test_taiex_outer_command_and_partial_cleanup_are_source_scoped(tmp_path: Path) -> None:
    args = SimpleNamespace(
        mode="daily",
        taiex_start_date="1999-01-05",
        end_date="2026-07-11",
        public_workers=3,
        timeout=40,
        retries=5,
        retry_backoff=2.0,
        daily_overlap_days=7,
        resume=True,
        request_interval=0.9,
        skip_raw=True,
    )
    command = _taiex_command(args, tmp_path / "public")

    assert command[1] == "downloader/download_tw_taiex_ohlc.py"
    assert command[command.index("--mode") + 1] == "daily"
    assert command[command.index("--request-interval") + 1] == "0.9"
    assert "--resume" in command
    assert "--skip-raw" in command

    partial_dir = tmp_path / "public" / "state" / "partials"
    partial_dir.mkdir(parents=True)
    taiex_partial = partial_dir / "twse_taiex_ohlc.key.parquet"
    public_partial = partial_dir / "twse_daily_ohlcv.key.parquet"
    taiex_partial.write_bytes(b"keep")
    public_partial.write_bytes(b"remove")
    _cleanup_published_partials(
        partial_dir,
        preserve_prefixes=("twse_taiex_ohlc.",),
    )
    assert taiex_partial.read_bytes() == b"keep"
    assert not public_partial.exists()


def test_outer_runs_taiex_before_strict_public_sources(tmp_path: Path) -> None:
    args = SimpleNamespace(
        operation="from-zero",
        mode="rebuild",
        public_start_date="earliest",
        taiex_start_date="1999-01-05",
        end_date="2026-07-11",
        public_workers=2,
        date_workers=8,
        timeout=30,
        retries=4,
        retry_backoff=1.0,
        sleep=0.0,
        flush_every_dates=250,
        daily_overlap_days=7,
        empty_recheck_days=30,
        resume=True,
        request_interval=None,
        skip_raw=False,
        skip_public=False,
        skip_taiex_ohlc=False,
        dry_run=True,
    )
    calls: list[tuple[str, list[str]]] = []

    class RecordingRunner:
        def run(self, name, command, *, outputs, allow_resume=True):
            calls.append((name, command))

    _run_market_history_stages(args, RecordingRunner(), tmp_path / "public")

    assert [name for name, _ in calls] == [
        "twse_taiex_ohlc",
        "official_public_sources",
    ]
    public = calls[1][1]
    assert public == _public_command(args, tmp_path / "public")
    assert "--require-taiex-session-calendar" in public
    assert "--resume" in public


def test_outer_skip_taiex_still_requires_existing_verified_calendar(tmp_path: Path) -> None:
    args = SimpleNamespace(
        operation="repair",
        mode="repair",
        public_start_date="earliest",
        taiex_start_date="1999-01-05",
        end_date="2026-07-11",
        public_workers=2,
        date_workers=8,
        timeout=30,
        retries=4,
        retry_backoff=1.0,
        sleep=0.0,
        flush_every_dates=250,
        daily_overlap_days=7,
        empty_recheck_days=30,
        resume=False,
        request_interval=0.1,
        skip_raw=True,
        skip_public=False,
        skip_taiex_ohlc=True,
        dry_run=True,
    )
    calls: list[tuple[str, list[str]]] = []

    class RecordingRunner:
        def run(self, name, command, *, outputs, allow_resume=True):
            calls.append((name, command))

    _run_market_history_stages(args, RecordingRunner(), tmp_path / "public")

    assert [name for name, _ in calls] == ["official_public_sources"]
    assert "--require-taiex-session-calendar" in calls[0][1]
    assert "--no-resume" in calls[0][1]

from __future__ import annotations

from urllib.error import URLError

import json
from pathlib import Path

from downloader.http_transport import HttpResponse

from downloader import download_fred_crypto_macro_vintages as fred


class _Response:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self) -> bytes:
        return b'{"observations": []}'


def test_fred_retry_uses_shared_bounded_backoff_contract(monkeypatch) -> None:
    attempts = 0
    delays: list[tuple[int, float, float]] = []

    def fake_urlopen(_request, *, timeout: int):
        nonlocal attempts
        assert timeout == 60
        attempts += 1
        if attempts == 1:
            raise URLError("temporary fixture")
        return _Response()

    def fake_retry(attempt: int, *, base: float, cap: float) -> float:
        delays.append((attempt, base, cap))
        return 0.0

    monkeypatch.setattr(fred, "urlopen", fake_urlopen)
    monkeypatch.setattr(fred, "retry_delay_seconds", fake_retry)
    monkeypatch.setattr(fred.time, "sleep", lambda _seconds: None)

    payload = fred._fetch_series(
        "DGS10",
        api_key="fixture",
        start_date="2026-01-01",
        end_date="2026-01-02",
        realtime_start="2026-01-01",
        realtime_end="2026-01-02",
        max_retries=1,
    )

    assert payload == b'{"observations": []}'
    assert delays == [(1, 1.0, 30.0)]


def test_fred_recognizes_structured_alfred_gap_case_insensitively() -> None:
    body = json.dumps(
        {"error_message": "The series DOES NOT EXIST in ALFRED but is in FRED"}
    ).encode()

    class Transport:
        def request_bytes(self, *_args, **_kwargs) -> HttpResponse:
            return HttpResponse(400, body, {}, 1)

    payload = fred._fetch_series(
        "DGS10",
        api_key="fixture",
        start_date="2000-01-01",
        end_date="2001-01-01",
        realtime_start="2000-01-01",
        realtime_end="2001-01-01",
        max_retries=0,
        transport=Transport(),  # type: ignore[arg-type]
    )

    assert json.loads(payload)["observations"] == []
    assert "point_in_time_gap" in json.loads(payload)


def test_fred_closed_window_cache_requires_hash_verified_raw_payload(
    tmp_path: Path,
) -> None:
    window = fred.FredWindow("DGS10", "2000-01-01", "2001-01-01")
    raw = b'{"observations": []}'

    class Transport:
        def request_bytes(self, *_args, **_kwargs) -> HttpResponse:
            return HttpResponse(200, raw, {}, 1)

    fetched = fred._fetch_window(
        tmp_path,
        window,
        api_key="fixture",
        observation_start="2000-01-01",
        observation_end="2026-01-01",
        max_retries=0,
        transport=Transport(),  # type: ignore[arg-type]
    )
    cached = fred._read_cached_window(
        tmp_path,
        window,
        observation_start="2000-01-01",
        end_date="2026-01-01",
        recheck_days=30,
        now=fred.datetime.now(fred.timezone.utc),
    )

    assert fetched.status == "complete"
    assert cached is not None
    assert cached.status == "cached_verified"
    assert cached.retrieved_at_utc == fetched.retrieved_at_utc

    Path(str(fetched.raw_path)).write_bytes(b"corrupt")
    assert (
        fred._read_cached_window(
            tmp_path,
            window,
            observation_start="2000-01-01",
            end_date="2026-01-01",
            recheck_days=30,
            now=fred.datetime.now(fred.timezone.utc),
        )
        is None
    )


def test_fred_window_failure_publishes_failure_receipt(tmp_path: Path) -> None:
    window = fred.FredWindow("DGS10", "2020-01-01", "2021-01-01")

    class Transport:
        def request_bytes(self, *_args, **_kwargs) -> HttpResponse:
            raise RuntimeError("fixture transport failure")

    result = fred._fetch_window(
        tmp_path,
        window,
        api_key="fixture",
        observation_start="2020-01-01",
        observation_end="2021-01-01",
        max_retries=0,
        transport=Transport(),  # type: ignore[arg-type]
    )

    receipt = json.loads(fred._window_receipt_path(tmp_path, window).read_text())
    assert result.status == "failed"
    assert receipt["status"] == "failed"
    assert "fixture transport failure" in receipt["error"]

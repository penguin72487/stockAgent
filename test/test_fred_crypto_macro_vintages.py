from __future__ import annotations

from urllib.error import URLError

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

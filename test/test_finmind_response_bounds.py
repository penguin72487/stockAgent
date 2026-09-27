from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import traceback
from typing import Any

import pytest
import requests

from downloader import download_finmind_complement as complement


DATASET = "TaiwanStockPrice"
TOKEN = "private-test-token"


class Limiter:
    def __init__(self) -> None:
        self.waits = 0
        self.deferrals: list[int] = []

    def wait(self) -> None:
        self.waits += 1

    def defer(self, seconds: int) -> None:
        self.deferrals.append(seconds)


class Response:
    def __init__(self, chunks: list[bytes | Exception], *, status: int = 200,
                 headers: dict[str, str] | None = None) -> None:
        self.status_code = status
        self.headers = headers or {}
        self.chunks = chunks
        self.yielded = 0
        self.json_calls = 0
        self.closes = 0
        self.chunk_sizes: list[int] = []

    def iter_content(self, *, chunk_size: int):
        self.chunk_sizes.append(chunk_size)
        for chunk in self.chunks:
            self.yielded += 1
            if isinstance(chunk, Exception):
                raise chunk
            yield chunk

    def json(self) -> Any:
        self.json_calls += 1
        return json.loads(b"".join(self.chunks))

    def close(self) -> None:
        self.closes += 1


class Session:
    def __init__(self, response: Response | Exception) -> None:
        self.response = response
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def get(self, endpoint: str, **kwargs: Any) -> Response:
        self.calls.append((endpoint, kwargs))
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


def assert_one_request(root: Path, session: Session, limiter: Limiter) -> None:
    assert len(session.calls) == limiter.waits == 1
    with sqlite3.connect(root / "request_traffic.sqlite3") as connection:
        assert connection.execute("SELECT dataset FROM requests").fetchall() == [(DATASET,)]
    kwargs = session.calls[0][1]
    assert kwargs["headers"] == {"Authorization": f"Bearer {TOKEN}"}
    assert TOKEN not in str(kwargs["params"])
    assert kwargs["timeout"] == (10, 90)


@pytest.mark.parametrize("content_length", [None, "1", "999999999"])
def test_bounded_success_counts_decoded_bytes_not_content_length(
    tmp_path: Path, content_length: str | None,
) -> None:
    rows = [{"date": "2026-09-25", "stock_id": "2330"}]
    body = json.dumps({"status": 200, "msg": "success", "data": rows}).encode()
    headers = {"Content-Encoding": "gzip"}
    if content_length is not None:
        headers["Content-Length"] = content_length
    response = Response([body[:20], b"", body[20:]], headers=headers)
    session, limiter = Session(response), Limiter()

    assert complement._fetch_rows(
        session, limiter, tmp_path, DATASET, TOKEN, {"dataset": DATASET},
        max_response_bytes=len(body),
    ) == rows
    assert response.closes == 1
    assert response.json_calls == 0
    assert session.calls[0][1]["stream"] is True
    assert_one_request(tmp_path, session, limiter)


def test_size_limit_stops_before_buffer_growth_and_further_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    buffered_sizes: list[int] = []

    class CheckedBuffer(bytearray):
        def extend(self, chunk: bytes) -> None:
            assert len(self) + len(chunk) <= 10
            super().extend(chunk)
            buffered_sizes.append(len(self))

    monkeypatch.setattr(complement, "bytearray", CheckedBuffer, raising=False)
    response = Response([b"12345678", b"9012", AssertionError("must not drain")],
                        headers={"Content-Length": "1", "Content-Encoding": "gzip"})
    session, limiter = Session(response), Limiter()
    with pytest.raises(complement.SourceError) as failure:
        complement._fetch_rows(session, limiter, tmp_path, DATASET, TOKEN, {},
                               max_response_bytes=10)

    assert failure.value.code == "response_size_limit"
    assert failure.value.retry_after == 0
    assert buffered_sizes == [8]
    assert response.yielded == 2
    assert response.closes == 1
    assert not limiter.deferrals
    assert_one_request(tmp_path, session, limiter)


def test_invalid_json_closes_without_exposing_response_text(tmp_path: Path) -> None:
    response = Response([f"private body {TOKEN}".encode()])
    session, limiter = Session(response), Limiter()
    with pytest.raises(complement.SourceError) as failure:
        complement._fetch_rows(session, limiter, tmp_path, DATASET, TOKEN, {},
                               max_response_bytes=1024)

    assert failure.value.code == "invalid_json"
    assert TOKEN not in "".join(traceback.format_exception(failure.value))
    assert response.closes == 1
    assert_one_request(tmp_path, session, limiter)


@pytest.mark.parametrize("status,headers,code,deferrals", [
    (429, {"Retry-After": "123"}, "rate_limited", [123]),
    (402, {"Retry-After": "invalid"}, "rate_limited", [3600]),
    (401, {}, "invalid_token", []),
    (500, {}, "http_500", []),
])
def test_http_failures_close_without_reading_body(
    tmp_path: Path, status: int, headers: dict[str, str], code: str, deferrals: list[int],
) -> None:
    response = Response([AssertionError("error body must not load")],
                        status=status, headers=headers)
    session, limiter = Session(response), Limiter()
    with pytest.raises(complement.SourceError) as failure:
        complement._fetch_rows(session, limiter, tmp_path, DATASET, TOKEN, {},
                               max_response_bytes=10)

    assert failure.value.code == code
    assert limiter.deferrals == deferrals
    assert response.closes == 1
    assert response.yielded == response.json_calls == 0
    assert_one_request(tmp_path, session, limiter)


@pytest.mark.parametrize("status,message,code,deferrals", [
    (400, "Your level is register. Please update your level", "not_entitled", []),
    (403, "ip banned", "ip_banned", [1800]),
])
def test_error_json_keeps_classification_and_closes(
    tmp_path: Path, status: int, message: str, code: str, deferrals: list[int],
) -> None:
    response = Response([json.dumps({"msg": message}).encode()], status=status)
    session, limiter = Session(response), Limiter()
    with pytest.raises(complement.SourceError) as failure:
        complement._fetch_rows(session, limiter, tmp_path, DATASET, TOKEN, {},
                               max_response_bytes=1024)

    assert failure.value.code == code
    assert limiter.deferrals == deferrals
    assert response.closes == 1
    assert response.json_calls == 0
    assert_one_request(tmp_path, session, limiter)


@pytest.mark.parametrize("during_stream", [False, True])
def test_timeout_is_safe_and_request_counted_once(tmp_path: Path, during_stream: bool) -> None:
    timeout = requests.ReadTimeout(f"private provider text {TOKEN}")
    response = Response([b"{", timeout]) if during_stream else timeout
    session, limiter = Session(response), Limiter()
    with pytest.raises(complement.SourceError) as failure:
        complement._fetch_rows(session, limiter, tmp_path, DATASET, TOKEN, {},
                               max_response_bytes=1024)

    assert failure.value.code == "ReadTimeout"
    assert TOKEN not in "".join(traceback.format_exception(failure.value))
    if isinstance(response, Response):
        assert response.closes == 1
    assert_one_request(tmp_path, session, limiter)


def test_omitted_limit_preserves_nonstreaming_request(tmp_path: Path) -> None:
    response = Response([b'{"status":200,"msg":"success","data":[]}'])
    session, limiter = Session(response), Limiter()
    assert complement._fetch_rows(session, limiter, tmp_path, DATASET, TOKEN, {}) == []
    assert "stream" not in session.calls[0][1]
    assert response.json_calls == 1
    assert response.yielded == 0
    assert_one_request(tmp_path, session, limiter)


@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
def test_invalid_limit_does_not_consume_a_request(tmp_path: Path, limit: Any) -> None:
    session, limiter = Session(Response([])), Limiter()
    with pytest.raises(ValueError, match="positive integer"):
        complement._fetch_rows(session, limiter, tmp_path, DATASET, TOKEN, {},
                               max_response_bytes=limit)
    assert not session.calls
    assert limiter.waits == 0
    assert not (tmp_path / "request_traffic.sqlite3").exists()

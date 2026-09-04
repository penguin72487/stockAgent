from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
import json
from pathlib import Path
import shlex
import subprocess
from urllib.error import HTTPError, URLError

import polars as pl
import pytest

from downloader.artifact_io import (
    atomic_write_bytes,
    atomic_write_json,
    atomic_write_parquet,
    sha256_bytes,
    sha256_file,
)
from downloader.http_transport import (
    HttpRequestPolicy,
    HttpStatusError,
    ResilientHttpTransport,
)
from scripts.write_downloader_step_receipt import write_receipt


class _Limiter:
    def __init__(self) -> None:
        self.waits: list[float] = []
        self.defers: list[float] = []

    def wait(self, cost: float = 1.0) -> None:
        self.waits.append(cost)

    def defer(self, seconds: float) -> None:
        self.defers.append(seconds)


class _Response:
    status = 200
    headers = {"content-type": "application/json"}

    def __init__(self, body: bytes) -> None:
        self._body = body

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self) -> bytes:
        return self._body


def test_atomic_artifacts_are_complete_under_same_process_thread_contention(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.bin"
    payloads = [bytes([index]) * 100_000 for index in range(16)]
    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(lambda payload: atomic_write_bytes(path, payload), payloads))

    assert path.read_bytes() in payloads
    assert not list(tmp_path.glob(".*.tmp"))

    atomic_write_json(tmp_path / "receipt.json", {"state": "complete"})
    assert json.loads((tmp_path / "receipt.json").read_text()) == {"state": "complete"}


def test_atomic_parquet_and_streaming_hash_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "fixture.parquet"
    frame = pl.DataFrame({"timestamp": [1, 2], "value": [3.0, 4.0]})
    atomic_write_parquet(
        path,
        frame,
        compression="zstd",
        write_statistics=True,
        row_group_size=1,
    )

    assert pl.read_parquet(path).equals(frame)
    assert sha256_file(path) == sha256_bytes(path.read_bytes())


def test_http_transport_retries_transient_network_error_with_one_shared_limiter() -> (
    None
):
    attempts = 0
    sleeps: list[float] = []
    limiter = _Limiter()

    def opener(_request, *, timeout: float):
        nonlocal attempts
        assert timeout == 12
        attempts += 1
        if attempts == 1:
            raise URLError("temporary")
        return _Response(b"ok")

    transport = ResilientHttpTransport(
        HttpRequestPolicy(
            provider="fixture",
            timeout_seconds=12,
            max_retries=1,
            retry_base_seconds=0.25,
        ),
        limiter=limiter,  # type: ignore[arg-type]
        opener=opener,
        sleeper=sleeps.append,
    )
    response = transport.request_bytes("https://example.test/data?api_key=secret")

    assert response.body == b"ok"
    assert response.attempts == 2
    assert limiter.waits == [1.0, 1.0]
    assert sleeps == [0.5]


def test_http_transport_never_exposes_query_credentials_in_error() -> None:
    limiter = _Limiter()

    def opener(request, *, timeout: float):
        del timeout
        raise HTTPError(
            request.full_url,
            400,
            "bad request",
            {},
            BytesIO(b"invalid https://example.test/data?api_key=secret"),
        )

    transport = ResilientHttpTransport(
        HttpRequestPolicy(provider="fixture", max_retries=0),
        limiter=limiter,  # type: ignore[arg-type]
        opener=opener,
    )
    with pytest.raises(HttpStatusError) as caught:
        transport.request_bytes("https://example.test/data?api_key=secret")

    assert "secret" not in str(caught.value)
    assert str(caught.value).startswith("HTTP 400 for https://example.test/data")


def test_http_retry_after_defers_shared_bucket_without_double_sleep() -> None:
    attempts = 0
    sleeps: list[float] = []
    limiter = _Limiter()

    def opener(request, *, timeout: float):
        nonlocal attempts
        del timeout
        attempts += 1
        if attempts == 1:
            raise HTTPError(
                request.full_url,
                429,
                "rate limited",
                {"Retry-After": "7"},
                BytesIO(b"retry later"),
            )
        return _Response(b"ok")

    transport = ResilientHttpTransport(
        HttpRequestPolicy(
            provider="fixture",
            max_retries=1,
            retry_base_seconds=0.25,
        ),
        limiter=limiter,  # type: ignore[arg-type]
        opener=opener,
        sleeper=sleeps.append,
    )

    assert transport.request_bytes("https://example.test/data").body == b"ok"
    assert limiter.waits == [1.0, 1.0]
    assert limiter.defers == [7.0]
    assert sleeps == []


def test_scheduler_step_receipt_omits_commands_and_publishes_latest(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "run"
    latest_dir = tmp_path / "latest"
    payload = write_receipt(
        receipt_dir=run_dir,
        latest_dir=latest_dir,
        run_id="run-1",
        run_mode="once",
        step="fred_macro",
        state="failed",
        started_epoch=1_700_000_000,
        exit_code=7,
        elapsed_seconds=12,
        runner_pid=123,
    )

    persisted = json.loads((run_dir / "fred_macro.json").read_text())
    assert persisted == json.loads((latest_dir / "fred_macro.json").read_text())
    assert persisted == payload
    assert persisted["exit_code"] == 7
    assert persisted["command_recorded"] is False
    assert "command" not in persisted


def test_daily_runner_records_real_failed_exit_code(tmp_path: Path) -> None:
    receipt_dir = tmp_path / "run"
    latest_dir = tmp_path / "latest"
    command = "\n".join(
        (
            "set -euo pipefail",
            "export RUN_ID=fixture-run RUN_MODE=once FAIL_FAST=0 TEE_LOG=0",
            f"export STEP_RECEIPT_DIR={shlex.quote(str(receipt_dir))}",
            f"export STEP_RECEIPT_LATEST_DIR={shlex.quote(str(latest_dir))}",
            "source downloader/run_daily_all_markets.sh",
            "FAILED_STEPS=()",
            "if run_step fixture_failure bash -c 'exit 7'; then exit 99; fi",
        )
    )
    subprocess.run(
        ["bash", "-c", command],
        cwd=Path(__file__).resolve().parents[1],
        check=True,
        capture_output=True,
        text=True,
    )

    receipt = json.loads((receipt_dir / "fixture_failure.json").read_text())
    assert receipt["state"] == "failed"
    assert receipt["exit_code"] == 7

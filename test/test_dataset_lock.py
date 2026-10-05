from __future__ import annotations

from contextlib import contextmanager
import errno
import json
import os
from pathlib import Path
import select
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

from downloader import download_binance_perp_15m as binance
from downloader import download_okx_perp_daily as okx


fcntl = pytest.importorskip("fcntl")
locks = sys.modules[binance.exclusive_dataset_lock.__module__]
COLLECTORS = (binance, okx)


@contextmanager
def _locked_by_child(path: Path):
    script = (
        "import fcntl, sys\n"
        "with open(sys.argv[1], 'a+') as handle:\n"
        "    fcntl.flock(handle, fcntl.LOCK_EX)\n"
        "    print('locked', flush=True)\n"
        "    sys.stdin.read(1)\n"
    )
    child = subprocess.Popen(
        [sys.executable, "-c", script, str(path)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert select.select([child.stdout], [], [], 5)[0], "lock holder did not start"
        assert child.stdout.readline().strip() == "locked"
        yield child
    finally:
        try:
            child.communicate(input="\n", timeout=5)
        except subprocess.TimeoutExpired:
            child.kill()
            child.communicate(timeout=5)


def test_real_subprocess_contention_times_out_then_releases(tmp_path, capsys):
    path = tmp_path / ".download.lock"
    with _locked_by_child(path):
        started = time.monotonic()
        with pytest.raises(locks.DatasetLockTimeout, match="timed out"):
            with locks.exclusive_dataset_lock(
                path, provider="test", timeout_seconds=0.08,
                poll_interval_seconds=0.01,
            ):
                pytest.fail("entered a workspace held by another process")
        assert 0.07 <= time.monotonic() - started < 2.0
    inode = path.stat().st_ino
    with locks.exclusive_dataset_lock(path, provider="test", timeout_seconds=0):
        with path.open("a+") as probe:
            with pytest.raises(BlockingIOError):
                fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
    assert path.stat().st_ino == inode
    events = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    assert [event["state"] for event in events] == [
        "waiting", "timeout", "waiting", "acquired",
    ]
    assert events[1]["reason"] == "dataset_writer_lock_timeout"
    assert events[1]["lock_wait_seconds"] >= 0.08


def test_deadline_uses_nonblocking_attempts_and_caps_the_last_sleep(tmp_path, monkeypatch):
    clock = [0.0]
    sleeps = []
    descriptors = []
    flags = []

    def busy(fd, operation):
        descriptors.append(fd)
        flags.append(operation)
        raise BlockingIOError(errno.EAGAIN, "writer active")

    def advance(seconds):
        sleeps.append(seconds)
        clock[0] += seconds

    monkeypatch.setattr(locks, "fcntl", SimpleNamespace(
        flock=busy, LOCK_EX=fcntl.LOCK_EX, LOCK_NB=fcntl.LOCK_NB,
    ))
    monkeypatch.setattr(locks.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(locks.time, "sleep", advance)
    with pytest.raises(locks.DatasetLockTimeout):
        with locks.exclusive_dataset_lock(
            tmp_path / ".download.lock", provider="test", timeout_seconds=0.6,
        ):
            pytest.fail("busy lock acquired")
    assert sleeps == pytest.approx([0.25, 0.25, 0.1])
    assert flags == [fcntl.LOCK_EX | fcntl.LOCK_NB] * 3
    with pytest.raises(OSError) as error:
        os.fstat(descriptors[-1])
    assert error.value.errno == errno.EBADF


def test_unsupported_locking_fails_before_creating_workspace(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(locks, "fcntl", None)
    path = tmp_path / "new-workspace" / ".download.lock"
    with pytest.raises(RuntimeError, match="locking is unavailable"):
        with locks.exclusive_dataset_lock(path, provider="test"):
            pytest.fail("unsupported backend entered workspace")
    assert not path.parent.exists()
    event = json.loads(capsys.readouterr().err)
    assert event["state"] == "unsupported"
    assert event["reason"] == "exclusive_file_lock_unavailable"


def test_unexpected_lock_error_is_not_retried(tmp_path, monkeypatch, capsys):
    descriptors = []

    def broken(fd, operation):
        descriptors.append(fd)
        raise OSError(errno.EIO, "synthetic locking failure")

    monkeypatch.setattr(locks, "fcntl", SimpleNamespace(
        flock=broken, LOCK_EX=fcntl.LOCK_EX, LOCK_NB=fcntl.LOCK_NB,
    ))
    with pytest.raises(OSError, match="synthetic locking failure"):
        with locks.exclusive_dataset_lock(tmp_path / ".download.lock", provider="test"):
            pytest.fail("broken lock entered workspace")
    assert len(descriptors) == 1
    with pytest.raises(OSError) as error:
        os.fstat(descriptors[0])
    assert error.value.errno == errno.EBADF
    events = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    assert events[-1]["state"] == "error"


@pytest.mark.parametrize("collector", COLLECTORS)
def test_collector_lock_cli_default_and_explicit_override(collector, monkeypatch):
    assert collector.exclusive_dataset_lock is locks.exclusive_dataset_lock
    monkeypatch.setattr(sys, "argv", ["collector"])
    assert collector.parse_args().lock_timeout_seconds == 180.0
    monkeypatch.setattr(sys, "argv", ["collector", "--lock-timeout-seconds", "0"])
    assert collector.parse_args().lock_timeout_seconds == 0


@pytest.mark.parametrize("collector", COLLECTORS)
@pytest.mark.parametrize("timeout", ["-1", "nan", "inf", "-inf"])
def test_collector_rejects_invalid_lock_timeout(collector, timeout, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["collector", f"--lock-timeout-seconds={timeout}"])
    with pytest.raises(SystemExit) as error:
        collector.parse_args()
    assert error.value.code == 2


@pytest.mark.parametrize("collector", COLLECTORS)
def test_collector_timeout_cannot_enter_work_or_replace_source_evidence(
    collector, tmp_path, monkeypatch, capsys,
):
    names = (
        "symbols.csv", "download_report.csv", "download_receipt.json",
        "download_summary.json", "download_summary.historical_features.json",
        "download_summary.candles_only.json", "progress.json",
        "historical_feature_report.csv", "binance_historical_feature_catalog.json",
        "okx_historical_feature_catalog.json",
    )
    for name in names:
        (tmp_path / name).write_bytes(f"preserved {name}".encode())
    before = {name: (tmp_path / name).read_bytes() for name in names}

    def forbidden(*args, **kwargs):
        pytest.fail("timeout must not instantiate a client or enter source work")

    monkeypatch.setattr(collector, "_run_locked_download", forbidden)
    monkeypatch.setattr(collector, "BinanceClient" if collector is binance else "OkxClient", forbidden)
    monkeypatch.setattr(sys, "argv", [
        "collector", "--output-dir", str(tmp_path), "--lock-timeout-seconds", "0.04",
    ])
    with _locked_by_child(tmp_path / ".download.lock"):
        with pytest.raises(locks.DatasetLockTimeout):
            collector.main()
    assert {name: (tmp_path / name).read_bytes() for name in names} == before
    assert {path.name for path in tmp_path.iterdir()} == {*names, ".download.lock"}
    events = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    assert events[-1]["state"] == "timeout"


@pytest.mark.parametrize("collector", COLLECTORS)
@pytest.mark.parametrize("fail", [False, True])
def test_collector_holds_lock_through_work_and_releases_on_every_exit(
    collector, fail, tmp_path, monkeypatch,
):
    called = []

    def work(args, output_dir, *, lock_wait_seconds, **kwargs):
        called.append(True)
        assert output_dir == tmp_path
        assert lock_wait_seconds >= 0
        with (tmp_path / ".download.lock").open("a+") as probe:
            with pytest.raises(BlockingIOError):
                fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if fail:
            raise RuntimeError("synthetic work failure")

    monkeypatch.setattr(collector, "_run_locked_download", work)
    monkeypatch.setattr(sys, "argv", ["collector", "--output-dir", str(tmp_path)])
    if fail:
        with pytest.raises(RuntimeError, match="synthetic work failure"):
            collector.main()
    else:
        collector.main()
    assert called == [True]
    with (tmp_path / ".download.lock").open("a+") as probe:
        fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)

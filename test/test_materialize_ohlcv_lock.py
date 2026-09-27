"""The projection must share the canonical source lock through final receipts."""

from datetime import datetime, timezone
import fcntl
import json
import math
import os
from pathlib import Path
import sys

import polars as pl
import pytest

from downloader import materialize_ohlcv_daily as materializer


def cli(monkeypatch, source_dir, output_dir, *extra):
    monkeypatch.setattr(sys, "argv", [
        "materialize_ohlcv_daily", "--input-dir", str(source_dir),
        "--output-dir", str(output_dir), "--provider", "fixture", "--workers", "1", *extra,
    ])


def assert_lock_held(path):
    with path.open("a+") as handle:
        with pytest.raises(BlockingIOError):
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def assert_lock_released(path):
    with path.open("a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def test_cli_defaults_to_bounded_180_second_source_lock(tmp_path, monkeypatch):
    cli(monkeypatch, tmp_path / "1m", tmp_path / "daily")
    assert materializer.parse_args().lock_timeout_seconds == 180.0
    cli(monkeypatch, tmp_path / "1m", tmp_path / "daily", "--lock-timeout-seconds=0")
    assert materializer.parse_args().lock_timeout_seconds == 0.0


@pytest.mark.parametrize("invalid", ["-1", "nan", "inf", "-inf", "not-a-number"])
def test_cli_rejects_invalid_lock_timeout_without_creating_paths(tmp_path, monkeypatch, invalid):
    source_dir, output_dir = tmp_path / "1m", tmp_path / "daily"
    cli(monkeypatch, source_dir, output_dir, f"--lock-timeout-seconds={invalid}")
    with pytest.raises(SystemExit) as caught:
        materializer.main()
    assert caught.value.code == 2
    assert not source_dir.exists() and not output_dir.exists()


@pytest.mark.parametrize("existing_output", [False, True])
def test_busy_source_lock_does_not_initialize_or_overwrite_output(
    tmp_path, monkeypatch, existing_output,
):
    source_dir, output_dir = tmp_path / "1m", tmp_path / "daily"
    source_dir.mkdir()
    lock_path = source_dir / ".download.lock"
    previous = {}
    if existing_output:
        output_dir.mkdir()
        for name in ("progress.json", "download_summary.json", "download_report.csv"):
            path = output_dir / name
            path.write_bytes(b"unchanged previous receipt")
            previous[path] = path.read_bytes()
    cli(monkeypatch, source_dir, output_dir, "--lock-timeout-seconds=0", "--refresh")
    with lock_path.open("a+") as owner:
        fcntl.flock(owner.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(TimeoutError, match="dataset writer lock timed out"):
            materializer.main()
        assert_lock_held(lock_path)
    if existing_output:
        assert {path: path.read_bytes() for path in previous} == previous
        assert sorted(path.name for path in output_dir.iterdir()) == sorted(path.name for path in previous)
    else:
        assert not output_dir.exists()
    assert_lock_released(lock_path)


@pytest.mark.parametrize("fail", [False, True])
def test_locked_dispatch_passes_acquisition_evidence_and_always_releases(tmp_path, monkeypatch, fail):
    source_dir, output_dir = tmp_path / "1m", tmp_path / "daily"
    lock_path = source_dir / ".download.lock"
    failure = RuntimeError("original helper failure")
    seen = []

    def work(args, *, started, lock_wait_seconds):
        assert_lock_held(lock_path)
        assert not output_dir.exists()
        assert Path(args.input_dir) == source_dir
        assert started.tzinfo is not None
        assert math.isfinite(lock_wait_seconds) and lock_wait_seconds >= 0
        seen.append(lock_wait_seconds)
        if fail:
            raise failure

    monkeypatch.setattr(materializer, "_run_locked_materialization", work)
    cli(monkeypatch, source_dir, output_dir, "--lock-timeout-seconds=0")
    if fail:
        with pytest.raises(RuntimeError) as caught:
            materializer.main()
        assert caught.value is failure
    else:
        materializer.main()
    assert len(seen) == 1
    assert_lock_released(lock_path)


@pytest.mark.parametrize("outcome", ["success", "read_error", "atomic_replace_error"])
def test_real_main_holds_source_lock_through_work_reports_and_final_progress(
    tmp_path, monkeypatch, outcome,
):
    source_dir, output_dir = tmp_path / "1m", tmp_path / "daily"
    source_dir.mkdir()
    output_dir.mkdir()
    source, target = source_dir / "X_features.parquet", output_dir / "X_features.parquet"
    lock_path = source_dir / ".download.lock"
    frame = pl.DataFrame({
        "date": [datetime(2020, 1, 1, tzinfo=timezone.utc)],
        "open": [1.0], "max": [2.0], "min": [0.0], "close": [1.5], "Trading_Volume": [1.0],
    })
    frame.write_parquet(source)
    materializer._daily_frame(source).write_parquet(target)
    before = target.read_bytes()
    frame.with_columns(pl.lit(9.0).alias("Trading_Volume")).write_parquet(source)
    events = []
    read_original = materializer.read_logical_parquet
    parquet_original = materializer._write_parquet_atomic
    text_original = materializer.atomic_write_text
    progress_original = materializer.PersistentProgress
    replace_original = os.replace

    def read(*args, **kwargs):
        assert_lock_held(lock_path)
        events.append("source_read")
        if outcome == "read_error":
            raise OSError("fixture source read error")
        return read_original(*args, **kwargs)

    def parquet(*args, **kwargs):
        assert_lock_held(lock_path)
        events.append("parquet_write")
        return parquet_original(*args, **kwargs)

    def text(path, *args, **kwargs):
        assert_lock_held(lock_path)
        events.append(Path(path).name)
        return text_original(path, *args, **kwargs)

    class LockedProgress(progress_original):
        def _write(self, *args, **kwargs):
            assert_lock_held(lock_path)
            events.append("progress_write")
            return super()._write(*args, **kwargs)

    def replace(src, dst, *args, **kwargs):
        if Path(dst) == target:
            assert_lock_held(lock_path)
            if outcome == "atomic_replace_error":
                raise OSError("fixture atomic replace error")
        return replace_original(src, dst, *args, **kwargs)

    monkeypatch.setattr(materializer, "read_logical_parquet", read)
    monkeypatch.setattr(materializer, "_write_parquet_atomic", parquet)
    monkeypatch.setattr(materializer, "atomic_write_text", text)
    monkeypatch.setattr(materializer, "PersistentProgress", LockedProgress)
    monkeypatch.setattr(os, "replace", replace)
    cli(monkeypatch, source_dir, output_dir, "--lock-timeout-seconds=0", "--refresh")
    if outcome == "success":
        materializer.main()
        assert pl.read_parquet(target)["Trading_Volume"].item() == 9.0
    else:
        with pytest.raises(RuntimeError, match="daily materialization incomplete"):
            materializer.main()
        assert target.read_bytes() == before
    assert "source_read" in events
    assert "download_summary.json" in events and "download_report.csv" in events
    assert events[-1] == "progress_write"
    summary = json.loads((output_dir / "download_summary.json").read_text())
    progress = json.loads((output_dir / "progress.json").read_text())
    assert summary["status_counts"] == {"updated" if outcome == "success" else "failed": 1}
    assert progress["state"] == ("complete" if outcome == "success" else "failed")
    assert summary["lock_wait_seconds"] >= 0
    assert not list(output_dir.glob(f".{target.name}.*.tmp"))
    assert_lock_released(lock_path)

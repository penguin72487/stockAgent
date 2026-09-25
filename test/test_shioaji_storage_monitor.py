from __future__ import annotations

from datetime import UTC, datetime
import json
import os
from pathlib import Path

import pytest

from stockagent.live import shioaji_storage_monitor as storage_monitor
from stockagent.live.shioaji_storage_monitor import (
    StorageDatasetSpec,
    build_shioaji_storage_snapshot,
    write_shioaji_storage_snapshot,
)


def test_storage_snapshot_reconciles_size_growth_and_disk(tmp_path: Path) -> None:
    now = datetime(2026, 8, 14, 4, 0, tzinfo=UTC)
    source = tmp_path / "source"
    derived = tmp_path / "derived"
    source.mkdir()
    derived.mkdir()
    first = source / "ticks.parquet"
    second = derived / "features.parquet"
    first.write_bytes(b"a" * 100)
    second.write_bytes(b"b" * 50)
    observed = datetime(2026, 8, 12, 3, 0, tzinfo=UTC).timestamp()
    os.utime(first, (observed, observed))
    os.utime(second, (observed, observed))
    specs = (
        StorageDatasetSpec(
            "source",
            "來源",
            "source",
            "historical",
            (source,),
            "source files",
        ),
        StorageDatasetSpec(
            "derived",
            "衍生",
            "derived",
            "none",
            (derived,),
            "derived files",
        ),
    )
    payload = build_shioaji_storage_snapshot(tmp_path, now=now, specs=specs)
    assert payload["summary"]["total_bytes"] == 150
    assert payload["summary"]["source_bytes"] == 100
    assert payload["summary"]["derived_bytes"] == 50
    assert payload["summary"]["growth_window_bytes"] == 150
    assert payload["summary"]["average_daily_growth_bytes"] == pytest.approx(5)
    assert all(row["scan_seconds"] >= 0 for row in payload["datasets"])
    assert sum(row["bytes"] for row in payload["daily_growth"]) == 150
    assert "path" not in json.dumps(payload)


def test_storage_growth_uses_exact_taipei_midnight_boundaries(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    # At local 2026-08-14 12:00, the 30 completed days start on 7/15.
    now = datetime(2026, 8, 14, 4, tzinfo=UTC)
    mtimes = (
        datetime(2026, 7, 14, 15, 59, 59, tzinfo=UTC),
        datetime(2026, 7, 14, 16, tzinfo=UTC),
        datetime(2026, 8, 13, 15, 59, 59, tzinfo=UTC),
        datetime(2026, 8, 13, 16, tzinfo=UTC),
    )
    for index, observed in enumerate(mtimes):
        path = source / f"{index}.parquet"
        path.write_bytes(b"x" * (index + 1))
        os.utime(path, (observed.timestamp(), observed.timestamp()))
    (source / "alias.parquet").symlink_to(source / "1.parquet")
    spec = StorageDatasetSpec("source", "來源", "source", "historical", (source,), "source")
    snapshot = build_shioaji_storage_snapshot(tmp_path, now=now, specs=(spec,))
    row = snapshot["datasets"][0]
    growth = {item["date"]: item["bytes"] for item in row["daily_growth"]}
    assert row["bytes"] == 10
    assert row["files"] == 4
    assert row["growth_window_bytes"] == 2 + 3
    assert growth["2026-07-15"] == 2
    assert growth["2026-08-13"] == 3
    assert "2026-08-14" not in growth


def test_native_and_python_scans_have_identical_storage_semantics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    if storage_monitor.shutil.which("find", path="/usr/bin:/bin") is None:
        pytest.skip("GNU find is not installed")
    root = tmp_path / "source"
    nested = root / "nested"
    nested.mkdir(parents=True)
    first = root / "first"
    second = nested / "second"
    first.write_bytes(b"a" * 7)
    second.write_bytes(b"b" * 9)
    observed = datetime(2026, 8, 14, 4, tzinfo=UTC)
    for path, hour in ((first, 3), (second, 4)):
        timestamp = datetime(2026, 8, 13, hour, tzinfo=UTC).timestamp()
        os.utime(path, (timestamp, timestamp))
    (root / "file-link").symlink_to(first)
    (root / "dir-link").symlink_to(nested, target_is_directory=True)
    standalone = tmp_path / "state.json"
    standalone.write_bytes(b"{}")
    spec = StorageDatasetSpec(
        "source", "來源", "source", "historical", (root, standalone), "source"
    )
    native = storage_monitor._scan_dataset(spec, now=observed)
    monkeypatch.setattr(storage_monitor.shutil, "which", lambda command, path: None)
    fallback = storage_monitor._scan_dataset(spec, now=observed)
    assert native == fallback
    assert native["files"] == 3
    assert list(storage_monitor._iter_file_metadata(root / "file-link")) == []


def test_failed_native_scan_cannot_publish_partial_inventory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "source"
    root.mkdir()
    (root / "data").write_bytes(b"x")
    monkeypatch.setattr(
        storage_monitor.shutil, "which", lambda command, path: "/bin/false"
    )
    spec = StorageDatasetSpec("source", "來源", "source", "historical", (root,), "source")
    output = tmp_path / "summary.json"
    output.write_text('{"status":"previous"}', encoding="utf-8")
    monkeypatch.setattr(storage_monitor, "default_storage_datasets", lambda path: (spec,))
    with pytest.raises(RuntimeError, match="Storage scan failed"):
        storage_monitor.write_shioaji_storage_snapshot(
            tmp_path, output, now=datetime(2026, 8, 14, 4, tzinfo=UTC)
        )
    assert output.read_text(encoding="utf-8") == '{"status":"previous"}'


def test_storage_snapshot_writer_preserves_one_total_per_day(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "data").write_bytes(b"x")
    output = tmp_path / "summary.json"
    specs = (
        StorageDatasetSpec(
            "source", "來源", "source", "historical", (source,), "source"
        ),
    )
    first = build_shioaji_storage_snapshot(
        tmp_path,
        now=datetime(2026, 8, 13, 4, 0, tzinfo=UTC),
        specs=specs,
    )
    output.write_text(json.dumps(first), encoding="utf-8")
    written = write_shioaji_storage_snapshot(
        tmp_path,
        output,
        now=datetime(2026, 8, 14, 4, 0, tzinfo=UTC),
    )
    assert len(written["daily_totals"]) == 2
    assert output.stat().st_mode & 0o077 == 0


def test_storage_capacity_uses_daily_net_growth_after_seven_complete_days(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "data").write_bytes(b"x" * 200)
    specs = (
        StorageDatasetSpec(
            "source", "來源", "source", "historical", (source,), "source"
        ),
    )
    payload = build_shioaji_storage_snapshot(
        tmp_path,
        now=datetime(2026, 8, 14, 4, 0, tzinfo=UTC),
        previous={
            "daily_totals": [
                {"date": "2026-08-05", "bytes": 100},
                {"date": "2026-08-13", "bytes": 180},
            ]
        },
        specs=specs,
    )
    assert payload["summary"]["observed_growth_days"] == 8
    assert payload["summary"]["observed_average_daily_net_growth_bytes"] == 10
    assert payload["summary"]["capacity_growth_estimate_bytes"] == 10
    assert payload["summary"]["capacity_growth_source"] == "daily_total_net_growth"

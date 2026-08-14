from __future__ import annotations

from datetime import UTC, datetime
import json
import os
from pathlib import Path

import pytest

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
    assert sum(row["bytes"] for row in payload["daily_growth"]) == 150
    assert "path" not in json.dumps(payload)


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

"""Exercise the diagnostic's source and physical-generation boundaries."""

from __future__ import annotations

from datetime import date
import json
from pathlib import Path
import subprocess
import sys

import polars as pl

from downloader.artifact_io import sha256_file


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/audit_tw_futures_successor_daily_observations.py"


def test_source_bound_readback_keeps_missing_and_later_generations_distinct(tmp_path: Path) -> None:
    targets = tmp_path / "targets.csv"
    target_report = tmp_path / "target-report.json"
    daily = tmp_path / "daily.parquet"
    daily_manifest = tmp_path / "daily-manifest.json"
    detail = tmp_path / "detail.csv"
    report = tmp_path / "report.json"
    pl.DataFrame({
        "original_physical_contract": ["AAF:202001@a", "BBF:202001@b", "CCF:202001@c"],
        "boundary_date": [date(2020, 1, 3)] * 3,
        "successor_product": ["AA1", "BB1", "CC1"],
        "successor_contract": ["202001"] * 3,
        "conversion_effective_date": [date(2020, 1, 6)] * 3,
        "conservative_end_exclusive": [date(2020, 1, 10)] * 3,
    }).write_csv(targets)
    target_report.write_text(json.dumps({"target_boundaries": 3, "detail_sha256": sha256_file(targets),
                                         "evidence_window": "effective inclusive, later event exclusive"}))
    pl.DataFrame({
        "date": [date(2020, 1, 3), date(2020, 1, 3), date(2020, 1, 7), date(2020, 1, 10)],
        "product": ["AAF", "BBF", "AA1", "BB1"],
        "contract": ["202001"] * 4,
        "session": ["一般"] * 4,
        "open_interest": [0, 1, 1, 3],
        "volume": [None, 1, 0, 1],
        "settlement": [5.0, 6.0, 5.5, 6.5],
        "source_sha256": ["source"] * 4,
    }).write_parquet(daily)
    daily_manifest.write_text(json.dumps({"outputs": {daily.name: {"sha256": sha256_file(daily)}}}))
    command = [sys.executable, str(SCRIPT), "--successors", str(targets),
               "--successors-report", str(target_report), "--daily", str(daily),
               "--daily-manifest", str(daily_manifest), "--detail", str(detail), "--report", str(report)]
    subprocess.run(command, check=True, capture_output=True, text=True)

    result = json.loads(report.read_text())
    assert result["target_boundaries"] == 3
    assert result["origin_boundary_daily_rows"] == 2
    assert result["origin_boundary_zero_oi"] == 1
    assert result["origin_boundary_positive_oi"] == 1
    assert result["origin_boundary_missing_oi"] == 1
    assert result["successor_boundaries_with_daily_rows"] == 1
    assert result["successor_daily_rows"] == 1
    rows = {row["successor_product"]: row for row in pl.read_csv(detail).to_dicts()}
    assert rows["CC1"]["origin_boundary_open_interest"] is None
    assert rows["BB1"]["successor_daily_rows"] == 0  # later generation starts at the exclusive end
    assert result["inventory_transfer_or_valuation_admitted"] is False

    daily_manifest.write_text(json.dumps({"outputs": {daily.name: {"sha256": "0" * 64}}}))
    detail.unlink()
    report.unlink()
    rejected = subprocess.run(command, capture_output=True, text=True)
    assert rejected.returncode != 0
    assert not detail.exists() and not report.exists()

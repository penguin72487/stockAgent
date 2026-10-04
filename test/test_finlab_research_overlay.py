from __future__ import annotations

from datetime import date
import hashlib
import json
from pathlib import Path
import tempfile

import polars as pl

from stockagent.data.finlab_research_overlay import (
    _available_on, _wide_stock, build_finlab_research_overlay,
)
from stockagent.data.tw_public_research_taifex import MARKET_SYMBOL


def _source(root: Path, key: str, table: pl.DataFrame) -> None:
    path = root / "datasets" / f"{len(list((root / 'datasets').glob('*.parquet')))}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    table.write_parquet(path)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    receipts = root / "receipts"
    receipts.mkdir(exist_ok=True)
    (receipts / f"{digest}.json").write_text(json.dumps({
        "dataset": key, "parquet_path": str(path.relative_to(root)),
        "sha256": digest, "publication_time_status": "not_verified",
        "index_semantics": "source_timestamp_not_verified_publication_date",
    }), encoding="utf-8")


def test_availability_distinguishes_unknown_clock_and_exact_preopen():
    sessions = [date(2026, 9, 21), date(2026, 9, 22), date(2026, 9, 23)]
    assert _available_on("2026-09-21 00:00:00", sessions) == date(2026, 9, 22)
    assert _available_on("2026-09-21 08:59:59", sessions, event_clock=True) == date(2026, 9, 21)
    assert _available_on("2026-09-21 09:00:00", sessions, event_clock=True) == date(2026, 9, 22)
    assert _available_on("2026-09-23 10:00:00", sessions, event_clock=True) is None


def test_multiple_source_dates_before_one_session_use_latest_observation():
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "wide.parquet"
        pl.DataFrame({
            "source_index": ["2026-09-18 00:00:00", "2026-09-19 00:00:00"],
            "2330": [10.0, 11.0],
        }).write_parquet(path)
        table = _wide_stock(path, "twfl_test_raw", [date(2026, 9, 21), date(2026, 9, 22)])
        assert table.height == 1
        assert table.get_column("twfl_test_raw").to_list() == [11.0]


def test_research_overlay_joins_without_leakage_or_row_multiplication():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        base_path = root / "base.parquet"
        output_path = root / "research.parquet"
        finlab = root / "finlab"
        pl.DataFrame({
            "date": [date(2026, 9, 21), date(2026, 9, 21),
                     date(2026, 9, 22), date(2026, 9, 22),
                     date(2026, 9, 23), date(2026, 9, 23)],
            "symbol": ["2330", MARKET_SYMBOL] * 3,
            "base_feature": [1.0] * 6,
        }).write_parquet(base_path)
        _source(finlab, "monthly_revenue:當月營收", pl.DataFrame({
            "source_index": ["2026-09-21 00:00:00"], "2330": [100.0],
        }))
        _source(finlab, "important_info_announcement", pl.DataFrame({
            "source_index": ["0", "1"], "symbol": ["2330", "2330"],
            "date": ["2026-09-21 08:30:00", "2026-09-21 12:30:00"],
        }))
        summary = build_finlab_research_overlay(
            finlab_root=finlab, base_path=base_path, output_path=output_path,
        )
        result = pl.read_parquet(output_path).sort("date", "symbol")
        assert summary["rows"] == 6
        assert summary["strict_training_eligible"] is False
        stock = result.filter(pl.col("symbol") == "2330").sort("date")
        assert stock.get_column("twfl_material_announcement_count").to_list() == [1.0, 1.0, None]
        assert stock.get_column("twfl_monthly_revenue_raw").to_list() == [None, 100.0, None]
        assert build_finlab_research_overlay(
            finlab_root=finlab, base_path=base_path, output_path=output_path,
        )["reused"] is True

from __future__ import annotations

from datetime import date, datetime
import json
from pathlib import Path

import polars as pl

from downloader.download_shioaji_tw_kbars import UniverseRow
from scripts import audit_tw_stock_minute_coverage as coverage


def _row(symbol: str, clock: str, minute: int, volume: float = 1.0) -> dict:
    return {
        "symbol": symbol,
        "date": date(2026, 2, 25),
        "ts": datetime.fromisoformat(f"2026-02-25T{clock}:00"),
        "minutes_from_open": minute,
        "Open": 10.0,
        "High": 10.0,
        "Low": 10.0,
        "Close": 10.0,
        "Volume": volume,
        "Amount": 10.0 * volume,
    }


def test_full_day_clock_distinguishes_auction_and_missing_trade_bars(
    tmp_path: Path, monkeypatch,
) -> None:
    day = date(2026, 2, 25)
    partition = tmp_path / "minute" / f"trade_date={day}" / "data.parquet"
    partition.parent.mkdir(parents=True)
    pl.DataFrame([
        _row("2330", "09:01", 1),
        _row("2330", "13:27", 267),
        _row("2330", "13:30", 270),
        _row("0050", "09:01", 1, volume=0),
    ]).write_parquet(partition)
    partition.with_name("summary.json").write_text(json.dumps({
        "status": "ok", "trade_date": str(day),
        "output_sha256": coverage._sha256(partition),
    }))
    rows = [
        UniverseRow(symbol, symbol, "twse", "stock", tmp_path / f"{symbol}.parquet")
        for symbol in ("0050", "2330")
    ]
    monkeypatch.setattr(
        coverage, "_expected_by_date",
        lambda *_args: (rows, {day: {row.symbol: row for row in rows}}),
    )
    output = tmp_path / "report"
    report = coverage.audit(tmp_path, tmp_path / "minute", day, day, output)
    assert report["totals"]["observed_active_minutes"] == 2
    assert report["totals"]["unobserved_active_minutes"] == 2 * 266 - 2
    assert report["totals"]["physical_active_minutes"] == 3
    assert report["totals"]["missing_physical_minutes"] == 2 * 266 - 3
    assert report["totals"]["full_grid_physical_minutes"] == 4
    assert report["totals"]["missing_full_grid_minutes"] == 2 * 270 - 4
    assert report["totals"]["closing_auction_interval_trade_minutes"] == 1
    assert report["totals"]["zero_volume_minute_rows"] == 1
    assert report["totals"]["structural_auction_slots_1326_to_1329"] == 8
    assert report["totals"]["symbol_days_missing_open_0901"] == 1
    assert report["totals"]["symbol_days_missing_close_1330"] == 1
    assert report["unobserved_by_clock"]["09:01"] == 1
    assert report["unobserved_by_clock"]["13:30"] == 1
    pair = pl.read_parquet(output / "pair_coverage.parquet").sort("symbol")
    assert pair["missing_active_minutes"].to_list() == [266, 264]
    assert pair["missing_minutes_from_open"][1].to_list()[:2] == [2, 3]
    assert 266 not in pair["missing_minutes_from_open"][0].to_list()
    assert 267 not in pair["missing_full_grid_minutes_from_open"][1].to_list()
    assert pair["invalid_rows"].to_list() == [0, 0]
    assert pair["reference_status"].to_list() == ["official_positive_volume"] * 2


def test_receipt_mismatch_is_unassessable_not_zero_coverage(
    tmp_path: Path, monkeypatch,
) -> None:
    day = date(2026, 2, 25)
    row = UniverseRow("2330", "2330", "twse", "stock", tmp_path / "2330.parquet")
    monkeypatch.setattr(
        coverage, "_expected_by_date",
        lambda *_args: ([row], {day: {row.symbol: row}}),
    )
    report = coverage.audit(tmp_path, tmp_path / "missing", day, day, tmp_path / "report")
    assert report["totals"]["unassessable_symbol_days"] == 1
    assert report["totals"].get("unobserved_active_minutes", 0) == 0
    pair = pl.read_parquet(tmp_path / "report" / "pair_coverage.parquet")
    assert pair["status"][0] == "source_unassessable"
    assert pair["missing_active_minutes"].null_count() == 1


def test_minute_only_symbol_is_reported_without_claiming_official_daily_volume(
    tmp_path: Path, monkeypatch,
) -> None:
    day = date(2026, 2, 25)
    partition = tmp_path / "minute" / f"trade_date={day}" / "data.parquet"
    partition.parent.mkdir(parents=True)
    pl.DataFrame([_row("9999", "13:27", 267)]).write_parquet(partition)
    partition.with_name("summary.json").write_text(json.dumps({
        "status": "ok", "trade_date": str(day),
        "output_sha256": coverage._sha256(partition),
    }))
    row = UniverseRow("2330", "2330", "twse", "stock", tmp_path / "2330.parquet")
    monkeypatch.setattr(
        coverage, "_expected_by_date",
        lambda *_args: ([row], {day: {row.symbol: row}}),
    )
    report = coverage.audit(tmp_path, tmp_path / "minute", day, day, tmp_path / "report")
    assert report["totals"]["minute_only_unverified_symbol_days"] == 1
    assert report["totals"]["closing_auction_interval_trade_minutes"] == 1
    pair = pl.read_parquet(tmp_path / "report" / "pair_coverage.parquet").sort("symbol")
    assert pair["reference_status"].to_list() == [
        "official_positive_volume", "minute_only_unverified_daily_reference",
    ]

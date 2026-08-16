from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

from downloader import audit_ohlcv_data as audit


def test_intraday_duplicate_audit_preserves_timestamp_grain(tmp_path: Path) -> None:
    root = tmp_path / "arbitrary-provider"
    root.mkdir()
    path = root / "BTCUSDT_features.parquet"
    pl.DataFrame(
        {
            "date": [
                "2026-08-15 00:00:00",
                "2026-08-15 00:15:00",
                "2026-08-15 00:30:00",
            ],
            "open": [100.0, 101.0, 102.0],
            "max": [101.0, 102.0, 103.0],
            "min": [99.0, 100.0, 101.0],
            "close": [100.5, 101.5, 102.5],
            "Trading_Volume": [1.0, 2.0, 3.0],
        }
    ).write_parquet(path)
    args = argparse.Namespace(
        end_date="2026-08-15",
        stale_max_lag_days=0,
        daily_gap_days=10,
        intraday_gap_multiple=4.0,
    )

    result = audit._audit_file((root, path, args))

    assert result.status == "ok"
    assert result.duplicate_dates == 0
    assert result.max_gap_seconds == 900.0


def test_intraday_duplicate_audit_still_detects_exact_timestamp_duplicates(
    tmp_path: Path,
) -> None:
    root = tmp_path / "data_binance"
    root.mkdir()
    path = root / "BTCUSDT_features.parquet"
    pl.DataFrame(
        {
            "date": ["2026-08-15 00:00:00", "2026-08-15 00:00:00"],
            "open": [100.0, 100.0],
            "max": [101.0, 101.0],
            "min": [99.0, 99.0],
            "close": [100.5, 100.5],
            "Trading_Volume": [1.0, 1.0],
        }
    ).write_parquet(path)
    args = argparse.Namespace(
        end_date="2026-08-15",
        stale_max_lag_days=0,
        daily_gap_days=10,
        intraday_gap_multiple=4.0,
    )

    result = audit._audit_file((root, path, args))

    assert result.status == "warn"
    assert result.duplicate_dates == 2

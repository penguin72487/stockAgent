from __future__ import annotations

from datetime import date
import json
from pathlib import Path

import polars as pl

from downloader.backfill_tw_public_to_yahoo import (
    _normalize_delisted_archive_files,
    _official_delisted_symbols,
    _resolve_start_date,
    _update_return_price_provenance,
    _write_symbol,
)


def _quotes(days: list[date], closes: list[float]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "date": days,
            "open": closes,
            "max": closes,
            "min": closes,
            "close": closes,
            "adjclose": closes,
            "Trading_Volume": [1000.0] * len(days),
        }
    )


def test_write_symbol_can_create_missing_delisted_file(tmp_path: Path) -> None:
    fresh = _quotes([date(2020, 1, 2)], [10.0])
    skipped = _write_symbol(
        tmp_path,
        "1234",
        fresh,
        requested_end_date="2020-01-02",
        dry_run=False,
        create_missing=False,
    )
    assert skipped.status == "missing_existing_file"
    assert not (tmp_path / "1234_features.parquet").exists()

    created = _write_symbol(
        tmp_path,
        "1234",
        fresh,
        requested_end_date="2020-01-02",
        dry_run=False,
        create_missing=True,
    )
    assert created.status == "created_delisted"
    assert pl.read_parquet(tmp_path / "1234_features.parquet").height == 1


def test_official_overlay_preserves_adjusted_close_and_corporate_actions(tmp_path: Path) -> None:
    existing = _quotes([date(2020, 1, 2)], [10.0]).with_columns(
        pl.lit(8.0).alias("adjclose"),
        pl.lit(1.0).alias("Dividends"),
        pl.lit(2.0).alias("Stock Splits"),
    )
    existing.write_parquet(tmp_path / "1234_features.parquet")
    official = _quotes([date(2020, 1, 2)], [11.0])

    result = _write_symbol(
        tmp_path,
        "1234",
        official,
        requested_end_date="2020-01-02",
        dry_run=False,
        create_missing=False,
    )

    assert result.status == "updated"
    row = pl.read_parquet(tmp_path / "1234_features.parquet").row(0, named=True)
    assert row["close"] == 11.0
    assert row["adjclose"] == 8.0
    assert row["Dividends"] == 1.0
    assert row["Stock Splits"] == 2.0


def test_normalize_delisted_archive_candidates_to_canonical_symbol(tmp_path: Path) -> None:
    _quotes([date(2019, 1, 2)], [9.0]).write_parquet(
        tmp_path / "1234_TW_features.parquet"
    )
    _quotes([date(2020, 1, 2)], [10.0]).write_parquet(
        tmp_path / "1234_TWO_features.parquet"
    )
    _quotes([date(2020, 1, 2), date(2021, 1, 2)], [11.0, 12.0]).write_parquet(
        tmp_path / "1234_features.parquet"
    )

    normalized, removed = _normalize_delisted_archive_files(tmp_path, dry_run=False)

    assert (normalized, removed) == (1, 2)
    assert not (tmp_path / "1234_TW_features.parquet").exists()
    assert not (tmp_path / "1234_TWO_features.parquet").exists()
    frame = pl.read_parquet(tmp_path / "1234_features.parquet").sort("date")
    assert frame["close"].to_list() == [9.0, 11.0, 12.0]


def test_official_delisted_symbols_and_full_history_start(tmp_path: Path) -> None:
    public_dir = tmp_path / "public"
    symbols_root = tmp_path / "stocks"
    public_dir.mkdir()
    symbols_root.mkdir()
    pl.DataFrame(
        {"symbol": ["1234"], "date": [date(2010, 1, 2)]}
    ).write_parquet(public_dir / "twse_delisted_company.parquet")
    pl.DataFrame(
        {"symbol": ["5678"], "date": [date(2012, 1, 2)]}
    ).write_parquet(public_dir / "tpex_delisted_company.parquet")
    pl.DataFrame(
        {"date": [date(2004, 2, 11)], "證券代號": ["1234"], "收盤價": [10.0]}
    ).write_parquet(public_dir / "twse_daily_ohlcv.parquet")
    pl.DataFrame(
        {"date": [date(2007, 1, 2)], "代號": ["5678"], "收盤": [10.0]}
    ).write_parquet(public_dir / "tpex_daily_ohlcv.parquet")

    assert _official_delisted_symbols(public_dir) == {"1234", "5678"}
    assert _resolve_start_date(
        public_dir,
        symbols_root,
        None,
        create_missing_delisted=True,
    ) == date(2004, 2, 11)


def test_return_price_provenance_is_atomic_and_updatable(tmp_path: Path) -> None:
    assert _update_return_price_provenance(
        tmp_path,
        {"1234"},
        kind="official_raw_close",
        source="official",
        dry_run=False,
    ) == 1
    assert _update_return_price_provenance(
        tmp_path,
        {"1234"},
        kind="legacy_adjusted",
        source="licensed",
        dry_run=False,
    ) == 1

    payload = json.loads(
        (tmp_path / "return_price_provenance.json").read_text(encoding="utf-8")
    )
    assert payload["symbols"]["1234"]["kind"] == "legacy_adjusted"

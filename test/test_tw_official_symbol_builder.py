from __future__ import annotations

from datetime import date
import math
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow.parquet as pq
import pytest

from scripts.build_tw_official_symbol_parquets import (
    MIXED_FALLBACK_SOURCE_NAME,
    _official_frame,
    _legacy_official_frame,
    _normalized_reference_index,
    _source_adjustment_factors,
    _validate_yahoo_fallback_archive,
    _write_symbol,
    _write_official_quote_parquet,
)
from scripts.build_tw_yahoo_fallback_archive import (
    _read_symbol_fallback,
    _whitelist_markets,
    main as build_yahoo_fallback_archive,
)
from stockagent.data.tw_security import classify_tw_stock_or_etf


@pytest.mark.parametrize(
    ("symbol", "expected"),
    [
        ("2330", "stock"),
        ("9110", "stock"),
        ("0050", "etf"),
        ("00631L", "etf"),
        ("00400A", "etf"),
        ("0001", None),
        ("01001T", None),
        ("020000", None),
        ("03001P", None),
        ("2881A", None),
    ],
)
def test_tw_universe_contains_only_stocks_and_etfs(symbol: str, expected: str | None) -> None:
    assert classify_tw_stock_or_etf(symbol) == expected


def test_official_adjusted_index_starts_at_ten_and_chains_reference_returns() -> None:
    adjusted, missing = _normalized_reference_index(
        np.asarray([100.0, 96.0, 97.0, 90.0]),
        np.asarray([0.0, 1.0, 1.0, -5.0]),
        np.asarray([1000.0, 1000.0, 1000.0, 1000.0]),
    )

    assert missing == 0
    assert adjusted[0] == 10.0
    assert math.isclose(adjusted[1] / adjusted[0], 96.0 / 95.0)
    assert math.isclose(adjusted[2] / adjusted[1], 97.0 / 96.0)
    assert math.isclose(adjusted[3] / adjusted[2], 90.0 / 95.0)


def test_official_adjusted_index_freezes_zero_volume_unknown_reference() -> None:
    adjusted, missing = _normalized_reference_index(
        np.asarray([10.0, 10.0, 10.5]),
        np.asarray([np.nan, np.nan, 0.5]),
        np.asarray([1000.0, 0.0, 1000.0]),
    )

    assert missing == 0
    assert adjusted.tolist() == [10.0, 10.0, 10.5]


def test_official_adjusted_index_masks_positive_volume_unknown_reference() -> None:
    adjusted, missing = _normalized_reference_index(
        np.asarray([10.0, 10.2, 10.5]),
        np.asarray([0.0, np.nan, 0.3]),
        np.asarray([1000.0, 1000.0, 1000.0]),
    )

    assert missing == 1
    assert np.isnan(adjusted[1])
    assert math.isclose(adjusted[2], 10.0 * (10.5 / 10.2))


def test_corporate_action_reference_overrides_zero_change_marker() -> None:
    adjusted, missing = _normalized_reference_index(
        np.asarray([84.2, 80.0]),
        np.asarray([0.0, 0.0]),
        np.asarray([1000.0, 1000.0]),
        np.asarray([np.nan, 79.07]),
    )

    assert missing == 0
    assert math.isclose(adjusted[1] / adjusted[0], 80.0 / 79.07)


def test_explicit_official_adjusted_series_is_rebased_to_ten_by_return_factor() -> None:
    factors = _source_adjustment_factors(np.asarray([50.0, 51.0, 49.98]))
    adjusted, missing = _normalized_reference_index(
        np.asarray([100.0, 100.0, 100.0]),
        np.asarray([np.nan, np.nan, np.nan]),
        np.asarray([1000.0, 1000.0, 1000.0]),
        factor_override=factors,
    )

    assert missing == 0
    assert adjusted[0] == 10.0
    assert math.isclose(adjusted[1], 10.2)
    assert math.isclose(adjusted[2], 9.996)


def test_adjusted_ratio_does_not_bridge_two_archive_files() -> None:
    factors = _source_adjustment_factors(
        np.asarray([50.0, 51.0, 10.0, 10.2]),
        np.asarray([0, 0, 1, 1]),
    )

    assert np.isnan(factors[0])
    assert math.isclose(factors[1], 51.0 / 50.0)
    assert np.isnan(factors[2])
    assert math.isclose(factors[3], 10.2 / 10.0)


def test_legacy_official_archive_without_adjclose_keeps_reconstruction_inputs(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "twse_legacy.parquet"
    pl.DataFrame(
        {
            "date": [date(2000, 1, 4), date(2000, 1, 5)],
            "symbol": ["2330", "2330"],
            "market": ["twse", "twse"],
            "open": [10.0, 10.5],
            "high": [10.5, 11.0],
            "low": [9.5, 10.0],
            "close": [10.0, 10.5],
            "volume": [1000.0, 1200.0],
            "signed_change": [0.0, 0.5],
        }
    ).write_parquet(archive)

    normalized = _legacy_official_frame(archive)

    assert normalized["source_adjclose"].null_count() == 2
    assert normalized["signed_change"].to_list() == [0.0, 0.5]
    assert normalized["max"].to_list() == [10.5, 11.0]


def test_official_unlimited_etf_return_above_two_x_is_preserved() -> None:
    adjusted, missing = _normalized_reference_index(
        np.asarray([17.95, 38.83]),
        np.asarray([0.0, 20.88]),
        np.asarray([1000.0, 1000.0]),
    )

    assert missing == 0
    assert math.isclose(adjusted[1] / adjusted[0], 38.83 / 17.95)


def test_official_parquet_metadata_has_no_yahoo_lineage(tmp_path: Path) -> None:
    output = tmp_path / "2330_features.parquet"
    frame = pl.DataFrame(
        {
            "date": [date(2026, 7, 10)],
            "open": [100.0],
            "max": [101.0],
            "min": [99.0],
            "close": [100.5],
            "Trading_Volume": [1000.0],
            "adjclose": [10.0],
        }
    )

    _write_official_quote_parquet(frame, output, checked_through="2026-07-10")

    metadata = pq.read_schema(output).metadata or {}
    assert metadata[b"stockagent.source"] == b"twse_tpex_official"
    assert metadata[b"stockagent.official_checked_through"] == b"2026-07-10"
    assert not any(b"yahoo" in key.lower() for key in metadata)


def test_yahoo_fallback_filters_invalid_bars_and_preserves_source_factor(
    tmp_path: Path,
) -> None:
    source = tmp_path / "2330_features.parquet"
    pl.DataFrame(
        {
            "date": [date(2000, 1, 4), date(2000, 1, 5), date(2000, 1, 6)],
            "open": [100.0, 102.0, 104.0],
            "max": [101.0, 103.0, 103.0],
            "min": [99.0, 101.0, 102.0],
            "close": [100.0, 102.0, 104.0],
            "adjclose": [50.0, 51.0, 52.0],
            "Trading_Volume": [1000.0, 1200.0, 1300.0],
        }
    ).write_parquet(source)

    result, frame = _read_symbol_fallback(
        "2330",
        [(source, None)],
        manifest={"2330": ("TSMC", "twse")},
        official_markets={},
        start=date(2000, 1, 1),
        end=date(2000, 1, 31),
    )

    assert result.status == "ok"
    assert frame is not None
    assert frame.height == 2
    assert frame["quote_source"].unique().to_list() == ["yahoo_fallback"]
    assert frame["source_factor"][0] is None
    assert math.isclose(frame["source_factor"][1], 51.0 / 50.0)


def test_yahoo_fallback_archive_cli_writes_a_verifiable_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_dir = tmp_path / "yahoo"
    official_dir = tmp_path / "official"
    output = tmp_path / "fallback" / "yahoo_tw_ohlcv.parquet"
    input_dir.mkdir()
    official_dir.mkdir()
    pl.DataFrame(
        {
            "code": ["2330"],
            "name": ["TSMC"],
            "market": ["TWSE"],
            "yahoo_symbol": ["2330.TW"],
        }
    ).write_csv(input_dir / "symbols.csv")
    pl.DataFrame(
        {
            "date": [date(2000, 1, 4), date(2000, 1, 5)],
            "open": [100.0, 102.0],
            "max": [101.0, 103.0],
            "min": [99.0, 101.0],
            "close": [100.0, 102.0],
            "adjclose": [50.0, 51.0],
            "Trading_Volume": [1000.0, 1200.0],
        }
    ).write_parquet(input_dir / "2330_features.parquet")
    monkeypatch.setattr(
        "sys.argv",
        [
            "build_tw_yahoo_fallback_archive.py",
            "--input-dir",
            str(input_dir),
            "--official-input-dir",
            str(official_dir),
            "--output-path",
            str(output),
            "--start-date",
            "2000-01-01",
            "--end-date",
            "2000-01-31",
            "--workers",
            "1",
        ],
    )

    build_yahoo_fallback_archive()
    _validate_yahoo_fallback_archive(output)

    archive = pl.read_parquet(output)
    assert archive.height == 2
    assert output.with_suffix(".summary.json").exists()
    assert output.with_suffix(".report.csv").exists()


def test_yahoo_whitelist_resolves_a_single_successful_venue(tmp_path: Path) -> None:
    path = tmp_path / "yahoo_whitelist.txt"
    path.write_text("3697.TW\n00631L.TW\nBAD\n", encoding="utf-8")

    assert _whitelist_markets(path) == {"3697": "twse", "00631L": "twse"}


def test_official_row_wins_over_yahoo_and_mixed_output_keeps_row_lineage(
    tmp_path: Path,
) -> None:
    public_dir = tmp_path / "public"
    output_dir = tmp_path / "stocks"
    public_dir.mkdir()
    pl.DataFrame(
        {
            "date": [date(2004, 2, 11)],
            "證券代號": ["2330"],
            "證券名稱": ["台積電"],
            "開盤價": [20.0],
            "最高價": [21.0],
            "最低價": [19.0],
            "收盤價": [20.0],
            "成交股數": [2000.0],
            "漲跌(+/-)": ["+"],
            "漲跌價差": [1.0],
        }
    ).write_parquet(public_dir / "twse_daily_ohlcv.parquet")
    pl.DataFrame(
        schema={
            "date": pl.Date,
            "代號": pl.String,
            "名稱": pl.String,
            "開盤": pl.Float64,
            "最高": pl.Float64,
            "最低": pl.Float64,
            "收盤": pl.Float64,
            "成交股數": pl.Float64,
            "漲跌": pl.String,
        }
    ).write_parquet(public_dir / "tpex_daily_ohlcv.parquet")
    pl.DataFrame(
        schema={
            "date": pl.Date,
            "symbol": pl.String,
            "reference_price": pl.Float64,
            "opening_reference_price": pl.Float64,
        }
    ).write_parquet(public_dir / "tw_corporate_action_reference.parquet")
    fallback = tmp_path / "yahoo_fallback.parquet"
    pl.DataFrame(
        {
            "date": [date(2000, 1, 4), date(2004, 2, 11)],
            "symbol": ["2330", "2330"],
            "name": ["TSMC", "TSMC"],
            "market": ["twse", "twse"],
            "open": [10.0, 999.0],
            "max": [10.5, 999.0],
            "min": [9.5, 999.0],
            "close": [10.0, 999.0],
            "Trading_Volume": [1000.0, 999.0],
            "source_adjclose": [5.0, 500.0],
            "source_factor": [None, 100.0],
            "quote_source": ["yahoo_fallback", "yahoo_fallback"],
        }
    ).write_parquet(fallback)

    frame, _, _, _ = _official_frame(public_dir, fallback_paths=[fallback])
    official_row = frame.filter(pl.col("date") == date(2004, 2, 11)).row(0, named=True)
    assert official_row["close"] == 20.0
    assert official_row["quote_source"] == "twse_official"

    result = _write_symbol(
        output_dir,
        "2330",
        frame,
        requested_end_date="2004-02-11",
        dry_run=False,
    )
    assert result.status == "created", result.message
    assert result.fallback_rows == 1
    assert result.missing_adjustment_rows == 0
    output = pl.read_parquet(output_dir / "2330_features.parquet")
    assert output["data_source"].to_list() == ["yahoo_fallback", "twse_official"]
    assert output["adjustment_source"].to_list() == [
        "yahoo_fallback",
        "twse_official",
    ]
    metadata = pq.read_schema(output_dir / "2330_features.parquet").metadata or {}
    assert metadata[b"stockagent.source"] == MIXED_FALLBACK_SOURCE_NAME.encode()


def test_official_ohlcv_can_use_yahoo_factor_only_when_official_factor_is_missing(
    tmp_path: Path,
) -> None:
    frame = pl.DataFrame(
        {
            "date": [date(2000, 1, 4), date(2000, 1, 5)],
            "symbol": ["2330", "2330"],
            "name": ["TSMC", "TSMC"],
            "market": ["twse", "twse"],
            "open": [100.0, 105.0],
            "max": [101.0, 106.0],
            "min": [99.0, 104.0],
            "close": [100.0, 105.0],
            "Trading_Volume": [1000.0, 1200.0],
            "signed_change": [None, None],
            "source_reference": [None, None],
            "source_adjclose": [50.0, None],
            "source_factor": [None, None],
            "quote_source": ["yahoo_fallback", "twse_official"],
            "_legacy_source_id": [0, -1],
            "_source_priority": [0, 2],
            "_yahoo_fallback_factor": [None, 1.05],
            "reference_override": [None, None],
            "security_type": ["stock", "stock"],
        }
    )

    result = _write_symbol(
        tmp_path,
        "2330",
        frame,
        requested_end_date="2000-01-05",
        dry_run=False,
    )
    output = pl.read_parquet(tmp_path / "2330_features.parquet")

    assert result.missing_adjustment_rows == 0
    assert result.fallback_adjustment_rows == 1
    assert output["data_source"].to_list() == ["yahoo_fallback", "twse_official"]
    assert output["adjustment_source"].to_list() == [
        "yahoo_fallback",
        "yahoo_fallback",
    ]
    assert math.isclose(output["adjclose"][1] / output["adjclose"][0], 1.05)

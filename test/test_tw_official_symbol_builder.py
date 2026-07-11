from __future__ import annotations

from datetime import date
import math
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow.parquet as pq
import pytest

from scripts.build_tw_official_symbol_parquets import (
    _legacy_official_frame,
    _normalized_reference_index,
    _source_adjustment_factors,
    _write_official_quote_parquet,
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

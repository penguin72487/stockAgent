from __future__ import annotations

import io
import math
import zipfile
from dataclasses import dataclass

import polars as pl
import pytest

from downloader import okx_historical_features as features


def test_catalog_never_includes_snapshot_only_source() -> None:
    included = {
        "included",
        "included_existing",
        "included_limited_history",
    }
    for item in features.HISTORICAL_FEATURE_CATALOG:
        if item["download_status"] in included:
            assert "snapshot" not in item["category"]
            assert item["download_status"] != "excluded_snapshot"


def test_normalize_price_candles_keeps_only_completed_requested_rows() -> None:
    rows = [
        ["900000", "1", "3", "0.5", "2", "1"],
        ["1800000", "2", "4", "1", "3", "0"],
        ["2700000", "3", "5", "2", "4", "1"],
    ]

    frame = features._normalize_price_candles(
        rows,
        prefix="okx_mark",
        start_ms=900000,
        end_ms=1800000,
    )

    assert frame.height == 1
    assert frame["okx_mark_close"].to_list() == [2.0]


def test_funding_asof_uses_only_events_available_by_bar_close() -> None:
    base = pl.DataFrame(
        {
            "date": [
                "2026-01-01 00:00:00",
                "2026-01-01 00:15:00",
                "2026-01-01 00:30:00",
            ]
        }
    )
    event_ms = int(
        features.datetime(2026, 1, 1, 0, 20, tzinfo=features.timezone.utc).timestamp()
        * 1000
    )
    events = [
        {
            "ts": event_ms,
            "okx_funding_rate_at_settlement": 0.001,
            "okx_funding_realized_rate": 0.002,
            "okx_funding_interval_hours": 8.0,
            "okx_funding_formula_with_rate": 1.0,
            "okx_funding_method_current_period": 1.0,
        }
    ]

    materialized = features._materialize_funding_asof(
        base,
        events,
        start_ms=event_ms - 60 * 60 * 1000,
        end_ms=event_ms + 60 * 60 * 1000,
    )

    assert materialized["okx_funding_realized_rate"].to_list() == [
        None,
        pytest.approx(0.002),
        pytest.approx(0.002),
    ]
    assert materialized["okx_funding_age_hours"].to_list()[0] is None
    assert materialized["okx_funding_age_hours"].to_list()[1] == pytest.approx(
        10 / 60
    )


def test_derived_features_are_normalized_and_gap_aware() -> None:
    frame = pl.DataFrame(
        {
            "date": [
                "2026-01-01 00:00:00",
                "2026-01-01 00:15:00",
                "2026-01-01 01:00:00",
            ],
            "close": [100.0, 102.0, 110.0],
            "Trading_Volume": [10.0, 20.0, 30.0],
            "okx_mark_open": [100.0, 101.0, 109.0],
            "okx_mark_high": [102.0, 104.0, 111.0],
            "okx_mark_low": [99.0, 100.0, 108.0],
            "okx_mark_close": [101.0, 103.0, 110.0],
            "okx_index_open": [100.0, 100.5, 108.0],
            "okx_index_high": [101.0, 103.0, 110.0],
            "okx_index_low": [99.0, 100.0, 107.0],
            "okx_index_close": [100.5, 102.0, 109.0],
            "okx_open_interest_contracts": [1000.0, 1100.0, 1200.0],
            "okx_open_interest_usd": [100000.0, 120000.0, 130000.0],
            "okx_taker_sell_volume_contracts": [40.0, 25.0, 20.0],
            "okx_taker_buy_volume_contracts": [60.0, 75.0, 20.0],
        }
    )

    derived = features._add_derived_features(frame)

    assert derived["okx_taker_imbalance"].to_list() == pytest.approx(
        [0.2, 0.5, 0.0]
    )
    assert derived["okx_open_interest_usd_log_change_15m"][1] == pytest.approx(
        math.log(1.2)
    )
    assert derived["okx_open_interest_usd_log_change_15m"][2] is None
    assert derived["okx_mark_index_basis_log"][0] == pytest.approx(
        math.log(101.0 / 100.5)
    )


@dataclass
class _ArchiveClient:
    zip_bytes: bytes

    def get(self, path: str, params: dict) -> dict:
        assert path == features.MARKET_DATA_HISTORY_ENDPOINT
        return {
            "data": [
                {
                    "details": [
                        {
                            "groupDetails": [
                                {
                                    "filename": "BTC-USDT-SWAP-fundingrates-2026-01.zip",
                                    "url": "https://example.invalid/funding.zip",
                                }
                            ]
                        }
                    ]
                }
            ]
        }

    def get_bytes(self, url: str) -> bytes:
        assert url == "https://example.invalid/funding.zip"
        return self.zip_bytes


def test_funding_archive_parser_filters_symbol_and_range() -> None:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(
            "funding.csv",
            "instrument_name,funding_rate,funding_time\n"
            "BTC-USDT-SWAP,0.001,1767225600000\n"
            "ETH-USDT-SWAP,0.002,1767225600000\n",
        )
    client = _ArchiveClient(buffer.getvalue())

    rows = features._fetch_funding_archive(
        client,
        inst_family="BTC-USDT",
        okx_symbol="BTC-USDT-SWAP",
        start_ms=1767225500000,
        end_ms=1767225700000,
    )

    assert len(rows) == 1
    assert rows[0]["realizedRate"] == "0.001"

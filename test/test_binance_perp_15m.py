from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from downloader import download_binance_perp_15m as binance
from downloader import binance_historical_features as features


def _raw_candle(open_time: int, *, close: str = "101") -> list[object]:
    return [
        open_time,
        "100",
        "102",
        "99",
        close,
        "12.5",
        open_time + binance.CANDLE_INTERVAL_MS - 1,
        "1262.5",
        42,
        "7.0",
        "707.0",
        "0",
    ]


def test_binance_kline_limit_uses_best_documented_rows_per_weight_tier() -> None:
    alternatives = {
        99: 1,
        499: 2,
        1000: 5,
        1500: 10,
    }
    efficiency = {limit: limit / weight for limit, weight in alternatives.items()}

    assert binance.KLINE_LIMIT == 499
    assert efficiency[binance.KLINE_LIMIT] == max(efficiency.values())
    assert binance.KLINE_REQUEST_WEIGHT == alternatives[binance.KLINE_LIMIT]


def test_binance_symbols_preserve_unicode_but_reject_path_components() -> None:
    assert binance._safe_symbol("币安人生USDT") == "币安人生USDT"
    with pytest.raises(ValueError, match="unsafe Binance symbol"):
        binance._safe_symbol("../BTCUSDT")


def test_binance_runtime_exchange_limit_is_authoritative() -> None:
    payload = {
        "rateLimits": [
            {
                "rateLimitType": "REQUEST_WEIGHT",
                "interval": "MINUTE",
                "intervalNum": 1,
                "limit": 2400,
            }
        ]
    }
    faster = binance.BinanceClient(
        requested_weight_per_minute=9999,
        max_retries=0,
        retry_base=0.1,
    )
    faster.configure_exchange_limits(payload)
    slower = binance.BinanceClient(
        requested_weight_per_minute=1200,
        max_retries=0,
        retry_base=0.1,
    )
    slower.configure_exchange_limits(payload)

    assert faster.weight_per_minute == 2400
    assert faster.limiter.interval_seconds == pytest.approx(60 / 2400)
    assert slower.weight_per_minute == 1200
    assert slower.limiter.interval_seconds == pytest.approx(60 / 1200)


def test_normalized_binance_candles_keep_trade_and_taker_fields() -> None:
    frame = binance._normalize_candles([_raw_candle(1_700_000_100_000)])

    assert frame.columns == binance.OUTPUT_COLUMNS
    assert frame["Trading_Volume"].item() == 12.5
    assert frame["binance_volume_quote"].item() == 1262.5
    assert frame["binance_trade_count"].item() == 42
    assert frame["binance_taker_buy_base_volume"].item() == 7.0


def test_binance_candle_validation_fails_closed_on_impossible_ohlc() -> None:
    row = _raw_candle(1_700_000_100_000)
    row[2] = "98"
    with pytest.raises(ValueError, match="invalid OHLCV"):
        binance._normalize_candles([row])


def test_binance_overlap_replaces_tail_and_preserves_prior_features() -> None:
    existing = binance._normalize_candles(
        [_raw_candle(1_700_000_100_000, close="101")]
    ).with_columns(pl.lit(0.001).alias("prior_funding_feature"))
    fresh = binance._normalize_candles([_raw_candle(1_700_000_100_000, close="101.5")])

    merged, changed = binance._merge_existing_with_fresh(
        existing,
        fresh,
        1_700_000_100_000,
    )

    assert changed
    assert merged["close"].item() == 101.5
    assert merged["prior_funding_feature"].item() == 0.001


def test_binance_symbol_download_paginates_and_publishes_atomically(
    tmp_path: Path,
) -> None:
    start_ms = binance._date_to_ms("2026-01-01", end_of_day=False)

    class FakeClient:
        def get(self, path: str, params: dict[str, object], *, weight: float):
            assert path == binance.KLINE_ENDPOINT
            assert params["limit"] == str(binance.KLINE_LIMIT)
            assert weight == binance.KLINE_REQUEST_WEIGHT
            return [
                _raw_candle(start_ms),
                _raw_candle(start_ms + binance.CANDLE_INTERVAL_MS),
            ]

    record = binance.SymbolRecord(
        code="BTCUSDT",
        name="BTCUSDT",
        market="binance_usdm_perp",
        binance_symbol="BTCUSDT",
        pair="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        margin_asset="USDT",
        contract_type="PERPETUAL",
        status="TRADING",
        onboard_time=None,
    )
    result = binance._download_symbol(
        FakeClient(),
        record,
        tmp_path,
        start_ms=start_ms,
        end_ms=start_ms + binance.CANDLE_INTERVAL_MS,
        mode="incremental",
        refresh=False,
    )

    output = tmp_path / "BTCUSDT_features.parquet"
    assert result.status == "updated"
    assert result.rows == 2
    assert output.exists()
    assert not list(tmp_path.glob("*.tmp"))
    assert pl.read_parquet(output).height == 2


def test_binance_funding_asof_never_uses_a_future_settlement() -> None:
    start_ms = 1_700_000_100_000
    frame = pl.DataFrame(
        {
            "date": [
                features._ms_to_date_string(start_ms),
                features._ms_to_date_string(start_ms + features.CANDLE_INTERVAL_MS),
            ]
        }
    )
    events = [
        {
            "ts": start_ms + features.CANDLE_INTERVAL_MS,
            "binance_funding_rate": 0.001,
            "binance_funding_mark_price": 100.0,
            "binance_funding_interval_hours": 8.0,
        },
        {
            "ts": start_ms + 3 * features.CANDLE_INTERVAL_MS,
            "binance_funding_rate": 0.009,
            "binance_funding_mark_price": 999.0,
            "binance_funding_interval_hours": 8.0,
        },
    ]

    result = features._materialize_funding_asof(
        frame,
        events,
        start_ms=start_ms,
        end_ms=start_ms + features.CANDLE_INTERVAL_MS,
    )

    assert result["binance_funding_rate"].to_list() == [0.001, 0.001]
    assert result["binance_funding_age_hours"].to_list() == [0.0, pytest.approx(1 / 60)]


def test_binance_historical_features_join_every_public_family_causally(
    tmp_path: Path,
) -> None:
    start_ms = 1_780_000_000_000
    raw_candles = [
        _raw_candle(start_ms + index * binance.CANDLE_INTERVAL_MS) for index in range(4)
    ]
    output = tmp_path / "BTCUSDT_features.parquet"
    binance._normalize_candles(raw_candles).write_parquet(output)
    request_starts: list[tuple[str, int]] = []

    class FakeClient:
        def get(self, path: str, params: dict[str, object], *, weight: float):
            request_starts.append((path, int(params["startTime"])))
            if path in {
                features.MARK_PRICE_KLINES_ENDPOINT,
                features.INDEX_PRICE_KLINES_ENDPOINT,
                features.PREMIUM_INDEX_KLINES_ENDPOINT,
            }:
                return [
                    [
                        start_ms + index * features.CANDLE_INTERVAL_MS,
                        "100",
                        "102",
                        "99",
                        str(101 + index),
                    ]
                    for index in range(4)
                ]
            if path == features.FUNDING_RATE_ENDPOINT:
                return [
                    {
                        "fundingTime": start_ms + features.CANDLE_INTERVAL_MS,
                        "fundingRate": "0.0001",
                        "markPrice": "101",
                    },
                    {
                        "fundingTime": start_ms + 3 * features.CANDLE_INTERVAL_MS,
                        "fundingRate": "0.0002",
                        "markPrice": "103",
                    },
                ]
            timestamp = start_ms + features.CANDLE_INTERVAL_MS
            if path == features.OPEN_INTEREST_ENDPOINT:
                return [
                    {
                        "timestamp": timestamp,
                        "sumOpenInterest": "10",
                        "sumOpenInterestValue": "1000",
                        "CMCCirculatingSupply": "20",
                    }
                ]
            if path in {
                features.GLOBAL_RATIO_ENDPOINT,
                features.TOP_ACCOUNT_RATIO_ENDPOINT,
                features.TOP_POSITION_RATIO_ENDPOINT,
            }:
                return [
                    {
                        "timestamp": timestamp,
                        "longAccount": "0.6",
                        "shortAccount": "0.4",
                        "longShortRatio": "1.5",
                    }
                ]
            if path == features.TAKER_RATIO_ENDPOINT:
                return [
                    {
                        "timestamp": timestamp,
                        "buyVol": "12",
                        "sellVol": "8",
                        "buySellRatio": "1.5",
                    }
                ]
            if path == features.BASIS_ENDPOINT:
                return [
                    {
                        "timestamp": timestamp,
                        "indexPrice": "100",
                        "futuresPrice": "101",
                        "basis": "1",
                        "basisRate": "0.01",
                        "annualizedBasisRate": "",
                    }
                ]
            raise AssertionError(path)

    record = SimpleNamespace(code="BTCUSDT", binance_symbol="BTCUSDT", pair="BTCUSDT")
    result = features.enrich_symbol_historical_features(
        FakeClient(),
        record,
        output,
        start_ms=start_ms,
        end_ms=start_ms + 3 * features.CANDLE_INTERVAL_MS,
        observed_at_ms=start_ms + 4 * features.CANDLE_INTERVAL_MS,
    )

    enriched = pl.read_parquet(output)
    assert result.status == "updated"
    assert not json.loads(result.errors_json)
    assert enriched["binance_mark_close"].to_list() == [101.0, 102.0, 103.0, 104.0]
    assert enriched["binance_funding_rate"].to_list() == [
        0.0001,
        0.0001,
        0.0002,
        0.0002,
    ]
    assert enriched["binance_open_interest_value_usd"].to_list() == [
        None,
        1000.0,
        None,
        None,
    ]
    assert enriched["binance_taker_imbalance"].to_list()[1] == pytest.approx(0.2)
    assert enriched["binance_basis_annualized_rate"].null_count() == 4
    rolling_floor = (
        start_ms + 4 * features.CANDLE_INTERVAL_MS - features.SHORT_HISTORY_RETENTION_MS
    )
    assert all(
        start >= rolling_floor
        for path, start in request_starts
        if path.startswith("/futures/data/")
    )


def test_binance_pipeline_progress_persists_measured_eta(tmp_path: Path) -> None:
    path = tmp_path / "progress.json"
    total_units = 2 * (1 + len(features.FEATURE_STAGE_IDS))
    tracker = binance._PipelineProgress(
        path,
        total_units=total_units,
        started_at=datetime.now(timezone.utc),
    )
    tracker.update("candles", "updated", item="BTCUSDT")
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["state"] == "running"
    assert payload["current"] == 1
    assert payload["total"] == total_units
    assert payload["unit"] == "request-page-or-feature-stage"
    assert payload["remaining_seconds"] is not None
    assert payload["recent_errors"] == []

    tracker.update(
        "candles",
        "failed",
        item="BROKENUSDT",
        message="HTTPError: 429",
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["recent_errors"] == [
        {
            "phase": "candles",
            "item": "BROKENUSDT",
            "status": "failed",
            "message": "HTTPError: 429",
        }
    ]

    tracker.finish(failed=False)
    assert json.loads(path.read_text(encoding="utf-8"))["state"] == "complete"

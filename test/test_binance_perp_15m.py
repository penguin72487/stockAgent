from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from downloader import download_binance_perp_15m as binance


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

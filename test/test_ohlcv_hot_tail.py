from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace

import polars as pl

from downloader import download_binance_perp_15m as binance
from downloader import download_bybit_perp_daily as bybit
from downloader import download_okx_perp_daily as okx
from downloader.ohlcv_hot_tail import hot_tail_path, read_logical_parquet


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _binance_raw(open_time: int, close: str = "101") -> list[object]:
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


def test_logical_reader_merges_hot_tail_without_losing_base_features(
    tmp_path: Path,
) -> None:
    base = tmp_path / "BTCUSDT_features.parquet"
    pl.DataFrame(
        {
            "date": ["2026-01-01 00:00:00", "2026-01-01 00:01:00"],
            "close": [100.0, 101.0],
            "historical_feature": [0.1, 0.2],
        }
    ).write_parquet(base, statistics=True)
    tail = hot_tail_path(base)
    tail.parent.mkdir(parents=True)
    pl.DataFrame(
        {
            "date": ["2026-01-01 00:01:00", "2026-01-01 00:02:00"],
            "close": [101.5, 102.0],
        }
    ).write_parquet(tail, statistics=True)

    logical = read_logical_parquet(base)
    recent = read_logical_parquet(base, tail_rows=2)

    assert logical.height == 3
    assert logical["close"].to_list() == [100.0, 101.5, 102.0]
    assert logical["historical_feature"].to_list() == [0.1, 0.2, None]
    assert recent["date"].to_list() == [
        "2026-01-01 00:01:00",
        "2026-01-01 00:02:00",
    ]


def test_binance_tail_only_keeps_base_immutable_and_non_tail_compacts(
    tmp_path: Path,
) -> None:
    start_ms = binance._date_to_ms("2026-01-01", end_of_day=False)
    output = tmp_path / "BTCUSDT_features.parquet"
    binance._normalize_candles(
        [_binance_raw(start_ms), _binance_raw(start_ms + 60_000)]
    ).write_parquet(output, statistics=True)
    base_hash = _sha256(output)

    class FakeClient:
        def get(self, *_args, **_kwargs):
            return [
                _binance_raw(start_ms + 60_000, close="101.5"),
                _binance_raw(start_ms + 120_000, close="102"),
            ]

    record = SimpleNamespace(
        code="BTCUSDT",
        binance_symbol="BTCUSDT",
        market="binance_usdm_perp",
        onboard_time=None,
    )
    tail_result = binance._download_symbol(
        FakeClient(),
        record,
        tmp_path,
        start_ms=start_ms,
        end_ms=start_ms + 120_000,
        mode="incremental",
        refresh=False,
        tail_only=True,
    )

    assert tail_result.status == "updated"
    assert tail_result.rows == 3
    assert _sha256(output) == base_hash
    assert hot_tail_path(output).is_file()
    assert read_logical_parquet(output)["close"].to_list() == [101.0, 101.5, 102.0]

    compacted = binance._download_symbol(
        FakeClient(),
        record,
        tmp_path,
        start_ms=start_ms,
        end_ms=start_ms + 120_000,
        mode="incremental",
        refresh=False,
        tail_only=False,
    )

    assert compacted.status == "updated"
    assert not hot_tail_path(output).exists()
    assert pl.read_parquet(output).height == 3


def test_okx_tail_only_writes_only_hot_rows(tmp_path: Path) -> None:
    start_ms = okx._date_to_ms("2026-01-01", end_of_day=False)
    output = tmp_path / "BTCUSDTSWAP_features.parquet"
    base_rows = [
        [str(start_ms), "100", "102", "99", "101", "1", "1", "100", "1"],
        [str(start_ms + 60_000), "101", "103", "100", "102", "1", "1", "101", "1"],
    ]
    okx._write_parquet(okx._normalize_candles(base_rows), output)
    base_hash = _sha256(output)

    class FakeClient:
        calls = 0

        def get(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls > 1:
                return {"data": []}
            return {
                "data": [
                    [str(start_ms + 120_000), "102", "104", "101", "103", "1", "1", "102", "1"],
                    [str(start_ms + 60_000), "101", "103", "100", "102.5", "1", "1", "101", "1"],
                ]
            }

    record = SimpleNamespace(
        code="BTCUSDTSWAP",
        okx_symbol="BTC-USDT-SWAP",
        market="okx_swap",
        list_time=None,
    )
    result = okx._download_symbol_1m(
        FakeClient(),
        record,
        tmp_path,
        start_ms,
        start_ms + 120_000,
        "incremental",
        False,
        tail_only=True,
    )

    assert result.status == "updated"
    assert result.rows == 3
    assert _sha256(output) == base_hash
    assert hot_tail_path(output).is_file()


def test_bybit_tail_only_writes_only_hot_rows(tmp_path: Path) -> None:
    start_ms = bybit._date_to_ms("2026-01-01", end_of_day=False)
    output = tmp_path / "BTCUSDT_features.parquet"
    base_rows = [
        [str(start_ms), "100", "102", "99", "101", "1", "100"],
        [str(start_ms + 60_000), "101", "103", "100", "102", "1", "101"],
    ]
    bybit._write_parquet(bybit._normalize_candles(base_rows), output)
    base_hash = _sha256(output)

    class FakeClient:
        def get(self, *_args, **_kwargs):
            return {
                "result": {
                    "list": [
                        [str(start_ms + 120_000), "102", "104", "101", "103", "1", "102"],
                        [str(start_ms + 60_000), "101", "103", "100", "102.5", "1", "101"],
                    ]
                }
            }

    record = SimpleNamespace(
        code="BTCUSDT",
        bybit_symbol="BTCUSDT",
        market="bybit_linear_perp",
        category="linear",
        launch_time=None,
    )
    result = bybit._download_symbol_1m(
        FakeClient(),
        record,
        tmp_path,
        start_ms,
        start_ms + 120_000,
        "incremental",
        False,
        tail_only=True,
    )

    assert result.status == "updated"
    assert result.rows == 3
    assert _sha256(output) == base_hash
    assert hot_tail_path(output).is_file()


def test_bybit_tail_refresh_uses_footer_without_rescanning_historical_gaps(
    tmp_path: Path,
    monkeypatch,
) -> None:
    path = tmp_path / "GAPPEDUSDT_features.parquet"
    pl.DataFrame(
        {
            "date": [
                "2026-01-01 00:00:00",
                "2026-01-01 00:01:00",
                "2026-01-01 00:03:00",
            ],
            "close": [1.0, 1.1, 1.2],
        }
    ).write_parquet(path, statistics=True)
    monkeypatch.setattr(
        bybit,
        "_read_date_column",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("tail refresh must not rescan full historical timestamps")
        ),
    )

    info = bybit._load_existing_candle_info(path, require_contiguous=False)

    assert info.rows == 3
    assert info.interval_ok is True
    assert info.latest_ms == info.earliest_ms + 3 * 60_000

from __future__ import annotations

from datetime import date

import polars as pl
import pyarrow.parquet as pq

from downloader.download_alpaca_us_ohlcv import (
    AlpacaClient,
    PreparedRecord,
    SymbolRecord,
    _alpaca_symbol,
    _apply_batch,
    _bars_to_frame,
    _prepare_records,
    _write_parquet_atomic,
)


def _bar(day: str, close: float, *, volume: float = 1000.0) -> dict[str, object]:
    return {
        "t": f"{day}T04:00:00Z",
        "o": close - 1.0,
        "h": close + 1.0,
        "l": close - 2.0,
        "c": close,
        "v": volume,
    }


def test_alpaca_symbol_converts_class_share_but_not_warrant() -> None:
    assert _alpaca_symbol("BRK-B") == "BRK.B"
    assert _alpaca_symbol("BF-A") == "BF.A"
    assert _alpaca_symbol("ABCD-W") == "ABCD-W"


def test_bars_to_frame_normalizes_stockagent_schema() -> None:
    frame = _bars_to_frame(
        [_bar("2026-07-08", 100.0), _bar("2026-07-09", 101.5)],
        start_date=date(2026, 7, 8),
        end_date=date(2026, 7, 9),
    )

    assert frame.columns == ["date", "open", "max", "min", "close", "adjclose", "Trading_Volume"]
    assert frame.height == 2
    assert frame["close"].to_list() == [100.0, 101.5]
    assert frame["adjclose"].to_list() == [100.0, 101.5]


def test_alpaca_client_collects_all_pages(monkeypatch) -> None:
    client = AlpacaClient(
        api_key_id="test-key",
        api_secret_key="test-secret",
        requests_per_minute=200,
        timeout=10,
        retries=0,
        retry_backoff=0.1,
    )
    payloads = iter(
        [
            {"bars": {"AAPL": [_bar("2026-07-08", 100.0)]}, "next_page_token": "page-2"},
            {"bars": {"MSFT": [_bar("2026-07-08", 200.0)]}, "next_page_token": None},
        ]
    )
    seen_params: list[dict[str, str | int]] = []

    def fake_get(params):
        seen_params.append(dict(params))
        return next(payloads)

    monkeypatch.setattr(client, "_get", fake_get)
    bars, requests_made = client.fetch_bars(
        ["AAPL", "MSFT"],
        start_date=date(2026, 7, 8),
        end_date=date(2026, 7, 9),
        feed="sip",
        adjustment="raw",
    )

    assert requests_made == 2
    assert set(bars) == {"AAPL", "MSFT"}
    assert "page_token" not in seen_params[0]
    assert seen_params[1]["page_token"] == "page-2"
    assert seen_params[0]["symbols"] == "AAPL,MSFT"
    assert seen_params[0]["end"] == "2026-07-10"


def test_prepare_records_skips_delisted_without_active_symbol_remap(tmp_path) -> None:
    pending, results = _prepare_records(
        [SymbolRecord(code="OLD_DL", yahoo_symbol="OLD_DL"), SymbolRecord(code="AAPL", yahoo_symbol="AAPL")],
        output_dir=tmp_path,
        start_date=date(2000, 1, 1),
        end_date=date(2026, 7, 9),
        mode="daily-update",
        repair_overlap_days=7,
        daily_stale_max_lag_days=0,
    )

    assert [item.record.code for item in pending] == ["AAPL"]
    assert len(results) == 1
    assert results[0].code == "OLD_DL"
    assert results[0].status == "delisted_skip"


def test_apply_batch_repairs_overlap_and_writes_alpaca_metadata(tmp_path) -> None:
    output_path = tmp_path / "AAPL_features.parquet"
    existing = _bars_to_frame(
        [_bar("2026-07-08", 100.0), _bar("2026-07-09", 101.0)],
        start_date=date(2026, 7, 8),
        end_date=date(2026, 7, 9),
    )
    _write_parquet_atomic(existing, output_path, requested_end_date="2026-07-09")
    prepared = PreparedRecord(
        record=SymbolRecord(code="AAPL", yahoo_symbol="AAPL"),
        output_path=output_path,
        effective_start=date(2026, 7, 9),
    )

    results = _apply_batch(
        [prepared],
        {"AAPL": [_bar("2026-07-09", 102.0), _bar("2026-07-10", 103.0)]},
        {},
        end_date=date(2026, 7, 10),
    )

    output = pl.read_parquet(output_path)
    assert output["close"].to_list() == [100.0, 102.0, 103.0]
    assert results[0].status == "updated"
    assert results[0].last_date == "2026-07-10"
    metadata = pq.ParquetFile(output_path).schema_arrow.metadata or {}
    assert metadata[b"stockagent.source"] == b"alpaca"
    assert metadata[b"stockagent.provider"] == b"alpaca_market_data"


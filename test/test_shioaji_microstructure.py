from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path

import polars as pl

from downloader.build_tw_top_market_cap_universe import build_universe
from downloader.shioaji_capture_parts import select_capture_part_paths
from downloader.stream_shioaji_tw_microstructure import (
    BOOK_SCHEMA,
    PartWriter,
    normalize_book,
    normalize_tick,
)


class Payload:
    def __init__(self, values: dict[str, object]) -> None:
        self.values = values

    def to_dict(self) -> dict[str, object]:
        return dict(self.values)


def test_market_cap_universe_combines_twse_and_tpex_company_stocks() -> None:
    output = build_universe(
        [
            {
                "公司代號": "2330",
                "公司簡稱": "台積電",
                "已發行普通股數或TDR原股發行股數": "1000",
            },
            {
                "公司代號": "2317",
                "公司簡稱": "鴻海",
                "已發行普通股數或TDR原股發行股數": "2000",
            },
        ],
        [
            {"Date": "1150717", "Code": "2330", "ClosingPrice": "1000"},
            {"Date": "1150717", "Code": "2317", "ClosingPrice": "100"},
            # ETF/non-company quote must be excluded by the company master join.
            {"Date": "1150717", "Code": "0050", "ClosingPrice": "200"},
        ],
        [
            {
                "SecuritiesCompanyCode": "6488",
                "CompanyAbbreviation": "環球晶",
            }
        ],
        [
            {
                "Date": "1150717",
                "SecuritiesCompanyCode": "6488",
                "ClosePrice": "500",
                "Capitals": "1000",
                "MarketValue": "0.8",
            }
        ],
        count=2,
    )
    assert output["symbol"].to_list() == ["2330", "6488"]
    assert output["market_cap_rank"].to_list() == [1, 2]
    assert output["market"].to_list() == ["twse", "tpex"]


def test_tick_and_book_normalization_preserve_event_and_receive_time() -> None:
    event_dt = datetime(2026, 7, 20, 9, 0, 1, 123456)
    common = {
        "code": "2330",
        "date": date(2026, 7, 20),
        "time": time(9, 0, 1, 123456),
        "datetime": event_dt,
    }
    tick = normalize_tick(
        "TSE",
        Payload(
            {
                **common,
                "open": Decimal("1000"),
                "avg_price": Decimal("1001"),
                "close": Decimal("1002"),
                "high": Decimal("1003"),
                "low": Decimal("999"),
                "amount": Decimal("1002000"),
                "total_amount": Decimal("2000000"),
                "volume": 1,
                "total_volume": 2,
                "tick_type": 1,
                "chg_type": 2,
                "price_chg": Decimal("2"),
                "pct_chg": 20,
            }
        ),
        event_seq=7,
        worker_index=0,
        receive_ts_ns=10,
        receive_monotonic_ns=20,
    )
    assert tick["code"] == "2330"
    assert tick["close"] == 1002.0
    assert tick["event_seq"] == 7
    assert tick["receive_ts_ns"] == 10

    book = normalize_book(
        "TSE",
        Payload(
            {
                **common,
                "bid_price": [Decimal("1001"), Decimal("1000")],
                "bid_volume": [1, 2],
                "diff_bid_vol": [1, -1],
                "ask_price": [Decimal("1002"), Decimal("1003")],
                "ask_volume": [3, 4],
                "diff_ask_vol": [0, 2],
            }
        ),
        event_seq=8,
        worker_index=0,
        receive_ts_ns=11,
        receive_monotonic_ns=21,
    )
    assert book["bid_price_1"] == 1001.0
    assert book["ask_volume_2"] == 4
    assert book["bid_price_5"] is None


def test_part_writer_uses_atomic_partitioned_parquet(tmp_path: Path) -> None:
    writer = PartWriter(
        tmp_path,
        "book_events",
        BOOK_SCHEMA,
        worker_index=0,
        capture_id="capture_a",
        flush_rows=1,
        flush_seconds=60.0,
    )
    event_dt = datetime(2026, 7, 20, 9, 0, 1)
    row = normalize_book(
        "TSE",
        Payload(
            {
                "code": "2330",
                "date": date(2026, 7, 20),
                "time": event_dt.time(),
                "datetime": event_dt,
                "bid_price": [1000] * 5,
                "bid_volume": [1] * 5,
                "diff_bid_vol": [0] * 5,
                "ask_price": [1001] * 5,
                "ask_volume": [1] * 5,
                "diff_ask_vol": [0] * 5,
            }
        ),
        event_seq=1,
        worker_index=0,
        receive_ts_ns=1,
        receive_monotonic_ns=2,
    )
    writer.append(row)
    writer.maybe_flush()
    paths = list(tmp_path.rglob("*.parquet"))
    assert len(paths) == 1
    assert "trade_date=2026-07-20" in str(paths[0])
    assert "hour=09" in str(paths[0])
    assert paths[0].name.startswith("capture=capture_a-worker=00-")
    output = pl.read_parquet(paths[0])
    assert output.height == 1
    assert output["code"].item() == "2330"
    assert not list(tmp_path.rglob("*.tmp"))


def test_capture_part_selection_excludes_same_day_previous_run(tmp_path: Path) -> None:
    partition = tmp_path / "ticks" / "trade_date=2026-07-22" / "hour=09"
    partition.mkdir(parents=True)
    old = partition / "capture=old-worker=00-part=000001-1.parquet"
    current = partition / "capture=current-worker=00-part=000001-2.parquet"
    old.touch()
    current.touch()
    manifests = [
        {
            "schema_version": 3,
            "capture_id": "current",
            "worker_index": 0,
            "tick_parts": 1,
        }
    ]

    selected = select_capture_part_paths(
        capture_root=tmp_path,
        kind="ticks",
        trade_date="2026-07-22",
        manifests=manifests,
    )

    assert selected == [current]


def test_legacy_capture_part_selection_uses_manifest_write_window(
    tmp_path: Path,
) -> None:
    partition = tmp_path / "ticks" / "trade_date=2026-07-22" / "hour=09"
    partition.mkdir(parents=True)
    old = partition / "worker=00-part=000001-1784680200000000000.parquet"
    current = partition / "worker=00-part=000001-1784683800000000000.parquet"
    old.touch()
    current.touch()
    manifests = [
        {
            "schema_version": 2,
            "worker_index": 0,
            "tick_parts": 1,
            "started_at_utc": "2026-07-22T01:00:00+00:00",
            "finished_at_utc": "2026-07-22T02:00:00+00:00",
        }
    ]

    selected = select_capture_part_paths(
        capture_root=tmp_path,
        kind="ticks",
        trade_date="2026-07-22",
        manifests=manifests,
    )

    assert selected == [current]

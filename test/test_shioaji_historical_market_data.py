from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace

import polars as pl

from downloader.download_shioaji_historical_market_data import (
    RECEIPT_SCHEMA_VERSION,
    SOURCE,
    HistoryContract,
    _atomic_write_json,
    _kbar_frame,
    _kbar_paths,
    _tick_paths,
    _valid_receipt,
    build_tasks,
    observed_tick_dates,
    select_latest_option_infos,
)


def _option(
    code: str,
    root: str,
    expiry: date,
    strike: float,
    right: str,
) -> SimpleNamespace:
    values = {
        "code": code,
        "root": root,
        "delivery_date": expiry,
        "underlying_code": "IX0001",
        "strike_price": strike,
        "option_right": right,
    }
    return SimpleNamespace(dict=lambda: values)


def _row(
    *,
    collection: str,
    priority: int,
    code: str,
    security_type: str = "OPT",
    begin: date = date(2026, 8, 20),
    end: date = date(2026, 8, 28),
) -> HistoryContract:
    return HistoryContract(
        collection=collection,
        priority=priority,
        security_type=security_type,
        asset_class="index" if security_type == "IND" else "options",
        code=code,
        root="TX1" if collection == "latest_weekly_option" else "TXO",
        name=code,
        exchange="TAIFEX" if security_type != "IND" else "TSE",
        begin_date=begin,
        end_date=end,
    )


def _ns(value: datetime) -> int:
    return int((value - datetime(1970, 1, 1)).total_seconds() * 1_000_000_000)


def test_select_latest_weekly_and_monthly_keeps_every_strike_and_right() -> None:
    completed = date(2026, 8, 28)
    infos = [
        _option("W1C", "TX1", date(2026, 9, 2), 46000, "C"),
        _option("W1P", "TX1", date(2026, 9, 2), 46000, "P"),
        _option("W2C", "TX2", date(2026, 9, 9), 46000, "C"),
        _option("F1C", "TXU", date(2026, 9, 4), 46000, "C"),
        _option("M1C", "TXO", date(2026, 9, 16), 45000, "C"),
        _option("M1P", "TXO", date(2026, 9, 16), 45000, "P"),
        _option("M2C", "TXO", date(2026, 10, 21), 45000, "C"),
        _option("OLD", "TXY", completed, 45000, "C"),
    ]

    weekly, monthly = select_latest_option_infos(
        infos, completed_session=completed
    )

    assert {item.dict()["code"] for item in weekly} == {"W1C", "W1P"}
    assert {item.dict()["code"] for item in monthly} == {"M1C", "M1P"}


def test_fop_kbars_exclude_next_trading_date_partial_night_session() -> None:
    row = _row(
        collection="latest_weekly_option",
        priority=0,
        code="TX146000I6",
    )
    payload = SimpleNamespace(
        dict=lambda: {
            "ts": [
                _ns(datetime(2026, 8, 28, 13, 0)),
                _ns(datetime(2026, 8, 28, 15, 0)),
            ],
            "Open": [1.0, 2.0],
            "High": [1.0, 2.0],
            "Low": [1.0, 2.0],
            "Close": [1.0, 2.0],
            "Volume": [1, 1],
            "Amount": [1.0, 2.0],
        }
    )

    frame, trading_dates = _kbar_frame(payload, row=row)

    assert frame.height == 1
    assert trading_dates == [date(2026, 8, 28)]
    assert frame.item(0, "Close") == 1.0


def test_tick_targets_derive_only_from_verified_kbar_receipts(tmp_path: Path) -> None:
    row = _row(
        collection="latest_weekly_option",
        priority=0,
        code="WEEK",
    )
    data_path, receipt_path = _kbar_paths(
        tmp_path, row, row.begin_date, row.end_date
    )
    data_path.parent.mkdir(parents=True)
    pl.DataFrame({"ts": [1]}).write_parquet(data_path)
    _atomic_write_json(
        receipt_path,
        {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "source": SOURCE,
            "status": "complete",
            "method": "kbars",
            "contract": row.code,
            "rows": 1,
            "observed_trading_dates": ["2026-08-27", "2026-08-28"],
            "sha256": __import__("hashlib").sha256(data_path.read_bytes()).hexdigest(),
        },
    )

    assert observed_tick_dates(tmp_path, row, chunk_days=29) == [
        date(2026, 8, 27),
        date(2026, 8, 28),
    ]
    tasks = build_tasks(tmp_path, [row], chunk_days=29)
    assert [(task.method, task.start) for task in tasks] == [
        ("ticks", date(2026, 8, 28)),
        ("ticks", date(2026, 8, 27)),
    ]


def test_task_order_is_collection_priority_then_newest(tmp_path: Path) -> None:
    weekly = _row(
        collection="latest_weekly_option",
        priority=0,
        code="WEEK",
    )
    monthly = _row(
        collection="latest_monthly_option",
        priority=1,
        code="MONTH",
        begin=date(2026, 7, 1),
    )

    tasks = build_tasks(tmp_path, [monthly, weekly], chunk_days=29)

    assert tasks[0].contract.code == "WEEK"
    monthly_ends = [task.end for task in tasks if task.contract.code == "MONTH"]
    assert monthly_ends == sorted(monthly_ends, reverse=True)


def test_source_empty_receipt_is_terminal_without_fake_parquet(tmp_path: Path) -> None:
    row = _row(
        collection="indices",
        priority=3,
        code="IX0001",
        security_type="IND",
        begin=date(2020, 3, 2),
        end=date(2020, 3, 2),
    )
    data_path, receipt_path = _kbar_paths(
        tmp_path, row, row.begin_date, row.end_date
    )
    _atomic_write_json(
        receipt_path,
        {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "source": SOURCE,
            "status": "source_empty",
            "method": "kbars",
            "contract": row.code,
            "rows": 0,
            "observed_trading_dates": [],
        },
    )

    assert _valid_receipt(
        receipt_path, data_path, method="kbars", code=row.code
    ) is not None
    assert build_tasks(tmp_path, [row], chunk_days=29) == []
    assert not data_path.exists()


def test_corrupt_tick_artifact_invalidates_receipt(tmp_path: Path) -> None:
    row = _row(
        collection="latest_weekly_option",
        priority=0,
        code="WEEK",
    )
    data_path, receipt_path = _tick_paths(tmp_path, row, date(2026, 8, 28))
    data_path.parent.mkdir(parents=True)
    data_path.write_bytes(b"not parquet")
    _atomic_write_json(
        receipt_path,
        {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "source": SOURCE,
            "status": "complete",
            "method": "ticks",
            "contract": row.code,
            "sha256": "wrong",
        },
    )

    assert _valid_receipt(
        receipt_path, data_path, method="ticks", code=row.code
    ) is None

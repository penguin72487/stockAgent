from __future__ import annotations

from datetime import date
import hashlib
import json
from pathlib import Path
import sys

import polars as pl
import pytest

from downloader.download_shioaji_tw_kbars import (
    UniverseRow,
    _chunk_paths,
    _completed_daily_result,
    aggregate_daily,
    iter_date_chunks,
    normalize_kbars,
)
from scripts.build_tw_shioaji_dataset import merge_symbol_frames
from scripts import build_tw_shioaji_dataset as dataset_builder


def test_shioaji_date_chunks_are_inclusive_and_never_exceed_30_days() -> None:
    chunks = list(iter_date_chunks(date(2020, 3, 2), date(2020, 5, 5), 30))
    assert chunks == [
        (date(2020, 3, 2), date(2020, 3, 31)),
        (date(2020, 4, 1), date(2020, 4, 30)),
        (date(2020, 5, 1), date(2020, 5, 5)),
    ]
    with pytest.raises(ValueError, match="between 1 and 30"):
        list(iter_date_chunks(date(2020, 3, 2), date(2020, 3, 3), 31))


def test_shioaji_chunk_storage_is_daily_only(tmp_path: Path) -> None:
    chunk_path, receipt_path = _chunk_paths(
        tmp_path,
        "2330",
        date(2020, 3, 2),
        date(2020, 3, 31),
    )
    assert chunk_path == (
        tmp_path / "daily_chunks" / "2330" / "2020-03-02_2020-03-31.parquet"
    )
    assert receipt_path == chunk_path.with_suffix(".receipt.json")
    assert "raw" not in chunk_path.parts


def test_shioaji_kbars_normalize_and_aggregate_volume_lots_to_shares() -> None:
    payload = {
        "ts": [1583110860000000000, 1583110920000000000],
        "Open": [100.0, 101.0],
        "High": [102.0, 103.0],
        "Low": [99.0, 100.0],
        "Close": [101.0, 102.0],
        "Volume": [2, 3],
        "Amount": [202_000.0, 306_000.0],
    }
    minute = normalize_kbars(
        payload,
        symbol="2330",
        market="twse",
        contract_unit=1000.0,
    )
    daily = aggregate_daily(minute, name="台積電")
    assert daily.to_dicts() == [
        {
            "date": date(2020, 3, 2),
            "symbol": "2330",
            "name": "台積電",
            "market": "twse",
            "open": 100.0,
            "max": 103.0,
            "min": 99.0,
            "close": 102.0,
            "Trading_Volume": 5000.0,
            "Trading_Value": 508000.0,
            "shioaji_volume_lots": 5.0,
            "shioaji_minute_bars": 2,
            "data_source": "shioaji_kbars_1m",
        }
    ]


def test_shioaji_daily_aggregation_infers_historical_volume_unit_from_amount() -> None:
    minute = normalize_kbars(
        {
            "ts": [1583110860000000000],
            "Open": [100.0],
            "High": [101.0],
            "Low": [99.0],
            "Close": [100.0],
            "Volume": [2],
            "Amount": [200_000.0],
        },
        symbol="2330",
        market="twse",
        contract_unit=1.0,
    )
    daily = aggregate_daily(minute, name="台積電")
    assert daily["Trading_Volume"].item() == 2000.0
    assert daily["shioaji_volume_lots"].item() == 2000.0


def _base_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "date": [
                date(2020, 2, 27),
                date(2020, 3, 2),
                date(2020, 3, 3),
                date(2020, 3, 4),
            ],
            "open": [99.0, 100.0, 50.0, 51.0],
            "max": [101.0, 102.0, 52.0, 53.0],
            "min": [98.0, 99.0, 49.0, 50.0],
            "close": [100.0, 100.0, 50.0, 52.0],
            "Trading_Volume": [1000.0, 2000.0, 3000.0, 4000.0],
            # 2-for-1 split on 2020-03-03: adjusted return is flat.
            "adjclose": [10.0, 10.0, 10.0, 10.4],
            "data_source": ["twse_official"] * 4,
            "fallback_reason": [None] * 4,
            "adjustment_source": ["twse_official"] * 4,
            "return_quarantined": [False] * 4,
            "return_quarantine_reason": [None] * 4,
        }
    )


def _shioaji_frame(*, omit_middle: bool = False) -> pl.DataFrame:
    dates = [date(2020, 3, 2), date(2020, 3, 3), date(2020, 3, 4)]
    closes = [101.0, 50.5, 53.0]
    if omit_middle:
        dates.pop(1)
        closes.pop(1)
    rows = []
    for index, (value_date, close) in enumerate(zip(dates, closes, strict=True)):
        rows.append(
            {
                "date": value_date,
                "symbol": "2330",
                "name": "台積電",
                "market": "twse",
                "open": close - 1.0,
                "max": close + 1.0,
                "min": close - 2.0,
                "close": close,
                "Trading_Volume": float(10_000 + index),
                "Trading_Value": float(1_000_000 + index),
                "shioaji_volume_lots": float(10 + index),
                "shioaji_minute_bars": 270,
                "data_source": "shioaji_kbars_1m",
            }
        )
    return pl.DataFrame(rows)


def test_hybrid_dataset_replaces_all_public_quotes_after_symbol_cutover() -> None:
    output, stats = merge_symbol_frames(
        _base_frame(), _shioaji_frame(), cutover=date(2020, 3, 2)
    )
    assert output.get_column("date").to_list() == [
        date(2020, 2, 27),
        date(2020, 3, 2),
        date(2020, 3, 3),
        date(2020, 3, 4),
    ]
    assert output.get_column("data_source").to_list() == [
        "twse_official",
        "shioaji_kbars_1m",
        "shioaji_kbars_1m",
        "shioaji_kbars_1m",
    ]
    # The split adjustment comes from public point-in-time evidence, while the
    # raw price ratio comes from Shioaji: 101 -> 50.5 remains a flat return.
    assert output.get_column("adjclose").to_list() == pytest.approx(
        [10.0, 10.1, 10.1, 10.6]
    )
    assert stats == {
        "public_rows": 1,
        "shioaji_rows": 3,
        "dropped_public_rows_after_cutover": 0,
        "shioaji_only_rows": 0,
        "quarantined_transitions": 0,
    }


def test_hybrid_dataset_drops_public_gap_instead_of_silent_fallback() -> None:
    output, stats = merge_symbol_frames(
        _base_frame(),
        _shioaji_frame(omit_middle=True),
        cutover=date(2020, 3, 2),
    )
    assert output.get_column("date").to_list() == [
        date(2020, 2, 27),
        date(2020, 3, 2),
        date(2020, 3, 4),
    ]
    assert date(2020, 3, 3) not in output.get_column("date").to_list()
    assert stats["dropped_public_rows_after_cutover"] == 1


def test_hybrid_dataset_retains_only_receipt_declared_source_gap() -> None:
    output, stats = merge_symbol_frames(
        _base_frame(),
        _shioaji_frame(omit_middle=True),
        cutover=date(2020, 3, 2),
        declared_source_gap_dates={date(2020, 3, 3)},
    )
    fallback = output.filter(pl.col("date") == date(2020, 3, 3)).row(
        0, named=True
    )
    assert fallback["data_source"] == "twse_official"
    assert fallback["fallback_reason"] == "shioaji_declared_source_gap"
    assert fallback["close"] == 50.0
    assert stats["public_source_gap_fallback_rows"] == 1
    assert stats["dropped_public_rows_after_cutover"] == 0


def test_hybrid_dataset_quarantines_new_row_without_public_adjustment_evidence() -> None:
    shioaji = _shioaji_frame().vstack(
        pl.DataFrame(
            {
                "date": [date(2020, 3, 5)],
                "symbol": ["2330"],
                "name": ["台積電"],
                "market": ["twse"],
                "open": [53.0],
                "max": [55.0],
                "min": [52.0],
                "close": [54.0],
                "Trading_Volume": [11_000.0],
                "Trading_Value": [1_100_000.0],
                "shioaji_volume_lots": [11.0],
                "shioaji_minute_bars": [270],
                "data_source": ["shioaji_kbars_1m"],
            }
        )
    )
    output, stats = merge_symbol_frames(
        _base_frame(), shioaji, cutover=date(2020, 3, 2)
    )
    prior = output.filter(pl.col("date") == date(2020, 3, 4)).row(0, named=True)
    latest = output.filter(pl.col("date") == date(2020, 3, 5)).row(0, named=True)
    assert prior["return_quarantined"] is True
    assert (
        prior["return_quarantine_reason"]
        == "shioaji_missing_public_adjustment_evidence"
    )
    assert latest["adjustment_source"] == "shioaji_unadjusted_close_quarantined"
    assert stats["shioaji_only_rows"] == 1
    assert stats["quarantined_transitions"] == 1


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def test_hybrid_dataset_builder_writes_separate_receipt_backed_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base_root = tmp_path / "data_tw_public" / "stocks"
    shioaji_root = tmp_path / "data_tw_public" / "shioaji"
    daily_root = shioaji_root / "daily"
    daily_chunk_root = shioaji_root / "daily_chunks" / "2330"
    output_root = shioaji_root / "stocks"
    base_root.mkdir(parents=True)
    daily_root.mkdir(parents=True)
    daily_chunk_root.mkdir(parents=True)
    pl.DataFrame(
        {
            "code": ["2330", "9999"],
            "name": ["台積電", "下市測試"],
            "market": ["twse", "tpex"],
            "security_type": ["stock", "stock"],
            "source": ["twse_tpex_official", "twse_tpex_official"],
        }
    ).write_csv(base_root / "symbols.csv")
    _base_frame().write_parquet(base_root / "2330_features.parquet")
    _base_frame().write_parquet(base_root / "9999_features.parquet")
    daily_path = daily_root / "2330.parquet"
    _shioaji_frame().write_parquet(daily_path)
    daily_chunk_path = daily_chunk_root / "2020-03-02_2020-03-04.parquet"
    _shioaji_frame().write_parquet(daily_chunk_path)
    daily_chunk_path.with_suffix(".receipt.json").write_text(
        json.dumps(
            {
                "schema_version": 2,
                "source": "shioaji_kbars_1m",
                "storage_frequency": "daily",
                "symbol": "2330",
                "start_date": "2020-03-02",
                "end_date": "2020-03-04",
                "status": "ok",
                "rows": 3,
                "daily_rows": 3,
                "source_minute_rows": 810,
                "expected_positive_volume_sessions": 3,
                "output_receipt": {
                    "path": str(daily_chunk_path),
                    "size": daily_chunk_path.stat().st_size,
                    "sha256": _file_sha256(daily_chunk_path),
                },
            }
        ),
        encoding="utf-8",
    )
    daily_path.with_suffix(".summary.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "source": "shioaji_kbars_1m",
                "storage_frequency": "daily",
                "symbol": "2330",
                "requested_start": "2020-03-02",
                "requested_end": "2020-03-04",
                "chunks": 1,
                "source_minute_rows": 810,
                "daily_rows": 3,
                "output_receipt": {
                    "path": str(daily_path),
                    "size": daily_path.stat().st_size,
                    "sha256": _file_sha256(daily_path),
                },
            }
        ),
        encoding="utf-8",
    )
    completed = _completed_daily_result(
        shioaji_root,
        UniverseRow(
            symbol="2330",
            name="台積電",
            market="twse",
            security_type="stock",
            base_path=base_root / "2330_features.parquet",
        ),
        [(date(2020, 3, 2), date(2020, 3, 4))],
        requested_start=date(2020, 3, 2),
        requested_end=date(2020, 3, 4),
    )
    assert completed is not None
    assert completed.status == "complete"
    pl.DataFrame(
        {
            "symbol": ["2330", "9999"],
            "status": ["complete", "contract_unavailable"],
            "message": ["", "contract_not_found"],
        }
    ).write_csv(shioaji_root / "download_report.csv")
    (shioaji_root / "download_summary.json").write_text(
        json.dumps(
            {
                "source": "shioaji_kbars_1m",
                "storage_frequency": "daily",
                "historical_start": "2020-03-02",
                "universe_coverage_complete": True,
                "failed_symbols": 0,
                "partial_symbols": 0,
                "end_date": "2020-03-04",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_tw_shioaji_dataset.py",
            "--base-stock-root",
            str(base_root),
            "--shioaji-root",
            str(shioaji_root),
            "--output-dir",
            str(output_root),
        ],
    )
    dataset_builder.main()
    output = pl.read_parquet(output_root / "2330_features.parquet")
    assert output.get_column("data_source").to_list() == [
        "twse_official",
        "shioaji_kbars_1m",
        "shioaji_kbars_1m",
        "shioaji_kbars_1m",
    ]
    summary = json.loads(
        (output_root / "shioaji_dataset_summary.json").read_text(encoding="utf-8")
    )
    assert summary["hybrid_symbols"] == 1
    assert summary["public_only_contract_unavailable_symbols"] == 1
    assert summary["shioaji_rows"] == 3
    assert _file_sha256(output_root / "9999_features.parquet") == _file_sha256(
        base_root / "9999_features.parquet"
    )
    audit = __import__(
        "scripts.audit_tw_shioaji_dataset",
        fromlist=["audit"],
    ).audit(
        base_stock_root=base_root,
        shioaji_root=shioaji_root,
        dataset_root=output_root,
        verify_chunk_checksums=True,
    )
    assert audit["status"] == "ok"
    assert audit["symbols"] == 2
    assert audit["public_only_contract_unavailable_symbols"] == 1
    assert audit["daily_chunk_receipts"] == 1
    assert audit["storage_frequency"] == "daily"
    assert not (shioaji_root / "raw").exists()

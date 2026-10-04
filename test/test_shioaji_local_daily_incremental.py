from __future__ import annotations

from argparse import Namespace
from datetime import date, datetime
import hashlib
import json
from pathlib import Path

import polars as pl

from downloader import download_shioaji_tw_kbars as daily_downloader
from downloader.download_shioaji_tw_kbars import (
    UniverseRow,
    _incremental_local_daily_plan,
    _local_minute_chunk_signatures,
    _materialize_local_daily_symbol,
    _verified_minute_symbol_frame,
    aggregate_daily,
    normalize_kbars,
)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _minute(day: date, price: float) -> pl.DataFrame:
    ts = int(datetime(day.year, day.month, day.day, 9, 1).timestamp() * 1e9)
    return normalize_kbars(
        {
            "ts": [ts],
            "Open": [price],
            "High": [price],
            "Low": [price],
            "Close": [price],
            "Volume": [1],
            "Amount": [price * 1000],
        },
        symbol="2330",
        market="twse",
        contract_unit=1000.0,
    )


def _entry(
    root: Path, start: date, end: date, frame: pl.DataFrame
) -> dict[str, object]:
    path = root / "minute_chunks" / "2330" / f"{start}_{end}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(path)
    return {
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "status": "ok",
        "data_path": str(path),
        "data_sha256": _sha(path),
        "rows": frame.height,
        "source_gap_dates": [],
    }


def _manifest(
    root: Path, entries: list[dict[str, object]], end: date
) -> dict[str, object]:
    payload = {
        "source": "shioaji_kbars_1m",
        "storage_frequency": "minute",
        "symbol": "2330",
        "requested_start": "2020-03-02",
        "requested_end": end.isoformat(),
        "source_gap_dates": [],
        "chunks": entries,
    }
    path = root / "symbols" / "2330.manifest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return payload


def test_incremental_daily_reuses_verified_prefix_and_rebuilds_changed_tail(
    tmp_path: Path,
) -> None:
    first = date(2020, 3, 2)
    second = date(2020, 3, 3)
    third = date(2020, 3, 4)
    root = tmp_path / "minute"
    output = tmp_path / "daily"
    base = tmp_path / "public.parquet"
    pl.DataFrame(
        {"date": [first, second, third], "Trading_Volume": [1000, 1000, 1000]}
    ).write_parquet(base)
    row = UniverseRow("2330", "台積電", "twse", "stock", base)
    initial = [
        _entry(root, first, first, _minute(first, 100)),
        _entry(root, second, second, _minute(second, 101)),
    ]
    old_manifest = _manifest(root, initial, second)
    old_minute, old_receipt, old_gaps, old_verified = _verified_minute_symbol_frame(
        root, row, requested_start=first, requested_end=second
    )
    _materialize_local_daily_symbol(
        output,
        row,
        requested_start=first,
        requested_end=second,
        chunks=[(first, second)],
        minute=old_minute,
        minute_manifest_receipt=old_receipt,
        source_gap_dates=old_gaps,
        verified_source_chunks=old_verified,
        minute_source_chunks=_local_minute_chunk_signatures(
            old_manifest, start=first, end=second
        ),
    )
    new_tail = pl.concat([_minute(second, 102), _minute(third, 103)])
    updated = [initial[0], _entry(root, second, third, new_tail)]
    new_manifest = _manifest(root, updated, third)
    plan = _incremental_local_daily_plan(
        output,
        row,
        requested_start=first,
        requested_end=third,
        minute_manifest=new_manifest,
    )
    assert plan is not None
    assert plan[0] == second
    assert plan[2:] == (1, 1)
    suffix, receipt, gaps, verified = _verified_minute_symbol_frame(
        root, row, requested_start=plan[0], requested_end=third
    )
    result = _materialize_local_daily_symbol(
        output,
        row,
        requested_start=first,
        requested_end=third,
        chunks=[(first, third)],
        minute=suffix,
        minute_manifest_receipt=receipt,
        source_gap_dates=gaps,
        verified_source_chunks=verified,
        minute_source_chunks=_local_minute_chunk_signatures(
            new_manifest, start=first, end=third
        ),
        prefix_daily=plan[1],
        prefix_source_minute_rows=plan[2],
        reused_source_chunks=plan[3],
    )
    expected = aggregate_daily(
        pl.concat([_minute(first, 100), new_tail]).sort("ts"), name="台積電"
    )
    assert pl.read_parquet(result.output_path).to_dicts() == expected.to_dicts()
    assert result.source_minute_rows == 3
    summary = json.loads((output / "daily" / "2330.summary.json").read_text())
    assert summary["source_minute_chunks_verified"] == 2
    assert summary["source_minute_chunks_reused"] == 1
    assert summary["minute_manifest_receipt"]["sha256"] == _sha(
        root / "symbols" / "2330.manifest.json"
    )


def test_incremental_daily_fails_closed_on_historical_change_or_corrupt_output(
    tmp_path: Path,
) -> None:
    first = date(2020, 3, 2)
    second = date(2020, 3, 3)
    third = date(2020, 3, 4)
    root = tmp_path / "minute"
    output = tmp_path / "daily"
    base = tmp_path / "base.parquet"
    pl.DataFrame(
        {"date": [first, second, third], "Trading_Volume": [1000, 1000, 1000]}
    ).write_parquet(base)
    row = UniverseRow("2330", "台積電", "twse", "stock", base)
    entries = [
        _entry(root, first, first, _minute(first, 100)),
        _entry(root, second, second, _minute(second, 101)),
    ]
    manifest = _manifest(root, entries, second)
    minute, receipt, gaps, verified = _verified_minute_symbol_frame(
        root, row, requested_start=first, requested_end=second
    )
    _materialize_local_daily_symbol(
        output,
        row,
        requested_start=first,
        requested_end=second,
        chunks=[(first, second)],
        minute=minute,
        minute_manifest_receipt=receipt,
        source_gap_dates=gaps,
        verified_source_chunks=verified,
        minute_source_chunks=_local_minute_chunk_signatures(
            manifest, start=first, end=second
        ),
    )
    changed = [
        _entry(root, first, first, _minute(first, 99)),
        entries[1],
        _entry(root, third, third, _minute(third, 102)),
    ]
    changed_manifest = _manifest(root, changed, third)
    assert (
        _incremental_local_daily_plan(
            output,
            row,
            requested_start=first,
            requested_end=third,
            minute_manifest=changed_manifest,
        )
        is None
    )
    safe_manifest = _manifest(root, [entries[0], entries[1], changed[2]], third)
    assert (
        _incremental_local_daily_plan(
            output,
            row,
            requested_start=first,
            requested_end=third,
            minute_manifest=safe_manifest,
        )
        is not None
    )
    (output / "daily" / "2330.parquet").write_bytes(b"corrupt")
    assert (
        _incremental_local_daily_plan(
            output,
            row,
            requested_start=first,
            requested_end=third,
            minute_manifest=safe_manifest,
        )
        is None
    )


def test_local_only_job_uses_incremental_daily_without_broker(
    tmp_path: Path, monkeypatch
) -> None:
    first, second, third = date(2020, 3, 2), date(2020, 3, 3), date(2020, 3, 4)
    root, output = tmp_path / "minute", tmp_path / "daily"
    base = tmp_path / "public.parquet"
    pl.DataFrame(
        {"date": [first, second, third], "Trading_Volume": [1000, 1000, 1000]}
    ).write_parquet(base)
    row = UniverseRow("2330", "台積電", "twse", "stock", base)
    entries = [
        _entry(root, first, first, _minute(first, 100)),
        _entry(root, second, second, _minute(second, 101)),
    ]
    _manifest(root, entries, second)
    (root / "download_report.csv").write_text(
        "symbol,status\n2330,complete\n", encoding="utf-8"
    )

    def source_summary(end: date) -> None:
        (root / "download_summary.json").write_text(
            json.dumps(
                {
                    "source": "shioaji_kbars_1m",
                    "storage_frequency": "minute",
                    "resumable_collection_complete": True,
                    "failed_symbols": 0,
                    "partial_symbols": 0,
                    "start_date": first.isoformat(),
                    "end_date": end.isoformat(),
                    "simulation": True,
                }
            ),
            encoding="utf-8",
        )

    args = Namespace(
        minute_cache_root=root,
        output_dir=output,
        chunk_days=30,
        start_date=first.isoformat(),
        end_date=second.isoformat(),
        symbols="2330",
        max_symbols=0,
        simulation=True,
    )
    monkeypatch.setattr(
        daily_downloader, "record_avoided_query", lambda **_kwargs: None
    )
    source_summary(second)
    daily_downloader._run_local_materialization(
        args, start=first, end=second, universe=[row], selected=[row]
    )
    assert (
        json.loads((output / "daily" / "2330.summary.json").read_text())[
            "source_minute_chunks_reused"
        ]
        == 0
    )

    tail = pl.concat([_minute(second, 102), _minute(third, 103)])
    _manifest(root, [entries[0], _entry(root, second, third, tail)], third)
    source_summary(third)
    args.end_date = third.isoformat()
    daily_downloader._run_local_materialization(
        args, start=first, end=third, universe=[row], selected=[row]
    )
    summary = json.loads((output / "daily" / "2330.summary.json").read_text())
    assert summary["source_minute_chunks_reused"] == 1
    assert summary["source_minute_chunks_verified"] == 2
    assert (
        pl.read_parquet(output / "daily" / "2330.parquet").to_dicts()
        == aggregate_daily(
            pl.concat([_minute(first, 100), tail]).sort("ts"), name="台積電"
        ).to_dicts()
    )
    run_summary = json.loads((output / "download_summary.json").read_text())
    assert run_summary["complete_symbols"] == 1
    assert run_summary["api_requests_started"] == 0

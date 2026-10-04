from __future__ import annotations

from argparse import Namespace
from datetime import date, datetime, timedelta
import hashlib
import json
from pathlib import Path

import polars as pl
import pytest

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
from downloader.shioaji_daily_calendar import (
    DAILY_CALENDAR_CONTRACT, calendar_prefix_matches, session_dates_sha256,
)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _calendar(start: date, end: date, sessions: set[date] | None = None):
    sessions = sessions if sessions is not None else {
        start + timedelta(days=index) for index in range((end - start).days + 1)
    }
    return sessions, {
        "contract": DAILY_CALENDAR_CONTRACT, "root": "/fixture/calendar", "sha256": "a" * 64,
        "start_date": start.isoformat(), "end_date": end.isoformat(),
        "session_dates_sha256": session_dates_sha256(sessions, start, end),
    }


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
        official_sessions=_calendar(first, second)[0],
        official_calendar=_calendar(first, second)[1],
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
        official_sessions=_calendar(first, third)[0],
        official_calendar=_calendar(first, third)[1],
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
        official_sessions=_calendar(first, second)[0],
        official_calendar=_calendar(first, second)[1],
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
    monkeypatch.setattr(
        daily_downloader, "load_daily_calendar", lambda _root, start, end: _calendar(start, end)
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


def test_daily_calendar_quarantines_closed_day_and_preserves_raw_source(tmp_path):
    first, closed, last = date(2020, 3, 6), date(2020, 3, 8), date(2020, 3, 9)
    root, output = tmp_path / "minute", tmp_path / "daily"
    frame = pl.concat([_minute(first, 100), _minute(closed, 900), _minute(last, 101)])
    entry = _entry(root, first, last, frame)
    manifest = _manifest(root, [entry], last)
    raw_sha = _sha(Path(str(entry["data_path"])))
    sessions, calendar = _calendar(first, last, {first, last})
    row = UniverseRow("2330", "台積電", "twse", "stock", tmp_path / "base.parquet")
    receipt_path = root / "symbols" / "2330.manifest.json"
    result = _materialize_local_daily_symbol(
        output, row, requested_start=first, requested_end=last, chunks=[(first, last)],
        minute=frame, minute_manifest_receipt={"path": str(receipt_path), "sha256": _sha(receipt_path)},
        source_gap_dates=[], verified_source_chunks=1,
        minute_source_chunks=_local_minute_chunk_signatures(manifest, start=first, end=last),
        official_sessions=sessions, official_calendar=calendar,
    )
    daily = pl.read_parquet(result.output_path)
    assert daily["date"].to_list() == [first, last]
    assert daily["close"].to_list() == [100, 101]
    assert daily["Trading_Volume"].to_list() == [1000, 1000]
    assert result.source_minute_rows == 3
    assert result.quarantined_non_session_source_rows == 1
    summary = json.loads(Path(result.output_path).with_suffix(".summary.json").read_text())
    assert summary["quarantined_non_session_source_rows"] == {closed.isoformat(): 1}
    assert _sha(Path(str(entry["data_path"]))) == raw_sha


@pytest.mark.parametrize("failure", ["changed_source", "changed_output", "changed_calendar"])
def test_calendar_upgrade_rejects_changed_identity(tmp_path, failure):
    first, closed, last = date(2020, 3, 6), date(2020, 3, 8), date(2020, 3, 9)
    root, output = tmp_path / "minute", tmp_path / "daily"
    raw = pl.concat([_minute(first, 100), _minute(closed, 900), _minute(last, 101)])
    manifest = _manifest(root, [_entry(root, first, last, raw)], last)
    row = UniverseRow("2330", "台積電", "twse", "stock", tmp_path / "base.parquet")
    path = root / "symbols" / "2330.manifest.json"
    signatures = _local_minute_chunk_signatures(manifest, start=first, end=last)
    all_sessions, legacy_calendar = _calendar(first, last)
    result = _materialize_local_daily_symbol(
        output, row, requested_start=first, requested_end=last, chunks=[(first, last)],
        minute=raw, minute_manifest_receipt={"path": str(path), "sha256": _sha(path)},
        source_gap_dates=[], verified_source_chunks=1, minute_source_chunks=signatures,
        official_sessions=all_sessions, official_calendar=legacy_calendar,
    )
    summary_path = Path(result.output_path).with_suffix(".summary.json")
    summary = json.loads(summary_path.read_text())
    if failure != "changed_calendar":
        summary.pop("official_calendar")
    summary_path.write_text(json.dumps(summary))
    sessions, calendar = _calendar(first, last, {first, last})
    if failure == "changed_source":
        signatures[0] = dict(signatures[0], data_sha256="b" * 64)
    elif failure == "changed_output":
        Path(result.output_path).write_bytes(b"bad bytes")
    before = Path(result.output_path).read_bytes()
    assert daily_downloader._upgrade_local_daily_calendar(
        output, row, start=first, end=last, chunks=[(first, last)], manifest=manifest,
        manifest_sha256=_sha(path), source_chunks=signatures,
        official_sessions=sessions, official_calendar=calendar,
    ) is None
    assert Path(result.output_path).read_bytes() == before


def test_legacy_calendar_upgrade_is_equivalent_to_full_materialization(tmp_path):
    first, closed, last = date(2020, 3, 6), date(2020, 3, 8), date(2020, 3, 9)
    root, output = tmp_path / "minute", tmp_path / "daily"
    raw = pl.concat([_minute(first, 100), _minute(closed, 900), _minute(last, 101)])
    manifest = _manifest(root, [_entry(root, first, last, raw)], last)
    row = UniverseRow("2330", "台積電", "twse", "stock", tmp_path / "base.parquet")
    path = root / "symbols" / "2330.manifest.json"
    signatures = _local_minute_chunk_signatures(manifest, start=first, end=last)
    all_sessions, old_calendar = _calendar(first, last)
    kwargs = dict(
        requested_start=first, requested_end=last, chunks=[(first, last)], minute=raw,
        minute_manifest_receipt={"path": str(path), "sha256": _sha(path)},
        source_gap_dates=[], verified_source_chunks=1, minute_source_chunks=signatures,
    )
    old = _materialize_local_daily_symbol(
        output, row, **kwargs, official_sessions=all_sessions, official_calendar=old_calendar
    )
    summary_path = Path(old.output_path).with_suffix(".summary.json")
    summary = json.loads(summary_path.read_text()); summary.pop("official_calendar")
    summary_path.write_text(json.dumps(summary))
    sessions, calendar = _calendar(first, last, {first, last})
    assert daily_downloader._completed_daily_result(
        output, row, [(first, last)], requested_start=first, requested_end=last,
        minute_manifest_sha256=_sha(path), official_sessions=sessions, official_calendar=calendar,
    ) is None
    upgraded = daily_downloader._upgrade_local_daily_calendar(
        output, row, start=first, end=last, chunks=[(first, last)], manifest=manifest,
        manifest_sha256=_sha(path), source_chunks=signatures,
        official_sessions=sessions, official_calendar=calendar,
    )
    full = _materialize_local_daily_symbol(
        tmp_path / "full", row, **kwargs, official_sessions=sessions, official_calendar=calendar
    )
    assert upgraded is not None
    assert pl.read_parquet(upgraded.output_path).to_dicts() == pl.read_parquet(full.output_path).to_dicts()
    assert upgraded.source_minute_rows == full.source_minute_rows == 3
    assert upgraded.quarantined_non_session_source_rows == full.quarantined_non_session_source_rows == 1
    assert daily_downloader._completed_daily_result(
        output, row, [(first, last)], requested_start=first, requested_end=last,
        minute_manifest_sha256=_sha(path), official_sessions=sessions, official_calendar=calendar,
    ) is not None


def test_calendar_prefix_allows_new_sessions_but_rejects_historical_date_change():
    first, closed, last, next_day = (date(2020, 3, day) for day in (6, 8, 9, 10))
    old_sessions, old = _calendar(first, last, {first, last})
    new_sessions, new = _calendar(first, next_day, old_sessions | {next_day})
    assert calendar_prefix_matches(old, new_sessions, new, last)
    assert not calendar_prefix_matches(old, new_sessions | {closed}, new, last)
    assert not calendar_prefix_matches(dict(old, contract="unknown"), new_sessions, new, last)

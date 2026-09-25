from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from stockagent.live.benchmark_history_projection import (
    load_benchmark_projection,
    projection_delta_path,
    projection_head_path,
    write_benchmark_projection,
)
from stockagent.live.tw_day_trade_dashboard import build_dashboard_history_snapshot
from scripts.promote_tw_day_trade_replay import _validate_benchmarks


def _benchmark_mark(session_date: str, minute: str, equity: float) -> dict[str, object]:
    return {
        "benchmark_id": "benchmark_0050",
        "session_date": session_date,
        "minute": f"{session_date}T{minute}+08:00",
        "initial_capital_twd": 100.0,
        "total_equity_twd": equity,
        "last_mark_price": equity,
        "large_repeated_provenance": "endpoint only",
    }


def _source_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_benchmark_validator_uses_hash_matched_projection_and_falls_back_on_corruption(
    tmp_path: Path, monkeypatch,
) -> None:
    from scripts import promote_tw_day_trade_replay as promotion

    session_date = "2026-08-13"
    marks = [
        {
            "benchmark_id": benchmark_id,
            "session_date": session_date,
            "minute": (datetime.fromisoformat(f"{session_date}T{start}+08:00")
                       + timedelta(minutes=index)).isoformat(timespec="minutes"),
        }
        for benchmark_id, start, count in (
            ("benchmark_0050", "09:00", 271),
            ("benchmark_2330", "09:00", 271),
            ("benchmark_tx_continuous", "08:45", 300),
        )
        for index in range(count)
    ]
    source = tmp_path / "benchmark_history.json"
    source.write_text(json.dumps({"marks": marks}), encoding="utf-8")
    write_benchmark_projection(
        state_dir=tmp_path,
        source_path=source,
        source_sha256=_source_sha256(source),
        origins={},
        marks=marks,
        created_at="2026-08-13T14:00:00+08:00",
    )
    with monkeypatch.context() as patch:
        patch.setattr(promotion, "_load_object", lambda _path: (_ for _ in ()).throw(
            AssertionError("verified projection should avoid decoding canonical JSON")
        ))
        result = _validate_benchmarks(tmp_path, completed_session_dates=[session_date])
    assert result["rows"] == {
        "benchmark_0050": 271,
        "benchmark_2330": 271,
        "benchmark_tx_continuous": 300,
    }

    head = json.loads(projection_head_path(tmp_path).read_text(encoding="utf-8"))
    shard = tmp_path / "benchmark_history_projection/v1" / head["sessions"][session_date]["path"]
    shard.write_bytes(b"corrupt")
    assert _validate_benchmarks(tmp_path, completed_session_dates=[session_date])["rows"] == result["rows"]


def test_benchmark_projection_is_immutable_incremental_and_private(tmp_path: Path) -> None:
    source = tmp_path / "benchmark_history.json"
    first_marks = [
        _benchmark_mark("2026-08-13", "09:00", 101.0),
        _benchmark_mark("2026-08-13", "13:30", 102.0),
        _benchmark_mark("2026-08-14", "09:00", 103.0),
    ]
    source.write_text(json.dumps({"marks": first_marks}) + "\n", encoding="utf-8")
    first = write_benchmark_projection(
        state_dir=tmp_path,
        source_path=source,
        source_sha256=_source_sha256(source),
        origins={"benchmark_0050": {"entry_price": 100.0}},
        marks=first_marks,
        created_at="2026-08-14T14:00:00+08:00",
    )
    head = json.loads(projection_head_path(tmp_path).read_text())
    old_path = head["sessions"]["2026-08-13"]["path"]
    old_digest = head["sessions"]["2026-08-13"]["sha256"]
    assert first["projection_changed_sessions"] == ["2026-08-13", "2026-08-14"]

    changed_marks = [*first_marks, _benchmark_mark("2026-08-15", "09:00", 104.0)]
    source.write_text(json.dumps({"marks": changed_marks}) + "\n", encoding="utf-8")
    second = write_benchmark_projection(
        state_dir=tmp_path,
        source_path=source,
        source_sha256=_source_sha256(source),
        origins={"benchmark_0050": {"entry_price": 100.0}},
        marks=changed_marks,
        created_at="2026-08-15T14:00:00+08:00",
    )
    current = json.loads(projection_head_path(tmp_path).read_text())

    assert second["projection_changed_sessions"] == ["2026-08-15"]
    assert current["sessions"]["2026-08-13"]["path"] == old_path
    assert current["sessions"]["2026-08-13"]["sha256"] == old_digest
    assert len(projection_delta_path(tmp_path).read_text().splitlines()) == 3
    stable_head_stat = projection_head_path(tmp_path).stat()
    stable_delta = projection_delta_path(tmp_path).read_bytes()
    unchanged = write_benchmark_projection(
        state_dir=tmp_path,
        source_path=source,
        source_sha256=_source_sha256(source),
        origins={"benchmark_0050": {"entry_price": 100.0}},
        marks=changed_marks,
        created_at="2026-08-15T14:01:00+08:00",
    )
    assert unchanged["projection_changed_sessions"] == []
    assert projection_head_path(tmp_path).stat() == stable_head_stat
    assert projection_delta_path(tmp_path).read_bytes() == stable_delta
    assert oct(projection_head_path(tmp_path).stat().st_mode & 0o777) == "0o600"
    for entry in current["sessions"].values():
        shard = tmp_path / "benchmark_history_projection/v1" / entry["path"]
        assert oct(shard.stat().st_mode & 0o777) == "0o600"

    selected = load_benchmark_projection(
        state_dir=tmp_path,
        source_path=source,
        selected_sessions=["2026-08-15"],
    )
    assert selected is not None
    assert list(selected.marks_by_session) == ["2026-08-15"]
    assert set(selected.session_entries) == {
        "2026-08-13",
        "2026-08-14",
        "2026-08-15",
    }


def test_dashboard_session_projection_rebuilds_only_appended_day(
    tmp_path: Path, monkeypatch
) -> None:
    state = tmp_path / "state"
    cache = tmp_path / "cache"
    state.mkdir()
    cache.mkdir()
    monkeypatch.setenv("STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR", str(cache))
    (state / "state.json").write_text(
        json.dumps({"product": "tw_day_trade"}) + "\n", encoding="utf-8"
    )
    marks_path = state / "marks.jsonl"

    def mark(session_date: str, equity: float) -> dict[str, object]:
        return {
            "market": "tw_day_trade",
            "session_date": session_date,
            "minute": f"{session_date}T09:01+08:00",
            "initial_capital_twd": 100.0,
            "total_equity_twd": equity,
            "historical_minute_replay": True,
            "fresh_trade_notional_coverage_ratio": 0.75,
        }

    rows = [mark("2026-08-13", 101.0), mark("2026-08-14", 102.0)]
    marks_path.write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
    )
    baseline = build_dashboard_history_snapshot(
        state_dir=state,
        range_key="all",
        resolution="1m",
        history_encoding="minute_columns_v2",
        use_memory_cache=False,
        use_persistent_cache=False,
        use_session_projection=False,
    )
    bootstrap = build_dashboard_history_snapshot(
        state_dir=state,
        range_key="all",
        resolution="1m",
        history_encoding="minute_columns_v2",
        use_memory_cache=False,
        use_persistent_cache=False,
    )
    assert bootstrap == baseline

    with marks_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(mark("2026-08-15", 103.0)) + "\n")
    from stockagent.live import tw_day_trade_dashboard as dashboard

    original = dashboard._rows_for_sessions
    selected: list[tuple[str, ...]] = []

    def observed(*args, **kwargs):
        selected.append(tuple(args[1]))
        return original(*args, **kwargs)

    monkeypatch.setattr(dashboard, "_rows_for_sessions", observed)
    incremental = build_dashboard_history_snapshot(
        state_dir=state,
        range_key="all",
        resolution="1m",
        history_encoding="minute_columns_v2",
        use_memory_cache=False,
        use_persistent_cache=False,
    )
    full = build_dashboard_history_snapshot(
        state_dir=state,
        range_key="all",
        resolution="1m",
        history_encoding="minute_columns_v2",
        use_memory_cache=False,
        use_persistent_cache=False,
        use_session_projection=False,
    )

    assert incremental == full
    assert selected == [("2026-08-15",), ("2026-08-15",)]
    root = next(cache.glob("history-session-projection-v3-*"))
    assert len((root / "delta.jsonl").read_text().splitlines()) == 3
    assert all((path.stat().st_mode & 0o777) == 0o600 for path in root.rglob("*") if path.is_file())


def test_corrupt_benchmark_projection_fails_closed(tmp_path: Path) -> None:
    source = tmp_path / "benchmark_history.json"
    marks = [_benchmark_mark("2026-08-13", "09:00", 101.0)]
    source.write_text(json.dumps({"marks": marks}) + "\n", encoding="utf-8")
    write_benchmark_projection(
        state_dir=tmp_path,
        source_path=source,
        source_sha256=_source_sha256(source),
        origins={},
        marks=marks,
        created_at="2026-08-13T14:00:00+08:00",
    )
    head = json.loads(projection_head_path(tmp_path).read_text())
    shard = tmp_path / "benchmark_history_projection/v1" / head["sessions"]["2026-08-13"]["path"]
    descriptor = os.open(shard, os.O_WRONLY | os.O_TRUNC)
    try:
        os.write(descriptor, b"corrupt")
    finally:
        os.close(descriptor)

    assert (
        load_benchmark_projection(state_dir=tmp_path, source_path=source) is None
    )

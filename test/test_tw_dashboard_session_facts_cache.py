from __future__ import annotations

import json
import os
from pathlib import Path
import pickle
import threading

import pytest

from stockagent.live import tw_day_trade_dashboard as dashboard


DAY = "2026-09-24"
OTHER = "2026-09-23"


@pytest.fixture(autouse=True)
def private_caches(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(dashboard, "_SESSION_TAIL_CACHE", {})
    monkeypatch.setattr(dashboard, "_LEDGER_SESSION_INDEX_CACHE", {})
    cache = tmp_path / "cache"
    cache.mkdir()
    monkeypatch.setenv("STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR", str(cache))


def _write(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _read(path: Path, day: str = DAY, *, fallback: bool = True):
    return dashboard._tail_for_session(path, None, day, recorded_at_fallback=fallback)


def test_complete_cache_preserves_all_fields_order_and_nested_aliases(tmp_path, monkeypatch):
    path = tmp_path / "marks.jsonl"
    expected = [
        {"session_date": DAY, "nested": {"arbitrary": [1, None, {"x": "雪"}]}},
        {"session_date": DAY, "explicit_null": None, "nested": {"arbitrary": [False]}},
    ]
    _write(path, [expected[0], {"session_date": OTHER}, expected[1]])
    first = _read(path)
    assert first == expected
    first[0]["nested"]["arbitrary"][2]["x"] = "caller mutation"
    first[1]["injected"] = True
    monkeypatch.setattr(dashboard, "_rows_for_sessions", lambda *_a, **_k: pytest.fail("cache miss"))
    second = _read(path)
    assert second == expected
    assert "explicit_null" not in second[0]
    second[0]["nested"]["arbitrary"].clear()
    assert _read(path) == expected


@pytest.mark.parametrize("kind", ("append", "historical_backfill", "rewrite", "replace"))
def test_complete_cache_invalidates_changed_source(tmp_path, kind):
    path = tmp_path / "marks.jsonl"
    _write(path, [{"session_date": DAY, "value": 1}])
    assert _read(path)[0]["value"] == 1
    if kind in {"append", "historical_backfill"}:
        day = OTHER if kind == "historical_backfill" else DAY
        with path.open("a") as stream:
            stream.write(json.dumps({"session_date": day, "value": 2}) + "\n")
        assert _read(path, day)[-1]["value"] == 2
    elif kind == "rewrite":
        _write(path, [{"session_date": DAY, "value": 2}])
        assert _read(path)[0]["value"] == 2
    else:
        replacement = tmp_path / "replacement"
        _write(replacement, [{"session_date": DAY, "value": 2}])
        os.replace(replacement, path)
        assert _read(path)[0]["value"] == 2


@pytest.mark.parametrize("evict_rows", (False, True))
def test_ctime_only_date_rewrite_bypasses_stale_memory_and_disk_index(tmp_path, monkeypatch, evict_rows):
    path = tmp_path / "marks.jsonl"
    _write(path, [{"session_date": DAY, "value": 1}, {"session_date": OTHER, "value": 2}])
    assert _read(path)[0]["value"] == 1
    previous = path.stat()
    if evict_rows:
        dashboard._SESSION_TAIL_CACHE.clear()
    # Identical size/mtime and offsets, but both session assignments swap.
    _write(path, [{"session_date": OTHER, "value": 1}, {"session_date": DAY, "value": 2}])
    os.utime(path, ns=(previous.st_atime_ns, previous.st_mtime_ns))
    assert path.stat().st_size == previous.st_size
    assert path.stat().st_ctime_ns != previous.st_ctime_ns
    monkeypatch.setattr(dashboard, "_load_persistent_ledger_index",
                        lambda *_a, **_k: pytest.fail("must not reload legacy stale spans"))
    assert _read(path) == [{"session_date": DAY, "value": 2}]
    assert _read(path, OTHER) == [{"session_date": OTHER, "value": 1}]


def test_normal_append_indexes_only_new_lines(tmp_path, monkeypatch):
    path = tmp_path / "marks.jsonl"
    _write(path, [{"session_date": DAY, "value": 1}])
    _read(path)
    original = dashboard._ledger_line_session_date
    scanned = []

    def track(line, **kwargs):
        scanned.append(json.loads(line)["value"])
        return original(line, **kwargs)

    monkeypatch.setattr(dashboard, "_ledger_line_session_date", track)
    with path.open("a") as stream:
        stream.write(json.dumps({"session_date": DAY, "value": 2}) + "\n")
    assert [row["value"] for row in _read(path)] == [1, 2]
    assert scanned == [2]


def test_oversized_cache_entry_keeps_complete_rows_and_index_observation(tmp_path, monkeypatch):
    path = tmp_path / "marks.jsonl"
    rows = [{"session_date": DAY, "value": i, "nested": ["x" * 50]} for i in range(10)]
    _write(path, rows)
    monkeypatch.setattr(dashboard, "_SESSION_FACTS_CACHE_MAX_ITEM_BYTES", 1)
    assert _read(path) == rows
    assert _read(path) == rows
    assert dashboard._SESSION_TAIL_CACHE == {}
    index = dashboard._LEDGER_SESSION_INDEX_CACHE[(path.resolve(), True)]
    assert index.facts_signature == dashboard.metadata_signature(path.stat())


def test_complete_cache_has_shared_entry_and_serialized_byte_bounds(tmp_path, monkeypatch):
    monkeypatch.setattr(dashboard, "_TAIL_CACHE_MAX_ENTRIES", 2)
    monkeypatch.setattr(dashboard, "_SESSION_FACTS_CACHE_MAX_BYTES", 250)
    for i in range(8):
        path = tmp_path / f"marks-{i}.jsonl"
        rows = [{"session_date": DAY, "value": str(i) * 40}]
        _write(path, rows)
        assert _read(path) == rows
        assert len(dashboard._SESSION_TAIL_CACHE) <= 2
        entries = list(dashboard._SESSION_TAIL_CACHE.values())
        assert sum(len(entry.payload) for entry in entries) <= 250
    assert len(entries) < 8


@pytest.mark.parametrize("stage", ("read", "cache_hit", "serialize", "oversize"))
def test_source_drift_raises_instead_of_returning_or_caching_old_facts(tmp_path, monkeypatch, stage):
    path = tmp_path / "marks.jsonl"
    _write(path, [{"session_date": DAY, "value": 1}])

    def change():
        _write(path, [{"session_date": DAY, "value": 2}])

    if stage == "read":
        original = dashboard._rows_for_sessions

        def read(*args, **kwargs):
            result = original(*args, **kwargs)
            change()
            return result

        monkeypatch.setattr(dashboard, "_rows_for_sessions", read)
    elif stage == "cache_hit":
        _read(path)
        original = pickle.loads

        def loads(value):
            result = original(value)
            change()
            return result

        monkeypatch.setattr(dashboard.pickle, "loads", loads)
    else:
        original = dashboard._SessionRowsBuffer.write

        def write(buffer, value):
            change()
            return original(buffer, value)

        monkeypatch.setattr(dashboard._SessionRowsBuffer, "write", write)
        if stage == "oversize":
            monkeypatch.setattr(dashboard, "_SESSION_FACTS_CACHE_MAX_ITEM_BYTES", 1)
    with pytest.raises(OSError, match="ledger changed"):
        _read(path)


def test_resource_error_is_not_a_cache_miss(tmp_path, monkeypatch):
    path = tmp_path / "marks.jsonl"
    _write(path, [{"session_date": DAY}])

    def no_memory(*_a, **_k):
        raise MemoryError("out of memory")

    monkeypatch.setattr(dashboard._SessionRowsBuffer, "write", no_memory)
    with pytest.raises(MemoryError):
        _read(path)


@pytest.mark.parametrize("decode_error", (False, True))
def test_ctime_drift_failed_read_keeps_observation_for_next_retry(tmp_path, monkeypatch, decode_error):
    path = tmp_path / "marks.jsonl"
    _write(path, [{"session_date": DAY, "value": 1}, {"session_date": OTHER, "value": 2}])
    previous = path.stat()
    original = dashboard._rows_for_sessions

    def drift(*args, **kwargs):
        result = original(*args, **kwargs)
        _write(path, [{"session_date": OTHER, "value": 1}, {"session_date": DAY, "value": 2}])
        os.utime(path, ns=(previous.st_atime_ns, previous.st_mtime_ns))
        if decode_error:
            raise ValueError("decode failed after source drift")
        return result

    monkeypatch.setattr(dashboard, "_rows_for_sessions", drift)
    with pytest.raises(ValueError if decode_error else OSError):
        _read(path)
    monkeypatch.setattr(dashboard, "_rows_for_sessions", original)
    monkeypatch.setattr(dashboard, "_load_persistent_ledger_index",
                        lambda *_a, **_k: pytest.fail("retry must reject stale index"))
    assert _read(path) == [{"session_date": DAY, "value": 2}]


def test_fallback_policy_has_separate_cache_keys(tmp_path):
    path = tmp_path / "events.jsonl"
    _write(path, [{"recorded_at": f"{DAY}T09:01:00+08:00", "nested": {"value": None}}])
    assert len(_read(path, fallback=True)) == 1
    assert _read(path, fallback=False) == []
    assert len(_read(path, fallback=True)) == 1


@pytest.mark.parametrize("second_day", (DAY, OTHER))
def test_concurrent_complete_readers_do_not_cache_old_spans_under_new_ctime(tmp_path, monkeypatch, second_day):
    path = tmp_path / "marks.jsonl"
    _write(path, [{"session_date": DAY, "value": 1}, {"session_date": OTHER, "value": 2}])
    previous = path.stat()
    original = dashboard._rows_for_sessions
    built, release = threading.Event(), threading.Event()
    failures = []

    def pause_first(*args, **kwargs):
        result = original(*args, **kwargs)
        if threading.current_thread().name == "first-facts-reader":
            built.set()
            assert release.wait(5)
        return result

    def first():
        try:
            _read(path)
        except Exception as error:
            failures.append(error)

    monkeypatch.setattr(dashboard, "_rows_for_sessions", pause_first)
    reader = threading.Thread(target=first, name="first-facts-reader")
    reader.start()
    try:
        assert built.wait(5)
        index = dashboard._LEDGER_SESSION_INDEX_CACHE[(path.resolve(), True)]
        assert index.facts_signature == dashboard.metadata_signature(previous)
        _write(path, [{"session_date": OTHER, "value": 1}, {"session_date": DAY, "value": 2}])
        os.utime(path, ns=(previous.st_atime_ns, previous.st_mtime_ns))
        expected = [{"session_date": second_day, "value": 2 if second_day == DAY else 1}]
        assert _read(path, second_day) == expected
    finally:
        release.set()
        reader.join(5)
    assert not reader.is_alive()
    assert len(failures) == 1 and isinstance(failures[0], OSError)
    assert _read(path, second_day) == expected


def test_index_handoff_rejects_old_expected_signature_without_relabeling_new_index(tmp_path):
    path = tmp_path / "marks.jsonl"
    _write(path, [{"session_date": DAY, "value": 1}])
    original_signature = dashboard.metadata_signature(path.stat())
    _read(path)
    _write(path, [{"session_date": DAY, "value": 2}])
    assert _read(path)[0]["value"] == 2
    current = dashboard._LEDGER_SESSION_INDEX_CACHE[(path.resolve(), True)]
    current_signature = current.facts_signature
    with pytest.raises(OSError, match="before index handoff"):
        dashboard._ledger_session_index(path, observed_signature=original_signature)
    assert current.facts_signature == current_signature


@pytest.mark.parametrize("disk_only", (False, True))
def test_rejected_initial_handoff_keeps_legacy_index_invalidation_evidence(tmp_path, monkeypatch, disk_only):
    path = tmp_path / "marks.jsonl"
    _write(path, [{"session_date": DAY, "value": 1}, {"session_date": OTHER, "value": 2}])
    previous = path.stat()
    legacy = dashboard._ledger_session_index(path, recorded_at_fallback=True)
    assert legacy.facts_signature is None
    if disk_only:
        dashboard._LEDGER_SESSION_INDEX_CACHE.clear()
    original = dashboard._ledger_session_index

    def drift_before_handoff(*args, **kwargs):
        _write(path, [{"session_date": OTHER, "value": 1}, {"session_date": DAY, "value": 2}])
        os.utime(path, ns=(previous.st_atime_ns, previous.st_mtime_ns))
        return original(*args, **kwargs)

    monkeypatch.setattr(dashboard, "_ledger_session_index", drift_before_handoff)
    with pytest.raises(OSError, match="before index handoff"):
        _read(path)
    observed = dashboard._LEDGER_SESSION_INDEX_CACHE[(path.resolve(), True)]
    assert observed.facts_signature == dashboard.metadata_signature(previous)
    monkeypatch.setattr(dashboard, "_ledger_session_index", original)
    monkeypatch.setattr(dashboard, "_load_persistent_ledger_index",
                        lambda *_a, **_k: pytest.fail("retry must reject stale disk spans"))
    assert _read(path) == [{"session_date": DAY, "value": 2}]


def test_rejected_postload_handoff_keeps_observation_for_retry(tmp_path, monkeypatch):
    path = tmp_path / "marks.jsonl"
    _write(path, [{"session_date": DAY, "value": 1}, {"session_date": OTHER, "value": 2}])
    previous = path.stat()
    dashboard._ledger_session_index(path, recorded_at_fallback=True)
    dashboard._LEDGER_SESSION_INDEX_CACHE.clear()
    original = dashboard._load_persistent_ledger_index

    def drift_during_load(*args, **kwargs):
        index = original(*args, **kwargs)
        _write(path, [{"session_date": OTHER, "value": 1}, {"session_date": DAY, "value": 2}])
        os.utime(path, ns=(previous.st_atime_ns, previous.st_mtime_ns))
        return index

    monkeypatch.setattr(dashboard, "_load_persistent_ledger_index", drift_during_load)
    with pytest.raises(OSError, match="during index handoff"):
        _read(path)
    observed = dashboard._LEDGER_SESSION_INDEX_CACHE[(path.resolve(), True)]
    assert observed.facts_signature == dashboard.metadata_signature(previous)
    monkeypatch.setattr(dashboard, "_load_persistent_ledger_index", original)
    assert _read(path) == [{"session_date": DAY, "value": 2}]
